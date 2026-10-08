"""持久化存储：基于 SQLite 的只追加事件账本。

设计要点：
- grants 记录每个 (分析者, 数据集) 的预算上限；consumed 完全由只追加的
  events 表回放得出，不存在可被覆盖的"当前余额"字段；
- 每次 consume 在单写锁 + BEGIN IMMEDIATE 的一个事务内完成多数据集扣减，
  任一步失败即 ROLLBACK，绝不留下部分扣减；
- 重新打开库时回放事件，校验序列连续、无重复、累计不越上限；
  任何倒退/缺口都抛 LedgerRollbackError，拒绝在可疑状态上继续记账。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
from collections.abc import Callable, Iterable
from pathlib import Path

from ledger.errors import (
    AnalystNotFoundError,
    BudgetInsufficientError,
    DatasetNotFoundError,
    InvalidParameterError,
    LedgerRollbackError,
    QueryConflictError,
    ReservationConflictError,
    ReservationExpiredError,
    ReservationNotFoundError,
    ReservationStateError,
    UnauthorizedDatasetError,
)
from ledger.models import (
    BalanceView,
    ChargeResult,
    ConsumeResult,
    ReservationHold,
    ReservationItem,
    ReservationView,
    ReserveResult,
    ReleaseResult,
    SettleResult,
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS analysts (
    analyst_id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS datasets (
    dataset_id  TEXT PRIMARY KEY,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS grants (
    analyst_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    budget_limit REAL NOT NULL CHECK (budget_limit >= 0),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (analyst_id, dataset_id),
    FOREIGN KEY (analyst_id) REFERENCES analysts(analyst_id),
    FOREIGN KEY (dataset_id)  REFERENCES datasets(dataset_id)
);
CREATE TABLE IF NOT EXISTS events (
    seq        INTEGER PRIMARY KEY,
    query_id   TEXT NOT NULL,
    analyst_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    amount     REAL NOT NULL CHECK (amount > 0),
    event_type TEXT NOT NULL DEFAULT 'direct'
                   CHECK (event_type IN ('direct','settle')),
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (analyst_id) REFERENCES analysts(analyst_id),
    FOREIGN KEY (dataset_id)  REFERENCES datasets(dataset_id)
);
-- 直接扣减按 (query_id, dataset_id) 幂等
CREATE UNIQUE INDEX IF NOT EXISTS uq_events_direct
    ON events(query_id, dataset_id) WHERE event_type = 'direct';
-- 预占结算按 (预占标识=query_id, dataset_id) 幂等，与直接扣减互不干扰
CREATE UNIQUE INDEX IF NOT EXISTS uq_events_settle
    ON events(query_id, dataset_id) WHERE event_type = 'settle';
CREATE INDEX IF NOT EXISTS idx_events_pair ON events(analyst_id, dataset_id, seq);
CREATE INDEX IF NOT EXISTS idx_events_query ON events(query_id);

-- 预占单：先占住额度，稍后按实际用量结算或整笔归还
CREATE TABLE IF NOT EXISTS reservations (
    reservation_id TEXT PRIMARY KEY,
    analyst_id     TEXT NOT NULL,
    status         TEXT NOT NULL CHECK (status IN ('held','settled','released','expired')),
    created_at     REAL NOT NULL,          -- epoch 秒（UTC）
    expires_at     REAL NOT NULL,          -- epoch 秒（UTC）
    finalized_at   REAL,                  -- 结算/归还/过期生效时刻
    replay_digest  TEXT,                  -- 首次结算/归还的内容指纹，用于幂等一致性
    FOREIGN KEY (analyst_id) REFERENCES analysts(analyst_id)
);
CREATE INDEX IF NOT EXISTS idx_reservations_analyst ON reservations(analyst_id);

CREATE TABLE IF NOT EXISTS reservation_items (
    reservation_id TEXT NOT NULL,
    dataset_id     TEXT NOT NULL,
    amount         REAL NOT NULL CHECK (amount > 0),   -- 预占额度
    settled        REAL NOT NULL DEFAULT 0 CHECK (settled >= 0),
    PRIMARY KEY (reservation_id, dataset_id),
    FOREIGN KEY (reservation_id) REFERENCES reservations(reservation_id),
    FOREIGN KEY (dataset_id)     REFERENCES datasets(dataset_id)
);
"""


class LedgerStore:
    """SQLite 后端的账本存储，进程内写操作串行化。"""

    def __init__(
        self,
        db_path: str | Path = ":memory:",
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._db_path = str(db_path)
        self._write_lock = threading.RLock()
        self._clock = clock
        self._conn: sqlite3.Connection | None = None

    # ------------------------------------------------------------------ #
    # 生命周期
    # ------------------------------------------------------------------ #
    def open(self) -> "LedgerStore":
        if self._conn is not None:
            return self
        conn = sqlite3.connect(
            self._db_path, check_same_thread=False, isolation_level=None
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 5000")
        if self._db_path != ":memory:":
            conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA foreign_keys = OFF")
        self._migrate_legacy_schema(conn)
        conn.execute("PRAGMA foreign_keys = ON")
        conn.executescript(_SCHEMA)
        self._conn = conn
        # 重启后先把停机期间已到期的预占置为 expired（仅状态迁移，释放占用；
        # 到期预占从不含在 active reserved 口径内，故不会凭空增减额度）
        self.sweep_expired()
        self._verify_integrity_on_load()
        return self

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "LedgerStore":
        return self.open()

    def __exit__(self, *exc: object) -> None:
        self.close()

    @staticmethod
    def _migrate_legacy_schema(conn: sqlite3.Connection) -> None:
        """旧版 events 表没有 event_type 且带表级 UNIQUE：原地重建为新结构。

        既有事件全部是直接扣减（event_type='direct'），seq 顺序与金额原样保留。
        """
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(events)")}
        if not cols or "event_type" in cols:
            return
        conn.executescript(
            """
            ALTER TABLE events RENAME TO events_legacy;
            CREATE TABLE events (
                seq        INTEGER PRIMARY KEY,
                query_id   TEXT NOT NULL,
                analyst_id TEXT NOT NULL,
                dataset_id TEXT NOT NULL,
                amount     REAL NOT NULL CHECK (amount > 0),
                event_type TEXT NOT NULL DEFAULT 'direct'
                               CHECK (event_type IN ('direct','settle')),
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            INSERT INTO events(seq, query_id, analyst_id, dataset_id, amount,
                               event_type, created_at)
            SELECT seq, query_id, analyst_id, dataset_id, amount,
                   'direct', created_at FROM events_legacy;
            DROP TABLE events_legacy;
            """
        )

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("LedgerStore 尚未 open()")
        return self._conn

    # ------------------------------------------------------------------ #
    # 基础数据维护
    # ------------------------------------------------------------------ #
    def create_analyst(self, analyst_id: str) -> None:
        with self._write_lock:
            self.conn.execute(
                "INSERT OR IGNORE INTO analysts(analyst_id) VALUES (?)",
                (analyst_id,),
            )

    def analyst_exists(self, analyst_id: str) -> bool:
        with self._write_lock:
            row = self.conn.execute(
                "SELECT 1 FROM analysts WHERE analyst_id = ?", (analyst_id,)
            ).fetchone()
        return row is not None

    def create_dataset(self, dataset_id: str) -> None:
        with self._write_lock:
            self.conn.execute(
                "INSERT OR IGNORE INTO datasets(dataset_id) VALUES (?)",
                (dataset_id,),
            )

    def dataset_exists(self, dataset_id: str) -> bool:
        with self._write_lock:
            row = self.conn.execute(
                "SELECT 1 FROM datasets WHERE dataset_id = ?", (dataset_id,)
            ).fetchone()
        return row is not None

    def grant_budget(
        self, analyst_id: str, dataset_id: str, limit: float
    ) -> None:
        """授权或上调预算上限。

        重复授权只允许"不变或上调"；下调上限属于账本倒退，直接拒绝。
        """
        if not self.analyst_exists(analyst_id):
            raise AnalystNotFoundError(
                f"分析者不存在: {analyst_id}",
                details={"analyst_id": analyst_id},
            )
        if not self.dataset_exists(dataset_id):
            raise DatasetNotFoundError(
                f"数据集不存在: {dataset_id}",
                details={"dataset_id": dataset_id},
            )
        with self._write_lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                row = self.conn.execute(
                    "SELECT budget_limit FROM grants "
                    "WHERE analyst_id = ? AND dataset_id = ?",
                    (analyst_id, dataset_id),
                ).fetchone()
                if row is None:
                    self.conn.execute(
                        "INSERT INTO grants(analyst_id, dataset_id, budget_limit) "
                        "VALUES (?, ?, ?)",
                        (analyst_id, dataset_id, limit),
                    )
                elif limit < row["budget_limit"]:
                    raise LedgerRollbackError(
                        "拒绝下调已授权预算上限（账本不可倒退）",
                        details={
                            "analyst_id": analyst_id,
                            "dataset_id": dataset_id,
                            "existing_limit": row["budget_limit"],
                            "requested_limit": limit,
                        },
                    )
                else:
                    self.conn.execute(
                        "UPDATE grants SET budget_limit = ?, "
                        "updated_at = datetime('now') "
                        "WHERE analyst_id = ? AND dataset_id = ?",
                        (limit, analyst_id, dataset_id),
                    )
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

    def is_authorized(self, analyst_id: str, dataset_id: str) -> bool:
        with self._write_lock:
            row = self.conn.execute(
                "SELECT 1 FROM grants WHERE analyst_id = ? AND dataset_id = ?",
                (analyst_id, dataset_id),
            ).fetchone()
        return row is not None

    # ------------------------------------------------------------------ #
    # 查询
    # ------------------------------------------------------------------ #
    def get_balance(self, analyst_id: str, dataset_id: str) -> BalanceView | None:
        with self._write_lock:
            now = self._clock()
            row = self.conn.execute(
                """
                SELECT g.budget_limit AS lim,
                       COALESCE((SELECT SUM(e.amount) FROM events e
                                 WHERE e.analyst_id = g.analyst_id
                                   AND e.dataset_id = g.dataset_id), 0) AS consumed,
                       COALESCE((SELECT SUM(ri.amount)
                                 FROM reservation_items ri
                                 JOIN reservations r ON r.reservation_id = ri.reservation_id
                                 WHERE r.analyst_id = g.analyst_id
                                   AND ri.dataset_id = g.dataset_id
                                   AND r.status = 'held' AND r.expires_at > ?), 0) AS reserved
                FROM grants g
                WHERE g.analyst_id = ? AND g.dataset_id = ?
                """,
                (now, analyst_id, dataset_id),
            ).fetchone()
        if row is None:
            return None
        consumed = float(row["consumed"])
        limit = float(row["lim"])
        reserved = float(row["reserved"])
        return BalanceView(
            analyst_id=analyst_id,
            dataset_id=dataset_id,
            limit=limit,
            consumed=consumed,
            balance=limit - consumed,
            reserved=reserved,
        )

    def list_balances(self, analyst_id: str) -> list[BalanceView]:
        with self._write_lock:
            now = self._clock()
            rows = self.conn.execute(
            """
            SELECT g.dataset_id AS dataset_id, g.budget_limit AS lim,
                   COALESCE((SELECT SUM(e.amount) FROM events e
                             WHERE e.analyst_id = g.analyst_id
                               AND e.dataset_id = g.dataset_id), 0) AS consumed,
                   COALESCE((SELECT SUM(ri.amount)
                             FROM reservation_items ri
                             JOIN reservations r ON r.reservation_id = ri.reservation_id
                             WHERE r.analyst_id = g.analyst_id
                               AND ri.dataset_id = g.dataset_id
                               AND r.status = 'held' AND r.expires_at > ?), 0) AS reserved
            FROM grants g WHERE g.analyst_id = ?
            ORDER BY g.dataset_id
            """,
            (now, analyst_id),
        ).fetchall()
        return [
            BalanceView(
                analyst_id=analyst_id,
                dataset_id=r["dataset_id"],
                limit= float(r["lim"]),
                consumed=float(r["consumed"]),
                balance=float(r["lim"]) - float(r["consumed"]),
                reserved=float(r["reserved"]),
            )
            for r in rows
        ]

    # ------------------------------------------------------------------ #
    # 记账（核心）
    # ------------------------------------------------------------------ #
    def consume(
        self,
        *,
        query_id: str,
        analyst_id: str,
        items: Iterable[tuple[str, float]],
    ) -> ConsumeResult:
        """原子地完成一次（可能跨多数据集的）组合扣减。

        判定顺序（全部在一个写事务内）：
        1. 分析者/数据集是否存在、是否已授权；
        2. query_id 是否已有成功记录：有则校验请求是否一致，
           一致返回幂等重放，不一致返回 query_conflict；
        3. 每个数据集余额是否充足，任一不足则整体失败；
        4. 一次性写入全部事件并提交。
        """
        items = list(items)
        with self._write_lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                self._sweep_expired_tx()
                result = self._consume_tx(query_id, analyst_id, items)
                self.conn.execute("COMMIT")
                return result
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

    def _consume_tx(
        self, query_id: str, analyst_id: str, items: list[tuple[str, float]]
    ) -> ConsumeResult:
        conn = self.conn

        # 1) 实体与授权
        if not self.analyst_exists(analyst_id):
            raise AnalystNotFoundError(
                f"分析者不存在: {analyst_id}",
                details={"analyst_id": analyst_id, "query_id": query_id},
            )
        for dataset_id, _cost in items:
            if not self.dataset_exists(dataset_id):
                raise DatasetNotFoundError(
                    f"数据集不存在: {dataset_id}",
                    details={
                        "dataset_id": dataset_id,
                        "analyst_id": analyst_id,
                        "query_id": query_id,
                    },
                )
            if not self.is_authorized(analyst_id, dataset_id):
                raise UnauthorizedDatasetError(
                    f"分析者 {analyst_id} 未被授权访问数据集 {dataset_id}",
                    details={
                        "analyst_id": analyst_id,
                        "dataset_id": dataset_id,
                        "query_id": query_id,
                    },
                )

        # 2) 幂等：同一 query_id 的成功记录（只看直接扣减，settle 事件互不干扰）
        existing = conn.execute(
            "SELECT analyst_id, dataset_id, amount FROM events "
            "WHERE query_id = ? AND event_type = 'direct' ORDER BY dataset_id",
            (query_id,),
        ).fetchall()
        if existing:
            if any(r["analyst_id"] != analyst_id for r in existing):
                raise QueryConflictError(
                    f"查询标识 {query_id} 已属于其他分析者",
                    details={
                        "query_id": query_id,
                        "requested_analyst": analyst_id,
                        "existing_analyst": existing[0]["analyst_id"],
                    },
                )
            existing_map = {r["dataset_id"]: float(r["amount"]) for r in existing}
            requested_map = {d: c for d, c in items}
            if existing_map != requested_map:
                raise QueryConflictError(
                    f"查询标识 {query_id} 已存在但扣减内容不一致",
                    details={
                        "query_id": query_id,
                        "existing": existing_map,
                        "requested": requested_map,
                    },
                )
            return self._load_consume_result(query_id, analyst_id, replay=True)

        # 3) 余额充足性（任一不足整体失败）：已扣减 + 未过期预占 + 本次扣减 ≤ 上限
        balances: dict[str, BalanceView] = {}
        for dataset_id, cost in items:
            bal = self.get_balance(analyst_id, dataset_id)
            assert bal is not None  # 上面已校验授权
            if bal.consumed + bal.reserved + cost > bal.limit + 1e-9:
                raise BudgetInsufficientError(
                    f"数据集 {dataset_id} 可用额度不足: "
                    f"需 {cost}, 已消耗 {bal.consumed}, 预占中 {bal.reserved}, "
                    f"可用 {bal.available}",
                    details={
                        "query_id": query_id,
                        "analyst_id": analyst_id,
                        "dataset_id": dataset_id,
                        "requested_cost": cost,
                        "limit": bal.limit,
                        "consumed": bal.consumed,
                        "reserved": bal.reserved,
                        "balance": bal.balance,
                        "available": bal.available,
                    },
                )
            balances[dataset_id] = bal

        # 4) 写入事件（单事务批量插入，seq 连续）
        next_seq_row = conn.execute("SELECT COALESCE(MAX(seq), 0) + 1 AS n FROM events")
        next_seq = int(next_seq_row.fetchone()["n"])
        results: list[ChargeResult] = []
        for offset, (dataset_id, cost) in enumerate(items):
            conn.execute(
                "INSERT INTO events(seq, query_id, analyst_id, dataset_id, amount, "
                "event_type) VALUES (?, ?, ?, ?, ?, 'direct')",
                (next_seq + offset, query_id, analyst_id, dataset_id, cost),
            )
            before = balances[dataset_id].balance
            results.append(
                ChargeResult(
                    dataset_id=dataset_id,
                    charged=cost,
                    balance_before=before,
                    balance_after=before - cost,
                )
            )
        return self._to_result(query_id, analyst_id, False, results)

    def get_consume_record(
        self, query_id: str, analyst_id: str | None = None
    ) -> ConsumeResult | None:
        with self._write_lock:
            clause = "WHERE query_id = ? AND event_type = 'direct'"
            params: list[object] = [query_id]
            if analyst_id is not None:
                clause += " AND analyst_id = ?"
                params.append(analyst_id)
            rows = self.conn.execute(
                f"SELECT DISTINCT analyst_id FROM events {clause}", params
            ).fetchall()
            if not rows:
                return None
            return self._load_consume_result(query_id, rows[0]["analyst_id"], replay=True)

    # ------------------------------------------------------------------ #
    # 预占 reservation（先占额度、后结算）
    # ------------------------------------------------------------------ #
    def reserve(
        self,
        *,
        reservation_id: str,
        analyst_id: str,
        items: Iterable[tuple[str, float]],
        ttl_seconds: float,
    ) -> ReserveResult:
        """原子地预占（可跨多数据集）：要么全部占上，要么整体不生效。

        同一 reservation_id 重复提交：分析者与各数据集预占额一致时幂等重放
        （返回与首次相同的 expires_at）；不一致 -> reservation_conflict。
        """
        items = list(items)
        with self._write_lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                self._sweep_expired_tx()
                result = self._reserve_tx(reservation_id, analyst_id, items, ttl_seconds)
                self.conn.execute("COMMIT")
                return result
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

    def _reserve_tx(
        self,
        reservation_id: str,
        analyst_id: str,
        items: list[tuple[str, float]],
        ttl_seconds: float,
    ) -> ReserveResult:
        conn = self.conn

        # 1) 实体与授权（与 consume 同一判定顺序）
        self._require_analyst_datasets_authorized(reservation_id, analyst_id, items)

        # 2) 幂等/冲突
        head = conn.execute(
            "SELECT analyst_id, status, expires_at FROM reservations "
            "WHERE reservation_id = ?",
            (reservation_id,),
        ).fetchone()
        if head is not None:
            if head["analyst_id"] != analyst_id:
                raise ReservationConflictError(
                    f"预占标识 {reservation_id} 已属于其他分析者",
                    details={
                        "reservation_id": reservation_id,
                        "requested_analyst": analyst_id,
                        "existing_analyst": head["analyst_id"],
                    },
                )
            existing_items = self._reservation_item_map(reservation_id)
            requested_map = dict(items)
            if existing_items != requested_map:
                raise ReservationConflictError(
                    f"预占标识 {reservation_id} 已存在但预占内容不一致",
                    details={
                        "reservation_id": reservation_id,
                        "existing": existing_items,
                        "requested": requested_map,
                    },
                )
            # 完全一致 -> 幂等重放首次结论（expires_at 不随 ttl 参数改变）
            return self._load_reserve_result(reservation_id, analyst_id, replay=True)

        # head is None：不存在（此前未成功过；失败的请求不占标识）

        # 3) 统一口径下的可用额度：已扣减 + 未过期预占 + 本次预占 ≤ 上限
        holds: list[ReservationHold] = []
        now = self._clock()
        expires_at = now + ttl_seconds
        for dataset_id, amount in items:
            bal = self.get_balance(analyst_id, dataset_id)
            assert bal is not None
            if bal.consumed + bal.reserved + amount > bal.limit + 1e-9:
                raise BudgetInsufficientError(
                    f"数据集 {dataset_id} 可预占额度不足: 需 {amount}, "
                    f"已消耗 {bal.consumed}, 预占中 {bal.reserved}, 可用 {bal.available}",
                    details={
                        "reservation_id": reservation_id,
                        "analyst_id": analyst_id,
                        "dataset_id": dataset_id,
                        "requested_amount": amount,
                        "limit": bal.limit,
                        "consumed": bal.consumed,
                        "reserved": bal.reserved,
                        "available": bal.available,
                    },
                )
            holds.append(
                ReservationHold(
                    dataset_id=dataset_id,
                    reserved=amount,
                    settled=0.0,
                    refunded=0.0,
                    balance_before=bal.available,
                    balance_after=bal.available - amount,
                )
            )

        # 4) 写入预占单与明细（单事务批量插入，任一步失败整体回滚）
        conn.execute(
            "INSERT INTO reservations(reservation_id, analyst_id, status, "
            "created_at, expires_at) VALUES (?, ?, 'held', ?, ?)",
            (reservation_id, analyst_id, now, expires_at),
        )
        for dataset_id, amount in items:
            conn.execute(
                "INSERT INTO reservation_items(reservation_id, dataset_id, amount) "
                "VALUES (?, ?, ?)",
                (reservation_id, dataset_id, amount),
            )
        return ReserveResult(
            reservation_id=reservation_id,
            analyst_id=analyst_id,
            replay=False,
            expires_at=_iso(expires_at),
            items=tuple(holds),
        )

    def settle_reservation(
        self,
        *,
        reservation_id: str,
        analyst_id: str,
        actual: Iterable[tuple[str, float]],
    ) -> SettleResult:
        """按实际用量结算：占用转为已扣减，多占部分退回；整体原子。

        重复提交且实际用量一致 -> 幂等重放首次结论；过期预占 -> reservation_expired；
        已结算/已归还再结算 -> reservation_state_invalid；内容不一致 -> conflict。
        """
        actual = list(actual)
        with self._write_lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                self._sweep_expired_tx()
                result = self._settle_tx(reservation_id, analyst_id, actual)
                self.conn.execute("COMMIT")
                return result
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

    def _settle_tx(
        self,
        reservation_id: str,
        analyst_id: str,
        actual: list[tuple[str, float]],
    ) -> SettleResult:
        conn = self.conn
        head = self._load_head_or_404(reservation_id, analyst_id)
        status = head["status"]
        now = self._clock()

        digest = _digest(actual)
        if status == "settled":
            if head["replay_digest"] != digest:
                raise ReservationConflictError(
                    f"预占 {reservation_id} 已结算，但本次实际用量与首次不一致",
                    details={
                        "reservation_id": reservation_id,
                        "submitted_digest": digest,
                        "existing_digest": head["replay_digest"],
                    },
                )
            return self._load_settle_result(reservation_id, analyst_id, replay=True)
        if status == "expired":
            raise ReservationExpiredError(
                f"预占 {reservation_id} 已过有效期并自动释放，不能再结算",
                details={
                    "reservation_id": reservation_id,
                    "expires_at": _iso(head["expires_at"]),
                    "now": _iso(now),
                },
            )
        if status in ("released",):
            raise ReservationStateError(
                f"预占 {reservation_id} 已整笔归还，不能再结算",
                details={"reservation_id": reservation_id, "status": status},
            )

        # status == held
        reserved_map = self._reservation_item_map(reservation_id)
        actual_map = dict(actual)
        unknown = sorted(set(actual_map) - set(reserved_map))
        if unknown:
            raise ReservationConflictError(
                f"结算包含不属于该预占的数据集: {unknown}",
                details={"reservation_id": reservation_id, "unknown_datasets": unknown},
            )
        # 结算口径：每个预占数据集都必须给出实际用量（0 表示该数据集全额退回）
        if set(actual_map) != set(reserved_map):
            missing = sorted(set(reserved_map) - set(actual_map))
            raise InvalidParameterError(
                "结算必须覆盖预占中的全部数据集（未用数据集请显式给 0）",
                details={"reservation_id": reservation_id, "missing_datasets": missing},
            )
        for dataset_id, used in actual_map.items():
            if used > reserved_map[dataset_id] + 1e-9:
                raise InvalidParameterError(
                    f"数据集 {dataset_id} 实际用量 {used} 超过预占 {reserved_map[dataset_id]}",
                    details={
                        "reservation_id": reservation_id,
                        "dataset_id": dataset_id,
                        "actual": used,
                        "reserved": reserved_map[dataset_id],
                    },
                )

        # 占用转扣减：写入 settle 事件（query_id 用 settle:<rid> 命名空间，
        # 与直接扣减互不冲突），更新明细 settled，并把预占单置为 settled
        next_seq_row = conn.execute("SELECT COALESCE(MAX(seq), 0) + 1 AS n FROM events")
        next_seq = int(next_seq_row.fetchone()["n"])
        # 结算前一次性快照：available_before 已包含本预占占用；
        # 结算后该预占不再占用，其中 settled 部分转为已扣减、refunded 部分退回
        snapshots = {
            d: self.get_balance(analyst_id, d) for d in reserved_map
        }
        holds: list[ReservationHold] = []
        for dataset_id, used in actual:
            reserved = reserved_map[dataset_id]
            refunded = reserved - used
            if used > 0:
                conn.execute(
                    "INSERT INTO events(seq, query_id, analyst_id, dataset_id, "
                    "amount, event_type) VALUES (?, ?, ?, ?, ?, 'settle')",
                    (next_seq, f"settle:{reservation_id}",
                     analyst_id, dataset_id, used),
                )
                next_seq += 1
            conn.execute(
                "UPDATE reservation_items SET settled = ? "
                "WHERE reservation_id = ? AND dataset_id = ?",
                (used, reservation_id, dataset_id),
            )
            snap = snapshots[dataset_id]
            assert snap is not None
            holds.append(
                ReservationHold(
                    dataset_id=dataset_id,
                    reserved=reserved,
                    settled=used,
                    refunded=refunded,
                    balance_before=snap.available,
                    balance_after=snap.available + refunded,
                )
            )
        conn.execute(
            "UPDATE reservations SET status = 'settled', finalized_at = ?, "
            "replay_digest = ? WHERE reservation_id = ?",
            (now, digest, reservation_id),
        )
        return SettleResult(
            reservation_id=reservation_id,
            analyst_id=analyst_id,
            replay=False,
            total_settled=sum(h.settled for h in holds),
            total_refunded=sum(h.refunded for h in holds),
            items=tuple(holds),
        )

    def release_reservation(
        self,
        *,
        reservation_id: str,
        analyst_id: str,
    ) -> ReleaseResult:
        """整笔归还未结算的预占；可重复提交只生效一次。

        已过期 -> reservation_expired（明确告知占用已由系统自动释放）；
        已结算 -> reservation_state_invalid；重复归还 -> 幂等重放首次结论。
        """
        with self._write_lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                self._sweep_expired_tx()
                result = self._release_tx(reservation_id, analyst_id)
                self.conn.execute("COMMIT")
                return result
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

    def _release_tx(self, reservation_id: str, analyst_id: str) -> ReleaseResult:
        head = self._load_head_or_404(reservation_id, analyst_id)
        status = head["status"]
        now = self._clock()

        if status == "released":
            return self._load_release_result(reservation_id, analyst_id, replay=True)
        if status == "expired":
            raise ReservationExpiredError(
                f"预占 {reservation_id} 已过期并由系统自动释放，无需再归还",
                details={
                    "reservation_id": reservation_id,
                    "expires_at": _iso(head["expires_at"]),
                    "now": _iso(now),
                },
            )
        if status == "settled":
            raise ReservationStateError(
                f"预占 {reservation_id} 已结算，不能整笔归还",
                details={"reservation_id": reservation_id, "status": status},
            )

        # held -> released：仅状态迁移，占用即从 reserved 口径移除（额度退回）
        reserved_map = self._reservation_item_map(reservation_id)
        snapshots = {
            d: self.get_balance(analyst_id, d) for d in reserved_map
        }
        conn = self.conn
        conn.execute(
            "UPDATE reservations SET status = 'released', finalized_at = ?, "
            "replay_digest = ? WHERE reservation_id = ?",
            (now, _digest([]), reservation_id),
        )
        holds = [
            ReservationHold(
                dataset_id=dataset_id,
                reserved=amount,
                settled=0.0,
                refunded=amount,
                balance_before=snapshots[dataset_id].available,
                balance_after=snapshots[dataset_id].available + amount,
            )
            for dataset_id, amount in sorted(reserved_map.items())
        ]
        return ReleaseResult(
            reservation_id=reservation_id,
            analyst_id=analyst_id,
            replay=False,
            status="released",
            total_refunded=sum(h.refunded for h in holds),
            items=tuple(holds),
        )

    def get_reservation(
        self, reservation_id: str, analyst_id: str | None = None
    ) -> ReservationView | None:
        with self._write_lock:
            self._sweep_expired_tx()
            clause = "WHERE reservation_id = ?"
            params: list[object] = [reservation_id]
            if analyst_id is not None:
                clause += " AND analyst_id = ?"
                params.append(analyst_id)
            head = self.conn.execute(
                f"SELECT analyst_id, status, created_at, expires_at "
                f"FROM reservations {clause}",
                params,
            ).fetchone()
            if head is None:
                return None
            rows = self.conn.execute(
                "SELECT dataset_id, amount FROM reservation_items "
                "WHERE reservation_id = ? ORDER BY dataset_id",
                (reservation_id,),
            ).fetchall()
        return ReservationView(
            reservation_id=reservation_id,
            analyst_id=head["analyst_id"],
            status=head["status"],
            created_at=_iso(head["created_at"]),
            expires_at=_iso(head["expires_at"]),
            items=tuple(
                ReservationItem(dataset_id=r["dataset_id"], amount=float(r["amount"]))
                for r in rows
            ),
        )

    def sweep_expired(self) -> int:
        """把已过有效期的 held 预占置为 expired 并释放占用，返回处理条数。"""
        with self._write_lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                n = self._sweep_expired_tx()
                self.conn.execute("COMMIT")
                return n
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

    def _sweep_expired_tx(self) -> int:
        """事务内过期清理：held 且 expires_at <= now -> expired。"""
        now = self._clock()
        cur = self.conn.execute(
            "UPDATE reservations SET status = 'expired', finalized_at = ? "
            "WHERE status = 'held' AND expires_at <= ?",
            (now, now),
        )
        return cur.rowcount if cur.rowcount != -1 else 0

    # ------------------------------------------------------------------ #
    # 预占内部辅助
    # ------------------------------------------------------------------ #
    def _require_analyst_datasets_authorized(
        self,
        request_id: str,
        analyst_id: str,
        items: list[tuple[str, float]],
    ) -> None:
        if not self.analyst_exists(analyst_id):
            raise AnalystNotFoundError(
                f"分析者不存在: {analyst_id}",
                details={"analyst_id": analyst_id, "reservation_id": request_id},
            )
        for dataset_id, _amount in items:
            if not self.dataset_exists(dataset_id):
                raise DatasetNotFoundError(
                    f"数据集不存在: {dataset_id}",
                    details={
                        "dataset_id": dataset_id,
                        "analyst_id": analyst_id,
                        "reservation_id": request_id,
                    },
                )
            if not self.is_authorized(analyst_id, dataset_id):
                raise UnauthorizedDatasetError(
                    f"分析者 {analyst_id} 未被授权访问数据集 {dataset_id}",
                    details={
                        "analyst_id": analyst_id,
                        "dataset_id": dataset_id,
                        "reservation_id": request_id,
                    },
                )

    def _load_head_or_404(self, reservation_id: str, analyst_id: str) -> sqlite3.Row:
        head = self.conn.execute(
            "SELECT analyst_id, status, expires_at, replay_digest "
            "FROM reservations WHERE reservation_id = ?",
            (reservation_id,),
        ).fetchone()
        if head is None:
            raise ReservationNotFoundError(
                f"预占不存在: {reservation_id}",
                details={"reservation_id": reservation_id},
            )
        if head["analyst_id"] != analyst_id:
            # 不向他人透露存在性细节，统一按不存在处理
            raise ReservationNotFoundError(
                f"预占不存在: {reservation_id}",
                details={"reservation_id": reservation_id, "analyst_id": analyst_id},
            )
        return head

    def _reservation_item_map(self, reservation_id: str) -> dict[str, float]:
        rows = self.conn.execute(
            "SELECT dataset_id, amount FROM reservation_items "
            "WHERE reservation_id = ? ORDER BY dataset_id",
            (reservation_id,),
        ).fetchall()
        return {r["dataset_id"]: float(r["amount"]) for r in rows}

    def _load_reserve_result(
        self, reservation_id: str, analyst_id: str, replay: bool
    ) -> ReserveResult:
        head = self.conn.execute(
            "SELECT expires_at FROM reservations WHERE reservation_id = ?",
            (reservation_id,),
        ).fetchone()
        items = [
            ReservationHold(
                dataset_id=r["dataset_id"],
                reserved=float(r["amount"]),
                settled=0.0,
                refunded=0.0,
                balance_before=0.0,
                balance_after=0.0,
            )
            for r in self.conn.execute(
                "SELECT dataset_id, amount FROM reservation_items "
                "WHERE reservation_id = ? ORDER BY dataset_id",
                (reservation_id,),
            )
        ]
        return ReserveResult(
            reservation_id=reservation_id,
            analyst_id=analyst_id,
            replay=replay,
            expires_at=_iso(head["expires_at"]),
            items=tuple(items),
        )

    def _load_settle_result(
        self, reservation_id: str, analyst_id: str, replay: bool
    ) -> SettleResult:
        rows = self.conn.execute(
            "SELECT dataset_id, amount, settled FROM reservation_items "
            "WHERE reservation_id = ? ORDER BY dataset_id",
            (reservation_id,),
        ).fetchall()
        items = [
            ReservationHold(
                dataset_id=r["dataset_id"],
                reserved=float(r["amount"]),
                settled=float(r["settled"]),
                refunded=float(r["amount"]) - float(r["settled"]),
                balance_before=0.0,
                balance_after=0.0,
            )
            for r in rows
        ]
        return SettleResult(
            reservation_id=reservation_id,
            analyst_id=analyst_id,
            replay=replay,
            total_settled=sum(i.settled for i in items),
            total_refunded=sum(i.refunded for i in items),
            items=tuple(items),
        )

    def _load_release_result(
        self, reservation_id: str, analyst_id: str, replay: bool
    ) -> ReleaseResult:
        rows = self.conn.execute(
            "SELECT dataset_id, amount FROM reservation_items "
            "WHERE reservation_id = ? ORDER BY dataset_id",
            (reservation_id,),
        ).fetchall()
        items = [
            ReservationHold(
                dataset_id=r["dataset_id"],
                reserved=float(r["amount"]),
                settled=0.0,
                refunded=float(r["amount"]),
                balance_before=0.0,
                balance_after=0.0,
            )
            for r in rows
        ]
        return ReleaseResult(
            reservation_id=reservation_id,
            analyst_id=analyst_id,
            replay=replay,
            status="released",
            total_refunded=sum(i.refunded for i in items),
            items=tuple(items),
        )

    # ------------------------------------------------------------------ #
    # 内部辅助
    # ------------------------------------------------------------------ #
    def _load_consume_result(
        self, query_id: str, analyst_id: str, replay: bool
    ) -> ConsumeResult:
        """按 seq 顺序重建该 query_id 每条扣减发生时的前后余额。"""
        rows = self.conn.execute(
            """
            SELECT ev.dataset_id AS dataset_id, ev.amount AS amount,
                   ev.seq AS seq,
                   g.budget_limit AS lim,
                   COALESCE((SELECT SUM(e.amount) FROM events e
                             WHERE e.analyst_id = ev.analyst_id
                               AND e.dataset_id = ev.dataset_id
                               AND e.seq < ev.seq), 0) AS consumed_before
            FROM events ev
            JOIN grants g
              ON g.analyst_id = ev.analyst_id AND g.dataset_id = ev.dataset_id
            WHERE ev.query_id = ? AND ev.analyst_id = ?
              AND ev.event_type = 'direct'
            ORDER BY ev.seq
            """,
            (query_id, analyst_id),
        ).fetchall()
        items = [
            ChargeResult(
                dataset_id=r["dataset_id"],
                charged=float(r["amount"]),
                balance_before=float(r["lim"]) - float(r["consumed_before"]),
                balance_after=float(r["lim"])
                - float(r["consumed_before"])
                - float(r["amount"]),
            )
            for r in rows
        ]
        return self._to_result(query_id, analyst_id, replay, items)

    def _verify_integrity_on_load(self) -> None:
        """回放全部事件，账本有任何倒退/缺口/越限都拒绝继续服务。"""
        conn = self.conn
        count = conn.execute("SELECT COUNT(*) AS c FROM events").fetchone()["c"]
        max_seq = conn.execute("SELECT COALESCE(MAX(seq), 0) AS m FROM events").fetchone()["m"]
        if count != max_seq:
            raise LedgerRollbackError(
                "事件序列不连续：疑似事件被删除或回滚，拒绝在该账本上继续记账",
                details={"event_count": count, "max_seq": max_seq},
            )
        expected = 1
        cumulative: dict[tuple[str, str], float] = {}
        for row in conn.execute(
            "SELECT seq, query_id, analyst_id, dataset_id, amount "
            "FROM events ORDER BY seq"
        ):
            if row["seq"] != expected:
                raise LedgerRollbackError(
                    f"事件序号缺口: 期望 {expected}, 实际 {row['seq']}",
                    details={"expected": expected, "actual": row["seq"]},
                )
            expected += 1
            key = (row["analyst_id"], row["dataset_id"])
            cumulative[key] = cumulative.get(key, 0.0) + float(row["amount"])
            grant = conn.execute(
                "SELECT budget_limit FROM grants "
                "WHERE analyst_id = ? AND dataset_id = ?",
                key,
            ).fetchone()
            if grant is None:
                raise LedgerRollbackError(
                    "存在无对应授权的扣减事件，账本状态非法",
                    details={"analyst_id": key[0], "dataset_id": key[1]},
                )
            if cumulative[key] > float(grant["budget_limit"]) + 1e-9:
                raise LedgerRollbackError(
                    "回放发现累计消耗超过授权上限，账本状态非法",
                    details={
                        "analyst_id": key[0],
                        "dataset_id": key[1],
                        "consumed": cumulative[key],
                        "limit": float(grant["budget_limit"]),
                    },
                )

        # 预占口径完整性：held 且未到期的占用 + 已扣减，任何时刻不得越过上限；
        # settled 明细不得超过当时的预占额（防止被外部篡改）。
        held_rows = conn.execute(
            """
            SELECT r.analyst_id AS analyst_id, ri.dataset_id AS dataset_id,
                   SUM(ri.amount) AS held_amount
            FROM reservations r
            JOIN reservation_items ri ON ri.reservation_id = r.reservation_id
            WHERE r.status = 'held' AND r.expires_at > ?
            GROUP BY r.analyst_id, ri.dataset_id
            """,
            (self._clock(),),
        ).fetchall()
        for row in held_rows:
            key = (row["analyst_id"], row["dataset_id"])
            grant = conn.execute(
                "SELECT budget_limit FROM grants "
                "WHERE analyst_id = ? AND dataset_id = ?",
                key,
            ).fetchone()
            used = cumulative.get(key, 0.0)
            held_amount = float(row["held_amount"])
            if grant is None or used + held_amount > float(grant["budget_limit"]) + 1e-9:
                raise LedgerRollbackError(
                    "回放发现已扣减与未过期预占之和超过授权上限，账本状态非法",
                    details={
                        "analyst_id": key[0],
                        "dataset_id": key[1],
                        "consumed": used,
                        "reserved": held_amount,
                        "limit": None if grant is None else float(grant["budget_limit"]),
                    },
                )
        bad_item = conn.execute(
            "SELECT reservation_id, dataset_id, amount, settled "
            "FROM reservation_items WHERE settled > amount + 1e-9 LIMIT 1"
        ).fetchone()
        if bad_item is not None:
            raise LedgerRollbackError(
                "存在结算额超过预占额的预占明细，账本状态非法",
                details={
                    "reservation_id": bad_item["reservation_id"],
                    "dataset_id": bad_item["dataset_id"],
                    "amount": float(bad_item["amount"]),
                    "settled": float(bad_item["settled"]),
                },
            )

    @staticmethod
    def _to_result(
        query_id: str,
        analyst_id: str,
        replay: bool,
        rows: Iterable[ChargeResult],
    ) -> ConsumeResult:
        items = tuple(rows)
        return ConsumeResult(
            query_id=query_id,
            analyst_id=analyst_id,
            replay=replay,
            total_charged=sum(i.charged for i in items),
            items=items,
        )


def _iso(epoch_seconds: float) -> str:
    """epoch 秒 -> UTC ISO8601 字符串。"""
    return time.strftime(
        "%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(epoch_seconds))
    )


def _digest(items: list[tuple[str, float]]) -> str:
    """对 (数据集, 金额) 列表做稳定指纹，用于幂等一致性判定。"""
    payload = json.dumps(
        sorted((d, round(float(a), 9)) for d, a in items),
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
