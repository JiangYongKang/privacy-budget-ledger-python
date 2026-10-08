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

import sqlite3
import threading
import time
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from pathlib import Path


def _iso(epoch_seconds: float) -> str:
    """epoch 秒 -> UTC ISO 字符串，便于返回与日志阅读。"""
    return datetime.fromtimestamp(epoch_seconds, tz=UTC).isoformat()

from ledger.errors import (
    AnalystNotFoundError,
    BudgetInsufficientError,
    DatasetNotFoundError,
    InvalidParameterError,
    LedgerRollbackError,
    QueryConflictError,
    ReservationClosedError,
    ReservationConflictError,
    ReservationExpiredError,
    ReservationNotFoundError,
    UnauthorizedDatasetError,
)
from ledger.models import (
    BalanceView,
    ChargeResult,
    ConsumeResult,
    ReleaseResult,
    ReservationItem,
    ReservationResult,
    SettleItem,
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
    ref_type   TEXT NOT NULL DEFAULT 'consume',
    ref_id     TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (query_id, dataset_id),
    FOREIGN KEY (analyst_id) REFERENCES analysts(analyst_id),
    FOREIGN KEY (dataset_id)  REFERENCES datasets(dataset_id)
);
CREATE INDEX IF NOT EXISTS idx_events_pair ON events(analyst_id, dataset_id, seq);
CREATE INDEX IF NOT EXISTS idx_events_query ON events(query_id);
CREATE TABLE IF NOT EXISTS reservations (
    reservation_id TEXT PRIMARY KEY,
    analyst_id     TEXT NOT NULL,
    status         TEXT NOT NULL
                   CHECK (status IN ('held','settled','released','expired')),
    ttl_seconds    INTEGER NOT NULL CHECK (ttl_seconds > 0),
    created_at     REAL NOT NULL,
    expires_at     REAL NOT NULL,
    settled_at     REAL,
    closed_at      REAL,
    fingerprint    TEXT NOT NULL,
    FOREIGN KEY (analyst_id) REFERENCES analysts(analyst_id)
);
CREATE INDEX IF NOT EXISTS idx_reservations_expiry
    ON reservations(status, expires_at);
CREATE TABLE IF NOT EXISTS reservation_items (
    reservation_id TEXT NOT NULL,
    dataset_id     TEXT NOT NULL,
    amount         REAL NOT NULL CHECK (amount > 0),
    charged        REAL,
    refunded       REAL,
    available_before REAL,
    available_after  REAL,
    PRIMARY KEY (reservation_id, dataset_id),
    FOREIGN KEY (reservation_id) REFERENCES reservations(reservation_id),
    FOREIGN KEY (dataset_id) REFERENCES datasets(dataset_id)
);
CREATE INDEX IF NOT EXISTS idx_ri_pair
    ON reservation_items(dataset_id, reservation_id);
"""


class LedgerStore:
    """SQLite 后端的账本存储，进程内写操作串行化。"""

    def __init__(
        self,
        db_path: str | Path = ":memory:",
        *,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._db_path = str(db_path)
        self._write_lock = threading.RLock()
        self._conn: sqlite3.Connection | None = None
        self._clock = clock or time.time

    def now(self) -> float:
        """当前时间（epoch 秒）。可注入，便于本地快速验证有效期。"""
        return float(self._clock())

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
        conn.executescript(_SCHEMA)
        self._conn = conn
        self._migrate_legacy_events()
        # 打开库即把已到期的 held 预占翻为 expired（释放额度），再做完整性校验，
        # 保证校验基于与运行期一致的口径：只有未过期 held 预占占额度。
        self.sweep_expired()
        self._verify_integrity_on_load()
        return self

    def _migrate_legacy_events(self) -> None:
        """给旧库的 events 表补齐 ref_type/ref_id 列（只追加，不改动任何数据）。"""
        cols = {r["name"] for r in self.conn.execute("PRAGMA table_info(events)")}
        if "ref_type" not in cols:
            self.conn.execute(
                "ALTER TABLE events ADD COLUMN ref_type TEXT NOT NULL DEFAULT 'consume'"
            )
        if "ref_id" not in cols:
            self.conn.execute("ALTER TABLE events ADD COLUMN ref_id TEXT")

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "LedgerStore":
        return self.open()

    def __exit__(self, *exc: object) -> None:
        self.close()

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
    def _held_reserved_sql(self) -> str:
        """对某 (analyst, dataset) 上【仍有效】的预占求和的相关子查询片段。

        已到 expires_at 的 held 预占不再占额度（懒失效），其状态行会在
        下一次预占操作 / sweep / 访问该预占时翻成 expired。
        """
        return (
            "COALESCE((SELECT SUM(ri.amount) FROM reservation_items ri "
            "JOIN reservations r ON r.reservation_id = ri.reservation_id "
            "WHERE r.analyst_id = g.analyst_id AND ri.dataset_id = g.dataset_id "
            "AND r.status = 'held' AND r.expires_at > ?), 0)"
        )

    def get_balance(self, analyst_id: str, dataset_id: str) -> BalanceView | None:
        with self._write_lock:
            row = self.conn.execute(
                f"""
                SELECT g.budget_limit AS lim,
                       COALESCE((SELECT SUM(e.amount) FROM events e
                                 WHERE e.analyst_id = g.analyst_id
                                   AND e.dataset_id = g.dataset_id), 0) AS consumed,
                       {self._held_reserved_sql()} AS reserved
                FROM grants g
                WHERE g.analyst_id = ? AND g.dataset_id = ?
                """,
                (self.now(), analyst_id, dataset_id),
            ).fetchone()
        if row is None:
            return None
        return self._balance_from_row(analyst_id, dataset_id, row)

    def list_balances(self, analyst_id: str) -> list[BalanceView]:
        with self._write_lock:
            rows = self.conn.execute(
                f"""
                SELECT g.dataset_id AS dataset_id, g.budget_limit AS lim,
                       COALESCE((SELECT SUM(e.amount) FROM events e
                                 WHERE e.analyst_id = g.analyst_id
                                   AND e.dataset_id = g.dataset_id), 0) AS consumed,
                       {self._held_reserved_sql()} AS reserved
                FROM grants g WHERE g.analyst_id = ?
                ORDER BY g.dataset_id
                """,
                (self.now(), analyst_id),
            ).fetchall()
        return [
            self._balance_from_row(analyst_id, r["dataset_id"], r) for r in rows
        ]

    @staticmethod
    def _balance_from_row(analyst_id: str, dataset_id: str, row) -> BalanceView:
        consumed = float(row["consumed"])
        reserved = float(row["reserved"])
        limit = float(row["lim"])
        return BalanceView(
            analyst_id=analyst_id,
            dataset_id=dataset_id,
            limit=limit,
            consumed=consumed,
            balance=limit - consumed,
            reserved=reserved,
            available=limit - consumed - reserved,
        )

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

        # 2) 幂等：同一 query_id 的成功记录
        existing = conn.execute(
            "SELECT analyst_id, dataset_id, amount FROM events "
            "WHERE query_id = ? ORDER BY dataset_id",
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

        # 3) 额度充足性（任一不足整体失败）：
        #    口径为 已消耗 + 未过期预占 <= 上限，即按"可用额度"判定，
        #    预占与直接扣减混着并发时也不会越限。
        balances: dict[str, BalanceView] = {}
        for dataset_id, cost in items:
            bal = self.get_balance(analyst_id, dataset_id)
            assert bal is not None  # 上面已校验授权
            if bal.available < cost - 1e-9:
                raise BudgetInsufficientError(
                    f"数据集 {dataset_id} 额度不足: "
                    f"需 {cost}, 可用 {bal.available}"
                    f"（已消耗 {bal.consumed}, 预占中 {bal.reserved}）",
                    details={
                        "query_id": query_id,
                        "analyst_id": analyst_id,
                        "dataset_id": dataset_id,
                        "requested_cost": cost,
                        "limit": bal.limit,
                        "consumed": bal.consumed,
                        "reserved": bal.reserved,
                        "available": bal.available,
                        # 兼容旧口径：balance = limit - consumed
                        "balance": bal.balance,
                    },
                )
            balances[dataset_id] = bal

        # 4) 写入事件（单事务批量插入，seq 连续）
        next_seq_row = conn.execute("SELECT COALESCE(MAX(seq), 0) + 1 AS n FROM events")
        next_seq = int(next_seq_row.fetchone()["n"])
        results: list[ChargeResult] = []
        for offset, (dataset_id, cost) in enumerate(items):
            conn.execute(
                "INSERT INTO events(seq, query_id, analyst_id, dataset_id, amount) "
                "VALUES (?, ?, ?, ?, ?)",
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
            clause = "WHERE query_id = ?"
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

        # 预占一致性校验：
        # - 每条明细都必须能找到主表；
        # - 状态为 held 的预占必须未过期（打开库时已 sweep）；
        # - 任一时刻 已消耗 + 未过期 held 预占 不得超过授权上限；
        # - settled 预占的实扣/退回金额必须与明细自洽。
        held_total: dict[tuple[str, str], float] = {}
        for rrow in conn.execute(
            "SELECT reservation_id, analyst_id, status, expires_at, fingerprint "
            "FROM reservations"
        ):
            rid = rrow["reservation_id"]
            irows = conn.execute(
                "SELECT dataset_id, amount, charged, refunded "
                "FROM reservation_items WHERE reservation_id = ?",
                (rid,),
            ).fetchall()
            if not irows:
                raise LedgerRollbackError(
                    f"预占 {rid} 没有任何明细，账本状态非法",
                    details={"reservation_id": rid},
                )
            for ir in irows:
                grant = conn.execute(
                    "SELECT budget_limit FROM grants "
                    "WHERE analyst_id = ? AND dataset_id = ?",
                    (rrow["analyst_id"], ir["dataset_id"]),
                ).fetchone()
                if grant is None:
                    raise LedgerRollbackError(
                        f"预占 {rid} 存在无对应授权的数据集 {ir['dataset_id']}",
                        details={
                            "reservation_id": rid,
                            "analyst_id": rrow["analyst_id"],
                            "dataset_id": ir["dataset_id"],
                        },
                    )
                amount = float(ir["amount"])
                if rrow["status"] == "held":
                    if float(rrow["expires_at"]) <= self.now():
                        raise LedgerRollbackError(
                            f"预占 {rid} 已过期但状态仍为 held，账本状态非法",
                            details={"reservation_id": rid},
                        )
                    key = (rrow["analyst_id"], ir["dataset_id"])
                    held_total[key] = held_total.get(key, 0.0) + amount
                elif rrow["status"] == "settled":
                    charged = float(ir["charged"])
                    refunded = float(ir["refunded"])
                    if abs((charged + refunded) - amount) > 1e-9:
                        raise LedgerRollbackError(
                            f"预占 {rid} 结算金额与预占额不自洽",
                            details={
                                "reservation_id": rid,
                                "dataset_id": ir["dataset_id"],
                                "reserved": amount,
                                "charged": charged,
                                "refunded": refunded,
                            },
                        )
        for (analyst_id, dataset_id), reserved_sum in held_total.items():
            grant_row = conn.execute(
                "SELECT budget_limit FROM grants "
                "WHERE analyst_id = ? AND dataset_id = ?",
                (analyst_id, dataset_id),
            ).fetchone()
            used = cumulative.get((analyst_id, dataset_id), 0.0)
            limit = float(grant_row["budget_limit"])
            if used + reserved_sum > limit + 1e-9:
                raise LedgerRollbackError(
                    "回放发现 已消耗+有效预占 超过授权上限，账本状态非法",
                    details={
                        "analyst_id": analyst_id,
                        "dataset_id": dataset_id,
                        "consumed": used,
                        "reserved": reserved_sum,
                        "limit": limit,
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

    # ------------------------------------------------------------------ #
    # 预占 / 结算 / 归还
    # ------------------------------------------------------------------ #
    @staticmethod
    def _fingerprint(analyst_id: str, items: list[tuple[str, float]]) -> str:
        """对"分析者 + 各数据集金额"生成稳定指纹，用于标识冲突判定。"""
        body = ",".join(f"{d}={a:.10g}" for d, a in sorted(items))
        return f"{analyst_id}|{body}"

    def _to_reservation_result(
        self,
        *,
        reservation_id: str,
        analyst_id: str,
        replay: bool,
        status: str,
        expires_at: float,
        ttl_seconds: int,
        rows: list[sqlite3.Row] | list[tuple[object, ...]],
    ) -> ReservationResult:
        items: list[ReservationItem] = []
        for r in rows:
            if isinstance(r, sqlite3.Row):
                dataset_id = r["dataset_id"]
                reserved = float(r["amount"])
                before = r["available_before"]
                after = r["available_after"]
            else:
                dataset_id, reserved, before, after = r
            items.append(
                ReservationItem(
                    dataset_id=str(dataset_id),
                    reserved=float(reserved),
                    available_before=float("nan") if before is None else float(before),
                    available_after=float("nan") if after is None else float(after),
                )
            )
        return ReservationResult(
            reservation_id=reservation_id,
            analyst_id=analyst_id,
            replay=replay,
            status=status,
            expires_at=_iso(expires_at),
            ttl_seconds=ttl_seconds,
            total_reserved=sum(i.reserved for i in items),
            items=tuple(items),
        )

    def reserve(
        self,
        *,
        reservation_id: str,
        analyst_id: str,
        items: list[tuple[str, float]],
        ttl_seconds: int,
    ) -> ReservationResult:
        """原子预占：所有数据集一起占住或整体不生效。"""
        if ttl_seconds <= 0:
            raise InvalidParameterError(
                "ttl_seconds 必须是正整数秒",
                details={"field": "ttl_seconds", "value": ttl_seconds},
            )
        with self._write_lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                result = self._reserve_tx(
                    reservation_id, analyst_id, items, ttl_seconds
                )
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
        ttl_seconds: int,
    ) -> ReservationResult:
        conn = self.conn

        # 1) 实体与授权（与 consume 相同的可区分判定顺序）
        if not self.analyst_exists(analyst_id):
            raise AnalystNotFoundError(
                f"分析者不存在: {analyst_id}",
                details={"analyst_id": analyst_id, "reservation_id": reservation_id},
            )
        for dataset_id, _amount in items:
            if not self.dataset_exists(dataset_id):
                raise DatasetNotFoundError(
                    f"数据集不存在: {dataset_id}",
                    details={
                        "dataset_id": dataset_id,
                        "analyst_id": analyst_id,
                        "reservation_id": reservation_id,
                    },
                )
            if not self.is_authorized(analyst_id, dataset_id):
                raise UnauthorizedDatasetError(
                    f"分析者 {analyst_id} 未被授权访问数据集 {dataset_id}",
                    details={
                        "analyst_id": analyst_id,
                        "dataset_id": dataset_id,
                        "reservation_id": reservation_id,
                    },
                )

        # 2) 幂等 / 冲突 / 过期
        existing = conn.execute(
            "SELECT * FROM reservations WHERE reservation_id = ?",
            (reservation_id,),
        ).fetchone()
        fingerprint = self._fingerprint(analyst_id, items)
        now = self.now()
        if existing is not None:
            if existing["analyst_id"] != analyst_id:
                raise ReservationConflictError(
                    f"预占标识 {reservation_id} 已属于其他分析者",
                    details={
                        "reservation_id": reservation_id,
                        "requested_analyst": analyst_id,
                        "existing_analyst": existing["analyst_id"],
                    },
                )
            if existing["fingerprint"] != fingerprint:
                existing_fp = existing["fingerprint"]
                raise ReservationConflictError(
                    f"预占标识 {reservation_id} 已存在但预占内容不一致",
                    details={
                        "reservation_id": reservation_id,
                        "existing_fingerprint": existing_fp,
                        "requested_fingerprint": fingerprint,
                    },
                )
            # 内容一致：held 且未过期 -> 幂等重放；held 但到期 -> 懒失效并拒绝
            if existing["status"] == "held":
                if float(existing["expires_at"]) <= now:
                    self._mark_expired_locked(existing)
                    raise ReservationExpiredError(
                        f"预占 {reservation_id} 已过期，占用已自动释放，"
                        "不能据此结算或复用",
                        details={
                            "reservation_id": reservation_id,
                            "expires_at": _iso(float(existing["expires_at"])),
                            "now": _iso(now),
                        },
                    )
                rows = conn.execute(
                    "SELECT * FROM reservation_items WHERE reservation_id = ? "
                    "ORDER BY dataset_id",
                    (reservation_id,),
                ).fetchall()
                return self._to_reservation_result(
                    reservation_id=reservation_id,
                    analyst_id=analyst_id,
                    replay=True,
                    status="held",
                    expires_at=float(existing["expires_at"]),
                    ttl_seconds=int(existing["ttl_seconds"]),
                    rows=rows,
                )
            if existing["status"] == "expired":
                raise ReservationExpiredError(
                    f"预占 {reservation_id} 已过期，不能复用",
                    details={"reservation_id": reservation_id},
                )
            raise ReservationClosedError(
                f"预占 {reservation_id} 已{existing['status']}，不能再次预占",
                details={
                    "reservation_id": reservation_id,
                    "status": existing["status"],
                },
            )

        # 3) 可用额度充足性（已消耗 + 有效预占 + 本次预占 <= 上限），任一不足整体失败
        snap: dict[str, tuple[BalanceView, float]] = {}
        for dataset_id, amount in items:
            bal = self.get_balance(analyst_id, dataset_id)
            assert bal is not None
            if bal.available < amount - 1e-9:
                raise BudgetInsufficientError(
                    f"数据集 {dataset_id} 可预占额度不足: 需 {amount}, "
                    f"可用 {bal.available}（已消耗 {bal.consumed}, "
                    f"预占中 {bal.reserved}）",
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
            snap[dataset_id] = (bal, amount)

        # 4) 写入预占主表 + 明细（同事务，要么全占上要么整体不生效）
        created_at = now
        expires_at = now + ttl_seconds
        conn.execute(
            "INSERT INTO reservations(reservation_id, analyst_id, status, "
            "ttl_seconds, created_at, expires_at, fingerprint) "
            "VALUES (?, ?, 'held', ?, ?, ?, ?)",
            (reservation_id, analyst_id, int(ttl_seconds),
             created_at, expires_at, fingerprint),
        )
        out_rows: list[tuple[object, ...]] = []
        for dataset_id, amount in items:
            bal, _amt = snap[dataset_id]
            before, after = bal.available, bal.available - amount
            conn.execute(
                "INSERT INTO reservation_items(reservation_id, dataset_id, amount, "
                "available_before, available_after) VALUES (?, ?, ?, ?, ?)",
                (reservation_id, dataset_id, amount, before, after),
            )
            out_rows.append((dataset_id, amount, before, after))
        return self._to_reservation_result(
            reservation_id=reservation_id,
            analyst_id=analyst_id,
            replay=False,
            status="held",
            expires_at=expires_at,
            ttl_seconds=ttl_seconds,
            rows=out_rows,
        )

    def settle_reservation(
        self,
        *,
        reservation_id: str,
        analyst_id: str,
        items: list[tuple[str, float]],
    ) -> SettleResult:
        """按实际用量结算预占：实扣入账，多占部分退回。"""
        with self._write_lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                result = self._settle_tx(reservation_id, analyst_id, items)
                self.conn.execute("COMMIT")
                return result
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

    def _settle_tx(
        self,
        reservation_id: str,
        analyst_id: str,
        items: list[tuple[str, float]],
    ) -> SettleResult:
        conn = self.conn
        row = conn.execute(
            "SELECT * FROM reservations WHERE reservation_id = ?",
            (reservation_id,),
        ).fetchone()
        if row is None:
            raise ReservationNotFoundError(
                f"预占标识不存在: {reservation_id}",
                details={"reservation_id": reservation_id},
            )
        if row["analyst_id"] != analyst_id:
            raise ReservationConflictError(
                f"预占标识 {reservation_id} 属于其他分析者",
                details={
                    "reservation_id": reservation_id,
                    "requested_analyst": analyst_id,
                    "existing_analyst": row["analyst_id"],
                },
            )
        now = self.now()
        if row["status"] == "held" and float(row["expires_at"]) <= now:
            self._mark_expired_locked(row)
            row = conn.execute(
                "SELECT * FROM reservations WHERE reservation_id = ?",
                (reservation_id,),
            ).fetchone()
        if row["status"] == "expired":
            raise ReservationExpiredError(
                f"预占 {reservation_id} 已过期并自动释放，拒绝结算",
                details={
                    "reservation_id": reservation_id,
                    "expires_at": _iso(float(row["expires_at"])),
                    "now": _iso(now),
                },
            )
        if row["status"] == "released":
            raise ReservationClosedError(
                f"预占 {reservation_id} 已整笔归还，不能结算",
                details={"reservation_id": reservation_id, "status": "released"},
            )

        held_rows = conn.execute(
            "SELECT dataset_id, amount, charged, refunded, available_before, "
            "available_after FROM reservation_items WHERE reservation_id = ?",
            (reservation_id,),
        ).fetchall()
        held_map = {r["dataset_id"]: r for r in held_rows}
        requested_map = dict(items)

        # 请求数据集必须是预占集合的子集；金额不得超过该数据集的预占额
        for dataset_id, cost in items:
            held = held_map.get(dataset_id)
            if held is None:
                raise ReservationConflictError(
                    f"结算项 {dataset_id} 不在预占 {reservation_id} 的数据集范围内",
                    details={
                        "reservation_id": reservation_id,
                        "dataset_id": dataset_id,
                        "reserved_datasets": sorted(held_map),
                    },
                )
            if cost > float(held["amount"]) + 1e-9:
                raise InvalidParameterError(
                    f"数据集 {dataset_id} 实际用量 {cost} 超过预占 "
                    f"{float(held['amount'])}",
                    details={
                        "field": f"charges[{dataset_id}].cost",
                        "cost": cost,
                        "reserved": float(held["amount"]),
                    },
                )

        # 已结算：幂等重放，且重复提交内容必须与首次一致
        if row["status"] == "settled":
            settled_map = {d: float(r["charged"]) for d, r in held_map.items()}
            if settled_map != requested_map:
                raise ReservationConflictError(
                    f"预占 {reservation_id} 已结算，但本次实际用量与首次不一致",
                    details={
                        "reservation_id": reservation_id,
                        "existing": settled_map,
                        "requested": requested_map,
                    },
                )
            return self._load_settle_result(reservation_id, analyst_id, replay=True)

        # 首次结算：写 settle 事件、更新明细与主表（单事务）
        next_seq_row = conn.execute(
            "SELECT COALESCE(MAX(seq), 0) + 1 AS n FROM events"
        ).fetchone()
        next_seq = int(next_seq_row["n"])
        seq_offset = 0
        settle_items: list[SettleItem] = []
        total_charged = 0.0
        total_refunded = 0.0
        for dataset_id in sorted(held_map):
            held = held_map[dataset_id]
            reserved = float(held["amount"])
            cost = float(requested_map.get(dataset_id, 0.0))
            refunded = reserved - cost
            total_charged += cost
            total_refunded += refunded
            if cost > 0:
                conn.execute(
                    "INSERT INTO events(seq, query_id, analyst_id, dataset_id, "
                    "amount, ref_type, ref_id) VALUES (?, ?, ?, ?, ?, 'settle', ?)",
                    (next_seq + seq_offset, f"settle:{reservation_id}:{dataset_id}",
                     analyst_id, dataset_id, cost, reservation_id),
                )
                seq_offset += 1
            conn.execute(
                "UPDATE reservation_items SET charged = ?, refunded = ? "
                "WHERE reservation_id = ? AND dataset_id = ?",
                (cost, refunded, reservation_id, dataset_id),
            )
            settle_items.append(
                SettleItem(
                    dataset_id=dataset_id,
                    reserved=reserved,
                    charged=cost,
                    refunded=refunded,
                    available_before=float(held["available_before"]),
                    available_after=float(held["available_after"]) + refunded,
                )
            )
        conn.execute(
            "UPDATE reservations SET status = 'settled', settled_at = ?, "
            "closed_at = ? WHERE reservation_id = ?",
            (now, now, reservation_id),
        )
        return SettleResult(
            reservation_id=reservation_id,
            analyst_id=analyst_id,
            replay=False,
            total_charged=total_charged,
            total_refunded=total_refunded,
            items=tuple(settle_items),
        )

    def _load_settle_result(
        self, reservation_id: str, analyst_id: str, *, replay: bool
    ) -> SettleResult:
        rows = self.conn.execute(
            "SELECT dataset_id, amount, charged, refunded, available_before, "
            "available_after FROM reservation_items WHERE reservation_id = ? "
            "ORDER BY dataset_id",
            (reservation_id,),
        ).fetchall()
        items = [
            SettleItem(
                dataset_id=r["dataset_id"],
                reserved=float(r["amount"]),
                charged=float(r["charged"]),
                refunded=float(r["refunded"]),
                available_before=float(r["available_before"]),
                available_after=float(r["available_after"]) + float(r["refunded"]),
            )
            for r in rows
        ]
        return SettleResult(
            reservation_id=reservation_id,
            analyst_id=analyst_id,
            replay=replay,
            total_charged=sum(i.charged for i in items),
            total_refunded=sum(i.refunded for i in items),
            items=tuple(items),
        )

    def release_reservation(
        self, reservation_id: str, analyst_id: str | None = None
    ) -> ReleaseResult:
        """整笔归还预占，释放全部占用。重复归还只生效一次、结论一致。"""
        with self._write_lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                result = self._release_tx(reservation_id, analyst_id)
                self.conn.execute("COMMIT")
                return result
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

    def _release_tx(
        self, reservation_id: str, analyst_id: str | None
    ) -> ReleaseResult:
        conn = self.conn
        row = conn.execute(
            "SELECT * FROM reservations WHERE reservation_id = ?",
            (reservation_id,),
        ).fetchone()
        if row is None:
            raise ReservationNotFoundError(
                f"预占标识不存在: {reservation_id}",
                details={"reservation_id": reservation_id},
            )
        if analyst_id is not None and row["analyst_id"] != analyst_id:
            raise ReservationConflictError(
                f"预占标识 {reservation_id} 属于其他分析者",
                details={
                    "reservation_id": reservation_id,
                    "requested_analyst": analyst_id,
                    "existing_analyst": row["analyst_id"],
                },
            )
        owner = row["analyst_id"]
        now = self.now()

        # held 但已到期：懒失效，按"过期自动释放"给出一致结论
        if row["status"] == "held" and float(row["expires_at"]) <= now:
            self._mark_expired_locked(row)
            row = conn.execute(
                "SELECT * FROM reservations WHERE reservation_id = ?",
                (reservation_id,),
            ).fetchone()

        if row["status"] == "settled":
            raise ReservationClosedError(
                f"预占 {reservation_id} 已结算，不能整笔归还",
                details={"reservation_id": reservation_id, "status": "settled"},
            )
        if row["status"] in ("released", "expired"):
            total = float(
                conn.execute(
                    "SELECT COALESCE(SUM(amount), 0) AS s FROM reservation_items "
                    "WHERE reservation_id = ?",
                    (reservation_id,),
                ).fetchone()["s"]
            )
            return ReleaseResult(
                reservation_id=reservation_id,
                analyst_id=owner,
                replay=True,
                status=row["status"],
                total_refunded=total,
            )

        # 首次主动归还
        total = float(
            conn.execute(
                "SELECT COALESCE(SUM(amount), 0) AS s FROM reservation_items "
                "WHERE reservation_id = ?",
                (reservation_id,),
            ).fetchone()["s"]
        )
        conn.execute(
            "UPDATE reservations SET status = 'released', closed_at = ? "
            "WHERE reservation_id = ?",
            (now, reservation_id),
        )
        return ReleaseResult(
            reservation_id=reservation_id,
            analyst_id=owner,
            replay=False,
            status="released",
            total_refunded=total,
        )

    def sweep_expired(self) -> list[str]:
        """把所有已到期但仍为 held 的预占翻为 expired 并释放额度。

        幂等：返回本次被失效的预占标识列表；重复执行不会重复释放。
        """
        with self._write_lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                now = self.now()
                rows = self.conn.execute(
                    "SELECT * FROM reservations WHERE status = 'held' "
                    "AND expires_at <= ? ORDER BY reservation_id",
                    (now,),
                ).fetchall()
                for row in rows:
                    self._mark_expired_locked(row)
                expired_ids = [r["reservation_id"] for r in rows]
                self.conn.execute("COMMIT")
                return expired_ids
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

    def _mark_expired_locked(self, held_row: sqlite3.Row) -> None:
        """调用方必须已持有写事务。把一个 held 预占标记为 expired。"""
        self.conn.execute(
            "UPDATE reservations SET status = 'expired', closed_at = ? "
            "WHERE reservation_id = ? AND status = 'held'",
            (self.now(), held_row["reservation_id"]),
        )

    def get_reservation(
        self, reservation_id: str
    ) -> tuple[ReservationResult | None, str]:
        """查询预占；若 held 已到期则懒失效。返回 (结果或None, 当前状态)。"""
        with self._write_lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                row = self.conn.execute(
                    "SELECT * FROM reservations WHERE reservation_id = ?",
                    (reservation_id,),
                ).fetchone()
                if row is None:
                    self.conn.execute("COMMIT")
                    return None, "not_found"
                if row["status"] == "held" and float(row["expires_at"]) <= self.now():
                    self._mark_expired_locked(row)
                    row = self.conn.execute(
                        "SELECT * FROM reservations WHERE reservation_id = ?",
                        (reservation_id,),
                    ).fetchone()
                items = self.conn.execute(
                    "SELECT * FROM reservation_items WHERE reservation_id = ? "
                    "ORDER BY dataset_id",
                    (reservation_id,),
                ).fetchall()
                result = self._to_reservation_result(
                    reservation_id=reservation_id,
                    analyst_id=row["analyst_id"],
                    replay=False,
                    status=row["status"],
                    expires_at=float(row["expires_at"]),
                    ttl_seconds=int(row["ttl_seconds"]),
                    rows=items,
                )
                self.conn.execute("COMMIT")
                return result, row["status"]
            except Exception:
                self.conn.execute("ROLLBACK")
                raise
