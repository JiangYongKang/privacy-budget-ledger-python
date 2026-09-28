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
from collections.abc import Iterable
from pathlib import Path

from ledger.errors import (
    AnalystNotFoundError,
    BudgetInsufficientError,
    DatasetNotFoundError,
    LedgerRollbackError,
    QueryConflictError,
    UnauthorizedDatasetError,
)
from ledger.models import BalanceView, ChargeResult, ConsumeResult

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
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (query_id, dataset_id),
    FOREIGN KEY (analyst_id) REFERENCES analysts(analyst_id),
    FOREIGN KEY (dataset_id)  REFERENCES datasets(dataset_id)
);
CREATE INDEX IF NOT EXISTS idx_events_pair ON events(analyst_id, dataset_id, seq);
CREATE INDEX IF NOT EXISTS idx_events_query ON events(query_id);
"""


class LedgerStore:
    """SQLite 后端的账本存储，进程内写操作串行化。"""

    def __init__(self, db_path: str | Path = ":memory:") -> None:
        self._db_path = str(db_path)
        self._write_lock = threading.RLock()
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
        conn.executescript(_SCHEMA)
        self._conn = conn
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
            row = self.conn.execute(
                """
                SELECT g.budget_limit AS lim,
                       COALESCE((SELECT SUM(e.amount) FROM events e
                                 WHERE e.analyst_id = g.analyst_id
                                   AND e.dataset_id = g.dataset_id), 0) AS consumed
                FROM grants g
                WHERE g.analyst_id = ? AND g.dataset_id = ?
                """,
                (analyst_id, dataset_id),
            ).fetchone()
        if row is None:
            return None
        consumed = float(row["consumed"])
        limit = float(row["lim"])
        return BalanceView(
            analyst_id=analyst_id,
            dataset_id=dataset_id,
            limit=limit,
            consumed=consumed,
            balance=limit - consumed,
        )

    def list_balances(self, analyst_id: str) -> list[BalanceView]:
        with self._write_lock:
            rows = self.conn.execute(
            """
            SELECT g.dataset_id AS dataset_id, g.budget_limit AS lim,
                   COALESCE((SELECT SUM(e.amount) FROM events e
                             WHERE e.analyst_id = g.analyst_id
                               AND e.dataset_id = g.dataset_id), 0) AS consumed
            FROM grants g WHERE g.analyst_id = ?
            ORDER BY g.dataset_id
            """,
            (analyst_id,),
        ).fetchall()
        return [
            BalanceView(
                analyst_id=analyst_id,
                dataset_id=r["dataset_id"],
                limit= float(r["lim"]),
                consumed=float(r["consumed"]),
                balance=float(r["lim"]) - float(r["consumed"]),
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

        # 3) 余额充足性（任一不足整体失败）
        balances: dict[str, BalanceView] = {}
        for dataset_id, cost in items:
            bal = self.get_balance(analyst_id, dataset_id)
            assert bal is not None  # 上面已校验授权
            if bal.balance < cost:
                raise BudgetInsufficientError(
                    f"数据集 {dataset_id} 预算不足: "
                    f"需 {cost}, 剩余 {bal.balance}",
                    details={
                        "query_id": query_id,
                        "analyst_id": analyst_id,
                        "dataset_id": dataset_id,
                        "requested_cost": cost,
                        "limit": bal.limit,
                        "consumed": bal.consumed,
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
