"""SQLite 持久层：预算余额表 + 只追加的查询登记表。

并发模型：每个线程一个连接（WAL，busy_timeout），写操作一律用
``BEGIN IMMEDIATE`` 提前拿写锁；扣减采用带条件的 UPDATE
（``spent + ? <= budget_limit``），因此并发争抢同一额度时最多一个请求成功，
余额永远不会越过上限，失败时整笔事务回滚，不存在部分扣减。
"""

from __future__ import annotations

import sqlite3
import threading
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .errors import ErrorCode, LedgerError

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS analysts (
    analyst_id TEXT PRIMARY KEY,
    name TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS datasets (
    dataset_id TEXT PRIMARY KEY,
    name TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- 口径单调：budget_limit 只允许上调；spent 只增不减，且 spent <= budget_limit。
CREATE TABLE IF NOT EXISTS budgets (
    analyst_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    budget_limit REAL NOT NULL CHECK (budget_limit > 0),
    spent REAL NOT NULL DEFAULT 0 CHECK (spent >= 0),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (analyst_id, dataset_id),
    FOREIGN KEY (analyst_id) REFERENCES analysts(analyst_id),
    FOREIGN KEY (dataset_id) REFERENCES datasets(dataset_id),
    CHECK (spent <= budget_limit)
);

-- 查询登记表：query_id 全局唯一，结果只追加，服务重启后仍在。
CREATE TABLE IF NOT EXISTS query_records (
    query_id TEXT PRIMARY KEY,
    analyst_id TEXT NOT NULL,
    status TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    result_json TEXT NOT NULL,
    error_code TEXT,
    error_message TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""


class Txn:
    """绑定单条连接的显式事务（BEGIN IMMEDIATE）。"""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def get_analyst(self, analyst_id: str) -> str | None:
        row = self._conn.execute(
            "SELECT analyst_id FROM analysts WHERE analyst_id = ?", (analyst_id,)
        ).fetchone()
        return None if row is None else row[0]

    def get_dataset(self, dataset_id: str) -> str | None:
        row = self._conn.execute(
            "SELECT dataset_id FROM datasets WHERE dataset_id = ?", (dataset_id,)
        ).fetchone()
        return None if row is None else row[0]

    def get_budget(self, analyst_id: str, dataset_id: str) -> tuple[float, float] | None:
        """返回 (budget_limit, spent)，无授权记录返回 None。"""
        row = self._conn.execute(
            "SELECT budget_limit, spent FROM budgets "
            "WHERE analyst_id = ? AND dataset_id = ?",
            (analyst_id, dataset_id),
        ).fetchone()
        return None if row is None else (float(row[0]), float(row[1]))

    def get_query_record(self, query_id: str) -> sqlite3.Row | None:
        return self._conn.execute(
            "SELECT query_id, analyst_id, status, request_hash, result_json, "
            "error_code, error_message FROM query_records WHERE query_id = ?",
            (query_id,),
        ).fetchone()

    def apply_deduction(self, analyst_id: str, dataset_id: str, cost: float) -> int:
        """条件扣减：仅当扣减后不越限时生效。返回受影响行数（必须为 1）。"""
        cur = self._conn.execute(
            "UPDATE budgets SET spent = spent + ?, "
            "updated_at = datetime('now') "
            "WHERE analyst_id = ? AND dataset_id = ? AND spent + ? <= budget_limit",
            (cost, analyst_id, dataset_id, cost),
        )
        return cur.rowcount

    def insert_query_record(
        self,
        query_id: str,
        analyst_id: str,
        status: str,
        request_hash: str,
        result_json: str,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None:
        self._conn.execute(
            "INSERT INTO query_records (query_id, analyst_id, status, request_hash, "
            "result_json, error_code, error_message) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (query_id, analyst_id, status, request_hash, result_json, error_code, error_message),
        )


class Storage:
    """SQLite 存储。

    文件路径使用 WAL 与线程本地连接；``:memory:`` 使用共享缓存内存库，
    另保留一个锚点连接使其在整个 Storage 生命周期内不被回收。
    """

    def __init__(self, db_path: str | Path = ":memory:") -> None:
        self.db_path = str(db_path)
        self._is_memory = self.db_path == ":memory:"
        if self._is_memory:
            self._uri = f"file:pbl_mem_{uuid.uuid4().hex}?mode=memory&cache=shared"
        else:
            p = Path(self.db_path).expanduser().resolve()
            p.parent.mkdir(parents=True, exist_ok=True)
            self._uri = p.as_uri()
        self._local = threading.local()
        self._anchor = self._connect()
        if not self._is_memory:
            self._anchor.execute("PRAGMA journal_mode=WAL")

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self._uri,
            uri=True,
            isolation_level=None,  # 显式事务
            check_same_thread=False,
            timeout=5.0,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA foreign_keys=ON")
        if not self._is_memory:
            conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._connect()
            self._local.conn = conn
        return conn

    def init_schema(self) -> None:
        self._anchor.executescript(SCHEMA_SQL)

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None
        self._anchor.close()

    # ---- 读路径（短事务自动提交） ----
    def get_budget(self, analyst_id: str, dataset_id: str) -> tuple[float, float] | None:
        return Txn(self._conn()).get_budget(analyst_id, dataset_id)

    def get_query_record(self, query_id: str) -> sqlite3.Row | None:
        return Txn(self._conn()).get_query_record(query_id)

    def analyst_exists(self, analyst_id: str) -> bool:
        return Txn(self._conn()).get_analyst(analyst_id) is not None

    def dataset_exists(self, dataset_id: str) -> bool:
        return Txn(self._conn()).get_dataset(dataset_id) is not None

    # ---- 写路径 ----
    def create_analyst(self, analyst_id: str, name: str = "") -> None:
        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                "INSERT INTO analysts (analyst_id, name) VALUES (?, ?)",
                (analyst_id, name),
            )
            conn.execute("COMMIT")
        except sqlite3.IntegrityError:
            conn.execute("ROLLBACK")
            raise
        except Exception:
            conn.execute("ROLLBACK")
            raise

    def create_dataset(self, dataset_id: str, name: str = "") -> None:
        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                "INSERT INTO datasets (dataset_id, name) VALUES (?, ?)",
                (dataset_id, name),
            )
            conn.execute("COMMIT")
        except sqlite3.IntegrityError:
            conn.execute("ROLLBACK")
            raise
        except Exception:
            conn.execute("ROLLBACK")
            raise

    def grant_budget(
        self, analyst_id: str, dataset_id: str, budget_limit: float
    ) -> tuple[float, float, bool]:
        """授权/上调预算上限。

        返回 (budget_limit, spent, is_new)。只允许首次授权或上调；
        任何下调（含下调到已消耗量以下）都视为账本倒退而拒绝。
        """
        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT budget_limit, spent FROM budgets "
                "WHERE analyst_id = ? AND dataset_id = ?",
                (analyst_id, dataset_id),
            ).fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO budgets (analyst_id, dataset_id, budget_limit, spent) "
                    "VALUES (?, ?, ?, 0)",
                    (analyst_id, dataset_id, budget_limit),
                )
                out = (budget_limit, 0.0, True)
            else:
                current_limit, spent = float(row[0]), float(row[1])
                # 口径单调：上限只能上调；低于当前上限（尤其低于已消耗量）的
                # 设置一律视为账本倒退并拒绝。
                if budget_limit < current_limit:
                    raise LedgerError(
                        ErrorCode.LEDGER_ROLLBACK_REJECTED,
                        "budget_limit 只能上调，拒绝任何回退账本的设置",
                        {
                            "analyst_id": analyst_id,
                            "dataset_id": dataset_id,
                            "current_limit": current_limit,
                            "requested_limit": budget_limit,
                            "spent": spent,
                        },
                    )
                out = (budget_limit, spent, False)
                conn.execute(
                    "UPDATE budgets SET budget_limit = ?, "
                    "updated_at = datetime('now') "
                    "WHERE analyst_id = ? AND dataset_id = ?",
                    (budget_limit, analyst_id, dataset_id),
                )
            conn.execute("COMMIT")
            return out
        except Exception:
            conn.execute("ROLLBACK")
            raise

    @contextmanager
    def transaction(self) -> Iterator[Txn]:
        """打开 IMMEDIATE 事务；异常自动 ROLLBACK，正常退出 COMMIT。"""
        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")
        txn = Txn(conn)
        try:
            yield txn
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")
