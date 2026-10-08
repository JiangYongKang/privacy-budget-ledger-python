"""预占在服务重启后的持久化：有效期内仍占用、停机期间到期自动释放、口径不丢不多。"""

from __future__ import annotations

import sqlite3

from ledger.models import DatasetCharge, ReservationItem
from ledger.service import build_default_service
from ledger.store import LedgerStore
from tests.conftest import log_case


class FakeClock:
    def __init__(self, t: float) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t


def test_active_reservation_survives_restart(file_db):
    clock = FakeClock(1000.0)
    svc = build_default_service(file_db, default_ttl_seconds=100.0, clock=clock)
    svc.register_analyst("a1")
    svc.register_dataset("d1")
    svc.register_dataset("d2")
    svc.authorize("a1", "d1", 10.0)
    svc.authorize("a1", "d2", 5.0)
    # 直接扣 3 + 组合预占 d1=4,d2=2
    svc.consume(query_id="q0", analyst_id="a1", charges=[DatasetCharge("d1", 3.0)])
    svc.reserve(
        reservation_id="rv-persist", analyst_id="a1",
        items=[ReservationItem("d1", 4.0), ReservationItem("d2", 2.0)],
    )
    svc._store.close()

    # 重启，时间只前进 50（未到 100 的期）
    clock.t = 1050.0
    svc2 = build_default_service(file_db, default_ttl_seconds=100.0, clock=clock)
    b1 = svc2.get_balance("a1", "d1")
    b2 = svc2.get_balance("a1", "d2")
    assert (b1.consumed, b1.reserved, b1.available) == (3.0, 4.0, 3.0)
    assert (b2.consumed, b2.reserved, b2.available) == (0.0, 2.0, 3.0)
    view = svc2.get_reservation("rv-persist")
    assert view.status == "held" and len(view.items) == 2
    # 重启后仍可对该预占结算
    s = svc2.settle(
        reservation_id="rv-persist", analyst_id="a1",
        actual=[ReservationItem("d1", 2.0), ReservationItem("d2", 2.0)],
    )
    assert (s.total_settled, s.total_refunded) == (4.0, 2.0)
    b1 = svc2.get_balance("a1", "d1")
    assert (b1.consumed, b1.reserved, b1.available) == (5.0, 0.0, 5.0)
    log_case(
        "有效期内预占重启后仍占用且可结算",
        reservation_id="rv-persist",
        重启前d1可用=3.0, 重启后d1可用=b1.available,
        判定依据="预占单与明细落盘,过期口径按expires_at重算",
    )
    svc2._store.close()


def test_reservation_expired_during_downtime_is_released_on_restart(file_db):
    clock = FakeClock(1000.0)
    svc = build_default_service(file_db, default_ttl_seconds=10.0, clock=clock)
    svc.register_analyst("a1")
    svc.register_dataset("d1")
    svc.authorize("a1", "d1", 10.0)
    svc.reserve(reservation_id="rv-down", analyst_id="a1",
                items=[ReservationItem("d1", 7.0)])
    svc._store.close()

    # 停机跨越了有效期：重启时先 sweep，再做完整性校验
    clock.t = 2000.0
    svc2 = build_default_service(file_db, default_ttl_seconds=10.0, clock=clock)
    assert svc2.get_reservation("rv-down").status == "expired"
    bal = svc2.get_balance("a1", "d1")
    assert (bal.consumed, bal.reserved, bal.available) == (0.0, 0.0, 10.0)
    log_case(
        "停机期间到期的预占重启时自动释放且不凭空增减额度",
        reservation_id="rv-down", 重启后可用=bal.available,
        判定依据="open()先sweep_expired再完整性校验",
    )
    svc2._store.close()


def test_restart_with_held_reservation_passes_integrity_check(file_db):
    # held 预占与已扣减之和在上限内时，重启完整性校验必须放行而不是误报倒退
    clock = FakeClock(1000.0)
    svc = build_default_service(file_db, default_ttl_seconds=100.0, clock=clock)
    svc.register_analyst("a1")
    svc.register_dataset("d1")
    svc.authorize("a1", "d1", 10.0)
    svc.consume(query_id="q", analyst_id="a1", charges=[DatasetCharge("d1", 6.0)])
    svc.reserve(reservation_id="rv-fit", analyst_id="a1",
                items=[ReservationItem("d1", 4.0)])  # 恰好顶满
    svc._store.close()

    clock.t = 1010.0
    store = LedgerStore(file_db, clock=clock).open()  # 不抛 LedgerRollbackError
    row = store.get_balance("a1", "d1")
    assert (row.consumed, row.reserved, row.available) == (6.0, 4.0, 0.0)
    store.close()
    log_case("顶满上限的在持预占重启后完整性校验放行",
             reservation_id="rv-fit", 判定依据="consumed+reserved==limit合法")


def test_legacy_events_table_migrates_without_losing_data(file_db):
    # 旧版 events 表：表级 UNIQUE、无 event_type 列
    conn = sqlite3.connect(file_db)
    conn.executescript(
        """
        CREATE TABLE analysts(analyst_id TEXT PRIMARY KEY,
            created_at TEXT DEFAULT (datetime('now')));
        CREATE TABLE datasets(dataset_id TEXT PRIMARY KEY,
            created_at TEXT DEFAULT (datetime('now')));
        CREATE TABLE grants(analyst_id TEXT, dataset_id TEXT,
            budget_limit REAL CHECK(budget_limit>=0),
            updated_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY(analyst_id,dataset_id));
        CREATE TABLE events(seq INTEGER PRIMARY KEY, query_id TEXT NOT NULL,
            analyst_id TEXT NOT NULL, dataset_id TEXT NOT NULL,
            amount REAL NOT NULL CHECK(amount>0),
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE(query_id,dataset_id));
        INSERT INTO analysts VALUES('a1','x');
        INSERT INTO datasets VALUES('d1','x');
        INSERT INTO grants VALUES('a1','d1',10,'x');
        INSERT INTO events(seq,query_id,analyst_id,dataset_id,amount)
        VALUES(1,'old-q','a1','d1',4);
        """
    )
    conn.commit()
    conn.close()

    svc = build_default_service(file_db)
    assert svc.get_balance("a1", "d1").consumed == 4.0
    # 旧的直接扣减仍幂等；同名 settle 事件不再与其冲突
    assert svc.consume(query_id="old-q", analyst_id="a1",
                       charges=[DatasetCharge("d1", 4.0)]).replay is True
    svc.reserve(reservation_id="rv-mig", analyst_id="a1",
                items=[ReservationItem("d1", 3.0)])
    s = svc.settle(reservation_id="rv-mig", analyst_id="a1",
                   actual=[ReservationItem("d1", 2.0)])
    assert (s.total_settled, s.total_refunded) == (2.0, 1.0)
    assert svc.get_balance("a1", "d1").consumed == 6.0
    svc._store.close()
    log_case("旧版库自动迁移到事件来源区分结构且数据不丢",
             reservation_id="rv-mig", 已扣=6.0,
             判定依据="_migrate_legacy_schema+部分唯一索引")
