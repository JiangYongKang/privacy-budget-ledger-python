"""服务重启后额度保留；账本倒退（删事件/篡改上限）必须被拒绝。"""

from __future__ import annotations

import sqlite3

import pytest

from ledger.errors import LedgerRollbackError
from ledger.models import DatasetCharge
from ledger.service import BudgetService, build_default_service
from ledger.store import LedgerStore
from tests.conftest import log_case


def test_consumed_budget_survives_restart(file_db):
    svc = build_default_service(file_db)
    svc.register_analyst("a1")
    svc.register_dataset("d1")
    svc.register_dataset("d2")
    svc.authorize("a1", "d1", 10.0)
    svc.authorize("a1", "d2", 5.0)
    svc.consume(
        query_id="persist-q",
        analyst_id="a1",
        charges=[DatasetCharge("d1", 4.0), DatasetCharge("d2", 2.0)],
    )
    svc._store.close()

    svc2 = build_default_service(file_db)  # 重新打开 = 模拟重启
    b1 = svc2.get_balance("a1", "d1")
    b2 = svc2.get_balance("a1", "d2")
    assert (b1.consumed, b1.balance) == (4.0, 6.0)
    assert (b2.consumed, b2.balance) == (2.0, 3.0)
    rec = svc2.get_query_record("persist-q")
    assert rec is not None and rec.total_charged == 6.0
    # 重启后再用同 query_id 提交仍然幂等，不会二次扣减
    r = svc2.consume(
        query_id="persist-q",
        analyst_id="a1",
        charges=[DatasetCharge("d1", 4.0), DatasetCharge("d2", 2.0)],
    )
    assert r.replay is True
    assert svc2.get_balance("a1", "d1").consumed == 4.0
    log_case(
        "重启后额度与幂等记录保留",
        query_id="persist-q",
        重启前d1余额=6.0, 重启后d1余额=b1.balance,
        判定依据="文件库只追加事件,重启时回放",
    )
    svc2._store.close()


def test_deleted_event_is_detected_as_rollback(file_db):
    svc = build_default_service(file_db)
    svc.register_analyst("a1")
    svc.register_dataset("d1")
    svc.authorize("a1", "d1", 10.0)
    svc.consume(query_id="q1", analyst_id="a1", charges=[DatasetCharge("d1", 1.0)])
    svc.consume(query_id="q2", analyst_id="a1", charges=[DatasetCharge("d1", 1.0)])
    svc._store.close()

    # 外部篡改：删除中间事件（模拟账本倒退）
    conn = sqlite3.connect(file_db)
    conn.execute("DELETE FROM events WHERE query_id = 'q1'")
    conn.commit()
    conn.close()

    with pytest.raises(LedgerRollbackError) as ei:
        LedgerStore(file_db).open()
    assert ei.value.code == "ledger_rollback_detected"
    log_case(
        "删除事件被判定为账本倒退并拒绝服务",
        query_id="q1/q2",
        判定="ledger_rollback_detected(序列不连续)",
    )


def test_lowered_limit_tamper_is_detected(file_db):
    svc = build_default_service(file_db)
    svc.register_analyst("a1")
    svc.register_dataset("d1")
    svc.authorize("a1", "d1", 10.0)
    svc.consume(query_id="q1", analyst_id="a1", charges=[DatasetCharge("d1", 8.0)])
    svc._store.close()

    conn = sqlite3.connect(file_db)
    conn.execute("UPDATE grants SET budget_limit = 5 WHERE analyst_id='a1' AND dataset_id='d1'")
    conn.commit()
    conn.close()

    with pytest.raises(LedgerRollbackError):
        LedgerStore(file_db).open()
    log_case("篡改上限导致已消耗超限被拒绝", query_id="q1", 判定="ledger_rollback_detected(回放越限)")
