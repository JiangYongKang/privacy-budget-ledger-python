"""同一查询标识重复提交：只记一次账、只给一次成功结论。"""

from __future__ import annotations

import pytest

from ledger.models import DatasetCharge
from tests.conftest import log_case


def test_duplicate_submission_charges_once(service):
    charges = [DatasetCharge("d1", 4.0), DatasetCharge("d2", 2.5)]
    r1 = service.consume(query_id="q-ide-1", analyst_id="a1", charges=charges)
    r2 = service.consume(query_id="q-ide-1", analyst_id="a1", charges=charges)
    r3 = service.consume(query_id="q-ide-1", analyst_id="a1", charges=charges)

    assert r1.replay is False
    assert r2.replay is True and r3.replay is True
    # 重放结论与首次完全一致
    for a, b in zip(r1.items, r2.items):
        assert (a.dataset_id, a.balance_before, a.charged, a.balance_after) == (
            b.dataset_id, b.balance_before, b.charged, b.balance_after
        )
    b1 = service.get_balance("a1", "d1")
    b2 = service.get_balance("a1", "d2")
    assert (b1.consumed, b2.consumed) == (4.0, 2.5)
    assert (b1.balance, b2.balance) == (6.0, 2.5)
    log_case(
        "同一查询提交3次只扣一次",
        query_id="q-ide-1",
        扣减前d1=10.0, 本次扣减d1=4.0, 扣减后d1=b1.balance,
        扣减前d2=5.0, 本次扣减d2=2.5, 扣减后d2=b2.balance,
        判定依据="query_id命中既有事件->幂等重放",
    )


def test_different_query_ids_charge_separately(service):
    service.consume(query_id="q-a", analyst_id="a1", charges=[DatasetCharge("d1", 3.0)])
    service.consume(query_id="q-b", analyst_id="a1", charges=[DatasetCharge("d1", 3.0)])
    assert service.get_balance("a1", "d1").consumed == 6.0
    log_case("不同查询标识分别记账", query_id="q-a/q-b", 扣减后d1=4.0, 判定依据="不同query_id独立事件")


def test_query_record_traceable(service):
    service.consume(
        query_id="q-trace",
        analyst_id="a1",
        charges=[DatasetCharge("d1", 2.0), DatasetCharge("d2", 1.0)],
    )
    rec = service.get_query_record("q-trace")
    assert rec is not None
    assert [i.dataset_id for i in rec.items] == ["d1", "d2"]
    assert rec.total_charged == 3.0
    assert service.get_query_record("not-exist") is None
    log_case(
        "查询可追溯(按query_id回放明细)",
        query_id="q-trace",
        数据集=[i.dataset_id for i in rec.items],
        总扣减=rec.total_charged,
        判定依据="events按seq回放",
    )


def test_failed_submission_does_not_poison_query_id(service):
    # 首次因不足失败：不允许该 query_id 留下"半记录"，之后同 id 用合法请求应成功
    with pytest.raises(Exception):
        service.consume(
            query_id="q-retry",
            analyst_id="a1",
            charges=[DatasetCharge("d2", 99.0)],
        )
    r = service.consume(
        query_id="q-retry",
        analyst_id="a1",
        charges=[DatasetCharge("d2", 1.0)],
    )
    assert r.replay is False and r.total_charged == 1.0
    log_case("失败不占用查询标识", query_id="q-retry", 扣减后d2=4.0, 判定依据="失败事务已回滚无事件")
