"""跨多数据集组合扣减：全成功或整体不生效。"""

from __future__ import annotations

import pytest

from ledger.errors import BudgetInsufficientError
from ledger.models import DatasetCharge
from tests.conftest import log_case


def test_multi_dataset_all_or_nothing_success(service):
    r = service.consume(
        query_id="q-multi-ok",
        analyst_id="a1",
        charges=[DatasetCharge("d1", 4.0), DatasetCharge("d2", 3.0)],
    )
    assert [i.dataset_id for i in r.items] == ["d1", "d2"]
    assert [i.balance_after for i in r.items] == [6.0, 2.0]
    log_case(
        "跨数据集组合扣减成功",
        query_id="q-multi-ok",
        d1="10.0-4.0->6.0", d2="5.0-3.0->2.0",
        判定依据="两数据集均有足额余额,单事务提交",
    )


def test_multi_dataset_rollback_when_any_step_fails(service):
    # 顺序：d1 先（足），d2 后（不足，需 40）。d1 不得被部分扣减
    with pytest.raises(BudgetInsufficientError) as ei:
        service.consume(
            query_id="q-multi-fail",
            analyst_id="a1",
            charges=[DatasetCharge("d1", 8.0), DatasetCharge("d2", 40.0)],
        )
    assert ei.value.details["dataset_id"] == "d2"
    assert service.get_balance("a1", "d1").consumed == 0.0
    assert service.get_balance("a1", "d2").consumed == 0.0
    assert service.get_query_record("q-multi-fail") is None
    log_case(
        "任意一步失败整体回滚",
        query_id="q-multi-fail",
        d1扣减后=service.get_balance("a1", "d1").consumed,
        d2扣减后=service.get_balance("a1", "d2").consumed,
        判定依据="单事务ROLLBACK,无部分扣减",
    )


def test_balance_aggregation_after_mixed_queries(service):
    service.consume(query_id="m1", analyst_id="a1", charges=[DatasetCharge("d1", 2.0)])
    service.consume(query_id="m2", analyst_id="a1",
                    charges=[DatasetCharge("d1", 3.0), DatasetCharge("d2", 5.0)])
    service.consume(query_id="m2", analyst_id="a1",
                    charges=[DatasetCharge("d1", 3.0), DatasetCharge("d2", 5.0)])  # 幂等
    with pytest.raises(BudgetInsufficientError):
        service.consume(query_id="m3", analyst_id="a1",
                        charges=[DatasetCharge("d1", 6.0)])  # 已用 5,剩 5
    b1 = service.get_balance("a1", "d1")
    b2 = service.get_balance("a1", "d2")
    assert (b1.limit, b1.consumed, b1.balance) == (10.0, 5.0, 5.0)
    assert (b2.limit, b2.consumed, b2.balance) == (5.0, 5.0, 0.0)
    log_case(
        "混合查询后额度口径稳定",
        query_id="m1/m2/m3",
        d1=f"limit={b1.limit} consumed={b1.consumed} balance={b1.balance}",
        d2=f"limit={b2.limit} consumed={b2.consumed} balance={b2.balance}",
        判定依据="只追加事件回放,重复不计,失败不计",
    )
