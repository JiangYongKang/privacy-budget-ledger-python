"""失败原因必须可区分：不存在 / 越权 / 不足 / 冲突 / 非法参数。"""

from __future__ import annotations

import pytest

from ledger.errors import (
    AnalystNotFoundError,
    BudgetInsufficientError,
    DatasetNotFoundError,
    InvalidParameterError,
    LedgerRollbackError,
    QueryConflictError,
    UnauthorizedDatasetError,
)
from ledger.models import DatasetCharge
from tests.conftest import log_case



def _consume(svc, query_id="q", analyst_id="a1", charges=None):
    return svc.consume(
        query_id=query_id,
        analyst_id=analyst_id,
        charges=charges if charges is not None else [DatasetCharge("d1", 1.0)],
    )


def test_analyst_not_found_is_distinct(service):
    with pytest.raises(AnalystNotFoundError) as ei:
        _consume(service, query_id="q-anon", analyst_id="ghost")
    assert ei.value.code == "analyst_not_found"
    log_case(
        "不存在分析者被拒绝",
        query_id="q-anon",
        判定="analyst_not_found",
        扣减发生="否",
    )
    # 失败不产生任何扣减
    assert service.get_balance("a1", "d1").consumed == 0.0


def test_dataset_not_found_is_distinct(service):
    with pytest.raises(DatasetNotFoundError) as ei:
        _consume(
            service,
            query_id="q-nods",
            charges=[DatasetCharge("missing", 1.0)],
        )
    assert ei.value.code == "dataset_not_found"
    log_case("不存在数据集被拒绝", query_id="q-nods", 判定="dataset_not_found")


def test_unauthorized_dataset_is_distinct(service):
    # d3 存在但 a1 未被授权
    with pytest.raises(UnauthorizedDatasetError) as ei:
        _consume(
            service,
            query_id="q-noauth",
            charges=[DatasetCharge("d3", 1.0)],
        )
    assert ei.value.code == "unauthorized_dataset"
    # a2 存在，但未授权 d1
    with pytest.raises(UnauthorizedDatasetError):
        _consume(
            service,
            query_id="q-noauth2",
            analyst_id="a2",
            charges=[DatasetCharge("d1", 1.0)],
        )
    log_case(
        "越权访问被拒绝且不扣减",
        query_id="q-noauth",
        判定="unauthorized_dataset",
        d1余额=service.get_balance("a1", "d1").balance,
    )


def test_budget_insufficient_is_distinct_and_no_partial_charge(service):
    # d1 余 10、d2 余 5；d2 不足 => 整体失败，d1 也不能被扣
    with pytest.raises(BudgetInsufficientError) as ei:
        _consume(
            service,
            query_id="q-poor",
            charges=[DatasetCharge("d1", 9.0), DatasetCharge("d2", 6.0)],
        )
    assert ei.value.code == "budget_insufficient"
    assert ei.value.details["dataset_id"] == "d2"
    assert ei.value.details["balance"] == 5.0
    assert service.get_balance("a1", "d1").consumed == 0.0
    assert service.get_balance("a1", "d2").consumed == 0.0
    assert service.get_query_record("q-poor") is None
    log_case(
        "组合扣减中一项不足整体回滚",
        query_id="q-poor",
        判定="budget_insufficient(d2)",
        d1扣减后=service.get_balance("a1", "d1").consumed,
        d2扣减后=service.get_balance("a1", "d2").consumed,
    )


def test_query_conflict_is_distinct(service):
    _consume(
        service,
        query_id="q-dup",
        charges=[DatasetCharge("d1", 2.0)],
    )
    with pytest.raises(QueryConflictError) as ei:
        _consume(
            service,
            query_id="q-dup",
            charges=[DatasetCharge("d1", 3.0)],  # 同 id 不同内容
        )
    assert ei.value.code == "query_conflict"
    log_case("查询标识冲突被拒绝", query_id="q-dup", 判定="query_conflict")

    service.register_analyst("a9")
    service.authorize("a9", "d1", 10.0)
    with pytest.raises(QueryConflictError):
        _consume(
            service,
            query_id="q-dup",
            analyst_id="a9",
            charges=[DatasetCharge("d1", 2.0)],  # 同内容但不同分析者
        )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"query_id": "  ", "analyst_id": "a1", "charges": [DatasetCharge("d1", 1)]},
        {"query_id": "q", "analyst_id": "", "charges": [DatasetCharge("d1", 1)]},
        {"query_id": "q", "analyst_id": "a1", "charges": []},
        {"query_id": "q", "analyst_id": "a1", "charges": [DatasetCharge("d1", 0)]},
        {"query_id": "q", "analyst_id": "a1", "charges": [DatasetCharge("d1", -1)]},
        {"query_id": "q", "analyst_id": "a1", "charges": [DatasetCharge("d1", float("nan"))]},
        {"query_id": "q", "analyst_id": "a1", "charges": [DatasetCharge("d1", float("inf"))]},
        {"query_id": "q", "analyst_id": "a1",
         "charges": [DatasetCharge("d1", 1), DatasetCharge("d1", 1)]},
    ],
)
def test_invalid_parameter_is_distinct(service, kwargs):
    with pytest.raises(InvalidParameterError) as ei:
        service.consume(**kwargs)
    assert ei.value.code == "invalid_parameter"
    log_case("非法参数被拒绝", query_id=kwargs["query_id"], 判定="invalid_parameter")


def test_budget_limit_cannot_be_lowered(service):
    with pytest.raises(LedgerRollbackError) as ei:
        service.authorize("a1", "d1", 3.0)  # 原 10 -> 3 属于账本倒退
    assert ei.value.code == "ledger_rollback_detected"
    assert service.get_balance("a1", "d1").limit == 10.0
    log_case("下调预算上限被拒绝", 判定="ledger_rollback_detected", 当前上限=10.0)
