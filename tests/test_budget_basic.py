"""基础口径：授权扣减、明确拒绝、额度不足、账本单调。"""

from __future__ import annotations

import pytest

from privacy_budget_ledger.errors import ErrorCode, LedgerError
from privacy_budget_ledger.models import (
    BudgetGrant,
    QueryItem,
    QueryRequest,
)


def _query(query_id: str, analyst: str, *pairs) -> QueryRequest:
    return QueryRequest(
        query_id=query_id,
        analyst_id=analyst,
        items=[QueryItem(dataset_id=d, cost=c) for d, c in pairs],
    )


def test_single_deduction_success(ledger, seeded):
    result = ledger.submit_query(_query("q-1", "a1", ("ds1", 3.0)))
    assert result.status == "SUCCESS"
    assert result.idempotent_replay is False
    e = result.entries[0]
    assert (e.spent_before, e.spent_after, e.remaining) == (0.0, 3.0, 7.0)

    view = ledger.get_budget("a1", "ds1")
    assert (view.budget_limit, view.spent, view.remaining) == (10.0, 3.0, 7.0)


def test_nonexistent_analyst_rejected_without_deduction(ledger, seeded):
    with pytest.raises(LedgerError) as ei:
        ledger.submit_query(_query("q-x", "ghost", ("ds1", 1.0)))
    assert ei.value.code is ErrorCode.ANALYST_NOT_FOUND
    # 数据集本身的额度未发生任何变化
    assert ledger.get_budget("a1", "ds1").spent == 0.0


def test_nonexistent_dataset_rejected_without_deduction(ledger, seeded):
    with pytest.raises(LedgerError) as ei:
        ledger.submit_query(_query("q-x", "a1", ("nope", 1.0)))
    assert ei.value.code is ErrorCode.DATASET_NOT_FOUND
    assert ledger.get_budget("a1", "ds1").spent == 0.0


def test_unauthorized_dataset_rejected(ledger, seeded):
    # a2 只被授权 ds1，访问 ds2 必须被明确拒绝
    with pytest.raises(LedgerError) as ei:
        ledger.submit_query(_query("q-x", "a2", ("ds2", 1.0)))
    assert ei.value.code is ErrorCode.UNAUTHORIZED_DATASET
    assert ledger.get_budget("a1", "ds2").spent == 0.0
    assert ledger.get_budget("a2", "ds1").spent == 0.0


def test_unauthorized_budget_view_distinguishable(ledger, seeded):
    with pytest.raises(LedgerError) as ei:
        ledger.get_budget("a1", "ds3")
    assert ei.value.code is ErrorCode.UNAUTHORIZED_DATASET


@pytest.mark.parametrize("cost", ["0", "-1", "NaN", "Infinity"])
def test_invalid_cost_rejected_at_api(client, seeded, ledger, cost):
    # 非法数值在模型层或领域层被拒，HTTP 统一映射为 INVALID_PARAMETER，且零扣减
    payload = {"query_id": "q-bad", "analyst_id": "a1",
               "items": [{"dataset_id": "ds1", "cost": cost}]}
    r = client.post("/queries", json=payload)
    assert r.status_code == 422, r.text
    assert r.json()["error_code"] == ErrorCode.INVALID_PARAMETER.value
    assert ledger.get_budget("a1", "ds1").spent == 0.0


def test_empty_items_rejected_at_api(client, seeded):
    r = client.post("/queries", json={"query_id": "q-empty", "analyst_id": "a1", "items": []})
    assert r.status_code == 422
    assert r.json()["error_code"] == ErrorCode.INVALID_PARAMETER.value


def test_duplicate_dataset_in_one_request_rejected(ledger, seeded):
    with pytest.raises(LedgerError) as ei:
        ledger.submit_query(_query("q-dup", "a1", ("ds1", 1.0), ("ds1", 1.0)))
    assert ei.value.code is ErrorCode.INVALID_PARAMETER
    assert ledger.get_budget("a1", "ds1").spent == 0.0


def test_budget_insufficient_does_not_deduct(ledger, seeded):
    ledger.submit_query(_query("q-first", "a1", ("ds1", 8.0)))
    with pytest.raises(LedgerError) as ei:
        ledger.submit_query(_query("q-over", "a1", ("ds1", 3.0)))
    assert ei.value.code is ErrorCode.BUDGET_INSUFFICIENT
    details = ei.value.details
    assert details["remaining_before"] == 2.0
    assert details["requested_cost"] == 3.0
    # 失败后余额停留在 8，没有任何部分扣减
    assert ledger.get_budget("a1", "ds1").spent == 8.0


def test_exact_limit_is_allowed(ledger, seeded):
    r = ledger.submit_query(_query("q-exact", "a1", ("ds1", 10.0)))
    assert r.status == "SUCCESS"
    assert r.entries[0].remaining == 0.0
    with pytest.raises(LedgerError) as ei:
        ledger.submit_query(_query("q-over-1", "a1", ("ds1", 0.0001)))
    assert ei.value.code is ErrorCode.BUDGET_INSUFFICIENT


def test_budget_limit_decrease_is_rejected_as_rollback(ledger, seeded):
    ledger.submit_query(_query("q-spend", "a1", ("ds1", 6.0)))
    # 上调允许
    ledger.grant_budget(BudgetGrant(analyst_id="a1", dataset_id="ds1", budget_limit=12.0))
    assert ledger.get_budget("a1", "ds1").budget_limit == 12.0
    # 任何下调（即使仍高于 spent）都被拒绝；已消耗额度保持不变
    with pytest.raises(LedgerError) as ei:
        ledger.grant_budget(BudgetGrant(analyst_id="a1", dataset_id="ds1", budget_limit=11.0))
    assert ei.value.code is ErrorCode.LEDGER_ROLLBACK_REJECTED
    view = ledger.get_budget("a1", "ds1")
    assert (view.budget_limit, view.spent) == (12.0, 6.0)


def test_error_codes_are_distinguishable(ledger, seeded):
    dup = QueryRequest(
        query_id="q", analyst_id="a1",
        items=[QueryItem(dataset_id="ds1", cost=1.0),
               QueryItem(dataset_id="ds1", cost=2.0)],
    )
    cases = [
        (lambda: ledger.submit_query(_query("q", "ghost", ("ds1", 1.0))),
         ErrorCode.ANALYST_NOT_FOUND),
        (lambda: ledger.submit_query(_query("q", "a1", ("nope", 1.0))),
         ErrorCode.DATASET_NOT_FOUND),
        (lambda: ledger.submit_query(_query("q", "a2", ("ds2", 1.0))),
         ErrorCode.UNAUTHORIZED_DATASET),
        (lambda: ledger.submit_query(dup), ErrorCode.INVALID_PARAMETER),
    ]
    seen = set()
    for action, code in cases:
        with pytest.raises(LedgerError) as ei:
            action()
        assert ei.value.code is code
        seen.add(ei.value.code)
    assert len(seen) == len(cases)
