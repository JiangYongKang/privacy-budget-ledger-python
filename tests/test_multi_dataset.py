"""跨数据集组合扣减：全成或全不成，无部分扣减中间态。"""

from __future__ import annotations

import pytest

from privacy_budget_ledger.errors import ErrorCode, LedgerError
from privacy_budget_ledger.models import BudgetGrant, QueryItem, QueryRequest


def _query(query_id: str, analyst: str, *pairs) -> QueryRequest:
    return QueryRequest(
        query_id=query_id,
        analyst_id=analyst,
        items=[QueryItem(dataset_id=d, cost=c) for d, c in pairs],
    )


def test_multi_dataset_all_succeed(ledger, seeded):
    r = ledger.submit_query(_query("q-multi", "a1", ("ds1", 2.0), ("ds2", 3.0)))
    assert r.status == "SUCCESS"
    assert [(e.dataset_id, e.spent_before, e.spent_after, e.remaining) for e in r.entries] == [
        ("ds1", 0.0, 2.0, 8.0),
        ("ds2", 0.0, 3.0, 7.0),
    ]
    assert ledger.get_budget("a1", "ds1").spent == 2.0
    assert ledger.get_budget("a1", "ds2").spent == 3.0


def test_multi_dataset_rolls_back_when_one_insufficient(ledger, seeded):
    # ds1 额度充足(10)、ds2 只剩 2：组合请求必须整体失败
    ledger.submit_query(_query("q-pre", "a1", ("ds2", 8.0)))
    with pytest.raises(LedgerError) as ei:
        ledger.submit_query(_query("q-bundle", "a1", ("ds1", 5.0), ("ds2", 5.0)))
    assert ei.value.code is ErrorCode.BUDGET_INSUFFICIENT
    assert ei.value.details["dataset_id"] == "ds2"

    # 关键断言：先处理的 ds1 也不得留下扣减
    assert ledger.get_budget("a1", "ds1").spent == 0.0
    assert ledger.get_budget("a1", "ds2").spent == 8.0

    # 失败 query_id 未登记，额度恢复后可用同一标识重试成功
    with pytest.raises(LedgerError):
        ledger.get_query("q-bundle")
    ledger.grant_budget(BudgetGrant(analyst_id="a1", dataset_id="ds2", budget_limit=20.0))
    r = ledger.submit_query(_query("q-bundle", "a1", ("ds1", 5.0), ("ds2", 5.0)))
    assert r.status == "SUCCESS"
    assert ledger.get_budget("a1", "ds1").spent == 5.0
    assert ledger.get_budget("a1", "ds2").spent == 13.0


def test_multi_dataset_unauthorized_middle_item_rolls_back(ledger, seeded):
    # a1 授权 ds1/ds2 但没有 ds3；ds3 在第二项，第一项也不得被扣
    with pytest.raises(LedgerError) as ei:
        ledger.submit_query(_query("q-auth", "a1", ("ds1", 1.0), ("ds3", 1.0)))
    assert ei.value.code is ErrorCode.UNAUTHORIZED_DATASET
    assert ledger.get_budget("a1", "ds1").spent == 0.0


def test_multi_dataset_missing_one_rejects_all(ledger, seeded):
    with pytest.raises(LedgerError) as ei:
        ledger.submit_query(_query("q-miss", "a1", ("ds1", 1.0), ("ghost-ds", 1.0)))
    assert ei.value.code is ErrorCode.DATASET_NOT_FOUND
    assert ledger.get_budget("a1", "ds1").spent == 0.0


def test_multi_dataset_idempotent_replay_returns_all_entries(ledger, seeded):
    body = lambda: _query("q-rep", "a1", ("ds1", 1.0), ("ds2", 2.0))
    ledger.submit_query(body())
    r = ledger.submit_query(body())
    assert r.idempotent_replay is True
    assert [e.dataset_id for e in r.entries] == ["ds1", "ds2"]
    assert ledger.get_budget("a1", "ds1").spent == 1.0
    assert ledger.get_budget("a1", "ds2").spent == 2.0
