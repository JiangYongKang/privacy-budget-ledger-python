"""幂等消耗：唯一查询标识只记一次账；重启后额度与结论保留。"""

from __future__ import annotations

import pytest

from privacy_budget_ledger.errors import ErrorCode, LedgerError
from privacy_budget_ledger.ledger import BudgetLedger
from privacy_budget_ledger.models import QueryItem, QueryRequest
from privacy_budget_ledger.storage import Storage


def _query(query_id: str, analyst: str, *pairs) -> QueryRequest:
    return QueryRequest(
        query_id=query_id,
        analyst_id=analyst,
        items=[QueryItem(dataset_id=d, cost=c) for d, c in pairs],
    )


def test_repeated_submission_deducts_once(ledger, seeded):
    r1 = ledger.submit_query(_query("q-idem", "a1", ("ds1", 4.0)))
    r2 = ledger.submit_query(_query("q-idem", "a1", ("ds1", 4.0)))
    r3 = ledger.submit_query(_query("q-idem", "a1", ("ds1", 4.0)))

    assert r1.idempotent_replay is False
    for r in (r2, r3):
        assert r.status == "SUCCESS"
        assert r.idempotent_replay is True
        assert r.entries == r1.entries  # 回放首次结论，金额完全一致

    view = ledger.get_budget("a1", "ds1")
    assert (view.spent, view.remaining) == (4.0, 6.0)  # 只扣了一次


def test_same_query_id_different_body_is_conflict(ledger, seeded):
    ledger.submit_query(_query("q-c", "a1", ("ds1", 1.0)))
    with pytest.raises(LedgerError) as ei:
        ledger.submit_query(_query("q-c", "a1", ("ds1", 2.0)))  # 金额不同
    assert ei.value.code is ErrorCode.QUERY_ID_CONFLICT
    # 冲突请求不得产生任何扣减
    assert ledger.get_budget("a1", "ds1").spent == 1.0

    with pytest.raises(LedgerError) as ei:
        ledger.submit_query(_query("q-c", "a1", ("ds2", 1.0)))  # 数据集不同
    assert ei.value.code is ErrorCode.QUERY_ID_CONFLICT
    assert ledger.get_budget("a1", "ds2").spent == 0.0


def test_item_order_difference_is_same_request(ledger, seeded):
    r1 = ledger.submit_query(_query("q-order", "a1", ("ds1", 1.0), ("ds2", 2.0)))
    # 仅条目顺序不同：视为同一请求，回放而不是冲突，也不二次扣减
    r2 = ledger.submit_query(_query("q-order", "a1", ("ds2", 2.0), ("ds1", 1.0)))
    assert r2.idempotent_replay is True
    assert ledger.get_budget("a1", "ds1").spent == 1.0
    assert ledger.get_budget("a1", "ds2").spent == 2.0


def test_failed_submission_is_not_registered_and_can_be_retried(ledger, seeded):
    # 首次因额度不足失败，不应登记 query_id
    with pytest.raises(LedgerError) as ei:
        ledger.submit_query(_query("q-retry", "a1", ("ds1", 12.0)))
    assert ei.value.code is ErrorCode.BUDGET_INSUFFICIENT

    with pytest.raises(LedgerError) as ei:
        ledger.get_query("q-retry")
    assert ei.value.code is ErrorCode.QUERY_NOT_FOUND

    # 上调额度后用同一 query_id 重试，应当成功（失败不占位）
    from privacy_budget_ledger.models import BudgetGrant
    ledger.grant_budget(BudgetGrant(analyst_id="a1", dataset_id="ds1", budget_limit=20.0))
    r = ledger.submit_query(_query("q-retry", "a1", ("ds1", 12.0)))
    assert r.status == "SUCCESS"
    assert ledger.get_budget("a1", "ds1").spent == 12.0


def test_get_query_returns_registered_conclusion(ledger, seeded):
    ledger.submit_query(_query("q-lookup", "a1", ("ds1", 2.5)))
    view = ledger.get_query("q-lookup")
    assert view.status == "SUCCESS"
    assert view.idempotent_replay is True
    assert view.entries[0].cost == 2.5


def test_persistence_across_restart(db_path, ledger, seeded):
    ledger.submit_query(_query("q-persist", "a1", ("ds1", 3.5), ("ds2", 1.5)))

    # 模拟服务重启：丢弃内存对象，按同一文件路径新建账本
    storage2 = Storage(db_path)
    storage2.init_schema()
    reopened = BudgetLedger(storage2)
    try:
        assert reopened.get_budget("a1", "ds1").spent == 3.5
        assert reopened.get_budget("a1", "ds2").spent == 1.5
        # 重启后重复提交同一 query_id：只回放，不再扣减
        r = reopened.submit_query(_query("q-persist", "a1", ("ds1", 3.5), ("ds2", 1.5)))
        assert r.idempotent_replay is True
        assert reopened.get_budget("a1", "ds1").spent == 3.5
        assert reopened.get_budget("a1", "ds2").spent == 1.5
        # 历史结论可查
        assert reopened.get_query("q-persist").entries[1].spent_after == 1.5
    finally:
        storage2.close()
