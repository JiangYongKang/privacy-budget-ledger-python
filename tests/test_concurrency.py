"""并发争抢：累计消耗不越上限；同一 query_id 并发只成功一次。"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from privacy_budget_ledger.errors import ErrorCode, LedgerError
from privacy_budget_ledger.models import QueryItem, QueryRequest


def _query(query_id: str, analyst: str, cost: float, dataset: str = "ds1") -> QueryRequest:
    return QueryRequest(
        query_id=query_id,
        analyst_id=analyst,
        items=[QueryItem(dataset_id=dataset, cost=cost)],
    )


def test_concurrent_distinct_queries_never_exceed_limit(ledger, seeded):
    # 上限 10，20 个线程各请求 cost=1：恰好 10 个成功、10 个额度不足
    n = 20
    results: list[tuple[str, str]] = []
    lock = threading.Lock()

    def worker(i: int):
        qid = f"q-c-{i:02d}"
        try:
            ledger.submit_query(_query(qid, "a1", 1.0))
            outcome = "SUCCESS"
        except LedgerError as exc:
            outcome = exc.code.value
        with lock:
            results.append((qid, outcome))

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(worker, range(n)))

    success = [q for q, o in results if o == "SUCCESS"]
    rejected = [o for _, o in results if o == ErrorCode.BUDGET_INSUFFICIENT.value]
    assert len(success) == 10, results
    assert len(rejected) == 10, results
    assert len(set(success)) == 10  # 成功的 query_id 互不相同

    view = ledger.get_budget("a1", "ds1")
    assert view.spent == pytest.approx(10.0)
    assert view.remaining == pytest.approx(0.0)
    assert view.spent <= view.budget_limit

    # 每个成功 query_id 都只登记一次，回放不再扣减
    for qid in success:
        r = ledger.submit_query(_query(qid, "a1", 1.0))
        assert r.idempotent_replay is True
    assert ledger.get_budget("a1", "ds1").spent == pytest.approx(10.0)


def test_concurrent_same_query_id_succeeds_exactly_once(ledger, seeded):
    # 16 个线程用完全相同的请求体和 query_id 并发提交
    n = 16
    outcomes = []
    lock = threading.Lock()
    barrier = threading.Barrier(n)

    def worker():
        barrier.wait()  # 尽量放大同时进入事务的窗口
        try:
            r = ledger.submit_query(_query("q-same", "a1", 3.0))
            outcome = ("REPLAY" if r.idempotent_replay else "SUCCESS")
        except LedgerError as exc:
            outcome = exc.code.value
        with lock:
            outcomes.append(outcome)

    with ThreadPoolExecutor(max_workers=n) as pool:
        list(pool.map(lambda _: worker(), range(n)))

    assert outcomes.count("SUCCESS") == 1, outcomes
    assert outcomes.count("REPLAY") == n - 1, outcomes
    # 只记一次账
    assert ledger.get_budget("a1", "ds1").spent == pytest.approx(3.0)


def test_concurrent_multi_dataset_total_never_exceeds(ledger, seeded):
    # 60 个线程并发组合扣减 ds1/ds2，各 cost=0.5；两数据集上限均为 10
    n = 60

    def worker(i: int):
        try:
            ledger.submit_query(
                QueryRequest(
                    query_id=f"q-m-{i:02d}",
                    analyst_id="a1",
                    items=[QueryItem(dataset_id="ds1", cost=0.5),
                           QueryItem(dataset_id="ds2", cost=0.5)],
                )
            )
            return "SUCCESS"
        except LedgerError as exc:
            return exc.code.value

    with ThreadPoolExecutor(max_workers=12) as pool:
        outcomes = list(pool.map(worker, range(n)))

    assert outcomes.count("SUCCESS") == 20, {o: outcomes.count(o) for o in set(outcomes)}
    assert outcomes.count(ErrorCode.BUDGET_INSUFFICIENT.value) == 40
    assert ledger.get_budget("a1", "ds1").spent == pytest.approx(10.0)
    assert ledger.get_budget("a1", "ds2").spent == pytest.approx(10.0)
