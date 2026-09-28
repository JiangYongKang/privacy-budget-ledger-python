"""并发争抢同一份额度：累计消耗不得越过上限，幂等结果必须稳定。"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from ledger.errors import (
    BudgetInsufficientError,
    LedgerError,
)
from ledger.models import DatasetCharge
from ledger.service import BudgetService
from ledger.store import LedgerStore
from tests.conftest import log_case


def test_concurrent_contention_never_exceeds_limit():
    store = LedgerStore(":memory:").open()
    svc = BudgetService(store)
    svc.register_analyst("a1")
    svc.register_dataset("d1")
    svc.authorize("a1", "d1", 10.0)

    n = 40
    barrier = threading.Barrier(n)  # 尽量让所有线程同时发起

    def worker(i: int):
        barrier.wait()
        try:
            r = svc.consume(
                query_id=f"cq-{i}",
                analyst_id="a1",
                charges=[DatasetCharge("d1", 1.0)],
            )
            return ("ok", r.replay)
        except BudgetInsufficientError:
            return ("budget_insufficient", None)
        except LedgerError as e:
            return (e.code, None)

    with ThreadPoolExecutor(max_workers=n) as pool:
        results = [f.result() for f in as_completed([pool.submit(worker, i) for i in range(n)])]

    ok = [r for r in results if r[0] == "ok"]
    insufficient = [r for r in results if r[0] == "budget_insufficient"]
    others = {r[0] for r in results} - {"ok", "budget_insufficient"}
    assert not others, f"出现非预期结果: {others}"
    bal = svc.get_balance("a1", "d1")
    assert len(ok) == 10, f"成功数应为10, 实际 {len(ok)}: {sorted(results)}"
    assert len(insufficient) == 30
    assert all(replay is False for _, replay in ok)
    assert bal.consumed == 10.0 and bal.balance == 0.0 and bal.consumed <= bal.limit
    log_case(
        "40线程并发争抢额度10",
        query_id="cq-0..cq-39",
        成功数=len(ok), 不足失败数=len(insufficient),
        扣减前余额=10.0, 扣减后余额=bal.balance,
        判定依据="单写锁+BEGIN IMMEDIATE串行化,事务内复核余额",
    )


def test_concurrent_same_query_id_charges_once():
    store = LedgerStore(":memory:").open()
    svc = BudgetService(store)
    svc.register_analyst("a1")
    svc.register_dataset("d1")
    svc.authorize("a1", "d1", 10.0)

    n = 25
    barrier = threading.Barrier(n)

    def worker():
        barrier.wait()
        try:
            r = svc.consume(
                query_id="same-q",
                analyst_id="a1",
                charges=[DatasetCharge("d1", 3.0)],
            )
            return "replay" if r.replay else "accepted"
        except LedgerError as e:
            return e.code

    with ThreadPoolExecutor(max_workers=n) as pool:
        outcomes = [f.result() for f in as_completed([pool.submit(worker) for _ in range(n)])]

    accepted = outcomes.count("accepted")
    replay = outcomes.count("replay")
    bal = svc.get_balance("a1", "d1")
    assert accepted == 1, outcomes
    assert replay == n - 1, outcomes
    assert bal.consumed == 3.0 and bal.balance == 7.0
    log_case(
        "25线程并发提交同一query_id",
        query_id="same-q",
        首次成功=accepted, 幂等重放=replay,
        扣减前余额=10.0, 扣减后余额=bal.balance,
        判定依据="UNIQUE(query_id,dataset_id)+事务内先查后插",
    )


def test_concurrent_multi_dataset_no_partial_state():
    store = LedgerStore(":memory:").open()
    svc = BudgetService(store)
    svc.register_analyst("a1")
    svc.register_dataset("d1")
    svc.register_dataset("d2")
    svc.authorize("a1", "d1", 10.0)
    svc.authorize("a1", "d2", 10.0)

    # 每个请求扣 d1+d2 各 3；限额 10 => 恰好 3 个全成功（9），第 4 个整体失败
    n = 8
    barrier = threading.Barrier(n)

    def worker(i: int):
        barrier.wait()
        try:
            svc.consume(
                query_id=f"mq-{i}",
                analyst_id="a1",
                charges=[DatasetCharge("d1", 3.0), DatasetCharge("d2", 3.0)],
            )
            return "ok"
        except BudgetInsufficientError:
            return "insufficient"

    with ThreadPoolExecutor(max_workers=n) as pool:
        outcomes = [f.result() for f in as_completed([pool.submit(worker, i) for i in range(n)])]

    b1, b2 = svc.get_balance("a1", "d1"), svc.get_balance("a1", "d2")
    assert outcomes.count("ok") == 3, outcomes
    assert outcomes.count("insufficient") == 5
    # 两数据集消耗必须完全一致：不存在只扣一半的中间状态
    assert b1.consumed == b2.consumed == 9.0
    log_case(
        "8线程并发跨数据集组合扣减",
        query_id="mq-0..mq-7",
        全成功=outcomes.count("ok"), 整体失败=outcomes.count("insufficient"),
        d1扣减后=b1.consumed, d2扣减后=b2.consumed,
        判定依据="组合扣减在同一事务,任一不足则全部不生效",
    )


def test_concurrent_mixed_idempotent_and_distinct_stable():
    """混合压力：部分线程发同一 query_id（只应扣一次），部分发独立 query_id，
    跨两个数据集，最终每个 (分析者,数据集) 累计消耗都必须稳定且不越上限。"""
    store = LedgerStore(":memory:").open()
    svc = BudgetService(store)
    svc.register_analyst("a1")
    svc.register_dataset("d1")
    svc.register_dataset("d2")
    svc.authorize("a1", "d1", 20.0)
    svc.authorize("a1", "d2", 20.0)

    n = 60
    barrier = threading.Barrier(n)

    def worker(i: int):
        barrier.wait()
        # 前 20 个线程全部用同一个 query_id 重复提交（应只扣一次 d1=4）
        # 后 40 个线程各自独立 query_id，每个扣 d1=1,d2=1（应成功 16 个，用 16）
        if i < 20:
            qid, charges = "shared-q", [DatasetCharge("d1", 4.0)]
        else:
            qid, charges = f"solo-{i}", [DatasetCharge("d1", 1.0), DatasetCharge("d2", 1.0)]
        try:
            r = svc.consume(query_id=qid, analyst_id="a1", charges=charges)
            return ("accepted" if not r.replay else "replay", qid)
        except BudgetInsufficientError:
            return ("budget_insufficient", qid)

    with ThreadPoolExecutor(max_workers=n) as pool:
        outcomes = [f.result() for f in as_completed([pool.submit(worker, i) for i in range(n)])]

    statuses = [o[0] for o in outcomes]
    b1 = svc.get_balance("a1", "d1")
    b2 = svc.get_balance("a1", "d2")
    # shared 恰好 1 次 accepted、19 次 replay；solo 中 d1 可用 16（20-4）
    assert statuses.count("accepted") == 1 + 16, statuses
    assert statuses.count("replay") == 19
    assert statuses.count("budget_insufficient") == 24
    assert b1.consumed == 20.0 and b1.balance == 0.0
    assert b2.consumed == 16.0 and b2.balance == 4.0
    log_case(
        "60线程混合(幂等+独立+多数据集)压力",
        query_id="shared-q/solo-*",
        d1扣减后=b1.consumed, d2扣减后=b2.consumed,
        判定依据="组合事务+query唯一约束+余额事务内复核",
    )
