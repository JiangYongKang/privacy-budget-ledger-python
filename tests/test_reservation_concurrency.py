"""预占与直接扣减并发混跑：已扣减 + 未过期预占任何时刻不越上限。"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from ledger.errors import BudgetInsufficientError, LedgerError
from ledger.models import DatasetCharge, ReservationItem
from ledger.service import BudgetService
from ledger.store import LedgerStore
from tests.conftest import log_case


def test_mixed_reserve_and_consume_never_exceeds_limit():
    # d1 上限 20；20 个直接扣减各 1 + 20 个预占各 1 同时打进来，
    # 最终 accepted 的（扣减 + 仍held预占 + 已结算）之和必须恰好 20。
    store = LedgerStore(":memory:").open()
    svc = BudgetService(store, default_ttl_seconds=3600)
    svc.register_analyst("a1")
    svc.register_dataset("d1")
    svc.authorize("a1", "d1", 20.0)

    n_each = 20
    barrier = threading.Barrier(n_each * 2)

    def consume_worker(i: int):
        barrier.wait()
        try:
            svc.consume(query_id=f"cq-{i}", analyst_id="a1",
                        charges=[DatasetCharge("d1", 1.0)])
            return ("consume_ok",)
        except BudgetInsufficientError:
            return ("consume_short",)
        except LedgerError as e:
            return (e.code,)

    def reserve_worker(i: int):
        barrier.wait()
        try:
            svc.reserve(reservation_id=f"rv-{i}", analyst_id="a1",
                        items=[ReservationItem("d1", 1.0)])
            return ("reserve_ok",)
        except BudgetInsufficientError:
            return ("reserve_short",)
        except LedgerError as e:
            return (e.code,)

    with ThreadPoolExecutor(max_workers=n_each * 2) as pool:
        futures = [pool.submit(consume_worker, i) for i in range(n_each)]
        futures += [pool.submit(reserve_worker, i) for i in range(n_each)]
        results = [f.result() for f in as_completed(futures)]

    kinds = {r[0] for r in results}
    assert kinds <= {"consume_ok", "consume_short", "reserve_ok", "reserve_short"}, kinds
    n_consume_ok = sum(1 for r in results if r[0] == "consume_ok")
    n_reserve_ok = sum(1 for r in results if r[0] == "reserve_ok")
    bal = svc.get_balance("a1", "d1")
    total_usage = bal.consumed + bal.reserved
    assert total_usage == 20.0, f"已扣+预占={total_usage}, 超出或不足"
    assert n_consume_ok + n_reserve_ok == 20
    assert bal.consumed + bal.reserved <= bal.limit
    log_case(
        "40线程预占/直接扣减混跑不越限",
        直接扣减成功=n_consume_ok, 预占成功=n_reserve_ok,
        已扣=bal.consumed, 在持预占=bal.reserved, 上限=bal.limit,
        判定依据="写锁串行+BEGIN IMMEDIATE+统一口径consumed+reserved复核",
    )


def test_concurrent_settle_same_reservation_applies_once():
    # 同一预占被多个线程同时结算，实际扣账只发生一次且结论一致
    store = LedgerStore(":memory:").open()
    svc = BudgetService(store, default_ttl_seconds=3600)
    svc.register_analyst("a1")
    svc.register_dataset("d1")
    svc.authorize("a1", "d1", 10.0)
    svc.reserve(reservation_id="rv-conc", analyst_id="a1",
                items=[ReservationItem("d1", 6.0)])

    n = 8
    barrier = threading.Barrier(n)

    def worker():
        barrier.wait()
        try:
            s = svc.settle(reservation_id="rv-conc", analyst_id="a1",
                           actual=[ReservationItem("d1", 4.0)])
            return ("ok", s.replay, s.total_settled, s.total_refunded)
        except LedgerError as e:
            return (e.code, None, None, None)

    with ThreadPoolExecutor(max_workers=n) as pool:
        futures = [pool.submit(worker) for _ in range(n)]
        results = [f.result() for f in futures]

    # 一个首次成功，其余全部 conflict（digest 相同情况下也可能全部拿到稳定结论；
    # 这里要求：恰好一次首扣，其它请求要么幂等重放要么冲突，绝不二次扣账）
    assert all(r[0] in ("ok",) for r in results), {r[0] for r in results}
    firsts = [r for r in results if r[1] is False]
    replays = [r for r in results if r[1] is True]
    assert len(firsts) == 1 and len(replays) == n - 1
    assert all(r[2] == 4.0 and r[3] == 2.0 for r in results)
    bal = svc.get_balance("a1", "d1")
    assert (bal.consumed, bal.reserved, bal.available) == (4.0, 0.0, 6.0)
    log_case(
        "同一预占并发结算只生效一次",
        线程数=n, 首次=len(firsts), 重放=len(replays),
        已扣=bal.consumed, 判定依据="状态机+digest在单写事务内判定",
    )


def test_concurrent_reserve_same_id_applies_once():
    store = LedgerStore(":memory:").open()
    svc = BudgetService(store, default_ttl_seconds=3600)
    svc.register_analyst("a1")
    svc.register_dataset("d1")
    svc.authorize("a1", "d1", 10.0)

    n = 10
    barrier = threading.Barrier(n)

    def worker():
        barrier.wait()
        try:
            r = svc.reserve(reservation_id="rv-same", analyst_id="a1",
                            items=[ReservationItem("d1", 3.0)])
            return ("ok", r.replay)
        except LedgerError as e:
            return (e.code, None)

    with ThreadPoolExecutor(max_workers=n) as pool:
        futures = [pool.submit(worker) for _ in range(n)]
        results = [f.result() for f in futures]
    assert {r[0] for r in results} == {"ok"}
    firsts = [r for r in results if r[1] is False]
    assert len(firsts) == 1
    assert svc.get_balance("a1", "d1").reserved == 3.0
    log_case("同一预占标识并发提交只占用一次", 线程数=n,
             首次=len(firsts), 在持预占=3.0,
             判定依据="PRIMARY KEY+事务内幂等重放")
