"""预占与直接扣减并发混跑：任何时刻 已消耗 + 有效预占 <= 授权上限。"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from ledger.errors import (
    BudgetInsufficientError,
    LedgerError,
)
from ledger.models import DatasetCharge, ReservationCharge
from ledger.service import BudgetService
from ledger.store import LedgerStore
from tests.conftest import log_case


def test_concurrent_reserve_and_consume_never_exceeds_limit():
    """限额 10：30 个直接扣 1 + 30 个预占 1 同时打到同一额度上。

    只有合计 10 个"占住/扣掉"能成功；其余必须 budget_insufficient，
    且最终 consumed + reserved 恰好 = 10，绝不可能 > 10。
    """
    store = LedgerStore(":memory:").open()
    svc = BudgetService(store, default_ttl_seconds=3600)
    svc.register_analyst("a1")
    svc.register_dataset("d1")
    svc.authorize("a1", "d1", 10.0)

    n_consume = 30
    n_reserve = 30
    n = n_consume + n_reserve
    barrier = threading.Barrier(n)

    def worker(i: int):
        barrier.wait()
        try:
            if i < n_consume:
                svc.consume(
                    query_id=f"cq-{i}", analyst_id="a1",
                    charges=[DatasetCharge("d1", 1.0)],
                )
                return ("consume_ok", None)
            svc.reserve_budget(
                reservation_id=f"rv-{i}", analyst_id="a1",
                charges=[ReservationCharge("d1", 1.0)],
            )
            return ("reserve_ok", None)
        except BudgetInsufficientError:
            return ("insufficient", None)
        except LedgerError as e:  # 并发下不应出现其它错误
            return (e.code, None)

    with ThreadPoolExecutor(max_workers=n) as pool:
        results = [
            f.result()
            for f in as_completed([pool.submit(worker, i) for i in range(n)])
        ]

    statuses = [r[0] for r in results]
    others = set(statuses) - {"consume_ok", "reserve_ok", "insufficient"}
    assert not others, f"出现非预期结果: {others}"
    consumed_ok = statuses.count("consume_ok")
    reserved_ok = statuses.count("reserve_ok")
    insufficient = statuses.count("insufficient")

    bal = svc.get_balance("a1", "d1")
    assert consumed_ok + reserved_ok == 10, statuses
    assert insufficient == n - 10
    assert bal.consumed == float(consumed_ok)
    assert bal.reserved == float(reserved_ok)
    assert bal.consumed + bal.reserved <= bal.limit
    assert bal.available == 0.0
    log_case(
        "60线程:30直接扣+30预占混打额度10",
        直接扣成功=consumed_ok, 预占成功=reserved_ok,
        不足拒绝=insufficient,
        consumed=bal.consumed, reserved=bal.reserved,
        判定依据="单写锁串行,两类操作按 consumed+reserved<=limit 同一口径复核",
    )


def test_concurrent_reserve_blocks_consume_then_settle_frees():
    """预占先到先得：并发下预占占住的额度，直接扣减拿不到；结算后退回部分立即可用。"""
    store = LedgerStore(":memory:").open()
    svc = BudgetService(store, default_ttl_seconds=3600)
    svc.register_analyst("a1")
    svc.register_dataset("d1")
    svc.authorize("a1", "d1", 10.0)

    # 先预占 8（并发开始前已落库）
    svc.reserve_budget(
        reservation_id="hold", analyst_id="a1",
        charges=[ReservationCharge("d1", 8.0)],
    )

    n = 20
    barrier = threading.Barrier(n)

    def worker(i: int):
        barrier.wait()
        try:
            # 只剩可用 2：每个请求要 1，恰好 2 个成功
            svc.consume(
                query_id=f"take-{i}", analyst_id="a1",
                charges=[DatasetCharge("d1", 1.0)],
            )
            return "ok"
        except BudgetInsufficientError:
            return "insufficient"
        except LedgerError as e:
            return e.code

    with ThreadPoolExecutor(max_workers=n) as pool:
        outcomes = [
            f.result()
            for f in as_completed([pool.submit(worker, i) for i in range(n)])
        ]
    assert set(outcomes) <= {"ok", "insufficient"}, outcomes
    assert outcomes.count("ok") == 2, outcomes

    # 结算预占：实际只用 3 => 退回 5，额度立刻可再用
    st = svc.settle_reservation(
        reservation_id="hold", analyst_id="a1",
        charges=[ReservationCharge("d1", 3.0)],
    )
    assert st.total_refunded == 5.0
    bal = svc.get_balance("a1", "d1")
    assert bal.consumed == 5.0 and bal.reserved == 0.0 and bal.available == 5.0
    log_case(
        "预占占住后并发直接扣减受限,结算退回恢复可用",
        reservation_id="hold",
        并发成功=outcomes.count("ok"),
        consumed=bal.consumed, reserved=bal.reserved, available=bal.available,
        判定依据="预占计入可用额度复核,settle释放差额",
    )


def test_concurrent_same_reservation_id_holds_once():
    """25 线程并发用同一 reservation_id 提交相同预占：只占一份。"""
    store = LedgerStore(":memory:").open()
    svc = BudgetService(store, default_ttl_seconds=3600)
    svc.register_analyst("a1")
    svc.register_dataset("d1")
    svc.authorize("a1", "d1", 10.0)

    n = 25
    barrier = threading.Barrier(n)

    def worker():
        barrier.wait()
        try:
            r = svc.reserve_budget(
                reservation_id="same-rv", analyst_id="a1",
                charges=[ReservationCharge("d1", 4.0)],
            )
            return "replay" if r.replay else "accepted"
        except LedgerError as e:
            return e.code

    with ThreadPoolExecutor(max_workers=n) as pool:
        outcomes = [
            f.result()
            for f in as_completed([pool.submit(worker) for _ in range(n)])
        ]
    assert outcomes.count("accepted") == 1, outcomes
    assert outcomes.count("replay") == n - 1, outcomes
    bal = svc.get_balance("a1", "d1")
    assert bal.reserved == 4.0 and bal.consumed == 0.0 and bal.available == 6.0
    log_case(
        "25线程并发同一reservation_id",
        reservation_id="same-rv",
        首次成功=outcomes.count("accepted"),
        幂等重放=outcomes.count("replay"),
        reserved=bal.reserved, available=bal.available,
        判定依据="reservations主键+事务内先查后插,重复提交重放",
    )
