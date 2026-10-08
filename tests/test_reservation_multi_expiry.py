"""跨数据集组合预占的原子性 + 预占有效期到期自动释放。"""

from __future__ import annotations

import pytest

from ledger.errors import (
    BudgetInsufficientError,
    ReservationExpiredError,
)
from ledger.models import ReservationItem
from ledger.service import BudgetService
from ledger.store import LedgerStore
from tests.conftest import log_case


@pytest.fixture
def clock_service():
    """可控时钟的内存服务：a1 授权 d1=10, d2=5；初始时刻 1000。"""
    state = {"now": 1000.0}
    store = LedgerStore(":memory:", clock=lambda: state["now"]).open()
    svc = BudgetService(store, default_ttl_seconds=100.0,
                        clock=lambda: state["now"])
    svc.register_analyst("a1")
    svc.register_dataset("d1")
    svc.register_dataset("d2")
    svc.authorize("a1", "d1", 10.0)
    svc.authorize("a1", "d2", 5.0)
    return svc, state


def test_combined_reserve_all_or_nothing(clock_service):
    svc, _ = clock_service
    r = svc.reserve(
        reservation_id="rv-multi", analyst_id="a1",
        items=[ReservationItem("d1", 6.0), ReservationItem("d2", 4.0)],
    )
    assert {(h.dataset_id, h.reserved) for h in r.items} == {("d1", 6.0), ("d2", 4.0)}
    assert svc.get_balance("a1", "d1").available == 4.0
    assert svc.get_balance("a1", "d2").available == 1.0

    # d2 再多占 2 会越线 -> 两数据集整体不占
    with pytest.raises(BudgetInsufficientError):
        svc.reserve(
            reservation_id="rv-multi-fail", analyst_id="a1",
            items=[ReservationItem("d1", 1.0), ReservationItem("d2", 2.0)],
        )
    assert svc.get_balance("a1", "d1").reserved == 6.0
    assert svc.get_balance("a1", "d2").reserved == 4.0
    log_case(
        "跨数据集组合预占要么全占要么整体不生效",
        reservation_id="rv-multi-fail",
        d1预占=svc.get_balance("a1", "d1").reserved,
        d2预占=svc.get_balance("a1", "d2").reserved,
        判定依据="单事务批量插入,任一不足ROLLBACK",
    )


def test_combined_settle_refunds_per_dataset(clock_service):
    svc, _ = clock_service
    svc.reserve(
        reservation_id="rv-m2", analyst_id="a1",
        items=[ReservationItem("d1", 6.0), ReservationItem("d2", 4.0)],
    )
    # 实际 d1=6（全用），d2=1（退3）
    s = svc.settle(
        reservation_id="rv-m2", analyst_id="a1",
        actual=[ReservationItem("d1", 6.0), ReservationItem("d2", 1.0)],
    )
    by = {h.dataset_id: h for h in s.items}
    assert (by["d1"].settled, by["d1"].refunded) == (6.0, 0.0)
    assert (by["d2"].settled, by["d2"].refunded) == (1.0, 3.0)
    assert (svc.get_balance("a1", "d1").consumed, svc.get_balance("a1", "d1").available) == (6.0, 4.0)
    assert (svc.get_balance("a1", "d2").consumed, svc.get_balance("a1", "d2").available) == (1.0, 4.0)
    log_case("组合预占结算按数据集各自多退少补", reservation_id="rv-m2",
             d1结算=6.0, d2结算=1.0, d2退回=3.0,
             判定依据="逐数据集占用转扣减,同一事务提交")


def test_expiry_auto_releases_and_settle_rejected(clock_service):
    svc, state = clock_service
    svc.reserve(
        reservation_id="rv-ttl", analyst_id="a1",
        items=[ReservationItem("d1", 6.0)], ttl_seconds=10.0,
    )
    assert svc.get_balance("a1", "d1").available == 4.0

    # 未到期：结算允许
    state["now"] = 1009.0
    view = svc.get_reservation("rv-ttl")
    assert view.status == "held"

    # 到期：下一次写操作惰性清理，状态变 expired、额度退回
    state["now"] = 1011.0
    n = svc.sweep_expired()
    assert n == 1
    assert svc.get_reservation("rv-ttl").status == "expired"
    bal = svc.get_balance("a1", "d1")
    assert (bal.reserved, bal.available) == (0.0, 10.0)

    # 过期后结算必须被明确拒绝，而不是照常扣账
    with pytest.raises(ReservationExpiredError) as ei:
        svc.settle(reservation_id="rv-ttl", analyst_id="a1",
                   actual=[ReservationItem("d1", 1.0)])
    assert ei.value.code == "reservation_expired"
    assert bal.consumed == 0.0 and svc.get_balance("a1", "d1").consumed == 0.0
    log_case(
        "预占到期自动释放,过期结算被明确拒绝",
        reservation_id="rv-ttl", 到期时刻=1010.0, 当前=state["now"],
        释放后可用=svc.get_balance("a1", "d1").available,
        判定依据="expires_at<=now惰性置expired,settle校验状态",
    )


def test_expiry_is_lazy_on_next_write_and_does_not_mint_budget(clock_service):
    svc, state = clock_service
    svc.reserve(
        reservation_id="rv-lazy", analyst_id="a1",
        items=[ReservationItem("d1", 8.0)], ttl_seconds=5.0,
    )
    # 占了8，想再直接扣3 -> 不足
    from ledger.models import DatasetCharge
    from ledger.errors import BudgetInsufficientError as BIE
    with pytest.raises(BIE):
        svc.consume(query_id="q-blocked", analyst_id="a1",
                    charges=[DatasetCharge("d1", 3.0)])
    state["now"] = 1006.0  # 过期
    # 下一笔写操作（consume）事务内先清理过期预占，额度退回后扣减成功
    r = svc.consume(query_id="q-after-expiry", analyst_id="a1",
                    charges=[DatasetCharge("d1", 3.0)])
    assert r.total_charged == 3.0
    bal = svc.get_balance("a1", "d1")
    assert (bal.consumed, bal.reserved, bal.available) == (3.0, 0.0, 7.0)
    log_case("过期释放在下一笔写操作事务内惰性生效", reservation_id="rv-lazy",
             已扣=3.0, 可用=7.0, 判定依据="写事务内sweep,先释放再记账")


def test_release_after_expiry_is_rejected_not_double_refund(clock_service):
    svc, state = clock_service
    svc.reserve(
        reservation_id="rv-dbl", analyst_id="a1",
        items=[ReservationItem("d1", 4.0)], ttl_seconds=5.0,
    )
    state["now"] = 1006.0
    with pytest.raises(ReservationExpiredError):
        svc.release(reservation_id="rv-dbl", analyst_id="a1")
    # 只释放一次，可用额度恢复为完整的10，没有凭空多出
    assert svc.get_balance("a1", "d1").available == 10.0
    log_case("过期后归还被拒绝且不重复退款", reservation_id="rv-dbl",
             可用=10.0, 判定依据="expired状态拒绝release,系统已自动释放")


def test_ttl_is_configurable(clock_service):
    svc, state = clock_service
    # 短 TTL 便于本地快速验证
    svc.reserve(reservation_id="rv-short", analyst_id="a1",
                items=[ReservationItem("d1", 1.0)], ttl_seconds=1.0)
    state["now"] = 1001.5
    with pytest.raises(ReservationExpiredError):
        svc.settle(reservation_id="rv-short", analyst_id="a1",
                   actual=[ReservationItem("d1", 1.0)])
