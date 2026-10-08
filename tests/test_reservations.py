"""预占生命周期：先预占、后结算/归还、幂等只生效一次、失败原因可区分。"""

from __future__ import annotations

import pytest

from ledger.errors import (
    AnalystNotFoundError,
    BudgetInsufficientError,
    DatasetNotFoundError,
    InvalidParameterError,
    ReservationConflictError,
    ReservationExpiredError,
    ReservationNotFoundError,
    ReservationStateError,
    UnauthorizedDatasetError,
)
from ledger.models import DatasetCharge, ReservationItem
from tests.conftest import log_case


def test_reserve_then_settle_refunds_overage(service):
    # d1 上限10：预占6，实际结算4，应退回2
    r = service.reserve(
        reservation_id="rv-1", analyst_id="a1",
        items=[ReservationItem("d1", 6.0)],
    )
    assert r.replay is False and r.expires_at
    hold = r.items[0]
    assert (hold.reserved, hold.balance_before, hold.balance_after) == (6.0, 10.0, 4.0)
    bal = service.get_balance("a1", "d1")
    assert (bal.consumed, bal.reserved, bal.available) == (0.0, 6.0, 4.0)

    s = service.settle(
        reservation_id="rv-1", analyst_id="a1",
        actual=[ReservationItem("d1", 4.0)],
    )
    assert s.total_settled == 4.0 and s.total_refunded == 2.0
    h = s.items[0]
    assert (h.settled, h.refunded, h.balance_after) == (4.0, 2.0, 6.0)
    bal = service.get_balance("a1", "d1")
    assert (bal.consumed, bal.reserved, bal.available) == (4.0, 0.0, 6.0)
    log_case(
        "预占后按实际用量结算,多占退回",
        reservation_id="rv-1", 预占=6.0, 实际=4.0, 退回=2.0,
        结算后可用=bal.available, 判定依据="占用转扣减,差额释放",
    )


def test_reserve_zero_usage_settle_refunds_all(service):
    service.reserve(
        reservation_id="rv-zero", analyst_id="a1",
        items=[ReservationItem("d1", 3.0)],
    )
    s = service.settle(
        reservation_id="rv-zero", analyst_id="a1",
        actual=[ReservationItem("d1", 0.0)],
    )
    assert s.total_settled == 0.0 and s.total_refunded == 3.0
    bal = service.get_balance("a1", "d1")
    assert (bal.consumed, bal.reserved, bal.available) == (0.0, 0.0, 10.0)
    log_case(
        "零用量结算=全额退回且不产生扣减",
        reservation_id="rv-zero", 预占=3.0, 结算=0.0,
        判定依据="actual=0允许,只释放占用",
    )


def test_reserve_then_release_returns_full_hold(service):
    r = service.reserve(
        reservation_id="rv-rel", analyst_id="a1",
        items=[ReservationItem("d1", 7.0)],
    )
    assert service.get_balance("a1", "d1").available == 3.0
    rel = service.release(reservation_id="rv-rel", analyst_id="a1")
    assert rel.status == "released" and rel.total_refunded == 7.0
    assert rel.items[0].balance_after == 10.0
    assert service.get_balance("a1", "d1").available == 10.0
    assert service.get_reservation("rv-rel").status == "released"
    log_case(
        "任务取消整笔归还预占",
        reservation_id="rv-rel", 退回=7.0, 归还后可用=10.0,
        判定依据="held->released,占用从reserved口径移除",
    )


def test_reserve_settle_release_are_each_idempotent(service):
    service.reserve(
        reservation_id="rv-idem", analyst_id="a1",
        items=[ReservationItem("d1", 5.0)],
    )
    r2 = service.reserve(
        reservation_id="rv-idem", analyst_id="a1",
        items=[ReservationItem("d1", 5.0)],
    )
    assert r2.replay is True and r2.expires_at  # 结论与首次一致

    s1 = service.settle(
        reservation_id="rv-idem", analyst_id="a1",
        actual=[ReservationItem("d1", 3.0)],
    )
    s2 = service.settle(
        reservation_id="rv-idem", analyst_id="a1",
        actual=[ReservationItem("d1", 3.0)],
    )
    assert s1.replay is False and s2.replay is True
    assert (s2.total_settled, s2.total_refunded) == (3.0, 2.0)
    assert service.get_balance("a1", "d1").consumed == 3.0  # 没有二次扣账

    service.reserve(
        reservation_id="rv-rel2", analyst_id="a1",
        items=[ReservationItem("d1", 2.0)],
    )
    rel1 = service.release(reservation_id="rv-rel2", analyst_id="a1")
    rel2 = service.release(reservation_id="rv-rel2", analyst_id="a1")
    assert rel1.replay is False and rel2.replay is True
    assert rel2.total_refunded == 2.0
    assert service.get_balance("a1", "d1").reserved == 0.0
    log_case(
        "预占/结算/归还重复提交均只生效一次",
        reservation_id="rv-idem/rv-rel2",
        结算次数=2, 实际扣账=service.get_balance("a1", "d1").consumed,
        判定依据="digest指纹+状态机,重复提交重放首次结论",
    )


def test_reserve_conflicts_on_different_content(service):
    service.reserve(
        reservation_id="rv-c", analyst_id="a1",
        items=[ReservationItem("d1", 5.0)],
    )
    with pytest.raises(ReservationConflictError):
        service.reserve(
            reservation_id="rv-c", analyst_id="a1",
            items=[ReservationItem("d1", 6.0)],
        )
    # 冲突不产生任何额外占用
    assert service.get_balance("a1", "d1").reserved == 5.0
    log_case("同一预占标识内容不一致被拒绝且零影响",
             reservation_id="rv-c", 判定依据="reservation_conflict")


def test_settle_with_different_actual_is_conflict(service):
    service.reserve(
        reservation_id="rv-sc", analyst_id="a1",
        items=[ReservationItem("d1", 5.0)],
    )
    service.settle(reservation_id="rv-sc", analyst_id="a1",
                   actual=[ReservationItem("d1", 3.0)])
    with pytest.raises(ReservationConflictError):
        service.settle(reservation_id="rv-sc", analyst_id="a1",
                       actual=[ReservationItem("d1", 4.0)])
    assert service.get_balance("a1", "d1").consumed == 3.0


def test_settle_after_release_is_state_error(service):
    service.reserve(
        reservation_id="rv-st", analyst_id="a1",
        items=[ReservationItem("d1", 5.0)],
    )
    service.release(reservation_id="rv-st", analyst_id="a1")
    with pytest.raises(ReservationStateError):
        service.settle(reservation_id="rv-st", analyst_id="a1",
                       actual=[ReservationItem("d1", 1.0)])


def test_release_after_settle_is_state_error(service):
    service.reserve(
        reservation_id="rv-rs", analyst_id="a1",
        items=[ReservationItem("d1", 5.0)],
    )
    service.settle(reservation_id="rv-rs", analyst_id="a1",
                   actual=[ReservationItem("d1", 2.0)])
    with pytest.raises(ReservationStateError):
        service.release(reservation_id="rv-rs", analyst_id="a1")


def test_reservation_not_found(service):
    with pytest.raises(ReservationNotFoundError):
        service.release(reservation_id="nope", analyst_id="a1")
    with pytest.raises(ReservationNotFoundError):
        service.settle(reservation_id="nope", analyst_id="a1",
                       actual=[ReservationItem("d1", 1.0)])


def test_reserve_validates_entities_and_authorization(service):
    with pytest.raises(AnalystNotFoundError):
        service.reserve(reservation_id="x", analyst_id="ghost",
                        items=[ReservationItem("d1", 1.0)])
    with pytest.raises(DatasetNotFoundError):
        service.reserve(reservation_id="x", analyst_id="a1",
                        items=[ReservationItem("ghost", 1.0)])
    with pytest.raises(UnauthorizedDatasetError):
        service.reserve(reservation_id="x", analyst_id="a1",
                        items=[ReservationItem("d3", 1.0)])
    log_case("预占的实体/授权错误仍然可区分且零占用",
             reservation_id="x", 判定依据="analyst/dataset/unauthorized")


def test_reserve_parameter_validation(service):
    with pytest.raises(InvalidParameterError):
        service.reserve(reservation_id=" ", analyst_id="a1",
                        items=[ReservationItem("d1", 1.0)])
    with pytest.raises(InvalidParameterError):
        service.reserve(reservation_id="z", analyst_id="a1", items=[])
    with pytest.raises(InvalidParameterError):
        service.reserve(reservation_id="z", analyst_id="a1",
                        items=[ReservationItem("d1", 0.0)])
    with pytest.raises(InvalidParameterError):
        service.reserve(reservation_id="z", analyst_id="a1",
                        items=[ReservationItem("d1", 1.0),
                               ReservationItem("d1", 2.0)])
    with pytest.raises(InvalidParameterError):
        service.reserve(reservation_id="z", analyst_id="a1",
                        items=[ReservationItem("d1", 1.0)], ttl_seconds=0)


def test_settle_cannot_charge_more_than_reserved(service):
    service.reserve(
        reservation_id="rv-over", analyst_id="a1",
        items=[ReservationItem("d1", 3.0)],
    )
    with pytest.raises(InvalidParameterError):
        service.settle(reservation_id="rv-over", analyst_id="a1",
                       actual=[ReservationItem("d1", 3.5)])
    # 被拒绝后占用与扣减都不变
    bal = service.get_balance("a1", "d1")
    assert (bal.consumed, bal.reserved) == (0.0, 3.0)


def test_reserve_insufficient_is_atomic_and_distinct(service):
    # d1 可用10, d2 可用5；d2 预占6 不足 => 整体失败，d1 也不占用
    with pytest.raises(BudgetInsufficientError) as ei:
        service.reserve(
            reservation_id="rv-poor", analyst_id="a1",
            items=[ReservationItem("d1", 9.0), ReservationItem("d2", 6.0)],
        )
    assert ei.value.code == "budget_insufficient"
    assert service.get_balance("a1", "d1").reserved == 0.0
    log_case("组合预占任一不足整体不生效",
             reservation_id="rv-poor", 判定依据="budget_insufficient+事务回滚")


def test_unified_accounting_reserve_and_consume_share_limit(service):
    """已扣减 + 未过期预占任何时刻不越上限。"""
    service.consume(query_id="q-base", analyst_id="a1",
                    charges=[DatasetCharge("d1", 6.0)])
    service.reserve(reservation_id="rv-u", analyst_id="a1",
                    items=[ReservationItem("d1", 3.0)])
    bal = service.get_balance("a1", "d1")
    assert (bal.consumed, bal.reserved, bal.available) == (6.0, 3.0, 1.0)
    # 再直接扣 2 -> 越线被拒
    with pytest.raises(BudgetInsufficientError):
        service.consume(query_id="q-too-much", analyst_id="a1",
                        charges=[DatasetCharge("d1", 2.0)])
    # 再预占 1 恰好可（边界）
    service.reserve(reservation_id="rv-edge", analyst_id="a1",
                    items=[ReservationItem("d1", 1.0)])
    assert service.get_balance("a1", "d1").available == 0.0
    log_case(
        "直接扣减与预占统一口径不越限",
        reservation_id="rv-u/rv-edge", 已扣=6.0, 预占=4.0, 上限=10.0,
        判定依据="consumed+reserved+new<=limit",
    )
