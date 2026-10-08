"""预占与直接扣减的边界共存：事件来源隔离、结算缺项、浮点口径。"""

from __future__ import annotations

import pytest

from ledger.errors import InvalidParameterError, ReservationConflictError
from ledger.models import DatasetCharge, ReservationItem
from tests.conftest import log_case


def test_settle_events_do_not_collide_with_user_query_ids(service):
    # 用户直接扣减恰好叫 settle:rv -> 不允许与预占结算事件互相撞唯一约束
    service.reserve(reservation_id="rv", analyst_id="a1",
                    items=[ReservationItem("d1", 3.0)])
    r = service.consume(query_id="settle:rv", analyst_id="a1",
                        charges=[DatasetCharge("d1", 1.0)])
    assert r.total_charged == 1.0
    s = service.settle(reservation_id="rv", analyst_id="a1",
                       actual=[ReservationItem("d1", 2.0)])
    assert s.total_settled == 2.0
    bal = service.get_balance("a1", "d1")
    assert (bal.consumed, bal.reserved, bal.available) == (3.0, 0.0, 7.0)
    # 直接扣减查询仍可独立幂等回放
    replay = service.consume(query_id="settle:rv", analyst_id="a1",
                             charges=[DatasetCharge("d1", 1.0)])
    assert replay.replay is True
    assert service.get_balance("a1", "d1").consumed == 3.0
    log_case("预占结算与同名直接扣减互不冲突", reservation_id="rv",
             已扣=3.0, 判定依据="事件按来源direct/settle分别唯一")


def test_settle_must_cover_all_reserved_datasets(service):
    service.reserve(reservation_id="rv-miss", analyst_id="a1",
                    items=[ReservationItem("d1", 2.0), ReservationItem("d2", 2.0)])
    with pytest.raises(InvalidParameterError):
        service.settle(reservation_id="rv-miss", analyst_id="a1",
                       actual=[ReservationItem("d1", 1.0)])
    # 失败不改动占用
    assert service.get_balance("a1", "d1").reserved == 2.0
    assert service.get_balance("a1", "d2").reserved == 2.0


def test_settle_with_unrelated_dataset_is_conflict(service):
    service.reserve(reservation_id="rv-ext", analyst_id="a1",
                    items=[ReservationItem("d1", 2.0)])
    with pytest.raises(ReservationConflictError):
        service.settle(reservation_id="rv-ext", analyst_id="a1",
                       actual=[ReservationItem("d1", 2.0), ReservationItem("d2", 1.0)])


def test_decimal_accounting(service):
    service.reserve(reservation_id="rv-dec", analyst_id="a1",
                    items=[ReservationItem("d1", 0.3)])
    s = service.settle(reservation_id="rv-dec", analyst_id="a1",
                       actual=[ReservationItem("d1", 0.1)])
    assert s.total_settled == pytest.approx(0.1)
    assert s.total_refunded == pytest.approx(0.2)
    bal = service.get_balance("a1", "d1")
    assert bal.consumed == pytest.approx(0.1)
    assert bal.available == pytest.approx(9.9)
