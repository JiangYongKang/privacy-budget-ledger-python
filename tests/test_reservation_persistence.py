"""预占在服务重启后仍然有效：占用不丢、不凭空多出额度、到期自动释放。"""

from __future__ import annotations

from pathlib import Path

import pytest

from ledger.errors import ReservationExpiredError
from ledger.models import ReservationCharge
from ledger.service import BudgetService
from ledger.store import LedgerStore
from tests.conftest import log_case


class FakeClock:
    def __init__(self, start: float = 5_000.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


def _fresh_service(db_path: Path, clock: FakeClock, ttl: int = 100) -> BudgetService:
    store = LedgerStore(db_path, clock=clock).open()
    return BudgetService(store, default_ttl_seconds=ttl)


def test_held_reservation_survives_restart(tmp_path: Path):
    db = tmp_path / "ledger.db"
    clock = FakeClock()
    svc = _fresh_service(db, clock)
    svc.register_analyst("a1")
    svc.register_dataset("d1")
    svc.authorize("a1", "d1", 10.0)
    svc.reserve_budget(
        reservation_id="p1", analyst_id="a1",
        charges=[ReservationCharge("d1", 7.0)], ttl_seconds=100,
    )
    svc._store.close()

    # 时间只前进 30s（未过期）后"重启"：占用必须仍在
    clock.advance(30)
    svc2 = _fresh_service(db, clock)
    b = svc2.get_balance("a1", "d1")
    assert b.consumed == 0.0 and b.reserved == 7.0 and b.available == 3.0
    view = svc2.get_reservation("p1")
    assert view.status == "held"

    # 重启后仍能正常结算
    st = svc2.settle_reservation(
        reservation_id="p1", analyst_id="a1",
        charges=[ReservationCharge("d1", 4.0)],
    )
    assert st.total_charged == 4.0 and st.total_refunded == 3.0
    b2 = svc2.get_balance("a1", "d1")
    assert b2.consumed == 4.0 and b2.reserved == 0.0 and b2.available == 6.0
    log_case(
        "有效期内重启:占用不丢且可结算",
        reservation_id="p1",
        重启后预占=b.reserved, 结算后消耗=b2.consumed,
        判定依据="预占主表/明细持久化,重开库回放校验通过",
    )


def test_restart_after_expiry_releases_quota(tmp_path: Path):
    db = tmp_path / "ledger.db"
    clock = FakeClock()
    svc = _fresh_service(db, clock)
    svc.register_analyst("a1")
    svc.register_dataset("d1")
    svc.authorize("a1", "d1", 10.0)
    svc.reserve_budget(
        reservation_id="p2", analyst_id="a1",
        charges=[ReservationCharge("d1", 8.0)], ttl_seconds=100,
    )
    svc._store.close()

    # 超过有效期后重启：打开库即 sweep，额度自动退回，结算被拒
    clock.advance(200)
    svc2 = _fresh_service(db, clock)
    view = svc2.get_reservation("p2")
    assert view.status == "expired"
    b = svc2.get_balance("a1", "d1")
    assert b.reserved == 0.0 and b.consumed == 0.0 and b.available == 10.0
    with pytest.raises(ReservationExpiredError):
        svc2.settle_reservation(
            reservation_id="p2", analyst_id="a1",
            charges=[ReservationCharge("d1", 1.0)],
        )
    log_case(
        "重启时已到期:自动失效退额度且拒绝结算",
        reservation_id="p2",
        状态=view.status, 可用=b.available,
        判定依据="open()先sweep再校验,过期预占不再占额度",
    )


def test_settled_reservation_persists_no_double_charge(tmp_path: Path):
    db = tmp_path / "ledger.db"
    clock = FakeClock()
    svc = _fresh_service(db, clock)
    svc.register_analyst("a1")
    svc.register_dataset("d1")
    svc.authorize("a1", "d1", 10.0)
    svc.reserve_budget(
        reservation_id="p3", analyst_id="a1",
        charges=[ReservationCharge("d1", 6.0)],
    )
    svc.settle_reservation(
        reservation_id="p3", analyst_id="a1",
        charges=[ReservationCharge("d1", 5.0)],
    )
    svc._store.close()

    svc2 = _fresh_service(db, clock)
    b = svc2.get_balance("a1", "d1")
    assert b.consumed == 5.0 and b.reserved == 0.0 and b.available == 5.0
    # 重启后重复结算仍是幂等重放，不会二次扣账
    st = svc2.settle_reservation(
        reservation_id="p3", analyst_id="a1",
        charges=[ReservationCharge("d1", 5.0)],
    )
    assert st.replay and st.total_charged == 5.0
    assert svc2.get_balance("a1", "d1").consumed == 5.0
    log_case(
        "结算记录重启保留且重复结算不重扣",
        reservation_id="p3",
        consumed=b.consumed,
        判定依据="settle事件只追加持久化,已结算重复提交重放",
    )


def test_restart_no_phantom_quota(tmp_path: Path):
    """已释放的预占重启后不得重新占用额度。"""
    db = tmp_path / "ledger.db"
    clock = FakeClock()
    svc = _fresh_service(db, clock)
    svc.register_analyst("a1")
    svc.register_dataset("d1")
    svc.authorize("a1", "d1", 10.0)
    svc.reserve_budget(
        reservation_id="p4", analyst_id="a1",
        charges=[ReservationCharge("d1", 9.0)],
    )
    svc.release_reservation(reservation_id="p4", analyst_id="a1")
    svc._store.close()

    clock.advance(10)
    svc2 = _fresh_service(db, clock)
    b = svc2.get_balance("a1", "d1")
    assert b.consumed == 0.0 and b.reserved == 0.0 and b.available == 10.0
    log_case(
        "已归还预占重启后不复活",
        reservation_id="p4",
        可用=b.available,
        判定依据="released状态持久化,不参与held占用求和",
    )
