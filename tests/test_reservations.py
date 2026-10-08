"""预占-结算/归还：先占住、后结账，多占退回，到期自动释放，重复提交只生效一次。

所有用例使用可注入的 FakeClock，把有效期调短以快速验证过期行为。
"""

from __future__ import annotations

import pytest

from ledger.errors import (
    BudgetInsufficientError,
    InvalidParameterError,
    ReservationClosedError,
    ReservationConflictError,
    ReservationExpiredError,
    ReservationNotFoundError,
    UnauthorizedDatasetError,
)
from ledger.models import DatasetCharge, ReservationCharge
from ledger.service import BudgetService
from ledger.store import LedgerStore
from tests.conftest import log_case


class FakeClock:
    def __init__(self, start: float = 1_000.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def svc(clock: FakeClock) -> BudgetService:
    store = LedgerStore(":memory:", clock=clock).open()
    s = BudgetService(store, default_ttl_seconds=60)
    s.register_analyst("a1")
    s.register_analyst("a2")
    s.register_dataset("d1")
    s.register_dataset("d2")
    s.register_dataset("d3")  # 存在但 a1 未授权
    s.authorize("a1", "d1", 10.0)
    s.authorize("a1", "d2", 5.0)
    return s


# ---------------------------------------------------------------------- #
# 1) 预占后结算：实扣入账，多占退回
# ---------------------------------------------------------------------- #
def test_reserve_then_settle_refunds_unused(svc, clock):
    r = svc.reserve_budget(
        reservation_id="r1",
        analyst_id="a1",
        charges=[ReservationCharge("d1", 6.0), ReservationCharge("d2", 3.0)],
    )
    assert r.status == "held" and not r.replay
    b1 = svc.get_balance("a1", "d1")
    b2 = svc.get_balance("a1", "d2")
    assert (b1.consumed, b1.reserved, b1.available) == (0.0, 6.0, 4.0)
    assert (b2.consumed, b2.reserved, b2.available) == (0.0, 3.0, 2.0)

    # 实际只用了 d1=4, d2=3 => d1 退回 2
    st = svc.settle_reservation(
        reservation_id="r1",
        analyst_id="a1",
        charges=[ReservationCharge("d1", 4.0), ReservationCharge("d2", 3.0)],
    )
    assert not st.replay
    assert st.total_charged == 7.0 and st.total_refunded == 2.0
    assert {(i.dataset_id, i.charged, i.refunded) for i in st.items} == {
        ("d1", 4.0, 2.0),
        ("d2", 3.0, 0.0),
    }
    b1 = svc.get_balance("a1", "d1")
    b2 = svc.get_balance("a1", "d2")
    assert (b1.consumed, b1.reserved, b1.available) == (4.0, 0.0, 6.0)
    assert (b2.consumed, b2.reserved, b2.available) == (3.0, 0.0, 2.0)
    log_case(
        "预占后结算多占退回",
        reservation_id="r1",
        d1="预占6实扣4退2", d2="预占3实扣3退0",
        判定依据="settle事件实扣入账,预占释放,差额退回",
    )


def test_settle_allow_zero_usage_full_refund(svc, clock):
    svc.reserve_budget(
        reservation_id="r0", analyst_id="a1",
        charges=[ReservationCharge("d1", 5.0)],
    )
    st = svc.settle_reservation(
        reservation_id="r0", analyst_id="a1",
        charges=[ReservationCharge("d1", 0.0)],
    )
    assert st.total_charged == 0.0 and st.total_refunded == 5.0
    b = svc.get_balance("a1", "d1")
    assert b.consumed == 0.0 and b.reserved == 0.0 and b.available == 10.0
    log_case(
        "实际用量为0全额退回",
        reservation_id="r0",
        consumed=b.consumed, available=b.available,
        判定依据="不产生settle事件,预占整笔释放",
    )


# ---------------------------------------------------------------------- #
# 2) 预占后归还
# ---------------------------------------------------------------------- #
def test_reserve_then_release_returns_all(svc, clock):
    svc.reserve_budget(
        reservation_id="r2", analyst_id="a1",
        charges=[ReservationCharge("d1", 7.0)],
    )
    assert svc.get_balance("a1", "d1").available == 3.0
    rel = svc.release_reservation(reservation_id="r2", analyst_id="a1")
    assert rel.status == "released" and not rel.replay
    assert rel.total_refunded == 7.0
    b = svc.get_balance("a1", "d1")
    assert b.reserved == 0.0 and b.available == 10.0 and b.consumed == 0.0
    log_case(
        "整笔归还释放全部占用",
        reservation_id="r2",
        退回=rel.total_refunded, 归还后可用=b.available,
        判定依据="held->released,额度全部退回,无扣减事件",
    )


# ---------------------------------------------------------------------- #
# 3) 到期自动释放；过期后结算被明确拒绝
# ---------------------------------------------------------------------- #
def test_expiry_auto_releases_and_settle_rejected(svc, clock):
    svc.reserve_budget(
        reservation_id="r3", analyst_id="a1",
        charges=[ReservationCharge("d1", 8.0)], ttl_seconds=10,
    )
    assert svc.get_balance("a1", "d1").available == 2.0

    # 未到期时显式 sweep：什么都不发生
    assert svc.sweep_expired_reservations() == []

    clock.advance(11)
    # 余额查询口径本身就不再计入已到期预占（懒失效）
    assert svc.get_balance("a1", "d1").available == 10.0
    expired = svc.sweep_expired_reservations()
    assert expired == ["r3"]
    # sweep 幂等：第二次没有新释放
    assert svc.sweep_expired_reservations() == []

    view = svc.get_reservation("r3")
    assert view.status == "expired"

    with pytest.raises(ReservationExpiredError) as ei:
        svc.settle_reservation(
            reservation_id="r3", analyst_id="a1",
            charges=[ReservationCharge("d1", 1.0)],
        )
    assert ei.value.code == "reservation_expired"
    # 过期后额度完整，没有凭空扣账
    b = svc.get_balance("a1", "d1")
    assert b.consumed == 0.0 and b.reserved == 0.0 and b.available == 10.0
    log_case(
        "到期自动释放且拒绝结算",
        reservation_id="r3",
        释放=expired, consumed=b.consumed, available=b.available,
        判定依据="expires_at<=now->expired,settle明确拒绝而非照常扣账",
    )


def test_lazy_expiry_on_settle_without_explicit_sweep(svc, clock):
    svc.reserve_budget(
        reservation_id="r3b", analyst_id="a1",
        charges=[ReservationCharge("d1", 4.0)], ttl_seconds=5,
    )
    clock.advance(6)
    with pytest.raises(ReservationExpiredError):
        svc.settle_reservation(
            reservation_id="r3b", analyst_id="a1",
            charges=[ReservationCharge("d1", 1.0)],
        )
    assert svc.get_reservation("r3b").status == "expired"
    log_case(
        "未显式清理时结算也会触发懒失效",
        reservation_id="r3b",
        判定依据="settle事务内检测到期先转expired再拒绝",
    )


# ---------------------------------------------------------------------- #
# 4) 重复提交只生效一次，且结论一致
# ---------------------------------------------------------------------- #
def test_reserve_idempotent_replay(svc, clock):
    charges = [ReservationCharge("d1", 5.0)]
    r1 = svc.reserve_budget(reservation_id="r4", analyst_id="a1", charges=charges)
    r2 = svc.reserve_budget(reservation_id="r4", analyst_id="a1", charges=charges)
    r3 = svc.reserve_budget(reservation_id="r4", analyst_id="a1", charges=charges)
    assert not r1.replay and r2.replay and r3.replay
    assert r1.expires_at == r2.expires_at == r3.expires_at
    assert [i.available_after for i in r1.items] == [
        i.available_after for i in r2.items
    ]
    b = svc.get_balance("a1", "d1")
    assert b.reserved == 5.0  # 只占一次
    log_case(
        "预占重复提交只占一次",
        reservation_id="r4",
        reserved=b.reserved, available=b.available,
        判定依据="同一fingerprint幂等重放,不重复占用",
    )


def test_settle_idempotent_replay(svc, clock):
    svc.reserve_budget(
        reservation_id="r5", analyst_id="a1",
        charges=[ReservationCharge("d1", 6.0), ReservationCharge("d2", 4.0)],
    )
    charges = [ReservationCharge("d1", 2.0), ReservationCharge("d2", 4.0)]
    s1 = svc.settle_reservation(reservation_id="r5", analyst_id="a1", charges=charges)
    s2 = svc.settle_reservation(reservation_id="r5", analyst_id="a1", charges=charges)
    assert not s1.replay and s2.replay
    assert s1.total_charged == s2.total_charged == 6.0
    assert s1.total_refunded == s2.total_refunded == 4.0
    b1 = svc.get_balance("a1", "d1")
    b2 = svc.get_balance("a1", "d2")
    assert b1.consumed == 2.0 and b2.consumed == 4.0
    log_case(
        "结算重复提交只扣一次",
        reservation_id="r5",
        d1实扣=b1.consumed, d2实扣=b2.consumed,
        判定依据="settled状态+fingerprint校验,重复结算重放首次结论",
    )


def test_release_idempotent_replay(svc, clock):
    svc.reserve_budget(
        reservation_id="r6", analyst_id="a1",
        charges=[ReservationCharge("d1", 3.0)],
    )
    rel1 = svc.release_reservation(reservation_id="r6", analyst_id="a1")
    rel2 = svc.release_reservation(reservation_id="r6", analyst_id="a1")
    assert not rel1.replay and rel2.replay
    assert rel1.total_refunded == rel2.total_refunded == 3.0
    assert svc.get_balance("a1", "d1").reserved == 0.0
    log_case(
        "归还重复提交只生效一次",
        reservation_id="r6",
        退回=rel2.total_refunded,
        判定依据="released状态幂等重放,额度不重复退回",
    )


# ---------------------------------------------------------------------- #
# 5) 同一标识提交不同内容 / 不同分析者 -> conflict
# ---------------------------------------------------------------------- #
def test_reserve_conflict_on_different_content(svc, clock):
    svc.reserve_budget(
        reservation_id="r7", analyst_id="a1",
        charges=[ReservationCharge("d1", 5.0)],
    )
    with pytest.raises(ReservationConflictError) as ei:
        svc.reserve_budget(
            reservation_id="r7", analyst_id="a1",
            charges=[ReservationCharge("d1", 6.0)],
        )
    assert ei.value.code == "reservation_conflict"
    # 冲突时占用保持首次的 5，不被改动
    assert svc.get_balance("a1", "d1").reserved == 5.0

    with pytest.raises(ReservationConflictError):
        # a2 先获得 d1 授权，才能走到"标识属于其他分析者"的冲突判定
        svc.authorize("a2", "d1", 10.0)
        svc.reserve_budget(
            reservation_id="r7", analyst_id="a2",
            charges=[ReservationCharge("d1", 5.0)],
        )
    log_case(
        "同一预占标识不同内容被拒",
        reservation_id="r7",
        reserved=svc.get_balance("a1", "d1").reserved,
        判定依据="fingerprint/analyst不一致->reservation_conflict,占用不变",
    )


def test_settle_conflict_when_content_differs(svc, clock):
    svc.reserve_budget(
        reservation_id="r7b", analyst_id="a1",
        charges=[ReservationCharge("d1", 5.0)],
    )
    svc.settle_reservation(
        reservation_id="r7b", analyst_id="a1",
        charges=[ReservationCharge("d1", 2.0)],
    )
    with pytest.raises(ReservationConflictError):
        svc.settle_reservation(
            reservation_id="r7b", analyst_id="a1",
            charges=[ReservationCharge("d1", 3.0)],
        )
    assert svc.get_balance("a1", "d1").consumed == 2.0
    log_case(
        "结算已成功后换用量重提被拒",
        reservation_id="r7b",
        consumed=svc.get_balance("a1", "d1").consumed,
        判定依据="已结算fingerprint不一致->reservation_conflict,账不变",
    )


# ---------------------------------------------------------------------- #
# 6) 跨数据集组合预占：全占上或整体不生效
# ---------------------------------------------------------------------- #
def test_multi_dataset_reserve_all_or_nothing(svc, clock):
    # d1 需 9（可用10，足），d2 需 6（可用5，不足）=> 整体失败
    with pytest.raises(BudgetInsufficientError) as ei:
        svc.reserve_budget(
            reservation_id="r8", analyst_id="a1",
            charges=[ReservationCharge("d1", 9.0), ReservationCharge("d2", 6.0)],
        )
    assert ei.value.details["dataset_id"] == "d2"
    b1, b2 = svc.get_balance("a1", "d1"), svc.get_balance("a1", "d2")
    assert b1.reserved == 0.0 and b2.reserved == 0.0
    with pytest.raises(ReservationNotFoundError):
        svc.get_reservation("r8")
    log_case(
        "组合预占任一不足整体回滚",
        reservation_id="r8",
        d1预占=b1.reserved, d2预占=b2.reserved,
        判定依据="单事务,d2不足ROLLBACK,d1也不占",
    )


def test_multi_dataset_reserve_success_atomic(svc, clock):
    r = svc.reserve_budget(
        reservation_id="r9", analyst_id="a1",
        charges=[ReservationCharge("d1", 4.0), ReservationCharge("d2", 3.0)],
    )
    assert {i.dataset_id for i in r.items} == {"d1", "d2"}
    assert svc.get_balance("a1", "d1").reserved == 4.0
    assert svc.get_balance("a1", "d2").reserved == 3.0
    log_case(
        "组合预占同时占住",
        reservation_id="r9",
        d1预占=4.0, d2预占=3.0,
        判定依据="主表+明细同一事务提交",
    )


# ---------------------------------------------------------------------- #
# 7) 原有错误分类仍然成立，且失败不动任何额度
# ---------------------------------------------------------------------- #
def test_reserve_reuses_existing_error_categories(svc, clock):
    # 未授权（d3 存在但 a1 未授权）
    with pytest.raises(UnauthorizedDatasetError):
        svc.reserve_budget(
            reservation_id="re1", analyst_id="a1",
            charges=[ReservationCharge("d3", 1.0)],
        )
    # 参数非法：空 charges / 非正 amount
    with pytest.raises(InvalidParameterError):
        svc.reserve_budget(
            reservation_id="re2", analyst_id="a1", charges=[]
        )
    with pytest.raises(InvalidParameterError):
        svc.reserve_budget(
            reservation_id="re3", analyst_id="a1",
            charges=[ReservationCharge("d1", 0.0)],
        )
    # 不存在的预占
    with pytest.raises(ReservationNotFoundError):
        svc.release_reservation(reservation_id="missing", analyst_id="a1")
    # 已结算不能归还；已归还不能结算
    svc.reserve_budget(
        reservation_id="re4", analyst_id="a1",
        charges=[ReservationCharge("d1", 2.0)],
    )
    svc.settle_reservation(
        reservation_id="re4", analyst_id="a1",
        charges=[ReservationCharge("d1", 1.0)],
    )
    with pytest.raises(ReservationClosedError):
        svc.release_reservation(reservation_id="re4", analyst_id="a1")

    svc.reserve_budget(
        reservation_id="re5", analyst_id="a1",
        charges=[ReservationCharge("d1", 2.0)],
    )
    svc.release_reservation(reservation_id="re5", analyst_id="a1")
    with pytest.raises(ReservationClosedError):
        svc.settle_reservation(
            reservation_id="re5", analyst_id="a1",
            charges=[ReservationCharge("d1", 1.0)],
        )
    log_case(
        "预占沿用原有错误分类且终结态互斥",
        reservation_id="re1..re5",
        判定依据="unauthorized/invalid_parameter/not_found/closed各自可区分,失败零额度变动",
    )


def test_settle_cannot_exceed_reserved(svc, clock):
    svc.reserve_budget(
        reservation_id="re6", analyst_id="a1",
        charges=[ReservationCharge("d1", 3.0)],
    )
    with pytest.raises(InvalidParameterError):
        svc.settle_reservation(
            reservation_id="re6", analyst_id="a1",
            charges=[ReservationCharge("d1", 3.5)],
        )
    # 预占仍然有效，未被动过
    assert svc.get_balance("a1", "d1").reserved == 3.0
    log_case(
        "实际用量不得超过预占",
        reservation_id="re6",
        reserved=svc.get_balance("a1", "d1").reserved,
        判定依据="cost>reserved->invalid_parameter,占用不变",
    )


def test_consume_and_reserve_share_one_ledger(svc, clock):
    """新预占与直接扣减同一口径：混用时 consumed + reserved <= limit。"""
    # 直接扣 4，再预占 5，剩余可用 1
    svc.consume(query_id="c1", analyst_id="a1", charges=[DatasetCharge("d1", 4.0)])
    svc.reserve_budget(
        reservation_id="re7", analyst_id="a1",
        charges=[ReservationCharge("d1", 5.0)],
    )
    with pytest.raises(BudgetInsufficientError):
        svc.consume(query_id="c2", analyst_id="a1", charges=[DatasetCharge("d1", 2.0)])
    with pytest.raises(BudgetInsufficientError):
        svc.reserve_budget(
            reservation_id="re8", analyst_id="a1",
            charges=[ReservationCharge("d1", 2.0)],
        )
    b = svc.get_balance("a1", "d1")
    assert b.consumed + b.reserved == 9.0 and b.available == 1.0
    log_case(
        "直接扣减与预占共用一本账",
        query_id="c1/c2", reservation_id="re7/re8",
        consumed=b.consumed, reserved=b.reserved, available=b.available,
        判定依据="已消耗+未过期预占<=上限,两种写入互相挡住",
    )
