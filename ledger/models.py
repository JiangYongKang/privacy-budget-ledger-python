"""领域模型与 API 请求/响应结构。"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class DatasetCharge:
    """一次查询在单个数据集上的扣减项。"""

    dataset_id: str
    cost: float


@dataclass(frozen=True, slots=True)
class ChargeResult:
    """单个数据集的扣减结果（用于明细返回）。"""

    dataset_id: str
    charged: float
    balance_before: float
    balance_after: float


@dataclass(frozen=True, slots=True)
class ConsumeResult:
    query_id: str
    analyst_id: str
    replay: bool
    total_charged: float
    items: tuple[ChargeResult, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class BalanceView:
    analyst_id: str
    dataset_id: str
    limit: float
    consumed: float
    balance: float
    reserved: float = 0.0

    @property
    def available(self) -> float:
        """可再动用额度 = 上限 − 已结算消耗 − 未过期预占。"""
        return self.limit - self.consumed - self.reserved


# ---- 预占（reservation）领域模型 ----
@dataclass(frozen=True, slots=True)
class ReservationItem:
    """预占明细：单个数据集上预占的额度。"""

    dataset_id: str
    amount: float


@dataclass(frozen=True, slots=True)
class ReservationHold:
    """单个数据集的预占/结算/归还结果明细。"""

    dataset_id: str
    reserved: float
    settled: float
    refunded: float
    balance_before: float
    balance_after: float


@dataclass(frozen=True, slots=True)
class ReserveResult:
    reservation_id: str
    analyst_id: str
    replay: bool
    expires_at: str
    items: tuple[ReservationHold, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class SettleResult:
    reservation_id: str
    analyst_id: str
    replay: bool
    total_settled: float
    total_refunded: float
    items: tuple[ReservationHold, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class ReleaseResult:
    reservation_id: str
    analyst_id: str
    replay: bool
    status: str  # released / expired
    total_refunded: float
    items: tuple[ReservationHold, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class ReservationView:
    """预占单的当前视图。"""

    reservation_id: str
    analyst_id: str
    status: str  # held / settled / released / expired
    created_at: str
    expires_at: str
    items: tuple[ReservationItem, ...] = field(default_factory=tuple)


# ---- API schemas（pydantic 在 api.py 内定义，这里仅保留领域模型占位）----
