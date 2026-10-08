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
    available: float = 0.0


# ---- 预占（先占住、后结算）相关模型 ------------------------------------- #
@dataclass(frozen=True, slots=True)
class ReservationCharge:
    """预占请求中单个数据集的占用项。"""

    dataset_id: str
    amount: float


@dataclass(frozen=True, slots=True)
class ReservationItem:
    """预占结果中单个数据集的占用明细。"""

    dataset_id: str
    reserved: float
    available_before: float
    available_after: float


@dataclass(frozen=True, slots=True)
class ReservationResult:
    reservation_id: str
    analyst_id: str
    replay: bool
    status: str  # held / expired / settled / released
    expires_at: str
    ttl_seconds: int
    total_reserved: float
    items: tuple[ReservationItem, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class SettleItem:
    """结算结果中单个数据集的明细：预占多少、实扣多少、退回多少。"""

    dataset_id: str
    reserved: float
    charged: float
    refunded: float
    available_before: float
    available_after: float


@dataclass(frozen=True, slots=True)
class SettleResult:
    reservation_id: str
    analyst_id: str
    replay: bool
    total_charged: float
    total_refunded: float
    items: tuple[SettleItem, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class ReleaseResult:
    reservation_id: str
    analyst_id: str
    replay: bool
    status: str  # released / expired
    total_refunded: float


# ---- API schemas（pydantic 在 api.py 内定义，这里仅保留领域模型占位）----
