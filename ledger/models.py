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


# ---- API schemas（pydantic 在 api.py 内定义，这里仅保留领域模型占位）----
