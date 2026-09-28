"""账本应用服务：参数校验、鉴权顺序、幂等判定与结构化日志。

判定依据（顺序固定，便于追溯）：
  参数合法 -> 分析者存在 -> 各数据集存在 -> 各数据集已授权
  -> query_id 幂等/冲突 -> 各额度余额充足 -> 原子提交。
"""

from __future__ import annotations

import logging
import math
from collections.abc import Iterable
from pathlib import Path

from ledger.errors import (
    AnalystNotFoundError,
    DatasetNotFoundError,
    InvalidParameterError,
    LedgerError,
    UnauthorizedDatasetError,
)
from ledger.models import BalanceView, ConsumeResult, DatasetCharge
from ledger.store import LedgerStore

logger = logging.getLogger("privacy_budget_ledger")


class BudgetService:
    """面向分析者的预算记账服务（无状态，状态全部在 store）。"""

    def __init__(self, store: LedgerStore) -> None:
        self._store = store

    # ------------------------------------------------------------------ #
    # 管理面
    # ------------------------------------------------------------------ #
    def register_analyst(self, analyst_id: str) -> None:
        analyst_id = _require_nonempty_id(analyst_id, "analyst_id")
        self._store.create_analyst(analyst_id)
        logger.info("register_analyst analyst_id=%s 依据=管理操作", analyst_id)

    def register_dataset(self, dataset_id: str) -> None:
        dataset_id = _require_nonempty_id(dataset_id, "dataset_id")
        self._store.create_dataset(dataset_id)
        logger.info("register_dataset dataset_id=%s 依据=管理操作", dataset_id)

    def authorize(self, analyst_id: str, dataset_id: str, limit: float) -> None:
        analyst_id = _require_nonempty_id(analyst_id, "analyst_id")
        dataset_id = _require_nonempty_id(dataset_id, "dataset_id")
        if not _is_finite_number(limit) or limit < 0:
            raise InvalidParameterError(
                "预算上限必须是非负有限数",
                details={"field": "limit", "value": limit},
            )
        if not self._store.analyst_exists(analyst_id):
            raise AnalystNotFoundError(
                f"分析者不存在: {analyst_id}",
                details={"analyst_id": analyst_id},
            )
        if not self._store.dataset_exists(dataset_id):
            raise DatasetNotFoundError(
                f"数据集不存在: {dataset_id}",
                details={"dataset_id": dataset_id},
            )
        self._store.grant_budget(analyst_id, dataset_id, float(limit))
        logger.info(
            "authorize analyst_id=%s dataset_id=%s limit=%.6f 依据=授权成功",
            analyst_id, dataset_id, limit,
        )

    # ------------------------------------------------------------------ #
    # 查询面
    # ------------------------------------------------------------------ #
    def get_balance(self, analyst_id: str, dataset_id: str) -> BalanceView:
        analyst_id = _require_nonempty_id(analyst_id, "analyst_id")
        dataset_id = _require_nonempty_id(dataset_id, "dataset_id")
        if not self._store.analyst_exists(analyst_id):
            raise AnalystNotFoundError(
                f"分析者不存在: {analyst_id}",
                details={"analyst_id": analyst_id},
            )
        if not self._store.dataset_exists(dataset_id):
            raise DatasetNotFoundError(
                f"数据集不存在: {dataset_id}",
                details={"dataset_id": dataset_id},
            )
        balance = self._store.get_balance(analyst_id, dataset_id)
        if balance is None:
            raise UnauthorizedDatasetError(
                f"分析者 {analyst_id} 未被授权访问数据集 {dataset_id}",
                details={"analyst_id": analyst_id, "dataset_id": dataset_id},
            )
        return balance

    def list_balances(self, analyst_id: str) -> list[BalanceView]:
        analyst_id = _require_nonempty_id(analyst_id, "analyst_id")
        if not self._store.analyst_exists(analyst_id):
            raise AnalystNotFoundError(
                f"分析者不存在: {analyst_id}",
                details={"analyst_id": analyst_id},
            )
        return self._store.list_balances(analyst_id)

    # ------------------------------------------------------------------ #
    # 记账面
    # ------------------------------------------------------------------ #
    def consume(
        self,
        *,
        query_id: str,
        analyst_id: str,
        charges: Iterable[DatasetCharge],
    ) -> ConsumeResult:
        """以 query_id 幂等消耗；跨数据集全部成功或整体不生效。"""
        query_id = _require_nonempty_id(query_id, "query_id")
        analyst_id = _require_nonempty_id(analyst_id, "analyst_id")
        normalized = _validate_charges(charges)

        # 先在事务外给出实体/授权的可区分错误（store 内部还会在事务内复核）
        if not self._store.analyst_exists(analyst_id):
            logger.warning(
                "consume拒绝 query_id=%s analyst_id=%s 依据=analyst_not_found",
                query_id, analyst_id,
            )
            raise AnalystNotFoundError(
                f"分析者不存在: {analyst_id}",
                details={"analyst_id": analyst_id, "query_id": query_id},
            )
        for dataset_id, _cost in normalized:
            if not self._store.dataset_exists(dataset_id):
                logger.warning(
                    "consume拒绝 query_id=%s analyst_id=%s dataset_id=%s 依据=dataset_not_found",
                    query_id, analyst_id, dataset_id,
                )
                raise DatasetNotFoundError(
                    f"数据集不存在: {dataset_id}",
                    details={
                        "dataset_id": dataset_id,
                        "analyst_id": analyst_id,
                        "query_id": query_id,
                    },
                )
            if not self._store.is_authorized(analyst_id, dataset_id):
                logger.warning(
                    "consume拒绝 query_id=%s analyst_id=%s dataset_id=%s 依据=unauthorized_dataset",
                    query_id, analyst_id, dataset_id,
                )
                raise UnauthorizedDatasetError(
                    f"分析者 {analyst_id} 未被授权访问数据集 {dataset_id}",
                    details={
                        "analyst_id": analyst_id,
                        "dataset_id": dataset_id,
                        "query_id": query_id,
                    },
                )

        before_snapshot = {
            d: self._store.get_balance(analyst_id, d) for d, _ in normalized
        }
        try:
            result = self._store.consume(
                query_id=query_id,
                analyst_id=analyst_id,
                items=normalized,
            )
        except LedgerError as exc:
            # 事务已整体回滚；逐项记录判定依据（真实错误码）与当时余额
            for dataset_id, cost in normalized:
                bal = before_snapshot.get(dataset_id)
                logger.warning(
                    "consume失败回滚 query_id=%s analyst_id=%s dataset_id=%s "
                    "拟扣=%.6f 扣减前余额=%.6f 判定依据=%s(事务整体不生效,无任何扣减)",
                    query_id, analyst_id, dataset_id, cost,
                    bal.balance if bal else float("nan"), exc.code,
                )
            raise

        verdict = "idempotent_replay(同一查询重复提交,不重复扣减)" if result.replay else "accepted(首次提交,已记账)"
        for item in result.items:
            logger.info(
                "consume结论 query_id=%s analyst_id=%s dataset_id=%s "
                "扣减前余额=%.6f 本次扣减=%.6f 扣减后余额=%.6f 判定依据=%s",
                query_id, analyst_id, item.dataset_id,
                item.balance_before, item.charged, item.balance_after, verdict,
            )
        return result

    def get_query_record(self, query_id: str) -> ConsumeResult | None:
        query_id = _require_nonempty_id(query_id, "query_id")
        return self._store.get_consume_record(query_id)


# ---------------------------------------------------------------------- #
# 参数校验辅助
# ---------------------------------------------------------------------- #
def _require_nonempty_id(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise InvalidParameterError(
            f"参数 {field} 必须是非空字符串",
            details={"field": field, "value": repr(value)},
        )
    return value.strip()


def _is_finite_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value))


def _validate_charges(
    charges: Iterable[DatasetCharge],
) -> list[tuple[str, float]]:
    items = list(charges)
    if not items:
        raise InvalidParameterError(
            "charges 至少包含一个扣减项", details={"field": "charges"}
        )
    normalized: list[tuple[str, float]] = []
    seen: set[str] = set()
    for idx, charge in enumerate(items):
        if not isinstance(charge, DatasetCharge):
            raise InvalidParameterError(
                f"charges[{idx}] 必须是 DatasetCharge",
                details={"field": f"charges[{idx}]", "value": repr(charge)},
            )
        dataset_id = _require_nonempty_id(charge.dataset_id, f"charges[{idx}].dataset_id")
        if not _is_finite_number(charge.cost) or charge.cost <= 0:
            raise InvalidParameterError(
                f"charges[{idx}].cost 必须是正数",
                details={"field": f"charges[{idx}].cost", "value": charge.cost},
            )
        if dataset_id in seen:
            raise InvalidParameterError(
                f"同一次查询中数据集 {dataset_id} 出现多次，请合并扣减项",
                details={"field": f"charges[{idx}].dataset_id", "dataset_id": dataset_id},
            )
        seen.add(dataset_id)
        normalized.append((dataset_id, float(charge.cost)))
    return normalized


def build_default_service(db_path: str | Path = ":memory:") -> BudgetService:
    """构造并打开一个默认服务实例。"""
    store = LedgerStore(db_path).open()
    return BudgetService(store)
