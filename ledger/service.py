"""账本应用服务：参数校验、鉴权顺序、幂等判定与结构化日志。

判定依据（顺序固定，便于追溯）：
  参数合法 -> 分析者存在 -> 各数据集存在 -> 各数据集已授权
  -> query_id 幂等/冲突 -> 各额度余额充足 -> 原子提交。
"""

from __future__ import annotations

import logging
import math
import os
import time
from collections.abc import Callable, Iterable
from pathlib import Path

from ledger.errors import (
    AnalystNotFoundError,
    DatasetNotFoundError,
    InvalidParameterError,
    LedgerError,
    UnauthorizedDatasetError,
)
from ledger.models import (
    BalanceView,
    ConsumeResult,
    DatasetCharge,
    ReleaseResult,
    ReservationItem,
    ReservationView,
    ReserveResult,
    SettleResult,
)
from ledger.store import LedgerStore

logger = logging.getLogger("privacy_budget_ledger")

DEFAULT_RESERVATION_TTL_SECONDS = float(
    os.environ.get("BUDGET_LEDGER_RESERVATION_TTL", "3600")
)


class BudgetService:
    """面向分析者的预算记账服务（无状态，状态全部在 store）。"""

    def __init__(
        self,
        store: LedgerStore,
        *,
        default_ttl_seconds: float = DEFAULT_RESERVATION_TTL_SECONDS,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._store = store
        self._default_ttl_seconds = default_ttl_seconds
        self._clock = clock

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

    # ------------------------------------------------------------------ #
    # 预占面（先预占、后结算/归还）
    # ------------------------------------------------------------------ #
    def reserve(
        self,
        *,
        reservation_id: str,
        analyst_id: str,
        items: Iterable[ReservationItem],
        ttl_seconds: float | None = None,
    ) -> ReserveResult:
        """先占住额度：跨数据集全占上或整体不生效；同标识重复提交幂等。"""
        reservation_id = _require_nonempty_id(reservation_id, "reservation_id")
        analyst_id = _require_nonempty_id(analyst_id, "analyst_id")
        normalized = _validate_reservation_items(items)
        ttl = self._resolve_ttl(ttl_seconds)

        self._require_entities_authorized(reservation_id, analyst_id, normalized)

        try:
            result = self._store.reserve(
                reservation_id=reservation_id,
                analyst_id=analyst_id,
                items=normalized,
                ttl_seconds=ttl,
            )
        except LedgerError as exc:
            logger.warning(
                "reserve失败回滚 reservation_id=%s analyst_id=%s 判定依据=%s(整体不生效,无任何占用)",
                reservation_id, analyst_id, exc.code,
            )
            raise

        verdict = (
            "idempotent_replay(同一预占重复提交,不重复占用)"
            if result.replay else "accepted(首次提交,已预占)"
        )
        for item in result.items:
            logger.info(
                "reserve结论 reservation_id=%s analyst_id=%s dataset_id=%s "
                "本次预占=%.6f 占用后可用=%.6f 到期=%s 判定依据=%s",
                reservation_id, analyst_id, item.dataset_id,
                item.reserved, item.balance_after, result.expires_at, verdict,
            )
        return result

    def settle(
        self,
        *,
        reservation_id: str,
        analyst_id: str,
        actual: Iterable[ReservationItem],
    ) -> SettleResult:
        """按实际用量结算：多占退回；过期预占被明确拒绝；重复提交幂等。"""
        reservation_id = _require_nonempty_id(reservation_id, "reservation_id")
        analyst_id = _require_nonempty_id(analyst_id, "analyst_id")
        normalized = _validate_actual_usage(actual)

        try:
            result = self._store.settle_reservation(
                reservation_id=reservation_id,
                analyst_id=analyst_id,
                actual=normalized,
            )
        except LedgerError as exc:
            logger.warning(
                "settle拒绝 reservation_id=%s analyst_id=%s 判定依据=%s(不动任何额度)",
                reservation_id, analyst_id, exc.code,
            )
            raise

        verdict = (
            "idempotent_replay(同一结算重复提交,不重复扣账)"
            if result.replay else "settled(占用转扣减,多占退回)"
        )
        for item in result.items:
            logger.info(
                "settle结论 reservation_id=%s analyst_id=%s dataset_id=%s "
                "预占=%.6f 实际结算=%.6f 退回=%.6f 结算后可用=%.6f 判定依据=%s",
                reservation_id, analyst_id, item.dataset_id,
                item.reserved, item.settled, item.refunded,
                item.balance_after, verdict,
            )
        return result

    def release(
        self,
        *,
        reservation_id: str,
        analyst_id: str,
    ) -> ReleaseResult:
        """任务取消时整笔归还预占；重复提交只生效一次。"""
        reservation_id = _require_nonempty_id(reservation_id, "reservation_id")
        analyst_id = _require_nonempty_id(analyst_id, "analyst_id")

        try:
            result = self._store.release_reservation(
                reservation_id=reservation_id,
                analyst_id=analyst_id,
            )
        except LedgerError as exc:
            logger.warning(
                "release拒绝 reservation_id=%s analyst_id=%s 判定依据=%s",
                reservation_id, analyst_id, exc.code,
            )
            raise

        verdict = (
            "idempotent_replay(同一归还重复提交,不重复退回)"
            if result.replay else "released(整笔归还,占用释放)"
        )
        for item in result.items:
            logger.info(
                "release结论 reservation_id=%s analyst_id=%s dataset_id=%s "
                "退回预占=%.6f 归还后可用=%.6f 判定依据=%s",
                reservation_id, analyst_id, item.dataset_id,
                item.refunded, item.balance_after, verdict,
            )
        return result

    def get_reservation(self, reservation_id: str) -> ReservationView | None:
        reservation_id = _require_nonempty_id(reservation_id, "reservation_id")
        return self._store.get_reservation(reservation_id)

    def sweep_expired(self) -> int:
        """主动把到期未结算/归还的预占置为 expired 并释放占用。"""
        n = self._store.sweep_expired()
        if n:
            logger.info("sweep_expired结论 释放过期预占=%d笔 判定依据=expires_at<=now", n)
        return n

    # ------------------------------------------------------------------ #
    # 预占辅助
    # ------------------------------------------------------------------ #
    def _resolve_ttl(self, ttl_seconds: float | None) -> float:
        ttl = self._default_ttl_seconds if ttl_seconds is None else ttl_seconds
        if not _is_finite_number(ttl) or ttl <= 0:
            raise InvalidParameterError(
                "ttl_seconds 必须是正的有限数",
                details={"field": "ttl_seconds", "value": ttl},
            )
        return float(ttl)

    def _require_entities_authorized(
        self,
        request_id: str,
        analyst_id: str,
        items: list[tuple[str, float]],
    ) -> None:
        """事务外先给出可区分的实体/授权错误（store 事务内仍会复核）。"""
        if not self._store.analyst_exists(analyst_id):
            logger.warning(
                "reserve拒绝 reservation_id=%s analyst_id=%s 依据=analyst_not_found",
                request_id, analyst_id,
            )
            raise AnalystNotFoundError(
                f"分析者不存在: {analyst_id}",
                details={"analyst_id": analyst_id, "reservation_id": request_id},
            )
        for dataset_id, _amount in items:
            if not self._store.dataset_exists(dataset_id):
                logger.warning(
                    "reserve拒绝 reservation_id=%s analyst_id=%s dataset_id=%s 依据=dataset_not_found",
                    request_id, analyst_id, dataset_id,
                )
                raise DatasetNotFoundError(
                    f"数据集不存在: {dataset_id}",
                    details={
                        "dataset_id": dataset_id,
                        "analyst_id": analyst_id,
                        "reservation_id": request_id,
                    },
                )
            if not self._store.is_authorized(analyst_id, dataset_id):
                logger.warning(
                    "reserve拒绝 reservation_id=%s analyst_id=%s dataset_id=%s 依据=unauthorized_dataset",
                    request_id, analyst_id, dataset_id,
                )
                raise UnauthorizedDatasetError(
                    f"分析者 {analyst_id} 未被授权访问数据集 {dataset_id}",
                    details={
                        "analyst_id": analyst_id,
                        "dataset_id": dataset_id,
                        "reservation_id": request_id,
                    },
                )


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


def _validate_reservation_items(
    items: Iterable[ReservationItem],
) -> list[tuple[str, float]]:
    raw = list(items)
    if not raw:
        raise InvalidParameterError(
            "items 至少包含一个预占项", details={"field": "items"}
        )
    normalized: list[tuple[str, float]] = []
    seen: set[str] = set()
    for idx, item in enumerate(raw):
        if not isinstance(item, ReservationItem):
            raise InvalidParameterError(
                f"items[{idx}] 必须是 ReservationItem",
                details={"field": f"items[{idx}]", "value": repr(item)},
            )
        dataset_id = _require_nonempty_id(item.dataset_id, f"items[{idx}].dataset_id")
        if not _is_finite_number(item.amount) or item.amount <= 0:
            raise InvalidParameterError(
                f"items[{idx}].amount 必须是正数",
                details={"field": f"items[{idx}].amount", "value": item.amount},
            )
        if dataset_id in seen:
            raise InvalidParameterError(
                f"同一次预占中数据集 {dataset_id} 出现多次，请合并预占项",
                details={"field": f"items[{idx}].dataset_id", "dataset_id": dataset_id},
            )
        seen.add(dataset_id)
        normalized.append((dataset_id, float(item.amount)))
    return normalized


def _validate_actual_usage(
    actual: Iterable[ReservationItem],
) -> list[tuple[str, float]]:
    raw = list(actual)
    if not raw:
        raise InvalidParameterError(
            "actual 至少包含一个结算项", details={"field": "actual"}
        )
    normalized: list[tuple[str, float]] = []
    seen: set[str] = set()
    for idx, item in enumerate(raw):
        if not isinstance(item, ReservationItem):
            raise InvalidParameterError(
                f"actual[{idx}] 必须是 ReservationItem",
                details={"field": f"actual[{idx}]", "value": repr(item)},
            )
        dataset_id = _require_nonempty_id(item.dataset_id, f"actual[{idx}].dataset_id")
        if not _is_finite_number(item.amount) or item.amount < 0:
            raise InvalidParameterError(
                f"actual[{idx}].amount 必须是非负数（0 表示全额退回）",
                details={"field": f"actual[{idx}].amount", "value": item.amount},
            )
        if dataset_id in seen:
            raise InvalidParameterError(
                f"结算中数据集 {dataset_id} 出现多次，请合并结算项",
                details={"field": f"actual[{idx}].dataset_id", "dataset_id": dataset_id},
            )
        seen.add(dataset_id)
        normalized.append((dataset_id, float(item.amount)))
    return normalized


def build_default_service(
    db_path: str | Path = ":memory:",
    *,
    default_ttl_seconds: float | None = None,
    clock: Callable[[], float] = time.time,
) -> BudgetService:
    """构造并打开一个默认服务实例。"""
    ttl = (
        DEFAULT_RESERVATION_TTL_SECONDS
        if default_ttl_seconds is None
        else float(default_ttl_seconds)
    )
    store = LedgerStore(db_path, clock=clock).open()
    return BudgetService(store, default_ttl_seconds=ttl, clock=clock)
