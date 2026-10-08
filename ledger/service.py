"""账本应用服务：参数校验、鉴权顺序、幂等判定与结构化日志。

判定依据（顺序固定，便于追溯）：
  参数合法 -> 分析者存在 -> 各数据集存在 -> 各数据集已授权
  -> query_id 幂等/冲突 -> 各额度余额充足 -> 原子提交。

预占（reserve/settle/release）沿用同一套判定顺序，且与直接扣减共用同一本账：
任一时刻 已消耗 + 未过期预占 <= 授权上限。
"""

from __future__ import annotations

import logging
import math
import os
from collections.abc import Iterable
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
    ReservationCharge,
    ReservationResult,
    SettleResult,
)
from ledger.store import LedgerStore

logger = logging.getLogger("privacy_budget_ledger")

# 预占默认有效期（秒）；可用环境变量调短，方便本地快速验证过期行为。
DEFAULT_RESERVATION_TTL_SECONDS = int(
    os.environ.get("BUDGET_RESERVATION_TTL_SECONDS", "900")
)


class BudgetService:
    """面向分析者的预算记账服务（无状态，状态全部在 store）。"""

    def __init__(
        self,
        store: LedgerStore,
        *,
        default_ttl_seconds: int | None = None,
    ) -> None:
        self._store = store
        ttl = (
            default_ttl_seconds
            if default_ttl_seconds is not None
            else DEFAULT_RESERVATION_TTL_SECONDS
        )
        if not isinstance(ttl, int) or isinstance(ttl, bool) or ttl <= 0:
            raise InvalidParameterError(
                "默认预占有效期必须是正整数秒",
                details={"field": "default_ttl_seconds", "value": ttl},
            )
        self._default_ttl = ttl

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
    # 预占面：先预占、后结算 / 归还
    # ------------------------------------------------------------------ #
    def reserve_budget(
        self,
        *,
        reservation_id: str,
        analyst_id: str,
        charges: Iterable[ReservationCharge],
        ttl_seconds: int | None = None,
    ) -> ReservationResult:
        """预占额度：跨多个数据集要么全占上，要么整体不生效。"""
        reservation_id = _require_nonempty_id(reservation_id, "reservation_id")
        analyst_id = _require_nonempty_id(analyst_id, "analyst_id")
        normalized = _validate_reservation_charges(charges)
        ttl = _validate_ttl(ttl_seconds if ttl_seconds is not None else self._default_ttl)

        self._check_entities_for_write(reservation_id, analyst_id, normalized, "reserve")

        before_snapshot = {
            d: self._store.get_balance(analyst_id, d) for d, _ in normalized
        }
        try:
            result = self._store.reserve(
                reservation_id=reservation_id,
                analyst_id=analyst_id,
                items=normalized,
                ttl_seconds=ttl,
            )
        except LedgerError as exc:
            for dataset_id, amount in normalized:
                bal = before_snapshot.get(dataset_id)
                logger.warning(
                    "reserve失败回滚 reservation_id=%s analyst_id=%s dataset_id=%s "
                    "拟占=%.6f 可用额度=%.6f 判定依据=%s(事务整体不生效,无任何占用)",
                    reservation_id, analyst_id, dataset_id, amount,
                    bal.available if bal else float("nan"), exc.code,
                )
            raise

        verdict = (
            "idempotent_replay(同一预占重复提交,不重复占用)"
            if result.replay else "accepted(首次预占,已占住额度)"
        )
        for item in result.items:
            logger.info(
                "reserve结论 reservation_id=%s analyst_id=%s dataset_id=%s "
                "占用前可用=%.6f 本次占用=%.6f 占用后可用=%.6f 到期=%s 判定依据=%s",
                reservation_id, analyst_id, item.dataset_id,
                item.available_before, item.reserved, item.available_after,
                result.expires_at, verdict,
            )
        return result

    def settle_reservation(
        self,
        *,
        reservation_id: str,
        analyst_id: str,
        charges: Iterable[ReservationCharge],
    ) -> SettleResult:
        """按实际用量结算：实扣入账，多占退回。重复提交只生效一次、结论一致。"""
        reservation_id = _require_nonempty_id(reservation_id, "reservation_id")
        analyst_id = _require_nonempty_id(analyst_id, "analyst_id")
        normalized = _validate_settlement_charges(charges)
        try:
            result = self._store.settle_reservation(
                reservation_id=reservation_id,
                analyst_id=analyst_id,
                items=normalized,
            )
        except LedgerError as exc:
            logger.warning(
                "settle拒绝 reservation_id=%s analyst_id=%s 判定依据=%s(未结算,占用不变)",
                reservation_id, analyst_id, exc.code,
            )
            raise
        verdict = (
            "idempotent_replay(同一预占重复结算,不重复扣账)"
            if result.replay else "accepted(首次结算,实扣入账,多占已退回)"
        )
        for item in result.items:
            logger.info(
                "settle结论 reservation_id=%s analyst_id=%s dataset_id=%s "
                "预占=%.6f 实扣=%.6f 退回=%.6f 结算后可用=%.6f 判定依据=%s",
                reservation_id, analyst_id, item.dataset_id,
                item.reserved, item.charged, item.refunded,
                item.available_after, verdict,
            )
        return result

    def release_reservation(
        self, *, reservation_id: str, analyst_id: str | None = None
    ) -> ReleaseResult:
        """整笔归还预占。重复归还只生效一次，返回与首次一致的结论。"""
        reservation_id = _require_nonempty_id(reservation_id, "reservation_id")
        if analyst_id is not None:
            analyst_id = _require_nonempty_id(analyst_id, "analyst_id")
        try:
            result = self._store.release_reservation(reservation_id, analyst_id)
        except LedgerError as exc:
            logger.warning(
                "release拒绝 reservation_id=%s analyst_id=%s 判定依据=%s(占用不变)",
                reservation_id, analyst_id, exc.code,
            )
            raise
        verdict = (
            "idempotent_replay(重复归还,只生效一次)"
            if result.replay else f"accepted({result.status},全部占用已释放)"
        )
        logger.info(
            "release结论 reservation_id=%s analyst_id=%s 退回额度=%.6f 状态=%s 判定依据=%s",
            reservation_id, result.analyst_id, result.total_refunded,
            result.status, verdict,
        )
        return result

    def sweep_expired_reservations(self) -> list[str]:
        """主动清理所有已到期预占（懒失效之外的显式入口）。"""
        expired_ids = self._store.sweep_expired()
        for rid in expired_ids:
            logger.info(
                "expire自动释放 reservation_id=%s 判定依据=超过有效期TTL,占用额度已退回",
                rid,
            )
        return expired_ids

    def get_reservation(self, reservation_id: str) -> ReservationResult:
        reservation_id = _require_nonempty_id(reservation_id, "reservation_id")
        result, status = self._store.get_reservation(reservation_id)
        if result is None:
            from ledger.errors import ReservationNotFoundError

            raise ReservationNotFoundError(
                f"预占标识不存在: {reservation_id}",
                details={"reservation_id": reservation_id},
            )
        logger.info(
            "reserve查询 reservation_id=%s analyst_id=%s 状态=%s 判定依据=当前持久化状态",
            reservation_id, result.analyst_id, status,
        )
        return result

    def _check_entities_for_write(
        self,
        request_id: str,
        analyst_id: str,
        normalized: list[tuple[str, float]],
        op: str,
    ) -> None:
        """与 consume 一致：先在事务外给出可区分的实体/授权错误。"""
        if not self._store.analyst_exists(analyst_id):
            logger.warning(
                "%s拒绝 %s=%s analyst_id=%s 依据=analyst_not_found",
                op, "reservation_id" if op == "reserve" else "query_id",
                request_id, analyst_id,
            )
            raise AnalystNotFoundError(
                f"分析者不存在: {analyst_id}",
                details={"analyst_id": analyst_id, "reservation_id": request_id},
            )
        for dataset_id, _amount in normalized:
            if not self._store.dataset_exists(dataset_id):
                logger.warning(
                    "%s拒绝 reservation_id=%s analyst_id=%s dataset_id=%s 依据=dataset_not_found",
                    op, request_id, analyst_id, dataset_id,
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
                    "%s拒绝 reservation_id=%s analyst_id=%s dataset_id=%s 依据=unauthorized_dataset",
                    op, request_id, analyst_id, dataset_id,
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


def _validate_ttl(ttl: object) -> int:
    if (
        not isinstance(ttl, int)
        or isinstance(ttl, bool)
        or ttl <= 0
        or ttl > 60 * 60 * 24 * 365
    ):
        raise InvalidParameterError(
            "预占有效期 ttl_seconds 必须是 1..31536000 的正整数秒",
            details={"field": "ttl_seconds", "value": ttl},
        )
    return ttl


def _validate_reservation_charges(
    charges: Iterable[ReservationCharge],
) -> list[tuple[str, float]]:
    items = list(charges)
    if not items:
        raise InvalidParameterError(
            "charges 至少包含一个预占项", details={"field": "charges"}
        )
    normalized: list[tuple[str, float]] = []
    seen: set[str] = set()
    for idx, charge in enumerate(items):
        if not isinstance(charge, ReservationCharge):
            raise InvalidParameterError(
                f"charges[{idx}] 必须是 ReservationCharge",
                details={"field": f"charges[{idx}]", "value": repr(charge)},
            )
        dataset_id = _require_nonempty_id(charge.dataset_id, f"charges[{idx}].dataset_id")
        if not _is_finite_number(charge.amount) or charge.amount <= 0:
            raise InvalidParameterError(
                f"charges[{idx}].amount 必须是正数",
                details={"field": f"charges[{idx}].amount", "value": charge.amount},
            )
        if dataset_id in seen:
            raise InvalidParameterError(
                f"同一次预占中数据集 {dataset_id} 出现多次，请合并预占项",
                details={"field": f"charges[{idx}].dataset_id", "dataset_id": dataset_id},
            )
        seen.add(dataset_id)
        normalized.append((dataset_id, float(charge.amount)))
    return normalized


def _validate_settlement_charges(
    charges: Iterable[ReservationCharge],
) -> list[tuple[str, float]]:
    """结算项允许为 0（预占了但实际没用），但仍必须为非负有限数。"""
    items = list(charges)
    if not items:
        raise InvalidParameterError(
            "charges 至少包含一个结算项（未使用的数据集请传 0）",
            details={"field": "charges"},
        )
    normalized: list[tuple[str, float]] = []
    seen: set[str] = set()
    for idx, charge in enumerate(items):
        if not isinstance(charge, ReservationCharge):
            raise InvalidParameterError(
                f"charges[{idx}] 必须是 ReservationCharge",
                details={"field": f"charges[{idx}]", "value": repr(charge)},
            )
        dataset_id = _require_nonempty_id(charge.dataset_id, f"charges[{idx}].dataset_id")
        if not _is_finite_number(charge.amount) or charge.amount < 0:
            raise InvalidParameterError(
                f"charges[{idx}].amount 必须是非负数",
                details={"field": f"charges[{idx}].amount", "value": charge.amount},
            )
        if dataset_id in seen:
            raise InvalidParameterError(
                f"同一次结算中数据集 {dataset_id} 出现多次，请合并结算项",
                details={"field": f"charges[{idx}].dataset_id", "dataset_id": dataset_id},
            )
        seen.add(dataset_id)
        normalized.append((dataset_id, float(charge.amount)))
    return normalized


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


def build_default_service(
    db_path: str | Path = ":memory:",
    *,
    clock=None,
    default_ttl_seconds: int | None = None,
) -> BudgetService:
    """构造并打开一个默认服务实例。

    clock 可注入（测试用），default_ttl_seconds 控制预占默认有效期；
    两者都不传时 TTL 取环境变量 BUDGET_RESERVATION_TTL_SECONDS（默认 900 秒）。
    """
    store = LedgerStore(db_path, clock=clock).open()
    return BudgetService(store, default_ttl_seconds=default_ttl_seconds)
