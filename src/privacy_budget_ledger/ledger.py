"""账本服务：鉴权、校验、幂等与组合扣减的编排。

规则收口处：
* 不存在/越权/非法参数在任何写事务之前拒绝，绝不产生扣减；
* 成功消耗以 ``query_id`` 全局唯一登记，重复提交回放首次结论（幂等）；
  同一 ``query_id`` 携带不同请求体则判为查询标识冲突；
* 跨数据集组合扣减在单条 IMMEDIATE 事务内完成，条件更新保证不越上限，
  任一条目失败 -> 整笔 ROLLBACK，无中间状态。
"""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3

from .errors import ErrorCode, LedgerError
from .models import (
    AnalystCreate,
    BudgetGrant,
    BudgetView,
    DatasetCreate,
    QueryRequest,
    QueryResult,
    QueryResultEntry,
)
from .storage import Storage

logger = logging.getLogger("privacy_budget_ledger")


def canonical_request(analyst_id: str, items: list[tuple[str, float]]) -> str:
    """规范化请求体：条目按 dataset_id 排序，消除提交顺序差异。"""
    payload = {
        "analyst_id": analyst_id,
        "items": sorted(({"dataset_id": d, "cost": c} for d, c in items),
                        key=lambda x: x["dataset_id"]),
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def request_hash(analyst_id: str, items: list[tuple[str, float]]) -> str:
    return hashlib.sha256(canonical_request(analyst_id, items).encode("utf-8")).hexdigest()


class BudgetLedger:
    """对外领域服务。"""

    def __init__(self, storage: Storage) -> None:
        self.storage = storage

    # ---------- 管理面 ----------
    def register_analyst(self, req: AnalystCreate) -> None:
        try:
            self.storage.create_analyst(req.analyst_id, req.name)
        except sqlite3.IntegrityError as exc:
            raise LedgerError(
                ErrorCode.ANALYST_ALREADY_EXISTS,
                f"analyst_id 已存在: {req.analyst_id}",
                {"analyst_id": req.analyst_id},
            ) from exc
        logger.info("登记分析者 analyst_id=%s 判定依据=此前不存在则插入成功", req.analyst_id)

    def register_dataset(self, req: DatasetCreate) -> None:
        try:
            self.storage.create_dataset(req.dataset_id, req.name)
        except sqlite3.IntegrityError as exc:
            raise LedgerError(
                ErrorCode.DATASET_ALREADY_EXISTS,
                f"dataset_id 已存在: {req.dataset_id}",
                {"dataset_id": req.dataset_id},
            ) from exc
        logger.info("登记数据集 dataset_id=%s 判定依据=此前不存在则插入成功", req.dataset_id)

    def grant_budget(self, req: BudgetGrant) -> None:
        # 实体不存在要先于授权拒绝，且不写任何数据
        if not self.storage.analyst_exists(req.analyst_id):
            raise LedgerError(
                ErrorCode.ANALYST_NOT_FOUND,
                f"分析者不存在: {req.analyst_id}",
                {"analyst_id": req.analyst_id},
            )
        if not self.storage.dataset_exists(req.dataset_id):
            raise LedgerError(
                ErrorCode.DATASET_NOT_FOUND,
                f"数据集不存在: {req.dataset_id}",
                {"dataset_id": req.dataset_id},
            )
        _, spent, is_new = self.storage.grant_budget(
            req.analyst_id, req.dataset_id, req.budget_limit
        )
        action = "首次授权" if is_new else "上调上限"
        logger.info(
            "%s analyst_id=%s dataset_id=%s budget_limit=%.6f spent=%.6f remaining=%.6f "
            "判定依据=budget_limit 单调上调且不小于已消耗",
            action, req.analyst_id, req.dataset_id, req.budget_limit, spent,
            req.budget_limit - spent,
        )

    # ---------- 查询面 ----------
    def get_budget(self, analyst_id: str, dataset_id: str) -> BudgetView:
        if not self.storage.analyst_exists(analyst_id):
            raise LedgerError(
                ErrorCode.ANALYST_NOT_FOUND,
                f"分析者不存在: {analyst_id}",
                {"analyst_id": analyst_id},
            )
        if not self.storage.dataset_exists(dataset_id):
            raise LedgerError(
                ErrorCode.DATASET_NOT_FOUND,
                f"数据集不存在: {dataset_id}",
                {"dataset_id": dataset_id},
            )
        row = self.storage.get_budget(analyst_id, dataset_id)
        if row is None:
            raise LedgerError(
                ErrorCode.UNAUTHORIZED_DATASET,
                f"分析者 {analyst_id} 未被授权访问数据集 {dataset_id}",
                {"analyst_id": analyst_id, "dataset_id": dataset_id},
            )
        limit, spent = row
        return BudgetView(
            analyst_id=analyst_id,
            dataset_id=dataset_id,
            budget_limit=limit,
            spent=spent,
            remaining=limit - spent,
        )

    def get_query(self, query_id: str) -> QueryResult:
        if not query_id:
            raise LedgerError(
                ErrorCode.INVALID_PARAMETER, "query_id 不能为空", {"field": "query_id"}
            )
        row = self.storage.get_query_record(query_id)
        if row is None:
            raise LedgerError(
                ErrorCode.QUERY_NOT_FOUND,
                f"查询标识不存在: {query_id}",
                {"query_id": query_id},
            )
        result = QueryResult.model_validate_json(row["result_json"])
        result.idempotent_replay = True  # 对历史结果的查询统一标记为回放视图
        return result

    # ---------- 核心：提交查询（幂等 + 组合原子扣减） ----------
    def submit_query(self, req: QueryRequest) -> QueryResult:
        items: list[tuple[str, float]] = [(it.dataset_id, it.cost) for it in req.items]

        # 1) 请求体内部合法性（重复数据集条目视为参数非法）
        seen: set[str] = set()
        for dataset_id, cost in items:
            if not dataset_id:
                raise LedgerError(
                    ErrorCode.INVALID_PARAMETER,
                    "items.dataset_id 不能为空",
                    {"field": "items.dataset_id"},
                )
            if not (cost == cost) or cost in (float("inf"), float("-inf")) or cost <= 0:
                # 用 repr 回显，避免 NaN/Infinity 进入 JSON 响应体导致序列化失败
                raise LedgerError(
                    ErrorCode.INVALID_PARAMETER,
                    "items.cost 必须为有限正数",
                    {"dataset_id": dataset_id, "cost": repr(cost)},
                )
            if dataset_id in seen:
                raise LedgerError(
                    ErrorCode.INVALID_PARAMETER,
                    "同一请求内 dataset_id 不允许重复",
                    {"dataset_id": dataset_id},
                )
            seen.add(dataset_id)

        digest = request_hash(req.analyst_id, items)

        # 2) 快速幂等路径：已登记的成功结论直接回放
        existing = self.storage.get_query_record(req.query_id)
        if existing is not None:
            return self._replay_or_conflict(req.query_id, existing, digest)

        # 3) 实体与授权检查：任何不满足都在事务外拒绝，零扣减
        if not self.storage.analyst_exists(req.analyst_id):
            self._log_reject(req.query_id, "分析者不存在", ErrorCode.ANALYST_NOT_FOUND)
            raise LedgerError(
                ErrorCode.ANALYST_NOT_FOUND,
                f"分析者不存在: {req.analyst_id}",
                {"analyst_id": req.analyst_id},
            )
        for dataset_id, _ in items:
            if not self.storage.dataset_exists(dataset_id):
                self._log_reject(
                    req.query_id, f"数据集不存在 {dataset_id}", ErrorCode.DATASET_NOT_FOUND
                )
                raise LedgerError(
                    ErrorCode.DATASET_NOT_FOUND,
                    f"数据集不存在: {dataset_id}",
                    {"dataset_id": dataset_id},
                )
            if self.storage.get_budget(req.analyst_id, dataset_id) is None:
                self._log_reject(
                    req.query_id,
                    f"分析者未被授权数据集 {dataset_id}",
                    ErrorCode.UNAUTHORIZED_DATASET,
                )
                raise LedgerError(
                    ErrorCode.UNAUTHORIZED_DATASET,
                    f"分析者 {req.analyst_id} 未被授权访问数据集 {dataset_id}",
                    {"analyst_id": req.analyst_id, "dataset_id": dataset_id},
                )

        # 4) 单条 IMMEDIATE 事务：登记 + 全部扣减，要么全成要么全不成
        try:
            with self.storage.transaction() as txn:
                # 事务内再查一次，淘汰并发窗口内的重复提交
                raced = txn.get_query_record(req.query_id)
                if raced is not None:
                    return self._replay_or_conflict(req.query_id, raced, digest)

                entries: list[QueryResultEntry] = []
                for dataset_id, cost in items:
                    budget = txn.get_budget(req.analyst_id, dataset_id)
                    assert budget is not None  # 第 3 步已验证
                    limit, spent_before = budget
                    if spent_before + cost > limit:
                        logger.info(
                            "query_id=%s 拒绝扣减 dataset_id=%s cost=%.6f "
                            "扣减前余额=%.6f/上限=%.6f 扣减后将为=%.6f "
                            "判定依据=spent_before+cost > budget_limit，整笔回滚不产生扣减",
                            req.query_id, dataset_id, cost,
                            limit - spent_before, limit, spent_before + cost,
                        )
                        raise LedgerError(
                            ErrorCode.BUDGET_INSUFFICIENT,
                            f"数据集 {dataset_id} 预算不足",
                            {
                                "analyst_id": req.analyst_id,
                                "dataset_id": dataset_id,
                                "budget_limit": limit,
                                "spent_before": spent_before,
                                "remaining_before": limit - spent_before,
                                "requested_cost": cost,
                            },
                        )
                    affected = txn.apply_deduction(req.analyst_id, dataset_id, cost)
                    if affected != 1:
                        # 理论上不可达：条件更新与上面的判断等价；留作并发兜底
                        raise LedgerError(
                            ErrorCode.BUDGET_INSUFFICIENT,
                            f"数据集 {dataset_id} 预算不足（条件更新未命中）",
                            {"dataset_id": dataset_id},
                        )
                    spent_after = spent_before + cost
                    entries.append(
                        QueryResultEntry(
                            dataset_id=dataset_id,
                            cost=cost,
                            budget_limit=limit,
                            spent_before=spent_before,
                            spent_after=spent_after,
                            remaining=limit - spent_after,
                        )
                    )
                    logger.info(
                        "query_id=%s 扣减成功 dataset_id=%s cost=%.6f "
                        "扣减前已用=%.6f 扣减后已用=%.6f 扣减前余额=%.6f 扣减后余额=%.6f "
                        "上限=%.6f 判定依据=条件 UPDATE 命中 1 行且 spent+cost<=limit",
                        req.query_id, dataset_id, cost, spent_before, spent_after,
                        limit - spent_before, limit - spent_after, limit,
                    )

                result = QueryResult(
                    query_id=req.query_id,
                    analyst_id=req.analyst_id,
                    status="SUCCESS",
                    idempotent_replay=False,
                    entries=entries,
                )
                txn.insert_query_record(
                    query_id=req.query_id,
                    analyst_id=req.analyst_id,
                    status="SUCCESS",
                    request_hash=digest,
                    result_json=result.model_dump_json(),
                )
                logger.info(
                    "query_id=%s 组合查询提交完成 条目数=%d 判定依据=全部条目扣减成功且 "
                    "query_id 登记为 SUCCESS，事务 COMMIT；重复提交将只回放此结论",
                    req.query_id, len(entries),
                )
                return result
        except sqlite3.IntegrityError as exc:
            # 并发下另一个线程已抢先登记同一 query_id
            row = self.storage.get_query_record(req.query_id)
            if row is not None:
                logger.info(
                    "query_id=%s 并发下被其他请求抢先登记，本次不重复扣减，转回放 "
                    "判定依据=唯一索引冲突后重读已有记录",
                    req.query_id,
                )
                return self._replay_or_conflict(req.query_id, row, digest)
            raise LedgerError(
                ErrorCode.QUERY_ID_CONFLICT,
                f"查询标识冲突: {req.query_id}",
                {"query_id": req.query_id},
            ) from exc

    # ---------- 内部辅助 ----------
    def _replay_or_conflict(
        self, query_id: str, row: sqlite3.Row, digest: str
    ) -> QueryResult:
        if row["request_hash"] != digest:
            logger.info(
                "query_id=%s 拒绝 判定依据=同一 query_id 已登记不同请求体 "
                "stored_hash=%s incoming_hash=%s -> QUERY_ID_CONFLICT",
                query_id, row["request_hash"][:12], digest[:12],
            )
            raise LedgerError(
                ErrorCode.QUERY_ID_CONFLICT,
                f"查询标识已被不同请求占用: {query_id}",
                {
                    "query_id": query_id,
                    "registered_analyst_id": row["analyst_id"],
                    "registered_status": row["status"],
                },
            )
        result = QueryResult.model_validate_json(row["result_json"])
        result.idempotent_replay = True
        logger.info(
            "query_id=%s 幂等回放 analyst_id=%s 条目数=%d 扣减前后余额以首次登记为准、不再扣减 "
            "判定依据=request_hash 一致且状态=%s",
            query_id, result.analyst_id, len(result.entries), row["status"],
        )
        return result

    @staticmethod
    def _log_reject(query_id: str, reason: str, code: ErrorCode) -> None:
        logger.info(
            "query_id=%s 请求被拒绝 原因=%s error_code=%s 判定依据=进入扣减事务前校验失败，"
            "未开启写事务，因此无任何扣减",
            query_id, reason, code.value,
        )
