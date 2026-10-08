"""FastAPI HTTP 接口：把领域错误映射为可区分的失败响应。

错误响应统一形如：
    {"error": {"code": "<machine_readable>", "message": "...", "details": {...}}}
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator

from ledger.errors import LedgerError
from ledger.models import DatasetCharge, ReservationCharge
from ledger.service import BudgetService, build_default_service

DEFAULT_DB_PATH = os.environ.get("BUDGET_LEDGER_DB", "data/budget_ledger.db")


# ---------------------------------------------------------------------- #
# 请求 / 响应模型
# ---------------------------------------------------------------------- #
class IdBody(BaseModel):
    id: str = Field(min_length=1, description="分析者或数据集标识")


class GrantBody(BaseModel):
    limit: float = Field(ge=0, description="预算上限（非负）")

    @field_validator("limit")
    @classmethod
    def _finite(cls, v: float) -> float:
        if v != v or v in (float("inf"), float("-inf")):
            raise ValueError("limit 必须是有限数")
        return v


class ChargeBody(BaseModel):
    dataset_id: str = Field(min_length=1)
    cost: float = Field(gt=0, description="本次扣减量（正数）")

    @field_validator("cost")
    @classmethod
    def _finite(cls, v: float) -> float:
        if v != v or v in (float("inf"), float("-inf")):
            raise ValueError("cost 必须是有限数")
        return v


class ConsumeBody(BaseModel):
    query_id: str = Field(min_length=1, description="全局唯一查询标识，用于幂等")
    analyst_id: str = Field(min_length=1)
    charges: list[ChargeBody] = Field(min_length=1)


class ReserveChargeBody(BaseModel):
    dataset_id: str = Field(min_length=1)
    amount: float = Field(gt=0, description="预占量（正数）")

    @field_validator("amount")
    @classmethod
    def _finite(cls, v: float) -> float:
        if v != v or v in (float("inf"), float("-inf")):
            raise ValueError("amount 必须是有限数")
        return v


class SettleChargeBody(BaseModel):
    dataset_id: str = Field(min_length=1)
    amount: float = Field(ge=0, description="实际用量（非负，0 表示该数据集没用）")

    @field_validator("amount")
    @classmethod
    def _finite(cls, v: float) -> float:
        if v != v or v in (float("inf"), float("-inf")):
            raise ValueError("amount 必须是有限数")
        return v


class ReserveBody(BaseModel):
    reservation_id: str = Field(min_length=1, description="预占标识，用于幂等与后续结算/归还")
    analyst_id: str = Field(min_length=1)
    charges: list[ReserveChargeBody] = Field(min_length=1)
    ttl_seconds: int | None = Field(
        default=None, ge=1, description="有效期秒数；不传用服务默认值"
    )


class SettleBody(BaseModel):
    reservation_id: str = Field(min_length=1)
    analyst_id: str = Field(min_length=1)
    charges: list[SettleChargeBody] = Field(min_length=1)


class ReleaseBody(BaseModel):
    reservation_id: str = Field(min_length=1)
    analyst_id: str = Field(min_length=1)


# ---------------------------------------------------------------------- #
# 应用组装
# ---------------------------------------------------------------------- #
def create_app(service: BudgetService | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        nonlocal service
        if service is None:
            db_path = Path(os.environ.get("BUDGET_LEDGER_DB", "data/budget_ledger.db"))
            db_path.parent.mkdir(parents=True, exist_ok=True)
            service = build_default_service(db_path)
        _app.state.service = service
        try:
            yield
        finally:
            _app.state.service = None
            service._store.close()

    app = FastAPI(title="privacy-budget-ledger", version="0.1.0", lifespan=lifespan)

    @app.exception_handler(LedgerError)
    async def _ledger_error_handler(_request: Request, exc: LedgerError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.http_status,
            content={
                "error": {
                    "code": exc.code,
                    "message": exc.message,
                    "details": exc.details,
                }
            },
        )

    @app.exception_handler(RequestValidationError)
    async def _validation_handler(_request: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "error": {
                    "code": "invalid_parameter",
                    "message": "请求参数非法",
                    "details": {"issues": exc.errors()},
                }
            },
        )

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/analysts", status_code=201)
    def create_analyst(body: IdBody) -> dict[str, str]:
        service.register_analyst(body.id)
        return {"analyst_id": body.id}

    @app.post("/datasets", status_code=201)
    def create_dataset(body: IdBody) -> dict[str, str]:
        service.register_dataset(body.id)
        return {"dataset_id": body.id}

    @app.put("/analysts/{analyst_id}/datasets/{dataset_id}/grant")
    def grant(analyst_id: str, dataset_id: str, body: GrantBody) -> dict[str, object]:
        service.authorize(analyst_id, dataset_id, body.limit)
        return {
            "analyst_id": analyst_id,
            "dataset_id": dataset_id,
            "limit": body.limit,
        }

    @app.get("/analysts/{analyst_id}/datasets/{dataset_id}/balance")
    def balance(analyst_id: str, dataset_id: str) -> dict[str, object]:
        view = service.get_balance(analyst_id, dataset_id)
        return _balance_dict(view)

    @app.get("/analysts/{analyst_id}/balances")
    def balances(analyst_id: str) -> dict[str, object]:
        views = service.list_balances(analyst_id)
        return {
            "analyst_id": analyst_id,
            "datasets": [_balance_dict(v) for v in views],
        }

    @app.post("/consume")
    def consume(body: ConsumeBody) -> dict[str, object]:
        dataset_ids = [c.dataset_id for c in body.charges]
        if len(dataset_ids) != len(set(dataset_ids)):
            # 与服务层一致：重复数据集给出可区分的 invalid_parameter
            from ledger.errors import InvalidParameterError

            raise InvalidParameterError(
                "同一次查询中存在重复的 dataset_id，请先合并扣减项",
                details={"field": "charges.dataset_id", "values": dataset_ids},
            )
        result = service.consume(
            query_id=body.query_id,
            analyst_id=body.analyst_id,
            charges=[
                DatasetCharge(dataset_id=c.dataset_id, cost=c.cost)
                for c in body.charges
            ],
        )
        return {
            "query_id": result.query_id,
            "analyst_id": result.analyst_id,
            "status": "idempotent_replay" if result.replay else "accepted",
            "total_charged": result.total_charged,
            "items": [
                {
                    "dataset_id": i.dataset_id,
                    "charged": i.charged,
                    "balance_before": i.balance_before,
                    "balance_after": i.balance_after,
                }
                for i in result.items
            ],
        }

    @app.get("/queries/{query_id}")
    def get_query(query_id: str) -> dict[str, object]:
        record = service.get_query_record(query_id)
        if record is None:
            return JSONResponse(
                status_code=404,
                content={
                    "error": {
                        "code": "query_not_found",
                        "message": f"查询标识不存在: {query_id}",
                        "details": {"query_id": query_id},
                    }
                },
            )
        return {
            "query_id": record.query_id,
            "analyst_id": record.analyst_id,
            "total_charged": record.total_charged,
            "items": [
                {
                    "dataset_id": i.dataset_id,
                    "charged": i.charged,
                    "balance_before": i.balance_before,
                    "balance_after": i.balance_after,
                }
                for i in record.items
            ],
        }

    @app.post("/reservations", status_code=201)
    def reserve(body: ReserveBody) -> dict[str, object]:
        dataset_ids = [c.dataset_id for c in body.charges]
        if len(dataset_ids) != len(set(dataset_ids)):
            from ledger.errors import InvalidParameterError

            raise InvalidParameterError(
                "同一次预占中存在重复的 dataset_id，请先合并预占项",
                details={"field": "charges.dataset_id", "values": dataset_ids},
            )
        result = service.reserve_budget(
            reservation_id=body.reservation_id,
            analyst_id=body.analyst_id,
            charges=[
                ReservationCharge(dataset_id=c.dataset_id, amount=c.amount)
                for c in body.charges
            ],
            ttl_seconds=body.ttl_seconds,
        )
        return {
            "reservation_id": result.reservation_id,
            "analyst_id": result.analyst_id,
            "status": "idempotent_replay" if result.replay else result.status,
            "expires_at": result.expires_at,
            "ttl_seconds": result.ttl_seconds,
            "total_reserved": result.total_reserved,
            "items": [
                {
                    "dataset_id": i.dataset_id,
                    "reserved": i.reserved,
                    "available_before": i.available_before,
                    "available_after": i.available_after,
                }
                for i in result.items
            ],
        }

    @app.post("/reservations/settle")
    def settle(body: SettleBody) -> dict[str, object]:
        dataset_ids = [c.dataset_id for c in body.charges]
        if len(dataset_ids) != len(set(dataset_ids)):
            from ledger.errors import InvalidParameterError

            raise InvalidParameterError(
                "同一次结算中存在重复的 dataset_id，请先合并结算项",
                details={"field": "charges.dataset_id", "values": dataset_ids},
            )
        result = service.settle_reservation(
            reservation_id=body.reservation_id,
            analyst_id=body.analyst_id,
            charges=[
                ReservationCharge(dataset_id=c.dataset_id, amount=c.amount)
                for c in body.charges
            ],
        )
        return {
            "reservation_id": result.reservation_id,
            "analyst_id": result.analyst_id,
            "status": "idempotent_replay" if result.replay else "settled",
            "total_charged": result.total_charged,
            "total_refunded": result.total_refunded,
            "items": [
                {
                    "dataset_id": i.dataset_id,
                    "reserved": i.reserved,
                    "charged": i.charged,
                    "refunded": i.refunded,
                    "available_after": i.available_after,
                }
                for i in result.items
            ],
        }

    @app.post("/reservations/release")
    def release(body: ReleaseBody) -> dict[str, object]:
        result = service.release_reservation(
            reservation_id=body.reservation_id,
            analyst_id=body.analyst_id,
        )
        return {
            "reservation_id": result.reservation_id,
            "analyst_id": result.analyst_id,
            "status": "idempotent_replay" if result.replay else result.status,
            "total_refunded": result.total_refunded,
        }

    @app.post("/reservations/sweep-expired")
    def sweep_expired() -> dict[str, object]:
        expired_ids = service.sweep_expired_reservations()
        return {"expired": expired_ids, "count": len(expired_ids)}

    @app.get("/reservations/{reservation_id}")
    def get_reservation(reservation_id: str) -> dict[str, object]:
        result = service.get_reservation(reservation_id)
        return {
            "reservation_id": result.reservation_id,
            "analyst_id": result.analyst_id,
            "status": result.status,
            "expires_at": result.expires_at,
            "ttl_seconds": result.ttl_seconds,
            "total_reserved": result.total_reserved,
            "items": [
                {"dataset_id": i.dataset_id, "reserved": i.reserved}
                for i in result.items
            ],
        }

    return app


def _balance_dict(view) -> dict[str, object]:
    return {
        "analyst_id": view.analyst_id,
        "dataset_id": view.dataset_id,
        "limit": view.limit,
        "consumed": view.consumed,
        "balance": view.balance,
        "reserved": view.reserved,
        "available": view.available,
    }


# 默认应用：启动时按 BUDGET_LEDGER_DB 打开文件库（默认 data/budget_ledger.db），
# 服务重启后已消耗额度保留；测试可用 create_app(注入服务) 或内存库。
app = create_app()
