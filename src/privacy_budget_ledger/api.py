"""FastAPI HTTP 层：路由与错误码 -> HTTP 状态码映射。"""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .errors import HTTP_STATUS, ErrorCode, LedgerError
from .ledger import BudgetLedger
from .models import (
    AnalystCreate,
    BudgetGrant,
    BudgetView,
    DatasetCreate,
    QueryRequest,
    QueryResult,
)
from .storage import Storage


def _error_body(code: ErrorCode, message: str, details: dict | None = None) -> JSONResponse:
    return JSONResponse(
        status_code=HTTP_STATUS[code],
        content={
            "error": True,
            "error_code": code.value,
            "message": message,
            "details": details or {},
        },
    )


def create_app(ledger: BudgetLedger | None = None, db_path: str = ":memory:") -> FastAPI:
    """构造应用。注入 ledger 时优先用之（测试），否则按 db_path 自建文件账本。"""
    app = FastAPI(
        title="隐私预算记账服务",
        version="0.1.0",
        description="分析者 x 数据集维度的隐私预算记账、幂等消耗与组合扣减。",
    )

    if ledger is None:
        storage = Storage(db_path)
        storage.init_schema()
        ledger = BudgetLedger(storage)
    app.state.ledger = ledger

    @app.exception_handler(LedgerError)
    async def _on_ledger_error(_: Request, exc: LedgerError) -> JSONResponse:
        return _error_body(exc.code, exc.message, exc.details)

    @app.exception_handler(RequestValidationError)
    async def _on_validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
        return _error_body(
            ErrorCode.INVALID_PARAMETER,
            "请求参数不合法",
            {"validation": exc.errors()},
        )

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.post("/admin/analysts", status_code=201)
    def create_analyst(req: AnalystCreate) -> dict:
        ledger.register_analyst(req)
        return {"analyst_id": req.analyst_id, "created": True}

    @app.post("/admin/datasets", status_code=201)
    def create_dataset_api(req: DatasetCreate) -> dict:
        ledger.register_dataset(req)
        return {"dataset_id": req.dataset_id, "created": True}

    @app.put("/admin/budgets")
    def grant_budget(req: BudgetGrant) -> dict:
        """首次授权或单调上调预算上限；下调返回 LEDGER_ROLLBACK_REJECTED。"""
        ledger.grant_budget(req)
        return {
            "analyst_id": req.analyst_id,
            "dataset_id": req.dataset_id,
            "budget_limit": req.budget_limit,
        }

    @app.get(
        "/analysts/{analyst_id}/datasets/{dataset_id}/budget",
        response_model=BudgetView,
    )
    def get_budget(analyst_id: str, dataset_id: str) -> BudgetView:
        return ledger.get_budget(analyst_id, dataset_id)

    @app.post("/queries", response_model=QueryResult)
    def submit_query(req: QueryRequest) -> QueryResult:
        return ledger.submit_query(req)

    @app.get("/queries/{query_id}", response_model=QueryResult)
    def get_query(query_id: str) -> QueryResult:
        return ledger.get_query(query_id)

    return app
