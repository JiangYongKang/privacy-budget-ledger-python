"""Pydantic 数据模型：请求、响应与账本视图。"""

from __future__ import annotations

from pydantic import BaseModel, Field


class DatasetCreate(BaseModel):
    dataset_id: str = Field(min_length=1)
    name: str = ""


class AnalystCreate(BaseModel):
    analyst_id: str = Field(min_length=1)
    name: str = ""


class BudgetGrant(BaseModel):
    """为 (分析者, 数据集) 授权预算上限。"""

    analyst_id: str = Field(min_length=1)
    dataset_id: str = Field(min_length=1)
    budget_limit: float = Field(gt=0)


class BudgetView(BaseModel):
    analyst_id: str
    dataset_id: str
    budget_limit: float
    spent: float
    remaining: float


class QueryItem(BaseModel):
    """组合查询中的单个数据集条目，cost 必须为正。"""

    dataset_id: str = Field(min_length=1)
    cost: float = Field(gt=0)


class QueryRequest(BaseModel):
    query_id: str = Field(min_length=1)
    analyst_id: str = Field(min_length=1)
    items: list[QueryItem] = Field(min_length=1)


class QueryResultEntry(BaseModel):
    dataset_id: str
    cost: float
    budget_limit: float
    spent_before: float
    spent_after: float
    remaining: float


class QueryResult(BaseModel):
    query_id: str
    analyst_id: str
    status: str  # SUCCESS / FAILED
    idempotent_replay: bool
    entries: list[QueryResultEntry] = []
    error_code: str | None = None
    error_message: str | None = None
