"""可区分的失败原因分类。

所有业务失败都抛出 :class:`LedgerError`，并携带稳定的错误码；
HTTP 层据此映射为不同的状态码与错误体。
"""

from __future__ import annotations

from enum import Enum


class ErrorCode(str, Enum):
    """账本失败原因码（对调用方稳定，禁止改名/改值）。"""

    INVALID_PARAMETER = "INVALID_PARAMETER"
    ANALYST_NOT_FOUND = "ANALYST_NOT_FOUND"
    DATASET_NOT_FOUND = "DATASET_NOT_FOUND"
    UNAUTHORIZED_DATASET = "UNAUTHORIZED_DATASET"
    BUDGET_INSUFFICIENT = "BUDGET_INSUFFICIENT"
    QUERY_ID_CONFLICT = "QUERY_ID_CONFLICT"
    QUERY_NOT_FOUND = "QUERY_NOT_FOUND"
    LEDGER_ROLLBACK_REJECTED = "LEDGER_ROLLBACK_REJECTED"
    DATASET_ALREADY_EXISTS = "DATASET_ALREADY_EXISTS"
    ANALYST_ALREADY_EXISTS = "ANALYST_ALREADY_EXISTS"


# 错误码 -> HTTP 状态码
HTTP_STATUS: dict[ErrorCode, int] = {
    ErrorCode.INVALID_PARAMETER: 422,
    ErrorCode.ANALYST_NOT_FOUND: 404,
    ErrorCode.DATASET_NOT_FOUND: 404,
    ErrorCode.QUERY_NOT_FOUND: 404,
    ErrorCode.UNAUTHORIZED_DATASET: 403,
    ErrorCode.BUDGET_INSUFFICIENT: 409,
    ErrorCode.QUERY_ID_CONFLICT: 409,
    ErrorCode.LEDGER_ROLLBACK_REJECTED: 409,
    ErrorCode.DATASET_ALREADY_EXISTS: 409,
    ErrorCode.ANALYST_ALREADY_EXISTS: 409,
}


class LedgerError(Exception):
    """带稳定错误码的业务异常。"""

    def __init__(self, code: ErrorCode, message: str, details: dict | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}
