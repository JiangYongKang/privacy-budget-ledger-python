"""账本领域错误：每类失败原因都有可区分的错误码。"""

from __future__ import annotations


class LedgerError(Exception):
    """所有账本错误的基类。"""

    code: str = "ledger_error"
    http_status: int = 400

    def __init__(self, message: str, *, details: dict | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}


class AnalystNotFoundError(LedgerError):
    code = "analyst_not_found"
    http_status = 404


class DatasetNotFoundError(LedgerError):
    code = "dataset_not_found"
    http_status = 404


class UnauthorizedDatasetError(LedgerError):
    code = "unauthorized_dataset"
    http_status = 403


class BudgetInsufficientError(LedgerError):
    code = "budget_insufficient"
    http_status = 409


class QueryConflictError(LedgerError):
    code = "query_conflict"
    http_status = 409


class InvalidParameterError(LedgerError):
    code = "invalid_parameter"
    http_status = 422


class LedgerRollbackError(LedgerError):
    """账本倒退（事件序列被回退/覆盖/余额非法）时拒绝服务。"""

    code = "ledger_rollback_detected"
    http_status = 500


class ReservationNotFoundError(LedgerError):
    """预占标识不存在。"""

    code = "reservation_not_found"
    http_status = 404


class ReservationExpiredError(LedgerError):
    """预占已过期：占用已自动释放，不能再据此结算。"""

    code = "reservation_expired"
    http_status = 409


class ReservationClosedError(LedgerError):
    """预占已终结（已结算/已归还），不能再执行与之互斥的操作。"""

    code = "reservation_closed"
    http_status = 409


class ReservationConflictError(LedgerError):
    """同一预占标识被用于与首次不一致的内容/分析者。"""

    code = "reservation_conflict"
    http_status = 409
