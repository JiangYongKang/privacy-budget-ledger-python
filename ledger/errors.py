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


# ---- 预占（先占额度、后结算）相关错误 ----
class ReservationNotFoundError(LedgerError):
    code = "reservation_not_found"
    http_status = 404


class ReservationExpiredError(LedgerError):
    """预占已过有效期并被自动释放，再拿去结算必须被明确拒绝。"""

    code = "reservation_expired"
    http_status = 410


class ReservationConflictError(LedgerError):
    """同一预占标识被提交了不一致的内容（数据集/金额/分析者）。"""

    code = "reservation_conflict"
    http_status = 409


class ReservationStateError(LedgerError):
    """预占当前状态不允许该操作（如对已归还的预占做结算）。"""

    code = "reservation_state_invalid"
    http_status = 409
