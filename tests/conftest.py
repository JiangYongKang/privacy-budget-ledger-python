"""pytest 公共夹具与日志配置。

要求：单测日志必须能看到 查询标识 / 扣减前后余额 / 判定依据。
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pytest

from ledger.service import BudgetService
from ledger.store import LedgerStore

# 保证用例运行时一定能看到账本判定日志（即便未启用 log_cli）
_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(
    logging.Formatter("%(levelname)s %(name)s: %(message)s")
)
_logger = logging.getLogger("privacy_budget_ledger")
_logger.setLevel(logging.INFO)
if not any(getattr(h, "_pytest_ledger", False) for h in _logger.handlers):
    _handler._pytest_ledger = True  # type: ignore[attr-defined]
    _logger.addHandler(_handler)
_logger.propagate = True


def log_case(title: str, **fields: object) -> None:
    """统一的用例判定行：包含查询标识、前后余额、判定依据等。"""
    tail = " ".join(f"{k}={v}" for k, v in fields.items())
    logging.getLogger("privacy_budget_ledger").info("[用例] %s | %s", title, tail)


@pytest.fixture
def service() -> BudgetService:
    """内存账本 + 标准测试数据：a1 授权 d1=10, d2=5。"""
    store = LedgerStore(":memory:").open()
    svc = BudgetService(store)
    svc.register_analyst("a1")
    svc.register_analyst("a2")  # 存在但未授权任何数据集
    svc.register_dataset("d1")
    svc.register_dataset("d2")
    svc.register_dataset("d3")  # 存在但 a1/a2 都未授权
    svc.authorize("a1", "d1", 10.0)
    svc.authorize("a1", "d2", 5.0)
    return svc


@pytest.fixture
def file_db(tmp_path: Path):
    return tmp_path / "ledger.db"
