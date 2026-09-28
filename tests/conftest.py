"""测试夹具：每用例独立临时账本文件；另提供 HTTP 客户端与预置数据。"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from privacy_budget_ledger.api import create_app
from privacy_budget_ledger.ledger import BudgetLedger
from privacy_budget_ledger.logging_config import configure_logging
from privacy_budget_ledger.models import AnalystCreate, BudgetGrant, DatasetCreate
from privacy_budget_ledger.storage import Storage


@pytest.fixture(autouse=True)
def _logging():
    configure_logging(logging.INFO)


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "test_ledger.db"


@pytest.fixture
def storage(db_path: Path) -> Storage:
    s = Storage(db_path)
    s.init_schema()
    yield s
    s.close()


@pytest.fixture
def ledger(storage: Storage) -> BudgetLedger:
    return BudgetLedger(storage)


@pytest.fixture
def client(ledger: BudgetLedger) -> TestClient:
    return TestClient(create_app(ledger=ledger))


@pytest.fixture
def seeded(ledger: BudgetLedger):
    """预置：analyst a1，数据集 ds1/ds2/ds3，额度 10/10；a2 仅有 ds1。"""
    ledger.register_analyst(AnalystCreate(analyst_id="a1", name="分析师一"))
    ledger.register_analyst(AnalystCreate(analyst_id="a2", name="分析师二"))
    ledger.register_dataset(DatasetCreate(dataset_id="ds1", name="数据集一"))
    ledger.register_dataset(DatasetCreate(dataset_id="ds2", name="数据集二"))
    ledger.register_dataset(DatasetCreate(dataset_id="ds3", name="数据集三"))
    ledger.grant_budget(BudgetGrant(analyst_id="a1", dataset_id="ds1", budget_limit=10.0))
    ledger.grant_budget(BudgetGrant(analyst_id="a1", dataset_id="ds2", budget_limit=10.0))
    ledger.grant_budget(BudgetGrant(analyst_id="a2", dataset_id="ds1", budget_limit=10.0))
    return {"analysts": ["a1", "a2"], "datasets": ["ds1", "ds2", "ds3"]}
