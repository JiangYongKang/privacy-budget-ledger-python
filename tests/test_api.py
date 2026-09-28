"""HTTP 端到端：管理面 + 提交/查询 + 错误码可区分 + 重启读文件。"""

from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from privacy_budget_ledger.api import create_app
from privacy_budget_ledger.ledger import BudgetLedger
from privacy_budget_ledger.storage import Storage


def _setup(client: TestClient) -> None:
    assert client.post("/admin/analysts", json={"analyst_id": "a1"}).status_code == 201
    assert client.post("/admin/datasets", json={"dataset_id": "ds1"}).status_code == 201
    r = client.put("/admin/budgets", json={
        "analyst_id": "a1", "dataset_id": "ds1", "budget_limit": 5.0})
    assert r.status_code == 200


def test_full_http_flow_and_idempotent_replay(client, seeded):
    body = {"query_id": "q-http", "analyst_id": "a1",
            "items": [{"dataset_id": "ds1", "cost": 2.0}]}

    r1 = client.post("/queries", json=body)
    assert r1.status_code == 200, r1.text
    assert r1.json()["status"] == "SUCCESS"
    assert r1.json()["idempotent_replay"] is False

    r2 = client.post("/queries", json=body)
    assert r2.status_code == 200
    assert r2.json()["idempotent_replay"] is True
    assert r2.json()["entries"][0]["spent_after"] == 2.0

    r3 = client.get("/queries/q-http")
    assert r3.status_code == 200
    assert r3.json()["query_id"] == "q-http"

    budget = client.get("/analysts/a1/datasets/ds1/budget").json()
    assert budget == {"analyst_id": "a1", "dataset_id": "ds1",
                      "budget_limit": 10.0, "spent": 2.0, "remaining": 8.0}


def test_http_error_codes_are_distinguishable(client, seeded):
    def err(payload):
        r = client.post("/queries", json=payload)
        assert r.status_code >= 400, r.text
        return r.status_code, r.json()["error_code"]

    assert err({"query_id": "q", "analyst_id": "ghost",
                "items": [{"dataset_id": "ds1", "cost": 1.0}]}) == (404, "ANALYST_NOT_FOUND")
    assert err({"query_id": "q", "analyst_id": "a1",
                "items": [{"dataset_id": "ghost", "cost": 1.0}]}) == (404, "DATASET_NOT_FOUND")
    assert err({"query_id": "q", "analyst_id": "a2",
                "items": [{"dataset_id": "ds2", "cost": 1.0}]}) == (403, "UNAUTHORIZED_DATASET")

    client.post("/queries", json={"query_id": "qc", "analyst_id": "a1",
                                  "items": [{"dataset_id": "ds1", "cost": 1.0}]})
    assert err({"query_id": "qc", "analyst_id": "a1",
                "items": [{"dataset_id": "ds1", "cost": 2.0}]}) == (409, "QUERY_ID_CONFLICT")

    assert err({"query_id": "qbig", "analyst_id": "a1",
                "items": [{"dataset_id": "ds1", "cost": 999.0}]}) == (409, "BUDGET_INSUFFICIENT")

    assert err({"query_id": "q", "analyst_id": "a1", "items": []}) == (422, "INVALID_PARAMETER")

    r = client.get("/queries/missing")
    assert (r.status_code, r.json()["error_code"]) == (404, "QUERY_NOT_FOUND")


def test_http_budget_rollback_rejected(client, seeded):
    r = client.put("/admin/budgets", json={
        "analyst_id": "a1", "dataset_id": "ds1", "budget_limit": 9.0})
    assert r.status_code == 409
    assert r.json()["error_code"] == "LEDGER_ROLLBACK_REJECTED"
    # 上限维持原值
    assert client.get("/analysts/a1/datasets/ds1/budget").json()["budget_limit"] == 10.0


def test_http_app_bootstraps_from_file(tmp_path: Path):
    db = tmp_path / "boot.db"
    app1 = create_app(db_path=str(db))
    c1 = TestClient(app1)
    _setup(c1)
    c1.post("/queries", json={"query_id": "q-boot", "analyst_id": "a1",
                              "items": [{"dataset_id": "ds1", "cost": 2.0}]})

    # “重启”：新建 app 指向同一文件，余额与结论必须都在
    app2 = create_app(db_path=str(db))
    c2 = TestClient(app2)
    b = c2.get("/analysts/a1/datasets/ds1/budget").json()
    assert (b["spent"], b["remaining"]) == (2.0, 3.0)
    r = c2.post("/queries", json={"query_id": "q-boot", "analyst_id": "a1",
                                  "items": [{"dataset_id": "ds1", "cost": 2.0}]})
    assert r.json()["idempotent_replay"] is True
    assert c2.get("/analysts/a1/datasets/ds1/budget").json()["spent"] == 2.0
