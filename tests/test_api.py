"""HTTP 层：端到端可区分错误码 + 幂等响应。"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from ledger.api import create_app
from ledger.service import build_default_service


@pytest.fixture
def client():
    svc = build_default_service(":memory:")
    with TestClient(create_app(svc)) as c:
        yield c


def _seed(client: TestClient) -> None:
    client.post("/analysts", json={"id": "a1"})
    client.post("/datasets", json={"id": "d1"})
    client.post("/datasets", json={"id": "d2"})
    client.put("/analysts/a1/datasets/d1/grant", json={"limit": 10})
    client.put("/analysts/a1/datasets/d2/grant", json={"limit": 5})


def test_consume_and_idempotent_replay_over_http(client):
    _seed(client)
    body = {
        "query_id": "http-q1",
        "analyst_id": "a1",
        "charges": [{"dataset_id": "d1", "cost": 3}, {"dataset_id": "d2", "cost": 2}],
    }
    r1 = client.post("/consume", json=body)
    r2 = client.post("/consume", json=body)
    assert r1.status_code == 200 and r1.json()["status"] == "accepted"
    assert r2.status_code == 200 and r2.json()["status"] == "idempotent_replay"
    assert r1.json()["items"] == r2.json()["items"]

    bal = client.get("/analysts/a1/datasets/d1/balance").json()
    assert bal == {"analyst_id": "a1", "dataset_id": "d1",
                   "limit": 10.0, "consumed": 3.0, "balance": 7.0}
    rec = client.get("/queries/http-q1").json()
    assert rec["total_charged"] == 5.0


@pytest.mark.parametrize(
    "body,expected_code",
    [
        ({"query_id": "q", "analyst_id": "ghost",
          "charges": [{"dataset_id": "d1", "cost": 1}]}, "analyst_not_found"),
        ({"query_id": "q", "analyst_id": "a1",
          "charges": [{"dataset_id": "ghost", "cost": 1}]}, "dataset_not_found"),
        ({"query_id": "q", "analyst_id": "a1",
          "charges": [{"dataset_id": "d1", "cost": 99}]}, "budget_insufficient"),
        ({"query_id": "q", "analyst_id": "a1",
          "charges": [{"dataset_id": "d1", "cost": 0}]}, "invalid_parameter"),
    ],
)
def test_error_codes_over_http(client, body, expected_code):
    _seed(client)
    r = client.post("/consume", json=body)
    assert r.status_code >= 400
    assert r.json()["error"]["code"] == expected_code, (r.status_code, r.text)


def test_unauthorized_over_http(client):
    _seed(client)
    client.post("/analysts", json={"id": "a9"})
    r = client.post("/consume", json={
        "query_id": "q", "analyst_id": "a9",
        "charges": [{"dataset_id": "d1", "cost": 1}]})
    assert r.status_code == 403 and r.json()["error"]["code"] == "unauthorized_dataset"


def test_query_conflict_over_http(client):
    _seed(client)
    body1 = {"query_id": "qc", "analyst_id": "a1",
             "charges": [{"dataset_id": "d1", "cost": 1}]}
    assert client.post("/consume", json=body1).status_code == 200
    body2 = {"query_id": "qc", "analyst_id": "a1",
             "charges": [{"dataset_id": "d1", "cost": 2}]}
    r = client.post("/consume", json=body2)
    assert r.status_code == 409 and r.json()["error"]["code"] == "query_conflict"


def test_validation_error_uses_envelope(client):
    _seed(client)
    r = client.post("/consume", json={
        "query_id": "q", "analyst_id": "a1",
        "charges": [{"dataset_id": "d1", "cost": -1}]})
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "invalid_parameter"
