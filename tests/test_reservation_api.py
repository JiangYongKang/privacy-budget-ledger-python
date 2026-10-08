"""预占相关 HTTP 端点端到端：状态码、统一错误信封、幂等结论一致。"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from ledger.api import create_app
from ledger.service import BudgetService
from ledger.store import LedgerStore


class FakeClock:
    def __init__(self, t: float) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t


@pytest.fixture
def client():
    clock = FakeClock(1000.0)
    svc = BudgetService(LedgerStore(":memory:", clock=clock).open(),
                        default_ttl_seconds=100.0, clock=clock)
    c = TestClient(create_app(svc))
    c.post("/analysts", json={"id": "a1"})
    c.post("/datasets", json={"id": "d1"})
    c.post("/datasets", json={"id": "d2"})
    c.put("/analysts/a1/datasets/d1/grant", json={"limit": 10})
    c.put("/analysts/a1/datasets/d2/grant", json={"limit": 5})
    c.clock = clock
    return c


def test_reserve_settle_release_flow_over_http(client):
    r = client.post("/reservations", json={
        "reservation_id": "rv1", "analyst_id": "a1",
        "items": [{"dataset_id": "d1", "amount": 6}, {"dataset_id": "d2", "amount": 4}],
        "ttl_seconds": 50,
    })
    assert r.status_code == 201
    body = r.json()
    assert body["status"] == "accepted" and body["expires_at"]
    assert {(i["dataset_id"], i["reserved"], i["balance_after"]) for i in body["items"]} == {
        ("d1", 6.0, 4.0), ("d2", 4.0, 1.0),
    }

    # 余额视图同时含 consumed/reserved/available
    bal = client.get("/analysts/a1/datasets/d1/balance").json()
    assert bal["reserved"] == 6.0 and bal["available"] == 4.0

    # 重复提交同样内容 -> 幂等，expires_at 与首次一致
    r2 = client.post("/reservations", json={
        "reservation_id": "rv1", "analyst_id": "a1",
        "items": [{"dataset_id": "d1", "amount": 6}, {"dataset_id": "d2", "amount": 4}],
        "ttl_seconds": 999,
    })
    assert r2.json()["status"] == "idempotent_replay"
    assert r2.json()["expires_at"] == body["expires_at"]

    s = client.post("/reservations/rv1/settle", json={
        "analyst_id": "a1",
        "actual": [{"dataset_id": "d1", "amount": 5}, {"dataset_id": "d2", "amount": 0}],
    })
    assert s.status_code == 200
    sb = s.json()
    assert sb["total_settled"] == 5.0 and sb["total_refunded"] == 5.0

    g = client.get("/reservations/rv1")
    assert g.status_code == 200 and g.json()["status"] == "settled"


def test_release_and_replay_over_http(client):
    client.post("/reservations", json={
        "reservation_id": "rv2", "analyst_id": "a1",
        "items": [{"dataset_id": "d1", "amount": 3}],
    })
    r1 = client.post("/reservations/rv2/release", json={"analyst_id": "a1"})
    r2 = client.post("/reservations/rv2/release", json={"analyst_id": "a1"})
    assert r1.json()["status"] == "released"
    assert r2.json()["status"] == "idempotent_replay"
    assert r1.json()["total_refunded"] == r2.json()["total_refunded"] == 3.0


def test_expired_settle_is_410(client):
    client.post("/reservations", json={
        "reservation_id": "rv3", "analyst_id": "a1",
        "items": [{"dataset_id": "d1", "amount": 2}], "ttl_seconds": 5,
    })
    client.clock.t = 1006.0
    r = client.post("/reservations/rv3/settle", json={
        "analyst_id": "a1", "actual": [{"dataset_id": "d1", "amount": 1}],
    })
    assert r.status_code == 410
    err = r.json()["error"]
    assert err["code"] == "reservation_expired"
    assert "expires_at" in err["details"]
    # 过期后额度已退回
    bal = client.get("/analysts/a1/datasets/d1/balance").json()
    assert bal["reserved"] == 0.0 and bal["available"] == 10.0


@pytest.mark.parametrize("path,body,expected_code", [
    ("/reservations", {"reservation_id": "r", "analyst_id": "a1",
                       "items": [{"dataset_id": "d1", "amount": 1.5}]}, 201),
    # amount 必须为正
    ("/reservations", {"reservation_id": "r", "analyst_id": "a1",
                       "items": [{"dataset_id": "d1", "amount": 0}]}, 422),
    # 空 items
    ("/reservations", {"reservation_id": "r", "analyst_id": "a1", "items": []}, 422),
    # 未授权数据集
    ("/reservations", {"reservation_id": "r", "analyst_id": "a1",
                       "items": [{"dataset_id": "ghost", "amount": 1}]}, 404),
    # 越权
    ("/reservations", {"reservation_id": "r", "analyst_id": "a1",
                       "items": [{"dataset_id": "d1", "amount": 11}]}, 409),
    # ttl 必须为正
    ("/reservations", {"reservation_id": "r2", "analyst_id": "a1",
                       "items": [{"dataset_id": "d1", "amount": 1}], "ttl_seconds": -1}, 422),
])
def test_reserve_http_status_codes(client, path, body, expected_code):
    r = client.post(path, json=body)
    assert r.status_code == expected_code, r.text


def test_settle_overreserve_is_422_and_no_movement(client):
    client.post("/reservations", json={
        "reservation_id": "rv4", "analyst_id": "a1",
        "items": [{"dataset_id": "d1", "amount": 2}],
    })
    r = client.post("/reservations/rv4/settle", json={
        "analyst_id": "a1", "actual": [{"dataset_id": "d1", "amount": 2.5}],
    })
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "invalid_parameter"
    bal = client.get("/analysts/a1/datasets/d1/balance").json()
    assert bal["consumed"] == 0.0 and bal["reserved"] == 2.0


def test_get_unknown_reservation_404(client):
    r = client.get("/reservations/ghost")
    assert r.status_code == 404 and r.json()["error"]["code"] == "reservation_not_found"
