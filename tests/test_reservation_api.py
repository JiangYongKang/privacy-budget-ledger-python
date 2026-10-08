"""预占相关 HTTP 接口端到端：状态码、错误信封、幂等响应、余额口径。"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from ledger.api import create_app
from ledger.service import BudgetService
from ledger.store import LedgerStore
from tests.conftest import log_case


class FakeClock:
    def __init__(self, start: float = 10_000.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def client(clock):
    store = LedgerStore(":memory:", clock=clock).open()
    svc = BudgetService(store, default_ttl_seconds=60)
    app = create_app(svc)
    return TestClient(app), clock


def test_reserve_settle_release_flow_over_http(client):
    c, _clock = client
    c.post("/analysts", json={"id": "a1"})
    c.post("/datasets", json={"id": "d1"})
    c.put("/analysts/a1/datasets/d1/grant", json={"limit": 10})

    body = {
        "reservation_id": "h1",
        "analyst_id": "a1",
        "charges": [{"dataset_id": "d1", "amount": 6}],
        "ttl_seconds": 60,
    }
    r1 = c.post("/reservations", json=body)
    assert r1.status_code == 201, r1.text
    assert r1.json()["status"] == "held"
    assert r1.json()["total_reserved"] == 6.0

    r2 = c.post("/reservations", json=body)
    assert r2.status_code in (200, 201) and r2.json()["status"] == "idempotent_replay"

    bal = c.get("/analysts/a1/datasets/d1/balance").json()
    assert bal["reserved"] == 6.0 and bal["available"] == 4.0

    st = c.post(
        "/reservations/settle",
        json={
            "reservation_id": "h1", "analyst_id": "a1",
            "charges": [{"dataset_id": "d1", "amount": 4}],
        },
    )
    assert st.status_code == 200
    assert st.json()["status"] == "settled"
    assert st.json()["total_charged"] == 4.0
    assert st.json()["total_refunded"] == 2.0

    view = c.get("/reservations/h1")
    assert view.status_code == 200 and view.json()["status"] == "settled"
    log_case(
        "HTTP预占-结算闭环",
        reservation_id="h1",
        预占=6, 实扣=4, 退回=2,
        判定依据="/reservations + /settle,重复预占idempotent_replay",
    )


def test_release_and_expiry_over_http(client):
    c, clock = client
    c.post("/analysts", json={"id": "a1"})
    c.post("/datasets", json={"id": "d1"})
    c.put("/analysts/a1/datasets/d1/grant", json={"limit": 10})

    c.post(
        "/reservations",
        json={
            "reservation_id": "h2", "analyst_id": "a1",
            "charges": [{"dataset_id": "d1", "amount": 7}],
            "ttl_seconds": 10,
        },
    )
    rel1 = c.post(
        "/reservations/release",
        json={"reservation_id": "h2", "analyst_id": "a1"},
    )
    assert rel1.status_code == 200 and rel1.json()["status"] == "released"
    rel2 = c.post(
        "/reservations/release",
        json={"reservation_id": "h2", "analyst_id": "a1"},
    )
    assert rel2.json()["status"] == "idempotent_replay"
    assert rel2.json()["total_refunded"] == 7.0

    # 另一个预占走到过期
    c.post(
        "/reservations",
        json={
            "reservation_id": "h3", "analyst_id": "a1",
            "charges": [{"dataset_id": "d1", "amount": 8}],
            "ttl_seconds": 10,
        },
    )
    clock.advance(11)
    sweep = c.post("/reservations/sweep-expired")
    assert sweep.json()["expired"] == ["h3"]

    settle = c.post(
        "/reservations/settle",
        json={
            "reservation_id": "h3", "analyst_id": "a1",
            "charges": [{"dataset_id": "d1", "amount": 1}],
        },
    )
    assert settle.status_code == 409
    assert settle.json()["error"]["code"] == "reservation_expired"
    log_case(
        "HTTP归还幂等+过期拒绝结算",
        reservation_id="h2/h3",
        退回=7, 过期结算码=settle.json()["error"]["code"],
        判定依据="released幂等,expired->409 reservation_expired",
    )


@pytest.mark.parametrize(
    "body,expected_code",
    [
        (  # 空 charges
            {"reservation_id": "x", "analyst_id": "a1", "charges": []},
            "invalid_parameter",
        ),
        (  # amount 非正
            {"reservation_id": "x", "analyst_id": "a1",
             "charges": [{"dataset_id": "d1", "amount": 0}]},
            "invalid_parameter",
        ),
        (  # ttl 非正
            {"reservation_id": "x", "analyst_id": "a1", "ttl_seconds": 0,
             "charges": [{"dataset_id": "d1", "amount": 1}]},
            "invalid_parameter",
        ),
    ],
)
def test_reserve_invalid_body_envelope(client, body, expected_code):
    c, _ = client
    c.post("/analysts", json={"id": "a1"})
    c.post("/datasets", json={"id": "d1"})
    c.put("/analysts/a1/datasets/d1/grant", json={"limit": 10})
    r = c.post("/reservations", json=body)
    assert r.status_code == 422
    assert r.json()["error"]["code"] == expected_code
    # 任何非法请求都不得占用额度
    assert c.get("/analysts/a1/datasets/d1/balance").json()["reserved"] == 0.0
