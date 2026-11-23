"""Component tests for Request Service."""

from __future__ import annotations

import uuid

import httpx
import pytest
from ems_common.storage_client import CoordinatorClient
from fastapi.testclient import TestClient
from request_service.events import InMemoryEventPublisher
from request_service.main import create_app
from request_service.settings import Settings

from tests.component.fake_coordinator import FakeCoordinatorApp

pytestmark = pytest.mark.component


@pytest.fixture
def fake_coord():
    return FakeCoordinatorApp()


@pytest.fixture
def publisher():
    return InMemoryEventPublisher()


@pytest.fixture
def setup_env(fake_coord, publisher):
    # Mock transport for coordinator
    mock_coord_http = httpx.Client(transport=httpx.MockTransport(fake_coord.handle))
    coord_client = CoordinatorClient(
        base_url="http://test-coordinator",
        http_client=mock_coord_http,
    )

    # Track calls to resource service
    resource_service_calls: list[httpx.Request] = []

    def resource_handler(request: httpx.Request) -> httpx.Response:
        if "test-coordinator" in str(request.url):
            return fake_coord.handle(request)
        resource_service_calls.append(request)
        if "/internal/resources/" in str(request.url) and "/release" in str(request.url):
            return httpx.Response(200, json={"released": True})
        return httpx.Response(200, json={"status": "ok"})

    mock_outbound_http = httpx.Client(transport=httpx.MockTransport(resource_handler))

    settings = Settings(
        coordinator_url="http://test-coordinator",
        resource_service_url="http://test-resource-service",
        event_publisher="in_memory",
    )

    app = create_app(
        storage_client=coord_client,
        settings=settings,
        event_publisher=publisher,
        http_client=mock_outbound_http,
    )

    with TestClient(app) as tc:
        yield tc, fake_coord, publisher, resource_service_calls


def test_create_request(setup_env):
    tc, _, pub, _ = setup_env
    payload = {
        "type": "water",
        "location": {"lat": 12.9716, "lon": 77.5946},
        "details": "Need 50L drinking water",
    }
    resp = tc.post("/requests", json=payload)
    assert resp.status_code == 201
    assert "Location" in resp.headers
    data = resp.json()
    assert data["type"] == "water"
    assert data["status"] == "PENDING"
    assert data["matchAttemptCount"] == 0
    assert "requestId" in data
    assert "version" in data
    # eventPending should be stripped from public response
    assert "eventPending" not in data
    assert resp.headers["Location"] == f"/requests/{data['requestId']}"

    # Event publisher should have received match event
    assert len(pub.published) == 1
    assert pub.published[0]["requestId"] == data["requestId"]
    assert pub.published[0]["type"] == "water"


def test_create_request_validation_error(setup_env):
    tc, _, _, _ = setup_env
    # Invalid type
    r1 = tc.post("/requests", json={"type": "invalid", "location": {"lat": 0, "lon": 0}, "details": "help"})
    assert r1.status_code == 400
    assert r1.json()["error"]["code"] == "INVALID_INPUT"

    # Out-of-bounds coordinates
    r2 = tc.post("/requests", json={"type": "food", "location": {"lat": 120.0, "lon": 0}, "details": "help"})
    assert r2.status_code == 400
    assert r2.json()["error"]["code"] == "INVALID_INPUT"


def test_get_request(setup_env):
    tc, _, _, _ = setup_env
    req_id = tc.post(
        "/requests",
        json={"type": "food", "location": {"lat": 10.0, "lon": 20.0}, "details": "Rice"},
    ).json()["requestId"]

    r = tc.get(f"/requests/{req_id}")
    assert r.status_code == 200
    data = r.json()
    assert data["requestId"] == req_id
    assert "eventPending" not in data

    # 404 unknown
    assert tc.get(f"/requests/{uuid.uuid4()}").status_code == 404
    # 400 malformed
    assert tc.get("/requests/not-a-uuid").status_code == 400


def test_list_requests_and_filters(setup_env):
    tc, _, _, _ = setup_env
    tc.post("/requests", json={"type": "water", "location": {"lat": 1.0, "lon": 2.0}, "details": "D1"})
    tc.post("/requests", json={"type": "food", "location": {"lat": 1.0, "lon": 2.0}, "details": "D2"})

    # All
    r = tc.get("/requests")
    assert r.status_code == 200
    assert r.json()["count"] == 2
    assert r.json()["partial"] is False

    # Filter by type
    r_w = tc.get("/requests?type=water")
    assert r_w.status_code == 200
    assert r_w.json()["count"] == 1

    # Filter by status
    r_p = tc.get("/requests?status=PENDING")
    assert r_p.status_code == 200
    assert r_p.json()["count"] == 2

    # Invalid filters
    assert tc.get("/requests?type=bad").status_code == 400
    assert tc.get("/requests?status=BAD").status_code == 400


def test_status_transition_and_resource_release(setup_env):
    tc, _fake_coord, _, res_calls = setup_env
    # Create request and simulate it being MATCHED
    req_id = tc.post(
        "/requests",
        json={"type": "water", "location": {"lat": 1.0, "lon": 2.0}, "details": "D"},
    ).json()["requestId"]

    res_id = str(uuid.uuid4())
    # Match the request
    tc.post(
        f"/internal/requests/{req_id}/matchresult",
        json={"outcome": "MATCHED", "resourceId": res_id},
    )

    # Client transitions MATCHED -> IN_PROGRESS
    r_prog = tc.put(f"/requests/{req_id}/status", json={"status": "IN_PROGRESS"})
    assert r_prog.status_code == 200
    assert r_prog.json()["status"] == "IN_PROGRESS"
    # No release called yet
    assert len(res_calls) == 0

    # Client transitions IN_PROGRESS -> COMPLETED
    r_comp = tc.put(f"/requests/{req_id}/status", json={"status": "COMPLETED"})
    assert r_comp.status_code == 200
    assert r_comp.json()["status"] == "COMPLETED"
    # Resource release SHOULD be called
    assert len(res_calls) == 1
    assert f"/internal/resources/{res_id}/release" in str(res_calls[0].url)

    # Client cannot transition to PENDING (409)
    r_bad = tc.put(f"/requests/{req_id}/status", json={"status": "PENDING"})
    assert r_bad.status_code == 409
    assert r_bad.json()["error"]["code"] == "INVALID_TRANSITION"


def test_manual_match_trigger(setup_env):
    tc, _, pub, _ = setup_env
    req_id = tc.post(
        "/requests",
        json={"type": "water", "location": {"lat": 1.0, "lon": 2.0}, "details": "D"},
    ).json()["requestId"]
    pub.published.clear()

    # Manual match while PENDING
    r_pend = tc.post(f"/match/{req_id}")
    assert r_pend.status_code == 200
    assert r_pend.json()["status"] == "PENDING"
    assert r_pend.json()["matchQueued"] is True
    assert len(pub.published) == 1

    # Simulate NO_MATCH outcome
    tc.post(
        f"/internal/requests/{req_id}/matchresult",
        json={"outcome": "NO_MATCH"},
    )
    pub.published.clear()

    # Manual match while NO_MATCH -> transitions back to PENDING and publishes
    r_retry = tc.post(f"/match/{req_id}")
    assert r_retry.status_code == 200
    assert r_retry.json()["status"] == "PENDING"
    assert r_retry.json()["matchQueued"] is True
    assert len(pub.published) == 1

    # Match it now
    tc.post(
        f"/internal/requests/{req_id}/matchresult",
        json={"outcome": "MATCHED", "resourceId": str(uuid.uuid4())},
    )

    # Manual match while MATCHED -> 409
    r_409 = tc.post(f"/match/{req_id}")
    assert r_409.status_code == 409
    assert r_409.json()["error"]["code"] == "INVALID_TRANSITION"


def test_manual_match_pending_sets_event_pending_on_publish_failure(setup_env):
    tc, _, pub, _ = setup_env
    # 1. Create a request normally
    req_id = tc.post(
        "/requests",
        json={"type": "food", "location": {"lat": 12.0, "lon": 77.0}, "details": "Rice packets"},
    ).json()["requestId"]

    # Request is PENDING and initially eventPending is False (confirmed published)
    r_init = tc.get(f"/internal/requests/{req_id}")
    assert r_init.json()["status"] == "PENDING"
    assert r_init.json()["eventPending"] is False

    # 2. Simulate RabbitMQ downtime during manual match retry
    pub.publish = lambda evt: False

    # 3. Manually trigger match on already-PENDING request
    r_match = tc.post(f"/match/{req_id}")
    assert r_match.status_code == 200
    assert r_match.json()["status"] == "PENDING"
    assert r_match.json()["matchQueued"] is False

    # 4. Storage MUST record eventPending=True so outbox sweeper can recover it
    r_after = tc.get(f"/internal/requests/{req_id}")
    assert r_after.json()["status"] == "PENDING"
    assert r_after.json()["eventPending"] is True



def test_match_result_and_idempotency(setup_env):
    tc, _, _, _ = setup_env
    req_id = tc.post(
        "/requests",
        json={"type": "medicine", "location": {"lat": 1.0, "lon": 2.0}, "details": "Insulin"},
    ).json()["requestId"]
    res_id = str(uuid.uuid4())

    # Apply MATCHED
    r1 = tc.post(
        f"/internal/requests/{req_id}/matchresult",
        json={"outcome": "MATCHED", "resourceId": res_id, "expectedVersion": 1},
    )
    assert r1.status_code == 200
    data1 = r1.json()
    assert data1["status"] == "MATCHED"
    assert data1["matchedResourceId"] == res_id
    assert data1["matchAttemptCount"] == 1
    assert data1["eventPending"] is False

    # Idempotent re-application of same MATCHED result returns 200
    r_idem = tc.post(
        f"/internal/requests/{req_id}/matchresult",
        json={"outcome": "MATCHED", "resourceId": res_id},
    )
    assert r_idem.status_code == 200

    # Conflict with wrong resourceId or wrong status
    r_conf = tc.post(
        f"/internal/requests/{req_id}/matchresult",
        json={"outcome": "MATCHED", "resourceId": str(uuid.uuid4())},
    )
    assert r_conf.status_code == 409


def test_internal_request_endpoint(setup_env):
    tc, _, _, _ = setup_env
    req_id = tc.post(
        "/requests",
        json={"type": "transportation", "location": {"lat": 1.0, "lon": 2.0}, "details": "Van"},
    ).json()["requestId"]

    r = tc.get(f"/internal/requests/{req_id}")
    assert r.status_code == 200
    data = r.json()
    assert "eventPending" in data
    assert "version" in data


def test_coordinator_unavailable_and_health(setup_env):
    tc, fake_coord, _, _ = setup_env
    assert tc.get("/health").status_code == 200
    assert tc.get("/health/live").status_code == 200
    assert tc.get("/health/ready").status_code == 200

    # Coordinator down
    fake_coord.set_down(True)
    r_ready = tc.get("/health/ready")
    assert r_ready.status_code == 503
    assert r_ready.headers.get("Retry-After") == "3"

    r_req = tc.get("/requests")
    assert r_req.status_code == 503
    assert r_req.headers.get("Retry-After") == "3"
    assert r_req.json()["error"]["code"] == "STORAGE_UNAVAILABLE"

    fake_coord.set_down(False)
