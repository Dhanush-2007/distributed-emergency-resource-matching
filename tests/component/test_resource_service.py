"""Component tests for Resource Service."""

from __future__ import annotations

import concurrent.futures
import uuid

import httpx
import pytest
from ems_common.storage_client import CoordinatorClient
from fastapi.testclient import TestClient
from resource_service.main import create_app
from resource_service.settings import Settings

from tests.component.fake_coordinator import FakeCoordinatorApp

pytestmark = pytest.mark.component


@pytest.fixture
def fake_coord():
    return FakeCoordinatorApp()


@pytest.fixture
def client(fake_coord):
    mock_http = httpx.Client(transport=httpx.MockTransport(fake_coord.handle))
    coord_client = CoordinatorClient(
        base_url="http://test-coordinator",
        http_client=mock_http,
    )
    settings = Settings(
        coordinator_url="http://test-coordinator",
        request_service_url="http://test-request-service",
        reconcile_enabled=False,
    )
    app = create_app(storage_client=coord_client, settings=settings)
    with TestClient(app) as tc:
        yield tc, fake_coord


def test_create_resource(client):
    tc, _ = client
    payload = {
        "name": "Central Water Tank",
        "type": "water",
        "location": {"lat": 12.9716, "lon": 77.5946},
        "availability": True,
    }
    resp = tc.post("/resources", json=payload)
    assert resp.status_code == 201
    assert "Location" in resp.headers
    data = resp.json()
    assert data["name"] == "Central Water Tank"
    assert data["type"] == "water"
    assert data["status"] == "AVAILABLE"
    assert data["availability"] is True
    assert data["version"] == 1
    assert "resourceId" in data
    assert resp.headers["Location"] == f"/resources/{data['resourceId']}"


def test_create_resource_unavailable(client):
    tc, _ = client
    payload = {
        "name": "Offline Shelter",
        "type": "shelter",
        "location": {"lat": 12.0, "lon": 77.0},
        "availability": False,
    }
    resp = tc.post("/resources", json=payload)
    assert resp.status_code == 201
    data = resp.json()
    assert data["status"] == "OFFLINE"
    assert data["availability"] is False


def test_get_resource(client):
    tc, _ = client
    payload = {
        "name": "Ambulance 1",
        "type": "medicine",
        "location": {"lat": 13.0, "lon": 77.5},
        "availability": True,
    }
    res_id = tc.post("/resources", json=payload).json()["resourceId"]

    resp = tc.get(f"/resources/{res_id}")
    assert resp.status_code == 200
    assert resp.json()["resourceId"] == res_id

    # Unknown
    unknown_id = str(uuid.uuid4())
    resp404 = tc.get(f"/resources/{unknown_id}")
    assert resp404.status_code == 404
    assert resp404.json()["error"]["code"] == "NOT_FOUND"

    # Malformed
    resp400 = tc.get("/resources/bad-uuid")
    assert resp400.status_code == 400
    assert resp400.json()["error"]["code"] == "INVALID_INPUT"


def test_list_resources_and_filters(client):
    tc, _ = client
    tc.post("/resources", json={"name": "W1", "type": "water", "location": {"lat": 10.0, "lon": 20.0}, "availability": True})
    tc.post("/resources", json={"name": "W2", "type": "water", "location": {"lat": 10.0, "lon": 20.0}, "availability": False})
    tc.post("/resources", json={"name": "F1", "type": "food", "location": {"lat": 10.0, "lon": 20.0}, "availability": True})

    # List all
    r = tc.get("/resources")
    assert r.status_code == 200
    assert r.json()["count"] == 3

    # Filter by type
    r_w = tc.get("/resources?type=water")
    assert r_w.status_code == 200
    assert r_w.json()["count"] == 2

    # Filter by status
    r_off = tc.get("/resources?status=OFFLINE")
    assert r_off.status_code == 200
    assert r_off.json()["count"] == 1

    # Filter by availability
    r_av = tc.get("/resources?availability=true")
    assert r_av.status_code == 200
    assert r_av.json()["count"] == 2

    # Invalid filters
    assert tc.get("/resources?type=unknown").status_code == 400
    assert tc.get("/resources?status=BAD").status_code == 400
    assert tc.get("/resources?availability=maybe").status_code == 400


def test_status_update_and_fencing(client):
    tc, _ = client
    res_id = tc.post("/resources", json={"name": "T1", "type": "transportation", "location": {"lat": 1.0, "lon": 2.0}, "availability": True}).json()["resourceId"]

    # Available -> Offline
    r1 = tc.put(f"/resources/{res_id}/status", json={"status": "OFFLINE"})
    assert r1.status_code == 200
    assert r1.json()["status"] == "OFFLINE"
    assert r1.json()["availability"] is False

    # Offline -> Available
    r2 = tc.put(f"/resources/{res_id}/status", json={"status": "AVAILABLE"})
    assert r2.status_code == 200
    assert r2.json()["status"] == "AVAILABLE"
    assert r2.json()["availability"] is True

    # Client cannot set ASSIGNED
    r_bad = tc.put(f"/resources/{res_id}/status", json={"status": "ASSIGNED"})
    assert r_bad.status_code == 409
    assert r_bad.json()["error"]["code"] == "INVALID_TRANSITION"


def test_candidates(client):
    tc, _ = client
    tc.post("/resources", json={"name": "W1", "type": "water", "location": {"lat": 10.0, "lon": 20.0}, "availability": True})
    tc.post("/resources", json={"name": "W2", "type": "water", "location": {"lat": 10.0, "lon": 20.0}, "availability": False})
    tc.post("/resources", json={"name": "F1", "type": "food", "location": {"lat": 10.0, "lon": 20.0}, "availability": True})

    res = tc.get("/internal/resources/candidates?type=water")
    assert res.status_code == 200
    data = res.json()
    assert data["count"] == 1
    assert data["items"][0]["name"] == "W1"
    assert data["partial"] is False

    # Missing type param
    assert tc.get("/internal/resources/candidates").status_code == 400


def test_reserve_and_release(client):
    tc, _ = client
    res_id = tc.post("/resources", json={"name": "R1", "type": "water", "location": {"lat": 1.0, "lon": 2.0}, "availability": True}).json()["resourceId"]
    req_id = str(uuid.uuid4())

    # Reserve
    r_res = tc.post(f"/internal/resources/{res_id}/reserve", json={"requestId": req_id, "expectedVersion": 1})
    assert r_res.status_code == 200
    assert r_res.json()["status"] == "ASSIGNED"
    assert r_res.json()["availability"] is False
    assert r_res.json()["assignedRequestId"] == req_id

    # Idempotent reserve with same requestId
    r_idem = tc.post(f"/internal/resources/{res_id}/reserve", json={"requestId": req_id})
    assert r_idem.status_code == 200

    # Conflict with different requestId
    other_req = str(uuid.uuid4())
    r_conf = tc.post(f"/internal/resources/{res_id}/reserve", json={"requestId": other_req})
    assert r_conf.status_code == 409
    assert r_conf.json()["error"]["code"] == "RESOURCE_ALREADY_ASSIGNED"

    # By-request lookup
    r_by = tc.get(f"/internal/resources/by-request/{req_id}")
    assert r_by.status_code == 200
    assert r_by.json()["resourceId"] == res_id

    # Release by owner
    r_rel = tc.post(f"/internal/resources/{res_id}/release", json={"requestId": req_id})
    assert r_rel.status_code == 200
    assert r_rel.json()["released"] is True
    assert r_rel.json()["resource"]["status"] == "AVAILABLE"
    assert r_rel.json()["resource"]["availability"] is True

    # Release again is idempotent no-op (released=False)
    r_rel2 = tc.post(f"/internal/resources/{res_id}/release", json={"requestId": req_id})
    assert r_rel2.status_code == 200
    assert r_rel2.json()["released"] is False

    # By-request now 404
    assert tc.get(f"/internal/resources/by-request/{req_id}").status_code == 404


def test_concurrent_reserve(client):
    tc, _ = client
    res_id = tc.post("/resources", json={"name": "Shared", "type": "medicine", "location": {"lat": 1.0, "lon": 2.0}, "availability": True}).json()["resourceId"]

    req1 = str(uuid.uuid4())
    req2 = str(uuid.uuid4())

    def do_reserve(req):
        return tc.post(f"/internal/resources/{res_id}/reserve", json={"requestId": req})

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
        f1 = ex.submit(do_reserve, req1)
        f2 = ex.submit(do_reserve, req2)
        results = [f1.result(), f2.result()]

    statuses = [r.status_code for r in results]
    assert 200 in statuses
    assert 409 in statuses


def test_coordinator_unavailable(client):
    tc, fake_coord = client
    fake_coord.set_down(True)

    r = tc.get("/resources")
    assert r.status_code == 503
    assert r.headers.get("Retry-After") == "3"
    assert r.json()["error"]["code"] == "STORAGE_UNAVAILABLE"

    fake_coord.set_down(False)


def test_reconciler_heals_stale_assignment(fake_coord):
    mock_http = httpx.Client(transport=httpx.MockTransport(fake_coord.handle))
    coord_client = CoordinatorClient(
        base_url="http://test-coordinator",
        http_client=mock_http,
    )

    req_id = str(uuid.uuid4())
    res_id = str(uuid.uuid4())

    # Create assigned resource directly in fake_coord
    fake_coord._storage[f"resource:{res_id}"] = {
        "value": {
            "resourceId": res_id,
            "name": "Stale Res",
            "type": "water",
            "status": "ASSIGNED",
            "availability": False,
            "assignedRequestId": req_id,
            "createdAt": "2026-01-01T00:00:00Z",
            "updatedAt": "2026-01-01T00:00:00Z",
        },
        "version": 1,
    }

    # Mock request service transport that returns 404
    def mock_req_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"error": {"code": "NOT_FOUND", "message": "not found", "details": []}})

    req_http = httpx.Client(transport=httpx.MockTransport(mock_req_handler))
    from resource_service.reconciler import Reconciler

    rec = Reconciler(
        storage_client=coord_client,
        request_service_url="http://mock-requests",
        grace_sec=0.0,
        interval_sec=60.0,
        http_client=req_http,
    )

    rec.run_once()

    # Resource should be released
    val, _ver = coord_client.get(f"resource:{res_id}")
    assert val["status"] == "AVAILABLE"
    assert val["availability"] is True
    assert val["assignedRequestId"] is None

    # Test error handling: if request service throws error, do not release
    # Now assign it again to req2
    req_id2 = str(uuid.uuid4())
    fake_coord._storage[f"resource:{res_id}"] = {
        "value": {
            "resourceId": res_id,
            "name": "Stale Res",
            "type": "water",
            "status": "ASSIGNED",
            "availability": False,
            "assignedRequestId": req_id2,
            "createdAt": "2026-01-01T00:00:00Z",
            "updatedAt": "2026-01-01T00:00:00Z",
        },
        "version": 2,
    }

    def failing_handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("Connection refused")

    rec_fail = Reconciler(
        storage_client=coord_client,
        request_service_url="http://mock-requests",
        grace_sec=0.0,
        interval_sec=60.0,
        http_client=httpx.Client(transport=httpx.MockTransport(failing_handler)),
    )
    rec_fail.run_once()

    # Still ASSIGNED
    val2, _ = coord_client.get(f"resource:{res_id}")
    assert val2["status"] == "ASSIGNED"
    assert val2["assignedRequestId"] == req_id2
