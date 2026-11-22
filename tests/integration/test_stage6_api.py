"""Integration tests for Stage 6 Request and Resource Services against Compose stack."""

from __future__ import annotations

import subprocess
import time
import uuid

import httpx
import pytest

REQ_BASE = "http://localhost:8001"
RES_BASE = "http://localhost:8002"
COORD_BASE = "http://localhost:8000"


@pytest.fixture(scope="module")
def wait_for_services():
    """Ensure both Request Service and Resource Service are healthy before running tests."""
    client = httpx.Client(timeout=5.0)
    deadline = time.time() + 30.0
    while time.time() < deadline:
        try:
            r_req = client.get(f"{REQ_BASE}/health")
            r_res = client.get(f"{RES_BASE}/health")
            if r_req.status_code == 200 and r_res.status_code == 200:
                break
        except Exception:  # noqa: BLE001, S110
            pass
        time.sleep(1.0)
    else:
        pytest.fail("Request Service or Resource Service failed to become ready in time")


@pytest.mark.integration
def test_services_health(wait_for_services):
    client = httpx.Client(timeout=5.0)
    r1 = client.get(f"{REQ_BASE}/health")
    assert r1.status_code == 200
    assert r1.json()["service"] == "request-service"

    r2 = client.get(f"{RES_BASE}/health")
    assert r2.status_code == 200
    assert r2.json()["service"] == "resource-service"


@pytest.mark.integration
def test_full_request_lifecycle(wait_for_services):
    client = httpx.Client(timeout=5.0)

    # 1. Create water request
    payload = {
        "type": "water",
        "location": {"lat": 12.9716, "lon": 77.5946},
        "details": "Water needed at community center",
    }
    resp = client.post(f"{REQ_BASE}/requests", json=payload)
    assert resp.status_code == 201
    assert "Location" in resp.headers
    data = resp.json()
    req_id = data["requestId"]
    assert data["status"] == "PENDING"
    assert data["type"] == "water"
    assert data["matchQueued"] in (True, False)
    assert "eventPending" not in data

    # 2. Get single
    resp_get = client.get(f"{REQ_BASE}/requests/{req_id}")
    assert resp_get.status_code == 200
    assert resp_get.json()["requestId"] == req_id

    # 3. List requests
    resp_list = client.get(f"{REQ_BASE}/requests?type=water")
    assert resp_list.status_code == 200
    req_ids = [it["requestId"] for it in resp_list.json()["items"]]
    assert req_id in req_ids

    # 4. Manual /match trigger
    resp_match = client.post(f"{REQ_BASE}/match/{req_id}")
    assert resp_match.status_code == 200
    assert resp_match.json()["status"] in ("PENDING", "NO_MATCH", "MATCHED")
    assert resp_match.json()["matchQueued"] in (True, False)

    # 5. Transition PENDING -> CANCELLED
    resp_cancel = client.put(f"{REQ_BASE}/requests/{req_id}/status", json={"status": "CANCELLED"})
    assert resp_cancel.status_code == 200
    assert resp_cancel.json()["status"] == "CANCELLED"

    # 6. Try to transition CANCELLED -> IN_PROGRESS (409)
    resp_inv = client.put(f"{REQ_BASE}/requests/{req_id}/status", json={"status": "IN_PROGRESS"})
    assert resp_inv.status_code == 409
    assert resp_inv.json()["error"]["code"] == "INVALID_TRANSITION"


@pytest.mark.integration
def test_full_resource_lifecycle(wait_for_services):
    client = httpx.Client(timeout=5.0)

    # 1. Register water resource
    payload = {
        "name": "Water Tanker Alpha",
        "type": "water",
        "location": {"lat": 12.9500, "lon": 77.6000},
        "availability": True,
    }
    resp = client.post(f"{RES_BASE}/resources", json=payload)
    assert resp.status_code == 201
    assert "Location" in resp.headers
    res_data = resp.json()
    res_id = res_data["resourceId"]
    assert res_data["status"] == "AVAILABLE"
    assert res_data["availability"] is True

    # 2. List resources filtered by type=water
    resp_list = client.get(f"{RES_BASE}/resources?type=water")
    assert resp_list.status_code == 200
    res_ids = [it["resourceId"] for it in resp_list.json()["items"]]
    assert res_id in res_ids

    # 3. Get single
    resp_get = client.get(f"{RES_BASE}/resources/{res_id}")
    assert resp_get.status_code == 200
    assert resp_get.json()["resourceId"] == res_id

    # 4. Update status to OFFLINE
    resp_off = client.put(f"{RES_BASE}/resources/{res_id}/status", json={"status": "OFFLINE"})
    assert resp_off.status_code == 200
    assert resp_off.json()["status"] == "OFFLINE"
    assert resp_off.json()["availability"] is False

    # 5. Update back to AVAILABLE
    resp_avail = client.put(f"{RES_BASE}/resources/{res_id}/status", json={"status": "AVAILABLE"})
    assert resp_avail.status_code == 200
    assert resp_avail.json()["status"] == "AVAILABLE"
    assert resp_avail.json()["availability"] is True

    # 6. Try to update status to ASSIGNED (409)
    resp_bad = client.put(f"{RES_BASE}/resources/{res_id}/status", json={"status": "ASSIGNED"})
    assert resp_bad.status_code == 409
    assert resp_bad.json()["error"]["code"] == "INVALID_TRANSITION"


@pytest.mark.integration
def test_candidates_endpoint(wait_for_services):
    client = httpx.Client(timeout=5.0)

    # Create one water and one food resource
    r_w = client.post(
        f"{RES_BASE}/resources",
        json={"name": "Water Point", "type": "water", "location": {"lat": 12.0, "lon": 77.0}, "availability": True},
    ).json()["resourceId"]

    r_f = client.post(
        f"{RES_BASE}/resources",
        json={"name": "Food Pantry", "type": "food", "location": {"lat": 12.0, "lon": 77.0}, "availability": True},
    ).json()["resourceId"]

    # Query candidates for water
    resp_cand = client.get(f"{RES_BASE}/internal/resources/candidates?type=water")
    assert resp_cand.status_code == 200
    data = resp_cand.json()
    cand_ids = [c["resourceId"] for c in data["items"]]
    assert r_w in cand_ids
    assert r_f not in cand_ids


@pytest.mark.integration
def test_direct_reservation_and_release(wait_for_services):
    client = httpx.Client(timeout=5.0)

    res_id = client.post(
        f"{RES_BASE}/resources",
        json={"name": "Reserve Target", "type": "medicine", "location": {"lat": 12.0, "lon": 77.0}, "availability": True},
    ).json()["resourceId"]

    req1 = str(uuid.uuid4())
    req2 = str(uuid.uuid4())

    # Reserve resource for req1
    r_res = client.post(f"{RES_BASE}/internal/resources/{res_id}/reserve", json={"requestId": req1})
    assert r_res.status_code == 200
    assert r_res.json()["status"] == "ASSIGNED"
    assert r_res.json()["availability"] is False

    # Idempotent re-reserve returns 200
    r_idem = client.post(f"{RES_BASE}/internal/resources/{res_id}/reserve", json={"requestId": req1})
    assert r_idem.status_code == 200

    # Reserve same resource for req2 returns 409
    r_conf = client.post(f"{RES_BASE}/internal/resources/{res_id}/reserve", json={"requestId": req2})
    assert r_conf.status_code == 409
    assert r_conf.json()["error"]["code"] == "RESOURCE_ALREADY_ASSIGNED"

    # Release resource
    r_rel = client.post(f"{RES_BASE}/internal/resources/{res_id}/release", json={"requestId": req1})
    assert r_rel.status_code == 200
    assert r_rel.json()["released"] is True
    assert r_rel.json()["resource"]["status"] == "AVAILABLE"
    assert r_rel.json()["resource"]["availability"] is True


@pytest.mark.integration
def test_coordinator_fault_propagation(wait_for_services):
    client = httpx.Client(timeout=15.0)

    # Stop coordinator container to inject 503
    subprocess.run(["docker", "stop", "ems-coordinator"], check=True)
    try:
        # Both /requests and /resources should return 503 with Retry-After header
        r_req = client.get(f"{REQ_BASE}/requests")
        assert r_req.status_code == 503
        assert r_req.headers.get("Retry-After") == "3"
        assert r_req.json()["error"]["code"] == "STORAGE_UNAVAILABLE"
        assert "message" in r_req.json()["error"]
        assert "details" in r_req.json()["error"]

        r_res = client.get(f"{RES_BASE}/resources")
        assert r_res.status_code == 503
        assert r_res.headers.get("Retry-After") == "3"
        assert r_res.json()["error"]["code"] == "STORAGE_UNAVAILABLE"
        assert "message" in r_res.json()["error"]
        assert "details" in r_res.json()["error"]

        # /health/live 200 with /health/ready 503 on both services
        assert client.get(f"{REQ_BASE}/health/live").status_code == 200
        assert client.get(f"{REQ_BASE}/health/ready").status_code == 503
        assert client.get(f"{RES_BASE}/health/live").status_code == 200
        assert client.get(f"{RES_BASE}/health/ready").status_code == 503
    finally:
        # Restart coordinator and wait for recovery until shards are HEALTHY
        subprocess.run(["docker", "start", "ems-coordinator"], check=True)
        recovered = False
        for _ in range(60):
            time.sleep(0.5)
            try:
                r1 = client.get(f"{COORD_BASE}/health/live", timeout=2.0)
                r2 = client.get(f"{REQ_BASE}/health/ready", timeout=2.0)
                r3 = client.get(f"{RES_BASE}/health/ready", timeout=2.0)
                r_shards = client.get(f"{COORD_BASE}/diagnostics/shards", timeout=2.0)
                if (
                    r1.status_code == 200
                    and r2.status_code == 200
                    and r3.status_code == 200
                    and r_shards.status_code == 200
                    and all(s.get("state") == "HEALTHY" for s in r_shards.json())
                ):
                    recovered = True
                    break
            except Exception:  # noqa: BLE001, S110
                pass
        assert recovered, "Coordinator or services failed to recover after restart"
