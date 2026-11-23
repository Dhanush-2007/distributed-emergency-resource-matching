"""Sequential end-to-end integration tests for Stage 7 Matching Service."""

from __future__ import annotations

import subprocess
import time
import uuid

import httpx
import pytest

REQ_BASE = "http://localhost:8001"
RES_BASE = "http://localhost:8002"
MATCH_BASE = "http://localhost:8003"

pytestmark = pytest.mark.integration


def _services_ready() -> bool:
    try:
        c = httpx.Client(timeout=2.0)
        return (
            c.get(f"{REQ_BASE}/health").status_code == 200
            and c.get(f"{RES_BASE}/health").status_code == 200
        )
    except Exception:  # noqa: BLE001
        return False


@pytest.fixture(scope="module")
def wait_for_services():
    if not _services_ready():
        pytest.skip("Services not ready")


def poll_request_status(client: httpx.Client, req_id: str, timeout: float = 12.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = client.get(f"{REQ_BASE}/requests/{req_id}")
        if r.status_code == 200:
            data = r.json()
            if data.get("status") in ("MATCHED", "NO_MATCH"):
                return data
        time.sleep(0.3)
    r = client.get(f"{REQ_BASE}/requests/{req_id}")
    return r.json() if r.status_code == 200 else {}


def test_sequential_matching_nearest(wait_for_services):
    client = httpx.Client(timeout=5.0)

    # 1. Register 2 water resources at different distances + 1 food
    tag = uuid.uuid4().hex[:6]
    r_near_resp = client.post(f"{RES_BASE}/resources", json={
        "name": f"near water {tag}", "type": "water", "location": {"lat": 12.972, "lon": 77.595}, "availability": True
    })
    assert r_near_resp.status_code == 201
    r_near = r_near_resp.json()

    r_far_resp = client.post(f"{RES_BASE}/resources", json={
        "name": f"far water {tag}", "type": "water", "location": {"lat": 13.050, "lon": 77.650}, "availability": True
    })
    assert r_far_resp.status_code == 201
    r_far = r_far_resp.json()

    r_food_resp = client.post(f"{RES_BASE}/resources", json={
        "name": f"close food {tag}", "type": "food", "location": {"lat": 12.9717, "lon": 77.5947}, "availability": True
    })
    assert r_food_resp.status_code == 201
    r_food = r_food_resp.json()

    # 2. Submit request for water
    corr_id = f"corr-{uuid.uuid4().hex[:8]}"
    req_resp = client.post(f"{REQ_BASE}/requests", json={
        "type": "water", "location": {"lat": 12.9716, "lon": 77.5946}, "details": f"need water {tag}"
    }, headers={"X-Correlation-Id": corr_id})
    assert req_resp.status_code == 201
    assert req_resp.json().get("matchQueued") is True
    req_id = req_resp.json()["requestId"]

    # 3. Wait for request to become MATCHED
    req_data = poll_request_status(client, req_id, timeout=12.0)
    assert req_data.get("status") == "MATCHED", f"Expected MATCHED, got: {req_data}"
    assert req_data.get("matchedResourceId") == r_near["resourceId"], "Should match nearest water resource"

    # 4. Verify resource statuses
    res_near = client.get(f"{RES_BASE}/resources/{r_near['resourceId']}").json()
    res_far = client.get(f"{RES_BASE}/resources/{r_far['resourceId']}").json()
    res_food = client.get(f"{RES_BASE}/resources/{r_food['resourceId']}").json()
    assert res_near["status"] == "ASSIGNED"
    assert res_near.get("assignedRequestId") == req_id
    assert res_far["status"] == "AVAILABLE"
    assert res_food["status"] == "AVAILABLE"

    # 5. Check logs for candidate table event and correlation ID
    deadline = time.time() + 15.0
    found_candidates = False
    found_corr = False
    last_logs = ""
    while time.time() < deadline:
        logs_res = subprocess.run(
            ["docker", "compose", "logs", "--no-color", "--since", "3m", "matching-service", "request-service"],
            check=False,
            capture_output=True,
            text=True,
        )
        last_logs = logs_res.stdout
        if "Candidate" in last_logs or "Candidates" in last_logs or "match.candidates" in last_logs or "distance" in last_logs.lower():
            found_candidates = True
        if corr_id in last_logs:
            found_corr = True
        if found_candidates and found_corr:
            break
        time.sleep(1.0)

    assert found_candidates, f"Candidate evaluation not found in logs: {last_logs[-500:]}"
    assert found_corr, f"Correlation ID {corr_id} not found in logs: {last_logs[-500:]}"


def test_no_match_and_retry(wait_for_services):
    client = httpx.Client(timeout=5.0)
    tag = uuid.uuid4().hex[:6]

    # 1. Request shelter (no shelter available)
    req_resp = client.post(f"{REQ_BASE}/requests", json={
        "type": "shelter", "location": {"lat": 12.9716, "lon": 77.5946}, "details": f"need shelter {tag}"
    })
    assert req_resp.status_code == 201
    req_id = req_resp.json()["requestId"]

    # 2. Poll until NO_MATCH
    req_data = poll_request_status(client, req_id, timeout=12.0)
    assert req_data.get("status") == "NO_MATCH", f"Expected NO_MATCH, got {req_data}"
    assert req_data.get("matchedResourceId") is None

    # 3. Register shelter resource
    res_resp = client.post(f"{RES_BASE}/resources", json={
        "name": f"shelter {tag}", "type": "shelter", "location": {"lat": 12.980, "lon": 77.600}, "availability": True
    })
    assert res_resp.status_code == 201
    res = res_resp.json()

    # 4. Trigger match retry via POST /match/{id}
    retry_resp = client.post(f"{REQ_BASE}/match/{req_id}")
    assert retry_resp.status_code == 200

    # 5. Poll until MATCHED
    req_data = poll_request_status(client, req_id, timeout=12.0)
    assert req_data.get("status") == "MATCHED"
    assert req_data.get("matchedResourceId") == res["resourceId"]


def test_duplicate_event_idempotency(wait_for_services):
    from tests.fault.stack import RabbitManagementClient

    client = httpx.Client(timeout=5.0)
    tag = uuid.uuid4().hex[:6]

    # Create resource and request
    res_resp = client.post(f"{RES_BASE}/resources", json={
        "name": f"meds {tag}", "type": "medicine", "location": {"lat": 12.9716, "lon": 77.5946}, "availability": True
    })
    assert res_resp.status_code == 201
    res = res_resp.json()

    req = client.post(f"{REQ_BASE}/requests", json={
        "type": "medicine", "location": {"lat": 12.9716, "lon": 77.5946}, "details": f"meds {tag}"
    }).json()

    req_data = poll_request_status(client, req["requestId"], timeout=12.0)
    assert req_data.get("status") == "MATCHED"
    assert req_data.get("matchedResourceId") == res["resourceId"]

    # Send duplicate match event directly via RabbitMQ
    mgmt = RabbitManagementClient()
    evt = {
        "eventId": str(uuid.uuid4()),
        "eventType": "request.created",
        "timestamp": "2026-11-20T00:00:00Z",
        "requestId": req["requestId"],
        "type": "medicine",
        "location": {"lat": 12.9716, "lon": 77.5946},
    }
    routed = mgmt.publish_message("emergency.events", "request.match", evt)
    assert routed is True

    time.sleep(1.0)
    req_data2 = client.get(f"{REQ_BASE}/requests/{req['requestId']}").json()
    assert req_data2.get("status") == "MATCHED"
    assert req_data2.get("matchedResourceId") == res["resourceId"]

    # Ensure resource was not double-assigned or corrupted
    res_data = client.get(f"{RES_BASE}/resources/{res['resourceId']}").json()
    assert res_data.get("status") == "ASSIGNED"
    assert res_data.get("assignedRequestId") == req["requestId"]
