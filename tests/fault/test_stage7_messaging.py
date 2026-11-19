"""Fault-injection tests for Stage 7 messaging, broker outages, worker crashes and DLQ."""

from __future__ import annotations

import os
import subprocess
import time
import uuid

import httpx
import pytest

from tests.fault.stack import RabbitManagementClient, wait_until

REQ_BASE = "http://localhost:8001"
RES_BASE = "http://localhost:8002"
MGMT_URL = "http://localhost:15672"

def _is_cluster_available() -> bool:
    try:
        r = httpx.get("http://localhost:8000/health/live", timeout=2.0)
        return r.status_code == 200
    except (httpx.HTTPError, OSError):
        return False


pytestmark = [
    pytest.mark.fault,
    pytest.mark.skipif(
        not _is_cluster_available(),
        reason="Fault tests require a running Docker Compose cluster.",
    ),
]


def _poll_request(client: httpx.Client, req_id: str, timeout: float = 25.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = client.get(f"{REQ_BASE}/requests/{req_id}")
        if r.status_code == 200:
            data = r.json()
            if data.get("status") in ("MATCHED", "NO_MATCH"):
                return data
        time.sleep(0.5)
    r = client.get(f"{REQ_BASE}/requests/{req_id}")
    return r.json() if r.status_code == 200 else {}


def test_broker_outage_and_sweeper_recovery():
    """Stopping RabbitMQ during request creation leaves eventPending=true; restarting recovers."""
    client = httpx.Client(timeout=10.0)
    tag = uuid.uuid4().hex[:6]

    # Register resource beforehand
    res_resp = client.post(f"{RES_BASE}/resources", json={
        "name": f"water-outage-{tag}", "type": "water", "location": {"lat": 12.9716, "lon": 77.5946}, "availability": True
    })
    assert res_resp.status_code == 201

    # 1. Stop RabbitMQ
    subprocess.run(["docker", "compose", "stop", "rabbitmq"], check=True, capture_output=True)

    try:
        # 2. Create request while broker is down
        req_resp = client.post(f"{REQ_BASE}/requests", json={
            "type": "water", "location": {"lat": 12.9716, "lon": 77.5946}, "details": f"outage water {tag}"
        })
        assert req_resp.status_code == 201
        data = req_resp.json()
        assert data.get("matchQueued") is False
        req_id = data["requestId"]

        # Request is stored as PENDING
        req_data = client.get(f"{REQ_BASE}/requests/{req_id}").json()
        assert req_data["status"] == "PENDING"
    finally:
        # 3. Restart RabbitMQ
        subprocess.run(["docker", "compose", "start", "rabbitmq"], check=True, capture_output=True)
        # Wait for rabbitmq to become healthy
        assert wait_until(lambda: httpx.get(f"{MGMT_URL}/api/overview", auth=("ems", "ems-dev-password"), timeout=2.0).status_code == 200, timeout=30.0)

    # 4. Wait for sweeper to pick up the pending request or trigger match
    matched_data = _poll_request(client, req_id, timeout=30.0)
    assert matched_data.get("status") == "MATCHED"


def test_worker_crash_redelivery():
    """A worker killed while processing an unacked message allows redelivery to a new worker."""
    client = httpx.Client(timeout=10.0)
    tag = uuid.uuid4().hex[:6]

    # Register resource
    res = client.post(f"{RES_BASE}/resources", json={
        "name": f"worker-crash-{tag}", "type": "transportation", "location": {"lat": 12.9716, "lon": 77.5946}, "availability": True
    }).json()

    # Restart matching-service with 5-second delay
    env = dict(os.environ, FAULT_MATCH_DELAY_MS="5000", FAULT_MATCH_FAIL="off")
    subprocess.run(["docker", "compose", "up", "-d", "matching-service"], env=env, check=True, capture_output=True)
    time.sleep(2.0)

    # Submit request
    req = client.post(f"{REQ_BASE}/requests", json={
        "type": "transportation", "location": {"lat": 12.9716, "lon": 77.5946}, "details": f"crash test {tag}"
    }).json()
    req_id = req["requestId"]

    # Kill matching worker while processing
    time.sleep(1.0)
    subprocess.run(["docker", "compose", "kill", "matching-service"], check=True, capture_output=True)

    # Restart matching worker without delay
    env = dict(os.environ, FAULT_MATCH_DELAY_MS="0", FAULT_MATCH_FAIL="off")
    subprocess.run(["docker", "compose", "up", "-d", "matching-service"], env=env, check=True, capture_output=True)

    # Poll until matched
    matched_data = _poll_request(client, req_id, timeout=20.0)
    assert matched_data.get("status") == "MATCHED"
    assert matched_data.get("matchedResourceId") == res["resourceId"]


def test_injected_failure_to_dlq():
    """Worker with FAULT_MATCH_FAIL=always retries 3 times and dead-letters to request.match.dlq."""
    mgmt = RabbitManagementClient()
    mgmt.purge_queue("request.match.dlq")

    # Restart matching-service with FAULT_MATCH_FAIL=always
    env = dict(os.environ, FAULT_MATCH_FAIL="always", FAULT_MATCH_DELAY_MS="0")
    subprocess.run(["docker", "compose", "up", "-d", "matching-service"], env=env, check=True, capture_output=True)
    time.sleep(2.0)

    client = httpx.Client(timeout=10.0)
    tag = uuid.uuid4().hex[:6]

    # Create food request
    client.post(f"{REQ_BASE}/requests", json={
        "type": "food", "location": {"lat": 12.9716, "lon": 77.5946}, "details": f"dlq test {tag}"
    })

    try:
        # Wait for message to move through retry queue (5s TTL * 3 retries = ~15s) to DLQ
        def check_dlq() -> bool:
            return mgmt.queue_depth("request.match.dlq") >= 1

        assert wait_until(check_dlq, timeout=30.0), f"DLQ depth is {mgmt.queue_depth('request.match.dlq')}, expected >= 1"
        assert mgmt.queue_depth("request.match.dlq") == 1
    finally:
        # Restore normal matching-service
        env = dict(os.environ, FAULT_MATCH_FAIL="off", FAULT_MATCH_DELAY_MS="0")
        subprocess.run(["docker", "compose", "up", "-d", "matching-service"], env=env, check=True, capture_output=True)
        mgmt.purge_queue("request.match.dlq")


def test_drained_queues():
    """Assert all queues are drained after processing completes."""
    mgmt = RabbitManagementClient()
    assert wait_until(lambda: mgmt.queue_depth("request.match") == 0 and mgmt.unacked_count("request.match") == 0, timeout=15.0)
    assert mgmt.queue_depth("request.match") == 0
    assert mgmt.queue_depth("request.match.retry") == 0
