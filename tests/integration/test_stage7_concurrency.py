"""Concurrency integration tests for Stage 7: 3 matching workers, 50 requests competing for 5 resources."""

from __future__ import annotations

import concurrent.futures
import subprocess
import time
import uuid

import httpx
import pytest

from tests.fault.stack import wait_until

REQ_BASE = "http://localhost:8001"
RES_BASE = "http://localhost:8002"
COORD_BASE = "http://localhost:8000"

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module", autouse=True)
def scale_matching_workers():
    """Ensure matching-service is scaled to 3 instances."""
    subprocess.run(
        ["docker", "compose", "up", "-d", "--scale", "matching-service=3", "--wait"],
        check=True,
        capture_output=True,
    )
    # Verify 3 healthy matching containers
    def check_workers() -> bool:
        res = subprocess.run(
            ["docker", "compose", "ps", "matching-service", "--format", "{{.Status}}"],
            check=False,
            capture_output=True,
            text=True,
        )
        lines = [line.strip() for line in res.stdout.strip().splitlines() if line.strip()]
        return len(lines) == 3 and all("healthy" in line.lower() or "up" in line.lower() for line in lines)

    assert wait_until(check_workers, timeout=30.0), "Matching service did not scale to 3 instances"


def _run_concurrency_drill(iteration: int) -> None:
    client = httpx.Client(timeout=10.0)
    tag = f"drill-{iteration}-{uuid.uuid4().hex[:6]}"

    # 0. Clean up any preexisting available water resources
    pre_resp = client.get(f"{RES_BASE}/resources?type=water")
    if pre_resp.status_code == 200:
        for item in pre_resp.json().get("items", []):
            if item.get("status") == "AVAILABLE":
                client.put(f"{RES_BASE}/resources/{item['resourceId']}/status", json={"status": "OFFLINE"})

    # 1. Register 5 water resources
    resource_ids = []
    for i in range(5):
        resp = client.post(f"{RES_BASE}/resources", json={
            "name": f"water-{tag}-{i}",
            "type": "water",
            "location": {"lat": 12.9700 + (i * 0.005), "lon": 77.5900 + (i * 0.005)},
            "availability": True,
        })
        assert resp.status_code == 201, f"Failed to register resource: {resp.text}"
        resource_ids.append(resp.json()["resourceId"])

    assert len(resource_ids) == 5

    # 2. Fire 50 simultaneous requests concurrently
    def create_one_request(index: int) -> str:
        with httpx.Client(timeout=10.0) as c:
            r = c.post(f"{REQ_BASE}/requests", json={
                "type": "water",
                "location": {"lat": 12.9716, "lon": 77.5946},
                "details": f"urgent water {tag} #{index}",
            })
            assert r.status_code == 201, f"Request creation failed: {r.text}"
            return r.json()["requestId"]

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
        request_ids = list(pool.map(create_one_request, range(50)))

    assert len(request_ids) == 50

    # 3. Poll until all 50 requests reach terminal status (MATCHED or NO_MATCH)
    deadline = time.time() + 35.0
    terminal_requests: dict[str, dict] = {}

    while time.time() < deadline and len(terminal_requests) < 50:
        for req_id in request_ids:
            if req_id in terminal_requests:
                continue
            r = client.get(f"{REQ_BASE}/requests/{req_id}")
            if r.status_code == 200:
                data = r.json()
                if data.get("status") in ("MATCHED", "NO_MATCH"):
                    terminal_requests[req_id] = data
        if len(terminal_requests) < 50:
            time.sleep(0.5)

    assert len(terminal_requests) == 50, (
        f"Only {len(terminal_requests)}/50 requests completed in time: "
        f"{[rid for rid in request_ids if rid not in terminal_requests]}"
    )

    # 4. Assertions on requests
    matched_requests = [d for d in terminal_requests.values() if d["status"] == "MATCHED"]
    no_match_requests = [d for d in terminal_requests.values() if d["status"] == "NO_MATCH"]

    assert len(matched_requests) == 5, f"Expected exactly 5 MATCHED, got {len(matched_requests)}"
    assert len(no_match_requests) == 45, f"Expected exactly 45 NO_MATCH, got {len(no_match_requests)}"

    # 5. Assertions on assigned resources
    assigned_res_ids = [d["matchedResourceId"] for d in matched_requests]
    assert len(set(assigned_res_ids)) == 5, f"Duplicate assignment detected: {assigned_res_ids}"
    assert set(assigned_res_ids).issubset(set(resource_ids)), "Matched resource not in pool"

    # 6. Verify resources in resource-service
    for res_id in resource_ids:
        r = client.get(f"{RES_BASE}/resources/{res_id}")
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "ASSIGNED"
        assert data.get("assignedRequestId") in request_ids


def test_concurrency_round_1():
    _run_concurrency_drill(1)


def test_concurrency_round_2():
    _run_concurrency_drill(2)
