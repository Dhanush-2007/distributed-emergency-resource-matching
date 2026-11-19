"""Stage 8 Docker Compose stack integration tests."""

from __future__ import annotations

import json
import os
import subprocess
import uuid

import httpx
import pytest

from tests.fault.stack import wait_until

COORD_URL = os.getenv("COORDINATOR_URL", "http://localhost:8000").rstrip("/")
REQ_URL = os.getenv("REQUEST_SERVICE_URL", "http://localhost:8001").rstrip("/")
RES_URL = os.getenv("RESOURCE_SERVICE_URL", "http://localhost:8002").rstrip("/")


def _is_cluster_available() -> bool:
    try:
        r = httpx.get(f"{COORD_URL}/health/live", timeout=2.0)
        return r.status_code == 200
    except (httpx.HTTPError, OSError):
        return False


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not _is_cluster_available(),
        reason="Stack integration tests require a running Docker Compose cluster.",
    ),
]


def test_service_health_and_published_ports():
    """Verify health endpoints and ensure only ports 8000, 8001, 8002, 15672 are published."""
    with httpx.Client(timeout=5.0) as client:
        assert client.get(f"{COORD_URL}/health/live").status_code == 200
        assert client.get(f"{REQ_URL}/health/live").status_code == 200
        assert client.get(f"{RES_URL}/health/live").status_code == 200

    out = subprocess.run(
        ["docker", "compose", "ps", "--format", "json"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout

    containers = [json.loads(line) for line in out.strip().splitlines() if line.strip()]
    published_ports: set[int] = set()
    for c in containers:
        publishers = c.get("Publishers") or []
        for p in publishers:
            pub_port = p.get("PublishedPort")
            if pub_port:
                published_ports.add(int(pub_port))

    expected_ports = {8000, 8001, 8002, 15672}
    assert published_ports == expected_ports, f"Unexpected published ports: {published_ports} vs {expected_ports}"


def test_full_lifecycle_flow_and_resource_reassignment():
    """Test full cycle: register -> match -> complete -> release -> re-match."""
    tag = uuid.uuid4().hex[:6]
    with httpx.Client(timeout=5.0) as client:
        # Register resource
        r_resp = client.post(
            f"{RES_URL}/resources",
            json={
                "name": f"Lifecyle-Res-{tag}",
                "type": "transportation",
                "location": {"lat": 12.9716, "lon": 77.5946},
                "availability": True,
            },
        )
        assert r_resp.status_code == 201
        res_id = r_resp.json()["resourceId"]

        # Create request
        req1 = client.post(
            f"{REQ_URL}/requests",
            json={
                "type": "transportation",
                "location": {"lat": 12.9716, "lon": 77.5946},
                "details": f"Lifecycle req 1 {tag}",
            },
        ).json()["requestId"]

        assert wait_until(
            lambda: client.get(f"{REQ_URL}/requests/{req1}").json().get("status") == "MATCHED",
            timeout=15.0,
        )

        # Check resource is ASSIGNED
        res_state = client.get(f"{RES_URL}/resources/{res_id}").json()
        assert res_state["status"] == "ASSIGNED"
        assert res_state["assignedRequestId"] == req1

        # Mark request COMPLETED (triggers release)
        up_resp = client.put(f"{REQ_URL}/requests/{req1}/status", json={"status": "COMPLETED"})
        assert up_resp.status_code == 200

        # Verify resource becomes AVAILABLE again
        assert wait_until(
            lambda: client.get(f"{RES_URL}/resources/{res_id}").json().get("status") == "AVAILABLE",
            timeout=15.0,
        )

        # Create second request, should match the released resource
        req2 = client.post(
            f"{REQ_URL}/requests",
            json={
                "type": "transportation",
                "location": {"lat": 12.9716, "lon": 77.5946},
                "details": f"Lifecycle req 2 {tag}",
            },
        ).json()["requestId"]

        assert wait_until(
            lambda: client.get(f"{REQ_URL}/requests/{req2}").json().get("status") == "MATCHED",
            timeout=15.0,
        )
        assert client.get(f"{REQ_URL}/requests/{req2}").json().get("matchedResourceId") == res_id


def test_shard_distribution_after_seed():
    """Verify data distribution over three shards after seeding."""
    subprocess.run(
        [".venv/bin/python", "scripts/seed.py", "--requests", "30", "--resources", "30"],
        check=True,
    )
    with httpx.Client(timeout=5.0) as client:
        r = client.get(f"{COORD_URL}/diagnostics/shards")
        assert r.status_code == 200
        shards = r.json()
        assert len(shards) == 3
        for s in shards:
            assert s["records"] > 0, f"Shard {s['shardId']} has 0 records after seed"


def test_scaling_matching_service_to_3():
    """Verify scaling matching-service to 3 workers maintains normal processing."""
    try:
        subprocess.run(
            ["docker", "compose", "up", "-d", "--scale", "matching-service=3", "--no-recreate"],
            check=True,
        )
        ps_out = subprocess.run(
            ["docker", "compose", "ps", "--format", "{{.Service}}"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        workers = [s for s in ps_out.splitlines() if s.strip() == "matching-service"]
        assert len(workers) == 3

        with httpx.Client(timeout=5.0) as client:
            client.post(
                f"{RES_URL}/resources",
                json={
                    "name": "Scale-Res",
                    "type": "shelter",
                    "location": {"lat": 12.97, "lon": 77.59},
                    "availability": True,
                },
            )
            req = client.post(
                f"{REQ_URL}/requests",
                json={
                    "type": "shelter",
                    "location": {"lat": 12.97, "lon": 77.59},
                    "details": "Scale test req",
                },
            ).json()

            assert wait_until(
                lambda: client.get(f"{REQ_URL}/requests/{req['requestId']}").json().get("status") == "MATCHED",
                timeout=15.0,
            )
    finally:
        subprocess.run(
            ["docker", "compose", "up", "-d", "--scale", "matching-service=1", "--no-recreate"],
            check=True,
        )


def test_data_persistence_across_restarts():
    """Verify data survives container restarts and docker compose down/up (volumes preserved)."""
    tag = uuid.uuid4().hex[:6]
    with httpx.Client(timeout=10.0) as client:
        # Create a persistent request
        req = client.post(
            f"{REQ_URL}/requests",
            json={
                "type": "medicine",
                "location": {"lat": 12.97, "lon": 77.59},
                "details": f"Persist test {tag}",
            },
        ).json()
        req_id = req["requestId"]

        # Restart storage node, coordinator, and rabbitmq
        subprocess.run(["docker", "restart", "ems-s0-a", "ems-s0-b"], check=True)
        subprocess.run(["docker", "restart", "ems-coordinator"], check=True)
        subprocess.run(["docker", "restart", "ems-rabbitmq"], check=True)

        assert wait_until(lambda: client.get(f"{REQ_URL}/requests/{req_id}").status_code == 200, timeout=25.0)

        # Test docker compose down (preserving volumes) then up
        subprocess.run(["docker", "compose", "down"], check=True)
        subprocess.run(["docker", "compose", "up", "-d", "--wait"], check=True)

        # Verify record still readable after compose cycle
        assert wait_until(lambda: client.get(f"{REQ_URL}/requests/{req_id}").status_code == 200, timeout=30.0)
        persisted_req = client.get(f"{REQ_URL}/requests/{req_id}").json()
        assert persisted_req["requestId"] == req_id
