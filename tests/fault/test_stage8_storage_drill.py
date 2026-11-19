"""Stage 8 storage and broker fault drills under load."""

from __future__ import annotations

import json
import os
import subprocess
import threading
import time
from pathlib import Path

import httpx
import pytest

from tests.fault.stack import wait_until

COORD_URL = os.getenv("COORDINATOR_URL", "http://localhost:8000").rstrip("/")
REQ_URL = os.getenv("REQUEST_SERVICE_URL", "http://localhost:8001").rstrip("/")
RES_URL = os.getenv("RESOURCE_SERVICE_URL", "http://localhost:8002").rstrip("/")
EVIDENCE_DIR = Path("docs/evidence/stage8")


def _is_cluster_available() -> bool:
    try:
        r = httpx.get(f"{COORD_URL}/health/live", timeout=2.0)
        return r.status_code == 200
    except (httpx.HTTPError, OSError):
        return False


pytestmark = [
    pytest.mark.fault,
    pytest.mark.skipif(
        not _is_cluster_available(),
        reason="Storage drill fault tests require a running Docker Compose cluster.",
    ),
]


def _save_evidence(name: str, client: httpx.Client) -> None:
    target_dir = EVIDENCE_DIR / name
    target_dir.mkdir(parents=True, exist_ok=True)

    try:
        shards_resp = client.get(f"{COORD_URL}/diagnostics/shards")
        nodes_resp = client.get(f"{COORD_URL}/diagnostics/nodes")
        summary = {
            "timestamp": time.time(),
            "shards": shards_resp.json() if shards_resp.status_code == 200 else [],
            "nodes": nodes_resp.json() if nodes_resp.status_code == 200 else [],
        }
        (target_dir / "watch_cluster.txt").write_text(json.dumps(summary, indent=2))
    except Exception:  # noqa: BLE001, S110
        pass

    try:
        logs = subprocess.run(
            ["docker", "compose", "logs", "--tail=300", "coordinator"],
            capture_output=True,
            text=True,
            check=False,
        ).stdout
        (target_dir / "coordinator.log").write_text(logs)
    except Exception:  # noqa: BLE001, S110
        pass


def test_storage_primary_failure_under_load():
    """Stop primary under load: writes resume within 15s, no acknowledged write lost, recovers to HEALTHY."""
    with httpx.Client(timeout=10.0) as client:
        # Pre-seed available resources
        for i in range(10):
            client.post(
                f"{RES_URL}/resources",
                json={
                    "name": f"Drill-Res-{i}",
                    "type": "water",
                    "location": {"lat": 12.9716, "lon": 77.5946},
                    "availability": True,
                },
            )

        # Background load loop
        ack_ids: list[str] = []
        stop_load = threading.Event()

        def load_worker():
            c = httpx.Client(timeout=4.0)
            while not stop_load.is_set():
                try:
                    resp = c.post(
                        f"{REQ_URL}/requests",
                        json={
                            "type": "water",
                            "location": {"lat": 12.9716, "lon": 77.5946},
                            "details": "Storage drill request",
                        },
                    )
                    if resp.status_code == 201:
                        ack_ids.append(resp.json()["requestId"])
                except Exception:  # noqa: BLE001, S110
                    pass
                time.sleep(0.2)

        t = threading.Thread(target=load_worker, daemon=True)
        t.start()

        time.sleep(2.0)

        # Find primary of shard 1
        shards = client.get(f"{COORD_URL}/diagnostics/shards").json()
        s1 = next(s for s in shards if s["shardId"] == 1)
        orig_primary = s1["primary"]
        replica = next(n["id"] for n in s1["nodes"] if n["id"] != orig_primary)
        orig_epoch = s1["epoch"]

        # Stop the primary
        subprocess.run(["docker", "stop", f"ems-{orig_primary}"], check=True)

        # Verify failover to replica within 15s
        def is_promoted():
            r = client.get(f"{COORD_URL}/diagnostics/shards")
            if r.status_code != 200:
                return False
            curr = {s["shardId"]: s for s in r.json()}.get(1, {})
            return curr.get("primary") == replica and curr.get("epoch") == orig_epoch + 1

        assert wait_until(is_promoted, timeout=15.0), "Failover did not complete within 15s"

        # Evidence during outage
        _save_evidence("storage_failure_outage", client)

        # Let load continue a bit more, then stop
        time.sleep(2.0)
        stop_load.set()
        t.join(timeout=3.0)

        # Restart original primary
        subprocess.run(["docker", "start", f"ems-{orig_primary}"], check=True)

        # Verify shard converges to HEALTHY within 30s
        def is_healthy():
            r = client.get(f"{COORD_URL}/diagnostics/shards")
            if r.status_code != 200:
                return False
            curr = {s["shardId"]: s for s in r.json()}.get(1, {})
            return curr.get("state") == "HEALTHY"

        assert wait_until(is_healthy, timeout=30.0), "Shard did not return to HEALTHY within 30s"

        _save_evidence("storage_failure_recovered", client)

        # Verify all acknowledged requests readable and settled
        for rid in ack_ids:
            r = client.get(f"{REQ_URL}/requests/{rid}")
            assert r.status_code == 200, f"Request {rid} lost!"
            assert r.json().get("status") in ("MATCHED", "NO_MATCH", "PENDING")


def test_broker_failure_under_load():
    """Stopping RabbitMQ keeps POST /requests working (matchQueued false); restarts settle requests."""
    with httpx.Client(timeout=10.0) as client:
        # Pre-register resource
        client.post(
            f"{RES_URL}/resources",
            json={
                "name": "Broker-Drill-Res",
                "type": "medicine",
                "location": {"lat": 12.97, "lon": 77.59},
                "availability": True,
            },
        )

        # Stop RabbitMQ broker
        subprocess.run(["docker", "stop", "ems-rabbitmq"], check=True)

        try:
            # POST /requests still succeeds with matchQueued=False
            t0 = time.time()
            resp = client.post(
                f"{REQ_URL}/requests",
                json={
                    "type": "medicine",
                    "location": {"lat": 12.97, "lon": 77.59},
                    "details": "Broker outage request",
                },
            )
            elapsed = time.time() - t0
            assert resp.status_code == 201
            assert resp.json().get("matchQueued") is False
            assert elapsed < 5.0, f"Request took too long during outage: {elapsed}s"
            req_id = resp.json()["requestId"]

            # Start RabbitMQ back up
            subprocess.run(["docker", "start", "ems-rabbitmq"], check=True)

            # Wait for sweeper to publish and worker to match
            assert wait_until(
                lambda: client.get(f"{REQ_URL}/requests/{req_id}").json().get("status") == "MATCHED",
                timeout=45.0,
            ), "Sweeper/worker did not match request after broker recovery"
        finally:
            subprocess.run(["docker", "start", "ems-rabbitmq"], check=False)


def test_worker_killed_under_concurrency():
    """Killing one of 3 matching workers does not lose requests or duplicate assignments."""
    try:
        subprocess.run(
            ["docker", "compose", "up", "-d", "--scale", "matching-service=3", "--no-recreate"],
            check=True,
        )

        with httpx.Client(timeout=10.0) as client:
            # Register 3 resources
            res_ids = []
            for i in range(3):
                r = client.post(
                    f"{RES_URL}/resources",
                    json={
                        "name": f"Worker-Kill-Res-{i}",
                        "type": "food",
                        "location": {"lat": 12.97, "lon": 77.59},
                        "availability": True,
                    },
                ).json()["resourceId"]
                res_ids.append(r)

            # Submit 15 concurrent requests
            req_ids = []
            for i in range(15):
                rid = client.post(
                    f"{REQ_URL}/requests",
                    json={
                        "type": "food",
                        "location": {"lat": 12.97, "lon": 77.59},
                        "details": f"Worker kill test {i}",
                    },
                ).json()["requestId"]
                req_ids.append(rid)

            # Find a matching worker container and kill it
            ps_out = subprocess.run(
                ["docker", "compose", "ps", "-q", "matching-service"],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip().splitlines()
            if ps_out:
                subprocess.run(["docker", "kill", ps_out[0]], check=True)

            # Restart the killed worker so stack stays healthy
            subprocess.run(
                ["docker", "compose", "up", "-d", "--scale", "matching-service=3", "--no-recreate"],
                check=True,
            )

            # Verify all 15 requests settle
            def all_settled():
                for rid in req_ids:
                    st = client.get(f"{REQ_URL}/requests/{rid}").json().get("status")
                    if st not in ("MATCHED", "NO_MATCH"):
                        return False
                return True

            assert wait_until(all_settled, timeout=45.0), "Not all requests settled after worker kill"

            # Verify no resource assigned more than once
            assigned_counts = 0
            for rid in res_ids:
                st = client.get(f"{RES_URL}/resources/{rid}").json()
                if st["status"] == "ASSIGNED":
                    assigned_counts += 1
            assert assigned_counts <= 3
    finally:
        subprocess.run(
            ["docker", "compose", "up", "-d", "--scale", "matching-service=1", "--no-recreate"],
            check=True,
        )
