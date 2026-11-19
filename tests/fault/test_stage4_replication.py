"""Fault-injection tests for Stage 4 replication and recovery under node failure."""

import json
import os
import subprocess
import time
import uuid

import httpx
import pytest
from ems_common.sharding import shard_of

COORDINATOR_URL = os.getenv("COORDINATOR_URL", "http://localhost:8000").rstrip("/")


def _is_cluster_available() -> bool:
    try:
        r = httpx.get(f"{COORDINATOR_URL}/health/live", timeout=2.0)
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


def test_replica_downtime_degrades_shard_and_recovers_on_restart():
    """Stopping replica causes writes to be degraded; restarting replica catches up to HEALTHY."""
    client = httpx.Client(base_url=COORDINATOR_URL, timeout=10.0)

    # 1. Ensure cluster is healthy initially
    shards = client.get("/diagnostics/shards").json()
    assert all(s["state"] == "HEALTHY" for s in shards), f"Initial cluster not healthy: {shards}"

    # 2. Stop replica s1-b
    subprocess.run(["docker", "stop", "ems-s1-b"], check=True)
    try:
        # 3. Find 10 keys that hash to shard 1
        keys_for_shard1 = []
        for i in range(1000):
            k = f"request:fault:{i}:{uuid.uuid4().hex[:6]}"
            if shard_of(k, 3) == 1:
                keys_for_shard1.append(k)
                if len(keys_for_shard1) == 10:
                    break
        assert len(keys_for_shard1) == 10

        # 4. Write all 10 keys through the coordinator
        for k in keys_for_shard1:
            val = {"key": k, "data": "degraded_write_payload"}
            res = client.put(f"/internal/data/{k}", json={"value": val, "ifVersion": 0})
            assert res.status_code in (200, 201), f"Write failed: {res.status_code} {res.text}"

        # 5. Shard 1 must report DEGRADED
        diag = client.get("/diagnostics/shards").json()
        s1 = next(s for s in diag if s["shardId"] == 1)
        assert s1["state"] == "DEGRADED", f"Expected DEGRADED, got {s1['state']}"
        assert s1["degraded"] is True

        # 6. Restart replica s1-b
        subprocess.run(["docker", "start", "ems-s1-b"], check=True)

        # 7. Wait for replica to catch up and shard to return to HEALTHY
        recovered = False
        for _ in range(30):
            diag = client.get("/diagnostics/shards").json()
            s1 = next(s for s in diag if s["shardId"] == 1)
            if s1["state"] == "HEALTHY" and not s1["degraded"]:
                recovered = True
                break
            time.sleep(1)

        assert recovered, f"Shard 1 did not recover to HEALTHY: {s1}"

        # 8. Verify s1-b has all 10 keys via SQLite query
        check_cmd = [
            "docker",
            "exec",
            "ems-s1-b",
            "python",
            "-c",
            (
                "import sqlite3, json, sys; "
                "conn = sqlite3.connect('/data/s1-b.db'); "
                "keys = json.loads(sys.argv[1]); "
                "cur = conn.cursor(); "
                "found = [cur.execute('SELECT key FROM kv WHERE key = ? AND deleted = 0', (k,)).fetchone() for k in keys]; "
                "print(all(f is not None for f in found))"
            ),
            json.dumps(keys_for_shard1),
        ]
        proc = subprocess.run(check_cmd, capture_output=True, text=True, check=True)
        assert proc.stdout.strip() == "True", "Not all 10 keys were caught up on s1-b!"

    finally:
        # Guarantee container is restarted if anything failed
        subprocess.run(["docker", "start", "ems-s1-b"], check=False)
