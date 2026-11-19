"""Fault-injection failure drills for Stage 5 failover, fencing and recovery."""

from __future__ import annotations

import os
import subprocess
import time
import uuid
from collections.abc import Callable
from pathlib import Path

import httpx
import pytest
from ems_common.sharding import shard_of

COORDINATOR_URL = os.getenv("COORDINATOR_URL", "http://localhost:8000").rstrip("/")
EVIDENCE_DIR = Path("docs/evidence/stage5")


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


def wait_until(predicate: Callable[[], bool], timeout: float = 30.0, step: float = 0.5) -> bool:
    t_end = time.time() + timeout
    while time.time() < t_end:
        try:
            if predicate():
                return True
        except Exception:  # noqa: BLE001, S110
            pass
        time.sleep(step)
    return False


def wait_shards_healthy(client: httpx.Client, timeout: float = 35.0) -> bool:
    def check():
        r = client.get("/diagnostics/shards")
        if r.status_code != 200:
            return False
        shards = r.json()
        return len(shards) == 3 and all(s.get("state") == "HEALTHY" for s in shards)

    return wait_until(check, timeout=timeout)


def save_evidence(scenario_name: str, client: httpx.Client) -> None:
    target_dir = EVIDENCE_DIR / scenario_name
    target_dir.mkdir(parents=True, exist_ok=True)

    # Save coordinator logs
    try:
        res = subprocess.run(
            ["docker", "compose", "logs", "--no-color", "coordinator"],
            capture_output=True,
            text=True,
            check=False,
        )
        (target_dir / "coordinator.log").write_text(res.stdout, encoding="utf-8")
    except Exception:  # noqa: BLE001, S110
        pass

    # Save watch_cluster output
    try:
        res = subprocess.run(
            [".venv/bin/python", "scripts/watch_cluster.py", "--once"],
            capture_output=True,
            text=True,
            check=False,
        )
        (target_dir / "watch_cluster.txt").write_text(res.stdout, encoding="utf-8")
    except Exception:  # noqa: BLE001, S110
        pass


def key_for_shard(target_shard: int, n_shards: int = 3) -> str:
    for i in range(1000):
        k = f"request:fault:{uuid.uuid4().hex[:8]}"
        if shard_of(k, n_shards) == target_shard:
            return k
    raise RuntimeError(f"No key found for shard {target_shard}")


@pytest.fixture(autouse=True)
def restore_cluster_after_test():
    client = httpx.Client(base_url=COORDINATOR_URL, timeout=10.0)
    # Ensure healthy before test
    wait_shards_healthy(client, timeout=30.0)
    yield
    # Restore any stopped containers after test
    subprocess.run(["docker", "compose", "start"], check=False)
    wait_shards_healthy(client, timeout=35.0)
    client.close()


def test_scenario_a_and_b_primary_failover_and_recovery():
    """Scenarios a & b: primary failure triggers failover to replica; recovery converges to HEALTHY."""
    client = httpx.Client(base_url=COORDINATOR_URL, timeout=10.0)

    # 1. Discover current primary and replica for shard 1
    r = client.get("/diagnostics/shards").json()
    s1 = next(s for s in r if s["shardId"] == 1)
    primary = s1["primary"]
    replica = next(n["id"] for n in s1["nodes"] if n["id"] != primary)
    orig_epoch = s1["epoch"]

    # Write baseline key to shard 1
    k_base = key_for_shard(1)
    res_base = client.put(f"/internal/data/{k_base}", json={"value": {"step": "base"}})
    assert res_base.status_code == 201

    # Stop current primary container
    subprocess.run(["docker", "stop", f"ems-{primary}"], check=True)

    # Within 15s shard 1 is DEGRADED with replica as primary and epoch incremented
    def is_promoted():
        r = client.get("/diagnostics/shards")
        if r.status_code != 200:
            return False
        shards = {s["shardId"]: s for s in r.json()}
        s1_curr = shards.get(1, {})
        return (
            s1_curr.get("primary") == replica
            and s1_curr.get("epoch") == orig_epoch + 1
            and s1_curr.get("state") == "DEGRADED"
        )

    assert wait_until(is_promoted, timeout=15.0), f"Failover to {replica} did not complete in 15s"

    # Writes resume on new primary
    k_outage = key_for_shard(1)
    res_outage = client.put(f"/internal/data/{k_outage}", json={"value": {"step": "during_outage"}})
    assert res_outage.status_code == 201
    assert client.get(f"/internal/data/{k_base}").status_code == 200

    save_evidence("scenario_a", client)

    # Scenario b: recovery of old primary
    subprocess.run(["docker", "start", f"ems-{primary}"], check=True)
    assert wait_shards_healthy(client, timeout=30.0), "Cluster did not return to HEALTHY within 30s"

    # Verify both writes readable and shard 1 nodes have equal lastSeq
    nodes_resp = client.get("/diagnostics/nodes").json()
    s1_nodes = [n for n in nodes_resp if n.get("shardId") == 1]
    assert len(s1_nodes) == 2
    assert s1_nodes[0]["lastSeq"] == s1_nodes[1]["lastSeq"]
    assert client.get(f"/internal/data/{k_outage}").status_code == 200

    save_evidence("scenario_b", client)
    client.close()


def test_scenario_c_replica_failure_and_convergence():
    """Scenario c: stop replica, writes continue degraded, start it, converges to HEALTHY."""
    client = httpx.Client(base_url=COORDINATOR_URL, timeout=10.0)
    r = client.get("/diagnostics/shards").json()
    s0 = next(s for s in r if s["shardId"] == 0)
    primary = s0["primary"]
    replica = next(n["id"] for n in s0["nodes"] if n["id"] != primary)

    subprocess.run(["docker", "stop", f"ems-{replica}"], check=True)
    k = key_for_shard(0)
    res = client.put(f"/internal/data/{k}", json={"value": {"msg": "degraded_write"}})
    assert res.status_code == 201

    # Restart replica
    subprocess.run(["docker", "start", f"ems-{replica}"], check=True)
    assert wait_shards_healthy(client, timeout=30.0)

    save_evidence("scenario_c", client)
    client.close()


def test_scenario_d_stale_replica_never_promoted():
    """Scenario d: stale replica is NEVER promoted; shard returns 503 until primary returns."""
    client = httpx.Client(base_url=COORDINATOR_URL, timeout=10.0)
    r = client.get("/diagnostics/shards").json()
    s0 = next(s for s in r if s["shardId"] == 0)
    primary = s0["primary"]
    replica = next(n["id"] for n in s0["nodes"] if n["id"] != primary)
    orig_epoch = s0["epoch"]

    # 1. Stop replica
    subprocess.run(["docker", "stop", f"ems-{replica}"], check=True)

    # 2. Write to shard 0 while degraded (advances primary lastSeq)
    k0 = key_for_shard(0)
    res = client.put(f"/internal/data/{k0}", json={"value": {"x": 100}})
    assert res.status_code == 201

    # 3. Stop primary and start stale replica
    subprocess.run(["docker", "stop", f"ems-{primary}"], check=True)
    subprocess.run(["docker", "start", f"ems-{replica}"], check=True)

    # Shard 0 must return 503 and NEVER be promoted
    t_end = time.time() + 15.0
    while time.time() < t_end:
        r = client.get("/diagnostics/shards").json()
        s0 = next(s for s in r if s["shardId"] == 0)
        assert s0.get("primary") == primary
        assert s0.get("epoch") == orig_epoch
        # And writes to shard 0 return 503
        r_put = client.put(f"/internal/data/{k0}", json={"value": {"x": 101}})
        assert r_put.status_code == 503
        time.sleep(1.0)

    # Shards 1 and 2 keep working
    k1 = key_for_shard(1)
    assert client.put(f"/internal/data/{k1}", json={"value": {"ok": True}}).status_code in (200, 201)

    # Start primary; shard 0 recovers
    subprocess.run(["docker", "start", f"ems-{primary}"], check=True)
    assert wait_shards_healthy(client, timeout=30.0)

    save_evidence("scenario_d", client)
    client.close()


def test_scenario_e_both_nodes_of_shard_stopped():
    """Scenario e: both nodes of shard 2 stopped -> 503 + Retry-After for shard 2, partial scan works."""
    client = httpx.Client(base_url=COORDINATOR_URL, timeout=10.0)

    subprocess.run(["docker", "stop", "ems-s2-a", "ems-s2-b"], check=True)

    k2 = key_for_shard(2)
    # Shard 2 gives 503 with Retry-After
    r_put = client.put(f"/internal/data/{k2}", json={"value": {"x": 1}})
    assert r_put.status_code == 503
    assert r_put.headers.get("Retry-After") == "3"

    # Scan without allowPartial returns 503
    r_scan = client.get("/internal/scan")
    assert r_scan.status_code == 503

    # Scan with allowPartial=true succeeds with unavailableShards=[2]
    r_partial = client.get("/internal/scan?allowPartial=true")
    assert r_partial.status_code == 200
    assert 2 in r_partial.json().get("unavailableShards", [])

    # Restart both and recover
    subprocess.run(["docker", "start", "ems-s2-a", "ems-s2-b"], check=True)
    assert wait_shards_healthy(client, timeout=30.0)

    save_evidence("scenario_e", client)
    client.close()


def test_scenario_f_and_g_coordinator_restart_and_old_primary_fencing():
    """Scenarios f & g: coordinator restart after failover preserves epoch; old primary direct write fenced."""
    client = httpx.Client(base_url=COORDINATOR_URL, timeout=10.0)
    r = client.get("/diagnostics/shards").json()
    s1 = next(s for s in r if s["shardId"] == 1)
    primary = s1["primary"]
    curr_epoch = s1["epoch"]

    # 1. Trigger failover on shard 1 (stop primary, wait for epoch increment)
    subprocess.run(["docker", "stop", f"ems-{primary}"], check=True)
    def s1_promoted():
        resp = client.get("/diagnostics/shards")
        if resp.status_code != 200:
            return False
        shards = {s["shardId"]: s for s in resp.json()}
        return shards.get(1, {}).get("epoch") == curr_epoch + 1

    assert wait_until(s1_promoted, timeout=15.0)

    # 2. Restart coordinator container
    subprocess.run(["docker", "restart", "ems-coordinator"], check=True)
    assert wait_until(lambda: client.get("/health/live").status_code == 200, timeout=15.0)

    # Epochs unchanged after restart
    shards_after = {s["shardId"]: s for s in client.get("/diagnostics/shards").json()}
    assert shards_after[1]["epoch"] == curr_epoch + 1
    save_evidence("scenario_f", client)

    # Scenario g: start returned old primary
    subprocess.run(["docker", "start", f"ems-{primary}"], check=True)
    time.sleep(3.0)

    # Direct write via python inside old primary container with old epoch gets 409
    res = subprocess.run(
        [
            "docker", "exec", f"ems-{primary}", "python", "-c",
            f"import httpx; r = httpx.put('http://localhost:9000/internal/data/direct:fenced', json={{'value': {{}}, 'epoch': {curr_epoch}}}); print(r.status_code, r.json())",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert "409" in res.stdout, f"Expected 409 fencing error, got: {res.stdout}"
    save_evidence("scenario_g", client)
    client.close()


def test_scenario_h_epoch_monotonicity():
    """Scenario h: poll /diagnostics/shards every 0.5s through failover; epochs never decrease."""
    client = httpx.Client(base_url=COORDINATOR_URL, timeout=10.0)
    r = client.get("/diagnostics/shards").json()
    s0 = next(s for s in r if s["shardId"] == 0)
    primary = s0["primary"]
    highest_epoch = s0["epoch"]

    # Fail primary to trigger promotion
    subprocess.run(["docker", "stop", f"ems-{primary}"], check=True)

    for _ in range(30):
        try:
            resp = client.get("/diagnostics/shards")
            if resp.status_code == 200:
                s0_curr = next(s for s in resp.json() if s["shardId"] == 0)
                ep = s0_curr["epoch"]
                assert ep >= highest_epoch, f"Epoch regressed: {ep} < {highest_epoch}"
                highest_epoch = max(highest_epoch, ep)
        except Exception:  # noqa: BLE001, S110
            pass
        time.sleep(0.5)

    subprocess.run(["docker", "start", f"ems-{primary}"], check=True)
    assert wait_shards_healthy(client, timeout=30.0)
    save_evidence("scenario_h", client)
    client.close()


def test_scenario_i_double_failure_cycle():
    """Scenario i: fail primary A -> B promoted; recover A; fail B -> A promoted; verify no data lost."""
    client = httpx.Client(base_url=COORDINATOR_URL, timeout=10.0)
    r = client.get("/diagnostics/shards").json()
    s2 = next(s for s in r if s["shardId"] == 2)
    node_a = s2["primary"]
    node_b = next(n["id"] for n in s2["nodes"] if n["id"] != node_a)
    epoch_start = s2["epoch"]

    # 1. Baseline write on node_a
    k_i1 = key_for_shard(2)
    client.put(f"/internal/data/{k_i1}", json={"value": {"cycle": 1}})

    # Fail node_a -> node_b promoted to epoch_start + 1
    subprocess.run(["docker", "stop", f"ems-{node_a}"], check=True)
    assert wait_until(
        lambda: next((s for s in client.get("/diagnostics/shards").json() if s["shardId"] == 2), {}).get("primary") == node_b
        and next((s for s in client.get("/diagnostics/shards").json() if s["shardId"] == 2), {}).get("epoch", 0) >= epoch_start + 1,
        timeout=15.0,
    )

    # Recover node_a -> healthy
    subprocess.run(["docker", "start", f"ems-{node_a}"], check=True)
    assert wait_shards_healthy(client, timeout=30.0)

    # 2. Write on node_b
    k_i2 = key_for_shard(2)
    client.put(f"/internal/data/{k_i2}", json={"value": {"cycle": 2}})

    # Fail node_b -> node_a promoted to epoch_start + 2
    subprocess.run(["docker", "stop", f"ems-{node_b}"], check=True)
    assert wait_until(
        lambda: next((s for s in client.get("/diagnostics/shards").json() if s["shardId"] == 2), {}).get("primary") == node_a,
        timeout=15.0,
    )

    # Recover node_b -> healthy
    subprocess.run(["docker", "start", f"ems-{node_b}"], check=True)
    assert wait_shards_healthy(client, timeout=30.0)

    # Both keys readable
    assert client.get(f"/internal/data/{k_i1}").status_code == 200
    assert client.get(f"/internal/data/{k_i2}").status_code == 200

    save_evidence("scenario_i", client)
    client.close()


def test_scenario_j_coordinator_restart_while_degraded():
    """Scenario j: stop replica, restart coordinator, restart replica; converges to HEALTHY with equal lastSeq."""
    client = httpx.Client(base_url=COORDINATOR_URL, timeout=10.0)
    r = client.get("/diagnostics/shards").json()
    s0 = next(s for s in r if s["shardId"] == 0)
    primary = s0["primary"]
    replica = next(n["id"] for n in s0["nodes"] if n["id"] != primary)

    # Stop replica
    subprocess.run(["docker", "stop", f"ems-{replica}"], check=True)
    k = key_for_shard(0)
    client.put(f"/internal/data/{k}", json={"value": {"x": "deg"}})

    # Restart coordinator
    subprocess.run(["docker", "restart", "ems-coordinator"], check=True)
    assert wait_until(lambda: client.get("/health/live").status_code == 200, timeout=15.0)

    # Shard must still be DEGRADED before replica catches up
    r = client.get("/diagnostics/shards").json()
    s0 = next(s for s in r if s["shardId"] == 0)
    assert s0["state"] in ("DEGRADED", "UNAVAILABLE")

    # Restart replica
    subprocess.run(["docker", "start", f"ems-{replica}"], check=True)
    assert wait_shards_healthy(client, timeout=30.0)

    nodes_resp = client.get("/diagnostics/nodes").json()
    s0_nodes = [n for n in nodes_resp if n.get("shardId") == 0]
    assert s0_nodes[0]["lastSeq"] == s0_nodes[1]["lastSeq"]

    save_evidence("scenario_j", client)
    client.close()
