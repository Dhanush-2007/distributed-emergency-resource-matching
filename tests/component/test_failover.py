"""Component tests for coordinator failover manager (Build item 5.3)."""

from __future__ import annotations

import pytest
from coordinator.main import Settings, create_app
from ems_common.sharding import shard_of
from starlette.testclient import TestClient

from tests.component.fakes import FakeStorageCluster

pytestmark = pytest.mark.component


def key_for_shard(sid: int, n_shards: int = 2) -> str:
    for i in range(100):
        k = f"key:{i}"
        if shard_of(k, n_shards) == sid:
            return k
    raise RuntimeError(f"No key found for shard {sid}")


class FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self._time = start

    def __call__(self) -> float:
        return self._time

    def advance(self, seconds: float) -> None:
        self._time += seconds


@pytest.fixture
def failover_env(tmp_path):
    clock = FakeClock(1000.0)
    cluster = FakeStorageCluster([
        ("s0-a", 0, "http://s0-a:9000"),
        ("s0-b", 0, "http://s0-b:9000"),
        ("s1-a", 1, "http://s1-a:9000"),
        ("s1-b", 1, "http://s1-b:9000"),
    ])
    http_client = cluster.get_client()

    settings = Settings(
        state_dir=tmp_path / "state",
        n_shards=2,
        heartbeat_interval_sec=2.0,
        missed_heartbeats_limit=3,
        health_check_interval_sec=1.0,
        probe_timeout_sec=0.5,
    )
    settings.health_check_enabled = False  # driven manually via tick()

    app = create_app(
        settings=settings,
        http_client=http_client,
        clock=clock,
    )
    client = TestClient(app)

    # Register nodes for shard 0
    client.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s0-a",
            "shardId": 0,
            "endpoint": "http://s0-a:9000",
            "role": "PRIMARY",
            "epoch": 1,
            "status": "READY",
            "lastSeq": 10,
        },
    )
    client.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s0-b",
            "shardId": 0,
            "endpoint": "http://s0-b:9000",
            "role": "REPLICA",
            "epoch": 1,
            "status": "READY",
            "lastSeq": 10,
        },
    )

    # Register nodes for shard 1
    client.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s1-a",
            "shardId": 1,
            "endpoint": "http://s1-a:9000",
            "role": "PRIMARY",
            "epoch": 1,
            "status": "READY",
            "lastSeq": 5,
        },
    )
    client.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s1-b",
            "shardId": 1,
            "endpoint": "http://s1-b:9000",
            "role": "REPLICA",
            "epoch": 1,
            "status": "READY",
            "lastSeq": 5,
        },
    )

    return {
        "app": app,
        "client": client,
        "cluster": cluster,
        "clock": clock,
        "state_dir": tmp_path / "state",
    }


def test_promote_on_primary_down_with_healthy_replica(failover_env):
    """Primary failure triggers failover to healthy replica at epoch 2."""
    app = failover_env["app"]
    client = failover_env["client"]
    cluster = failover_env["cluster"]
    clock = failover_env["clock"]

    # Stop s0-a and advance clock past missed heartbeat limit
    cluster.set_down("s0-a", True)
    clock.advance(10.0)

    # Tick health checker to detect DOWN and run failover
    app.state.health_checker.tick()

    # Check shard state persisted
    shard0 = app.state.shard_state.get_shard(0)
    assert shard0["epoch"] == 2
    assert shard0["primaryNodeId"] == "s0-b"
    assert shard0["degraded"] is True

    # Check cluster node updated
    assert cluster.dbs["s0-b"].role == "PRIMARY"
    assert cluster.dbs["s0-b"].epoch == 2

    # Writes to shard 0 now succeed on new primary s0-b with epoch 2
    # In a 2-shard ring: 'request:0' -> shard 0
    # Let's find a key that maps to shard 0
    key_s0 = key_for_shard(0, 2)
    res = client.put(f"/internal/data/{key_s0}", json={"value": {"x": 42}})
    assert res.status_code == 201


def test_no_promotion_when_shard_degraded(failover_env):
    """Degraded shard with replica UP is NOT promoted and becomes UNAVAILABLE."""
    app = failover_env["app"]
    client = failover_env["client"]
    cluster = failover_env["cluster"]
    clock = failover_env["clock"]

    # Mark shard 0 degraded
    app.state.shard_state.set_degraded(0, True, 10)

    cluster.set_down("s0-a", True)
    clock.advance(10.0)
    app.state.health_checker.tick()

    # Shard state not promoted
    shard0 = app.state.shard_state.get_shard(0)
    assert shard0["epoch"] == 1
    assert shard0["primaryNodeId"] == "s0-a"

    # Writes to shard 0 return 503
    key_s0 = key_for_shard(0, 2)
    res = client.put(f"/internal/data/{key_s0}", json={"value": {"x": 1}})
    assert res.status_code == 503
    assert res.headers.get("Retry-After") == "3"


def test_no_promotion_when_replica_catching_up(failover_env):
    """Replica in CATCHING_UP status is NOT promoted."""
    app = failover_env["app"]
    client = failover_env["client"]
    cluster = failover_env["cluster"]
    clock = failover_env["clock"]

    # Update s0-b status to CATCHING_UP
    client.post(
        "/internal/nodes/heartbeat",
        json={
            "nodeId": "s0-b",
            "shardId": 0,
            "role": "REPLICA",
            "epoch": 1,
            "status": "CATCHING_UP",
            "lastSeq": 10,
            "replicaAckSeq": 10,
            "recordCount": {},
        },
    )

    cluster.set_down("s0-a", True)
    clock.advance(10.0)
    app.state.health_checker.tick()

    shard0 = app.state.shard_state.get_shard(0)
    assert shard0["epoch"] == 1
    assert shard0["primaryNodeId"] == "s0-a"


def test_both_nodes_down_gives_503_for_that_shard_only(failover_env):
    """When both nodes of shard 0 are down, shard 0 returns 503 while shard 1 works."""
    app = failover_env["app"]
    client = failover_env["client"]
    cluster = failover_env["cluster"]
    clock = failover_env["clock"]

    cluster.set_down("s0-a", True)
    cluster.set_down("s0-b", True)
    clock.advance(10.0)
    app.state.health_checker.tick()

    key_s0 = key_for_shard(0, 2)
    key_s1 = key_for_shard(1, 2)

    # Shard 0 fails with 503
    res0 = client.put(f"/internal/data/{key_s0}", json={"value": {"x": 1}})
    assert res0.status_code == 503
    assert res0.headers.get("Retry-After") == "3"

    # Shard 1 unaffected
    res1 = client.put(f"/internal/data/{key_s1}", json={"value": {"x": 2}})
    assert res1.status_code == 201


def test_role_call_failure_retried_without_second_increment(failover_env):
    """Role call failure during failover keeps the same epoch on retry."""
    app = failover_env["app"]
    cluster = failover_env["cluster"]
    clock = failover_env["clock"]

    # Make s0-a down, and s0-b initially down so role call fails
    cluster.set_down("s0-a", True)
    cluster.set_down("s0-b", True)
    clock.advance(10.0)
    app.state.health_checker.tick()

    # Now make s0-b UP in registry with fresh heartbeat, but cluster still raises ConnectError
    app.state.registry.set_health("s0-b", "UP")
    rec_b = app.state.registry.get("s0-b")
    assert rec_b is not None
    rec_b.last_heartbeat = clock()
    app.state.failover_manager.evaluate_shard(0)

    # Shard state was updated to epoch 2
    assert app.state.shard_state.get_epoch(0) == 2

    # Now let s0-b accept HTTP requests
    cluster.set_down("s0-b", False)

    # Next tick retries with SAME epoch 2
    app.state.health_checker.tick()
    assert app.state.shard_state.get_epoch(0) == 2
    assert cluster.dbs["s0-b"].epoch == 2
    assert cluster.dbs["s0-b"].role == "PRIMARY"


def test_old_primary_registering_gets_replica_and_new_epoch(failover_env):
    """Old primary re-registering after failover gets assigned role REPLICA and epoch 2."""
    app = failover_env["app"]
    client = failover_env["client"]
    cluster = failover_env["cluster"]
    clock = failover_env["clock"]

    cluster.set_down("s0-a", True)
    clock.advance(10.0)
    app.state.health_checker.tick()

    # s0-a returns and registers with old role PRIMARY epoch 1
    resp = client.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s0-a",
            "shardId": 0,
            "endpoint": "http://s0-a:9000",
            "role": "PRIMARY",
            "epoch": 1,
            "status": "READY",
            "lastSeq": 10,
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["role"] == "REPLICA"
    assert data["epoch"] == 2


def test_force_promote(failover_env, caplog):
    """Force promote succeeds even if replica was behind and logs warning."""
    app = failover_env["app"]
    client = failover_env["client"]

    # Set s0-b lastSeq to 2 (behind s0-a which is 10)
    client.post(
        "/internal/nodes/heartbeat",
        json={
            "nodeId": "s0-b",
            "shardId": 0,
            "role": "REPLICA",
            "epoch": 1,
            "status": "READY",
            "lastSeq": 2,
            "replicaAckSeq": 2,
            "recordCount": {},
        },
    )

    with caplog.at_level("WARNING"):
        resp = client.post("/diagnostics/shards/0/force-promote?nodeId=s0-b")
    assert resp.status_code == 200
    assert resp.json()["primaryNodeId"] == "s0-b"
    assert resp.json()["epoch"] == 2
    assert "stale replica (s0-b) with seq 2 < primary seq 10" in caplog.text

    shard0 = app.state.shard_state.get_shard(0)
    assert shard0["epoch"] == 2
    assert shard0["primaryNodeId"] == "s0-b"
    assert shard0["degraded"] is True


def test_coordinator_restart_preserves_epoch(failover_env):
    """A new coordinator instance on the same state_dir preserves epoch and primary."""
    app = failover_env["app"]
    cluster = failover_env["cluster"]
    clock = failover_env["clock"]
    state_dir = failover_env["state_dir"]

    # Trigger failover to epoch 2
    cluster.set_down("s0-a", True)
    clock.advance(10.0)
    app.state.health_checker.tick()

    # Create brand new coordinator instance pointing to the same state_dir
    new_settings = Settings(
        state_dir=state_dir,
        n_shards=2,
        heartbeat_interval_sec=2.0,
        missed_heartbeats_limit=3,
        health_check_interval_sec=1.0,
        probe_timeout_sec=0.5,
    )
    new_settings.health_check_enabled = False
    new_app = create_app(
        settings=new_settings,
        http_client=cluster.get_client(),
        clock=clock,
    )
    shard0 = new_app.state.shard_state.get_shard(0)
    assert shard0["epoch"] == 2
    assert shard0["primaryNodeId"] == "s0-b"
