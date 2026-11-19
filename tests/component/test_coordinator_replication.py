"""Component tests for coordinator replication awareness and degraded handling (Build item 4.5)."""

import pytest
from coordinator.main import Settings, create_app
from fastapi.testclient import TestClient

pytestmark = pytest.mark.component


@pytest.fixture
def coordinator_setup(tmp_path):
    settings = Settings(
        state_dir=tmp_path / "coordinator_state",
        n_shards=3,
        heartbeat_interval_sec=2.0,
        missed_heartbeats_limit=3,
    )
    app = create_app(settings=settings)
    client = TestClient(app)
    return client, settings


def test_registration_role_hints(coordinator_setup):
    """PRIMARY hint becomes primary; REPLICA hint does not become primary."""
    client, _ = coordinator_setup

    # Register replica first on shard 1
    r_resp = client.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s1-b",
            "shardId": 1,
            "endpoint": "http://s1-b:9000",
            "role": "REPLICA",
            "epoch": 1,
            "status": "READY",
            "lastSeq": 0,
        },
    )
    assert r_resp.status_code == 200
    assert r_resp.json()["role"] == "REPLICA"

    # Shard 1 still has no primary
    shards = client.get("/diagnostics/shards").json()
    shard1 = next(s for s in shards if s["shardId"] == 1)
    assert shard1["primary"] is None
    assert shard1["state"] == "UNAVAILABLE"

    # Now register primary on shard 1
    p_resp = client.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s1-a",
            "shardId": 1,
            "endpoint": "http://s1-a:9000",
            "role": "PRIMARY",
            "epoch": 1,
            "status": "READY",
            "lastSeq": 0,
        },
    )
    assert p_resp.status_code == 200
    assert p_resp.json()["role"] == "PRIMARY"

    # Shard 1 now has s1-a as primary and is HEALTHY
    shards = client.get("/diagnostics/shards").json()
    shard1 = next(s for s in shards if s["shardId"] == 1)
    assert shard1["primary"] == "s1-a"
    assert shard1["state"] == "HEALTHY"


def test_report_degraded_validation(coordinator_setup):
    """POST /internal/nodes/degraded validation rules."""
    client, _ = coordinator_setup

    # Register primary s0-a
    client.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s0-a",
            "shardId": 0,
            "endpoint": "http://s0-a:9000",
            "role": "PRIMARY",
            "epoch": 1,
            "status": "READY",
            "lastSeq": 5,
        },
    )

    # 1. Unknown shard -> 400
    res = client.post(
        "/internal/nodes/degraded",
        json={"nodeId": "s0-a", "shardId": 99, "epoch": 1, "lastSeq": 5},
    )
    assert res.status_code == 400
    assert res.json()["error"]["code"] == "INVALID_INPUT"

    # 2. Not primary -> 409 NOT_PRIMARY
    res = client.post(
        "/internal/nodes/degraded",
        json={"nodeId": "s0-b", "shardId": 0, "epoch": 1, "lastSeq": 5},
    )
    assert res.status_code == 409
    assert res.json()["error"]["code"] == "NOT_PRIMARY"

    # 3. Stale epoch -> 409 STALE_EPOCH
    res = client.post(
        "/internal/nodes/degraded",
        json={"nodeId": "s0-a", "shardId": 0, "epoch": 99, "lastSeq": 5},
    )
    assert res.status_code == 409
    assert res.json()["error"]["code"] == "STALE_EPOCH"

    # 4. Valid degraded report -> 200
    res = client.post(
        "/internal/nodes/degraded",
        json={"nodeId": "s0-a", "shardId": 0, "epoch": 1, "lastSeq": 10},
    )
    assert res.status_code == 200
    assert res.json()["state"] == "DEGRADED"

    # Diagnostics shows DEGRADED
    shards = client.get("/diagnostics/shards").json()
    shard0 = next(s for s in shards if s["shardId"] == 0)
    assert shard0["state"] == "DEGRADED"
    assert shard0["degraded"] is True


def test_degraded_persistence_and_recovery_on_heartbeat(coordinator_setup):
    """Degraded state persists and is cleared when replica catches up on heartbeat."""
    client, settings = coordinator_setup

    # Register primary and replica
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

    # Mark degraded at seq 15
    client.post(
        "/internal/nodes/degraded",
        json={"nodeId": "s0-a", "shardId": 0, "epoch": 1, "lastSeq": 15},
    )

    # Primary heartbeats at seq 15
    client.post(
        "/internal/nodes/heartbeat",
        json={
            "nodeId": "s0-a",
            "shardId": 0,
            "role": "PRIMARY",
            "epoch": 1,
            "status": "READY",
            "lastSeq": 15,
            "replicaAckSeq": 10,
        },
    )

    # Verify degraded persists across a coordinator reload
    new_app = create_app(settings=settings)
    new_client = TestClient(new_app)
    # Re-register nodes in new in-memory registry
    new_client.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s0-a",
            "shardId": 0,
            "endpoint": "http://s0-a:9000",
            "role": "PRIMARY",
            "epoch": 1,
            "status": "READY",
            "lastSeq": 15,
        },
    )
    new_client.post(
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

    shards = new_client.get("/diagnostics/shards").json()
    assert shards[0]["degraded"] is True
    assert shards[0]["state"] == "DEGRADED"

    # Replica heartbeats catching up to seq 15
    new_client.post(
        "/internal/nodes/heartbeat",
        json={
            "nodeId": "s0-b",
            "shardId": 0,
            "role": "REPLICA",
            "epoch": 1,
            "status": "READY",
            "lastSeq": 15,
            "replicaAckSeq": None,
        },
    )

    # Shard recovered to HEALTHY!
    shards_recovered = new_client.get("/diagnostics/shards").json()
    assert shards_recovered[0]["degraded"] is False
    assert shards_recovered[0]["state"] == "HEALTHY"
