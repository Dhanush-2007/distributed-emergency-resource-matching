"""Component tests for the coordinator core (registry, shard-state persistence, register, heartbeat)."""

import json
import shutil
import tempfile
from pathlib import Path

import pytest
from coordinator.main import Settings, create_app
from fastapi.testclient import TestClient


@pytest.fixture
def temp_state_dir():
    d = tempfile.mkdtemp()
    yield Path(d)
    shutil.rmtree(d, ignore_errors=True)


@pytest.mark.component
def test_first_node_becomes_primary_epoch_1(temp_state_dir):
    """First node registering on a shard becomes PRIMARY with epoch 1 (or max(1, node epoch))."""
    settings = Settings(state_dir=temp_state_dir, n_shards=3)
    app = create_app(settings=settings)
    client = TestClient(app)

    res = client.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s0-a",
            "shardId": 0,
            "endpoint": "http://s0-a:9000",
            "role": "PRIMARY",
            "epoch": 1,
            "lastSeq": 0,
            "status": "STARTING",
        },
    )
    assert res.status_code == 200
    assert res.json() == {"role": "PRIMARY", "epoch": 1}

    # Verify persisted in shard_state.json
    state_file = temp_state_dir / "shard_state.json"
    assert state_file.exists()
    with open(state_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert data["shards"]["0"] == {
        "epoch": 1,
        "primaryNodeId": "s0-a",
        "degraded": False,
        "degradedSeq": 0,
    }


@pytest.mark.component
def test_second_node_becomes_replica(temp_state_dir):
    """Second node registering on the same shard receives role REPLICA with the stored epoch."""
    settings = Settings(state_dir=temp_state_dir, n_shards=3)
    app = create_app(settings=settings)
    client = TestClient(app)

    # First node becomes PRIMARY
    res1 = client.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s0-a",
            "shardId": 0,
            "endpoint": "http://s0-a:9000",
            "role": "PRIMARY",
            "epoch": 1,
            "lastSeq": 0,
            "status": "STARTING",
        },
    )
    assert res1.status_code == 200
    assert res1.json()["role"] == "PRIMARY"

    # Second node with hint PRIMARY still gets REPLICA
    res2 = client.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s0-b",
            "shardId": 0,
            "endpoint": "http://s0-b:9000",
            "role": "PRIMARY",
            "epoch": 1,
            "lastSeq": 0,
            "status": "STARTING",
        },
    )
    assert res2.status_code == 200
    assert res2.json() == {"role": "REPLICA", "epoch": 1}


@pytest.mark.component
def test_replica_hint_never_becomes_first_primary(temp_state_dir):
    """A node registering with role hint REPLICA never becomes the first primary on a shard."""
    settings = Settings(state_dir=temp_state_dir, n_shards=3)
    app = create_app(settings=settings)
    client = TestClient(app)

    # Register node with REPLICA hint first
    res1 = client.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s1-b",
            "shardId": 1,
            "endpoint": "http://s1-b:9000",
            "role": "REPLICA",
            "epoch": 1,
            "lastSeq": 0,
            "status": "STARTING",
        },
    )
    assert res1.status_code == 200
    assert res1.json()["role"] == "REPLICA"

    # Shard state must NOT have assigned a primary yet
    state_file = temp_state_dir / "shard_state.json"
    if state_file.exists():
        with open(state_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            assert "1" not in data.get("shards", {})

    # Second node registers with PRIMARY hint and becomes PRIMARY
    res2 = client.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s1-a",
            "shardId": 1,
            "endpoint": "http://s1-a:9000",
            "role": "PRIMARY",
            "epoch": 1,
            "lastSeq": 0,
            "status": "STARTING",
        },
    )
    assert res2.status_code == 200
    assert res2.json()["role"] == "PRIMARY"

    with open(state_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert data["shards"]["1"]["primaryNodeId"] == "s1-a"


@pytest.mark.component
def test_reregister_is_idempotent(temp_state_dir):
    """Registering again with the same nodeId updates registry and returns the same role."""
    settings = Settings(state_dir=temp_state_dir, n_shards=3)
    app = create_app(settings=settings)
    client = TestClient(app)

    res1 = client.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s0-a",
            "shardId": 0,
            "endpoint": "http://s0-a:9000",
            "role": "PRIMARY",
            "epoch": 1,
            "lastSeq": 0,
            "status": "STARTING",
        },
    )
    assert res1.status_code == 200
    assert res1.json() == {"role": "PRIMARY", "epoch": 1}

    # Re-register with status READY and seq 10
    res2 = client.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s0-a",
            "shardId": 0,
            "endpoint": "http://s0-a:9000",
            "role": "PRIMARY",
            "epoch": 1,
            "lastSeq": 10,
            "status": "READY",
        },
    )
    assert res2.status_code == 200
    assert res2.json() == {"role": "PRIMARY", "epoch": 1}

    # Registry entry was updated
    reg = app.state.registry.get("s0-a")
    assert reg.status == "READY"
    assert reg.last_seq == 10


@pytest.mark.component
def test_invalid_shard_and_empty_fields_400(temp_state_dir):
    """Invalid shardId or empty nodeId/endpoint return 400 INVALID_INPUT."""
    settings = Settings(state_dir=temp_state_dir, n_shards=3)
    app = create_app(settings=settings)
    client = TestClient(app)

    # ShardId out of range (negative)
    r1 = client.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s0-a",
            "shardId": -1,
            "endpoint": "http://s0-a:9000",
        },
    )
    assert r1.status_code == 400
    assert r1.json()["error"]["code"] == "INVALID_INPUT"

    # ShardId out of range (>= N_SHARDS)
    r2 = client.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s0-a",
            "shardId": 3,
            "endpoint": "http://s0-a:9000",
        },
    )
    assert r2.status_code == 400
    assert r2.json()["error"]["code"] == "INVALID_INPUT"

    # Empty nodeId
    r3 = client.post(
        "/internal/nodes/register",
        json={
            "nodeId": "   ",
            "shardId": 0,
            "endpoint": "http://s0-a:9000",
        },
    )
    assert r3.status_code == 400
    assert r3.json()["error"]["code"] == "INVALID_INPUT"

    # Empty endpoint
    r4 = client.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s0-a",
            "shardId": 0,
            "endpoint": "",
        },
    )
    assert r4.status_code == 400
    assert r4.json()["error"]["code"] == "INVALID_INPUT"


@pytest.mark.component
def test_heartbeat_unknown_node_404(temp_state_dir):
    """Heartbeat for an unregistered node returns 404 NOT_FOUND."""
    settings = Settings(state_dir=temp_state_dir, n_shards=3)
    app = create_app(settings=settings)
    client = TestClient(app)

    res = client.post(
        "/internal/nodes/heartbeat",
        json={
            "nodeId": "unknown-node",
            "shardId": 0,
            "role": "PRIMARY",
            "epoch": 1,
            "status": "READY",
            "lastSeq": 0,
            "replicaAckSeq": None,
            "recordCount": {"request": 0, "resource": 0, "other": 0},
        },
    )
    assert res.status_code == 404
    body = res.json()
    assert body["error"]["code"] == "NOT_FOUND"


@pytest.mark.component
def test_heartbeat_updates_record_count(temp_state_dir):
    """Heartbeat updates node recordCount and returns shard state role/epoch."""
    settings = Settings(state_dir=temp_state_dir, n_shards=3)
    app = create_app(settings=settings)
    client = TestClient(app)

    # Register
    client.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s0-a",
            "shardId": 0,
            "endpoint": "http://s0-a:9000",
            "role": "PRIMARY",
            "epoch": 1,
            "lastSeq": 0,
            "status": "STARTING",
        },
    )

    # Send heartbeat with recordCount
    res = client.post(
        "/internal/nodes/heartbeat",
        json={
            "nodeId": "s0-a",
            "shardId": 0,
            "role": "PRIMARY",
            "epoch": 1,
            "status": "READY",
            "lastSeq": 42,
            "replicaAckSeq": 40,
            "recordCount": {"request": 10, "resource": 5, "other": 2},
        },
    )
    assert res.status_code == 200
    assert res.json() == {"role": "PRIMARY", "epoch": 1}

    # Verify registry record was updated
    reg = app.state.registry.get("s0-a")
    assert reg.last_seq == 42
    assert reg.replica_ack_seq == 40
    assert reg.record_count == {"request": 10, "resource": 5, "other": 2}


@pytest.mark.component
def test_health_up_then_down_with_fake_clock(temp_state_dir):
    """Verify derived health transitions from UP to DOWN with a fake clock."""
    current_time = 100.0

    def fake_clock():
        return current_time

    settings = Settings(
        state_dir=temp_state_dir,
        heartbeat_interval_sec=2.0,
        missed_heartbeats_limit=3,  # threshold = 6.0
    )
    app = create_app(settings=settings, clock=fake_clock)
    client = TestClient(app)

    # Register at t=100.0
    client.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s0-a",
            "shardId": 0,
            "endpoint": "http://s0-a:9000",
            "role": "PRIMARY",
            "epoch": 1,
            "lastSeq": 0,
            "status": "STARTING",
        },
    )

    # Derived health is UP (elapsed = 0 <= 6.0)
    assert app.state.registry.get_health("s0-a") == "UP"

    # Advance clock by 5s (elapsed = 5.0 <= 6.0) -> still UP
    current_time = 105.0
    app.state.health_checker.tick()
    assert app.state.registry.get_health("s0-a") == "UP"

    # Advance clock past threshold (elapsed = 7.0 > 6.0) -> probe fails -> DOWN
    current_time = 107.0
    app.state.health_checker.tick()
    assert app.state.registry.get_health("s0-a") == "DOWN"

    # Heartbeat arrives at t=107.0 -> UP again
    client.post(
        "/internal/nodes/heartbeat",
        json={
            "nodeId": "s0-a",
            "shardId": 0,
            "role": "PRIMARY",
            "epoch": 1,
            "status": "READY",
            "lastSeq": 1,
            "replicaAckSeq": None,
            "recordCount": {"request": 0, "resource": 0, "other": 0},
        },
    )
    assert app.state.registry.get_health("s0-a") == "UP"


@pytest.mark.component
def test_shard_state_survives_app_restart(temp_state_dir):
    """A new app instance on the same STATE_DIR reloads primary and epoch from disk."""
    settings = Settings(state_dir=temp_state_dir, n_shards=3)

    # App 1: s0-a becomes primary of shard 0 with epoch 3
    app1 = create_app(settings=settings)
    client1 = TestClient(app1)
    res = client1.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s0-a",
            "shardId": 0,
            "endpoint": "http://s0-a:9000",
            "role": "PRIMARY",
            "epoch": 3,
            "lastSeq": 0,
            "status": "STARTING",
        },
    )
    assert res.json() == {"role": "PRIMARY", "epoch": 3}

    # App 2: created on same state_dir (simulating coordinator restart)
    app2 = create_app(settings=settings)
    client2 = TestClient(app2)

    # In-memory registry is empty, but shard_state remembered primary
    assert app2.state.shard_state.get_primary(0) == "s0-a"
    assert app2.state.shard_state.get_epoch(0) == 3

    # Unknown node heartbeat gives 404 because registry is empty
    hb = client2.post(
        "/internal/nodes/heartbeat",
        json={
            "nodeId": "s0-a",
            "shardId": 0,
            "role": "PRIMARY",
            "epoch": 3,
            "status": "READY",
            "lastSeq": 0,
            "replicaAckSeq": None,
            "recordCount": {},
        },
    )
    assert hb.status_code == 404

    # When s0-b registers, it is recognized as REPLICA
    r_b = client2.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s0-b",
            "shardId": 0,
            "endpoint": "http://s0-b:9000",
            "role": "PRIMARY",  # hint is ignored since primary is already known
            "epoch": 1,
            "lastSeq": 0,
            "status": "STARTING",
        },
    )
    assert r_b.status_code == 200
    assert r_b.json() == {"role": "REPLICA", "epoch": 3}

    # When s0-a re-registers, it is recognized as PRIMARY with preserved epoch 3
    r_a = client2.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s0-a",
            "shardId": 0,
            "endpoint": "http://s0-a:9000",
            "role": "PRIMARY",
            "epoch": 1,
            "lastSeq": 0,
            "status": "STARTING",
        },
    )
    assert r_a.status_code == 200
    assert r_a.json() == {"role": "PRIMARY", "epoch": 3}


@pytest.mark.component
def test_corrupt_state_file_does_not_crash(temp_state_dir):
    """Corrupt shard_state.json does not crash the coordinator; it starts empty."""
    state_file = temp_state_dir / "shard_state.json"
    with open(state_file, "w", encoding="utf-8") as f:
        f.write("corrupt json {{{ not valid")

    settings = Settings(state_dir=temp_state_dir, n_shards=3)
    app = create_app(settings=settings)
    client = TestClient(app)

    # App functions normally
    res = client.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s2-a",
            "shardId": 2,
            "endpoint": "http://s2-a:9000",
            "role": "PRIMARY",
            "epoch": 1,
            "lastSeq": 0,
            "status": "STARTING",
        },
    )
    assert res.status_code == 200
    assert res.json() == {"role": "PRIMARY", "epoch": 1}


@pytest.mark.component
def test_health_endpoints(temp_state_dir):
    """Check /health, /health/live, and /health/ready endpoints."""
    settings = Settings(state_dir=temp_state_dir, n_shards=3)
    app = create_app(settings=settings)
    client = TestClient(app)

    r_live = client.get("/health/live")
    assert r_live.status_code == 200
    assert r_live.json() == {"status": "ok"}

    r_ready = client.get("/health/ready")
    assert r_ready.status_code == 200
    assert r_ready.json() == {"status": "ok"}

    r_health = client.get("/health")
    assert r_health.status_code == 200
    assert r_health.json()["status"] == "ok"
    assert r_health.json()["shards"] == 3
