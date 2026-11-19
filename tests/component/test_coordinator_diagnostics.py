"""Component tests for coordinator diagnostics endpoints."""

import shutil
import tempfile
from pathlib import Path

import pytest
from coordinator.main import Settings, create_app
from ems_common.sharding import shard_of
from fastapi.testclient import TestClient

from tests.component.fakes import FakeStorageCluster


@pytest.fixture
def temp_state_dir():
    d = tempfile.mkdtemp()
    yield Path(d)
    shutil.rmtree(d, ignore_errors=True)


@pytest.mark.component
def test_diagnostics_route_matches_data_routing(temp_state_dir):
    """GET /diagnostics/route returns the exact same shard as data routing without touching data."""
    node_configs = [
        ("s0-a", 0, "http://s0-a:9000"),
        ("s1-a", 1, "http://s1-a:9000"),
        ("s2-a", 2, "http://s2-a:9000"),
    ]
    cluster = FakeStorageCluster(node_configs)
    http_client = cluster.get_client()

    settings = Settings(state_dir=temp_state_dir, n_shards=3)
    app = create_app(settings=settings, http_client=http_client)
    client = TestClient(app)

    # Register all 3 primaries
    for node_id, shard_id, endpoint in node_configs:
        client.post(
            "/internal/nodes/register",
            json={
                "nodeId": node_id,
                "shardId": shard_id,
                "endpoint": endpoint,
                "role": "PRIMARY",
                "epoch": 1,
                "lastSeq": 0,
                "status": "READY",
            },
        )

    # Check multiple keys
    for i in range(15):
        key = f"request:{i}:uuid"
        expected_shard = shard_of(key, 3)

        res = client.get(f"/diagnostics/route?key={key}")
        assert res.status_code == 200
        data = res.json()
        assert data["key"] == key
        assert data["shardId"] == expected_shard
        assert data["role"] == "PRIMARY"
        assert data["health"] == "UP"
        assert data["nodeId"] == f"s{expected_shard}-a"
        assert data["endpoint"] == f"http://s{expected_shard}-a:9000"

    http_client.close()


@pytest.mark.component
def test_diagnostics_route_400_when_key_is_missing(temp_state_dir):
    """Missing or empty key returns 400 INVALID_INPUT in the standard error envelope."""
    settings = Settings(state_dir=temp_state_dir, n_shards=3)
    app = create_app(settings=settings)
    client = TestClient(app)

    # Omitted key parameter
    r1 = client.get("/diagnostics/route")
    assert r1.status_code == 400
    assert r1.json()["error"]["code"] == "INVALID_INPUT"

    # Empty key parameter
    r2 = client.get("/diagnostics/route?key=")
    assert r2.status_code == 400
    assert r2.json()["error"]["code"] == "INVALID_INPUT"

    # Whitespace key parameter
    r3 = client.get("/diagnostics/route?key=%20%20")
    assert r3.status_code == 400
    assert r3.json()["error"]["code"] == "INVALID_INPUT"


@pytest.mark.component
def test_nodes_and_shards_response_shapes(temp_state_dir):
    """Verify exact response shapes of GET /diagnostics/nodes and /diagnostics/shards."""
    node_configs = [
        ("s0-a", 0, "http://s0-a:9000"),
        ("s0-b", 0, "http://s0-b:9000"),
    ]
    cluster = FakeStorageCluster(node_configs)
    http_client = cluster.get_client()

    settings = Settings(state_dir=temp_state_dir, n_shards=3)
    app = create_app(settings=settings, http_client=http_client)
    client = TestClient(app)

    # Register primary s0-a
    client.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s0-a",
            "shardId": 0,
            "endpoint": "http://s0-a:9000",
            "role": "PRIMARY",
            "epoch": 2,
            "lastSeq": 10,
            "status": "READY",
        },
    )
    # Register replica s0-b
    client.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s0-b",
            "shardId": 0,
            "endpoint": "http://s0-b:9000",
            "role": "REPLICA",
            "epoch": 2,
            "lastSeq": 8,
            "status": "READY",
        },
    )

    # Test /diagnostics/nodes shape
    r_nodes = client.get("/diagnostics/nodes")
    assert r_nodes.status_code == 200
    nodes = r_nodes.json()
    assert isinstance(nodes, list)
    assert len(nodes) == 2

    node_map = {n["nodeId"]: n for n in nodes}
    n0 = node_map["s0-a"]
    expected_node_keys = {
        "nodeId",
        "shardId",
        "endpoint",
        "role",
        "epoch",
        "health",
        "status",
        "lastHeartbeatAgeSec",
        "lastSeq",
        "recordCount",
    }
    assert set(n0.keys()) == expected_node_keys
    assert n0["role"] == "PRIMARY"
    assert n0["health"] == "UP"
    assert n0["status"] == "READY"
    assert isinstance(n0["lastHeartbeatAgeSec"], (int, float))

    # Test /diagnostics/shards shape
    r_shards = client.get("/diagnostics/shards")
    assert r_shards.status_code == 200
    shards = r_shards.json()
    assert isinstance(shards, list)
    assert len(shards) == 3  # N_SHARDS=3

    shard0 = shards[0]
    expected_shard_keys = {
        "shardId",
        "state",
        "epoch",
        "primary",
        "records",
        "recordCount",
        "nodes",
    }
    assert set(shard0.keys()) == expected_shard_keys
    assert shard0["shardId"] == 0
    assert shard0["state"] == "HEALTHY"
    assert shard0["epoch"] == 2
    assert shard0["primary"] == "s0-a"
    assert isinstance(shard0["nodes"], list)
    assert len(shard0["nodes"]) == 2
    assert any(n["id"] == "s0-a" and n["role"] == "PRIMARY" for n in shard0["nodes"])
    assert any(n["id"] == "s0-b" and n["role"] == "REPLICA" for n in shard0["nodes"])

    http_client.close()


@pytest.mark.component
def test_shard_state_flips_healthy_to_unavailable_on_stale_heartbeat(temp_state_dir):
    """Shard state flips from HEALTHY to UNAVAILABLE when primary heartbeat goes stale."""
    current_time = 100.0

    def fake_clock():
        return current_time

    settings = Settings(
        state_dir=temp_state_dir,
        n_shards=3,
        heartbeat_interval_sec=2.0,
        missed_heartbeats_limit=3,  # threshold = 6.0
    )
    app = create_app(settings=settings, clock=fake_clock)
    client = TestClient(app)

    # Register s0-a at t=100.0
    client.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s0-a",
            "shardId": 0,
            "endpoint": "http://s0-a:9000",
            "role": "PRIMARY",
            "epoch": 1,
            "lastSeq": 0,
            "status": "READY",
        },
    )

    # At t=100.0, shard 0 is HEALTHY
    shards = client.get("/diagnostics/shards").json()
    assert shards[0]["state"] == "HEALTHY"

    # Advance clock to t=105.0 (still within 6s threshold)
    current_time = 105.0
    shards = client.get("/diagnostics/shards").json()
    assert shards[0]["state"] == "HEALTHY"

    # Advance clock past threshold: t=107.0 (> 100.0 + 6.0) -> UNAVAILABLE
    current_time = 107.0
    shards = client.get("/diagnostics/shards").json()
    assert shards[0]["state"] == "UNAVAILABLE"

    # Receive heartbeat at t=107.0 -> HEALTHY again
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
    shards = client.get("/diagnostics/shards").json()
    assert shards[0]["state"] == "HEALTHY"


@pytest.mark.component
def test_counts_update_after_heartbeat(temp_state_dir):
    """Shard diagnostics records and recordCount update after receiving a heartbeat."""
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
            "status": "READY",
        },
    )

    # Send heartbeat with recordCount
    client.post(
        "/internal/nodes/heartbeat",
        json={
            "nodeId": "s0-a",
            "shardId": 0,
            "role": "PRIMARY",
            "epoch": 1,
            "status": "READY",
            "lastSeq": 25,
            "replicaAckSeq": None,
            "recordCount": {"request": 15, "resource": 8, "other": 3},
        },
    )

    shards = client.get("/diagnostics/shards").json()
    shard0 = shards[0]
    assert shard0["recordCount"] == {"request": 15, "resource": 8, "other": 3}
    assert shard0["records"] == 26


@pytest.mark.component
def test_unregistered_shard_shape(temp_state_dir):
    """If a shard has no registered primary, return shardId, UNAVAILABLE, null primary, zero counts."""
    settings = Settings(state_dir=temp_state_dir, n_shards=3)
    app = create_app(settings=settings)
    client = TestClient(app)

    # Only register shard 0
    client.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s0-a",
            "shardId": 0,
            "endpoint": "http://s0-a:9000",
            "role": "PRIMARY",
            "epoch": 1,
            "lastSeq": 0,
            "status": "READY",
        },
    )

    shards = client.get("/diagnostics/shards").json()
    assert len(shards) == 3

    # Shard 1 has no registered primary
    shard1 = shards[1]
    assert shard1["shardId"] == 1
    assert shard1["state"] == "UNAVAILABLE"
    assert shard1["primary"] is None
    assert shard1["records"] == 0
    assert shard1["recordCount"] == {"request": 0, "resource": 0, "other": 0}
    assert shard1["nodes"] == []

    # Shard 2 has no registered primary
    shard2 = shards[2]
    assert shard2["shardId"] == 2
    assert shard2["state"] == "UNAVAILABLE"
    assert shard2["primary"] is None
    assert shard2["records"] == 0
    assert shard2["recordCount"] == {"request": 0, "resource": 0, "other": 0}
    assert shard2["nodes"] == []
