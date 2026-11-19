"""Component tests for coordinator routing to shard primaries."""

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


@pytest.fixture
def cluster_and_app(temp_state_dir):
    """Sets up a 3-shard cluster and coordinator app with primaries registered."""
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

    # Register all 3 primaries with status READY
    for node_id, shard_id, endpoint in node_configs:
        res = client.post(
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
        assert res.status_code == 200

    yield cluster, client, app
    http_client.close()


@pytest.mark.component
def test_same_key_maps_to_same_shard():
    """Verify sharding determinism: same key always maps to the same shard."""
    key = "request:3f2c9a1e-0001"
    sid = shard_of(key, 3)
    for _ in range(20):
        assert shard_of(key, 3) == sid


@pytest.mark.component
def test_keys_spread_over_all_three_nodes(cluster_and_app):
    """Verify that multiple keys route across all three fake storage nodes."""
    cluster, client, _ = cluster_and_app

    # Write keys until each shard has data
    for i in range(30):
        key = f"resource:{i}"
        client.put(f"/internal/data/{key}", json={"value": {"index": i}})

    # Check each shard's database has received records
    for node_id in ("s0-a", "s1-a", "s2-a"):
        assert len(cluster.dbs[node_id].data) > 0


@pytest.mark.component
def test_put_201_then_200(cluster_and_app):
    """PUT returns 201 for new records, then 200 for updates."""
    _, client, _ = cluster_and_app
    key = "request:demo-uuid-1"

    # Create -> 201
    r1 = client.put(f"/internal/data/{key}", json={"value": {"status": "PENDING"}})
    assert r1.status_code == 201
    b1 = r1.json()
    assert b1["key"] == key
    assert b1["version"] == 1

    # Update -> 200
    r2 = client.put(f"/internal/data/{key}", json={"value": {"status": "MATCHED"}})
    assert r2.status_code == 200
    b2 = r2.json()
    assert b2["key"] == key
    assert b2["version"] == 2


@pytest.mark.component
def test_if_version_conflict_passes_409_through(cluster_and_app):
    """ifVersion conflicts from storage nodes pass through as 409 VERSION_CONFLICT."""
    _, client, _ = cluster_and_app
    key = "request:conflict-key"

    # Create with ifVersion=0
    client.put(f"/internal/data/{key}", json={"value": {"x": 1}, "ifVersion": 0})

    # ifVersion=0 on existing key -> 409
    res = client.put(f"/internal/data/{key}", json={"value": {"x": 2}, "ifVersion": 0})
    assert res.status_code == 409
    assert res.json()["error"]["code"] == "VERSION_CONFLICT"

    # Stale version -> 409
    res_stale = client.put(f"/internal/data/{key}", json={"value": {"x": 3}, "ifVersion": 99})
    assert res_stale.status_code == 409
    assert res_stale.json()["error"]["code"] == "VERSION_CONFLICT"


@pytest.mark.component
def test_get_and_delete_behavior(cluster_and_app):
    """GET and DELETE forward correctly and missing keys return 404 NOT_FOUND."""
    _, client, _ = cluster_and_app
    key = "resource:del-test"

    # GET missing key -> 404
    r_miss = client.get(f"/internal/data/{key}")
    assert r_miss.status_code == 404
    assert r_miss.json()["error"]["code"] == "NOT_FOUND"

    # PUT
    client.put(f"/internal/data/{key}", json={"value": {"type": "ambulance"}})

    # GET existing
    r_get = client.get(f"/internal/data/{key}")
    assert r_get.status_code == 200
    assert r_get.json()["value"] == {"type": "ambulance"}

    # DELETE
    r_del = client.delete(f"/internal/data/{key}")
    assert r_del.status_code == 200
    assert r_del.json()["key"] == key

    # GET deleted -> 404
    r_del_get = client.get(f"/internal/data/{key}")
    assert r_del_get.status_code == 404


@pytest.mark.component
def test_scan_merges_across_shards_and_is_sorted(cluster_and_app):
    """Scan queries all 3 shards, merges results, and sorts by key."""
    _, client, _ = cluster_and_app

    # Put keys in different shards
    client.put("/internal/data/item:c", json={"value": {"val": 3}})
    client.put("/internal/data/item:a", json={"value": {"val": 1}})
    client.put("/internal/data/item:b", json={"value": {"val": 2}})
    client.put("/internal/data/other:x", json={"value": {"val": 99}})

    res = client.get("/internal/scan?prefix=item:")
    assert res.status_code == 200
    data = res.json()
    assert data["count"] == 3
    assert data["partial"] is False
    keys = [item["key"] for item in data["items"]]
    assert keys == ["item:a", "item:b", "item:c"]


@pytest.mark.component
def test_scan_503_when_down_and_partial_when_allow_partial(cluster_and_app):
    """Scan returns 503 when a node is down, but partial results when allowPartial=true."""
    cluster, client, _ = cluster_and_app

    client.put("/internal/data/item:1", json={"value": {}})
    client.put("/internal/data/item:2", json={"value": {}})

    # Take down node s1-a
    cluster.set_down("s1-a", True)

    # allowPartial=false (default) -> 503
    r_fail = client.get("/internal/scan?prefix=item:")
    assert r_fail.status_code == 503
    assert r_fail.headers.get("Retry-After") == "3"
    assert r_fail.json()["error"]["code"] == "STORAGE_UNAVAILABLE"

    # allowPartial=true -> 200 with partial=True and unavailableShards
    r_part = client.get("/internal/scan?prefix=item:&allowPartial=true")
    assert r_part.status_code == 200
    data = r_part.json()
    assert data["partial"] is True
    assert 1 in data["unavailableShards"]


@pytest.mark.component
def test_503_retry_after_when_primary_unregistered_or_down(temp_state_dir):
    """503 STORAGE_UNAVAILABLE + Retry-After: 3 when primary is unregistered or DOWN."""
    current_time = 100.0

    def fake_clock():
        return current_time

    node_configs = [("s0-a", 0, "http://s0-a:9000")]
    cluster = FakeStorageCluster(node_configs)
    http_client = cluster.get_client()

    settings = Settings(
        state_dir=temp_state_dir,
        n_shards=3,
        heartbeat_interval_sec=2.0,
        missed_heartbeats_limit=3,  # threshold = 6.0
    )
    app = create_app(settings=settings, http_client=http_client, clock=fake_clock)
    client = TestClient(app)

    # Shard 0 primary is not registered yet -> 503
    # Find a key that maps to shard 0
    k0 = None
    for i in range(100):
        k = f"k:{i}"
        if shard_of(k, 3) == 0:
            k0 = k
            break
    assert k0 is not None

    r_unreg = client.put(f"/internal/data/{k0}", json={"value": {}})
    assert r_unreg.status_code == 503
    assert r_unreg.headers.get("Retry-After") == "3"
    assert r_unreg.json()["error"]["code"] == "STORAGE_UNAVAILABLE"

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

    # Now it succeeds at t=100.0
    r_ok = client.put(f"/internal/data/{k0}", json={"value": {"active": True}})
    assert r_ok.status_code == 201

    # Advance clock past heartbeat timeout (t=107.0 > 100.0 + 6.0) -> node health is DOWN
    current_time = 107.0
    r_down = client.get(f"/internal/data/{k0}")
    assert r_down.status_code == 503
    assert r_down.headers.get("Retry-After") == "3"
    assert r_down.json()["error"]["code"] == "STORAGE_UNAVAILABLE"

    http_client.close()


@pytest.mark.component
def test_node_409_stale_epoch_passes_through(cluster_and_app):
    """Storage node 409 STALE_EPOCH passes through to the client."""
    cluster, client, _ = cluster_and_app

    # Storage node s0-a epoch is 2 in DB, but coordinator thinks epoch is 1
    cluster.dbs["s0-a"].epoch = 2

    # Find key for shard 0
    k0 = None
    for i in range(100):
        k = f"key:{i}"
        if shard_of(k, 3) == 0:
            k0 = k
            break

    res = client.put(f"/internal/data/{k0}", json={"value": {"x": 1}})
    assert res.status_code == 409
    assert res.json()["error"]["code"] == "STALE_EPOCH"


@pytest.mark.component
def test_data_responses_never_contain_node_endpoint(cluster_and_app):
    """Ensure internal node endpoints are never exposed in success or error bodies."""
    cluster, client, _ = cluster_and_app
    key = "request:privacy-test"

    # Success PUT
    r_put = client.put(f"/internal/data/{key}", json={"value": {"sensitive": "data"}})
    assert "9000" not in r_put.text
    assert "s0-a" not in r_put.text
    assert "http://" not in r_put.text

    # Success GET
    r_get = client.get(f"/internal/data/{key}")
    assert "http://" not in r_get.text

    # 404 GET
    r_404 = client.get("/internal/data/request:nonexistent-uuid")
    assert "http://" not in r_404.text

    # 503 Error
    cluster.set_down("s0-a", True)
    cluster.set_down("s1-a", True)
    cluster.set_down("s2-a", True)
    r_503 = client.put(f"/internal/data/{key}", json={"value": {"retry": 1}})
    assert "http://" not in r_503.text
