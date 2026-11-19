"""Component tests for replica read fallback (Build item 5.4)."""

from __future__ import annotations

import logging

import pytest
from coordinator.main import Settings, create_app
from ems_common.sharding import shard_of
from starlette.testclient import TestClient

from tests.component.fakes import FakeStorageCluster

pytestmark = pytest.mark.component


def key_for_shard(sid: int, n_shards: int = 2) -> str:
    for i in range(100):
        k = f"item:{i}"
        if shard_of(k, n_shards) == sid:
            return k
    raise RuntimeError(f"No key found for shard {sid}")


@pytest.fixture
def fallback_env(tmp_path):
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
    )
    settings.health_check_enabled = False

    app = create_app(settings=settings, http_client=http_client)
    client = TestClient(app)

    # Register shard 0 nodes
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

    # Register shard 1 nodes
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
    }


def test_fallback_succeeds_during_detection_window(fallback_env, caplog):
    """Primary is UP in registry but dead; GET falls back to replica and logs route.fallback."""
    client = fallback_env["client"]
    cluster = fallback_env["cluster"]

    k0 = key_for_shard(0, 2)
    # Populate key in replica s0-b
    cluster.dbs["s0-b"].data[k0] = {"value": {"msg": "from_replica"}, "version": 1, "deleted": False}

    # Primary s0-a goes dead (connection error), but health checker has not marked it DOWN yet
    cluster.set_down("s0-a", True)

    with caplog.at_level(logging.INFO):
        res = client.get(f"/internal/data/{k0}")
    assert res.status_code == 200
    assert res.json()["value"] == {"msg": "from_replica"}

    # route.fallback logged
    fallback_records = [r for r in caplog.records if getattr(r, "event", None) == "route.fallback"]
    assert len(fallback_records) >= 1
    assert fallback_records[0].nodeId == "s0-b"


def test_no_fallback_when_shard_degraded(fallback_env):
    """When shard is degraded, no read fallback occurs and returns 503."""
    app = fallback_env["app"]
    client = fallback_env["client"]
    cluster = fallback_env["cluster"]

    k0 = key_for_shard(0, 2)
    cluster.dbs["s0-b"].data[k0] = {"value": {"msg": "stale"}, "version": 1, "deleted": False}

    # Set shard degraded
    app.state.shard_state.set_degraded(0, True, 10)

    # Primary s0-a dead
    cluster.set_down("s0-a", True)

    res = client.get(f"/internal/data/{k0}")
    assert res.status_code == 503
    assert res.headers.get("Retry-After") == "3"


def test_no_fallback_when_replica_catching_up(fallback_env):
    """When replica is in CATCHING_UP status, no fallback occurs and returns 503."""
    client = fallback_env["client"]
    cluster = fallback_env["cluster"]

    k0 = key_for_shard(0, 2)
    cluster.dbs["s0-b"].data[k0] = {"value": {"msg": "stale"}, "version": 1, "deleted": False}

    # Update replica to CATCHING_UP
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
    res = client.get(f"/internal/data/{k0}")
    assert res.status_code == 503
    assert res.headers.get("Retry-After") == "3"


def test_writes_get_503_without_retry(fallback_env):
    """Writes to dead primary get 503 and never retry on replica."""
    client = fallback_env["client"]
    cluster = fallback_env["cluster"]

    k0 = key_for_shard(0, 2)
    cluster.set_down("s0-a", True)

    res_put = client.put(f"/internal/data/{k0}", json={"value": {"val": 1}})
    assert res_put.status_code == 503

    res_del = client.delete(f"/internal/data/{k0}")
    assert res_del.status_code == 503

    # Replica s0-b was never touched
    assert k0 not in cluster.dbs["s0-b"].data


def test_scan_fallback_succeeds(fallback_env, caplog):
    """Scan falls back to in-sync replica when primary is dead."""
    client = fallback_env["client"]
    cluster = fallback_env["cluster"]

    # Shard 0 primary s0-a dead, replica s0-b has item
    k0 = key_for_shard(0, 2)
    k1 = key_for_shard(1, 2)

    cluster.dbs["s0-b"].data[k0] = {"value": {"data": 0}, "version": 1, "deleted": False}
    cluster.dbs["s1-a"].data[k1] = {"value": {"data": 1}, "version": 1, "deleted": False}

    cluster.set_down("s0-a", True)

    with caplog.at_level(logging.INFO):
        res = client.get("/internal/scan?prefix=item:")
    assert res.status_code == 200
    body = res.json()
    assert body["count"] == 2
    assert body["partial"] is False
    keys = [item["key"] for item in body["items"]]
    assert k0 in keys
    assert k1 in keys

    # route.fallback logged for SCAN
    fallback_records = [
        r for r in caplog.records
        if getattr(r, "event", None) == "route.fallback" and getattr(r, "op", None) == "SCAN"
    ]
    assert len(fallback_records) >= 1


def test_replica_read_returns_404_for_missing_key(fallback_env):
    """When primary is dead and replica does not have the key, return 404."""
    client = fallback_env["client"]
    cluster = fallback_env["cluster"]

    k0 = key_for_shard(0, 2)
    cluster.set_down("s0-a", True)

    res = client.get(f"/internal/data/{k0}")
    assert res.status_code == 404
    assert res.json()["error"]["code"] == "NOT_FOUND"
