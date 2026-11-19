"""Component tests for sync and snapshot endpoints (Build item 4.2)."""

import pytest
from fastapi.testclient import TestClient
from storage_node.db import StorageDB
from storage_node.main import create_app

pytestmark = pytest.mark.component


@pytest.fixture
def primary_app(tmp_path):
    db_path = tmp_path / "primary_sync.db"
    db = StorageDB(db_path, oplog_retain=5)
    db.update_meta(role="PRIMARY", epoch=1)

    app = create_app(node_id="s0-a", shard_id=0, db_instance=db, standalone=True)
    client = TestClient(app)
    return db, client


def test_sync_pagination_and_caught_up(primary_app):
    db, client = primary_app

    for i in range(1, 11):
        db.put(f"key:{i}", {"v": i}, if_version=None, epoch=1)

    # Empty result when caught up
    res_caught_up = client.get("/internal/sync", params={"since": 10, "epoch": 1})
    assert res_caught_up.status_code == 200
    data = res_caught_up.json()
    assert data["snapshotRequired"] is False
    assert len(data["entries"]) == 0
    assert data["lastSeq"] == 10

    # Paginate with limit
    # Note: oplog_retain=5 pruned entries with seq <= 10 - 5 = 5.
    # Entries 6..10 remain in oplog.
    # When since=6, seq 6 is present in oplog and entry_epoch_at(6) == 1.
    res_page = client.get("/internal/sync", params={"since": 6, "epoch": 1, "limit": 2})
    assert res_page.status_code == 200
    pdata = res_page.json()
    assert pdata["snapshotRequired"] is False
    assert len(pdata["entries"]) == 2
    assert pdata["entries"][0]["seq"] == 7
    assert pdata["entries"][1]["seq"] == 8
    assert pdata["fromSeq"] == 6
    assert pdata["toSeq"] == 8


def test_pruning_and_snapshot_required_on_truncated_log(primary_app):
    db, client = primary_app
    # Write 10 entries with oplog_retain=5
    for i in range(1, 11):
        db.put(f"key:{i}", {"v": i}, if_version=None, epoch=1)

    # since=2 was pruned from oplog
    res = client.get("/internal/sync", params={"since": 2, "epoch": 1})
    assert res.status_code == 200
    assert res.json()["snapshotRequired"] is True
    assert res.json()["entries"] == []


def test_snapshot_required_on_epoch_mismatch_and_future_seq(primary_app):
    db, client = primary_app
    for i in range(1, 6):
        db.put(f"key:{i}", {"v": i}, if_version=None, epoch=1)

    # since=3 exists but follower claims epoch=2
    res_epoch_mismatch = client.get("/internal/sync", params={"since": 3, "epoch": 2})
    assert res_epoch_mismatch.status_code == 200
    assert res_epoch_mismatch.json()["snapshotRequired"] is True

    # since=99 > lastSeq (5)
    res_future = client.get("/internal/sync", params={"since": 99, "epoch": 1})
    assert res_future.status_code == 200
    assert res_future.json()["snapshotRequired"] is True


def test_sync_and_snapshot_not_primary_on_replica(tmp_path):
    db = StorageDB(tmp_path / "replica_sync.db")
    db.update_meta(role="REPLICA", epoch=1)

    app = create_app(node_id="s0-b", shard_id=0, db_instance=db, standalone=True)
    client = TestClient(app)

    res_sync = client.get("/internal/sync", params={"since": 0, "epoch": 0})
    assert res_sync.status_code == 409
    assert res_sync.json()["error"]["code"] == "NOT_PRIMARY"

    res_snap = client.get("/internal/snapshot")
    assert res_snap.status_code == 409
    assert res_snap.json()["error"]["code"] == "NOT_PRIMARY"


def test_snapshot_export_round_trip(primary_app):
    db, client = primary_app
    db.put("k1", {"x": 10}, if_version=None, epoch=1)
    db.put("k2", {"y": 20}, if_version=None, epoch=1)
    db.delete("k1", epoch=1)

    res = client.get("/internal/snapshot")
    assert res.status_code == 200
    snap = res.json()
    assert snap["lastSeq"] == 3
    assert snap["epoch"] == 1
    assert len(snap["records"]) == 2

    # Check tombstones included
    k1_rec = next(r for r in snap["records"] if r["key"] == "k1")
    assert k1_rec["deleted"] == 1
    assert k1_rec["version"] == 2

    k2_rec = next(r for r in snap["records"] if r["key"] == "k2")
    assert k2_rec["deleted"] == 0
    assert k2_rec["value"] == {"y": 20}
    assert k2_rec["version"] == 1
