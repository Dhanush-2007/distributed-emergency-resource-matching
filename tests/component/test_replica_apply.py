"""Component tests for replica apply path (Build item 4.1)."""

import pytest
from fastapi.testclient import TestClient
from storage_node.db import StorageDB
from storage_node.main import create_app

pytestmark = pytest.mark.component


@pytest.fixture
def replica_setup(tmp_path):
    db_path = tmp_path / "replica.db"
    db = StorageDB(db_path)
    db.update_meta(role="REPLICA", epoch=1)

    app = create_app(
        node_id="s0-b",
        shard_id=0,
        db_instance=db,
        standalone=True,
    )
    client = TestClient(app)
    return db, client, db_path


def test_in_order_apply_and_duplicate(replica_setup):
    db, client, _ = replica_setup

    payload1 = {
        "epoch": 1,
        "seq": 1,
        "op": "PUT",
        "key": "request:001",
        "value": {"status": "PENDING"},
        "version": 1,
    }
    res = client.post("/internal/replicate", json=payload1)
    assert res.status_code == 200
    data = res.json()
    assert data["ack"] == 1
    assert data["lastSeq"] == 1
    assert data["result"] == "APPLIED"

    rec = db.get("request:001")
    assert rec is not None
    assert rec.value == {"status": "PENDING"}
    assert rec.version == 1

    # Duplicate acknowledged without change
    res_dup = client.post("/internal/replicate", json=payload1)
    assert res_dup.status_code == 200
    assert res_dup.json()["result"] == "DUPLICATE"


def test_gap_detection(replica_setup):
    _db, client, _ = replica_setup

    payload = {
        "epoch": 1,
        "seq": 5,
        "op": "PUT",
        "key": "request:002",
        "value": {"status": "PENDING"},
        "version": 1,
    }
    res = client.post("/internal/replicate", json=payload)
    assert res.status_code == 409
    body = res.json()
    assert body["error"]["code"] == "GAP"
    assert body["error"]["details"] == [{"lastSeq": 0}]


def test_stale_epoch_rejected(replica_setup):
    db, client, _ = replica_setup
    db.update_meta(epoch=2)

    payload = {
        "epoch": 1,
        "seq": 1,
        "op": "PUT",
        "key": "request:003",
        "value": {"test": 1},
        "version": 1,
    }
    res = client.post("/internal/replicate", json=payload)
    assert res.status_code == 409
    body = res.json()
    assert body["error"]["code"] == "STALE_EPOCH"
    assert body["error"]["details"] == [{"epoch": 2}]


def test_higher_epoch_adopted(replica_setup):
    db, client, _ = replica_setup

    payload = {
        "epoch": 3,
        "seq": 1,
        "op": "PUT",
        "key": "request:004",
        "value": {"test": 1},
        "version": 1,
    }
    res = client.post("/internal/replicate", json=payload)
    assert res.status_code == 200
    assert db.stats()["epoch"] == 3


def test_not_replica_on_primary(tmp_path):
    db_path = tmp_path / "primary.db"
    db = StorageDB(db_path)
    db.update_meta(role="PRIMARY", epoch=1)

    app = create_app(
        node_id="s0-a",
        shard_id=0,
        db_instance=db,
        standalone=True,
    )
    client = TestClient(app)

    payload = {
        "epoch": 1,
        "seq": 1,
        "op": "PUT",
        "key": "request:005",
        "value": {"test": 1},
        "version": 1,
    }
    res = client.post("/internal/replicate", json=payload)
    assert res.status_code == 409
    assert res.json()["error"]["code"] == "NOT_REPLICA"


def test_tombstone_replicates_and_get_404(replica_setup):
    _db, client, _ = replica_setup

    client.post(
        "/internal/replicate",
        json={
            "epoch": 1,
            "seq": 1,
            "op": "PUT",
            "key": "resource:001",
            "value": {"type": "ambulance"},
            "version": 1,
        },
    )
    assert client.get("/internal/data/resource:001").status_code == 200

    del_res = client.post(
        "/internal/replicate",
        json={
            "epoch": 1,
            "seq": 2,
            "op": "DELETE",
            "key": "resource:001",
            "value": None,
            "version": 2,
        },
    )
    assert del_res.status_code == 200

    get_res = client.get("/internal/data/resource:001")
    assert get_res.status_code == 404
    assert get_res.json()["error"]["code"] == "NOT_FOUND"


def test_replica_rejects_put_delete_allows_get_scan(replica_setup):
    _, client, _ = replica_setup

    put_res = client.put("/internal/data/request:001", json={"value": {"x": 1}})
    assert put_res.status_code == 409
    assert put_res.json()["error"]["code"] == "NOT_PRIMARY"

    del_res = client.delete("/internal/data/request:001")
    assert del_res.status_code == 409
    assert del_res.json()["error"]["code"] == "NOT_PRIMARY"

    scan_res = client.get("/internal/scan")
    assert scan_res.status_code == 200


def test_20_mixed_operations_and_duplicate_replay(tmp_path):
    primary_db = StorageDB(tmp_path / "p.db")
    replica_db = StorageDB(tmp_path / "r.db")
    replica_db.update_meta(role="REPLICA", epoch=1)

    app = create_app(node_id="s0-b", shard_id=0, db_instance=replica_db, standalone=True)
    client = TestClient(app)

    ops = []
    for i in range(1, 21):
        if i % 4 == 0:
            key = f"key:{i - 1}"
            res = primary_db.delete(key, epoch=1)
            op = {"epoch": 1, "seq": res.seq, "op": "DELETE", "key": key, "value": None, "version": res.version}
        else:
            key = f"key:{i}"
            val = {"val": i}
            res = primary_db.put(key, val, if_version=None, epoch=1)
            op = {"epoch": 1, "seq": res.seq, "op": "PUT", "key": key, "value": val, "version": res.version}
        ops.append(op)

    for op in ops:
        res = client.post("/internal/replicate", json=op)
        assert res.status_code == 200

    # Compare oplogs
    p_oplog = primary_db.oplog_since(0, limit=100)
    r_oplog = replica_db.oplog_since(0, limit=100)
    assert len(p_oplog) == 20
    assert len(r_oplog) == 20
    for p_entry, r_entry in zip(p_oplog, r_oplog, strict=False):
        assert p_entry["seq"] == r_entry["seq"]
        assert p_entry["op"] == r_entry["op"]
        assert p_entry["key"] == r_entry["key"]
        assert p_entry["version"] == r_entry["version"]
        assert p_entry["value"] == r_entry["value"]

    # Replay all 20 ops
    for op in ops:
        res = client.post("/internal/replicate", json=op)
        assert res.status_code == 200
        assert res.json()["result"] == "DUPLICATE"

    # Verify identical records (key, value, version, deleted)
    p_snap = primary_db.export_snapshot()
    r_snap = replica_db.export_snapshot()
    assert p_snap["lastSeq"] == r_snap["lastSeq"]
    assert len(p_snap["records"]) == len(r_snap["records"])
    for pr, rr in zip(p_snap["records"], r_snap["records"], strict=False):
        assert pr["key"] == rr["key"]
        assert pr["value"] == rr["value"]
        assert pr["version"] == rr["version"]
        assert pr["deleted"] == rr["deleted"]


def test_initial_role_and_reopen_retention(tmp_path, monkeypatch):
    db_file = tmp_path / "boot.db"
    monkeypatch.setenv("INITIAL_ROLE", "replica")
    db1 = StorageDB(db_file)
    assert db1.stats()["role"] == "REPLICA"
    db1.update_meta(epoch=5)
    db1.close()

    # Reopen with INITIAL_ROLE=primary: must keep REPLICA and epoch=5
    monkeypatch.setenv("INITIAL_ROLE", "primary")
    db2 = StorageDB(db_file)
    st = db2.stats()
    assert st["role"] == "REPLICA"
    assert st["epoch"] == 5
    db2.close()


def test_malformed_replicate_body_400(replica_setup):
    _, client, _ = replica_setup
    res = client.post(
        "/internal/replicate",
        content="not-json",
        headers={"Content-Type": "application/json"},
    )
    assert res.status_code == 400
    assert res.json()["error"]["code"] == "INVALID_INPUT"
