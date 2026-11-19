"""Component tests for follower recovery (Build item 4.4)."""

import time

import pytest

from tests.component.pair import StoragePairHarness

pytestmark = pytest.mark.component


def test_empty_replica_and_primary_writes_converge(tmp_path):
    h = StoragePairHarness(tmp_path)
    # Write 30 entries to primary while replica is not yet started
    for i in range(1, 31):
        h.primary_db.put(f"key:{i}", {"v": i}, if_version=None, epoch=1)

    assert h.primary_db.stats()["last_seq"] == 30
    assert h.replica_db.stats()["last_seq"] == 0

    with h.p_client as pc, h.r_client as rc:
        h.p_client = pc
        h.r_client = rc
        # Wait for replica to catch up via recovery
        time.sleep(0.5)
        # Check convergence
        assert h.replica_db.stats()["last_seq"] == 30
        assert h.r_client.get("/health").json()["status"] == "READY"
        assert h.r_client.get("/health/ready").status_code == 200


def test_snapshot_when_log_is_truncated(tmp_path):
    h = StoragePairHarness(tmp_path)
    h.primary_db.oplog_retain = 5
    for i in range(1, 31):
        h.primary_db.put(f"item:{i}", {"v": i}, if_version=None, epoch=1)

    # Primary oplog only has entries 26..30. Replica is at seq 0.
    # Follower recovery must detect snapshotRequired and install snapshot.
    with h.p_client as pc, h.r_client as rc:
        h.p_client = pc
        h.r_client = rc
        time.sleep(0.5)
        assert h.replica_db.stats()["last_seq"] == 30
        assert h.r_client.get("/health").json()["status"] == "READY"
        rec = h.replica_db.get("item:10")
        assert rec is not None
        assert rec.value == {"v": 10}


def test_snapshot_when_replica_has_divergent_tail(tmp_path):
    h = StoragePairHarness(tmp_path)
    # Replica has a write at epoch 1
    h.replica_db.apply_replicated(epoch=1, seq=1, op="PUT", key="k:old", value={"old": True}, version=1)

    # Primary has advanced to epoch 2
    h.primary_db.update_meta(epoch=2)
    h.primary_db.put("k:new", {"new": True}, if_version=None, epoch=2)

    with h.p_client as pc, h.r_client as rc:
        h.p_client = pc
        h.r_client = rc
        time.sleep(0.5)
        assert h.replica_db.stats()["epoch"] == 2
        assert h.replica_db.get("k:new") is not None


def test_gap_triggers_immediate_sync(tmp_path):
    h = StoragePairHarness(tmp_path)
    with h.p_client as pc, h.r_client as rc:
        h.p_client = pc
        h.r_client = rc

        # Write seq 1 and seq 2 on primary directly without replication
        h.primary_db.put("k:1", {"x": 1}, if_version=None, epoch=1)
        h.primary_db.put("k:2", {"x": 2}, if_version=None, epoch=1)

        # Send seq 3 directly to replica /internal/replicate -> triggers 409 GAP
        gap_res = h.r_client.post(
            "/internal/replicate",
            json={"epoch": 1, "seq": 3, "op": "PUT", "key": "k:3", "value": {"x": 3}, "version": 1},
        )
        assert gap_res.status_code == 409
        assert gap_res.json()["error"]["code"] == "GAP"

        # The 409 GAP triggers immediate recovery sync pull
        time.sleep(0.5)
        assert h.replica_db.stats()["last_seq"] == 2
