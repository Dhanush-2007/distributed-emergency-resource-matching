"""Component tests for follower recovery (Build item 4.4)."""

import threading
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

        # Make replica unable to sync during the first part of the test
        rm = h.replica_app.state.get_recovery_manager()
        sync_gate = threading.Event()
        orig_get = rm._http.get

        def gated_get(*args, **kwargs):
            while not sync_gate.is_set() and not rm._stop_event.is_set():
                sync_gate.wait(timeout=0.05)
            return orig_get(*args, **kwargs)

        rm._http.get = gated_get

        try:
            # Write seq 1 and seq 2 on primary only
            h.primary_db.put("k:1", {"x": 1}, if_version=None, epoch=1)
            h.primary_db.put("k:2", {"x": 2}, if_version=None, epoch=1)
            assert h.primary_db.stats()["last_seq"] == 2
            replica_last_seq = h.replica_db.stats()["last_seq"]
            assert replica_last_seq == 0

            # Send seq 4 (a true gap) to /internal/replicate and assert 409 GAP
            gap_res = h.r_client.post(
                "/internal/replicate",
                json={"epoch": 1, "seq": 4, "op": "PUT", "key": "k:4", "value": {"x": 4}, "version": 1},
            )
            assert gap_res.status_code == 409
            assert gap_res.json()["error"]["code"] == "GAP"
            assert gap_res.json()["error"]["details"] == [{"lastSeq": replica_last_seq}]

            # Assert nothing changed on replica
            assert h.replica_db.stats()["last_seq"] == replica_last_seq
            assert h.replica_db.get("k:4") is None

            # Let sync run and assert replica converges to primary
            sync_gate.set()
            rm.trigger_sync()

            for _ in range(50):
                if h.replica_db.stats()["last_seq"] == h.primary_db.stats()["last_seq"]:
                    break
                time.sleep(0.05)

            assert h.replica_db.stats()["last_seq"] == h.primary_db.stats()["last_seq"]
            assert h.replica_db.stats()["last_seq"] == 2
            rec1 = h.replica_db.get("k:1")
            rec2 = h.replica_db.get("k:2")
            assert rec1 is not None and rec1.value == {"x": 1}
            assert rec2 is not None and rec2.value == {"x": 2}
            assert h.replica_db.get("k:4") is None
        finally:
            sync_gate.set()
            rm._http.get = orig_get
