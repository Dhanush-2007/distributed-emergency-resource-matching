"""Component tests for snapshot installation (Build item 4.2)."""

import pytest
from storage_node.db import StorageDB

pytestmark = pytest.mark.component


def test_install_snapshot_and_entry_epoch_at(tmp_path):
    replica_db_path = tmp_path / "replica_snap.db"
    replica_db = StorageDB(replica_db_path)
    replica_db.update_meta(role="REPLICA", epoch=1)

    snapshot_payload = {
        "lastSeq": 42,
        "epoch": 3,
        "lastLogEpoch": 3,
        "records": [
            {
                "key": "req:100",
                "value": {"status": "MATCHED"},
                "version": 4,
                "deleted": 0,
                "updatedAt": "2026-11-19T12:00:00Z",
            },
            {
                "key": "req:101",
                "value": {},
                "version": 2,
                "deleted": 1,
                "updatedAt": "2026-11-19T12:01:00Z",
            },
        ],
    }

    replica_db.install_snapshot(snapshot_payload)

    stats = replica_db.stats()
    assert stats["last_seq"] == 42
    assert stats["epoch"] == 3
    assert stats["role"] == "REPLICA"

    # Live record accessible
    rec = replica_db.get("req:100")
    assert rec is not None
    assert rec.value == {"status": "MATCHED"}
    assert rec.version == 4

    # Deleted record not returned by get
    assert replica_db.get("req:101") is None

    # entry_epoch_at works for snapshot_seq
    assert replica_db.entry_epoch_at(42) == 3
    assert replica_db.entry_epoch_at(0) == 0
    assert replica_db.entry_epoch_at(10) is None


def test_install_snapshot_takes_max_epoch(tmp_path):
    replica_db = StorageDB(tmp_path / "replica_max_epoch.db")
    replica_db.update_meta(role="REPLICA", epoch=5)

    snap = {
        "lastSeq": 10,
        "epoch": 2,
        "lastLogEpoch": 2,
        "records": [],
    }
    replica_db.install_snapshot(snap)

    assert replica_db.stats()["epoch"] == 5
