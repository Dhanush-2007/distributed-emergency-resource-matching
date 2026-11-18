"""Component tests for storage node database layer."""

import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from app.db import StorageDB
from ems_common.errors import NotFound, VersionConflict


@pytest.fixture
def temp_db_dir():
    d = tempfile.mkdtemp()
    yield Path(d)
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def db(temp_db_dir):
    db_file = temp_db_dir / "test.db"
    instance = StorageDB(db_file)
    yield instance
    instance.close()


@pytest.mark.component
def test_create_and_get(db):
    res = db.put("request:1", {"foo": "bar"}, if_version=0, epoch=1)
    assert res.version == 1
    assert res.seq == 1
    assert res.created is True

    record = db.get("request:1")
    assert record is not None
    assert record.key == "request:1"
    assert record.value == {"foo": "bar"}
    assert record.version == 1
    assert record.deleted == 0


@pytest.mark.component
def test_update_and_version_increment(db):
    db.put("request:1", {"val": 1}, if_version=0, epoch=1)
    res = db.put("request:1", {"val": 2}, if_version=1, epoch=1)
    assert res.version == 2
    assert res.seq == 2
    assert res.created is False

    record = db.get("request:1")
    assert record is not None
    assert record.value == {"val": 2}
    assert record.version == 2


@pytest.mark.component
def test_upsert(db):
    # if_version=None on absent key creates
    res1 = db.put("k1", {"step": 1}, if_version=None, epoch=1)
    assert res1.version == 1
    assert res1.created is True

    # if_version=None on existing key updates
    res2 = db.put("k1", {"step": 2}, if_version=None, epoch=1)
    assert res2.version == 2
    assert res2.created is False
    assert db.get("k1").value == {"step": 2}


@pytest.mark.component
def test_create_only_conflict(db):
    db.put("k1", {"val": 1}, if_version=0, epoch=1)
    with pytest.raises(VersionConflict):
        db.put("k1", {"val": 2}, if_version=0, epoch=1)


@pytest.mark.component
def test_version_conflict(db):
    db.put("k1", {"val": 1}, if_version=0, epoch=1)
    with pytest.raises(VersionConflict):
        db.put("k1", {"val": 2}, if_version=99, epoch=1)


@pytest.mark.component
def test_delete_and_get_returns_none(db):
    db.put("k1", {"val": 1}, if_version=0, epoch=1)
    del_res = db.delete("k1", epoch=1)
    assert del_res.version == 2
    assert del_res.seq == 2

    assert db.get("k1") is None

    # Deleting missing or already deleted key raises NotFound
    with pytest.raises(NotFound):
        db.delete("k1", epoch=1)

    with pytest.raises(NotFound):
        db.delete("k_nonexistent", epoch=1)


@pytest.mark.component
def test_create_over_tombstone(db):
    db.put("k1", {"val": 1}, if_version=0, epoch=1)  # v1, seq1
    db.delete("k1", epoch=1)                         # v2, seq2

    # Create over tombstone with if_version=0
    res = db.put("k1", {"val": 2}, if_version=0, epoch=1)
    assert res.version == 3
    assert res.seq == 3
    assert res.created is True

    record = db.get("k1")
    assert record is not None
    assert record.value == {"val": 2}
    assert record.version == 3


@pytest.mark.component
def test_scan_with_prefix(db):
    db.put("resource:water:1", {"name": "w1"}, if_version=None, epoch=1)
    db.put("resource:water:2", {"name": "w2"}, if_version=None, epoch=1)
    db.put("resource:food:1", {"name": "f1"}, if_version=None, epoch=1)
    db.put("request:1", {"help": "water"}, if_version=None, epoch=1)

    # Delete one
    db.delete("resource:water:2", epoch=1)

    water_resources = db.scan("resource:water:")
    assert len(water_resources) == 1
    assert water_resources[0].key == "resource:water:1"
    assert water_resources[0].value == {"name": "w1"}

    all_resources = db.scan("resource:")
    assert len(all_resources) == 2
    keys = [r.key for r in all_resources]
    assert keys == ["resource:food:1", "resource:water:1"]


@pytest.mark.component
def test_seq_contiguity_and_stats(db):
    db.put("k1", {"v": 1}, if_version=None, epoch=1)  # seq 1
    db.put("k2", {"v": 2}, if_version=None, epoch=1)  # seq 2
    db.delete("k1", epoch=1)                          # seq 3
    db.put("k3", {"v": 3}, if_version=None, epoch=1)  # seq 4

    stats = db.stats()
    assert stats["records"] == 2  # k2 and k3 (k1 is deleted)
    assert stats["last_seq"] == 4
    assert stats["epoch"] == 1
    assert stats["role"] == "PRIMARY"


@pytest.mark.component
def test_reopening_db_keeps_data_and_last_seq(temp_db_dir):
    db_file = temp_db_dir / "persist.db"

    db1 = StorageDB(db_file)
    db1.put("k1", {"a": 10}, if_version=0, epoch=1)
    db1.put("k2", {"b": 20}, if_version=0, epoch=1)
    db1.close()

    db2 = StorageDB(db_file)
    assert db2.get("k1").value == {"a": 10}
    assert db2.get("k2").value == {"b": 20}
    stats = db2.stats()
    assert stats["last_seq"] == 2
    assert stats["records"] == 2

    # Next write gets seq 3
    res = db2.put("k3", {"c": 30}, if_version=0, epoch=1)
    assert res.seq == 3
    db2.close()


@pytest.mark.component
def test_50_threads_conditional_updates_single_winner(temp_db_dir):
    db_file = temp_db_dir / "concurrent.db"
    storage = StorageDB(db_file)

    # Initial record at version 1
    storage.put("shared_key", {"counter": 0}, if_version=0, epoch=1)

    winners = []
    conflicts = []

    def try_update(thread_id: int):
        try:
            # Everyone tries to update from version 1 to next version
            res = storage.put(
                "shared_key",
                {"counter": thread_id},
                if_version=1,
                epoch=1,
            )
            winners.append((thread_id, res))
        except VersionConflict as exc:
            conflicts.append((thread_id, exc))

    with ThreadPoolExecutor(max_workers=50) as executor:
        futures = [executor.submit(try_update, i) for i in range(50)]
        for f in futures:
            f.result()

    # Exactly one thread should win
    assert len(winners) == 1
    # 49 threads should get VersionConflict
    assert len(conflicts) == 49

    record = storage.get("shared_key")
    assert record.version == 2
    assert record.value == {"counter": winners[0][0]}

    storage.close()
