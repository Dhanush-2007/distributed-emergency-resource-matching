"""Component tests for primary replication and degraded-write rule (Build item 4.3)."""

from concurrent.futures import ThreadPoolExecutor

import pytest

from tests.component.pair import StoragePairHarness

pytestmark = pytest.mark.component


@pytest.fixture
def pair_harness(tmp_path):
    h = StoragePairHarness(tmp_path)
    # Enter context managers so background threads start and nodes register
    with h.p_client as pc, h.r_client as rc:
        h.p_client = pc
        h.r_client = rc
        # Wait for replica to finish initial catch-up and become READY, and primary probe it
        import time
        start = time.time()
        while time.time() - start < 3.0:
            rm = h.primary_app.state.get_replica_monitor()
            if rm and rm.peer_ok:
                break
            if rm:
                rm.check_peer_once()
            time.sleep(0.05)
        yield h


def test_both_healthy_replicates_true(pair_harness):
    h = pair_harness
    res = h.p_client.put(
        "/internal/data/request:001",
        json={"value": {"status": "PENDING"}},
    )
    assert res.status_code == 201
    body = res.json()
    assert body["replicated"] is True
    assert body["version"] == 1
    assert body["seq"] == 1

    # Replica has the record and identical lastSeq
    assert h.primary_db.stats()["last_seq"] == 1
    assert h.replica_db.stats()["last_seq"] == 1
    rec = h.replica_db.get("request:001")
    assert rec is not None
    assert rec.value == {"status": "PENDING"}


def test_replica_down_records_degraded_before_acknowledging(pair_harness):
    h = pair_harness

    # Put a key when replica was healthy to establish baseline
    h.p_client.put("/internal/data/request:001", json={"value": {"x": 1}})

    # Now make replica return 500 or unreachable by marking its role OFFLINE / peer not ok
    h.replica_db.update_meta(role="OFFLINE")

    h.fake_coord.calls.clear()
    res = h.p_client.put(
        "/internal/data/request:002",
        json={"value": {"x": 2}},
    )
    assert res.status_code == 201
    assert res.json()["replicated"] is False

    # Check call order: degraded call to coordinator was recorded
    deg_calls = [c for c in h.fake_coord.calls if c[0] == "/internal/nodes/degraded"]
    assert len(deg_calls) >= 1
    assert deg_calls[0][1]["nodeId"] == "s0-a"
    assert deg_calls[0][1]["lastSeq"] == 2


def test_coordinator_unreachable_while_degraded_returns_503(pair_harness):
    h = pair_harness
    h.replica_db.update_meta(role="OFFLINE")
    h.fake_coord.should_fail = True

    res = h.p_client.put(
        "/internal/data/request:003",
        json={"value": {"x": 3}},
    )
    assert res.status_code == 503
    assert res.json()["error"]["code"] == "STORAGE_UNAVAILABLE"


def test_delete_replicates_tombstone(pair_harness):
    h = pair_harness
    h.p_client.put("/internal/data/resource:10", json={"value": {"type": "van"}})
    assert h.r_client.get("/internal/data/resource:10").status_code == 200

    del_res = h.p_client.delete("/internal/data/resource:10")
    assert del_res.status_code == 200
    assert del_res.json()["replicated"] is True

    # Replica GET now returns 404
    assert h.r_client.get("/internal/data/resource:10").status_code == 404


def test_if_version_conflict_does_not_replicate(pair_harness):
    h = pair_harness
    h.p_client.put("/internal/data/req:conflict", json={"value": {"x": 1}})
    r_seq_before = h.replica_db.stats()["last_seq"]

    # Conflicting PUT with ifVersion=0
    res = h.p_client.put(
        "/internal/data/req:conflict",
        json={"value": {"x": 2}, "ifVersion": 0},
    )
    assert res.status_code == 409
    assert res.json()["error"]["code"] == "VERSION_CONFLICT"

    # Replica seq unchanged
    assert h.replica_db.stats()["last_seq"] == r_seq_before


def test_50_concurrent_writers_keep_primary_and_replica_identical(pair_harness):
    h = pair_harness

    def write_worker(idx):
        key = f"key:concurrent:{idx}"
        return h.p_client.put(f"/internal/data/{key}", json={"value": {"idx": idx}})

    with ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(write_worker, range(50)))

    for r in results:
        assert r.status_code in (200, 201)

    p_stats = h.primary_db.stats()
    r_stats = h.replica_db.stats()
    assert p_stats["last_seq"] == 50
    assert r_stats["last_seq"] == 50

    p_records = h.primary_db.scan("")
    r_records = h.replica_db.scan("")
    assert len(p_records) == 50
    assert len(r_records) == 50
    for pr, rr in zip(p_records, r_records, strict=False):
        assert pr.key == rr.key
        assert pr.value == rr.value
        assert pr.version == rr.version
