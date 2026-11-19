"""Component tests for storage role endpoint and fencing (Build item 5.2)."""

import time

import pytest

from tests.component.pair import StoragePairHarness

pytestmark = pytest.mark.component


def test_promote_ready_replica_accepts_writes_with_new_epoch(tmp_path):
    """Promoting a READY replica to PRIMARY updates epoch, accepts new writes, rejects old epoch."""
    h = StoragePairHarness(tmp_path)
    with h.p_client as pc, h.r_client as rc:
        # Initial write to primary
        w1 = pc.put("/internal/data/key:1", json={"value": {"v": 1}, "epoch": 1})
        assert w1.status_code == 201

        # Wait for replica to catch up to READY
        for _ in range(20):
            if rc.get("/health/ready").status_code == 200:
                break
            time.sleep(0.1)
        assert rc.get("/health/ready").status_code == 200

        # Promote replica s0-b to PRIMARY with epoch 2
        p_resp = rc.post("/internal/role", json={"role": "PRIMARY", "epoch": 2})
        assert p_resp.status_code == 200
        data = p_resp.json()
        assert data["role"] == "PRIMARY"
        assert data["epoch"] == 2

        # Replica now accepts writes with epoch 2
        w2 = rc.put("/internal/data/key:2", json={"value": {"v": 2}, "epoch": 2})
        assert w2.status_code == 201

        # Old epoch 1 is rejected with 409 STALE_EPOCH
        w_old = rc.put("/internal/data/key:3", json={"value": {"v": 3}, "epoch": 1})
        assert w_old.status_code == 409
        assert w_old.json()["error"]["code"] == "STALE_EPOCH"
        h.close()


def test_promote_while_catching_up_returns_409_not_ready(tmp_path):
    """Cannot promote a replica to PRIMARY while its status is CATCHING_UP."""
    h = StoragePairHarness(tmp_path)
    with h.r_client as rc:
        # Force catching up status via test client
        rc.app.state.manual_status = "CATCHING_UP"
        p_res = rc.post("/internal/role", json={"role": "PRIMARY", "epoch": 2})
        assert p_res.status_code == 409
        assert p_res.json()["error"]["code"] == "NOT_READY"
        h.close()


def test_old_primary_demotes_self_when_replicate_gets_stale_epoch(tmp_path):
    """Old primary's in-flight write gets STALE_EPOCH from peer and demotes itself."""
    h = StoragePairHarness(tmp_path)
    with h.p_client as pc, h.r_client as rc:
        # Wait for replica to be ready and primary replica monitor to see peer_ok
        for _ in range(30):
            rm = h.primary_app.state.get_replica_monitor()
            if rm:
                rm.check_peer_once()
                if rm.peer_ok:
                    break
            time.sleep(0.05)

        # 1. First write succeeds at epoch 1 and is replicated
        w1 = pc.put("/internal/data/key:1", json={"value": {"v": 1}, "epoch": 1})
        assert w1.status_code == 201
        assert w1.json()["replicated"] is True

        # Wait for replica to be ready
        for _ in range(20):
            if rc.get("/health/ready").status_code == 200:
                break
            time.sleep(0.05)

        # 2. Promote replica to PRIMARY with epoch 2
        r_prom = rc.post("/internal/role", json={"role": "PRIMARY", "epoch": 2})
        assert r_prom.status_code == 200

        # 3. Old primary attempts write at epoch 1.
        # It replicates to s0-b, which is now at epoch 2 and rejects with 409 STALE_EPOCH.
        # Primary demotes itself and returns 409 STALE_EPOCH unacknowledged!
        w_stale = pc.put("/internal/data/key:stale", json={"value": {"v": 99}, "epoch": 1})
        assert w_stale.status_code == 409
        assert w_stale.json()["error"]["code"] == "STALE_EPOCH"

        # 4. Old primary is now REPLICA!
        assert h.primary_db.get_meta("role") == "REPLICA"
        assert int(h.primary_db.get_meta("epoch")) == 2

        # 5. Subsequent writes to old primary return 409 NOT_PRIMARY
        w_sub = pc.put("/internal/data/key:sub", json={"value": {"v": 100}, "epoch": 2})
        assert w_sub.status_code == 409
        assert w_sub.json()["error"]["code"] == "NOT_PRIMARY"

        h.close()


def test_heartbeat_response_demotes_primary(tmp_path):
    """When coordinator heartbeat response returns role REPLICA, primary demotes itself."""
    h = StoragePairHarness(tmp_path)
    # Configure fake coordinator to tell primary it is now REPLICA with epoch 2
    h.fake_coord.heartbeat_override = {"role": "REPLICA", "epoch": 2}

    with h.p_client:
        # Trigger heartbeat on coordinator_client
        h.p_client.app.state.coord_client = getattr(h.p_client.app.state, "coord_client", None)
        # Call coordinator client's _heartbeat method
        # Wait a brief moment or trigger heartbeat directly
        for _ in range(20):
            if h.primary_db.get_meta("role") == "REPLICA":
                break
            time.sleep(0.2)

        assert h.primary_db.get_meta("role") == "REPLICA"
        assert int(h.primary_db.get_meta("epoch")) == 2
        h.close()


def test_restarted_node_stored_primary_becomes_replica_by_coordinator(tmp_path):
    """Restarted node with stored PRIMARY adopts REPLICA when coordinator says so."""
    h = StoragePairHarness(tmp_path)
    # Configure fake coordinator to respond to register with REPLICA, epoch 3
    h.fake_coord.register_override = {"role": "REPLICA", "epoch": 3}

    # Primary starts with stored role PRIMARY
    h.primary_db.update_meta(role="PRIMARY", epoch=1)

    with h.p_client:
        for _ in range(20):
            if h.primary_db.get_meta("role") == "REPLICA":
                break
            time.sleep(0.1)

        assert h.primary_db.get_meta("role") == "REPLICA"
        assert int(h.primary_db.get_meta("epoch")) == 3
        h.close()


def test_role_endpoint_idempotency(tmp_path):
    """Same role and epoch again is idempotent and returns 200."""
    h = StoragePairHarness(tmp_path)
    with h.r_client as rc:
        # Demote to REPLICA epoch 1
        r1 = rc.post("/internal/role", json={"role": "REPLICA", "epoch": 1})
        assert r1.status_code == 200

        # Send same role and epoch again
        r2 = rc.post("/internal/role", json={"role": "REPLICA", "epoch": 1})
        assert r2.status_code == 200
        assert r2.json()["role"] == "REPLICA"
        assert r2.json()["epoch"] == 1
        h.close()


def test_stale_role_epoch_rejected(tmp_path):
    """Requesting a lower epoch returns 409 STALE_EPOCH."""
    h = StoragePairHarness(tmp_path)
    h.replica_db.update_meta(role="REPLICA", epoch=3)

    with h.r_client as rc:
        res = rc.post("/internal/role", json={"role": "PRIMARY", "epoch": 2})
        assert res.status_code == 409
        assert res.json()["error"]["code"] == "STALE_EPOCH"
        h.close()


def test_stale_epoch_replication_no_deadlock_regression(tmp_path):
    """Primary receiving STALE_EPOCH from replica demotes without deadlock and finishes < 5s."""
    t0 = time.time()
    h = StoragePairHarness(tmp_path)
    with h.p_client as pc, h.r_client as rc:
        for _ in range(30):
            rm = h.primary_app.state.get_replica_monitor()
            if rm:
                rm.check_peer_once()
                if rm.peer_ok:
                    break
            time.sleep(0.05)

        # Baseline write
        w1 = pc.put("/internal/data/key:reg1", json={"value": {"x": 1}, "epoch": 1})
        assert w1.status_code == 201

        # Promote peer to higher epoch 2
        r_prom = rc.post("/internal/role", json={"role": "PRIMARY", "epoch": 2})
        assert r_prom.status_code == 200

        # In-flight write to old primary receives STALE_EPOCH, releases write lock, demotes
        w2 = pc.put("/internal/data/key:reg2", json={"value": {"x": 2}, "epoch": 1})
        assert w2.status_code == 409
        assert w2.json()["error"]["code"] == "STALE_EPOCH"

        # Demoted to REPLICA with adopted epoch 2
        assert h.primary_db.get_meta("role") == "REPLICA"
        assert int(h.primary_db.get_meta("epoch")) == 2
        h.close()

    duration = time.time() - t0
    assert duration < 5.0, f"Test took {duration:.2f}s, exceeding 5s limit"
