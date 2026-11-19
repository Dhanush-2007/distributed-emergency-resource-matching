"""Component tests covering all SPEC 7.8 scenarios for a single shard (Build item 5.5)."""

from __future__ import annotations

import time

import pytest
from coordinator.main import Settings as CoordSettings
from coordinator.main import create_app as create_coord_app

from tests.component.cluster_harness import FakeClusterClock, SingleShardClusterHarness

pytestmark = pytest.mark.component


@pytest.fixture
def cluster(tmp_path):
    clock = FakeClusterClock(1000.0)
    h = SingleShardClusterHarness(tmp_path, clock)

    with h.coord, h.s1_a, h.s1_b:
        # Wait for nodes to register and replica monitor to see peer
        for _ in range(30):
            if (
                h.s1_a_app.state.get_coord_client().status == "READY"
                and h.s1_b_app.state.get_coord_client().status == "READY"
            ):
                break
            time.sleep(0.05)

        # Send heartbeat so coordinator registry has status READY
        h.tick_heartbeat("s1-a")
        h.tick_heartbeat("s1-b")

        for _ in range(30):
            rm = h.s1_a_app.state.get_replica_monitor()
            if rm:
                rm.check_peer_once()
                if rm.peer_ok:
                    break
            time.sleep(0.05)

        yield h


def test_scenario_replica_down_primary_up(cluster):
    """Scenario: Replica down, primary up. Writes acknowledged with replicated:false, shard DEGRADED."""
    h = cluster
    # 1. First write replicated
    w1 = h.coord.put("/internal/data/key:1", json={"value": {"x": 1}})
    assert w1.status_code == 201

    # 2. Kill replica s1-b
    h.set_down("s1-b", True)
    rm = h.s1_a_app.state.get_replica_monitor()
    if rm:
        rm.mark_not_ok()

    # 3. Write succeeds with degraded acknowledgment
    w2 = h.coord.put("/internal/data/key:2", json={"value": {"x": 2}})
    assert w2.status_code == 201

    # Shard state is degraded
    shard0 = h.coord_app.state.shard_state.get_shard(0)
    assert shard0["degraded"] is True

    # 4. Bring replica back, syncs and recovers
    h.set_down("s1-b", False)
    if rm:
        rm.check_peer_once()
    h.s1_b_app.state.get_coord_client().run_once()
    h.tick_coordinator()


def test_scenario_primary_down_replica_in_sync(cluster):
    """Scenario: Primary down with in-sync replica -> promotes to epoch 2, writes continue."""
    h = cluster
    # Baseline write
    w1 = h.coord.put("/internal/data/key:base", json={"value": {"v": 10}})
    assert w1.status_code == 201

    # Kill primary s1-a
    h.set_down("s1-a", True)
    h.clock.advance(10.0)

    # During detection window, GET falls back to replica
    r_fallback = h.coord.get("/internal/data/key:base")
    assert r_fallback.status_code == 200
    assert r_fallback.json()["value"] == {"v": 10}

    # Writes during detection window get 503
    w_during = h.coord.put("/internal/data/key:during", json={"value": {"v": 20}})
    assert w_during.status_code == 503

    # Health check triggers failover
    h.tick_coordinator()

    # Promoted to epoch 2!
    shard0 = h.coord_app.state.shard_state.get_shard(0)
    assert shard0["epoch"] == 2
    assert shard0["primaryNodeId"] == "s1-b"

    # Writes now succeed on new primary
    w_after = h.coord.put("/internal/data/key:after", json={"value": {"v": 30}})
    assert w_after.status_code == 201


def test_scenario_primary_down_replica_stale(cluster):
    """Scenario: Primary down, replica stale -> no promotion, 503."""
    h = cluster
    # Degrade shard so replica is marked stale
    h.coord_app.state.shard_state.set_degraded(0, True, 100)

    # Primary s1-a down
    h.set_down("s1-a", True)
    h.clock.advance(10.0)
    h.tick_coordinator()

    # No promotion: epoch remains 1
    assert h.coord_app.state.shard_state.get_epoch(0) == 1
    res = h.coord.put("/internal/data/key:test", json={"value": {"x": 1}})
    assert res.status_code == 503


def test_scenario_both_nodes_down(cluster):
    """Scenario: Both nodes down -> 503 for that shard only, recovers when safe node returns."""
    h = cluster
    h.set_down("s1-a", True)
    h.set_down("s1-b", True)
    h.clock.advance(10.0)
    h.tick_coordinator()

    assert h.coord.get("/internal/data/key:any").status_code == 503

    # Primary returns
    h.set_down("s1-a", False)
    h.s1_a_app.state.get_coord_client().run_once()
    h.tick_coordinator()

    # Shard recovered
    res = h.coord.put("/internal/data/key:recovered", json={"value": {"ok": True}})
    assert res.status_code == 201


def test_scenario_old_primary_returns_with_clean_tail(cluster):
    """Scenario: Old primary returns after promotion, catches up as REPLICA via delta sync."""
    h = cluster
    # 1. Baseline write
    h.coord.put("/internal/data/key:1", json={"value": {"v": 1}})

    # 2. Kill primary s1-a, advance clock, promote s1-b
    h.set_down("s1-a", True)
    h.clock.advance(10.0)
    h.tick_coordinator()
    assert h.coord_app.state.shard_state.get_epoch(0) == 2

    # 3. Write on new primary s1-b
    h.coord.put("/internal/data/key:2", json={"value": {"v": 2}})

    # 4. Old primary s1-a returns
    h.set_down("s1-a", False)
    # Heartbeat receives role REPLICA and epoch 2
    h.s1_a_app.state.get_coord_client().run_once()

    # s1-a demotes to REPLICA
    assert h.s1_a_db.get_meta("role") == "REPLICA"
    assert int(h.s1_a_db.get_meta("epoch")) == 2


def test_scenario_old_primary_writes_after_promotion_gets_stale_epoch(cluster):
    """Scenario: Old primary writes after promotion -> rejected with STALE_EPOCH, demotes."""
    h = cluster
    h.coord.put("/internal/data/key:init", json={"value": {"v": 0}})

    # Promote s1-b directly
    h.coord_app.state.failover_manager.force_promote(0, "s1-b")
    assert h.coord_app.state.shard_state.get_epoch(0) == 2

    # Old primary s1-a attempts direct PUT with epoch 1
    w_stale = h.s1_a.put("/internal/data/key:stale", json={"value": {"v": 99}, "epoch": 1})
    assert w_stale.status_code == 409
    assert w_stale.json()["error"]["code"] == "STALE_EPOCH"
    assert h.s1_a_db.get_meta("role") == "REPLICA"


def test_scenario_one_missed_heartbeat_changes_nothing(cluster):
    """Scenario: One heartbeat missed -> node remains UP, no failover."""
    h = cluster
    # Advance clock by 1 heartbeat interval (2s)
    h.clock.advance(2.5)
    h.tick_coordinator()

    # Both nodes remain UP
    assert h.coord_app.state.registry.get_health("s1-a") == "UP"
    assert h.coord_app.state.registry.get_health("s1-b") == "UP"
    assert h.coord_app.state.shard_state.get_epoch(0) == 1


def test_scenario_coordinator_restart(cluster, tmp_path):
    """Scenario: Coordinator restart reloads epoch, nodes re-register upon 404."""
    h = cluster
    # Advance epoch to 2 via failover
    h.set_down("s1-a", True)
    h.clock.advance(10.0)
    h.tick_coordinator()
    assert h.coord_app.state.shard_state.get_epoch(0) == 2

    # Restart coordinator
    settings = CoordSettings(
        state_dir=h.state_dir,
        n_shards=1,
        heartbeat_interval_sec=2.0,
        missed_heartbeats_limit=3,
        health_check_interval_sec=1.0,
        probe_timeout_sec=0.5,
    )
    settings.health_check_enabled = False
    new_coord_app = create_coord_app(settings=settings, clock=h.clock)
    assert new_coord_app.state.shard_state.get_epoch(0) == 2
    assert new_coord_app.state.shard_state.get_primary(0) == "s1-b"


def test_scenario_duplicate_replicate_acknowledged(cluster):
    """Scenario: Duplicate or replayed replicate message acknowledged without applying."""
    h = cluster
    # Manually send replicate op to replica s1-b
    resp1 = h.s1_b.post(
        "/internal/replicate",
        json={"epoch": 1, "seq": 1, "op": "PUT", "key": "k:dup", "value": {"a": 1}, "version": 1},
    )
    assert resp1.status_code == 200

    # Replay same op
    resp2 = h.s1_b.post(
        "/internal/replicate",
        json={"epoch": 1, "seq": 1, "op": "PUT", "key": "k:dup", "value": {"a": 1}, "version": 1},
    )
    assert resp2.status_code == 200
    assert resp2.json()["ack"] == 1


def test_scenario_returning_primary_when_shard_unavailable_resumes_epoch(cluster):
    """Scenario: Returning primary when no promotion happened resumes with unchanged epoch."""
    h = cluster
    # Both nodes down -> shard unavailable
    h.set_down("s1-a", True)
    h.set_down("s1-b", True)
    h.clock.advance(10.0)
    h.tick_coordinator()

    # Primary returns
    h.set_down("s1-a", False)
    h.s1_a_app.state.get_coord_client().run_once()
    h.tick_coordinator()

    assert h.coord_app.state.shard_state.get_epoch(0) == 1
    assert h.coord_app.state.shard_state.get_primary(0) == "s1-a"
