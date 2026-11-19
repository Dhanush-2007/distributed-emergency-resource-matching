"""Unit tests for pure shard health and recovery evaluation functions (Build item 4.5)."""

import pytest
from coordinator.registry import NodeRecord
from coordinator.shard_health import clear_degraded_if_caught_up, derive_shard_state

pytestmark = pytest.mark.unit


def _make_node(
    node_id: str,
    role: str,
    status: str = "READY",
    last_seq: int = 10,
    last_heartbeat: float = 100.0,
) -> NodeRecord:
    return NodeRecord(
        node_id=node_id,
        shard_id=0,
        endpoint=f"http://{node_id}:9000",
        role=role,
        epoch=1,
        status=status,
        last_seq=last_seq,
        last_heartbeat=last_heartbeat,
    )


class TestDeriveShardState:
    def test_primary_none_is_unavailable(self):
        state = derive_shard_state(
            shard_info={"degraded": False},
            primary_node=None,
            replica_node=None,
            now=100.0,
        )
        assert state == "UNAVAILABLE"

    def test_primary_timed_out_is_unavailable(self):
        # threshold is 3 * 2.0 = 6.0. Last heartbeat at 90.0, now is 100.0 (> 6.0)
        p = _make_node("p", role="PRIMARY", last_heartbeat=90.0)
        r = _make_node("r", role="REPLICA", last_heartbeat=100.0)
        state = derive_shard_state(
            shard_info={"degraded": False},
            primary_node=p,
            replica_node=r,
            now=100.0,
        )
        assert state == "UNAVAILABLE"

    def test_primary_not_ready_is_unavailable(self):
        p = _make_node("p", role="PRIMARY", status="STARTING", last_heartbeat=100.0)
        r = _make_node("r", role="REPLICA", status="READY", last_heartbeat=100.0)
        state = derive_shard_state(
            shard_info={"degraded": False},
            primary_node=p,
            replica_node=r,
            now=100.0,
        )
        assert state == "UNAVAILABLE"

    def test_replica_none_is_degraded(self):
        p = _make_node("p", role="PRIMARY", last_heartbeat=100.0)
        state = derive_shard_state(
            shard_info={"degraded": False},
            primary_node=p,
            replica_node=None,
            now=100.0,
        )
        assert state == "DEGRADED"

    def test_replica_timed_out_is_degraded(self):
        p = _make_node("p", role="PRIMARY", last_heartbeat=100.0)
        r = _make_node("r", role="REPLICA", last_heartbeat=90.0)
        state = derive_shard_state(
            shard_info={"degraded": False},
            primary_node=p,
            replica_node=r,
            now=100.0,
        )
        assert state == "DEGRADED"

    def test_replica_not_ready_is_degraded(self):
        p = _make_node("p", role="PRIMARY", status="READY", last_heartbeat=100.0)
        r = _make_node("r", role="REPLICA", status="CATCHING_UP", last_heartbeat=100.0)
        state = derive_shard_state(
            shard_info={"degraded": False},
            primary_node=p,
            replica_node=r,
            now=100.0,
        )
        assert state == "DEGRADED"

    def test_replica_seq_mismatch_is_degraded(self):
        p = _make_node("p", role="PRIMARY", last_seq=10, last_heartbeat=100.0)
        r = _make_node("r", role="REPLICA", last_seq=8, last_heartbeat=100.0)
        state = derive_shard_state(
            shard_info={"degraded": False},
            primary_node=p,
            replica_node=r,
            now=100.0,
        )
        assert state == "DEGRADED"

    def test_shard_marked_degraded_is_degraded(self):
        p = _make_node("p", role="PRIMARY", last_seq=10, last_heartbeat=100.0)
        r = _make_node("r", role="REPLICA", last_seq=10, last_heartbeat=100.0)
        state = derive_shard_state(
            shard_info={"degraded": True, "degradedSeq": 10},
            primary_node=p,
            replica_node=r,
            now=100.0,
        )
        assert state == "DEGRADED"

    def test_all_conditions_met_is_healthy(self):
        p = _make_node("p", role="PRIMARY", last_seq=10, last_heartbeat=100.0)
        r = _make_node("r", role="REPLICA", last_seq=10, last_heartbeat=100.0)
        state = derive_shard_state(
            shard_info={"degraded": False},
            primary_node=p,
            replica_node=r,
            now=100.0,
        )
        assert state == "HEALTHY"


class TestClearDegradedIfCaughtUp:
    def test_not_degraded_remains_unchanged(self):
        info = {"degraded": False, "degradedSeq": 0}
        p = _make_node("p", role="PRIMARY", last_seq=10)
        r = _make_node("r", role="REPLICA", last_seq=10)
        res = clear_degraded_if_caught_up(info, p, r, now=100.0)
        assert res["degraded"] is False

    def test_primary_none_does_not_clear(self):
        info = {"degraded": True, "degradedSeq": 5}
        r = _make_node("r", role="REPLICA", last_seq=10)
        res = clear_degraded_if_caught_up(info, None, r, now=100.0)
        assert res["degraded"] is True

    def test_replica_none_does_not_clear(self):
        info = {"degraded": True, "degradedSeq": 5}
        p = _make_node("p", role="PRIMARY", last_seq=10)
        res = clear_degraded_if_caught_up(info, p, None, now=100.0)
        assert res["degraded"] is True

    def test_replica_down_does_not_clear(self):
        info = {"degraded": True, "degradedSeq": 5}
        p = _make_node("p", role="PRIMARY", last_seq=10, last_heartbeat=100.0)
        r = _make_node("r", role="REPLICA", last_seq=10, last_heartbeat=90.0)
        res = clear_degraded_if_caught_up(info, p, r, now=100.0)
        assert res["degraded"] is True

    def test_replica_not_ready_does_not_clear(self):
        info = {"degraded": True, "degradedSeq": 5}
        p = _make_node("p", role="PRIMARY", last_seq=10, last_heartbeat=100.0)
        r = _make_node(
            "r", role="REPLICA", status="CATCHING_UP", last_seq=10, last_heartbeat=100.0
        )
        res = clear_degraded_if_caught_up(info, p, r, now=100.0)
        assert res["degraded"] is True

    def test_replica_seq_below_degraded_seq_does_not_clear(self):
        info = {"degraded": True, "degradedSeq": 15}
        p = _make_node("p", role="PRIMARY", last_seq=10, last_heartbeat=100.0)
        r = _make_node("r", role="REPLICA", last_seq=10, last_heartbeat=100.0)
        res = clear_degraded_if_caught_up(info, p, r, now=100.0)
        assert res["degraded"] is True

    def test_replica_seq_not_equal_to_primary_does_not_clear(self):
        info = {"degraded": True, "degradedSeq": 5}
        p = _make_node("p", role="PRIMARY", last_seq=12, last_heartbeat=100.0)
        r = _make_node("r", role="REPLICA", last_seq=10, last_heartbeat=100.0)
        res = clear_degraded_if_caught_up(info, p, r, now=100.0)
        assert res["degraded"] is True

    def test_all_conditions_satisfied_clears_degraded(self):
        info = {"degraded": True, "degradedSeq": 5}
        p = _make_node("p", role="PRIMARY", last_seq=12, last_heartbeat=100.0)
        r = _make_node("r", role="REPLICA", last_seq=12, last_heartbeat=100.0)
        res = clear_degraded_if_caught_up(info, p, r, now=100.0)
        assert res["degraded"] is False
