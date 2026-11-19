"""Pure functions for evaluating shard health and recovery state."""

from __future__ import annotations

from typing import Any


def _is_node_up(
    node: Any,
    now: float,
    missed_heartbeats_limit: int,
    heartbeat_interval_sec: float,
) -> bool:
    if node is None:
        return False
    if hasattr(node, "health") and callable(node.health):
        return node.health(now, heartbeat_interval_sec, missed_heartbeats_limit) == "UP"
    if isinstance(node, dict):
        if "health" in node and node["health"] is not None:
            return node["health"] == "UP"
        if "lastHeartbeat" in node:
            return (now - float(node["lastHeartbeat"])) <= (
                missed_heartbeats_limit * heartbeat_interval_sec
            )
        return True
    if hasattr(node, "last_heartbeat"):
        return (now - float(node.last_heartbeat)) <= (
            missed_heartbeats_limit * heartbeat_interval_sec
        )
    if hasattr(node, "health"):
        return node.health == "UP"
    return False


def _get_status(node: Any) -> str | None:
    if node is None:
        return None
    if isinstance(node, dict):
        return node.get("status")
    return getattr(node, "status", None)


def _get_role(node: Any) -> str | None:
    if node is None:
        return None
    if isinstance(node, dict):
        return node.get("role")
    return getattr(node, "role", None)


def _get_last_seq(node: Any) -> int | None:
    if node is None:
        return None
    if isinstance(node, dict):
        val = node.get("lastSeq", node.get("last_seq"))
        return int(val) if val is not None else None
    val = getattr(node, "last_seq", getattr(node, "lastSeq", None))
    return int(val) if val is not None else None


def derive_shard_state(
    shard_info: dict[str, Any] | None,
    primary_node: Any,
    replica_node: Any,
    now: float,
    missed_heartbeats_limit: int = 3,
    heartbeat_interval_sec: float = 2.0,
) -> str:
    """Derive shard state: HEALTHY | DEGRADED | UNAVAILABLE.

    - HEALTHY: primary UP+READY, replica UP+READY, lastSeq equal, not degraded.
    - DEGRADED: a primary that is UP+READY serves but the replica is missing or not in sync.
    - UNAVAILABLE: no primary that is UP and READY.
    """
    if not _is_node_up(primary_node, now, missed_heartbeats_limit, heartbeat_interval_sec):
        return "UNAVAILABLE"
    if _get_status(primary_node) != "READY":
        return "UNAVAILABLE"
    if _get_role(primary_node) not in (None, "PRIMARY"):
        return "UNAVAILABLE"

    # Primary is UP and READY
    if not _is_node_up(replica_node, now, missed_heartbeats_limit, heartbeat_interval_sec):
        return "DEGRADED"
    if _get_status(replica_node) != "READY":
        return "DEGRADED"

    p_seq = _get_last_seq(primary_node)
    r_seq = _get_last_seq(replica_node)
    if p_seq is None or r_seq is None or p_seq != r_seq:
        return "DEGRADED"

    degraded = bool(shard_info.get("degraded", False)) if shard_info else False
    if degraded:
        return "DEGRADED"

    return "HEALTHY"


def clear_degraded_if_caught_up(
    shard_info: dict[str, Any],
    primary_node: Any,
    replica_node: Any,
    now: float | None = None,
    missed_heartbeats_limit: int = 3,
    heartbeat_interval_sec: float = 2.0,
) -> dict[str, Any]:
    """Clear degraded state if replica is caught up.

    Clarification 3:
    "The coordinator clears degraded only when the replica is UP, READY,
    replica.lastSeq >= degradedSeq and replica.lastSeq == primary.lastSeq (latest heartbeats)."
    """
    if not shard_info.get("degraded", False):
        return dict(shard_info)

    if replica_node is None or primary_node is None:
        return dict(shard_info)

    if now is not None:
        if not _is_node_up(
            replica_node, now, missed_heartbeats_limit, heartbeat_interval_sec
        ):
            return dict(shard_info)
    else:
        if (
            hasattr(replica_node, "health")
            and not callable(replica_node.health)
            and replica_node.health != "UP"
        ):
            return dict(shard_info)
        if (
            isinstance(replica_node, dict)
            and replica_node.get("health") is not None
            and replica_node["health"] != "UP"
        ):
            return dict(shard_info)

    if _get_status(replica_node) != "READY":
        return dict(shard_info)

    degraded_seq = int(shard_info.get("degradedSeq", 0))
    p_seq = _get_last_seq(primary_node)
    r_seq = _get_last_seq(replica_node)

    if p_seq is None or r_seq is None:
        return dict(shard_info)

    if r_seq >= degraded_seq and r_seq == p_seq:
        updated = dict(shard_info)
        updated["degraded"] = False
        return updated

    return dict(shard_info)
