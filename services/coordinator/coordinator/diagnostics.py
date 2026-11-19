"""Coordinator read-only diagnostics service."""

from __future__ import annotations

from typing import Any

from ems_common.errors import InvalidInput
from ems_common.sharding import shard_of

from coordinator.registry import NodeRegistry
from coordinator.shard_health import derive_shard_state
from coordinator.shard_state import ShardStateManager


class DiagnosticsService:
    """Provides read-only cluster introspection and routing diagnostics."""

    def __init__(
        self,
        registry: NodeRegistry,
        shard_state: ShardStateManager,
        n_shards: int,
    ) -> None:
        self._registry = registry
        self._shard_state = shard_state
        self._n_shards = n_shards

    def get_route(self, key: str | None) -> dict[str, Any]:
        """Compute the target shard and primary node without touching data."""
        if not key or not key.strip():
            raise InvalidInput(
                "Missing required 'key' query parameter",
                details=[{"field": "key", "problem": "missing"}],
            )

        sid = shard_of(key, self._n_shards)
        primary_id = self._shard_state.get_primary(sid)
        stored_epoch = self._shard_state.get_epoch(sid) or 1

        if not primary_id:
            return {
                "key": key,
                "shardId": sid,
                "nodeId": None,
                "role": None,
                "epoch": stored_epoch,
                "health": "DOWN",
                "endpoint": None,
            }

        node = self._registry.get(primary_id)
        if node is None:
            return {
                "key": key,
                "shardId": sid,
                "nodeId": primary_id,
                "role": "PRIMARY",
                "epoch": stored_epoch,
                "health": "DOWN",
                "endpoint": None,
            }

        health = self._registry.get_health(primary_id) or "DOWN"
        return {
            "key": key,
            "shardId": sid,
            "nodeId": node.node_id,
            "role": node.role,
            "epoch": stored_epoch or node.epoch,
            "health": health,
            "endpoint": node.endpoint,
        }

    def get_nodes(self) -> list[dict[str, Any]]:
        """List all nodes in the registry with derived health and heartbeat age."""
        now = self._registry._clock()
        nodes = []
        for n in self._registry.list_nodes():
            last_hb = n.get("lastHeartbeat", now)
            age = max(0.0, now - last_hb)
            nodes.append({
                "nodeId": n["nodeId"],
                "shardId": n["shardId"],
                "endpoint": n["endpoint"],
                "role": n["role"],
                "epoch": n["epoch"],
                "health": n["health"],
                "status": n["status"],
                "lastHeartbeatAgeSec": round(age, 3),
                "lastSeq": n["lastSeq"],
                "recordCount": n["recordCount"],
            })
        return nodes

    def get_shards(self) -> list[dict[str, Any]]:
        """List state, epoch, primary, degraded, and counts for every shard."""
        shards: list[dict[str, Any]] = []
        now = self._registry._clock()
        all_nodes = self._registry.list_nodes()

        for sid in range(self._n_shards):
            shard_info = self._shard_state.get_shard(sid) or {
                "epoch": 1,
                "primaryNodeId": "",
                "degraded": False,
                "degradedSeq": 0,
            }
            stored_primary = shard_info.get("primaryNodeId") or None
            stored_epoch = shard_info.get("epoch", 1)
            is_degraded = bool(shard_info.get("degraded", False))

            # Filter nodes belonging to this shard
            shard_nodes = [
                {
                    "id": n["nodeId"],
                    "nodeId": n["nodeId"],
                    "role": n["role"],
                    "health": n["health"],
                    "status": n["status"],
                    "lastSeq": n["lastSeq"],
                }
                for n in all_nodes
                if n["shardId"] == sid
            ]

            primary_node = (
                self._registry.get(stored_primary) if stored_primary else None
            )
            replica_nodes = [
                n
                for n in self._registry.get_nodes_for_shard(sid)
                if n.node_id != stored_primary
            ]
            replica_node = replica_nodes[0] if replica_nodes else None

            state = derive_shard_state(
                shard_info=shard_info,
                primary_node=primary_node,
                replica_node=replica_node,
                now=now,
                missed_heartbeats_limit=self._registry._missed_heartbeats_limit,
                heartbeat_interval_sec=self._registry._heartbeat_interval,
            )

            if primary_node is not None:
                rc = dict(primary_node.record_count)
                total_records = sum(rc.values())
            else:
                rc = {"request": 0, "resource": 0, "other": 0}
                total_records = 0

            shards.append({
                "shardId": sid,
                "state": state,
                "epoch": stored_epoch,
                "primary": stored_primary,
                "degraded": is_degraded,
                "records": total_records,
                "recordCount": rc,
                "nodes": shard_nodes,
            })

        return shards
