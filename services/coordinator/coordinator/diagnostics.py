"""Coordinator read-only diagnostics service."""

from __future__ import annotations

from typing import Any

from ems_common.errors import InvalidInput
from ems_common.sharding import shard_of

from coordinator.registry import NodeRegistry
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
        """List state, epoch, primary, and counts for every shard."""
        shards: list[dict[str, Any]] = []

        all_nodes = self._registry.list_nodes()

        for sid in range(self._n_shards):
            stored_primary = self._shard_state.get_primary(sid)
            stored_epoch = self._shard_state.get_epoch(sid) or 1

            # Filter nodes belonging to this shard
            shard_nodes = [
                {
                    "id": n["nodeId"],
                    "nodeId": n["nodeId"],
                    "health": n["health"],
                    "role": n["role"],
                }
                for n in all_nodes
                if n["shardId"] == sid
            ]

            if not stored_primary:
                shards.append({
                    "shardId": sid,
                    "state": "UNAVAILABLE",
                    "epoch": stored_epoch,
                    "primary": None,
                    "records": 0,
                    "recordCount": {"request": 0, "resource": 0, "other": 0},
                    "nodes": shard_nodes,
                })
                continue

            primary_node = self._registry.get(stored_primary)
            if primary_node is None:
                shards.append({
                    "shardId": sid,
                    "state": "UNAVAILABLE",
                    "epoch": stored_epoch,
                    "primary": stored_primary,
                    "records": 0,
                    "recordCount": {"request": 0, "resource": 0, "other": 0},
                    "nodes": shard_nodes,
                })
                continue

            primary_health = self._registry.get_health(stored_primary)
            is_healthy = primary_health == "UP" and primary_node.status == "READY"
            state = "HEALTHY" if is_healthy else "UNAVAILABLE"

            rc = dict(primary_node.record_count)
            total_records = sum(rc.values())

            shards.append({
                "shardId": sid,
                "state": state,
                "epoch": stored_epoch,
                "primary": stored_primary,
                "records": total_records,
                "recordCount": rc,
                "nodes": shard_nodes,
            })

        return shards
