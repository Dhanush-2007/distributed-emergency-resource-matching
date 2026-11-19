"""Thread-safe in-memory registry of storage nodes."""

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any


@dataclass
class NodeRecord:
    node_id: str
    shard_id: int
    endpoint: str
    role: str
    epoch: int
    status: str
    last_seq: int
    last_heartbeat: float
    replica_ack_seq: int | None = None
    record_count: dict[str, int] = field(default_factory=dict)

    def health(self, now: float, interval: float, limit: int) -> str:
        """Derived health: UP if within heartbeat timeout window, else DOWN."""
        threshold = interval * limit
        if (now - self.last_heartbeat) <= threshold:
            return "UP"
        return "DOWN"


class NodeRegistry:
    """Thread-safe registry for storage node instances."""

    def __init__(
        self,
        heartbeat_interval: float = 2.0,
        missed_heartbeats_limit: int = 3,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._lock = threading.Lock()
        self._nodes: dict[str, NodeRecord] = {}
        self._heartbeat_interval = heartbeat_interval
        self._missed_heartbeats_limit = missed_heartbeats_limit
        self._clock = clock

    def register(
        self,
        node_id: str,
        shard_id: int,
        endpoint: str,
        role: str,
        epoch: int,
        status: str,
        last_seq: int,
    ) -> NodeRecord:
        """Register a node or update its registration info idempotently."""
        with self._lock:
            now = self._clock()
            existing = self._nodes.get(node_id)
            if existing is not None:
                existing.shard_id = shard_id
                existing.endpoint = endpoint
                existing.role = role
                existing.epoch = epoch
                existing.status = status
                existing.last_seq = last_seq
                existing.last_heartbeat = now
                return existing

            record = NodeRecord(
                node_id=node_id,
                shard_id=shard_id,
                endpoint=endpoint,
                role=role,
                epoch=epoch,
                status=status,
                last_seq=last_seq,
                last_heartbeat=now,
                replica_ack_seq=None,
                record_count={"request": 0, "resource": 0, "other": 0},
            )
            self._nodes[node_id] = record
            return record

    def get(self, node_id: str) -> NodeRecord | None:
        """Retrieve a node record by nodeId."""
        with self._lock:
            return self._nodes.get(node_id)

    def update_heartbeat(
        self,
        node_id: str,
        shard_id: int,
        role: str,
        epoch: int,
        status: str,
        last_seq: int,
        replica_ack_seq: int | None,
        record_count: dict[str, int],
    ) -> NodeRecord | None:
        """Update node heartbeat and statistics."""
        with self._lock:
            record = self._nodes.get(node_id)
            if record is None:
                return None
            record.shard_id = shard_id
            record.role = role
            record.epoch = epoch
            record.status = status
            record.last_seq = last_seq
            record.replica_ack_seq = replica_ack_seq
            record.record_count = dict(record_count)
            record.last_heartbeat = self._clock()
            return record

    def get_health(self, node_id: str) -> str | None:
        """Return derived health (UP/DOWN) for a specific node, or None if unknown."""
        with self._lock:
            record = self._nodes.get(node_id)
            if record is None:
                return None
            return record.health(
                self._clock(),
                self._heartbeat_interval,
                self._missed_heartbeats_limit,
            )

    def list_nodes(self) -> list[dict[str, Any]]:
        """List all nodes in registry with derived health."""
        with self._lock:
            now = self._clock()
            results = []
            for record in self._nodes.values():
                results.append({
                    "nodeId": record.node_id,
                    "shardId": record.shard_id,
                    "endpoint": record.endpoint,
                    "role": record.role,
                    "epoch": record.epoch,
                    "status": record.status,
                    "lastSeq": record.last_seq,
                    "replicaAckSeq": record.replica_ack_seq,
                    "recordCount": dict(record.record_count),
                    "lastHeartbeat": record.last_heartbeat,
                    "health": record.health(
                        now,
                        self._heartbeat_interval,
                        self._missed_heartbeats_limit,
                    ),
                })
            return results
