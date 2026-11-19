"""Coordinator failover management for storage node primary promotion."""

from __future__ import annotations

import logging
import threading
from typing import Any

import httpx

from coordinator.registry import NodeRegistry
from coordinator.shard_state import ShardStateManager

logger = logging.getLogger(__name__)


class FailoverManager:
    """Manages primary failover, evaluation, and promotion across shards."""

    def __init__(
        self,
        registry: NodeRegistry,
        shard_state: ShardStateManager,
        n_shards: int,
        http_client: Any = None,
        role_timeout_sec: float = 3.0,
        clock: Any = None,
        heartbeat_interval_sec: float = 2.0,
        missed_heartbeats_limit: int = 3,
    ) -> None:
        import time

        self._registry = registry
        self._shard_state = shard_state
        self._n_shards = n_shards
        self._http = http_client or httpx.Client(timeout=role_timeout_sec)
        self._owns_http = http_client is None
        self._role_timeout = role_timeout_sec
        self._clock = clock or time.time
        self._heartbeat_interval = heartbeat_interval_sec
        self._missed_limit = missed_heartbeats_limit
        self._lock = threading.Lock()
        self._pending_epochs: dict[int, int] = {}
        self._ever_registered_primaries: set[str] = set()

    def record_primary_registration(self, node_id: str) -> None:
        """Record that a node has registered as primary during coordinator lifetime."""
        with self._lock:
            self._ever_registered_primaries.add(node_id)

    def evaluate_all_shards(self) -> None:
        """Evaluate failover for all shards whose primary is DOWN."""
        for sid in range(self._n_shards):
            self.evaluate_shard(sid)

    def evaluate_shard(self, shard_id: int) -> bool:
        """Evaluate failover for a shard. Returns True if promotion succeeded."""
        logger.info(
            "Evaluating failover for shard",
            extra={"event": "failover.evaluate", "shardId": shard_id},
        )

        with self._lock:
            pending_epoch = self._pending_epochs.get(shard_id)

        shard_nodes = self._registry.get_nodes_for_shard(shard_id)
        primaries = [n for n in shard_nodes if n.role == "PRIMARY"]
        replicas = [n for n in shard_nodes if n.role == "REPLICA"]

        p_node = primaries[0] if primaries else None
        replica = replicas[0] if replicas else None

        if p_node is None and pending_epoch is None:
            return False

        if p_node is not None and p_node.health != "DOWN" and pending_epoch is None:
            return False

        if p_node is not None:
            with self._lock:
                ever_registered = p_node.node_id in self._ever_registered_primaries
            if not ever_registered and pending_epoch is None:
                return False

        if replica is None and pending_epoch is not None:
            primary_id = self._shard_state.get_primary(shard_id)
            replica = self._registry.get(primary_id) if primary_id else None

        if replica is None:
            logger.warning(
                "Shard unavailable: no replica found",
                extra={"event": "failover.unavailable", "shardId": shard_id},
            )
            return False

        if replica.health != "UP":
            logger.warning(
                "Shard unavailable: both nodes down",
                extra={"event": "failover.unavailable", "shardId": shard_id},
            )
            return False

        # Ensure replica is genuinely reachable if its heartbeat is overdue
        now = self._clock()
        threshold = self._heartbeat_interval * self._missed_limit
        if (now - replica.last_heartbeat) > threshold:
            try:
                p_resp = self._http.get(
                    f"{replica.endpoint.rstrip('/')}/health",
                    timeout=self._role_timeout,
                )
                if p_resp.status_code != 200:
                    replica.health = "DOWN"
                    return False
                replica.last_heartbeat = now
            except Exception:  # noqa: BLE001
                replica.health = "DOWN"
                return False

        if replica.status != "READY":
            return False

        if pending_epoch is None:
            shard_info = self._shard_state.get_shard(shard_id) or {}
            is_degraded = bool(shard_info.get("degraded", False))
            if is_degraded:
                logger.warning(
                    "Skipping failover on degraded shard",
                    extra={
                        "event": "failover.skip_stale",
                        "shardId": shard_id,
                        "reason": "shard_is_degraded",
                    },
                )
                logger.warning(
                    "Shard unavailable due to stale replica",
                    extra={"event": "failover.unavailable", "shardId": shard_id},
                )
                return False

            if p_node is not None and replica.last_seq < p_node.last_seq:
                logger.warning(
                    "Skipping failover on stale replica",
                    extra={
                        "event": "failover.skip_stale",
                        "shardId": shard_id,
                        "replicaLastSeq": replica.last_seq,
                        "primaryLastSeq": p_node.last_seq,
                    },
                )
                logger.warning(
                    "Shard unavailable due to stale replica",
                    extra={"event": "failover.unavailable", "shardId": shard_id},
                )
                return False

            curr_epoch = self._shard_state.get_epoch(shard_id) or (p_node.epoch if p_node else 1)
            new_epoch = curr_epoch + 1
            with self._lock:
                self._pending_epochs[shard_id] = new_epoch

            # Persist atomically FIRST
            self._shard_state.update_shard(
                shard_id,
                {
                    "epoch": new_epoch,
                    "primaryNodeId": replica.node_id,
                    "degraded": True,
                    "degradedSeq": replica.last_seq,
                },
            )
        else:
            new_epoch = pending_epoch

        # Call POST /internal/role on replica
        try:
            resp = self._http.post(
                f"{replica.endpoint.rstrip('/')}/internal/role",
                json={"role": "PRIMARY", "epoch": new_epoch},
                timeout=self._role_timeout,
            )
            if resp.status_code == 200:
                with self._registry._lock:
                    replica.role = "PRIMARY"
                    replica.epoch = new_epoch
                    if p_node is not None:
                        p_node.role = "REPLICA"
                with self._lock:
                    self._pending_epochs.pop(shard_id, None)
                    self._ever_registered_primaries.add(replica.node_id)
                logger.info(
                    "Failover promoted replica to primary",
                    extra={
                        "event": "failover.promote",
                        "shardId": shard_id,
                        "oldPrimary": p_node.node_id if p_node else None,
                        "newPrimary": replica.node_id,
                        "epoch": new_epoch,
                    },
                )
                return True
            logger.error(
                "Role call returned error status",
                extra={
                    "event": "failover.error",
                    "shardId": shard_id,
                    "statusCode": resp.status_code,
                },
            )
            return False
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Role call failed with exception",
                extra={
                    "event": "failover.error",
                    "shardId": shard_id,
                    "error": str(exc),
                },
            )
            return False

    def force_promote(self, shard_id: int, node_id: str) -> dict[str, Any]:
        """Force promote named node to primary regardless of staleness."""
        target_node = self._registry.get(node_id)
        if target_node is None or target_node.shard_id != shard_id:
            from ems_common.errors import InvalidInput

            raise InvalidInput(
                f"Node '{node_id}' does not belong to shard {shard_id}",
                details=[{"field": "nodeId", "problem": "invalid_node"}],
            )

        if target_node.health != "UP":
            from ems_common.errors import StorageUnavailable

            raise StorageUnavailable(
                f"Node '{node_id}' is DOWN; cannot force promote",
                headers={"Retry-After": "3"},
            )

        with self._registry._lock:
            nodes_in_shard = [n for n in self._registry._nodes.values() if n.shard_id == shard_id]
        primary_node = next((n for n in nodes_in_shard if n.role == "PRIMARY"), None)
        primary_seq = primary_node.last_seq if primary_node else max((n.last_seq for n in nodes_in_shard), default=0)

        if target_node.last_seq < primary_seq:
            logger.warning(
                "Shard %d has stale replica (%s) with seq %d < primary seq %d; promotion may lose data. Proceed?",
                shard_id,
                node_id,
                target_node.last_seq,
                primary_seq,
                extra={
                    "event": "failover.stale_warning",
                    "shardId": shard_id,
                    "replicaId": node_id,
                    "replicaSeq": target_node.last_seq,
                    "primarySeq": primary_seq,
                },
            )

        curr_epoch = self._shard_state.get_epoch(shard_id) or target_node.epoch or 1
        new_epoch = curr_epoch + 1

        # Persist atomically FIRST
        self._shard_state.update_shard(
            shard_id,
            {
                "epoch": new_epoch,
                "primaryNodeId": node_id,
                "degraded": True,
                "degradedSeq": target_node.last_seq,
            },
        )

        resp = self._http.post(
            f"{target_node.endpoint.rstrip('/')}/internal/role",
            json={"role": "PRIMARY", "epoch": new_epoch},
            timeout=self._role_timeout,
        )
        if resp.status_code != 200:
            from ems_common.errors import StorageUnavailable

            raise StorageUnavailable(
                f"Failed to set role on node '{node_id}': status {resp.status_code}",
                headers={"Retry-After": "3"},
            )

        with self._registry._lock:
            target_node.role = "PRIMARY"
            target_node.epoch = new_epoch
            for n in self._registry._nodes.values():
                if n.shard_id == shard_id and n.node_id != node_id:
                    n.role = "REPLICA"

        with self._lock:
            self._pending_epochs.pop(shard_id, None)
            self._ever_registered_primaries.add(node_id)

        logger.warning(
            "Force promoted node to primary",
            extra={
                "event": "failover.force_promote",
                "shardId": shard_id,
                "nodeId": node_id,
                "epoch": new_epoch,
            },
        )
        return {
            "status": "ok",
            "shardId": shard_id,
            "primaryNodeId": node_id,
            "epoch": new_epoch,
        }
