"""Coordinator routing module for storage shard primary operations."""

from __future__ import annotations

import logging
from typing import Any

import httpx
from ems_common.errors import StorageUnavailable
from ems_common.sharding import shard_of

from coordinator.registry import NodeRecord, NodeRegistry
from coordinator.shard_state import ShardStateManager

logger = logging.getLogger(__name__)


class ShardRouter:
    """Routes data operations to shard primary storage nodes."""

    def __init__(
        self,
        registry: NodeRegistry,
        shard_state: ShardStateManager,
        n_shards: int,
        http_client: httpx.Client,
    ) -> None:
        self._registry = registry
        self._shard_state = shard_state
        self._n_shards = n_shards
        self._http_client = http_client

    def _resolve_primary(self, shard_id: int) -> tuple[NodeRecord, int]:
        """Resolve the primary node for a shard.

        Must be:
          1. Registered in registry
          2. Health UP
          3. Status READY
        Otherwise raises StorageUnavailable (503) with header Retry-After: 3.
        """
        primary_id = self._shard_state.get_primary(shard_id)
        if not primary_id:
            raise StorageUnavailable(
                "Storage shard unavailable",
                headers={"Retry-After": "3"},
            )

        node = self._registry.get(primary_id)
        if node is None or node.role != "PRIMARY":
            raise StorageUnavailable(
                "Storage shard unavailable",
                headers={"Retry-After": "3"},
            )

        health = self._registry.get_health(primary_id)
        if health != "UP":
            raise StorageUnavailable(
                "Storage shard unavailable",
                headers={"Retry-After": "3"},
            )

        if node.status != "READY":
            raise StorageUnavailable(
                "Storage shard unavailable",
                headers={"Retry-After": "3"},
            )

        epoch = self._shard_state.get_epoch(shard_id) or node.epoch
        return node, epoch

    def _get_read_fallback_replica(self, shard_id: int) -> NodeRecord | None:
        """Find an in-sync replica eligible for read fallback."""
        shard_info = self._shard_state.get_shard(shard_id) or {}
        if shard_info.get("degraded", False):
            return None

        primary_id = self._shard_state.get_primary(shard_id)
        p_node = self._registry.get(primary_id) if primary_id else None
        p_last_seq = p_node.last_seq if p_node else 0

        replicas = [
            n
            for n in self._registry.get_nodes_for_shard(shard_id)
            if n.role == "REPLICA"
        ]
        if not replicas:
            return None

        replica = replicas[0]
        if (
            replica.health == "UP"
            and replica.status == "READY"
            and replica.last_seq >= p_last_seq
        ):
            return replica
        return None

    def put_data(
        self,
        key: str,
        value: dict[str, Any],
        if_version: int | None,
        correlation_id: str | None = None,
    ) -> tuple[int, dict[str, Any]]:
        """Forward PUT request to the primary node for key's shard."""
        sid = shard_of(key, self._n_shards)
        node, epoch = self._resolve_primary(sid)

        logger.info(
            "Routed data operation",
            extra={
                "event": "route",
                "key": key,
                "shardId": sid,
                "nodeId": node.node_id,
                "role": node.role,
                "epoch": epoch,
                "op": "PUT",
            },
        )

        base_url = node.endpoint.rstrip("/")
        headers = {}
        if correlation_id:
            headers["X-Correlation-Id"] = correlation_id

        payload: dict[str, Any] = {
            "value": value,
            "ifVersion": if_version,
            "epoch": epoch,
        }

        try:
            resp = self._http_client.put(
                f"{base_url}/internal/data/{key}",
                json=payload,
                headers=headers,
            )
        except (httpx.ConnectError, httpx.TimeoutException, httpx.RequestError):
            raise StorageUnavailable(
                "Storage unavailable", headers={"Retry-After": "3"}
            ) from None

        if resp.status_code >= 500:
            raise StorageUnavailable(
                "Storage unavailable", headers={"Retry-After": "3"}
            )

        return resp.status_code, resp.json()

    def get_data(
        self,
        key: str,
        correlation_id: str | None = None,
    ) -> tuple[int, dict[str, Any]]:
        """Forward GET request to primary, falling back to in-sync replica if unavailable."""
        sid = shard_of(key, self._n_shards)
        primary_node: NodeRecord | None = None
        epoch = 1

        try:
            primary_node, epoch = self._resolve_primary(sid)
        except StorageUnavailable:
            primary_node = None

        headers = {}
        if correlation_id:
            headers["X-Correlation-Id"] = correlation_id

        if primary_node is not None:
            logger.info(
                "Routed data operation",
                extra={
                    "event": "route",
                    "key": key,
                    "shardId": sid,
                    "nodeId": primary_node.node_id,
                    "role": primary_node.role,
                    "epoch": epoch,
                    "op": "GET",
                },
            )
            base_url = primary_node.endpoint.rstrip("/")
            try:
                resp = self._http_client.get(
                    f"{base_url}/internal/data/{key}",
                    headers=headers,
                )
                if resp.status_code < 500:
                    return resp.status_code, resp.json()
            except (httpx.ConnectError, httpx.TimeoutException, httpx.RequestError):
                pass

        # Primary DOWN, failed, or timed out. Check fallback replica.
        fallback_replica = self._get_read_fallback_replica(sid)
        if fallback_replica is not None:
            logger.info(
                "Fallback read to replica",
                extra={
                    "event": "route.fallback",
                    "key": key,
                    "shardId": sid,
                    "nodeId": fallback_replica.node_id,
                    "op": "GET",
                },
            )
            base_url = fallback_replica.endpoint.rstrip("/")
            try:
                resp = self._http_client.get(
                    f"{base_url}/internal/data/{key}",
                    headers=headers,
                )
                if resp.status_code < 500:
                    return resp.status_code, resp.json()
            except (httpx.ConnectError, httpx.TimeoutException, httpx.RequestError):
                pass

        raise StorageUnavailable("Storage unavailable", headers={"Retry-After": "3"})

    def delete_data(
        self,
        key: str,
        correlation_id: str | None = None,
    ) -> tuple[int, dict[str, Any]]:
        """Forward DELETE request to the primary node for key's shard."""
        sid = shard_of(key, self._n_shards)
        node, epoch = self._resolve_primary(sid)

        logger.info(
            "Routed data operation",
            extra={
                "event": "route",
                "key": key,
                "shardId": sid,
                "nodeId": node.node_id,
                "role": node.role,
                "epoch": epoch,
                "op": "DELETE",
            },
        )

        base_url = node.endpoint.rstrip("/")
        headers = {}
        if correlation_id:
            headers["X-Correlation-Id"] = correlation_id

        try:
            resp = self._http_client.delete(
                f"{base_url}/internal/data/{key}?epoch={epoch}",
                headers=headers,
            )
        except (httpx.ConnectError, httpx.TimeoutException, httpx.RequestError):
            raise StorageUnavailable(
                "Storage unavailable", headers={"Retry-After": "3"}
            ) from None

        if resp.status_code >= 500:
            raise StorageUnavailable(
                "Storage unavailable", headers={"Retry-After": "3"}
            )

        return resp.status_code, resp.json()

    def scan_data(
        self,
        prefix: str,
        allow_partial: bool,
        correlation_id: str | None = None,
    ) -> dict[str, Any]:
        """Query every shard's primary GET /internal/scan?prefix=, merge and sort by key."""
        all_items: list[dict[str, Any]] = []
        unavailable_shards: list[int] = []

        headers = {}
        if correlation_id:
            headers["X-Correlation-Id"] = correlation_id

        for sid in range(self._n_shards):
            scan_success = False
            primary_node = None
            try:
                primary_node, epoch = self._resolve_primary(sid)
            except StorageUnavailable:
                primary_node = None

            if primary_node is not None:
                logger.info(
                    "Routed data operation",
                    extra={
                        "event": "route",
                        "key": prefix,
                        "shardId": sid,
                        "nodeId": primary_node.node_id,
                        "role": primary_node.role,
                        "epoch": epoch,
                        "op": "SCAN",
                    },
                )
                base_url = primary_node.endpoint.rstrip("/")
                try:
                    resp = self._http_client.get(
                        f"{base_url}/internal/scan",
                        params={"prefix": prefix},
                        headers=headers,
                    )
                    if resp.status_code < 500:
                        data = resp.json()
                        all_items.extend(data.get("items", []))
                        scan_success = True
                except Exception as exc:  # noqa: BLE001
                    logger.debug("Primary scan failed for shard %s: %s", sid, exc)

            if not scan_success:
                fallback_replica = self._get_read_fallback_replica(sid)
                if fallback_replica is not None:
                    logger.info(
                        "Fallback read to replica",
                        extra={
                            "event": "route.fallback",
                            "key": prefix,
                            "shardId": sid,
                            "nodeId": fallback_replica.node_id,
                            "op": "SCAN",
                        },
                    )
                    base_url = fallback_replica.endpoint.rstrip("/")
                    try:
                        resp = self._http_client.get(
                            f"{base_url}/internal/scan",
                            params={"prefix": prefix},
                            headers=headers,
                        )
                        if resp.status_code < 500:
                            data = resp.json()
                            all_items.extend(data.get("items", []))
                            scan_success = True
                    except Exception as exc:  # noqa: BLE001
                        logger.debug("Replica scan fallback failed for shard %s: %s", sid, exc)

            if not scan_success:
                if not allow_partial:
                    raise StorageUnavailable(
                        "One or more storage shards unavailable",
                        headers={"Retry-After": "3"},
                    ) from None
                unavailable_shards.append(sid)

        all_items.sort(key=lambda x: str(x.get("key", "")))
        result: dict[str, Any] = {
            "items": all_items,
            "count": len(all_items),
            "partial": bool(unavailable_shards),
        }
        if unavailable_shards:
            result["unavailableShards"] = sorted(unavailable_shards)
        return result
