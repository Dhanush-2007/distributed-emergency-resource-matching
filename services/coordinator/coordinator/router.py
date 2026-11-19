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
        if node is None:
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
        """Forward GET request to the primary node for key's shard."""
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
                "op": "GET",
            },
        )

        base_url = node.endpoint.rstrip("/")
        headers = {}
        if correlation_id:
            headers["X-Correlation-Id"] = correlation_id

        try:
            resp = self._http_client.get(
                f"{base_url}/internal/data/{key}",
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
            try:
                node, epoch = self._resolve_primary(sid)
            except StorageUnavailable:
                if not allow_partial:
                    raise StorageUnavailable(
                        "One or more storage shards unavailable",
                        headers={"Retry-After": "3"},
                    ) from None
                unavailable_shards.append(sid)
                continue

            logger.info(
                "Routed data operation",
                extra={
                    "event": "route",
                    "key": prefix,
                    "shardId": sid,
                    "nodeId": node.node_id,
                    "role": node.role,
                    "epoch": epoch,
                    "op": "SCAN",
                },
            )

            base_url = node.endpoint.rstrip("/")
            try:
                resp = self._http_client.get(
                    f"{base_url}/internal/scan",
                    params={"prefix": prefix},
                    headers=headers,
                )
                if resp.status_code >= 500:
                    raise StorageUnavailable("Storage unavailable")
                data = resp.json()
                items = data.get("items", [])
                all_items.extend(items)
            except Exception:  # noqa: BLE001
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
