"""Background health checker for storage nodes."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from typing import Any

import httpx

from coordinator.registry import NodeRecord, NodeRegistry

logger = logging.getLogger(__name__)


class HealthChecker:
    """Monitors storage node heartbeats and issues health probes when heartbeats lag."""

    def __init__(
        self,
        registry: NodeRegistry,
        heartbeat_interval_sec: float = 2.0,
        missed_heartbeats_limit: int = 3,
        probe_timeout_sec: float = 1.0,
        health_check_interval_sec: float = 1.0,
        http_client: Any = None,
        clock: Callable[[], float] = time.time,
        on_node_down: Callable[[NodeRecord], None] | None = None,
    ) -> None:
        self._registry = registry
        self._heartbeat_interval = heartbeat_interval_sec
        self._missed_limit = missed_heartbeats_limit
        self._probe_timeout = probe_timeout_sec
        self._check_interval = health_check_interval_sec
        self._http = http_client or httpx.Client(timeout=probe_timeout_sec)
        self._owns_http = http_client is None
        self._clock = clock
        self._on_node_down = on_node_down
        self._on_tick: Callable[[], None] | None = None
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def set_on_node_down(self, callback: Callable[[NodeRecord], None] | None) -> None:
        self._on_node_down = callback

    def set_on_tick(self, callback: Callable[[], None] | None) -> None:
        self._on_tick = callback

    def start(self) -> None:
        """Start the background monitoring thread."""
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="coordinator-health-checker",
            daemon=True,
        )
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        """Stop the background thread and close resources."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
        if self._owns_http and hasattr(self._http, "close"):
            try:
                self._http.close()
            except Exception as exc:  # noqa: BLE001
                logger.debug("Error closing health checker http client: %s", exc)

    def _run(self) -> None:
        while not self._stop_event.is_set():
            self._stop_event.wait(self._check_interval)
            if self._stop_event.is_set():
                break
            try:
                self.tick()
            except Exception as exc:  # noqa: BLE001
                logger.error("Error during health checker tick: %s", exc)

    def tick(self) -> None:
        """Single evaluation step: check ages, log missed intervals, and probe lagged nodes."""
        now = self._clock()
        threshold = self._heartbeat_interval * self._missed_limit
        records = self._registry.get_all_records()
        down_nodes: list[NodeRecord] = []

        for node in records:
            age = now - node.last_heartbeat

            # Log node.heartbeat_missed once when age first exceeds one interval
            if age > self._heartbeat_interval and not node.missed_logged:
                node.missed_logged = True
                logger.warning(
                    "Node heartbeat missed",
                    extra={
                        "event": "node.heartbeat_missed",
                        "nodeId": node.node_id,
                        "shardId": node.shard_id,
                        "ageSec": round(age, 3),
                    },
                )

            # Only probe UP nodes whose heartbeat age exceeds the threshold
            if node.health == "UP" and age > threshold:
                probe_ok = False
                base_url = node.endpoint.rstrip("/")
                try:
                    resp = self._http.get(
                        f"{base_url}/health",
                        timeout=self._probe_timeout,
                    )
                    if resp.status_code == 200:
                        probe_ok = True
                except Exception as exc:  # noqa: BLE001
                    logger.debug("Probe failed for node %s: %s", node.node_id, exc)

                if probe_ok:
                    node.last_heartbeat = now
                    node.missed_logged = False
                    logger.info(
                        "Node probe succeeded",
                        extra={
                            "event": "node.probe_ok",
                            "nodeId": node.node_id,
                            "shardId": node.shard_id,
                            "endpoint": node.endpoint,
                        },
                    )
                else:
                    node.health = "DOWN"
                    logger.error(
                        "Node marked DOWN",
                        extra={
                            "event": "node.down",
                            "nodeId": node.node_id,
                            "shardId": node.shard_id,
                            "ageSec": round(age, 3),
                            "timestamp": now,
                        },
                    )
                    down_nodes.append(node)

        for node in down_nodes:
            if self._on_node_down is not None:
                try:
                    self._on_node_down(node)
                except Exception as exc:  # noqa: BLE001
                    logger.error(
                        "Error in on_node_down hook for %s: %s",
                        node.node_id,
                        exc,
                    )

        if self._on_tick is not None:
            try:
                self._on_tick()
            except Exception as exc:  # noqa: BLE001
                logger.error("Error in on_tick hook: %s", exc)
