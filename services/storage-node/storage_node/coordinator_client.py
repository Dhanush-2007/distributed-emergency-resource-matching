"""Coordinator registration and heartbeat client for the storage node.

This module runs a background thread (started/stopped via the lifespan) that:
  1. Registers the node with the coordinator (retrying with exponential backoff).
  2. Sends periodic heartbeats and applies role/epoch updates from the response.
  3. Re-registers automatically when the coordinator responds 404 (coordinator restart).

The HTTP client is injectable for tests (pass an httpx.Client or any object
that exposes .post(url, json=..., timeout=...) and whose response has .status_code
and .json()).
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from typing import Any, Protocol, runtime_checkable

logger = logging.getLogger(__name__)


@runtime_checkable
class HttpResponse(Protocol):
    status_code: int

    def json(self) -> Any: ...


@runtime_checkable
class HttpClient(Protocol):
    """Minimal interface required by CoordinatorClient."""

    def post(self, url: str, *, json: Any, timeout: float) -> HttpResponse: ...


class CoordinatorClient:
    """Background thread that registers and heartbeats with the coordinator.

    Parameters
    ----------
    node_id, shard_id, advertise_url, initial_role:
        Identity fields sent in the registration payload.
    coordinator_url:
        Base URL of the coordinator, e.g. ``http://coordinator:8000``.
    db:
        StorageDB instance – used to read epoch/lastSeq/role and to persist
        the authoritative role and epoch returned by the coordinator.
    heartbeat_interval:
        Seconds between heartbeats once registered.
    http_timeout:
        Seconds to wait for each HTTP call.
    http_client:
        Injectable HTTP client. Defaults to ``httpx.Client()``.
    initial_backoff:
        Initial delay before registration retry.
    max_backoff:
        Maximum delay before registration retry.
    on_status_change:
        Optional callable(status: str) invoked when the node status changes
        between STARTING and READY.
    """

    def __init__(
        self,
        *,
        node_id: str,
        shard_id: int,
        advertise_url: str,
        initial_role: str,
        coordinator_url: str,
        db: Any,  # StorageDB – avoid circular import
        heartbeat_interval: float = 2.0,
        http_timeout: float = 3.0,
        http_client: HttpClient | None = None,
        initial_backoff: float = 0.5,
        max_backoff: float = 5.0,
        on_status_change: Any = None,
        get_replica_ack_seq: Callable[[], int | None] | None = None,
        on_registered: Callable[[str, int], None] | None = None,
    ) -> None:
        self._node_id = node_id
        self._shard_id = shard_id
        self._advertise_url = advertise_url
        self._initial_role = initial_role.upper()  # PRIMARY | REPLICA
        self._coordinator_url = coordinator_url.rstrip("/")
        self._db = db
        self._heartbeat_interval = heartbeat_interval
        self._http_timeout = http_timeout
        self._initial_backoff = initial_backoff
        self._max_backoff = max_backoff
        self._on_status_change = on_status_change
        self._get_replica_ack_seq = get_replica_ack_seq
        self._on_registered = on_registered

        self._owns_http = False
        if http_client is None:
            import httpx

            http_client = httpx.Client()
            self._owns_http = True
        self._http = http_client

        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

        # Node status: STARTING until registered, then READY.
        self._status = "STARTING"
        self._status_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def status(self) -> str:
        with self._status_lock:
            return self._status

    def start(self) -> None:
        """Start the background thread."""
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run, name=f"coordinator-client-{self._node_id}", daemon=True
        )
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        """Signal the background thread to stop and wait for it."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
        if self._owns_http and hasattr(self._http, "close"):
            try:
                self._http.close()
            except Exception:  # noqa: BLE001, S110
                pass

    # ------------------------------------------------------------------
    # Thread body
    # ------------------------------------------------------------------

    def _run(self) -> None:
        # Phase 1: register (infinite retry with exponential backoff).
        self._register_with_backoff()
        if self._stop_event.is_set():
            return

        # Phase 2: heartbeat loop.
        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=self._heartbeat_interval)
            if self._stop_event.is_set():
                break
            self._heartbeat()

    def _register_with_backoff(self) -> None:
        """POST /internal/nodes/register, retrying until 200."""
        backoff = self._initial_backoff
        while not self._stop_event.is_set():
            try:
                payload = self._build_register_payload()
                resp = self._http.post(
                    f"{self._coordinator_url}/internal/nodes/register",
                    json=payload,
                    timeout=self._http_timeout,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    self._apply_coordinator_response(data)
                    assigned_role = (data.get("role") or self._initial_role).upper()
                    assigned_epoch = int(data.get("epoch", 1))

                    if assigned_role == "PRIMARY":
                        self._set_status("READY")
                    else:
                        self._set_status("CATCHING_UP")

                    if self._on_registered is not None:
                        self._on_registered(assigned_role, assigned_epoch)

                    logger.info(
                        "Registered with coordinator",
                        extra={
                            "event": "coordinator.registered",
                            "nodeId": self._node_id,
                            "role": data.get("role"),
                            "epoch": data.get("epoch"),
                        },
                    )
                    return
                else:
                    logger.warning(
                        "Registration failed (non-200)",
                        extra={
                            "event": "coordinator.register_failed",
                            "status": resp.status_code,
                            "backoff": backoff,
                        },
                    )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Registration error",
                    extra={
                        "event": "coordinator.register_error",
                        "error": str(exc),
                        "backoff": backoff,
                    },
                )

            # Wait with backoff before retrying.
            self._stop_event.wait(timeout=backoff)
            backoff = min(backoff * 2, self._max_backoff)

    def _heartbeat(self) -> None:
        """POST /internal/nodes/heartbeat. On 404 -> re-register."""
        try:
            payload = self._build_heartbeat_payload()
            resp = self._http.post(
                f"{self._coordinator_url}/internal/nodes/heartbeat",
                json=payload,
                timeout=self._http_timeout,
            )
            if resp.status_code == 200:
                data = resp.json()
                self._apply_coordinator_response(data)
                logger.debug(
                    "Heartbeat sent",
                    extra={
                        "event": "coordinator.heartbeat_ok",
                        "nodeId": self._node_id,
                    },
                )
            elif resp.status_code == 404:
                # Coordinator forgot us (it restarted) – re-register.
                logger.warning(
                    "Heartbeat 404 – re-registering",
                    extra={"event": "coordinator.heartbeat_404", "nodeId": self._node_id},
                )
                self._set_status("STARTING")
                self._register_with_backoff()
            else:
                logger.warning(
                    "Heartbeat non-200",
                    extra={
                        "event": "coordinator.heartbeat_error",
                        "status": resp.status_code,
                    },
                )
        except Exception as exc:  # noqa: BLE001
            # Connection error – log and keep trying; never crash.
            logger.warning(
                "Heartbeat connection error",
                extra={"event": "coordinator.heartbeat_conn_error", "error": str(exc)},
            )

    # ------------------------------------------------------------------
    # Payload builders
    # ------------------------------------------------------------------

    def _build_register_payload(self) -> dict[str, Any]:
        stats = self._db.stats()
        return {
            "nodeId": self._node_id,
            "shardId": self._shard_id,
            "endpoint": self._advertise_url,
            "role": self._initial_role,
            "epoch": stats["epoch"],
            "lastSeq": stats["last_seq"],
            "status": self._status,
        }

    def _build_heartbeat_payload(self) -> dict[str, Any]:
        stats = self._db.stats()
        ack_seq = self._get_replica_ack_seq() if self._get_replica_ack_seq is not None else None
        return {
            "nodeId": self._node_id,
            "shardId": self._shard_id,
            "role": stats["role"],
            "epoch": stats["epoch"],
            "status": self._status,
            "lastSeq": stats["last_seq"],
            "replicaAckSeq": ack_seq,
            "recordCount": stats["recordCount"],
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _apply_coordinator_response(self, data: dict[str, Any]) -> None:
        """Persist role and epoch received from the coordinator."""
        new_role = data.get("role")
        new_epoch = data.get("epoch")
        if new_role is not None or new_epoch is not None:
            self._db.update_meta(role=new_role, epoch=new_epoch)

    def _set_status(self, status: str) -> None:
        with self._status_lock:
            prev = self._status
            self._status = status
        if prev != status and self._on_status_change is not None:
            self._on_status_change(status)
