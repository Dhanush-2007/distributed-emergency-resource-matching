"""Replica monitor and replication logic for primary storage node (Build item 4.3)."""

from __future__ import annotations

import logging
import threading
from typing import Any

import httpx
from ems_common.errors import StaleEpoch, StorageUnavailable

logger = logging.getLogger(__name__)


class ReplicaMonitor:
    """Monitors the peer storage node and determines if it is ready for replication."""

    def __init__(
        self,
        peer_url: str,
        heartbeat_interval: float = 2.0,
        http_client: Any = None,
    ) -> None:
        self._peer_url = peer_url.rstrip("/")
        self._heartbeat_interval = heartbeat_interval
        self._owns_http = False
        if http_client is None:
            http_client = httpx.Client(timeout=1.0)
            self._owns_http = True
        self._http = http_client

        self._lock = threading.Lock()
        self._peer_ok = False
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def peer_ok(self) -> bool:
        with self._lock:
            return self._peer_ok

    @peer_ok.setter
    def peer_ok(self, value: bool) -> None:
        with self._lock:
            self._peer_ok = value

    def mark_not_ok(self) -> None:
        with self._lock:
            self._peer_ok = False

    def start(self) -> None:
        if not self._peer_url:
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="replica-monitor",
            daemon=True,
        )
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
        if self._owns_http and hasattr(self._http, "close"):
            try:
                self._http.close()
            except Exception as exc:  # noqa: BLE001
                logger.debug("Error closing http client: %s", exc)

    def check_peer_once(self) -> bool:
        """Run a single health check probe against the peer."""
        if not self._peer_url:
            self.mark_not_ok()
            return False
        try:
            resp = self._http.get(f"{self._peer_url}/health", timeout=1.0)
            if resp.status_code == 200:
                data = resp.json()
                is_ready = data.get("status") == "READY"
                is_replica = (data.get("role") or "").upper() == "REPLICA"
                ok = is_ready and is_replica
                with self._lock:
                    self._peer_ok = ok
                return ok
        except Exception as exc:  # noqa: BLE001
            logger.debug("Peer probe failed: %s", exc)
        self.mark_not_ok()
        return False

    def run_once(self) -> bool:
        """Run a single peer probe iteration manually."""
        return self.check_peer_once()

    def _run(self) -> None:
        # Probe immediately on start
        self.check_peer_once()
        while not self._stop_event.is_set():
            self._stop_event.wait(self._heartbeat_interval)
            if self._stop_event.is_set():
                break
            self.check_peer_once()


def replicate_or_degrade(
    *,
    peer_url: str,
    coordinator_url: str,
    replica_monitor: ReplicaMonitor | None,
    http_client: Any,
    replication_timeout: float,
    node_id: str,
    shard_id: int,
    epoch: int,
    seq: int,
    op: str,
    key: str,
    value: dict[str, Any] | None,
    version: int,
) -> tuple[bool, int | None]:
    """Execute replication to peer or report degraded to coordinator before returning.

    Returns:
        (replicated: bool, replica_ack_seq: int | None)
    Raises:
        StaleEpoch: if peer returns 409 STALE_EPOCH.
        StorageUnavailable: if degraded report fails (cannot reach coordinator).
    """
    peer_ok = False
    if replica_monitor is not None and peer_url:
        peer_ok = replica_monitor.peer_ok

    if peer_ok and peer_url:
        replicate_payload = {
            "epoch": epoch,
            "seq": seq,
            "op": op,
            "key": key,
            "value": value,
            "version": version,
        }
        repl_timeout: Any = replication_timeout
        if isinstance(replication_timeout, (int, float)):
            repl_timeout = httpx.Timeout(
                float(replication_timeout),
                connect=min(1.0, float(replication_timeout)),
            )
        try:
            resp = http_client.post(
                f"{peer_url.rstrip('/')}/internal/replicate",
                json=replicate_payload,
                timeout=repl_timeout,
            )
            if resp.status_code == 200:
                data = resp.json()
                ack_seq = data.get("ack", seq)
                return True, ack_seq

            if resp.status_code == 409:
                err_data = resp.json().get("error", {})
                code = err_data.get("code")
                if code == "STALE_EPOCH":
                    # Peer has higher epoch; do not acknowledge, propagate StaleEpoch
                    details = err_data.get("details", [{"epoch": epoch}])
                    raise StaleEpoch(
                        message=err_data.get("message", "Stale epoch on primary"),
                        details=details,
                    )
                if code == "GAP":
                    # Gap detected; mark peer not ok and fall through to degraded
                    if replica_monitor is not None:
                        replica_monitor.mark_not_ok()
                else:
                    if replica_monitor is not None:
                        replica_monitor.mark_not_ok()
            else:
                if replica_monitor is not None:
                    replica_monitor.mark_not_ok()
        except StaleEpoch:
            raise
        except Exception:  # noqa: BLE001
            # Timeout or connection error
            if replica_monitor is not None:
                replica_monitor.mark_not_ok()

    # Degraded write path: must report to coordinator BEFORE acknowledging
    degraded_payload = {
        "nodeId": node_id,
        "shardId": shard_id,
        "epoch": epoch,
        "lastSeq": seq,
    }
    try:
        deg_resp = http_client.post(
            f"{coordinator_url.rstrip('/')}/internal/nodes/degraded",
            json=degraded_payload,
            timeout=replication_timeout,
        )
        if deg_resp.status_code != 200:
            raise StorageUnavailable(
                f"Failed to record degraded state on coordinator: status {deg_resp.status_code}"
            )
    except Exception as exc:
        raise StorageUnavailable(
            f"Coordinator unreachable while recording degraded state: {exc}"
        ) from exc

    return False, None
