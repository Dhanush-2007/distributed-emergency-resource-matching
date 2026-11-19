"""Follower recovery and sync pull loop for storage node (Build item 4.4)."""

from __future__ import annotations

import logging
import threading
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class FollowerRecoveryManager:
    """Manages follower recovery, periodic idle sync, and status progression."""

    def __init__(
        self,
        node_id: str,
        peer_url: str,
        db: Any,
        heartbeat_interval: float = 2.0,
        http_client: Any = None,
        on_status_change: Any = None,
        status_getter: Any = None,
    ) -> None:
        self._node_id = node_id
        self._peer_url = peer_url.rstrip("/")
        self._db = db
        self._heartbeat_interval = heartbeat_interval
        self._owns_http = False
        if http_client is None:
            http_client = httpx.Client(timeout=5.0)
            self._owns_http = True
        self._http = http_client
        self._on_status_change = on_status_change
        self._status_getter = status_getter

        self._stop_event = threading.Event()
        self._wake_sync_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._recovery_thread: threading.Thread | None = None
        self._lock = threading.Lock()

    def start(self) -> None:
        if not self._peer_url:
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name=f"follower-recovery-{self._node_id}",
            daemon=True,
        )
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop_event.set()
        self._wake_sync_event.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
        if self._recovery_thread is not None:
            self._recovery_thread.join(timeout=timeout)
        if self._owns_http and hasattr(self._http, "close"):
            try:
                self._http.close()
            except Exception as exc:  # noqa: BLE001
                logger.debug("Error closing recovery http client: %s", exc)

    def start_recovery_thread(self) -> None:
        """Ensure full recovery runs asynchronously in a daemon thread."""
        with self._lock:
            if self._recovery_thread is not None and self._recovery_thread.is_alive():
                return
            self._recovery_thread = threading.Thread(
                target=self.run_full_recovery,
                name=f"follower-catchup-{self._node_id}",
                daemon=True,
            )
            self._recovery_thread.start()

    def trigger_sync(self) -> None:
        """Wake up the follower sync loop immediately (e.g. upon 409 GAP)."""
        self._wake_sync_event.set()

    def _set_status(self, new_status: str) -> None:
        if self._on_status_change is not None:
            self._on_status_change(new_status)

    def run_full_recovery(self) -> None:
        """Execute full recovery loop until caught up with peer."""
        logger.info(
            "Starting follower catch-up",
            extra={"event": "catchup.start", "nodeId": self._node_id},
        )
        backoff = 0.5
        while not self._stop_event.is_set():
            try:
                caught_up = self._do_recovery_round()
                if caught_up:
                    self._set_status("READY")
                    logger.info(
                        "Follower catch-up complete",
                        extra={"event": "catchup.end", "nodeId": self._node_id},
                    )
                    return
                # Reset backoff on progress
                backoff = 0.5
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Follower recovery attempt failed, retrying: %s",
                    exc,
                    extra={
                        "event": "catchup.error",
                        "nodeId": self._node_id,
                        "error": str(exc),
                        "backoff": backoff,
                    },
                )
                self._stop_event.wait(backoff)
                backoff = min(backoff * 2, 5.0)

    def _do_recovery_round(self) -> bool:
        """Run recovery iterations until either caught up (returns True) or error."""
        while not self._stop_event.is_set():
            stats = self._db.stats()
            last_seq = stats["last_seq"]
            last_log_epoch_str = self._db.get_meta("last_log_epoch")
            last_log_epoch = (
                int(last_log_epoch_str) if last_log_epoch_str.isdigit() else 0
            )

            sync_resp = self._http.get(
                f"{self._peer_url}/internal/sync",
                params={"since": last_seq, "epoch": last_log_epoch, "limit": 500},
                timeout=5.0,
            )
            if sync_resp.status_code != 200:
                raise RuntimeError(
                    f"GET /internal/sync returned {sync_resp.status_code}"
                )

            data = sync_resp.json()
            if data.get("snapshotRequired"):
                logger.info(
                    "Snapshot required for follower catch-up",
                    extra={"event": "catchup.snapshot", "nodeId": self._node_id},
                )
                snap_resp = self._http.get(
                    f"{self._peer_url}/internal/snapshot",
                    timeout=10.0,
                )
                if snap_resp.status_code != 200:
                    raise RuntimeError(
                        f"GET /internal/snapshot returned {snap_resp.status_code}"
                    )
                snapshot = snap_resp.json()
                self._db.install_snapshot(snapshot)
                continue

            entries = data.get("entries", [])
            primary_last_seq = data.get("lastSeq", 0)

            if entries:
                for entry in entries:
                    self._db.apply_replicated(
                        epoch=entry["epoch"],
                        seq=entry["seq"],
                        op=entry["op"],
                        key=entry["key"],
                        value=entry["value"],
                        version=entry["version"],
                    )
                logger.info(
                    "Applied catch-up entries",
                    extra={
                        "event": "catchup.progress",
                        "nodeId": self._node_id,
                        "count": len(entries),
                    },
                )

            # Done condition: no entries and our last_seq == primary lastSeq
            current_last_seq = self._db.stats()["last_seq"]
            if not entries and current_last_seq == primary_last_seq:
                return True

            # If there were entries, loop immediately to fetch next page

    def _run(self) -> None:
        """Main background loop."""
        # Initial check: if our status is CATCHING_UP, run full recovery
        current_st = self._status_getter() if self._status_getter else "STARTING"
        if current_st == "CATCHING_UP":
            self.run_full_recovery()

        while not self._stop_event.is_set():
            # Wait for wake event or heartbeat interval
            self._wake_sync_event.wait(self._heartbeat_interval)
            self._wake_sync_event.clear()
            if self._stop_event.is_set():
                break

            current_st = self._status_getter() if self._status_getter else "READY"
            stats = self._db.stats()
            if stats["role"] != "REPLICA":
                continue

            try:
                last_seq = stats["last_seq"]
                last_log_epoch_str = self._db.get_meta("last_log_epoch")
                last_log_epoch = (
                    int(last_log_epoch_str) if last_log_epoch_str.isdigit() else 0
                )
                sync_resp = self._http.get(
                    f"{self._peer_url}/internal/sync",
                    params={"since": last_seq, "epoch": last_log_epoch, "limit": 500},
                    timeout=5.0,
                )
                if sync_resp.status_code == 200:
                    data = sync_resp.json()
                    if data.get("snapshotRequired"):
                        self._set_status("CATCHING_UP")
                        self.run_full_recovery()
                    else:
                        entries = data.get("entries", [])
                        for entry in entries:
                            self._db.apply_replicated(
                                epoch=entry["epoch"],
                                seq=entry["seq"],
                                op=entry["op"],
                                key=entry["key"],
                                value=entry["value"],
                                version=entry["version"],
                            )
            except Exception as exc:  # noqa: BLE001
                logger.debug("Idle sync pull failed: %s", exc)
