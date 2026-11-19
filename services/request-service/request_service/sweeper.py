"""Outbox sweeper background loop for unconfirmed match events."""

from __future__ import annotations

import logging
import threading
from datetime import UTC, datetime
from typing import Any

from ems_common.models import now_iso, request_key
from ems_common.storage_client import CoordinatorClient

from request_service.events import EventPublisher, create_match_event

logger = logging.getLogger("request-service")


class OutboxSweeper:
    """Detects and republishes unconfirmed match events for PENDING requests."""

    def __init__(
        self,
        storage_client: CoordinatorClient,
        publisher: EventPublisher,
        interval_sec: float = 15.0,
        grace_sec: float = 10.0,
    ) -> None:
        self._storage_client = storage_client
        self._publisher = publisher
        self._interval_sec = interval_sec
        self._grace_sec = grace_sec

        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="outbox-sweeper",
            daemon=True,
        )
        self._thread.start()

    def stop(self, timeout: float = 2.0) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def _run(self) -> None:
        while not self._stop_event.wait(self._interval_sec):
            try:
                self.run_once()
            except Exception as exc:  # noqa: BLE001
                logger.warning("Sweeper pass failed: %s", exc)

    def run_once(self) -> int:
        """Run single sweep pass. Returns number of republished events."""
        try:
            items, _ = self._storage_client.scan("request:", allow_partial=True)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Sweeper scan failed: %s", exc)
            return 0

        now = datetime.now(UTC).timestamp()
        republished_count = 0

        for it in items:
            rec = it.get("value", {})
            if rec.get("status") != "PENDING" or rec.get("eventPending") is not True:
                continue

            req_id = rec.get("requestId")
            if not req_id:
                k = it.get("key", "")
                if k.startswith("request:"):
                    req_id = k[len("request:"):]

            if not req_id:
                continue

            # Check grace period against updatedAt or createdAt
            time_str = rec.get("updatedAt") or rec.get("createdAt")
            if time_str:
                try:
                    time_ts = datetime.fromisoformat(time_str).timestamp()
                    if now - time_ts < self._grace_sec:
                        continue
                except Exception:  # noqa: BLE001, S110
                    pass

            # Create and publish new match event
            evt = create_match_event(request_id=req_id)
            published = self._publisher.publish(evt)
            if not published:
                continue

            # On confirm, clear eventPending with conditional write if still PENDING
            cleared = False

            def clear_pending(r: dict[str, Any]) -> dict[str, Any]:
                nonlocal cleared
                if r.get("status") == "PENDING":
                    r["eventPending"] = False
                    r["updatedAt"] = now_iso()
                    cleared = True
                return r

            try:
                self._storage_client.update(request_key(req_id), clear_pending)
                if cleared:
                    republished_count += 1
                    logger.info(
                        "Sweeper republished match event",
                        extra={
                            "event": "sweeper.republish",
                            "requestId": req_id,
                            "eventId": evt.get("eventId"),
                        },
                    )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Sweeper failed to clear eventPending on %s: %s", req_id, exc)

        return republished_count
