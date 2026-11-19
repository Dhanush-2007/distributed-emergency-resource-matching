"""Background reconciler for stale resource assignments."""

from __future__ import annotations

import logging
import threading
from datetime import UTC, datetime
from typing import Any

import httpx
from ems_common.models import now_iso, resource_key
from ems_common.storage_client import CoordinatorClient

logger = logging.getLogger("resource-service")


class Reconciler:
    """Detects and releases stale ASSIGNED resources whose requests were lost or completed."""

    def __init__(
        self,
        storage_client: CoordinatorClient,
        request_service_url: str,
        grace_sec: float = 60.0,
        interval_sec: float = 60.0,
        http_client: httpx.Client | None = None,
    ) -> None:
        self._storage_client = storage_client
        self._request_service_url = request_service_url.rstrip("/")
        self._grace_sec = grace_sec
        self._interval_sec = interval_sec

        self._owns_http = False
        if http_client is None:
            http_client = httpx.Client(timeout=3.0)
            self._owns_http = True
        self._http = http_client

        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="resource-reconciler",
            daemon=True,
        )
        self._thread.start()

    def stop(self, timeout: float = 2.0) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
        if self._owns_http and hasattr(self._http, "close"):
            try:
                self._http.close()
            except Exception:  # noqa: BLE001, S110
                pass

    def _run(self) -> None:
        while not self._stop_event.wait(self._interval_sec):
            try:
                self.run_once()
            except Exception as exc:  # noqa: BLE001
                logger.warning("Reconciler run failed: %s", exc)

    def run_once(self) -> None:
        """Execute single reconciliation pass over assigned resources."""
        if not self._request_service_url:
            return

        try:
            items, partial = self._storage_client.scan("resource:", allow_partial=False)
            if partial:
                logger.warning("Skipping reconcile: scan is partial")
                return
        except Exception as exc:  # noqa: BLE001
            logger.warning("Skipping reconcile: storage scan failed: %s", exc)
            return

        now = datetime.now(UTC).timestamp()

        for item in items:
            val = item.get("value", {})
            if val.get("status") != "ASSIGNED" or not val.get("assignedRequestId"):
                continue

            res_id = val.get("resourceId")
            req_id = val.get("assignedRequestId")

            updated_at_str = val.get("updatedAt")
            if updated_at_str:
                try:
                    updated_ts = datetime.fromisoformat(updated_at_str).timestamp()
                    if now - updated_ts < self._grace_sec:
                        continue
                except Exception:  # noqa: BLE001, S110
                    pass

            # Query Request Service
            try:
                resp = self._http.get(
                    f"{self._request_service_url}/internal/requests/{req_id}",
                    timeout=2.0,
                )
                should_release = False
                if resp.status_code == 404:
                    should_release = True
                elif resp.status_code == 200:
                    req_data = resp.json()
                    st = req_data.get("status")
                    matched_id = req_data.get("matchedResourceId")
                    if st not in ("MATCHED", "IN_PROGRESS") or matched_id != res_id:
                        should_release = True
                else:
                    # Non-conclusive response, skip for this round
                    continue

                if should_release:
                    self._release_resource(res_id, req_id)
            except Exception as exc:  # noqa: BLE001
                logger.debug("Request service check failed for %s: %s", req_id, exc)
                continue

    def _release_resource(self, res_id: str, req_id: str) -> None:
        def mutate(rec: dict[str, Any]) -> dict[str, Any]:
            if rec.get("assignedRequestId") == req_id:
                rec["status"] = "AVAILABLE"
                rec["availability"] = True
                rec["assignedRequestId"] = None
                rec["updatedAt"] = now_iso()
            return rec

        try:
            self._storage_client.update(resource_key(res_id), mutate)
            logger.info(
                "Reconciler released stale resource",
                extra={
                    "event": "reconcile.release",
                    "resourceId": res_id,
                    "assignedRequestId": req_id,
                },
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to release resource %s in reconciler: %s", res_id, exc)
