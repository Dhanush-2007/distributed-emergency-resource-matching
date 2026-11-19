"""Coordinator storage client with retries, correlation propagation and helpers."""

from __future__ import annotations

import copy
import os
import time
import uuid
from collections.abc import Callable
from typing import Any

import httpx

from ems_common.errors import NotFound, StorageUnavailable, VersionConflict
from ems_common.logging import get_correlation_id


class CoordinatorClient:
    """Client for coordinator storage endpoints (/internal/data and /internal/scan)."""

    def __init__(
        self,
        base_url: str | None = None,
        http_client: httpx.Client | None = None,
        retries: int = 3,
        timeout: float | None = None,
    ) -> None:
        self.base_url = (
            base_url or os.getenv("COORDINATOR_URL", "http://localhost:8000")
        ).rstrip("/")
        self.retries = retries
        self.timeout = (
            timeout
            if timeout is not None
            else float(os.getenv("HTTP_TIMEOUT_SEC", "3"))
        )

        self._owns_http = False
        if http_client is None:
            http_client = httpx.Client(timeout=self.timeout)
            self._owns_http = True
        self._http = http_client

    def close(self) -> None:
        if self._owns_http and hasattr(self._http, "close"):
            self._http.close()

    def _headers(self) -> dict[str, str]:
        cid = get_correlation_id() or str(uuid.uuid4())
        return {"X-Correlation-Id": cid, "Content-Type": "application/json"}

    def _calc_backoff(self, attempt: int, resp: httpx.Response | None = None) -> float:
        backoff = min(0.2 * (2**attempt), 2.0)
        if resp is not None and "Retry-After" in resp.headers:
            try:
                retry_after = float(resp.headers["Retry-After"])
                return min(retry_after, 2.0)
            except ValueError:
                pass
        return backoff

    def get(self, key: str) -> tuple[dict[str, Any], int]:
        """Fetch record value and version, retrying on 503 or connection errors."""
        url = f"{self.base_url}/internal/data/{key}"
        last_exc: Exception | None = None

        for attempt in range(self.retries + 1):
            try:
                resp = self._http.get(url, headers=self._headers(), timeout=self.timeout)
                if resp.status_code == 200:
                    data = resp.json()
                    return data["value"], data["version"]
                if resp.status_code == 404:
                    err_json = resp.json().get("error", {})
                    raise NotFound(
                        message=err_json.get("message", f"Key '{key}' not found"),
                        details=err_json.get("details", []),
                    )
                if resp.status_code == 503:
                    if attempt < self.retries:
                        time.sleep(self._calc_backoff(attempt, resp))
                        continue
                    err_json = resp.json().get("error", {})
                    raise StorageUnavailable(
                        message=err_json.get("message", "Storage unavailable"),
                        details=err_json.get("details", []),
                        headers={"Retry-After": resp.headers.get("Retry-After", "3")},
                    )
                resp.raise_for_status()
            except (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError) as exc:
                last_exc = exc
                if attempt < self.retries:
                    time.sleep(self._calc_backoff(attempt))
                    continue
                raise StorageUnavailable(
                    f"Coordinator connection failed: {exc}",
                    headers={"Retry-After": "3"},
                ) from exc

        raise StorageUnavailable(f"Get failed after retries: {last_exc}")

    def put(
        self,
        key: str,
        value: dict[str, Any],
        if_version: int | None = None,
    ) -> tuple[int, dict[str, Any]]:
        """Write record unconditionally or with optimistic concurrency check."""
        url = f"{self.base_url}/internal/data/{key}"
        body: dict[str, Any] = {"value": value}
        if if_version is not None:
            body["ifVersion"] = if_version

        try:
            resp = self._http.put(
                url, json=body, headers=self._headers(), timeout=self.timeout
            )
            if resp.status_code in (200, 201):
                return resp.status_code, resp.json()
            if resp.status_code == 409:
                err_json = resp.json().get("error", {})
                raise VersionConflict(
                    message=err_json.get("message", "Version conflict"),
                    details=err_json.get("details", []),
                )
            if resp.status_code == 503:
                err_json = resp.json().get("error", {})
                raise StorageUnavailable(
                    message=err_json.get("message", "Storage unavailable"),
                    details=err_json.get("details", []),
                    headers={"Retry-After": resp.headers.get("Retry-After", "3")},
                )
            resp.raise_for_status()
            return resp.status_code, resp.json()
        except (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError) as exc:
            raise StorageUnavailable(
                f"Coordinator write failed: {exc}",
                headers={"Retry-After": "3"},
            ) from exc

    def delete(self, key: str) -> tuple[int, dict[str, Any]]:
        """Delete record."""
        url = f"{self.base_url}/internal/data/{key}"
        try:
            resp = self._http.delete(
                url, headers=self._headers(), timeout=self.timeout
            )
            if resp.status_code == 200:
                return resp.status_code, resp.json()
            if resp.status_code == 404:
                err_json = resp.json().get("error", {})
                raise NotFound(
                    message=err_json.get("message", f"Key '{key}' not found"),
                    details=err_json.get("details", []),
                )
            if resp.status_code == 503:
                err_json = resp.json().get("error", {})
                raise StorageUnavailable(
                    message=err_json.get("message", "Storage unavailable"),
                    details=err_json.get("details", []),
                    headers={"Retry-After": resp.headers.get("Retry-After", "3")},
                )
            resp.raise_for_status()
            return resp.status_code, resp.json()
        except (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError) as exc:
            raise StorageUnavailable(
                f"Coordinator delete failed: {exc}",
                headers={"Retry-After": "3"},
            ) from exc

    def scan(
        self, prefix: str = "", allow_partial: bool = False
    ) -> tuple[list[dict[str, Any]], bool]:
        """Scan prefix, retrying on 503 or connection errors."""
        url = f"{self.base_url}/internal/scan"
        params = {"prefix": prefix, "allowPartial": str(allow_partial).lower()}
        last_exc: Exception | None = None

        for attempt in range(self.retries + 1):
            try:
                resp = self._http.get(
                    url, params=params, headers=self._headers(), timeout=self.timeout
                )
                if resp.status_code == 200:
                    data = resp.json()
                    is_partial = bool(data.get("unavailableShards"))
                    return data.get("items", []), is_partial
                if resp.status_code == 503:
                    if attempt < self.retries:
                        time.sleep(self._calc_backoff(attempt, resp))
                        continue
                    err_json = resp.json().get("error", {})
                    raise StorageUnavailable(
                        message=err_json.get("message", "Storage unavailable"),
                        details=err_json.get("details", []),
                        headers={"Retry-After": resp.headers.get("Retry-After", "3")},
                    )
                resp.raise_for_status()
            except (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError) as exc:
                last_exc = exc
                if attempt < self.retries:
                    time.sleep(self._calc_backoff(attempt))
                    continue
                raise StorageUnavailable(
                    f"Coordinator scan failed: {exc}",
                    headers={"Retry-After": "3"},
                ) from exc

        raise StorageUnavailable(f"Scan failed after retries: {last_exc}")

    def create_if_absent(
        self, key: str, value: dict[str, Any]
    ) -> tuple[dict[str, Any], int]:
        """Create a new record (ifVersion=0); handle 409 idempotently if retrying."""
        retried = False
        for attempt in range(self.retries + 1):
            try:
                _, res = self.put(key, value, if_version=0)
                return value, res.get("version", 1)
            except StorageUnavailable:
                retried = True
                if attempt < self.retries:
                    time.sleep(self._calc_backoff(attempt))
                    continue
                raise
            except VersionConflict:
                if retried:
                    try:
                        existing_val, existing_ver = self.get(key)
                        if existing_val == value:
                            return existing_val, existing_ver
                    except Exception:  # noqa: BLE001, S110
                        pass
                raise

        raise StorageUnavailable(f"Create failed after retries for key '{key}'")

    def update(
        self,
        key: str,
        mutate: Callable[[dict[str, Any]], dict[str, Any]],
        max_attempts: int = 5,
    ) -> tuple[dict[str, Any], int]:
        """Atomic read-modify-write with optimistic concurrency."""
        for _ in range(max_attempts):
            curr_val, curr_ver = self.get(key)
            new_val = mutate(copy.deepcopy(curr_val))
            if new_val == curr_val:
                return curr_val, curr_ver
            try:
                _, res = self.put(key, new_val, if_version=curr_ver)
                return new_val, res.get("version", curr_ver + 1)
            except VersionConflict:
                continue

        raise VersionConflict(
            f"Exceeded max update attempts ({max_attempts}) on key '{key}'"
        )
