"""HTTP clients for Request and Resource services with Transient error handling."""

from __future__ import annotations

import logging
import os
from typing import Any

import httpx
from ems_common.logging import get_correlation_id

logger = logging.getLogger("matching-service")


class Transient(Exception):
    """Raised for retryable infrastructure failures (timeouts, 5xx, connection errors)."""


class RequestServiceClient:
    """Client for internal Request Service endpoints."""

    def __init__(
        self,
        base_url: str | None = None,
        timeout: float = 3.0,
        http_client: httpx.Client | None = None,
    ) -> None:
        self.base_url = (base_url or os.getenv("REQUEST_SERVICE_URL", "http://localhost:8001")).rstrip("/")
        self.timeout = timeout
        self._owns_http = False
        if http_client is None:
            http_client = httpx.Client(timeout=timeout)
            self._owns_http = True
        self._http = http_client

    def _headers(self) -> dict[str, str]:
        cid = get_correlation_id()
        headers = {"Content-Type": "application/json"}
        if cid:
            headers["X-Correlation-Id"] = cid
        return headers

    def close(self) -> None:
        if self._owns_http and hasattr(self._http, "close"):
            try:
                self._http.close()
            except Exception:  # noqa: BLE001, S110
                pass

    def get_request(self, request_id: str) -> tuple[int, dict[str, Any]]:
        url = f"{self.base_url}/internal/requests/{request_id}"
        try:
            resp = self._http.get(url, headers=self._headers(), timeout=self.timeout)
            if resp.status_code >= 500:
                raise Transient(f"Request service 5xx error ({resp.status_code}): {resp.text}")
            return resp.status_code, resp.json()
        except (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError) as exc:
            raise Transient(f"Request service unreachable: {exc}") from exc

    def match_result(
        self,
        request_id: str,
        outcome: str,
        resource_id: str | None = None,
        version: int | None = None,
    ) -> tuple[int, dict[str, Any]]:
        url = f"{self.base_url}/internal/requests/{request_id}/matchresult"
        body: dict[str, Any] = {"outcome": outcome}
        if resource_id:
            body["resourceId"] = resource_id
        if version is not None:
            body["version"] = version

        try:
            resp = self._http.post(url, json=body, headers=self._headers(), timeout=self.timeout)
            if resp.status_code >= 500:
                raise Transient(f"Request service matchresult 5xx error ({resp.status_code}): {resp.text}")
            return resp.status_code, resp.json()
        except (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError) as exc:
            raise Transient(f"Request service unreachable on matchresult: {exc}") from exc


class ResourceServiceClient:
    """Client for internal Resource Service endpoints."""

    def __init__(
        self,
        base_url: str | None = None,
        timeout: float = 3.0,
        http_client: httpx.Client | None = None,
    ) -> None:
        self.base_url = (base_url or os.getenv("RESOURCE_SERVICE_URL", "http://localhost:8002")).rstrip("/")
        self.timeout = timeout
        self._owns_http = False
        if http_client is None:
            http_client = httpx.Client(timeout=timeout)
            self._owns_http = True
        self._http = http_client

    def _headers(self) -> dict[str, str]:
        cid = get_correlation_id()
        headers = {"Content-Type": "application/json"}
        if cid:
            headers["X-Correlation-Id"] = cid
        return headers

    def close(self) -> None:
        if self._owns_http and hasattr(self._http, "close"):
            try:
                self._http.close()
            except Exception:  # noqa: BLE001, S110
                pass

    def candidates(self, req_type: str) -> tuple[list[dict[str, Any]], bool]:
        url = f"{self.base_url}/internal/resources/candidates"
        try:
            resp = self._http.get(
                url,
                params={"type": req_type},
                headers=self._headers(),
                timeout=self.timeout,
            )
            if resp.status_code >= 500:
                raise Transient(f"Resource service 5xx error ({resp.status_code}): {resp.text}")
            data = resp.json()
            return data.get("items", []), bool(data.get("partial", False))
        except (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError) as exc:
            raise Transient(f"Resource service unreachable on candidates: {exc}") from exc

    def by_request(self, request_id: str) -> dict[str, Any] | None:
        url = f"{self.base_url}/internal/resources/by-request/{request_id}"
        try:
            resp = self._http.get(url, headers=self._headers(), timeout=self.timeout)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 404:
                return None
            if resp.status_code >= 500:
                raise Transient(f"Resource service 5xx error ({resp.status_code}): {resp.text}")
            return None
        except (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError) as exc:
            raise Transient(f"Resource service unreachable on by-request: {exc}") from exc

    def reserve(
        self,
        resource_id: str,
        request_id: str,
        expected_version: int | None = None,
    ) -> int:
        url = f"{self.base_url}/internal/resources/{resource_id}/reserve"
        body: dict[str, Any] = {"requestId": request_id}
        if expected_version is not None:
            body["expectedVersion"] = expected_version

        try:
            resp = self._http.post(url, json=body, headers=self._headers(), timeout=self.timeout)
            if resp.status_code in (200, 404, 409):
                return resp.status_code
            if resp.status_code >= 500:
                raise Transient(f"Resource service reserve 5xx error ({resp.status_code}): {resp.text}")
            return resp.status_code
        except (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError) as exc:
            raise Transient(f"Resource service unreachable on reserve: {exc}") from exc

    def release(self, resource_id: str, request_id: str) -> bool:
        url = f"{self.base_url}/internal/resources/{resource_id}/release"
        body = {"requestId": request_id}
        try:
            resp = self._http.post(url, json=body, headers=self._headers(), timeout=self.timeout)
            if resp.status_code >= 500:
                raise Transient(f"Resource service release 5xx error ({resp.status_code}): {resp.text}")
            return resp.status_code == 200
        except (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError) as exc:
            raise Transient(f"Resource service unreachable on release: {exc}") from exc
