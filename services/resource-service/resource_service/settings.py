"""Configuration settings for Resource Service."""

from __future__ import annotations

import os


class Settings:
    """Resource Service settings configured via environment variables or explicit args."""

    def __init__(
        self,
        port: int | None = None,
        coordinator_url: str | None = None,
        request_service_url: str | None = None,
        http_timeout_sec: float | None = None,
        reconcile_enabled: bool | None = None,
        reconcile_interval_sec: float | None = None,
        reconcile_grace_sec: float | None = None,
    ) -> None:
        self.port: int = port if port is not None else int(os.getenv("PORT", "8002"))
        self.coordinator_url: str = (
            coordinator_url
            if coordinator_url is not None
            else os.getenv("COORDINATOR_URL", "http://localhost:8000")
        )
        self.request_service_url: str = (
            request_service_url
            if request_service_url is not None
            else os.getenv("REQUEST_SERVICE_URL", "http://localhost:8001")
        )
        self.http_timeout_sec: float = (
            http_timeout_sec
            if http_timeout_sec is not None
            else float(os.getenv("HTTP_TIMEOUT_SEC", "3.0"))
        )
        reconcile_env = os.getenv("RECONCILE_ENABLED", "true").lower() in ("true", "1", "yes")
        self.reconcile_enabled: bool = (
            reconcile_enabled if reconcile_enabled is not None else reconcile_env
        )
        self.reconcile_interval_sec: float = (
            reconcile_interval_sec
            if reconcile_interval_sec is not None
            else float(os.getenv("RECONCILE_INTERVAL_SEC", "60.0"))
        )
        self.reconcile_grace_sec: float = (
            reconcile_grace_sec
            if reconcile_grace_sec is not None
            else float(os.getenv("RECONCILE_GRACE_SEC", "60.0"))
        )
