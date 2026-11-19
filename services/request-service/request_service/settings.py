"""Configuration settings for Request Service."""

from __future__ import annotations

import os


class Settings:
    """Request Service settings configured via environment variables or explicit args."""

    def __init__(
        self,
        port: int | None = None,
        coordinator_url: str | None = None,
        resource_service_url: str | None = None,
        event_publisher: str | None = None,
        http_timeout_sec: float | None = None,
    ) -> None:
        self.port: int = port if port is not None else int(os.getenv("PORT", "8001"))
        self.coordinator_url: str = (
            coordinator_url
            if coordinator_url is not None
            else os.getenv("COORDINATOR_URL", "http://localhost:8000")
        )
        self.resource_service_url: str = (
            resource_service_url
            if resource_service_url is not None
            else os.getenv("RESOURCE_SERVICE_URL", "http://localhost:8002")
        )
        self.event_publisher: str = (
            event_publisher
            if event_publisher is not None
            else os.getenv("EVENT_PUBLISHER", "noop")
        )
        self.http_timeout_sec: float = (
            http_timeout_sec
            if http_timeout_sec is not None
            else float(os.getenv("HTTP_TIMEOUT_SEC", "3.0"))
        )
