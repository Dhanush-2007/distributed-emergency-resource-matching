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
        rabbitmq_host: str | None = None,
        rabbitmq_port: int | None = None,
        rabbitmq_user: str | None = None,
        rabbitmq_pass: str | None = None,
        sweep_enabled: bool | None = None,
        sweep_interval_sec: float | None = None,
        sweep_grace_sec: float | None = None,
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

        self.rabbitmq_host: str = (
            rabbitmq_host
            if rabbitmq_host is not None
            else os.getenv("RABBITMQ_HOST", "rabbitmq")
        )
        self.rabbitmq_port: int = (
            rabbitmq_port
            if rabbitmq_port is not None
            else int(os.getenv("RABBITMQ_PORT", "5672"))
        )
        self.rabbitmq_user: str | None = (
            rabbitmq_user
            if rabbitmq_user is not None
            else os.getenv("RABBITMQ_USER")
        )
        self.rabbitmq_pass: str | None = (
            rabbitmq_pass
            if rabbitmq_pass is not None
            else os.getenv("RABBITMQ_PASS")
        )

        sweep_env = os.getenv("SWEEP_ENABLED", "true").lower() in ("true", "1", "yes")
        self.sweep_enabled: bool = (
            sweep_enabled if sweep_enabled is not None else sweep_env
        )
        self.sweep_interval_sec: float = (
            sweep_interval_sec
            if sweep_interval_sec is not None
            else float(os.getenv("SWEEP_INTERVAL_SEC", "15.0"))
        )
        self.sweep_grace_sec: float = (
            sweep_grace_sec
            if sweep_grace_sec is not None
            else float(os.getenv("SWEEP_GRACE_SEC", "10.0"))
        )
