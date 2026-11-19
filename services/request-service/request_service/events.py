"""Event publishing abstraction for Request Service."""

from __future__ import annotations

import logging
import uuid
from typing import Any, Protocol

logger = logging.getLogger("request-service")


class EventPublisher(Protocol):
    """Protocol for match event publishers."""

    def publish(self, event: dict[str, Any]) -> bool:
        """Publish event. Return True on confirmed publish, False otherwise."""
        ...


class NoopEventPublisher:
    """No-op publisher used when RabbitMQ is not connected (Stage 6)."""

    def publish(self, event: dict[str, Any]) -> bool:
        logger.info(
            "NoopEventPublisher dropped event",
            extra={
                "event": "event.noop_dropped",
                "eventId": event.get("eventId"),
                "requestId": event.get("requestId"),
            },
        )
        return False


class InMemoryEventPublisher:
    """In-memory event publisher for component and unit testing."""

    def __init__(self) -> None:
        self.published: list[dict[str, Any]] = []

    def publish(self, event: dict[str, Any]) -> bool:
        self.published.append(dict(event))
        return True


def create_match_event(
    request_id: str,
    req_type: str,
    location: dict[str, float],
    created_at: str,
) -> dict[str, Any]:
    """Build match request event payload."""
    return {
        "eventId": str(uuid.uuid4()),
        "requestId": request_id,
        "type": req_type,
        "location": location,
        "createdAt": created_at,
    }
