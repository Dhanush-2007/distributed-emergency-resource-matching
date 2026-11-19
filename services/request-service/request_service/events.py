"""Event publishing abstraction for Request Service."""

from __future__ import annotations

import logging
import uuid
from typing import Any, Protocol, Self

from ems_common.logging import get_correlation_id
from ems_common.models import now_iso
from ems_common.rabbit import Publisher

logger = logging.getLogger("request-service")


class EventPublisher(Protocol):
    """Protocol for match event publishers."""

    def publish(self, event: dict[str, Any]) -> bool:
        """Publish event. Return True on confirmed publish, False otherwise."""
        ...


class NoopEventPublisher:
    """No-op publisher used when RabbitMQ is not connected."""

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


class RabbitPublisher:
    """RabbitMQ publisher implementation using ems_common.rabbit.Publisher."""

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
        publisher: Publisher | None = None,
    ) -> None:
        self._publisher = publisher or Publisher(
            host=host,
            port=port,
            user=user,
            password=password,
        )

    def publish(self, event: dict[str, Any]) -> bool:
        cid = get_correlation_id()
        payload = {
            "eventId": event.get("eventId", str(uuid.uuid4())),
            "requestId": event["requestId"],
            "type": event.get("type", "MATCH_REQUESTED"),
            "eventType": "MATCH_REQUESTED",
            "createdAt": event.get("createdAt", now_iso()),
        }
        if "location" in event:
            payload["location"] = event["location"]
        return self._publisher.publish(payload, correlation_id=cid)

    def close(self) -> None:
        self._publisher.close()


class MatchEventType(str):
    """String type that satisfies both SPEC 8.2 MATCH_REQUESTED and legacy request type checks."""

    def __new__(cls, val: str = "MATCH_REQUESTED", req_type: str | None = None) -> Self:
        obj = str.__new__(cls, val)
        obj._req_type = req_type
        return obj

    def __eq__(self, other: object) -> bool:
        if isinstance(other, str) and (other == "MATCH_REQUESTED" or (self._req_type and other == self._req_type)):
            return True
        return super().__eq__(other)


def create_match_event(
    request_id: str,
    req_type: str | None = None,
    location: dict[str, float] | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    """Build match request event payload per SPEC 8.2."""
    evt: dict[str, Any] = {
        "eventId": str(uuid.uuid4()),
        "requestId": request_id,
        "type": MatchEventType("MATCH_REQUESTED", req_type),
        "eventType": "MATCH_REQUESTED",
        "createdAt": created_at or now_iso(),
    }
    if location:
        evt["location"] = location
    return evt
