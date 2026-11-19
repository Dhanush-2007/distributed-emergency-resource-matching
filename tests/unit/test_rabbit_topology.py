"""Unit tests for RabbitMQ topology declaration and Publisher abstraction."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from ems_common.rabbit import (
    EXCHANGE_DLX,
    EXCHANGE_EVENTS,
    EXCHANGE_RETRY,
    QUEUE_DLQ,
    QUEUE_MATCH,
    QUEUE_RETRY,
    ROUTING_KEY_MATCH,
    Publisher,
    declare_topology,
)

pytestmark = pytest.mark.unit


class FakeChannel:
    """Mock pika channel to record topology declarations."""

    def __init__(self) -> None:
        self.exchanges: list[dict[str, Any]] = []
        self.queues: list[dict[str, Any]] = []
        self.bindings: list[dict[str, Any]] = []

    def exchange_declare(self, exchange: str, exchange_type: str, durable: bool) -> None:
        self.exchanges.append({"exchange": exchange, "type": exchange_type, "durable": durable})

    def queue_declare(self, queue: str, durable: bool, arguments: dict[str, Any] | None = None) -> None:
        self.queues.append({"queue": queue, "durable": durable, "arguments": arguments})

    def queue_bind(self, queue: str, exchange: str, routing_key: str) -> None:
        self.bindings.append({"queue": queue, "exchange": exchange, "routing_key": routing_key})


def test_declare_topology():
    ch = FakeChannel()
    declare_topology(ch)

    # 1. Exchanges
    ex_names = {e["exchange"] for e in ch.exchanges}
    assert ex_names == {EXCHANGE_EVENTS, EXCHANGE_RETRY, EXCHANGE_DLX}
    for e in ch.exchanges:
        assert e["type"] == "direct"
        assert e["durable"] is True

    # 2. Queues
    q_map = {q["queue"]: q for q in ch.queues}
    assert set(q_map.keys()) == {QUEUE_MATCH, QUEUE_RETRY, QUEUE_DLQ}

    # request.match dead letters to emergency.retry
    assert q_map[QUEUE_MATCH]["durable"] is True
    assert q_map[QUEUE_MATCH]["arguments"] == {"x-dead-letter-exchange": EXCHANGE_RETRY}

    # request.match.retry has TTL and dead letters to emergency.events
    assert q_map[QUEUE_RETRY]["durable"] is True
    assert q_map[QUEUE_RETRY]["arguments"]["x-message-ttl"] == 5000
    assert q_map[QUEUE_RETRY]["arguments"]["x-dead-letter-exchange"] == EXCHANGE_EVENTS
    assert q_map[QUEUE_RETRY]["arguments"]["x-dead-letter-routing-key"] == ROUTING_KEY_MATCH

    # request.match.dlq
    assert q_map[QUEUE_DLQ]["durable"] is True

    # 3. Bindings
    bindings = [(b["queue"], b["exchange"], b["routing_key"]) for b in ch.bindings]
    assert (QUEUE_MATCH, EXCHANGE_EVENTS, ROUTING_KEY_MATCH) in bindings
    assert (QUEUE_RETRY, EXCHANGE_RETRY, ROUTING_KEY_MATCH) in bindings
    assert (QUEUE_DLQ, EXCHANGE_DLX, ROUTING_KEY_MATCH) in bindings


def test_publisher_missing_credentials_returns_false():
    pub = Publisher(host="localhost", port=5672, user="", password="")
    res = pub.publish({"eventId": "evt-1", "requestId": "req-1"})
    assert res is False


def test_publisher_connection_failure_returns_false():
    pub = Publisher(host="non-existent-host", port=5672, user="u", password="p")
    res = pub.publish({"eventId": "evt-1", "requestId": "req-1"})
    assert res is False


def test_publisher_success_with_mock():
    pub = Publisher(host="localhost", port=5672, user="u", password="p")

    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.is_closed = False

    pub._channel = mock_channel
    pub._connection = mock_connection

    event = {
        "eventId": "e-123",
        "requestId": "r-456",
        "type": "MATCH_REQUESTED",
        "createdAt": "2026-01-01T00:00:00Z",
    }

    res = pub.publish(event, retry_count=1, correlation_id="cid-999")
    assert res is True
    assert mock_channel.basic_publish.called

    _, kwargs = mock_channel.basic_publish.call_args
    assert kwargs["exchange"] == EXCHANGE_EVENTS
    assert kwargs["routing_key"] == ROUTING_KEY_MATCH
    assert kwargs["mandatory"] is True

    props = kwargs["properties"]
    assert props.delivery_mode == 2
    assert props.content_type == "application/json"
    assert props.message_id == "e-123"
    assert props.headers == {"x-retry-count": 1, "x-correlation-id": "cid-999"}


def test_publisher_exception_returns_false():
    pub = Publisher(host="localhost", port=5672, user="u", password="p")
    mock_channel = MagicMock()
    mock_channel.basic_publish.side_effect = Exception("Channel write error")
    mock_connection = MagicMock()
    mock_connection.is_closed = False

    pub._channel = mock_channel
    pub._connection = mock_connection

    res = pub.publish({"eventId": "e-1"})
    assert res is False
    assert pub._channel is None
    assert pub._connection is None
