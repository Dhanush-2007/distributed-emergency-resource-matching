"""RabbitMQ topology declaration and thread-safe publisher."""

from __future__ import annotations

import json
import logging
import os
import threading
from typing import Any

import pika

from ems_common.logging import get_correlation_id

logger = logging.getLogger("ems-rabbit")

EXCHANGE_EVENTS = "emergency.events"
EXCHANGE_RETRY = "emergency.retry"
EXCHANGE_DLX = "emergency.dlx"

QUEUE_MATCH = "request.match"
QUEUE_RETRY = "request.match.retry"
QUEUE_DLQ = "request.match.dlq"

ROUTING_KEY_MATCH = "request.match"

DEFAULT_RETRY_TTL_MS = 5000
DEFAULT_MAX_RETRIES = 3


def get_retry_ttl_ms() -> int:
    return int(os.getenv("RETRY_TTL_MS", str(DEFAULT_RETRY_TTL_MS)))


def get_max_retries() -> int:
    return int(os.getenv("MAX_RETRIES", str(DEFAULT_MAX_RETRIES)))


def declare_topology(channel: Any) -> None:
    """Declare exchanges, queues and bindings idempotently with exact arguments."""
    retry_ttl = get_retry_ttl_ms()

    # 1. Exchanges
    channel.exchange_declare(exchange=EXCHANGE_EVENTS, exchange_type="direct", durable=True)
    channel.exchange_declare(exchange=EXCHANGE_RETRY, exchange_type="direct", durable=True)
    channel.exchange_declare(exchange=EXCHANGE_DLX, exchange_type="direct", durable=True)

    # 2. Queues & Bindings
    # Primary queue: dead-letters unhandled failures to emergency.retry
    channel.queue_declare(
        queue=QUEUE_MATCH,
        durable=True,
        arguments={"x-dead-letter-exchange": EXCHANGE_RETRY},
    )
    channel.queue_bind(queue=QUEUE_MATCH, exchange=EXCHANGE_EVENTS, routing_key=ROUTING_KEY_MATCH)

    # Retry queue: messages wait for TTL, then dead-letter back to emergency.events
    channel.queue_declare(
        queue=QUEUE_RETRY,
        durable=True,
        arguments={
            "x-message-ttl": retry_ttl,
            "x-dead-letter-exchange": EXCHANGE_EVENTS,
            "x-dead-letter-routing-key": ROUTING_KEY_MATCH,
        },
    )
    channel.queue_bind(queue=QUEUE_RETRY, exchange=EXCHANGE_RETRY, routing_key=ROUTING_KEY_MATCH)

    # Dead letter queue
    channel.queue_declare(queue=QUEUE_DLQ, durable=True)
    channel.queue_bind(queue=QUEUE_DLQ, exchange=EXCHANGE_DLX, routing_key=ROUTING_KEY_MATCH)


class Publisher:
    """Thread-safe RabbitMQ publisher with confirms and lazy reconnection."""

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
    ) -> None:
        self._host = host or os.getenv("RABBITMQ_HOST", "rabbitmq")
        self._port = port or int(os.getenv("RABBITMQ_PORT", "5672"))
        self._user = user if user is not None else os.getenv("RABBITMQ_USER")
        self._password = password if password is not None else os.getenv("RABBITMQ_PASS")

        self._lock = threading.Lock()
        self._connection: pika.BlockingConnection | None = None
        self._channel: Any | None = None

    def _connect(self) -> bool:
        if not self._user or not self._password:
            logger.warning("RabbitMQ credentials not provided")
            return False

        try:
            credentials = pika.PlainCredentials(self._user, self._password)
            parameters = pika.ConnectionParameters(
                host=self._host,
                port=self._port,
                credentials=credentials,
                connection_attempts=1,
                socket_timeout=2.0,
            )
            conn = pika.BlockingConnection(parameters)
            ch = conn.channel()
            ch.confirm_delivery()
            declare_topology(ch)
            self._connection = conn
            self._channel = ch
            return True
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to connect to RabbitMQ: %s", exc)
            self._cleanup()
            return False

    def _cleanup(self) -> None:
        if self._connection is not None:
            try:
                self._connection.close()
            except Exception:  # noqa: BLE001, S110
                pass
        self._connection = None
        self._channel = None

    def publish(
        self,
        event: dict[str, Any],
        retry_count: int = 0,
        exchange: str = EXCHANGE_EVENTS,
        routing_key: str = ROUTING_KEY_MATCH,
        correlation_id: str | None = None,
    ) -> bool:
        """Publish event with publisher confirms and mandatory=True. Returns False on failure."""
        with self._lock:
            if (self._channel is None or self._connection is None or self._connection.is_closed) and not self._connect():
                return False

            cid = correlation_id or get_correlation_id()
            event_id = str(event.get("eventId", ""))
            body_bytes = json.dumps(event).encode("utf-8")

            properties = pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json",
                message_id=event_id,
                headers={
                    "x-retry-count": retry_count,
                    "x-correlation-id": cid,
                },
            )

            try:
                assert self._channel is not None
                self._channel.basic_publish(
                    exchange=exchange,
                    routing_key=routing_key,
                    body=body_bytes,
                    properties=properties,
                    mandatory=True,
                )
                return True
            except Exception as exc:  # noqa: BLE001
                logger.warning("RabbitMQ publish failed: %s", exc)
                self._cleanup()
                return False

    def close(self) -> None:
        with self._lock:
            self._cleanup()
