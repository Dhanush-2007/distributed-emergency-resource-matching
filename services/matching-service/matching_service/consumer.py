"""RabbitMQ consumer for Matching Service."""

from __future__ import annotations

import json
import logging
import os
import threading
from typing import Any

import pika
from ems_common.logging import correlation_id_ctx
from ems_common.rabbit import (
    EXCHANGE_DLX,
    EXCHANGE_RETRY,
    QUEUE_MATCH,
    ROUTING_KEY_MATCH,
    declare_topology,
    get_max_retries,
)

from matching_service.clients import (
    RequestServiceClient,
    ResourceServiceClient,
    Transient,
)
from matching_service.matcher import match_request

logger = logging.getLogger("matching-service")


class MatchingConsumer:
    """Consumes match events, invokes matcher, handles retries and dead-lettering."""

    def __init__(
        self,
        req_client: RequestServiceClient,
        res_client: ResourceServiceClient,
        host: str | None = None,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
    ) -> None:
        self._req_client = req_client
        self._res_client = res_client
        self._host = host or os.getenv("RABBITMQ_HOST", "rabbitmq")
        self._port = port or int(os.getenv("RABBITMQ_PORT", "5672"))
        self._user = user if user is not None else os.getenv("RABBITMQ_USER")
        self._password = password if password is not None else os.getenv("RABBITMQ_PASS")

        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._is_connected = False
        self._connection: pika.BlockingConnection | None = None
        self._channel: Any | None = None

    @property
    def is_ready(self) -> bool:
        return self._is_connected and self._connection is not None and self._connection.is_open

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="matching-consumer",
            daemon=True,
        )
        self._thread.start()

    def stop(self, timeout: float = 3.0) -> None:
        self._stop_event.set()
        if self._connection is not None:
            try:
                self._connection.add_callback_threadsafe(self._connection.close)
            except Exception:  # noqa: BLE001, S110
                pass
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def _run_loop(self) -> None:
        backoff = 0.5
        while not self._stop_event.is_set():
            try:
                self._connect_and_consume()
                backoff = 0.5
            except Exception as exc:  # noqa: BLE001
                self._is_connected = False
                logger.warning("Consumer disconnected (%s). Retrying in %.1fs...", exc, backoff)
                self._stop_event.wait(backoff)
                backoff = min(backoff * 2.0, 10.0)

    def _connect_and_consume(self) -> None:
        if not self._user or not self._password:
            logger.warning("RabbitMQ credentials not provided for consumer")
            self._stop_event.wait(2.0)
            return

        credentials = pika.PlainCredentials(self._user, self._password)
        params = pika.ConnectionParameters(
            host=self._host,
            port=self._port,
            credentials=credentials,
            heartbeat=60,
            socket_timeout=5.0,
            connection_attempts=1,
        )
        conn = pika.BlockingConnection(params)
        ch = conn.channel()
        ch.confirm_delivery()
        ch.basic_qos(prefetch_count=1)
        declare_topology(ch)

        self._connection = conn
        self._channel = ch
        self._is_connected = True
        logger.info("Matching consumer connected to RabbitMQ and ready")

        try:
            for method, properties, body in ch.consume(queue=QUEUE_MATCH, inactivity_timeout=1.0):
                if self._stop_event.is_set():
                    break
                if method is None:
                    continue

                self.handle_message(ch, method, properties, body)
        finally:
            self._is_connected = False
            try:
                ch.cancel()
                conn.close()
            except Exception:  # noqa: BLE001, S110
                pass
            self._channel = None
            self._connection = None

    def handle_message(
        self,
        ch: Any,
        method: Any,
        properties: Any,
        body: bytes,
    ) -> None:
        """Process incoming queue message with safe delivery semantics."""
        headers = properties.headers or {} if properties else {}
        cid = headers.get("x-correlation-id") or (properties.message_id if properties else None)
        token = correlation_id_ctx.set(cid)

        try:
            # Parse message payload
            try:
                payload = json.loads(body.decode("utf-8"))
                req_id = payload.get("requestId")
                if not req_id:
                    raise ValueError("Missing requestId in event payload")
            except Exception as exc:  # noqa: BLE001
                logger.warning("Malformed message received (%s). Dead-lettering...", exc)
                ok = self._republish(
                    ch,
                    exchange=EXCHANGE_DLX,
                    routing_key=ROUTING_KEY_MATCH,
                    body=body,
                    properties=properties,
                    retry_count=0,
                    last_error=f"Malformed: {exc}",
                )
                if ok:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                else:
                    logger.error("Failed to dead-letter malformed message; nacking with requeue")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                return

            retry_count = int(headers.get("x-retry-count", 0))

            try:
                outcome = match_request(
                    request_id=req_id,
                    req_client=self._req_client,
                    res_client=self._res_client,
                    retry_count=retry_count,
                )
                logger.info(
                    "Match request completed: %s",
                    outcome,
                    extra={"event": "match.completed", "requestId": req_id, "outcome": outcome},
                )
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Transient as exc:
                n = retry_count + 1
                max_retries = get_max_retries()
                logger.warning(
                    "Transient match failure (attempt %d/%d): %s",
                    n,
                    max_retries,
                    exc,
                    extra={"event": "match.transient", "requestId": req_id, "attempt": n},
                )

                if n < max_retries:
                    # Republish to retry exchange
                    ok = self._republish(
                        ch,
                        exchange=EXCHANGE_RETRY,
                        routing_key=ROUTING_KEY_MATCH,
                        body=body,
                        properties=properties,
                        retry_count=n,
                    )
                else:
                    # Dead-letter to dlx
                    ok = self._republish(
                        ch,
                        exchange=EXCHANGE_DLX,
                        routing_key=ROUTING_KEY_MATCH,
                        body=body,
                        properties=properties,
                        retry_count=n,
                        last_error=str(exc),
                    )

                if ok:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                else:
                    logger.error("Republish failed to confirm; nacking with requeue")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        finally:
            correlation_id_ctx.reset(token)

    def _republish(
        self,
        ch: Any,
        exchange: str,
        routing_key: str,
        body: bytes,
        properties: Any,
        retry_count: int,
        last_error: str | None = None,
    ) -> bool:
        """Republish message with updated headers using publisher confirms."""
        new_headers = dict(properties.headers or {}) if properties else {}
        new_headers["x-retry-count"] = retry_count
        if last_error:
            new_headers["x-last-error"] = last_error

        new_props = pika.BasicProperties(
            delivery_mode=2,
            content_type=properties.content_type if properties else "application/json",
            message_id=properties.message_id if properties else None,
            headers=new_headers,
        )

        try:
            ch.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=body,
                properties=new_props,
                mandatory=True,
            )
            return True
        except Exception as exc:  # noqa: BLE001
            logger.warning("Republish failed: %s", exc)
            return False
