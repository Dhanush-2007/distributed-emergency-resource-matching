"""Component tests for Matching Service consumer (safe delivery, retry, and DLX)."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import pika
import pytest
from ems_common.rabbit import EXCHANGE_DLX, EXCHANGE_RETRY, ROUTING_KEY_MATCH
from matching_service.clients import Transient
from matching_service.consumer import MatchingConsumer

pytestmark = pytest.mark.component


class FakeChannel:
    """Mock pika channel tracking basic_ack, basic_nack, and basic_publish calls."""

    def __init__(self) -> None:
        self.published: list[dict[str, Any]] = []
        self.acked_tags: list[int] = []
        self.nacked_tags: list[tuple[int, bool]] = []
        self.publish_fails = False

    def basic_ack(self, delivery_tag: int) -> None:
        self.acked_tags.append(delivery_tag)

    def basic_nack(self, delivery_tag: int, requeue: bool = False) -> None:
        self.nacked_tags.append((delivery_tag, requeue))

    def basic_publish(
        self,
        exchange: str,
        routing_key: str,
        body: bytes,
        properties: pika.BasicProperties,
        mandatory: bool = True,
    ) -> None:
        if self.publish_fails:
            raise RuntimeError("Broker publish reject")
        self.published.append({
            "exchange": exchange,
            "routing_key": routing_key,
            "body": body,
            "properties": properties,
            "mandatory": mandatory,
        })


@pytest.fixture
def fake_consumer():
    req_client = MagicMock()
    res_client = MagicMock()
    consumer = MatchingConsumer(req_client=req_client, res_client=res_client)
    return consumer, req_client, res_client


def make_delivery(
    request_id: str,
    delivery_tag: int = 1,
    retry_count: int = 0,
    correlation_id: str = "cid-123",
) -> tuple[Any, Any, bytes]:
    method = MagicMock()
    method.delivery_tag = delivery_tag

    props = pika.BasicProperties(
        content_type="application/json",
        message_id="msg-1",
        headers={"x-retry-count": retry_count, "x-correlation-id": correlation_id},
    )

    body = json.dumps({"eventId": "evt-1", "requestId": request_id, "type": "MATCH_REQUESTED"}).encode()
    return method, props, body


def test_consumer_acks_on_success(fake_consumer):
    consumer, req_client, res_client = fake_consumer
    ch = FakeChannel()

    req_data = {"requestId": "r1", "type": "water", "status": "PENDING", "location": {"lat": 0, "lon": 0}}
    req_client.get_request.return_value = (200, req_data)
    res_client.by_request.return_value = None
    res_client.candidates.return_value = ([{"resourceId": "res-1", "type": "water", "location": {"lat": 0, "lon": 0}, "version": 1}], False)
    res_client.reserve.return_value = 200
    req_client.match_result.return_value = (200, {"status": "MATCHED"})

    method, props, body = make_delivery("r1", delivery_tag=42)
    consumer.handle_message(ch, method, props, body)

    # Message must be acknowledged
    assert 42 in ch.acked_tags
    assert len(ch.published) == 0


def test_consumer_acks_on_business_outcome_duplicate_or_dropped(fake_consumer):
    consumer, req_client, _ = fake_consumer
    ch = FakeChannel()

    # 404 unknown -> DROPPED -> ack
    req_client.get_request.return_value = (404, {})
    method, props, body = make_delivery("r-unknown", delivery_tag=10)
    consumer.handle_message(ch, method, props, body)
    assert 10 in ch.acked_tags

    # Non-pending -> DUPLICATE -> ack
    req_client.get_request.return_value = (200, {"requestId": "r-dup", "status": "COMPLETED"})
    method2, props2, body2 = make_delivery("r-dup", delivery_tag=11)
    consumer.handle_message(ch, method2, props2, body2)
    assert 11 in ch.acked_tags


def test_consumer_retries_on_transient(fake_consumer):
    consumer, req_client, _ = fake_consumer
    ch = FakeChannel()

    # match_request raises Transient on first attempt (retry_count=0)
    req_client.get_request.side_effect = Transient("Coordinator down")

    method, props, body = make_delivery("r-retry", delivery_tag=100, retry_count=0)
    consumer.handle_message(ch, method, props, body)

    # Republished to emergency.retry with x-retry-count = 1
    assert len(ch.published) == 1
    pub = ch.published[0]
    assert pub["exchange"] == EXCHANGE_RETRY
    assert pub["routing_key"] == ROUTING_KEY_MATCH
    assert pub["properties"].headers["x-retry-count"] == 1

    # Original message acknowledged only after confirmed republish
    assert 100 in ch.acked_tags


def test_consumer_dead_letters_after_max_retries(fake_consumer, monkeypatch):
    monkeypatch.setenv("MAX_RETRIES", "3")
    consumer, req_client, _ = fake_consumer
    ch = FakeChannel()

    # Message is already at retry_count = 2 (so next is n=3 >= MAX_RETRIES)
    req_client.get_request.side_effect = Transient("Storage unreachable")

    method, props, body = make_delivery("r-dlq", delivery_tag=200, retry_count=2)
    consumer.handle_message(ch, method, props, body)

    # Published to emergency.dlx
    assert len(ch.published) == 1
    pub = ch.published[0]
    assert pub["exchange"] == EXCHANGE_DLX
    assert pub["routing_key"] == ROUTING_KEY_MATCH
    assert pub["properties"].headers["x-retry-count"] == 3
    assert "x-last-error" in pub["properties"].headers
    assert "Storage unreachable" in pub["properties"].headers["x-last-error"]

    # Original message acknowledged
    assert 200 in ch.acked_tags


def test_consumer_dead_letters_malformed_message(fake_consumer):
    consumer, _, _ = fake_consumer
    ch = FakeChannel()

    method = MagicMock()
    method.delivery_tag = 50
    props = pika.BasicProperties(headers={})
    bad_body = b"not-a-valid-json"

    consumer.handle_message(ch, method, props, bad_body)

    # Published to DLX
    assert len(ch.published) == 1
    assert ch.published[0]["exchange"] == EXCHANGE_DLX
    assert 50 in ch.acked_tags


def test_consumer_nacks_with_requeue_if_republish_fails(fake_consumer):
    consumer, req_client, _ = fake_consumer
    ch = FakeChannel()
    ch.publish_fails = True  # Broker rejects republish

    req_client.get_request.side_effect = Transient("Down")

    method, props, body = make_delivery("r-fail", delivery_tag=77, retry_count=0)
    consumer.handle_message(ch, method, props, body)

    # Must NOT ack; must nack with requeue=True
    assert 77 not in ch.acked_tags
    assert (77, True) in ch.nacked_tags


def test_consumer_nacks_with_requeue_if_malformed_dlx_publish_fails(fake_consumer):
    consumer, _, _ = fake_consumer
    ch = FakeChannel()
    ch.publish_fails = True  # Broker rejects DLX publish

    method = MagicMock()
    method.delivery_tag = 99
    props = pika.BasicProperties(headers={})
    bad_body = b"not-a-valid-json"

    consumer.handle_message(ch, method, props, bad_body)

    # Must NOT ack malformed message if DLX republish failed; must nack with requeue=True
    assert 99 not in ch.acked_tags
    assert (99, True) in ch.nacked_tags

