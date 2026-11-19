"""Integration test verifying RabbitMQ topology via the Management HTTP API."""

from __future__ import annotations

import httpx
import pytest

from tests.fault.stack import RabbitManagementClient

pytestmark = pytest.mark.integration


def _is_mgmt_available() -> bool:
    try:
        r = httpx.get("http://localhost:15672/api/overview", auth=("ems", "ems-dev-password"), timeout=2.0)
        return r.status_code == 200
    except Exception:  # noqa: BLE001
        return False


@pytest.mark.skipif(not _is_mgmt_available(), reason="RabbitMQ Management API not reachable")
class TestRabbitTopologyLive:
    def test_exchanges_declared(self):
        client = RabbitManagementClient()
        exchanges = {ex["name"]: ex for ex in client.list_exchanges()}

        for name in ["emergency.events", "emergency.retry", "emergency.dlx"]:
            assert name in exchanges, f"Exchange {name} missing"
            ex = exchanges[name]
            assert ex["type"] == "direct"
            assert ex["durable"] is True

    def test_queues_and_arguments_declared(self):
        client = RabbitManagementClient()
        queues = {q["name"]: q for q in client.list_queues()}

        assert "request.match" in queues
        q_match = queues["request.match"]
        assert q_match["durable"] is True
        assert q_match["arguments"].get("x-dead-letter-exchange") == "emergency.retry"

        assert "request.match.retry" in queues
        q_retry = queues["request.match.retry"]
        assert q_retry["durable"] is True
        assert q_retry["arguments"].get("x-dead-letter-exchange") == "emergency.events"
        assert q_retry["arguments"].get("x-dead-letter-routing-key") == "request.match"
        assert q_retry["arguments"].get("x-message-ttl") == 5000

        assert "request.match.dlq" in queues
        q_dlq = queues["request.match.dlq"]
        assert q_dlq["durable"] is True

    def test_bindings_declared(self):
        client = RabbitManagementClient()
        bindings = client.list_bindings()

        # Check for destination queue and source exchange
        triplets = {(b.get("source"), b.get("destination"), b.get("routing_key")) for b in bindings}

        assert ("emergency.events", "request.match", "request.match") in triplets
        assert ("emergency.retry", "request.match.retry", "request.match") in triplets
        assert ("emergency.dlx", "request.match.dlq", "request.match") in triplets
