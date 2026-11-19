"""Component tests for Request Service publishing and outbox sweeper."""

from __future__ import annotations

import concurrent.futures
import uuid
from typing import Any

import httpx
import pytest
from ems_common.models import request_key
from ems_common.storage_client import CoordinatorClient
from fastapi.testclient import TestClient
from request_service.events import InMemoryEventPublisher
from request_service.main import create_app
from request_service.settings import Settings
from request_service.sweeper import OutboxSweeper

from tests.component.fake_coordinator import FakeCoordinatorApp

pytestmark = pytest.mark.component


class ControllablePublisher:
    """Fake publisher that can be configured to succeed or fail."""

    def __init__(self, succeed: bool = True) -> None:
        self.succeed = succeed
        self.published: list[dict[str, Any]] = []

    def publish(self, event: dict[str, Any]) -> bool:
        if not self.succeed:
            return False
        self.published.append(dict(event))
        return True


@pytest.fixture
def fake_coord():
    return FakeCoordinatorApp()


@pytest.fixture
def setup_harness(fake_coord):
    mock_coord_http = httpx.Client(transport=httpx.MockTransport(fake_coord.handle))
    coord_client = CoordinatorClient(
        base_url="http://test-coordinator",
        http_client=mock_coord_http,
    )

    publisher = ControllablePublisher(succeed=True)

    settings = Settings(
        coordinator_url="http://test-coordinator",
        resource_service_url="http://test-resource-service",
        event_publisher="in_memory",
        sweep_enabled=False,
    )

    app = create_app(
        storage_client=coord_client,
        settings=settings,
        event_publisher=publisher,
    )

    with TestClient(app) as tc:
        yield tc, fake_coord, publisher, coord_client


def test_publish_success_clears_flag(setup_harness):
    tc, _, pub, coord_client = setup_harness
    pub.succeed = True

    payload = {"type": "water", "location": {"lat": 1.0, "lon": 2.0}, "details": "Water"}
    resp = tc.post("/requests", json=payload)
    assert resp.status_code == 201
    data = resp.json()
    assert data["matchQueued"] is True
    req_id = data["requestId"]

    assert len(pub.published) == 1
    assert pub.published[0]["requestId"] == req_id
    assert pub.published[0]["type"] == "MATCH_REQUESTED"

    # Verify eventPending is False in storage
    rec, _ = coord_client.get(request_key(req_id))
    assert rec["eventPending"] is False


def test_publish_failure_retains_flag(setup_harness):
    tc, _, pub, coord_client = setup_harness
    pub.succeed = False

    payload = {"type": "food", "location": {"lat": 1.0, "lon": 2.0}, "details": "Food"}
    resp = tc.post("/requests", json=payload)
    assert resp.status_code == 201
    data = resp.json()
    assert data["matchQueued"] is False
    req_id = data["requestId"]

    assert len(pub.published) == 0

    # Verify eventPending is True in storage
    rec, _ = coord_client.get(request_key(req_id))
    assert rec["eventPending"] is True


def test_post_match_publishes_event(setup_harness):
    tc, _, pub, _ = setup_harness
    pub.succeed = True

    req_id = tc.post("/requests", json={"type": "water", "location": {"lat": 1.0, "lon": 2.0}, "details": "Water"}).json()["requestId"]
    pub.published.clear()

    # Manual match trigger
    resp_match = tc.post(f"/match/{req_id}")
    assert resp_match.status_code == 200
    assert resp_match.json()["matchQueued"] is True
    assert len(pub.published) == 1
    assert pub.published[0]["requestId"] == req_id
    assert pub.published[0]["type"] == "MATCH_REQUESTED"


def test_sweeper_republishes_eligible_requests(fake_coord):
    mock_coord_http = httpx.Client(transport=httpx.MockTransport(fake_coord.handle))
    coord_client = CoordinatorClient(
        base_url="http://test-coordinator",
        http_client=mock_coord_http,
    )
    pub = InMemoryEventPublisher()
    sweeper = OutboxSweeper(
        storage_client=coord_client,
        publisher=pub,
        interval_sec=1.0,
        grace_sec=0.0,  # 0s grace for immediate sweep in test
    )

    # 1. Eligible request: PENDING, eventPending=True, old timestamp
    req1 = str(uuid.uuid4())
    coord_client.create_if_absent(
        request_key(req1),
        {
            "requestId": req1,
            "type": "water",
            "status": "PENDING",
            "eventPending": True,
            "createdAt": "2026-01-01T00:00:00Z",
            "updatedAt": "2026-01-01T00:00:00Z",
        },
    )

    # 2. Ineligible request: PENDING, but eventPending=False
    req2 = str(uuid.uuid4())
    coord_client.create_if_absent(
        request_key(req2),
        {
            "requestId": req2,
            "type": "water",
            "status": "PENDING",
            "eventPending": False,
            "createdAt": "2026-01-01T00:00:00Z",
            "updatedAt": "2026-01-01T00:00:00Z",
        },
    )

    # 3. Ineligible request: MATCHED, eventPending=True (e.g. state changed)
    req3 = str(uuid.uuid4())
    coord_client.create_if_absent(
        request_key(req3),
        {
            "requestId": req3,
            "type": "food",
            "status": "MATCHED",
            "eventPending": True,
            "createdAt": "2026-01-01T00:00:00Z",
            "updatedAt": "2026-01-01T00:00:00Z",
        },
    )

    # Run sweeper
    count = sweeper.run_once()
    assert count == 1
    assert len(pub.published) == 1
    assert pub.published[0]["requestId"] == req1

    # Verify req1 flag cleared in storage
    rec1, _ = coord_client.get(request_key(req1))
    assert rec1["eventPending"] is False


def test_racing_sweepers_produce_no_errors(fake_coord):
    mock_coord_http = httpx.Client(transport=httpx.MockTransport(fake_coord.handle))
    coord_client = CoordinatorClient(
        base_url="http://test-coordinator",
        http_client=mock_coord_http,
    )
    pub = InMemoryEventPublisher()
    sweeper1 = OutboxSweeper(storage_client=coord_client, publisher=pub, grace_sec=0.0)
    sweeper2 = OutboxSweeper(storage_client=coord_client, publisher=pub, grace_sec=0.0)

    # Create 5 pending requests with eventPending=True
    req_ids = [str(uuid.uuid4()) for _ in range(5)]
    for rid in req_ids:
        coord_client.create_if_absent(
            request_key(rid),
            {
                "requestId": rid,
                "type": "water",
                "status": "PENDING",
                "eventPending": True,
                "createdAt": "2026-01-01T00:00:00Z",
                "updatedAt": "2026-01-01T00:00:00Z",
            },
        )

    # Run two sweepers concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
        f1 = ex.submit(sweeper1.run_once)
        f2 = ex.submit(sweeper2.run_once)
        assert f1.result() >= 0
        assert f2.result() >= 0

    # All requests should have eventPending cleared
    for rid in req_ids:
        r, _ = coord_client.get(request_key(rid))
        assert r["eventPending"] is False

    # Total published events must be >= 5 (duplicates allowed, but no unhandled exceptions)
    assert len(pub.published) >= 5
