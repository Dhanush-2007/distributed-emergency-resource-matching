"""Unit tests for the matching algorithm (SPEC 8.5)."""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock

import pytest
from matching_service.clients import Transient
from matching_service.matcher import match_request

pytestmark = pytest.mark.unit


class FakeClients:
    """Mock clients for testing matcher logic."""

    def __init__(self) -> None:
        self.req_client = MagicMock()
        self.res_client = MagicMock()
        self.released: list[str] = []

        # Default release mock
        def fake_release(res_id: str, req_id: str) -> bool:
            self.released.append(res_id)
            return True

        self.res_client.release.side_effect = fake_release
        self.res_client.by_request.return_value = None


def test_nearest_of_type_chosen():
    f = FakeClients()
    req_id = str(uuid.uuid4())
    req_data = {
        "requestId": req_id,
        "type": "water",
        "location": {"lat": 12.0, "lon": 77.0},
        "status": "PENDING",
    }
    f.req_client.get_request.return_value = (200, req_data)

    # 2 water at different distances, 1 food
    w_far = {"resourceId": "w-far", "type": "water", "location": {"lat": 13.0, "lon": 77.0}, "version": 1}
    w_near = {"resourceId": "w-near", "type": "water", "location": {"lat": 12.1, "lon": 77.0}, "version": 1}

    f.res_client.candidates.return_value = ([w_far, w_near], False)
    f.res_client.reserve.return_value = 200
    f.req_client.match_result.return_value = (200, {"status": "MATCHED"})

    outcome = match_request(req_id, f.req_client, f.res_client)
    assert outcome == "MATCHED"

    # Nearest water must be reserved first
    first_reserve_call = f.res_client.reserve.call_args_list[0]
    assert first_reserve_call[0][0] == "w-near"

    # match_result must be called with w-near
    f.req_client.match_result.assert_called_once_with(req_id, outcome="MATCHED", resource_id="w-near")


def test_tie_broken_by_resource_id():
    f = FakeClients()
    req_id = str(uuid.uuid4())
    f.req_client.get_request.return_value = (200, {"requestId": req_id, "type": "water", "location": {"lat": 10.0, "lon": 20.0}, "status": "PENDING"})

    # Two candidates at exact same location
    c_b = {"resourceId": "b-res", "type": "water", "location": {"lat": 10.0, "lon": 20.0}, "version": 1}
    c_a = {"resourceId": "a-res", "type": "water", "location": {"lat": 10.0, "lon": 20.0}, "version": 1}

    f.res_client.candidates.return_value = ([c_b, c_a], False)
    f.res_client.reserve.return_value = 200
    f.req_client.match_result.return_value = (200, {"status": "MATCHED"})

    outcome = match_request(req_id, f.req_client, f.res_client)
    assert outcome == "MATCHED"

    # Lexicographical tie break: a-res before b-res
    assert f.res_client.reserve.call_args_list[0][0][0] == "a-res"


def test_no_match_when_no_candidates():
    f = FakeClients()
    req_id = str(uuid.uuid4())
    f.req_client.get_request.return_value = (200, {"requestId": req_id, "type": "water", "location": {"lat": 0, "lon": 0}, "status": "PENDING"})

    f.res_client.candidates.return_value = ([], False)
    f.req_client.match_result.return_value = (200, {"status": "NO_MATCH"})

    outcome = match_request(req_id, f.req_client, f.res_client)
    assert outcome == "NO_MATCH"
    f.req_client.match_result.assert_called_once_with(req_id, outcome="NO_MATCH")


def test_partial_and_empty_candidates_raises_transient():
    f = FakeClients()
    req_id = str(uuid.uuid4())
    f.req_client.get_request.return_value = (200, {"requestId": req_id, "type": "water", "location": {"lat": 0, "lon": 0}, "status": "PENDING"})

    # Candidates empty, but partial is True (a shard was down)
    f.res_client.candidates.return_value = ([], True)

    with pytest.raises(Transient):
        match_request(req_id, f.req_client, f.res_client)


def test_max_match_distance_km(monkeypatch):
    monkeypatch.setenv("MAX_MATCH_DISTANCE_KM", "50.0")
    f = FakeClients()
    req_id = str(uuid.uuid4())
    f.req_client.get_request.return_value = (200, {"requestId": req_id, "type": "water", "location": {"lat": 12.0, "lon": 77.0}, "status": "PENDING"})

    # Candidate 200 km away
    c_far = {"resourceId": "c-far", "type": "water", "location": {"lat": 14.0, "lon": 77.0}, "version": 1}
    # Initial candidates and retry candidates both return only far candidate
    f.res_client.candidates.return_value = ([c_far], False)
    f.req_client.match_result.return_value = (200, {"status": "NO_MATCH"})

    outcome = match_request(req_id, f.req_client, f.res_client)
    assert outcome == "NO_MATCH"
    # Never attempted reservation because it's beyond max distance
    assert f.res_client.reserve.call_count == 0


def test_lost_race_409_tries_next_candidate():
    f = FakeClients()
    req_id = str(uuid.uuid4())
    f.req_client.get_request.return_value = (200, {"requestId": req_id, "type": "water", "location": {"lat": 10.0, "lon": 20.0}, "status": "PENDING"})

    c1 = {"resourceId": "c1", "type": "water", "location": {"lat": 10.1, "lon": 20.0}, "version": 1}
    c2 = {"resourceId": "c2", "type": "water", "location": {"lat": 10.2, "lon": 20.0}, "version": 1}

    f.res_client.candidates.return_value = ([c1, c2], False)
    # First candidate gives 409 conflict, second succeeds
    f.res_client.reserve.side_effect = [409, 200]
    f.req_client.match_result.return_value = (200, {"status": "MATCHED"})

    outcome = match_request(req_id, f.req_client, f.res_client)
    assert outcome == "MATCHED"
    assert f.res_client.reserve.call_count == 2
    f.req_client.match_result.assert_called_once_with(req_id, outcome="MATCHED", resource_id="c2")


def test_by_request_idempotent_retry():
    f = FakeClients()
    req_id = str(uuid.uuid4())
    f.req_client.get_request.return_value = (200, {"requestId": req_id, "type": "water", "status": "PENDING"})

    # Already reserved in previous crashed attempt
    f.res_client.by_request.return_value = {"resourceId": "already-reserved"}
    f.req_client.match_result.return_value = (200, {"status": "MATCHED"})

    outcome = match_request(req_id, f.req_client, f.res_client)
    assert outcome == "MATCHED"

    # candidates() should never be queried because by_request hit
    assert f.res_client.candidates.call_count == 0
    f.req_client.match_result.assert_called_once_with(req_id, outcome="MATCHED", resource_id="already-reserved")


def test_match_result_409_releases_resource():
    f = FakeClients()
    req_id = str(uuid.uuid4())
    f.req_client.get_request.return_value = (200, {"requestId": req_id, "type": "water", "location": {"lat": 0, "lon": 0}, "status": "PENDING"})

    cand = {"resourceId": "c1", "type": "water", "location": {"lat": 0, "lon": 0}, "version": 1}
    f.res_client.candidates.return_value = ([cand], False)
    f.res_client.reserve.return_value = 200

    # Request changed status concurrently (e.g. cancelled) -> matchresult returns 409
    f.req_client.match_result.return_value = (409, {"error": "INVALID_TRANSITION"})

    outcome = match_request(req_id, f.req_client, f.res_client)
    assert outcome == "DUPLICATE"
    # Resource MUST be released
    assert "c1" in f.released


def test_failed_match_result_releases_and_raises_transient():
    f = FakeClients()
    req_id = str(uuid.uuid4())
    f.req_client.get_request.return_value = (200, {"requestId": req_id, "type": "water", "location": {"lat": 0, "lon": 0}, "status": "PENDING"})

    cand = {"resourceId": "c1", "type": "water", "location": {"lat": 0, "lon": 0}, "version": 1}
    f.res_client.candidates.return_value = ([cand], False)
    f.res_client.reserve.return_value = 200

    # match_result fails with Transient on all 3 attempts
    f.req_client.match_result.side_effect = Transient("Network down")

    with pytest.raises(Transient):
        match_request(req_id, f.req_client, f.res_client)

    # Resource MUST be released to compensate
    assert "c1" in f.released


def test_duplicate_when_not_pending():
    f = FakeClients()
    req_id = str(uuid.uuid4())
    f.req_client.get_request.return_value = (200, {"requestId": req_id, "status": "CANCELLED"})

    outcome = match_request(req_id, f.req_client, f.res_client)
    assert outcome == "DUPLICATE"
    assert f.res_client.candidates.call_count == 0


def test_unknown_request_dropped():
    f = FakeClients()
    req_id = str(uuid.uuid4())
    f.req_client.get_request.return_value = (404, {"error": "NOT_FOUND"})

    outcome = match_request(req_id, f.req_client, f.res_client)
    assert outcome == "DROPPED"


def test_fault_match_fail(monkeypatch):
    monkeypatch.setenv("FAULT_MATCH_FAIL", "2")
    f = FakeClients()
    req_id = str(uuid.uuid4())
    f.req_client.get_request.return_value = (200, {"requestId": req_id, "status": "PENDING"})

    # Attempt 0 < 2 -> raises Transient
    with pytest.raises(Transient):
        match_request(req_id, f.req_client, f.res_client, retry_count=0)

    # Attempt 1 < 2 -> raises Transient
    with pytest.raises(Transient):
        match_request(req_id, f.req_client, f.res_client, retry_count=1)

    # Attempt 2 >= 2 -> does not raise fault
    f.res_client.candidates.return_value = ([], False)
    f.req_client.match_result.return_value = (200, {"status": "NO_MATCH"})
    outcome = match_request(req_id, f.req_client, f.res_client, retry_count=2)
    assert outcome == "NO_MATCH"


def test_matcher_complete_first_scan_partial_second_scan_raises_transient():
    """Bug 3 regression: First scan complete but conflict; second scan partial -> must raise Transient."""
    f = FakeClients()
    req_id = str(uuid.uuid4())
    f.req_client.get_request.return_value = (200, {"requestId": req_id, "type": "water", "location": {"lat": 0, "lon": 0}, "status": "PENDING"})

    cand1 = {"resourceId": "c1", "type": "water", "location": {"lat": 0, "lon": 0}, "version": 1}
    cand2 = {"resourceId": "c2", "type": "water", "location": {"lat": 0, "lon": 0}, "version": 1}

    # First scan returns cand1 (complete: partial=False)
    # Second scan returns cand2 (incomplete: partial=True)
    f.res_client.candidates.side_effect = [
        ([cand1], False),
        ([cand2], True),
    ]

    # Both candidates return 409 conflict during reservation
    f.res_client.reserve.return_value = 409

    # Must raise Transient because latest scan was partial
    with pytest.raises(Transient):
        match_request(req_id, f.req_client, f.res_client)

    # match_result must NOT be called with NO_MATCH
    assert f.req_client.match_result.call_count == 0


def test_matcher_partial_first_scan_complete_second_scan_records_no_match():
    """Bug 3 regression: First scan partial but conflict; second scan complete -> records NO_MATCH."""
    f = FakeClients()
    req_id = str(uuid.uuid4())
    f.req_client.get_request.return_value = (200, {"requestId": req_id, "type": "water", "location": {"lat": 0, "lon": 0}, "status": "PENDING"})

    cand1 = {"resourceId": "c1", "type": "water", "location": {"lat": 0, "lon": 0}, "version": 1}

    # First scan returns cand1 (partial=True)
    # Second scan returns empty list (complete: partial=False)
    f.res_client.candidates.side_effect = [
        ([cand1], True),
        ([], False),
    ]

    # cand1 returns 409 conflict
    f.res_client.reserve.return_value = 409
    f.req_client.match_result.return_value = (200, {"status": "NO_MATCH"})

    outcome = match_request(req_id, f.req_client, f.res_client)
    assert outcome == "NO_MATCH"
    f.req_client.match_result.assert_called_once_with(req_id, outcome="NO_MATCH")

