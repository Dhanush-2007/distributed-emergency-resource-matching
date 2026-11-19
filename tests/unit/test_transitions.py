"""Unit tests for request and resource state transitions."""

import pytest
from ems_common.errors import InvalidInput, InvalidTransition
from ems_common.transitions import (
    validate_request_client_transition,
    validate_resource_client_transition,
)

pytestmark = pytest.mark.unit


def test_valid_request_client_transitions():
    # MATCHED -> IN_PROGRESS (no release)
    st, rel = validate_request_client_transition("MATCHED", "IN_PROGRESS")
    assert st == "IN_PROGRESS"
    assert rel is False

    # IN_PROGRESS -> COMPLETED (releases)
    st, rel = validate_request_client_transition("IN_PROGRESS", "COMPLETED")
    assert st == "COMPLETED"
    assert rel is True

    # MATCHED -> COMPLETED (releases)
    st, rel = validate_request_client_transition("MATCHED", "COMPLETED")
    assert st == "COMPLETED"
    assert rel is True

    # MATCHED -> CANCELLED (releases)
    st, rel = validate_request_client_transition("MATCHED", "CANCELLED")
    assert st == "CANCELLED"
    assert rel is True

    # IN_PROGRESS -> CANCELLED (releases)
    st, rel = validate_request_client_transition("IN_PROGRESS", "CANCELLED")
    assert st == "CANCELLED"
    assert rel is True

    # PENDING -> CANCELLED (no release)
    st, rel = validate_request_client_transition("PENDING", "CANCELLED")
    assert st == "CANCELLED"
    assert rel is False

    # NO_MATCH -> CANCELLED (no release)
    st, rel = validate_request_client_transition("NO_MATCH", "CANCELLED")
    assert st == "CANCELLED"
    assert rel is False

    # Idempotent same state
    st, rel = validate_request_client_transition("CANCELLED", "CANCELLED")
    assert st == "CANCELLED"
    assert rel is False


def test_disallowed_request_client_transitions():
    # Client cannot set PENDING, MATCHED, NO_MATCH
    for target in ("PENDING", "MATCHED", "NO_MATCH"):
        with pytest.raises(InvalidTransition):
            validate_request_client_transition("CANCELLED", target)
        with pytest.raises(InvalidTransition):
            validate_request_client_transition("MATCHED", target)

    # Disallowed from terminal states
    with pytest.raises(InvalidTransition):
        validate_request_client_transition("COMPLETED", "IN_PROGRESS")
    with pytest.raises(InvalidTransition):
        validate_request_client_transition("COMPLETED", "CANCELLED")
    with pytest.raises(InvalidTransition):
        validate_request_client_transition("CANCELLED", "COMPLETED")

    # Invalid enum value -> 400 InvalidInput
    with pytest.raises(InvalidInput):
        validate_request_client_transition("PENDING", "UNKNOWN_STATUS")


def test_resource_client_transitions():
    # AVAILABLE <-> OFFLINE
    assert validate_resource_client_transition("AVAILABLE", "OFFLINE") == "OFFLINE"
    assert validate_resource_client_transition("OFFLINE", "AVAILABLE") == "AVAILABLE"
    assert validate_resource_client_transition("AVAILABLE", "AVAILABLE") == "AVAILABLE"
    assert validate_resource_client_transition("OFFLINE", "OFFLINE") == "OFFLINE"

    # Current ASSIGNED cannot be modified directly
    with pytest.raises(InvalidTransition):
        validate_resource_client_transition("ASSIGNED", "AVAILABLE")
    with pytest.raises(InvalidTransition):
        validate_resource_client_transition("ASSIGNED", "OFFLINE")

    # Target ASSIGNED is not client-settable
    with pytest.raises(InvalidTransition):
        validate_resource_client_transition("AVAILABLE", "ASSIGNED")
    with pytest.raises(InvalidTransition):
        validate_resource_client_transition("OFFLINE", "ASSIGNED")

    # Invalid enum value -> 400 InvalidInput
    with pytest.raises(InvalidInput):
        validate_resource_client_transition("AVAILABLE", "INVALID_STATUS")
