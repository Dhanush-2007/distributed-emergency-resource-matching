"""Pure functions for request and resource state transitions."""

from __future__ import annotations

from ems_common.errors import InvalidInput, InvalidTransition
from ems_common.models import RequestStatus, ResourceStatus

CLIENT_ALLOWED_REQUEST_TARGETS = {
    RequestStatus.IN_PROGRESS.value,
    RequestStatus.COMPLETED.value,
    RequestStatus.CANCELLED.value,
}

VALID_REQUEST_TRANSITIONS: dict[str, set[str]] = {
    RequestStatus.MATCHED.value: {
        RequestStatus.IN_PROGRESS.value,
        RequestStatus.COMPLETED.value,
        RequestStatus.CANCELLED.value,
    },
    RequestStatus.IN_PROGRESS.value: {
        RequestStatus.COMPLETED.value,
        RequestStatus.CANCELLED.value,
    },
    RequestStatus.PENDING.value: {
        RequestStatus.CANCELLED.value,
    },
    RequestStatus.NO_MATCH.value: {
        RequestStatus.CANCELLED.value,
    },
}

RESOURCE_RELEASING_TRANSITIONS = {
    (RequestStatus.MATCHED.value, RequestStatus.COMPLETED.value),
    (RequestStatus.MATCHED.value, RequestStatus.CANCELLED.value),
    (RequestStatus.IN_PROGRESS.value, RequestStatus.COMPLETED.value),
    (RequestStatus.IN_PROGRESS.value, RequestStatus.CANCELLED.value),
}


def validate_request_client_transition(
    current_status: str, target_status: str
) -> tuple[str, bool]:
    """Validate client-initiated transition for a request.

    Returns:
        tuple[str, bool]: (canonical target status, whether the transition releases a resource)
    Raises:
        InvalidInput: If target_status is not a valid enum value (HTTP 400).
        InvalidTransition: If transition is disallowed or target is not client-settable (HTTP 409).
    """
    valid_statuses = {s.value for s in RequestStatus}
    target_clean = target_status.strip().upper()
    if target_clean not in valid_statuses:
        raise InvalidInput(
            f"Invalid request status '{target_status}'",
            details=[{"field": "status", "problem": f"Invalid status '{target_status}'"}],
        )

    # Client cannot set PENDING, MATCHED, or NO_MATCH
    if target_clean not in CLIENT_ALLOWED_REQUEST_TARGETS:
        raise InvalidTransition(
            f"Clients cannot set status to '{target_clean}'",
            details=[{"field": "status", "problem": f"Status '{target_clean}' cannot be set by client"}],
        )

    current_clean = current_status.strip().upper()

    # Idempotent no-op transition
    if current_clean == target_clean:
        return target_clean, False

    allowed_targets = VALID_REQUEST_TRANSITIONS.get(current_clean, set())
    if target_clean not in allowed_targets:
        raise InvalidTransition(
            f"Cannot transition request from '{current_clean}' to '{target_clean}'",
            details=[{"current": current_clean, "target": target_clean}],
        )

    releases_resource = (current_clean, target_clean) in RESOURCE_RELEASING_TRANSITIONS
    return target_clean, releases_resource


def validate_resource_client_transition(
    current_status: str, target_status: str
) -> str:
    """Validate client-initiated transition for a resource.

    Returns:
        str: canonical target status
    Raises:
        InvalidInput: If target_status is not a valid enum value (HTTP 400).
        InvalidTransition: If target is ASSIGNED or current is ASSIGNED (HTTP 409).
    """
    valid_statuses = {s.value for s in ResourceStatus}
    target_clean = target_status.strip().upper()
    if target_clean not in valid_statuses:
        raise InvalidInput(
            f"Invalid resource status '{target_status}'",
            details=[{"field": "status", "problem": f"Invalid status '{target_status}'"}],
        )

    current_clean = current_status.strip().upper()

    if current_clean == ResourceStatus.ASSIGNED.value:
        raise InvalidTransition(
            "Cannot modify status of an ASSIGNED resource",
            details=[{"current": current_clean, "target": target_clean}],
        )

    if target_clean == ResourceStatus.ASSIGNED.value:
        raise InvalidTransition(
            "Clients cannot set resource status to ASSIGNED",
            details=[{"current": current_clean, "target": target_clean}],
        )

    # Allowed: AVAILABLE <-> OFFLINE (and idempotent no-op)
    valid_pairs = {
        (ResourceStatus.AVAILABLE.value, ResourceStatus.AVAILABLE.value),
        (ResourceStatus.AVAILABLE.value, ResourceStatus.OFFLINE.value),
        (ResourceStatus.OFFLINE.value, ResourceStatus.AVAILABLE.value),
        (ResourceStatus.OFFLINE.value, ResourceStatus.OFFLINE.value),
    }

    if (current_clean, target_clean) not in valid_pairs:
        raise InvalidTransition(
            f"Cannot transition resource from '{current_clean}' to '{target_clean}'",
            details=[{"current": current_clean, "target": target_clean}],
        )

    return target_clean
