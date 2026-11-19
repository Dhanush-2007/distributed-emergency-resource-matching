"""Unit tests for ems_common models, enums and validation."""

import pytest
from ems_common.errors import InvalidInput
from ems_common.models import (
    CreateRequestBody,
    CreateResourceBody,
    Location,
    MatchOutcome,
    MatchResultBody,
    ReleaseBody,
    RequestStatus,
    RequestType,
    ReserveBody,
    ResourceStatus,
    now_iso,
    parse_uuid,
    request_key,
    resource_key,
)
from pydantic import ValidationError

pytestmark = pytest.mark.unit


def test_enums():
    assert RequestType.WATER == "water"
    assert RequestStatus.PENDING == "PENDING"
    assert ResourceStatus.AVAILABLE == "AVAILABLE"
    assert MatchOutcome.MATCHED == "MATCHED"


def test_location_validation():
    loc = Location(lat=45.0, lon=-122.0)
    assert loc.lat == 45.0
    assert loc.lon == -122.0

    with pytest.raises(ValidationError):
        Location(lat=95.0, lon=0.0)

    with pytest.raises(ValidationError):
        Location(lat=0.0, lon=-185.0)

    # Forbid extra fields
    with pytest.raises(ValidationError):
        Location(lat=0.0, lon=0.0, extra_field="bad")


def test_create_request_body():
    body = CreateRequestBody(
        type=RequestType.WATER,
        location=Location(lat=10.0, lon=20.0),
        details="Need fresh drinking water",
    )
    assert body.type == RequestType.WATER
    assert body.details == "Need fresh drinking water"

    # Details min length 1
    with pytest.raises(ValidationError):
        CreateRequestBody(
            type=RequestType.WATER,
            location=Location(lat=10.0, lon=20.0),
            details="",
        )

    # Forbid extra fields like version or status on create
    with pytest.raises(ValidationError):
        CreateRequestBody.model_validate(
            {
                "type": "water",
                "location": {"lat": 10.0, "lon": 20.0},
                "details": "Water",
                "status": "PENDING",
            }
        )


def test_create_resource_body():
    body = CreateResourceBody(
        name="Truck-1",
        type=RequestType.TRANSPORTATION,
        location=Location(lat=10.0, lon=20.0),
        availability=True,
    )
    assert body.name == "Truck-1"
    assert body.availability is True

    # Name is optional
    body2 = CreateResourceBody(
        type=RequestType.FOOD,
        location=Location(lat=10.0, lon=20.0),
        availability=False,
    )
    assert body2.name is None
    assert body2.availability is False


def test_match_result_body():
    valid_uuid = "12345678-1234-5678-1234-567812345678"
    m_body = MatchResultBody(outcome=MatchOutcome.MATCHED, resourceId=valid_uuid)
    assert m_body.resource_id == valid_uuid

    # Outcome MATCHED requires resourceId
    with pytest.raises(ValidationError):
        MatchResultBody(outcome=MatchOutcome.MATCHED)

    # Outcome NO_MATCH does not require resourceId
    nm_body = MatchResultBody(outcome=MatchOutcome.NO_MATCH)
    assert nm_body.resource_id is None


def test_reserve_and_release_bodies():
    valid_uuid = "12345678-1234-5678-1234-567812345678"
    r_body = ReserveBody(requestId=valid_uuid, expectedVersion=2)
    assert r_body.request_id == valid_uuid
    assert r_body.expected_version == 2

    rel_body = ReleaseBody(requestId=valid_uuid)
    assert rel_body.request_id == valid_uuid


def test_parse_uuid_and_keys():
    canonical = "12345678-1234-5678-1234-567812345678"
    upper = canonical.upper()

    assert parse_uuid(upper) == canonical
    assert request_key(upper) == f"request:{canonical}"
    assert resource_key(upper) == f"resource:{canonical}"

    with pytest.raises(InvalidInput) as exc:
        parse_uuid("not-a-uuid", field_name="requestId")
    assert exc.value.details[0]["field"] == "requestId"


def test_now_iso():
    ts = now_iso()
    assert ts.endswith("Z")
    assert "T" in ts
