"""Domain models, enums and validation schemas for requests and resources."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ems_common.errors import InvalidInput


class RequestType(str, Enum):
    WATER = "water"
    FOOD = "food"
    MEDICINE = "medicine"
    TRANSPORTATION = "transportation"
    SHELTER = "shelter"


class RequestStatus(str, Enum):
    PENDING = "PENDING"
    MATCHED = "MATCHED"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    NO_MATCH = "NO_MATCH"
    CANCELLED = "CANCELLED"


class ResourceStatus(str, Enum):
    AVAILABLE = "AVAILABLE"
    ASSIGNED = "ASSIGNED"
    OFFLINE = "OFFLINE"


class MatchOutcome(str, Enum):
    MATCHED = "MATCHED"
    NO_MATCH = "NO_MATCH"


class Location(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    lat: float = Field(..., ge=-90.0, le=90.0)
    lon: float = Field(..., ge=-180.0, le=180.0)


class CreateRequestBody(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    type: RequestType
    location: Location
    details: str = Field(..., min_length=1, max_length=1000)


class StatusBody(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    status: str

    @field_validator("status")
    @classmethod
    def validate_non_empty(cls, v: str) -> str:
        s = v.strip()
        if not s:
            raise ValueError("status cannot be empty")
        return s


class CreateResourceBody(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    name: str | None = Field(default=None, min_length=1, max_length=100)
    type: RequestType
    location: Location
    availability: bool


class MatchResultBody(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    outcome: MatchOutcome
    resource_id: str | None = Field(default=None, alias="resourceId")
    version: int | None = Field(default=None, alias="version")

    @model_validator(mode="before")
    @classmethod
    def handle_version_aliases(cls, data: Any) -> Any:
        if isinstance(data, dict):
            d = dict(data)
            if "expectedVersion" in d and "version" not in d:
                d["version"] = d.pop("expectedVersion")
            return d
        return data

    @model_validator(mode="after")
    def check_resource_id_for_matched(self) -> MatchResultBody:
        if self.outcome == MatchOutcome.MATCHED:
            if not self.resource_id:
                raise ValueError("resourceId is required when outcome is MATCHED")
            self.resource_id = parse_uuid(self.resource_id, field_name="resourceId")
        return self


class ReserveBody(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    request_id: str = Field(..., alias="requestId")
    expected_version: int | None = Field(default=None, alias="expectedVersion")

    @model_validator(mode="before")
    @classmethod
    def handle_version_aliases(cls, data: Any) -> Any:
        if isinstance(data, dict):
            d = dict(data)
            if "version" in d and "expectedVersion" not in d:
                d["expectedVersion"] = d.pop("version")
            return d
        return data

    @field_validator("request_id")
    @classmethod
    def validate_req_id(cls, v: str) -> str:
        return parse_uuid(v, field_name="requestId")


class ReleaseBody(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    request_id: str = Field(..., alias="requestId")

    @field_validator("request_id")
    @classmethod
    def validate_req_id(cls, v: str) -> str:
        return parse_uuid(v, field_name="requestId")


def parse_uuid(value: Any, field_name: str = "id") -> str:
    """Parse string value into canonical lowercase UUID string or raise InvalidInput."""
    if not isinstance(value, str):
        raise InvalidInput(
            f"Invalid UUID for {field_name}",
            details=[{"field": field_name, "problem": "Expected string UUID"}],
        )
    try:
        val_uuid = uuid.UUID(value.strip())
        return str(val_uuid).lower()
    except (ValueError, AttributeError):
        raise InvalidInput(
            f"Invalid UUID for {field_name}",
            details=[{"field": field_name, "problem": f"Invalid UUID '{value}'"}],
        ) from None


def request_key(req_id: str) -> str:
    return f"request:{parse_uuid(req_id, field_name='id')}"


def resource_key(res_id: str) -> str:
    return f"resource:{parse_uuid(res_id, field_name='id')}"


def now_iso() -> str:
    """Generate server-side UTC ISO-8601 timestamp with trailing Z."""
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
