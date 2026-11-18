"""Unit tests for errors and error handlers."""

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel, Field
import pytest

from ems_common.errors import (
    EmsError,
    InvalidInput,
    InvalidTransition,
    NotFound,
    StorageUnavailable,
    VersionConflict,
    register_error_handlers,
)


class DummyPayload(BaseModel):
    name: str = Field(..., min_length=2)
    count: int = Field(..., gt=0)


def create_test_app() -> FastAPI:
    app = FastAPI()
    register_error_handlers(app)

    @app.get("/trigger/invalid-input")
    def trigger_invalid_input():
        raise InvalidInput("custom invalid input", details=[{"field": "name", "problem": "invalid"}])

    @app.get("/trigger/not-found")
    def trigger_not_found():
        raise NotFound("item not found")

    @app.get("/trigger/version-conflict")
    def trigger_version_conflict():
        raise VersionConflict("version conflict occurred")

    @app.get("/trigger/invalid-transition")
    def trigger_invalid_transition():
        raise InvalidTransition("forbidden status jump")

    @app.get("/trigger/storage-unavailable")
    def trigger_storage_unavailable():
        raise StorageUnavailable("all shards down")

    @app.post("/trigger/validation")
    def trigger_validation(payload: DummyPayload):
        return {"status": "ok"}

    @app.get("/trigger/unhandled")
    def trigger_unhandled():
        raise RuntimeError("boom")

    return app


@pytest.fixture
def client():
    app = create_test_app()
    return TestClient(app, raise_server_exceptions=False)


@pytest.mark.unit
def test_invalid_input(client):
    res = client.get("/trigger/invalid-input")
    assert res.status_code == 400
    assert res.json() == {
        "error": {
            "code": "INVALID_INPUT",
            "message": "custom invalid input",
            "details": [{"field": "name", "problem": "invalid"}],
        }
    }


@pytest.mark.unit
def test_not_found(client):
    res = client.get("/trigger/not-found")
    assert res.status_code == 404
    assert res.json() == {
        "error": {
            "code": "NOT_FOUND",
            "message": "item not found",
            "details": [],
        }
    }


@pytest.mark.unit
def test_version_conflict(client):
    res = client.get("/trigger/version-conflict")
    assert res.status_code == 409
    assert res.json() == {
        "error": {
            "code": "VERSION_CONFLICT",
            "message": "version conflict occurred",
            "details": [],
        }
    }


@pytest.mark.unit
def test_invalid_transition(client):
    res = client.get("/trigger/invalid-transition")
    assert res.status_code == 409
    assert res.json() == {
        "error": {
            "code": "INVALID_TRANSITION",
            "message": "forbidden status jump",
            "details": [],
        }
    }


@pytest.mark.unit
def test_storage_unavailable(client):
    res = client.get("/trigger/storage-unavailable")
    assert res.status_code == 503
    assert res.json() == {
        "error": {
            "code": "STORAGE_UNAVAILABLE",
            "message": "all shards down",
            "details": [],
        }
    }


@pytest.mark.unit
def test_request_validation_error_mapped_to_400(client):
    # Missing fields and wrong type
    res = client.post("/trigger/validation", json={"name": "a", "count": -1})
    assert res.status_code == 400
    data = res.json()
    assert "error" in data
    assert data["error"]["code"] == "INVALID_INPUT"
    assert data["error"]["message"] == "Request validation failed"
    assert len(data["error"]["details"]) > 0
    # Ensure details list has {field, problem}
    fields = [d["field"] for d in data["error"]["details"]]
    assert "name" in fields or "count" in fields
    for d in data["error"]["details"]:
        assert "field" in d
        assert "problem" in d


@pytest.mark.unit
def test_unhandled_exception_mapped_to_500(client):
    res = client.get("/trigger/unhandled")
    assert res.status_code == 500
    assert res.json() == {
        "error": {
            "code": "INTERNAL_ERROR",
            "message": "Internal server error",
            "details": [],
        }
    }
