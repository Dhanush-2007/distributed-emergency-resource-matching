"""Unit tests for CoordinatorClient storage operations."""

import json
from typing import Any

import httpx
import pytest
from ems_common.errors import NotFound, VersionConflict
from ems_common.logging import correlation_id_ctx
from ems_common.storage_client import CoordinatorClient

pytestmark = pytest.mark.unit


def test_get_success_and_not_found():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/internal/data/req:1":
            return httpx.Response(200, json={"value": {"x": 1}, "version": 3})
        return httpx.Response(404, json={"error": {"code": "NOT_FOUND", "message": "Not found"}})

    client = CoordinatorClient(
        base_url="http://test-coord:8000",
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    val, ver = client.get("req:1")
    assert val == {"x": 1}
    assert ver == 3

    with pytest.raises(NotFound):
        client.get("req:2")


def test_get_retries_on_503():
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            return httpx.Response(
                503,
                headers={"Retry-After": "0.01"},
                json={"error": {"code": "STORAGE_UNAVAILABLE", "message": "Busy"}},
            )
        return httpx.Response(200, json={"value": {"ok": True}, "version": 1})

    client = CoordinatorClient(
        base_url="http://test-coord:8000",
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        retries=3,
    )
    val, ver = client.get("req:retry")
    assert val == {"ok": True}
    assert ver == 1
    assert attempts == 3


def test_put_version_conflict_and_storage_unavailable():
    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode())
        if body.get("ifVersion") == 0:
            return httpx.Response(
                409, json={"error": {"code": "VERSION_CONFLICT", "message": "Exists"}}
            )
        return httpx.Response(200, json={"key": "k", "version": 2})

    client = CoordinatorClient(
        base_url="http://test-coord:8000",
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    with pytest.raises(VersionConflict):
        client.put("k", {"x": 1}, if_version=0)

    status, body = client.put("k", {"x": 1}, if_version=1)
    assert status == 200
    assert body["version"] == 2


def test_scan_and_allow_partial():
    def handler(request: httpx.Request) -> httpx.Response:
        allow_part = request.url.params.get("allowPartial") == "true"
        if allow_part:
            return httpx.Response(
                200,
                json={"items": [{"key": "r:1", "value": {"v": 1}}], "unavailableShards": [1]},
            )
        return httpx.Response(200, json={"items": [{"key": "r:1", "value": {"v": 1}}], "unavailableShards": []})

    client = CoordinatorClient(
        base_url="http://test-coord:8000",
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    items, partial = client.scan(prefix="r:", allow_partial=True)
    assert len(items) == 1
    assert partial is True

    items2, partial2 = client.scan(prefix="r:", allow_partial=False)
    assert len(items2) == 1
    assert partial2 is False


def test_create_if_absent_idempotent_retry():
    storage: dict[str, dict[str, Any]] = {}
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        path = request.url.path
        key = path.split("/")[-1]
        if request.method == "GET":
            if key in storage:
                return httpx.Response(200, json={"value": storage[key]["value"], "version": storage[key]["version"]})
            return httpx.Response(404, json={"error": {"code": "NOT_FOUND"}})

        if request.method == "PUT":
            attempts += 1
            body = json.loads(request.content.decode())
            if attempts == 1:
                # Save internally, but pretend network crashed on response
                storage[key] = {"value": body["value"], "version": 1}
                return httpx.Response(503, headers={"Retry-After": "0.01"}, json={"error": {"code": "STORAGE_UNAVAILABLE"}})
            # Second attempt receives 409 because key exists
            if key in storage:
                return httpx.Response(409, json={"error": {"code": "VERSION_CONFLICT"}})
            storage[key] = {"value": body["value"], "version": 1}
            return httpx.Response(201, json={"version": 1})

        return httpx.Response(404)

    client = CoordinatorClient(
        base_url="http://test-coord:8000",
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        retries=3,
    )
    val, ver = client.create_if_absent("req:absent", {"data": 123})
    assert val == {"data": 123}
    assert ver == 1


def test_update_optimistic_concurrency():
    storage = {"counter": 1}
    version = 1

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal version
        if request.method == "GET":
            return httpx.Response(200, json={"value": storage, "version": version})
        if request.method == "PUT":
            body = json.loads(request.content.decode())
            if body.get("ifVersion") != version:
                return httpx.Response(409, json={"error": {"code": "VERSION_CONFLICT"}})
            storage.update(body["value"])
            version += 1
            return httpx.Response(200, json={"version": version})
        return httpx.Response(404)

    client = CoordinatorClient(
        base_url="http://test-coord:8000",
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    def inc(record: dict[str, Any]) -> dict[str, Any]:
        record["counter"] += 1
        return record

    new_val, new_ver = client.update("item:1", inc)
    assert new_val["counter"] == 2
    assert new_ver == 2


def test_correlation_id_propagated():
    observed_cid = None

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal observed_cid
        observed_cid = request.headers.get("X-Correlation-Id")
        return httpx.Response(200, json={"value": {}, "version": 1})

    client = CoordinatorClient(
        base_url="http://test-coord:8000",
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    token = correlation_id_ctx.set("corr-abc-123")
    try:
        client.get("test:key")
        assert observed_cid == "corr-abc-123"
    finally:
        correlation_id_ctx.reset(token)
