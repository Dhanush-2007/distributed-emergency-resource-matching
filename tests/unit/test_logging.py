"""Unit tests for JSON logging and correlation ID middleware."""

import json
import logging
from unittest.mock import patch
import uuid

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from ems_common.logging import (
    JSONFormatter,
    correlation_id_ctx,
    get_correlation_id,
    register_logging_middleware,
    setup_json_logger,
)


@pytest.mark.unit
def test_json_formatter_structure():
    formatter = JSONFormatter(service_name="test-service")
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="test.py",
        lineno=10,
        msg="test message",
        args=(),
        exc_info=None,
    )
    # Set correlation id in context
    token = correlation_id_ctx.set("corr-12345")
    try:
        output = formatter.format(record)
        data = json.loads(output)
        assert data["level"] == "INFO"
        assert data["service"] == "test-service"
        assert data["event"] == "test message"
        assert data["correlationId"] == "corr-12345"
        assert "ts" in data
    finally:
        correlation_id_ctx.reset(token)


@pytest.mark.unit
def test_json_formatter_extra_context():
    formatter = JSONFormatter(service_name="test-service")
    record = logging.LogRecord(
        name="test",
        level=logging.WARNING,
        pathname="test.py",
        lineno=10,
        msg="resource reserved",
        args=(),
        exc_info=None,
    )
    record.__dict__["resourceId"] = "res-1"
    record.__dict__["requestId"] = "req-1"

    output = formatter.format(record)
    data = json.loads(output)
    assert data["resourceId"] == "res-1"
    assert data["requestId"] == "req-1"


@pytest.mark.unit
def test_correlation_id_middleware_generates_and_echoes():
    app = FastAPI()
    register_logging_middleware(app)

    observed_cid = None

    @app.get("/test")
    def test_endpoint():
        nonlocal observed_cid
        observed_cid = get_correlation_id()
        return {"status": "ok"}

    client = TestClient(app)

    # 1. Request without X-Correlation-Id -> generated and returned
    res = client.get("/test")
    assert res.status_code == 200
    returned_cid = res.headers.get("X-Correlation-Id")
    assert returned_cid is not None
    # Validate UUID
    uuid.UUID(returned_cid)
    assert observed_cid == returned_cid

    # 2. Request with X-Correlation-Id -> preserved and returned
    custom_cid = "custom-id-999"
    res2 = client.get("/test", headers={"X-Correlation-Id": custom_cid})
    assert res2.status_code == 200
    assert res2.headers.get("X-Correlation-Id") == custom_cid
    assert observed_cid == custom_cid


@pytest.mark.unit
def test_setup_json_logger():
    logger = setup_json_logger(service_name="custom-service")
    assert logger.name == "custom-service"
    assert len(logger.handlers) > 0
    assert isinstance(logger.handlers[0].formatter, JSONFormatter)
