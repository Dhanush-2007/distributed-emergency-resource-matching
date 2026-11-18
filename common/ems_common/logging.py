"""Structured JSON logging and correlation ID support."""

import contextvars
import json
import logging
from datetime import datetime, timezone
from typing import Any
import uuid

from fastapi import FastAPI, Request, Response
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

# Context variable for request correlation ID
correlation_id_ctx: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "correlation_id", default=None
)


def get_correlation_id() -> str | None:
    return correlation_id_ctx.get()


def set_correlation_id(cid: str) -> None:
    correlation_id_ctx.set(cid)


class JSONFormatter(logging.Formatter):
    """Custom formatter outputting logs as JSON objects."""

    def __init__(self, service_name: str = "ems") -> None:
        super().__init__()
        self.service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": getattr(record, "service", self.service_name),
            "event": record.getMessage(),
            "correlationId": get_correlation_id()
            or getattr(record, "correlationId", None),
        }

        # Include extra fields if passed via extra={}
        standard_attrs = {
            "name",
            "msg",
            "args",
            "levelname",
            "levelno",
            "pathname",
            "filename",
            "module",
            "exc_info",
            "exc_text",
            "stack_info",
            "lineno",
            "funcName",
            "created",
            "msecs",
            "relativeCreated",
            "thread",
            "threadName",
            "processName",
            "process",
            "message",
            "service",
            "event",
            "correlationId",
            "ts",
        }

        extra_context = {
            k: v for k, v in record.__dict__.items() if k not in standard_attrs
        }
        if extra_context:
            log_entry.update(extra_context)

        return json.dumps(log_entry)


def setup_json_logger(
    service_name: str = "ems", level: int = logging.INFO
) -> logging.Logger:
    """Configure and return the root or service logger with JSON formatting."""
    logger = logging.getLogger(service_name)
    logger.setLevel(level)

    # Avoid adding duplicate handlers if re-called
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JSONFormatter(service_name=service_name))
        logger.addHandler(handler)

    return logger


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """Middleware that reads or generates X-Correlation-Id and echoes it in responses."""

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        cid = request.headers.get("X-Correlation-Id")
        if not cid:
            cid = str(uuid.uuid4())

        token = correlation_id_ctx.set(cid)
        try:
            response = await call_next(request)
            response.headers["X-Correlation-Id"] = cid
            return response
        finally:
            correlation_id_ctx.reset(token)


def register_logging_middleware(app: FastAPI) -> None:
    """Register correlation ID middleware on a FastAPI app."""
    app.add_middleware(CorrelationIdMiddleware)
