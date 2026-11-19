"""Error definitions and FastAPI exception handlers."""

from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from starlette.responses import JSONResponse


class EmsError(Exception):
    """Base exception for EMS application errors."""

    def __init__(
        self,
        http_status: int,
        code: str,
        message: str,
        details: list[dict[str, Any]] | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(message)
        self.http_status = http_status
        self.code = code
        self.message = message
        self.details = details or []
        self.headers = headers or {}

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            }
        }


class InvalidInput(EmsError):
    def __init__(
        self,
        message: str = "Request validation failed",
        details: list[dict[str, Any]] | None = None,
    ) -> None:
        super().__init__(
            http_status=400,
            code="INVALID_INPUT",
            message=message,
            details=details,
        )


class NotFound(EmsError):
    def __init__(
        self,
        message: str = "Resource not found",
        details: list[dict[str, Any]] | None = None,
    ) -> None:
        super().__init__(
            http_status=404,
            code="NOT_FOUND",
            message=message,
            details=details,
        )


class VersionConflict(EmsError):
    def __init__(
        self,
        message: str = "Version conflict",
        details: list[dict[str, Any]] | None = None,
    ) -> None:
        super().__init__(
            http_status=409,
            code="VERSION_CONFLICT",
            message=message,
            details=details,
        )


class InvalidTransition(EmsError):
    def __init__(
        self,
        message: str = "Invalid status transition",
        details: list[dict[str, Any]] | None = None,
    ) -> None:
        super().__init__(
            http_status=409,
            code="INVALID_TRANSITION",
            message=message,
            details=details,
        )


class StorageUnavailable(EmsError):
    def __init__(
        self,
        message: str = "Storage unavailable",
        details: list[dict[str, Any]] | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(
            http_status=503,
            code="STORAGE_UNAVAILABLE",
            message=message,
            details=details,
            headers=headers,
        )


class StaleEpoch(EmsError):
    def __init__(
        self,
        message: str = "Stale epoch",
        details: list[dict[str, Any]] | None = None,
    ) -> None:
        super().__init__(
            http_status=409,
            code="STALE_EPOCH",
            message=message,
            details=details,
        )


class NotPrimary(EmsError):
    def __init__(
        self,
        message: str = "Node is not primary",
        details: list[dict[str, Any]] | None = None,
    ) -> None:
        super().__init__(
            http_status=409,
            code="NOT_PRIMARY",
            message=message,
            details=details,
        )


def _extract_cid(request: Request) -> str | None:
    for k, v in request.headers.items():
        if k.lower() == "x-correlation-id":
            return v
    return None


def register_error_handlers(app: FastAPI) -> None:
    """Register custom exception handlers on a FastAPI application."""

    @app.exception_handler(EmsError)
    async def ems_error_handler(request: Request, exc: EmsError) -> JSONResponse:
        resp = JSONResponse(
            status_code=exc.http_status,
            content=exc.to_dict(),
            headers=exc.headers if exc.headers else None,
        )
        cid = _extract_cid(request)
        if cid:
            resp.headers["X-Correlation-Id"] = cid
        return resp

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        details = []
        for error in exc.errors():
            loc = error.get("loc", ())
            # Ignore leading body/query/path location if present or join cleanly
            field_parts = [
                str(part)
                for part in loc
                if part not in ("body",) and not str(part).isdigit()
            ]
            field = ".".join(field_parts) if field_parts else "body"
            details.append({"field": field, "problem": error.get("msg", "invalid")})

        err = InvalidInput(message="Request validation failed", details=details)
        resp = JSONResponse(status_code=err.http_status, content=err.to_dict())
        cid = _extract_cid(request)
        if cid:
            resp.headers["X-Correlation-Id"] = cid
        return resp

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(
        request: Request, exc: Exception
    ) -> JSONResponse:
        resp = JSONResponse(
            status_code=500,
            content={
                "error": {
                    "code": "INTERNAL_ERROR",
                    "message": "Internal server error",
                    "details": [],
                }
            },
        )
        cid = _extract_cid(request)
        if cid:
            resp.headers["X-Correlation-Id"] = cid
        return resp
