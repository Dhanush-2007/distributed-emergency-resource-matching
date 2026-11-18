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
    ) -> None:
        super().__init__(message)
        self.http_status = http_status
        self.code = code
        self.message = message
        self.details = details or []

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
    ) -> None:
        super().__init__(
            http_status=503,
            code="STORAGE_UNAVAILABLE",
            message=message,
            details=details,
        )


def register_error_handlers(app: FastAPI) -> None:
    """Register custom exception handlers on a FastAPI application."""

    @app.exception_handler(EmsError)
    async def ems_error_handler(request: Request, exc: EmsError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=exc.to_dict())

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        details = []
        for error in exc.errors():
            loc = error.get("loc", ())
            # Ignore leading body/query/path location if present or join cleanly
            field_parts = [str(part) for part in loc if part not in ("body",)]
            field = ".".join(field_parts) if field_parts else "body"
            details.append({"field": field, "problem": error.get("msg", "invalid")})

        err = InvalidInput(message="Request validation failed", details=details)
        return JSONResponse(status_code=err.http_status, content=err.to_dict())

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(
        request: Request, exc: Exception
    ) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content={
                "error": {
                    "code": "INTERNAL_ERROR",
                    "message": "Internal server error",
                    "details": [],
                }
            },
        )
