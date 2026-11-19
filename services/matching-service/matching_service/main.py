"""Matching Service FastAPI application and health probes."""

from __future__ import annotations

from contextlib import asynccontextmanager

from ems_common.errors import StorageUnavailable, register_error_handlers
from ems_common.logging import register_logging_middleware, setup_json_logger
from fastapi import FastAPI

from matching_service.clients import RequestServiceClient, ResourceServiceClient
from matching_service.consumer import MatchingConsumer

logger = setup_json_logger(service_name="matching-service")


def create_app(
    consumer: MatchingConsumer | None = None,
    req_client: RequestServiceClient | None = None,
    res_client: ResourceServiceClient | None = None,
) -> FastAPI:
    request_client = req_client or RequestServiceClient()
    resource_client = res_client or ResourceServiceClient()
    matching_consumer = consumer or MatchingConsumer(
        req_client=request_client,
        res_client=resource_client,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):  # type: ignore[misc]
        matching_consumer.start()
        yield
        matching_consumer.stop()
        request_client.close()
        resource_client.close()

    app = FastAPI(title="Matching Service", lifespan=lifespan)
    register_logging_middleware(app)
    register_error_handlers(app)

    app.state.consumer = matching_consumer

    @app.get("/health")
    def health():
        return {"status": "ok", "service": "matching-service"}

    @app.get("/health/live")
    def health_live():
        return {"status": "ok"}

    @app.get("/health/ready")
    def health_ready():
        if matching_consumer.is_ready:
            return {"status": "ready"}
        raise StorageUnavailable("Consumer not connected to broker", headers={"Retry-After": "3"})

    return app


app = create_app()
