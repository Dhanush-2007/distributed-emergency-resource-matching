"""FastAPI application for the coordinator core."""

import os
import time
from collections.abc import Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
from ems_common.errors import InvalidInput, NotFound, register_error_handlers
from ems_common.logging import register_logging_middleware, setup_json_logger
from fastapi import FastAPI, Query, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from coordinator.diagnostics import DiagnosticsService
from coordinator.registry import NodeRegistry
from coordinator.router import ShardRouter
from coordinator.shard_state import ShardStateManager


class Settings:
    """Coordinator settings configured via environment variables."""

    def __init__(
        self,
        port: int | None = None,
        n_shards: int | None = None,
        heartbeat_interval_sec: float | None = None,
        missed_heartbeats_limit: int | None = None,
        state_dir: Path | str | None = None,
        http_timeout_sec: float | None = None,
    ) -> None:
        self.port: int = port if port is not None else int(os.getenv("PORT", "8000"))
        self.n_shards: int = (
            n_shards if n_shards is not None else int(os.getenv("N_SHARDS", "3"))
        )
        self.heartbeat_interval_sec: float = (
            heartbeat_interval_sec
            if heartbeat_interval_sec is not None
            else float(os.getenv("HEARTBEAT_INTERVAL_SEC", "2"))
        )
        self.missed_heartbeats_limit: int = (
            missed_heartbeats_limit
            if missed_heartbeats_limit is not None
            else int(os.getenv("MISSED_HEARTBEATS_LIMIT", "3"))
        )

        if state_dir is not None:
            self.state_dir: Path = Path(state_dir)
            self.state_dir.mkdir(parents=True, exist_ok=True)
        else:
            raw_state_dir = os.getenv("STATE_DIR", "/state")
            state_path = Path(raw_state_dir)
            try:
                state_path.mkdir(parents=True, exist_ok=True)
                test_file = state_path / ".perm_check"
                test_file.touch()
                test_file.unlink()
                self.state_dir = state_path
            except (PermissionError, OSError):
                local_fallback = Path("./state")
                local_fallback.mkdir(parents=True, exist_ok=True)
                self.state_dir = local_fallback

        self.http_timeout_sec: float = (
            http_timeout_sec
            if http_timeout_sec is not None
            else float(os.getenv("HTTP_TIMEOUT_SEC", "3"))
        )


class RegisterNodeRequest(BaseModel):
    nodeId: str
    shardId: int
    endpoint: str
    role: str = "PRIMARY"
    epoch: int = 1
    lastSeq: int = 0
    status: str = "STARTING"


class HeartbeatRequest(BaseModel):
    nodeId: str
    shardId: int
    role: str
    epoch: int
    status: str
    lastSeq: int
    replicaAckSeq: int | None = None
    recordCount: dict[str, int] = Field(default_factory=dict)


class PutDataRequest(BaseModel):
    value: dict[str, Any] = Field(...)
    ifVersion: int | None = None


def create_app(
    settings: Settings | None = None,
    http_client: Any = None,
    clock: Callable[[], float] = time.time,
) -> FastAPI:
    """Factory to create a configured coordinator FastAPI application."""
    conf = settings or Settings()
    logger = setup_json_logger(service_name="coordinator")

    registry = NodeRegistry(
        heartbeat_interval=conf.heartbeat_interval_sec,
        missed_heartbeats_limit=conf.missed_heartbeats_limit,
        clock=clock,
    )
    shard_state = ShardStateManager(state_dir=conf.state_dir)

    owns_http = False
    client_instance = http_client
    if client_instance is None:
        client_instance = httpx.Client(timeout=conf.http_timeout_sec)
        owns_http = True

    router = ShardRouter(
        registry=registry,
        shard_state=shard_state,
        n_shards=conf.n_shards,
        http_client=client_instance,
    )
    diagnostics = DiagnosticsService(
        registry=registry,
        shard_state=shard_state,
        n_shards=conf.n_shards,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):  # type: ignore[misc]
        yield
        if owns_http and hasattr(client_instance, "close"):
            client_instance.close()

    application = FastAPI(title="EMS Coordinator", lifespan=lifespan)
    register_logging_middleware(application)
    register_error_handlers(application)

    # Expose registry, shard state, and router for testing
    application.state.settings = conf
    application.state.registry = registry
    application.state.shard_state = shard_state
    application.state.router = router
    application.state.diagnostics = diagnostics
    application.state.http_client = client_instance

    @application.get("/health/live")
    def health_live() -> dict[str, str]:
        return {"status": "ok"}

    @application.get("/health/ready")
    def health_ready() -> dict[str, str]:
        return {"status": "ok"}

    @application.get("/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "service": "coordinator",
            "shards": conf.n_shards,
        }

    @application.post("/internal/nodes/register")
    def register_node(payload: RegisterNodeRequest) -> dict[str, Any]:
        node_id = payload.nodeId.strip() if payload.nodeId else ""
        endpoint = payload.endpoint.strip() if payload.endpoint else ""
        if not node_id:
            raise InvalidInput(
                "nodeId must be non-empty",
                details=[{"field": "nodeId", "problem": "empty"}],
            )
        if not endpoint:
            raise InvalidInput(
                "endpoint must be non-empty",
                details=[{"field": "endpoint", "problem": "empty"}],
            )
        if payload.shardId < 0 or payload.shardId >= conf.n_shards:
            raise InvalidInput(
                f"shardId must be between 0 and {conf.n_shards - 1}",
                details=[{
                    "field": "shardId",
                    "problem": f"out of range [0, {conf.n_shards - 1}]",
                }],
            )

        sid = payload.shardId
        existing_primary = shard_state.get_primary(sid)
        stored_epoch = shard_state.get_epoch(sid)

        if existing_primary:
            if node_id == existing_primary:
                assigned_role = "PRIMARY"
            else:
                assigned_role = "REPLICA"
            assigned_epoch = (
                stored_epoch if stored_epoch is not None else max(1, payload.epoch)
            )
        else:
            role_hint = (payload.role or "PRIMARY").upper()
            if role_hint == "REPLICA":
                assigned_role = "REPLICA"
                assigned_epoch = max(1, payload.epoch)
            else:
                assigned_role = "PRIMARY"
                assigned_epoch = max(1, payload.epoch)
                shard_state.set_primary(sid, node_id, assigned_epoch)
                logger.info(
                    "Shard primary assigned",
                    extra={
                        "event": "shard.primary_assigned",
                        "shardId": sid,
                        "nodeId": node_id,
                        "epoch": assigned_epoch,
                    },
                )

        registry.register(
            node_id=node_id,
            shard_id=sid,
            endpoint=endpoint,
            role=assigned_role,
            epoch=assigned_epoch,
            status=payload.status,
            last_seq=payload.lastSeq,
        )

        logger.info(
            "Node registered",
            extra={
                "event": "node.register",
                "nodeId": node_id,
                "shardId": sid,
                "role": assigned_role,
                "epoch": assigned_epoch,
                "status": payload.status,
            },
        )

        return {"role": assigned_role, "epoch": assigned_epoch}

    @application.post("/internal/nodes/heartbeat")
    def node_heartbeat(payload: HeartbeatRequest) -> dict[str, Any]:
        node = registry.get(payload.nodeId)
        if node is None:
            raise NotFound(
                "unknown node, please register",
                details=[{"field": "nodeId", "problem": "node not registered"}],
            )

        registry.update_heartbeat(
            node_id=payload.nodeId,
            shard_id=payload.shardId,
            role=payload.role,
            epoch=payload.epoch,
            status=payload.status,
            last_seq=payload.lastSeq,
            replica_ack_seq=payload.replicaAckSeq,
            record_count=payload.recordCount,
        )

        sid = payload.shardId
        primary_id = shard_state.get_primary(sid)
        stored_epoch = shard_state.get_epoch(sid)

        epoch = stored_epoch if stored_epoch is not None else payload.epoch
        if primary_id:
            role = "PRIMARY" if payload.nodeId == primary_id else "REPLICA"
        else:
            role = payload.role.upper()

        logger.debug(
            "Node heartbeat",
            extra={
                "event": "node.heartbeat",
                "nodeId": payload.nodeId,
                "shardId": sid,
                "role": role,
                "epoch": epoch,
            },
        )

        return {"role": role, "epoch": epoch}

    # ------------------------------------------------------------------
    # Data routing endpoints
    # ------------------------------------------------------------------

    @application.put("/internal/data/{key:path}")
    def put_data(key: str, payload: PutDataRequest, request: Request) -> Response:
        cid = request.headers.get("x-correlation-id")
        code, body = router.put_data(
            key=key,
            value=payload.value,
            if_version=payload.ifVersion,
            correlation_id=cid,
        )
        return JSONResponse(status_code=code, content=body)

    @application.get("/internal/data/{key:path}")
    def get_data(key: str, request: Request) -> Response:
        cid = request.headers.get("x-correlation-id")
        code, body = router.get_data(key=key, correlation_id=cid)
        return JSONResponse(status_code=code, content=body)

    @application.delete("/internal/data/{key:path}")
    def delete_data(key: str, request: Request) -> Response:
        cid = request.headers.get("x-correlation-id")
        code, body = router.delete_data(key=key, correlation_id=cid)
        return JSONResponse(status_code=code, content=body)

    @application.get("/internal/scan")
    def scan_data(
        request: Request,
        prefix: str = Query(default=""),
        allowPartial: bool = Query(default=False),
    ) -> dict[str, Any]:
        cid = request.headers.get("x-correlation-id")
        return router.scan_data(
            prefix=prefix,
            allow_partial=allowPartial,
            correlation_id=cid,
        )

    # ------------------------------------------------------------------
    # Diagnostics endpoints
    # ------------------------------------------------------------------

    @application.get("/diagnostics/route")
    def diagnostics_route(key: str | None = Query(default=None)) -> dict[str, Any]:
        return diagnostics.get_route(key)

    @application.get("/diagnostics/nodes")
    def diagnostics_nodes() -> list[dict[str, Any]]:
        return diagnostics.get_nodes()

    @application.get("/diagnostics/shards")
    def diagnostics_shards() -> list[dict[str, Any]]:
        return diagnostics.get_shards()

    return application


app = create_app()
