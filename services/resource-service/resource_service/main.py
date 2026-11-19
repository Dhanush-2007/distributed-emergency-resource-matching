"""Resource Service application and endpoints."""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from typing import Any

import httpx
from ems_common.errors import (
    InvalidInput,
    NotFound,
    ResourceAlreadyAssigned,
    StorageUnavailable,
    VersionConflict,
    register_error_handlers,
)
from ems_common.logging import register_logging_middleware, setup_json_logger
from ems_common.models import (
    CreateResourceBody,
    ReleaseBody,
    RequestType,
    ReserveBody,
    ResourceStatus,
    StatusBody,
    now_iso,
    parse_uuid,
    resource_key,
)
from ems_common.storage_client import CoordinatorClient
from ems_common.transitions import validate_resource_client_transition
from fastapi import FastAPI, Query, Response

from resource_service.reconciler import Reconciler
from resource_service.settings import Settings

logger = setup_json_logger(service_name="resource-service")


def create_app(
    storage_client: CoordinatorClient | None = None,
    settings: Settings | None = None,
    http_client: httpx.Client | None = None,
) -> FastAPI:
    cfg = settings or Settings()
    coord_client = storage_client or CoordinatorClient(
        base_url=cfg.coordinator_url,
        timeout=cfg.http_timeout_sec,
    )

    reconciler: Reconciler | None = None
    if cfg.reconcile_enabled:
        reconciler = Reconciler(
            storage_client=coord_client,
            request_service_url=cfg.request_service_url,
            grace_sec=cfg.reconcile_grace_sec,
            interval_sec=cfg.reconcile_interval_sec,
            http_client=http_client,
        )

    @asynccontextmanager
    async def lifespan(app: FastAPI):  # type: ignore[misc]
        if reconciler is not None:
            reconciler.start()
        yield
        if reconciler is not None:
            reconciler.stop()
        coord_client.close()

    app = FastAPI(title="Resource Service", lifespan=lifespan)
    register_logging_middleware(app)
    register_error_handlers(app)

    app.state.storage_client = coord_client
    app.state.reconciler = reconciler

    @app.post("/resources", status_code=201)
    def create_resource(body: CreateResourceBody, response: Response):
        res_id = str(uuid.uuid4())
        initial_status = ResourceStatus.AVAILABLE.value if body.availability else ResourceStatus.OFFLINE.value
        record = {
            "resourceId": res_id,
            "name": body.name,
            "type": body.type.value,
            "location": body.location.model_dump(),
            "availability": body.availability,
            "status": initial_status,
            "assignedRequestId": None,
            "createdAt": now_iso(),
            "updatedAt": now_iso(),
        }
        val, ver = coord_client.create_if_absent(resource_key(res_id), record)
        response.headers["Location"] = f"/resources/{res_id}"
        logger.info(
            "Created resource",
            extra={"event": "resource.created", "resourceId": res_id, "type": body.type.value},
        )
        return dict(val, version=ver)

    @app.get("/resources")
    def list_resources(
        type: str | None = Query(None),
        status: str | None = Query(None),
        availability: str | None = Query(None),
    ):
        if type is not None and type not in {t.value for t in RequestType}:
            raise InvalidInput(f"Invalid type filter '{type}'", details=[{"field": "type", "problem": "invalid_type"}])
        if status is not None and status not in {s.value for s in ResourceStatus}:
            raise InvalidInput(f"Invalid status filter '{status}'", details=[{"field": "status", "problem": "invalid_status"}])
        avail_bool: bool | None = None
        if availability is not None:
            av_lower = availability.strip().lower()
            if av_lower == "true":
                avail_bool = True
            elif av_lower == "false":
                avail_bool = False
            else:
                raise InvalidInput(f"Invalid availability '{availability}'", details=[{"field": "availability", "problem": "expected_bool"}])

        items, _ = coord_client.scan("resource:", allow_partial=False)
        results = []
        for it in items:
            rec = it.get("value", {})
            if type is not None and rec.get("type") != type:
                continue
            if status is not None and rec.get("status") != status:
                continue
            if avail_bool is not None and rec.get("availability") is not avail_bool:
                continue
            if "resourceId" not in rec:
                k = it.get("key", "")
                if k.startswith("resource:"):
                    rec["resourceId"] = k[len("resource:"):]
            results.append(dict(rec, version=it.get("version", 1)))

        results.sort(key=lambda r: (r.get("createdAt", ""), r.get("resourceId", "")))
        return {"items": results, "count": len(results)}

    @app.get("/resources/{resource_id}")
    def get_resource(resource_id: str):
        cid = parse_uuid(resource_id)
        val, ver = coord_client.get(resource_key(cid))
        return dict(val, version=ver)

    @app.put("/resources/{resource_id}/status")
    def update_resource_status(resource_id: str, body: StatusBody):
        cid = parse_uuid(resource_id)

        def mutate(rec: dict[str, Any]) -> dict[str, Any]:
            target_st = validate_resource_client_transition(rec["status"], body.status)
            rec["status"] = target_st
            rec["availability"] = (target_st == ResourceStatus.AVAILABLE.value)
            rec["updatedAt"] = now_iso()
            return rec

        val, ver = coord_client.update(resource_key(cid), mutate)
        return dict(val, version=ver)

    @app.get("/internal/resources/candidates")
    def get_candidates(type: str = Query(...)):
        if type not in {t.value for t in RequestType}:
            raise InvalidInput(f"Invalid candidate type '{type}'", details=[{"field": "type", "problem": "invalid_type"}])

        items, is_partial = coord_client.scan("resource:", allow_partial=True)
        results = []
        for it in items:
            rec = it.get("value", {})
            if rec.get("type") == type and rec.get("status") == "AVAILABLE" and rec.get("availability") is True:
                if "resourceId" not in rec:
                    k = it.get("key", "")
                    if k.startswith("resource:"):
                        rec["resourceId"] = k[len("resource:"):]
                results.append(dict(rec, version=it.get("version", 1)))

        return {"items": results, "count": len(results), "partial": is_partial}

    @app.get("/internal/resources/by-request/{request_id}")
    def get_by_request(request_id: str):
        req_uuid = parse_uuid(request_id, field_name="requestId")
        items, _ = coord_client.scan("resource:", allow_partial=False)
        for it in items:
            rec = it.get("value", {})
            if rec.get("status") == "ASSIGNED" and rec.get("assignedRequestId") == req_uuid:
                return dict(rec, version=it.get("version", 1))

        raise NotFound(f"No resource assigned to request '{req_uuid}'")

    @app.post("/internal/resources/{resource_id}/reserve")
    def reserve_resource(resource_id: str, body: ReserveBody):
        cid = parse_uuid(resource_id)
        logger.info("Attempting reservation", extra={"event": "reserve.attempt", "resourceId": cid, "requestId": body.request_id})

        val, ver = coord_client.get(resource_key(cid))
        if val.get("status") == "ASSIGNED" and val.get("assignedRequestId") == body.request_id:
            logger.info("Reservation idempotent ok", extra={"event": "reserve.ok", "resourceId": cid})
            return dict(val, version=ver)

        if val.get("status") != "AVAILABLE":
            logger.warning("Reservation conflict", extra={"event": "reserve.conflict", "resourceId": cid})
            raise ResourceAlreadyAssigned(f"Resource '{cid}' is not available for reservation")

        if body.expected_version is not None and body.expected_version != ver:
            logger.warning("Reservation version conflict", extra={"event": "reserve.conflict", "resourceId": cid})
            raise VersionConflict(f"Resource '{cid}' version mismatch: expected {body.expected_version}, got {ver}")

        val["status"] = "ASSIGNED"
        val["availability"] = False
        val["assignedRequestId"] = body.request_id
        val["updatedAt"] = now_iso()

        try:
            _, res = coord_client.put(resource_key(cid), val, if_version=ver)
            new_ver = res.get("version", ver + 1)
            logger.info("Reservation successful", extra={"event": "reserve.ok", "resourceId": cid})
            return dict(val, version=new_ver)
        except VersionConflict as exc:
            logger.warning("Reservation version conflict", extra={"event": "reserve.conflict", "resourceId": cid})
            raise ResourceAlreadyAssigned(f"Resource '{cid}' reservation conflict") from exc

    @app.post("/internal/resources/{resource_id}/release")
    def release_resource(resource_id: str, body: ReleaseBody):
        cid = parse_uuid(resource_id)
        released = False

        def mutate(rec: dict[str, Any]) -> dict[str, Any]:
            nonlocal released
            if rec.get("assignedRequestId") == body.request_id:
                rec["status"] = "AVAILABLE"
                rec["availability"] = True
                rec["assignedRequestId"] = None
                rec["updatedAt"] = now_iso()
                released = True
            return rec

        val, ver = coord_client.update(resource_key(cid), mutate)
        if released:
            logger.info("Resource released", extra={"event": "release", "resourceId": cid, "requestId": body.request_id})
        return {"released": released, "resource": dict(val, version=ver)}

    @app.get("/health")
    def health():
        return {"status": "ok", "service": "resource-service"}

    @app.get("/health/live")
    def health_live():
        return {"status": "ok"}

    @app.get("/health/ready")
    def health_ready():
        try:
            r = httpx.get(f"{cfg.coordinator_url}/health/live", timeout=1.0)
            if r.status_code == 200:
                return {"status": "ready"}
        except Exception:  # noqa: BLE001, S110
            pass
        raise StorageUnavailable("Coordinator unreachable", headers={"Retry-After": "3"})

    return app


app = create_app()
