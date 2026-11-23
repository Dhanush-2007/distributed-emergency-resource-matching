"""Request Service application and endpoints."""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from typing import Any

import httpx
from ems_common.errors import (
    InvalidInput,
    InvalidTransition,
    StorageUnavailable,
    VersionConflict,
    register_error_handlers,
)
from ems_common.logging import register_logging_middleware, setup_json_logger
from ems_common.models import (
    CreateRequestBody,
    MatchOutcome,
    MatchResultBody,
    RequestStatus,
    RequestType,
    StatusBody,
    now_iso,
    parse_uuid,
    request_key,
)
from ems_common.storage_client import CoordinatorClient
from ems_common.transitions import validate_request_client_transition
from fastapi import FastAPI, Query, Response

from request_service.events import (
    EventPublisher,
    InMemoryEventPublisher,
    NoopEventPublisher,
    RabbitPublisher,
    create_match_event,
)
from request_service.settings import Settings
from request_service.sweeper import OutboxSweeper

logger = setup_json_logger(service_name="request-service")


def _to_public_record(record: dict[str, Any], version: int) -> dict[str, Any]:
    """Strip internal fields like eventPending from public API responses."""
    pub = dict(record)
    pub.pop("eventPending", None)
    pub["version"] = version
    return pub


def create_app(
    storage_client: CoordinatorClient | None = None,
    settings: Settings | None = None,
    event_publisher: EventPublisher | None = None,
    http_client: httpx.Client | None = None,
) -> FastAPI:
    cfg = settings or Settings()
    coord_client = storage_client or CoordinatorClient(
        base_url=cfg.coordinator_url,
        timeout=cfg.http_timeout_sec,
    )

    if event_publisher is not None:
        publisher = event_publisher
    elif cfg.event_publisher.lower() == "rabbit":
        publisher = RabbitPublisher(
            host=cfg.rabbitmq_host,
            port=cfg.rabbitmq_port,
            user=cfg.rabbitmq_user,
            password=cfg.rabbitmq_pass,
        )
    elif cfg.event_publisher.lower() == "in_memory":
        publisher = InMemoryEventPublisher()
    else:
        publisher = NoopEventPublisher()

    sweeper: OutboxSweeper | None = None
    if cfg.sweep_enabled:
        sweeper = OutboxSweeper(
            storage_client=coord_client,
            publisher=publisher,
            interval_sec=cfg.sweep_interval_sec,
            grace_sec=cfg.sweep_grace_sec,
        )

    owns_http = False
    if http_client is None:
        http_client = httpx.Client(timeout=cfg.http_timeout_sec)
        owns_http = True

    @asynccontextmanager
    async def lifespan(app: FastAPI):  # type: ignore[misc]
        if sweeper is not None:
            sweeper.start()
        yield
        if sweeper is not None:
            sweeper.stop()
        if hasattr(publisher, "close"):
            publisher.close()
        coord_client.close()
        if owns_http and hasattr(http_client, "close"):
            try:
                http_client.close()
            except Exception:  # noqa: BLE001, S110
                pass

    app = FastAPI(title="Request Service", lifespan=lifespan)
    register_logging_middleware(app)
    register_error_handlers(app)

    app.state.storage_client = coord_client
    app.state.publisher = publisher
    app.state.http_client = http_client
    app.state.sweeper = sweeper

    @app.post("/requests", status_code=201)
    def create_request(body: CreateRequestBody, response: Response):
        req_id = str(uuid.uuid4())
        created_time = now_iso()
        record = {
            "requestId": req_id,
            "type": body.type.value,
            "location": body.location.model_dump(),
            "details": body.details,
            "status": RequestStatus.PENDING.value,
            "matchedResourceId": None,
            "matchAttemptCount": 0,
            "eventPending": True,
            "createdAt": created_time,
            "updatedAt": created_time,
        }

        evt = create_match_event(
            request_id=req_id,
            req_type=body.type.value,
            location=body.location.model_dump(),
            created_at=created_time,
        )

        if not isinstance(publisher, RabbitPublisher):
            published = publisher.publish(evt)
            if published:
                record["eventPending"] = False
            val, ver = coord_client.create_if_absent(request_key(req_id), record)
        else:
            val, ver = coord_client.create_if_absent(request_key(req_id), record)
            published = publisher.publish(evt)
            if published:
                def clear_pending(rec: dict[str, Any]) -> dict[str, Any]:
                    rec["eventPending"] = False
                    return rec

                try:
                    val, ver = coord_client.update(request_key(req_id), clear_pending)
                except Exception:  # noqa: BLE001, S110
                    pass

        response.headers["Location"] = f"/requests/{req_id}"
        logger.info(
            "Created request",
            extra={"event": "request.created", "requestId": req_id, "type": body.type.value},
        )
        res_payload = _to_public_record(val, ver)
        res_payload["matchQueued"] = published
        return res_payload

    @app.get("/requests/{request_id}")
    def get_request(request_id: str):
        cid = parse_uuid(request_id)
        val, ver = coord_client.get(request_key(cid))
        return _to_public_record(val, ver)

    @app.get("/requests")
    def list_requests(
        type: str | None = Query(None),
        status: str | None = Query(None),
    ):
        if type is not None and type not in {t.value for t in RequestType}:
            raise InvalidInput(f"Invalid type filter '{type}'", details=[{"field": "type", "problem": "invalid_type"}])
        if status is not None and status not in {s.value for s in RequestStatus}:
            raise InvalidInput(f"Invalid status filter '{status}'", details=[{"field": "status", "problem": "invalid_status"}])

        items, is_partial = coord_client.scan("request:", allow_partial=True)
        results = []
        for it in items:
            rec = it.get("value", {})
            if type is not None and rec.get("type") != type:
                continue
            if status is not None and rec.get("status") != status:
                continue
            if "requestId" not in rec:
                k = it.get("key", "")
                if k.startswith("request:"):
                    rec["requestId"] = k[len("request:"):]
            results.append(_to_public_record(rec, it.get("version", 1)))

        results.sort(key=lambda r: (r.get("createdAt", ""), r.get("requestId", "")))
        return {"items": results, "count": len(results), "partial": is_partial}

    @app.put("/requests/{request_id}/status")
    def update_request_status(request_id: str, body: StatusBody):
        cid = parse_uuid(request_id)
        resource_id_to_release: str | None = None
        should_release = False

        def mutate(rec: dict[str, Any]) -> dict[str, Any]:
            nonlocal resource_id_to_release, should_release
            target_st, rel = validate_request_client_transition(rec.get("status", ""), body.status)
            should_release = rel
            resource_id_to_release = rec.get("matchedResourceId")
            rec["status"] = target_st
            rec["updatedAt"] = now_iso()
            return rec

        val, ver = coord_client.update(request_key(cid), mutate)

        if should_release and resource_id_to_release:
            for attempt in range(3):
                try:
                    rel_resp = http_client.post(
                        f"{cfg.resource_service_url}/internal/resources/{resource_id_to_release}/release",
                        json={"requestId": cid},
                        timeout=cfg.http_timeout_sec,
                    )
                    if rel_resp.status_code == 200:
                        break
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "Attempt %s: failed to release resource %s for request %s: %s",
                        attempt + 1,
                        resource_id_to_release,
                        cid,
                        exc,
                    )

        return _to_public_record(val, ver)

    @app.post("/match/{request_id}")
    def trigger_match(request_id: str):
        cid = parse_uuid(request_id)
        val, _ver = coord_client.get(request_key(cid))
        curr_status = val.get("status")

        if curr_status in (
            RequestStatus.MATCHED.value,
            RequestStatus.IN_PROGRESS.value,
            RequestStatus.COMPLETED.value,
            RequestStatus.CANCELLED.value,
        ):
            raise InvalidTransition(f"Cannot trigger match for request in status '{curr_status}'")

        def mutate_match(rec: dict[str, Any]) -> dict[str, Any]:
            if rec.get("status") == RequestStatus.NO_MATCH.value:
                rec["status"] = RequestStatus.PENDING.value
                rec["updatedAt"] = now_iso()
            rec["eventPending"] = True
            return rec

        updated_val, _ = coord_client.update(request_key(cid), mutate_match)

        evt = create_match_event(
            request_id=cid,
            req_type=updated_val["type"],
            location=updated_val["location"],
            created_at=updated_val.get("createdAt", now_iso()),
        )
        published = publisher.publish(evt)
        if published:
            def clear_pending(rec: dict[str, Any]) -> dict[str, Any]:
                rec["eventPending"] = False
                return rec

            try:
                coord_client.update(request_key(cid), clear_pending)
            except Exception:  # noqa: BLE001, S110
                pass

        return {"requestId": cid, "status": RequestStatus.PENDING.value, "matchQueued": published}

    @app.get("/internal/requests/{request_id}")
    def get_internal_request(request_id: str):
        cid = parse_uuid(request_id)
        val, ver = coord_client.get(request_key(cid))
        return dict(val, version=ver)

    @app.post("/internal/requests/{request_id}/matchresult")
    @app.post("/internal/requests/{request_id}/match-result")
    def apply_match_result(request_id: str, body: MatchResultBody):
        cid = parse_uuid(request_id)
        val, ver = coord_client.get(request_key(cid))

        # Idempotent check
        if val.get("status") == body.outcome.value and val.get("matchedResourceId") == body.resource_id:
            return dict(val, version=ver)

        if body.version is not None and body.version != ver:
            raise VersionConflict(f"Request '{cid}' version mismatch: expected {body.version}, got {ver}")

        curr_st = val.get("status")
        if curr_st != RequestStatus.PENDING.value:
            raise InvalidTransition(f"Cannot apply match result to request in status '{curr_st}'")

        val["status"] = body.outcome.value
        val["matchedResourceId"] = body.resource_id if body.outcome == MatchOutcome.MATCHED else None
        val["eventPending"] = False
        val["matchAttemptCount"] = val.get("matchAttemptCount", 0) + 1
        val["updatedAt"] = now_iso()

        _, res = coord_client.put(request_key(cid), val, if_version=ver)
        new_ver = res.get("version", ver + 1)
        return dict(val, version=new_ver)

    @app.get("/health")
    def health():
        return {"status": "ok", "service": "request-service"}

    @app.get("/health/live")
    def health_live():
        return {"status": "ok"}

    @app.get("/health/ready")
    def health_ready():
        try:
            r = http_client.get(f"{cfg.coordinator_url}/health/live", timeout=1.0)
            if r.status_code == 200:
                return {"status": "ready"}
        except Exception:  # noqa: BLE001, S110
            pass
        raise StorageUnavailable("Coordinator unreachable", headers={"Retry-After": "3"})

    return app


app = create_app()
