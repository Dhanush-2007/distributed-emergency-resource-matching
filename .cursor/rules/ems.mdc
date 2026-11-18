---
description: Project rules for the Distributed Emergency Resource Matching System
alwaysApply: true
---

# Project rules (always applied)

The authoritative design is `docs/SPEC.md`. Read the sections relevant to the task before writing code. If a prompt and the spec disagree, stop and ask; do not guess.

## Scope discipline
- Implement ONLY what the current prompt asks. Do not add features, endpoints, services, files or dependencies that were not requested.
- Do not start the next stage. Do not change contracts (API shapes, status values, key names, error codes, message formats) unless explicitly told to.
- When finished, list the files you created or changed and show the test results.

## Stack and layout
- Python 3.11, FastAPI + uvicorn, pydantic v2, httpx, pika (RabbitMQ), SQLite for storage nodes, pytest, ruff.
- Monorepo: `common/ems_common/` (shared library), `services/<name>/app/` (storage-node, coordinator, request-service, resource-service, matching-service), `k8s/`, `scripts/`, `tests/{unit,component,integration,fault}/`, `docs/`.
- Ports: coordinator 8000, request-service 8001, resource-service 8002, matching-service 8003, storage nodes 9000, RabbitMQ 5672/15672.
- Storage nodes are named s0-a, s0-b, s1-a, s1-b, s2-a, s2-b (the name does not imply primary or replica).

## Conventions
- Configuration only through environment variables. No hard-coded URLs, ports, credentials or thresholds.
- JSON field names are camelCase. Timestamps are UTC ISO-8601 generated server-side.
- Error responses always use: `{"error": {"code": "...", "message": "...", "details": [...]}}`. Validation errors are HTTP 400 (never 422). Codes: INVALID_INPUT 400, NOT_FOUND 404, INVALID_TRANSITION 409, VERSION_CONFLICT 409, RESOURCE_ALREADY_ASSIGNED 409, STALE_EPOCH 409, NOT_PRIMARY 409, STORAGE_UNAVAILABLE 503, INTERNAL_ERROR 500.
- Logs are structured JSON with `ts`, `level`, `service`, `event`, `correlationId`. Every HTTP call carries `X-Correlation-Id`.
- Keys are namespaced: `request:{uuid}` and `resource:{uuid}`. Shard = `int.from_bytes(sha256(key).digest()[:8], "big") % 3` via `ems_common.sharding.shard_of`. NEVER use Python's built-in `hash()`.
- Concurrency control uses per-key integer versions and conditional writes (`ifVersion`: absent = upsert, 0 = create-only, n = must equal n).
- Domain services never talk to storage nodes directly; only through the coordinator. The Matching Service has no storage access.
- Request statuses: PENDING, MATCHED, IN_PROGRESS, COMPLETED, NO_MATCH, CANCELLED. Resource statuses: AVAILABLE, ASSIGNED, OFFLINE.

## Quality
- Every feature ships with tests in `tests/` (unit or component), and tests must pass before you finish.
- Keep functions small and typed; run `ruff` before finishing.
- Fault-injection switches (`FAULT_*` environment variables) must default to off.
- Safety over availability in storage: never acknowledge a write that was not committed on the required copy; return 503 rather than serve or promote data that cannot be proven current.
