# API Specification & Contracts

This document records the exact HTTP REST API contracts for the Distributed Emergency Resource Matching System.

---

## 1. Conventions & Error Envelope

All responses are JSON (`Content-Type: application/json`).
Every response carries the `X-Correlation-Id` header (echoed from request or generated).

Errors strictly follow the standard envelope:
```json
{
  "error": {
    "code": "INVALID_INPUT",
    "message": "Request validation failed",
    "details": [
      {
        "field": "value",
        "problem": "Input should be a valid dictionary"
      }
    ]
  }
}
```

Standard Error Codes:
- `400 INVALID_INPUT`: Malformed payload, invalid types, or malformed JSON.
- `404 NOT_FOUND`: Key or resource does not exist or has been deleted.
- `409 VERSION_CONFLICT`: Optimistic concurrency check failed (`ifVersion` mismatch or create-only on existing record).
- `409 STALE_EPOCH`: Node epoch does not match request epoch.
- `409 NOT_PRIMARY`: Write requested on a non-primary node.
- `409 INVALID_TRANSITION`: Disallowed status transition.
- `503 STORAGE_UNAVAILABLE`: Storage node or shard unreachable.
- `500 INTERNAL_ERROR`: Unhandled server exception.

---

## 2. Storage Node API (Port 9000)

Storage nodes persist key-value records with versions and sequence numbers into SQLite WAL files.

### 2.1 Health & Probes

#### `GET /health`
Returns node diagnostic state.
- **Success**: `200 OK`
- **Response**:
```json
{
  "nodeId": "s0-a",
  "shardId": 0,
  "role": "PRIMARY",
  "status": "READY",
  "epoch": 1,
  "lastSeq": 3,
  "records": 2
}
```

#### `GET /health/live` & `GET /health/ready`
Liveness and readiness probes.
- **Success**: `200 OK`
- **Response**:
```json
{
  "status": "ok"
}
```

---

### 2.2 Data Operations

#### `PUT /internal/data/{key:path}`
Create or update a record.

- **Path Parameter**: `key` (e.g., `request:3f2c9a1e-0001`, `resource:water:001`)
- **Headers**:
  - `Content-Type: application/json`
  - `X-Correlation-Id: <uuid>` (optional)
- **Request Body**:
```json
{
  "value": {
    "type": "water",
    "status": "PENDING"
  },
  "ifVersion": 0,
  "epoch": 1
}
```
*Note on `ifVersion`*:
- `None` (omitted): Upsert.
- `0`: Create-only (must not exist as a live record).
- `n > 0`: Update only if current live version equals `n`.

- **Success Responses**:
  - `201 Created` (if newly created)
  - `200 OK` (if updated)
```json
{
  "key": "request:3f2c9a1e-0001",
  "version": 1,
  "seq": 1,
  "replicated": false
}
```
- **Error Responses**:
  - `400 INVALID_INPUT`: `value` is not a JSON object or JSON is malformed.
  - `409 VERSION_CONFLICT`: `ifVersion` conflict.
  - `409 STALE_EPOCH`: Mismatched epoch.
  - `409 NOT_PRIMARY`: Node is not in `PRIMARY` role.

#### `GET /internal/data/{key:path}`
Retrieve an active record.

- **Success**: `200 OK`
```json
{
  "key": "request:3f2c9a1e-0001",
  "value": {
    "type": "water",
    "status": "PENDING"
  },
  "version": 1
}
```
- **Error Responses**:
  - `404 NOT_FOUND`: Key does not exist or has been deleted (tombstone).

#### `DELETE /internal/data/{key:path}`
Soft-deletes a record with a tombstone (`deleted=1`), increments the version, and records `DELETE` in the oplog.

- **Query Parameters**: `epoch` (optional integer)
- **Success**: `200 OK`
```json
{
  "key": "request:3f2c9a1e-0001",
  "version": 2,
  "seq": 2
}
```
- **Error Responses**:
  - `404 NOT_FOUND`: Key does not exist or is already deleted.
  - `409 STALE_EPOCH`: Mismatched epoch.
  - `409 NOT_PRIMARY`: Node is not in `PRIMARY` role.

#### `GET /internal/scan?prefix={prefix}`
Returns all live records matching a key prefix, ordered by key ascending.

- **Query Parameters**: `prefix` (string, defaults to `""`)
- **Success**: `200 OK`
```json
{
  "items": [
    {
      "key": "request:3f2c9a1e-0001",
      "value": {
        "type": "water",
        "status": "PENDING"
      },
      "version": 1
    }
  ],
  "count": 1
}
```

---

## 3. Storage Replication & Synchronization API (Stage 4)

### 3.1 Replication Apply

#### `POST /internal/replicate`
Applies an oplog operation to a replica.

- **Caller**: Primary node during write replication.
- **Request Body**:
```json
{
  "op": "PUT",
  "key": "request:3f2c9a1e-0001",
  "value": { "type": "water", "status": "PENDING" },
  "version": 1,
  "seq": 10,
  "epoch": 1,
  "ts": "2026-11-19T21:00:00Z"
}
```
- **Success Responses**:
  - `200 OK`: `{"status": "applied", "seq": 10}` or `{"status": "duplicate", "seq": 10}` (idempotent replay of previously applied sequence)
- **Error Responses**:
  - `409 NOT_REPLICA`: Called on a node whose role is not `REPLICA`.
  - `409 STALE_EPOCH`: `payload.epoch != node.epoch`.
  - `409 GAP`: `payload.seq > last_applied_seq + 1`. Follower must catch up via sync or snapshot.

---

### 3.2 Follower Sync & Snapshot

#### `GET /internal/sync?since={seq}&epoch={epoch}&limit={limit}`
Retrieves log entries strictly after `since`.

- **Query Parameters**:
  - `since` (int): Sequence number follower has already applied.
  - `epoch` (int): Follower's epoch at `since`.
  - `limit` (int, default 100): Maximum number of entries.
- **Response**:
```json
{
  "snapshotRequired": false,
  "fromSeq": 11,
  "toSeq": 15,
  "entries": [
    {
      "seq": 11,
      "epoch": 1,
      "op": "PUT",
      "key": "request:1",
      "value": { "type": "water" },
      "version": 1,
      "ts": "2026-11-19T21:00:00Z"
    }
  ]
}
```
If `snapshotRequired` is `true`, follower must fetch snapshot via `/internal/snapshot`.

#### `GET /internal/snapshot`
Streams all live and deleted records, along with latest sequence and epoch.

- **Response**:
```json
{
  "lastSeq": 100,
  "epoch": 1,
  "lastLogEpoch": 1,
  "records": [
    {
      "key": "request:1",
      "value": { "type": "water" },
      "version": 1,
      "deleted": 0,
      "updatedAt": "2026-11-19T21:00:00Z"
    }
  ]
}
```

---

## 4. Coordinator Replication Awareness (Stage 4)

#### `POST /internal/nodes/degraded`
Reports that a primary is serving un-replicated writes because its replica is unavailable or lagged.

- **Caller**: Shard primary before acknowledging any un-replicated write.
- **Request Body**:
```json
{
  "nodeId": "s0-a",
  "shardId": 0,
  "epoch": 1,
  "lastSeq": 15
}
```
- **Success**: `200 OK`
```json
{
  "state": "DEGRADED"
}
```
- **Error Responses**:
  - `400 INVALID_INPUT`: Unknown shard ID.
  - `409 NOT_PRIMARY`: Caller is not the current primary for the shard.
  - `409 STALE_EPOCH`: Caller epoch does not match current shard epoch.

#### `GET /diagnostics/shards`
Returns cluster shard states with replication awareness.

- **Response**:
```json
[
  {
    "shardId": 0,
    "state": "HEALTHY",
    "epoch": 1,
    "primary": "s0-a",
    "degraded": false,
    "records": 43,
    "recordCount": { "request": 20, "resource": 15, "other": 8 },
    "nodes": [
      {
        "id": "s0-a",
        "nodeId": "s0-a",
        "role": "PRIMARY",
        "health": "UP",
        "status": "READY",
        "lastSeq": 62
      },
      {
        "id": "s0-b",
        "nodeId": "s0-b",
        "role": "REPLICA",
        "health": "UP",
        "status": "READY",
        "lastSeq": 62
      }
    ]
  }
]
```
Shard States:
- `HEALTHY`: Primary is `UP` + `READY`, replica is `UP` + `READY`, sequences are equal, and `degraded` is `false`.
- `DEGRADED`: Primary is `UP` + `READY`, but replica is missing, lagged, or shard is marked `degraded`.
- `UNAVAILABLE`: Primary is not `UP` and `READY`.

---

## 5. Failover, Fencing & Role Management (Stage 5)

### 5.1 Storage Role Endpoint

#### `POST /internal/role`
Authoritatively reassigns a storage node's role and fencing epoch.

- **Caller**: Coordinator failover manager during primary promotion or old-primary demotion.
- **Request Body**:
```json
{
  "role": "PRIMARY",
  "epoch": 2
}
```
- **Success Response**: `200 OK`
```json
{
  "role": "PRIMARY",
  "epoch": 2,
  "lastSeq": 105
}
```
- **Error Responses**:
  - `400 INVALID_INPUT`: Invalid role name (must be `PRIMARY` or `REPLICA`).
  - `409 STALE_EPOCH`: Requested epoch is strictly lower than node's current epoch.
  - `409 NOT_READY`: Requested promotion to `PRIMARY` while node status is not `READY` (e.g. `CATCHING_UP`).

---

### 5.2 Force Promotion Endpoint

#### `POST /diagnostics/shards/{shardId}/force-promote?nodeId={nodeId}`
Operator intervention escape hatch to force promotion of a named node to `PRIMARY` regardless of whether it is lagged or degraded.

- **Path Parameters**: `shardId` (int): Shard identifier.
- **Query Parameters**: `nodeId` (string): Target node identifier.
- **Behavior**:
  - Validates target node exists on shard and is `UP`.
  - Increments shard epoch (`epoch + 1`) and persists atomically to coordinator disk state file.
  - Logs `WARNING: Shard <id> has stale replica (<id>) with seq <M> < primary seq <N>; promotion may lose data. Proceed?` if the target node has lower sequence than primary.
  - Authoritatively calls `POST /internal/role` on target node.
- **Success Response**: `200 OK`
```json
{
  "status": "ok",
  "shardId": 1,
  "primaryNodeId": "s1-b",
  "epoch": 2,
  "degraded": true
}
```
- **Error Responses**:
  - `400 INVALID_INPUT`: Invalid shard ID or node does not belong to shard.
  - `503 STORAGE_UNAVAILABLE`: Target node is `DOWN` or role call failed (`Retry-After: 3`).

---

### 5.3 Failure Detection & Read Fallback

- **Node Health States**: Each node has an explicit stored health state `UP` or `DOWN`.
  - Background probe thread sends `GET /health/live` to each registered node every `HEALTH_CHECK_INTERVAL_SEC` (default 1s) with timeout `PROBE_TIMEOUT_SEC` (default 1s).
  - A node is marked `DOWN` if its heartbeat is missing past `HEARTBEAT_TIMEOUT_SEC` (default 6s) or if its HTTP probe fails.
  - Marking a primary `DOWN` triggers immediate automated failover: the healthy in-sync replica is promoted with `epoch + 1`.
- **Read Fallback on Outage**:
  - For read operations (`GET /internal/data/{key}` and `GET /internal/scan`), if the primary is `DOWN` or returns a connection/timeout error, the coordinator transparently attempts a single retry against an in-sync replica with equal `lastSeq`.
  - Mutation operations (`PUT`, `DELETE`) are strictly never retried to preserve linearizability and prevent split-brain writes.

---

## 6. Request Service API (Port 8001, Stage 6)

The Request Service manages emergency aid requests, orchestrates match events, enforces client status transitions, and automatically triggers resource releases when requests reach terminal states (`COMPLETED` or `CANCELLED`).

### 6.1 Public Endpoints

#### `POST /requests`
Create a new emergency request and queue asynchronous matching.
- **Request Body**:
```json
{
  "type": "water",
  "location": { "lat": 12.9716, "lon": 77.5946 },
  "details": "Drinking water needed"
}
```
- **Success Response**: `201 Created`
  - `Location: /requests/{requestId}`
```json
{
  "requestId": "3f2c9a1e-8b1a-4f56-91e2-b1c2d3e4f5a6",
  "type": "water",
  "location": { "lat": 12.9716, "lon": 77.5946 },
  "details": "Drinking water needed",
  "status": "PENDING",
  "matchedResourceId": null,
  "matchAttemptCount": 0,
  "createdAt": "2026-11-15T10:00:00Z",
  "updatedAt": "2026-11-15T10:00:00Z",
  "version": 1,
  "matchQueued": false
}
```

#### `GET /requests/{id}`
Retrieve a request by UUID.
- **Success Response**: `200 OK` (returns public record with `version`).
- **Errors**: `400 INVALID_INPUT` (malformed UUID), `404 NOT_FOUND` (unknown ID).

#### `GET /requests`
List requests, supporting partial results if storage shards are down.
- **Query Parameters**:
  - `type` (optional): `water`, `food`, `medicine`, `transportation`, `shelter`.
  - `status` (optional): `PENDING`, `MATCHED`, `IN_PROGRESS`, `COMPLETED`, `NO_MATCH`, `CANCELLED`.
- **Success Response**: `200 OK`
```json
{
  "items": [ /* list of requests sorted by (createdAt, requestId) */ ],
  "count": 1,
  "partial": false
}
```

#### `PUT /requests/{id}/status`
Update request status according to valid client transition rules.
- **Request Body**: `{"status": "IN_PROGRESS"}` or `{"status": "COMPLETED"}` or `{"status": "CANCELLED"}`.
- **Success Response**: `200 OK` (returns updated request).
- **Side Effects**: If transitioning to `COMPLETED` or `CANCELLED` and `matchedResourceId` was assigned, calls `POST /internal/resources/{matchedResourceId}/release`.
- **Errors**: `409 INVALID_TRANSITION` (e.g., trying to set `PENDING`, `MATCHED`, or `NO_MATCH`).

#### `POST /match/{requestId}`
Manual re-trigger for matching.
- **Success Response**: `200 OK` `{"requestId": "...", "status": "PENDING", "matchQueued": false}`
- **Errors**: `409 INVALID_TRANSITION` (if request is `MATCHED`, `IN_PROGRESS`, `COMPLETED`, or `CANCELLED`).

### 6.2 Internal Endpoints

#### `GET /internal/requests/{id}`
Used by Matching Service and Reconciler; returns full stored record including `eventPending` and `version`.

#### `POST /internal/requests/{id}/matchresult` (or `/match-result`)
Records outcome from Matching Service.
- **Request Body**: `{"outcome": "MATCHED", "resourceId": "...", "version": 1}` or `{"outcome": "NO_MATCH"}`.
- **Success Response**: `200 OK` (idempotent if already matched with same resource).
- **Errors**: `409 VERSION_CONFLICT`, `409 INVALID_TRANSITION` (if request no longer `PENDING`).

---

## 7. Resource Service API (Port 8002, Stage 6)

The Resource Service manages emergency aid resources and volunteers, handles candidate searches, provides atomic reservations via optimistic conditional writes, and runs a background reconciler for orphaned assignments.

### 7.1 Public Endpoints

#### `POST /resources`
Register a resource.
- **Request Body**:
```json
{
  "name": "Red Cross water truck 1",
  "type": "water",
  "location": { "lat": 12.9352, "lon": 77.6245 },
  "availability": true
}
```
- **Success Response**: `201 Created`
  - `Location: /resources/{resourceId}`
```json
{
  "resourceId": "19416df0-26aa-464c-8e07-94d798a49d3f",
  "name": "Red Cross water truck 1",
  "type": "water",
  "location": { "lat": 12.9352, "lon": 77.6245 },
  "availability": true,
  "status": "AVAILABLE",
  "assignedRequestId": null,
  "createdAt": "2026-11-15T10:00:00Z",
  "updatedAt": "2026-11-15T10:00:00Z",
  "version": 1
}
```
*Invariant*: `availability == (status == "AVAILABLE")`.

#### `GET /resources/{id}`
Retrieve a single resource by UUID.

#### `GET /resources`
List resources across all shards (requires full scan).
- **Query Parameters**:
  - `type` (optional): `water`, `food`, `medicine`, `transportation`, `shelter`.
  - `status` (optional): `AVAILABLE`, `ASSIGNED`, `OFFLINE`.
  - `availability` (optional): `true`, `false`.
- **Success Response**: `200 OK` `{"items": [...], "count": 1}`

#### `PUT /resources/{id}/status`
Update resource availability or status.
- **Request Body**: `{"status": "AVAILABLE"}` or `{"status": "OFFLINE"}`.
- **Success Response**: `200 OK` (updates status and matching boolean `availability`).
- **Errors**: `409 INVALID_TRANSITION` (clients cannot set status to `ASSIGNED`).

### 7.2 Internal Endpoints

#### `GET /internal/resources/candidates?type={type}`
Candidate lookup for matching service (`status == AVAILABLE`, `availability == true`). Supports partial results (`"partial": true/false`).

#### `GET /internal/resources/by-request/{requestId}`
Find resource assigned to a specific request ID (makes matching retries idempotent).
- **Errors**: `404 NOT_FOUND` if no resource assigned to request.

#### `POST /internal/resources/{id}/reserve`
Atomic reservation of an available resource.
- **Request Body**: `{"requestId": "...", "expectedVersion": 1}`.
- **Success Response**: `200 OK` (sets `status: "ASSIGNED"`, `availability: false`, `assignedRequestId: requestId`). Idempotent if already assigned to the same request.
- **Errors**: `409 RESOURCE_ALREADY_ASSIGNED` (already assigned to another request), `409 VERSION_CONFLICT` (version mismatch).

#### `POST /internal/resources/{id}/release`
Releases an assigned resource back to `AVAILABLE`.
- **Request Body**: `{"requestId": "..."}`.
- **Success Response**: `200 OK` `{"released": true/false, "resource": {...}}`. Idempotent.

### 7.3 Background Reconciler
Runs every `RECONCILE_INTERVAL_SEC` (default 60s). Scans `ASSIGNED` resources. For each resource past grace period, checks `GET /internal/requests/{assignedRequestId}` on Request Service. If request is `404`, or request status is not `MATCHED`/`IN_PROGRESS`, releases resource back to `AVAILABLE`.

---

## 8. RabbitMQ & Matching Service (Port 8003, Stage 7)

### 8.1 Messaging Topology

RabbitMQ broker handles asynchronous matching decoupling Request and Matching services.

#### Exchanges (Direct, Durable)
- `emergency.events`: Primary events exchange.
- `emergency.retry`: Holding exchange for retries with message TTL.
- `emergency.dlx`: Dead-letter exchange for messages exceeding max retries or unrecoverable malformed payloads.

#### Queues & Dead-Letter Configuration
- `request.match`:
  - Durable: `true`
  - Dead Letter Exchange: `emergency.retry`
  - Routing key: `request.match` (bound to `emergency.events`)
- `request.match.retry`:
  - Durable: `true`
  - Message TTL: `5000` ms (`x-message-ttl`)
  - Dead Letter Exchange: `emergency.events`
  - Dead Letter Routing Key: `request.match`
  - Routing key: `request.match` (bound to `emergency.retry`)
- `request.match.dlq`:
  - Durable: `true`
  - Routing key: `request.match` (bound to `emergency.dlx`)

#### Event Payload Schema
```json
{
  "eventId": "f7d3d1f0-...",
  "eventType": "request.created",
  "timestamp": "2026-11-20T00:00:00Z",
  "requestId": "6ed56b56-...",
  "type": "water",
  "location": { "lat": 12.9716, "lon": 77.5946 }
}
```

### 8.2 Outbox & Sweeper
Request Service writes records to coordinator storage with `eventPending=True`. Upon successful publish to `emergency.events`, `eventPending` is cleared to `False`. If the broker is unreachable, the request creation succeeds (`matchQueued=False`) and the background `OutboxSweeper` polls storage, republishes stale pending events, and clears `eventPending`.

### 8.3 Matching Service
- **Consumes**: `request.match` with manual ack and `prefetch_count=1`.
- **Workflow**:
  1. Fetch request from Request Service via `GET /internal/requests/{id}`. If not found, drop/ack. If not `PENDING`, drop/ack.
  2. Idempotency check via Resource Service `GET /internal/resources/by-request/{id}`. If already assigned, record `MATCHED` and ack.
  3. Query candidates via Resource Service `GET /internal/resources/candidates?type={type}`.
  4. Compute haversine distances, filter within `MAX_MATCH_DISTANCE_KM` (if set), sort by distance ascending, tie-break by `resourceId`.
  5. Attempt atomic reservation `POST /internal/resources/{id}/reserve`.
  6. On success: record `POST /internal/requests/{id}/matchresult` (`outcome="MATCHED"`).
  7. If all candidates fail or none eligible: record `POST /internal/requests/{id}/matchresult` (`outcome="NO_MATCH"`).
  8. Ack message upon successful outcome recording.
  9. On transient failures, message is published to `emergency.retry` with header `x-retry-count`. When retries exceed `MAX_RETRIES` (3), message is routed to `request.match.dlq`.

#### Probes & Health (Port 8003)
- `GET /health`: JSON `{"status": "ok", "service": "matching-service", "consumer_ready": true}`.
- `GET /health/live`: `200 OK`.
- `GET /health/ready`: `200 OK` if consumer connection to broker is established; `503` otherwise.


---

## 9. Stack Orchestration and Everyday Commands (Stage 8)

The system provides standardized `make` targets and utility scripts to manage the full distributed stack:

### 9.1 Makefile Commands
- `make up`: Starts all 11 Compose containers in the proper dependency order (`docker compose up -d --wait`).
- `make down`: Gracefully stops all stack containers (`docker compose down`).
- `make reset`: Runs `scripts/reset.sh compose` to tear down volumes, recreate the stack, purge RabbitMQ queues, and seed deterministic resources.
- `make itest`: Runs the end-to-end integration test suite against the live stack.
- `make ftest`: Runs the cluster fault, failover, and partition drill suites.
- `make all-tests`: Sequentially executes `make lint`, `make test`, `make itest`, and `make ftest`.
- `make images`: Builds all five application container images (`ems/storage-node:1.0`, `ems/coordinator:1.0`, `ems/request-service:1.0`, `ems/resource-service:1.0`, `ems/matching-service:1.0`).

### 9.2 Utility Scripts
- `scripts/seed.py`: Seeds resources and requests. `--fixed` creates the deterministic demo dataset (3 water resources at ~0.6km, 2km, 6km and 1 food resource); `--requests N --resources N` seeds random test distributions.
- `scripts/reset.sh [compose]`: Idempotent stack teardown, volume reset, queue purge, and deterministic seed.
- `scripts/app_load.py`: Submits sustained request load with 503 backoff retries and verifies all requests settle.
- `scripts/test_fresh_clone.sh`: Verifies that a clean repository clone without a `.env` file starts and matches resources out of the box.
- `scripts/test_storage_persistence.sh`: Proves that storage node data survives container restarts on a Docker named volume.
