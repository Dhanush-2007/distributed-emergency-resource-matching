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
