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
