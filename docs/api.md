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
