# Discovered API Contracts

Live-verified against Coordinator (:8000), Request Service (:8001), and Resource Service (:8002).

## Error Envelope
All error responses (400, 404, 409, 503) adhere to:
```json
{
  "error": {
    "code": "INVALID_INPUT",
    "message": "Request validation failed",
    "details": [
      { "field": "type", "problem": "Input should be 'water', 'food', 'medicine', 'transportation' or 'shelter'" }
    ]
  }
}
```
404 Example:
```json
{
  "error": {
    "code": "NOT_FOUND",
    "message": "Key 'request:00000000-0000-0000-0000-000000000000' not found",
    "details": []
  }
}
```
409 Example:
```json
{
  "error": {
    "code": "INVALID_TRANSITION",
    "message": "Cannot transition request from 'CANCELLED' to 'IN_PROGRESS'",
    "details": [{ "current": "CANCELLED", "target": "IN_PROGRESS" }]
  }
}
```

## Request Service (:8001)

### `POST /requests`
- Request: `{"type": "water", "location": {"lat": 12.9716, "lon": 77.5946}, "details": "string"}`
- Response (201):
```json
{
  "requestId": "ad4e4091-176a-442a-96d0-1b3824d14f28",
  "type": "water",
  "location": { "lat": 12.9716, "lon": 77.5946 },
  "details": "Emergency drinking water needed at hospital",
  "status": "PENDING",
  "matchedResourceId": null,
  "matchAttemptCount": 0,
  "createdAt": "2026-11-23T09:45:59Z",
  "updatedAt": "2026-11-23T09:45:59Z",
  "version": 1,
  "matchQueued": false
}
```

### `GET /requests/{id}`
- Response (200): same shape as creation without `matchQueued`.

### `GET /requests`
- Response (200):
```json
{
  "items": [
    {
      "requestId": "40e6e0e7-2fdc-4219-a2fa-065f8546ca45",
      "type": "water",
      "location": { "lat": 12.9716, "lon": 77.5946 },
      "details": "Flood, 5 people",
      "status": "MATCHED",
      "matchedResourceId": "fa737d82-10f6-4367-975b-c4e112b1663b",
      "matchAttemptCount": 1,
      "createdAt": "2026-11-23T09:32:12Z",
      "updatedAt": "2026-11-23T09:32:13Z",
      "version": 3
    }
  ],
  "count": 35,
  "partial": false
}
```

### `PUT /requests/{id}/status`
- Request: `{"status": "IN_PROGRESS"}` / `{"status": "COMPLETED"}` / `{"status": "CANCELLED"}`
- Response (200): updated request object with incremented `version` and new `status`.

### `POST /match/{id}`
- Response (200):
```json
{
  "requestId": "fc4e564e-7827-4e8d-ae9b-81bb4825fbcb",
  "status": "PENDING",
  "matchQueued": false
}
```

## Resource Service (:8002)

### `POST /resources`
- Request: `{"name": "Truck 1", "type": "water", "location": {"lat": 12.975, "lon": 77.595}, "availability": true}`
- Response (201):
```json
{
  "resourceId": "2973f0b8-9c38-4030-950a-9b46d5408834",
  "name": "Test Water Truck 99",
  "type": "water",
  "location": { "lat": 12.975, "lon": 77.595 },
  "availability": true,
  "status": "AVAILABLE",
  "assignedRequestId": null,
  "createdAt": "2026-11-23T09:45:59Z",
  "updatedAt": "2026-11-23T09:45:59Z",
  "version": 1
}
```

### `GET /resources`
- Response (200): `{"items": [...], "count": 39}`

### `GET /resources/{id}`
- Response (200): single resource object.

### `PUT /resources/{id}/status`
- Request: `{"status": "AVAILABLE"}` or `{"status": "OFFLINE"}`
- Response (200): single resource object with updated `status` and `availability`.

## Coordinator (:8000)

### `GET /diagnostics/shards`
- Response (200): array of shards
```json
[
  {
    "shardId": 0,
    "state": "HEALTHY",
    "epoch": 1,
    "primary": "s0-a",
    "degraded": false,
    "records": 38,
    "recordCount": { "request": 22, "resource": 16, "other": 0 },
    "nodes": [
      { "id": "s0-b", "nodeId": "s0-b", "role": "REPLICA", "health": "UP", "status": "READY", "lastSeq": 98 },
      { "id": "s0-a", "nodeId": "s0-a", "role": "PRIMARY", "health": "UP", "status": "READY", "lastSeq": 98 }
    ]
  }
]
```

### `GET /diagnostics/nodes`
- Response (200): array of nodes
```json
[
  {
    "nodeId": "s1-a",
    "shardId": 1,
    "endpoint": "http://ems-s1-a-0.ems-s1-a.ems.svc.cluster.local:9000",
    "role": "REPLICA",
    "epoch": 2,
    "health": "UP",
    "status": "READY",
    "lastHeartbeatAgeSec": 1.564,
    "lastSeq": 83,
    "recordCount": { "request": 21, "resource": 10, "other": 0 }
  }
]
```
