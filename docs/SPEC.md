# Distributed Emergency Resource Matching System: Specification

> **Purpose of this file.** This is the single source of truth for the project, written for use inside Cursor (`@docs/SPEC.md`). It contains the requirements, architecture, contracts, algorithms, implementation stages and acceptance tests. When a prompt and this file disagree, **stop and ask** instead of guessing.

## How to use this file

- Read the sections relevant to the current task before writing code (section numbers are stable; prompts refer to them).
- **Implement only the current stage.** Later stages depend on the exact contracts defined here (API shapes, status values, key names, error codes, message formats).
- Every feature needs tests. A stage is finished only when all of its exit criteria (section 9) pass.
- Contracts (sections 5, 6, 7, 8) must not be changed without explicit approval.

## Stage index

| Stage | Builds | Main output |
|---|---|---|
| 0 | Repo scaffold and shared library | Layout, rules, `ems_common`, test setup |
| 1 | Storage node (standalone) | SQLite key/value service plus Dockerfile |
| 2 | Coordinator and three primaries | Hash routing, registry, diagnostics |
| 3 | Kubernetes walking skeleton | Coordinator and 3 nodes on Minikube |
| 4 | Replication | Op log, replicate, sync, snapshot |
| 5 | Failure detection, failover, recovery | Promotion, epochs, rejoin |
| 6 | Resource Service and Request Service | All seven public endpoints |
| 7 | RabbitMQ and Matching Service | Async matching, retries, DLQ |
| 8 | Docker Compose integration | One-command stack, failure drills |
| 9 | Kubernetes full deployment | Manifests, scaling, self-healing |
| 10 | Verification and documentation | Tests, runbook, report |

Detailed tasks and exit criteria for each stage are in section 9.

---

## 1. Project overview

### 1.1 Purpose

The project builds a **distributed emergency resource matching system**. People affected by an emergency (for example a flood) submit requests for help such as water, food, medicine, transportation or shelter. Volunteers, NGOs and other sources of assistance register the resources they can offer. The system stores both kinds of records across several storage nodes, processes new requests asynchronously, and matches each request to a suitable available resource using **resource type** and **location**.

The project is graded as much on infrastructure behaviour as on application behaviour. It must therefore demonstrate, with evidence: **distributed storage** (a coordinator and three shards), **replication** (at least one replica per shard), **asynchronous communication** (RabbitMQ), **fault tolerance** (heartbeats, failover, recovery), **containerization** (Docker) and **orchestration** (Kubernetes).

### 1.2 Example scenario

A user affected by flooding needs drinking water. The client submits a request. The Request Service stores it (state `PENDING`) and publishes a match event to RabbitMQ. A Matching Service worker consumes the event, finds the nearest available water resource, reserves it atomically and records the match. The request becomes `MATCHED` and the resource becomes `ASSIGNED`.

**Scenario flow**

```
User -> Request Service -> Coordinator -> Storage shard (primary + replica)
                       |
                       +-> RabbitMQ -> Matching Service -> Resource Service (reserve)
                                              |
                                              +-> Request Service (record MATCHED)
```

### 1.3 Guiding principles

- **Smallest complete version first.** Every mandatory behaviour must work and be observable before any optional feature is added.
- **Contracts before code.** API shapes, statuses, key names and message formats are frozen first and kept in one shared library.
- **Safety over availability in storage.** A write is never acknowledged unless it is durably committed on the required active copy, and the system returns `503` instead of serving or promoting data it cannot prove is current.
- **Idempotent by design.** RabbitMQ delivers at least once, so every consumer step and every state transition must be safe to repeat.
- **Observable by design.** Logs and diagnostic endpoints are first-class features, because they are the evidence shown during the demonstration.
- **Test continuously, including failures.** Replication, concurrency, messaging and Kubernetes tests are written alongside the code, not at the end.

### 1.4 System at a glance

| Parameter | This design |
|---|---|
| **Coordinator** | 1 (single instance, state recoverable from storage-node registrations plus a small persisted shard-state file) |
| **Microservices** | 3 domain services: **Request Service**, **Resource Service**, **Matching Service** |
| **Message broker** | 1 RabbitMQ instance (durable direct exchange, durable queues, dead-letter handling) |
| **Storage shards** | 3 logical shards, selected by `SHA-256(key)` modulo 3 |
| **Replication** | 1 replica per shard, so 6 storage nodes in total (`s0-a`, `s0-b`, `s1-a`, `s1-b`, `s2-a`, `s2-b`) |
| **Containerization** | Docker (mandatory), one Dockerfile per component, Docker Compose for local integration |
| **Orchestration** | Kubernetes (mandatory) on a local cluster; cloud deployment is not required |
| **Clients** | Postman collection, curl examples and a small Python client / load script |

### 1.5 Scope

#### In scope

- All five operations (`createRequest`, `registerResource`, `matchRequest`, `updateStatus`, `getRequest`) exposed through the seven required REST endpoints.
- Coordinator-routed distributed storage with sharding, replication, heartbeat-based failure detection, failover and recovery synchronization.
- Asynchronous matching through RabbitMQ with publisher confirms, manual acknowledgements, retries and a dead-letter queue.
- Docker images, Docker Compose integration, Kubernetes deployment, scaling and self-healing demonstrations.
- Automated tests, a scripted and repeatable demonstration, and a short design-justification report.

#### Out of scope (document as limitations or optional extras)

- Frontend or mobile application, authentication and authorization, public-cloud hosting.
- Consensus protocols (Raft/Paxos), multiple coordinators, dynamic shard rebalancing, network-partition tolerance.
- Geo-indexing, route planning, multi-unit quantities, notifications and advanced matching strategies.
- Optional extras if everything mandatory is finished: single-entry Ingress, automatic retry of `NO_MATCH` requests, Horizontal Pod Autoscaler, Prometheus metrics.

## 2. Requirements to be fulfilled

Every requirement in the project brief is listed below with an identifier, so that the testing and demonstration sections can reference it. The last column points to the section of this plan that satisfies it.

| ID | Requirement | Brief | Plan |
|---|---|---|---|
| **FR-1** | `createRequest(type, location, details)` creates an emergency request and returns `201`. | §1, §5 | 5, 6, 8 |
| **FR-2** | `registerResource(type, location, availability)` registers a resource or volunteer and returns `201`. | §1, §5 | 5, 6 |
| **FR-3** | `matchRequest(requestId)` finds and assigns a suitable available resource, using resource type and location. | §1, §7 | 8 |
| **FR-4** | `updateStatus(requestId, status)` updates a request status; resource status can be updated too. | §1, §5 | 5, 6 |
| **FR-5** | `getRequest(requestId)` retrieves request information (`200`, `404` when unknown). | §1, §5 | 6 |
| **FR-6** | All seven REST endpoints exist with the specified methods, paths and success codes; JSON bodies; correct codes for invalid input, missing items and unavailable services. | §5 | 6 |
| **FR-7** | Request record contains: unique ID, type, location, details, current status, creation time. Resource record contains: unique ID, type, location, availability, current status. | §3 | 5 |
| **FR-8** | Supported request types include water, food, medicine, transportation and shelter. | §3.1 | 5 |
| **SR-1** | One coordinator determines the shard and routes storage operations. | §2, §4 | 7 |
| **SR-2** | Three storage shards with at least one replica each; data spread across all shards. | §2, §4 | 7 |
| **SR-3** | Hash-based partitioning, for example `hash(request_id) mod 3`. | §4 | 7.1 |
| **SR-4** | The client never needs to know the physical storage location. | §4 | 3, 7 |
| **SR-5** | Internal APIs: node register, heartbeat, store/update, retrieve, delete data. | §6 | 6.5, 7 |
| **MR-1** | RabbitMQ carries asynchronous communication between request processing and matching. | §2, §6 | 8 |
| **MR-2** | Processing sequence: submit, store, publish, consume, identify resource, assign and update statuses. | §7 | 3.4, 8 |
| **MR-3** | Matching considers at least resource type and location. | §7 | 8.6 |
| **FT-1** | Storage nodes report availability to the coordinator with periodic heartbeats. | §8 | 7.5 |
| **FT-2** | Detect unavailable nodes; avoid routing new requests to them. | §8 | 7.5, 7.2 |
| **FT-3** | Use an available replica when data remains accessible; continue processing when enough replicas exist. | §8 | 7.6 |
| **FT-4** | Synchronize data when a failed node recovers. | §8 | 7.7 |
| **FT-5** | Failure demonstrated by stopping a container or pod and restarting it, showing before, during and after. | §8 | 14 |
| **DK-1** | Dockerfiles for required services, built images, components run as containers, container-to-container communication configured. | §9.1 | 10 |
| **K8-1** | Kubernetes Deployments, Services, pod replicas and service-to-service communication. | §9.2 | 11 |
| **K8-2** | Microservice scaling, and pod failure with automatic recovery. | §9.2 | 11.6 |
| **K8-3** | Local Kubernetes (Minikube, kind or Docker Desktop); cloud optional. | §9.2 | 11 |
| **DM-1** | Demonstrate: request creation/retrieval; resource registration; matching; distribution over 3 shards; asynchronous processing of multiple requests; storage failure; node recovery; Kubernetes pod recovery; Kubernetes scaling. | §10 | 14 |
| **TS-1** | Test normal and failure conditions: CRUD, matching, simultaneous requests, RabbitMQ delivery, shard selection, replica availability, recovery sync, pod recovery, scaling. | §11 | 13 |
| **JU-1** | Justify the design choices left to the team: language, storage, sharding, replica sync, matching, RabbitMQ layout, service boundaries, Kubernetes config, monitoring, optional cloud. | §12 | 4, 15 |

### 2.1 Operation-to-endpoint mapping

| Operation in the brief | REST endpoint | Owning service |
|---|---|---|
| `createRequest(type, location, details)` | `POST /requests` | Request Service |
| `getRequest(requestId)` | `GET /requests/{id}` | Request Service |
| `updateStatus(requestId, status)` | `PUT /requests/{id}/status` | Request Service |
| `registerResource(type, location, availability)` | `POST /resources` | Resource Service |
| (list / update resources) | `GET /resources`, `PUT /resources/{id}/status` | Resource Service |
| `matchRequest(requestId)` | `POST /match/{requestId}` | Request Service (queues the event); executed by Matching Service |

### 2.2 Assumptions and failure model

- Locations are stored as latitude/longitude pairs (decimal degrees). Distance is great-circle (Haversine) distance.
- A resource serves one request at a time. When a request is completed or cancelled, its resource is released back to `AVAILABLE` (a reusable volunteer or asset).
- Failure model: **crash-stop of one storage node per shard at a time**, plus crashes of application pods and of the RabbitMQ consumer. Network partitions and simultaneous loss of both nodes of a shard are handled safely (by returning `503`) but are not required to keep working.
- The single coordinator is assumed available during demonstrations. Its limitations are documented (section 16).
- Kubernetes runs on a single local node; "pods spread across nodes" is therefore not demonstrated.

## 3. System architecture

### 3.1 Architecture diagram

```
                    Clients (Postman / curl / Python load script)
                       |                                  |
                       v                                  v
  +-----------------+ publish +----------+ consume +------------------+ reserve +------------------+
  | Request Service |-------->| RabbitMQ |-------->| Matching Service |-------->| Resource Service |
  |      :8001      |         |  :5672   |         |      :8003       |         |      :8002       |
  +--------+--------+         +----------+         +------------------+         +--------+---------+
           |   (Matching also calls Request Service to record the outcome)                |
           +---------------------------------+--------------------------------------------+
                                             v
                               +----------------------------+
                               |     Coordinator :8000      |
                               |  registry, routing, scan,  |
                               |  heartbeats, failover      |
                               +-------------+--------------+
                  +----------------------------+----------------------------+
                  v                            v                            v
        +------------------+         +------------------+         +------------------+
        |     Shard 0      |         |     Shard 1      |         |     Shard 2      |
        | s0-a  <->  s0-b  |         | s1-a  <->  s1-b  |         | s2-a  <->  s2-b  |
        | primary  replica |         | primary  replica |         | primary  replica |
        +------------------+         +------------------+         +------------------+
```

Storage traffic always goes through the coordinator. The match event goes through RabbitMQ. The Matching Service calls the Request and Resource services over internal HTTP. Which node is primary and which is replica can change after a failover; the node names do not imply a role.

### 3.2 Components and responsibilities

| Component | Responsibilities | Data owned | Calls |
|---|---|---|---|
| **Request Service** (:8001) | Public request APIs (`POST/GET /requests`, `PUT /requests/{id}/status`, `POST /match/{id}`). Validation, status rules, publishes match events, republishes unconfirmed events (outbox sweeper), accepts match outcomes from the Matching Service, releases resources on completion or cancellation. | `request:*` records | Coordinator, RabbitMQ, Resource Service (release) |
| **Resource Service** (:8002) | Public resource APIs. Candidate search by type, atomic reserve and release with conditional writes, reconciler for stuck reservations. | `resource:*` records | Coordinator |
| **Matching Service** (:8003) | RabbitMQ consumer. Implements `match_request()`: load request, find candidates, choose nearest, reserve, record result, ack. Exposes health only. Scales horizontally. | none (stateless) | RabbitMQ, Request Service, Resource Service |
| **Coordinator** (:8000) | Registers and monitors storage nodes (heartbeats), maps keys to shards, routes reads and writes, scatter-gather scans, detects failures, promotes replicas, fences with epochs, exposes diagnostics. | node registry; persisted shard epochs and primaries | Storage nodes |
| **Storage node** (:9000, ×6) | Persist key/value records with versions in SQLite; maintain an ordered operation log; replicate (primary to replica); serve sync and snapshot requests; report heartbeats. | its shard data | Coordinator (register/heartbeat), peer node |
| **RabbitMQ** (:5672, :15672) | Buffers match events, distributes them to workers, retries and dead-letters failures. | durable queues | - |

### 3.3 Communication rules

- Only the **Request Service and Resource Service** talk to the coordinator for data. **No domain service ever addresses a physical storage node.** The client never learns where data lives.
- The **Matching Service has no storage access**. It works through the internal HTTP APIs of the Request and Resource services, which keeps record ownership clear and exercises genuine service-to-service communication in Kubernetes.
- Synchronous communication is REST/JSON over HTTP. The only asynchronous path is the match event through RabbitMQ.
- Every cross-service call carries an `X-Correlation-Id` header (also stored in each RabbitMQ message) so a single request can be traced across all logs.
- Addresses are always service DNS names (`coordinator`, `resource-service`, ...) supplied through environment variables; pod IPs are never used.

### 3.4 End-to-end flows

#### Flow A: create a request and match it automatically

1. Client sends `POST /requests` to the Request Service. The service validates the body and generates a UUID.
2. It writes `request:{uuid}` (status `PENDING`, `eventPending=true`) through the coordinator. The coordinator hashes the key, picks the shard, forwards to the shard primary, which commits, replicates to the replica and acknowledges.
3. The Request Service publishes `{eventId, requestId}` to RabbitMQ with publisher confirms. On confirm it clears `eventPending`. It responds `201` with the stored record. (If the broker is unavailable the request is still saved and the sweeper publishes later.)
4. A Matching Service worker consumes the event. It loads the request; if it is no longer `PENDING` it acknowledges and stops (duplicate).
5. The worker asks the Resource Service for eligible candidates of the same type, sorts them by distance, and tries to reserve the nearest one with a conditional write. A conflict (`409`) means another worker took it, so the next candidate is tried.
6. The worker tells the Request Service the outcome (`MATCHED` with the resource ID, or `NO_MATCH`). Only then does it acknowledge the RabbitMQ message.
7. The client calls `GET /requests/{id}` and sees status `MATCHED` and `matchedResourceId`.

#### Flow B: manual or retry matching

1. Client sends `POST /match/{requestId}`. Unknown ID gives `404`; a request already `MATCHED`, `IN_PROGRESS`, `COMPLETED` or `CANCELLED` gives `409`.
2. `NO_MATCH` requests are moved back to `PENDING`. The Request Service publishes the same match event as in Flow A and returns `200` with `matchQueued`. The event is consumed by the same `match_request()` code path.

#### Flow C: read path

1. Client calls `GET /requests/{id}`. The Request Service asks the coordinator for `request:{id}`.
2. The coordinator computes the shard and reads from the healthy primary. If the primary is unreachable and the replica is known to be in sync, the coordinator reads from the replica. If neither can be proven safe it answers `503`.

#### Flow D: storage node failure

1. A storage node stops sending heartbeats. After three missed intervals the coordinator marks it `DOWN` and stops routing to it.
2. If it was a primary and its replica is proven in sync, the coordinator increments the shard epoch, promotes the replica and routes new writes to it. Reads and writes continue.
3. When the failed node restarts it re-registers, is told it is now a follower, replays missing log entries (or loads a snapshot), and is marked ready only after catching up.

## 4. Technology and design decisions

The brief leaves these choices to the team and asks that they can be justified. Each decision below includes its rationale so it can be reused directly in the design report.

| Area | Decision | Justification |
|---|---|---|
| **Language / framework** | Python 3.11, FastAPI + Uvicorn, Pydantic v2, httpx, pika | One language for all components, low memory per container (important when running about a dozen containers on a laptop), fast to write, built-in validation and OpenAPI docs. |
| **Shard storage engine** | SQLite (WAL mode) file per storage node on a persistent volume; tables `kv`, `oplog`, `meta` | Embedded, durable, transactional, and trivial to containerize. Sharding and replication are the graded features, not the database engine. |
| **Partitioning** | Fixed 3 shards; `shardId = int(first 8 bytes of SHA-256(key)) mod 3` implemented once in a shared library | Deterministic across processes and languages. Python's built-in `hash()` is randomized per process and must never be used. The mod-N rebalancing limitation is stated in the report. |
| **Replication** | Leader-based, one replica per shard, ordered operation log with sequence numbers, semi-synchronous | A single ordered log removes dual-write divergence. Acknowledging after both copies commit gives zero data loss for acknowledged writes under a single failure. |
| **Failure detection** | Heartbeat every 2 s; `DOWN` after 3 missed intervals (about 6 s); thresholds configurable | Fast enough for a live demo yet tolerant of one delayed heartbeat. |
| **Failover safety** | Shard epoch (fencing token) persisted by coordinator and nodes; promotion only when the replica is provably in sync; log-matching check on rejoin | Prevents a stale or returning old primary from corrupting data and prevents promotion of stale data. Serves as an educational split-brain safeguard. |
| **Concurrency control** | Per-key integer versions; conditional write `ifVersion` | Atomic reservation without distributed locks. Same mechanism serves reservation, state transitions and create-if-absent. |
| **Resource listing** | Coordinator scatter-gather scan by key prefix, merged and filtered by the Resource Service | Hash sharding cannot answer range or filter queries from a single shard. |
| **Messaging** | Durable direct exchange, durable queues, persistent messages, publisher confirms, manual acks, prefetch 1, retry queue with TTL, dead-letter queue | Covers delivery guarantees end to end. At-least-once delivery is made safe by idempotent processing. |
| **Matching strategy** | Same type, `availability=true`, status `AVAILABLE`; nearest by Haversine; ties broken by `resourceId` | Uses type and location as required; deterministic and therefore testable and repeatable in the demo. |
| **Service boundaries** | Request Service owns requests; Resource Service owns resources and reservation; Matching Service owns orchestration only | Clear data ownership; the Matching Service scales independently and is stateless. |
| **Containers** | One image per component, non-root user, `python:3.11-slim`, health checks; Docker Compose for integration | Reproducible builds; Compose gives a fast inner loop before Kubernetes. |
| **Kubernetes** | Namespace `ems`; Deployments + ClusterIP Services for apps; six single-replica StatefulSets (one per storage node) with PVCs; ConfigMap, Secret, probes, resource limits | A single-replica StatefulSet per node lets a specific node be held down (`scale --replicas=0`) and restored, which makes the before/during/after failure demonstration clean and controlled. |
| **Monitoring / logging** | Structured JSON logs with correlation IDs; coordinator diagnostics endpoints; RabbitMQ management UI; a CLI cluster watcher | Produces the evidence required for each demonstration step with no extra infrastructure. |
| **Cloud** | Not used (optional in the brief); local Minikube/kind/Docker Desktop only | Keeps the project reproducible on any laptop at no cost. |

## 5. Data model, statuses and validation

### 5.1 EmergencyRequest

| Field | Type | Notes |
|---|---|---|
| `requestId` | UUID string | Required by brief. Generated by the Request Service before the first write. |
| `type` | enum | `water`, `food`, `medicine`, `transportation`, `shelter` |
| `location` | object | `{"lat": -90..90, "lon": -180..180}` numbers |
| `details` | string | 1 to 1000 characters |
| `status` | enum | `PENDING`, `MATCHED`, `IN_PROGRESS`, `COMPLETED`, `NO_MATCH`, `CANCELLED` |
| `createdAt`, `updatedAt` | ISO-8601 UTC | Set by the service, never by the client |
| `matchedResourceId` | UUID or null | Set when `MATCHED` |
| `matchAttemptCount` | integer | Incremented on every matching attempt |
| `eventPending` | boolean | True from creation (or from `POST /match`) until RabbitMQ confirms the match event. Used by the sweeper. |
| `version` | integer | Storage version of the record; returned to clients, used internally for conditional writes |

### 5.2 Resource

| Field | Type | Notes |
|---|---|---|
| `resourceId` | UUID string | Required by brief |
| `name` | string, optional | Organisation, volunteer or vehicle name (1 to 100 characters) |
| `type` | enum | Same enum as requests |
| `location` | object | Same shape as requests |
| `availability` | boolean | Required by brief. Invariant: `availability == (status == AVAILABLE)` |
| `status` | enum | `AVAILABLE`, `ASSIGNED`, `OFFLINE` |
| `assignedRequestId` | UUID or null | Set while `ASSIGNED` |
| `createdAt`, `updatedAt`, `version` | - | As for requests |

### 5.3 Key scheme and storage record

- Keys are namespaced: `request:{uuid}` and `resource:{uuid}`. The shard is computed from the **full key string**, so the same ID always maps to the same shard.
- A storage record is `{key, value (JSON), version, deleted, updatedAt}`. Versions start at 1 on creation and increase by 1 on every update or delete (deletes are tombstones, so they replicate and sync like any other operation).
- Every successful write also appends `{seq, epoch, key, op, value, version, ts}` to the node's operation log. Sequence numbers are contiguous per shard and are the basis of replication and recovery.

### 5.4 Status machines

#### Request status transitions

| From | To | Triggered by | Side effects |
|---|---|---|---|
| (new) | `PENDING` | Client `POST /requests` | Match event published |
| `PENDING` | `MATCHED` | Matching Service (internal) | Sets `matchedResourceId`; resource is `ASSIGNED` |
| `PENDING` | `NO_MATCH` | Matching Service (internal) | No eligible resource found |
| `NO_MATCH` | `PENDING` | Client `POST /match/{id}` | Match event published again |
| `MATCHED` | `IN_PROGRESS` | Client `PUT .../status` | - |
| `IN_PROGRESS` | `COMPLETED` | Client `PUT .../status` | Resource released to `AVAILABLE` |
| `MATCHED`, `IN_PROGRESS` | `COMPLETED` / `CANCELLED` | Client `PUT .../status` | Resource released to `AVAILABLE` |
| `PENDING`, `NO_MATCH` | `CANCELLED` | Client `PUT .../status` | No resource to release |

Clients may set only `IN_PROGRESS`, `COMPLETED` and `CANCELLED` through `PUT /requests/{id}/status`. `PENDING`, `MATCHED` and `NO_MATCH` are system-controlled. Any other combination is rejected with `409 INVALID_TRANSITION`; a value outside the enum is `400`.

#### Resource status transitions

| From | To | Triggered by |
|---|---|---|
| (new) | `AVAILABLE` (availability true) or `OFFLINE` (availability false) | Client `POST /resources` |
| `AVAILABLE` | `ASSIGNED` | Matching Service reservation (conditional write) |
| `ASSIGNED` | `AVAILABLE` | Release (request completed/cancelled, compensation, reconciler) |
| `AVAILABLE` | `OFFLINE` | Client `PUT /resources/{id}/status` |
| `OFFLINE` | `AVAILABLE` | Client `PUT /resources/{id}/status` |

A client cannot change an `ASSIGNED` resource (`409`). Setting a resource `OFFLINE` or `AVAILABLE` also sets `availability` accordingly.

### 5.5 Validation rules

- Unknown JSON fields are rejected. Missing or mistyped fields give `400 INVALID_INPUT` with a `details` list naming the field. FastAPI's default `422` must be remapped to `400` with a custom exception handler.
- Path IDs must be valid UUIDs: a malformed ID gives `400`; a well-formed but unknown ID gives `404`.
- All timestamps are generated server-side in UTC. Clients never set `status`, `createdAt` or `version` on creation.

## 6. API specification

### 6.1 Conventions

- All bodies are JSON (`Content-Type: application/json`). Field names are camelCase. Timestamps are ISO-8601 UTC.
- Every response carries `X-Correlation-Id` (echoed if the client sent one, generated otherwise).
- Errors use one envelope, produced by one shared exception handler:

```
{ "error": { "code": "INVALID_INPUT",
             "message": "Request validation failed",
             "details": [ { "field": "location.lat", "problem": "must be between -90 and 90" } ] } }
```

| HTTP | Error code | Used when |
|---|---|---|
| **400** | `INVALID_INPUT` | Validation failure, malformed UUID, unknown enum value, unknown field |
| **404** | `NOT_FOUND` | Well-formed ID that does not exist |
| **409** | `INVALID_TRANSITION`, `VERSION_CONFLICT`, `RESOURCE_ALREADY_ASSIGNED` | Forbidden status change, lost optimistic-concurrency race, reservation conflict |
| **503** | `STORAGE_UNAVAILABLE` | No usable storage copy for the key (includes `Retry-After` header) |
| **500** | `INTERNAL_ERROR` | Unexpected failure (logged with correlation ID) |

### 6.2 Public client API (all seven endpoints)

| Method | Endpoint | Service | Purpose | Success |
|---|---|---|---|---|
| POST | `/requests` | Request | Create request, save as `PENDING`, queue match event | 201 |
| GET | `/requests/{id}` | Request | Retrieve a request and its current status | 200 |
| PUT | `/requests/{id}/status` | Request | Update request status (client-allowed transitions) | 200 |
| POST | `/resources` | Resource | Register a resource or volunteer | 201 |
| GET | `/resources` | Resource | List resources; optional filters `type`, `status`, `availability` | 200 |
| PUT | `/resources/{id}/status` | Resource | Update resource status or availability | 200 |
| POST | `/match/{requestId}` | Request | Queue (or re-queue) asynchronous matching | 200 |

Convenience endpoints (not required, useful for demos and tests): `GET /resources/{id}`, `GET /requests` (list), `GET /health/live`, `GET /health/ready`. Request Service and Resource Service are exposed on separate ports (8001 and 8002); the Postman collection holds both base URLs as environment variables. An optional Kubernetes Ingress can present a single base URL.

#### POST /requests

```
// request
{ "type": "water",
  "location": { "lat": 12.9716, "lon": 77.5946 },
  "details": "Flooded street, 5 people need drinking water" }

// 201 Created, Location: /requests/3f2c...
{ "requestId": "3f2c9a1e-...", "type": "water", "status": "PENDING",
  "location": { "lat": 12.9716, "lon": 77.5946 }, "details": "Flooded street, ...",
  "createdAt": "2026-11-15T10:00:00Z", "updatedAt": "2026-11-15T10:00:00Z",
  "matchedResourceId": null, "matchAttemptCount": 0, "version": 1,
  "matchQueued": true }
```

`matchQueued` is `false` when RabbitMQ was unavailable at that moment; the record is still saved and the sweeper publishes the event later. Errors: `400`, `503`.

#### GET /requests/{id}  and  PUT /requests/{id}/status

```
// PUT body
{ "status": "IN_PROGRESS" }
// 200 -> full updated request. 400 bad value, 404 unknown id, 409 invalid transition, 503 storage
```

#### POST /resources  and  GET /resources

```
// POST body
{ "name": "Red Cross water truck 1", "type": "water",
  "location": { "lat": 12.9352, "lon": 77.6245 }, "availability": true }
// 201 -> { "resourceId": "...", "status": "AVAILABLE", "availability": true, "assignedRequestId": null, ... }

// GET /resources?type=water&status=AVAILABLE
{ "items": [ { ...resource... } ], "count": 1 }
```

#### PUT /resources/{id}/status  and  POST /match/{requestId}

```
// PUT /resources/{id}/status body (AVAILABLE or OFFLINE only; ASSIGNED is system-controlled)
{ "status": "OFFLINE" }                       // 200 -> updated resource, availability=false

// POST /match/{requestId}  (no body)
// 200 { "requestId": "3f2c...", "status": "PENDING", "matchQueued": true }
// 404 unknown id, 409 request already MATCHED / IN_PROGRESS / COMPLETED / CANCELLED
```

### 6.3 Internal service APIs (service to service)

| Service | Endpoint | Purpose |
|---|---|---|
| Request | `GET /internal/requests/{id}` | Matching Service loads a request |
| Request | `POST /internal/requests/{id}/match-result` | Body `{"outcome":"MATCHED","resourceId":"..."}` or `{"outcome":"NO_MATCH"}`. Applies only if the request is still `PENDING`, otherwise `409` (caller compensates). Increments `matchAttemptCount`. |
| Resource | `GET /internal/resources/candidates?type=water` | Eligible resources of a type (`availability=true`, `AVAILABLE`), plus `partial` flag if a shard was unreachable |
| Resource | `GET /internal/resources/by-request/{requestId}` | Find a resource already reserved for a request (makes retries idempotent) |
| Resource | `POST /internal/resources/{id}/reserve` | Body `{"requestId":"...","expectedVersion":n}`. Conditional write `AVAILABLE` to `ASSIGNED`; `409` on conflict |
| Resource | `POST /internal/resources/{id}/release` | Body `{"requestId":"..."}`. Releases only if still assigned to that request |

### 6.4 Coordinator API

| Method / path | Purpose |
|---|---|
| `POST /internal/nodes/register` | Storage node announces `{nodeId, shardId, endpoint, role hint, epoch, lastSeq, status}`. Response assigns role and epoch. |
| `POST /internal/nodes/heartbeat` | Periodic `{nodeId, role, epoch, status, lastSeq, replicaAckSeq, recordCount}`. Response returns the authoritative role/epoch (control channel). Unknown node gives `404` so the node re-registers (coordinator restart). |
| `POST /internal/nodes/degraded` | Primary reports that its replica is out of sync. Recorded **before** the primary acknowledges the first degraded write. |
| `PUT /internal/data/{key}` | Store or update. Body `{"value":{...},"ifVersion":n}`. Omitted `ifVersion` = upsert, `0` = create only. `201` created, `200` updated, `409` conflict, `503` shard unavailable. |
| `GET /internal/data/{key}` | Retrieve `{key, value, version}`; `404` unknown; `503` unavailable. |
| `DELETE /internal/data/{key}` | Tombstone delete (replicated). |
| `GET /internal/scan?prefix=resource:&allowPartial=false` | Scatter-gather across the three shards. `503` if a shard is unavailable unless `allowPartial=true`, in which case the response sets `"partial": true`. |
| `GET /diagnostics/nodes` | All nodes: shard, role, health, epoch, last heartbeat age, last sequence. |
| `GET /diagnostics/shards` | Per shard: state (`HEALTHY`/`DEGRADED`/`UNAVAILABLE`), epoch, primary, record counts. |
| `GET /diagnostics/route?key=...` | Shows computed shard and target node without touching data. |

### 6.5 Storage node API

| Method / path | Purpose |
|---|---|
| `GET /health` | `{nodeId, shardId, role, status (STARTING/CATCHING_UP/READY), epoch, lastSeq, records}` |
| `PUT /internal/data/{key}` | Primary only. Checks role and epoch, evaluates `ifVersion`, commits locally, replicates, acknowledges. Returns `{version, seq, replicated}`. |
| `GET /internal/data/{key}` | Returns the record or `404`. |
| `DELETE /internal/data/{key}` | Primary only. Tombstone. |
| `GET /internal/scan?prefix=` | All live records with the prefix. |
| `POST /internal/replicate` | Replica only. `{epoch, seq, op, key, value, version}`. Applies in order; `409 GAP` (with own `lastSeq`) or `409 STALE_EPOCH`. |
| `GET /internal/sync?since=&epoch=` | Primary serves log entries after `since`, in pages. Performs the log-matching check; answers `snapshotRequired` if histories diverge or the log was truncated. |
| `GET /internal/snapshot` | Full copy of all records and tombstones plus `lastSeq` and `epoch`. |
| `POST /internal/role` | Coordinator instructs promotion or demotion `{role, epoch}`. |

## 7. Distributed storage design

### 7.1 Sharding

**Shared library function (implemented once, imported by the coordinator and tests)**

```
def shard_of(key: str, n_shards: int = 3) -> int:
    digest = hashlib.sha256(key.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % n_shards
```

- Never use a process-randomized hash. A **test-vector file** (about 20 fixed keys with expected shard IDs) is checked in and verified by unit tests, so any accidental change is caught immediately.
- The shard is computed from the full namespaced key (`request:{uuid}`, `resource:{uuid}`). UUIDs are random, so keys spread evenly; seeding about 60 keys shows data on all three shards.
- Replicas are copies, not extra shards. Exactly three logical shards exist; each is served by a primary and a replica node.
- Every routing decision is logged as `key -> shardId -> node (role, epoch)` and can be queried through `GET /diagnostics/route`.
- Limitation to state: changing the number of shards would remap most keys (mod-N). Rebalancing is out of scope.

### 7.2 Coordinator

#### State

- **Node registry** (in memory): `nodeId, shardId, endpoint, role, epoch, health (UP/DOWN), status, lastHeartbeat, lastSeq, replicaAckSeq, recordCount`.
- **Shard state** (persisted to a small JSON file on a PersistentVolume, written atomically): per shard `epoch` and `primaryNodeId`. This prevents epoch regression after a coordinator restart.
- **Shard health** is derived: `HEALTHY` (both nodes up, ready, replica in sync), `DEGRADED` (exactly one safe node serving), `UNAVAILABLE` (no node that can safely serve).

#### Routing rules

| Operation | Rule |
|---|---|
| **Write** | Route to the shard primary only if it is `UP`, `READY` and the shard has a valid epoch. Otherwise answer `503 STORAGE_UNAVAILABLE` with `Retry-After`. Failed nodes are never used. |
| **Read** | Prefer the primary. If the primary is unreachable and the replica is `UP`, `READY` and **known in sync**, read from the replica. If no copy can be proven current, answer `503`. |
| **Scan** | Query every shard (primary preferred, in-sync replica as fallback), merge, return. `503` if a shard is unavailable unless `allowPartial=true`. |
| **Connection failure mid-request** | Reads retry once on the replica. Writes are not blindly retried by the coordinator; the caller retries with backoff and relies on `ifVersion` semantics (a retried create that returns `409` for the same payload counts as success). |

### 7.3 Storage node

#### Schema

```
kv(key TEXT PRIMARY KEY, value TEXT /*JSON*/, version INTEGER, deleted INTEGER, updated_at TEXT)
oplog(seq INTEGER PRIMARY KEY, epoch INTEGER, key TEXT, op TEXT /*PUT|DELETE*/,
      value TEXT, version INTEGER, ts TEXT)
meta(k TEXT PRIMARY KEY, v TEXT)     -- last_applied_seq, epoch, role
-- SQLite opened with journal_mode=WAL, synchronous=FULL; data directory is a mounted volume
```

#### Startup sequence

1. Open the database and read `last_applied_seq`, `epoch` from `meta`. Status `STARTING`.
2. Register with the coordinator (retry with backoff until accepted). The coordinator answers with the authoritative role and epoch.
3. If assigned **primary**: verify local integrity, set status `READY`.
4. If assigned **follower**: status `CATCHING_UP`, run the recovery procedure (section 7.7), then `READY`.
5. Start the heartbeat loop (every `HEARTBEAT_INTERVAL_SEC`).

#### Write path (primary)

1. Reject unless `role == PRIMARY` and the request epoch equals the node epoch (`409 STALE_EPOCH` / `409 NOT_PRIMARY`).
2. Take the node-wide write lock (writes are serialized, which defines the sequence order).
3. Evaluate `ifVersion` against the current record (`0` = must not exist, `n` = must equal `n`, absent = upsert). Mismatch gives `409 VERSION_CONFLICT`.
4. In one SQLite transaction: write the new row (`version = old + 1`), append the log entry (`seq = last + 1`) and commit durably.
5. If the replica is considered healthy: send `POST /internal/replicate` and wait up to `REPLICATION_TIMEOUT_SEC` for its acknowledgement. Success gives `replicated: true`.
6. If the replica is down, lagging or times out: execute the **degraded-write rule** below, then acknowledge with `replicated: false`.
7. Release the lock and return `{version, seq, replicated}`.

> **Degraded-write rule**
>
> A write is acknowledged on the primary alone only after the primary has told the coordinator (`POST /internal/nodes/degraded`) that the replica is out of sync, and the coordinator has recorded it and set the shard to `DEGRADED`. If the coordinator cannot be reached, the write is rejected with `503`. This guarantees that a stale replica is never promoted by mistake, and that no write is ever silently acknowledged without the required active copy.

#### Read path

Reads are served by the primary (read-your-writes). A follower serves reads only when the coordinator routes one to it because it is known in sync. Deleted records return `404`.

### 7.4 Replication

- **Model:** one primary and one replica per shard. The primary assigns sequence numbers; the replica applies operations strictly in order.
- **Replica apply rule:** if `seq == lastApplied + 1` apply and acknowledge; if `seq <= lastApplied` acknowledge without applying (idempotent); if `seq > lastApplied + 1` answer `409 GAP` and start catching up from the primary.
- **Epoch rule:** a replica rejects `replicate` messages whose epoch is lower than its own (`409 STALE_EPOCH`). A primary that receives `STALE_EPOCH` immediately demotes itself to follower (it has been fenced).
- **Conditional writes replicate as results:** the replica receives the final value and version, never the condition, so replay is deterministic.
- **Log retention:** the log is kept for the last `OPLOG_RETAIN` operations (default 10,000). A replica that is further behind receives a snapshot instead.
- **Consistency provided:** while both copies are healthy, every acknowledged write exists on both before the client hears about it. Reads from the primary see all acknowledged writes.

### 7.5 Heartbeats and failure detection

- Each storage node sends `POST /internal/nodes/heartbeat` every `HEARTBEAT_INTERVAL_SEC` (default 2 s) with its role, epoch, status, last sequence and the highest sequence its replica has acknowledged.
- A background coordinator task runs every second. A node with no heartbeat for `MISSED_HEARTBEATS_LIMIT` intervals (default 3, so about 6 s) is probed once directly (`GET /health`, 1 s timeout) to avoid a false positive, then marked `DOWN`.
- Routing stops using a `DOWN` node immediately. The detection, the state change and the routing change are all logged with timestamps (evidence for the demonstration).
- All thresholds are environment variables so they can be shortened for tests.

### 7.6 Failover (promotion)

When a primary is marked `DOWN` the coordinator evaluates promotion:

| Condition | Action |
|---|---|
| Replica is `UP`, `READY` and the shard was `HEALTHY` at the last report (`replicaAckSeq == lastSeq`, no degraded report) | Promote: `epoch += 1`, persist shard state, send `POST /internal/role {PRIMARY, epoch}` to the replica, then route writes to it. Shard becomes `DEGRADED` (single node). |
| Replica is up but the shard was `DEGRADED` (replica known stale) | **Do not promote.** Shard becomes `UNAVAILABLE`; requests get `503`. Normal service resumes when the old primary returns. (Documented operator override: `POST /diagnostics/shards/{id}/force-promote`, logged loudly, for emergencies only.) |
| Both nodes down | Shard `UNAVAILABLE`; `503` for keys on that shard. The other two shards are unaffected. |

The new primary starts accepting writes only after it has acknowledged the promotion. Stale-epoch requests from the coordinator or from an old primary are rejected by every node.

### 7.7 Recovery and synchronization

Applies to any node that starts or restarts as a follower (a returning replica, or a returning old primary that has been demoted).

1. Send `GET /internal/sync?since=<lastSeq>&epoch=<epoch of my last log entry>` to the current primary.
2. **Log-matching check:** the primary verifies that its log entry at `since` has the same epoch. If it does, the histories agree and the primary returns the missing entries in pages. If the entry is missing (log truncated) or its epoch differs (the returning node holds writes the new primary never saw), the primary answers `snapshotRequired`.
3. On `snapshotRequired` the node downloads `GET /internal/snapshot`, writes it to a temporary database file, then atomically swaps it in. Unreplicated writes that were never acknowledged to a client are discarded; this is stated in the report.
4. The node applies pages until its `lastSeq` equals the primary's. Writes arriving meanwhile are covered by the replica apply rule and the catch-up loop.
5. Status becomes `READY`. The next heartbeat shows `lastSeq == primary lastSeq`; the coordinator marks the shard `HEALTHY` and clears the degraded flag. Only now may the node serve reads or be promoted.

If a primary returns and **no** promotion happened while it was away (the shard was `UNAVAILABLE`), it resumes as primary with the unchanged epoch and its replica catches up from it.

### 7.8 Special cases and expected behaviour

| Scenario | Expected behaviour |
|---|---|
| **Replica down, primary up** | Writes acknowledged with `replicated:false` after the coordinator records the degraded state; reads from primary; shard `DEGRADED`; replica syncs on return. |
| **Primary down, replica in sync** | Detection about 6 s, promotion, writes continue, no acknowledged data lost. During the detection window reads fall back to the replica; writes get `503` with `Retry-After` and the callers retry. |
| **Primary down, replica stale** | No promotion, `503` for that shard until the old primary returns. |
| **Both nodes of a shard down** | `503` for that shard only; other shards unaffected; recovers automatically when a safe node returns. |
| **Old primary returns after promotion** | Told it is a follower, log-matching check, delta or snapshot sync, then `READY`. |
| **Replication times out but replica is alive** | Treated as replica failure for that write: degraded-write rule; replica later detects a gap and catches up. |
| **One heartbeat missed** | Nothing happens (threshold is three). |
| **Coordinator restarts** | Persisted shard epochs reload; nodes receive `404` on heartbeat and re-register; shards route again once their primary has registered; until then `503`. |
| **Duplicate or replayed replicate message** | Acknowledged without applying (idempotent). |
| **Old primary tries to write or replicate after promotion** | Rejected with `STALE_EPOCH`; it demotes itself. |

> **Stated limitation**
>
> The single coordinator is a single point of failure, and without a consensus protocol the epoch fencing is an educational safeguard, not a production-grade split-brain guarantee. Raft-style leader election and coordinator replication are future work.

## 8. Messaging and matching design

### 8.1 RabbitMQ topology

| Object | Name | Settings |
|---|---|---|
| **Exchange** | `emergency.events` | direct, durable |
| **Queue** | `request.match` | durable; bound with routing key `request.match`; dead-letter exchange `emergency.retry` |
| **Exchange** | `emergency.retry` | direct, durable (receives failed messages) |
| **Queue** | `request.match.retry` | durable; `x-message-ttl` 5000 ms; on expiry dead-letters back to `emergency.events` with key `request.match` |
| **Exchange** | `emergency.dlx` | direct, durable |
| **Queue** | `request.match.dlq` | durable; holds messages that failed `MAX_RETRIES` (default 3) times; inspected in the management UI |

Every service declares the full topology idempotently at start-up with **identical arguments** (a mismatch makes RabbitMQ refuse the declaration). Broker credentials come from a Kubernetes Secret / Compose environment, never from the code.

### 8.2 Message format

```
properties: delivery_mode=2 (persistent), content_type=application/json, message_id=<eventId>
headers:    x-retry-count (int), x-correlation-id
body:       { "eventId": "uuid", "requestId": "uuid", "type": "MATCH_REQUESTED", "createdAt": "ISO-8601" }
```

Messages carry identifiers only; the consumer always reads the current request from the Request Service, so a stale message can never overwrite fresher state.

### 8.3 Publishing and the pending-event (outbox) sweeper

Storage and RabbitMQ do not share a transaction, so the design makes the request record itself the outbox:

1. Save the request first, with `eventPending = true` (atomic with creation).
2. Publish with **publisher confirms** and `mandatory=True`. A `nack`, an unroutable return or a broker exception counts as failure.
3. On confirm, clear `eventPending` with a conditional write (retry on version conflict; if the request has meanwhile left `PENDING` the flag is cleared lazily).
4. A **sweeper** thread in the Request Service runs every `SWEEP_INTERVAL_SEC` (default 15 s). It scans `request:` records (`allowPartial=true`) and republishes every request that is `PENDING`, has `eventPending == true` and was updated more than `SWEEP_GRACE_SEC` (default 10 s) ago.
5. Several Request Service replicas may sweep at once; duplicate events are harmless because the consumer is idempotent.

Result: a request saved while the broker is down, or a crash between "saved" and "published", is never lost.

### 8.4 Consumer behaviour

- The Matching Service runs one consumer per process in a background thread with automatic reconnect (exponential backoff). `prefetch_count = 1`.
- The message is acknowledged **only after** the match outcome has been recorded (or the message has been moved to the retry queue / dead-letter queue).
- Business outcomes are never retried through the broker: a duplicate, an unknown request, a `NO_MATCH` result or a successful match are all acknowledged.
- Transient failures (storage `503`, Request/Resource Service unreachable, timeouts) are retried: the consumer republishes the message to `emergency.retry` with `x-retry-count + 1` and acknowledges the original. After `MAX_RETRIES` it publishes to `emergency.dlx` instead.
- If the worker crashes before acknowledging, RabbitMQ redelivers the message to another worker; idempotency makes this safe.

### 8.5 The match_request() algorithm

One function implements matching. It is called by the queue consumer for both automatic matching and `POST /match`, because both paths publish the same event.

**Pseudo-code**

```
match_request(requestId):
 1  req = RequestService.get(requestId)                     # 404 -> ack and drop
 2  if req.status != PENDING: return DUPLICATE              # already matched/cancelled -> ack
 3  existing = ResourceService.by_request(requestId)        # crash recovery / idempotency
 4  if existing: goto 8 with existing
 5  cands, partial = ResourceService.candidates(req.type)   # type match, available only
 6  if cands is empty:
        if partial: raise Transient                         # a shard was unreachable: retry later
        return RequestService.match_result(NO_MATCH)
 7  sort cands by (haversine_km(req.location, c.location), c.resourceId)
    for c in cands:
        r = ResourceService.reserve(c.id, requestId, c.version)
        if r == 200: chosen = c; break
        if r == 409: continue                               # lost race: try next nearest
        else: raise Transient
    if no chosen: re-fetch candidates once; if still none -> NO_MATCH
 8  res = RequestService.match_result(MATCHED, chosen.id)
    if res == 409:                                          # request changed meanwhile
        ResourceService.release(chosen.id, requestId); return DUPLICATE
    if res failed after 3 tries: ResourceService.release(...); raise Transient
 9  return MATCHED
```

- **Haversine** with Earth radius 6371.0088 km. Ties are broken by `resourceId` so the result is deterministic.
- The matcher logs a candidate table (resource, distance, decision) for each request: this is the evidence that the nearest resource of the correct type was chosen.
- Optional configuration `MAX_MATCH_DISTANCE_KM` (default unlimited) ignores resources that are too far away.

### 8.6 Atomic reservation

- The Resource Service reads the resource, checks `status == AVAILABLE` and `availability == true`, then writes `status = ASSIGNED, assignedRequestId = requestId` with `ifVersion = <version read>`.
- The shard primary evaluates the condition inside its write lock and SQLite transaction. Two workers reserving the same resource at the same time cannot both succeed: one gets `200`, the other `409 RESOURCE_ALREADY_ASSIGNED`.
- Release is also conditional: the resource returns to `AVAILABLE` only if `assignedRequestId` still equals the requesting ID.

### 8.7 Compensation and reconciliation

- **Compensation:** if the match outcome cannot be recorded after a successful reservation, the worker releases the resource (conditional release).
- **Idempotent retry:** step 3 of the algorithm finds a reservation already made for this request, so a worker that crashed after reserving does not reserve a second resource.
- **Reconciler (backstop):** a periodic job in the Resource Service (every `RECONCILE_INTERVAL_SEC`, default 60 s) finds `ASSIGNED` resources older than `RECONCILE_GRACE_SEC` whose request is not `MATCHED` or `IN_PROGRESS` with that resource ID, and releases them.
- On `COMPLETED` or `CANCELLED` the Request Service asks the Resource Service to release the resource; the reconciler also covers a lost release call.

### 8.8 Scaling the matching workers

Start with exactly one worker and prove the sequential flow and the reservation tests first. Then scale the Matching Service (`--scale matching-service=3` in Compose, `kubectl scale` in Kubernetes). RabbitMQ distributes messages round-robin; because reservation uses conditional writes and the consumer is idempotent, the concurrency test (50 requests competing for 5 resources) must still assign each resource at most once.

## 9. Implementation process

The work is organized as an ordered series of stages. Each stage produces something that runs, is tested, and is wrapped in a container as soon as it exists. A stage is finished only when **all** of its exit criteria are met; the next stage builds on it.

### 9.1 Repository layout

```
emergency-matching/
  README.md  Makefile  docker-compose.yml  .env.example
  common/ems_common/        sharding.py  models.py  errors.py  logging.py  haversine.py
                            storage_client.py (coordinator client + retry)  rabbit.py (topology, publisher)
  services/
    storage-node/   storage_node/{main,db,replication,recovery}.py   Dockerfile  requirements.txt
    coordinator/    coordinator/{main,registry,router,health_checker,failover}.py  Dockerfile
    request-service/  resource-service/  matching-service/    (each: <package_name>/, Dockerfile, requirements.txt)
                      (request_service/, resource_service/, matching_service/)
  k8s/              00-namespace.yaml  01-config.yaml  02-secret.yaml  rabbitmq.yaml
                    storage-node.tpl.yaml  generate-storage.sh  coordinator.yaml  services.yaml
  scripts/          seed.py  load_loop.py  watch_cluster.py  find_key_for_shard.py  reset.sh  demo_*.sh
  tests/            unit/  component/  integration/  fault/   (pytest markers)
  postman/          ems.postman_collection.json  ems.postman_environment.json
  docs/             architecture.md  api.md  diagrams/  evidence/  report/
```

### 9.2 Working rules

- A task is **done** when: code is written, unit tests pass, its Dockerfile builds (if it is a deployable component), logs follow the shared format, and docs/contract files are updated.
- Contracts (JSON shapes, statuses, key names, message formats, error codes) live in `common/` and `docs/api.md`. Changing one requires everyone's agreement.
- `make test` runs unit and component tests; `make up` starts Compose; `make itest` runs integration tests; `make k8s-up` deploys to Kubernetes.
- Fault-injection switches (`FAULT_*` environment variables) are added with the feature they test and are **off by default**.

### 9.3 Suggested ownership (if working as a team of three)

| Owner | Stages and components |
|---|---|
| **A** | Storage node, coordinator, sharding, replication, heartbeats, failover and recovery (stages 1, 2, 4, 5) |
| **B** | Request Service, Resource Service, data models, validation, status rules, reservation logic (stage 6) |
| **C** | RabbitMQ, Matching Service, Docker Compose, Kubernetes manifests, scripts (stages 3, 7, 8, 9) |
| **All** | Stage 0 contracts, end-to-end tests, demonstration runbook, evidence capture and the report (stage 10) |

### Stage 0 - Foundations

**Goal.** Everyone can build, run and test the same skeleton, and the contracts are frozen.

**Tasks**

- Verify tooling: Python 3.11, pip/venv, Docker and Compose v2, `kubectl`, Minikube (or kind), curl, Postman, Git.
- Create the repository layout, `Makefile`, `.env.example`, `.dockerignore`, linting (ruff) and pytest configuration.
- Write `ems_common`: shard function with test vectors, models and enums, error envelope and exception handlers (including `422` to `400`), JSON logger with correlation IDs, Haversine function, coordinator client with retry/backoff, RabbitMQ topology helper.
- Freeze: JSON schemas, status transition tables, key naming, event schema, public and internal API tables (sections 5 and 6).
- Create service stubs that start and answer `/health/live` and `/health/ready`.

> **Exit criteria**
>
> • Every teammate can clone, run `make test` and start all stubs.
>
> • Shard test vectors and Haversine tests pass.
>
> • Contract documents agreed.

### Stage 1 - Storage node (standalone)

**Goal.** A single storage node that is correct, durable and observable on its own.

**Tasks**

- Implement the SQLite layer: `kv`, `oplog`, `meta`, WAL mode, atomic write transaction, version handling.
- Implement `PUT`, `GET`, `DELETE` (tombstone), `scan`, `/health`, environment-variable configuration, structured logging.
- Implement `ifVersion` semantics (absent / `0` / `n`) and `409` responses.
- Write the Dockerfile (non-root, volume at `/data`, health check). Build and run the container.
- Tests: CRUD, missing key, create-only conflict, version conflict, scan prefix, tombstone, persistence after container restart.

> **Exit criteria**
>
> • All CRUD and conflict tests pass.
>
> • Data survives `docker restart` of the container.
>
> • Image builds and runs with a mounted volume.

### Stage 2 - Coordinator and three primaries

**Goal.** Deterministic hash routing across three shards with a visible distribution.

**Tasks**

- Implement node register and heartbeat handling, the in-memory registry and the persisted shard-state file.
- Implement routing for `PUT/GET/DELETE /internal/data/{key}` and the scatter-gather `scan`.
- Implement diagnostics: `/diagnostics/nodes`, `/diagnostics/shards` (with record counts), `/diagnostics/route`.
- Log every routing decision (`key -> shard -> node`). Write the coordinator Dockerfile.
- Run coordinator plus three storage nodes in Compose. Write `scripts/seed.py` and a distribution script that writes about 60 keys and prints per-shard counts.

> **Exit criteria**
>
> • The same key always lands on the same shard; the log shows it.
>
> • All three shards receive data; counts printed.
>
> • Clients use only the coordinator, never a node address.

### Stage 3 - Kubernetes walking skeleton

**Goal.** Remove Kubernetes risk early by deploying the thin slice that already exists.

**Tasks**

- Start Minikube (about 4 GB RAM, 2 or more CPUs). Load or build the images inside the cluster.
- Write the namespace, ConfigMap, the storage-node template with PVC and `generate-storage.sh`, and the coordinator Deployment/Service/PVC.
- Deploy the coordinator and the three primaries. Check service DNS, node registration, PVC persistence, and that deleting a pod brings it back.

> **Exit criteria**
>
> • All pods `Running` and `Ready`; nodes registered in the coordinator.
>
> • Data survives deleting a storage pod (PVC).
>
> • The load/deploy procedure is written down.

### Stage 4 - Replication

**Goal.** Every shard has a replica that is provably identical when both copies are healthy.

**Tasks**

- Add the six-node layout (`s0-a`, `s0-b`, ...), initial role assignment, and the peer address configuration.
- Implement the primary write path with sequence numbers, `POST /internal/replicate`, the replica apply rule (in order, idempotent, gap detection).
- Implement the degraded-write rule and `POST /internal/nodes/degraded`; shard health derivation in the coordinator.
- Implement `GET /internal/sync` and `GET /internal/snapshot` and the follower catch-up loop.
- Add heartbeat fields `lastSeq` and `replicaAckSeq`.

> **Exit criteria**
>
> • After a batch of writes, primary and replica contain identical records and equal `lastSeq`.
>
> • Stopping a replica leaves the system writable (degraded) and a restart converges the data.
>
> • Gap and duplicate replicate cases are covered by tests.

### Stage 5 - Failure detection, failover and recovery

**Goal.** A storage node can be stopped and restarted with no loss of acknowledged data and a fully logged timeline.

**Tasks**

- Implement the coordinator health-checker (heartbeat timeout plus direct probe), the `DOWN` state, and failed-node avoidance in routing.
- Implement promotion rules, shard epochs (persisted), the `POST /internal/role` call, and stale-epoch rejection on nodes.
- Implement rejoin: demotion of a returning old primary, the log-matching check, delta and snapshot recovery, `READY` only after catch-up.
- Handle both-nodes-down and stale-replica cases with `503`; implement the optional force-promote override; handle coordinator restart (re-register on `404`).
- Build `scripts/watch_cluster.py` and `scripts/load_loop.py` to show before, during and after.

> **Exit criteria**
>
> • Stop a primary under a running load loop: no acknowledged write lost, writes resume after promotion.
>
> • Restart it: it becomes a follower, syncs and the shard returns to `HEALTHY`.
>
> • Both-down gives clean `503` for that shard only.

### Stage 6 - Resource Service and Request Service

**Goal.** All seven public endpoints work through the coordinator with correct validation and status rules.

**Tasks**

- Implement the Resource Service: register, list with filters (via coordinator scan), update status, internal candidates, reserve, release, by-request lookup, reconciler.
- Implement the Request Service: create, get, update status with the transition table, `POST /match/{id}` (publishing stubbed behind an interface), internal get and match-result, release on completion/cancellation.
- Implement validation, the error envelope, `409` handling and retry-on-version-conflict for ordinary updates.
- Write both Dockerfiles. Create the Postman collection with all seven endpoints and the error cases.

> **Exit criteria**
>
> • The full REST table passes, including `400`, `404`, `409` and `503` paths.
>
> • Reserve is atomic: two concurrent reserves on one resource give exactly one `200`.

### Stage 7 - RabbitMQ and Matching Service

**Goal.** End-to-end asynchronous matching with safe delivery semantics.

**Tasks**

- Add RabbitMQ to Compose. Implement topology declaration, the publisher with confirms, `eventPending` handling and the sweeper.
- Implement the Matching Service: consumer with manual ack and prefetch 1, `match_request()`, retry queue, dead-letter queue, reconnect logic, health endpoint.
- Implement compensation and idempotent retry (by-request lookup). Add fault-injection switches (`FAULT_MATCH_DELAY_MS`, `FAULT_MATCH_FAIL`).
- Run with one worker first; scale to several only after the sequential tests pass.

> **Exit criteria**
>
> • A created request becomes `MATCHED` with the nearest correct-type resource.
>
> • 20+ concurrent requests: no resource is assigned twice, duplicates never create a second assignment.
>
> • Broker outage and consumer crash tests pass.

### Stage 8 - Docker Compose integration

**Goal.** The whole stack starts with one command and passes the integration suite.

**Tasks**

- Finish `docker-compose.yml`: networks, named volumes, health checks, `depends_on` conditions, environment configuration, published ports.
- Add the seed and reset scripts. Run the integration and fault tests (stop and start storage containers, stop RabbitMQ, kill a worker).

> **Exit criteria**
>
> • `docker compose up -d` from a fresh clone gives a working system.
>
> • The storage-failure drill works under load with Compose.

### Stage 9 - Kubernetes full deployment

**Goal.** Every Kubernetes item in the brief is deployed and demonstrated.

**Tasks**

- Write manifests for RabbitMQ, the three domain services, and the remaining storage StatefulSets; add ConfigMap, Secret, probes, resource requests/limits and NodePorts.
- Verify service DNS between all components, PVC persistence, pod self-healing, and scaling of the Matching Service from one to three replicas.
- Rehearse storage failure with `kubectl scale sts/<node> --replicas=0` and restore.

> **Exit criteria**
>
> • All pods `Ready`; end-to-end flow works inside the cluster.
>
> • Pod deletion self-heals; Matching Service scales to 3 with continued processing; storage failure and recovery shown.

### Stage 10 - Verification, demonstration and documentation

**Goal.** Everything required is repeatable, evidenced and documented.

**Tasks**

- Run the full test matrix (section 13) and keep the results.
- Write the reset and seed scripts, the demonstration runbook (section 14), and capture logs/screenshots for all nine scenarios.
- Write the README, API documentation, architecture and sequence diagrams, and the design report (section 15).
- Rehearse the demonstration end to end at least twice using only scripted commands.

> **Exit criteria**
>
> • Every checklist item in section 17 is ticked.
>
> • Two consecutive clean rehearsals.

## 10. Containerization (Docker)

### 10.1 Images

| Image | Built from | Port | Volume |
|---|---|---|---|
| `ems/storage-node` | `services/storage-node` | 9000 | `/data` (SQLite) |
| `ems/coordinator` | `services/coordinator` | 8000 | `/state` (shard-state file) |
| `ems/request-service` | `services/request-service` | 8001 | - |
| `ems/resource-service` | `services/resource-service` | 8002 | - |
| `ems/matching-service` | `services/matching-service` | 8003 | - |
| `rabbitmq:3.13-management` | official image | 5672, 15672 | `/var/lib/rabbitmq` |

The same `storage-node` image runs all six storage nodes; only environment variables differ.

### 10.2 Dockerfile pattern

**Example (build context is the repository root so the shared library can be copied)**

```
FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY services/request-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt     # dependency layer cached
COPY common/ /app/common/
RUN pip install --no-cache-dir /app/common
COPY services/request-service/app /app/app
RUN useradd -r -u 10001 appuser && chown -R appuser /app
USER appuser
EXPOSE 8001
HEALTHCHECK --interval=10s --timeout=3s --retries=5 \
  CMD python -c "import urllib.request;urllib.request.urlopen('http://localhost:8001/health/live')"
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001"]
```

- Build: `docker build -f services/request-service/Dockerfile -t ems/request-service:1.0 .` from the repository root. A `Makefile` target builds all five images.
- Use a `.dockerignore` (tests, docs, `.git`, caches). Pin dependency versions in `requirements.txt`.
- The storage node creates its data directory with correct ownership for the non-root user; mounted volumes must be writable by it.

### 10.3 Docker Compose layout

- One user-defined bridge network `ems-net`; containers reach each other by service name (`coordinator`, `s0-a`, `rabbitmq`, ...). Ports are published to the host only for the Request Service (8001), Resource Service (8002), coordinator diagnostics (8000) and the RabbitMQ UI (15672).
- A YAML extension block (`x-storage-node`) defines the shared storage-node settings; storage-node containers get explicit names (`ems-s0-a` ... `ems-s2-b`) so they can be stopped by name; each of the six nodes overrides `NODE_ID`, `SHARD_ID`, `PEER_URL`, `ADVERTISE_URL` and its own named volume.
- Health checks on RabbitMQ, the coordinator and every service; `depends_on` with `condition: service_healthy` for start-up order. Services must **also** retry their connections with backoff, because ordering at start-up is not a substitute for resilience at run time.
- The Matching Service has no fixed `container_name`, so `docker compose up -d --scale matching-service=3` works.
- Named volumes for each storage node, the coordinator state and RabbitMQ so that data survives container restarts; `docker compose down -v` is the reset.

### 10.4 Everyday commands

```
docker compose build                          # build all images
docker compose up -d                          # start the full stack
docker compose ps                             # health and ports
docker compose logs -f coordinator            # follow one component
docker stop ems-s1-a ; docker start ems-s1-a     # storage node failure and recovery
docker compose up -d --scale matching-service=3      # scale workers
docker compose down -v                        # stop and reset all data
```

## 11. Kubernetes deployment

### 11.1 Object inventory

| Workload | Kind | Replicas | Service | Storage | Notes |
|---|---|---|---|---|---|
| **Coordinator** | Deployment (`strategy: Recreate`) | 1 | ClusterIP `coordinator` + NodePort 30080 (diagnostics) | PVC (shard state) | Single instance by design; Recreate avoids two coordinators during a rollout |
| **Storage nodes `s0-a` ... `s2-b`** | 6 × StatefulSet | 1 each | Headless Service per node (stable DNS) | PVC from `volumeClaimTemplates` | Generated from one template; roles come from the coordinator, not the pod ordinal |
| **Request Service** | Deployment | 1 (scalable) | ClusterIP + NodePort 30081 | - | Includes the outbox sweeper |
| **Resource Service** | Deployment | 1 (scalable) | ClusterIP + NodePort 30082 | - | Includes the reconciler |
| **Matching Service** | Deployment | 1, then 3 | ClusterIP (health) | - | Stateless consumer; scale for throughput |
| **RabbitMQ** | StatefulSet or Deployment | 1 | ClusterIP (5672) + NodePort 31672 (UI) | PVC | Credentials from a Secret |
| **Configuration** | ConfigMap `ems-config`, Secret `ems-secret` | - | - | - | Service URLs, thresholds, broker credentials |

### 11.2 Design details

- **Namespace** `ems` holds everything.
- **Stable identity for storage nodes:** each node is its own single-replica StatefulSet with a headless Service, giving DNS names such as `ems-s1-a-0.ems-s1-a.ems.svc.cluster.local`. Nodes advertise this address to the coordinator, so a restarted pod (new IP) keeps the same identity.
- **Probes:** liveness on `/health/live` (process alive); readiness on `/health/ready` (dependencies reachable; for a storage node, status `READY`). Add a startup probe for slow storage nodes.
- **Resources:** requests/limits sized for a laptop, for example 100m CPU and 128 Mi RAM for Python services (limit 256 Mi), 64 Mi for storage nodes, 256 Mi / 512 Mi for RabbitMQ.
- **Configuration:** addresses and thresholds in the ConfigMap; broker credentials in the Secret; no hard-coded values in images.
- **Images:** `imagePullPolicy: IfNotPresent` with local images loaded by `minikube image load` (or by building inside Minikube's Docker daemon with `eval $(minikube docker-env)`; for kind use `kind load docker-image`).
- **Access:** NodePorts, or `kubectl port-forward` / `minikube service --url` when the driver does not expose NodePorts directly. An Ingress is an optional extra.
- **Resource budget:** about 11 pods at rest (6 storage, coordinator, RabbitMQ, 3 services) plus 2 more when scaled; about 4 GB for the Minikube VM is sufficient with Python services.

### 11.3 Deployment order

1. Namespace, ConfigMap, Secret.
2. RabbitMQ and its PVC.
3. Storage-node StatefulSets (generated) and PVCs.
4. Coordinator (nodes register as soon as it is up; they also retry until it exists).
5. Request, Resource and Matching Services.
6. Seed data and smoke test through the NodePort or port-forward.

```
minikube start --memory=4096 --cpus=2
make images && for i in storage-node coordinator request-service resource-service matching-service; do \
    minikube image load ems/$i:1.0; done
./k8s/generate-storage.sh > k8s/storage-nodes.yaml
kubectl apply -f k8s/00-namespace.yaml -f k8s/01-config.yaml -f k8s/02-secret.yaml
kubectl apply -f k8s/rabbitmq.yaml -f k8s/storage-nodes.yaml -f k8s/coordinator.yaml -f k8s/services.yaml
kubectl -n ems get pods -w
```

### 11.4 Required Kubernetes demonstrations

| Brief requirement | Demonstration | Evidence |
|---|---|---|
| **Deployments, Services, replicas** | `kubectl -n ems get deploy,sts,svc,pods` | Listing showing all objects Ready |
| **Service-to-service communication** | Create a request; Request Service to coordinator to storage; Matching Service to Resource and Request Services by DNS name | Matching logs and a `MATCHED` request |
| **Pod failure and automatic recovery** | `kubectl -n ems delete pod -l app=request-service`; keep calling the API | New pod becomes Ready; API recovers; pod list before/after |
| **Microservice scaling** | `kubectl -n ems scale deploy/matching-service --replicas=3`; submit many requests | Three Ready pods; logs from three workers; all requests matched |
| **Storage failure and recovery** | `kubectl -n ems scale sts/ems-s1-a --replicas=0` under load, then `--replicas=1` | Heartbeat loss, promotion, continued operation, catch-up logs |

> **Why one StatefulSet per storage node**
>
> A plain `kubectl delete pod` on a StatefulSet pod restarts within seconds, so the failed state may be too short to show. Because each node has its own single-replica StatefulSet, scaling that StatefulSet to zero holds exactly one chosen node down for as long as needed (including a primary) and scaling it back to one demonstrates recovery.

## 12. Observability and diagnostics

The demonstration is judged on visible behaviour, so observability is built in rather than added at the end.

| Mechanism | Details | Used as evidence for |
|---|---|---|
| **Structured JSON logs** | Every line has `ts`, `level`, `service`, `nodeId` (storage), `correlationId`, `event` and context. The same correlation ID flows through HTTP headers and RabbitMQ messages. | All scenarios |
| **Key log events** | Routing (`key -> shard -> node`); heartbeat received/missed; node `DOWN`/`UP`; promotion with new epoch; degraded write; replication gap; catch-up start/progress/end; snapshot swap; reservation attempt/conflict/release; match decision with candidate table; message published/confirmed/acked/retried/dead-lettered; sweeper republish. | Distribution, failure, recovery, matching, messaging |
| **Coordinator diagnostics** | `/diagnostics/nodes`, `/diagnostics/shards` (state, epoch, primary, counts), `/diagnostics/route?key=` | Distribution, before/during/after failure |
| **RabbitMQ management UI** | Queue depth, consumers, message rates, contents of `request.match.dlq` | Asynchronous processing, retry, DLQ |
| `scripts/watch_cluster.py` | Polls diagnostics every second and prints a one-screen table: shard, state, primary, epoch, node health, record counts | Failure timeline |
| `scripts/load_loop.py` | Continuously creates and reads records; prints success/failure counters and latency | Continued operation during failure |
| **Health endpoints** | `/health/live`, `/health/ready` on every service; storage `/health` with role and status | Kubernetes probes and recovery |

## 13. Testing strategy

Tests are written with the features they cover and run continuously. Four layers are used; `pytest` markers (`unit`, `component`, `integration`, `fault`) select them.

| Layer | What it covers | Runs against |
|---|---|---|
| **Unit** | Hash stability and test vectors; validation; status transition tables; Haversine distance (known city pair within tolerance); nearest-resource choice and tie-break; version/conflict logic; stale-epoch rejection; replica apply rule (in order, duplicate, gap); log-matching decision; retry counting | In-process, no containers |
| **Component** | Storage node CRUD and persistence (reopen DB); replication pair in-process; coordinator routing with fake nodes; Request/Resource services with a fake coordinator client; matching with fake services | In-process (FastAPI `TestClient`, temp directories) |
| **Integration** | Public API through the real stack; matching end to end; distribution; RabbitMQ behaviour | Docker Compose stack (and later Kubernetes) |
| **Fault** | Container/pod stop and restart, broker outage, worker crash, both-nodes-down | Compose and Kubernetes, scripted |

### 13.1 Test matrix

| Area | Minimum checks | Req. |
|---|---|---|
| **Requests** | Create (201, stored fields, `PENDING`), get (200), unknown ID (404), malformed ID (400), invalid type/location/details (400), valid and invalid status updates (200/409/400) | FR-1, 4, 5, 6 |
| **Resources** | Register (201), list with and without filters, update status, `ASSIGNED` is read-only to clients (409), availability invariant | FR-2, 4, 6 |
| **Matching correctness** | 1 water request, 2 water resources at different distances, 1 food resource: nearest **water** resource is chosen, request `MATCHED`, resource `ASSIGNED`; no eligible resource gives `NO_MATCH`; `POST /match` retries a `NO_MATCH` after a resource is added | FR-3, MR-3 |
| **Concurrency** | 50 simultaneous requests competing for 5 resources: exactly 5 `MATCHED`, 45 `NO_MATCH`, each resource assigned at most once; two simultaneous reserves of one resource give one `200` and one `409` | TS-1 |
| **Messaging** | Many queued requests all processed; duplicate event does not duplicate the assignment; worker killed mid-processing then message redelivered and handled once; broker down while creating a request (still `201`, matched after broker returns via sweeper); injected failure retried then dead-lettered after 3 attempts | MR-1, MR-2 |
| **Sharding** | Known keys map to expected shards; the same ID always the same shard; seeded keys appear on shards 0, 1 and 2; clients never use node addresses | SR-2, 3, 4 |
| **Replication** | After writes primary and replica equal (records and `lastSeq`); duplicate and gap replicate messages handled; degraded write only after coordinator notified | SR-2 |
| **Failure and recovery** | Stop a primary under load: detection about 6 s, promotion, writes resume, no acknowledged write lost; restart: follower, delta sync, `HEALTHY`; stop a replica: writes continue degraded, restart converges; stale-replica case gives 503 and no promotion; both nodes down gives 503 for that shard only; returning old primary with divergent tail gets a snapshot; coordinator restart recovers | FT-1 to FT-5 |
| **Containers** | Each image builds; `docker compose up` from a clean clone works; data survives container restart | DK-1 |
| **Kubernetes** | Service DNS between all components; PVC persistence; application pod deletion self-heals; Matching Service scaled to 3 keeps processing; storage StatefulSet scaled to 0 and back | K8-1, K8-2 |

### 13.2 Fault-injection switches (off by default)

- `FAULT_MATCH_DELAY_MS` slows the matcher so a worker can be killed mid-processing.
- `FAULT_MATCH_FAIL=always|n` forces transient failures to exercise retry and dead-lettering.
- `REPLICATION_TIMEOUT_SEC` and heartbeat thresholds are lowered in test configurations for fast failover tests.

### 13.3 Evidence

Every test run and demonstration step saves its evidence into `docs/evidence/NN-name/`: command transcripts, relevant log excerpts, screenshots of the RabbitMQ UI and of the cluster watcher, and `kubectl` listings.

## 14. Demonstration runbook

The nine scenarios below follow the brief's required demonstration exactly. They run on the **Kubernetes deployment** (the final system); the Compose stack is used for rehearsal and as a fallback. Every failure scenario is shown as **before, during and after**.

### 14.1 Preparation

- Images built and loaded; cluster running with all pods `Ready`; `reset.sh` run so counters and queues start clean; `seed.py` has registered a fixed set of resources (several water resources at different distances, one food resource).
- Windows open: terminal for commands, `watch_cluster.py`, `kubectl -n ems get pods -w`, RabbitMQ UI, coordinator logs, Postman.
- Environment variables set: `REQ=http://<node>:30081`, `RES=http://<node>:30082`, `COORD=http://<node>:30080`.
- `find_key_for_shard.py 1` has produced a request ID that hashes to the shard that will be failed; the exact pod names are written on the runbook.
- Backup: a screen recording of a clean run, and the Compose stack ready.

### 14.2 Scenarios

#### 1. Request creation and retrieval

- `curl -X POST $REQ/requests -H "Content-Type: application/json" -d '{"type":"water","location":{"lat":12.9716,"lon":77.5946},"details":"Flood, 5 people"}'`
- `curl $REQ/requests/<id>`

**Evidence:** Responses `201` then `200`, all required fields present (ID, type, location, details, status `PENDING` or `MATCHED`, creation time).

#### 2. Resource registration

- `curl -X POST $RES/resources -d '{"name":"Truck 1","type":"water","location":{...},"availability":true}'`
- `curl "$RES/resources?type=water"`

**Evidence:** `201`, then a list showing type, location, `availability: true`, status `AVAILABLE`.

#### 3. Resource matching

- Create a water request; `curl -X POST $REQ/match/<id>` if it was not already matched; `curl $REQ/requests/<id>` and `curl $RES/resources/<resourceId>`; show the matcher's candidate table in the Matching Service log.

**Evidence:** Nearest water resource chosen (not the food one, not a farther water one); request `MATCHED` with `matchedResourceId`; resource `ASSIGNED` with `assignedRequestId`.

#### 4. Data distribution across the three shards

- `python scripts/seed.py --requests 30 --resources 30`
- `curl $COORD/diagnostics/shards`
- `curl "$COORD/diagnostics/route?key=request:<id>"`

**Evidence:** Per-shard record counts all non-zero; routing log lines `key -> shardId -> node`; the same key always routes to the same shard.

#### 5. Asynchronous processing

- `python scripts/load_loop.py --create 30 --burst` while watching `request.match` in the RabbitMQ UI and the worker log.

**Evidence:** Queue depth rises then drains; consumer log lines with correlation IDs; every request ends `MATCHED` or `NO_MATCH`; no resource assigned twice.

#### 6. Storage failure

- Before: `watch_cluster.py` shows shard 1 `HEALTHY`, primary `s1-a`, epoch 1; start `load_loop.py`.
- During: `kubectl -n ems scale sts/ems-s1-a --replicas=0`; then `curl $REQ/requests/<id-on-shard-1>`.

**Evidence:** Heartbeat-missed and `DOWN` log lines after about 6 s; promotion of `s1-b` with epoch 2; shard `DEGRADED`; the load loop shows at most a brief burst of `503` then success; the shard-1 request is still readable and new writes succeed.

#### 7. Node recovery

- After: `kubectl -n ems scale sts/ems-s1-a --replicas=1`; watch the table.

**Evidence:** `s1-a` registers as follower, catch-up log shows entries replayed (or snapshot), status `READY`; shard returns to `HEALTHY`; both nodes report equal `lastSeq` and record counts; data written during the outage is present on `s1-a`.

#### 8. Kubernetes pod recovery

- `kubectl -n ems get pods` then `kubectl -n ems delete pod -l app=request-service`; keep issuing `curl $REQ/requests/<id>`.

**Evidence:** Replacement pod created and `Ready` within seconds; API answers again; list before and after.

#### 9. Kubernetes scaling

- `kubectl -n ems scale deploy/matching-service --replicas=3`; run the burst from scenario 5.

**Evidence:** Three `Ready` pods; logs from all three workers; all requests processed with no double assignment; service stays available.

### 14.3 Reset between runs

`scripts/reset.sh` deletes and recreates the data (Compose: `down -v`; Kubernetes: delete the namespace PVCs and restart the pods), purges the RabbitMQ queues, and re-runs `seed.py`. The demonstration must be repeatable from a clean state with no improvised commands.

## 15. Submission package and design report

The brief does not prescribe a fixed file list, so confirm the final format and the submission location with the instructor or TA. The package below covers everything the project asks to be shown or justified.

| Item | Contents |
|---|---|
| **Git repository** | All source code (services, coordinator, storage node, shared library), Dockerfiles, Compose file, Kubernetes manifests and generator script, scripts, tests |
| **README** | Architecture summary, prerequisites, setup, build and run commands (Compose and Kubernetes), how to run tests, how to run the demonstration |
| **Diagrams** | Architecture diagram; sequence diagrams for (a) normal create-and-match and (b) storage failure and recovery; shard and replication diagram |
| **API documentation** | Public and internal APIs, error codes, example requests and responses; Postman collection and environment; curl examples |
| **Test results** | Output of unit, component, integration and fault test runs against the test matrix |
| **Demonstration runbook and evidence** | Step-by-step commands and expected results for all nine scenarios, with logs, screenshots and `kubectl` listings (before, during and after for failures) |
| **Design report** | See outline below (short and focused) |

### 15.1 Design report outline

1. **Introduction and requirements** - problem, scope, requirement coverage table.
2. **Architecture** - components, communication, end-to-end flows.
3. **Design choices and justification** (the list left to the team in the brief): programming language and framework; shard storage mechanism; sharding and partitioning strategy; replica synchronization mechanism; resource-matching strategy; RabbitMQ exchange, queue and routing configuration; microservice boundaries and internal API design; Kubernetes deployment configuration; monitoring and logging approach; cloud choice (none).
4. **Fault tolerance** - heartbeat detection, failover rules, epochs, recovery, special cases, and the guarantees given.
5. **Testing and results** - test matrix, outcomes, demonstration evidence.
6. **Limitations and trade-offs** - see section 16.
7. **Conclusion and future work.**

## 16. Risks and limitations

### 16.1 Risks and mitigations

| Risk | Mitigation |
|---|---|
| **The storage subsystem (replication, failover, recovery) takes disproportionate effort** | Build it before the application services, keep to the defined minimum (sequence log, heartbeat, promotion, catch-up), and finish Stage 5 exit criteria before adding features |
| **Double assignment or duplicate processing** | Conditional writes and the "already matched" check from the first matching implementation; the concurrency test is a gate for Stage 7 |
| **Lost match event between save and publish** | Request record is the outbox (`eventPending`) plus the sweeper; broker-outage test |
| **Kubernetes problems discovered late** | Walking skeleton in Stage 3; Compose and Kubernetes configurations kept in step |
| **Laptop memory exhausted** | Python services, one replica of each application service, scale only the Matching Service, Minikube with about 4 GB, resource limits set |
| **Failure demonstration goes wrong live** | Fixed request ID on the failed shard, noted pod names, scripted commands, two rehearsals, backup recording, Compose fallback |
| **Pod restart too fast to show the "during" state** | One StatefulSet per storage node, scaled to zero to hold the node down |
| **Image not found in the cluster** | `imagePullPolicy: IfNotPresent` and an explicit image-load step in the deployment script |
| **RabbitMQ declaration mismatch errors** | One shared topology helper with fixed arguments used by every service |
| **Time lost to optional features** | Optional list is closed until all checklist items in section 17 are met |

### 16.2 Limitations to state in the report

- **Single coordinator** is a single point of failure (the brief requires one). Its state is recoverable from node registrations and a persisted shard-state file, but it is not replicated.
- **No consensus protocol.** Epoch fencing and the log-matching check are educational safeguards; they do not give production-grade split-brain protection under network partitions.
- **Fixed `mod 3` sharding** does not rebalance when the number of shards changes.
- **Single failure assumption.** Simultaneous loss of both copies of a shard makes that shard unavailable (`503`), by design, rather than risking inconsistent data.
- **Unreplicated, unacknowledged writes** held by a returning old primary are discarded during snapshot recovery.
- **Listing resources** scans all shards; adequate at project scale but not for large data sets (no secondary indexes).
- **No authentication** and no encryption in transit; Kubernetes runs on a single local node.
- `NO_MATCH` requests are retried only on demand (`POST /match`) unless the optional automatic retry is implemented.

## 17. Final completion checklist

- [ ] All seven public REST endpoints work with JSON bodies and meaningful status codes (`200`, `201`, `400`, `404`, `409`, `503`).
- [ ] Requests and resources are stored through the coordinator and are spread across exactly three logical shards by the SHA-256 rule; the client never sees storage locations.
- [ ] Every shard has a primary and a replica; heartbeats detect failed nodes; failed nodes receive no traffic.
- [ ] During a node failure an in-sync replica is used, acknowledged data is not lost, and the system keeps processing; a stale copy is never promoted.
- [ ] A restarted node synchronizes (delta or snapshot) and rejoins as `READY` only after catching up.
- [ ] RabbitMQ processes multiple requests asynchronously; duplicate delivery and consumer crashes never produce a duplicate assignment; failures are retried and dead-lettered.
- [ ] Matching considers type and location and reserves resources atomically; `POST /match/{id}` and automatic matching share one code path.
- [ ] Dockerfiles exist for all components; the full stack runs under Docker Compose from a clean clone.
- [ ] Kubernetes Deployments, Services, pod replicas, service-to-service communication, scaling and automatic pod recovery are all demonstrated.
- [ ] All nine required demonstration scenarios are scripted, repeatable and have saved evidence (before, during and after for failures).
- [ ] The test matrix has been run under normal and failure conditions and the results are saved.
- [ ] README, API documentation, diagrams, Postman collection, runbook and design report are complete; the submission format has been confirmed with the instructor or TA.

> **Implementation principle**
> Build the smallest complete version first. Make every mandatory behaviour work and be observable before adding anything optional.

## Appendix A. Configuration reference

All configuration is supplied through environment variables (Compose `environment`, Kubernetes ConfigMap and Secret). Defaults shown are for local runs.

| Component | Variable | Default / example | Meaning |
|---|---|---|---|
| Storage node | `NODE_ID`, `SHARD_ID` | `s1-a`, `1` | Identity and shard |
|  | `INITIAL_ROLE` | `primary` / `replica` | Hint used only at first bootstrap |
|  | `ADVERTISE_URL`, `PEER_URL` | node DNS names | Address given to coordinator; partner node address |
|  | `COORDINATOR_URL` | `http://coordinator:8000` | Registration and heartbeat target |
|  | `DATA_DIR` | `/data` | SQLite location (mounted volume) |
|  | `HEARTBEAT_INTERVAL_SEC` | `2` | Heartbeat period |
|  | `REPLICATION_TIMEOUT_SEC` | `2` | Wait for replica acknowledgement |
|  | `OPLOG_RETAIN` | `10000` | Log entries kept before snapshot is required |
| Coordinator | `N_SHARDS` | `3` | Number of logical shards |
|  | `MISSED_HEARTBEATS_LIMIT` | `3` | Intervals before `DOWN` |
|  | `HEALTH_CHECK_INTERVAL_SEC` | `1` | Checker period |
|  | `STATE_DIR` | `/state` | Shard-state file location (volume) |
| Request / Resource | `COORDINATOR_URL` | `http://coordinator:8000` | Storage access |
|  | `RABBITMQ_HOST`, `_PORT`, `_USER`, `_PASS` | Secret / `.env` | Broker connection |
|  | `SWEEP_INTERVAL_SEC`, `SWEEP_GRACE_SEC` | `15`, `10` | Outbox sweeper (Request Service) |
|  | `RECONCILE_INTERVAL_SEC`, `RECONCILE_GRACE_SEC` | `60`, `60` | Stuck-reservation reconciler (Resource Service) |
|  | `HTTP_TIMEOUT_SEC` | `3` | Internal client timeout |
| Matching | `REQUEST_SERVICE_URL`, `RESOURCE_SERVICE_URL` | service DNS names | Internal HTTP calls |
|  | `MAX_RETRIES` | `3` | Attempts before dead-letter |
|  | `MAX_MATCH_DISTANCE_KM` | unset | Optional distance cut-off |
|  | `FAULT_MATCH_DELAY_MS`, `FAULT_MATCH_FAIL` | off | Test fault injection |

## Appendix B. Sample internal payloads

```
// Storage node heartbeat  POST /internal/nodes/heartbeat
{ "nodeId": "s1-a", "shardId": 1, "role": "PRIMARY", "epoch": 1, "status": "READY",
  "lastSeq": 412, "replicaAckSeq": 412, "recordCount": { "request": 11, "resource": 9 } }

// Primary -> replica  POST /internal/replicate
{ "epoch": 1, "seq": 413, "op": "PUT", "key": "request:3f2c9a1e-...", "version": 2,
  "value": { "requestId": "3f2c9a1e-...", "status": "MATCHED", "matchedResourceId": "9b1d..." } }

// Follower catch-up  GET /internal/sync?since=400&epoch=1   (response)
{ "snapshotRequired": false, "fromSeq": 401, "toSeq": 412, "entries": [ { "seq": 401, ... } ] }
```

```
// GET /diagnostics/shards
[ { "shardId": 0, "state": "HEALTHY",  "epoch": 1, "primary": "s0-a", "records": 21 },
  { "shardId": 1, "state": "DEGRADED", "epoch": 2, "primary": "s1-b", "records": 20,
    "nodes": [ { "id": "s1-a", "health": "DOWN" }, { "id": "s1-b", "health": "UP", "role": "PRIMARY" } ] },
  { "shardId": 2, "state": "HEALTHY",  "epoch": 1, "primary": "s2-a", "records": 19 } ]
```