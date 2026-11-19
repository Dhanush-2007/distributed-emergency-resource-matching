# System Architecture: Distributed Emergency Resource Matching System (EMS)

The Emergency Resource Matching System (EMS) is a resilient, horizontally partitioned, asynchronous microservice platform designed to coordinate disaster response requests with available emergency resources under high concurrency, node failures, and network volatility.

---

## 1. High-Level System Architecture

The system comprises three user-facing application services, a central coordinator, a 3-shard replicated custom storage engine (6 storage nodes), and RabbitMQ message broker.

```mermaid
graph TD
    Client(["Clients & External Callers"])

    subgraph EdgeServices["Application Services Layer"]
        REQ["Request Service<br/>(Port 8001)"]
        RES["Resource Service<br/>(Port 8002)"]
        MATCH["Matching Service<br/>(Port 8003, Scalable 1..3)"]
    end

    subgraph Messaging["Message Broker"]
        RMQ[("RabbitMQ<br/>(emergency.events / request.match)")]
    end

    subgraph StorageControl["Routing & Coordination Layer"]
        COORD["Coordinator<br/>(Port 8000 / Shard Diagnostics)"]
    end

    subgraph Shards["Distributed Replicated Storage Engine (SHA-256 Partitioning)"]
        subgraph Shard0["Shard 0 (mod 3 = 0)"]
            S0A[("s0-a<br/>(Primary/Replica)")]
            S0B[("s0-b<br/>(Primary/Replica)")]
        end
        subgraph Shard1["Shard 1 (mod 3 = 1)"]
            S1A[("s1-a<br/>(Primary/Replica)")]
            S1B[("s1-b<br/>(Primary/Replica)")]
        end
        subgraph Shard2["Shard 2 (mod 3 = 2)"]
            S2A[("s2-a<br/>(Primary/Replica)")]
            S2B[("s2-b<br/>(Primary/Replica)")]
        end
    end

    Client -->|POST /requests<br/>GET /requests| REQ
    Client -->|POST /resources<br/>GET /resources| RES
    Client -->|POST /match/:id| REQ

    REQ -->|Publish request.created| RMQ
    RMQ -->|Consume request.match| MATCH

    MATCH -->|GET /resources (Find candidates)| RES
    MATCH -->|POST /resources/:id/reserve| RES
    MATCH -->|PUT /requests/:id/status| REQ

    REQ -->|Route key: request:id| COORD
    RES -->|Route key: resource:id| COORD

    COORD -.->|Forward read/write| S0A
    COORD -.->|Forward read/write| S1A
    COORD -.->|Forward read/write| S2A

    S0A <===>|Replication & Sync| S0B
    S1A <===>|Replication & Sync| S1B
    S2A <===>|Replication & Sync| S2B
```

---

## 2. Sharding and Storage Replication Topology

The storage engine implements 3 logical shards. Partitioning is determined deterministically via SHA-256:
$$\text{shardId} = \text{uint32}(\text{sha256}(\text{key})[:4]) \pmod 3$$

```mermaid
graph LR
    subgraph Routing["Deterministic Sharding"]
        Key["Key: 'request:uuid' or 'resource:uuid'"] --> Hash["SHA-256 Hash"]
        Hash --> Mod["uint32(hash[0:4]) % 3"]
    end

    Mod -->|0| Shard0
    Mod -->|1| Shard1
    Mod -->|2| Shard2

    subgraph Shard0["Logical Shard 0"]
        S0P["Primary: s0-a (Epoch E)"]
        S0R["Replica: s0-b (Epoch E)"]
        S0P -->|Sync / Replicate / Snapshot| S0R
    end

    subgraph Shard1["Logical Shard 1"]
        S1P["Primary: s1-a (Epoch E)"]
        S1R["Replica: s1-b (Epoch E)"]
        S1P -->|Sync / Replicate / Snapshot| S1R
    end

    subgraph Shard2["Logical Shard 2"]
        S2P["Primary: s2-a (Epoch E)"]
        S2R["Replica: s2-b (Epoch E)"]
        S2P -->|Sync / Replicate / Snapshot| S2R
    end
```

---

## 3. End-to-End Sequence Diagrams

### 3.1 Flow A & B: Request Creation, Asynchronous Queuing, and Resource Matching

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant REQ as Request Service
    participant COORD as Coordinator
    participant S_NODE as Shard Storage Node
    participant RMQ as RabbitMQ
    participant MATCH as Matching Service
    participant RES as Resource Service

    Client->>REQ: POST /requests (payload)
    Note over REQ: Generates UUID, sets status=PENDING, eventPending=true
    REQ->>COORD: PUT /storage/data/request:<id> (ifVersion=0)
    COORD->>S_NODE: Write to Primary (replicated to peer)
    S_NODE-->>COORD: 200 OK (version=1, seq=S)
    COORD-->>REQ: 200 OK
    REQ-->>Client: 201 Created (requestId, status: PENDING)

    REQ->>RMQ: Publish to emergency.events (rk: request.match)
    Note over REQ: On publish ack: sets eventPending=false in storage

    RMQ->>MATCH: Deliver message (requestId)
    Note over MATCH: Evaluates request details & location

    MATCH->>RES: GET /resources?type=water
    RES-->>MATCH: 200 OK (List of available resources)

    Note over MATCH: Filter eligibility, compute Haversine distance, sort nearest

    MATCH->>RES: POST /resources/:id/reserve (requestId)
    Note over RES: Optimistic concurrency check (version=V, availability=true)
    RES->>COORD: PUT /storage/data/resource:<id> (ifVersion=V)
    COORD-->>RES: 200 OK (new version, assignedRequestId=requestId)
    RES-->>MATCH: 200 OK (Resource Reserved)

    MATCH->>REQ: PUT /requests/:id/status (status=MATCHED, matchedResourceId)
    REQ->>COORD: PUT /storage/data/request:<id> (ifVersion=V_req)
    COORD-->>REQ: 200 OK
    REQ-->>MATCH: 200 OK
    MATCH->>RMQ: basic_ack
```

### 3.2 Failure, Heartbeat Loss, Promotion, and Recovery Sequence

```mermaid
sequenceDiagram
    autonumber
    participant Pri as Primary (s1-a)
    participant Coord as Coordinator
    participant Rep as Replica (s1-b)

    Note over Pri: Primary dies / pod killed
    Coord->>Pri: Probe GET /health (Heartbeat)
    Note over Coord: Connection refused / timeout (Miss 1)
    Coord->>Pri: Heartbeat (Miss 2)
    Coord->>Pri: Heartbeat (Miss 3, ~6 seconds elapsed)
    Note over Coord: Mark s1-a DOWN. Verify s1-b in-sync & UP.

    Coord->>Coord: Increment Epoch: E -> E+1, mark degraded=true
    Coord->>Rep: POST /internal/role {role: PRIMARY, epoch: E+1}
    Rep-->>Coord: 200 OK (Acknowledged promotion)
    Note over Coord: Update routing table: s1-b is now Primary for Shard 1

    Note over Pri: Old Primary (s1-a) recovers / restarts
    Pri->>Coord: Heartbeat (Reports epoch E, role PRIMARY hint)
    Coord-->>Pri: 409 STALE_EPOCH / 404 (Demote to FOLLOWER, target epoch E+1)
    Note over Pri: Adopts role REPLICA, epoch E+1, status CATCHING_UP

    Pri->>Rep: GET /internal/sync?since=seq&epoch=E
    alt Histories Agree (Log Matching OK)
        Rep-->>Pri: 200 OK (Oplog delta entries)
        Pri->>Pri: Apply missing entries in order
    else Split History / Truncated Oplog
        Rep-->>Pri: 200 OK {snapshotRequired: true}
        Pri->>Rep: GET /internal/snapshot
        Rep-->>Pri: 200 OK (Database dump)
        Pri->>Pri: Atomically swap database file
    end

    Note over Pri: Status becomes READY (lastSeq == primary lastSeq)
    Pri->>Coord: Heartbeat {status: READY, lastSeq: S, role: REPLICA}
    Note over Coord: Both nodes READY & in-sync: clear degraded, mark Shard 1 HEALTHY
```

---

## 4. Architectural Flows (A through D)

### Flow A: Request Creation and Outbox Safety
1. Caller submits `POST /requests`.
2. The Request Service generates a UUID, assigns status `PENDING`, sets `eventPending: true`, and writes the record through the Coordinator to the primary storage node via optimistic concurrency (`ifVersion: 0`).
3. Once the storage write succeeds, HTTP `201 Created` is returned to the client.
4. The Request Service publishes the `request.match` event to RabbitMQ exchange `emergency.events`. Upon broker acknowledgement, it updates `eventPending: false`.

### Flow B: Matching and Atomic Reservation
1. A Matching Service worker consumes an event from queue `request.match`.
2. It fetches candidate resources from the Resource Service (`GET /resources?type=<type>`).
3. Candidates are filtered for `status: AVAILABLE` and `availability: true`.
4. Distances are computed using the Haversine formula. The closest candidate is selected; ties are broken by resource ID ascending.
5. The worker attempts atomic reservation via `POST /resources/<id>/reserve`. The Resource Service uses optimistic concurrency (`ifVersion`).
   - If `409 VersionConflict`, another worker reserved the resource; the matcher retries with the next nearest candidate.
   - If no candidates remain, the request transitions to `NO_MATCH`.
6. Once reserved, the worker transitions the request to `MATCHED` with `matchedResourceId` and acknowledges the RabbitMQ message.

### Flow C: Outbox Sweeper Fallback
If the RabbitMQ broker is unreachable when a request is created, the request is safely persisted with `eventPending: true`. A background outbox sweeper thread periodically scans requests with `eventPending: true` and republishes them once broker connectivity is restored, guaranteeing at-least-once delivery.

### Flow D: Storage Replication, Degraded Writes, and Catch-up Sync
1. Normal writes route through the Primary. The Primary executes the write, increments sequence number `lastSeq`, and sends `POST /internal/replicate` to the peer replica.
2. If the replica is unreachable or fails to acknowledge within `REPLICATION_TIMEOUT_SEC`:
   - The Primary reports degraded state to the Coordinator: `POST /internal/nodes/degraded`.
   - The write is acknowledged with `replicated: false`.
   - The Coordinator marks the shard as `DEGRADED`.
3. When the offline replica recovers:
   - It issues `GET /internal/sync` to the primary.
   - Missing oplog entries are applied in sequence until `lastSeq == primary.lastSeq`.
   - The replica marks itself `READY`. On the next heartbeat cycle, the Coordinator restores shard state to `HEALTHY`.

---

## 5. Data Model & State Transitions

### Key Schema
All state is partitioned across shards using namespaced keys:
- Requests: `request:<uuid>`
- Resources: `resource:<uuid>`

### Request Lifecycle Finite State Machine
- `PENDING` $\rightarrow$ `MATCHED` (Successfully matched and reserved)
- `PENDING` $\rightarrow$ `NO_MATCH` (No available resource found)
- `PENDING` $\rightarrow$ `CANCELLED` (Cancelled by user/operator)
- `MATCHED` $\rightarrow$ `FULFILLED` (Resource arrived/incident resolved)
- `MATCHED` $\rightarrow$ `CANCELLED` (Incident aborted; associated resource released)
- `NO_MATCH` $\rightarrow$ `PENDING` (Retried manually via `POST /match/:id` or on new resource addition)

### Resource Lifecycle Finite State Machine
- `AVAILABLE` $\rightarrow$ `ASSIGNED` (Reserved atomically for a request; `availability: false`)
- `ASSIGNED` $\rightarrow$ `AVAILABLE` (Released upon fulfillment or request cancellation)
- `AVAILABLE` $\rightarrow$ `MAINTENANCE` (Temporarily taken offline)
- `MAINTENANCE` $\rightarrow$ `AVAILABLE` (Returned to service)

---

## 6. RabbitMQ Topology & Dead Lettering

| Entity | Name | Type | Durable | Configuration |
|---|---|---|---|---|
| Exchange | `emergency.events` | `direct` | Yes | Primary event bus |
| Queue | `request.match` | `quorum`/`classic` | Yes | Bound to `emergency.events` with routing key `request.match`. DLX: `emergency.retry` |
| Exchange | `emergency.retry` | `direct` | Yes | Retry exchange |
| Queue | `request.match.retry` | `classic` | Yes | `x-message-ttl: 5000`, DLX: `emergency.events`, routing key: `request.match` |
| Exchange | `emergency.dlx` | `direct` | Yes | Dead letter exchange for poison pills |
| Queue | `request.match.dlq` | `classic` | Yes | Bound to `emergency.dlx` after 3 failed retries |
