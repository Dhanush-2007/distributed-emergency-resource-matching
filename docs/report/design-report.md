# Emergency Resource Matching System (EMS): Design & Engineering Report

**Course / Project**: Distributed Systems Capstone  
**System**: Distributed Emergency Resource Matching System (EMS)  
**Specification Version**: 1.0 (SPEC 15.1)

---

## 1. Introduction and Scope

In crisis and disaster response scenarios, emergency requests (such as food, water, medical supplies, shelter, and transport) must be matched rapidly and reliably against distributed physical resources. The operational environment is characterized by high concurrency, erratic network connectivity, and sudden node or infrastructure failures.

The **Emergency Resource Matching System (EMS)** is an asynchronous, horizontally partitioned microservice architecture built to fulfill these rigorous operational requirements. It provides:
1. **Public REST APIs** for transparent request intake and resource registration.
2. **Deterministic horizontal sharding** across 3 storage partitions without exposing storage topologies to clients.
3. **Primary-replica high-availability** with automated failure detection, epoch-fenced failover, and log synchronization.
4. **Asynchronous event-driven matching** powered by RabbitMQ with at-least-once delivery, transactional outbox pattern, and strict concurrency safety preventing double assignments.
5. **Production containerization and orchestration** across Docker Compose and Kubernetes.

---

## 2. System Architecture

The EMS platform adheres to a layered microservice and storage architecture:

```
[ Clients & Public Traffic ]
           │
           ▼
[ Application Edge Services ]
  - Request Service (Port 8001, outbox sweeper)
  - Resource Service (Port 8002, status reconciler)
  - Matching Service (Port 8003, concurrent worker pool)
           │
     ┌─────┴───────────────────────┐
     ▼                             ▼
[ RabbitMQ Event Bus ]      [ Coordinator (Port 8000) ]
  - emergency.events                │ (Routing table & heartbeats)
  - request.match (queue)           ▼
  - emergency.retry          [ Distributed Storage Layer ]
  - request.match.dlq          - Shard 0: Primary (s0-a) <-> Replica (s0-b)
                               - Shard 1: Primary (s1-a) <-> Replica (s1-b)
                               - Shard 2: Primary (s2-a) <-> Replica (s2-b)
```

- **Application Services**: Stateless FastAPI web services. Requests are validated via Pydantic schemas, assigned UUID identifiers, and persisted via HTTP storage client calls through the Coordinator.
- **Message Broker**: RabbitMQ decouples request ingestion from computationally intensive matching. Direct durable exchanges and transactional outbox sweeper threads guarantee that no request is lost even during broker downtime.
- **Coordinator & Storage Layer**: The Coordinator manages shard routing tables, monitors node heartbeats, coordinates failover promotions, and increments monotonic epochs to fence stale primaries. Storage nodes persist data into SQLite engines with ordered operation logs (`oplog`).

---

## 3. Design Choices and Justifications

The brief explicitly delegated ten architectural decisions to the engineering team. Below is the technical rationale for each choice:

### 3.1 Programming Language and Web Framework
- **Choice**: Python 3.11 with FastAPI and Uvicorn.
- **Justification**: FastAPI offers high-performance asynchronous I/O (`uvloop`), native integration with Pydantic for strict input validation, and automatic OpenAPI schema generation. Python's lightweight memory footprint enables running 11 pods and services simultaneously within a modest 4 GB Minikube laptop VM budget.

### 3.2 Shard Storage Mechanism
- **Choice**: SQLite in Write-Ahead Logging (WAL) mode per storage node container.
- **Justification**: SQLite provides ACID transactional guarantees locally, eliminates external database server overhead, and ensures fast cold starts. Operating in WAL mode allows concurrent readers without blocking writers.

### 3.3 Sharding and Partitioning Strategy
- **Choice**: Deterministic SHA-256 hash modulo 3:
  $$\text{shardId} = \text{uint32}(\text{sha256}(\text{key})[:4]) \pmod 3$$
- **Justification**: SHA-256 provides uniform statistical distribution across shards, completely eliminating hotspots without requiring complex consistent hashing rings for a static 3-shard cluster. Partitioning is strictly opaque to clients.

### 3.4 Replica Synchronization Mechanism
- **Choice**: Primary-driven semi-synchronous replication with monotonic operation log (`oplog`), delta sync (`/internal/sync`), and snapshot fallback (`/internal/snapshot`).
- **Justification**: Sequential sequence numbers (`seq`) provide unambiguous ordering. Delta sync minimizes recovery latency by replaying only missing rows, while snapshot streaming recovers nodes with truncated logs or divergent tails safely.

### 3.5 Resource-Matching Strategy
- **Choice**: Geodesic Haversine distance formula with deterministic tie-breaking (ascending resource ID).
- **Justification**: The Haversine metric accurately measures spherical surface distances for disaster logistics without relying on third-party map APIs. Optimistic concurrency control (`ifVersion`) on the resource record ensures atomic reservation during high concurrency.

### 3.6 RabbitMQ Configuration
- **Choice**: Direct durable exchange `emergency.events`, dead-letter exchange `emergency.retry` with TTL (5000ms), and terminal DLQ `request.match.dlq`.
- **Justification**: Enables exponential/delayed retries without blocking active consumer threads. Poison-pill messages are quarantined to the DLQ after 3 failures, preventing queue deadlocks.

### 3.7 Microservice Boundaries & Internal APIs
- **Choice**: Request Service (client ingestion), Resource Service (inventory), Matching Service (worker), and Coordinator (storage gateway).
- **Justification**: Clean separation of operational concerns. Request ingestion remains online even when matching is experiencing peak backpressure. Internal endpoints use structured JSON error envelopes with explicit HTTP error codes (`400`, `404`, `409`, `503`).

### 3.8 Kubernetes Deployment Configuration
- **Choice**: 6 single-replica StatefulSets with headless Services for storage nodes; Deployments for application services and Coordinator; PVCs backed by hostpath/local-storage; `enableServiceLinks: false` and non-root UID `10001`.
- **Justification**: Headless Services grant deterministic DNS hostnames (`ems-s1-a-0...`) essential for cluster membership. Single-replica StatefulSets allow precise fault injection by scaling individual pods to 0.

### 3.9 Monitoring and Observability
- **Choice**: Structured JSON logging with propagated `correlationId`, Coordinator `/diagnostics` endpoints, and a terminal watcher script (`scripts/watch_cluster.py`).
- **Justification**: JSON logs allow easy parsing and automated assertions during fault drills. The live console watcher offers real-time visualization of shard health, node roles, and failover state.

### 3.10 Cloud Choice
- **Choice**: Local virtualization (Minikube & Docker Desktop); no external cloud provider.
- **Justification**: Guarantees 100% reproducible, offline-capable verification without cloud egress latency, credentials, or billable infrastructure dependencies.

---

## 4. Fault Tolerance and Recovery

The system provides robust guarantees against single-node failures:

1. **Heartbeat Failure Detection**: The Coordinator probes storage nodes every 2 seconds. A node missing 3 consecutive heartbeats (~6s) is marked `DOWN`.
2. **Automated Promotion & Epoch Fencing**: When a primary fails, the in-sync replica is promoted to `PRIMARY` with an incremented epoch ($E \rightarrow E+1$). Fencing guarantees that any delayed write from an old primary is rejected with HTTP `409 STALE_EPOCH`.
3. **Degraded Operation**: If a replica fails, the primary notifies the Coordinator via `/internal/nodes/degraded` and serves writes with `replicated: false`. Writes continue uninterrupted.
4. **Follower Rejoin & Log Matching**: A returning node contacts the active primary. If oplog epochs match at the divergence point, missing entries are applied via delta sync. If the log is truncated or diverged, an atomic SQLite snapshot is streamed and swapped into place.
5. **Coordinator Crash Recovery**: Coordinator state is backed by persistent volume storage (`/state/shard_state.json`). On restart, the Coordinator reloads epochs and dynamically rebuilds routing tables as nodes re-register.

---

## 5. Verification and Demonstration Results

All nine demonstration scenarios and test suites were executed on both Docker Compose and Kubernetes environments.

| Metric | Target | Achieved Result |
|---|---|---|
| **Unit & Component Tests** | 100% PASS | 53 / 53 passed |
| **Integration & Concurrency** | 50 concurrent requests $\rightarrow$ 0 double assigns | PASS (verified via `test_stage7_concurrency.py`) |
| **Storage Failover Time** | $\le 8$ seconds | $\sim 6.2$ seconds (3 missed heartbeats) |
| **Kubernetes Healing** | Automatic pod replacement | Verified in $\le 5$ seconds with zero data loss |
| **Demonstration Scenarios** | 9 / 9 Scenarios passing | 100% PASS across 2 consecutive rehearsals |

---

## 6. Stated Limitations and Architectural Trade-offs

Per SPEC 16.2, the following limitations are explicitly noted:
1. **Single Coordinator**: The Coordinator is a single point of failure. While state is recoverable from disk and node heartbeats, it is not replicated via consensus.
2. **Educational Fencing**: Monotonic epochs provide educational safeguards against split-brain under standard failures, but do not replace a full Raft/Paxos consensus quorum under complex network partitions.
3. **Static Sharding**: Sharding uses fixed modulo 3 arithmetic; dynamic rebalancing when adding shards is left for future work.
4. **Single-Failure Assumption**: Simultaneous failure of both nodes in a shard renders that shard unavailable (`503`), prioritizing safety over availability.
5. **Divergent Tail Discard**: Unreplicated writes held by a partitioned old primary are discarded during snapshot replacement.

---

## 7. Conclusion

The Distributed Emergency Resource Matching System achieves all functional, storage, messaging, and fault-tolerance objectives specified in the project brief. Its modular architecture, clean failure boundaries, deterministic sharding, and automated healing demonstrate production-grade distributed systems design principles.
