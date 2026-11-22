# Distributed Emergency Resource Matching System

A distributed, fault-tolerant emergency resource matching platform designed to connect people in need with nearby available resources during emergency situations.

The system uses a microservices architecture, distributed storage, asynchronous messaging, data replication, and container orchestration to provide reliable resource allocation and demonstrate distributed systems concepts.

---

## Overview

During emergencies such as floods, natural disasters, and other crisis situations, people may require essential resources such as water, food, medicine, transportation, and shelter.

This project aims to build a distributed platform where individuals can submit emergency requests while volunteers, NGOs, and other organizations can register available resources. The system automatically identifies and assigns the nearest suitable resource based on resource type and geographical location.

Beyond resource matching, the project focuses on demonstrating important distributed computing concepts, including:

- Distributed data storage and hash-based sharding.
- Primary-replica replication.
- Heartbeat-based failure detection.
- Automatic failover and recovery synchronization.
- Asynchronous communication using RabbitMQ.
- Concurrent and atomic resource reservation.
- Containerization using Docker.
- Container orchestration using Kubernetes.

## System Architecture

The system consists of three domain microservices, a central coordinator, six distributed storage nodes, and a RabbitMQ message broker.

### Architecture Diagram

```text
                  CLIENTS
              (Postman / curl)
                     |
          +----------+----------+
          |                     |
          v                     v
   REQUEST SERVICE       RESOURCE SERVICE
      (:8001)                (:8002)
          |                     |
          |                     |
          v                     |
      COORDINATOR <-------------+
        (:8000)
          |
     +----+----+----------------+
     |         |                |
     v         v                v
  SHARD 0   SHARD 1          SHARD 2
     |         |                |
  +--+--+   +--+--+          +--+--+
  |     |   |     |          |     |
  P     R   P     R          P     R
  |     |   |     |          |     |
  +-----+   +-----+          +-----+

     P = Primary
     R = Replica

REQUEST SERVICE
       |
       | Publish match event
       v
   RABBITMQ
       |
       | Consume event
       v
 MATCHING SERVICE (:8003)
       |
       +------> RESOURCE SERVICE
       |          (Reserve resource)
       |
       +------> REQUEST SERVICE
                  (Record match outcome)
```

### Core Components

| Component | Responsibility |
|---|---|
| Request Service | Creates emergency requests, retrieves requests, updates statuses, and publishes matching events. |
| Resource Service | Registers resources, searches for suitable candidates, and handles atomic reservations and releases. |
| Matching Service | Processes matching events, identifies the nearest suitable resource, and coordinates assignment. |
| Coordinator | Manages storage-node registration, heartbeats, hash-based routing, failover, and diagnostics. |
| Storage Nodes | Persist records using SQLite, maintain operation logs, and replicate data between primary and replica nodes. |
| RabbitMQ | Handles asynchronous communication between request processing and matching workers. |

## Key Features

### 1. Emergency Request Management
- Create emergency requests for essential resources.
- Retrieve request information using unique identifiers.
- Update request statuses.
- Support resource categories such as water, food, medicine, transportation, and shelter.

### 2. Resource Management
- Register available resources and volunteers.
- Search resources based on type and geographical location.
- Track resource availability and assignment status.
- Release resources when requests are completed or cancelled.

### 3. Intelligent Resource Matching
- Match requests with resources of the appropriate type.
- Calculate geographical distance using the Haversine formula.
- Prioritize the nearest eligible resource.
- Use deterministic tie-breaking for consistent results.
- Prevent multiple requests from reserving the same resource.

### 4. Distributed Storage
- Three logical storage shards.
- SHA-256-based hash partitioning.
- Six storage nodes with one primary and one replica per shard.
- Coordinator-based routing so clients do not need to know physical storage locations.
- SQLite-backed persistent storage and version-controlled records.

### 5. Replication and Fault Tolerance
- Primary-replica data replication.
- Periodic heartbeat monitoring.
- Automatic detection of unavailable storage nodes.
- Replica promotion and epoch-based fencing.
- Delta synchronization and snapshot recovery.
- Protection against stale replica promotion.

### 6. Asynchronous Messaging
- RabbitMQ-based event processing.
- Durable exchanges and queues.
- Publisher confirmations and manual message acknowledgements.
- Retry mechanisms with exponential backoff.
- Dead-letter queue handling.
- Idempotent message processing to prevent duplicate assignments.

### 7. Containerization and Orchestration
- Individual Dockerfiles for application components.
- Docker Compose for local integration.
- Kubernetes Deployments, Services, StatefulSets, and PersistentVolumeClaims.
- Horizontal scaling of matching workers.
- Health checks and automatic pod recovery.

## Technology Stack

| Category | Technologies |
|---|---|
| Programming Language | Python 3.11 |
| Backend Framework | FastAPI |
| ASGI Server | Uvicorn |
| Database | SQLite |
| Messaging | RabbitMQ |
| Containerization | Docker |
| Container Orchestration | Kubernetes |
| Local Kubernetes | Minikube / kind |
| Testing | Pytest |
| API Testing | Postman |
| Version Control | Git and GitHub |

## Repository Structure

`	ext
emergency-matching/
├── LICENSE                 # MIT License
├── Makefile                # Unified build, test, and lifecycle targets
├── README.md               # System overview and quick start guide
├── docker-compose.yml      # 11-container Compose specification
├── pyproject.toml          # Ruff linter and tool configurations
├── pytest.ini              # Pytest configuration and timeout settings
├── requirements-dev.txt    # Developer test and lint tooling dependencies
├── .env.example            # Canonical environment variable template
├── .gitignore              # Ignored local artifacts and cache
├── .gitattributes          # Line ending normalizations
├── .dockerignore           # Build context exclusion rules
│
├── common/                 # Shared Python library
│   ├── ems_common/         # Data models, transitions, sharding, rabbitmq client
│   └── pyproject.toml      # Common package build spec
│
├── services/               # Five containerized microservices
│   ├── storage-node/       # Replicated SQLite storage node service
│   ├── coordinator/        # Topology router and failover coordinator
│   ├── request-service/    # Request intake and lifecycle API
│   ├── resource-service/   # Emergency resource inventory API
│   └── matching-service/   # Nearest-resource asynchronous matching engine
│
├── k8s/                    # Production Kubernetes manifests
│   ├── 00-namespace.yaml   # ems namespace
│   ├── 01-config.yaml      # Cluster ConfigMap
│   ├── 02-secret.yaml      # Base64 secret credentials
│   ├── rabbitmq.yaml       # RabbitMQ StatefulSet & headless service
│   ├── coordinator.yaml    # Coordinator Deployment & Service
│   ├── storage-nodes.yaml  # 6 storage StatefulSets & headless services
│   ├── generate_storage.py # Storage manifest generator
│   └── services.yaml       # Application deployments and NodePort services
│
├── scripts/                # Operational, testing, and demonstration scripts
│   ├── ops/                # Cluster administration & load generation
│   │   ├── seed.py, reset.sh, load_loop.py, watch_cluster.py
│   │   ├── find_key_for_shard.py, check_replication.py, app_load.py, k8s_ports.sh
│   │   ├── distribution.py, run_live_drill.sh
│   ├── demo/               # 9 sequential demonstration scenarios
│   │   ├── demo_01_request.sh .. demo_09_k8s_scaling.sh
│   │   ├── demo_storage_node.py, run_demo.sh, rehearse.sh
│   ├── k8s/                # Kubernetes lifecycle & verification
│   │   ├── k8s_up.sh, k8s_down.sh, k8s_verify_stage3.py, k8s_verify_stage9.py
│   └── verify/             # Automated validation & test runners
│       ├── test_storage_persistence.sh, test_fresh_clone.sh, run_all_tests.sh
│
├── tests/                  # 4-tier comprehensive automated test suite
│   ├── unit/               # In-memory pure unit tests
│   ├── component/          # In-process mock-cluster component tests
│   ├── integration/        # Full end-to-end integration tests
│   └── fault/              # Failure-injection, network partition & drill tests
│
├── postman/                # End-to-end API collection and environment
│   ├── ems.postman_collection.json
│   └── ems.postman_environment.json
│
└── docs/                   # System documentation and proof artifacts
    ├── README.md           # Documentation index
    ├── architecture.md     # Component architecture and storage topology
    ├── api.md              # REST API contract and status transitions
    ├── runbook.md          # Live demonstration runbook (Scenarios 1-9)
    ├── k8s-local.md        # Local Kubernetes execution guide
    ├── checklist.md        # Verification checklist
    ├── design/             # Authoritative technical specifications (SPEC.md)
    ├── dev/                # Assistant development guidelines
    ├── report/             # Design report and requirements coverage matrix
    └── evidence/           # Preserved demonstration and test transcripts
`

## API Endpoints

The platform exposes seven public REST endpoints.

| Method | Endpoint | Description |
|---|---|---|
| POST | `/requests` | Create an emergency request |
| GET | `/requests/{id}` | Retrieve a request |
| PUT | `/requests/{id}/status` | Update request status |
| POST | `/resources` | Register a resource |
| GET | `/resources` | List and filter resources |
| PUT | `/resources/{id}/status` | Update resource status |
| POST | `/match/{requestId}` | Trigger matching for a request |

The Request Service handles request operations, while the Resource Service handles resource operations. Matching is performed asynchronously by the Matching Service.

### Example Workflow

1. A person submits an emergency request for drinking water.
2. The Request Service validates and stores the request through the Coordinator.
3. The request is initially assigned the `PENDING` status.
4. A matching event is published to RabbitMQ.
5. The Matching Service consumes the event.
6. Suitable water resources are retrieved from the Resource Service.
7. The nearest available resource is selected using geographical distance.
8. The Resource Service atomically reserves the selected resource.
9. The Request Service records the matching outcome.
10. The request becomes `MATCHED`, and the resource becomes `ASSIGNED`.

## Quick Start

### Prerequisites

- Python 3.11+
- Docker & Docker Compose V2
- kubectl & Minikube (for Kubernetes execution)
- GNU Make

### Quick Start (Make & Compose)

`ash
# 1. Setup virtual environment and install dependencies
make install

# 2. Build images and start all 11 Docker Compose containers
make up

# 3. Run unit and component test suites
make test

# 4. Run full end-to-end integration test suite against running stack
make itest
`

### Kubernetes Start

`ash
# 1. Start local Minikube cluster
minikube start --memory=4096 --cpus=2

# 2. Build images, deploy all workloads, and wait for healthy shards
./scripts/k8s/k8s_up.sh

# 3. Start localhost port-forwarding in background
./scripts/ops/k8s_ports.sh
`

### Running the Demo & Test Commands

`ash
# Run all 9 demonstration scenarios sequentially (k8s or compose mode)
./scripts/demo/run_demo.sh compose

# Run two consecutive rehearsals to verify repeatability
./scripts/demo/rehearse.sh compose

# Run full comprehensive test suite across all 4 tiers
./scripts/verify/run_all_tests.sh
`

## Testing

The project follows a layered testing strategy to verify correctness under normal and failure conditions.

| Test Layer | Purpose |
|---|---|
| Unit Tests | Validate individual functions, sharding, Haversine distance, data models, and matching logic. |
| Component Tests | Verify individual services and their interactions with dependent components. |
| Integration Tests | Validate the complete system using Docker Compose and RabbitMQ. |
| Fault Tests | Verify failure detection, replication, recovery, retries, and Kubernetes self-healing. |

The test suite will cover:

- CRUD operations and API validation.
- Correct shard selection and data distribution.
- Primary-replica consistency.
- Concurrent resource reservations.
- Duplicate message handling.
- RabbitMQ retries and dead-lettering.
- Storage-node failure and recovery.
- Kubernetes pod recovery and service scaling.

The Makefile commands are:

```bash
make install    # Create .venv and install all dependencies
make test       # Run unit and component tests
make itest      # Run integration tests
make ftest      # Run fault tests
make all-tests  # Run lint, test, itest, and ftest suites
make up         # Start the full Docker Compose stack (--wait)
make down       # Stop the Docker Compose stack
make reset      # Reset Compose data and seed deterministic resources
make images     # Build all 5 service container images
```

## Fault Tolerance and Reliability

Fault tolerance is a central part of this project.

The system is designed to demonstrate:

- Detection of failed storage nodes through periodic heartbeats.
- Promotion of an in-sync replica when a primary becomes unavailable.
- Epoch-based fencing to reject writes from stale primaries.
- Synchronization of recovered nodes using operation logs or snapshots.
- Continued processing during supported single-node failures.
- Safe handling of unavailable or stale storage copies.
- Idempotent message processing and atomic resource reservation.

The design prioritizes data safety over availability when the system cannot establish that a storage copy is current.

## Design Limitations

This is an educational distributed systems project, not a production-ready emergency response platform.

Known design limitations include:

- The coordinator is a single point of failure.
- No Raft or Paxos consensus protocol is implemented.
- Multiple coordinator instances and dynamic shard rebalancing are outside the current scope.
- Network-partition tolerance is not guaranteed.
- Authentication, authorization, frontend applications, and public-cloud deployment are outside the current scope.
- Kubernetes demonstrations use a local cluster and do not demonstrate multi-machine pod distribution.

## Implementation Roadmap

The project is organized into incremental implementation stages:

| Stage | Component / Milestone |
|---|---|
| 0 | Foundations, shared library, and service skeletons |
| 1 | Standalone storage node |
| 2 | Coordinator and three primary shards |
| 3 | Kubernetes skeleton |
| 4 | Primary-replica replication |
| 5 | Heartbeats, failover, and recovery |
| 6 | Request and Resource Services |
| 7 | RabbitMQ integration |
| 8 | Matching Service |
| 9 | Docker Compose integration and scaling |
| 10 | End-to-end testing, demonstrations, and evidence collection |

Each stage builds upon the previous one and must satisfy its testing and completion criteria before proceeding.

### Running Demonstrations & Automated Tests

```bash
# Run all 9 demonstration scenarios sequentially (k8s or compose mode)
./scripts/demo/run_demo.sh k8s

# Run two consecutive rehearsals to verify repeatability (SPEC 16.1)
./scripts/demo/rehearse.sh k8s

# Run full comprehensive test suite across all 4 tiers (SPEC 13.1)
./scripts/verify/run_all_tests.sh
```

## Documentation

Comprehensive project documentation is available in the `docs/` directory:

- [System Architecture](docs/architecture.md) – Component architecture, Mermaid diagrams, flows A–D, data models, and FSMs.
- [API Reference](docs/api.md) – Complete REST API contracts, error envelopes, and curl examples.
- [Demonstration Runbook](docs/runbook.md) – Step-by-step commands and verification for all 9 demonstration scenarios.
- [Design & Engineering Report](docs/report/design-report.md) – Technical justifications for all 10 architectural choices, fault tolerance analysis, and results.
- [Requirements Coverage Matrix](docs/report/requirements-coverage.md) – Traceability matrix mapping FR-1..8, SR-1..5, MR-1..3, FT-1..5, DK-1, K8-1..3, DM-1, TS-1, JU-1 to code, tests, and evidence.
- [Completion Checklist](docs/checklist.md) – SPEC 17 checklist with concrete evidence for all 12 items.
- [Local Kubernetes Guide](docs/k8s-local.md) – Minikube setup, manifests, and port-forwarding instructions.
- [Evidence Directory](docs/evidence/README.md) – Index of logs, diagnostic dumps, and test transcripts.
- [Postman Collection](postman/) – Complete collection and environment files for live API testing.

## Contributors

Developed as part of the Cloud Computing – Infrastructure Project.

**Project:** Distributed Emergency Resource Matching System

---

*Building a reliable distributed system for emergency resource allocation through fault tolerance, replication, and intelligent matching.*
