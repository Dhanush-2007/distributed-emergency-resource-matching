# Requirements Traceability & Coverage Matrix

This document maps all architectural, functional, storage, messaging, fault tolerance, deployment, and testing requirements from **SPEC 2** to implementation files, tests, and demonstration evidence.

---

## 1. Functional Requirements (FR-1 to FR-8)

| ID | Requirement Description | Implementation Files | Automated Tests | Demo Scenario & Evidence | Status |
|---|---|---|---|---|---|
| **FR-1** | `createRequest(type, location, details)` creates request returning `201` | `services/request-service/request_service/main.py` | `tests/unit/test_request_service.py`, `tests/integration/test_stage6_api.py::test_create_and_get_request` | Scenario 1 (`docs/evidence/01-request/`) | COMPLIANT |
| **FR-2** | `registerResource(type, location, availability)` registers resource returning `201` | `services/resource-service/resource_service/main.py` | `tests/unit/test_resource_service.py`, `tests/integration/test_stage6_api.py::test_resource_lifecycle` | Scenario 2 (`docs/evidence/02-resource/`) | COMPLIANT |
| **FR-3** | `matchRequest(requestId)` matches suitable available resource by type and location | `services/matching-service/matching_service/matcher.py`, `services/matching-service/matching_service/engine.py` | `tests/unit/test_matching_logic.py`, `tests/integration/test_stage7_matching.py::test_e2e_nearest_matching` | Scenario 3 (`docs/evidence/03-matching/`) | COMPLIANT |
| **FR-4** | `updateStatus` updates request and resource statuses with transition validation | `services/request-service/request_service/main.py`, `services/resource-service/resource_service/main.py`, `common/ems_common/transitions.py` | `tests/unit/test_transitions.py`, `tests/integration/test_stage6_api.py::test_invalid_status_transitions` | Scenarios 1, 2, 3 (`docs/evidence/01-request/`) | COMPLIANT |
| **FR-5** | `getRequest(requestId)` retrieves request (`200`, `404` when unknown) | `services/request-service/request_service/main.py` | `tests/unit/test_request_service.py`, `tests/integration/test_stage6_api.py::test_not_found` | Scenario 1 (`docs/evidence/01-request/`) | COMPLIANT |
| **FR-6** | All seven public REST endpoints exist with specified HTTP status codes and JSON envelopes | `services/request-service/request_service/main.py`, `services/resource-service/resource_service/main.py`, `common/ems_common/errors.py` | `tests/component/test_request_service.py`, `tests/component/test_resource_service.py`, `tests/integration/test_stage6_api.py` | Scenarios 1, 2, 3 (`docs/evidence/01-request/`, `02-resource/`, `03-matching/`) | COMPLIANT |
| **FR-7** | Data records contain required fields (IDs, types, locations, details, statuses, timestamps) | `common/ems_common/models.py` | `tests/unit/test_models.py`, `tests/integration/test_stage6_api.py` | Scenarios 1, 2 (`docs/evidence/01-request/`, `02-resource/`) | COMPLIANT |
| **FR-8** | Supported request types: water, food, medicine, transportation, shelter | `common/ems_common/models.py` | `tests/unit/test_models.py`, `tests/integration/test_stage6_api.py::test_validation_errors` | Scenarios 1, 2, 3 (`docs/evidence/01-request/`) | COMPLIANT |

---

## 2. Storage & Sharding Requirements (SR-1 to SR-5)

| ID | Requirement Description | Implementation Files | Automated Tests | Demo Scenario & Evidence | Status |
|---|---|---|---|---|---|
| **SR-1** | Single coordinator determines shard and routes storage operations | `services/coordinator/coordinator/router.py`, `services/coordinator/coordinator/registry.py` | `tests/component/test_coordinator_routing.py`, `tests/component/test_coordinator_shards.py` | Scenario 4 (`docs/evidence/04-distribution/`) | COMPLIANT |
| **SR-2** | 3 storage shards with at least one replica each; data spread across all shards | `services/storage-node/storage_node/db.py`, `services/storage-node/storage_node/replication.py` | `tests/fault/test_stage4_replication.py`, `scripts/ops/check_replication.py` | Scenario 4 (`docs/evidence/04-distribution/`) | COMPLIANT |
| **SR-3** | Hash-based deterministic partitioning (`uint32(sha256(key)[:4]) % 3`) | `common/ems_common/sharding.py`, `services/coordinator/coordinator/router.py` | `tests/unit/test_hash.py`, `tests/unit/test_sharding.py` | Scenario 4 (`docs/evidence/04-distribution/`) | COMPLIANT |
| **SR-4** | Client never sees or uses physical storage node addresses | `common/ems_common/storage_client.py` | `tests/integration/test_stage6_api.py`, `tests/integration/test_stage8_stack.py` | Scenarios 1, 4 (`docs/evidence/04-distribution/`) | COMPLIANT |
| **SR-5** | Internal APIs: register, heartbeat, store/update, retrieve, delete data | `services/coordinator/coordinator/main.py`, `services/storage-node/storage_node/main.py` | `tests/component/test_storage_node.py`, `tests/component/test_cluster_harness.py` | Scenarios 4, 6, 7 (`docs/evidence/06-storage-failure/`) | COMPLIANT |

---

## 3. Messaging & Concurrency Requirements (MR-1 to MR-3, TS-1)

| ID | Requirement Description | Implementation Files | Automated Tests | Demo Scenario & Evidence | Status |
|---|---|---|---|---|---|
| **MR-1** | RabbitMQ carries asynchronous communication between request and matching | `common/ems_common/rabbit.py`, `services/request-service/request_service/events.py` | `tests/fault/test_stage7_messaging.py` | Scenario 5 (`docs/evidence/05-async-processing/`) | COMPLIANT |
| **MR-2** | Full sequence: submit $\rightarrow$ store $\rightarrow$ publish $\rightarrow$ consume $\rightarrow$ match $\rightarrow$ reserve $\rightarrow$ update | `services/matching-service/matching_service/worker.py` | `tests/integration/test_stage7_matching.py` | Scenarios 3, 5 (`docs/evidence/03-matching/`, `05-async-processing/`) | COMPLIANT |
| **MR-3** | Matching considers resource type and location | `services/matching-service/matching_service/matcher.py` | `tests/unit/test_matching_logic.py` | Scenario 3 (`docs/evidence/03-matching/`) | COMPLIANT |
| **TS-1** | Concurrency safety: simultaneous requests competing for resources; atomic reservation | `services/resource-service/resource_service/main.py`, `services/matching-service/matching_service/engine.py` | `tests/integration/test_stage7_concurrency.py::test_competing_requests` | Scenarios 5, 9 (`docs/evidence/09-k8s-scaling/`) | COMPLIANT |

---

## 4. Fault Tolerance & Recovery Requirements (FT-1 to FT-5)

| ID | Requirement Description | Implementation Files | Automated Tests | Demo Scenario & Evidence | Status |
|---|---|---|---|---|---|
| **FT-1** | Storage nodes report availability via periodic heartbeats | `services/storage-node/storage_node/main.py`, `services/coordinator/coordinator/health_checker.py` | `tests/component/test_cluster_harness.py` | Scenarios 6, 7 (`docs/evidence/06-storage-failure/`) | COMPLIANT |
| **FT-2** | Detect unavailable nodes; avoid routing traffic to failed nodes | `services/coordinator/coordinator/failover.py`, `services/coordinator/coordinator/router.py` | `tests/fault/test_stage5_failover.py` | Scenario 6 (`docs/evidence/06-storage-failure/`) | COMPLIANT |
| **FT-3** | Use available in-sync replica; continue processing writes degraded | `services/storage-node/storage_node/replication.py`, `services/coordinator/coordinator/shard_state.py` | `tests/fault/test_stage5_failover.py`, `tests/fault/test_stage8_storage_drill.py` | Scenario 6 (`docs/evidence/06-storage-failure/`) | COMPLIANT |
| **FT-4** | Rejoin and delta/snapshot synchronization upon node recovery | `services/storage-node/storage_node/recovery.py`, `services/storage-node/storage_node/db.py` | `tests/component/test_replica_sync.py`, `tests/fault/test_stage5_failover.py` | Scenario 7 (`docs/evidence/07-node-recovery/`) | COMPLIANT |
| **FT-5** | Failure demonstrated by stopping container/pod showing before, during, and after | `scripts/demo/demo_06_storage_failure.sh`, `scripts/demo/demo_07_node_recovery.sh` | `tests/fault/test_stage5_failover.py`, `tests/fault/test_stage8_storage_drill.py` | Scenarios 6, 7 (`docs/evidence/06-storage-failure/`, `07-node-recovery/`) | COMPLIANT |

---

## 5. Deployment & Orchestration Requirements (DK-1, K8-1 to K8-3)

| ID | Requirement Description | Implementation Files | Automated Tests | Demo Scenario & Evidence | Status |
|---|---|---|---|---|---|
| **DK-1** | Dockerfiles for all services, Compose file, container-to-container networking, clean clone up | `docker-compose.yml`, `services/*/Dockerfile` | `scripts/verify/test_fresh_clone.sh`, `tests/integration/test_stage8_stack.py` | `scripts/verify/test_fresh_clone.sh` (`docs/evidence/tests/test_fresh_clone.txt`) | COMPLIANT |
| **K8-1** | Kubernetes Deployments, StatefulSets, Services, pod replicas, inter-service DNS communication | `k8s/*.yaml`, `scripts/k8s/k8s_up.sh` | `tests/unit/test_k8s_manifests.py`, `scripts/k8s/k8s_verify_stage9.py` (Steps 1 & 2) | All K8s Scenarios (`docs/evidence/stage9/`) | COMPLIANT |
| **K8-2** | Microservice scaling, and pod failure with automatic recovery | `k8s/services.yaml`, `scripts/demo/demo_08_k8s_recovery.sh`, `scripts/demo/demo_09_k8s_scaling.sh` | `scripts/k8s/k8s_verify_stage9.py` (Steps 4 & 5) | Scenarios 8, 9 (`docs/evidence/08-k8s-pod-recovery/`, `09-k8s-scaling/`) | COMPLIANT |
| **K8-3** | Local Kubernetes execution (Minikube / kind / Docker Desktop) | `k8s/*.yaml`, `docs/k8s-local.md` | `scripts/k8s/k8s_verify_stage9.py` | Minikube deployment (`docs/k8s-local.md`) | COMPLIANT |

---

## 6. Demonstration, Testing & Justification Requirements (DM-1, TS-1, JU-1)

| ID | Requirement Description | Implementation Files | Automated Tests | Demo Scenario & Evidence | Status |
|---|---|---|---|---|---|
| **DM-1** | 9 Required demonstration scenarios executed with saved evidence | `scripts/demo/demo_01_request.sh` ... `demo_09_k8s_scaling.sh`, `scripts/demo/run_demo.sh` | `scripts/demo/rehearse.sh` | `docs/evidence/01-*` to `docs/evidence/09-*` | COMPLIANT |
| **TS-1** | 4-layer testing suite: unit, component, integration, fault test matrix | `tests/unit/`, `tests/component/`, `tests/integration/`, `tests/fault/` | `scripts/verify/run_all_tests.sh` | `docs/evidence/tests/SUMMARY.md` | COMPLIANT |
| **JU-1** | Full technical justification of all 10 architectural choices | `docs/report/design-report.md` (Section 3) | N/A (Documentation) | `docs/report/design-report.md` | COMPLIANT |
