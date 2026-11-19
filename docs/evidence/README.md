# Demonstration & Verification Evidence Directory

This directory stores all captured artifacts, logs, outputs, and diagnostic states across the test suite and demonstration scenarios.

## Evidence Directory Index

| Directory | Scenario / Test Area | Key Evidence Files |
|---|---|---|
| [`01-request-create-retrieve/`](01-request-create-retrieve/) | Scenario 1: Request Creation & Retrieval | `post_response.json`, `get_response.json` |
| [`02-resource-registration/`](02-resource-registration/) | Scenario 2: Resource Registration & Query | `post_response.json`, `get_response.json` |
| [`03-resource-matching/`](03-resource-matching/) | Scenario 3: Resource Matching & Tie-Break | `matched_request.json`, `matching_service.log` |
| [`04-data-distribution/`](04-data-distribution/) | Scenario 4: SHA-256 Shard Distribution | `seed_output.txt`, `diagnostics_shards.json`, `route1.json` |
| [`05-async-processing/`](05-async-processing/) | Scenario 5: Asynchronous Queue Draining | `load_output.txt`, `queue_depth_samples.txt`, `matching_worker.log` |
| [`06-storage-failure/`](06-storage-failure/) | Scenario 6: Storage Node Failure & Failover | `watch_cluster_before.txt`, `watch_cluster_during.txt`, `coordinator_failover.log` |
| [`07-node-recovery/`](07-node-recovery/) | Scenario 7: Storage Node Recovery & Convergence | `watch_cluster_after.txt`, `catchup_node.log`, `diagnostics_after.json` |
| [`08-k8s-pod-recovery/`](08-k8s-pod-recovery/) | Scenario 8: Kubernetes Pod Self-Healing | `pods_before.txt`, `query_loop.log`, `pods_after.txt`, `request_after_recovery.json` |
| [`09-k8s-scaling/`](09-k8s-scaling/) | Scenario 9: Kubernetes Microservice Scaling | `pods_scaled.txt`, `load_output.txt`, `scaled_worker_logs.txt` |
| [`rehearsal-1/`](rehearsal-1/) | Full Rehearsal Run 1 (100% PASS) | Complete snapshot of 01-09 scenario evidence |
| [`rehearsal-2/`](rehearsal-2/) | Full Rehearsal Run 2 (100% PASS) | Complete snapshot of 01-09 scenario evidence |
| [`tests/`](tests/) | Comprehensive Automated Test Suite | `make_lint.txt`, `make_test.txt`, `make_itest.txt`, `make_ftest.txt`, `SUMMARY.md` |
