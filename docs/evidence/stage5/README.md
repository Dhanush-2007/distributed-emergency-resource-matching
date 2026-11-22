# Stage 5 Failure Injection & Failover Evidence Catalog

This directory contains real timestamped evidence artifacts collected from the 7-container Docker Compose cluster failure drills (`tests/fault/test_stage5_failover.py`) and the 60-second live load drill (`scripts/ops/run_live_drill.sh`).

## Summary of Results

- **Unit & Component Tests**: 162/162 passed (`make test`)
- **Integration Tests**: 8/8 passed (`make itest`)
- **Fault-Injection Drills**: 9/9 passed (`make ftest`)
- **Storage Persistence Drill**: PASS (`scripts/verify/test_storage_persistence.sh`)
- **Live Failover Drill**: 528 acknowledged keys under 10 req/s load with 0 lost keys (`LOST == 0`, `epoch 1 -> 2`).

---

## Scenario Catalog

| Directory | Scenario Description | Key Assertions & Evidence Captured |
|:---|:---|:---|
| [`scenario_a/`](./scenario_a/) | Primary failure under load | `ems-s1-a` stopped; automated failover promoted `s1-b` to primary with `epoch = 2`; writes resumed on `s1-b`. |
| [`scenario_b/`](./scenario_b/) | Primary recovery & catchup | `ems-s1-a` restarted; demoted to `REPLICA`; caught up via sync to match `s1-b` `lastSeq`; shard returned to `HEALTHY`. |
| [`scenario_c/`](./scenario_c/) | Replica failure & convergence | Replica stopped; primary acknowledged un-replicated writes degraded; replica restarted and caught up; shard returned to `HEALTHY`. |
| [`scenario_d/`](./scenario_d/) | Stale replica never promoted | Replica stopped; degraded writes advanced primary sequence; primary stopped and stale replica restarted; coordinator returned `503 STORAGE_UNAVAILABLE` and NEVER promoted stale replica; primary restarted and restored shard. |
| [`scenario_e/`](./scenario_e/) | Dual node failure | Both nodes of shard 2 stopped; shard 2 returned `503 Service Unavailable` with `Retry-After: 3`; `/internal/scan?allowPartial=true` returned partial data with `unavailableShards=[2]`; full scan returned 503; node restart restored shard. |
| [`scenario_f/`](./scenario_f/) | Coordinator restart during failover | Coordinator restarted after promotion; persisted state file reloaded with `epoch = 2`; state reconciled without regressing epoch. |
| [`scenario_g/`](./scenario_g/) | Old primary fencing | Old primary returned; direct write attempt with old `epoch = 1` rejected with `409 STALE_EPOCH`. |
| [`scenario_h/`](./scenario_h/) | Epoch monotonicity | Continuous polling every 0.5s throughout failover; verified epoch strictly non-decreasing across all transitions. |
| [`scenario_i/`](./scenario_i/) | Double failure cycle | Failed A -> B promoted to epoch 2; recovered A; failed B -> A promoted to epoch 3; verified all writes from both cycles readable without data loss. |
| [`scenario_j/`](./scenario_j/) | Coordinator restart while degraded | Replica stopped and write degraded; coordinator restarted; recovered in `DEGRADED` state without false-healing; replica restarted and converged to `HEALTHY`. |

---

## Live Failover Drill (`live_drill_output.txt`)

Executed with continuous load:
- Command: `.venv/bin/python scripts/ops/load_loop.py --rate 10 --duration 60 --shard 1`
- At 15s: `docker stop ems-s1-a` (injected primary failure)
- At 35s: `docker start ems-s1-a` (restored node as follower)
- Final Summary:
  - **Total Acknowledged Keys**: 528
  - **Verified Readable**: 528
  - **Total 503 Retries**: 55
  - **Lost Keys**: 0
  - **Shard 1 Transition**: `epoch 1 -> 2`, primary `s1-a -> s1-b`, recovered to `HEALTHY` with both nodes synchronized at `lastSeq = 528`.
