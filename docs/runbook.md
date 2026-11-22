# Demonstration Runbook & Operational Procedures

This runbook documents the step-by-step procedures, commands, and expected evidence for all nine required demonstration scenarios defined in **SPEC 14**.

---

## 1. Preparation Checklist (SPEC 14.1)

Before commencing a live demonstration or examination:

- [ ] **Docker & Minikube VM Ready**: Minikube running with at least 4 GB RAM and 2 CPUs (`minikube status`).
- [ ] **Images Built and Loaded**:
  ```bash
  make images
  for img in storage-node coordinator request-service resource-service matching-service; do
      minikube image load ems/$img:1.0
  done
  ```
- [ ] **Stack Deployment & Health**:
  ```bash
  ./scripts/k8s/k8s_up.sh
  ./scripts/ops/k8s_ports.sh
  ```
- [ ] **Clean Seed State**:
  ```bash
  ./scripts/ops/reset.sh k8s
  ```
- [ ] **Environment Variables Exported**:
  ```bash
  export REQ=http://localhost:8001
  export RES=http://localhost:8002
  export COORD=http://localhost:8000
  export RABBIT=http://localhost:15672
  ```
- [ ] **Monitoring Windows Open**:
  - Terminal 1: `./.venv/bin/python scripts/ops/watch_cluster.py` (live cluster status table)
  - Terminal 2: `kubectl -n ems get pods -w`
  - Terminal 3: Demonstration command execution terminal
  - Browser: RabbitMQ UI at `http://localhost:15672` (guest/guest or ems/ems-dev-password)

---

## 2. Demonstration Scenarios (SPEC 14.2)

### Scenario 1: Request Creation and Retrieval
**Goal**: Verify standard input validation, UUID generation, PENDING state, and read-after-write consistency.

**Commands**:
```bash
# 1. Create a request
REQ_RESP=$(curl -s -X POST "$REQ/requests" \
  -H "Content-Type: application/json" \
  -d '{"type":"water","location":{"lat":12.9716,"lon":77.5946},"details":"Flood crisis, 5 people stranded"}')
echo "$REQ_RESP" | jq .

REQ_ID=$(echo "$REQ_RESP" | jq -r .requestId)

# 2. Retrieve the request
curl -s "$REQ/requests/$REQ_ID" | jq .
```
**Expected Output**:
- HTTP `201 Created` on creation, HTTP `200 OK` on query.
- Payload includes `requestId`, `type: "water"`, `location`, `details`, `status: "PENDING"`, and ISO timestamp `createdAt`.

---

### Scenario 2: Resource Registration and Query
**Goal**: Register resources with geolocation, verify listing and type-based filtering.

**Commands**:
```bash
# 1. Register a water resource
RES_RESP=$(curl -s -X POST "$RES/resources" \
  -H "Content-Type: application/json" \
  -d '{"name":"Water Tanker Alpha","type":"water","location":{"lat":12.9750,"lon":77.5950},"availability":true}')
echo "$RES_RESP" | jq .

# 2. Filter resources by type
curl -s "$RES/resources?type=water" | jq .
```
**Expected Output**:
- HTTP `201 Created`, returning `resourceId`, `status: "AVAILABLE"`, `availability: true`.
- Query returns an array containing all water resources with location coordinates.

---

### Scenario 3: Resource Matching (Haversine & Tie-breaking)
**Goal**: Verify nearest eligible resource selection based on distance, ignoring ineligible types or unavailable resources.

**Commands**:
```bash
# Register a closer food resource (100m) and a farther water resource (1500m)
curl -s -X POST "$RES/resources" -H "Content-Type: application/json" \
  -d '{"name":"Food Depot","type":"food","location":{"lat":12.9720,"lon":77.5946},"availability":true}'

curl -s -X POST "$RES/resources" -H "Content-Type: application/json" \
  -d '{"name":"Water Far","type":"water","location":{"lat":12.9850,"lon":77.5946},"availability":true}'

# Create a water request
REQ_RESP=$(curl -s -X POST "$REQ/requests" -H "Content-Type: application/json" \
  -d '{"type":"water","location":{"lat":12.9716,"lon":77.5946},"details":"Nearest water test"}')
REQ_ID=$(echo "$REQ_RESP" | jq -r .requestId)

# Wait 2 seconds for worker, or trigger manually
sleep 2
curl -s "$REQ/requests/$REQ_ID" | jq .
```
**Expected Output**:
- Request transitions to `MATCHED` with `matchedResourceId` set to the nearest *water* resource (Water Tanker Alpha, ~380m), NOT the food depot (100m) and NOT the distant water resource (1500m).
- Matching service logs display candidate distance evaluation table.

---

### Scenario 4: Data Distribution across Three Shards
**Goal**: Demonstrate deterministic SHA-256 sharding with zero client awareness of shard topology.

**Commands**:
```bash
# Seed records across the cluster
python3 scripts/ops/seed.py --requests 30 --resources 30

# Inspect shard distribution
curl -s "$COORD/diagnostics/shards" | jq .

# Inspect deterministic routing for a key
curl -s "$COORD/diagnostics/route?key=request:$REQ_ID" | jq .
```
**Expected Output**:
- Diagnostic response shows all three shards (0, 1, 2) contain records $> 0$.
- Diagnostics route endpoint returns exact `shardId`, `primary`, and `replica` without client direct access.

---

### Scenario 5: Asynchronous Processing & Queue Draining
**Goal**: Confirm RabbitMQ queue handles bursts without request loss or duplicate assignment.

**Commands**:
```bash
# Submit burst load of 30 requests
python3 scripts/ops/app_load.py --rate 10 --duration 3

# Monitor queue depth
curl -s -u ems:ems-dev-password "$RABBIT/api/queues/%2F/request.match" | jq '{messages, messages_ready, messages_unacknowledged}'
```
**Expected Output**:
- RabbitMQ queue depth rises during the burst, then drains cleanly to 0.
- All 30 requests reach terminal status `MATCHED` or `NO_MATCH`. No resource is assigned more than once.

---

### Scenario 6: Storage Node Failure & Failover
**Goal**: Demonstrate heartbeat failure detection (~6s), replica promotion, epoch fencing, and degraded writes.

**Commands**:
```bash
# Find a key that maps to shard 1
KEY=$(python3 scripts/ops/find_key_for_shard.py 1)
echo "Key on Shard 1: $KEY"

# Before: Check cluster state
python3 scripts/ops/watch_cluster.py --once

# Fail: Scale primary StatefulSet to 0
kubectl -n ems scale sts/ems-s1-a --replicas=0

# Monitor failover in watch_cluster.py (takes ~6s)
# During failover, query the key
curl -s "$REQ/requests/${KEY#request:}" | jq .
```
**Expected Output**:
- Coordinator misses 3 heartbeats, marks `s1-a` `DOWN`.
- Shard 1 promotes `s1-b` to `PRIMARY`, increments epoch to 2, and enters `DEGRADED` state.
- Reads and writes to Shard 1 remain available via promoted node `s1-b`.

---

### Scenario 7: Storage Node Recovery & Convergence
**Goal**: Demonstrate restarted node follower rejoin, oplog delta synchronization, and shard health restoration.

**Commands**:
```bash
# Recover: Scale primary StatefulSet back to 1
kubectl -n ems scale sts/ems-s1-a --replicas=1

# Monitor rejoin in watch_cluster.py
# s1-a starts as follower, catches up missing logs, marks READY
```
**Expected Output**:
- `s1-a` queries `/internal/sync`, replays missing oplog records, and achieves `lastSeq == primary.lastSeq`.
- Shard 1 clears `DEGRADED` status and returns to `HEALTHY`.

---

### Scenario 8: Kubernetes Pod Self-Healing
**Goal**: Verify Deployment controller replaces dead application pods automatically without data loss.

**Commands**:
```bash
# Before: List request-service pods
kubectl -n ems get pods -l app=request-service -o wide

# Delete the pod
kubectl -n ems delete pod -l app=request-service --wait=false

# Keep querying an existing request
curl -s "$REQ/requests/$REQ_ID" | jq .

# After: Verify new pod is Ready
kubectl -n ems wait --for=condition=Ready pod -l app=request-service --timeout=60s
kubectl -n ems get pods -l app=request-service -o wide
```
**Expected Output**:
- New pod created within seconds; brief connection blip or instant routing via Service endpoint.
- Existing request remains fully readable and consistent after pod replacement.

---

### Scenario 9: Kubernetes Microservice Scaling
**Goal**: Verify horizontal scaling of consumers under high load without race conditions.

**Commands**:
```bash
# Scale matching-service to 3 replicas
kubectl -n ems scale deployment/matching-service --replicas=3
kubectl -n ems wait --for=condition=Ready pod -l app=matching-service --timeout=60s

# Submit burst load
python3 scripts/ops/app_load.py --rate 15 --duration 2

# Inspect logs across all three worker pods
kubectl -n ems logs -l app=matching-service --tail=50 --prefix=true

# Scale back to 1
kubectl -n ems scale deployment/matching-service --replicas=1
```
**Expected Output**:
- 3 Ready matching pods process the queue concurrently.
- All requests are processed with zero duplicate resource assignments.

---

## 3. Reset Procedures (SPEC 14.3)

To ensure repeatable demonstrations, run the reset script between demonstration passes:

### In Kubernetes Mode:
```bash
./scripts/ops/reset.sh k8s
```
This tears down namespace PVCs, redeploys clean pods, purges RabbitMQ queues, restores port-forwards, and seeds fixed test fixtures.

### In Docker Compose Mode:
```bash
./scripts/ops/reset.sh compose
```
This runs `docker compose down -v`, brings up a fresh stack with `--wait`, purges queues, and seeds baseline data.

### Automated Run & Rehearsal
To run all 9 scenarios sequentially with summary reporting:
```bash
./scripts/demo/run_demo.sh k8s
```
To run the full two-pass rehearsal verification (SPEC 16.1):
```bash
./scripts/demo/rehearse.sh k8s
```
