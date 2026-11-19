#!/usr/bin/env bash
set -euo pipefail

MODE="${MODE:-k8s}"
REQ="${REQ:-http://localhost:8001}"
RES="${RES:-http://localhost:8002}"
RABBIT="${RABBIT:-http://localhost:15672}"

PYTHON_BIN="python3"
if [[ -x ".venv/bin/python" ]]; then
    PYTHON_BIN=".venv/bin/python"
fi

EVIDENCE_DIR="docs/evidence/09-k8s-scaling"
mkdir -p "$EVIDENCE_DIR"

if [[ "$MODE" != "k8s" ]]; then
    echo "=== Scenario 9: Kubernetes Microservice Scaling (SKIPPED in $MODE mode) ==="
    echo "SKIPPED: Scenario 9 requires Kubernetes mode." > "$EVIDENCE_DIR/summary.txt"
    exit 0
fi

echo "=== Scenario 9: Kubernetes Microservice Scaling ($MODE mode) ==="

# Trap to guarantee scaled back to 1 even on failure
cleanup() {
    echo "Scaling matching-service back to 1 replica..."
    kubectl -n ems scale deployment/matching-service --replicas=1 >/dev/null 2>&1 || true
}
trap cleanup EXIT

# 1. Scale matching-service to 3 replicas
echo "Scaling deployment/matching-service to 3 replicas..."
kubectl -n ems scale deployment/matching-service --replicas=3

echo "Waiting for 3 matching-service pods to be Ready..."
kubectl -n ems wait --for=condition=Ready pod -l app=matching-service --timeout=60s

# 2. Capture pod status
echo "Capturing 3 ready matching pods..."
kubectl -n ems get pods -l app=matching-service -o wide | tee "$EVIDENCE_DIR/pods_scaled.txt"

READY_COUNT=$(kubectl -n ems get deployment matching-service -o jsonpath='{.status.readyReplicas}')
echo "Ready replicas: $READY_COUNT"
if [[ "$READY_COUNT" -ne 3 ]]; then
    echo "FAIL: Expected 3 ready replicas, got $READY_COUNT"
    exit 1
fi

# 3. Submit a burst load of 30 requests
echo "Submitting 30-request burst to exercise concurrent workers..."
REQ_URL="$REQ" "$PYTHON_BIN" scripts/app_load.py --rate 15 --duration 2 | tee "$EVIDENCE_DIR/load_output.txt"

# 4. Wait for processing / queue to drain
echo "Waiting for queue to drain..."
for i in $(seq 1 30); do
    MESSAGES=$(curl -s -u ems:ems-dev-password "$RABBIT/api/queues/%2F/request.match" 2>/dev/null | "$PYTHON_BIN" -c "import sys, json; print(json.load(sys.stdin).get('messages', 0))" 2>/dev/null || echo "0")
    if [[ "$MESSAGES" == "0" ]]; then
        echo "Queue drained to 0 after $i checks."
        break
    fi
    sleep 0.5
done

# 5. Capture worker logs from all 3 pods
echo "Capturing worker logs from all pods..."
kubectl -n ems logs -l app=matching-service --tail=150 --prefix=true | tee "$EVIDENCE_DIR/scaled_worker_logs.txt"

# 6. Verify no double assignments
echo "Verifying no double assignments..."
"$PYTHON_BIN" - <<EOF
import json, urllib.request

req = urllib.request.urlopen("$RES/resources")
data = json.loads(req.read().decode())
resources = data if isinstance(data, list) else data.get("items", [])
assigned_reqs = set()
for r in resources:
    assigned_to = r.get("assignedRequestId")
    if assigned_to:
        assert assigned_to not in assigned_reqs, f"DOUBLE ASSIGNMENT DETECTED: {assigned_to}"
        assigned_reqs.add(assigned_to)
print(f"Verified: {len(assigned_reqs)} assigned resources checked, zero double assignments across 3 concurrent workers.")
EOF

echo "PASS: Scenario 9 - Kubernetes microservice scaling and concurrency verified."
