#!/usr/bin/env bash
set -euo pipefail

MODE="${MODE:-k8s}"
REQ="${REQ:-http://localhost:8001}"
COORD="${COORD:-http://localhost:8000}"

PYTHON_BIN="python3"
if [[ -x ".venv/bin/python" ]]; then
    PYTHON_BIN=".venv/bin/python"
fi

EVIDENCE_DIR="docs/evidence/08-k8s-pod-recovery"
mkdir -p "$EVIDENCE_DIR"

if [[ "$MODE" != "k8s" ]]; then
    echo "=== Scenario 8: Kubernetes Pod Recovery (SKIPPED in $MODE mode) ==="
    echo "SKIPPED: Scenario 8 requires Kubernetes mode." > "$EVIDENCE_DIR/summary.txt"
    exit 0
fi

echo "=== Scenario 8: Kubernetes Pod Recovery ($MODE mode) ==="

# 1. Create a request to verify data persistence across pod restart
echo "Creating test request..."
CREATE_RES=$(curl -s -X POST "$REQ/requests" \
    -H "Content-Type: application/json" \
    -d '{"type":"medicine","location":{"lat":12.9716,"lon":77.5946},"details":"Pod recovery validation request"}')
echo "$CREATE_RES" | tee "$EVIDENCE_DIR/request_created.json"

REQ_ID=$("$PYTHON_BIN" -c "import json, sys; print(json.loads('''$CREATE_RES''').get('requestId', ''))")
if [[ -z "$REQ_ID" ]]; then
    echo "FAIL: Could not create test request"
    exit 1
fi
echo "Test request created: $REQ_ID"

# 2. Capture pod state before deletion
echo "Capturing pod listing before deletion..."
kubectl -n ems get pods -l app=request-service -o wide | tee "$EVIDENCE_DIR/pods_before.txt"
OLD_POD=$(kubectl -n ems get pods -l app=request-service -o jsonpath='{.items[0].metadata.name}')
echo "Active request-service pod: $OLD_POD"

# 3. Background querying loop
echo "Starting querying loop during pod deletion..."
QUERY_LOG="$EVIDENCE_DIR/query_loop.log"
: > "$QUERY_LOG"

STOP_QUERY=0
run_queries() {
    while [[ "$STOP_QUERY" -eq 0 ]]; do
        STATUS=$(curl -s -o /dev/null -w "%{http_code}" --connect-timeout 1 "$REQ/requests/$REQ_ID" 2>/dev/null || echo "000")
        echo "$(date '+%Y-%m-%d %H:%M:%S') GET /requests/$REQ_ID -> HTTP $STATUS" >> "$QUERY_LOG"
        sleep 0.3
    done
}

run_queries &
QUERY_PID=$!

# 4. Delete the pod
echo "Deleting pod $OLD_POD (simulating crash / failure)..."
kubectl -n ems delete pod "$OLD_POD" --wait=false

# 5. Wait for replacement pod to be Ready
echo "Waiting for replacement request-service pod to become Ready..."
RECOVERED=0
for i in $(seq 1 45); do
    sleep 1
    POD_STATUS=$(kubectl -n ems get pods -l app=request-service -o jsonpath='{range .items[*]}{.metadata.name}:{.status.phase}:{.status.containerStatuses[*].ready}{"\n"}{end}' 2>/dev/null || echo "")
    while IFS= read -r line; do
        if [[ -n "$line" ]]; then
            pname=$(echo "$line" | cut -d: -f1)
            phase=$(echo "$line" | cut -d: -f2)
            ready=$(echo "$line" | cut -d: -f3)
            if [[ "$pname" != "$OLD_POD" && "$phase" == "Running" && "$ready" == "true" ]]; then
                RECOVERED=1
                echo "New pod $pname is Running and Ready!"
                break 2
            fi
        fi
    done <<< "$POD_STATUS"
done

if [[ "$RECOVERED" -ne 1 ]]; then
    echo "FAIL: Replacement request-service pod not ready within 45s"
    exit 1
fi

# Stop background queries
STOP_QUERY=1
kill "$QUERY_PID" 2>/dev/null || true
wait "$QUERY_PID" 2>/dev/null || true

# Refresh port-forwards in case connection was severed by pod termination
bash scripts/k8s_ports.sh >/dev/null 2>&1 || true
sleep 1

# 6. Capture pod state after recovery
echo "Capturing pod listing after recovery..."
kubectl -n ems get pods -l app=request-service -o wide | tee "$EVIDENCE_DIR/pods_after.txt"
NEW_POD=$(kubectl -n ems get pods -l app=request-service -o jsonpath='{.items[0].metadata.name}')
echo "New request-service pod: $NEW_POD"

if [[ "$OLD_POD" == "$NEW_POD" ]]; then
    echo "Warning: Pod name did not change, pod may have restarted in place"
fi

# 7. Verify zero data lost
echo "Verifying request $REQ_ID persists and is accessible..."
FETCH_RES=$(curl -s "$REQ/requests/$REQ_ID")
echo "$FETCH_RES" | tee "$EVIDENCE_DIR/request_after_recovery.json"

VERIFY=$("$PYTHON_BIN" - <<EOF
import json, sys
data = json.loads('''$FETCH_RES''')
assert data.get("requestId") == "$REQ_ID", "requestId mismatch"
assert data.get("type") == "medicine", "type mismatch"
print("Data integrity verified: request retrieved with complete fields.")
EOF
)
echo "$VERIFY"

echo "PASS: Scenario 8 - Kubernetes pod self-healing and zero data loss verified."
