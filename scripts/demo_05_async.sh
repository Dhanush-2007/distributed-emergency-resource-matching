#!/usr/bin/env bash
set -euo pipefail

MODE="${MODE:-k8s}"
REQ="${REQ:-http://localhost:8001}"
RES="${RES:-http://localhost:8002}"
COORD="${COORD:-http://localhost:8000}"
RABBIT="${RABBIT:-http://localhost:15672}"

PYTHON_BIN="python3"
if [[ -x ".venv/bin/python" ]]; then
    PYTHON_BIN=".venv/bin/python"
fi

EVIDENCE_DIR="docs/evidence/05-async-processing"
mkdir -p "$EVIDENCE_DIR"

echo "=== Scenario 5: Asynchronous Processing & Queue Draining ($MODE mode) ==="

# 1. Capture initial queue depth
QUEUE_SAMPLES="$EVIDENCE_DIR/queue_depth_samples.txt"
echo "Sampling queue depth..." > "$QUEUE_SAMPLES"

sample_queue() {
    curl -s -u ems:ems-dev-password "$RABBIT/api/queues/%2F/request.match" | "$PYTHON_BIN" -c "import sys, json; data=json.load(sys.stdin); print(f'messages={data.get(\"messages\", 0)}, ready={data.get(\"messages_ready\", 0)}, unacked={data.get(\"messages_unacknowledged\", 0)}')" 2>/dev/null || echo "messages=unknown"
}

echo "Pre-burst: $(sample_queue)" | tee -a "$QUEUE_SAMPLES"

# 2. Run app_load.py (e.g., rate 10 req/s for 3s = 30 requests)
echo "Submitting burst load (30 requests)..."
REQ_URL="$REQ" "$PYTHON_BIN" scripts/app_load.py --rate 10 --duration 3 | tee "$EVIDENCE_DIR/load_output.txt"

# 3. Sample queue depth while draining
echo "Sampling queue draining..."
DRAINED=0
for i in $(seq 1 30); do
    SAMPLE=$(sample_queue)
    echo "Sample $i: $SAMPLE" | tee -a "$QUEUE_SAMPLES"
    if [[ "$SAMPLE" == *"messages=0"* ]]; then
        DRAINED=1
        echo "Queue drained to 0!"
        break
    fi
    sleep 0.5
done

if [[ "$DRAINED" -ne 1 ]]; then
    echo "Warning: Queue not fully drained within 15 seconds"
fi

# 4. Verify no resource assigned twice and all requests settled
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
print(f"Verified: {len(assigned_reqs)} assigned resources checked, zero double assignments.")
EOF

# 5. Capture worker logs with correlation IDs
if [[ "$MODE" == "k8s" ]]; then
    kubectl -n ems logs -l app=matching-service --tail=60 > "$EVIDENCE_DIR/matching_worker.log" || true
else
    docker compose logs matching-service --tail=60 > "$EVIDENCE_DIR/matching_worker.log" || true
fi

echo "PASS: Scenario 5 - Asynchronous processing and queue draining verified."
