#!/usr/bin/env bash
set -euo pipefail

MODE="${MODE:-k8s}"
REQ="${REQ:-http://localhost:8001}"
RES="${RES:-http://localhost:8002}"
COORD="${COORD:-http://localhost:8000}"

PYTHON_BIN="python3"
if [[ -x ".venv/bin/python" ]]; then
    PYTHON_BIN=".venv/bin/python"
fi

EVIDENCE_DIR="docs/evidence/03-resource-matching"
mkdir -p "$EVIDENCE_DIR"

echo "=== Scenario 3: Resource Matching ($MODE mode) ==="
TAG=$("$PYTHON_BIN" -c "import uuid; print(uuid.uuid4().hex[:6])")

# 1. Register 3 resources: 1 food (very close ~0.02km), 1 water (near ~0.10km), 1 water (far ~5.0km)
echo "Registering resources..."
RES_FOOD=$(curl -s -X POST "$RES/resources" -H "Content-Type: application/json" -d "{\"name\":\"Food-Close-$TAG\",\"type\":\"food\",\"location\":{\"lat\":12.9718,\"lon\":77.5946},\"availability\":true}")
FOOD_ID=$("$PYTHON_BIN" -c "import sys, json; print(json.loads(sys.stdin.read())['resourceId'])" <<< "$RES_FOOD")

RES_WATER_NEAR=$(curl -s -X POST "$RES/resources" -H "Content-Type: application/json" -d "{\"name\":\"Water-Near-$TAG\",\"type\":\"water\",\"location\":{\"lat\":12.9725,\"lon\":77.5946},\"availability\":true}")
NEAR_WATER_ID=$("$PYTHON_BIN" -c "import sys, json; print(json.loads(sys.stdin.read())['resourceId'])" <<< "$RES_WATER_NEAR")

RES_WATER_FAR=$(curl -s -X POST "$RES/resources" -H "Content-Type: application/json" -d "{\"name\":\"Water-Far-$TAG\",\"type\":\"water\",\"location\":{\"lat\":13.0170,\"lon\":77.5946},\"availability\":true}")
FAR_WATER_ID=$("$PYTHON_BIN" -c "import sys, json; print(json.loads(sys.stdin.read())['resourceId'])" <<< "$RES_WATER_FAR")

echo "Created resources:"
echo "  Food (close, ~0.02km):      $FOOD_ID"
echo "  Water (near, ~0.10km):      $NEAR_WATER_ID"
echo "  Water (far, ~5.04km):       $FAR_WATER_ID"

# 2. Submit water request at (12.9716, 77.5946)
REQ_RESP=$(curl -s -X POST "$REQ/requests" -H "Content-Type: application/json" -d "{\"type\":\"water\",\"location\":{\"lat\":12.9716,\"lon\":77.5946},\"details\":\"Water match test $TAG\"}")
REQ_ID=$("$PYTHON_BIN" -c "import sys, json; print(json.loads(sys.stdin.read())['requestId'])" <<< "$REQ_RESP")
echo "Submitted water request: $REQ_ID"

# 3. Poll for MATCHED status (trigger POST /match if not matched yet)
MATCHED=0
for i in $(seq 1 20); do
    sleep 1
    STATUS_RESP=$(curl -s "$REQ/requests/$REQ_ID")
    STATUS=$("$PYTHON_BIN" -c "import sys, json; print(json.loads(sys.stdin.read()).get('status'))" <<< "$STATUS_RESP" 2>/dev/null || echo "")
    if [[ "$STATUS" == "MATCHED" ]]; then
        MATCHED=1
        break
    fi
done

if [[ "$MATCHED" -ne 1 ]]; then
    echo "Request not matched automatically; invoking on-demand POST $REQ/match/$REQ_ID..."
    curl -s -X POST "$REQ/match/$REQ_ID" >/dev/null || true
    sleep 2
    STATUS_RESP=$(curl -s "$REQ/requests/$REQ_ID")
    STATUS=$("$PYTHON_BIN" -c "import sys, json; print(json.loads(sys.stdin.read()).get('status'))" <<< "$STATUS_RESP" 2>/dev/null || echo "")
fi

echo "$STATUS_RESP" | tee "$EVIDENCE_DIR/matched_request.json"

# 4. Verify request matched nearest water resource
ASSIGNED_RES_ID=$("$PYTHON_BIN" -c "import sys, json; print(json.loads(sys.stdin.read())['matchedResourceId'])" <<< "$STATUS_RESP")
echo "Assigned resourceId: $ASSIGNED_RES_ID"

if [[ "$ASSIGNED_RES_ID" != "$NEAR_WATER_ID" ]]; then
    echo "FAIL: Expected nearest water resource $NEAR_WATER_ID, but got $ASSIGNED_RES_ID"
    exit 1
fi
echo "Verified: Nearest water resource ($NEAR_WATER_ID) selected over food ($FOOD_ID) and far water ($FAR_WATER_ID)."

# 5. Verify resource status is ASSIGNED with assignedRequestId == REQ_ID
RES_VERIFY=$(curl -s "$RES/resources/$NEAR_WATER_ID")
echo "$RES_VERIFY" | tee "$EVIDENCE_DIR/assigned_resource.json"

"$PYTHON_BIN" - <<EOF
import json, sys
data = json.loads('''$RES_VERIFY''')
assert data["status"] == "ASSIGNED", f"Expected ASSIGNED, got {data['status']}"
assert data["availability"] is False, "Expected availability false"
assert data["assignedRequestId"] == "$REQ_ID", f"Expected assignedRequestId $REQ_ID, got {data.get('assignedRequestId')}"
print("Verified: Resource is ASSIGNED with assignedRequestId matching request.")
EOF

# 6. Capture matcher logs and candidate table
if [[ "$MODE" == "k8s" ]]; then
    kubectl -n ems logs -l app=matching-service --tail=60 > "$EVIDENCE_DIR/matching_service.log" || true
else
    docker compose logs matching-service --tail=60 > "$EVIDENCE_DIR/matching_service.log" || true
fi

echo "PASS: Scenario 3 - Nearest water resource matching verified."
