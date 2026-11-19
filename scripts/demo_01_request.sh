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

EVIDENCE_DIR="docs/evidence/01-request-create-retrieve"
mkdir -p "$EVIDENCE_DIR"

echo "=== Scenario 1: Request Creation and Retrieval ($MODE mode) ==="

# 1. Create request
PAYLOAD='{"type":"water","location":{"lat":12.9716,"lon":77.5946},"details":"Flood, 5 people"}'
echo "Command: curl -s -w '\n%{http_code}' -X POST $REQ/requests -H 'Content-Type: application/json' -d '$PAYLOAD'" > "$EVIDENCE_DIR/commands.txt"

RESP=$(curl -s -w "\n%{http_code}" -X POST "$REQ/requests" -H "Content-Type: application/json" -d "$PAYLOAD")
HTTP_CODE=$(echo "$RESP" | tail -n1)
BODY=$(echo "$RESP" | sed '$d')

echo "POST Response ($HTTP_CODE):" | tee -a "$EVIDENCE_DIR/commands.txt"
echo "$BODY" | tee "$EVIDENCE_DIR/post_response.json"

if [[ "$HTTP_CODE" -ne 201 ]]; then
    echo "FAIL: Expected HTTP 201, got $HTTP_CODE"
    exit 1
fi

REQ_ID=$("$PYTHON_BIN" -c "import sys, json; print(json.loads(sys.stdin.read())['requestId'])" <<< "$BODY")
echo "Created requestId: $REQ_ID"

# 2. Retrieve request
echo -e "\nCommand: curl -s -w '\n%{http_code}' $REQ/requests/$REQ_ID" >> "$EVIDENCE_DIR/commands.txt"
GET_RESP=$(curl -s -w "\n%{http_code}" "$REQ/requests/$REQ_ID")
GET_CODE=$(echo "$GET_RESP" | tail -n1)
GET_BODY=$(echo "$GET_RESP" | sed '$d')

echo "GET Response ($GET_CODE):" | tee -a "$EVIDENCE_DIR/commands.txt"
echo "$GET_BODY" | tee "$EVIDENCE_DIR/get_response.json"

if [[ "$GET_CODE" -ne 200 ]]; then
    echo "FAIL: Expected HTTP 200, got $GET_CODE"
    exit 1
fi

# 3. Validate required fields
"$PYTHON_BIN" - <<EOF
import json, sys
data = json.loads('''$GET_BODY''')
required = ["requestId", "type", "location", "details", "status", "createdAt"]
for field in required:
    assert field in data, f"Missing required field {field}"
assert data["type"] == "water"
assert data["status"] in ("PENDING", "MATCHED")
assert data["location"]["lat"] == 12.9716 and data["location"]["lon"] == 77.5946
print("Validation OK: All required fields present and correct.")
EOF

# 4. Save relevant logs
if [[ "$MODE" == "k8s" ]]; then
    kubectl -n ems logs -l app=request-service --tail=25 > "$EVIDENCE_DIR/request_service.log" || true
else
    docker compose logs request-service --tail=25 > "$EVIDENCE_DIR/request_service.log" || true
fi

echo "PASS: Scenario 1 - Request creation and retrieval verified."
