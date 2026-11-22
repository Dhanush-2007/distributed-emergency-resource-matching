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

EVIDENCE_DIR="docs/evidence/02-resource-registration"
mkdir -p "$EVIDENCE_DIR"

echo "=== Scenario 2: Resource Registration ($MODE mode) ==="

# 1. Register resource
PAYLOAD='{"name":"Truck 1","type":"water","location":{"lat":12.9750,"lon":77.5946},"availability":true}'
echo "Command: curl -s -w '\n%{http_code}' -X POST $RES/resources -H 'Content-Type: application/json' -d '$PAYLOAD'" > "$EVIDENCE_DIR/commands.txt"

RESP=$(curl -s -w "\n%{http_code}" -X POST "$RES/resources" -H "Content-Type: application/json" -d "$PAYLOAD")
HTTP_CODE=$(echo "$RESP" | tail -n1)
BODY=$(echo "$RESP" | sed '$d')

echo "POST Response ($HTTP_CODE):" | tee -a "$EVIDENCE_DIR/commands.txt"
echo "$BODY" | tee "$EVIDENCE_DIR/post_response.json"

if [[ "$HTTP_CODE" -ne 201 ]]; then
    echo "FAIL: Expected HTTP 201, got $HTTP_CODE"
    exit 1
fi

RES_ID=$("$PYTHON_BIN" -c "import sys, json; print(json.loads(sys.stdin.read())['resourceId'])" <<< "$BODY")
echo "Created resourceId: $RES_ID"

# 2. List resources by type
echo -e "\nCommand: curl -s -w '\n%{http_code}' $RES/resources?type=water" >> "$EVIDENCE_DIR/commands.txt"
GET_RESP=$(curl -s -w "\n%{http_code}" "$RES/resources?type=water")
GET_CODE=$(echo "$GET_RESP" | tail -n1)
GET_BODY=$(echo "$GET_RESP" | sed '$d')

echo "GET Response ($GET_CODE):" | tee -a "$EVIDENCE_DIR/commands.txt"
echo "$GET_BODY" | tee "$EVIDENCE_DIR/get_response.json"

if [[ "$GET_CODE" -ne 200 ]]; then
    echo "FAIL: Expected HTTP 200, got $GET_CODE"
    exit 1
fi

# 3. Validate list contents
"$PYTHON_BIN" - <<EOF
import json, sys
data = json.loads('''$GET_BODY''')
items = data if isinstance(data, list) else data.get("items", [])
found = None
for item in items:
    if item.get("resourceId") == "$RES_ID":
        found = item
        break
assert found is not None, f"Created resource $RES_ID not found in list"
assert found["type"] == "water"
assert found["availability"] is True
assert found["status"] == "AVAILABLE"
print("Validation OK: Resource found with availability: true, status: AVAILABLE.")
EOF

# 4. Save relevant logs
if [[ "$MODE" == "k8s" ]]; then
    kubectl -n ems logs -l app=resource-service --tail=25 > "$EVIDENCE_DIR/resource_service.log" || true
else
    docker compose logs resource-service --tail=25 > "$EVIDENCE_DIR/resource_service.log" || true
fi

echo "PASS: Scenario 2 - Resource registration and listing verified."
