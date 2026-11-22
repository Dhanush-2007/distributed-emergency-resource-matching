#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_DIR=$(mktemp -d /tmp/ems-fresh-clone-XXXXXX)

cleanup() {
    echo "=== Cleaning up fresh clone test ==="
    if [[ -d "$TMP_DIR" ]]; then
        (cd "$TMP_DIR" && docker compose down -v >/dev/null 2>&1 || true)
        rm -rf "$TMP_DIR"
    fi
    echo "=== Restoring original repository stack ==="
    (cd "$ROOT_DIR" && docker compose up -d --wait >/dev/null 2>&1 || true)
}

trap cleanup EXIT

echo "=== 1. Tearing down any existing stack in root repo ==="
(cd "$ROOT_DIR" && docker compose down -v)

echo "=== 2. Copying working tree minus ignored files to $TMP_DIR ==="
(cd "$ROOT_DIR" && git ls-files -co --exclude-standard | rsync -a --files-from=- ./ "$TMP_DIR/")

# Ensure absolutely no .env exists
rm -f "$TMP_DIR/.env"

echo "=== 3. Starting stack from fresh clone without .env ==="
cd "$TMP_DIR"
docker compose up -d --wait

echo "=== 4. Running smoke test ==="
RES_RESP=$(curl -s -f -X POST http://localhost:8002/resources \
    -H "Content-Type: application/json" \
    -d '{"name": "Smoke Water", "type": "water", "location": {"lat": 12.9716, "lon": 77.5946}, "availability": true}')

RES_ID=$(echo "$RES_RESP" | python3 -c "import sys, json; print(json.load(sys.stdin)['resourceId'])")
echo "Created smoke resource: $RES_ID"

REQ_RESP=$(curl -s -f -X POST http://localhost:8001/requests \
    -H "Content-Type: application/json" \
    -d '{"type": "water", "location": {"lat": 12.9716, "lon": 77.5946}, "details": "Smoke test request"}')

REQ_ID=$(echo "$REQ_RESP" | python3 -c "import sys, json; print(json.load(sys.stdin)['requestId'])")
echo "Created smoke request: $REQ_ID"

MATCHED=false
for i in $(seq 1 30); do
    STATUS=$(curl -s "http://localhost:8001/requests/$REQ_ID" | python3 -c "import sys, json; print(json.load(sys.stdin).get('status', ''))")
    if [[ "$STATUS" == "MATCHED" ]]; then
        MATCHED=true
        echo "Request $REQ_ID matched within $i seconds!"
        break
    fi
    sleep 1
done

if [[ "$MATCHED" == "true" ]]; then
    echo "PASS"
    exit 0
else
    echo "FAIL: Request did not reach MATCHED status" >&2
    exit 1
fi
