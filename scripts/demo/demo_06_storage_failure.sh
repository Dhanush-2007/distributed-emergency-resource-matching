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

EVIDENCE_DIR="docs/evidence/06-storage-failure"
mkdir -p "$EVIDENCE_DIR"

echo "=== Scenario 6: Storage Node Failure ($MODE mode) ==="

# 1. State before failure
echo "Capturing cluster status before failure..."
"$PYTHON_BIN" scripts/ops/watch_cluster.py --once | tee "$EVIDENCE_DIR/watch_cluster_before.txt"

SHARDS_DATA=$(curl -s "$COORD/diagnostics/shards")
echo "$SHARDS_DATA" | tee "$EVIDENCE_DIR/diagnostics_before.json"

PRIMARY=$("$PYTHON_BIN" - <<EOF
import json, sys
data = json.loads('''$SHARDS_DATA''')
items = data if isinstance(data, list) else list(data.values())
for s in items:
    if s["shardId"] == 1:
        print(s["primary"])
        sys.exit(0)
sys.exit(1)
EOF
)

REPLICA="s1-b"
if [[ "$PRIMARY" == "s1-b" ]]; then
    REPLICA="s1-a"
fi

INIT_EPOCH=$("$PYTHON_BIN" - <<EOF
import json, sys
data = json.loads('''$SHARDS_DATA''')
items = data if isinstance(data, list) else list(data.values())
for s in items:
    if s["shardId"] == 1:
        print(s.get("epoch", 1))
        sys.exit(0)
EOF
)

echo "Shard 1 initial state: primary=$PRIMARY, replica=$REPLICA, epoch=$INIT_EPOCH"

# 2. Find and write a request specifically on shard 1
echo "Creating probe request on shard 1..."
PROBE_ID=$("$PYTHON_BIN" - <<EOF
import json, urllib.request, sys
from ems_common.sharding import shard_of

for _ in range(30):
    req = urllib.request.Request(
        "$REQ/requests",
        data=json.dumps({"type":"water","location":{"lat":12.9716,"lon":77.5946},"details":"Probe key shard 1"}).encode(),
        headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read().decode())
            rid = data.get("requestId")
            if shard_of(f"request:{rid}", 3) == 1:
                print(rid)
                sys.exit(0)
    except Exception:
        pass
sys.exit(1)
EOF
)
echo "Submitted probe request $PROBE_ID to verify shard 1 data availability."

# 3. Stop primary node
echo -e "\nTaking down primary node $PRIMARY..."
if [[ "$MODE" == "k8s" ]]; then
    kubectl -n ems scale "statefulset/ems-$PRIMARY" --replicas=0
else
    docker stop "ems-$PRIMARY"
fi

# 4. Wait for heartbeat detection (~6s) and promotion of replica
echo "Waiting for coordinator to detect DOWN and promote $REPLICA..."
PROMOTED=0
for i in $(seq 1 25); do
    sleep 1
    CUR_SHARDS=$(curl -s "$COORD/diagnostics/shards" 2>/dev/null || echo "")
    if [[ -n "$CUR_SHARDS" ]]; then
        STATE=$("$PYTHON_BIN" - <<EOF
import json, sys
try:
    data = json.loads('''$CUR_SHARDS''')
    items = data if isinstance(data, list) else list(data.values())
    for s in items:
        if s["shardId"] == 1:
            if s.get("primary") == "$REPLICA" and s.get("epoch", 0) > $INIT_EPOCH:
                print(f"PROMOTED:{s.get('epoch')}:{s.get('state', s.get('status'))}")
                sys.exit(0)
except Exception:
    pass
print("WAITING")
EOF
)
        if [[ "$STATE" == PROMOTED* ]]; then
            PROMOTED=1
            echo "Promotion verified: $STATE"
            break
        fi
    fi
done

if [[ "$PROMOTED" -ne 1 ]]; then
    echo "FAIL: Failover to $REPLICA did not occur within 25 seconds"
    exit 1
fi

# 5. Capture cluster status during failure (degraded state)
echo -e "\nCapturing cluster status during failure (DEGRADED)..."
"$PYTHON_BIN" scripts/ops/watch_cluster.py --once | tee "$EVIDENCE_DIR/watch_cluster_during.txt"
curl -s "$COORD/diagnostics/shards" | tee "$EVIDENCE_DIR/diagnostics_during.json"

# 6. Verify probe key is still readable on the newly promoted replica
echo "Verifying probe request $PROBE_ID is readable..."
READ_RESP=$(curl -s -w "\n%{http_code}" "$REQ/requests/$PROBE_ID")
READ_CODE=$(echo "$READ_RESP" | tail -n1)
READ_BODY=$(echo "$READ_RESP" | sed '$d')

if [[ "$READ_CODE" -ne 200 ]]; then
    echo "FAIL: Probe request $PROBE_ID returned $READ_CODE after failover"
    exit 1
fi
echo "$READ_BODY" | tee "$EVIDENCE_DIR/probe_read_during.json"
echo "Verified: Pre-failure request $PROBE_ID readable from new primary $REPLICA."

# 7. Verify new writes succeed on degraded shard
WRITE_RESP=$(curl -s -w "\n%{http_code}" -X POST "$REQ/requests" -H "Content-Type: application/json" -d "{\"type\":\"food\",\"location\":{\"lat\":12.9716,\"lon\":77.5946},\"details\":\"Degraded write test\"}")
WRITE_CODE=$(echo "$WRITE_RESP" | tail -n1)
if [[ "$WRITE_CODE" -ne 201 ]]; then
    echo "FAIL: Write during degraded state returned $WRITE_CODE"
    exit 1
fi
echo "Verified: New write succeeded during degraded mode (HTTP 201)."

# 8. Capture failure & promotion logs
if [[ "$MODE" == "k8s" ]]; then
    kubectl -n ems logs -l app=coordinator --tail=60 > "$EVIDENCE_DIR/coordinator_failover.log" || true
else
    docker compose logs coordinator --tail=60 > "$EVIDENCE_DIR/coordinator_failover.log" || true
fi

echo "PASS: Scenario 6 - Storage failure, heartbeat detection, and replica promotion verified."
