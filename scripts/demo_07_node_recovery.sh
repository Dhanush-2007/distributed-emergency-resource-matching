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

EVIDENCE_DIR="docs/evidence/07-node-recovery"
mkdir -p "$EVIDENCE_DIR"

echo "=== Scenario 7: Node Recovery ($MODE mode) ==="

# 1. Detect current primary and recovering follower of shard 1
SHARDS_DATA=$(curl -s "$COORD/diagnostics/shards")
CURRENT_PRIMARY=$("$PYTHON_BIN" - <<EOF
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

RECOVERING_NODE="s1-a"
if [[ "$CURRENT_PRIMARY" == "s1-a" ]]; then
    RECOVERING_NODE="s1-b"
fi

echo "Shard 1: Current Primary=$CURRENT_PRIMARY, Returning Follower=$RECOVERING_NODE"

# 2. Restart / scale returning node
echo "Bringing $RECOVERING_NODE back online..."
if [[ "$MODE" == "k8s" ]]; then
    kubectl -n ems scale "statefulset/ems-$RECOVERING_NODE" --replicas=1
else
    docker start "ems-$RECOVERING_NODE"
fi

# 3. Wait for node to register, catch up, and shard to return to HEALTHY
echo "Waiting for $RECOVERING_NODE to synchronize and shard 1 to report HEALTHY..."
RECOVERED=0
for i in $(seq 1 40); do
    sleep 2
    CUR_SHARDS=$(curl -s "$COORD/diagnostics/shards" 2>/dev/null || echo "")
    if [[ -n "$CUR_SHARDS" ]]; then
        HEALTH=$("$PYTHON_BIN" - <<EOF
import json, sys
try:
    data = json.loads('''$CUR_SHARDS''')
    items = data if isinstance(data, list) else list(data.values())
    for s in items:
        if s["shardId"] == 1:
            st = s.get("state", s.get("status"))
            if st == "HEALTHY":
                # Check nodes lastSeq equality
                nodes = s.get("nodes", [])
                if len(nodes) >= 2:
                    seqs = [n.get("lastSeq") for n in nodes]
                    if len(set(seqs)) == 1:
                        print(f"HEALTHY:seq={seqs[0]}")
                        sys.exit(0)
except Exception:
    pass
print("WAITING")
EOF
)
        if [[ "$HEALTH" == HEALTHY* ]]; then
            RECOVERED=1
            echo "Recovery verified: $HEALTH"
            break
        fi
    fi
done

if [[ "$RECOVERED" -ne 1 ]]; then
    echo "FAIL: Shard 1 did not recover to HEALTHY with equal sequence numbers within 80 seconds"
    exit 1
fi

# 4. State after recovery
echo -e "\nCapturing cluster status after recovery (HEALTHY)..."
"$PYTHON_BIN" scripts/watch_cluster.py --once | tee "$EVIDENCE_DIR/watch_cluster_after.txt"
curl -s "$COORD/diagnostics/shards" | tee "$EVIDENCE_DIR/diagnostics_after.json"

# 5. Capture storage node catch-up logs
if [[ "$MODE" == "k8s" ]]; then
    kubectl -n ems logs "statefulset/ems-$RECOVERING_NODE" --tail=60 > "$EVIDENCE_DIR/catchup_node.log" || true
    kubectl -n ems logs -l app=coordinator --tail=60 > "$EVIDENCE_DIR/coordinator_recovery.log" || true
else
    docker compose logs "ems-$RECOVERING_NODE" --tail=60 > "$EVIDENCE_DIR/catchup_node.log" || true
    docker compose logs coordinator --tail=60 > "$EVIDENCE_DIR/coordinator_recovery.log" || true
fi

echo "PASS: Scenario 7 - Node recovery, replication sync, and HEALTHY status verified."
