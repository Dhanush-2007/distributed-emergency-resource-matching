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

EVIDENCE_DIR="docs/evidence/04-data-distribution"
mkdir -p "$EVIDENCE_DIR"

echo "=== Scenario 4: Shard Distribution ($MODE mode) ==="

# 1. Run seed with 30 requests and 30 resources
echo "Running seed.py --requests 30 --resources 30..."
"$PYTHON_BIN" scripts/seed.py --requests 30 --resources 30 | tee "$EVIDENCE_DIR/seed_output.txt"

# 2. Check /diagnostics/shards
echo -e "\nQuerying $COORD/diagnostics/shards..."
SHARDS_JSON=$(curl -s "$COORD/diagnostics/shards")
echo "$SHARDS_JSON" | tee "$EVIDENCE_DIR/diagnostics_shards.json"

# Validate per-shard counts are all non-zero
"$PYTHON_BIN" - <<EOF
import json, sys
data = json.loads('''$SHARDS_JSON''')
items = data if isinstance(data, list) else list(data.values())
assert len(items) == 3, f"Expected 3 shards, got {len(items)}"
for s in items:
    sid = s["shardId"]
    counts = s.get("recordCount", {})
    total = sum(counts.values()) if counts else s.get("records", 0)
    print(f"Shard {sid}: {total} records ({counts})")
    assert total > 0, f"Shard {sid} has 0 records!"
print("Verified: Non-zero record counts across all three shards.")
EOF

# 3. Check route stability on the same key twice
TEST_KEY="request:demo-shard-test-key-001"
ROUTE1=$(curl -s "$COORD/diagnostics/route?key=$TEST_KEY")
ROUTE2=$(curl -s "$COORD/diagnostics/route?key=$TEST_KEY")
echo "$ROUTE1" | tee "$EVIDENCE_DIR/route1.json"
echo "$ROUTE2" | tee "$EVIDENCE_DIR/route2.json"

"$PYTHON_BIN" - <<EOF
import json, sys
r1 = json.loads('''$ROUTE1''')
r2 = json.loads('''$ROUTE2''')
assert r1["shardId"] == r2["shardId"], f"Route changed! {r1['shardId']} != {r2['shardId']}"
assert r1["nodeId"] == r2["nodeId"], f"Target node changed! {r1['nodeId']} != {r2['nodeId']}"
print(f"Verified: Deterministic routing for key $TEST_KEY -> Shard {r1['shardId']} -> Node {r1['nodeId']}")
EOF

# 4. Capture coordinator routing logs
if [[ "$MODE" == "k8s" ]]; then
    kubectl -n ems logs -l app=coordinator --tail=50 > "$EVIDENCE_DIR/coordinator_routing.log" || true
else
    docker compose logs coordinator --tail=50 > "$EVIDENCE_DIR/coordinator_routing.log" || true
fi

echo "PASS: Scenario 4 - Shard distribution and deterministic routing verified."
