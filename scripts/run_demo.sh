#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-${MODE:-k8s}}"
export MODE

echo "=========================================================="
echo " Starting Full Demonstration Run (Mode: $MODE)"
echo "=========================================================="

echo "Resetting environment..."
bash scripts/reset.sh "$MODE"

SCRIPTS=(
    "scripts/demo_01_request.sh"
    "scripts/demo_02_resource.sh"
    "scripts/demo_03_matching.sh"
    "scripts/demo_04_distribution.sh"
    "scripts/demo_05_async.sh"
    "scripts/demo_06_storage_failure.sh"
    "scripts/demo_07_node_recovery.sh"
    "scripts/demo_08_k8s_recovery.sh"
    "scripts/demo_09_k8s_scaling.sh"
)

NAMES=(
    "01. Request Creation & Retrieval"
    "02. Resource Registration & Query"
    "03. Resource Matching (Haversine & Tie-break)"
    "04. Data Distribution (SHA-256 Sharding)"
    "05. Asynchronous Queue Processing"
    "06. Storage Node Failure & Failover"
    "07. Storage Node Recovery & Convergence"
    "08. Kubernetes Pod Self-Healing"
    "09. Kubernetes Microservice Scaling"
)

TOTAL=${#SCRIPTS[@]}
STATUSES=()
DURATIONS=()
OVERALL_FAIL=0

START_ALL=$(date +%s)

for i in "${!SCRIPTS[@]}"; do
    SCRIPT="${SCRIPTS[$i]}"
    NAME="${NAMES[$i]}"
    echo ""
    echo "----------------------------------------------------------"
    echo "[$((i + 1))/$TOTAL] Running $NAME ($SCRIPT)..."
    echo "----------------------------------------------------------"
    
    START=$(date +%s)
    if bash "$SCRIPT"; then
        STATUS="PASS"
    else
        STATUS="FAIL"
        OVERALL_FAIL=1
    fi
    END=$(date +%s)
    DURATION=$((END - START))
    
    STATUSES+=("$STATUS")
    DURATIONS+=("${DURATION}s")
done

END_ALL=$(date +%s)
TOTAL_DURATION=$((END_ALL - START_ALL))

echo ""
echo "=========================================================================="
echo " DEMONSTRATION RUN SUMMARY (Mode: $MODE, Total Time: ${TOTAL_DURATION}s)"
echo "=========================================================================="
printf "%-4s | %-45s | %-10s | %-6s\n" "No." "Scenario" "Duration" "Status"
echo "--------------------------------------------------------------------------"

for i in "${!SCRIPTS[@]}"; do
    printf "%-4s | %-45s | %-10s | %-6s\n" "$((i + 1))" "${NAMES[$i]}" "${DURATIONS[$i]}" "${STATUSES[$i]}"
done
echo "=========================================================================="

if [[ "$OVERALL_FAIL" -eq 0 ]]; then
    echo "ALL 9 DEMONSTRATION SCENARIOS PASSED SUCCESSFULLY."
    exit 0
else
    echo "ONE OR MORE DEMONSTRATION SCENARIOS FAILED."
    exit 1
fi
