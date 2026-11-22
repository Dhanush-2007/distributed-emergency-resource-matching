#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-${MODE:-k8s}}"
export MODE

echo "=========================================================="
echo " STARTING DEMONSTRATION REHEARSALS (Mode: $MODE)"
echo " Two consecutive clean runs required by SPEC 16.1"
echo "=========================================================="

mkdir -p docs/evidence/rehearsal-1 docs/evidence/rehearsal-2

# Rehearsal 1
echo ""
echo "##########################################################"
echo " REHEARSAL 1 / 2"
echo "##########################################################"
if ! bash scripts/demo/run_demo.sh "$MODE"; then
    echo "FAIL: Rehearsal 1 failed."
    exit 1
fi

echo "Archiving Rehearsal 1 evidence..."
rm -rf docs/evidence/rehearsal-1/*
for d in docs/evidence/0*; do
    if [[ -d "$d" ]]; then
        cp -r "$d" docs/evidence/rehearsal-1/
    fi
done
echo "Rehearsal 1 PASS."

# Rehearsal 2
echo ""
echo "##########################################################"
echo " REHEARSAL 2 / 2"
echo "##########################################################"
if ! bash scripts/demo/run_demo.sh "$MODE"; then
    echo "FAIL: Rehearsal 2 failed."
    exit 1
fi

echo "Archiving Rehearsal 2 evidence..."
rm -rf docs/evidence/rehearsal-2/*
for d in docs/evidence/0*; do
    if [[ -d "$d" ]]; then
        cp -r "$d" docs/evidence/rehearsal-2/
    fi
done
echo "Rehearsal 2 PASS."

echo ""
echo "=========================================================="
echo " BOTH REHEARSALS COMPLETED WITH 100% PASS"
echo " Evidence preserved in docs/evidence/rehearsal-1 and docs/evidence/rehearsal-2"
echo "=========================================================="
