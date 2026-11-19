#!/usr/bin/env bash
set -euo pipefail

echo "=== Starting load loop (60s, rate 10, shard 1) ==="
.venv/bin/python scripts/load_loop.py --rate 10 --duration 60 --shard 1 > load_loop_output.txt 2>&1 &
LOAD_PID=$!

echo "Load loop running with PID ${LOAD_PID}. Sleeping 15s..."
sleep 15

echo "=== [15s] Stopping ems-s1-a ==="
docker stop ems-s1-a
sleep 3

echo "--- Status after stopping s1-a ---"
.venv/bin/python scripts/watch_cluster.py --once
curl -s http://localhost:8000/diagnostics/shards
echo ""

echo "Sleeping until 35s mark (17s)..."
sleep 17

echo "=== [35s] Starting ems-s1-a ==="
docker start ems-s1-a
sleep 5

echo "--- Status after restarting s1-a ---"
.venv/bin/python scripts/watch_cluster.py --once
curl -s http://localhost:8000/diagnostics/shards
echo ""

echo "Waiting for load loop to finish..."
wait ${LOAD_PID}

echo "=== Load Loop Final Summary ==="
cat load_loop_output.txt
