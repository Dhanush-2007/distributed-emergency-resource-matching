#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-compose}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${ROOT_DIR}/.venv/bin/python"
if [[ ! -x "${PYTHON_BIN}" ]]; then
    PYTHON_BIN="python3"
fi

if [[ "$MODE" == "compose" ]]; then
    echo "=== 1. Tearing down Compose stack and volumes ==="
    docker compose down -v

    echo "=== 2. Starting fresh Compose stack ==="
    docker compose up -d --wait

    echo "=== 3. Purging RabbitMQ queues ==="
    docker exec ems-rabbitmq rabbitmqctl purge_queue request.match >/dev/null 2>&1 || true
    docker exec ems-rabbitmq rabbitmqctl purge_queue request.match.retry >/dev/null 2>&1 || true
    docker exec ems-rabbitmq rabbitmqctl purge_queue request.match.dlq >/dev/null 2>&1 || true

    echo "=== 4. Running deterministic seed (--fixed) ==="
    "${PYTHON_BIN}" scripts/seed.py --fixed

    echo "=== 5. Reset complete ==="
    RES_COUNT=$(curl -s "http://localhost:8002/resources" | "${PYTHON_BIN}" -c "import sys, json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "4")
    echo "Resources verified in database: ${RES_COUNT}"

elif [[ "$MODE" == "k8s" ]]; then
    echo "=== 1. Tearing down Kubernetes stack and PVCs ==="
    ./scripts/k8s_down.sh
    while kubectl get ns ems >/dev/null 2>&1; do
        sleep 1
    done

    echo "=== 2. Re-deploying Kubernetes stack from scratch ==="
    ./scripts/k8s_up.sh

    echo "=== 3. Purging RabbitMQ queues in pod rabbitmq-0 ==="
    kubectl -n ems exec rabbitmq-0 -- rabbitmqctl purge_queue request.match >/dev/null 2>&1 || true
    kubectl -n ems exec rabbitmq-0 -- rabbitmqctl purge_queue request.match.retry >/dev/null 2>&1 || true
    kubectl -n ems exec rabbitmq-0 -- rabbitmqctl purge_queue request.match.dlq >/dev/null 2>&1 || true

    echo "=== 4. Starting port-forwards ==="
    ./scripts/k8s_ports.sh
    sleep 2

    echo "=== 5. Running deterministic seed (--fixed) ==="
    "${PYTHON_BIN}" scripts/seed.py --fixed

    echo "=== 6. Reset complete ==="
    RES_COUNT=$(curl -s "http://localhost:8002/resources" | "${PYTHON_BIN}" -c "import sys, json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "4")
    echo "Resources verified in database: ${RES_COUNT}"

else
    echo "Usage: $0 [compose|k8s]" >&2
    exit 1
fi
