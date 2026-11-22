#!/usr/bin/env bash
set -euo pipefail

PID_FILE="/tmp/ems_k8s_ports.pid"

stop_ports() {
    if [[ -f "$PID_FILE" ]]; then
        echo "Stopping existing port-forwards..."
        while read -r pid; do
            if [[ -n "$pid" ]]; then
                kill "$pid" 2>/dev/null || true
            fi
        done < "$PID_FILE"
        rm -f "$PID_FILE"
        echo "Port-forwards stopped."
    else
        echo "No active port-forward PID file found ($PID_FILE)."
    fi
}

if [[ "${1:-}" == "--stop" ]]; then
    stop_ports
    exit 0
fi

# Stop any previously running port-forwards
stop_ports

echo "Starting background port-forwards for ems services..."
kubectl -n ems port-forward svc/coordinator 8000:8000 >/dev/null 2>&1 &
PID1=$!
kubectl -n ems port-forward svc/request-service 8001:8001 >/dev/null 2>&1 &
PID2=$!
kubectl -n ems port-forward svc/resource-service 8002:8002 >/dev/null 2>&1 &
PID3=$!
kubectl -n ems port-forward svc/rabbitmq 15672:15672 >/dev/null 2>&1 &
PID4=$!

echo "$PID1" > "$PID_FILE"
echo "$PID2" >> "$PID_FILE"
echo "$PID3" >> "$PID_FILE"
echo "$PID4" >> "$PID_FILE"

sleep 1

echo "Port-forwards started in background (PIDs: $PID1, $PID2, $PID3, $PID4):"
echo "  - Coordinator:      http://localhost:8000"
echo "  - Request Service:  http://localhost:8001"
echo "  - Resource Service: http://localhost:8002"
echo "  - RabbitMQ UI:      http://localhost:15672"
echo "Run '$0 --stop' to terminate them."
