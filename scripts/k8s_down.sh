#!/usr/bin/env bash
set -euo pipefail

keep_data=false
if [[ "${1:-}" == "--keep-data" ]]; then
    keep_data=true
elif [[ -n "${1:-}" ]]; then
    echo "usage: $0 [--keep-data]" >&2
    exit 2
fi

# Stop any active port-forwards
if [[ -f "scripts/k8s_ports.sh" ]]; then
    ./scripts/k8s_ports.sh --stop >/dev/null 2>&1 || true
fi

if [[ "$keep_data" == true ]]; then
    kubectl -n ems delete -f k8s/services.yaml --ignore-not-found
    kubectl -n ems delete -f k8s/coordinator.yaml --ignore-not-found
    kubectl -n ems delete -f k8s/storage-nodes.yaml --ignore-not-found
    kubectl -n ems delete -f k8s/rabbitmq.yaml --ignore-not-found
    kubectl -n ems delete -f k8s/02-secret.yaml --ignore-not-found
    kubectl -n ems delete -f k8s/01-config.yaml --ignore-not-found
    echo "Kubernetes workloads removed; PVCs and namespace retained."
else
    kubectl delete namespace ems --ignore-not-found
    echo "Kubernetes namespace ems and all PVCs deleted."
fi
