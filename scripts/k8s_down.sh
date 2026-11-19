#!/usr/bin/env bash
set -euo pipefail

keep_data=false
if [[ "${1:-}" == "--keep-data" ]]; then
    keep_data=true
elif [[ -n "${1:-}" ]]; then
    echo "usage: $0 [--keep-data]" >&2
    exit 2
fi

if [[ "$keep_data" == true ]]; then
    kubectl -n ems delete deployment coordinator --ignore-not-found
    kubectl -n ems delete service coordinator coordinator-nodeport --ignore-not-found
    kubectl delete -f k8s/storage-nodes.yaml --ignore-not-found
    kubectl delete -f k8s/01-config.yaml --ignore-not-found
    echo "Kubernetes resources removed; PVCs retained."
else
    kubectl delete namespace ems --ignore-not-found
fi
