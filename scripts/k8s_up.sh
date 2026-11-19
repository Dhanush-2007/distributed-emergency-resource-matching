#!/usr/bin/env bash
set -euo pipefail

for command in docker kubectl minikube; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "required command not found: $command" >&2
        exit 1
    fi
done

if ! minikube status -p minikube >/dev/null 2>&1; then
    minikube start --memory="${MINIKUBE_MEMORY:-4096}" --cpus=2
fi
make images
minikube image load ems/storage-node:1.0
minikube image load ems/coordinator:1.0

.venv/bin/python k8s/generate_storage.py --nodes s0-a,s1-a,s2-a > k8s/storage-nodes.yaml
kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/01-config.yaml
kubectl apply -f k8s/coordinator.yaml
kubectl apply -f k8s/storage-nodes.yaml

kubectl -n ems rollout status deployment/coordinator --timeout=180s
for statefulset in ems-s0-a ems-s1-a ems-s2-a; do
    kubectl -n ems rollout status "statefulset/$statefulset" --timeout=180s
done

echo "Access instructions:"
echo "Primary method: kubectl -n ems port-forward svc/coordinator 8000:8000"
echo "Alternative method: minikube service coordinator-nodeport -n ems --url"
