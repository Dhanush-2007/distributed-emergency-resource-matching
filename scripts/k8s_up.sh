#!/usr/bin/env bash
set -euo pipefail

for command in docker kubectl minikube; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "required command not found: $command" >&2
        exit 1
    fi
done

if ! minikube status -p minikube >/dev/null 2>&1; then
    echo "Starting Minikube (memory=${MINIKUBE_MEMORY:-4096}MB)..."
    minikube start --memory="${MINIKUBE_MEMORY:-4096}" --cpus=2
fi

echo "=== 1. Building and loading images ==="
make images
for img in storage-node coordinator request-service resource-service matching-service; do
    echo "Loading ems/$img:1.0 into Minikube..."
    minikube image load "ems/$img:1.0"
done

echo "=== 2. Generating storage manifests ==="
.venv/bin/python k8s/generate_storage.py --all > k8s/storage-nodes.yaml

echo "=== 3. Applying manifests in order ==="
kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/01-config.yaml
kubectl apply -f k8s/02-secret.yaml
kubectl apply -f k8s/rabbitmq.yaml
kubectl apply -f k8s/storage-nodes.yaml
kubectl apply -f k8s/coordinator.yaml
kubectl apply -f k8s/services.yaml

echo "=== 4. Waiting for rollouts (timeout 180s) ==="
kubectl -n ems rollout status statefulset/rabbitmq --timeout=180s
for s in s0-a s0-b s1-a s1-b s2-a s2-b; do
    kubectl -n ems rollout status "statefulset/ems-$s" --timeout=180s
done
kubectl -n ems rollout status deployment/coordinator --timeout=180s
for d in request-service resource-service matching-service; do
    kubectl -n ems rollout status "deployment/$d" --timeout=180s
done

echo "=== 5. Waiting for all shards to become HEALTHY ==="
# Temporary port-forward to check coordinator diagnostics
kubectl -n ems port-forward svc/coordinator 18000:8000 >/dev/null 2>&1 &
PF_PID=$!
trap 'kill $PF_PID 2>/dev/null || true' EXIT

HEALTHY=0
for i in $(seq 1 45); do
    sleep 2
    STATUS=$(curl -s http://127.0.0.1:18000/diagnostics/shards 2>/dev/null || echo "")
    if [[ -n "$STATUS" ]] && [[ "$STATUS" == *"HEALTHY"* ]]; then
        HEALTHY_COUNT=$(python3 -c "import sys, json; data=json.loads(sys.stdin.read()); items = data if isinstance(data, list) else list(data.values()); print(sum(1 for s in items if s.get('state') == 'HEALTHY' or s.get('status') == 'HEALTHY'))" <<< "$STATUS" 2>/dev/null || echo "0")
        if [[ "$HEALTHY_COUNT" -ge 3 ]]; then
            HEALTHY=1
            echo "All 3 shards are HEALTHY!"
            break
        fi
    fi
done

kill $PF_PID 2>/dev/null || true
trap - EXIT

if [[ "$HEALTHY" -ne 1 ]]; then
    echo "Warning: Shards did not all report HEALTHY within the wait period."
fi

echo "=== 6. Stack is Ready! ==="
echo "Access instructions:"
echo "Start port-forwards:"
echo "  ./scripts/k8s_ports.sh"
echo "  - Coordinator:      http://localhost:8000"
echo "  - Request Service:  http://localhost:8001"
echo "  - Resource Service: http://localhost:8002"
echo "  - RabbitMQ UI:      http://localhost:15672 (user: ems / pass: ems-dev-password)"
echo ""
echo "Direct NodePort Access via Minikube:"
echo "  - Coordinator:      http://\$(minikube ip):30080"
echo "  - Request Service:  http://\$(minikube ip):30081"
echo "  - Resource Service: http://\$(minikube ip):30082"
echo "  - RabbitMQ UI:      http://\$(minikube ip):31672"
