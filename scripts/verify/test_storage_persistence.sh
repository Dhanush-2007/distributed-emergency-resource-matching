#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME="ems/storage-node:1.0"
CONTAINER_NAME="ems-storage-node-persist-test"
VOLUME_NAME="ems-storage-test-volume"
PORT=9000

echo "=== 1. Building storage node image ==="
docker build -f services/storage-node/Dockerfile -t "${IMAGE_NAME}" .

cleanup() {
    echo "=== Cleaning up ==="
    docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
    docker volume rm -f "${VOLUME_NAME}" >/dev/null 2>&1 || true
}

trap cleanup EXIT

cleanup

echo "=== 2. Creating named volume and starting container ==="
docker volume create "${VOLUME_NAME}"
docker run -d \
    --name "${CONTAINER_NAME}" \
    -p "${PORT}:9000" \
    -v "${VOLUME_NAME}:/data" \
    -e NODE_ID=s0-a \
    -e DATA_DIR=/data \
    -e STANDALONE=true \
    "${IMAGE_NAME}"

# Wait for container health
echo "Waiting for storage node to become ready..."
for i in {1..30}; do
    if curl -s "http://127.0.0.1:${PORT}/health" >/dev/null 2>&1; then
        echo "Storage node is up!"
        break
    fi
    sleep 1
done

echo "=== 3. Writing test record ==="
KEY="request:persist-0001"
PUT_RES=$(curl -s -X PUT "http://127.0.0.1:${PORT}/internal/data/${KEY}" \
    -H "Content-Type: application/json" \
    -d '{"value": {"type": "water", "status": "PENDING"}, "ifVersion": 0}')

echo "PUT Response: ${PUT_RES}"
if [[ "${PUT_RES}" != *"\"version\":1"* || "${PUT_RES}" != *"\"seq\":1"* ]]; then
    echo "Initial PUT failed!"
    echo "FAIL"
    exit 1
fi

echo "=== 4. Restarting container (docker restart) ==="
docker restart "${CONTAINER_NAME}"

echo "Waiting for storage node after restart..."
for i in {1..30}; do
    if curl -s "http://127.0.0.1:${PORT}/health" >/dev/null 2>&1; then
        echo "Storage node restarted successfully!"
        break
    fi
    sleep 1
done

echo "=== 5. Verifying record persistence after restart ==="
GET_RES=$(curl -s "http://127.0.0.1:${PORT}/internal/data/${KEY}")
echo "GET Response: ${GET_RES}"

HEALTH_RES=$(curl -s "http://127.0.0.1:${PORT}/health")
echo "Health Response: ${HEALTH_RES}"

if [[ "${GET_RES}" == *"\"version\":1"* && "${GET_RES}" == *"\"water\""* && "${HEALTH_RES}" == *"\"lastSeq\":1"* ]]; then
    echo "Record and sequence persisted across container restart!"
    echo "PASS"
    exit 0
else
    echo "Persistence verification failed!"
    echo "FAIL"
    exit 1
fi
