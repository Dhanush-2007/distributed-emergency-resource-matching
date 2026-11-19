# Storage Node Service

Standalone SQLite key/value service for the Distributed Emergency Resource Matching System.

## Run Locally

Activate virtual environment:
```powershell
.venv\Scripts\activate
```

Run storage node:
```powershell
$env:PYTHONPATH="services/storage-node"
python -m uvicorn storage_node.main:app --host 127.0.0.1 --port 9000
```

Or with custom node configuration:
```powershell
$env:NODE_ID="s0-a"
$env:SHARD_ID="0"
$env:PORT="9000"
$env:STANDALONE="true"
$env:DATA_DIR="./data"
$env:PYTHONPATH="services/storage-node"
python -m uvicorn storage_node.main:app --host 127.0.0.1 --port 9000
```

## Endpoints

- `GET /health` - Diagnostic and operational health state
- `GET /health/live` - Liveness probe
- `GET /health/ready` - Readiness probe
- `PUT /internal/data/{key}` - Put or update record (with optional `ifVersion` and `epoch`)
- `GET /internal/data/{key}` - Retrieve record by key
- `DELETE /internal/data/{key}` - Tombstone record (with optional `epoch`)
- `GET /internal/scan?prefix={prefix}` - Scatter-gather scan live records by prefix
