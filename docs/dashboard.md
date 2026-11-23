# Emergency Operations Dashboard

The EMS Dashboard is a dispatch control room interface for operators, coordinators, and emergency responders. It is served through an Nginx reverse-proxy container on port `8080`, providing access to all system APIs without cross-origin configuration or backend code modifications.

## Quick Start

1. Start the complete EMS stack including the dashboard:
   ```bash
   docker compose up -d --wait
   ```
2. Open your web browser:
   ```text
   http://localhost:8080
   ```

## Architecture & Reverse Proxy

The dashboard is packaged as a lightweight Alpine Nginx container on the internal Docker network `ems-net`. Nginx routes API traffic directly to the internal service hostnames:

- `/api/coord/*` &rarr; `http://coordinator:8000/*`
- `/api/req/*`   &rarr; `http://request-service:8001/*`
- `/api/res/*`   &rarr; `http://resource-service:8002/*`
- `/*`           &rarr; Static UI assets (`index.html`, `css/`, `js/`)

Nginx forwards correlation headers (`X-Correlation-Id`), disables caching on `/api/*` routes, and enables gzip compression for static assets.

## Views

### 1. Overview (`#overview`)
- **System KPIs**: Active requests count, available resources, healthy storage shards count, and pending match queue size.
- **Shard Health Matrix**: Compact overview of all 3 shards, current primaries, and degraded states.
- **Recent Activity**: 10 newest emergency requests with quick inspect actions.

### 2. Emergency Requests (`#requests`)
- **Intake Form**: Create emergency requests with type (`WATER`, `FOOD`, `MEDICAL`, `SHELTER`, `RESCUE`), latitude (`-90..90`), longitude (`-180..180`), and optional details (1000 character live limit).
- **Match Stepper**: 20-second active polling stepper following creation (Created &rarr; Queued &rarr; Matched/No Match).
- **Detail Drawer**: Slide-over drawer displaying raw JSON, coordinates, timeline, and calculated Haversine distance (`r = 6371.0088 km`) to matched resources.
- **Workflow State Transitions**: Buttons enforce SPEC 5.4 transitions (e.g. `IN_PROGRESS`, `COMPLETED`, `CANCELLED`).

### 3. Resources (`#resources`)
- **Registration**: Add emergency resources with name, category, and geolocation.
- **Status Control**: Toggle resource operational availability (`AVAILABLE` &harr; `OFFLINE`).

### 4. Cluster Diagnostics (`#cluster`)
- **Shard & Node Topology**: Real-time view of primary nodes, terms/epochs, and replication sequence numbers (`lastSeq`).
- **Lag Detection**: Highlights replica sequence lag in red if out of sync with primary.
- **Event Log**: In-memory event log capturing the last 20 cluster transitions (failovers, epoch bumps, health changes).
- **Failover Runbook**: Command snippets to test primary node failover and direct link to RabbitMQ management (`:15672`).

## Keyboard Shortcuts

- <kbd>g</kbd> <kbd>o</kbd> &rarr; Navigate to **Overview**
- <kbd>g</kbd> <kbd>r</kbd> &rarr; Navigate to **Requests**
- <kbd>g</kbd> <kbd>s</kbd> &rarr; Navigate to **Resources**
- <kbd>g</kbd> <kbd>c</kbd> &rarr; Navigate to **Cluster**
- <kbd>/</kbd> &rarr; Focus request filter / lookup ID input
- <kbd>Esc</kbd> &rarr; Close slide-over drawer / blur active input
