# Emergency Resource Matching System - Operations Dashboard

Hand-crafted operations dashboard for dispatch coordinators.
Runs on Nginx 1.27 Alpine as a reverse proxy to Coordinator (:8000), Request Service (:8001), and Resource Service (:8002).

## Architecture
- **Port:** `8080`
- **Reverse Proxy:**
  - `/api/coord/` -> `http://coordinator:8000/`
  - `/api/req/` -> `http://request-service:8001/`
  - `/api/res/` -> `http://resource-service:8002/`
- **Design System:** Calm "dispatch control room" theme. Paper background `#F4F1EA`, ink `#1B1B18`, signal orange `#D9480F` accent, hairline borders `#D9D4C7`, IBM Plex Sans & IBM Plex Mono typography.
- **Frontend Stack:** Vanilla ES Modules, zero build steps, zero external frameworks/CDNs.

## Hash Routes
- `#overview` - Cluster KPIs, shard summary table, newest 10 requests.
- `#requests` - Request creation form with validation, status polling, full requests table, detail drawer.
- `#resources` - Resource registration form, filterable resources table, availability toggling.
- `#cluster` - Per-shard health and node status, lastSeq alignment, real-time live failover event feed, failover demonstration commands.

## Keyboard Shortcuts
- `g` then `o` -> Navigate to Overview
- `g` then `r` -> Navigate to Requests
- `g` then `s` -> Navigate to Resources
- `g` then `c` -> Navigate to Cluster
- `/` -> Focus lookup / search input
