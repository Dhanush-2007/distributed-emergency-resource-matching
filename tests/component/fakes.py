"""Dict-backed fake storage nodes for coordinator component tests."""

from __future__ import annotations

import json
from typing import Any

import httpx


class FakeStorageDB:
    """In-memory key/value database for a fake storage node."""

    def __init__(self, node_id: str, shard_id: int) -> None:
        self.node_id = node_id
        self.shard_id = shard_id
        self.epoch = 1
        self.role = "PRIMARY"
        self.data: dict[str, dict[str, Any]] = {}
        self.seq = 0

    def put(
        self,
        key: str,
        value: dict[str, Any],
        if_version: int | None,
        epoch: int | None,
    ) -> tuple[int, dict[str, Any]]:
        if epoch is not None and epoch != self.epoch:
            return 409, {
                "error": {
                    "code": "STALE_EPOCH",
                    "message": f"Stale epoch {epoch}, expected {self.epoch}",
                    "details": [],
                }
            }

        existing = self.data.get(key)
        if existing is None:
            if if_version is not None and if_version != 0:
                return 409, {
                    "error": {
                        "code": "VERSION_CONFLICT",
                        "message": "Key does not exist",
                        "details": [],
                    }
                }
            self.seq += 1
            ver = 1
            self.data[key] = {"value": value, "version": ver, "deleted": False}
            return 201, {
                "key": key,
                "version": ver,
                "seq": self.seq,
                "replicated": False,
            }
        else:
            if existing["deleted"]:
                if if_version is not None and if_version != 0:
                    return 409, {
                        "error": {
                            "code": "VERSION_CONFLICT",
                            "message": "Key is deleted",
                            "details": [],
                        }
                    }
                self.seq += 1
                ver = existing["version"] + 1
                self.data[key] = {"value": value, "version": ver, "deleted": False}
                return 201, {
                    "key": key,
                    "version": ver,
                    "seq": self.seq,
                    "replicated": False,
                }
            else:
                if if_version == 0:
                    return 409, {
                        "error": {
                            "code": "VERSION_CONFLICT",
                            "message": "Key already exists",
                            "details": [],
                        }
                    }
                if if_version is not None and if_version != existing["version"]:
                    return 409, {
                        "error": {
                            "code": "VERSION_CONFLICT",
                            "message": "Version mismatch",
                            "details": [],
                        }
                    }
                self.seq += 1
                ver = existing["version"] + 1
                self.data[key] = {"value": value, "version": ver, "deleted": False}
                return 200, {
                    "key": key,
                    "version": ver,
                    "seq": self.seq,
                    "replicated": False,
                }

    def get(self, key: str) -> tuple[int, dict[str, Any]]:
        rec = self.data.get(key)
        if rec is None or rec["deleted"]:
            return 404, {
                "error": {
                    "code": "NOT_FOUND",
                    "message": f"Key '{key}' not found",
                    "details": [],
                }
            }
        return 200, {
            "key": key,
            "value": rec["value"],
            "version": rec["version"],
        }

    def delete(
        self, key: str, epoch: int | None
    ) -> tuple[int, dict[str, Any]]:
        if epoch is not None and epoch != self.epoch:
            return 409, {
                "error": {
                    "code": "STALE_EPOCH",
                    "message": f"Stale epoch {epoch}",
                    "details": [],
                }
            }
        rec = self.data.get(key)
        if rec is None or rec["deleted"]:
            return 404, {
                "error": {
                    "code": "NOT_FOUND",
                    "message": f"Key '{key}' not found",
                    "details": [],
                }
            }
        self.seq += 1
        rec["deleted"] = True
        rec["version"] += 1
        return 200, {
            "key": key,
            "version": rec["version"],
            "seq": self.seq,
        }

    def scan(self, prefix: str) -> dict[str, Any]:
        items = []
        for k in sorted(self.data.keys()):
            rec = self.data[k]
            if not rec["deleted"] and k.startswith(prefix):
                items.append({
                    "key": k,
                    "value": rec["value"],
                    "version": rec["version"],
                })
        return {"items": items, "count": len(items)}


class FakeStorageCluster:
    """Coordinates multiple fake storage nodes dispatched by hostname via httpx.MockTransport."""

    def __init__(self, node_configs: list[tuple[str, int, str]]) -> None:
        """node_configs: [(node_id, shard_id, endpoint), ...]."""
        self.dbs: dict[str, FakeStorageDB] = {}
        self.down_nodes: set[str] = set()
        self.host_to_node: dict[str, str] = {}

        for node_id, shard_id, endpoint in node_configs:
            db = FakeStorageDB(node_id, shard_id)
            self.dbs[node_id] = db

            parsed = httpx.URL(endpoint)
            self.host_to_node[parsed.host] = node_id

    def set_down(self, node_id: str, is_down: bool = True) -> None:
        """Simulate a node being down/unreachable."""
        if is_down:
            self.down_nodes.add(node_id)
        else:
            self.down_nodes.discard(node_id)

    def handler(self, request: httpx.Request) -> httpx.Response:
        """Dispatch HTTP requests to the target fake storage node database."""
        host = request.url.host
        node_id = self.host_to_node.get(host, host)
        if node_id in self.down_nodes:
            raise httpx.ConnectError(f"Node {node_id} is unreachable")

        db = self.dbs.get(node_id)
        if db is None:
            return httpx.Response(503, json={"error": f"Unknown fake host: {host}"})

        path = request.url.path
        method = request.method

        if path.startswith("/internal/data/"):
            key = path[len("/internal/data/") :]
            if method == "PUT":
                payload = json.loads(request.content)
                status_code, body = db.put(
                    key,
                    payload.get("value", {}),
                    payload.get("ifVersion"),
                    payload.get("epoch"),
                )
                return httpx.Response(status_code, json=body)
            elif method == "GET":
                status_code, body = db.get(key)
                return httpx.Response(status_code, json=body)
            elif method == "DELETE":
                epoch_str = request.url.params.get("epoch")
                epoch = int(epoch_str) if epoch_str is not None else None
                status_code, body = db.delete(key, epoch)
                return httpx.Response(status_code, json=body)

        elif path == "/internal/scan":
            prefix = request.url.params.get("prefix", "")
            body = db.scan(prefix)
            return httpx.Response(200, json=body)

        elif path == "/health":
            return httpx.Response(200, json={"status": "ok", "role": db.role, "epoch": db.epoch})

        elif path == "/internal/role":
            payload = json.loads(request.content)
            req_role = payload.get("role")
            req_epoch = int(payload.get("epoch", 1))
            db.role = req_role
            db.epoch = req_epoch
            return httpx.Response(200, json={"role": req_role, "epoch": req_epoch, "lastSeq": db.seq})

        return httpx.Response(404, json={"detail": "Not found"})

    def get_client(self) -> httpx.Client:
        """Return an httpx.Client configured with this cluster's transport."""
        return httpx.Client(transport=httpx.MockTransport(self.handler))
