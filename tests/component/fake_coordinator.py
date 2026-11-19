"""Thread-safe dict-backed fake coordinator for component tests."""

from __future__ import annotations

import copy
import json
import threading
from typing import Any
from urllib.parse import parse_qs, unquote

import httpx


class FakeCoordinatorApp:
    """Simulates coordinator's /internal/data and /internal/scan endpoints in memory."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._storage: dict[str, dict[str, Any]] = {}  # key -> {"value": ..., "version": int}
        self.is_down = False
        self.unavailable_shards: list[int] = []

    def set_down(self, down: bool = True) -> None:
        with self._lock:
            self.is_down = down

    def set_unavailable_shards(self, shards: list[int]) -> None:
        with self._lock:
            self.unavailable_shards = list(shards)

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        with self._lock:
            if self.is_down:
                return httpx.Response(
                    503,
                    headers={"Retry-After": "3"},
                    json={"error": {"code": "STORAGE_UNAVAILABLE", "message": "Coordinator down", "details": []}},
                )

            if path == "/health/live":
                return httpx.Response(200, json={"status": "ok"})

            if path.startswith("/internal/data/"):
                key = unquote(path[len("/internal/data/"):])
                if request.method == "GET":
                    if key not in self._storage:
                        return httpx.Response(
                            404,
                            json={"error": {"code": "NOT_FOUND", "message": f"Key '{key}' not found", "details": []}},
                        )
                    entry = self._storage[key]
                    return httpx.Response(
                        200,
                        json={
                            "key": key,
                            "value": copy.deepcopy(entry["value"]),
                            "version": entry["version"],
                        },
                    )

                if request.method == "PUT":
                    body = json.loads(request.content.decode("utf-8")) if request.content else {}
                    val = body.get("value", {})
                    if_ver = body.get("ifVersion")

                    curr = self._storage.get(key)
                    if if_ver is not None:
                        if if_ver == 0:
                            if curr is not None:
                                return httpx.Response(
                                    409,
                                    json={"error": {"code": "VERSION_CONFLICT", "message": f"Key '{key}' already exists", "details": []}},
                                )
                        else:
                            if curr is None or curr["version"] != if_ver:
                                return httpx.Response(
                                    409,
                                    json={"error": {"code": "VERSION_CONFLICT", "message": "Version conflict", "details": []}},
                                )

                    new_ver = (curr["version"] + 1) if curr is not None else 1
                    self._storage[key] = {
                        "value": copy.deepcopy(val),
                        "version": new_ver,
                    }
                    return httpx.Response(
                        200,
                        json={"key": key, "version": new_ver, "seq": new_ver},
                    )

                if request.method == "DELETE":
                    if key not in self._storage:
                        return httpx.Response(
                            404,
                            json={"error": {"code": "NOT_FOUND", "message": f"Key '{key}' not found", "details": []}},
                        )
                    entry = self._storage.pop(key)
                    return httpx.Response(
                        200,
                        json={"key": key, "version": entry["version"] + 1, "deleted": True},
                    )

            if path == "/internal/scan":
                qs = parse_qs(request.url.query.decode("ascii") if isinstance(request.url.query, bytes) else str(request.url.query))
                prefix = qs.get("prefix", [""])[0]
                allow_part = qs.get("allowPartial", ["false"])[0].lower() == "true"

                if self.unavailable_shards and not allow_part:
                    return httpx.Response(
                        503,
                        headers={"Retry-After": "3"},
                        json={"error": {"code": "STORAGE_UNAVAILABLE", "message": "Shards unavailable", "details": []}},
                    )

                items = []
                for k, entry in sorted(self._storage.items()):
                    if k.startswith(prefix):
                        items.append({
                            "key": k,
                            "value": copy.deepcopy(entry["value"]),
                            "version": entry["version"],
                        })
                return httpx.Response(
                    200,
                    json={
                        "items": items,
                        "count": len(items),
                        "unavailableShards": list(self.unavailable_shards),
                    },
                )

            return httpx.Response(404, json={"error": {"code": "NOT_FOUND", "message": "Not found", "details": []}})
