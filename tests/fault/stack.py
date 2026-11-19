"""Stack management and RabbitMQ management API helpers for fault and integration tests."""

from __future__ import annotations

import json
import os
import subprocess
import time
from collections.abc import Callable
from typing import Any

import httpx

RABBIT_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBIT_PORT = int(os.getenv("RABBITMQ_MGMT_PORT", "15672"))
RABBIT_USER = os.getenv("RABBITMQ_USER", "ems")
RABBIT_PASS = os.getenv("RABBITMQ_PASS", "ems-dev-password")
MGMT_URL = f"http://{RABBIT_HOST}:{RABBIT_PORT}"


def wait_until(predicate: Callable[[], bool], timeout: float = 30.0, step: float = 0.5) -> bool:
    t_end = time.time() + timeout
    while time.time() < t_end:
        try:
            if predicate():
                return True
        except Exception:  # noqa: BLE001, S110
            pass
        time.sleep(step)
    return False


def restart_compose_stack(scale_matching: int = 1) -> None:
    """Cleanly restarts Compose stack with docker compose down -v && up -d --wait."""
    subprocess.run(["docker", "compose", "down", "-v"], check=True, capture_output=True)
    cmd = ["docker", "compose", "up", "-d", "--wait"]
    if scale_matching > 1:
        cmd.extend(["--scale", f"matching-service={scale_matching}"])
    subprocess.run(cmd, check=True, capture_output=True)


class RabbitManagementClient:
    """Helper for querying RabbitMQ Management HTTP API."""

    def __init__(
        self,
        base_url: str = MGMT_URL,
        user: str = RABBIT_USER,
        password: str = RABBIT_PASS,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.auth = (user, password)

    def _client(self) -> httpx.Client:
        return httpx.Client(base_url=self.base_url, auth=self.auth, timeout=5.0)

    def queue_info(self, queue_name: str, vhost: str = "%2F") -> dict[str, Any]:
        with self._client() as c:
            r = c.get(f"/api/queues/{vhost}/{queue_name}")
            if r.status_code == 404:
                return {}
            r.raise_for_status()
            return r.json()

    def queue_depth(self, queue_name: str, vhost: str = "%2F") -> int:
        info = self.queue_info(queue_name, vhost)
        return int(info.get("messages", 0))

    def messages_ready(self, queue_name: str, vhost: str = "%2F") -> int:
        info = self.queue_info(queue_name, vhost)
        return int(info.get("messages_ready", 0))

    def unacked_count(self, queue_name: str, vhost: str = "%2F") -> int:
        info = self.queue_info(queue_name, vhost)
        return int(info.get("messages_unacknowledged", 0))

    def consumer_count(self, queue_name: str, vhost: str = "%2F") -> int:
        info = self.queue_info(queue_name, vhost)
        return int(info.get("consumers", 0))

    def list_queues(self, vhost: str = "%2F") -> list[dict[str, Any]]:
        with self._client() as c:
            r = c.get(f"/api/queues/{vhost}")
            r.raise_for_status()
            return r.json()

    def list_exchanges(self, vhost: str = "%2F") -> list[dict[str, Any]]:
        with self._client() as c:
            r = c.get(f"/api/exchanges/{vhost}")
            r.raise_for_status()
            return r.json()

    def list_bindings(self, vhost: str = "%2F") -> list[dict[str, Any]]:
        with self._client() as c:
            r = c.get(f"/api/bindings/{vhost}")
            r.raise_for_status()
            return r.json()

    def purge_queue(self, queue_name: str, vhost: str = "%2F") -> None:
        with self._client() as c:
            r = c.delete(f"/api/queues/{vhost}/{queue_name}/contents")
            r.raise_for_status()

    def publish_message(
        self,
        exchange: str,
        routing_key: str,
        payload: dict[str, Any] | str,
        properties: dict[str, Any] | None = None,
        vhost: str = "%2F",
    ) -> bool:
        body_str = payload if isinstance(payload, str) else json.dumps(payload)
        req_body = {
            "properties": properties or {},
            "routing_key": routing_key,
            "payload": body_str,
            "payload_encoding": "string",
        }
        with self._client() as c:
            r = c.post(f"/api/exchanges/{vhost}/{exchange}/publish", json=req_body)
            r.raise_for_status()
            return bool(r.json().get("routed", False))
