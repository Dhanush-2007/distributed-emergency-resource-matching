"""Component pair test harness using httpx.MockTransport (Build item 4.3)."""

from typing import Any

import httpx
from starlette.testclient import TestClient
from storage_node.db import StorageDB
from storage_node.main import create_app


class FakeCoordinator:
    def __init__(self):
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.should_fail = False

    def handle(self, request: httpx.Request) -> httpx.Response:
        url_path = request.url.path
        import json
        body = json.loads(request.content.decode()) if request.content else {}
        self.calls.append((url_path, body))

        if self.should_fail:
            return httpx.Response(status_code=500, json={"error": {"code": "INTERNAL_ERROR"}})

        if url_path == "/internal/nodes/degraded":
            return httpx.Response(status_code=200, json={"state": "DEGRADED"})

        if url_path == "/internal/nodes/register":
            role = body.get("role", "PRIMARY").upper()
            return httpx.Response(status_code=200, json={"role": role, "epoch": body.get("epoch", 1)})

        if url_path == "/internal/nodes/heartbeat":
            role = body.get("role", "PRIMARY").upper()
            return httpx.Response(status_code=200, json={"role": role, "epoch": body.get("epoch", 1)})

        return httpx.Response(status_code=404, json={"error": {"code": "NOT_FOUND"}})


class StoragePairHarness:
    """Wires primary and replica apps together via MockTransport with a fake coordinator."""

    def __init__(self, tmp_path):
        self.tmp_path = tmp_path
        self.fake_coord = FakeCoordinator()

        self.p_db_path = tmp_path / "p.db"
        self.r_db_path = tmp_path / "r.db"

        self.primary_db = StorageDB(self.p_db_path)
        self.replica_db = StorageDB(self.r_db_path)

        self.primary_db.update_meta(role="PRIMARY", epoch=1)
        self.replica_db.update_meta(role="REPLICA", epoch=1)

        self.primary_app = None
        self.replica_app = None
        self.p_client = None
        self.r_client = None

        self.build_apps()

    def build_apps(self):
        # We need mock transports that route peer and coord URLs
        def primary_transport_handler(request: httpx.Request) -> httpx.Response:
            url_str = str(request.url)
            if "coordinator" in url_str:
                return self.fake_coord.handle(request)
            if "replica" in url_str:
                # Forward to replica app via test client
                sub_path = request.url.raw_path.decode()
                r_resp = self.r_client.request(
                    method=request.method,
                    url=sub_path,
                    content=request.content,
                    headers=dict(request.headers),
                )
                return httpx.Response(
                    status_code=r_resp.status_code,
                    content=r_resp.content,
                    headers=dict(r_resp.headers),
                )
            return httpx.Response(status_code=404)

        def replica_transport_handler(request: httpx.Request) -> httpx.Response:
            url_str = str(request.url)
            if "coordinator" in url_str:
                return self.fake_coord.handle(request)
            if "primary" in url_str:
                sub_path = request.url.raw_path.decode()
                p_resp = self.p_client.request(
                    method=request.method,
                    url=sub_path,
                    content=request.content,
                    headers=dict(request.headers),
                )
                return httpx.Response(
                    status_code=p_resp.status_code,
                    content=p_resp.content,
                    headers=dict(p_resp.headers),
                )
            return httpx.Response(status_code=404)

        p_http = httpx.Client(transport=httpx.MockTransport(primary_transport_handler))
        r_http = httpx.Client(transport=httpx.MockTransport(replica_transport_handler))

        self.primary_app = create_app(
            node_id="s0-a",
            shard_id=0,
            db_instance=self.primary_db,
            standalone=False,
            coordinator_url="http://coordinator:8000",
            advertise_url="http://primary:9000",
            peer_url="http://replica:9000",
            http_client=p_http,
        )
        self.replica_app = create_app(
            node_id="s0-b",
            shard_id=0,
            db_instance=self.replica_db,
            standalone=False,
            coordinator_url="http://coordinator:8000",
            advertise_url="http://replica:9000",
            peer_url="http://primary:9000",
            initial_role="replica",
            http_client=r_http,
        )

        self.p_client = TestClient(self.primary_app)
        self.r_client = TestClient(self.replica_app)
