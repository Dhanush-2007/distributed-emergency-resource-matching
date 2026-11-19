"""Single-shard cluster harness with real coordinator and two real storage nodes."""

from __future__ import annotations

from pathlib import Path

import httpx
from coordinator.main import Settings as CoordSettings
from coordinator.main import create_app as create_coord_app
from starlette.testclient import TestClient
from storage_node.db import StorageDB
from storage_node.main import create_app as create_storage_app


class FakeClusterClock:
    """Deterministic shared clock for harness tests."""

    def __init__(self, start: float = 1000.0) -> None:
        self._time = start

    def __call__(self) -> float:
        return self._time

    def advance(self, seconds: float) -> None:
        self._time += seconds


class SingleShardClusterHarness:
    """Manages an in-process single-shard cluster (coordinator + s1-a + s1-b)."""

    def __init__(self, tmp_path: Path, clock: FakeClusterClock | None = None) -> None:
        self.tmp_path = tmp_path
        self.clock = clock or FakeClusterClock(1000.0)
        self.state_dir = tmp_path / "coord_state"
        self.s1_a_db_path = tmp_path / "s1-a.db"
        self.s1_b_db_path = tmp_path / "s1-b.db"

        self.s1_a_db = StorageDB(self.s1_a_db_path)
        self.s1_b_db = StorageDB(self.s1_b_db_path)

        self.s1_a_db.update_meta(role="PRIMARY", epoch=1)
        self.s1_b_db.update_meta(role="REPLICA", epoch=1)

        self.down_nodes: set[str] = set()
        self.closed = False

        self._coord_client: TestClient | None = None
        self._s1_a_client: TestClient | None = None
        self._s1_b_client: TestClient | None = None

        self.build_apps()

    def set_down(self, node_id: str, is_down: bool = True) -> None:
        """Mark a node as reachable or unreachable (raises ConnectError)."""
        if is_down:
            self.down_nodes.add(node_id)
        else:
            self.down_nodes.discard(node_id)

    def _route_request(self, target: str, request: httpx.Request) -> httpx.Response:
        if self.closed or target in self.down_nodes:
            raise httpx.ConnectError(f"Node {target} is unreachable")

        sub_path = request.url.raw_path.decode()
        if target == "coordinator":
            assert self._coord_client is not None
            resp = self._coord_client.request(
                method=request.method,
                url=sub_path,
                content=request.content,
                headers=dict(request.headers),
            )
        elif target == "s1-a":
            assert self._s1_a_client is not None
            resp = self._s1_a_client.request(
                method=request.method,
                url=sub_path,
                content=request.content,
                headers=dict(request.headers),
            )
        elif target == "s1-b":
            assert self._s1_b_client is not None
            resp = self._s1_b_client.request(
                method=request.method,
                url=sub_path,
                content=request.content,
                headers=dict(request.headers),
            )
        else:
            return httpx.Response(404, json={"error": f"Unknown target {target}"})

        return httpx.Response(
            status_code=resp.status_code,
            content=resp.content,
            headers=dict(resp.headers),
        )

    def build_apps(self) -> None:
        def coord_transport_handler(request: httpx.Request) -> httpx.Response:
            url_str = str(request.url)
            if "s1-a" in url_str:
                return self._route_request("s1-a", request)
            if "s1-b" in url_str:
                return self._route_request("s1-b", request)
            return httpx.Response(404)

        def s1_a_transport_handler(request: httpx.Request) -> httpx.Response:
            url_str = str(request.url)
            if "coordinator" in url_str:
                return self._route_request("coordinator", request)
            if "s1-b" in url_str:
                return self._route_request("s1-b", request)
            return httpx.Response(404)

        def s1_b_transport_handler(request: httpx.Request) -> httpx.Response:
            url_str = str(request.url)
            if "coordinator" in url_str:
                return self._route_request("coordinator", request)
            if "s1-a" in url_str:
                return self._route_request("s1-a", request)
            return httpx.Response(404)

        coord_http = httpx.Client(transport=httpx.MockTransport(coord_transport_handler))
        s1_a_http = httpx.Client(transport=httpx.MockTransport(s1_a_transport_handler))
        s1_b_http = httpx.Client(transport=httpx.MockTransport(s1_b_transport_handler))

        coord_settings = CoordSettings(
            state_dir=self.state_dir,
            n_shards=1,
            heartbeat_interval_sec=2.0,
            missed_heartbeats_limit=3,
            health_check_interval_sec=1.0,
            probe_timeout_sec=0.5,
        )
        coord_settings.health_check_enabled = False

        self.coord_app = create_coord_app(
            settings=coord_settings,
            http_client=coord_http,
            clock=self.clock,
        )

        self.s1_a_app = create_storage_app(
            node_id="s1-a",
            shard_id=0,
            db_instance=self.s1_a_db,
            standalone=False,
            coordinator_url="http://coordinator:8000",
            advertise_url="http://s1-a:9000",
            peer_url="http://s1-b:9000",
            initial_role="PRIMARY",
            heartbeat_interval=2.0,
            http_client=s1_a_http,
        )

        self.s1_b_app = create_storage_app(
            node_id="s1-b",
            shard_id=0,
            db_instance=self.s1_b_db,
            standalone=False,
            coordinator_url="http://coordinator:8000",
            advertise_url="http://s1-b:9000",
            peer_url="http://s1-a:9000",
            initial_role="REPLICA",
            heartbeat_interval=2.0,
            http_client=s1_b_http,
        )

        self._coord_client = TestClient(self.coord_app)
        self._s1_a_client = TestClient(self.s1_a_app)
        self._s1_b_client = TestClient(self.s1_b_app)

    @property
    def coord(self) -> TestClient:
        assert self._coord_client is not None
        return self._coord_client

    @property
    def s1_a(self) -> TestClient:
        assert self._s1_a_client is not None
        return self._s1_a_client

    @property
    def s1_b(self) -> TestClient:
        assert self._s1_b_client is not None
        return self._s1_b_client

    def tick_coordinator(self) -> None:
        """Tick coordinator health checker."""
        self.coord_app.state.health_checker.tick()

    def tick_heartbeat(self, node_id: str) -> None:
        """Run a manual heartbeat cycle for a storage node."""
        app = self.s1_a_app if node_id == "s1-a" else self.s1_b_app
        client = app.state.get_coord_client()
        if client:
            client.run_once()

    def tick_replica_monitor(self, node_id: str) -> None:
        """Run a manual replica monitor probe from a storage node."""
        app = self.s1_a_app if node_id == "s1-a" else self.s1_b_app
        rm = app.state.get_replica_monitor()
        if rm:
            rm.run_once()

    def close(self) -> None:
        self.closed = True
        for app in (self.s1_a_app, self.s1_b_app):
            if app is not None:
                rm = getattr(app.state, "get_recovery_manager", lambda: None)()
                if rm:
                    rm.stop(timeout=1.0)
                rep = getattr(app.state, "get_replica_monitor", lambda: None)()
                if rep:
                    rep.stop(timeout=1.0)
                cc = getattr(app.state, "get_coord_client", lambda: None)()
                if cc:
                    cc.stop(timeout=1.0)
