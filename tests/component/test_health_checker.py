"""Component tests for coordinator health checker and failure detection (Build item 5.1)."""

import httpx
import pytest
from coordinator.main import Settings, create_app
from fastapi.testclient import TestClient

pytestmark = pytest.mark.component


class FakeNodeHandler:
    """Mock HTTP transport handler for fake node probes."""

    def __init__(self):
        self.probes_succeed = True
        self.probe_count = 0

    def handle(self, request):
        if request.url.path.endswith("/health"):
            self.probe_count += 1
            if self.probes_succeed:
                return httpx.Response(200, json={"status": "ok"})
            return httpx.Response(500, json={"status": "error"})
        return httpx.Response(404)


def test_one_missed_interval_does_nothing(tmp_path):
    """Missed heartbeat interval logs warning but does not mark node DOWN."""
    now = 100.0

    def clock():
        return now

    settings = Settings(
        state_dir=tmp_path,
        heartbeat_interval_sec=2.0,
        missed_heartbeats_limit=3,  # threshold = 6.0
        health_check_enabled=False,
    )
    app = create_app(settings=settings, clock=clock)
    client = TestClient(app)

    # Register node at t=100.0
    client.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s0-a",
            "shardId": 0,
            "endpoint": "http://s0-a:9000",
            "role": "PRIMARY",
            "epoch": 1,
            "status": "READY",
            "lastSeq": 0,
        },
    )
    assert app.state.registry.get_health("s0-a") == "UP"

    # Advance clock by 3s (exceeds one interval 2.0s, but below threshold 6.0s)
    now = 103.0
    app.state.health_checker.tick()
    assert app.state.registry.get_health("s0-a") == "UP"


def test_down_after_threshold_with_failing_probe(tmp_path):
    """When heartbeat age exceeds threshold and probe fails, node is marked DOWN."""
    now = 100.0

    def clock():
        return now

    handler = FakeNodeHandler()
    handler.probes_succeed = False
    fake_http = httpx.Client(transport=httpx.MockTransport(handler.handle))

    settings = Settings(
        state_dir=tmp_path,
        heartbeat_interval_sec=2.0,
        missed_heartbeats_limit=3,  # threshold = 6.0
        probe_timeout_sec=1.0,
        health_check_enabled=False,
    )
    app = create_app(settings=settings, http_client=fake_http, clock=clock)
    client = TestClient(app)

    client.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s0-a",
            "shardId": 0,
            "endpoint": "http://s0-a:9000",
            "role": "PRIMARY",
            "epoch": 1,
            "status": "READY",
            "lastSeq": 0,
        },
    )

    # Advance clock past threshold: elapsed = 7.0 > 6.0
    now = 107.0
    app.state.health_checker.tick()
    assert app.state.registry.get_health("s0-a") == "DOWN"
    assert handler.probe_count >= 1


def test_probe_success_keeps_node_up(tmp_path):
    """When heartbeat age exceeds threshold but probe succeeds, node stays UP."""
    now = 100.0

    def clock():
        return now

    handler = FakeNodeHandler()
    handler.probes_succeed = True
    fake_http = httpx.Client(transport=httpx.MockTransport(handler.handle))

    settings = Settings(
        state_dir=tmp_path,
        heartbeat_interval_sec=2.0,
        missed_heartbeats_limit=3,  # threshold = 6.0
        health_check_enabled=False,
    )
    app = create_app(settings=settings, http_client=fake_http, clock=clock)
    client = TestClient(app)

    client.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s0-a",
            "shardId": 0,
            "endpoint": "http://s0-a:9000",
            "role": "PRIMARY",
            "epoch": 1,
            "status": "READY",
            "lastSeq": 0,
        },
    )

    # Advance clock past threshold
    now = 107.0
    app.state.health_checker.tick()
    assert app.state.registry.get_health("s0-a") == "UP"
    assert handler.probe_count >= 1


def test_down_primary_gives_503_for_its_shard_only(tmp_path):
    """DOWN primary gives 503 + Retry-After for its shard while other shards function."""
    now = 100.0

    def clock():
        return now

    settings = Settings(
        state_dir=tmp_path,
        n_shards=3,
        heartbeat_interval_sec=2.0,
        missed_heartbeats_limit=3,
        health_check_enabled=False,
    )
    app = create_app(settings=settings, clock=clock)
    client = TestClient(app)

    # Register primaries for shards 0 and 1
    client.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s0-a",
            "shardId": 0,
            "endpoint": "http://s0-a:9000",
            "role": "PRIMARY",
            "epoch": 1,
            "status": "READY",
            "lastSeq": 0,
        },
    )
    client.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s1-a",
            "shardId": 1,
            "endpoint": "http://s1-a:9000",
            "role": "PRIMARY",
            "epoch": 1,
            "status": "READY",
            "lastSeq": 0,
        },
    )

    # Mark s0-a DOWN
    app.state.registry.set_health("s0-a", "DOWN")

    # Shard 0 request fails with 503 + Retry-After: 3
    # Key mapping to shard 0
    from ems_common.sharding import shard_of

    key_s0 = next(f"request:{i}" for i in range(100) if shard_of(f"request:{i}", 3) == 0)
    res0 = client.get(f"/internal/data/{key_s0}")
    assert res0.status_code == 503
    assert res0.headers.get("retry-after") == "3"

    # Shard 1 is still UP in registry (even if target is fake, resolution succeeds)
    assert app.state.registry.get_health("s1-a") == "UP"


def test_down_node_returns_to_up_on_heartbeat_or_register(tmp_path):
    """Node returning from DOWN to UP on heartbeat or register."""
    now = 100.0

    def clock():
        return now

    settings = Settings(state_dir=tmp_path, health_check_enabled=False)
    app = create_app(settings=settings, clock=clock)
    client = TestClient(app)

    client.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s0-a",
            "shardId": 0,
            "endpoint": "http://s0-a:9000",
            "role": "PRIMARY",
            "epoch": 1,
            "status": "READY",
            "lastSeq": 0,
        },
    )
    app.state.registry.set_health("s0-a", "DOWN")
    assert app.state.registry.get_health("s0-a") == "DOWN"

    # Heartbeat arrives -> UP
    client.post(
        "/internal/nodes/heartbeat",
        json={
            "nodeId": "s0-a",
            "shardId": 0,
            "role": "PRIMARY",
            "epoch": 1,
            "status": "READY",
            "lastSeq": 1,
            "replicaAckSeq": None,
        },
    )
    assert app.state.registry.get_health("s0-a") == "UP"

    # Mark DOWN again, then re-register -> UP
    app.state.registry.set_health("s0-a", "DOWN")
    client.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s0-a",
            "shardId": 0,
            "endpoint": "http://s0-a:9000",
            "role": "PRIMARY",
            "epoch": 1,
            "status": "READY",
            "lastSeq": 1,
        },
    )
    assert app.state.registry.get_health("s0-a") == "UP"


def test_hook_called_once_per_transition(tmp_path):
    """on_node_down hook is invoked exactly once when UP -> DOWN."""
    now = 100.0

    def clock():
        return now

    down_calls = []

    def on_down(node):
        down_calls.append(node.node_id)

    handler = FakeNodeHandler()
    handler.probes_succeed = False
    fake_http = httpx.Client(transport=httpx.MockTransport(handler.handle))

    settings = Settings(
        state_dir=tmp_path,
        heartbeat_interval_sec=2.0,
        missed_heartbeats_limit=3,
        health_check_enabled=False,
    )
    app = create_app(settings=settings, http_client=fake_http, clock=clock)
    client = TestClient(app)
    app.state.health_checker.set_on_node_down(on_down)

    client.post(
        "/internal/nodes/register",
        json={
            "nodeId": "s0-a",
            "shardId": 0,
            "endpoint": "http://s0-a:9000",
            "role": "PRIMARY",
            "epoch": 1,
            "status": "READY",
            "lastSeq": 0,
        },
    )

    now = 107.0
    # First tick -> probes, marks DOWN, calls hook
    app.state.health_checker.tick()
    assert len(down_calls) == 1
    assert down_calls[0] == "s0-a"

    # Second tick while still DOWN -> hook not called again!
    app.state.health_checker.tick()
    assert len(down_calls) == 1


def test_health_check_disabled_starts_no_thread(tmp_path):
    """HEALTH_CHECK_ENABLED=false does not start background thread."""
    settings = Settings(state_dir=tmp_path, health_check_enabled=False)
    app = create_app(settings=settings)
    with TestClient(app):
        assert app.state.health_checker._thread is None
