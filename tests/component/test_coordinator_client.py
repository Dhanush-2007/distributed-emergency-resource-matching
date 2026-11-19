"""Component tests for storage node coordinator registration and heartbeat client."""

import shutil
import tempfile
import threading
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient
from storage_node.coordinator_client import CoordinatorClient
from storage_node.db import StorageDB
from storage_node.main import create_app


@pytest.fixture
def temp_data_dir():
    d = tempfile.mkdtemp()
    yield Path(d)
    shutil.rmtree(d, ignore_errors=True)


@pytest.mark.component
def test_registration_payload_shape(temp_data_dir):
    """Verify the exact payload shape sent to /internal/nodes/register."""
    captured_register: dict[str, Any] = {}
    ready_event = threading.Event()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/internal/nodes/register":
            import json

            captured_register.update(json.loads(request.content))
            return httpx.Response(200, json={"role": "PRIMARY", "epoch": 1})
        return httpx.Response(200, json={"role": "PRIMARY", "epoch": 1})

    mock_client = httpx.Client(transport=httpx.MockTransport(handler))
    db = StorageDB(temp_data_dir / "s0-a.db")

    client = CoordinatorClient(
        node_id="s0-a",
        shard_id=0,
        advertise_url="http://s0-a:9000",
        initial_role="primary",
        coordinator_url="http://coordinator:8000",
        db=db,
        heartbeat_interval=0.05,
        http_timeout=1.0,
        http_client=mock_client,
        initial_backoff=0.01,
        max_backoff=0.02,
        on_status_change=lambda status: ready_event.set() if status == "READY" else None,
    )
    try:
        client.start()
        assert ready_event.wait(timeout=2.0)
        assert client.status == "READY"

        assert captured_register == {
            "nodeId": "s0-a",
            "shardId": 0,
            "endpoint": "http://s0-a:9000",
            "role": "PRIMARY",
            "epoch": 1,
            "lastSeq": 0,
            "status": "STARTING",
        }
    finally:
        client.stop()
        mock_client.close()


@pytest.mark.component
def test_retry_until_coordinator_comes_up(temp_data_dir):
    """Verify exponential backoff retries until the coordinator returns 200."""
    attempts = 0
    lock = threading.Lock()
    ready_event = threading.Event()

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        with lock:
            attempts += 1
            curr = attempts
        if curr == 1:
            raise httpx.ConnectError("Coordinator down")
        if curr == 2:
            return httpx.Response(500, json={"error": "Not ready"})
        return httpx.Response(200, json={"role": "PRIMARY", "epoch": 1})

    mock_client = httpx.Client(transport=httpx.MockTransport(handler))
    db = StorageDB(temp_data_dir / "s0-a.db")

    client = CoordinatorClient(
        node_id="s0-a",
        shard_id=0,
        advertise_url="http://s0-a:9000",
        initial_role="primary",
        coordinator_url="http://coordinator:8000",
        db=db,
        heartbeat_interval=0.05,
        http_timeout=1.0,
        http_client=mock_client,
        initial_backoff=0.01,
        max_backoff=0.04,
        on_status_change=lambda status: ready_event.set() if status == "READY" else None,
    )
    try:
        assert client.status == "STARTING"
        client.start()
        assert ready_event.wait(timeout=3.0)
        assert client.status == "READY"
        with lock:
            assert attempts >= 3
    finally:
        client.stop()
        mock_client.close()


@pytest.mark.component
def test_heartbeat_payload_and_record_count(temp_data_dir):
    """Verify heartbeat payload shape and recordCount by namespace."""
    captured_heartbeat: dict[str, Any] = {}
    heartbeat_event = threading.Event()

    def handler(request: httpx.Request) -> httpx.Response:
        import json

        if request.url.path == "/internal/nodes/register":
            return httpx.Response(200, json={"role": "PRIMARY", "epoch": 1})
        if request.url.path == "/internal/nodes/heartbeat":
            captured_heartbeat.update(json.loads(request.content))
            heartbeat_event.set()
            return httpx.Response(200, json={"role": "PRIMARY", "epoch": 1})
        return httpx.Response(404)

    mock_client = httpx.Client(transport=httpx.MockTransport(handler))
    db = StorageDB(temp_data_dir / "s0-a.db")

    # Seed data with different namespaces
    db.put("request:101", {"type": "water"}, if_version=0, epoch=1)
    db.put("request:102", {"type": "food"}, if_version=0, epoch=1)
    db.put("resource:201", {"name": "ambulance"}, if_version=0, epoch=1)
    db.put("other-key", {"note": "test"}, if_version=0, epoch=1)
    # Put and delete a record to ensure tombstones are excluded
    db.put("request:deleted", {"type": "shelter"}, if_version=0, epoch=1)
    db.delete("request:deleted", epoch=1)

    app = create_app(
        node_id="s0-a",
        shard_id=0,
        db_instance=db,
        standalone=False,
        coordinator_url="http://coordinator:8000",
        advertise_url="http://s0-a:9000",
        initial_role="primary",
        heartbeat_interval=0.05,
        http_timeout=1.0,
        http_client=mock_client,
        initial_backoff=0.01,
        max_backoff=0.02,
    )

    with TestClient(app) as tc:
        assert heartbeat_event.wait(timeout=2.0)
        assert captured_heartbeat == {
            "nodeId": "s0-a",
            "shardId": 0,
            "role": "PRIMARY",
            "epoch": 1,
            "status": "READY",
            "lastSeq": 6,
            "replicaAckSeq": None,
            "recordCount": {"request": 2, "resource": 1, "other": 1},
        }

        # Check /health endpoint
        health_resp = tc.get("/health")
        assert health_resp.status_code == 200
        health = health_resp.json()
        assert health["status"] == "READY"
        assert health["records"] == 4
        assert health["recordCount"] == {"request": 2, "resource": 1, "other": 1}

    mock_client.close()


@pytest.mark.component
def test_re_registration_after_404(temp_data_dir):
    """Verify that receiving a 404 on heartbeat causes re-registration."""
    call_log: list[str] = []
    re_registered = threading.Event()
    lock = threading.Lock()

    def handler(request: httpx.Request) -> httpx.Response:
        with lock:
            path = request.url.path
            call_log.append(path)
            if path == "/internal/nodes/register":
                if call_log.count("/internal/nodes/register") == 2:
                    re_registered.set()
                return httpx.Response(200, json={"role": "PRIMARY", "epoch": 2})
            if path == "/internal/nodes/heartbeat":
                # First heartbeat returns 404 (coordinator restarted)
                return httpx.Response(404, json={"detail": "Unknown node"})
        return httpx.Response(404)

    mock_client = httpx.Client(transport=httpx.MockTransport(handler))
    db = StorageDB(temp_data_dir / "s0-a.db")

    client = CoordinatorClient(
        node_id="s0-a",
        shard_id=0,
        advertise_url="http://s0-a:9000",
        initial_role="primary",
        coordinator_url="http://coordinator:8000",
        db=db,
        heartbeat_interval=0.03,
        http_timeout=1.0,
        http_client=mock_client,
        initial_backoff=0.01,
        max_backoff=0.02,
    )
    try:
        client.start()
        assert re_registered.wait(timeout=3.0)
        # Check that the new epoch 2 was saved to db meta
        assert db.stats()["epoch"] == 2
        with lock:
            assert call_log.count("/internal/nodes/register") >= 2
    finally:
        client.stop()
        mock_client.close()


@pytest.mark.component
def test_node_survives_coordinator_downtime(temp_data_dir):
    """Verify node does not crash when coordinator experiences downtime."""
    first_registered = threading.Event()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/internal/nodes/register":
            first_registered.set()
            return httpx.Response(200, json={"role": "PRIMARY", "epoch": 1})
        # Heartbeat connection error
        raise httpx.ConnectError("Network is down")

    mock_client = httpx.Client(transport=httpx.MockTransport(handler))
    db = StorageDB(temp_data_dir / "s0-a.db")

    app = create_app(
        node_id="s0-a",
        shard_id=0,
        db_instance=db,
        standalone=False,
        coordinator_url="http://coordinator:8000",
        advertise_url="http://s0-a:9000",
        initial_role="primary",
        heartbeat_interval=0.02,
        http_timeout=1.0,
        http_client=mock_client,
        initial_backoff=0.01,
        max_backoff=0.02,
    )

    with TestClient(app) as tc:
        assert first_registered.wait(timeout=2.0)
        import time

        time.sleep(0.1)  # Let multiple failed heartbeats happen
        # Node must still be responsive
        resp = tc.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "READY"

        # Local reads and writes still work
        put_resp = tc.put("/internal/data/k:local", json={"value": {"x": 1}})
        assert put_resp.status_code == 201

    mock_client.close()


@pytest.mark.component
def test_standalone_true_makes_no_outbound_calls(temp_data_dir):
    """When STANDALONE=true, no outbound coordinator calls are made."""
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return httpx.Response(200, json={"role": "PRIMARY", "epoch": 1})

    mock_client = httpx.Client(transport=httpx.MockTransport(handler))
    db = StorageDB(temp_data_dir / "s0-a.db")

    app = create_app(
        node_id="s0-a",
        shard_id=0,
        db_instance=db,
        standalone=True,
        coordinator_url="http://coordinator:8000",
        http_client=mock_client,
    )

    with TestClient(app) as tc:
        h = tc.get("/health")
        assert h.status_code == 200
        assert h.json()["status"] == "READY"

        p = tc.put("/internal/data/request:one", json={"value": {"test": 1}})
        assert p.status_code == 201

    assert len(calls) == 0
    mock_client.close()


@pytest.mark.component
def test_reject_writes_503_until_ready(temp_data_dir):
    """When STANDALONE=false, writes are rejected with 503 until status is READY."""

    def handler(request: httpx.Request) -> httpx.Response:
        # Never succeed registration
        raise httpx.ConnectError("Coordinator unreachable")

    mock_client = httpx.Client(transport=httpx.MockTransport(handler))
    db = StorageDB(temp_data_dir / "s0-a.db")

    app = create_app(
        node_id="s0-a",
        shard_id=0,
        db_instance=db,
        standalone=False,
        coordinator_url="http://coordinator:8000",
        advertise_url="http://s0-a:9000",
        http_client=mock_client,
        initial_backoff=0.05,
        max_backoff=0.1,
    )

    with TestClient(app) as tc:
        # Check /health reports STARTING
        h = tc.get("/health")
        assert h.status_code == 200
        assert h.json()["status"] == "STARTING"

        # PUT write rejected with 503 STORAGE_UNAVAILABLE
        put_resp = tc.put(
            "/internal/data/request:001",
            json={"value": {"test": 1}},
        )
        assert put_resp.status_code == 503
        err = put_resp.json()
        assert err["error"]["code"] == "STORAGE_UNAVAILABLE"

        # DELETE write rejected with 503 STORAGE_UNAVAILABLE
        del_resp = tc.delete("/internal/data/request:001")
        assert del_resp.status_code == 503
        assert del_resp.json()["error"]["code"] == "STORAGE_UNAVAILABLE"

        # GET is not a write - missing key returns 404, not 503
        get_resp = tc.get("/internal/data/request:001")
        assert get_resp.status_code == 404

    mock_client.close()


@pytest.mark.component
def test_writes_enforce_coordinator_role_and_epoch(temp_data_dir):
    """Verify that role and epoch returned by coordinator are saved and enforced."""
    registered = threading.Event()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/internal/nodes/register":
            registered.set()
            # Coordinator assigns role REPLICA and epoch 5
            return httpx.Response(200, json={"role": "REPLICA", "epoch": 5})
        return httpx.Response(200, json={"role": "REPLICA", "epoch": 5})

    mock_client = httpx.Client(transport=httpx.MockTransport(handler))
    db = StorageDB(temp_data_dir / "s0-a.db")

    app = create_app(
        node_id="s0-a",
        shard_id=0,
        db_instance=db,
        standalone=False,
        coordinator_url="http://coordinator:8000",
        advertise_url="http://s0-a:9000",
        http_client=mock_client,
        heartbeat_interval=0.1,
        initial_backoff=0.01,
        max_backoff=0.02,
    )

    with TestClient(app) as tc:
        assert registered.wait(timeout=2.0)
        import time

        time.sleep(0.05)

        h = tc.get("/health").json()
        assert h["role"] == "REPLICA"
        assert h["epoch"] == 5

        # Write must be rejected with 409 NOT_PRIMARY
        put_resp = tc.put(
            "/internal/data/request:001",
            json={"value": {"test": 1}},
        )
        assert put_resp.status_code == 409
        assert put_resp.json()["error"]["code"] == "NOT_PRIMARY"

    mock_client.close()
