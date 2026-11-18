"""Component tests for storage node REST API."""

import shutil
import tempfile
import uuid
from pathlib import Path

import pytest
from app.main import create_app
from fastapi.testclient import TestClient


@pytest.fixture
def temp_data_dir():
    d = tempfile.mkdtemp()
    yield Path(d)
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def client(temp_data_dir):
    app = create_app(node_id="s0-a", shard_id=0, data_dir=temp_data_dir)
    return TestClient(app, raise_server_exceptions=False)


@pytest.mark.component
def test_create_201_then_update_200(client):
    key = "request:3f2c9a1e-0001"

    # Create -> 201
    res1 = client.put(
        f"/internal/data/{key}",
        json={"value": {"type": "water", "status": "PENDING"}, "ifVersion": 0},
    )
    assert res1.status_code == 201
    body1 = res1.json()
    assert body1["key"] == key
    assert body1["version"] == 1
    assert body1["seq"] == 1
    assert body1["replicated"] is False

    # Update -> 200
    res2 = client.put(
        f"/internal/data/{key}",
        json={"value": {"type": "water", "status": "MATCHED"}, "ifVersion": 1},
    )
    assert res2.status_code == 200
    body2 = res2.json()
    assert body2["key"] == key
    assert body2["version"] == 2
    assert body2["seq"] == 2
    assert body2["replicated"] is False


@pytest.mark.component
def test_get_after_put(client):
    key = "resource:9a8b7c6d-0002"
    client.put(f"/internal/data/{key}", json={"value": {"name": "water truck"}})

    res = client.get(f"/internal/data/{key}")
    assert res.status_code == 200
    assert res.json() == {
        "key": key,
        "value": {"name": "water truck"},
        "version": 1,
    }


@pytest.mark.component
def test_missing_and_deleted_key_404(client):
    key = "request:missing-uuid"

    # Missing key -> 404
    res1 = client.get(f"/internal/data/{key}")
    assert res1.status_code == 404
    err1 = res1.json()
    assert err1["error"]["code"] == "NOT_FOUND"
    assert "details" in err1["error"]

    # Delete missing key -> 404
    res_del_missing = client.delete(f"/internal/data/{key}")
    assert res_del_missing.status_code == 404
    assert res_del_missing.json()["error"]["code"] == "NOT_FOUND"

    # Create then delete
    client.put(f"/internal/data/{key}", json={"value": {"a": 1}})
    res_del = client.delete(f"/internal/data/{key}")
    assert res_del.status_code == 200
    assert res_del.json()["key"] == key

    # Deleted key GET -> 404
    res2 = client.get(f"/internal/data/{key}")
    assert res2.status_code == 404
    assert res2.json()["error"]["code"] == "NOT_FOUND"

    # Second DELETE on deleted key -> 404
    res3 = client.delete(f"/internal/data/{key}")
    assert res3.status_code == 404
    assert res3.json()["error"]["code"] == "NOT_FOUND"


@pytest.mark.component
def test_create_only_conflict_and_stale_if_version(client):
    key = "request:conflict-test"
    client.put(f"/internal/data/{key}", json={"value": {"x": 10}, "ifVersion": 0})

    # ifVersion=0 when key exists -> 409 VERSION_CONFLICT
    res_conflict = client.put(
        f"/internal/data/{key}", json={"value": {"x": 20}, "ifVersion": 0}
    )
    assert res_conflict.status_code == 409
    err = res_conflict.json()
    assert err["error"]["code"] == "VERSION_CONFLICT"
    assert "details" in err["error"]

    # stale ifVersion -> 409 VERSION_CONFLICT
    res_stale = client.put(
        f"/internal/data/{key}", json={"value": {"x": 30}, "ifVersion": 99}
    )
    assert res_stale.status_code == 409
    assert res_stale.json()["error"]["code"] == "VERSION_CONFLICT"


@pytest.mark.component
def test_wrong_epoch_409_stale_epoch(client):
    key = "request:epoch-test"

    # PUT with mismatched epoch
    res_put = client.put(
        f"/internal/data/{key}",
        json={"value": {"data": 1}, "epoch": 999},
    )
    assert res_put.status_code == 409
    err_put = res_put.json()
    assert err_put["error"]["code"] == "STALE_EPOCH"
    assert "details" in err_put["error"]

    # DELETE with mismatched epoch
    res_del = client.delete(f"/internal/data/{key}?epoch=999")
    assert res_del.status_code == 409
    assert res_del.json()["error"]["code"] == "STALE_EPOCH"


@pytest.mark.component
def test_non_object_value_400_invalid_input(client):
    key = "request:bad-value"

    # String instead of object
    res1 = client.put(f"/internal/data/{key}", json={"value": "string value"})
    assert res1.status_code == 400
    err1 = res1.json()
    assert err1["error"]["code"] == "INVALID_INPUT"
    assert any("value" in d["field"] for d in err1["error"]["details"])

    # List instead of object
    res2 = client.put(f"/internal/data/{key}", json={"value": [1, 2, 3]})
    assert res2.status_code == 400
    assert res2.json()["error"]["code"] == "INVALID_INPUT"


@pytest.mark.component
def test_malformed_json_400_not_422(client):
    key = "request:bad-json"
    res = client.put(
        f"/internal/data/{key}",
        content="not valid json {",
        headers={"Content-Type": "application/json"},
    )
    assert res.status_code == 400
    err = res.json()
    assert err["error"]["code"] == "INVALID_INPUT"
    assert "details" in err["error"]


@pytest.mark.component
def test_keys_containing_colons_and_uuids(client):
    u = str(uuid.uuid4())
    key = f"request:flood:zone-1:{u}"

    res_put = client.put(f"/internal/data/{key}", json={"value": {"id": u}})
    assert res_put.status_code == 201

    res_get = client.get(f"/internal/data/{key}")
    assert res_get.status_code == 200
    assert res_get.json()["key"] == key
    assert res_get.json()["value"] == {"id": u}


@pytest.mark.component
def test_scan_with_and_without_prefix(client):
    client.put("internal/data/request:1", json={"value": {"t": 1}})
    client.put("internal/data/request:2", json={"value": {"t": 2}})
    client.put("internal/data/resource:1", json={"value": {"r": 1}})

    # With prefix
    res_prefix = client.get("/internal/scan?prefix=request:")
    assert res_prefix.status_code == 200
    body_p = res_prefix.json()
    assert body_p["count"] == 2
    assert [item["key"] for item in body_p["items"]] == ["request:1", "request:2"]

    # Without prefix (returns all live items)
    res_all = client.get("/internal/scan")
    assert res_all.status_code == 200
    body_all = res_all.json()
    assert body_all["count"] == 3


@pytest.mark.component
def test_health_values_and_echoed_correlation_id(client):
    cid = "trace-test-uuid-999"

    # Make several writes with correlation header
    for i in range(3):
        res = client.put(
            f"/internal/data/k:{i}",
            json={"value": {"i": i}},
            headers={"X-Correlation-Id": cid},
        )
        assert res.status_code == 201
        assert res.headers.get("X-Correlation-Id") == cid

    # Check /health values
    h_res = client.get("/health", headers={"X-Correlation-Id": cid})
    assert h_res.status_code == 200
    assert h_res.headers.get("X-Correlation-Id") == cid
    h_data = h_res.json()
    assert h_data["nodeId"] == "s0-a"
    assert h_data["shardId"] == 0
    assert h_data["role"] == "PRIMARY"
    assert h_data["status"] == "READY"
    assert h_data["epoch"] == 1
    assert h_data["lastSeq"] == 3
    assert h_data["records"] == 3


@pytest.mark.component
def test_app_recreation_on_same_data_dir_persists(temp_data_dir):
    # 1. Start app, write 20 keys
    app1 = create_app(node_id="s0-a", shard_id=0, data_dir=temp_data_dir)
    client1 = TestClient(app1)

    for i in range(20):
        client1.put(f"/internal/data/persist:{i}", json={"value": {"index": i}})

    h1 = client1.get("/health").json()
    assert h1["records"] == 20
    assert h1["lastSeq"] == 20

    # 2. Re-create app on the exact same directory
    app2 = create_app(node_id="s0-a", shard_id=0, data_dir=temp_data_dir)
    client2 = TestClient(app2)

    h2 = client2.get("/health").json()
    assert h2["records"] == 20
    assert h2["lastSeq"] == 20

    # Verify all 20 keys are present and readable
    for i in range(20):
        res = client2.get(f"/internal/data/persist:{i}")
        assert res.status_code == 200
        assert res.json()["value"] == {"index": i}

    # New write continues contiguous sequence
    res_next = client2.put("/internal/data/persist:20", json={"value": {"index": 20}})
    assert res_next.status_code == 201
    assert res_next.json()["seq"] == 21
