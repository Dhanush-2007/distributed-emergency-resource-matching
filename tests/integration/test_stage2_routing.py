"""Integration tests for coordinator routing against a running stack."""

import os
import uuid

import httpx
import pytest
from ems_common.sharding import shard_of

COORDINATOR_URL = os.getenv("COORDINATOR_URL", "http://localhost:8000").rstrip("/")


def _is_coordinator_reachable() -> bool:
    try:
        resp = httpx.get(f"{COORDINATOR_URL}/health/live", timeout=2.0)
        return resp.status_code == 200
    except (httpx.RequestError, httpx.HTTPError, OSError):
        return False


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not _is_coordinator_reachable(),
        reason=f"Coordinator unreachable at {COORDINATOR_URL}. Integration tests require a running stack.",
    ),
]


@pytest.fixture
def http_client():
    with httpx.Client(base_url=COORDINATOR_URL, timeout=5.0) as client:
        yield client


def test_diagnostics_list_3_nodes_up(http_client):
    """Diagnostics endpoints report all nodes UP and HEALTHY."""
    res = http_client.get("/diagnostics/nodes")
    assert res.status_code == 200
    nodes = res.json()
    assert len(nodes) in (3, 6)
    for node in nodes:
        assert node["health"] == "UP"
        assert node["status"] == "READY"
        assert node["role"] in ("PRIMARY", "REPLICA")
    primaries = [n for n in nodes if n["role"] == "PRIMARY"]
    assert len(primaries) == 3

    shards_res = http_client.get("/diagnostics/shards")
    assert shards_res.status_code == 200
    shards = shards_res.json()
    assert len(shards) == 3
    for shard in shards:
        assert shard["state"] == "HEALTHY"
        assert shard["primary"] in ("s0-a", "s1-a", "s2-a")


def test_same_key_always_routes_to_same_shard(http_client):
    """The exact same key always routes to the exact same shard."""
    key = f"request:{uuid.uuid4()}"
    expected_shard = shard_of(key, 3)

    for _ in range(5):
        route_res = http_client.get("/diagnostics/route", params={"key": key})
        assert route_res.status_code == 200
        assert route_res.json()["shardId"] == expected_shard


def test_keys_cover_shards_0_1_2(http_client):
    """Writing random keys spreads data across all three shards (0, 1, and 2)."""
    seen_shards = set()
    for _ in range(30):
        key = f"request:{uuid.uuid4()}"
        sid = shard_of(key, 3)
        put_res = http_client.put(f"/internal/data/{key}", json={"value": {"test": 1}})
        assert put_res.status_code in (200, 201)
        seen_shards.add(sid)
        if len(seen_shards) == 3:
            break

    assert seen_shards == {0, 1, 2}


def test_put_get_delete_round_trip(http_client):
    """Full PUT, GET, and DELETE round trip through the coordinator."""
    key = f"resource:{uuid.uuid4()}"
    val = {"name": "test ambulance", "status": "AVAILABLE"}

    # PUT -> 201
    put_res = http_client.put(
        f"/internal/data/{key}", json={"value": val, "ifVersion": 0}
    )
    assert put_res.status_code == 201
    put_body = put_res.json()
    assert put_body["key"] == key
    assert put_body["version"] == 1

    # GET -> 200
    get_res = http_client.get(f"/internal/data/{key}")
    assert get_res.status_code == 200
    get_body = get_res.json()
    assert get_body["key"] == key
    assert get_body["value"] == val
    assert get_body["version"] == 1

    # DELETE -> 200
    del_res = http_client.delete(f"/internal/data/{key}")
    assert del_res.status_code == 200
    del_body = del_res.json()
    assert del_body["key"] == key

    # GET deleted -> 404
    get_after_del = http_client.get(f"/internal/data/{key}")
    assert get_after_del.status_code == 404
    assert get_after_del.json()["error"]["code"] == "NOT_FOUND"


def test_if_version_0_conflict_is_409(http_client):
    """Attempting ifVersion=0 on an existing key returns 409 VERSION_CONFLICT."""
    key = f"request:{uuid.uuid4()}"

    # Initial create
    res1 = http_client.put(
        f"/internal/data/{key}", json={"value": {"x": 1}, "ifVersion": 0}
    )
    assert res1.status_code == 201

    # Conflict with ifVersion=0
    res2 = http_client.put(
        f"/internal/data/{key}", json={"value": {"x": 2}, "ifVersion": 0}
    )
    assert res2.status_code == 409
    err = res2.json()
    assert err["error"]["code"] == "VERSION_CONFLICT"


def test_get_of_missing_key_is_404(http_client):
    """GET of an unknown key returns 404 NOT_FOUND."""
    missing_key = f"request:{uuid.uuid4()}"
    res = http_client.get(f"/internal/data/{missing_key}")
    assert res.status_code == 404
    err = res.json()
    assert err["error"]["code"] == "NOT_FOUND"


def test_scan_with_prefix_merges_all_shards(http_client):
    """GET /internal/scan with a prefix merges items from all shards and returns sorted."""
    scan_prefix = f"itest-{uuid.uuid4().hex[:8]}:"

    # Write keys matching this prefix that cover multiple shards
    written_keys = []
    for i in range(15):
        k = f"{scan_prefix}{i:02d}"
        http_client.put(f"/internal/data/{k}", json={"value": {"num": i}})
        written_keys.append(k)

    scan_res = http_client.get("/internal/scan", params={"prefix": scan_prefix})
    assert scan_res.status_code == 200
    scan_data = scan_res.json()
    assert scan_data["count"] == len(written_keys)
    assert scan_data["partial"] is False

    returned_keys = [item["key"] for item in scan_data["items"]]
    assert returned_keys == sorted(written_keys)


def test_data_responses_contain_no_node_endpoint(http_client):
    """Data operation responses never expose physical node endpoints or internal IPs."""
    key = f"request:{uuid.uuid4()}"

    put_res = http_client.put(f"/internal/data/{key}", json={"value": {"secure": True}})
    assert "9000" not in put_res.text
    assert "s0-a" not in put_res.text
    assert "s1-a" not in put_res.text
    assert "s2-a" not in put_res.text
    assert "http://" not in put_res.text

    get_res = http_client.get(f"/internal/data/{key}")
    assert "http://" not in get_res.text
    assert "9000" not in get_res.text

    del_res = http_client.delete(f"/internal/data/{key}")
    assert "http://" not in del_res.text
    assert "9000" not in del_res.text
