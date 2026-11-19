"""Unit tests validating Postman collection and environment artifacts."""

import json
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit


def test_postman_artifacts_validity():
    coll_path = Path("postman/ems.postman_collection.json")
    env_path = Path("postman/ems.postman_environment.json")

    assert coll_path.is_file(), "postman_collection.json does not exist"
    assert env_path.is_file(), "postman_environment.json does not exist"

    with open(coll_path, "r", encoding="utf-8") as f:
        collection = json.load(f)

    with open(env_path, "r", encoding="utf-8") as f:
        environment = json.load(f)

    # Check environment variables
    env_vars = {v["key"]: v["value"] for v in environment.get("values", [])}
    assert "baseUrlRequest" in env_vars
    assert "baseUrlResource" in env_vars
    assert "requestId" in env_vars
    assert "resourceId" in env_vars

    items = collection.get("item", [])
    assert len(items) >= 12, f"Expected at least 12 requests in collection, got {len(items)}"

    # Check for all seven public method + path pairs
    expected_pairs = {
        ("POST", "requests"),
        ("GET", "requests/{{requestId}}"),
        ("PUT", "requests/{{requestId}}/status"),
        ("POST", "match/{{requestId}}"),
        ("POST", "resources"),
        ("GET", "resources"),
        ("PUT", "resources/{{resourceId}}/status"),
    }

    found_pairs = set()
    for item in items:
        req = item.get("request", {})
        method = req.get("method", "").upper()
        url = req.get("url", {})
        path_list = url.get("path", [])
        path_str = "/".join(path_list)
        found_pairs.add((method, path_str))

    for exp_method, exp_path in expected_pairs:
        assert (exp_method, exp_path) in found_pairs, f"Missing pair ({exp_method}, {exp_path}) in collection"
