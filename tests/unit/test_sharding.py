"""Unit tests for sharding logic."""

import hashlib
import json
from pathlib import Path
import uuid

import pytest
from ems_common.sharding import shard_of


@pytest.mark.unit
def test_shard_vectors():
    """Verify shard_of matches shard_vectors.json and direct hashlib computation."""
    vector_file = Path(__file__).parent / "shard_vectors.json"
    with open(vector_file, "r", encoding="utf-8") as f:
        vectors = json.load(f)

    assert len(vectors) >= 20

    for item in vectors:
        key = item["key"]
        expected_shard = item["shard"]

        # Recompute expected with hashlib directly
        digest = hashlib.sha256(key.encode("utf-8")).digest()
        direct_shard = int.from_bytes(digest[:8], "big") % 3

        assert expected_shard == direct_shard
        assert shard_of(key) == direct_shard


@pytest.mark.unit
def test_shard_determinism():
    """Verify shard_of returns identical result across repeated invocations."""
    key = "request:123e4567-e89b-12d3-a456-426614174000"
    first = shard_of(key)
    for _ in range(50):
        assert shard_of(key) == first


@pytest.mark.unit
def test_shard_distribution_300_keys():
    """Assert 300 random UUID keys land on all 3 shards."""
    counts = {0: 0, 1: 0, 2: 0}
    for _ in range(300):
        key = f"request:{uuid.uuid4()}"
        shard = shard_of(key, n_shards=3)
        assert shard in (0, 1, 2)
        counts[shard] += 1

    # Every shard must have received at least some keys
    for shard_id in (0, 1, 2):
        assert counts[shard_id] > 0, f"Shard {shard_id} received no keys"
