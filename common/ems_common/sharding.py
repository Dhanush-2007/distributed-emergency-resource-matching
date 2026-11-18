"""Sharding logic for Distributed Emergency Resource Matching System."""

import hashlib


def shard_of(key: str, n_shards: int = 3) -> int:
    """Compute the shard index for a given key string.

    Uses the first 8 bytes of SHA-256(key) modulo n_shards.
    """
    digest = hashlib.sha256(key.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % n_shards
