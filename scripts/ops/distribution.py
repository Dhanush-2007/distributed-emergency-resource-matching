#!/usr/bin/env python3
"""Write keys across shards, verify distribution, and check diagnostics."""

import argparse
import os
import sys
import time
import uuid
from typing import Any

import httpx
from ems_common.sharding import shard_of


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Write N keys across coordinator shards and verify distribution."
    )
    parser.add_argument(
        "--keys",
        type=int,
        default=60,
        help="Number of keys to write (default: 60)",
    )
    args = parser.parse_args()

    coordinator_url = os.getenv("COORDINATOR_URL", "http://localhost:8000").rstrip("/")
    num_keys = args.keys
    if num_keys <= 0:
        print("Keys count must be > 0", file=sys.stderr)
        sys.exit(1)

    # Half request:<uuid>, half resource:<uuid>
    half = num_keys // 2
    rem = num_keys - half

    keys_to_write: list[tuple[str, dict[str, Any]]] = []
    for _ in range(half):
        k = f"request:{uuid.uuid4()}"
        keys_to_write.append((k, {"type": "water", "status": "PENDING"}))
    for _ in range(rem):
        k = f"resource:{uuid.uuid4()}"
        keys_to_write.append((k, {"type": "water", "availability": True}))

    routed_counts: dict[int, int] = {}
    key_values: dict[str, dict[str, Any]] = {}

    initial_records: dict[int, int] = {}
    with httpx.Client(base_url=coordinator_url, timeout=10.0) as client:
        # Read baseline diagnostics counts so we track newly added keys
        init_shards_resp = client.get("/diagnostics/shards")
        if init_shards_resp.status_code == 200:
            for s in init_shards_resp.json():
                initial_records[s["shardId"]] = s.get("records", 0)

        # Write keys
        for key, value in keys_to_write:
            put_resp = client.put(f"/internal/data/{key}", json={"value": value})
            if put_resp.status_code not in (200, 201):
                print(
                    f"PUT {key} failed with status {put_resp.status_code}: {put_resp.text}",
                    file=sys.stderr,
                )
                print("FAIL")
                sys.exit(1)

            # Query /diagnostics/route for each key
            route_resp = client.get("/diagnostics/route", params={"key": key})
            if route_resp.status_code != 200:
                print(
                    f"Route diagnostics for {key} failed with status {route_resp.status_code}",
                    file=sys.stderr,
                )
                print("FAIL")
                sys.exit(1)

            route_data = route_resp.json()
            sid = route_data["shardId"]
            local_sid = shard_of(key, 3)
            if sid != local_sid:
                print(
                    f"Shard mismatch for {key}: route says {sid}, shard_of says {local_sid}",
                    file=sys.stderr,
                )
                print("FAIL")
                sys.exit(1)

            routed_counts[sid] = routed_counts.get(sid, 0) + 1
            key_values[key] = value

        # Re-read every key with GET to verify its value
        for key, expected_val in key_values.items():
            get_resp = client.get(f"/internal/data/{key}")
            if get_resp.status_code != 200:
                print(
                    f"GET {key} failed with status {get_resp.status_code}: {get_resp.text}",
                    file=sys.stderr,
                )
                print("FAIL")
                sys.exit(1)

            body = get_resp.json()
            if body.get("value") != expected_val:
                print(
                    f"GET {key} value mismatch: got {body.get('value')}, expected {expected_val}",
                    file=sys.stderr,
                )
                print("FAIL")
                sys.exit(1)

        # Query /diagnostics/shards, polling briefly if needed for heartbeats to reflect counts
        timeout_sec = 10.0
        start_time = time.time()
        shards_data = []

        while time.time() - start_time < timeout_sec:
            shards_resp = client.get("/diagnostics/shards")
            if shards_resp.status_code != 200:
                print(
                    f"GET /diagnostics/shards failed with status {shards_resp.status_code}",
                    file=sys.stderr,
                )
                print("FAIL")
                sys.exit(1)

            shards_data = shards_resp.json()
            # Check if all shards have reported counts matching routed counts
            matches = True
            for shard_info in shards_data:
                sid = shard_info["shardId"]
                routed = routed_counts.get(sid, 0)
                diag_records = shard_info.get("records", 0)
                expected_records = initial_records.get(sid, 0) + routed
                if diag_records != expected_records:
                    matches = False
                    break
            if matches:
                break
            time.sleep(0.5)

    # Build report table
    print(
        f"{'ShardId':<8} {'Primary':<10} {'Routed Keys':<13} {'Diag Records':<14} {'Diag RecordCount'}"
    )
    print("-" * 65)

    all_shards_have_keys = True
    counts_match = True

    for shard_info in sorted(shards_data, key=lambda s: s["shardId"]):
        sid = shard_info["shardId"]
        primary = shard_info.get("primary") or "none"
        routed = routed_counts.get(sid, 0)
        diag_records = shard_info.get("records", 0)
        diag_rc = shard_info.get("recordCount", {})
        expected_records = initial_records.get(sid, 0) + routed

        print(
            f"{sid:<8} {primary:<10} {routed:<13} {diag_records:<14} {diag_rc}"
        )

        if routed == 0:
            all_shards_have_keys = False
        if diag_records != expected_records:
            counts_match = False

    if all_shards_have_keys and counts_match:
        print("PASS")
        sys.exit(0)
    else:
        print("FAIL")
        sys.exit(1)


if __name__ == "__main__":
    main()
