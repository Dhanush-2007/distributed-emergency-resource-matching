#!/usr/bin/env python3
"""Load loop script for coordinator operations.

Generates continuous read/write load against coordinator, tracks acknowledged
keys, and validates that no acknowledged writes are lost during failures.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import uuid

import httpx
from ems_common.sharding import shard_of


def generate_key(target_shard: int | None, n_shards: int = 3) -> str:
    while True:
        k = f"request:{uuid.uuid4()}"
        if target_shard is None or shard_of(k, n_shards) == target_shard:
            return k


def main() -> int:
    parser = argparse.ArgumentParser(description="Coordinator load generator")
    parser.add_argument("--rate", type=int, default=10, help="Target operations per second")
    parser.add_argument("--duration", type=int, default=60, help="Duration in seconds")
    parser.add_argument("--shard", type=int, default=None, help="Pin writes to specific shard N")
    parser.add_argument(
        "--coordinator",
        type=str,
        default=os.environ.get("COORDINATOR_URL", "http://localhost:8000"),
        help="Coordinator URL",
    )
    args = parser.parse_args()

    coord_url = args.coordinator.rstrip("/")
    client = httpx.Client(timeout=5.0)

    # Determine n_shards from diagnostics if possible
    n_shards = 3
    try:
        s_resp = client.get(f"{coord_url}/diagnostics/shards")
        if s_resp.status_code == 200:
            shards_data = s_resp.json()
            if shards_data:
                n_shards = max(s.get("shardId", 0) for s in shards_data) + 1
    except Exception:  # noqa: BLE001, S110
        pass

    acknowledged_keys: set[str] = set()
    total_503_retries = 0

    t_start = time.time()
    t_end = t_start + args.duration

    current_sec = int(t_start)
    sec_ok = 0
    sec_503 = 0
    sec_other = 0
    sec_latencies: list[float] = []

    print(
        f"Starting load loop: rate={args.rate}/s, duration={args.duration}s, "
        f"shard={args.shard if args.shard is not None else 'ALL'}, target={coord_url}"
    )

    try:
        while time.time() < t_end:
            loop_t0 = time.time()
            sec_idx = int(loop_t0)
            if sec_idx > current_sec:
                p50 = (
                    sorted(sec_latencies)[len(sec_latencies) // 2] * 1000.0
                    if sec_latencies
                    else 0.0
                )
                elapsed = int(loop_t0 - t_start)
                print(
                    f"[{elapsed:02d}s] ok={sec_ok:<4} 503={sec_503:<4} other={sec_other:<4} "
                    f"p50={p50:6.1f}ms  ack_keys={len(acknowledged_keys)}"
                )
                current_sec = sec_idx
                sec_ok = 0
                sec_503 = 0
                sec_other = 0
                sec_latencies = []

            # Issue PUT operation
            key = generate_key(args.shard, n_shards)
            req_t0 = time.time()
            try:
                resp = client.put(
                    f"{coord_url}/internal/data/{key}",
                    json={"value": {"id": key, "ts": req_t0}},
                )
                latency = time.time() - req_t0
                sec_latencies.append(latency)

                if resp.status_code in (200, 201):
                    acknowledged_keys.add(key)
                    sec_ok += 1
                elif resp.status_code == 503:
                    sec_503 += 1
                    total_503_retries += 1
                else:
                    sec_other += 1
            except Exception:  # noqa: BLE001
                sec_other += 1

            # Read back a recent key if any
            if acknowledged_keys:
                read_key = next(iter(acknowledged_keys))
                try:
                    client.get(f"{coord_url}/internal/data/{read_key}")
                except Exception:  # noqa: BLE001, S110
                    pass

            target_interval = 1.0 / max(1, args.rate)
            elapsed_loop = time.time() - loop_t0
            sleep_time = target_interval - elapsed_loop
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("\nLoad loop interrupted by user.")

    # Validation phase
    print(f"\nLoad phase finished. Verifying {len(acknowledged_keys)} acknowledged keys...")
    lost_count = 0
    verified_count = 0

    for k in acknowledged_keys:
        try:
            r = client.get(f"{coord_url}/internal/data/{k}")
            if r.status_code == 200:
                verified_count += 1
            else:
                lost_count += 1
        except Exception:  # noqa: BLE001
            lost_count += 1

    print("=== LOAD LOOP SUMMARY ===")
    print(f"Total Acknowledged Keys: {len(acknowledged_keys)}")
    print(f"Verified Readable:       {verified_count}")
    print(f"Total 503 Retries:       {total_503_retries}")
    print(f"LOST Keys:               {lost_count}")

    client.close()
    if lost_count > 0:
        print("FAIL: Acknowledged keys were lost!")
        return 1

    print("PASS: Zero acknowledged keys lost.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
