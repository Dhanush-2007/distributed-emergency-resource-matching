#!/usr/bin/env python3
"""Application load script for Distributed Emergency Resource Matching."""

from __future__ import annotations

import argparse
import os
import random
import sys
import time

import httpx

REQ_URL = os.environ.get("REQ_URL", "http://localhost:8001").rstrip("/")


def run_load(rate: float, duration: float) -> int:
    client = httpx.Client(timeout=5.0)
    interval = 1.0 / max(0.1, rate)
    t_end = time.time() + duration

    ack_ids: list[str] = []
    submitted = 0
    retried_503 = 0
    failed_submit = 0

    print(f"Starting load: rate={rate} req/s, duration={duration}s against {REQ_URL}")
    types = ["water", "food"]
    rng = random.Random(time.time())

    while time.time() < t_end:
        t_start = time.time()
        req_type = rng.choice(types)
        payload = {
            "type": req_type,
            "location": {"lat": 12.9716, "lon": 77.5946},
            "details": f"Load test request {submitted}",
        }

        submitted += 1
        req_id = None
        for attempt in range(4):
            try:
                resp = client.post(f"{REQ_URL}/requests", json=payload)
                if resp.status_code == 201:
                    req_id = resp.json()["requestId"]
                    ack_ids.append(req_id)
                    # Verify readable
                    client.get(f"{REQ_URL}/requests/{req_id}")
                    break
                if resp.status_code == 503:
                    retried_503 += 1
                    time.sleep(0.3 * (2**attempt))
                    continue
                failed_submit += 1
                break
            except Exception:  # noqa: BLE001
                time.sleep(0.3 * (2**attempt))

        elapsed = time.time() - t_start
        sleep_time = interval - elapsed
        if sleep_time > 0:
            time.sleep(sleep_time)

    print(f"Load loop finished. Submitted: {submitted}, Acknowledged: {len(ack_ids)}, 503 Retries: {retried_503}")
    print(f"Verifying {len(ack_ids)} acknowledged requests settle to MATCHED/NO_MATCH within 60s...")

    deadline = time.time() + 60.0
    pending_ids = set(ack_ids)
    matched_count = 0
    no_match_count = 0
    lost_count = 0

    while pending_ids and time.time() < deadline:
        still_pending = set()
        for rid in list(pending_ids):
            try:
                r = client.get(f"{REQ_URL}/requests/{rid}")
                if r.status_code != 200:
                    lost_count += 1
                    continue
                st = r.json().get("status")
                if st == "MATCHED":
                    matched_count += 1
                elif st == "NO_MATCH":
                    no_match_count += 1
                else:
                    still_pending.add(rid)
            except Exception:  # noqa: BLE001
                still_pending.add(rid)
        pending_ids = still_pending
        if pending_ids:
            time.sleep(1.0)

    lost_count += len(pending_ids)

    print("\n=== App Load Summary ===")
    print(f"Submitted:       {submitted}")
    print(f"Acknowledged:    {len(ack_ids)}")
    print(f"503 Retries:     {retried_503}")
    print(f"MATCHED:         {matched_count}")
    print(f"NO_MATCH:        {no_match_count}")
    print(f"Lost / Unsettled: {lost_count}")

    if lost_count > 0:
        print(f"ERROR: {lost_count} requests were lost or never settled!", file=sys.stderr)
        return 1

    print("SUCCESS: All acknowledged requests readable and settled.")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Application load generator")
    parser.add_argument("--rate", type=float, default=5.0, help="Requests per second")
    parser.add_argument("--duration", type=float, default=30.0, help="Duration in seconds")
    args = parser.parse_args()

    sys.exit(run_load(args.rate, args.duration))


if __name__ == "__main__":
    main()
