#!/usr/bin/env python3
"""Cluster watcher script.

Polls coordinator diagnostics endpoints and prints a formatted terminal dashboard.
"""

from __future__ import annotations

import argparse
import os
import sys
import time

import httpx


def clear_screen() -> None:
    sys.stdout.write("\033[2J\033[H")
    sys.stdout.flush()


def format_table(shards: list[dict], nodes: list[dict]) -> str:
    lines = []
    lines.append("=== EMS CLUSTER STATUS ===")
    lines.append(f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")

    lines.append("SHARDS:")
    lines.append(
        f"{'Shard':<8} {'State':<12} {'Primary':<12} {'Epoch':<8} {'Degraded':<10} {'DegradedSeq':<12}"
    )
    lines.append("-" * 65)
    for s in sorted(shards, key=lambda x: x.get("shardId", 0)):
        sid = s.get("shardId", 0)
        state = s.get("state", "UNKNOWN")
        primary = s.get("primaryNodeId", "-")
        epoch = s.get("epoch", 0)
        degraded = str(s.get("degraded", False))
        deg_seq = s.get("degradedSeq", 0)
        lines.append(
            f"{sid:<8} {state:<12} {primary:<12} {epoch:<8} {degraded:<10} {deg_seq:<12}"
        )

    lines.append("")
    lines.append("NODES:")
    lines.append(
        f"{'NodeId':<10} {'Shard':<8} {'Role':<10} {'Health':<8} {'Status':<12} {'LastSeq':<10} {'AckSeq':<8} {'Counts':<20}"
    )
    lines.append("-" * 90)
    for n in sorted(nodes, key=lambda x: (x.get("shardId", 0), x.get("nodeId", ""))):
        nid = n.get("nodeId", "-")
        sid = n.get("shardId", 0)
        role = n.get("role", "-")
        health = n.get("health", "-")
        st = n.get("status", "-")
        lseq = n.get("lastSeq", 0)
        ack = str(n.get("replicaAckSeq", "-"))
        counts = str(n.get("recordCount", {}))
        lines.append(
            f"{nid:<10} {sid:<8} {role:<10} {health:<8} {st:<12} {lseq:<10} {ack:<8} {counts:<20}"
        )

    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Watch EMS cluster state")
    parser.add_argument("--interval", type=float, default=1.0, help="Poll interval in seconds")
    parser.add_argument("--once", action="store_true", help="Print once and exit")
    parser.add_argument(
        "--coordinator",
        type=str,
        default=os.environ.get("COORDINATOR_URL", "http://localhost:8000"),
        help="Coordinator URL",
    )
    args = parser.parse_args()

    coord_url = args.coordinator.rstrip("/")
    client = httpx.Client(timeout=3.0)

    try:
        while True:
            try:
                s_resp = client.get(f"{coord_url}/diagnostics/shards")
                n_resp = client.get(f"{coord_url}/diagnostics/nodes")
                shards = s_resp.json() if s_resp.status_code == 200 else []
                nodes = n_resp.json() if n_resp.status_code == 200 else []
                output = format_table(shards, nodes)
            except Exception as exc:  # noqa: BLE001
                output = f"Error connecting to coordinator at {coord_url}: {exc}"

            if not args.once:
                clear_screen()
            print(output)

            if args.once:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        pass
    finally:
        client.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
