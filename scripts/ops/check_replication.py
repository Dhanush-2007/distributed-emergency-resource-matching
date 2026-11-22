#!/usr/bin/env python3
"""Stage 4 replication checker script.

Writes keys through the coordinator, waits for convergence, compares
primary and replica SQLite state across all shards, and prints PASS.
"""

import json
import os
import subprocess
import sys
import time
import uuid

import httpx

COORDINATOR_URL = os.getenv("COORDINATOR_URL", "http://localhost:8000").rstrip("/")


def inspect_node_db(container_name: str, db_name: str) -> dict:
    """Run Python one-liner in container to inspect sqlite database."""
    cmd = [
        "docker",
        "exec",
        container_name,
        "python",
        "-c",
        (
            f"import sqlite3, json; "
            f"conn = sqlite3.connect('/data/{db_name}.db'); "
            f"cur = conn.cursor(); "
            f"live = cur.execute('SELECT COUNT(*) FROM kv WHERE deleted = 0').fetchone()[0]; "
            f"tombstones = cur.execute('SELECT COUNT(*) FROM kv WHERE deleted = 1').fetchone()[0]; "
            f"rows = cur.execute('SELECT key, value, version, deleted FROM kv ORDER BY key').fetchall(); "
            f"meta = dict(cur.execute('SELECT k, v FROM meta').fetchall()); "
            f"print(json.dumps({{'live': live, 'tombstones': tombstones, 'rows': rows, 'meta': meta}}))"
        ),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return json.loads(proc.stdout.strip())


def main() -> int:
    print(f"Connecting to coordinator at {COORDINATOR_URL}...")
    client = httpx.Client(base_url=COORDINATOR_URL, timeout=10.0)

    # 1. Wait for coordinator to be live and healthy
    for _ in range(30):
        try:
            r = client.get("/health/live")
            if r.status_code == 200:
                break
        except (httpx.HTTPError, OSError):
            time.sleep(0.1)  # retry
        time.sleep(1)
    else:
        print("ERROR: Coordinator unreachable", file=sys.stderr)
        return 1

    # 2. Seed data: creates, updates, and deletes across shards
    print("Writing keys across shards (creates, updates, deletes)...")
    keys_created = []
    # Create 30 keys
    for i in range(30):
        kind = "request" if i % 2 == 0 else "resource"
        key = f"{kind}:{i}:{uuid.uuid4().hex[:8]}"
        val = {"id": i, "name": f"item-{i}", "ts": time.time()}
        r = client.put(f"/internal/data/{key}", json={"value": val, "ifVersion": 0})
        if r.status_code not in (200, 201):
            print(f"ERROR: Failed to create {key}: {r.status_code} {r.text}", file=sys.stderr)
            return 1
        keys_created.append((key, val))

    # Update 10 keys
    for key, val in keys_created[:10]:
        val["updated"] = True
        r = client.put(f"/internal/data/{key}", json={"value": val, "ifVersion": 1})
        if r.status_code != 200:
            print(f"ERROR: Failed to update {key}: {r.status_code} {r.text}", file=sys.stderr)
            return 1

    # Delete 10 keys
    for key, _ in keys_created[10:20]:
        r = client.delete(f"/internal/data/{key}")
        if r.status_code not in (200, 204):
            print(f"ERROR: Failed to delete {key}: {r.status_code} {r.text}", file=sys.stderr)
            return 1

    print("Data write complete. Waiting for convergence...")

    # 3. Wait for convergence
    converged = False
    for _ in range(25):
        diag = client.get("/diagnostics/shards").json()
        all_healthy = all(s.get("state") == "HEALTHY" for s in diag)
        if all_healthy:
            # Check lastSeq equality between nodes
            all_synced = True
            for s in diag:
                nodes = s.get("nodes", [])
                if len(nodes) < 2:
                    all_synced = False
                    break
                seqs = {n.get("lastSeq") for n in nodes}
                if len(seqs) > 1:
                    all_synced = False
                    break
            if all_synced:
                converged = True
                break
        time.sleep(1)

    if not converged:
        print("WARNING: Cluster did not report fully converged diagnostics within timeout")

    # 4. Compare SQLite state between primary and replica for each shard
    print("\n--- Verifying SQLite Replication State ---")
    results = []
    has_mismatch = False

    for sid in range(3):
        p_container = f"ems-s{sid}-a"
        r_container = f"ems-s{sid}-b"
        p_db = f"s{sid}-a"
        r_db = f"s{sid}-b"

        p_data = inspect_node_db(p_container, p_db)
        r_data = inspect_node_db(r_container, r_db)

        p_seq = int(p_data["meta"].get("last_applied_seq", 0))
        r_seq = int(r_data["meta"].get("last_applied_seq", 0))

        match = (
            p_seq == r_seq
            and p_data["live"] == r_data["live"]
            and p_data["tombstones"] == r_data["tombstones"]
            and p_data["rows"] == r_data["rows"]
        )
        if not match:
            has_mismatch = True

        results.append({
            "shard": sid,
            "primary": p_container,
            "replica": r_container,
            "p_seq": p_seq,
            "r_seq": r_seq,
            "live": p_data["live"],
            "tombstones": p_data["tombstones"],
            "match": match,
        })

    # 5. Print table
    print(f"{'Shard':<6} | {'Primary':<12} | {'Replica':<12} | {'Seq (P/R)':<12} | {'Live':<6} | {'Tombstones':<10} | {'Status'}")
    print("-" * 75)
    for res in results:
        status_str = "MATCH" if res["match"] else "MISMATCH"
        seq_str = f"{res['p_seq']}/{res['r_seq']}"
        print(
            f"s{res['shard']:<5} | {res['primary']:<12} | {res['replica']:<12} | "
            f"{seq_str:<12} | {res['live']:<6} | {res['tombstones']:<10} | {status_str}"
        )

    if has_mismatch:
        print("\nFAIL: Primary and replica state mismatch detected!", file=sys.stderr)
        return 1

    print("\nPASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
