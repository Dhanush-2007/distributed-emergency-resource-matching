#!/usr/bin/env python3
"""Kubernetes Stage 3 verification script.

Valid for the 3-node skeleton only (Stage 9 has its own verification with replicas).

Checks the full local Minikube deployment:
1. All Pods in namespace `ems` are Running & Ready; PVCs are Bound.
2. `/diagnostics/nodes` reports three UP nodes and DNS-style endpoints (*.svc.cluster.local).
3. Runs the existing `scripts/ops/distribution.py` client script.
4. Persistence test – write a key (shard 1), delete its storage pod, wait for
   recovery, then GET the key and verify value/version.
5. Coordinator self-healing – delete the coordinator pod, wait for it to be Ready,
   verify 3 nodes UP again, unchanged shard epochs, and the key from step 4 readable.
6. Prints a summary table.

The script uses only the Python standard library and `httpx`.
It invokes `kubectl` via `subprocess` and retries connections (up to 60 s) if a
port-forward is disrupted.
"""

import argparse
import json
import os
import subprocess
import sys
import time

import httpx

# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def run_cmd(
    cmd: list[str], capture: bool = True, env: dict[str, str] | None = None
) -> tuple[int, str]:
    """Run a shell command, return (returncode, output)."""
    result = subprocess.run(
        cmd,
        check=False,
        env=env,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return result.returncode, result.stdout.strip()

def kubectl(*args: str) -> tuple[int, str]:
    return run_cmd(["kubectl"] + list(args))

def wait_for(condition_fn, timeout: int = 120, interval: int = 5) -> bool:
    """Poll *condition_fn* until it returns True or timeout expires."""
    start = time.time()
    while time.time() - start < timeout:
        if condition_fn():
            return True
        time.sleep(interval)
    return False

def retry_http_get(
    url: str,
    headers: dict[str, str] | None = None,
    retries: int = 12,
    delay: int = 5,
) -> httpx.Response:
    """GET with retries (max *retries* attempts, up to 60s total)."""
    for attempt in range(retries):
        try:
            resp = httpx.get(url, headers=headers, timeout=10.0)
            resp.raise_for_status()
            return resp
        except (httpx.HTTPError, httpx.NetworkError, Exception) as exc:
            if attempt == retries - 1:
                print(f"\n[!] Connection to {url} failed after 60s: {exc}")
                print("[!] Reminder: if kubectl port-forward dropped after pod restart, please restart it:")
                print("    kubectl -n ems port-forward svc/coordinator 8000:8000\n")
                raise
            time.sleep(delay)
    raise RuntimeError("unreachable")

# ---------------------------------------------------------------------------
# Verification steps
# ---------------------------------------------------------------------------

def check_pods_and_pvcs(namespace: str = "ems") -> bool:
    ok = True
    # Pods
    rc, out = kubectl("-n", namespace, "get", "pods", "-o", "json")
    if rc != 0:
        print(f"FAIL: kubectl get pods -> {out}")
        return False
    data = json.loads(out)
    for pod in data.get("items", []):
        name = pod["metadata"]["name"]
        phase = pod["status"].get("phase")
        conditions = pod["status"].get("conditions", [])
        ready = any(c.get("type") == "Ready" and c.get("status") == "True" for c in conditions)
        if phase != "Running" or not ready:
            print(f"FAIL: pod {name} not ready (phase={phase}, ready={ready})")
            ok = False
    # PVCs
    rc, out = kubectl("-n", namespace, "get", "pvc", "-o", "json")
    if rc != 0:
        print(f"FAIL: kubectl get pvc -> {out}")
        return False
    pvc_data = json.loads(out)
    for pvc in pvc_data.get("items", []):
        name = pvc["metadata"]["name"]
        phase = pvc["status"].get("phase")
        if phase != "Bound":
            print(f"FAIL: pvc {name} not bound (phase={phase})")
            ok = False
    if ok:
        print("PASS: all pods Running & Ready, PVCs Bound")
    return ok


def check_diagnostics_nodes(base_url: str) -> bool:
    url = f"{base_url}/diagnostics/nodes"
    try:
        resp = retry_http_get(url)
    except (httpx.HTTPError, ValueError) as exc:
        print(f"FAIL: cannot GET {url}: {exc}")
        return False
    data = resp.json()
    nodes = data if isinstance(data, list) else data.get("nodes", [])
    if len(nodes) != 3:
        print(f"FAIL: expected 3 nodes, got {len(nodes)}")
        return False
    for n in nodes:
        if n.get("health") != "UP" or n.get("status") != "READY":
            print(
                f"FAIL: node {n.get('nodeId')} health={n.get('health')} "
                f"status={n.get('status')}"
            )
            return False
        endpoint = n.get("endpoint", "")
        if not endpoint.endswith(".svc.cluster.local:9000"):
            print(f"FAIL: node {n.get('nodeId')} endpoint not DNS (got {endpoint})")
            return False
    print("PASS: diagnostics/nodes reports 3 UP nodes with DNS endpoints")
    return True


def run_distribution(base_url: str) -> bool:
    # reuse the existing script; it reads COORDINATOR_URL from env if set
    env = os.environ.copy()
    env["COORDINATOR_URL"] = base_url
    rc, out = run_cmd(
        [sys.executable, "scripts/ops/distribution.py", "--keys", "10"],
        capture=True,
        env=env,
    )
    if rc != 0:
        print(f"FAIL: distribution script failed (rc={rc})\n{out}")
        return False
    if "PASS" in out:
        print("PASS: distribution script succeeded")
        return True
    else:
        print(f"FAIL: distribution script output missing PASS\n{out}")
        return False


def persistence_test(base_url: str) -> tuple[bool, str, int]:
    # generate a key that maps to shard 1 using ems_common sharding logic
    import uuid

    from ems_common import sharding
    # Loop until shard_of returns 1 (unlikely to loop many times)
    while True:
        candidate = f"request:{uuid.uuid4()}"
        if sharding.shard_of(candidate, 3) == 1:
            key = candidate
            break
    # write the key via PUT (use httpx directly)
    put_url = f"{base_url}/internal/data/{key}"
    payload = {"value": {"test": "persistence"}, "ifVersion": None, "epoch": 1}
    try:
        resp = httpx.put(put_url, json=payload, timeout=10.0)
        resp.raise_for_status()
        version = resp.json().get("version")
    except httpx.HTTPError as exc:
        print(f"FAIL: initial PUT failed: {exc}")
        return False, "", -1
    # delete the storage pod for shard 1
    pod_name = "ems-s1-a-0"
    rc, out = kubectl("-n", "ems", "delete", "pod", pod_name, "--wait=false")
    if rc != 0:
        print(f"FAIL: could not delete pod {pod_name}: {out}")
        return False, "", -1
    # wait for new pod to be Ready
    def pod_ready():
        rc, out = kubectl("-n", "ems", "get", "pod", pod_name, "-o", "json")
        if rc != 0:
            return False
        pod = json.loads(out)
        phase = pod["status"].get("phase")
        conds = pod["status"].get("conditions", [])
        ready = any(c.get("type") == "Ready" and c.get("status") == "True" for c in conds)
        return phase == "Running" and ready
    if not wait_for(pod_ready, timeout=120, interval=5):
        print(f"FAIL: pod {pod_name} did not become Ready after deletion")
        return False, "", -1
    # GET the key again
    get_url = f"{base_url}/internal/data/{key}"
    try:
        resp = retry_http_get(get_url)
        got = resp.json()
        if got.get("value") != {"test": "persistence"} or got.get("version") != version:
            print("FAIL: retrieved value/version mismatch after pod restart")
            return False, key, version
    except (httpx.HTTPError, ValueError) as exc:
        print(f"FAIL: GET after pod restart failed: {exc}")
        return False, key, version
    print("PASS: persistence test succeeded (key survives storage pod restart)")
    return True, key, version


def coordinator_healing_test(base_url: str, key: str, version: int) -> bool:
    # record epochs before restart
    try:
        shards_resp = retry_http_get(f"{base_url}/diagnostics/shards")
        pre_epochs = {s["shardId"]: s.get("epoch", 1) for s in shards_resp.json()}
    except (httpx.HTTPError, ValueError, RuntimeError) as exc:
        print(f"FAIL: could not get diagnostics/shards before coordinator restart: {exc}")
        return False

    # delete coordinator pod
    rc, out = kubectl("-n", "ems", "delete", "pod", "-l", "app=coordinator", "--wait=false")
    if rc != 0:
        print(f"FAIL: could not delete coordinator pod: {out}")
        return False
    # wait for new coordinator pod Ready
    def coord_ready():
        rc, out = kubectl("-n", "ems", "get", "pods", "-l", "app=coordinator", "-o", "json")
        if rc != 0:
            return False
        pods = json.loads(out).get("items", [])
        if not pods:
            return False
        pod = pods[0]
        phase = pod["status"].get("phase")
        conds = pod["status"].get("conditions", [])
        ready = any(c.get("type") == "Ready" and c.get("status") == "True" for c in conds)
        return phase == "Running" and ready
    if not wait_for(coord_ready, timeout=120, interval=5):
        print("FAIL: coordinator pod did not become Ready after deletion")
        return False
    # diagnostics nodes again
    if not check_diagnostics_nodes(base_url):
        return False

    # verify unchanged shard epochs
    try:
        shards_resp2 = retry_http_get(f"{base_url}/diagnostics/shards")
        post_epochs = {s["shardId"]: s.get("epoch", 1) for s in shards_resp2.json()}
        if pre_epochs != post_epochs:
            print(f"FAIL: shard epochs changed after coordinator restart: {pre_epochs} vs {post_epochs}")
            return False
    except (httpx.HTTPError, ValueError, RuntimeError) as exc:
        print(f"FAIL: could not get diagnostics/shards after coordinator restart: {exc}")
        return False

    # GET the key again to ensure it is still readable
    get_url = f"{base_url}/internal/data/{key}"
    try:
        resp = retry_http_get(get_url)
        got = resp.json()
        if got.get("value") != {"test": "persistence"} or got.get("version") != version:
            print("FAIL: key value/version changed after coordinator restart")
            return False
    except (httpx.HTTPError, ValueError) as exc:
        print(f"FAIL: GET after coordinator restart failed: {exc}")
        return False
    print("PASS: coordinator self-healing verified (3 nodes UP, epochs unchanged, key still readable)")
    return True


def main():
    parser = argparse.ArgumentParser(description="Verify Stage 3 deployment")
    parser.add_argument(
        "--coordinator-url",
        default="http://localhost:8000",
        help="Base URL for the coordinator (use port‑forwarded address)",
    )
    args = parser.parse_args()

    base = args.coordinator_url.rstrip('/')
    all_pass = True
    summaries: list[tuple[str, str]] = []

    # 1 – pods & PVCs
    ok = check_pods_and_pvcs()
    summaries.append(("Pods & PVCs", "PASS" if ok else "FAIL"))
    all_pass = all_pass and ok

    # 2 – diagnostics nodes
    ok = check_diagnostics_nodes(base)
    summaries.append(("Diagnostics nodes", "PASS" if ok else "FAIL"))
    all_pass = all_pass and ok

    # 3 – distribution script
    ok = run_distribution(base)
    summaries.append(("Distribution script", "PASS" if ok else "FAIL"))
    all_pass = all_pass and ok

    # 4 – persistence across storage pod restart
    ok, key, version = persistence_test(base)
    summaries.append(("Persistence (storage pod)", "PASS" if ok else "FAIL"))
    all_pass = all_pass and ok

    # 5 – coordinator self‑healing
    if ok:
        ok2 = coordinator_healing_test(base, key, version)
        summaries.append(("Coordinator self‑healing", "PASS" if ok2 else "FAIL"))
        all_pass = all_pass and ok2
    else:
        summaries.append(("Coordinator self‑healing", "SKIPPED"))

    # 6 – summary table
    print("\n=== Summary ===")
    print("| Check | Result |\n|---|---|")
    for name, result in summaries:
        print(f"| {name} | {result} |")

    if not all_pass:
        sys.exit(1)

if __name__ == "__main__":
    main()
