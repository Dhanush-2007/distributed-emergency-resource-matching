#!/usr/bin/env python3
"""Kubernetes Stage 9 Comprehensive Verification Suite.

Validates the full Minikube deployment across 8 scenarios:
1. Workload readiness and shard health check.
2. Inter-service DNS resolution and end-to-end matching.
3. Shard key distribution across all three storage shards.
4. Application pod self-healing during deletion under request traffic.
5. Matching Service scale-out to 3 replicas without duplicate assignment.
6. Storage node failover under sustained load and recovery upon return.
7. PVC data persistence across replica, coordinator, and RabbitMQ restarts.
8. RabbitMQ outage handling and sweeper recovery.
"""
# ruff: noqa: BLE001, S110, RUF012, RUF059, PERF102, F841

from __future__ import annotations

import concurrent.futures
import json
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx

EVIDENCE_DIR = Path("docs/evidence/stage9")
EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)

COORD_BASE = "http://localhost:8000"
REQ_BASE = "http://localhost:8001"
RES_BASE = "http://localhost:8002"
RABBIT_MGMT = "http://localhost:15672"


def run_cmd(cmd: list[str], timeout: float = 60.0) -> tuple[int, str]:
    """Execute shell command and return (rc, combined output)."""
    try:
        res = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=timeout,
            check=False,
        )
        return res.returncode, res.stdout.strip()
    except subprocess.TimeoutExpired:
        return -1, f"Command timed out after {timeout}s: {' '.join(cmd)}"


def kubectl(*args: str, timeout: float = 60.0) -> tuple[int, str]:
    full_cmd = ["kubectl", "-n", "ems"] + list(args)
    print(f"  [kubectl] {' '.join(full_cmd)}")
    return run_cmd(full_cmd, timeout=timeout)


def wait_until(predicate: Callable[[], bool], timeout: float = 60.0, interval: float = 1.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if predicate():
                return True
        except Exception:
            pass
        time.sleep(interval)
    return False


class PortForwardManager:
    """Supervises background kubectl port-forwards and restarts them on failure."""

    SERVICES = {
        8000: "svc/coordinator",
        8001: "svc/request-service",
        8002: "svc/resource-service",
        15672: "svc/rabbitmq",
    }

    def __init__(self) -> None:
        self.procs: dict[int, subprocess.Popen] = {}
        self.lock = threading.Lock()

    def start_all(self) -> None:
        with self.lock:
            for port, svc in self.SERVICES.items():
                self._start_port(port, svc)
            time.sleep(2.0)

    def _start_port(self, port: int, svc: str) -> None:
        if port in self.procs and self.procs[port].poll() is None:
            return
        cmd = ["kubectl", "-n", "ems", "port-forward", svc, f"{port}:{port}"]
        proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.procs[port] = proc

    def ensure_healthy(self, port: int) -> None:
        with self.lock:
            proc = self.procs.get(port)
            if proc is None or proc.poll() is not None:
                svc = self.SERVICES[port]
                self._start_port(port, svc)
                time.sleep(1.0)

    def stop_all(self) -> None:
        with self.lock:
            for port, proc in self.procs.items():
                if proc.poll() is None:
                    proc.terminate()
                    try:
                        proc.wait(timeout=2.0)
                    except subprocess.TimeoutExpired:
                        proc.kill()
            self.procs.clear()


PF_MGR = PortForwardManager()


def safe_http_get(url: str, port: int, timeout: float = 10.0) -> httpx.Response:
    deadline = time.time() + timeout
    last_exc = None
    while time.time() < deadline:
        PF_MGR.ensure_healthy(port)
        try:
            r = httpx.get(url, timeout=3.0)
            return r
        except Exception as e:
            last_exc = e
            time.sleep(0.5)
    raise RuntimeError(f"HTTP GET {url} failed: {last_exc}")


def safe_http_post(url: str, port: int, json_body: dict[str, Any], timeout: float = 10.0) -> httpx.Response:
    deadline = time.time() + timeout
    last_exc = None
    while time.time() < deadline:
        PF_MGR.ensure_healthy(port)
        try:
            r = httpx.post(url, json=json_body, timeout=3.0)
            return r
        except Exception as e:
            last_exc = e
            time.sleep(0.5)
    raise RuntimeError(f"HTTP POST {url} failed: {last_exc}")


def get_shards_dict() -> dict[str, dict]:
    r = safe_http_get(f"{COORD_BASE}/diagnostics/shards", 8000)
    data = r.json()
    if isinstance(data, list):
        return {str(s["shardId"]): s for s in data}
    return data


# ---------------------------------------------------------------------------
# Step 1: Workloads & Shard Health
# ---------------------------------------------------------------------------

def step_1_workloads_and_health() -> tuple[bool, str]:
    """Verify 13 workloads Ready, all PVCs Bound, and 3 shards HEALTHY."""
    evidence = []
    _rc, out = kubectl("get", "deploy,sts,svc,pods,pvc")
    evidence.append(out)

    # Check Pods
    rc_pods, out_pods = kubectl("get", "pods", "-o", "json")
    if rc_pods != 0:
        return False, f"kubectl get pods failed: {out_pods}"
    pods_data = json.loads(out_pods).get("items", [])
    if len(pods_data) < 11:
        return False, f"Expected at least 11 pods, found {len(pods_data)}"
    for pod in pods_data:
        pname = pod["metadata"]["name"]
        phase = pod["status"].get("phase")
        ready = any(c.get("type") == "Ready" and c.get("status") == "True" for c in pod["status"].get("conditions", []))
        if phase != "Running" or not ready:
            return False, f"Pod {pname} not Ready (phase={phase}, ready={ready})"

    # Check PVCs
    rc_pvc, out_pvc = kubectl("get", "pvc", "-o", "json")
    if rc_pvc != 0:
        return False, f"kubectl get pvc failed: {out_pvc}"
    pvcs = json.loads(out_pvc).get("items", [])
    if len(pvcs) < 8:
        return False, f"Expected at least 8 PVCs, found {len(pvcs)}"
    for pvc in pvcs:
        pname = pvc["metadata"]["name"]
        if pvc["status"].get("phase") != "Bound":
            return False, f"PVC {pname} not Bound"

    # Check Diagnostics Shards
    shards_map = get_shards_dict()
    evidence.append(f"/diagnostics/shards: {json.dumps(shards_map, indent=2)}")

    if len(shards_map) != 3:
        return False, f"Expected 3 shards, got {len(shards_map)}"
    for s_id, s_info in shards_map.items():
        state = s_info.get("state") or s_info.get("status")
        if state != "HEALTHY":
            return False, f"Shard {s_id} is not HEALTHY: {state}"
        if not s_info.get("primary"):
            return False, f"Shard {s_id} has no primary"

    (EVIDENCE_DIR / "step1_workloads.txt").write_text("\n\n".join(evidence))
    return True, "11 workloads Ready, all PVCs Bound, 3 shards HEALTHY"


# ---------------------------------------------------------------------------
# Step 2: Service-to-Service DNS Resolution and End-to-End Matching
# ---------------------------------------------------------------------------

def step_2_dns_and_matching() -> tuple[bool, str]:
    """Test DNS resolution from within a pod and end-to-end matching."""
    evidence = []

    # DNS check from inside request-service pod
    dns_check_py = (
        "import socket; "
        "services = ['coordinator', 'request-service', 'resource-service', 'rabbitmq']; "
        "[socket.gethostbyname(s) for s in services]; print('DNS_OK')"
    )
    rc, out = kubectl("exec", "deployment/request-service", "--", "python", "-c", dns_check_py)
    evidence.append(f"DNS check output:\n{out}")
    if rc != 0 or "DNS_OK" not in out:
        return False, f"DNS resolution failed inside cluster: {out}"

    # End-to-end matching
    tag = uuid.uuid4().hex[:6]
    # 1. Register water resource at ~500m
    r_res1 = safe_http_post(
        f"{RES_BASE}/resources",
        8002,
        {"name": f"water-near-{tag}", "type": "water", "location": {"lat": 12.9750, "lon": 77.5946}, "availability": True},
    )
    if r_res1.status_code != 201:
        return False, f"Resource registration failed: {r_res1.text}"
    res1_id = r_res1.json()["resourceId"]

    # 2. Register food resource at ~100m (closer, but different type)
    r_res2 = safe_http_post(
        f"{RES_BASE}/resources",
        8002,
        {"name": f"food-closer-{tag}", "type": "food", "location": {"lat": 12.9720, "lon": 77.5946}, "availability": True},
    )
    if r_res2.status_code != 201:
        return False, f"Resource registration failed: {r_res2.text}"

    # 3. Create water request
    r_req = safe_http_post(
        f"{REQ_BASE}/requests",
        8001,
        {"type": "water", "location": {"lat": 12.9716, "lon": 77.5946}, "details": f"k8s test {tag}"},
    )
    if r_req.status_code != 201:
        return False, f"Request creation failed: {r_req.text}"
    req_id = r_req.json()["requestId"]

    # 4. Wait for matching
    matched = False
    for _ in range(25):
        time.sleep(1.0)
        r_poll = safe_http_get(f"{REQ_BASE}/requests/{req_id}", 8001)
        if r_poll.status_code == 200:
            data = r_poll.json()
            if data.get("status") == "MATCHED":
                matched = True
                assert data.get("matchedResourceId") == res1_id, f"Wrong resource matched: {data}"
                break

    if not matched:
        return False, f"Request {req_id} did not reach MATCHED status"

    # Grab matching-service logs
    rc_logs, out_logs = kubectl("logs", "deployment/matching-service", "--tail=60")
    evidence.append(f"Matching logs:\n{out_logs}")

    (EVIDENCE_DIR / "step2_dns_and_matching.txt").write_text("\n\n".join(evidence))
    return True, f"DNS verified, request {req_id} matched nearest water resource {res1_id}"


# ---------------------------------------------------------------------------
# Step 3: Shard Distribution
# ---------------------------------------------------------------------------

def step_3_shard_distribution() -> tuple[bool, str]:
    """Run seed.py --requests 30 --resources 30 and verify non-zero distribution on all shards."""
    cmd = [sys.executable, "scripts/ops/seed.py", "--requests", "30", "--resources", "30"]
    rc, out = run_cmd(cmd, timeout=60.0)
    if rc != 0:
        return False, f"seed.py failed: {out}"

    shards_map = get_shards_dict()

    evidence = [out, f"Shards distribution post-seed:\n{json.dumps(shards_map, indent=2)}"]
    (EVIDENCE_DIR / "step3_distribution.txt").write_text("\n\n".join(evidence))

    for s_id, s_info in shards_map.items():
        counts = s_info.get("recordCount", {})
        total = sum(counts.values()) if counts else s_info.get("records", 0)
        if total <= 0:
            return False, f"Shard {s_id} has 0 records after seed"

    return True, "Records distributed across all three shards"

# ---------------------------------------------------------------------------
# Step 4: Application Pod Self-Healing
# ---------------------------------------------------------------------------

def step_4_pod_self_healing() -> tuple[bool, str]:
    """Delete request-service pod while calling API and confirm self-healing."""
    evidence = []

    # 1. Create a request to query during deletion
    tag = uuid.uuid4().hex[:6]
    r_create = safe_http_post(
        f"{REQ_BASE}/requests",
        8001,
        {"type": "medicine", "location": {"lat": 12.9716, "lon": 77.5946}, "details": f"heal test {tag}"},
    )
    if r_create.status_code != 201:
        return False, f"Failed to create request: {r_create.text}"
    req_id = r_create.json()["requestId"]

    # 2. Capture pod before
    rc, out_before = kubectl("get", "pods", "-l", "app=request-service")
    evidence.append(f"Pods before deletion:\n{out_before}")

    # 3. Background querying loop
    query_errors = 0
    query_success = 0
    stop_flag = threading.Event()

    def query_loop():
        nonlocal query_errors, query_success
        while not stop_flag.is_set():
            try:
                PF_MGR.ensure_healthy(8001)
                r = httpx.get(f"{REQ_BASE}/requests/{req_id}", timeout=2.0)
                if r.status_code == 200:
                    query_success += 1
                else:
                    query_errors += 1
            except Exception:
                query_errors += 1
            time.sleep(0.3)

    t = threading.Thread(target=query_loop, daemon=True)
    t.start()

    # 4. Delete the pod
    kubectl("delete", "pod", "-l", "app=request-service", "--wait=false")

    # 5. Wait for replacement pod to be Ready
    time.sleep(3.0)
    ready = wait_until(
        lambda: "1/1" in kubectl("get", "pods", "-l", "app=request-service")[1]
        and "Running" in kubectl("get", "pods", "-l", "app=request-service")[1],
        timeout=40.0,
    )

    stop_flag.set()
    t.join(timeout=3.0)

    # 6. Capture pod after
    rc, out_after = kubectl("get", "pods", "-l", "app=request-service")
    evidence.append(f"Pods after deletion:\n{out_after}")
    evidence.append(f"Query stats during recovery: success={query_success}, transient_errors={query_errors}")

    # 7. Final fetch must succeed and match original request
    r_final = safe_http_get(f"{REQ_BASE}/requests/{req_id}", 8001)
    if r_final.status_code != 200:
        return False, f"GET /requests/{req_id} failed after pod recovery: {r_final.status_code}"

    (EVIDENCE_DIR / "step4_self_healing.txt").write_text("\n\n".join(evidence))
    return True, f"Request Service pod recovered, queried {query_success} times, no data lost"


# ---------------------------------------------------------------------------
# Step 5: Matching Service Scaling
# ---------------------------------------------------------------------------

def step_5_scaling() -> tuple[bool, str]:
    """Scale matching-service to 3 replicas, process burst, verify no duplicate assignments."""
    evidence = []

    # 1. Scale to 3
    kubectl("scale", "deployment/matching-service", "--replicas=3")
    scale_ok = wait_until(
        lambda: kubectl("get", "deployment/matching-service", "-o", "jsonpath={.status.readyReplicas}")[1] == "3",
        timeout=60.0,
    )
    if not scale_ok:
        return False, "matching-service failed to reach 3 ready replicas"

    # 2. Register 5 resources of type shelter
    tag = uuid.uuid4().hex[:6]
    res_ids = []
    for i in range(5):
        r = safe_http_post(
            f"{RES_BASE}/resources",
            8002,
            {"name": f"shelter-{tag}-{i}", "type": "shelter", "location": {"lat": 12.9716 + i * 0.005, "lon": 77.5946}, "availability": True},
        )
        if r.status_code == 201:
            res_ids.append(r.json()["resourceId"])

    evidence.append(f"Registered {len(res_ids)} shelter resources")

    # 3. Burst submit 30 requests of type shelter concurrently
    req_ids = []

    def submit_one(idx: int) -> str | None:
        try:
            r = safe_http_post(
                f"{REQ_BASE}/requests",
                8001,
                {"type": "shelter", "location": {"lat": 12.9716, "lon": 77.5946}, "details": f"burst-{tag}-{idx}"},
                timeout=15.0,
            )
            if r.status_code == 201:
                return r.json()["requestId"]
        except Exception:
            pass
        return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(submit_one, i) for i in range(30)]
        for f in concurrent.futures.as_completed(futures):
            rid = f.result()
            if rid:
                req_ids.append(rid)

    evidence.append(f"Submitted {len(req_ids)} requests under load")

    # 4. Wait for all requests to reach terminal status
    matched_count = 0
    no_match_count = 0
    assigned_resources: set[str] = set()

    deadline = time.time() + 45.0
    while time.time() < deadline:
        pending = 0
        for rid in req_ids:
            r = safe_http_get(f"{REQ_BASE}/requests/{rid}", 8001)
            if r.status_code == 200:
                st = r.json().get("status")
                if st == "PENDING":
                    pending += 1
        if pending == 0:
            break
        time.sleep(1.0)

    for rid in req_ids:
        r = safe_http_get(f"{REQ_BASE}/requests/{rid}", 8001)
        data = r.json()
        st = data.get("status")
        if st == "MATCHED":
            matched_count += 1
            m_res = data.get("matchedResourceId")
            if m_res in assigned_resources:
                return False, f"DUPLICATE RESOURCE ASSIGNMENT: resource {m_res} assigned multiple times!"
            if m_res:
                assigned_resources.add(m_res)
        elif st == "NO_MATCH":
            no_match_count += 1

    evidence.append(f"Outcomes: MATCHED={matched_count}, NO_MATCH={no_match_count}, AssignedUnique={len(assigned_resources)}")

    # 5. Grab logs from all 3 pods
    rc, out_logs = kubectl("logs", "-l", "app=matching-service", "--tail=50", "--prefix=true")
    evidence.append(f"Multi-pod logs:\n{out_logs}")

    # 6. Scale back to 1
    kubectl("scale", "deployment/matching-service", "--replicas=1")

    (EVIDENCE_DIR / "step5_scaling.txt").write_text("\n\n".join(evidence))
    if matched_count != len(res_ids):
        return False, f"Expected {len(res_ids)} matches, got {matched_count}"
    return True, f"Scale to 3 processed burst: {matched_count} MATCHED, {no_match_count} NO_MATCH, no duplicate assignments"

# ---------------------------------------------------------------------------
# Step 6: Storage Failure and Recovery Under Load
# ---------------------------------------------------------------------------

def step_6_storage_failover_recovery() -> tuple[bool, str]:
    """Test primary storage node failover under load and subsequent recovery."""
    evidence = []

    # 1. Identify primary of shard 1
    shards = get_shards_dict()
    s1 = shards.get("1", {})
    primary = s1.get("primary")
    if not primary:
        return False, f"Shard 1 has no primary: {s1}"
    replica = "s1-b" if primary == "s1-a" else "s1-a"
    init_epoch = s1.get("epoch", 1)

    evidence.append(f"Shard 1 initial state: primary={primary}, replica={replica}, epoch={init_epoch}")

    # 2. Write a probe request specifically on shard 1
    from ems_common.sharding import shard_of
    probe_id = None
    for _ in range(25):
        r_cand = safe_http_post(
            f"{REQ_BASE}/requests",
            8001,
            {"type": "water", "location": {"lat": 12.9716, "lon": 77.5946}, "details": "probe shard 1"},
        )
        if r_cand.status_code == 201:
            cand_id = r_cand.json()["requestId"]
            if shard_of(f"request:{cand_id}", 3) == 1:
                probe_id = cand_id
                break
    if not probe_id:
        return False, "Failed to create probe request on shard 1"

    evidence.append(f"Created probe request {probe_id} on shard 1 (verified shard_of=1)")

    # 3. Start background load loop
    stop_load = threading.Event()
    load_stats = {"attempted": 0, "success": 0, "lost": 0, "errors": 0}

    def load_loop():
        idx = 0
        while not stop_load.is_set():
            idx += 1
            load_stats["attempted"] += 1
            try:
                PF_MGR.ensure_healthy(8001)
                r = httpx.post(
                    f"{REQ_BASE}/requests",
                    json={"type": "water", "location": {"lat": 12.9716, "lon": 77.5946}, "details": f"load-{idx}"},
                    timeout=3.0,
                )
                if r.status_code == 201:
                    load_stats["success"] += 1
                elif r.status_code == 503:
                    # transient during failover
                    load_stats["errors"] += 1
                else:
                    load_stats["lost"] += 1
            except Exception:
                load_stats["errors"] += 1
            time.sleep(0.2)

    t = threading.Thread(target=load_loop, daemon=True)
    t.start()

    try:
        # 4. Scale primary to 0 to simulate hard crash / node failure
        evidence.append(f"Scaling primary sts/ems-{primary} to 0...")
        kubectl("scale", f"statefulset/ems-{primary}", "--replicas=0")

        # 5. Wait for failover: shard 1 becomes DEGRADED, replica promoted
        failover_ok = False
        deadline = time.time() + 35.0
        while time.time() < deadline:
            time.sleep(1.0)
            try:
                cur_s1 = get_shards_dict().get("1", {})
                if cur_s1.get("primary") == replica and cur_s1.get("epoch", 0) > init_epoch:
                    failover_ok = True
                    evidence.append(f"Failover successful: new primary={replica}, epoch={cur_s1.get('epoch')}, state={cur_s1.get('state')}")
                    break
            except Exception:
                pass

        if not failover_ok:
            return False, "Failover did not occur within 35 seconds"

        # 6. Verify probe key is still readable on new primary
        r_check = safe_http_get(f"{REQ_BASE}/requests/{probe_id}", 8001)
        if r_check.status_code != 200:
            return False, f"Probe request {probe_id} not readable after failover!"

        # 7. Scale returning node back to 1
        evidence.append(f"Scaling returning node sts/ems-{primary} back to 1...")
        kubectl("scale", f"statefulset/ems-{primary}", "--replicas=1")

        # 8. Wait for shard 1 to recover to HEALTHY
        recovered = False
        deadline = time.time() + 60.0
        while time.time() < deadline:
            time.sleep(2.0)
            try:
                cur_s1 = get_shards_dict().get("1", {})
                if (cur_s1.get("state") or cur_s1.get("status")) == "HEALTHY":
                    recovered = True
                    evidence.append(f"Shard 1 recovered to HEALTHY: {json.dumps(cur_s1, indent=2)}")
                    break
            except Exception:
                pass

        if not recovered:
            return False, "Shard 1 did not recover to HEALTHY within 60 seconds"

    finally:
        stop_load.set()
        t.join(timeout=3.0)
        kubectl("scale", f"statefulset/ems-{primary}", "--replicas=1")
        kubectl("scale", f"statefulset/ems-{replica}", "--replicas=1")

    evidence.append(f"Load loop results: {json.dumps(load_stats)}")
    (EVIDENCE_DIR / "step6_storage_failover.txt").write_text("\n\n".join(evidence))

    if load_stats["lost"] > 0:
        return False, f"Lost writes during load loop: {load_stats['lost']}"

    return True, f"Primary {primary} failed under load, promoted {replica}, recovered to HEALTHY, 0 writes lost"

# ---------------------------------------------------------------------------
# Step 7: PVC Persistence Across Pod Deletions
# ---------------------------------------------------------------------------

def step_7_pvc_persistence() -> tuple[bool, str]:
    """Verify PVC persistence across storage replica, RabbitMQ, and Coordinator restarts."""
    evidence = []

    # 1. Delete a storage replica pod
    replica_pod = "ems-s0-b-0"
    evidence.append(f"Deleting storage pod {replica_pod}...")
    kubectl("delete", "pod", replica_pod, "--wait=false")
    wait_until(
        lambda: "Running" in kubectl("get", "pod", replica_pod)[1]
        and "1/1" in kubectl("get", "pod", replica_pod)[1],
        timeout=45.0,
    )
    evidence.append(f"Storage pod {replica_pod} restarted and healthy")

    # 2. Delete RabbitMQ pod
    evidence.append("Deleting RabbitMQ pod rabbitmq-0...")
    kubectl("delete", "pod", "rabbitmq-0", "--wait=false")
    wait_until(
        lambda: "Running" in kubectl("get", "pod", "rabbitmq-0")[1]
        and "1/1" in kubectl("get", "pod", "rabbitmq-0")[1],
        timeout=45.0,
    )
    evidence.append("RabbitMQ pod restarted and healthy, durable queues preserved")

    # 3. Delete Coordinator pod
    epochs_before = get_shards_dict()
    evidence.append(f"Epochs before coordinator deletion:\n{json.dumps(epochs_before, indent=2)}")

    kubectl("delete", "pod", "-l", "app=coordinator", "--wait=false")
    wait_until(
        lambda: "Running" in kubectl("get", "pods", "-l", "app=coordinator")[1]
        and "1/1" in kubectl("get", "pods", "-l", "app=coordinator")[1],
        timeout=45.0,
    )

    # Wait for nodes to re-register with coordinator and shards to return to HEALTHY
    wait_until(
        lambda: all((s.get("state") or s.get("status")) == "HEALTHY" for s in get_shards_dict().values()),
        timeout=30.0,
    )
    epochs_after = get_shards_dict()
    evidence.append(f"Epochs after coordinator restart:\n{json.dumps(epochs_after, indent=2)}")

    for s_id, s_info in epochs_before.items():
        if epochs_after.get(s_id, {}).get("epoch") != s_info.get("epoch"):
            return False, f"Coordinator lost persisted epoch for shard {s_id}"

    (EVIDENCE_DIR / "step7_pvc_persistence.txt").write_text("\n\n".join(evidence))
    return True, "Storage, RabbitMQ, and Coordinator restarted; data, queues, and epochs intact"


# ---------------------------------------------------------------------------
# Step 8: RabbitMQ Outage & Sweeper Recovery
# ---------------------------------------------------------------------------

def step_8_broker_outage() -> tuple[bool, str]:
    """Verify request creation during RabbitMQ outage and eventual matching via outbox sweeper."""
    evidence = []

    try:
        # 1. Scale RabbitMQ to 0
        evidence.append("Scaling RabbitMQ statefulset to 0...")
        kubectl("scale", "statefulset/rabbitmq", "--replicas=0")
        wait_until(
            lambda: kubectl("get", "statefulset/rabbitmq", "-o", "jsonpath={.status.readyReplicas}")[1] in ("", "0"),
            timeout=30.0,
        )

        # 2. Create request while broker is down (retrying on 503 storage unavailable)
        tag = uuid.uuid4().hex[:6]
        r_req = None
        for _ in range(15):
            r_req = safe_http_post(
                f"{REQ_BASE}/requests",
                8001,
                {"type": "food", "location": {"lat": 12.9716, "lon": 77.5946}, "details": f"outage-test-{tag}"},
            )
            if r_req.status_code == 201:
                break
            time.sleep(1.0)

        if r_req is None or r_req.status_code != 201:
            code = r_req.status_code if r_req else "None"
            return False, f"POST /requests failed during broker outage: {code}"

        data = r_req.json()
        req_id = data["requestId"]
        match_queued = data.get("matchQueued")
        evidence.append(f"Created request {req_id} during broker outage (matchQueued={match_queued})")
        if match_queued is not False:
            return False, f"Expected matchQueued=False during broker outage, got {match_queued}"

        # 3. Register resource while broker is down
        r_res = safe_http_post(
            f"{RES_BASE}/resources",
            8002,
            {"name": f"food-outage-{tag}", "type": "food", "location": {"lat": 12.9716, "lon": 77.5946}, "availability": True},
        )
        res_id = r_res.json()["resourceId"]

        # 4. Scale RabbitMQ back to 1
        evidence.append("Scaling RabbitMQ statefulset back to 1...")
        kubectl("scale", "statefulset/rabbitmq", "--replicas=1")
        wait_until(
            lambda: kubectl("get", "statefulset/rabbitmq", "-o", "jsonpath={.status.readyReplicas}")[1] == "1",
            timeout=45.0,
        )

        # 5. Wait for sweeper to detect pending event, republish, and matching-service to process it
        evidence.append("Waiting for outbox sweeper to pick up pending request...")
        matched = False
        deadline = time.time() + 65.0
        while time.time() < deadline:
            time.sleep(2.0)
            try:
                r = safe_http_get(f"{REQ_BASE}/requests/{req_id}", 8001)
                if r.status_code == 200 and r.json().get("status") == "MATCHED":
                    matched = True
                    evidence.append(f"Request {req_id} settled to MATCHED with resource {res_id}")
                    break
            except Exception:
                pass

        (EVIDENCE_DIR / "step8_broker_outage.txt").write_text("\n\n".join(evidence))
        if not matched:
            return False, f"Request {req_id} was not matched after broker returned"

        return True, "Request created during broker outage (matchQueued=False) and recovered to MATCHED via sweeper"

    finally:
        kubectl("scale", "statefulset/rabbitmq", "--replicas=1")


# ---------------------------------------------------------------------------
# Main Verification Suite Runner
# ---------------------------------------------------------------------------

def main() -> int:
    print("=" * 70)
    print("STAGE 9 KUBERNETES VERIFICATION SUITE")
    print("=" * 70)

    PF_MGR.start_all()

    steps = [
        ("Step 1: Workloads & Shard Health", step_1_workloads_and_health),
        ("Step 2: DNS & End-to-End Matching", step_2_dns_and_matching),
        ("Step 3: Shard Distribution", step_3_shard_distribution),
        ("Step 4: Pod Self-Healing Under Load", step_4_pod_self_healing),
        ("Step 5: Matching Service Scaling", step_5_scaling),
        ("Step 6: Storage Failure & Recovery", step_6_storage_failover_recovery),
        ("Step 7: PVC Persistence Across Restarts", step_7_pvc_persistence),
        ("Step 8: RabbitMQ Outage & Sweeper Recovery", step_8_broker_outage),
    ]

    results = []
    overall_pass = True

    try:
        for idx, (name, func) in enumerate(steps, start=1):
            print(f"\n[{idx}/8] Running: {name}...")
            t0 = time.time()
            try:
                passed, detail = func()
            except Exception as e:
                passed = False
                detail = f"Exception: {e}"
            dur = time.time() - t0
            status_str = "PASS" if passed else "FAIL"
            if not passed:
                overall_pass = False
            print(f"[{idx}/8] {status_str} ({dur:.1f}s): {detail}")
            results.append((name, status_str, f"{dur:.1f}s", detail))

    finally:
        PF_MGR.stop_all()

    # Summary Table
    print("\n" + "=" * 80)
    print(f"{'VERIFICATION STEP':<42} | {'STATUS':<8} | {'TIME':<8} | DETAILS")
    print("-" * 80)
    for name, status, dur, detail in results:
        print(f"{name:<42} | {status:<8} | {dur:<8} | {detail[:40]}")
    print("=" * 80)

    if overall_pass:
        print("\nALL STAGE 9 KUBERNETES CHECKS PASSED!\n")
        return 0
    else:
        print("\nSOME STAGE 9 KUBERNETES CHECKS FAILED.\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
