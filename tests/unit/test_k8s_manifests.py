"""Unit tests for Kubernetes manifests compliance (Stage 9)."""

from __future__ import annotations

import io
import re
import sys
from pathlib import Path

import pytest
import yaml

from k8s.generate_storage import main as gen_main

pytestmark = pytest.mark.unit

K8S_DIR = Path(__file__).resolve().parents[2] / "k8s"


def _load_all_docs() -> list[dict]:
    docs = []
    yaml_files = sorted(K8S_DIR.glob("*.yaml"))
    assert len(yaml_files) >= 6, f"Expected at least 6 yaml files, found {len(yaml_files)}"
    for f in yaml_files:
        content = f.read_text()
        parsed = list(yaml.safe_load_all(content))
        for doc in parsed:
            if doc:
                docs.append(doc)
    return docs


def test_committed_storage_nodes_equals_generator():
    """Verify that committed storage-nodes.yaml exactly matches generate_storage.py --all."""
    storage_yaml_path = K8S_DIR / "storage-nodes.yaml"
    assert storage_yaml_path.exists(), "k8s/storage-nodes.yaml does not exist"

    capture = io.StringIO()
    old_stdout = sys.stdout
    sys.stdout = capture
    try:
        gen_main(["--all"])
    finally:
        sys.stdout = old_stdout

    gen_output = capture.getvalue()
    committed_content = storage_yaml_path.read_text()

    # Compare parsed YAML docs to avoid trivial whitespace differences
    gen_docs = list(yaml.safe_load_all(gen_output))
    committed_docs = list(yaml.safe_load_all(committed_content))
    assert len(gen_docs) == 12
    assert gen_docs == committed_docs


def test_workload_specs_compliance():
    """Verify enableServiceLinks=false, probes, resources, securityContext, and replicas."""
    docs = _load_all_docs()
    workloads = [d for d in docs if d.get("kind") in ("Deployment", "StatefulSet")]

    # 6 storage StatefulSets + 1 rabbitmq StatefulSet + 1 coordinator Deploy + 3 app Deployments = 11
    assert len(workloads) == 11

    for w in workloads:
        name = w["metadata"]["name"]
        kind = w["kind"]
        pod_spec = w["spec"]["template"]["spec"]

        # enableServiceLinks false
        assert pod_spec.get("enableServiceLinks") is False, f"{kind}/{name} missing enableServiceLinks: false"

        # securityContext
        sec_ctx = pod_spec.get("securityContext", {})
        assert sec_ctx.get("runAsNonRoot") is True, f"{kind}/{name} missing runAsNonRoot: true"
        assert sec_ctx.get("runAsUser") in (10001, 999), f"{kind}/{name} unexpected runAsUser {sec_ctx.get('runAsUser')}"
        assert sec_ctx.get("fsGroup") in (10001, 999), f"{kind}/{name} unexpected fsGroup {sec_ctx.get('fsGroup')}"

        # StatefulSets must have replicas == 1
        if kind == "StatefulSet":
            assert w["spec"]["replicas"] == 1, f"StatefulSet/{name} has replicas != 1"

        # Container probes and resources
        containers = pod_spec.get("containers", [])
        assert len(containers) >= 1, f"{kind}/{name} has no containers"
        for c in containers:
            cname = c["name"]
            # Probes
            assert "livenessProbe" in c, f"{kind}/{name} container {cname} missing livenessProbe"
            assert "readinessProbe" in c, f"{kind}/{name} container {cname} missing readinessProbe"
            assert "startupProbe" in c, f"{kind}/{name} container {cname} missing startupProbe"

            # Resources
            res = c.get("resources", {})
            assert "requests" in res, f"{kind}/{name} container {cname} missing resources.requests"
            assert "limits" in res, f"{kind}/{name} container {cname} missing resources.limits"
            assert "memory" in res["requests"] and "cpu" in res["requests"]
            assert "memory" in res["limits"]


def test_no_hardcoded_credentials_or_pod_ips():
    """Ensure no plaintext passwords in manifests (only in ems-secret) and no raw pod IPs."""
    docs = _load_all_docs()
    ip_pattern = re.compile(r"\b(?:10\.\d{1,3}\.\d{1,3}\.\d{1,3}|172\.(?:1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3}|192\.168\.\d{1,3}\.\d{1,3})\b")

    for doc in docs:
        kind = doc.get("kind")
        name = doc.get("metadata", {}).get("name")
        doc_str = yaml.dump(doc)

        # Check for pod IP addresses
        assert not ip_pattern.search(doc_str), f"{kind}/{name} contains raw pod IP"

        # Credentials must not appear in plaintext outside ems-secret
        if kind != "Secret":
            assert "ems-dev-password" not in doc_str, f"{kind}/{name} contains hardcoded credential"


def test_image_tags():
    """All application images must use ems/<name>:1.0."""
    docs = _load_all_docs()
    workloads = [d for d in docs if d.get("kind") in ("Deployment", "StatefulSet")]

    for w in workloads:
        name = w["metadata"]["name"]
        for c in w["spec"]["template"]["spec"]["containers"]:
            img = c["image"]
            if name == "rabbitmq":
                assert img == "rabbitmq:3.13-management"
            else:
                assert re.match(r"^ems/[a-z0-9-]+:1\.0$", img), f"Invalid image tag {img} for {name}"
