import importlib.util
import os
import unittest

import yaml


def load_generator():
    spec = importlib.util.spec_from_file_location(
        "generate_storage", os.path.join(os.getcwd(), "k8s", "generate_storage.py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

class TestGenerateStorage(unittest.TestCase):
    def setUp(self):
        self.gen = load_generator()
    def _run(self, args):
        # simulate command line args
        return self.gen.main(args.split())
    def test_default_nodes(self):
        # capture stdout
        import sys
        from io import StringIO

        capture = StringIO()
        sys_stdout = sys.stdout
        sys.stdout = capture
        try:
            self.gen.main([])
        finally:
            sys.stdout = sys_stdout
        docs = list(yaml.safe_load_all(capture.getvalue()))
        # expect 3 * 2 = 6 documents (service+statefulset per node)
        self.assertEqual(len(docs), 6)
        names = [doc["metadata"]["name"] for doc in docs]
        # each node appears twice (service, statefulset)
        self.assertCountEqual(names, [
            "ems-s0-a", "ems-s0-a",
            "ems-s1-a", "ems-s1-a",
            "ems-s2-a", "ems-s2-a",
        ])
        # check services
        services = [d for d in docs if d["kind"] == "Service"]
        self.assertEqual(len(services), 3)
        for svc in services:
            self.assertEqual(svc["spec"]["clusterIP"], "None")
            self.assertTrue(svc["spec"].get("publishNotReadyAddresses"))

        # check that each StatefulSet has correct shard and role
        for doc in docs:
            if doc["kind"] == "StatefulSet":
                self.assertFalse(doc["spec"]["template"]["spec"].get("enableServiceLinks"))
                env = {e["name"]: e["value"] for e in doc["spec"]["template"]["spec"]["containers"][0]["env"] if "value" in e}
                self.assertIn("SHARD_ID", env)
                self.assertIn("INITIAL_ROLE", env)
                shard = int(env["SHARD_ID"])
                role = env["INITIAL_ROLE"]
                suffix = doc["metadata"]["name"].split('-')[-1]
                expected_role = "primary" if suffix == "a" else "replica"
                self.assertEqual(role, expected_role)
                self.assertIn(str(shard), str(env["SHARD_ID"]))
                # advertise URL format
                advertise = env.get("ADVERTISE_URL")
                self.assertTrue(advertise.startswith("http://ems-"))
                self.assertTrue(advertise.endswith(":9000"))
                # peer URL format
                peer = env.get("PEER_URL")
                self.assertTrue(peer.startswith("http://ems-"))
                self.assertTrue(peer.endswith(":9000"))
        # volumeClaimTemplates count
        for doc in docs:
            if doc["kind"] == "StatefulSet":
                self.assertEqual(len(doc["spec"].get("volumeClaimTemplates", [])), 1)
        # replicas
        for doc in docs:
            if doc["kind"] == "StatefulSet":
                self.assertEqual(doc["spec"]["replicas"], 1)

    def test_all_flag_gives_12_documents(self):
        import sys
        from io import StringIO

        capture = StringIO()
        sys_stdout = sys.stdout
        sys.stdout = capture
        try:
            self.gen.main(["--all"])
        finally:
            sys.stdout = sys_stdout
        docs = list(yaml.safe_load_all(capture.getvalue()))
        self.assertEqual(len(docs), 12)
        node_names = set()
        for doc in docs:
            node_names.add(doc["metadata"]["name"])
        expected = {
            "ems-s0-a", "ems-s0-b",
            "ems-s1-a", "ems-s1-b",
            "ems-s2-a", "ems-s2-b",
        }
        self.assertEqual(node_names, expected)

if __name__ == "__main__":
    unittest.main()
