"""Integration runner to start uvicorn and execute curl requests."""

import json
import subprocess
import time
import urllib.error
import urllib.request


def run_curl_examples():
    # Start storage node process
    import os

    env = os.environ.copy()
    env["PYTHONPATH"] = "services/storage-node"
    env["NODE_ID"] = "s0-a"
    env["SHARD_ID"] = "0"
    env["PORT"] = "9000"
    env["STANDALONE"] = "true"

    proc = subprocess.Popen(
        [".venv/Scripts/python.exe", "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", "9000"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        time.sleep(2)

        def make_request(method, url, data=None):
            headers = {"Content-Type": "application/json"}
            body = json.dumps(data).encode("utf-8") if data else None
            req = urllib.request.Request(url, data=body, headers=headers, method=method)
            try:
                with urllib.request.urlopen(req) as response:
                    return response.status, response.read().decode("utf-8")
            except urllib.error.HTTPError as e:
                return e.code, e.read().decode("utf-8")

        print("=== 1. Health ===")
        code, body = make_request("GET", "http://127.0.0.1:9000/health")
        print(f"HTTP {code}\n{body}\n")

        print("=== 2. Create Record (PUT ifVersion=0) ===")
        code, body = make_request(
            "PUT",
            "http://127.0.0.1:9000/internal/data/request:3f2c9a1e-0001",
            {"value": {"type": "water", "status": "PENDING"}, "ifVersion": 0},
        )
        print(f"HTTP {code}\n{body}\n")

        print("=== 3. Conflict (PUT ifVersion=0 on existing key) ===")
        code, body = make_request(
            "PUT",
            "http://127.0.0.1:9000/internal/data/request:3f2c9a1e-0001",
            {"value": {"type": "water"}, "ifVersion": 0},
        )
        print(f"HTTP {code}\n{body}\n")

        print("=== 4. Update Record (PUT ifVersion=1) ===")
        code, body = make_request(
            "PUT",
            "http://127.0.0.1:9000/internal/data/request:3f2c9a1e-0001",
            {"value": {"type": "water", "status": "MATCHED"}, "ifVersion": 1},
        )
        print(f"HTTP {code}\n{body}\n")

        print("=== 5. Get Record ===")
        code, body = make_request("GET", "http://127.0.0.1:9000/internal/data/request:3f2c9a1e-0001")
        print(f"HTTP {code}\n{body}\n")

        print("=== 6. Scan Prefix ===")
        code, body = make_request("GET", "http://127.0.0.1:9000/internal/scan?prefix=request:")
        print(f"HTTP {code}\n{body}\n")

        print("=== 7. Delete Record ===")
        code, body = make_request("DELETE", "http://127.0.0.1:9000/internal/data/request:3f2c9a1e-0001")
        print(f"HTTP {code}\n{body}\n")

        print("=== 8. Get Deleted (expect 404) ===")
        code, body = make_request("GET", "http://127.0.0.1:9000/internal/data/request:3f2c9a1e-0001")
        print(f"HTTP {code}\n{body}\n")

    finally:
        proc.terminate()
        proc.wait()


if __name__ == "__main__":
    run_curl_examples()
