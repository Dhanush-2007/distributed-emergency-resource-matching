import uuid

import httpx
import pytest

BASE_URL = "http://localhost:8080"


def is_dashboard_up():
    try:
        r = httpx.get(f"{BASE_URL}/", timeout=2.0)
        return r.status_code == 200
    except (httpx.HTTPError, OSError):
        return False


pytestmark = pytest.mark.skipif(
    not is_dashboard_up(),
    reason="Dashboard container not running on http://localhost:8080",
)


def test_dashboard_serves_html():
    with httpx.Client(base_url=BASE_URL, timeout=5.0) as client:
        r = client.get("/")
        assert r.status_code == 200
        assert "text/html" in r.headers.get("content-type", "")
        assert "EMS CONTROL" in r.text or "EMS Operations Control" in r.text
        assert 'id="main-content"' in r.text


def test_dashboard_serves_static_assets():
    with httpx.Client(base_url=BASE_URL, timeout=5.0) as client:
        r_css = client.get("/css/style.css")
        assert r_css.status_code == 200
        assert "text/css" in r_css.headers.get("content-type", "")

        r_js = client.get("/js/app.js")
        assert r_js.status_code == 200
        assert "javascript" in r_js.headers.get("content-type", "")


def test_proxy_coordinator_api():
    with httpx.Client(base_url=BASE_URL, timeout=5.0) as client:
        r = client.get("/api/coord/diagnostics/shards")
        assert r.status_code == 200
        data = r.json()
        shards = data if isinstance(data, list) else data.get("shards", [])
        assert len(shards) == 3


def test_proxy_request_service_api():
    with httpx.Client(base_url=BASE_URL, timeout=5.0) as client:
        r = client.get("/api/req/requests")
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, (list, dict))


def test_proxy_resource_service_api():
    with httpx.Client(base_url=BASE_URL, timeout=5.0) as client:
        r = client.get("/api/res/resources")
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, (list, dict))


def test_proxy_error_handling_400():
    with httpx.Client(base_url=BASE_URL, timeout=5.0) as client:
        r = client.post("/api/req/requests", json={})
        assert r.status_code == 400
        data = r.json()
        assert "error" in data
        assert data["error"].get("code") == "INVALID_INPUT"


def test_proxy_error_handling_404():
    with httpx.Client(base_url=BASE_URL, timeout=5.0) as client:
        r = client.get(f"/api/req/requests/{uuid.uuid4()}")
        assert r.status_code == 404
        data = r.json()
        assert "error" in data
        assert data["error"].get("code") == "NOT_FOUND"
