import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DASHBOARD_DIR = REPO_ROOT / "dashboard"


def test_index_html_structure():
    index_file = DASHBOARD_DIR / "index.html"
    assert index_file.exists(), "index.html must exist"
    content = index_file.read_text(encoding="utf-8")

    assert "<!DOCTYPE html>" in content
    assert '<meta charset="UTF-8">' in content or '<meta charset="utf-8">' in content
    assert 'name="viewport"' in content
    assert '<script type="module" src="/js/app.js"></script>' in content or '<script type="module" src="js/app.js"></script>' in content
    assert '<link rel="stylesheet" href="css/base.css">' in content
    assert '<link rel="stylesheet" href="css/layout.css">' in content
    assert '<link rel="stylesheet" href="css/components.css">' in content
    assert '<link rel="stylesheet" href="css/views.css">' in content

    required_ids = [
        "global-alert",
        "main-content",
        "side-drawer",
        "drawer-backdrop",
        "strip-coord",
        "strip-shards",
        "strip-time",
        "nav-overview",
        "nav-requests",
        "nav-resources",
        "nav-cluster",
    ]
    for dom_id in required_ids:
        assert f'id="{dom_id}"' in content, f"Missing required DOM id '{dom_id}' in index.html"


def test_css_files_and_palette():
    css_dir = DASHBOARD_DIR / "css"
    for name in ["base.css", "layout.css", "components.css", "views.css", "style.css"]:
        assert (css_dir / name).exists(), f"Missing stylesheet: {name}"

    base_css = (css_dir / "base.css").read_text(encoding="utf-8")
    assert "#F4F1EA" in base_css or "#f4f1ea" in base_css
    assert "#1B1B18" in base_css or "#1b1b18" in base_css
    assert "#D9D4C7" in base_css or "#d9d4c7" in base_css
    assert "#D9480F" in base_css or "#d9480f" in base_css


def test_javascript_files_exist_and_clean():
    js_dir = DASHBOARD_DIR / "js"
    assert js_dir.exists()

    expected_files = [
        "app.js",
        "api.js",
        "utils.js",
        "views/overview.js",
        "views/requests.js",
        "views/resources.js",
        "views/cluster.js",
        "views/requests_drawer.js",
    ]

    for rel_path in expected_files:
        js_file = js_dir / rel_path
        assert js_file.exists(), f"Missing JS file: {rel_path}"
        code = js_file.read_text(encoding="utf-8")

        # Static guardrails: no dangerous patterns
        assert "eval(" not in code, f"eval() forbidden in {rel_path}"
        assert "document.write" not in code, f"document.write forbidden in {rel_path}"
        assert "innerHTML" not in code, f"innerHTML forbidden in {rel_path}; use safe DOM builder"

        # Check internal imports resolve
        import_matches = re.findall(r'from\s+["\'](\.[^"\']+)["\']', code)
        for imp in import_matches:
            target = (js_file.parent / imp).resolve()
            assert target.exists(), f"Import '{imp}' in {rel_path} does not exist at {target}"


def test_no_raft_terminology():
    for f in DASHBOARD_DIR.rglob("*.js"):
        content = f.read_text(encoding="utf-8")
        assert not re.search(r"\braft\b", content, re.IGNORECASE), f"Raft terminology found in {f}"


def test_nginx_conf():
    nginx_conf = DASHBOARD_DIR / "nginx.conf"
    assert nginx_conf.exists()
    content = nginx_conf.read_text(encoding="utf-8")

    assert "listen 80;" in content
    assert "location /api/coord/" in content
    assert "location /api/req/" in content
    assert "location /api/res/" in content
    assert "proxy_pass" in content
    assert "gzip on;" in content


def test_dockerfile():
    dockerfile = DASHBOARD_DIR / "Dockerfile"
    assert dockerfile.exists()
    content = dockerfile.read_text(encoding="utf-8")

    assert "FROM nginx" in content
    assert "HEALTHCHECK" in content
    assert "COPY nginx.conf" in content
