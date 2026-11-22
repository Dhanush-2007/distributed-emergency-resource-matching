"""Component tests for coordinator client unreachable fault handling."""

import time

import httpx
import pytest
from ems_common.errors import StorageUnavailable
from ems_common.storage_client import CoordinatorClient

pytestmark = pytest.mark.component


def test_coordinator_unreachable_respects_deadline_get():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("Connection refused")

    mock_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = CoordinatorClient(
        base_url="http://fake-coord:8000",
        http_client=mock_client,
        retries=3,
        deadline=0.2,
    )
    start = time.monotonic()
    with pytest.raises(StorageUnavailable) as exc_info:
        client.get("test:key")
    elapsed = time.monotonic() - start

    assert exc_info.value.headers.get("Retry-After") is not None
    assert elapsed < 0.35, f"Elapsed {elapsed}s exceeded deadline 0.2s"


def test_coordinator_unreachable_scan_allow_partial_raises_storage_unavailable():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("Connection refused")

    mock_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = CoordinatorClient(
        base_url="http://fake-coord:8000",
        http_client=mock_client,
        retries=3,
        deadline=0.2,
    )
    start = time.monotonic()
    with pytest.raises(StorageUnavailable) as exc_info:
        client.scan(prefix="test:", allow_partial=True)
    elapsed = time.monotonic() - start

    assert exc_info.value.headers.get("Retry-After") is not None
    assert elapsed < 0.35, f"Elapsed {elapsed}s exceeded deadline 0.2s"
