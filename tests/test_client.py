"""Tests for the Roark HTTP client.

Uses ``httpx.MockTransport`` so we exercise the real ``httpx.AsyncClient`` paths
without any network. Verifies headers, payload shape, and the swallow-failures
contract (every method returns False / None on errors instead of raising).
"""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from pipecat_roark.client import API_KEY_HEADER, RoarkClient


WEBHOOK_URL = "https://webhook.example/"
UPLOAD_URL_ENDPOINT = "https://upload.example/"


def _client_with_mock(handler: Any) -> RoarkClient:
    """Build a RoarkClient whose internal AsyncClient uses the supplied mock."""
    client = RoarkClient(
        api_key="rk_test",
        webhook_url=WEBHOOK_URL,
        upload_url_endpoint=UPLOAD_URL_ENDPOINT,
    )
    # We poke a pre-built httpx.AsyncClient into the private slot so the same
    # mock transport handles every call. Direct equivalent of __aenter__.
    client._client = httpx.AsyncClient(  # type: ignore[attr-defined]
        transport=httpx.MockTransport(handler),
        headers={API_KEY_HEADER: "rk_test"},
    )
    return client


@pytest.mark.asyncio
async def test_post_call_started_sends_api_key_and_payload() -> None:
    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["api_key"] = request.headers.get(API_KEY_HEADER)
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={"ok": True})

    client = _client_with_mock(handler)
    ok = await client.post_call_started(
        {"event": "call-started", "pipecatCallId": "abc", "eventTimestamp": "t", "agentId": "a"}
    )
    await client.aclose()

    assert ok is True
    assert seen["url"] == WEBHOOK_URL
    assert seen["api_key"] == "rk_test"
    assert seen["body"]["event"] == "call-started"
    assert seen["body"]["agentId"] == "a"


@pytest.mark.asyncio
async def test_post_returns_false_on_5xx() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="boom")

    client = _client_with_mock(handler)
    ok = await client.post_call_ended(
        {"event": "call-ended", "pipecatCallId": "abc", "eventTimestamp": "t", "callEndedReason": "x"}
    )
    await client.aclose()
    assert ok is False


@pytest.mark.asyncio
async def test_request_upload_url_returns_none_on_failure() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    client = _client_with_mock(handler)
    out = await client.request_upload_url(pipecat_call_id="abc")
    await client.aclose()
    assert out is None


@pytest.mark.asyncio
async def test_request_upload_url_returns_response_on_success() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"uploadUrl": "https://s3/x", "s3Key": "k", "expiresInSeconds": 900})

    client = _client_with_mock(handler)
    out = await client.request_upload_url(pipecat_call_id="abc")
    await client.aclose()
    assert out == {"uploadUrl": "https://s3/x", "s3Key": "k", "expiresInSeconds": 900}
