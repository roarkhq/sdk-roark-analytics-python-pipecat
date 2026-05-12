"""HTTP client for the Roark webhook + presigned upload endpoints.

All methods are best-effort: failures are logged and surfaced as return values,
never raised. The observer must never break the call.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from ._types import CallEndedPayload, CallStartedPayload, UploadUrlResponse

API_KEY_HEADER = "x-roark-api-key"
DEFAULT_TIMEOUT_SECONDS = 10.0
DEFAULT_UPLOAD_TIMEOUT_SECONDS = 120.0  # WAV PUTs can be large for long calls

log = logging.getLogger("pipecat_roark.client")


class RoarkClient:
    """Async HTTP client for the Pipecat observer endpoints on Roark.

    One instance per ``RoarkObserver``; reuses an ``httpx.AsyncClient`` for
    keep-alive between the call-started, upload-url, PUT, and call-ended
    requests issued during a single call.
    """

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str = "https://api.roark.ai",
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "RoarkClient":
        await self._ensure_client()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.aclose()

    async def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self._timeout,
                headers={API_KEY_HEADER: self._api_key},
            )
        return self._client

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def post_call_started(self, payload: CallStartedPayload) -> bool:
        """POST call-started. Returns True on 2xx, False on any failure."""
        return await self._post_event("/v1/integrations/pipecat", dict(payload))

    async def post_call_ended(self, payload: CallEndedPayload) -> bool:
        """POST call-ended. Returns True on 2xx, False on any failure."""
        return await self._post_event("/v1/integrations/pipecat", dict(payload))

    async def request_upload_url(
        self, *, pipecat_call_id: str, kind: str = "mono", content_type: str = "audio/wav"
    ) -> UploadUrlResponse | None:
        """Ask Roark for a presigned S3 PUT URL. Returns None on failure."""
        client = await self._ensure_client()
        try:
            resp = await client.post(
                f"{self._base_url}/v1/integrations/pipecat/recording-upload-url",
                json={
                    "pipecatCallId": pipecat_call_id,
                    "kind": kind,
                    "contentType": content_type,
                },
            )
        except httpx.HTTPError as err:
            log.warning("upload-url request failed: %s", err)
            return None
        if resp.status_code >= 400:
            log.warning("upload-url returned %s: %s", resp.status_code, resp.text[:300])
            return None
        try:
            return resp.json()
        except ValueError as err:
            log.warning("upload-url response was not JSON: %s", err)
            return None

    async def upload_recording(
        self, *, upload_url: str, body: bytes, content_type: str = "audio/wav"
    ) -> bool:
        """PUT the WAV blob to the presigned S3 URL. Returns True on 2xx."""
        # Use a fresh client (no auth headers — the URL is presigned) and a
        # generous timeout. A long call's WAV can be tens of MB.
        try:
            async with httpx.AsyncClient(timeout=DEFAULT_UPLOAD_TIMEOUT_SECONDS) as upload_client:
                resp = await upload_client.put(
                    upload_url, content=body, headers={"content-type": content_type}
                )
        except httpx.HTTPError as err:
            log.warning("recording PUT failed: %s", err)
            return False
        if resp.status_code >= 400:
            log.warning("recording PUT returned %s: %s", resp.status_code, resp.text[:300])
            return False
        return True

    async def _post_event(self, path: str, body: dict[str, Any]) -> bool:
        client = await self._ensure_client()
        try:
            resp = await client.post(f"{self._base_url}{path}", json=body)
        except httpx.HTTPError as err:
            log.warning("POST %s failed: %s", path, err)
            return False
        if resp.status_code >= 400:
            log.warning("POST %s returned %s: %s", path, resp.status_code, resp.text[:300])
            return False
        return True
