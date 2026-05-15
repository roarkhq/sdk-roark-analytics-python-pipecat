"""HTTP client for the Roark webhook + presigned upload endpoints.

All methods are best-effort: failures are logged and surfaced as return values,
never raised. The observer must never break the call.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import httpx

from ._types import CallEndedPayload, CallStartedPayload, UploadUrlResponse

API_KEY_HEADER = "x-roark-api-key"
DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_UPLOAD_TIMEOUT_SECONDS = 120.0  # WAV PUTs can be large for long calls

# Endpoint URLs are not baked into source — they come from the constructor or
# env vars. See ``.env.example`` for the production values. This lets the same
# package run against prod / staging / regional stacks with no code changes,
# and avoids accidental prod hits from a misconfigured environment.
ENV_WEBHOOK_URL = "ROARK_WEBHOOK_URL"
ENV_UPLOAD_URL_ENDPOINT = "ROARK_UPLOAD_URL_ENDPOINT"


def _resolve(explicit: str | None, env_var: str) -> str:
    if explicit is not None:
        return explicit
    value = os.environ.get(env_var)
    if not value:
        raise ValueError(
            f"Roark endpoint not configured: pass the corresponding kwarg or set "
            f"the {env_var} env var (see .env.example for the production value)."
        )
    return value

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
        webhook_url: str | None = None,
        upload_url_endpoint: str | None = None,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        self._api_key = api_key
        # Precedence: explicit kwarg > env var. No source-level fallback — we
        # raise so misconfiguration surfaces at startup instead of mid-call.
        self._webhook_url = _resolve(webhook_url, ENV_WEBHOOK_URL)
        self._upload_url_endpoint = _resolve(upload_url_endpoint, ENV_UPLOAD_URL_ENDPOINT)
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
        return await self._post_event(dict(payload))

    async def post_call_ended(self, payload: CallEndedPayload) -> bool:
        """POST call-ended. Returns True on 2xx, False on any failure."""
        return await self._post_event(dict(payload))

    async def request_upload_url(
        self, *, pipecat_call_id: str, kind: str = "mono", content_type: str = "audio/wav"
    ) -> UploadUrlResponse | None:
        """Ask Roark for a presigned S3 PUT URL. Returns None on failure."""
        client = await self._ensure_client()
        url = self._upload_url_endpoint
        body = {
            "pipecatCallId": pipecat_call_id,
            "kind": kind,
            "contentType": content_type,
        }
        log.info(
            "upload-url POST -> %s pipecatCallId=%s kind=%s contentType=%s",
            url,
            pipecat_call_id,
            kind,
            content_type,
        )
        try:
            resp = await client.post(url, json=body)
        except httpx.HTTPError as err:
            # httpx.HTTPError subclasses often have empty str(); repr() carries the type.
            log.warning(
                "upload-url POST %s raised %s: %r",
                url,
                type(err).__name__,
                err,
                exc_info=log.isEnabledFor(logging.DEBUG),
            )
            return None
        if resp.status_code >= 400:
            log.warning(
                "upload-url POST %s returned HTTP %s: %s",
                url,
                resp.status_code,
                resp.text[:500],
            )
            return None
        try:
            parsed = resp.json()
        except ValueError as err:
            log.warning(
                "upload-url POST %s returned non-JSON (status %s): %r body=%s",
                url,
                resp.status_code,
                err,
                resp.text[:500],
            )
            return None
        s3_key = parsed.get("s3Key") if isinstance(parsed, dict) else None
        upload_url_host = (
            _redact_query(parsed.get("uploadUrl", ""))
            if isinstance(parsed, dict)
            else ""
        )
        log.info(
            "upload-url POST %s -> HTTP %s s3Key=%s uploadUrlHost=%s",
            url,
            resp.status_code,
            s3_key,
            upload_url_host,
        )
        return parsed

    async def _post_event(self, body: dict[str, Any]) -> bool:
        client = await self._ensure_client()
        url = self._webhook_url
        event = body.get("event", "?")
        call_id = body.get("pipecatCallId", "?")
        log.debug(
            "webhook POST -> %s event=%s pipecatCallId=%s keys=%s",
            url,
            event,
            call_id,
            sorted(body.keys()),
        )
        try:
            resp = await client.post(url, json=body)
        except httpx.HTTPError as err:
            log.warning(
                "webhook POST %s (event=%s, pipecatCallId=%s) raised %s: %r",
                url,
                event,
                call_id,
                type(err).__name__,
                err,
                exc_info=log.isEnabledFor(logging.DEBUG),
            )
            return False
        if resp.status_code >= 400:
            log.warning(
                "webhook POST %s (event=%s, pipecatCallId=%s) returned HTTP %s: %s",
                url,
                event,
                call_id,
                resp.status_code,
                resp.text[:500],
            )
            return False
        log.debug(
            "webhook POST %s (event=%s, pipecatCallId=%s) -> HTTP %s",
            url,
            event,
            call_id,
            resp.status_code,
        )
        return True

    async def upload_recording(
        self, *, upload_url: str, body: bytes, content_type: str = "audio/wav"
    ) -> bool:
        """PUT the WAV blob to the presigned S3 URL. Returns True on 2xx."""
        # Use a fresh client (no auth headers — the URL is presigned) and a
        # generous timeout. A long call's WAV can be tens of MB.
        size = len(body)
        log.info(
            "recording PUT -> %s bytes=%d content_type=%s",
            _redact_query(upload_url),
            size,
            content_type,
        )
        try:
            async with httpx.AsyncClient(timeout=DEFAULT_UPLOAD_TIMEOUT_SECONDS) as upload_client:
                resp = await upload_client.put(
                    upload_url, content=body, headers={"content-type": content_type}
                )
        except httpx.HTTPError as err:
            log.warning(
                "recording PUT %s (bytes=%d) raised %s: %r",
                _redact_query(upload_url),
                size,
                type(err).__name__,
                err,
                exc_info=log.isEnabledFor(logging.DEBUG),
            )
            return False
        if resp.status_code >= 400:
            log.warning(
                "recording PUT %s (bytes=%d) returned HTTP %s: %s",
                _redact_query(upload_url),
                size,
                resp.status_code,
                resp.text[:500],
            )
            return False
        log.info(
            "recording PUT %s (bytes=%d) -> HTTP %s",
            _redact_query(upload_url),
            size,
            resp.status_code,
        )
        return True


def _redact_query(url: str) -> str:
    """Strip query string from a presigned URL before logging — the signature is sensitive."""
    return url.split("?", 1)[0]
