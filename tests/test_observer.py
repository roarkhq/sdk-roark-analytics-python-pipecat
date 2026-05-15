"""Tests for the RoarkObserver.

Skip these if pipecat-ai isn't installed in the test env (the observer
imports it at construction time). They exercise the no-network path: we
swap the internal ``RoarkClient`` for a fake that records calls.
"""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("pipecat", reason="pipecat-ai not installed in this env")

from pipecat.frames.frames import StartFrame  # noqa: E402
from pipecat.observers.base_observer import FramePushed  # noqa: E402
from pipecat.processors.frame_processor import FrameDirection  # noqa: E402

from pipecat_roark.observer import RoarkObserver  # noqa: E402


def _push(frame: Any) -> FramePushed:
    """Build a minimal FramePushed for observer tests.

    Source/destination are observer-irrelevant here, so we pass ``None`` and
    cast — the observer only reads ``data.frame``.
    """
    return FramePushed(
        source=None,  # type: ignore[arg-type]
        destination=None,  # type: ignore[arg-type]
        frame=frame,
        direction=FrameDirection.DOWNSTREAM,
        timestamp=0,
    )


class _FakeClient:
    def __init__(self) -> None:
        self.started: list[dict[str, Any]] = []
        self.ended: list[dict[str, Any]] = []
        self.upload_requested = 0
        self.upload_url_response: dict[str, Any] | None = {
            "uploadUrl": "https://s3/x",
            "s3Key": "calls/p/abc/mono-recording.wav",
            "expiresInSeconds": 900,
        }
        self.upload_succeeded = True

    async def post_call_started(self, payload: dict[str, Any]) -> bool:
        self.started.append(payload)
        return True

    async def post_call_ended(self, payload: dict[str, Any]) -> bool:
        self.ended.append(payload)
        return True

    async def request_upload_url(self, **_: Any) -> dict[str, Any] | None:
        self.upload_requested += 1
        return self.upload_url_response

    async def upload_recording(self, **_: Any) -> bool:
        return self.upload_succeeded

    async def aclose(self) -> None:
        pass


@pytest.mark.asyncio
async def test_start_frame_posts_call_started_with_required_fields() -> None:
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1", agent_name="Agent 1")
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    await obs.on_push_frame(_push(StartFrame()))

    assert len(fake.started) == 1
    payload = fake.started[0]
    assert payload["event"] == "call-started"
    assert payload["agentId"] == "agent-1"
    assert payload["agentName"] == "Agent 1"
    assert payload["interfaceType"] == "WEB"
    assert "pipecatCallId" in payload
    assert "eventTimestamp" in payload


@pytest.mark.asyncio
async def test_start_frame_infers_phone_interface_from_phone_numbers() -> None:
    obs = RoarkObserver(
        api_key="rk_test",
        agent_id="agent-1",
        agent_phone_number="+15550000",
        customer_phone_number="+15551111",
    )
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    await obs.on_push_frame(_push(StartFrame()))
    assert fake.started[0]["interfaceType"] == "PHONE"
    assert fake.started[0]["agentPhoneNumber"] == "+15550000"
    assert fake.started[0]["customerPhoneNumber"] == "+15551111"


@pytest.mark.asyncio
async def test_record_audio_false_skips_upload_request() -> None:
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1", record_audio=False)
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    await obs.on_push_frame(_push(StartFrame()))
    await obs._flush_call_ended(reason="agent-ended")

    assert fake.upload_requested == 0
    assert len(fake.ended) == 1
    assert "recordingS3Key" not in fake.ended[0]


@pytest.mark.asyncio
async def test_multiple_start_frames_only_post_call_started_once() -> None:
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1", record_audio=False)
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    await obs.on_push_frame(_push(StartFrame()))
    await obs.on_push_frame(_push(StartFrame()))
    await obs.on_push_frame(_push(StartFrame()))

    assert len(fake.started) == 1


@pytest.mark.asyncio
async def test_double_flush_only_posts_once() -> None:
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1", record_audio=False)
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    await obs.on_push_frame(_push(StartFrame()))
    await obs._flush_call_ended(reason="agent-ended")
    await obs._flush_call_ended(reason="agent-ended")

    assert len(fake.ended) == 1
