"""Tests for the RoarkObserver.

Skip if pipecat-ai isn't installed. We swap the internal ``RoarkClient`` for a
fake that records calls so tests exercise no network. Audio upload paths are
exercised by faking AudioBufferProcessor.on_audio_data events rather than by
pushing audio frames through the observer — the real processor lives in the
pipeline and the observer only subscribes to its event.
"""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("pipecat", reason="pipecat-ai not installed in this env")

from pipecat.frames.frames import (  # noqa: E402
    EndFrame,
    StartFrame,
    TranscriptionFrame,
)
from pipecat.observers.base_observer import FramePushed  # noqa: E402
from pipecat.processors.frame_processor import FrameDirection  # noqa: E402

from pipecat_roark.observer import RoarkObserver  # noqa: E402


def _push(frame: Any) -> FramePushed:
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
        self.chunk_url_requests: list[dict[str, Any]] = []
        self.chunk_uploads: list[bytes] = []

    async def post_call_started(self, payload: dict[str, Any]) -> bool:
        self.started.append(payload)
        return True

    async def post_call_ended(self, payload: dict[str, Any]) -> bool:
        self.ended.append(payload)
        return True

    async def request_chunk_upload_url(self, **kwargs: Any) -> dict[str, Any] | None:
        self.chunk_url_requests.append(kwargs)
        return {
            "uploadUrl": f"https://s3/{kwargs['chunk_index']}",
            "s3Key": f"calls/p/abc/chunks/{kwargs['chunk_index']:06d}.pcm",
            "chunkIndex": kwargs["chunk_index"],
        }

    async def upload_chunk(
        self, *, upload_url: str, body: bytes, content_type: str = "audio/pcm"
    ) -> bool:
        self.chunk_uploads.append(body)
        return True

    async def aclose(self) -> None:
        pass


class _FakeAudioBufferProcessor:
    """Minimal stub of ``AudioBufferProcessor`` for unit tests.

    The real processor lives in the pipeline. From the observer's perspective the
    contract is: ``add_event_handler``, ``start_recording``, ``stop_recording``,
    ``sample_rate``, ``num_channels``. We don't simulate frame processing — tests
    drive ``on_audio_data`` directly to exercise the upload path.
    """

    def __init__(self, *, sample_rate: int = 24000, num_channels: int = 2) -> None:
        self.sample_rate = sample_rate
        self.num_channels = num_channels
        self._handlers: list[Any] = []
        self.start_calls = 0
        self.stop_calls = 0

    def add_event_handler(self, name: str, handler: Any) -> None:
        assert name == "on_audio_data"
        self._handlers.append(handler)

    async def start_recording(self) -> None:
        self.start_calls += 1

    async def stop_recording(self) -> None:
        self.stop_calls += 1

    async def emit_audio(self, pcm: bytes) -> None:
        for h in self._handlers:
            await h(self, pcm, self.sample_rate, self.num_channels)


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
async def test_sampling_rate_forwarded_when_set_and_omitted_when_unset() -> None:
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1")
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]
    await obs.on_push_frame(_push(StartFrame()))
    assert "samplingRate" not in fake.started[0]

    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1", sampling_rate=0.25)
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]
    await obs.on_push_frame(_push(StartFrame()))
    assert fake.started[0]["samplingRate"] == 0.25


@pytest.mark.asyncio
async def test_multiple_start_frames_only_post_call_started_once() -> None:
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1")
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    for _ in range(3):
        await obs.on_push_frame(_push(StartFrame()))
    assert len(fake.started) == 1


@pytest.mark.asyncio
async def test_double_flush_only_posts_once() -> None:
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1")
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    await obs.on_push_frame(_push(StartFrame()))
    await obs.on_push_frame(_push(EndFrame()))
    await obs.on_push_frame(_push(EndFrame()))

    assert len(fake.ended) == 1


@pytest.mark.asyncio
async def test_transcript_and_tool_calls_land_on_call_ended() -> None:
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1")
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    await obs.on_push_frame(_push(StartFrame()))
    frame = TranscriptionFrame(text="hello", user_id="user", timestamp="t")
    frame.finalized = True  # type: ignore[attr-defined]
    await obs.on_push_frame(_push(frame))
    await obs.on_push_frame(_push(EndFrame()))

    ended = fake.ended[0]
    assert len(ended["transcript"]) == 1
    assert ended["transcript"][0]["text"] == "hello"


@pytest.mark.asyncio
async def test_audio_buffer_processor_drives_chunk_uploads() -> None:
    abp = _FakeAudioBufferProcessor(sample_rate=24000, num_channels=2)
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1", audio_buffer_processor=abp)
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    await obs.on_push_frame(_push(StartFrame()))
    assert abp.start_calls == 1, "StartFrame should kick off recording on the processor"

    await abp.emit_audio(b"\x01\x02" * 512)
    await abp.emit_audio(b"\x03\x04" * 512)

    # Wait for the upload tasks the observer scheduled in the background.
    if obs._inflight_uploads:  # type: ignore[attr-defined]
        import asyncio

        await asyncio.gather(*list(obs._inflight_uploads))  # type: ignore[attr-defined]

    assert len(fake.chunk_url_requests) == 2
    # chunkIndex is monotonic and starts at 0; the body is the PCM that was emitted.
    assert [r["chunk_index"] for r in fake.chunk_url_requests] == [0, 1]
    assert len(fake.chunk_uploads) == 2

    await obs.on_push_frame(_push(EndFrame()))
    assert abp.stop_calls == 1, "EndFrame should drain the processor's tail buffer"

    ended = fake.ended[0]
    assert ended["recordingSampleRate"] == 24000
    assert ended["recordingNumChannels"] == 2


@pytest.mark.asyncio
async def test_audio_emit_after_end_is_dropped() -> None:
    abp = _FakeAudioBufferProcessor()
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1", audio_buffer_processor=abp)
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    await obs.on_push_frame(_push(StartFrame()))
    await obs.on_push_frame(_push(EndFrame()))

    # Anything the processor emits post-flush should be discarded.
    await abp.emit_audio(b"\x05\x06" * 512)
    assert len(fake.chunk_url_requests) == 0


@pytest.mark.asyncio
async def test_call_ended_without_audio_omits_recording_fields() -> None:
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1")
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    await obs.on_push_frame(_push(StartFrame()))
    await obs.on_push_frame(_push(EndFrame()))

    ended = fake.ended[0]
    assert "recordingSampleRate" not in ended
    assert "recordingNumChannels" not in ended
