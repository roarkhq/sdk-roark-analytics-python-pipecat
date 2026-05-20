"""Tests for the RoarkObserver.

Skip if pipecat-ai isn't installed. We swap the internal ``RoarkClient`` for a
fake that records calls so tests exercise no network. Audio upload paths are
exercised by faking AudioBufferProcessor.on_audio_data events rather than by
pushing audio frames through the observer — the real processor lives in the
pipeline and the observer only subscribes to its event.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

pytest.importorskip("pipecat", reason="pipecat-ai not installed in this env")

from pipecat.frames.frames import (  # noqa: E402
    BotStoppedSpeakingFrame,
    EndFrame,
    FunctionCallInProgressFrame,
    FunctionCallResultFrame,
    InterruptionFrame,
    TranscriptionFrame,
    TTSTextFrame,
)
from pipecat.observers.base_observer import FramePushed  # noqa: E402
from pipecat.processors.frame_processor import FrameDirection  # noqa: E402

from pipecat_roark.observer import RoarkObserver  # noqa: E402


def _user_frame(text: str, *, user_id: str = "user", timestamp: str = "t") -> TranscriptionFrame:
    frame = TranscriptionFrame(text=text, user_id=user_id, timestamp=timestamp)
    frame.finalized = True  # type: ignore[attr-defined]
    return frame


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
async def test_on_pipeline_started_posts_call_started_with_required_fields() -> None:
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1", agent_name="Agent 1")
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    await obs.on_pipeline_started()

    assert len(fake.started) == 1
    payload = fake.started[0]
    assert payload["event"] == "call-started"
    assert payload["agentId"] == "agent-1"
    assert payload["agentName"] == "Agent 1"
    assert "interfaceType" not in payload
    assert "callDirection" not in payload
    assert "pipecatCallId" in payload
    assert "eventTimestamp" in payload


@pytest.mark.asyncio
async def test_phone_numbers_forwarded_on_call_started() -> None:
    obs = RoarkObserver(
        api_key="rk_test",
        agent_id="agent-1",
        agent_phone_number="+15550000",
        customer_phone_number="+15551111",
    )
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    await obs.on_pipeline_started()
    assert fake.started[0]["agentPhoneNumber"] == "+15550000"
    assert fake.started[0]["customerPhoneNumber"] == "+15551111"
    assert "interfaceType" not in fake.started[0]
    assert "callDirection" not in fake.started[0]


@pytest.mark.asyncio
async def test_sampling_rate_forwarded_when_set_and_omitted_when_unset() -> None:
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1")
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]
    await obs.on_pipeline_started()
    assert "samplingRate" not in fake.started[0]

    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1", sampling_rate=0.25)
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]
    await obs.on_pipeline_started()
    assert fake.started[0]["samplingRate"] == 0.25


@pytest.mark.asyncio
async def test_multiple_on_pipeline_started_calls_only_post_once() -> None:
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1")
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    for _ in range(3):
        await obs.on_pipeline_started()
    assert len(fake.started) == 1


@pytest.mark.asyncio
async def test_double_flush_only_posts_once() -> None:
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1")
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    await obs.on_pipeline_started()
    await obs.on_push_frame(_push(EndFrame()))
    await obs.on_push_frame(_push(EndFrame()))

    assert len(fake.ended) == 1


@pytest.mark.asyncio
async def test_user_and_assistant_turns_captured_from_raw_frames() -> None:
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1")
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    await obs.on_pipeline_started()
    await obs.on_push_frame(
        _push(_user_frame("hello", user_id="user-42", timestamp="2026-05-18T12:00:00+00:00"))
    )
    # Assistant turn: TTS emits one or more TTSTextFrames; BotStoppedSpeakingFrame closes it.
    await obs.on_push_frame(_push(TTSTextFrame(text="hi", aggregated_by="sentence")))
    await obs.on_push_frame(_push(TTSTextFrame(text="there", aggregated_by="sentence")))
    await obs.on_push_frame(_push(BotStoppedSpeakingFrame()))
    await obs.on_push_frame(_push(EndFrame()))

    ended = fake.ended[0]
    transcript = ended["transcript"]
    assert [m["role"] for m in transcript] == ["user", "assistant"]
    assert transcript[0]["content"] == "hello"
    assert transcript[0]["userId"] == "user-42"
    assert transcript[0]["timestamp"] == "2026-05-18T12:00:00+00:00"
    # Assistant text-chunks should be joined with a separating space.
    assert transcript[1]["content"] == "hi there"
    # User spoke first, so agentSpokeFirst is False.
    assert ended["agentSpokeFirst"] is False


@pytest.mark.asyncio
async def test_assistant_first_sets_agent_spoke_first() -> None:
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1")
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    await obs.on_pipeline_started()
    await obs.on_push_frame(
        _push(TTSTextFrame(text="hi, how can I help?", aggregated_by="sentence"))
    )
    await obs.on_push_frame(_push(BotStoppedSpeakingFrame()))
    await obs.on_push_frame(_push(EndFrame()))

    assert fake.ended[0]["agentSpokeFirst"] is True


@pytest.mark.asyncio
async def test_interim_user_transcriptions_are_dropped() -> None:
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1")
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    await obs.on_pipeline_started()
    interim = TranscriptionFrame(text="hel", user_id="user", timestamp="t1")
    # `finalized` defaults to False on the dataclass; the observer must skip those.
    await obs.on_push_frame(_push(interim))
    await obs.on_push_frame(_push(_user_frame("hello", timestamp="t2")))
    await obs.on_push_frame(_push(EndFrame()))

    transcript = fake.ended[0]["transcript"]
    assert len(transcript) == 1
    assert transcript[0]["content"] == "hello"


@pytest.mark.asyncio
async def test_assistant_turn_flushed_on_end_frame_without_bot_stopped() -> None:
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1")
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    await obs.on_pipeline_started()
    await obs.on_push_frame(_push(TTSTextFrame(text="goodbye", aggregated_by="sentence")))
    # Pipeline ends mid-utterance; the observer must still capture what was said.
    await obs.on_push_frame(_push(EndFrame()))

    transcript = fake.ended[0]["transcript"]
    assert len(transcript) == 1
    assert transcript[0]["role"] == "assistant"
    assert transcript[0]["content"] == "goodbye"


@pytest.mark.asyncio
async def test_interruption_flushes_partial_assistant_turn() -> None:
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1")
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    await obs.on_pipeline_started()
    await obs.on_push_frame(_push(TTSTextFrame(text="let me explain", aggregated_by="sentence")))
    await obs.on_push_frame(_push(InterruptionFrame()))
    await obs.on_push_frame(_push(_user_frame("actually never mind")))
    await obs.on_push_frame(_push(EndFrame()))

    transcript = fake.ended[0]["transcript"]
    assert [m["role"] for m in transcript] == ["assistant", "user"]
    assert transcript[0]["content"] == "let me explain"


@pytest.mark.asyncio
async def test_tool_calls_emit_kind_discriminated_records() -> None:
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1")
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    await obs.on_pipeline_started()
    await obs.on_push_frame(
        _push(
            FunctionCallInProgressFrame(
                function_name="get_weather",
                tool_call_id="call_abc123",
                arguments={"city": "Tokyo"},
            )
        )
    )
    await obs.on_push_frame(
        _push(
            FunctionCallResultFrame(
                function_name="get_weather",
                tool_call_id="call_abc123",
                arguments={"city": "Tokyo"},
                result={"temp_c": 21},
            )
        )
    )
    await obs.on_push_frame(_push(EndFrame()))

    tool_calls = fake.ended[0]["toolCalls"]
    assert len(tool_calls) == 2

    invocation, result = tool_calls
    assert invocation["kind"] == "tool_call"
    assert invocation["toolCallId"] == "call_abc123"
    assert invocation["name"] == "get_weather"
    # arguments must be a JSON-encoded STRING, not a dict.
    assert isinstance(invocation["arguments"], str)
    assert json.loads(invocation["arguments"]) == {"city": "Tokyo"}
    assert isinstance(invocation["timestamp"], str)

    assert result["kind"] == "tool_result"
    assert result["toolCallId"] == "call_abc123"
    # content must be a string; dict results are JSON-encoded.
    assert isinstance(result["content"], str)
    assert json.loads(result["content"]) == {"temp_c": 21}
    assert isinstance(result["timestamp"], str)


@pytest.mark.asyncio
async def test_tool_call_string_arguments_pass_through_verbatim() -> None:
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1")
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    await obs.on_pipeline_started()
    await obs.on_push_frame(
        _push(
            FunctionCallInProgressFrame(
                function_name="end_call",
                tool_call_id="call_xyz",
                arguments='{"reason":"done"}',
            )
        )
    )
    await obs.on_push_frame(_push(EndFrame()))

    tool_calls = fake.ended[0]["toolCalls"]
    # Fire-and-forget tool with no result emitted: only the invocation lands.
    assert len(tool_calls) == 1
    assert tool_calls[0]["arguments"] == '{"reason":"done"}'


@pytest.mark.asyncio
async def test_audio_buffer_processor_drives_chunk_uploads() -> None:
    abp = _FakeAudioBufferProcessor(sample_rate=24000, num_channels=2)
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1", audio_buffer_processor=abp)
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    await obs.on_pipeline_started()
    assert abp.start_calls == 1, "on_pipeline_started should kick off recording on the processor"

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
async def test_default_audio_processor_is_created_when_none_passed() -> None:
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1")
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    # Default processor is created and exposed for the user to splice into their pipeline.
    from pipecat.processors.audio.audio_buffer_processor import AudioBufferProcessor

    abp = obs.audio_processor
    assert isinstance(abp, AudioBufferProcessor)
    # ``sample_rate`` is intentionally left unset so AudioBufferProcessor adopts the
    # pipeline's negotiated rate from the StartFrame (varies by provider — 8 kHz on
    # Twilio/Telnyx, 16/24/48 kHz on Daily/LiveKit). ``num_channels`` is set directly.
    assert abp._init_sample_rate is None  # noqa: SLF001 — constructor input
    assert abp.num_channels == 2

    # on_pipeline_started must invoke start_recording on the auto-created processor.
    calls: list[int] = []

    async def _track() -> None:
        calls.append(1)

    abp.start_recording = _track  # type: ignore[method-assign]
    await obs.on_pipeline_started()
    assert calls == [1]


@pytest.mark.asyncio
async def test_audio_emit_after_end_is_dropped() -> None:
    abp = _FakeAudioBufferProcessor()
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1", audio_buffer_processor=abp)
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    await obs.on_pipeline_started()
    await obs.on_push_frame(_push(EndFrame()))

    # Anything the processor emits post-flush should be discarded.
    await abp.emit_audio(b"\x05\x06" * 512)
    assert len(fake.chunk_url_requests) == 0


@pytest.mark.asyncio
async def test_same_frame_observed_multiple_times_is_deduped() -> None:
    """Regression: Pipecat calls ``on_push_frame`` once per processor hop,
    so the same frame instance fires N times. The observer must act once.
    """
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1")
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    await obs.on_pipeline_started()

    # Same TTSTextFrame instance pushed three times — as it would be across
    # three processor hops in a real pipeline.
    tts = TTSTextFrame(text="Hello!", aggregated_by="sentence")
    for _ in range(3):
        await obs.on_push_frame(_push(tts))

    # Same TranscriptionFrame instance observed multiple times too.
    user = _user_frame("hi there", timestamp="t1")
    for _ in range(3):
        await obs.on_push_frame(_push(user))

    # Same tool-call frames repeated across hops.
    invocation = FunctionCallInProgressFrame(
        function_name="ping", tool_call_id="call_1", arguments={}
    )
    result = FunctionCallResultFrame(
        function_name="ping", tool_call_id="call_1", arguments={}, result="pong"
    )
    for _ in range(3):
        await obs.on_push_frame(_push(invocation))
        await obs.on_push_frame(_push(result))

    await obs.on_push_frame(_push(BotStoppedSpeakingFrame()))
    await obs.on_push_frame(_push(EndFrame()))

    transcript = fake.ended[0]["transcript"]
    # One assistant turn ("Hello!" — NOT "Hello!Hello!Hello!") and one user turn.
    assistant = [m for m in transcript if m["role"] == "assistant"]
    users = [m for m in transcript if m["role"] == "user"]
    assert len(assistant) == 1
    assert assistant[0]["content"] == "Hello!"
    assert len(users) == 1
    assert users[0]["content"] == "hi there"

    # Tool call/result deduped to one each.
    tool_calls = fake.ended[0]["toolCalls"]
    assert len(tool_calls) == 2
    assert tool_calls[0]["kind"] == "tool_call"
    assert tool_calls[1]["kind"] == "tool_result"


@pytest.mark.asyncio
async def test_call_ended_without_audio_omits_recording_fields() -> None:
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1")
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    await obs.on_pipeline_started()
    await obs.on_push_frame(_push(EndFrame()))

    ended = fake.ended[0]
    assert "recordingSampleRate" not in ended
    assert "recordingNumChannels" not in ended
