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
    BotStartedSpeakingFrame,
    BotStoppedSpeakingFrame,
    EndFrame,
    FunctionCallInProgressFrame,
    FunctionCallResultFrame,
    InputAudioRawFrame,
    InterimTranscriptionFrame,
    InterruptionFrame,
    OutputAudioRawFrame,
    StartFrame,
    TranscriptionFrame,
    TTSTextFrame,
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame,
)
from pipecat.observers.base_observer import FramePushed  # noqa: E402
from pipecat.processors.frame_processor import FrameDirection  # noqa: E402

from pipecat_roark.observer import RoarkObserver  # noqa: E402


@pytest.fixture(autouse=True)
def _default_integration_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """``roark_integration_id`` is required; supply one via the env var for every
    test so constructions that don't care about it still succeed. Tests that
    exercise the id explicitly override or delete the env var themselves.
    """
    monkeypatch.setenv("ROARK_INTEGRATION_ID", "int_default")


def _user_frame(text: str, *, user_id: str = "user", timestamp: str = "t") -> TranscriptionFrame:
    # Deliberately leaves `finalized` at its dataclass default (False), as
    # streaming STTs (Deepgram, OpenAI, Speechmatics, the realtime models) do
    # for ordinary final transcripts. The observer must capture these — every
    # TranscriptionFrame is final; interims are InterimTranscriptionFrame.
    return TranscriptionFrame(text=text, user_id=user_id, timestamp=timestamp)


def _audio_in() -> InputAudioRawFrame:
    return InputAudioRawFrame(audio=b"\x00\x00" * 8, sample_rate=24000, num_channels=1)


def _audio_out() -> OutputAudioRawFrame:
    return OutputAudioRawFrame(audio=b"\x00\x00" * 8, sample_rate=24000, num_channels=1)


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
        # Mirrors AudioBufferProcessor._recording; the observer reads it to avoid
        # re-arming (and thereby resetting) a caller that already started recording.
        self._recording = False

    def add_event_handler(self, name: str, handler: Any) -> None:
        assert name == "on_audio_data"
        self._handlers.append(handler)

    async def start_recording(self) -> None:
        self.start_calls += 1
        self._recording = True

    async def stop_recording(self) -> None:
        self.stop_calls += 1
        self._recording = False

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
async def test_roark_integration_id_from_constructor_on_both_payloads() -> None:
    obs = RoarkObserver(
        api_key="rk_test", agent_id="agent-1", roark_integration_id="int_abc123"
    )
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    await obs.on_pipeline_started()
    await obs.on_push_frame(_push(EndFrame()))

    assert fake.started[0]["roarkIntegrationId"] == "int_abc123"
    assert fake.ended[0]["roarkIntegrationId"] == "int_abc123"


@pytest.mark.asyncio
async def test_roark_integration_id_from_env_on_both_payloads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ROARK_INTEGRATION_ID", "int_from_env")
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1")
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    await obs.on_pipeline_started()
    await obs.on_push_frame(_push(EndFrame()))

    assert fake.started[0]["roarkIntegrationId"] == "int_from_env"
    assert fake.ended[0]["roarkIntegrationId"] == "int_from_env"


@pytest.mark.asyncio
async def test_constructor_roark_integration_id_overrides_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ROARK_INTEGRATION_ID", "int_from_env")
    obs = RoarkObserver(
        api_key="rk_test", agent_id="agent-1", roark_integration_id="int_explicit"
    )
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    await obs.on_pipeline_started()
    await obs.on_push_frame(_push(EndFrame()))

    assert fake.started[0]["roarkIntegrationId"] == "int_explicit"
    assert fake.ended[0]["roarkIntegrationId"] == "int_explicit"


def test_missing_roark_integration_id_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """roark_integration_id is required — construction fails when neither the
    constructor arg nor the ROARK_INTEGRATION_ID env var supplies one.
    """
    monkeypatch.delenv("ROARK_INTEGRATION_ID", raising=False)
    with pytest.raises(ValueError, match="roark_integration_id is required"):
        RoarkObserver(api_key="rk_test", agent_id="agent-1")


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
    # Assistant turn: the bot starting to speak closes the user turn, then TTS
    # emits one or more TTSTextFrames; BotStoppedSpeakingFrame closes the assistant turn.
    await obs.on_push_frame(_push(BotStartedSpeakingFrame()))
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
async def test_user_turn_anchored_to_speech_onset_with_audio_offset() -> None:
    """A preceding UserStartedSpeakingFrame should drive the turn's timestamp
    (speech onset), overriding the STT-finalize frame timestamp, and the turn
    should carry an audio-relative offset measured from start of recording.
    """
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1")
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    await obs.on_pipeline_started()
    # First audio frame anchors the offset clock (WAV sample 0).
    await obs.on_push_frame(_push(_audio_in()))
    await obs.on_push_frame(_push(UserStartedSpeakingFrame()))
    await obs.on_push_frame(
        _push(_user_frame("hello", timestamp="2026-05-18T12:00:00+00:00"))
    )
    await obs.on_push_frame(_push(EndFrame()))

    user_turn = fake.ended[0]["transcript"][0]
    # VAD onset wins over the STT frame's own timestamp.
    assert user_turn["timestamp"] != "2026-05-18T12:00:00+00:00"
    assert "audioOffsetMs" in user_turn
    assert isinstance(user_turn["audioOffsetMs"], int)
    assert user_turn["audioOffsetMs"] >= 0


@pytest.mark.asyncio
async def test_assistant_turn_anchored_to_bot_started_speaking() -> None:
    """BotStartedSpeakingFrame (audio onset) should anchor the assistant turn
    and produce an audioOffsetMs, even though TTSTextFrames arrive separately.
    """
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1")
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    await obs.on_pipeline_started()
    # First audio frame anchors the offset clock (WAV sample 0).
    await obs.on_push_frame(_push(_audio_out()))
    await obs.on_push_frame(_push(BotStartedSpeakingFrame()))
    await obs.on_push_frame(_push(TTSTextFrame(text="hi there", aggregated_by="sentence")))
    await obs.on_push_frame(_push(BotStoppedSpeakingFrame()))
    await obs.on_push_frame(_push(EndFrame()))

    assistant_turn = fake.ended[0]["transcript"][0]
    assert assistant_turn["role"] == "assistant"
    assert assistant_turn["content"] == "hi there"
    assert "audioOffsetMs" in assistant_turn
    assert assistant_turn["audioOffsetMs"] >= 0


@pytest.mark.asyncio
async def test_user_turn_carries_end_edge_from_stopped_speaking() -> None:
    """A user turn must carry endTimestamp/endAudioOffsetMs anchored to
    UserStoppedSpeakingFrame (speech offset), so the turn's span is real and
    downstream needn't infer the end from the next turn's start.
    """
    import asyncio

    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1")
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    await obs.on_pipeline_started()
    await obs.on_push_frame(_push(_audio_in()))  # anchor the offset clock
    await obs.on_push_frame(_push(UserStartedSpeakingFrame()))
    await asyncio.sleep(0.02)  # let the turn occupy a real span
    await obs.on_push_frame(_push(_user_frame("hello")))
    await obs.on_push_frame(_push(UserStoppedSpeakingFrame()))
    # Bot replying flushes the user turn — its end must come from the stop edge
    # captured above, NOT this later bot-onset moment.
    await asyncio.sleep(0.02)
    await obs.on_push_frame(_push(BotStartedSpeakingFrame()))
    await obs.on_push_frame(_push(EndFrame()))

    user_turn = fake.ended[0]["transcript"][0]
    assert isinstance(user_turn["endTimestamp"], str) and user_turn["endTimestamp"]
    assert "endAudioOffsetMs" in user_turn
    assert isinstance(user_turn["endAudioOffsetMs"], int)
    # End is after start, and before the (later) bot onset that triggered flush.
    assert user_turn["endAudioOffsetMs"] >= user_turn["audioOffsetMs"]


@pytest.mark.asyncio
async def test_assistant_turn_carries_end_edge_from_bot_stopped() -> None:
    """An assistant turn must carry endTimestamp/endAudioOffsetMs anchored to
    BotStoppedSpeakingFrame (speech offset).
    """
    import asyncio

    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1")
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    await obs.on_pipeline_started()
    await obs.on_push_frame(_push(_audio_out()))  # anchor the offset clock
    await obs.on_push_frame(_push(BotStartedSpeakingFrame()))
    await obs.on_push_frame(_push(TTSTextFrame(text="hi there", aggregated_by="sentence")))
    await asyncio.sleep(0.02)
    await obs.on_push_frame(_push(BotStoppedSpeakingFrame()))
    await obs.on_push_frame(_push(EndFrame()))

    assistant_turn = fake.ended[0]["transcript"][0]
    assert assistant_turn["role"] == "assistant"
    assert isinstance(assistant_turn["endTimestamp"], str) and assistant_turn["endTimestamp"]
    assert "endAudioOffsetMs" in assistant_turn
    assert assistant_turn["endAudioOffsetMs"] >= assistant_turn["audioOffsetMs"]


@pytest.mark.asyncio
async def test_turn_end_falls_back_to_flush_moment_without_stop_frame() -> None:
    """When no *StoppedSpeakingFrame arrives before the flush (e.g. pipeline
    ends mid-turn), endTimestamp is still populated from the flush moment —
    the field is never omitted.
    """
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1")
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    await obs.on_pipeline_started()
    await obs.on_push_frame(_push(TTSTextFrame(text="goodbye", aggregated_by="sentence")))
    # EndFrame flushes the assistant turn with no BotStoppedSpeakingFrame seen.
    await obs.on_push_frame(_push(EndFrame()))

    assistant_turn = fake.ended[0]["transcript"][0]
    assert isinstance(assistant_turn["endTimestamp"], str) and assistant_turn["endTimestamp"]


@pytest.mark.asyncio
async def test_turn_end_offset_omitted_when_recording_not_started() -> None:
    """Without a recording anchor, endAudioOffsetMs is omitted (like
    audioOffsetMs) but endTimestamp is still present.
    """
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1")
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    # No on_pipeline_started / audio frame → no anchor.
    await obs.on_push_frame(_push(UserStartedSpeakingFrame()))
    await obs.on_push_frame(_push(_user_frame("hello", timestamp="t1")))
    await obs.on_push_frame(_push(UserStoppedSpeakingFrame()))
    await obs.on_push_frame(_push(EndFrame()))

    user_turn = fake.ended[0]["transcript"][0]
    assert "audioOffsetMs" not in user_turn
    assert "endAudioOffsetMs" not in user_turn
    assert isinstance(user_turn["endTimestamp"], str) and user_turn["endTimestamp"]


@pytest.mark.asyncio
async def test_audio_offset_omitted_when_recording_not_started() -> None:
    """Without a recording anchor (on_pipeline_started never ran), turns must
    omit audioOffsetMs rather than ship a bogus 0.
    """
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1")
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    # No on_pipeline_started → no recording anchor.
    await obs.on_push_frame(_push(_user_frame("hello", timestamp="t1")))
    await obs.on_push_frame(_push(EndFrame()))

    user_turn = fake.ended[0]["transcript"][0]
    assert "audioOffsetMs" not in user_turn
    # Falls back to the STT frame's own timestamp when no VAD onset is seen.
    assert user_turn["timestamp"] == "t1"


@pytest.mark.asyncio
async def test_offset_anchor_deferred_to_first_audio_frame() -> None:
    """Regression: WAV sample 0 is the first audio frame the processor records,
    not start_recording(). Arming the recording must NOT anchor the offset clock;
    only the first observed audio frame may. Anchoring at start_recording()
    inflated every offset by the dead time before audio, so the first seconds of
    the merged recording appeared to be missing.
    """
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1")
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    await obs.on_pipeline_started()
    # Recording is armed, but the offset clock is not anchored yet — no audio
    # frame has been recorded, so there is no WAV sample 0 to anchor to.
    assert obs._recording_active is True  # type: ignore[attr-defined]
    assert obs._recording_anchor_monotonic is None  # type: ignore[attr-defined]

    # The first audio frame establishes the anchor.
    await obs.on_push_frame(_push(_audio_out()))
    anchor = obs._recording_anchor_monotonic  # type: ignore[attr-defined]
    assert anchor is not None

    # Later audio frames must not move the anchor.
    await obs.on_push_frame(_push(_audio_in()))
    assert obs._recording_anchor_monotonic == anchor  # type: ignore[attr-defined]


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
async def test_interim_frames_ignored_final_segments_aggregated() -> None:
    """Interims (the separate InterimTranscriptionFrame class) are ignored, while
    multiple final TranscriptionFrame segments within one turn aggregate into a
    single user turn — independent of `finalized`, which streaming STTs leave
    False. This is the Deepgram / OpenAI / Speechmatics / realtime case.
    """
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1")
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    await obs.on_pipeline_started()
    await obs.on_push_frame(_push(UserStartedSpeakingFrame()))
    # Partial result → ignored by the type filter (not a TranscriptionFrame).
    interim = InterimTranscriptionFrame(text="I want", user_id="u", timestamp="t1")
    await obs.on_push_frame(_push(interim))
    # Two final segments for the same turn, both with finalized=False.
    await obs.on_push_frame(_push(_user_frame("I want to book", timestamp="t2")))
    await obs.on_push_frame(_push(_user_frame("a flight to Paris", timestamp="t3")))
    # Bot replying closes the user turn.
    await obs.on_push_frame(_push(BotStartedSpeakingFrame()))
    await obs.on_push_frame(_push(EndFrame()))

    transcript = fake.ended[0]["transcript"]
    users = [m for m in transcript if m["role"] == "user"]
    assert len(users) == 1
    assert users[0]["content"] == "I want to book a flight to Paris"


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
async def test_prearmed_byo_processor_is_not_rearmed() -> None:
    """Regression: when the caller passes a bring-your-own processor they have
    already armed (app-agent-service starts recording before constructing the
    observer), on_pipeline_started must NOT call start_recording again — doing
    so resets the buffer, wiping audio captured before this lagging callback and
    de-syncing the offset anchor from the recording's sample 0.
    """
    abp = _FakeAudioBufferProcessor(sample_rate=8000, num_channels=2)
    abp._recording = True  # caller already armed it
    obs = RoarkObserver(api_key="rk_test", agent_id="agent-1", audio_buffer_processor=abp)
    fake = _FakeClient()
    obs._client = fake  # type: ignore[assignment]

    await obs.on_pipeline_started()
    assert abp.start_calls == 0, "must not re-arm (reset) an already-recording processor"
    assert len(fake.started) == 1, "call-started should still be posted"


@pytest.mark.asyncio
async def test_default_audio_processor_is_created_when_none_passed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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

    # The auto-created processor arms recording INLINE on the StartFrame — not
    # from on_pipeline_started, whose lagging observer-queue delivery would let a
    # bot that speaks first lose its greeting. on_pipeline_started must therefore
    # NOT call start_recording on the default processor (doing so would reset its
    # buffers and wipe already-captured audio).
    calls: list[int] = []
    real_start = abp.start_recording

    async def _track() -> None:
        calls.append(1)
        await real_start()

    abp.start_recording = _track  # type: ignore[method-assign]
    await obs.on_pipeline_started()
    assert calls == [], "default processor must not be armed from on_pipeline_started"

    # Processing the StartFrame inline arms recording from sample 0. Stub the
    # heavy FrameProcessor.process_frame machinery (clock / task manager, set up
    # only inside a live pipeline) — we only need to exercise our StartFrame
    # override, which runs after super().process_frame().
    async def _noop_process_frame(self: Any, frame: Any, direction: Any) -> None:
        return None

    monkeypatch.setattr(AudioBufferProcessor, "process_frame", _noop_process_frame)

    await abp.process_frame(StartFrame(), FrameDirection.DOWNSTREAM)
    assert calls == [1]
    assert abp._recording is True  # noqa: SLF001 — internal recording flag


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


