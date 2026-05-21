"""RoarkObserver — Pipecat BaseObserver that ships call data to Roark.

The observer is drop-in: insert it into a pipeline's ``observers`` and it
captures everything it needs by watching raw frames flow through the pipeline.

* **Transcripts** are captured directly from STT and TTS frames — no extra
  pipeline wiring required. User turns come from ``TranscriptionFrame`` (final
  only); assistant turns are aggregated from ``TTSTextFrame`` chunks between
  utterance boundaries (``BotStoppedSpeakingFrame`` / ``InterruptionFrame`` /
  ``EndFrame`` / ``CancelFrame``).
* **Tool calls** come from ``FunctionCallInProgressFrame`` /
  ``FunctionCallResultFrame``. Each is shipped as a discrete ``tool_call`` /
  ``tool_result`` record discriminated by ``kind``; Roark pairs them by
  ``toolCallId``.
* **Audio** is delegated to Pipecat's ``AudioBufferProcessor``. The observer
  always creates one with sane defaults (stereo, ~256 KB chunks; sample rate
  is adopted from the pipeline's ``StartFrame`` so it tracks whatever the
  transport/provider negotiated — 8 kHz for Twilio/Telnyx, 16/24/48 kHz for
  Daily/LiveKit, etc.), exposed as ``observer.audio_processor`` for the user
  to splice into their pipeline. Power users may instead pass their own
  pre-configured instance via ``audio_buffer_processor=``.

Lifecycle:

1. ``on_pipeline_started`` (fired by Pipecat once after ``StartFrame`` has
   propagated through every processor) → POST ``call-started``;
   ``AudioBufferProcessor.start_recording()`` is invoked if recording is enabled.
2. During the call:
     * ``TranscriptionFrame`` (final) → user turn buffered.
     * ``TTSTextFrame`` → aggregated into a pending assistant turn.
     * ``BotStoppedSpeakingFrame`` / ``InterruptionFrame`` → pending
       assistant turn flushed.
     * ``FunctionCallInProgressFrame`` / ``FunctionCallResultFrame`` →
       tool-call messages buffered.
     * ``AudioBufferProcessor.on_audio_data`` → chunk PUT to S3.
3. ``EndFrame`` / ``CancelFrame`` / ``StopFrame`` → flush any pending
   assistant turn, drain in-flight uploads, POST ``call-ended``.

Failures are logged and swallowed — the observer never raises into the pipeline.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Literal, cast

from pipecat.observers.base_observer import BaseObserver, FramePushed

from ._types import (
    CallEndedPayload,
    CallStartedPayload,
    ToolCallMessage,
    ToolResultMessage,
    TranscriptMessage,
)
from .client import RoarkClient

if TYPE_CHECKING:  # pragma: no cover
    from pipecat.frames.frames import Frame
    from pipecat.processors.audio.audio_buffer_processor import AudioBufferProcessor

log = logging.getLogger("pipecat_roark.observer")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _arguments_to_json_string(value: object) -> str:
    """Stringify tool-call arguments without parsing them.

    Pipecat hands us either a string (already JSON-encoded by the LLM) or a
    dict (pre-parsed by the LLM service). The wire contract is a JSON string,
    so dicts are re-encoded with compact separators to mirror how Retell and
    OpenAI send them.
    """
    if value is None:
        return "{}"
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, separators=(",", ":"))
    except (TypeError, ValueError):
        return "{}"


def _result_to_string(value: object) -> str:
    """Stringify a tool result for the wire.

    Objects / lists → compact JSON. Scalars → ``str()``. ``None`` → ``""``.
    """
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (dict, list)):
        try:
            return json.dumps(value, separators=(",", ":"))
        except (TypeError, ValueError):
            return str(value)
    return str(value)


class RoarkObserver(BaseObserver):
    """Capture Pipecat pipeline activity and ship it to Roark."""

    def __init__(
        self,
        *,
        api_key: str,
        agent_id: str,
        agent_name: str | None = None,
        agent_prompt: str | None = None,
        roark_webhook_url: str | None = None,
        roark_chunk_upload_url_endpoint: str | None = None,
        audio_buffer_processor: AudioBufferProcessor | None = None,
        pipecat_call_id: str | None = None,
    ) -> None:
        super().__init__()

        client_kwargs: dict[str, str] = {"api_key": api_key}
        if roark_webhook_url is not None:
            client_kwargs["webhook_url"] = roark_webhook_url
        if roark_chunk_upload_url_endpoint is not None:
            client_kwargs["chunk_upload_url_endpoint"] = roark_chunk_upload_url_endpoint
        self._client = RoarkClient(**client_kwargs)

        self._agent_id = agent_id
        self._agent_name = agent_name
        self._agent_prompt = agent_prompt
        self._pipecat_call_id = pipecat_call_id or str(uuid.uuid4())

        self._transcript: list[TranscriptMessage] = []
        self._tool_calls: list[ToolCallMessage | ToolResultMessage] = []
        self._call_started_iso: str | None = None
        self._first_speaker: Literal["assistant", "user"] | None = None
        self._started_posted = False
        self._end_flushed = False

        # Pending assistant turn — text chunks streamed by TTS, flushed when the
        # bot stops speaking, gets interrupted, or the pipeline ends.
        self._assistant_text_parts: list[str] = []
        self._assistant_start_iso: str | None = None

        self._chunk_index = 0
        self._inflight_uploads: set[asyncio.Task[None]] = set()

        if audio_buffer_processor is None:
            from pipecat.processors.audio.audio_buffer_processor import (
                AudioBufferProcessor as _AudioBufferProcessor,
            )

            # Stereo (L=user, R=bot), ~256 KB chunks. Sample rate is left
            # unspecified so AudioBufferProcessor adopts the pipeline's
            # negotiated ``audio_out_sample_rate`` from the StartFrame — this
            # varies by provider (Twilio/Telnyx are 8 kHz, Daily/LiveKit are
            # typically 16/24/48 kHz). Hardcoding a rate would force resampling
            # at best and silent corruption at worst.
            audio_buffer_processor = _AudioBufferProcessor(
                num_channels=2,
                buffer_size=256 * 1024,
            )
        self.audio_processor: AudioBufferProcessor = audio_buffer_processor
        audio_buffer_processor.add_event_handler("on_audio_data", self._on_audio_data)

        # Pipecat invokes ``on_push_frame`` for every processor-to-processor
        # hop, so the same frame instance fires this callback N times. Dedupe
        # by frame id so each transcription/TTS/tool-call frame is acted on
        # exactly once — otherwise turns repeat ("Hello!Hello!Hello!").
        self._seen_frame_ids: set[int] = set()

    # ------------------------------------------------------------------ lifecycle

    async def on_pipeline_started(self) -> None:  # type: ignore[override]
        """Pipecat fires this once after ``StartFrame`` has propagated through
        every processor — the canonical "call begins" hook.
        """
        if self._started_posted:
            return
        await self._post_call_started()
        try:
            await self.audio_processor.start_recording()
        except Exception as err:  # pragma: no cover — defensive
            log.warning("AudioBufferProcessor.start_recording failed: %r", err)

    # ------------------------------------------------------------------ frames

    async def on_push_frame(self, data: FramePushed) -> None:  # type: ignore[override]
        from pipecat.frames.frames import (
            BotStoppedSpeakingFrame,
            CancelFrame,
            EndFrame,
            FunctionCallInProgressFrame,
            FunctionCallResultFrame,
            InterruptionFrame,
            StopFrame,
            TranscriptionFrame,
            TTSTextFrame,
        )

        frame: Frame = data.frame

        handled_types = (
            TranscriptionFrame,
            TTSTextFrame,
            BotStoppedSpeakingFrame,
            InterruptionFrame,
            FunctionCallInProgressFrame,
            FunctionCallResultFrame,
            EndFrame,
            CancelFrame,
            StopFrame,
        )
        if not isinstance(frame, handled_types):
            return
        fid = getattr(frame, "id", None)
        if isinstance(fid, int):
            if fid in self._seen_frame_ids:
                return
            self._seen_frame_ids.add(fid)

        if isinstance(frame, TranscriptionFrame):
            # STT may emit interim frames too; only commit on finalize.
            if getattr(frame, "finalized", True):
                self._record_user_transcription(frame)
            return

        if isinstance(frame, TTSTextFrame):
            self._accumulate_assistant_text(frame)
            return

        if isinstance(frame, (BotStoppedSpeakingFrame, InterruptionFrame)):
            self._flush_assistant_turn()
            # Fall through — these frames are not call terminators.

        if isinstance(frame, FunctionCallInProgressFrame):
            self._record_tool_invocation(frame)
            return

        if isinstance(frame, FunctionCallResultFrame):
            self._record_tool_result(frame)
            return

        if isinstance(frame, (EndFrame, CancelFrame, StopFrame)):
            # Capture any in-flight assistant text before the call-ended POST.
            self._flush_assistant_turn()
            await self._flush_call_ended(reason=self._reason_from_frame(frame))
            return

    async def aflush(self, *, reason: str = "client-disconnected") -> None:
        """Idempotently flush the call.

        Pipecat transports (notably SmallWebRTC) sometimes tear down without pushing
        EndFrame/CancelFrame through the observer; call this from your
        ``on_client_disconnected`` handler to guarantee ``call-ended`` is POSTed.
        Safe to call multiple times.
        """
        self._flush_assistant_turn()
        await self._flush_call_ended(reason=reason)

    # ------------------------------------------------------------------ call-started

    async def _post_call_started(self) -> None:
        self._started_posted = True
        self._call_started_iso = _utc_now_iso()

        payload: CallStartedPayload = {
            "event": "call-started",
            "pipecatCallId": self._pipecat_call_id,
            "eventTimestamp": self._call_started_iso,
            "agentId": self._agent_id,
        }
        if self._agent_name:
            payload["agentName"] = self._agent_name
        if self._agent_prompt:
            payload["agentPrompt"] = self._agent_prompt

        log.info("call-started: pipecatCallId=%s agentId=%s", self._pipecat_call_id, self._agent_id)
        await self._client.post_call_started(payload)

    # ------------------------------------------------------------------ audio

    async def _on_audio_data(
        self,
        _processor: AudioBufferProcessor,
        audio: bytes,
        _sample_rate: int,
        _num_channels: int,
    ) -> None:
        """AudioBufferProcessor.on_audio_data subscriber — upload one chunk."""
        if self._end_flushed or not audio:
            return
        idx = self._chunk_index
        self._chunk_index = idx + 1
        task = asyncio.create_task(self._do_upload_chunk(idx, audio))
        self._inflight_uploads.add(task)
        task.add_done_callback(self._inflight_uploads.discard)

    async def _do_upload_chunk(self, idx: int, pcm: bytes) -> None:
        upload = await self._client.request_chunk_upload_url(
            pipecat_call_id=self._pipecat_call_id, chunk_index=idx
        )
        if not upload:
            return
        await self._client.upload_chunk(upload_url=upload["uploadUrl"], body=pcm)

    # ------------------------------------------------------------------ call-ended

    async def _flush_call_ended(self, *, reason: str) -> None:
        if self._end_flushed:
            return
        self._end_flushed = True

        # Drain the processor's tail buffer before awaiting in-flight uploads so the
        # final chunk task is registered in the set.
        abp = self.audio_processor
        try:
            await abp.stop_recording()
        except Exception as err:  # pragma: no cover — defensive
            log.warning("AudioBufferProcessor.stop_recording failed: %r", err)

        if self._inflight_uploads:
            await asyncio.gather(*list(self._inflight_uploads), return_exceptions=True)

        ended_iso = _utc_now_iso()
        payload: CallEndedPayload = {
            "event": "call-ended",
            "pipecatCallId": self._pipecat_call_id,
            "eventTimestamp": ended_iso,
            "callStartedAt": self._call_started_iso,
            "callEndedAt": ended_iso,
            "callEndedReason": reason,
        }
        if self._first_speaker is not None:
            payload["agentSpokeFirst"] = self._first_speaker == "assistant"
        if self._chunk_index > 0:
            payload["recordingSampleRate"] = abp.sample_rate
            payload["recordingNumChannels"] = abp.num_channels
        if self._transcript:
            payload["transcript"] = list(self._transcript)
        else:
            log.warning(
                "call-ended with empty transcript (pipecatCallId=%s) — no "
                "TranscriptionFrame or TTSTextFrame was observed during the call.",
                self._pipecat_call_id,
            )
        if self._tool_calls:
            payload["toolCalls"] = list(self._tool_calls)

        log.info(
            "call-ended: pipecatCallId=%s reason=%s transcript=%d toolCalls=%d chunks=%d",
            self._pipecat_call_id,
            reason,
            len(self._transcript),
            len(self._tool_calls),
            self._chunk_index,
        )
        await self._client.post_call_ended(payload)
        await self._client.aclose()

        # Per-call buffers are gone now that the POST has been acknowledged.
        self._transcript.clear()
        self._tool_calls.clear()
        self._seen_frame_ids.clear()

    # ------------------------------------------------------------------ transcript

    def _record_user_transcription(self, frame: object) -> None:
        try:
            text = (getattr(frame, "text", "") or "").strip()
            if not text:
                return
            timestamp = getattr(frame, "timestamp", None)
            if not isinstance(timestamp, str) or not timestamp:
                timestamp = _utc_now_iso()

            entry: TranscriptMessage = {
                "role": "user",
                "content": text,
                "timestamp": timestamp,
            }
            user_id = getattr(frame, "user_id", None)
            if isinstance(user_id, str) and user_id:
                entry["userId"] = user_id
            language = getattr(frame, "language", None)
            if language is not None:
                # Pipecat's Language is a StrEnum; stringify either way.
                entry["language"] = str(language)

            if self._first_speaker is None:
                self._first_speaker = "user"
            self._transcript.append(entry)
            log.info(
                "transcript user turn captured: chars=%d total=%d",
                len(text),
                len(self._transcript),
            )
        except Exception as err:  # pragma: no cover — defensive
            log.warning("failed to capture user transcription: %r", err)

    def _accumulate_assistant_text(self, frame: object) -> None:
        try:
            text = getattr(frame, "text", "") or ""
            if not text:
                return
            if not self._assistant_text_parts:
                self._assistant_start_iso = _utc_now_iso()
            self._assistant_text_parts.append(text)
        except Exception as err:  # pragma: no cover — defensive
            log.warning("failed to accumulate assistant text: %r", err)

    def _flush_assistant_turn(self) -> None:
        if not self._assistant_text_parts:
            return
        try:
            content = _join_tts_chunks(self._assistant_text_parts).strip()
            timestamp = self._assistant_start_iso or _utc_now_iso()
            self._assistant_text_parts = []
            self._assistant_start_iso = None
            if not content:
                return
            entry: TranscriptMessage = {
                "role": "assistant",
                "content": content,
                "timestamp": timestamp,
            }
            if self._first_speaker is None:
                self._first_speaker = "assistant"
            self._transcript.append(entry)
            log.info(
                "transcript assistant turn captured: chars=%d total=%d",
                len(content),
                len(self._transcript),
            )
        except Exception as err:  # pragma: no cover — defensive
            log.warning("failed to flush assistant turn: %r", err)
            # Reset so a parser glitch doesn't poison the next turn.
            self._assistant_text_parts = []
            self._assistant_start_iso = None

    # ------------------------------------------------------------------ tool calls

    def _record_tool_invocation(self, frame: object) -> None:
        try:
            entry = cast(
                ToolCallMessage,
                {
                    "kind": "tool_call",
                    "toolCallId": str(getattr(frame, "tool_call_id", "") or ""),
                    "name": str(getattr(frame, "function_name", "") or ""),
                    "arguments": _arguments_to_json_string(
                        getattr(frame, "arguments", None)
                    ),
                    "timestamp": _utc_now_iso(),
                },
            )
            self._tool_calls.append(entry)
            log.info(
                "tool_call captured: name=%s toolCallId=%s",
                entry["name"],
                entry["toolCallId"],
            )
        except Exception as err:  # pragma: no cover — defensive
            log.warning("failed to capture tool invocation: %r", err)

    def _record_tool_result(self, frame: object) -> None:
        try:
            entry = cast(
                ToolResultMessage,
                {
                    "kind": "tool_result",
                    "toolCallId": str(getattr(frame, "tool_call_id", "") or ""),
                    "content": _result_to_string(getattr(frame, "result", None)),
                    "timestamp": _utc_now_iso(),
                },
            )
            self._tool_calls.append(entry)
            log.info(
                "tool_result captured: toolCallId=%s content_chars=%d",
                entry["toolCallId"],
                len(entry["content"]),
            )
        except Exception as err:  # pragma: no cover — defensive
            log.warning("failed to capture tool result: %r", err)

    @staticmethod
    def _reason_from_frame(frame: object) -> str:
        from pipecat.frames.frames import CancelFrame, EndFrame

        reason = getattr(frame, "reason", None)
        if isinstance(frame, (CancelFrame, EndFrame)):
            return str(reason) if reason else "agent-ended"
        return "unknown"


def _join_tts_chunks(parts: list[str]) -> str:
    """Join ``TTSTextFrame.text`` chunks with a single space between non-empty
    parts. ``TTSTextFrame`` is already aggregated (sentence / utterance), so its
    text is fully spaced internally — we only need to separate consecutive chunks.
    """
    result = ""
    for text in parts:
        if not text:
            continue
        if result and not result.endswith(" ") and not text.startswith(" "):
            result += " "
        result += text
    return result


__all__ = ["RoarkObserver"]
