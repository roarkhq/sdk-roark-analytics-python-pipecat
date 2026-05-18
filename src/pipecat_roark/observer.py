"""RoarkObserver — Pipecat BaseObserver that ships call data to Roark.

Audio capture is delegated to Pipecat's ``AudioBufferProcessor``: the user inserts
that processor into their pipeline and hands the instance to this observer, which
subscribes to ``on_audio_data`` and uploads each emitted PCM chunk to S3 via a
presigned URL fetched from Roark.

Lifecycle:

1. ``StartFrame`` → POST ``call-started``; ``AudioBufferProcessor.start_recording()``
   is invoked if a processor was provided.
2. During the call:
     * ``TranscriptionFrame`` (finalized) → transcript entry buffered.
     * ``FunctionCallInProgressFrame`` / ``FunctionCallResultFrame`` → tool-call
       messages buffered.
     * ``AudioBufferProcessor.on_audio_data`` → chunk PUT to S3.
3. ``EndFrame`` / ``CancelFrame`` / ``StopFrame`` → drain in-flight uploads and
   POST ``call-ended`` with transcript, tool calls, and PCM format metadata.

Failures are logged and swallowed — the observer never raises into the pipeline.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Literal, cast

from pipecat.observers.base_observer import BaseObserver, FramePushed

from ._types import (
    CallEndedPayload,
    CallStartedPayload,
    ToolCallInvocation,
    ToolCallResult,
    TranscriptEntry,
)
from .client import RoarkClient

if TYPE_CHECKING:  # pragma: no cover
    from pipecat.frames.frames import Frame
    from pipecat.processors.audio.audio_buffer_processor import AudioBufferProcessor

log = logging.getLogger("pipecat_roark.observer")

CallDirection = Literal["INBOUND", "OUTBOUND"]
InterfaceType = Literal["WEB", "PHONE"]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _arguments_to_json_string(value: object) -> str:
    if value is None:
        return "{}"
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value)
    except (TypeError, ValueError):
        return "{}"


class RoarkObserver(BaseObserver):
    """Capture Pipecat pipeline activity and ship it to Roark."""

    def __init__(
        self,
        *,
        api_key: str,
        agent_id: str,
        agent_name: str | None = None,
        agent_prompt: str | None = None,
        agent_phone_number: str | None = None,
        customer_phone_number: str | None = None,
        call_direction: CallDirection | None = None,
        interface_type: InterfaceType | None = None,
        roark_webhook_url: str | None = None,
        roark_chunk_upload_url_endpoint: str | None = None,
        sampling_rate: float | None = None,
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
        self._agent_phone_number = agent_phone_number
        self._customer_phone_number = customer_phone_number
        self._call_direction = call_direction
        self._interface_type = interface_type
        self._sampling_rate = sampling_rate
        self._pipecat_call_id = pipecat_call_id or str(uuid.uuid4())

        self._transcript: list[TranscriptEntry] = []
        self._tool_messages: list[ToolCallInvocation | ToolCallResult] = []
        self._call_started_at: float | None = None  # monotonic seconds
        self._call_started_iso: str | None = None
        self._first_speaker: Literal["agent", "user"] | None = None
        self._started_posted = False
        self._end_flushed = False

        self._chunk_index = 0
        self._inflight_uploads: set[asyncio.Task[None]] = set()
        self._audio_buffer_processor = audio_buffer_processor
        if audio_buffer_processor is not None:
            audio_buffer_processor.add_event_handler("on_audio_data", self._on_audio_data)

    # ------------------------------------------------------------------ frames

    async def on_push_frame(self, data: FramePushed) -> None:  # type: ignore[override]
        from pipecat.frames.frames import (
            CancelFrame,
            EndFrame,
            FunctionCallInProgressFrame,
            FunctionCallResultFrame,
            StartFrame,
            StopFrame,
            TranscriptionFrame,
        )

        frame: Frame = data.frame

        if isinstance(frame, StartFrame) and not self._started_posted:
            await self._post_call_started()
            abp = self._audio_buffer_processor
            if abp is not None:
                try:
                    await abp.start_recording()
                except Exception as err:  # pragma: no cover — defensive
                    log.warning("AudioBufferProcessor.start_recording failed: %r", err)
            return

        if isinstance(frame, TranscriptionFrame):
            if getattr(frame, "finalized", True):
                self._record_transcript_entry(frame.text, role="user")
            return

        if isinstance(frame, FunctionCallInProgressFrame):
            self._tool_messages.append(self._build_tool_invocation(frame))
            return

        if isinstance(frame, FunctionCallResultFrame):
            self._tool_messages.append(self._build_tool_result(frame))
            return

        if isinstance(frame, (EndFrame, CancelFrame, StopFrame)):
            await self._flush_call_ended(reason=self._reason_from_frame(frame))
            return

    async def aflush(self, *, reason: str = "client-disconnected") -> None:
        """Idempotently flush the call.

        Pipecat transports (notably SmallWebRTC) sometimes tear down without pushing
        EndFrame/CancelFrame through the observer; call this from your
        ``on_client_disconnected`` handler to guarantee ``call-ended`` is POSTed.
        Safe to call multiple times.
        """
        await self._flush_call_ended(reason=reason)

    # ------------------------------------------------------------------ call-started

    async def _post_call_started(self) -> None:
        self._started_posted = True
        self._call_started_at = time.monotonic()
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
        if self._agent_phone_number:
            payload["agentPhoneNumber"] = self._agent_phone_number
        if self._customer_phone_number:
            payload["customerPhoneNumber"] = self._customer_phone_number
        if self._call_direction:
            payload["callDirection"] = self._call_direction
        if self._interface_type:
            payload["interfaceType"] = self._interface_type
        elif self._agent_phone_number or self._customer_phone_number:
            payload["interfaceType"] = "PHONE"
        else:
            payload["interfaceType"] = "WEB"
        if self._sampling_rate is not None:
            payload["samplingRate"] = self._sampling_rate

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
        abp = self._audio_buffer_processor
        if abp is not None:
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
            payload["agentSpokeFirst"] = self._first_speaker == "agent"
        if abp is not None and self._chunk_index > 0:
            payload["recordingSampleRate"] = abp.sample_rate
            payload["recordingNumChannels"] = abp.num_channels
        if self._transcript:
            payload["transcript"] = list(self._transcript)
        if self._tool_messages:
            payload["toolCallMessages"] = list(self._tool_messages)

        log.info(
            "call-ended: pipecatCallId=%s reason=%s transcript=%d toolMsgs=%d chunks=%d",
            self._pipecat_call_id,
            reason,
            len(self._transcript),
            len(self._tool_messages),
            self._chunk_index,
        )
        await self._client.post_call_ended(payload)
        await self._client.aclose()

    # ------------------------------------------------------------------ helpers

    def _record_transcript_entry(self, text: str, *, role: Literal["agent", "user"]) -> None:
        if not text:
            return
        offset_ms = self._offset_ms_now()
        if self._first_speaker is None:
            self._first_speaker = role
        self._transcript.append(
            {"role": role, "text": text, "startMs": offset_ms, "endMs": offset_ms}
        )

    def _build_tool_invocation(self, frame: object) -> ToolCallInvocation:
        return cast(
            ToolCallInvocation,
            {
                "role": "tool_call_invocation",
                "toolCallId": getattr(frame, "tool_call_id", ""),
                "name": getattr(frame, "function_name", ""),
                "arguments": _arguments_to_json_string(getattr(frame, "arguments", None)),
                "secondsFromStart": self._offset_ms_now() / 1000.0,
            },
        )

    def _build_tool_result(self, frame: object) -> ToolCallResult:
        result = getattr(frame, "result", "")
        if not isinstance(result, (str, dict, list)):
            result = str(result)
        return cast(
            ToolCallResult,
            {
                "role": "tool_call_result",
                "toolCallId": getattr(frame, "tool_call_id", ""),
                "result": result,
                "secondsFromStart": self._offset_ms_now() / 1000.0,
            },
        )

    def _offset_ms_now(self) -> int:
        if self._call_started_at is None:
            return 0
        return max(0, int((time.monotonic() - self._call_started_at) * 1000))

    @staticmethod
    def _reason_from_frame(frame: object) -> str:
        from pipecat.frames.frames import CancelFrame, EndFrame

        reason = getattr(frame, "reason", None)
        if isinstance(frame, (CancelFrame, EndFrame)):
            return str(reason) if reason else "agent-ended"
        return "unknown"


__all__ = ["RoarkObserver"]
