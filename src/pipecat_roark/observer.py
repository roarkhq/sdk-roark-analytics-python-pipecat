"""RoarkObserver — Pipecat BaseObserver that ships call data to Roark.

Lifecycle, in order:

1. ``on_pipeline_started`` — POSTs ``call-started`` to Roark. The agent + call
   are lazy-registered on the Roark side from this single event (no separate
   "agent sync" pull exists for self-hosted Pipecat).

2. ``on_push_frame`` — buffers frames in memory:
     * ``TranscriptionFrame`` (finalized) → transcript entry
     * ``FunctionCallInProgressFrame`` → tool_call_invocation
     * ``FunctionCallResultFrame``     → tool_call_result
     * ``OutputAudioRawFrame`` / ``InputAudioRawFrame`` → PCM buffer (if record_audio)
     * ``EndFrame`` / ``CancelFrame`` → triggers end-of-call flush

3. End-of-call flush:
     a. If audio captured: request presigned upload URL, PUT the WAV.
     b. POST ``call-ended`` with batched transcript + tool calls + s3Key.

Failures at every step are logged and swallowed — the observer must never
raise into the pipeline. Worst case the call appears in Roark with partial
data instead of full data.
"""

from __future__ import annotations

import asyncio
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
from .audio import PCMRecorder
from .client import RoarkClient

if TYPE_CHECKING:  # pragma: no cover - import-only for typing
    from pipecat.frames.frames import Frame

log = logging.getLogger("pipecat_roark.observer")

CallDirection = Literal["INBOUND", "OUTBOUND"]
InterfaceType = Literal["WEB", "PHONE"]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class RoarkObserver(BaseObserver):
    """Capture Pipecat pipeline activity and ship it to Roark.

    Args:
        api_key: Roark API key (created on the API keys page in your Roark project).
        agent_id: Customer-stable identifier for the agent. Roark looks the agent
            up by this value across calls; first sight lazy-registers it.
        agent_name: Human-readable agent name. Defaults to ``agent_id`` if omitted.
        agent_prompt: System prompt at call start. Persisted as the agent's
            current prompt revision on the Roark side. Optional.
        agent_phone_number: Agent E.164 number (when using a telephony serializer).
        customer_phone_number: Customer E.164 number.
        call_direction: ``INBOUND`` or ``OUTBOUND``. Inferred from phone-number
            presence if omitted.
        interface_type: ``WEB`` (WebRTC) or ``PHONE`` (PSTN). Inferred from
            phone-number presence if omitted.
        roark_base_url: Base URL for Roark's API. Override for staging / self-host.
        record_audio: When True, the observer accumulates PCM frames in memory
            and uploads a WAV to Roark on call-ended. Default True.
        pipecat_call_id: Stable call identifier. Auto-generated if omitted.
    """

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
        roark_base_url: str = "https://api.roark.ai",
        record_audio: bool = True,
        pipecat_call_id: str | None = None,
    ) -> None:
        super().__init__()
        self._client = RoarkClient(api_key=api_key, base_url=roark_base_url)
        self._agent_id = agent_id
        self._agent_name = agent_name
        self._agent_prompt = agent_prompt
        self._agent_phone_number = agent_phone_number
        self._customer_phone_number = customer_phone_number
        self._call_direction = call_direction
        self._interface_type = interface_type
        self._record_audio = record_audio

        self._pipecat_call_id = pipecat_call_id or str(uuid.uuid4())
        self._recorder = PCMRecorder() if record_audio else None

        self._transcript: list[TranscriptEntry] = []
        self._tool_messages: list[ToolCallInvocation | ToolCallResult] = []
        self._call_started_at: float | None = None  # monotonic seconds
        self._call_started_iso: str | None = None
        self._first_speaker: Literal["agent", "user"] | None = None
        self._end_flushed = False
        # Guard for re-entrancy on EndFrame/CancelFrame — a single end may push
        # twice through the pipeline; we only want to flush once.
        self._flush_lock = asyncio.Lock()

    # ---------------------------------------------------------------------
    # BaseObserver hooks
    # ---------------------------------------------------------------------

    async def on_pipeline_started(self) -> None:  # type: ignore[override]
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

        ok = await self._client.post_call_started(payload)
        if not ok:
            log.warning("call-started POST failed for %s; continuing", self._pipecat_call_id)

    async def on_push_frame(self, data: FramePushed) -> None:  # type: ignore[override]
        # Heavy frame-type imports are deferred so importing pipecat_roark
        # doesn't pin every frame module. Pipecat is required at install time
        # (declared in pyproject.toml) so the import will always succeed here.
        from pipecat.frames.frames import (
            CancelFrame,
            EndFrame,
            FunctionCallInProgressFrame,
            FunctionCallResultFrame,
            InputAudioRawFrame,
            OutputAudioRawFrame,
            TranscriptionFrame,
        )

        frame: "Frame" = data.frame

        if isinstance(frame, TranscriptionFrame):
            # Pipecat marks transcripts as finalized once the STT has committed.
            # We only ship final transcripts; interim partials would just churn.
            if getattr(frame, "finalized", True):
                self._record_transcript_entry(frame.text, role="user")
            return

        if isinstance(frame, FunctionCallInProgressFrame):
            self._tool_messages.append(self._build_tool_invocation(frame))
            return

        if isinstance(frame, FunctionCallResultFrame):
            self._tool_messages.append(self._build_tool_result(frame))
            return

        if self._recorder is not None and isinstance(frame, OutputAudioRawFrame | InputAudioRawFrame):
            try:
                self._recorder.append(frame.audio, frame.sample_rate, frame.num_channels)
            except Exception as err:  # noqa: BLE001
                # Never raise into the pipeline — log and drop the chunk.
                log.debug("audio append dropped: %s", err)
            return

        if isinstance(frame, (EndFrame, CancelFrame)):
            await self._flush_call_ended(reason=self._reason_from_frame(frame))
            return

    # ---------------------------------------------------------------------
    # End-of-call flush
    # ---------------------------------------------------------------------

    async def _flush_call_ended(self, *, reason: str) -> None:
        async with self._flush_lock:
            if self._end_flushed:
                return
            self._end_flushed = True

        recording_s3_key: str | None = None
        if self._recorder is not None and not self._recorder.is_empty:
            recording_s3_key = await self._upload_recording()

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
        if recording_s3_key:
            payload["recordingS3Key"] = recording_s3_key
        if self._transcript:
            payload["transcript"] = list(self._transcript)
        if self._tool_messages:
            payload["toolCallMessages"] = list(self._tool_messages)

        ok = await self._client.post_call_ended(payload)
        if not ok:
            log.warning("call-ended POST failed for %s; call may be missing data", self._pipecat_call_id)

        await self._client.aclose()

    async def _upload_recording(self) -> str | None:
        assert self._recorder is not None
        wav = self._recorder.to_wav_bytes()
        if not wav:
            return None
        upload = await self._client.request_upload_url(
            pipecat_call_id=self._pipecat_call_id, kind="mono", content_type="audio/wav"
        )
        if not upload:
            return None
        ok = await self._client.upload_recording(
            upload_url=upload["uploadUrl"], body=wav, content_type="audio/wav"
        )
        if not ok:
            return None
        return upload["s3Key"]

    # ---------------------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------------------

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
        # Pipecat's frame fields use snake_case; the Roark contract uses camelCase
        # (matches the existing Vapi/Retell shape). We translate at the boundary.
        seconds = self._offset_ms_now() / 1000.0
        return cast(
            ToolCallInvocation,
            {
                "role": "tool_call_invocation",
                "toolCallId": getattr(frame, "tool_call_id", ""),
                "name": getattr(frame, "function_name", ""),
                "arguments": _arguments_to_json_string(getattr(frame, "arguments", None)),
                "secondsFromStart": seconds,
            },
        )

    def _build_tool_result(self, frame: object) -> ToolCallResult:
        seconds = self._offset_ms_now() / 1000.0
        result = getattr(frame, "result", "")
        # Roark accepts string-or-object on `result`; we coerce to string in the
        # client for transport simplicity (matches how Vapi serializes its
        # tool_call_result.message field).
        if not isinstance(result, (str, dict, list)):
            result = str(result)
        return cast(
            ToolCallResult,
            {
                "role": "tool_call_result",
                "toolCallId": getattr(frame, "tool_call_id", ""),
                "result": result,
                "secondsFromStart": seconds,
            },
        )

    def _offset_ms_now(self) -> int:
        if self._call_started_at is None:
            return 0
        return max(0, int((time.monotonic() - self._call_started_at) * 1000))

    @staticmethod
    def _reason_from_frame(frame: object) -> str:
        # EndFrame.reason and CancelFrame.reason are free-form. Coerce to a
        # short string so the Roark side maps cleanly to CallEndedStatusEnum.
        from pipecat.frames.frames import CancelFrame, EndFrame

        reason = getattr(frame, "reason", None)
        if isinstance(frame, CancelFrame):
            return str(reason) if reason else "agent-ended"
        if isinstance(frame, EndFrame):
            return str(reason) if reason else "agent-ended"
        return "unknown"


def _arguments_to_json_string(value: object) -> str:
    """Coerce a tool-call ``arguments`` value to the JSON string Roark expects."""
    import json

    if value is None:
        return "{}"
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value)
    except (TypeError, ValueError):
        return "{}"
