"""RoarkObserver — Pipecat BaseObserver that ships call data to Roark.

Lifecycle, in order:

1. ``on_push_frame`` — observes every frame transfer between processors:
     * ``StartFrame`` (first one) → POSTs ``call-started`` to Roark. The agent
       + call are lazy-registered on the Roark side from this single event
       (no separate "agent sync" pull exists for self-hosted Pipecat).
     * ``TranscriptionFrame`` (finalized) → transcript entry
     * ``FunctionCallInProgressFrame`` → tool_call_invocation
     * ``FunctionCallResultFrame``     → tool_call_result
     * ``OutputAudioRawFrame`` / ``InputAudioRawFrame`` → PCM buffer (if record_audio)
     * ``EndFrame`` / ``CancelFrame`` → triggers end-of-call flush

2. End-of-call flush:
     a. If audio captured: request presigned upload URL, PUT the WAV.
     b. POST ``call-ended`` with batched transcript + tool calls + s3Key.

``BaseObserver`` only exposes ``on_push_frame`` / ``on_process_frame`` hooks
— there is no ``on_pipeline_started``. ``StartFrame`` is the pipeline's
canonical start signal and travels through ``on_push_frame``, so that's
where we hook call-started.

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
        roark_webhook_url: Override the Pipecat webhook event endpoint
            (call-started / call-ended). Defaults to the production Lambda URL.
        roark_upload_url_endpoint: Override the presigned-recording-upload URL
            endpoint. Defaults to the production Lambda URL.
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
        roark_webhook_url: str | None = None,
        roark_upload_url_endpoint: str | None = None,
        record_audio: bool = True,
        pipecat_call_id: str | None = None,
    ) -> None:
        super().__init__()
        client_kwargs: dict[str, str] = {"api_key": api_key}
        if roark_webhook_url is not None:
            client_kwargs["webhook_url"] = roark_webhook_url
        if roark_upload_url_endpoint is not None:
            client_kwargs["upload_url_endpoint"] = roark_upload_url_endpoint
        self._client = RoarkClient(**client_kwargs)
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
        self._started_posted = False
        self._end_flushed = False
        # Guard for re-entrancy on EndFrame/CancelFrame — a single end may push
        # twice through the pipeline; we only want to flush once.
        self._flush_lock = asyncio.Lock()
        # Same idea for StartFrame: serialize concurrent first-frame paths.
        self._start_lock = asyncio.Lock()

    # ---------------------------------------------------------------------
    # BaseObserver hooks
    # ---------------------------------------------------------------------

    async def _post_call_started(self) -> None:
        async with self._start_lock:
            if self._started_posted:
                return
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

        log.info(
            "call-started: pipecatCallId=%s agentId=%s interface=%s direction=%s",
            self._pipecat_call_id,
            self._agent_id,
            payload.get("interfaceType"),
            payload.get("callDirection"),
        )
        ok = await self._client.post_call_started(payload)
        if not ok:
            log.warning("call-started POST failed for %s; continuing", self._pipecat_call_id)
        else:
            log.debug("call-started POST ok for %s", self._pipecat_call_id)

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
            StartFrame,
            TranscriptionFrame,
        )

        frame: "Frame" = data.frame

        # StartFrame is Pipecat's canonical pipeline-start signal. We post
        # call-started on the first one we see; the helper self-guards against
        # repeats (StartFrame can travel past multiple processors).
        if isinstance(frame, StartFrame) and not self._started_posted:
            await self._post_call_started()
            return

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
            buf_bytes = len(self._recorder._buf) if self._recorder is not None else 0  # type: ignore[attr-defined]
            log.info(
                "end-of-call flush triggered by %s; buffered_pcm_bytes=%d transcript_entries=%d tool_msgs=%d",
                type(frame).__name__,
                buf_bytes,
                len(self._transcript),
                len(self._tool_messages),
            )
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

        log.info(
            "call-ended: pipecatCallId=%s reason=%s transcript=%d toolMsgs=%d s3Key=%s",
            self._pipecat_call_id,
            reason,
            len(self._transcript),
            len(self._tool_messages),
            recording_s3_key,
        )
        ok = await self._client.post_call_ended(payload)
        if not ok:
            log.warning("call-ended POST failed for %s; call may be missing data", self._pipecat_call_id)
        else:
            log.debug("call-ended POST ok for %s", self._pipecat_call_id)

        await self._client.aclose()

    async def _upload_recording(self) -> str | None:
        assert self._recorder is not None
        log.info(
            "recording upload starting: pipecatCallId=%s",
            self._pipecat_call_id,
        )
        encode_start = time.monotonic()
        wav = self._recorder.to_wav_bytes()
        encode_ms = int((time.monotonic() - encode_start) * 1000)
        if not wav:
            log.warning(
                "recording upload skipped: empty WAV after encode (pipecatCallId=%s, encode_ms=%d)",
                self._pipecat_call_id,
                encode_ms,
            )
            return None
        log.info(
            "recording WAV encoded: pipecatCallId=%s bytes=%d encode_ms=%d",
            self._pipecat_call_id,
            len(wav),
            encode_ms,
        )

        req_start = time.monotonic()
        upload = await self._client.request_upload_url(
            pipecat_call_id=self._pipecat_call_id, kind="mono", content_type="audio/wav"
        )
        req_ms = int((time.monotonic() - req_start) * 1000)
        if not upload:
            log.warning(
                "recording upload aborted: failed to obtain presigned URL (pipecatCallId=%s, req_ms=%d)",
                self._pipecat_call_id,
                req_ms,
            )
            return None
        s3_key = upload.get("s3Key")
        log.info(
            "recording upload URL acquired: pipecatCallId=%s s3Key=%s expiresInSeconds=%s req_ms=%d",
            self._pipecat_call_id,
            s3_key,
            upload.get("expiresInSeconds"),
            req_ms,
        )

        put_start = time.monotonic()
        ok = await self._client.upload_recording(
            upload_url=upload["uploadUrl"], body=wav, content_type="audio/wav"
        )
        put_ms = int((time.monotonic() - put_start) * 1000)
        if not ok:
            log.warning(
                "recording PUT failed: pipecatCallId=%s s3Key=%s bytes=%d put_ms=%d",
                self._pipecat_call_id,
                s3_key,
                len(wav),
                put_ms,
            )
            return None
        log.info(
            "recording upload complete: pipecatCallId=%s s3Key=%s bytes=%d put_ms=%d",
            self._pipecat_call_id,
            s3_key,
            len(wav),
            put_ms,
        )
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
