"""Wire-format types for the Roark webhook contract.

Mirrors the Zod schemas defined in the Roark monorepo at
``src/packages/event-bus/events.ts`` (PipecatWebhookCallStarted /
PipecatWebhookCallEnded). Kept as TypedDicts rather than dataclasses so the
JSON serialization is `dict`-shaped without any conversion step.
"""

from __future__ import annotations

from typing import Literal, TypedDict


class ToolCallInvocation(TypedDict):
    role: Literal["tool_call_invocation"]
    toolCallId: str
    name: str
    arguments: str  # JSON string — Roark side `JSON.parse`s it
    secondsFromStart: float


class ToolCallResult(TypedDict, total=False):
    role: Literal["tool_call_result"]
    toolCallId: str
    result: str | dict  # observer flattens object results to JSON-string on send
    secondsFromStart: float


class TranscriptEntry(TypedDict):
    role: Literal["agent", "user", "system"]
    text: str
    startMs: int
    endMs: int


class CallStartedPayload(TypedDict, total=False):
    """Body of POST /v1/integrations/pipecat with event=call-started."""

    event: Literal["call-started"]
    pipecatCallId: str
    eventTimestamp: str  # ISO 8601 UTC

    agentId: str
    agentName: str
    agentPrompt: str
    agentPhoneNumber: str
    customerPhoneNumber: str
    callDirection: Literal["INBOUND", "OUTBOUND"]
    interfaceType: Literal["WEB", "PHONE"]


class CallEndedPayload(TypedDict, total=False):
    """Body of POST /v1/integrations/pipecat with event=call-ended."""

    event: Literal["call-ended"]
    pipecatCallId: str
    eventTimestamp: str  # ISO 8601 UTC

    callStartedAt: str | None
    callEndedAt: str | None
    callEndedReason: str
    agentSpokeFirst: bool
    recordingS3Key: str  # set after presigned PUT succeeds
    stereoRecordingS3Key: str
    transcript: list[TranscriptEntry]
    toolCallMessages: list[ToolCallInvocation | ToolCallResult]


class UploadUrlRequest(TypedDict):
    pipecatCallId: str
    kind: Literal["mono", "stereo"]
    contentType: str  # 'audio/wav' | 'audio/mpeg' | 'audio/webm'


class UploadUrlResponse(TypedDict):
    uploadUrl: str
    s3Key: str
    expiresInSeconds: int
