"""Wire-format types for the Roark webhook contract.

Mirrors the Zod schemas in the Roark monorepo
(``src/packages/event-bus/events.ts``). Kept as TypedDicts so the JSON
serialization is `dict`-shaped without any conversion step.
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
    result: str | dict
    secondsFromStart: float


class TranscriptEntry(TypedDict):
    role: Literal["agent", "user", "system"]
    text: str
    startMs: int
    endMs: int


class CallStartedPayload(TypedDict, total=False):
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
    # Accepted as 0..1 or 0..100; Roark normalizes.
    samplingRate: float


class CallEndedPayload(TypedDict, total=False):
    event: Literal["call-ended"]
    pipecatCallId: str
    eventTimestamp: str  # ISO 8601 UTC

    callStartedAt: str | None
    callEndedAt: str | None
    callEndedReason: str
    agentSpokeFirst: bool
    recordingSampleRate: int
    recordingNumChannels: int
    transcript: list[TranscriptEntry]
    toolCallMessages: list[ToolCallInvocation | ToolCallResult]


class ChunkUploadUrlResponse(TypedDict, total=False):
    uploadUrl: str
    s3Key: str
    chunkIndex: int
    expiresInSeconds: int
    method: Literal["PUT"]
    contentType: str
