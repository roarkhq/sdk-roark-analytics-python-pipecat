"""Wire-format types for the Roark webhook contract.

The transcript and tool-call shapes mirror Pipecat's own vocabulary
(``TranscriptionUpdateFrame``, ``FunctionCallInProgressFrame``,
``FunctionCallResultFrame``). Roark's ``@roarkanalytics/integrations/pipecat``
package maps them to internal Roark types on its side — this observer stays
dumb and forwards pipecat-native shapes verbatim.
"""

from __future__ import annotations

from typing import Literal, TypedDict


class TranscriptMessage(TypedDict, total=False):
    role: Literal["assistant", "user", "system"]
    content: str
    timestamp: str  # ISO 8601 UTC
    userId: str
    language: str  # BCP-47


class ToolCallMessage(TypedDict, total=False):
    kind: Literal["tool_call"]
    toolCallId: str
    name: str
    arguments: str  # JSON string — Roark side `JSON.parse`s it
    timestamp: str  # ISO 8601 UTC


class ToolResultMessage(TypedDict, total=False):
    kind: Literal["tool_result"]
    toolCallId: str
    content: str  # stringified result (json.dumps for objects, str() for scalars)
    timestamp: str  # ISO 8601 UTC


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
    transcript: list[TranscriptMessage]
    toolCalls: list[ToolCallMessage | ToolResultMessage]


class ChunkUploadUrlResponse(TypedDict, total=False):
    uploadUrl: str
    s3Key: str
    chunkIndex: int
    expiresInSeconds: int
    method: Literal["PUT"]
    contentType: str
