"""OpenTelemetry spans for the turn timings Pipecat measures but does not trace.

Pipecat traces the ``stt``, ``llm`` and ``tts`` stages of a turn. It does not trace
how long the turn took to release, nor how long each tool call took, though both are
already carried on frames it emits.

OpenTelemetry is imported lazily and is not a dependency of this package.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from pipecat.frames.frames import (
    Frame,
    FunctionCallInProgressFrame,
    FunctionCallResultFrame,
    StartFrame,
    UserStoppedSpeakingFrame,
    VADUserStoppedSpeakingFrame,
)

if TYPE_CHECKING:
    from opentelemetry.context import Context
    from opentelemetry.util.types import AttributeValue
    from pipecat.utils.tracing.tracing_context import TracingContext

logger = logging.getLogger(__name__)

TOOL_SPAN_NAME = "function_tool"
TURN_SPAN_NAME = "user_turn"
TURN_RELEASE_ATTRIBUTE = "roark.end_of_turn_seconds"

NANOSECONDS_PER_SECOND = 1_000_000_000
MAX_IN_FLIGHT_TOOL_CALLS = 256
MAX_TURN_RELEASE_SECONDS = 30.0


class SpanEmitter:
    """Emits turn-release and tool-call spans beneath Pipecat's current turn span.

    ``RoarkObserver`` owns one and feeds it the frames it is already inspecting, so
    emission inherits that observer's frame de-duplication and a tool call is timed
    from its first processor hop rather than its last.

    Inert until a ``StartFrame`` arrives, since that frame carries the tracing
    context these spans parent themselves through.
    """

    def __init__(self) -> None:
        self._tracing_context: TracingContext | None = None
        self._tool_starts: dict[str, tuple[str, int]] = {}
        self._quiet_since: float | None = None

    def observe(self, frame: Frame) -> None:
        """Translate one frame into span state, never raising into the pipeline."""
        try:
            self._dispatch(frame)
        except Exception as exc:
            logger.warning("span emission failed: %s: %s", type(exc).__name__, exc)

    def _dispatch(self, frame: Frame) -> None:
        if isinstance(frame, StartFrame):
            self._tracing_context = frame.tracing_context
        elif isinstance(frame, FunctionCallInProgressFrame):
            self._start_tool_call(frame)
        elif isinstance(frame, FunctionCallResultFrame):
            self._finish_tool_call(frame)
        elif isinstance(frame, VADUserStoppedSpeakingFrame):
            self._mark_caller_quiet(frame)
        elif isinstance(frame, UserStoppedSpeakingFrame):
            self._release_turn()

    def _start_tool_call(self, frame: FunctionCallInProgressFrame) -> None:
        """Open a timer, ignoring calls beyond the cap so a lost result cannot leak."""
        if len(self._tool_starts) < MAX_IN_FLIGHT_TOOL_CALLS:
            self._tool_starts[frame.tool_call_id] = (frame.function_name, time.time_ns())

    def _finish_tool_call(self, frame: FunctionCallResultFrame) -> None:
        """Close the timer opened for this call, if this emitter saw it open.

        A result with no recorded start is dropped rather than timed from an unknown
        origin, which would read as an implausibly fast tool call.
        """
        start = self._tool_starts.pop(frame.tool_call_id, None)
        if start is None:
            return
        function_name, start_ns = start
        self._emit(
            TOOL_SPAN_NAME,
            start_ns,
            time.time_ns(),
            {
                "gen_ai.operation.name": "execute_tool",
                "gen_ai.tool.name": function_name,
                "gen_ai.tool.call.id": frame.tool_call_id,
            },
        )

    def _mark_caller_quiet(self, frame: VADUserStoppedSpeakingFrame) -> None:
        """Note when the caller fell silent, correcting for the VAD hangover.

        ``timestamp`` is when VAD reached its decision, so the caller stopped
        ``stop_secs`` earlier. Pipecat's ``STTService`` starts its own timer from
        that same corrected instant, and the shared origin is what makes the two
        durations subtractable. A zero hangover means it starts no timer at all,
        leaving nothing to compare against.
        """
        self._quiet_since = frame.timestamp - frame.stop_secs if frame.stop_secs else None

    def _release_turn(self) -> None:
        """Close the turn, discarding a gap too long to be a caller pausing."""
        started, self._quiet_since = self._quiet_since, None
        if started is None:
            return
        released = time.time()
        seconds = released - started
        if 0.0 < seconds <= MAX_TURN_RELEASE_SECONDS:
            self._emit(
                TURN_SPAN_NAME,
                int(started * NANOSECONDS_PER_SECOND),
                int(released * NANOSECONDS_PER_SECOND),
                {TURN_RELEASE_ATTRIBUTE: seconds},
            )

    def _emit(
        self,
        name: str,
        start_ns: int,
        end_ns: int,
        attributes: dict[str, AttributeValue],
    ) -> None:
        """Record one span, or nothing while no turn is active.

        An unparented span would open a trace of its own, which nothing could join
        back to the conversation.
        """
        parent = self._turn_context()
        if parent is None:
            return

        from opentelemetry import trace

        span = trace.get_tracer("pipecat-roark").start_span(
            name, context=parent, start_time=start_ns
        )
        span.set_attributes(attributes)
        span.end(end_time=end_ns)

    def _turn_context(self) -> Context | None:
        return self._tracing_context.get_turn_context() if self._tracing_context else None
