"""OpenTelemetry spans for the parts of a turn Pipecat measures but does not trace.

Pipecat's built-in tracing covers the ``stt``, ``llm`` and ``tts`` stages of a turn.
Two things that shape perceived latency are missing from it:

* **How long the turn took to release.** The gap between the caller going quiet and
  the pipeline handing the turn on covers VAD silence detection, transcription and
  any turn-analyzer wait. Without it you can see how long transcription took, but
  not how much of the caller's wait belonged to the turn detector.
* **How long each tool call took.** A turn that spent four seconds in a booking API
  is indistinguishable, in the trace, from one that spent it in the model.

Both are already on frames Pipecat emits, so this is a translation layer rather than
new instrumentation. ``RoarkObserver`` drives it; there is nothing to register.

Entirely optional and inert by default: with no OpenTelemetry SDK installed, or with
Pipecat tracing disabled, nothing is emitted and nothing raises. Every failure inside
span emission is swallowed, because telemetry must never take down a live call.
"""

from __future__ import annotations

import logging
import time
from typing import Any

logger = logging.getLogger(__name__)

#: Span emitted per completed function call. Named to match what other voice
#: frameworks emit for the same thing, so one consumer reads them all alike.
TOOL_SPAN_NAME = "function_tool"

#: Span emitted per turn, carrying the turn-release delay.
TURN_SPAN_NAME = "user_turn"

#: Attribute on the turn span: seconds from the caller going quiet to the turn
#: being released. Transcription is inside this number, so subtracting the STT
#: stage's own time isolates what the turn detector contributed.
TURN_RELEASE_ATTRIBUTE = "roark.end_of_turn_seconds"

# Guards the in-flight tool map. A call whose result frame never arrives would
# otherwise hold an entry for the lifetime of the session.
_MAX_IN_FLIGHT_TOOL_CALLS = 256

# A turn release longer than this is a stalled pipeline or a start that was never
# cleared, not a caller pausing. Emitting it would put a multi-minute outlier into a
# distribution otherwise measured in hundreds of milliseconds.
_MAX_TURN_RELEASE_SECONDS = 30.0


class SpanEmitter:
    """Emits turn-release and tool-call spans as children of Pipecat's turn span.

    Internal. ``RoarkObserver`` owns one of these and feeds it the frames it is
    already inspecting, so the spans inherit that observer's frame de-duplication:
    one frame pushed across several processor hops is handled once, and a tool
    call's start is stamped at the first hop rather than the last.
    """

    def __init__(self) -> None:
        """Create an emitter.

        It stays inert until it sees a ``StartFrame``, which is what carries the
        pipeline's tracing context. An observer attached after the pipeline has
        already started therefore emits nothing.
        """
        self._tracing_context: Any | None = None
        self._tool_starts: dict[str, tuple[str, int]] = {}
        self._quiet_since: float | None = None

    def observe(self, frame: Any) -> None:
        """Translate one frame into span state. Never raises."""
        try:
            self._observe(frame)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("span emission failed: %s: %s", type(exc).__name__, exc)

    # -- frame handling ----------------------------------------------------

    def _observe(self, frame: Any) -> None:
        from pipecat.frames.frames import (
            FunctionCallInProgressFrame,
            FunctionCallResultFrame,
            StartFrame,
            UserStoppedSpeakingFrame,
            VADUserStoppedSpeakingFrame,
        )

        if isinstance(frame, StartFrame):
            # Pipecat hands the pipeline's tracing context out on the StartFrame.
            # Reading it here is what makes these spans land in the same trace, and
            # under the same turn, as the stt/llm/tts spans: the services read the
            # identical accessor to parent their own.
            self._tracing_context = getattr(frame, "tracing_context", None)
            return

        if isinstance(frame, FunctionCallInProgressFrame):
            self._on_tool_started(frame)
            return

        if isinstance(frame, FunctionCallResultFrame):
            self._on_tool_finished(frame)
            return

        # Checked before UserStoppedSpeakingFrame purely for clarity of ordering;
        # the two are siblings, not parent and child, so neither shadows the other.
        if isinstance(frame, VADUserStoppedSpeakingFrame):
            self._on_caller_went_quiet(frame)
            return

        if isinstance(frame, UserStoppedSpeakingFrame):
            self._on_turn_released()

    def _on_caller_went_quiet(self, frame: Any) -> None:
        stop_secs = getattr(frame, "stop_secs", 0.0) or 0.0
        timestamp = getattr(frame, "timestamp", None)
        if stop_secs == 0.0 or timestamp is None:
            # Pipecat's STT service skips its own timer when there is no configured
            # hangover, so there would be nothing comparable to subtract from.
            self._quiet_since = None
            return
        # VAD reports when it decided, so the caller actually stopped `stop_secs`
        # earlier. Correcting for that here is what makes this number comparable to
        # the STT stage's own timing, which applies the identical correction.
        self._quiet_since = float(timestamp) - float(stop_secs)

    def _on_turn_released(self) -> None:
        started = self._quiet_since
        self._quiet_since = None
        if started is None:
            return
        released = time.time()
        seconds = released - started
        if not 0.0 < seconds <= _MAX_TURN_RELEASE_SECONDS:
            return
        self._emit(
            TURN_SPAN_NAME,
            int(started * 1_000_000_000),
            int(released * 1_000_000_000),
            {TURN_RELEASE_ATTRIBUTE: seconds},
        )

    def _on_tool_started(self, frame: Any) -> None:
        if len(self._tool_starts) >= _MAX_IN_FLIGHT_TOOL_CALLS:
            return
        # Wall-clock nanoseconds, which is what the OpenTelemetry API needs in order
        # to backdate a span's start.
        self._tool_starts[frame.tool_call_id] = (frame.function_name, time.time_ns())

    def _on_tool_finished(self, frame: Any) -> None:
        started = self._tool_starts.pop(frame.tool_call_id, None)
        if started is None:
            # A result with no recorded start: the pipeline was already running when
            # the observer attached, or the pair was already consumed. Timing it from
            # an unknown origin would read as a very fast tool call.
            return
        function_name, start_ns = started
        self._emit(
            TOOL_SPAN_NAME,
            start_ns,
            time.time_ns(),
            {
                # OpenTelemetry's GenAI convention for a tool execution, so the span
                # reads correctly in any OTel-aware backend rather than only in Roark.
                "gen_ai.operation.name": "execute_tool",
                "gen_ai.tool.name": function_name,
                "gen_ai.tool.call.id": frame.tool_call_id,
            },
        )

    # -- emission ----------------------------------------------------------

    def _emit(self, name: str, start_ns: int, end_ns: int, attributes: dict[str, Any]) -> None:
        context = self._tracing_context
        if context is None:
            return
        parent = context.get_turn_context()
        if parent is None:
            # No turn is active: tracing or turn tracking is off, or this landed
            # between turns. An unparented span would go to a trace of its own,
            # where nothing can associate it with the conversation.
            return

        from opentelemetry import trace

        span = trace.get_tracer("pipecat-roark").start_span(
            name, context=parent, start_time=start_ns
        )
        for key, value in attributes.items():
            span.set_attribute(key, value)
        span.end(end_time=end_ns)
