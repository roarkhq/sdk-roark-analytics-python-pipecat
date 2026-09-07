"""OpenTelemetry spans for the parts of a turn Pipecat measures but does not trace.

Pipecat's built-in tracing covers the ``stt``, ``llm`` and ``tts`` stages of a turn.
Two things that shape perceived latency are missing from it:

* **How long the turn took to release.** The gap between the caller going quiet and
  the pipeline handing the turn on covers VAD silence detection, transcription and
  any turn-analyzer wait. Without it you can see how long transcription took, but
  not how much of the caller's wait belonged to the turn detector.
* **How long each tool call took.** A turn that spent four seconds in a booking API
  is indistinguishable, in the trace, from one that spent it in the model.

Both are already on frames Pipecat emits; this observer turns them into spans.

Entirely optional and inert by default: with no OpenTelemetry SDK installed, or with
Pipecat tracing disabled, nothing is emitted and nothing raises. Every failure inside
span emission is swallowed, because telemetry must never take down a live call.

Example::

    from pipecat_roark import RoarkObserver, RoarkSpanObserver

    roark = RoarkObserver(api_key="rk_live_replace_me", ...)
    spans = RoarkSpanObserver()

    task = PipelineTask(
        pipeline,
        params=PipelineParams(observers=[roark, spans]),
        enable_tracing=True,
        enable_turn_tracking=True,
    )
    spans.bind_task(task)  # required: spans hang from the turn span the task owns
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from pipecat.observers.base_observer import BaseObserver, FramePushed

if TYPE_CHECKING:  # pragma: no cover - typing only
    from pipecat.pipeline.task import PipelineTask

logger = logging.getLogger(__name__)

# Frame types are imported defensively: this package supports a range of Pipecat
# versions, and a frame that is missing on an older one should disable the feature
# that needs it rather than break the import for everybody.
try:
    from pipecat.frames.frames import (
        FunctionCallInProgressFrame,
        FunctionCallResultFrame,
    )

    _TOOL_FRAMES_AVAILABLE = True
except ImportError:  # pragma: no cover - depends on the installed Pipecat version
    _TOOL_FRAMES_AVAILABLE = False

try:
    from pipecat.frames.frames import (
        UserStoppedSpeakingFrame,
        VADUserStoppedSpeakingFrame,
    )

    _TURN_FRAMES_AVAILABLE = True
except ImportError:  # pragma: no cover - depends on the installed Pipecat version
    _TURN_FRAMES_AVAILABLE = False

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


class RoarkSpanObserver(BaseObserver):
    """Emits turn-release and tool-call spans alongside Pipecat's own tracing.

    Register it in ``PipelineParams(observers=[...])`` next to ``RoarkObserver``,
    then call :meth:`bind_task` with the ``PipelineTask``. Requires Pipecat tracing
    (``enable_tracing=True``, ``enable_turn_tracking=True``) to be on; without it
    there is no turn span for these to attach to and nothing is emitted.
    """

    def __init__(self) -> None:
        """Create the observer. Call :meth:`bind_task` before the pipeline runs."""
        super().__init__()
        self._task: PipelineTask | None = None
        self._warned_unbound = False
        self._tool_starts: dict[str, tuple[str, int]] = {}
        self._quiet_since: float | None = None

    def bind_task(self, task: PipelineTask) -> None:
        """Attach the task that owns the turn spans these spans hang from.

        Separate from ``__init__`` because the observer has to be constructed before
        the ``PipelineTask`` that receives it.

        Args:
            task: The task this observer was registered on.
        """
        self._task = task

    # -- internals ---------------------------------------------------------

    def _turn_parent(self) -> Any | None:
        """A parent context pointing at the current turn span, or ``None``."""
        task = self._task
        if task is None:
            if not self._warned_unbound:
                self._warned_unbound = True
                logger.warning(
                    "RoarkSpanObserver has no task bound, so no spans will be emitted. "
                    "Call bind_task(task) after constructing the PipelineTask."
                )
            return None

        observer = getattr(task, "turn_trace_observer", None)
        if observer is None:
            return None
        try:
            span_context = observer.get_current_turn_context()
            if span_context is None:
                return None
            from opentelemetry.trace import NonRecordingSpan, set_span_in_context

            return set_span_in_context(NonRecordingSpan(span_context))
        except Exception:
            return None

    def _emit(self, name: str, start_ns: int, end_ns: int, attributes: dict[str, Any]) -> None:
        """Record one span. Never raises."""
        parent = self._turn_parent()
        if parent is None:
            # Without a turn to hang from the span would land in a trace of its own,
            # where nothing can associate it with the conversation. Skip it.
            return
        try:
            from opentelemetry import trace

            span = trace.get_tracer("pipecat-roark").start_span(
                name, context=parent, start_time=start_ns
            )
            for key, value in attributes.items():
                span.set_attribute(key, value)
            span.end(end_time=end_ns)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("failed to emit %s span: %s: %s", name, type(exc).__name__, exc)

    # -- observer ----------------------------------------------------------

    async def on_push_frame(self, data: FramePushed) -> None:
        """Translate the relevant frames into spans."""
        frame = data.frame

        if _TURN_FRAMES_AVAILABLE:
            if isinstance(frame, VADUserStoppedSpeakingFrame):
                self._on_caller_went_quiet(frame)
                return
            if isinstance(frame, UserStoppedSpeakingFrame):
                self._on_turn_released()
                return

        if _TOOL_FRAMES_AVAILABLE:
            if isinstance(frame, FunctionCallInProgressFrame):
                self._on_tool_started(frame)
                return
            if isinstance(frame, FunctionCallResultFrame):
                self._on_tool_finished(frame)

    def _on_caller_went_quiet(self, frame: Any) -> None:
        stop_secs = getattr(frame, "stop_secs", 0.0) or 0.0
        if stop_secs == 0.0:
            # Pipecat's STT service skips its own timer when there is no configured
            # hangover, so there would be nothing comparable to subtract from.
            self._quiet_since = None
            return
        # VAD reports when it decided, so the caller actually stopped `stop_secs`
        # earlier. Correcting for that here is what makes this number comparable to
        # the STT stage's own timing, which applies the identical correction.
        self._quiet_since = float(frame.timestamp) - float(stop_secs)

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
            # A result with no recorded start: the observer was attached mid-call, or
            # the pair was already consumed. Timing it from an unknown origin would
            # read as a very fast tool call.
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
