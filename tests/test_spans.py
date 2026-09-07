"""Tests for the turn-release and tool-call spans.

These run against a real tracer with an in-memory exporter rather than a mock, so
the assertions cover what actually lands in a span: its name, its attributes, and
in particular its start time, which is backdated.

Contracts locked in here:

1. One ``function_tool`` span per completed function call, timed from the
   in-progress frame to the result frame.
2. A result with no matching start is ignored rather than timed from an unknown
   origin, which would read as a very fast tool call.
3. The in-flight map is bounded, so a tool whose result never arrives cannot grow
   without limit over a long session.
4. The ``user_turn`` span's start is corrected by ``stop_secs``. This is the whole
   point of the measurement: the corrected start is what makes the number
   comparable to the STT stage's own timing, which applies the identical
   correction.
5. Turn timings that cannot be interpreted (no VAD hangover, no preceding VAD
   frame, implausibly long) produce nothing rather than a misleading number.
6. Spans hang off the current turn, and are skipped when there is no turn.
7. A fault inside telemetry never propagates into the call.
8. The wiring works end to end through ``RoarkObserver``, which is what customers
   actually register, and the spans inherit its frame de-duplication.
"""

from types import SimpleNamespace
from unittest.mock import patch

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import NonRecordingSpan, set_span_in_context
from pipecat.frames.frames import (
    FunctionCallInProgressFrame,
    FunctionCallResultFrame,
    StartFrame,
    UserStoppedSpeakingFrame,
    VADUserStoppedSpeakingFrame,
)

from pipecat_roark import RoarkObserver
from pipecat_roark.spans import (
    _MAX_IN_FLIGHT_TOOL_CALLS,
    TOOL_SPAN_NAME,
    TURN_RELEASE_ATTRIBUTE,
    TURN_SPAN_NAME,
    SpanEmitter,
)

TRACE_ID = 0x1234567890ABCDEF1234567890ABCDEF
TURN_SPAN_ID = 0x1234567890ABCDEF


class _FakeTracingContext:
    """Stands in for the TracingContext Pipecat hands out on the StartFrame."""

    def __init__(self, active: bool = True):
        self._active = active

    def get_turn_context(self):
        if not self._active:
            return None
        span_context = trace.SpanContext(
            trace_id=TRACE_ID,
            span_id=TURN_SPAN_ID,
            is_remote=False,
            trace_flags=trace.TraceFlags(trace.TraceFlags.SAMPLED),
        )
        return set_span_in_context(NonRecordingSpan(span_context))


def _start_frame(active: bool = True) -> StartFrame:
    frame = StartFrame()
    frame.tracing_context = _FakeTracingContext(active)
    return frame


@pytest.fixture
def exporter():
    """A real tracer whose spans are collected in memory."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    with patch.object(trace, "get_tracer", lambda *a, **k: provider.get_tracer("test")):
        yield exporter


@pytest.fixture
def emitter():
    e = SpanEmitter()
    e.observe(_start_frame())
    return e


# -- tool calls ------------------------------------------------------------


def test_emits_one_span_per_completed_tool_call(emitter, exporter):
    emitter.observe(FunctionCallInProgressFrame("lookup", "call-1", {}))
    emitter.observe(FunctionCallResultFrame("lookup", "call-1", {}, {}))

    (span,) = exporter.get_finished_spans()
    assert span.name == TOOL_SPAN_NAME
    assert span.attributes["gen_ai.operation.name"] == "execute_tool"
    assert span.attributes["gen_ai.tool.name"] == "lookup"
    assert span.attributes["gen_ai.tool.call.id"] == "call-1"
    assert span.end_time >= span.start_time > 0


def test_tool_span_hangs_off_the_current_turn(emitter, exporter):
    emitter.observe(FunctionCallInProgressFrame("lookup", "call-1", {}))
    emitter.observe(FunctionCallResultFrame("lookup", "call-1", {}, {}))

    (span,) = exporter.get_finished_spans()
    assert span.parent.span_id == TURN_SPAN_ID
    assert span.context.trace_id == TRACE_ID


def test_tool_result_without_a_start_is_ignored(emitter, exporter):
    emitter.observe(FunctionCallResultFrame("orphan", "call-unknown", {}, {}))
    assert exporter.get_finished_spans() == ()


def test_in_flight_tool_map_is_bounded(emitter):
    for i in range(_MAX_IN_FLIGHT_TOOL_CALLS + 25):
        emitter.observe(FunctionCallInProgressFrame("never_returns", f"call-{i}", {}))
    assert len(emitter._tool_starts) <= _MAX_IN_FLIGHT_TOOL_CALLS


# -- turn release ----------------------------------------------------------


def test_turn_span_start_is_corrected_by_the_vad_hangover(emitter, exporter):
    # VAD reports at t=1000.0 after 0.2s of silence, so the caller actually stopped
    # at 999.8. The turn releases at 1000.5. Both the span's start and the recorded
    # delay have to run from 999.8, not from 1000.0 — that shared origin is what
    # makes the number comparable to the STT stage's own timing.
    emitter.observe(VADUserStoppedSpeakingFrame(timestamp=1000.0, stop_secs=0.2))
    with patch("pipecat_roark.spans.time.time", return_value=1000.5):
        emitter.observe(UserStoppedSpeakingFrame())

    (span,) = exporter.get_finished_spans()
    assert span.name == TURN_SPAN_NAME
    assert span.attributes[TURN_RELEASE_ATTRIBUTE] == pytest.approx(0.7)
    assert span.start_time == int(999.8 * 1_000_000_000)
    assert span.end_time == int(1000.5 * 1_000_000_000)


def test_no_turn_span_without_a_vad_hangover(emitter, exporter):
    # With stop_secs at zero Pipecat's STT service skips its own timer, so there
    # would be nothing comparable to subtract from.
    emitter.observe(VADUserStoppedSpeakingFrame(timestamp=1000.0, stop_secs=0.0))
    emitter.observe(UserStoppedSpeakingFrame())
    assert exporter.get_finished_spans() == ()


def test_no_turn_span_without_a_preceding_vad_frame(emitter, exporter):
    emitter.observe(UserStoppedSpeakingFrame())
    assert exporter.get_finished_spans() == ()


def test_implausibly_long_turn_release_is_dropped(emitter, exporter):
    # A stalled pipeline, not a caller pausing. Emitting it would put a multi-minute
    # outlier into a distribution measured in hundreds of milliseconds.
    emitter.observe(VADUserStoppedSpeakingFrame(timestamp=1000.0, stop_secs=0.2))
    with patch("pipecat_roark.spans.time.time", return_value=1100.0):
        emitter.observe(UserStoppedSpeakingFrame())
    assert exporter.get_finished_spans() == ()


def test_turn_start_is_consumed_so_it_cannot_be_reused(emitter, exporter):
    emitter.observe(VADUserStoppedSpeakingFrame(timestamp=1000.0, stop_secs=0.2))
    with patch("pipecat_roark.spans.time.time", return_value=1000.5):
        emitter.observe(UserStoppedSpeakingFrame())
        emitter.observe(UserStoppedSpeakingFrame())
    assert len(exporter.get_finished_spans()) == 1


# -- degraded conditions ---------------------------------------------------


def test_nothing_is_emitted_before_a_start_frame(exporter):
    # No StartFrame means no tracing context, so there is no turn to hang from and
    # an unparented span would land in a trace nothing can join to the call.
    emitter = SpanEmitter()
    emitter.observe(FunctionCallInProgressFrame("lookup", "call-1", {}))
    emitter.observe(FunctionCallResultFrame("lookup", "call-1", {}, {}))
    assert exporter.get_finished_spans() == ()


def test_nothing_is_emitted_when_no_turn_is_active(exporter):
    # Pipecat returns no turn context when tracing or turn tracking is off.
    emitter = SpanEmitter()
    emitter.observe(_start_frame(active=False))
    emitter.observe(FunctionCallInProgressFrame("lookup", "call-1", {}))
    emitter.observe(FunctionCallResultFrame("lookup", "call-1", {}, {}))
    assert exporter.get_finished_spans() == ()


def test_emission_failure_does_not_propagate(emitter):
    """A tracing fault must never take down a call in progress."""
    with patch.object(trace, "get_tracer", side_effect=RuntimeError("boom")):
        emitter.observe(FunctionCallInProgressFrame("lookup", "call-1", {}))
        emitter.observe(FunctionCallResultFrame("lookup", "call-1", {}, {}))


def test_turn_context_failure_does_not_propagate(exporter):
    class _Exploding:
        def get_turn_context(self):
            raise RuntimeError("boom")

    emitter = SpanEmitter()
    frame = StartFrame()
    frame.tracing_context = _Exploding()
    emitter.observe(frame)
    emitter.observe(FunctionCallInProgressFrame("lookup", "call-1", {}))
    emitter.observe(FunctionCallResultFrame("lookup", "call-1", {}, {}))
    assert exporter.get_finished_spans() == ()


# -- wiring through the observer customers actually register ---------------


def _observer(**kwargs) -> RoarkObserver:
    return RoarkObserver(
        api_key="rk_live_replace_me",
        agent_id="agent-1",
        runner_args=SimpleNamespace(session_id="session-1"),
        **kwargs,
    )


async def _push(observer: RoarkObserver, frame) -> None:
    await observer.on_push_frame(
        SimpleNamespace(source=None, destination=None, frame=frame, direction=None, timestamp=0)
    )


async def test_spans_are_emitted_with_no_setup_beyond_the_observer(exporter):
    observer = _observer()
    await _push(observer, _start_frame())
    await _push(observer, FunctionCallInProgressFrame("lookup", "call-1", {}))
    await _push(observer, FunctionCallResultFrame("lookup", "call-1", {}, {}))

    (span,) = exporter.get_finished_spans()
    assert span.name == TOOL_SPAN_NAME
    assert span.parent.span_id == TURN_SPAN_ID


async def test_repeated_pushes_of_one_frame_do_not_restamp_the_start(exporter):
    # A frame crossing several processor hops reaches the observer once per hop.
    # Without de-duplication the tool's start would be re-stamped to the last hop
    # and the measured duration would shrink.
    observer = _observer()
    await _push(observer, _start_frame())
    started = FunctionCallInProgressFrame("lookup", "call-1", {})
    # A clock that moves on every read, so a second stamp cannot coincidentally
    # equal the first and let this pass without testing anything.
    ticks = iter(range(1_000, 1_010))
    with patch("pipecat_roark.spans.time.time_ns", lambda: next(ticks)):
        await _push(observer, started)
        first = observer._spans._tool_starts["call-1"][1]
        await _push(observer, started)
    assert observer._spans._tool_starts["call-1"][1] == first == 1_000


async def test_emit_spans_false_switches_them_off(exporter):
    observer = _observer(emit_spans=False)
    await _push(observer, _start_frame())
    await _push(observer, FunctionCallInProgressFrame("lookup", "call-1", {}))
    await _push(observer, FunctionCallResultFrame("lookup", "call-1", {}, {}))
    assert exporter.get_finished_spans() == ()
