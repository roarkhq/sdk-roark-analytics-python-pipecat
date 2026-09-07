"""Tests for the OpenTelemetry span observer.

These run against a real tracer with an in-memory exporter rather than a mock, so
the assertions cover what actually lands in a span: its name, its attributes, and
in particular its start time, which the observer backdates.

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
"""

from unittest.mock import patch

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pipecat.frames.frames import (
    FunctionCallInProgressFrame,
    FunctionCallResultFrame,
    UserStoppedSpeakingFrame,
    VADUserStoppedSpeakingFrame,
)

from pipecat_roark.spans import (
    _MAX_IN_FLIGHT_TOOL_CALLS,
    TOOL_SPAN_NAME,
    TURN_RELEASE_ATTRIBUTE,
    TURN_SPAN_NAME,
    RoarkSpanObserver,
)

TRACE_ID = 0x1234567890ABCDEF1234567890ABCDEF
TURN_SPAN_ID = 0x1234567890ABCDEF


class _FakeTurnTraceObserver:
    """Stands in for the turn observer Pipecat hangs off the task."""

    def __init__(self, span_context=None):
        self._span_context = span_context

    def get_current_turn_context(self):
        return self._span_context


class _FakeTask:
    def __init__(self, turn_trace_observer=None):
        self.turn_trace_observer = turn_trace_observer


def _turn_context():
    return trace.SpanContext(
        trace_id=TRACE_ID,
        span_id=TURN_SPAN_ID,
        is_remote=False,
        trace_flags=trace.TraceFlags(trace.TraceFlags.SAMPLED),
    )


@pytest.fixture
def exporter():
    """A real tracer whose spans are collected in memory."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    with patch.object(trace, "get_tracer", lambda *a, **k: provider.get_tracer("test")):
        yield exporter


@pytest.fixture
def observer():
    obs = RoarkSpanObserver()
    obs.bind_task(_FakeTask(_FakeTurnTraceObserver(_turn_context())))
    return obs


async def _push(observer, frame):
    class _Data:
        pass

    data = _Data()
    data.frame = frame
    await observer.on_push_frame(data)


# -- tool calls ------------------------------------------------------------


async def test_emits_one_span_per_completed_tool_call(observer, exporter):
    await _push(observer, FunctionCallInProgressFrame("lookup", "call-1", {}))
    await _push(observer, FunctionCallResultFrame("lookup", "call-1", {}, {}))

    (span,) = exporter.get_finished_spans()
    assert span.name == TOOL_SPAN_NAME
    assert span.attributes["gen_ai.operation.name"] == "execute_tool"
    assert span.attributes["gen_ai.tool.name"] == "lookup"
    assert span.attributes["gen_ai.tool.call.id"] == "call-1"
    assert span.end_time >= span.start_time > 0


async def test_tool_span_hangs_off_the_current_turn(observer, exporter):
    await _push(observer, FunctionCallInProgressFrame("lookup", "call-1", {}))
    await _push(observer, FunctionCallResultFrame("lookup", "call-1", {}, {}))

    (span,) = exporter.get_finished_spans()
    assert span.parent.span_id == TURN_SPAN_ID
    assert span.context.trace_id == TRACE_ID


async def test_tool_result_without_a_start_is_ignored(observer, exporter):
    await _push(observer, FunctionCallResultFrame("orphan", "call-unknown", {}, {}))
    assert exporter.get_finished_spans() == ()


async def test_in_flight_tool_map_is_bounded(observer):
    for i in range(_MAX_IN_FLIGHT_TOOL_CALLS + 25):
        await _push(observer, FunctionCallInProgressFrame("never_returns", f"call-{i}", {}))
    assert len(observer._tool_starts) <= _MAX_IN_FLIGHT_TOOL_CALLS


# -- turn release ----------------------------------------------------------


async def test_turn_span_start_is_corrected_by_the_vad_hangover(observer, exporter):
    # VAD reports at t=1000.0 after 0.2s of silence, so the caller actually stopped
    # at 999.8. The turn releases at 1000.5. Both the span's start and the recorded
    # delay have to run from 999.8, not from 1000.0 — that shared origin is what
    # makes the number comparable to the STT stage's own timing.
    await _push(observer, VADUserStoppedSpeakingFrame(timestamp=1000.0, stop_secs=0.2))
    with patch("pipecat_roark.spans.time.time", return_value=1000.5):
        await _push(observer, UserStoppedSpeakingFrame())

    (span,) = exporter.get_finished_spans()
    assert span.name == TURN_SPAN_NAME
    assert span.attributes[TURN_RELEASE_ATTRIBUTE] == pytest.approx(0.7)
    assert span.start_time == int(999.8 * 1_000_000_000)
    assert span.end_time == int(1000.5 * 1_000_000_000)


async def test_no_turn_span_without_a_vad_hangover(observer, exporter):
    # With stop_secs at zero Pipecat's STT service skips its own timer, so there
    # would be nothing comparable to subtract from.
    await _push(observer, VADUserStoppedSpeakingFrame(timestamp=1000.0, stop_secs=0.0))
    await _push(observer, UserStoppedSpeakingFrame())
    assert exporter.get_finished_spans() == ()


async def test_no_turn_span_without_a_preceding_vad_frame(observer, exporter):
    await _push(observer, UserStoppedSpeakingFrame())
    assert exporter.get_finished_spans() == ()


async def test_implausibly_long_turn_release_is_dropped(observer, exporter):
    # A stalled pipeline, not a caller pausing. Emitting it would put a multi-minute
    # outlier into a distribution measured in hundreds of milliseconds.
    await _push(observer, VADUserStoppedSpeakingFrame(timestamp=1000.0, stop_secs=0.2))
    with patch("pipecat_roark.spans.time.time", return_value=1100.0):
        await _push(observer, UserStoppedSpeakingFrame())
    assert exporter.get_finished_spans() == ()


async def test_turn_start_is_consumed_so_it_cannot_be_reused(observer, exporter):
    await _push(observer, VADUserStoppedSpeakingFrame(timestamp=1000.0, stop_secs=0.2))
    with patch("pipecat_roark.spans.time.time", return_value=1000.5):
        await _push(observer, UserStoppedSpeakingFrame())
        await _push(observer, UserStoppedSpeakingFrame())
    assert len(exporter.get_finished_spans()) == 1


# -- degraded conditions ---------------------------------------------------


async def test_nothing_is_emitted_without_a_bound_task(exporter):
    # An unparented span would land in a trace of its own, where nothing can
    # associate it with the conversation.
    observer = RoarkSpanObserver()
    await _push(observer, FunctionCallInProgressFrame("lookup", "call-1", {}))
    await _push(observer, FunctionCallResultFrame("lookup", "call-1", {}, {}))
    assert exporter.get_finished_spans() == ()


async def test_nothing_is_emitted_when_no_turn_is_active(exporter):
    # Pipecat returns no turn context when tracing or turn tracking is off.
    observer = RoarkSpanObserver()
    observer.bind_task(_FakeTask(_FakeTurnTraceObserver(None)))
    await _push(observer, FunctionCallInProgressFrame("lookup", "call-1", {}))
    await _push(observer, FunctionCallResultFrame("lookup", "call-1", {}, {}))
    assert exporter.get_finished_spans() == ()


async def test_emission_failure_does_not_propagate(observer):
    """A tracing fault must never take down a call in progress."""
    with patch.object(trace, "get_tracer", side_effect=RuntimeError("boom")):
        await _push(observer, FunctionCallInProgressFrame("lookup", "call-1", {}))
        await _push(observer, FunctionCallResultFrame("lookup", "call-1", {}, {}))


async def test_turn_context_failure_does_not_propagate(observer, exporter):
    class _Exploding:
        def get_current_turn_context(self):
            raise RuntimeError("boom")

    observer.bind_task(_FakeTask(_Exploding()))
    await _push(observer, FunctionCallInProgressFrame("lookup", "call-1", {}))
    await _push(observer, FunctionCallResultFrame("lookup", "call-1", {}, {}))
    assert exporter.get_finished_spans() == ()
