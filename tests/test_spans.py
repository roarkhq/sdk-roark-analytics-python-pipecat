"""Tests for the turn-release and tool-call spans.

These run against a real tracer with an in-memory exporter rather than mocks, so
they assert on what actually lands in a span: name, attributes, parentage, and the
start time, which is backdated.
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
    Frame,
    FunctionCallInProgressFrame,
    FunctionCallResultFrame,
    StartFrame,
    UserStoppedSpeakingFrame,
    VADUserStoppedSpeakingFrame,
)
from pipecat.observers.base_observer import FramePushed
from pipecat.processors.frame_processor import FrameDirection

from pipecat_roark import RoarkObserver
from pipecat_roark._spans import (
    MAX_IN_FLIGHT_TOOL_CALLS,
    TOOL_SPAN_NAME,
    TURN_RELEASE_ATTRIBUTE,
    TURN_SPAN_NAME,
    SpanEmitter,
)

TRACE_ID = 0x1234567890ABCDEF1234567890ABCDEF
TURN_SPAN_ID = 0x1234567890ABCDEF


class FakeTracingContext:
    """Stands in for the TracingContext Pipecat hands out on the StartFrame."""

    def __init__(self, turn_active: bool = True) -> None:
        self._turn_active = turn_active

    def get_turn_context(self):
        if not self._turn_active:
            return None
        span_context = trace.SpanContext(
            trace_id=TRACE_ID,
            span_id=TURN_SPAN_ID,
            is_remote=False,
            trace_flags=trace.TraceFlags(trace.TraceFlags.SAMPLED),
        )
        return set_span_in_context(NonRecordingSpan(span_context))


def start_frame(turn_active: bool = True) -> StartFrame:
    frame = StartFrame()
    frame.tracing_context = FakeTracingContext(turn_active)
    return frame


def tool_started(call_id: str = "call-1", name: str = "lookup") -> FunctionCallInProgressFrame:
    return FunctionCallInProgressFrame(name, call_id, {})


def tool_finished(call_id: str = "call-1", name: str = "lookup") -> FunctionCallResultFrame:
    return FunctionCallResultFrame(name, call_id, {}, {})


@pytest.fixture
def exporter():
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    with patch.object(trace, "get_tracer", lambda *a, **k: provider.get_tracer("test")):
        yield exporter


@pytest.fixture
def emitter():
    emitter = SpanEmitter()
    emitter.observe(start_frame())
    return emitter


def test_emits_one_span_per_completed_tool_call(emitter, exporter):
    emitter.observe(tool_started())
    emitter.observe(tool_finished())

    (span,) = exporter.get_finished_spans()
    assert span.name == TOOL_SPAN_NAME
    assert span.attributes["gen_ai.operation.name"] == "execute_tool"
    assert span.attributes["gen_ai.tool.name"] == "lookup"
    assert span.attributes["gen_ai.tool.call.id"] == "call-1"
    assert span.end_time >= span.start_time > 0


def test_tool_span_hangs_off_the_current_turn(emitter, exporter):
    emitter.observe(tool_started())
    emitter.observe(tool_finished())

    (span,) = exporter.get_finished_spans()
    assert span.parent.span_id == TURN_SPAN_ID
    assert span.context.trace_id == TRACE_ID


def test_tool_result_without_a_start_is_ignored(emitter, exporter):
    emitter.observe(tool_finished("call-unknown", "orphan"))

    assert exporter.get_finished_spans() == ()


def test_in_flight_tool_calls_are_capped(emitter):
    for index in range(MAX_IN_FLIGHT_TOOL_CALLS + 25):
        emitter.observe(tool_started(f"call-{index}", "never_returns"))

    assert len(emitter._tool_starts) == MAX_IN_FLIGHT_TOOL_CALLS


def test_turn_span_start_is_corrected_by_the_vad_hangover(emitter, exporter):
    # VAD decides at 1000.0 after 0.2s of silence, so the caller stopped at 999.8.
    # Both the span start and the recorded delay must run from there, because the
    # STT stage times itself from the same corrected instant.
    emitter.observe(VADUserStoppedSpeakingFrame(timestamp=1000.0, stop_secs=0.2))
    with patch("pipecat_roark._spans.time.time", return_value=1000.5):
        emitter.observe(UserStoppedSpeakingFrame())

    (span,) = exporter.get_finished_spans()
    assert span.name == TURN_SPAN_NAME
    assert span.attributes[TURN_RELEASE_ATTRIBUTE] == pytest.approx(0.7)
    assert span.start_time == int(999.8 * 1_000_000_000)
    assert span.end_time == int(1000.5 * 1_000_000_000)


def test_no_turn_span_without_a_vad_hangover(emitter, exporter):
    emitter.observe(VADUserStoppedSpeakingFrame(timestamp=1000.0, stop_secs=0.0))
    emitter.observe(UserStoppedSpeakingFrame())

    assert exporter.get_finished_spans() == ()


def test_no_turn_span_without_a_preceding_vad_frame(emitter, exporter):
    emitter.observe(UserStoppedSpeakingFrame())

    assert exporter.get_finished_spans() == ()


def test_implausibly_long_turn_release_is_dropped(emitter, exporter):
    emitter.observe(VADUserStoppedSpeakingFrame(timestamp=1000.0, stop_secs=0.2))
    with patch("pipecat_roark._spans.time.time", return_value=1100.0):
        emitter.observe(UserStoppedSpeakingFrame())

    assert exporter.get_finished_spans() == ()


def test_turn_start_is_consumed_so_it_cannot_be_reused(emitter, exporter):
    emitter.observe(VADUserStoppedSpeakingFrame(timestamp=1000.0, stop_secs=0.2))
    with patch("pipecat_roark._spans.time.time", return_value=1000.5):
        emitter.observe(UserStoppedSpeakingFrame())
        emitter.observe(UserStoppedSpeakingFrame())

    assert len(exporter.get_finished_spans()) == 1


def test_nothing_is_emitted_before_a_start_frame(exporter):
    emitter = SpanEmitter()
    emitter.observe(tool_started())
    emitter.observe(tool_finished())

    assert exporter.get_finished_spans() == ()


def test_nothing_is_emitted_while_no_turn_is_active(exporter):
    emitter = SpanEmitter()
    emitter.observe(start_frame(turn_active=False))
    emitter.observe(tool_started())
    emitter.observe(tool_finished())

    assert exporter.get_finished_spans() == ()


def test_emission_failure_does_not_propagate(emitter):
    with patch.object(trace, "get_tracer", side_effect=RuntimeError("boom")):
        emitter.observe(tool_started())
        emitter.observe(tool_finished())


def test_turn_context_failure_does_not_propagate(exporter):
    class ExplodingTracingContext:
        def get_turn_context(self):
            raise RuntimeError("boom")

    emitter = SpanEmitter()
    frame = StartFrame()
    frame.tracing_context = ExplodingTracingContext()
    emitter.observe(frame)
    emitter.observe(tool_started())
    emitter.observe(tool_finished())

    assert exporter.get_finished_spans() == ()


def build_observer(**kwargs) -> RoarkObserver:
    return RoarkObserver(
        api_key="rk_live_replace_me",
        agent_id="agent-1",
        runner_args=SimpleNamespace(session_id="session-1"),
        **kwargs,
    )


async def push(observer: RoarkObserver, frame: Frame) -> None:
    await observer.on_push_frame(
        FramePushed(
            source=None,
            destination=None,
            frame=frame,
            direction=FrameDirection.DOWNSTREAM,
            timestamp=0,
        )
    )


async def test_spans_need_no_setup_beyond_the_observer(exporter):
    observer = build_observer()
    await push(observer, start_frame())
    await push(observer, tool_started())
    await push(observer, tool_finished())

    (span,) = exporter.get_finished_spans()
    assert span.name == TOOL_SPAN_NAME
    assert span.parent.span_id == TURN_SPAN_ID


async def test_repeated_pushes_of_one_frame_do_not_restamp_the_start(exporter):
    # One frame reaches the observer once per processor hop. Without the observer's
    # de-duplication the start would move to the last hop and the duration shrink.
    observer = build_observer()
    await push(observer, start_frame())
    frame = tool_started()
    clock = iter(range(1_000, 1_010))

    with patch("pipecat_roark._spans.time.time_ns", lambda: next(clock)):
        await push(observer, frame)
        await push(observer, frame)

    assert observer._spans._tool_starts["call-1"][1] == 1_000


async def test_emit_spans_false_switches_them_off(exporter):
    observer = build_observer(emit_spans=False)
    await push(observer, start_frame())
    await push(observer, tool_started())
    await push(observer, tool_finished())

    assert exporter.get_finished_spans() == ()
