"""Roark analytics observer for Pipecat.

Drop ``RoarkObserver`` into your ``PipelineTask`` to ship call lifecycle,
transcripts, tool calls, and recordings to Roark with no other code changes.

Example::

    from pipecat.pipeline.task import PipelineParams, PipelineTask
    from pipecat.processors.audio.audio_buffer_processor import AudioBufferProcessor
    from pipecat_roark import RoarkObserver

    audio_buffer = AudioBufferProcessor(sample_rate=24000, num_channels=2)
    pipeline = Pipeline([..., audio_buffer, ...])

    task = PipelineTask(
        pipeline,
        params=PipelineParams(
            observers=[
                RoarkObserver(
                    api_key="rk_live_replace_me",
                    agent_id="support-bot-v3",
                    runner_args=runner_args,
                    audio_buffer_processor=audio_buffer,
                ),
            ],
        ),
    )

When Pipecat tracing is enabled, the observer also emits OpenTelemetry spans for
the turn-release delay and for each tool call — timings Pipecat measures but does
not trace. No extra setup; pass ``emit_spans=False`` to switch them off.

See https://docs.roark.ai/integrations/pipecat for the full setup guide.
"""

from importlib.metadata import PackageNotFoundError, version

from .observer import RoarkObserver

try:
    # Single source of truth: the installed package metadata (driven by
    # pyproject.toml's ``version``). Avoids the hand-maintained string drifting
    # out of sync with the released version.
    __version__ = version("pipecat-roark")
except PackageNotFoundError:  # pragma: no cover — running from a source tree
    __version__ = "0.0.0+unknown"

__all__ = ["RoarkObserver"]
