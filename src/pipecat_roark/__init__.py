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
                    api_key="rk_...",
                    agent_id="support-bot-v3",
                    audio_buffer_processor=audio_buffer,
                ),
            ],
        ),
    )

See https://docs.roark.ai/integrations/pipecat for the full setup guide.
"""

from .observer import RoarkObserver

__all__ = ["RoarkObserver"]
__version__ = "0.1.1"
