"""Roark analytics observer for Pipecat.

Drop ``RoarkObserver`` into your ``PipelineTask`` to ship call lifecycle, transcripts,
tool calls, and recordings to Roark with no other code changes.

Example::

    from pipecat.pipeline.task import PipelineParams, PipelineTask
    from pipecat_roark import RoarkObserver

    task = PipelineTask(
        pipeline,
        params=PipelineParams(
            observers=[
                RoarkObserver(api_key="rk_...", agent_id="support-bot-v3"),
            ],
        ),
    )

See https://docs.roark.ai/integrations/pipecat for the full setup guide.
"""

from .observer import RoarkObserver

__all__ = ["RoarkObserver"]
__version__ = "0.1.0"
