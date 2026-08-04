"""Minimal example: drop RoarkObserver into a Pipecat pipeline.

Configuration via env (see ``.env.example``):
- ``ROARK_API_KEY``

The observer always creates a default ``AudioBufferProcessor`` (stereo,
~256 KB chunks; sample rate adopted from the pipeline's ``StartFrame``)
exposed as ``roark.audio_processor``. Splice it into your pipeline *after*
``transport.output()`` so the bot channel sees post-TTS audio. The processor
mixes user and bot audio, inserts silence during gaps, and emits chunks via
``on_audio_data`` which the observer ships to Roark.
"""

from __future__ import annotations

import os

from dotenv import load_dotenv
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask

from pipecat_roark import RoarkObserver

load_dotenv()


async def run_pipeline(runner_args: object) -> None:
    roark = RoarkObserver(
        api_key=os.environ["ROARK_API_KEY"],
        agent_id="example-agent",
        runner_args=runner_args,
        agent_name="Example Agent",
        agent_prompt="You are a helpful voice assistant.",
    )

    pipeline = Pipeline(
        [
            # transport.input(), stt, context_aggregator.user(), llm, tts,
            # transport.output(),
            roark.audio_processor,
            # context_aggregator.assistant(),
        ]
    )

    task = PipelineTask(pipeline, params=PipelineParams(observers=[roark]))

    runner = PipelineRunner()
    await runner.run(task)
