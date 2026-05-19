"""Minimal example: drop RoarkObserver into a Pipecat pipeline.

Configuration via env (see ``.env.example``):
- ``ROARK_API_KEY``
- ``ROARK_WEBHOOK_URL``
- ``ROARK_CHUNK_UPLOAD_URL_ENDPOINT``

Pass ``record_audio=True`` and the observer creates a default
``AudioBufferProcessor`` (stereo 24 kHz, ~256 KB chunks) exposed as
``roark.audio_processor``. Splice it into your pipeline *after*
``transport.output()`` so the bot channel sees post-TTS audio. The processor
mixes user and bot audio, inserts silence during gaps, and emits chunks via
``on_audio_data`` which the observer ships to S3.
"""

from __future__ import annotations

import asyncio
import os

from dotenv import load_dotenv
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask

from pipecat_roark import RoarkObserver

load_dotenv()


async def main() -> None:
    roark = RoarkObserver(
        api_key=os.environ["ROARK_API_KEY"],
        agent_id="example-agent",
        agent_name="Example Agent",
        agent_prompt="You are a helpful voice assistant.",
        record_audio=True,
    )

    pipeline = Pipeline([
        # transport.input(), stt, context_aggregator.user(), llm, tts,
        # transport.output(),
        roark.audio_processor,
        # context_aggregator.assistant(),
    ])

    task = PipelineTask(pipeline, params=PipelineParams(observers=[roark]))

    runner = PipelineRunner()
    await runner.run(task)


if __name__ == "__main__":
    asyncio.run(main())
