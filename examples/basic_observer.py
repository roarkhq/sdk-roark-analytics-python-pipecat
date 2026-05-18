"""Minimal example: drop RoarkObserver into a Pipecat pipeline.

Configuration via env (see ``.env.example``):
- ``ROARK_API_KEY``
- ``ROARK_WEBHOOK_URL``
- ``ROARK_CHUNK_UPLOAD_URL_ENDPOINT``

Audio capture relies on Pipecat's ``AudioBufferProcessor`` — insert it into the
pipeline and hand the instance to ``RoarkObserver``. The processor mixes user
and bot audio, resamples both sides to a common rate, inserts silence during
gaps, and emits chunks via ``on_audio_data`` which the observer ships to S3.
"""

from __future__ import annotations

import asyncio
import os

from dotenv import load_dotenv
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.audio.audio_buffer_processor import AudioBufferProcessor

from pipecat_roark import RoarkObserver

load_dotenv()


async def main() -> None:
    # Stereo output (L=user, R=bot) at 24 kHz; emit one chunk every ~256 KB
    # (~5.5 s at this rate). buffer_size is in bytes of buffered user audio.
    audio_buffer = AudioBufferProcessor(
        sample_rate=24000,
        num_channels=2,
        buffer_size=256 * 1024,
    )

    pipeline = Pipeline([
        # transport.input(), stt, context_aggregator, llm, tts,
        audio_buffer,
        # transport.output(),
    ])

    task = PipelineTask(
        pipeline,
        params=PipelineParams(
            observers=[
                RoarkObserver(
                    api_key=os.environ["ROARK_API_KEY"],
                    agent_id="example-agent",
                    agent_name="Example Agent",
                    agent_prompt="You are a helpful voice assistant.",
                    audio_buffer_processor=audio_buffer,
                ),
            ],
        ),
    )

    runner = PipelineRunner()
    await runner.run(task)


if __name__ == "__main__":
    asyncio.run(main())
