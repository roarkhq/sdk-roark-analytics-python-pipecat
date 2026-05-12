"""Minimal example: drop RoarkObserver into a Pipecat pipeline.

This file is illustrative — it does not stand up STT/LLM/TTS providers, just
shows the wiring. Adapt to your existing pipeline by adding the observer to
``observers=[...]`` on ``PipelineParams``.

Set the env var ROARK_API_KEY to a key from your Roark project's API keys page.
"""

from __future__ import annotations

import asyncio
import os

from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask

from pipecat_roark import RoarkObserver


async def main() -> None:
    api_key = os.environ["ROARK_API_KEY"]

    pipeline = Pipeline([
        # ... your STT, context aggregator, LLM, TTS, transport, etc. ...
    ])

    task = PipelineTask(
        pipeline,
        params=PipelineParams(
            observers=[
                RoarkObserver(
                    api_key=api_key,
                    agent_id="example-agent",
                    agent_name="Example Agent",
                    agent_prompt="You are a helpful voice assistant.",
                ),
            ],
        ),
    )

    runner = PipelineRunner()
    await runner.run(task)


if __name__ == "__main__":
    asyncio.run(main())
