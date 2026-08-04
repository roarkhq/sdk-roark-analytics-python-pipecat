"""Runnable foundational example: voice assistant with RoarkObserver.

A complete voice agent pipeline:

    transport in → Deepgram STT → LLM context → OpenAI LLM → Cartesia TTS → transport out

with ``RoarkObserver`` capturing call lifecycle, transcripts, tool calls, and a
stereo audio recording. The same file runs in two invocation modes:

- **Self-hosted (local process):** ``python examples/bot.py --transport webrtc``
  (or ``--transport daily``). Pipecat's runner (``pipecat.runner.run.main``)
  reads ``--transport`` / ``PIPECAT_TRANSPORT`` and spins up the chosen
  transport.

- **Pipecat Cloud (Daily-hosted):** ``pcc deploy`` then ``pcc agent start``.
  Pipecat Cloud calls ``bot(runner_args)`` per session, injecting
  ``DailyRunnerArguments`` (room URL + token) automatically.

``RoarkObserver`` is runtime-agnostic — the wiring below is identical in both modes.

Env vars (see ``.env.example``):
    Roark:
        - ``ROARK_API_KEY``
    Services:
        - ``DEEPGRAM_API_KEY``
        - ``OPENAI_API_KEY``
        - ``CARTESIA_API_KEY``

For Pipecat Cloud, set them as deployment secrets instead of a ``.env`` file::

    pcc secrets set roark-secrets \\
        ROARK_API_KEY=rk_live_replace_me \\
        DEEPGRAM_API_KEY=... \\
        OPENAI_API_KEY=... \\
        CARTESIA_API_KEY=...

Required pipecat-ai extras::

    uv pip install "pipecat-ai[silero,deepgram,openai,cartesia,webrtc,daily]"

Tested with pipecat-ai 0.0.108.
"""

from __future__ import annotations

import os

from dotenv import load_dotenv
from loguru import logger
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.frames.frames import LLMRunFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import (
    LLMContextAggregatorPair,
    LLMUserAggregatorParams,
)
from pipecat.runner.types import RunnerArguments
from pipecat.runner.utils import create_transport
from pipecat.services.cartesia.tts import CartesiaTTSService
from pipecat.services.deepgram.stt import DeepgramSTTService
from pipecat.services.openai.llm import OpenAILLMService
from pipecat.transports.base_transport import BaseTransport, TransportParams
from pipecat.transports.daily.transport import DailyParams

from pipecat_roark import RoarkObserver

load_dotenv(override=True)

SYSTEM_PROMPT = (
    "You are a helpful assistant in a voice conversation. Your responses will be "
    "spoken aloud, so avoid emojis, bullet points, or other formatting that can't "
    "be spoken. Respond briefly."
)


async def run_bot(transport: BaseTransport, runner_args: RunnerArguments) -> None:
    """Pipeline assembly — identical in self-hosted and Pipecat Cloud modes."""

    stt = DeepgramSTTService(api_key=os.environ["DEEPGRAM_API_KEY"])

    tts = CartesiaTTSService(
        api_key=os.environ["CARTESIA_API_KEY"],
        settings=CartesiaTTSService.Settings(
            voice="71a7ad14-091c-4e8e-a314-022ece01c121",  # British Reading Lady
        ),
    )

    llm = OpenAILLMService(
        api_key=os.environ["OPENAI_API_KEY"],
        settings=OpenAILLMService.Settings(system_instruction=SYSTEM_PROMPT),
    )

    context = LLMContext()
    user_aggregator, assistant_aggregator = LLMContextAggregatorPair(
        context,
        user_params=LLMUserAggregatorParams(vad_analyzer=SileroVADAnalyzer()),
    )

    roark = RoarkObserver(
        api_key=os.environ["ROARK_API_KEY"],
        agent_id="pipecat-roark-foundational",
        runner_args=runner_args,
        agent_name="Pipecat-Roark Foundational Example",
        agent_prompt=SYSTEM_PROMPT,
    )

    pipeline = Pipeline(
        [
            transport.input(),
            stt,
            user_aggregator,
            llm,
            tts,
            transport.output(),
            roark.audio_processor,  # after transport.output() — L=user, R=bot
            assistant_aggregator,
        ]
    )

    task = PipelineTask(
        pipeline,
        params=PipelineParams(
            enable_metrics=True,
            enable_usage_metrics=True,
            observers=[roark],
        ),
        idle_timeout_secs=runner_args.pipeline_idle_timeout_secs,
    )

    @transport.event_handler("on_client_connected")
    async def on_client_connected(transport, client):
        logger.info("Client connected — kicking off conversation")
        context.add_message(
            {"role": "developer", "content": "Please introduce yourself to the user."}
        )
        await task.queue_frames([LLMRunFrame()])

    # Pipecat Cloud sessions (and SmallWebRTC locally) can tear down without an
    # EndFrame reaching observers. aflush() guarantees the call is finalized.
    @transport.event_handler("on_client_disconnected")
    async def on_client_disconnected(transport, client):
        logger.info("Client disconnected — flushing Roark and cancelling task")
        await roark.aflush(reason="client-disconnected")
        await task.cancel()

    runner = PipelineRunner(handle_sigint=runner_args.handle_sigint)
    await runner.run(task)


async def bot(runner_args: RunnerArguments) -> None:
    """Pipecat Cloud entry point. Also invoked by ``pipecat.runner.run.main``
    when running locally via ``python examples/bot.py``.
    """
    transport_params = {
        "daily": lambda: DailyParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            vad_analyzer=SileroVADAnalyzer(),
        ),
        "webrtc": lambda: TransportParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            vad_analyzer=SileroVADAnalyzer(),
        ),
    }
    transport = await create_transport(runner_args, transport_params)
    await run_bot(transport, runner_args)


if __name__ == "__main__":
    # Local self-hosted entry. `pipecat.runner.run.main` reads --transport /
    # PIPECAT_TRANSPORT (daily | webrtc | …) and invokes bot() with the right
    # RunnerArguments subclass. On Pipecat Cloud this block is ignored — the
    # platform calls bot() directly.
    from pipecat.runner.run import main

    main()
