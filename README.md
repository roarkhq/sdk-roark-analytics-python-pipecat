# pipecat-roark

A [Roark](https://roark.ai) analytics observer for
[Pipecat](https://github.com/pipecat-ai/pipecat). Drop one observer into your
pipeline — Roark captures call lifecycle, transcripts, tool calls, and a
stereo audio recording. No other code changes required.

- **Tested with** `pipecat-ai` 0.0.108 and 1.7.0 (compatible with `>= 0.0.104, < 2`)
- **Python** 3.10+
- **Runtime-agnostic** — same code runs self-hosted *and* on Pipecat Cloud

> Maintained by [Roark](https://roark.ai). File issues at
> <https://github.com/roarkhq/sdk-roark-analytics-python-pipecat/issues>.

---

## Contents

- [Quick start](#quick-start)
- [How it works](#how-it-works)
- [Running modes](#running-modes)
- [Examples](#examples)
- [Advanced](#advanced)
  - [Call identity](#call-identity)
  - [Bring your own `AudioBufferProcessor`](#bring-your-own-audiobufferprocessor)
  - [Handling WebRTC disconnects](#handling-webrtc-disconnects)
  - [Correlating with OpenTelemetry tracing](#correlating-with-opentelemetry-tracing)
  - [Tracing turn release and tool calls](#tracing-turn-release-and-tool-calls)
- [Troubleshooting](#troubleshooting)
- [Configuration reference](#configuration-reference)
- [Development](#development)
- [License](#license)

---

## Quick start

### 1. Install

```bash
pip install pipecat-roark
```

### 2. Create a Pipecat integration & API key

Every Roark API key is **bound to a specific integration** — the key only
works for the integration it was created under. Before you can send calls from
this package, create the integration first:

1. In the [Roark dashboard](https://app.roark.ai), go to **Integrations** and
   create a new **Pipecat** integration.
2. Open that integration and generate an **API key** for it.
3. Copy the key (represented as `rk_live_replace_me` below) — this is the
   value you'll set as `ROARK_API_KEY` below.

> Use the key created **under the Pipecat integration**. A key from a different
> integration (or an account-level key not bound to one) will be rejected.

### 3. Configure

Set one env var:

```bash
ROARK_API_KEY=rk_live_replace_me
```

> The Roark API key is all you configure — the observer knows its own service
> endpoints. `ROARK_API_KEY` can also be passed as `api_key=` to `RoarkObserver`.

### 4. Wire the observer

Inside your Pipecat `bot(runner_args)` entry point, drop `RoarkObserver` into
the pipeline's `observers=[...]` list. Splice the auto-created
`roark.audio_processor` **after `transport.output()`** so it sees the bot's
audio post-TTS:

```python
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat_roark import RoarkObserver

roark = RoarkObserver(
    api_key="rk_live_replace_me",
    agent_id="support-bot-v3",
    runner_args=runner_args,
    agent_name="Support Bot v3",
    agent_prompt=SYSTEM_PROMPT,
)

pipeline = Pipeline([
    transport.input(), stt, context_aggregator.user(), llm, tts,
    transport.output(),
    roark.audio_processor,          # after transport.output() — L=user, R=bot
    context_aggregator.assistant(),
])

task = PipelineTask(pipeline, params=PipelineParams(observers=[roark]))
```

That's it — transcripts, tool calls, and the stereo recording flow to Roark
automatically.

---

## How it works

The observer subscribes to Pipecat frames and ships a compact event timeline
to Roark:

| Phase | What's captured |
|---|---|
| **Pipeline start** | `call-started` POST + recording begins. Agent is lazy-registered on Roark the first time it sees this `agent_id`. |
| **User turns** | Final `TranscriptionFrame`s (interim transcriptions ignored). |
| **Assistant turns** | `TTSTextFrame` chunks aggregated between `BotStoppedSpeakingFrame` / `InterruptionFrame` boundaries. |
| **Tool calls** | `FunctionCallInProgressFrame` + `FunctionCallResultFrame`, paired by `toolCallId`. |
| **Audio** | Stereo PCM chunks emitted by `AudioBufferProcessor`, streamed via presigned upload URLs (`POST /v1/integrations/pipecat/chunk-upload-url`). |
| **Pipeline end** | `EndFrame` / `CancelFrame` / `StopFrame` (or `aflush()`) flushes in-flight turns, drains uploads, and POSTs `call-ended`. Roark finalizes the recording on its side. |

Transcripts and tool calls are forwarded in Pipecat's native shape — Roark
maps them to its internal schema on its side.

### Audio capture defaults

The observer always creates a sane-default
[`AudioBufferProcessor`](https://docs.pipecat.ai/server/utilities/audio/audio-recording)
(stereo, ~256 KB chunks) exposed as `roark.audio_processor`. The sample rate
is **adopted from the pipeline's `StartFrame`**, so it tracks whatever the
transport/provider negotiated — 8 kHz on Twilio/Telnyx, 16/24/48 kHz on
Daily/LiveKit, etc. The rate is forwarded to Roark as the recording sample
rate.

### Failure mode

Failures are logged and swallowed — **the observer never raises into the
pipeline**. Your call keeps running even if Roark is unreachable.

---

## Running modes

`RoarkObserver` is **runtime-agnostic** — the same observer wiring works
whether your Pipecat agent runs as a self-hosted process or is deployed to
[Pipecat Cloud](https://docs.pipecat.daily.co/). Write one `bot(runner_args)`
entry point with Pipecat's
[`create_transport`](https://docs.pipecat.ai/server/utilities/runner) helper,
and the same file runs in both modes — see `examples/bot.py`.

> **Both modes use the same `ROARK_API_KEY`** — the key created under your
> Pipecat integration (see [Create a Pipecat integration & API key](#2-create-a-pipecat-integration--api-key)).
> Only where the key is *stored* differs: a local `.env` / secrets manager
> self-hosted, deployment secrets on Pipecat Cloud.

| | Self-hosted | Pipecat Cloud |
|---|---|---|
| Entry point | `python bot.py` → `pipecat.runner.run.main()` dispatches to `bot()` | Platform invokes `bot(runner_args)` per session |
| Room/token | You provision (Daily REST, `pipecat.runner.daily.configure`, …) | Injected via `DailyRunnerArguments` |
| Env vars | `.env` / your secrets manager | `pcc secrets set <name> KEY=value …` |
| Teardown | `EndFrame` is reliable | Sessions can vanish — wire [`aflush()` on disconnect](#handling-webrtc-disconnects) |
| Observer wiring | ← identical → | ← identical → |

### Self-hosted

```bash
cp .env.example .env
# fill in ROARK_API_KEY
uv sync --all-extras
uv run python examples/bot.py --transport daily   # or: --transport webrtc
```

### Pipecat Cloud

Set the same vars as deployment secrets, then deploy:

```bash
pcc secrets set roark-secrets \
    ROARK_API_KEY=rk_live_replace_me

pcc deploy
pcc agent start <agent-name>
```

Reference the secrets from your `pcc-deploy.toml` so the container sees them
as `os.environ["ROARK_API_KEY"]` (etc.) at runtime.

---

## Examples

Two example files ship with the package:

- **`examples/basic_observer.py`** — minimal transport-agnostic wiring sketch.
  Shows where `RoarkObserver` and `roark.audio_processor` slot into a
  `Pipeline` / `PipelineTask`. STT / LLM / TTS stages are omitted — copy them
  into your own pipeline.
- **`examples/bot.py`** — runnable foundational voice assistant
  (Deepgram STT → OpenAI LLM → Cartesia TTS) with `RoarkObserver` wired in.
  Same file runs self-hosted (`--transport webrtc` / `--transport daily`)
  **and** deploys to Pipecat Cloud unchanged.

```bash
cp .env.example .env
# fill in:
#   ROARK_API_KEY
#   DEEPGRAM_API_KEY, OPENAI_API_KEY, CARTESIA_API_KEY

uv sync --all-extras
uv pip install "pipecat-ai[silero,deepgram,openai,cartesia,webrtc,daily]"

# Local browser via Pipecat's built-in WebRTC (no third-party transport account):
uv run python examples/bot.py --transport webrtc
# Or Daily (see Pipecat runner docs for transport-specific setup):
uv run python examples/bot.py --transport daily
```

---

## Advanced

### Call identity

Pass the `runner_args` received by your Pipecat `bot` entry point directly to
the observer. It resolves one call ID internally, in this order:

1. SmallWebRTC `runner_args.webrtc_connection.pc_id`.
2. Pipecat Cloud `runner_args.session_id`.
3. A random UUID, generated once for this observer.

The native SmallWebRTC and Pipecat Cloud identifiers let Roark correlate both
sides of a supported simulation automatically. Other runner and transport
types still ingest normally, but their UUID fallback is local to the observer
and does not provide native-ID simulation merging.

The resolved value is available through the read-only
`observer.pipecat_call_id` property. Applications do not need to generate or
forward an identifier themselves.

### Bring your own `AudioBufferProcessor`

If you need to tune sample rate, channel count, or buffer size, instantiate
`AudioBufferProcessor` yourself and pass it via `audio_buffer_processor=`:

```python
from pipecat.processors.audio.audio_buffer_processor import AudioBufferProcessor

audio_buffer = AudioBufferProcessor(sample_rate=16000, num_channels=1, buffer_size=128 * 1024)

pipeline = Pipeline([..., transport.output(), audio_buffer, ...])

RoarkObserver(
    api_key="rk_live_replace_me",
    agent_id="support-bot-v3",
    runner_args=runner_args,
    audio_buffer_processor=audio_buffer,
)
```

### Handling WebRTC disconnects

Pipecat's WebRTC transports (notably `SmallWebRTC`) sometimes tear down
without pushing `EndFrame` through observers. Call `aflush()` from the
disconnect handler to guarantee the call is finalized on Roark:

```python
@transport.event_handler("on_client_disconnected")
async def _on_disconnect(_, __):
    await roark_observer.aflush(reason="client-disconnected")
```

`aflush()` is idempotent — the regular `EndFrame` path will no-op on the next call.

### Correlating with OpenTelemetry tracing

If you also enable Pipecat's OpenTelemetry tracing
(`PipelineTask(enable_tracing=True)`), use the observer's resolved, read-only
call ID as `PipelineTask.conversation_id`. Each Roark call can then be looked
up by the same value in your tracing backend:

```python
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat_roark import RoarkObserver

roark = RoarkObserver(
    api_key="rk_live_replace_me",
    agent_id="support-bot-v3",
    runner_args=runner_args,
)

task = PipelineTask(
    pipeline,
    params=PipelineParams(observers=[roark]),
    enable_tracing=True,
    conversation_id=roark.pipecat_call_id,
)
```

Pipecat sets `conversation.id` as a **span attribute** on a root
`"conversation"` span (and propagates it to every child span). The OTel
`traceId` itself is auto-generated and unrelated to your call ID; correlation
happens by attribute value. To find the trace for a Roark call, query your
backend by `conversation.id = <pipecatCallId>` (e.g., Honeycomb:
`where conversation.id = "..."`, Jaeger: tag filter, Datadog:
`@conversation.id:...`).

Because `roark.pipecat_call_id` is read-only, the lifecycle payload and tracing
attribute cannot drift after observer construction.

### Tracing turn release and tool calls

Pipecat's tracing covers the `stt`, `llm` and `tts` stages of a turn. Two
timings that shape how fast an agent feels are not in it:

- **Turn release** — the gap between the caller going quiet and the pipeline
  handing the turn on. It covers VAD silence detection, transcription and any
  turn-analyzer wait, so without it you can see how long transcription took but
  not how much of the caller's wait belonged to the turn detector.
- **Tool calls** — a turn that spent four seconds in a booking API is
  indistinguishable, in the trace, from one that spent it in the model.

`RoarkSpanObserver` adds both. Register it next to `RoarkObserver`, then hand it
the task:

```python
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat_roark import RoarkObserver, RoarkSpanObserver

roark = RoarkObserver(
    api_key="rk_live_replace_me",
    agent_id="support-bot-v3",
    runner_args=runner_args,
)
spans = RoarkSpanObserver()

task = PipelineTask(
    pipeline,
    params=PipelineParams(observers=[roark, spans]),
    enable_tracing=True,
    enable_turn_tracking=True,
    conversation_id=roark.pipecat_call_id,
)

spans.bind_task(task)  # required — the spans hang off the turn span the task owns
```

`bind_task` is a separate call because the observer has to exist before the
`PipelineTask` that receives it.

What it emits, per turn, as children of Pipecat's turn span:

| Span            | Carries                                                                       |
| --------------- | ----------------------------------------------------------------------------- |
| `user_turn`     | `roark.end_of_turn_seconds` — caller going quiet to the turn being released     |
| `function_tool` | `gen_ai.tool.name`, `gen_ai.tool.call.id`, `gen_ai.operation.name=execute_tool` |

Transcription happens inside the turn-release window, so subtracting the `stt`
stage's own duration from `roark.end_of_turn_seconds` isolates what the turn
detector contributed. Both spans use OpenTelemetry's GenAI attribute
conventions, so they read correctly in any OTel-aware backend, not only in
Roark.

Requires OpenTelemetry (`pip install "pipecat-ai[tracing]"`) and Pipecat
tracing switched on. Without either, or when no turn is active, the observer
emits nothing and raises nothing: a tracing fault must never take down a live
call.

---

## Troubleshooting

<details>
<summary><strong>Do I need <code>enable_tracing=True</code> on <code>PipelineTask</code>?</strong></summary>

<br>

No. `RoarkObserver` captures raw frames — it does not consume OpenTelemetry
spans. The tracing flag is unrelated. If you *do* enable it and want Roark
calls linked to their traces, see
[Correlating with OpenTelemetry tracing](#correlating-with-opentelemetry-tracing).

</details>

<details>
<summary><strong>Calls aren't finalizing on Roark</strong></summary>

<br>

Some transports (notably `SmallWebRTC`) tear down without pushing `EndFrame`
through observers. Wire `aflush()` into your disconnect handler — see
[Handling WebRTC disconnects](#handling-webrtc-disconnects).

</details>

<details>
<summary><strong>Recording captures user audio only / bot audio only</strong></summary>

<br>

The `AudioBufferProcessor` must sit **after `transport.output()`** so it sees
the bot's audio post-TTS. If it's placed earlier in the pipeline, the bot
channel will be silent.

</details>

<details>
<summary><strong>Transcripts arrive empty</strong></summary>

<br>

The observer warns `call-ended with empty transcript ... no TranscriptionFrame
or TTSTextFrame was observed during the call` when nothing was captured.
Usually this means the STT service isn't emitting finalized
`TranscriptionFrame`s, or the pipeline ended before any speech was processed.

</details>

---

## Configuration reference

| Parameter | Type | Default | Notes |
|-----------|------|---------|-------|
| `api_key` | `str` | — | **Required.** Roark API key. |
| `agent_id` | `str` | — | **Required.** Customer-stable agent identifier. |
| `runner_args` | `object` | — | **Required.** The arguments passed to your Pipecat `bot` entry point. Used to resolve a native SmallWebRTC or Pipecat Cloud identifier when available. |
| `agent_name` | `str \| None` | `None` | Display name. |
| `agent_prompt` | `str \| None` | `None` | System prompt. Persisted as the agent's prompt revision. |
| `audio_buffer_processor` | `AudioBufferProcessor \| None` | `None` | Power-user override — pass your own `AudioBufferProcessor` to control sample rate / channels / buffer size. If omitted, the observer creates a default (stereo, ~256 KB chunks; sample rate adopted from the pipeline's `StartFrame`) accessible via `observer.audio_processor`. |
| `pipecat_call_id` | `str \| None` | `None` | **Deprecated and ignored.** Accepted for compatibility and scheduled for removal in 0.3.0. Pass `runner_args` instead. |

`observer.pipecat_call_id` is the read-only resolved call ID. Use it as
`PipelineTask(conversation_id=...)` when OpenTelemetry tracing is enabled.

---

## Development

```bash
uv sync --all-extras
uv run pytest
uv run ruff check .
```

---

## License

MIT — see [LICENSE](./LICENSE).
