# pipecat-roark

A [Roark](https://roark.ai) analytics observer for
[Pipecat](https://github.com/pipecat-ai/pipecat). Drop one observer into your
pipeline — Roark captures call lifecycle, transcripts, tool calls, and a
stereo audio recording. No other code changes required.

- **Tested with** `pipecat-ai` 0.0.108 (compatible with `>= 0.0.40, < 1`)
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
  - [Bring your own `AudioBufferProcessor`](#bring-your-own-audiobufferprocessor)
  - [Handling WebRTC disconnects](#handling-webrtc-disconnects)
  - [Correlating with OpenTelemetry tracing](#correlating-with-opentelemetry-tracing)
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

### 2. Configure

Set one env var:

```bash
ROARK_API_KEY=rk_live_...
```

> The Roark API key is all you configure — the observer knows its own service
> endpoints. `ROARK_API_KEY` can also be passed as `api_key=` to `RoarkObserver`.

### 3. Wire the observer

Drop `RoarkObserver` into your pipeline's `observers=[...]` list. Splice the
auto-created `roark.audio_processor` **after `transport.output()`** so it sees
the bot's audio post-TTS:

```python
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat_roark import RoarkObserver

roark = RoarkObserver(
    api_key="rk_live_...",
    agent_id="support-bot-v3",
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

The default processor is Pipecat's stock `AudioBufferProcessor` with one
change: it **arms recording inline on the `StartFrame`** so a bot that speaks
first is captured from sample 0 (the "first couple of seconds missing" bug).
Silence is handled by the stock cross-channel sync — each channel (L = user,
R = agent) is padded up to the other's position as audio arrives, so real
inter-turn pauses are preserved and the merged tracks stay aligned on one
real-time timeline. A bring-your-own `AudioBufferProcessor` is used as-is.

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
    ROARK_API_KEY=rk_live_...

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

### Bring your own `AudioBufferProcessor`

If you need to tune sample rate, channel count, or buffer size, instantiate
`AudioBufferProcessor` yourself and pass it via `audio_buffer_processor=`:

```python
from pipecat.processors.audio.audio_buffer_processor import AudioBufferProcessor

audio_buffer = AudioBufferProcessor(sample_rate=16000, num_channels=1, buffer_size=128 * 1024)

pipeline = Pipeline([..., transport.output(), audio_buffer, ...])

RoarkObserver(
    api_key="rk_live_...",
    agent_id="support-bot-v3",
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
(`PipelineTask(enable_tracing=True)`), generate **one** call ID up front and
pass it to both sides — the observer's `pipecat_call_id` and `PipelineTask`'s
`conversation_id` — so each Roark call can be looked up by the same value in
your tracing backend:

```python
import uuid
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat_roark import RoarkObserver

call_id = str(uuid.uuid4())   # or your own external ID (Twilio CallSid, DB row id, …)

roark = RoarkObserver(
    api_key="rk_live_...",
    agent_id="support-bot-v3",
    pipecat_call_id=call_id,   # appears on the Roark record as `pipecatCallId`
)

task = PipelineTask(
    pipeline,
    params=PipelineParams(observers=[roark]),
    enable_tracing=True,
    conversation_id=call_id,   # set as the `conversation.id` span attribute by Pipecat
)
```

Pipecat sets `conversation.id` as a **span attribute** on a root
`"conversation"` span (and propagates it to every child span). The OTel
`traceId` itself is auto-generated and unrelated to your call ID; correlation
happens by attribute value. To find the trace for a Roark call, query your
backend by `conversation.id = <pipecatCallId>` (e.g., Honeycomb:
`where conversation.id = "..."`, Jaeger: tag filter, Datadog:
`@conversation.id:...`).

> If you omit `pipecat_call_id`, the observer generates one internally — fine
> for standalone use, but you won't be able to link a Roark call to its trace.
> With OTel enabled, **always pass the same value to both**.

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
| `agent_name` | `str \| None` | `None` | Display name. |
| `agent_prompt` | `str \| None` | `None` | System prompt. Persisted as the agent's prompt revision. |
| `audio_buffer_processor` | `AudioBufferProcessor \| None` | `None` | Power-user override — pass your own `AudioBufferProcessor` to control sample rate / channels / buffer size. If omitted, the observer creates a default (stereo, ~256 KB chunks; sample rate adopted from the pipeline's `StartFrame`) accessible via `observer.audio_processor`. |
| `pipecat_call_id` | `str \| None` | random UUID | Stable call identifier. Pass the same value to `PipelineTask(conversation_id=...)` when OTel tracing is enabled — see [Correlating with OpenTelemetry tracing](#correlating-with-opentelemetry-tracing). |

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
