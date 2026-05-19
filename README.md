# pipecat-roark

A [Roark](https://roark.ai) analytics observer for [Pipecat](https://github.com/pipecat-ai/pipecat).
Drop one observer into your pipeline and Roark captures call lifecycle, transcripts,
tool calls, and audio recordings — no other code changes required.

## Install

```bash
pip install pipecat-roark
```

Requires Python 3.10+ and `pipecat-ai >= 0.0.40`.

## Configuration

Set these env vars (or pass them as kwargs to `RoarkObserver`):

```bash
ROARK_API_KEY=rk_live_...
ROARK_WEBHOOK_URL=https://...lambda-url.us-east-1.on.aws/
ROARK_CHUNK_UPLOAD_URL_ENDPOINT=https://api.roark.ai/v1/pipecat/chunk-upload-url
```

Both URL vars are required; the observer raises at construction if they're missing.

## Usage

Drop `RoarkObserver` into your pipeline's `observers=[...]` list — that's it.
Transcripts and tool calls are captured automatically from the frames flowing
through the pipeline; no extra processors required.

### Minimal — transcripts and tool calls only

```python
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat_roark import RoarkObserver

task = PipelineTask(
    pipeline,
    params=PipelineParams(
        observers=[RoarkObserver(api_key="rk_live_...", agent_id="support-bot-v3")],
    ),
)
```

### With audio recording

Pass `record_audio=True` and the observer creates a sane-default
[`AudioBufferProcessor`](https://docs.pipecat.ai/server/utilities/audio/audio-recording)
(stereo, ~256 KB chunks) exposed as `roark.audio_processor`. The sample rate
is **adopted from the pipeline's `StartFrame`** so it tracks whatever the
transport/provider negotiated — 8 kHz on Twilio/Telnyx, 16/24/48 kHz on
Daily/LiveKit, etc. The actual rate is forwarded to Roark as
`recordingSampleRate` on `call-ended`. Splice the processor into your pipeline
**after `transport.output()`** so it sees the bot's audio post-TTS:

```python
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat_roark import RoarkObserver

roark = RoarkObserver(
    api_key="rk_live_...",
    agent_id="support-bot-v3",
    agent_name="Support Bot v3",
    agent_prompt=SYSTEM_PROMPT,
    record_audio=True,
)

pipeline = Pipeline([
    transport.input(), stt, context_aggregator.user(), llm, tts,
    transport.output(),
    roark.audio_processor,          # after transport.output() — L=user, R=bot
    context_aggregator.assistant(),
])

task = PipelineTask(pipeline, params=PipelineParams(observers=[roark]))
```

### Power-user: bring your own `AudioBufferProcessor`

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

`record_audio=True` and `audio_buffer_processor=` are mutually exclusive — pass
one or the other.

### What the observer does

1. On pipeline start, POSTs `call-started` and starts recording (if enabled).
   The agent is lazy-registered on Roark the first time it sees this `agent_id`.
2. Captures transcripts during the call:
     * **User turns** from `TranscriptionFrame` (final only — interim
       transcriptions are ignored).
     * **Assistant turns** by aggregating `TTSTextFrame` chunks between
       `BotStoppedSpeakingFrame` / `InterruptionFrame` boundaries.
3. Captures tool calls from `FunctionCallInProgressFrame` /
   `FunctionCallResultFrame`. Each is shipped as a `tool_call` / `tool_result`
   record discriminated by `kind`; Roark pairs them by `toolCallId`.
4. Streams pre-mixed stereo PCM chunks (emitted by `AudioBufferProcessor`) to S3
   via presigned URLs fetched from `POST /v1/pipecat/chunk-upload-url`.
5. On `EndFrame` / `CancelFrame` / `StopFrame` (or `aflush()` on transport
   disconnect), flushes any in-flight assistant turn, drains in-flight uploads,
   and POSTs `call-ended` with the transcript, tool calls, and PCM format
   metadata. Roark's call-ended Lambda concatenates the chunks and wraps the
   result in a WAV header.

Transcripts and tool calls are forwarded in Pipecat's native shape — Roark
maps them to its internal schema on its side.

Failures are logged and swallowed — the observer never raises into the pipeline.

## Telephony

When you wire a telephony serializer (Twilio / Telnyx / Plivo / SIP), pass the numbers:

```python
RoarkObserver(
    api_key="rk_live_...",
    agent_id="support-bot-v3",
    agent_phone_number="+15551234567",
    customer_phone_number="+15559876543",
    call_direction="INBOUND",
    interface_type="PHONE",
    audio_buffer_processor=audio_buffer,
)
```

## WebRTC transports

Pipecat's WebRTC transports (notably `SmallWebRTC`) sometimes tear down without
pushing `EndFrame` through observers. Call `aflush()` from the disconnect
handler to guarantee `call-ended` is POSTed:

```python
@transport.event_handler("on_client_disconnected")
async def _on_disconnect(_, __):
    await roark_observer.aflush(reason="client-disconnected")
```

`aflush()` is idempotent; the regular `EndFrame` path will no-op on the next call.

## Troubleshooting

**Do I need `enable_tracing=True` on `PipelineTask`?** No. `RoarkObserver`
captures raw frames — it does not consume OpenTelemetry spans. The tracing flag
is unrelated.

**`call-ended` never POSTs.** Some transports (notably `SmallWebRTC`) tear down
without pushing `EndFrame` through observers. Wire `aflush()` into your
disconnect handler — see [WebRTC transports](#webrtc-transports).

**Audio recording captures user audio only / bot audio only.** The
`AudioBufferProcessor` must sit **after `transport.output()`** so it sees the
bot's audio post-TTS. If it's placed earlier in the pipeline, the bot channel
will be silent.

**Transcripts arrive empty.** The observer warns
`call-ended with empty transcript ... no TranscriptionFrame or TTSTextFrame was
observed during the call` when nothing was captured. Usually this means the STT
service isn't emitting finalized `TranscriptionFrame`s, or the pipeline ended
before any speech was processed.

## Configuration reference

| Parameter | Type | Default | Notes |
|-----------|------|---------|-------|
| `api_key` | `str` | — | Required. Roark API key. |
| `agent_id` | `str` | — | Required. Customer-stable agent identifier. |
| `agent_name` | `str \| None` | `None` | Display name. |
| `agent_prompt` | `str \| None` | `None` | System prompt. Persisted as the agent's prompt revision. |
| `agent_phone_number` | `str \| None` | `None` | E.164. |
| `customer_phone_number` | `str \| None` | `None` | E.164. |
| `call_direction` | `'INBOUND' \| 'OUTBOUND' \| None` | inferred | |
| `interface_type` | `'WEB' \| 'PHONE' \| None` | inferred from phone numbers | |
| `roark_webhook_url` | `str \| None` | `$ROARK_WEBHOOK_URL` (required) | |
| `roark_chunk_upload_url_endpoint` | `str \| None` | `$ROARK_CHUNK_UPLOAD_URL_ENDPOINT` (required) | |
| `sampling_rate` | `float \| None` | `None` | Per-call sampling rate. Accepts `0..1` or `0..100`. |
| `record_audio` | `bool` | `False` | Create a default `AudioBufferProcessor` (stereo, ~256 KB chunks; sample rate adopted from the pipeline's `StartFrame` so it tracks the provider) accessible via `observer.audio_processor`. Mutually exclusive with `audio_buffer_processor`. |
| `audio_buffer_processor` | `AudioBufferProcessor \| None` | `None` | Power-user override: pass your own `AudioBufferProcessor` instance to control sample rate / channels / buffer size. |
| `pipecat_call_id` | `str \| None` | random UUID | Stable call identifier. |

## Development

```bash
uv sync --all-extras
uv run pytest
uv run ruff check .
```

## License

MIT — see [LICENSE](./LICENSE).
