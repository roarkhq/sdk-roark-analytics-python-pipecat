# Changelog

All notable changes to `pipecat-roark` are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Each transcript turn now carries a real end edge** — `endTimestamp` (ISO
  8601 UTC) and `endAudioOffsetMs` (ms from the recording's first audio frame).
  The end is anchored to the speech-offset VAD frame
  (`UserStoppedSpeakingFrame` for the user, `BotStoppedSpeakingFrame` — or the
  `InterruptionFrame` that cut the bot off — for the assistant). Previously a
  turn shipped only its start (`timestamp` / `audioOffsetMs`), forcing consumers
  to assume a turn ends where the next begins; that collapsed the inter-turn
  silence and misplaced markers on the post-call player. When no stop frame
  arrives before the turn is flushed (e.g. the pipeline ends mid-turn),
  `endTimestamp` falls back to the flush moment and is never omitted;
  `endAudioOffsetMs` is omitted only when recording isn't anchored, exactly as
  `audioOffsetMs` already is.

## [0.1.3] - 2026-05-28

### Fixed

- **User turns are now captured from every STT, not just those that set
  `frame.finalized`.** User transcripts are aggregated from `TranscriptionFrame`
  segments and flushed at the turn boundary (`BotStartedSpeakingFrame`, the next
  `UserStartedSpeakingFrame`, or `EndFrame` / `CancelFrame`) instead of being
  committed per frame gated on `frame.finalized`. Streaming STTs (Deepgram,
  OpenAI realtime, Speechmatics) leave `finalized` `False` on ordinary final
  results, so the previous gate silently dropped most user turns. Every
  `TranscriptionFrame` is a final result — interims are the separate
  `InterimTranscriptionFrame` class — so segments are now accumulated
  unconditionally and joined into one turn. Multi-segment utterances are merged;
  back-to-back user turns with no bot reply between them are flushed separately.
- **A pre-armed bring-your-own `AudioBufferProcessor` is no longer reset on
  `on_pipeline_started`.** When a caller passes a processor they have already
  started recording on (e.g. to also drive their own chunk upload), the observer
  previously called `start_recording()` again from its lagging
  `on_pipeline_started` callback — which calls `_reset_recording()`, wiping any
  audio captured before the callback ran (typically the bot greeting) and
  de-syncing the `audioOffsetMs` anchor from the recording's sample 0, so
  transcript markers drifted. The observer now re-arms only if the processor is
  not already recording, and tracks the offset anchor from the first observed
  audio frame so timestamps stay aligned regardless of who armed recording.

### Changed

- Repository moved to
  [`roarkhq/sdk-roark-analytics-python-pipecat`](https://github.com/roarkhq/sdk-roark-analytics-python-pipecat)
  (the PyPI package and import remain `pipecat-roark` / `pipecat_roark`). Package
  metadata URLs updated accordingly; the old URLs redirect.

## [0.1.2] - 2026-05-26

### Fixed

- **The bot's opening greeting is no longer lost from the recording.** The
  default `AudioBufferProcessor` now arms recording *inline* on the pipeline's
  `StartFrame` instead of from the observer's `on_pipeline_started` callback.
  Observer callbacks are delivered on a lagging per-observer queue, so a bot
  that speaks first could push its greeting audio through the inline processor
  before the queued `start_recording()` ran — and `AudioBufferProcessor`
  silently drops every frame until recording is armed. Arming on the
  `StartFrame` is deterministic (Pipecat pushes queued frames, the greeting
  included, only after the `StartFrame` has traversed the pipeline) and works on
  every supported Pipecat version. The 0.1.1 change mis-attributed this to
  webhook latency; the real cause was the observer dispatch queue. A
  bring-your-own `AudioBufferProcessor` is still armed from
  `on_pipeline_started` (best-effort).
- `pipecat_roark.__version__` is now read from the installed package metadata
  instead of a hand-maintained literal, which had drifted to `0.1.0` in the
  0.1.1 release and made it impossible to tell which version was installed.

## [0.1.1] - 2026-05-25

### Added

- Turns are now anchored to speech onset: each turn is timestamped at its
  speech-onset VAD frame (`UserStartedSpeakingFrame` /
  `BotStartedSpeakingFrame`) instead of the STT-finalize / TTS-text edge.
- `audioOffsetMs` on each turn, measured from the recording's first audio
  frame (WAV sample 0), so dashboards place speaker markers on the
  recording's own sample timeline rather than wall clock. The offset anchor
  is deferred to the first observed audio frame rather than
  `start_recording()`, avoiding the dead-time inflation that made the first
  seconds of merged audio appear missing.

### Changed

- **BREAKING:** Roark service endpoints are now built into the client. The
  `ROARK_WEBHOOK_URL` and `ROARK_CHUNK_UPLOAD_URL_ENDPOINT` env vars are no
  longer read or required — `ROARK_API_KEY` (or `api_key=`) is the only Roark
  configuration. Remove those two vars from your environment / deployment
  secrets; they are now ignored.

## [0.1.0] - 2026-05-22

Initial public release. Drop-in `RoarkObserver` for Pipecat that ships call
lifecycle, transcripts, tool calls, and audio recordings to Roark. Compatible
with `pipecat-ai >= 0.0.40, < 1`; tested with `pipecat-ai` 0.0.108. Prepared
for submission to the
[Pipecat community-integrations](https://github.com/pipecat-ai/pipecat/blob/main/COMMUNITY_INTEGRATIONS.md)
listing.

### Added

- `RoarkObserver` (`BaseObserver` subclass) capturing call lifecycle from
  native Pipecat frames — `TranscriptionFrame`, `TTSTextFrame`,
  `BotStoppedSpeakingFrame`, `InterruptionFrame`, `FunctionCallInProgressFrame`,
  `FunctionCallResultFrame`, `EndFrame`, `CancelFrame`, `StopFrame`.
- Default `AudioBufferProcessor` auto-created with stereo channels and
  ~256 KB chunks. Sample rate is adopted from the pipeline's `StartFrame` so
  it tracks whatever the transport negotiated (8 kHz Twilio/Telnyx,
  16/24/48 kHz Daily/LiveKit). Exposed as `observer.audio_processor` for
  power users; pass `audio_buffer_processor=` to override.
- Chunked audio upload via presigned S3 URLs requested per chunk from the
  Roark chunk-upload endpoint; in-flight uploads are drained before
  `call-ended` is posted.
- Frame deduplication by frame id (Pipecat invokes `on_push_frame` once per
  processor-to-processor hop; without dedupe the same transcription would
  repeat N times).
- `aflush(reason=...)` idempotent escape hatch for WebRTC transports that
  tear down without pushing `EndFrame` (notably `SmallWebRTC`).
- OpenTelemetry correlation via shared `pipecat_call_id` ↔
  `PipelineTask.conversation_id`.
- `examples/basic_observer.py` — minimal transport-agnostic wiring sketch.
- `examples/bot.py` — runnable foundational voice assistant
  (Deepgram STT → OpenAI LLM → Cartesia TTS) using the canonical Pipecat
  0.0.108 runner pattern (`LLMContext` + `LLMContextAggregatorPair`,
  `create_transport`, `on_client_connected` / `on_client_disconnected`).
  Same file runs self-hosted (`--transport webrtc` / `--transport daily`)
  and deploys to Pipecat Cloud unchanged.

### Configuration

- `ROARK_WEBHOOK_URL` and `ROARK_CHUNK_UPLOAD_URL_ENDPOINT` env vars are
  required at construction time — read directly from the environment, no
  kwarg overrides.
- `api_key`, `agent_id` are required; `agent_name`, `agent_prompt`,
  `pipecat_call_id`, `audio_buffer_processor` are optional.

[Unreleased]: https://github.com/roarkhq/sdk-roark-analytics-python-pipecat/compare/v0.1.3...HEAD
[0.1.3]: https://github.com/roarkhq/sdk-roark-analytics-python-pipecat/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/roarkhq/sdk-roark-analytics-python-pipecat/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/roarkhq/sdk-roark-analytics-python-pipecat/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/roarkhq/sdk-roark-analytics-python-pipecat/releases/tag/v0.1.0
