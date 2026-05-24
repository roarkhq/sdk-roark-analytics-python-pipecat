# Changelog

All notable changes to `pipecat-roark` are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/roarkhq/pipecat-roark/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/roarkhq/pipecat-roark/releases/tag/v0.1.0
