# pipecat-roark

A [Roark](https://roark.ai) analytics observer for [Pipecat](https://github.com/pipecat-ai/pipecat).
Drop one observer into your Pipecat pipeline and Roark captures call lifecycle, transcripts, tool
calls, and recordings — no other code changes required.

## Install

```bash
pip install pipecat-roark
```

Requires Python 3.10+ and `pipecat-ai >= 0.0.40`.

## Usage

```python
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat_roark import RoarkObserver

task = PipelineTask(
    pipeline,
    params=PipelineParams(
        observers=[
            RoarkObserver(
                api_key="rk_live_...",          # from your Roark project's API keys page
                agent_id="support-bot-v3",       # stable identifier for this agent
                agent_name="Support Bot v3",
                agent_prompt=SYSTEM_PROMPT,      # captured as the agent's prompt revision
            ),
        ],
    ),
)
```

That's it. The observer:

1. POSTs `call-started` when the pipeline starts. The agent is lazy-registered on the Roark
   side the first time we see this `agent_id`.
2. Buffers transcripts, tool calls, and (optionally) audio during the call.
3. On `EndFrame` / `CancelFrame`, uploads the WAV to Roark via a presigned S3 URL and POSTs
   `call-ended` with the batched data.

The observer never raises into the pipeline. Network failures are logged and swallowed; the
worst case is a call that lands in Roark with partial data.

## What gets captured

| Pipecat frame                       | Roark field                          |
|-------------------------------------|--------------------------------------|
| `TranscriptionFrame` (finalized)    | `transcript[].text`                  |
| `FunctionCallInProgressFrame`       | `toolCallMessages[role=tool_call_invocation]` |
| `FunctionCallResultFrame`           | `toolCallMessages[role=tool_call_result]`     |
| `OutputAudioRawFrame` / `InputAudioRawFrame` | WAV upload → `recordingS3Key`     |
| `EndFrame` / `CancelFrame`          | triggers end-of-call flush           |

## Telephony

When you wire a telephony serializer (Twilio / Telnyx / Plivo / SIP), pass the phone numbers to
the observer so they appear on the call in Roark:

```python
RoarkObserver(
    api_key="rk_live_...",
    agent_id="support-bot-v3",
    agent_phone_number="+15551234567",
    customer_phone_number="+15559876543",
    call_direction="INBOUND",
    interface_type="PHONE",
)
```

## Disabling audio capture

If you handle recording yourself or simply don't want it, pass `record_audio=False`. The call
still lands in Roark with transcripts and tool calls.

```python
RoarkObserver(api_key="rk_live_...", agent_id="support-bot-v3", record_audio=False)
```

## Configuration reference

| Parameter | Type | Default | Notes |
|-----------|------|---------|-------|
| `api_key` | str | — | Required. Roark API key. |
| `agent_id` | str | — | Required. Customer-stable agent identifier. |
| `agent_name` | str \| None | `None` | Display name; falls back to `agent_id`. |
| `agent_prompt` | str \| None | `None` | System prompt. Persisted as the agent's prompt revision. |
| `agent_phone_number` | str \| None | `None` | E.164. |
| `customer_phone_number` | str \| None | `None` | E.164. |
| `call_direction` | `'INBOUND'` \| `'OUTBOUND'` \| None | inferred | |
| `interface_type` | `'WEB'` \| `'PHONE'` \| None | inferred from phone numbers | |
| `roark_webhook_url` | str \| None | Roark Lambda URL | Override the Pipecat webhook endpoint (call-started / call-ended). |
| `roark_upload_url_endpoint` | str \| None | Roark Lambda URL | Override the recording-upload-URL endpoint. |
| `record_audio` | bool | `True` | Buffer PCM and upload WAV at end-of-call. |
| `pipecat_call_id` | str \| None | random UUID | Stable call identifier; useful for idempotency. |

## Status

Public beta. Standalone package today; we plan to upstream the same observer as
`pipecat.observers.roark` in core Pipecat once the contract stabilises.

## License

MIT — see [LICENSE](./LICENSE).
