"""Self-arming stereo recorder for the Roark recording.

This is Pipecat's stock :class:`AudioBufferProcessor` with exactly one change:
it **arms recording inline on the ``StartFrame``** instead of waiting for an
observer's ``on_pipeline_started`` callback. Everything else — resampling,
the ``on_audio_data`` / ``on_track_audio_data`` events, and crucially the
silence handling — is inherited unchanged.

Why we do NOT touch the silence algorithm
------------------------------------------

The stock processor keeps the two channels aligned by clocking silence off the
**continuous input stream**: every time audio arrives on one channel it pads
the *other* channel up to the same byte position (``_sync_buffer_to_position``),
skipping the pad while a channel is actively speaking so an utterance is never
spliced mid-word. Because the transport delivers mic frames continuously and the
output transport paces TTS at real time, both channels advance at the real-time
rate and genuine inter-turn pauses are filled with silence on whichever channel
is quiet. This is the same mechanism ``app-agent-service`` relies on, and it is
correct.

An earlier version of this module replaced that with a monotonic **wall clock**:
each channel was padded to ``elapsed * sample_rate`` and a frame that arrived
"ahead" of the clock (bursty TTS) was appended as-is to avoid overlap. That is
broken for the bot channel — a TTS burst pushes the bot write-head permanently
*ahead* of the wall clock, so ``gap = elapsed - write_head`` stays ≤ 0 and the
real silence between bot turns is **never** filled. The bot's turns glue
back-to-back, the recording ends up shorter than wall-clock time, and every
``audioOffsetMs`` the observer computes (from a real wall clock) drifts past
where the speech actually sits in the audio. Delegating to the stock
cross-channel sync fixes both the recording and the transcript alignment.

Inline arming
-------------

``AudioBufferProcessor`` drops every audio frame until ``start_recording()``
flips ``_recording`` on. The obvious trigger — an observer's
``on_pipeline_started`` — runs on a *lagging* per-observer queue, so a bot that
speaks first can reach this processor before the queued ``start_recording()`` is
drained and its greeting is discarded (the "first couple of seconds missing"
bug). Arming on the ``StartFrame`` as it passes through this processor is
deterministic: Pipecat pushes queued audio only *after* the ``StartFrame`` has
propagated. It also works on every Pipecat version, unlike ``on_pipeline_started``
which older releases never delivered to observers.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pipecat.processors.audio.audio_buffer_processor import AudioBufferProcessor

if TYPE_CHECKING:  # pragma: no cover
    from pipecat.frames.frames import Frame
    from pipecat.processors.frame_processor import FrameDirection

log = logging.getLogger("pipecat_roark.recorder")


class InlineArmingAudioBufferProcessor(AudioBufferProcessor):
    """``AudioBufferProcessor`` that arms recording inline on the ``StartFrame``.

    Drop-in for the stock processor on the recording path: same constructor
    surface (``sample_rate`` / ``num_channels`` / ``buffer_size``), same
    ``on_audio_data`` event, same stock silence handling (cross-channel sync).
    The only behavioural change is that ``start_recording()`` fires the instant
    the ``StartFrame`` is handled, so a bot that speaks first is captured from
    sample 0. See the module docstring for the full rationale.
    """

    async def process_frame(self, frame: Frame, direction: FrameDirection) -> None:
        """Arm recording inline the instant the ``StartFrame`` is handled."""
        from pipecat.frames.frames import StartFrame

        await super().process_frame(frame, direction)
        if isinstance(frame, StartFrame) and not self._recording:
            await self.start_recording()


__all__ = ["InlineArmingAudioBufferProcessor"]
