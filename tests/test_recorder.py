"""Tests for InlineArmingAudioBufferProcessor.

Skip if pipecat-ai isn't installed. These tests make no network calls. They
verify the two things this subclass is responsible for:

1. It arms recording inline the moment a ``StartFrame`` is processed (so a bot
   that speaks first is captured from sample 0).
2. It does NOT override the stock silence algorithm — inter-turn pauses are
   filled by the stock cross-channel sync (driven by the continuous input
   stream), not glued back-to-back.

Audio frames are fed at the processor's own sample rate so resampling is a
pass-through and the bytes we assert on are exactly the bytes we wrote.
"""

from __future__ import annotations

import pytest

pytest.importorskip("pipecat", reason="pipecat-ai not installed in this env")

from pipecat.frames.frames import (  # noqa: E402
    InputAudioRawFrame,
    OutputAudioRawFrame,
)
from pipecat.tests.utils import run_test  # noqa: E402

from pipecat_roark._recorder import InlineArmingAudioBufferProcessor  # noqa: E402

SR = 24000  # 24 kHz: 24 samples/ms, 48 bytes/ms (int16 mono).
_BYTES_PER_SAMPLE = 2
_SAMPLE = b"\x11\x22"  # one non-zero int16 sample, to tell audio from silence.


def _make_recorder(*, buffer_size: int = 0) -> InlineArmingAudioBufferProcessor:
    rec = InlineArmingAudioBufferProcessor(
        num_channels=2, buffer_size=buffer_size, sample_rate=SR
    )
    # Set by hand for the tests that feed _process_recording directly (no live
    # pipeline / StartFrame); run_test-driven tests get this from the StartFrame.
    rec._sample_rate = SR  # noqa: SLF001
    return rec


def _audio(ms: int) -> bytes:
    """`ms` milliseconds of distinctly non-zero mono int16 audio."""
    return _SAMPLE * (SR * ms // 1000)


def _bytes_ms(ms: int) -> int:
    return SR * ms // 1000 * _BYTES_PER_SAMPLE


def _samples(buffer: bytearray) -> int:
    return len(buffer) // _BYTES_PER_SAMPLE


def _in(ms: int) -> InputAudioRawFrame:
    return InputAudioRawFrame(audio=_audio(ms), sample_rate=SR, num_channels=1)


def _out(ms: int) -> OutputAudioRawFrame:
    return OutputAudioRawFrame(audio=_audio(ms), sample_rate=SR, num_channels=1)


# ----------------------------------------------------------------- inline arming


@pytest.mark.asyncio
async def test_start_frame_arms_and_captures_opening_audio() -> None:
    """``run_test`` feeds a StartFrame then a bot frame, then an EndFrame which
    drains the recording. The StartFrame must arm recording inline (no dependency
    on a lagging ``on_pipeline_started``), so the bot's opening audio is captured
    instead of dropped — the first-seconds-missing bug stays fixed.

    We assert via ``on_track_audio_data`` (fired on the drain): if arming had
    failed, the bot frame would be dropped while ``_recording`` was False and the
    buffers would be empty, so the handler would never fire.
    """
    rec = InlineArmingAudioBufferProcessor(num_channels=2, buffer_size=0, sample_rate=SR)
    captured: list[bytes] = []

    @rec.event_handler("on_track_audio_data")
    async def _on_tracks(_proc, _user, bot, _sr, _ch) -> None:  # noqa: ANN001
        captured.append(bot)

    await run_test(
        rec,
        frames_to_send=[_out(100)],
        expected_down_frames=[OutputAudioRawFrame],
    )

    assert rec.sample_rate == SR
    # The bot's opening audio was captured on the drain, not discarded.
    assert captured, "on_track_audio_data never fired — recording was not armed"
    assert captured[-1] == _audio(100)


# ------------------------------------------------------- delegated silence sync


@pytest.mark.asyncio
async def test_inter_turn_silence_is_filled_by_stock_cross_channel_sync() -> None:
    """A continuous mic stream runs while the bot is quiet, then the bot replies.
    The stock cross-channel sync pads the bot channel with leading silence up to
    the user's position, so the bot's reply lands where it really occurred — NOT
    glued to sample 0. This is exactly what the old wall-clock mixer broke: it
    appended bot bursts back-to-back with no inter-turn silence.

    The bot's leading silence reflects the user's length *one frame before* the
    bot arrives, because the sync runs before each user frame is appended.
    """
    rec = _make_recorder()
    rec._recording = True  # noqa: SLF001

    # Continuous mic: three 100 ms frames while the bot stays silent...
    for _ in range(3):
        await rec._process_recording(_in(100))  # noqa: SLF001
    # ...then the bot replies with 100 ms.
    await rec._process_recording(_out(100))  # noqa: SLF001

    user = rec._user_audio_buffer  # noqa: SLF001
    bot = rec._bot_audio_buffer  # noqa: SLF001

    # User is its real 300 ms; bot is 200 ms leading silence + 100 ms of reply,
    # NOT 100 ms glued to sample 0.
    assert _samples(user) == SR * 300 // 1000
    assert _samples(bot) == SR * 300 // 1000
    lead = _bytes_ms(200)
    assert bytes(bot[:lead]) == b"\x00" * lead
    assert bytes(bot[lead:]) == _audio(100)


@pytest.mark.asyncio
async def test_no_wall_clock_state_is_introduced() -> None:
    """Regression guard: the recorder must NOT resurrect the old wall-clock
    fields (anchor / base_samples / clock). Silence is the stock algorithm's job.
    """
    rec = _make_recorder()
    assert not hasattr(rec, "_anchor")
    assert not hasattr(rec, "_base_samples")
    assert not hasattr(rec, "_wall_clock")
