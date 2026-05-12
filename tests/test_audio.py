"""Tests for the PCM → WAV recorder.

No pipecat dependency required — exercises ``audio.py`` in isolation.
"""

from __future__ import annotations

import io
import wave

from pipecat_roark.audio import PCMRecorder


def _silent_pcm(num_samples: int) -> bytes:
    """num_samples * 2 bytes of zero (16-bit mono silence)."""
    return b"\x00\x00" * num_samples


def test_empty_recorder_returns_none() -> None:
    rec = PCMRecorder()
    assert rec.is_empty
    assert rec.to_wav_bytes() is None


def test_appends_and_serializes_wav() -> None:
    rec = PCMRecorder()
    rec.append(_silent_pcm(8000), sample_rate=16000, num_channels=1)
    rec.append(_silent_pcm(8000), sample_rate=16000, num_channels=1)

    wav_bytes = rec.to_wav_bytes()
    assert wav_bytes is not None

    with wave.open(io.BytesIO(wav_bytes), "rb") as wav:
        assert wav.getnchannels() == 1
        assert wav.getsampwidth() == 2
        assert wav.getframerate() == 16000
        assert wav.getnframes() == 16000  # 8000 + 8000


def test_param_mismatch_drops_silently() -> None:
    rec = PCMRecorder()
    rec.append(_silent_pcm(4000), sample_rate=16000, num_channels=1)
    # Sample-rate change → drop, don't splice
    rec.append(_silent_pcm(4000), sample_rate=48000, num_channels=1)
    assert rec.dropped_bytes == len(_silent_pcm(4000))


def test_max_bytes_cap_clips_overflow() -> None:
    cap = 1024
    rec = PCMRecorder(max_bytes=cap)
    rec.append(b"\x00" * 600, sample_rate=16000, num_channels=1)
    rec.append(b"\x00" * 600, sample_rate=16000, num_channels=1)  # overflows by 176

    wav_bytes = rec.to_wav_bytes()
    assert wav_bytes is not None
    # Buffer was clipped to exactly `cap` bytes of payload.
    with wave.open(io.BytesIO(wav_bytes), "rb") as wav:
        assert wav.getnframes() == cap // 2  # 16-bit samples
    assert rec.dropped_bytes == 176
