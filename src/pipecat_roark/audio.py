"""PCM buffering + WAV serialization for the recording upload path.

We accumulate raw PCM bytes from ``OutputAudioRawFrame`` (and optionally
``InputAudioRawFrame``) during the call. At end-of-call we wrap the buffer in
a WAV header and ship it to S3 via the presigned upload URL.

A bounded byte cap (default 200MB) protects against runaway calls — the
observer must never block or crash the pipeline, so we trim instead of failing
when the cap is hit.
"""

from __future__ import annotations

import io
import wave
from threading import Lock


class PCMRecorder:
    """Thread-safe PCM accumulator that mints a WAV blob on demand.

    Pipecat audio frames are 16-bit signed little-endian PCM. ``sample_rate``
    and ``num_channels`` are fixed at first append — subsequent appends with
    different parameters are silently dropped (logged by the caller) to avoid
    splicing incompatible audio segments into a single WAV.
    """

    DEFAULT_MAX_BYTES = 200 * 1024 * 1024  # 200MB ≈ 60 min mono 16-bit @ 48kHz

    def __init__(self, max_bytes: int = DEFAULT_MAX_BYTES) -> None:
        self._max_bytes = max_bytes
        self._buf = bytearray()
        self._lock = Lock()
        self._sample_rate: int | None = None
        self._num_channels: int | None = None
        self._dropped_bytes = 0

    @property
    def is_empty(self) -> bool:
        return len(self._buf) == 0

    @property
    def dropped_bytes(self) -> int:
        return self._dropped_bytes

    def append(self, pcm: bytes, sample_rate: int, num_channels: int) -> None:
        with self._lock:
            # First chunk fixes the audio params; later mismatches are dropped.
            if self._sample_rate is None:
                self._sample_rate = sample_rate
                self._num_channels = num_channels
            elif sample_rate != self._sample_rate or num_channels != self._num_channels:
                self._dropped_bytes += len(pcm)
                return

            # Bounded cap — drop overflow rather than fail the pipeline.
            available = self._max_bytes - len(self._buf)
            if available <= 0:
                self._dropped_bytes += len(pcm)
                return
            if len(pcm) > available:
                self._buf.extend(pcm[:available])
                self._dropped_bytes += len(pcm) - available
            else:
                self._buf.extend(pcm)

    def to_wav_bytes(self) -> bytes | None:
        """Serialize the buffered PCM as a WAV file. Returns None if no audio captured."""
        with self._lock:
            if not self._buf or self._sample_rate is None or self._num_channels is None:
                return None
            out = io.BytesIO()
            with wave.open(out, "wb") as wav:
                wav.setnchannels(self._num_channels)
                wav.setsampwidth(2)  # 16-bit
                wav.setframerate(self._sample_rate)
                wav.writeframes(bytes(self._buf))
            return out.getvalue()
