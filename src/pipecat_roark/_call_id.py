"""Resolve a Pipecat call identifier from runner arguments."""

from __future__ import annotations

import uuid
from collections.abc import Mapping


def _read_value(value: object | None, key: str) -> object | None:
    if value is None:
        return None

    if isinstance(value, Mapping):
        return value.get(key)

    return getattr(value, key, None)


def _read_non_empty_string(value: object | None, key: str) -> str | None:
    candidate = _read_value(value, key)
    return candidate if isinstance(candidate, str) and candidate else None


def _resolve_pipecat_call_id(runner_args: object) -> str:
    """Return one call identifier for a Pipecat runner session.

    SmallWebRTC's peer-connection identifier takes priority because it names
    the connection carrying the media. Pipecat Cloud's session identifier is
    used when no peer connection is present. Other runner types receive a
    random UUID that is stable for the lifetime of one observer.

    Args:
        runner_args: Any Pipecat runner-arguments object or mapping. Concrete
            runner classes are deliberately avoided because Pipecat Cloud and
            the open-source runner publish different argument types.

    Returns:
        A non-empty identifier for the observer's call lifecycle.
    """

    connection = _read_value(runner_args, "webrtc_connection")
    pc_id = _read_non_empty_string(connection, "pc_id")
    if pc_id:
        return pc_id

    session_id = _read_non_empty_string(runner_args, "session_id")
    if session_id:
        return session_id

    return str(uuid.uuid4())
