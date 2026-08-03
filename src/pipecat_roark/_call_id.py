"""Resolve Pipecat call and Roark simulation identifiers from runner arguments."""

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


def resolve_pipecat_call_id(runner_args: object | None) -> str:
    """Return one call identifier for a Pipecat runner session.

    Pipecat Cloud's session identifier is preferred because it is shared with
    the caller that started the cloud session. SmallWebRTC runners instead
    expose a peer-connection identifier, which is returned unchanged. Runner
    types without either native identifier receive a random UUID, matching
    ``RoarkObserver``'s default behavior.

    Args:
        runner_args: Any Pipecat runner-arguments object or mapping. The
            function avoids concrete runner classes so it remains compatible
            across supported Pipecat versions.

    Returns:
        A non-empty identifier suitable for ``RoarkObserver.pipecat_call_id``.
    """

    session_id = _read_non_empty_string(runner_args, "session_id")
    if session_id:
        return session_id

    connection = _read_value(runner_args, "webrtc_connection")
    pc_id = _read_non_empty_string(connection, "pc_id")
    if pc_id:
        return pc_id

    return str(uuid.uuid4())


def resolve_roark_simulation_job_id(runner_args: object | None) -> str | None:
    """Return the Roark simulation job ID carried in runner session data.

    Roark simulation initiators place this value in the reserved
    ``body["_roark"]["simulationJobId"]`` envelope. It is separate from the
    Pipecat-native call identifier and is forwarded as lifecycle metadata.

    Args:
        runner_args: Any Pipecat runner-arguments object or mapping. Nested
            ``body`` and ``_roark`` values may also be mappings or objects.

    Returns:
        The non-empty simulation job ID when supplied, otherwise ``None``.
    """

    body = _read_value(runner_args, "body")
    roark_envelope = _read_value(body, "_roark")
    return _read_non_empty_string(roark_envelope, "simulationJobId")
