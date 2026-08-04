"""Tests for internal transport-aware Pipecat call ID resolution."""

from __future__ import annotations

from types import SimpleNamespace
from uuid import UUID

from pipecat_roark._call_id import _resolve_pipecat_call_id


def test_small_webrtc_pc_id_takes_priority_over_cloud_session_id() -> None:
    runner_args = SimpleNamespace(
        session_id="639f91d8-d511-4677-a83b-bd7564d5d92f",
        webrtc_connection=SimpleNamespace(
            pc_id="SmallWebRTCConnection#0-0123456789abcdef0123456789abcdef"
        ),
    )

    assert _resolve_pipecat_call_id(runner_args) == (
        "SmallWebRTCConnection#0-0123456789abcdef0123456789abcdef"
    )


def test_cloud_session_id_is_preserved_without_webrtc_connection() -> None:
    runner_args = SimpleNamespace(session_id="639f91d8-d511-4677-a83b-bd7564d5d92f")

    assert _resolve_pipecat_call_id(runner_args) == "639f91d8-d511-4677-a83b-bd7564d5d92f"


def test_mapping_runner_arguments_are_supported() -> None:
    runner_args = {
        "session_id": "cloud-session-id",
        "webrtc_connection": {"pc_id": "connection#1"},
    }

    assert _resolve_pipecat_call_id(runner_args) == "connection#1"


def test_different_small_webrtc_connections_resolve_differently() -> None:
    first = SimpleNamespace(webrtc_connection=SimpleNamespace(pc_id="connection#1"))
    second = SimpleNamespace(webrtc_connection=SimpleNamespace(pc_id="connection#2"))

    assert _resolve_pipecat_call_id(first) != _resolve_pipecat_call_id(second)


def test_unsupported_runner_uses_uuid_fallback() -> None:
    call_id = _resolve_pipecat_call_id(SimpleNamespace())

    assert str(UUID(call_id)) == call_id


def test_empty_identifiers_use_uuid_fallback() -> None:
    runner_args = SimpleNamespace(
        session_id="",
        webrtc_connection=SimpleNamespace(pc_id=""),
    )

    call_id = _resolve_pipecat_call_id(runner_args)

    assert str(UUID(call_id)) == call_id
