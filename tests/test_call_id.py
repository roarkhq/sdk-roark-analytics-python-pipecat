"""Tests for transport-aware Pipecat call ID resolution."""

from __future__ import annotations

from types import SimpleNamespace
from uuid import UUID

from pipecat_roark import resolve_pipecat_call_id, resolve_roark_simulation_job_id


def test_cloud_session_id_takes_priority() -> None:
    runner_args = SimpleNamespace(
        session_id="639f91d8-d511-4677-a83b-bd7564d5d92f",
        webrtc_connection=SimpleNamespace(pc_id="SmallWebRTCConnection#0-other"),
    )

    assert resolve_pipecat_call_id(runner_args) == "639f91d8-d511-4677-a83b-bd7564d5d92f"


def test_small_webrtc_pc_id_is_preserved() -> None:
    runner_args = SimpleNamespace(
        webrtc_connection=SimpleNamespace(
            pc_id="SmallWebRTCConnection#0-0123456789abcdef0123456789abcdef"
        )
    )

    assert resolve_pipecat_call_id(runner_args) == (
        "SmallWebRTCConnection#0-0123456789abcdef0123456789abcdef"
    )


def test_different_small_webrtc_connections_resolve_differently() -> None:
    first = SimpleNamespace(webrtc_connection=SimpleNamespace(pc_id="connection#1"))
    second = SimpleNamespace(webrtc_connection=SimpleNamespace(pc_id="connection#2"))

    assert resolve_pipecat_call_id(first) != resolve_pipecat_call_id(second)


def test_unsupported_runner_uses_uuid_fallback() -> None:
    call_id = resolve_pipecat_call_id(SimpleNamespace())

    assert str(UUID(call_id)) == call_id


def test_empty_identifiers_use_uuid_fallback() -> None:
    runner_args = SimpleNamespace(
        session_id="",
        webrtc_connection=SimpleNamespace(pc_id=""),
    )

    call_id = resolve_pipecat_call_id(runner_args)

    assert str(UUID(call_id)) == call_id


def test_roark_envelope_does_not_replace_native_call_id() -> None:
    runner_args = {
        "body": {"_roark": {"simulationJobId": "simulation-job-id"}},
        "session_id": "cloud-session-id",
    }

    assert resolve_pipecat_call_id(runner_args) == "cloud-session-id"


def test_resolves_simulation_job_id_from_mapping_envelope() -> None:
    runner_args = {"body": {"_roark": {"simulationJobId": "simulation-job-id"}}}

    assert resolve_roark_simulation_job_id(runner_args) == "simulation-job-id"


def test_resolves_simulation_job_id_from_object_envelope() -> None:
    runner_args = SimpleNamespace(
        body=SimpleNamespace(_roark=SimpleNamespace(simulationJobId="object-job-id"))
    )

    assert resolve_roark_simulation_job_id(runner_args) == "object-job-id"


def test_missing_or_invalid_simulation_job_id_returns_none() -> None:
    cases = [
        None,
        SimpleNamespace(),
        SimpleNamespace(body=None),
        SimpleNamespace(body=["not", "an", "object"]),
        SimpleNamespace(body={"_roark": None}),
        SimpleNamespace(body={"_roark": {"simulationJobId": ""}}),
        SimpleNamespace(body={"_roark": {"simulationJobId": 42}}),
    ]

    assert all(resolve_roark_simulation_job_id(case) is None for case in cases)
