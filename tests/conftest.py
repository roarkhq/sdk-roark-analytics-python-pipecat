"""Shared pytest fixtures.

``RoarkClient`` requires both endpoint URLs (kwarg or env var) at construction.
Tests that don't care about endpoint resolution get stub URLs via this autouse
fixture so they can build observers/clients without configuring env in every test.
"""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _stub_roark_endpoints(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ROARK_WEBHOOK_URL", "https://webhook.test/")
    monkeypatch.setenv("ROARK_CHUNK_UPLOAD_URL_ENDPOINT", "https://chunks.test/")
