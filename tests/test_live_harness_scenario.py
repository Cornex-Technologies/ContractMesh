"""Explicit opt-in live REST scenario; never collected by the normal suite."""

import os

import pytest

from scripts.live_harness_scenario import run_live_harness_scenario


@pytest.mark.live
@pytest.mark.integration
@pytest.mark.asyncio
async def test_live_late_compatibility_rest_scenario():
    if os.environ.get("IS_DEMO_MODE", "false").lower() == "true":
        pytest.skip("Live scenario requires IS_DEMO_MODE=false")
    if not os.environ.get("COCKROACH_DATABASE_URL") or not os.environ.get("CODECLAIM_BASE_URL"):
        pytest.skip("Set CockroachDB and coordinator live-test environment before running")
    assert await run_live_harness_scenario(manual=False)
