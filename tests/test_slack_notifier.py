"""Offline tests for Slack's outbox-projection role."""

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from unittest.mock import MagicMock

from coordinator import slack_notifier


def test_slack_delivery_migration_is_durable_and_idempotent():
    content = (Path(__file__).parent.parent / "coordinator" / "migrations" / "005_slack_notification_outbox.sql").read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS slack_notification_deliveries" in content
    assert "event_id UUID PRIMARY KEY REFERENCES coordinator_outbox" in content
    assert "CREATE TABLE IF NOT EXISTS slack_notification_attempts" in content
    assert "UNIQUE(event_id, attempt_number)" in content


def test_hackathon_notifiable_events_filter():
    """Verify only the 3 hackathon event categories are processed."""
    assert "CONTRACT_CHANGED" in slack_notifier.NOTIFIABLE_EVENTS
    assert "BREAKING_CONTRACT_PUBLISHED" in slack_notifier.NOTIFIABLE_EVENTS
    assert "ENDPOINT_RETIRED" in slack_notifier.NOTIFIABLE_EVENTS
    assert "COMPATIBILITY_WORK_CREATED" in slack_notifier.NOTIFIABLE_EVENTS
    assert "DRIFT_DETECTED" in slack_notifier.NOTIFIABLE_EVENTS
    assert "REPLAN_REQUIRED" in slack_notifier.NOTIFIABLE_EVENTS
    assert "COMPATIBILITY_BLOCKED" in slack_notifier.NOTIFIABLE_EVENTS
    assert "COMPATIBILITY_INCOMPATIBLE" in slack_notifier.NOTIFIABLE_EVENTS


def test_blocked_notification_is_concise_sanitized_and_actionable():
    """Verify Category 3: Compatibility blocked notification displays all required metadata."""
    message = slack_notifier.format_slack_message({
        "event_type": "COMPATIBILITY_BLOCKED",
        "source_service": "orders-service",
        "payload": {
            "target_service": "orders-service",
            "source_service": "billing-service",
            "source_contract_revision": 2,
            "reason_code": "UNAVAILABLE_REQUIRED_INPUT",
            "unavailable_required_input": "customer_id",
            "breaking_change": "customer_id is now required",
            "sources_checked": ["models/order.py", "clients/guest_checkout.py"],
            "worktree_path": "worktrees/task-orders-guest-checkout",
            "source_commit": "c0ffee12345",
            "requested_resolution": "Make customer_id optional for guest checkouts",
            # Sensitive fields that must be scrubbed:
            "source_code": "def secret(): pass",
            "prompt": "private prompt",
            "thought": "internal model thought",
            "customer_email": "alice@example.com",
            "api_key": "sk-live-123456",
        },
    })
    rendered = message["blocks"][1]["text"]["text"]
    assert "🚨 Compatibility Blocked" in message["text"]
    assert "orders-service" in rendered
    assert "billing-service" in rendered
    assert "UNAVAILABLE_REQUIRED_INPUT" in rendered
    assert "customer_id" in rendered
    assert "worktrees/task-orders-guest-checkout" in rendered
    assert "c0ffee12" in rendered
    assert "Human decision required" in rendered

    # Verify sensitive data is NOT present in text or blocks
    full_output = json.dumps(message)
    assert "def secret()" not in full_output
    assert "private prompt" not in full_output
    assert "internal model thought" not in full_output
    assert "alice@example.com" not in full_output
    assert "sk-live-123456" not in full_output


def test_breaking_contract_published_notification():
    """Verify Category 1: Breaking contract published notification."""
    message = slack_notifier.format_slack_message({
        "event_type": "CONTRACT_CHANGED",
        "source_service": "billing-service",
        "payload": {
            "service_name": "billing-service",
            "revision_number": 2,
            "schema_diff": {
                "diff_summary": "Removed card_token, added payment_method_id",
                "migration_note": "Migrate all callers to pass payment_method_id",
            },
        },
    })
    rendered = message["blocks"][1]["text"]["text"]
    assert "💥 Breaking Contract Published" in message["text"]
    assert "billing-service" in rendered
    assert "v2" in rendered
    assert "Removed card_token, added payment_method_id" in rendered
    assert "Migrate all callers to pass payment_method_id" in rendered


def test_compatibility_work_created_notification():
    """Verify Category 2: Compatibility work created / replan required notification."""
    message = slack_notifier.format_slack_message({
        "event_type": "COMPATIBILITY_WORK_CREATED",
        "source_service": "coordinator",
        "payload": {
            "source_service": "billing-service",
            "target_service": "orders-service",
            "old_contract_revision": 1,
            "source_contract_revision": 2,
            "breaking_diff": {
                "diff_summary": "Required payment_method_id parameter",
            },
        },
    })
    rendered = message["blocks"][1]["text"]["text"]
    assert "⚡ Compatibility Work Created (Replan Required)" in message["text"]
    assert "orders-service" in rendered
    assert "billing-service" in rendered
    assert "v1 → v2" in rendered


@pytest.mark.asyncio
async def test_slack_claim_avoids_nullable_outer_join_lock(monkeypatch):
    """CockroachDB-compatible claim reads the outbox and delivery rows separately."""
    executed = []
    cursor = AsyncMock()

    async def execute(sql, params=None):
        executed.append((sql, params))

    cursor.execute = execute
    cursor.fetchone = AsyncMock(side_effect=[
        {
            "event_id": "evt-claim-1",
            "event_type": "CONTRACT_CHANGED",
            "source_service": "billing-service",
            "payload": {"schema_diff": {"is_breaking": True}},
            "created_at": None,
        },
        None,
    ])
    conn = MagicMock()
    cursor_context = AsyncMock()
    cursor_context.__aenter__.return_value = cursor
    cursor_context.__aexit__.return_value = None
    conn.cursor.return_value = cursor_context

    async def run_transaction(fn):
        return await fn(conn)

    monkeypatch.setattr(slack_notifier, "run_transaction", run_transaction)
    result = await slack_notifier.claim_next_notification()

    assert result["event_id"] == "evt-claim-1"
    assert result["attempt_number"] == 1
    claim_sql = executed[0][0]
    assert "LEFT JOIN slack_notification_deliveries" not in claim_sql
    assert "FOR UPDATE SKIP LOCKED" in claim_sql
    assert "NOT EXISTS" in claim_sql
    assert "FROM slack_notification_deliveries" in executed[1][0]


@pytest.mark.asyncio
async def test_disabled_slack_never_claims_or_delivers(monkeypatch):
    monkeypatch.setattr(slack_notifier.settings, "slack_notifications_enabled", False)
    claim = AsyncMock()
    monkeypatch.setattr(slack_notifier, "claim_next_notification", claim)
    assert await slack_notifier.notify_one() is None
    claim.assert_not_awaited()


@pytest.mark.asyncio
async def test_slack_failure_is_non_blocking_and_records_attempt(monkeypatch):
    """Verify Slack HTTP failure records the failure attempt without raising exceptions."""
    monkeypatch.setattr(slack_notifier.settings, "slack_notifications_enabled", True)
    monkeypatch.setattr(slack_notifier.settings, "slack_webhook_url", "https://hooks.slack.com/services/test/mock")

    mock_event = {
        "event_id": "evt-12345",
        "event_type": "COMPATIBILITY_BLOCKED",
        "source_service": "orders-service",
        "payload": {"target_service": "orders-service", "missing_requirement": "customer_id"},
        "attempt_number": 1,
    }
    monkeypatch.setattr(slack_notifier, "claim_next_notification", AsyncMock(return_value=mock_event))
    recorded_deliveries = []
    async def mock_record(event_id, attempt, response_code=None, error=None):
        recorded_deliveries.append({"event_id": event_id, "attempt": attempt, "code": response_code, "error": error})
    monkeypatch.setattr(slack_notifier, "record_delivery", mock_record)

    with patch("httpx.AsyncClient.post", AsyncMock(side_effect=Exception("Slack network timeout"))):
        result = await slack_notifier.notify_one()
        assert result is not None
        assert result["event_id"] == "evt-12345"
        assert result["delivered"] is False
        assert len(recorded_deliveries) == 1
        assert recorded_deliveries[0]["code"] is None
        assert "Slack network timeout" in recorded_deliveries[0]["error"]
