"""Tests for the guarded CodeClaim demo-data reset command."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from scripts.reset_demo_data import RESETTABLE_TABLES, reset_demo_data


def test_schema_migrations_is_not_in_reset_allowlist():
    assert "schema_migrations" not in RESETTABLE_TABLES
    assert "microservices" in RESETTABLE_TABLES
    assert "coordinator_outbox" in RESETTABLE_TABLES


def test_reset_dry_run_reports_counts_without_truncating():
    cursor = AsyncMock()
    cursor.fetchone.side_effect = [
        {"database": "codeclaim_db"},
        {"count": 3},
        {"count": 1},
    ]
    cursor.fetchall.return_value = [
        {"table_name": "microservices"},
        {"table_name": "coordinator_outbox"},
    ]

    cursor_context = AsyncMock()
    cursor_context.__aenter__.return_value = cursor
    cursor_context.__aexit__.return_value = None
    connection = MagicMock()
    connection.cursor.return_value = cursor_context
    transaction_context = AsyncMock()
    transaction_context.__aenter__.return_value = None
    transaction_context.__aexit__.return_value = None
    connection.transaction.return_value = transaction_context
    pool_context = AsyncMock()
    pool_context.__aenter__.return_value = connection
    pool_context.__aexit__.return_value = None
    pool = MagicMock()
    pool.connection.return_value = pool_context

    with patch("scripts.reset_demo_data.get_pool", AsyncMock(return_value=pool)), patch(
        "scripts.reset_demo_data.close_pool", AsyncMock()
    ):
        result = asyncio.run(reset_demo_data(expected_database="codeclaim_db", apply=False))

    assert result["applied"] is False
    assert result["before"] == {"coordinator_outbox": 3, "microservices": 1}
    assert not any("DELETE FROM" in str(call.args[0]) for call in cursor.execute.call_args_list)
