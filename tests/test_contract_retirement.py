"""Regression tests for explicit endpoint retirement and inventory reconciliation."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from coordinator.contract_registry import publish_contract_inventory, retire_contract
from coordinator.drift_worker import process_claimed_event


def _mock_connection(cursor: AsyncMock) -> MagicMock:
    conn = MagicMock()
    cursor_context = AsyncMock()
    cursor_context.__aenter__.return_value = cursor
    cursor_context.__aexit__.return_value = None
    conn.cursor.return_value = cursor_context
    return conn


def test_retirement_migration_has_lifecycle_tombstones_and_inventory_ledger():
    migration = (Path(__file__).parent.parent / "coordinator" / "migrations" / "006_contract_retirement_inventory.sql").read_text(encoding="utf-8")
    assert "lifecycle_state" in migration
    assert "CREATE TABLE IF NOT EXISTS contract_retirements" in migration
    assert "CREATE TABLE IF NOT EXISTS contract_inventory_publications" in migration
    assert "CREATE TABLE IF NOT EXISTS contract_inventory_findings" in migration


@pytest.mark.asyncio
async def test_retire_contract_writes_tombstone_outbox_and_audit_in_one_transaction():
    cursor = AsyncMock()
    executed: list[tuple[str, object]] = []

    async def execute(sql, params=None):
        executed.append((sql, params))

    cursor.execute = execute
    cursor.fetchone.side_effect = [
        {"contract_id": "contract-1", "lifecycle_state": "ACTIVE"},
        {"revision": 4},
        {"contract_revision_id": "revision-5"},
        {"retirement_id": "retirement-1"},
        {"event_id": "event-1"},
    ]
    conn = _mock_connection(cursor)

    async def run_in_mock_transaction(fn):
        return await fn(conn)

    with patch("coordinator.contract_registry.run_transaction", side_effect=run_in_mock_transaction), \
         patch("coordinator.compatibility.create_compatibility_work_for_contract_change", new=AsyncMock()):
        result = await retire_contract(
            service_name="billing-service", endpoint_path="/v1/charges", http_method="POST",
            source_commit="commit-5", migration_note="Use POST /v2/payments instead", retired_by="billing-agent",
        )

    assert result == {
        "contract_id": "contract-1", "retirement_id": "retirement-1",
        "retirement_revision": 5, "outbox_event_id": "event-1",
    }
    sql = " ".join(statement for statement, _ in executed)
    assert "UPDATE service_contract_revisions SET is_active=false" in sql
    assert "x-codeclaim-retired" in str([params for _, params in executed])
    assert "UPDATE service_contracts SET lifecycle_state='RETIRED'" in sql
    assert "INSERT INTO contract_retirements" in sql
    assert "ENDPOINT_RETIRED" in sql
    assert "contract_audit_history" in sql


@pytest.mark.asyncio
async def test_inventory_absence_creates_review_event_without_declaring_a_retirement():
    cursor = AsyncMock()
    executed: list[tuple[str, object]] = []

    async def execute(sql, params=None):
        executed.append((sql, params))

    cursor.execute = execute
    cursor.fetchone.side_effect = [
        None,  # no immutable publication already exists for this source commit
        {"inventory_id": "inventory-1"},
        {"finding_id": "finding-1"},
        {"event_id": "event-review-1"},
    ]
    cursor.fetchall.return_value = [
        {"contract_id": "contract-present", "contract_key": "billing-service:POST:/v1/payments", "revision": 3},
        {"contract_id": "contract-missing", "contract_key": "billing-service:GET:/v1/charges", "revision": 4},
    ]
    conn = _mock_connection(cursor)

    async def run_in_mock_transaction(fn):
        return await fn(conn)

    with patch("coordinator.contract_registry.run_transaction", side_effect=run_in_mock_transaction), \
         patch("coordinator.compatibility.create_compatibility_work_for_contract_change", new=AsyncMock()):
        result = await publish_contract_inventory(
            service_name="billing-service", source_commit="commit-6",
            contracts=[{"http_method": "POST", "endpoint_path": "/v1/payments"}],
            published_by="billing-agent",
        )

    assert result["is_idempotent_noop"] is False
    assert result["missing_active_contracts"] == [{
        "finding_id": "finding-1", "contract_id": "contract-missing", "outbox_event_id": "event-review-1",
    }]
    sql = " ".join(statement for statement, _ in executed)
    assert "contract_inventory_publications" in sql
    assert "contract_inventory_findings" in sql
    assert "ENDPOINT_RETIREMENT_REVIEW_REQUIRED" in sql
    assert "lifecycle_state='ACTIVE'" in sql


@pytest.mark.asyncio
async def test_retirement_propagates_compatibility_work_failure():
    cursor = AsyncMock()
    cursor.fetchone.side_effect = [
        {"contract_id": "contract-1", "lifecycle_state": "ACTIVE"},
        {"revision": 4},
        {"contract_revision_id": "revision-5"},
        {"retirement_id": "retirement-1"},
        {"event_id": "event-1"},
    ]
    conn = _mock_connection(cursor)

    async def run_in_mock_transaction(fn):
        return await fn(conn)

    with patch("coordinator.contract_registry.run_transaction", side_effect=run_in_mock_transaction), \
         patch("coordinator.compatibility.create_compatibility_work_for_contract_change", new=AsyncMock(side_effect=RuntimeError("work creation failed"))):
        with pytest.raises(RuntimeError, match="work creation failed"):
            await retire_contract(
                service_name="billing-service", endpoint_path="/v1/charges", http_method="POST",
                source_commit="commit-5", migration_note="Use POST /v2/payments instead", retired_by="billing-agent",
            )


@pytest.mark.asyncio
async def test_inventory_propagates_compatibility_work_failure():
    cursor = AsyncMock()
    cursor.fetchone.side_effect = [
        None,
        {"inventory_id": "inventory-1"},
        {"finding_id": "finding-1"},
        {"event_id": "event-review-1"},
    ]
    cursor.fetchall.return_value = [
        {"contract_id": "contract-missing", "contract_key": "billing-service:GET:/v1/charges", "revision": 4},
    ]
    conn = _mock_connection(cursor)

    async def run_in_mock_transaction(fn):
        return await fn(conn)

    with patch("coordinator.contract_registry.run_transaction", side_effect=run_in_mock_transaction), \
         patch("coordinator.compatibility.create_compatibility_work_for_contract_change", new=AsyncMock(side_effect=RuntimeError("work creation failed"))):
        with pytest.raises(RuntimeError, match="work creation failed"):
            await publish_contract_inventory(
                service_name="billing-service", source_commit="commit-6",
                contracts=[], published_by="billing-agent",
            )


@pytest.mark.asyncio
async def test_inventory_same_commit_is_immutable():
    cursor = AsyncMock()
    cursor.fetchone.return_value = {
        "inventory_id": "inventory-1",
        "contract_keys": ["billing-service:GET:/v1/charges"],
    }
    conn = _mock_connection(cursor)

    async def run_in_mock_transaction(fn):
        return await fn(conn)

    with patch("coordinator.contract_registry.run_transaction", side_effect=run_in_mock_transaction):
        with pytest.raises(ValueError, match="inventory for a source_commit is immutable"):
            await publish_contract_inventory(
                service_name="billing-service", source_commit="commit-6",
                contracts=[{"http_method": "POST", "endpoint_path": "/v1/payments"}],
                published_by="billing-agent",
            )


@pytest.mark.asyncio
async def test_inventory_review_replans_tasks_even_when_they_use_current_revision():
    cursor = AsyncMock()
    executed: list[tuple[str, object]] = []

    async def execute(sql, params=None):
        executed.append((sql, params))

    cursor.execute = execute
    cursor.fetchall.return_value = []
    conn = _mock_connection(cursor)
    event = {
        "event_id": "event-review-1",
        "event_type": "ENDPOINT_RETIREMENT_REVIEW_REQUIRED",
        "source_service": "billing-service",
        "outbox_payload": {
            "contract_id": "contract-missing", "revision_number": 4,
            "schema_diff": {"is_breaking": False, "classification": "REVIEW_REQUIRED"},
        },
    }

    with patch("coordinator.drift_worker.create_compatibility_work_for_contract_change", new=AsyncMock()) as create_work:
        result = await process_claimed_event(conn, event)

    assert result["status"] == "PROCESSED"
    create_work.assert_awaited_once()
    dependency_query = next(sql for sql, _ in executed if "task_contract_dependencies" in sql)
    assert "d.assumed_revision <= %s" in dependency_query
