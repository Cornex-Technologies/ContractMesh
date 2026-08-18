"""Focused tests for completed-consumer late detection and deployment gating."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from coordinator.compatibility import approve_compatibility_work, claim_next_work_item
from coordinator.deployer import ProcessSupervisor, promote_deployment


class _Cursor:
    def __init__(self, fetchone_values=None, fetchall_values=None):
        self.fetchone_values = list(fetchone_values or [])
        self.fetchall_values = list(fetchall_values or [])
        self.queries = []
        self.fetchone_calls = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    async def execute(self, query, params=None):
        self.queries.append((query, params))

    async def fetchone(self):
        self.fetchone_calls += 1
        return self.fetchone_values.pop(0) if self.fetchone_values else None

    async def fetchall(self):
        return self.fetchall_values.pop(0) if self.fetchall_values else []


class _Conn:
    def __init__(self, cursor):
        self.cursor_instance = cursor

    def cursor(self, **kwargs):
        return self.cursor_instance


@pytest.mark.asyncio
async def test_late_unassigned_work_is_claimed_and_audited():
    cursor = _Cursor(
        fetchone_values=[
            {
                "harness_id": "h-orders",
                "harness_name": "codex-orders",
                "harness_type": "codex",
                "service_name": "orders-service",
                "repository_url": "C:/repos/orders-service",
                "status": "ACTIVE",
            },
            {
                "work_item_id": "work-1",
                "source_contract_revision": 2,
                "source_contract_id": "contract-billing",
                "target_service": "orders-service",
                "target_repository": "C:/repos/orders-service",
                "harness_id": None,
                "state": "PENDING",
                "correlation_id": "corr-1",
                "payload": {
                    "source_service": "billing-service",
                    "contract_id": "contract-billing",
                    "consumer_assumed_revision": 1,
                    "interface_dependency_id": "dep-1",
                    "http_method": "POST",
                    "endpoint_path": "/v1/charges",
                },
            },
            {"harness_id": "h-orders"},
            {"task_id": "task-1", "status": "OPTIMISTIC_EXECUTING", "plan_revision": 1},
            {
                "confirmation_status": "CONFIRMED",
                "provider_service": "billing-service",
                "consumer_service": "orders-service",
                "contract_id": "contract-billing",
                "assumed_provider_revision": 1,
                "http_method": "POST",
                "endpoint_path": "/v1/charges",
            },
            {"work_item_id": "work-1"},
            {"event_id": "event-claimed"},
        ],
    )
    conn = _Conn(cursor)

    async def run_tx(fn):
        return await fn(conn)

    with patch("coordinator.compatibility.run_transaction", side_effect=run_tx):
        result = await claim_next_work_item(
            "h-orders", worktree_path="C:/worktree", base_commit="orders-sha"
        )

    assert result is not None
    assert result["assignment_mode"] == "late_unassigned"
    assert result["task"]["task_id"] == "task-1"
    sql = " ".join(query for query, _ in cursor.queries)
    assert "harness_id IS NULL" in sql
    assert "FOR UPDATE SKIP LOCKED" in sql
    assert "COMPATIBILITY_WORK_CLAIMED" in sql
    assert "contract_audit_history" in sql


@pytest.mark.asyncio
async def test_approval_rebinds_confirmed_dependency_before_verified_state():
    cursor = _Cursor(
        fetchone_values=[
            {
                "work_item_id": "work-1",
                "target_service": "orders-service",
                "source_contract_id": "contract-billing",
                "source_contract_revision": 2,
                "task_id": "task-2",
                "state": "AWAITING_APPROVAL",
                "payload": {
                    "source_service": "billing-service",
                    "interface_dependency_id": "dep-1",
                },
            },
            {
                "provider_service": "billing-service",
                "consumer_service": "orders-service",
                "http_method": "POST",
                "endpoint_path": "/v1/charges",
                "path_parameters": {},
                "query_parameters": {},
                "declared_headers": {},
                "request_body_schema": {},
                "response_schemas": {"200": {}},
                "consumer_repository": "C:/repos/orders-service",
                "consumer_source_file": "clients/billing_client.py",
                "consumer_source_evidence": {"source_commit": "orders-sha", "content_sha256": "hash"},
            },
            {"dependency_id": "dep-2"},
            {"state": "VERIFIED"},
            {"event_id": "event-compatibility-verified"},
            {"event_id": "event-plan-approved"},
        ],
    )
    conn = _Conn(cursor)

    async def run_tx(fn):
        return await fn(conn)

    with patch("coordinator.compatibility.run_transaction", side_effect=run_tx):
        result = await approve_compatibility_work("work-1", "operator")

    assert result["state"] == "VERIFIED"
    assert result["dependency_rebound"] is True
    assert result["dependency_id"] == "dep-2"
    assert result["rebound_contract_revision"] == 2
    assert result["plan_approval_outbox_event_id"] == "event-plan-approved"
    sql = " ".join(query for query, _ in cursor.queries)
    assert "INSERT INTO http_interface_dependencies" in sql
    assert "UPDATE task_contract_dependencies" in sql
    assert "COMPATIBILITY_VERIFIED" in sql
    assert "PLAN_APPROVED" in sql


@pytest.mark.asyncio
async def test_provider_deployment_is_durably_rejected_by_unresolved_work(tmp_path):
    cursor = _Cursor(
        fetchall_values=[
            [],
            [{
                "work_item_id": "work-1",
                "consumer_service": "orders-service",
                "consumer_repository": "C:/repos/orders-service",
                "source_contract_id": "contract-billing",
                "source_contract_revision": 2,
                "state": "PENDING",
                "provider_service": "billing-service",
                "classification": "BREAKING",
            }],
        ],
        fetchone_values=[
            {"next_version": 9},
            {
                "deployment_id": "deployment-rejected",
                "service_name": "billing-service",
                "source_commit": "billing-sha",
                "status": "FAILED",
                "reload_version": 9,
                "health_check": {},
                "completed_at": "now",
            },
            {"event_id": "deployment-failed-event"},
        ],
    )
    conn = _Conn(cursor)

    async def run_tx(fn):
        return await fn(conn)

    with patch("coordinator.deployer.run_transaction", side_effect=run_tx):
        result = await promote_deployment(
            service_name="billing-service", source_commit="billing-sha", base_dir=tmp_path
        )

    assert result["status"] == "FAILED"
    assert result["compatibility_blockers"][0]["work_item_id"] == "work-1"
    assert result["is_healthy"] is False
    sql = " ".join(query for query, _ in cursor.queries)
    assert "source_contract_id" in sql
    assert "DEPLOYMENT_FAILED" in sql


def test_demo_service_processes_use_main_app_entrypoint(tmp_path):
    supervisor = ProcessSupervisor(tmp_path)
    process = MagicMock()
    process.pid = 1234
    process.poll.return_value = None

    with patch("coordinator.deployer.subprocess.Popen", return_value=process) as popen:
        billing = supervisor.start_service("billing-service", cwd=tmp_path / "billing")
        orders = supervisor.start_service("orders-service", cwd=tmp_path / "orders")

    assert billing["running"] and orders["running"]
    assert popen.call_count == 2
    commands = [call.args[0] for call in popen.call_args_list]
    assert commands[0][3] == "main:app"
    assert commands[1][3] == "main:app"
    supervisor.stop_all()


def test_late_claim_migration_adds_target_and_provider_indexes():
    migration = Path(__file__).parents[1] / "coordinator" / "migrations" / "013_late_compatibility_claims.sql"
    content = migration.read_text(encoding="utf-8")
    assert "idx_compatibility_work_claimable_target" in content
    assert "idx_compatibility_work_provider_gate" in content
