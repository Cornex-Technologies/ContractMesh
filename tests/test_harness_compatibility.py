"""Offline and unit tests for deterministic harness-neutral compatibility coordination."""

from __future__ import annotations

import io
import inspect
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from coordinator.app import RegisterHarnessTaskRequest, app
from coordinator.compatibility import (
    approve_compatibility_work,
    cancel_compatibility_work,
    complete_compatibility_work,
    complete_harness_task,
    expire_compatibility_work,
    fail_compatibility_work,
    record_compatibility_incident,
    record_compatibility_result,
    record_harness_checkpoint,
    register_harness,
    register_harness_task,
    build_compatibility_coordination_key,
    validate_checkpoint_payload,
    _create_task_with_cursor,
)
from coordinator.reconciliation import create_agent_task
from coordinator.config import settings
from coordinator.deployer import promote_deployment
from coordinator.differencer import compute_schema_diff
from coordinator.mcp_server import mcp
from coordinator.memory import BedrockCohereEmbedV4Embeddings


def test_harness_workflow_migration_defines_durable_tables_and_states():
    migration = Path(__file__).parent.parent / "coordinator" / "migrations" / "003_harness_compatibility_workflow.sql"
    content = migration.read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS harness_registrations" in content
    assert "CREATE TABLE IF NOT EXISTS compatibility_work_items" in content
    assert "CREATE TABLE IF NOT EXISTS compatibility_dispatch_attempts" in content
    assert "idempotency_key STRING NOT NULL UNIQUE" in content
    assert "AWAITING_APPROVAL" in content
    assert "hop_count <= 5" in content


def test_semantic_compatibility_work_identity_is_migration_backed():
    migration_014 = (Path(__file__).parent.parent / "coordinator" / "migrations" / "014_compatibility_work_coordination_key.sql").read_text(encoding="utf-8")
    migration_015 = (Path(__file__).parent.parent / "coordinator" / "migrations" / "015_backfill_compatibility_coordination_keys.sql").read_text(encoding="utf-8")
    migration_016 = (Path(__file__).parent.parent / "coordinator" / "migrations" / "016_enforce_compatibility_coordination_key.sql").read_text(encoding="utf-8")

    assert "ADD COLUMN IF NOT EXISTS coordination_key STRING" in migration_014
    assert "'SUPERSEDED'" in migration_014
    assert "row_number() OVER" in migration_015
    assert "Superseded by the canonical compatibility obligation" in migration_015
    assert "UNIQUE INDEX" in migration_016
    assert "coordination_key" in migration_016


def test_semantic_compatibility_work_key_is_stable_across_event_delivery():
    first = build_compatibility_coordination_key(
        source_contract_id="contract-1", source_contract_revision=2, interface_dependency_id="dependency-1"
    )
    second = build_compatibility_coordination_key(
        source_contract_id="contract-1", source_contract_revision=2, interface_dependency_id="dependency-1"
    )
    different_revision = build_compatibility_coordination_key(
        source_contract_id="contract-1", source_contract_revision=3, interface_dependency_id="dependency-1"
    )

    assert first == second
    assert first != different_revision


def test_task_registration_requires_operational_summary_not_prompt():
    assert "task_prompt" not in RegisterHarnessTaskRequest.model_fields
    assert "task_prompt" not in inspect.signature(create_agent_task).parameters


@pytest.mark.asyncio
async def test_compatibility_claim_rejects_revoked_dependency():
    cursor = AsyncMock()
    cursor.fetchone.side_effect = [
        {"task_id": "task-1", "status": "OPTIMISTIC_EXECUTING", "plan_revision": 1},
        {
            "confirmation_status": "DECLARED",
            "provider_service": "billing-service",
            "consumer_service": "orders-service",
            "contract_id": "contract-1",
            "assumed_provider_revision": 2,
            "http_method": "POST",
            "endpoint_path": "/v1/charges",
        },
    ]

    with pytest.raises(ValueError, match="unconfirmed"):
        await _create_task_with_cursor(
            cursor,
            agent_id="harness:orders",
            service_name="orders-service",
            task_summary="Apply billing compatibility update",
            worktree_path="worktrees/task-1",
            base_commit="commit-1",
            dependencies=[{
                "provider_service": "billing-service",
                "contract_id": "contract-1",
                "assumed_revision": 2,
                "dependency_path": "clients/billing.py",
                "interface_dependency_id": "dependency-1",
                "http_method": "POST",
                "endpoint_path": "/v1/charges",
            }],
        )


@pytest.mark.asyncio
async def test_harness_registration_links_audit_to_outbox(monkeypatch):
    cursor = AsyncMock()
    cursor.fetchone.side_effect = [
        {
            "harness_id": "harness-1",
            "harness_name": "codex-runner",
            "harness_type": "codex",
            "service_name": "orders-service",
            "repository_url": "C:/work/orders",
            "dispatch_mode": "poll",
            "dispatch_url": None,
            "status": "ACTIVE",
        },
        {"event_id": "event-harness-registered"},
    ]
    statements = []

    async def execute(sql, params=None):
        statements.append(sql)

    cursor.execute = execute
    conn = MagicMock()
    context = AsyncMock()
    context.__aenter__.return_value = cursor
    context.__aexit__.return_value = None
    conn.cursor.return_value = context

    async def run_in_transaction(fn):
        return await fn(conn)

    monkeypatch.setattr("coordinator.compatibility.run_transaction", run_in_transaction)
    result = await register_harness(
        harness_name="codex-runner",
        harness_type="codex",
        service_name="orders-service",
        repository_url="C:/work/orders",
    )
    assert result["harness_id"] == "harness-1"
    sql = " ".join(statements)
    assert "HARNESS_REGISTERED" in sql
    assert "RETURNING event_id" in sql
    assert "outbox_event_id" in sql


def test_review_and_human_decision_migration_states_exist():
    migration = Path(__file__).parent.parent / "coordinator" / "migrations" / "004_compatibility_review_and_incidents.sql"
    content = migration.read_text(encoding="utf-8")
    for state in ("REVIEW_REQUIRED", "BLOCKED", "INCOMPATIBLE", "VERIFIED", "COMPLETED"):
        assert state in content
    assert "CREATE TABLE IF NOT EXISTS compatibility_incidents" in content


def test_structured_incompatibility_migration_defines_evidence_columns():
    migration = Path(__file__).parent.parent / "coordinator" / "migrations" / "008_structured_incompatibility_incidents.sql"
    content = migration.read_text(encoding="utf-8")
    assert "reason_code STRING" in content
    assert "unavailable_required_input STRING" in content
    assert "provider_service STRING" in content
    assert "sources_checked JSONB" in content
    assert "worktree_path STRING" in content
    assert "source_commit STRING" in content
    assert "changed_files JSONB" in content


def test_semantic_changes_fail_closed_to_review_and_cannot_downgrade_structural_break():
    base = {"type": "object", "properties": {"amount": {"type": "number"}}}
    semantic = compute_schema_diff(base, base, publisher_compatibility={"semantic_change": True})
    assert semantic.classification == "REVIEW_REQUIRED"
    structural = compute_schema_diff(
        base,
        {"type": "object", "properties": {}},
        publisher_compatibility={"classification": "NON_BREAKING", "semantic_change": True},
    )
    assert structural.classification == "BREAKING"
    assert structural.is_breaking is True


def test_checkpoint_payload_rejects_chain_of_thought_and_scratchpad():
    """Fail closed if chain-of-thought, thoughts, scratchpad, prompts, or raw chat histories are submitted."""
    cot_payloads = [
        {"phase": "IMPLEMENTING", "scratchpad": "private reasoning step"},
        {"phase": "IMPLEMENTING", "thought": "I should call API v2"},
        {"phase": "IMPLEMENTING", "chain_of_thought": "step 1 -> step 2"},
        {"phase": "IMPLEMENTING", "cot": "internal model reasoning"},
        {"phase": "IMPLEMENTING", "chat_history": [{"role": "user", "content": "hello"}]},
        {"phase": "IMPLEMENTING", "raw_chat": "full chat dump"},
        {"phase": "IMPLEMENTING", "prompt": "system prompt here"},
        {"phase": "IMPLEMENTING", "messages": ["msg1", "msg2"]},
        {"phase": "IMPLEMENTING", "unsupported_extra_field": 123},
    ]
    for bad_payload in cot_payloads:
        with pytest.raises(ValueError):
            validate_checkpoint_payload(bad_payload)


def test_checkpoint_payload_validates_operational_metadata():
    """Verify structured metadata fields are cleanly validated and normalized."""
    clean = validate_checkpoint_payload({
        "task_id": "task-uuid-123",
        "plan_revision": 2,
        "phase": "TESTING",
        "files_changed": ["clients/billing_client.py", "tests/test_billing.py"],
        "assumed_contract_revisions": {"billing-service": 2},
        "test_status": "PASSED",
        "summary": "Updated billing client and verified contract tests pass.",
    })
    assert clean["task_id"] == "task-uuid-123"
    assert clean["plan_revision"] == 2
    assert clean["phase"] == "TESTING"
    assert clean["files_changed"] == ["clients/billing_client.py", "tests/test_billing.py"]
    assert clean["changed_files"] == ["clients/billing_client.py", "tests/test_billing.py"]
    assert clean["assumed_contract_revisions"] == {"billing-service": 2}
    assert clean["test_status"] == "PASSED"
    assert clean["summary"] == "Updated billing client and verified contract tests pass."


def test_checkpoint_payload_accepts_changed_files_alias():
    clean = validate_checkpoint_payload({
        "phase": "PLANNING",
        "changed_files": ["routes/checkout.py"],
        "assumed_contract_revisions": {},
        "test_status": "NOT_RUN",
    })
    assert clean["files_changed"] == ["routes/checkout.py"]
    assert clean["phase"] == "PLANNING"


def test_cohere_v4_uses_document_and_query_specific_payloads():
    provider = BedrockCohereEmbedV4Embeddings.__new__(BedrockCohereEmbedV4Embeddings)
    provider.model_id = "cohere.embed-v4:0"
    provider.dimension = 1536
    provider.client = MagicMock()
    provider.client.invoke_model.return_value = {
        "body": io.BytesIO(json.dumps({"embeddings": [[0.1, 0.2]]}).encode("utf-8"))
    }

    assert provider.embed_query("find payment contracts") == [0.1, 0.2]
    payload = json.loads(provider.client.invoke_model.call_args.kwargs["body"])
    assert payload["input_type"] == "search_query"
    assert payload["texts"] == ["find payment contracts"]

    provider.client.invoke_model.return_value = {
        "body": io.BytesIO(json.dumps({"embeddings": [[0.3], [0.4]]}).encode("utf-8"))
    }
    assert provider.embed_documents(["contract A", "contract B"]) == [[0.3], [0.4]]
    payload = json.loads(provider.client.invoke_model.call_args.kwargs["body"])
    assert payload["input_type"] == "search_document"
    assert payload["embedding_types"] == ["float"]


@pytest.mark.asyncio
async def test_dispatcher_returns_none_without_pending_work(monkeypatch):
    from coordinator import compatibility_dispatcher

    async def no_work():
        return None

    monkeypatch.setattr(compatibility_dispatcher, "claim_next_dispatch", no_work)
    assert await compatibility_dispatcher.dispatch_one_pending_work_item() is None


@pytest.mark.asyncio
async def test_record_harness_checkpoint_deterministic_continue(monkeypatch):
    """When no drift is active, coordinator responds with CONTINUE."""
    harness_mock = {"harness_name": "codex-runner", "harness_type": "codex"}

    monkeypatch.setattr("coordinator.compatibility.fetch_one", AsyncMock(return_value=harness_mock))
    monkeypatch.setattr("coordinator.compatibility.run_transaction", AsyncMock(return_value=None))
    monkeypatch.setattr("coordinator.reconciliation.check_task_drift", AsyncMock(return_value=None))

    resp = await record_harness_checkpoint(
        harness_id="h-1",
        task_id="t-1",
        checkpoint={
            "phase": "IMPLEMENTING",
            "files_changed": ["main.py"],
            "assumed_contract_revisions": {"billing": 1},
            "test_status": "NOT_RUN",
        },
    )
    assert resp["instruction"] == "CONTINUE"
    assert "checkpoint_outbox_id" in resp


@pytest.mark.asyncio
async def test_checkpoint_locks_task_and_optional_work_in_separate_queries(monkeypatch):
    """Checkpoint transactions must avoid locking the nullable side of an outer join."""
    executed_queries = []

    class RecordingCursor:
        def __init__(self):
            self.rows = [
                {
                    "task_id": "task-1",
                    "service_name": "orders-service",
                    "status": "OPTIMISTIC_EXECUTING",
                    "plan_revision": 1,
                },
                None,
                {"event_id": "outbox-1"},
            ]

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def execute(self, query, params=None):
            executed_queries.append(" ".join(str(query).split()))

        async def fetchone(self):
            return self.rows.pop(0)

    class RecordingConnection:
        def __init__(self, cursor):
            self._cursor = cursor

        def cursor(self, **kwargs):
            return self._cursor

    cursor = RecordingCursor()

    monkeypatch.setattr(
        "coordinator.compatibility.fetch_one",
        AsyncMock(return_value={"harness_name": "codex-runner", "harness_type": "codex"}),
    )

    async def run_transaction(fn):
        return await fn(RecordingConnection(cursor))

    monkeypatch.setattr("coordinator.compatibility.run_transaction", run_transaction)
    monkeypatch.setattr("coordinator.reconciliation.check_task_drift", AsyncMock(return_value=None))

    result = await record_harness_checkpoint(
        harness_id="harness-1",
        task_id="task-1",
        checkpoint={
            "phase": "IMPLEMENTING",
            "files_changed": ["main.py"],
            "assumed_contract_revisions": {"billing-service": 2},
            "test_status": "NOT_RUN",
        },
    )

    assert result["instruction"] == "CONTINUE"
    assert "LEFT JOIN" not in executed_queries[0].upper()
    assert "FROM active_agent_tasks" in executed_queries[0]
    assert "FOR UPDATE" in executed_queries[0]
    assert "FROM compatibility_work_items" in executed_queries[1]
    assert "FOR UPDATE" in executed_queries[1]


@pytest.mark.asyncio
async def test_normal_task_completion_locks_task_and_optional_work_separately(monkeypatch):
    """Normal completion uses the same CockroachDB-safe ownership locking pattern."""
    executed_queries = []

    class RecordingCursor:
        def __init__(self):
            self.rows = [
                {
                    "task_id": "task-2",
                    "service_name": "orders-service",
                    "status": "OPTIMISTIC_EXECUTING",
                    "plan_revision": 1,
                },
                None,
                {"task_id": "task-2", "status": "COMPLETED", "plan_revision": 1},
                {"event_id": "outbox-2"},
            ]

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def execute(self, query, params=None):
            executed_queries.append(" ".join(str(query).split()))

        async def fetchone(self):
            return self.rows.pop(0)

    class RecordingConnection:
        def __init__(self, cursor):
            self._cursor = cursor

        def cursor(self, **kwargs):
            return self._cursor

    cursor = RecordingCursor()
    monkeypatch.setattr(
        "coordinator.compatibility.fetch_one",
        AsyncMock(return_value={"harness_name": "codex-runner", "harness_type": "codex"}),
    )

    async def run_transaction(fn):
        return await fn(RecordingConnection(cursor))

    monkeypatch.setattr("coordinator.compatibility.run_transaction", run_transaction)

    result = await complete_harness_task(
        harness_id="harness-1",
        task_id="task-2",
        summary="Completed normal task",
        test_results={"returncode": 0, "all_passed": True},
    )

    assert result["status"] == "COMPLETED"
    assert "LEFT JOIN" not in executed_queries[0].upper()
    assert "FROM active_agent_tasks" in executed_queries[0]
    assert "FOR UPDATE" in executed_queries[0]
    assert "FROM compatibility_work_items" in executed_queries[1]
    assert "FOR UPDATE" in executed_queries[1]


@pytest.mark.asyncio
async def test_record_harness_checkpoint_deterministic_replan_required(monkeypatch):
    """When drift is active, coordinator responds with authoritative REPLAN_REQUIRED containing diff, notes, and audit IDs."""
    harness_mock = {"harness_name": "claude-code", "harness_type": "claude"}
    drift_mock = {
        "drift_id": "drift-999",
        "task_id": "t-2",
        "source_service": "billing-service",
        "target_service": "orders-service",
        "old_contract_revision": 1,
        "new_contract_revision": 2,
        "breaking_diff": {
            "is_breaking": True,
            "diff_summary": "Removed card_token, added payment_method_id",
            "migration_note": "Migrate all callers to pass payment_method_id as UUID string",
        },
        "migration_notes": "Migrate all callers to pass payment_method_id as UUID string",
        "audit_ids": {
            "drift_id": "drift-999",
            "task_id": "t-2",
            "source_service": "billing-service",
            "target_service": "orders-service",
        },
        "drift_status": "ACTIVE_INTERVENTION",
        "task_status": "REPLAN_REQUIRED",
        "plan_revision": 2,
    }

    monkeypatch.setattr("coordinator.compatibility.fetch_one", AsyncMock(return_value=harness_mock))
    monkeypatch.setattr("coordinator.compatibility.run_transaction", AsyncMock(return_value=None))
    monkeypatch.setattr("coordinator.reconciliation.check_task_drift", AsyncMock(return_value=drift_mock))

    resp = await record_harness_checkpoint(
        harness_id="h-2",
        task_id="t-2",
        checkpoint={
            "phase": "TESTING",
            "plan_revision": 2,
            "files_changed": ["clients/billing.py"],
            "assumed_contract_revisions": {"billing-service": 1},
            "test_status": "FAILED",
        },
    )
    assert resp["instruction"] == "REPLAN_REQUIRED"
    assert resp["new_contract_revision"] == 2
    assert resp["old_contract_revision"] == 1
    assert resp["schema_diff"]["is_breaking"] is True
    assert "payment_method_id" in resp["migration_notes"]
    assert resp["audit_ids"]["drift_id"] == "drift-999"
    assert resp["audit_ids"]["task_id"] == "t-2"


@pytest.mark.asyncio
async def test_record_compatibility_result_rejects_failing_tests():
    """Compatibility results strictly require passing test evidence."""
    with pytest.raises(ValueError, match="passing test evidence"):
        await record_compatibility_result("work-1", test_results={"all_passed": False, "returncode": 1}, summary="Failed")

    with pytest.raises(ValueError, match="passing test evidence"):
        await record_compatibility_result("work-1", test_results={"all_passed": True, "returncode": 2}, summary="Bad rc")


@pytest.mark.asyncio
async def test_record_structured_blocked_incident_preserves_work():
    """Agent 2 encounters missing customer_id on guest checkout and submits structured BLOCKED incident."""
    executed_queries = []

    class MockCursor:
        def __init__(self):
            self.fetchone_calls = 0

        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            pass
        async def execute(self, query, params=None):
            executed_queries.append((query, params))
        async def fetchone(self):
            self.fetchone_calls += 1
            if self.fetchone_calls == 1:
                return {
                    "target_service": "orders-service",
                    "state": "EXECUTING",
                    "task_id": "task-guest-checkout-1",
                    "payload": {
                        "source_service": "billing-service",
                        "source_contract_revision": 2,
                        "breaking_diff": {"diff_summary": "Required customer_id field added"},
                    },
                    "worktree_path": "worktrees/task-orders-guest-checkout",
                    "base_commit": "c0ffee123",
                    "checkpoint_state": {"files_changed": ["clients/billing_client.py"]},
                }
            return {"event_id": "outbox-incident-guest-checkout"}

    class MockConn:
        def cursor(self, **kwargs):
            return MockCursor()

    async def mock_run_tx(fn):
        return await fn(MockConn())

    with patch("coordinator.compatibility.run_transaction", side_effect=mock_run_tx):
        res = await record_compatibility_incident(
            work_item_id="work-100",
            outcome="BLOCKED",
            unavailable_required_input="customer_id",
            reason_code="UNAVAILABLE_REQUIRED_INPUT",
            provider_service="billing-service",
            provider_contract_revision=2,
            sources_checked=["models/order.py", "clients/guest_checkout.py"],
            worktree_path="worktrees/task-orders-guest-checkout",
            source_commit="c0ffee123",
            changed_files=["clients/billing_client.py"],
            requested_resolution="Allow optional customer_id for guest checkout transactions or add guest payment endpoint",
        )

        assert res["work_item_id"] == "work-100"
        assert res["state"] == "BLOCKED"
        assert res["status"] == "HUMAN_DECISION_REQUIRED"
        assert res["reason_code"] == "UNAVAILABLE_REQUIRED_INPUT"
        assert res["unavailable_required_input"] == "customer_id"
        assert res["worktree_preserved"] == "worktrees/task-orders-guest-checkout"
        assert res["commit_preserved"] == "c0ffee123"

        # Verify SQL statements executed:
        # 1. compatibility_work_items updated with BLOCKED state and preserved metadata
        work_update = next(q for q in executed_queries if "UPDATE compatibility_work_items" in q[0])
        assert work_update[1][0] == "BLOCKED"
        assert "[UNAVAILABLE_REQUIRED_INPUT]" in work_update[1][1]
        payload_saved = json.loads(work_update[1][2])
        assert payload_saved["preserved_worktree"] == "worktrees/task-orders-guest-checkout"
        assert payload_saved["preserved_commit"] == "c0ffee123"

        # 2. compatibility_incidents record inserted/upserted with HUMAN_DECISION_REQUIRED
        inc_insert = next(q for q in executed_queries if "INSERT INTO compatibility_incidents" in q[0])
        assert inc_insert[1][1] == "BLOCKED"
        assert "customer_id" in inc_insert[1][2]
        inc_evidence = json.loads(inc_insert[1][3])
        assert inc_evidence["sources_checked"] == ["models/order.py", "clients/guest_checkout.py"]
        assert inc_evidence["provider_service"] == "billing-service"

        # 3. contract_audit_history append-only event recorded
        audit_insert = next(q for q in executed_queries if "INSERT INTO contract_audit_history" in q[0])
        assert audit_insert[1][0] == "COMPATIBILITY_BLOCKED"
        assert "orders-service" in audit_insert[1][1]
        assert "Requires human design decision" in audit_insert[1][2]

        # 4. coordinator_outbox event published
        outbox_insert = next(q for q in executed_queries if "INSERT INTO coordinator_outbox" in q[0])
        assert outbox_insert[1][1] == "COMPATIBILITY_BLOCKED"
        outbox_payload = json.loads(outbox_insert[1][2])
        assert outbox_payload["status"] == "HUMAN_DECISION_REQUIRED"
        assert outbox_payload["unavailable_required_input"] == "customer_id"


@pytest.mark.asyncio
async def test_record_structured_incompatible_incident():
    """Agent submits INCOMPATIBLE result when data types or semantics conflict."""
    executed_queries = []

    class MockCursor:
        def __init__(self):
            self.fetchone_calls = 0

        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            pass
        async def execute(self, query, params=None):
            executed_queries.append((query, params))
        async def fetchone(self):
            self.fetchone_calls += 1
            if self.fetchone_calls == 1:
                return {
                    "target_service": "orders-service",
                    "state": "EXECUTING",
                    "task_id": "task-2",
                    "payload": {"source_service": "billing-service", "source_contract_revision": 3},
                    "worktree_path": "worktrees/task-orders-2",
                    "base_commit": "deadbeef",
                    "checkpoint_state": {},
                }
            return {"event_id": "outbox-incident-incompatible"}

    class MockConn:
        def cursor(self, **kwargs):
            return MockCursor()

    async def mock_run_tx(fn):
        return await fn(MockConn())

    with patch("coordinator.compatibility.run_transaction", side_effect=mock_run_tx):
        res = await record_compatibility_incident(
            work_item_id="work-200",
            outcome="INCOMPATIBLE",
            reason_code="SEMANTIC_DOMAIN_MISMATCH",
            missing_requirement="Billing requires ISO-4217 numeric currency codes instead of 3-letter strings",
            requested_resolution="Maintain 3-letter currency string support in Billing v3",
                sources_checked=["currency_mapper.py"],
                worktree_path="worktrees/task-orders-2",
                source_commit="deadbeef",
                changed_files=["currency_mapper.py"],
            )
        assert res["state"] == "INCOMPATIBLE"
        assert res["status"] == "HUMAN_DECISION_REQUIRED"
        assert res["reason_code"] == "SEMANTIC_DOMAIN_MISMATCH"


@pytest.mark.asyncio
async def test_promote_deployment_blocks_on_active_compatibility_incident():
    """promote_deployment durably rejects target service unresolved incidents."""
    class MockCursor:
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            pass
        async def execute(self, query, params=None):
            pass
        async def fetchall(self):
            return [{
                "incident_id": "inc-uuid-1",
                "incident_type": "BLOCKED",
                "missing_requirement": "customer_id",
                "reason_code": "UNAVAILABLE_REQUIRED_INPUT",
            }]
        fetchone_calls = 0
        async def fetchone(self):
            self.fetchone_calls = getattr(self, "fetchone_calls", 0) + 1
            if self.fetchone_calls == 1:
                return {"next_version": 1}
            if self.fetchone_calls == 2:
                return {
                    "deployment_id": "dep-blocked-1",
                    "service_name": "orders-service",
                    "source_commit": "validcommit12345",
                    "status": "FAILED",
                    "reload_version": 1,
                    "health_check": {},
                    "completed_at": "now",
                }
            if self.fetchone_calls == 3:
                return {"event_id": "deployment-failed-event"}
            return None

    class MockConn:
        def cursor(self, **kwargs):
            return MockCursor()

    async def mock_run_tx(fn):
        return await fn(MockConn())

    with patch("coordinator.deployer.run_transaction", side_effect=mock_run_tx):
        result = await promote_deployment(
            service_name="orders-service",
            source_commit="validcommit12345",
        )
    assert result["status"] == "FAILED"
    assert result["compatibility_blockers"][0]["incident_id"] == "inc-uuid-1"
    assert result["is_healthy"] is False


def test_mcp_server_exposes_harness_neutral_tools():
    """Verify FastMCP registry exposes all required deterministic coordination tools."""
    tool_names = [tool.name for tool in mcp._tool_manager.list_tools()]
    assert "register_task" in tool_names
    assert "checkpoint_task" in tool_names
    assert "get_pending_drift" in tool_names
    assert "claim_compatibility_work" in tool_names
    assert "complete_task" in tool_names
    assert "submit_compatibility_evidence" in tool_names
    assert "report_incompatible_contract" in tool_names
    assert "discover_relevant_contracts" in tool_names
    assert "retire_endpoint" in tool_names
    assert "publish_endpoint_inventory" in tool_names


@pytest.mark.asyncio
async def test_harness_rest_endpoints():
    """Verify authenticated REST API operations for external harnesses."""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Unauthenticated request rejected
        resp_unauth = await client.get("/harnesses/h-test/tasks/t-test/drift")
        assert resp_unauth.status_code == 401

        # 2. Authenticated drift query
        with patch("coordinator.app.authenticate_harness", AsyncMock(return_value={"harness_id": "h-test", "harness_name": "test-runner", "status": "ACTIVE"})):
            with patch("coordinator.app.check_task_drift", AsyncMock(return_value=None)):
                resp = await client.get(
                    "/harnesses/h-test/tasks/t-test/drift",
                    headers={"X-Harness-Token": "valid-secret-token"},
                )
                assert resp.status_code == 200
                assert resp.json()["instruction"] == "CONTINUE"

        # 3. Checkpoint endpoint with CoT rejected
        with patch("coordinator.app.authenticate_harness", AsyncMock(return_value={"harness_id": "h-test", "harness_name": "test-runner", "status": "ACTIVE"})):
            bad_cp = {"phase": "IMPLEMENTING", "scratchpad": "reasoning"}
            resp_bad = await client.post(
                "/harnesses/h-test/tasks/t-test/checkpoint",
                json=bad_cp,
                headers={"X-Harness-Token": "valid-secret-token"},
            )
            assert resp_bad.status_code == 400

        # 4. Valid checkpoint endpoint
        valid_cp = {
            "task_id": "t-test",
            "plan_revision": 1,
            "phase": "IMPLEMENTING",
            "files_changed": ["src/app.py"],
            "assumed_contract_revisions": {"billing": 1},
            "test_status": "NOT_RUN",
        }
        with patch("coordinator.app.authenticate_harness", AsyncMock(return_value={"harness_id": "h-test", "harness_name": "test-runner", "status": "ACTIVE"})):
            with patch("coordinator.app.record_harness_checkpoint", AsyncMock(return_value={"instruction": "CONTINUE"})):
                resp_good = await client.post(
                    "/harnesses/h-test/tasks/t-test/checkpoint",
                    json=valid_cp,
                    headers={"X-Harness-Token": "valid-secret-token"},
                )
                assert resp_good.status_code == 200
                assert resp_good.json()["instruction"] == "CONTINUE"

        # 5. Structured incident endpoint
        incident_req = {
            "outcome": "BLOCKED",
            "unavailable_required_input": "customer_id",
            "reason_code": "UNAVAILABLE_REQUIRED_INPUT",
            "provider_service": "billing-service",
            "provider_contract_revision": 2,
            "sources_checked": ["models/order.py"],
            "worktree_path": "worktrees/task-orders",
            "source_commit": "abc12345",
            "changed_files": ["clients/billing.py"],
            "requested_resolution": "Make customer_id optional for guest checkout",
        }
        with patch("coordinator.app.authenticate_harness", AsyncMock(return_value={"harness_id": "h-test", "harness_name": "test-runner", "status": "ACTIVE"})):
            with patch("coordinator.app.fetch_one", AsyncMock(return_value={"harness_id": "h-test"})):
                with patch("coordinator.app.record_compatibility_incident", AsyncMock(return_value={"work_item_id": "work-1", "state": "BLOCKED", "status": "HUMAN_DECISION_REQUIRED"})):
                    resp_inc = await client.post(
                        "/harnesses/h-test/compatibility-work/work-1/incident",
                        json=incident_req,
                        headers={"X-Harness-Token": "valid-secret-token"},
                    )
                    assert resp_inc.status_code == 200
                    assert resp_inc.json()["state"] == "BLOCKED"
                    assert resp_inc.json()["status"] == "HUMAN_DECISION_REQUIRED"
