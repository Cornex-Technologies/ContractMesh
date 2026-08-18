"""Section 7 Verification Suite: Checkpoint-Aware Agent Runner & Reconciliation."""

import json
import shutil
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from coordinator.agent_runner import (
    adapt_agent_b_code_and_reconcile,
    adapt_worktree_billing_client,
    create_agent_b_worktree,
    execute_agent_b_step_with_drift_check,
    run_agent_a_publish_revision_1,
    run_agent_a_publish_revision_2,
    run_full_reconciliation_workflow,
    run_worktree_pytest,
    start_agent_b_checkout_task,
)
from coordinator.db import check_health, close_pool, init_db
from coordinator.reconciliation import (
    approve_reconciled_plan,
    check_task_drift,
    create_agent_task,
    reject_reconciled_plan,
    start_replanning,
    submit_reconciled_plan,
)


# ==============================================================================
# 1. Task Registration & Dependency Binding Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_create_agent_task_and_dependencies():
    """Verify create_agent_task registers active task and binds assumed upstream dependencies."""
    executed_statements = []

    mock_cur = AsyncMock()
    async def mock_execute(sql, params=None):
        executed_statements.append((sql, params))

    mock_cur.execute = mock_execute
    mock_cur.fetchone.side_effect = [
        {
            "task_id": "11111111-1111-1111-1111-111111111111",
            "status": "OPTIMISTIC_EXECUTING",
            "plan_revision": 1,
            "created_at": "2026-08-17T12:00:00Z",
        },
        {
            "confirmation_status": "CONFIRMED",
            "provider_service": "billing-service",
            "consumer_service": "orders-service",
            "contract_id": "22222222-2222-2222-2222-222222222222",
            "assumed_provider_revision": 1,
        },
        {"event_id": "outbox-task-registered"},
    ]

    mock_conn = MagicMock()
    mock_cur_ctx = AsyncMock()
    mock_cur_ctx.__aenter__.return_value = mock_cur
    mock_cur_ctx.__aexit__.return_value = None
    mock_conn.cursor.return_value = mock_cur_ctx

    with patch("coordinator.reconciliation.run_transaction") as mock_run_tx:
        async def run_tx_side_effect(fn):
            return await fn(mock_conn)
        mock_run_tx.side_effect = run_tx_side_effect

        task = await create_agent_task(
            agent_id="agent-b",
            service_name="orders-service",
            task_summary="Build checkout compatibility",
            worktree_path="worktrees/task-1",
            base_commit="commit-sha-12345",
            dependencies=[
                {
                    "provider_service": "billing-service",
                    "contract_id": "22222222-2222-2222-2222-222222222222",
                    "assumed_revision": 1,
                    "interface_dependency_id": "33333333-3333-3333-3333-333333333333",
                }
            ],
        )

        assert task["task_id"] == "11111111-1111-1111-1111-111111111111"
        assert task["status"] == "OPTIMISTIC_EXECUTING"
        assert task["plan_revision"] == 1
        assert len(task["dependencies"]) == 1
        assert task["dependencies"][0]["provider_service"] == "billing-service"


# ==============================================================================
# 2. Checkpoint Boundary Drift Interception Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_check_task_drift_returns_active_intervention():
    """Verify check_task_drift returns structured drift payload when intervention is active."""
    mock_drift_row = {
        "drift_id": "drift-888",
        "target_task_id": "task-orders-102",
        "source_service": "billing-service",
        "target_service": "orders-service",
        "old_contract_revision": 1,
        "new_contract_revision": 2,
        "breaking_diff": {
            "is_breaking": True,
            "breaking_changes": [{"field": "payment_method_id", "change": "new required field"}],
            "diff_summary": "New required fields: payment_method_id",
        },
        "drift_status": "ACTIVE_INTERVENTION",
        "task_status": "REPLAN_REQUIRED",
        "plan_revision": 1,
    }

    with patch("coordinator.reconciliation.fetch_one", AsyncMock(return_value=mock_drift_row)):
        drift = await check_task_drift("task-orders-102")
        
        assert drift is not None
        assert drift["drift_id"] == "drift-888"
        assert drift["old_contract_revision"] == 1
        assert drift["new_contract_revision"] == 2
        assert drift["breaking_diff"]["is_breaking"] is True
        assert drift["task_status"] == "REPLAN_REQUIRED"


# ==============================================================================
# 3. Fail-Closed Test Evidence & Conditional State Machine Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_reconciliation_rejects_missing_or_failed_test_evidence():
    """Verify submit_reconciled_plan fails closed when test evidence is missing or failed."""
    # 1. Missing test results
    with pytest.raises(ValueError, match="test_results evidence dictionary is required"):
        await submit_reconciled_plan(
            task_id="task-101",
            drift_id="drift-888",
            adapted_files=["clients/billing_client.py"],
            test_results={},
            plan_summary="Plan with empty tests",
        )

    # 2. Test results all_passed=False
    with pytest.raises(ValueError, match="did not report all_passed=True"):
        await submit_reconciled_plan(
            task_id="task-101",
            drift_id="drift-888",
            adapted_files=["clients/billing_client.py"],
            test_results={"all_passed": False, "returncode": 1},
            plan_summary="Plan with failing tests",
        )

    # 3. Test results non-zero returncode
    with pytest.raises(ValueError, match="non-zero returncode"):
        await submit_reconciled_plan(
            task_id="task-101",
            drift_id="drift-888",
            adapted_files=["clients/billing_client.py"],
            test_results={"all_passed": True, "returncode": 2},
            plan_summary="Plan with exit code 2",
        )


@pytest.mark.asyncio
async def test_human_approval_state_machine_manual():
    """Verify manual reconciliation lifecycle: REPLAN_REQUIRED -> REPLANNING -> AWAITING_APPROVAL -> RECONCILED."""
    mock_conn = MagicMock()
    mock_cur = AsyncMock()

    # 1. Test start_replanning
    mock_cur.fetchone.side_effect = [
        {"task_id": "task-101", "status": "REPLAN_REQUIRED", "plan_revision": 1},
        {"task_id": "task-101", "status": "REPLANNING", "plan_revision": 2},
        {"event_id": "outbox-replan-1"},
    ]
    mock_conn.cursor.return_value.__aenter__.return_value = mock_cur

    with patch("coordinator.reconciliation.run_transaction") as mock_run_tx, \
         patch("coordinator.reconciliation.persist_http_interface_dependency", AsyncMock(side_effect=["interface-1", "interface-2"])):
        async def run_tx_side_effect(fn):
            return await fn(mock_conn)
        mock_run_tx.side_effect = run_tx_side_effect

        replan_res = await start_replanning("task-101")
        assert replan_res["status"] == "REPLANNING"
        assert replan_res["plan_revision"] == 2

    # 2. Test submit_reconciled_plan (Manual Mode: auto_reconcile=False)
    mock_cur.fetchone.side_effect = [
        {"drift_id": "drift-888", "source_service": "billing-service", "new_contract_revision": 2, "status": "ACTIVE_INTERVENTION"},
            {"task_id": "task-101", "status": "AWAITING_APPROVAL", "plan_revision": 2},
            {"event_id": "outbox-awaiting-approval-1"},
    ]

    with patch("coordinator.reconciliation.run_transaction") as mock_run_tx:
        async def run_tx_side_effect(fn):
            return await fn(mock_conn)
        mock_run_tx.side_effect = run_tx_side_effect

        submit_res = await submit_reconciled_plan(
            task_id="task-101",
            drift_id="drift-888",
            adapted_files=["clients/billing_client.py"],
            test_results={"all_passed": True, "returncode": 0},
            plan_summary="Migrated to payment_method_id",
            auto_reconcile=False,
        )
        assert submit_res["status"] == "AWAITING_APPROVAL"
        assert submit_res["auto_reconciled"] is False

    # 3. Test approve_reconciled_plan (Operator Human Approval)
    mock_cur.fetchone.side_effect = [
        {"task_id": "task-101", "status": "RECONCILED", "plan_revision": 2},
        {"drift_id": "drift-888", "source_service": "billing-service", "new_contract_revision": 2},
        {
            "dependency_id": "dep-old-1",
            "provider_service": "billing-service",
            "consumer_service": "orders-service",
            "contract_id": "ctr-1",
            "http_method": "POST",
            "endpoint_path": "/v1/charges",
            "consumer_repository": "repos/orders-service",
            "consumer_source_file": "clients/billing_client.py",
        },
            {"dependency_id": "dep-new-1"},
            {"event_id": "outbox-reconciled-1"},
        {"event_id": "outbox-evt-1"},
    ]

    with patch("coordinator.reconciliation.run_transaction") as mock_run_tx:
        async def run_tx_side_effect(fn):
            return await fn(mock_conn)
        mock_run_tx.side_effect = run_tx_side_effect

        approve_res = await approve_reconciled_plan(
            task_id="task-101",
            approved_by="lead-engineer",
        )
        assert approve_res["status"] == "RECONCILED"
        assert approve_res["approved_by"] == "lead-engineer"


@pytest.mark.asyncio
async def test_reconciliation_human_rejection_lifecycle():
    """Verify human rejection transitions task back to REPLANNING with feedback."""
    mock_conn = MagicMock()
    mock_cur = AsyncMock()

    mock_cur.fetchone.side_effect = [
        {"task_id": "task-101", "status": "AWAITING_APPROVAL", "plan_revision": 2},
        {"task_id": "task-101", "status": "REPLANNING", "plan_revision": 3},
        {"event_id": "outbox-evt-2"},
    ]
    mock_conn.cursor.return_value.__aenter__.return_value = mock_cur

    with patch("coordinator.reconciliation.run_transaction") as mock_run_tx:
        async def run_tx_side_effect(fn):
            return await fn(mock_conn)
        mock_run_tx.side_effect = run_tx_side_effect

        reject_res = await reject_reconciled_plan(
            task_id="task-101",
            rejection_reason="Please add retry logic to the new payment client",
            rejected_by="qa-reviewer",
        )
        assert reject_res["status"] == "REPLANNING"
        assert reject_res["plan_revision"] == 3
        assert "retry logic" in reject_res["rejection_reason"]


# ==============================================================================
# 4. Auto-Reconcile Demo Mode Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_reconciliation_auto_reconcile_demo_mode():
    """Verify that when auto_reconcile=True, submit_reconciled_plan transitions directly to RECONCILED."""
    mock_conn = MagicMock()
    mock_cur = AsyncMock()

    mock_cur.fetchone.side_effect = [
        {"drift_id": "drift-888", "source_service": "billing-service", "new_contract_revision": 2, "status": "ACTIVE_INTERVENTION"},
        {"task_id": "task-101", "status": "RECONCILED", "plan_revision": 2},
        {
            "dependency_id": "dep-old-1",
            "provider_service": "billing-service",
            "consumer_service": "orders-service",
            "contract_id": "ctr-1",
            "http_method": "POST",
            "endpoint_path": "/v1/charges",
            "consumer_repository": "repos/orders-service",
            "consumer_source_file": "clients/billing_client.py",
        },
        {"dependency_id": "dep-new-1"},
        {"event_id": "outbox-reconciled-demo"},
    ]
    mock_conn.cursor.return_value.__aenter__.return_value = mock_cur

    with patch("coordinator.reconciliation.run_transaction") as mock_run_tx:
        async def run_tx_side_effect(fn):
            return await fn(mock_conn)
        mock_run_tx.side_effect = run_tx_side_effect

        res = await submit_reconciled_plan(
            task_id="task-101",
            drift_id="drift-888",
            adapted_files=["clients/billing_client.py"],
            test_results={"all_passed": True, "returncode": 0},
            plan_summary="Auto-adapted for video demo",
            auto_reconcile=True,
        )

        assert res["status"] == "RECONCILED"
        assert res["auto_reconciled"] is True


# ==============================================================================
# 5. Real Worktree Code Adaptation & Pytest Execution Tests
# ==============================================================================


def test_real_worktree_adaptation_and_pytest(tmp_path):
    """Verify Agent B creates real worktree, modifies client file, and runs real pytest subprocess."""
    base_dir = Path(__file__).parent.parent
    src_repo = base_dir / "repos" / "orders-service"

    # 1. Create temporary worktree
    wt_dir = tmp_path / "worktree-orders"
    shutil.copytree(src_repo, wt_dir)

    # 2. Modify code in worktree
    modified_file = adapt_worktree_billing_client(wt_dir)
    assert modified_file.exists()
    content = modified_file.read_text(encoding="utf-8")
    assert "BillingClient = BillingClientV2" in content

    # 3. Run real pytest subprocess in worktree
    test_results = run_worktree_pytest(wt_dir, timeout_seconds=15.0)
    assert test_results["all_passed"] is True
    assert test_results["returncode"] == 0
    assert "passed" in test_results["stdout"]


@pytest.mark.asyncio
async def test_multi_task_concurrent_worktree_isolation(tmp_path):
    """Verify multiple concurrent tasks create distinct isolated worktrees and refuse destructive overwrites."""
    mock_conn = MagicMock()
    mock_cur = AsyncMock()
    mock_cur.fetchone.side_effect = [
        {"task_id": "task-uuid-1", "status": "OPTIMISTIC_EXECUTING", "plan_revision": 1},
        {"confirmation_status": "CONFIRMED", "provider_service": "billing-service", "consumer_service": "orders-service", "contract_id": "ctr-1", "assumed_provider_revision": 1, "http_method": "POST", "endpoint_path": "/v1/charges"},
        {"event_id": "outbox-task-1"},
        {"task_id": "task-uuid-2", "status": "OPTIMISTIC_EXECUTING", "plan_revision": 1},
        {"confirmation_status": "CONFIRMED", "provider_service": "billing-service", "consumer_service": "orders-service", "contract_id": "ctr-1", "assumed_provider_revision": 1, "http_method": "POST", "endpoint_path": "/v1/charges"},
        {"event_id": "outbox-task-2"},
    ]
    mock_conn.cursor.return_value.__aenter__.return_value = mock_cur

    with patch("coordinator.reconciliation.run_transaction") as mock_run_tx, \
         patch("coordinator.reconciliation.persist_http_interface_dependency", AsyncMock(side_effect=["interface-1", "interface-2"])):
        async def run_tx_side_effect(fn):
            return await fn(mock_conn)
        mock_run_tx.side_effect = run_tx_side_effect

        t1 = await start_agent_b_checkout_task(
            contract_id="ctr-1",
            base_dir=tmp_path,
            task_id="task-uuid-1",
        )
        t2 = await start_agent_b_checkout_task(
            contract_id="ctr-1",
            base_dir=tmp_path,
            task_id="task-uuid-2",
        )

        wt1 = Path(t1["worktree_path"])
        wt2 = Path(t2["worktree_path"])

        assert wt1 != wt2
        assert wt1.exists()
        assert wt2.exists()
        assert wt1.name == "task-orders-task-uuid-1"
        assert wt2.name == "task-orders-task-uuid-2"

        # Refusal test: Attempting to create existing worktree must raise FileExistsError
        with pytest.raises(FileExistsError, match="refusing to destructively overwrite"):
            create_agent_b_worktree(task_id="task-uuid-1", base_dir=tmp_path)




# ==============================================================================
# 6. Live CockroachDB Multi-Agent Integration Test
# ==============================================================================


@pytest.mark.integration
@pytest.mark.asyncio
async def test_live_agent_runner_and_reconciliation_pipeline():
    """Live test: Executes complete Agent A (Billing) & Agent B (Orders) reconciliation flow."""
    health = await check_health(timeout_seconds=3.0)
    if health.get("status") != "healthy":
        pytest.skip(f"Live CockroachDB is not reachable: {health.get('error')}")

    try:
        await init_db()

        result = await run_full_reconciliation_workflow(auto_reconcile=True)
        assert result["contract_id"] is not None
        assert result["task_id"] is not None
        assert result["drift_id"] is not None
        assert result["reconciliation_status"] == "RECONCILED"
        assert result["auto_reconciled"] is True
        assert result["test_results"]["all_passed"] is True

    finally:
        await close_pool()
