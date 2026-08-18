"""Demo Adapter: Checkpoint-Aware Multi-Agent Orchestration & Workflow Runner.

NOTE: This is a scripted demonstration adapter showing how external harnesses (such as Codex,
Claude Code, Cursor, or internal runners) interact with CodeClaim's deterministic coordinator.
It is NOT the platform execution model; CodeClaim coordinator is harness-neutral and provides
durable state transitions and contract compatibility via authenticated REST and MCP APIs.

Coordinates the end-to-end multi-agent drift and reconciliation demonstration:
1. Agent A (Billing Service Provider Demo Adapter):
   - Publishes initial Contract Revision 1.0 ('card_token')
   - Later publishes breaking Contract Revision 2.0 ('payment_method_id')
2. Agent B (Orders Service Consumer Demo Adapter):
   - Creates an isolated worktree `worktrees/task-orders-checkout`
   - Starts in-flight task to build checkout integration assuming Revision 1.0
   - Advances through discrete execution milestones with structured checkpoint metadata
   - Intercepts REPLAN_REQUIRED drift payload at a clean checkpoint boundary
   - Replans and modifies real code in worktree to adapt `billing_client.py` to 'payment_method_id'
   - Executes real pytest subprocess to capture authoritative exit code and test evidence
   - Submits verified reconciled plan and reaches RECONCILED state
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import shutil
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any, Optional
import httpx

from coordinator.config import settings
from coordinator.contract_registry import (
    extract_pydantic_schema_from_repo,
    get_service_git_commit,
    publish_contract_revision,
)
from coordinator.drift_worker import process_all_pending_events
from coordinator.memory import (
    discover_and_verify_dependencies,
    load_agent_checkpoint,
    save_agent_checkpoint,
    store_contract_semantic_memory,
)
from coordinator.reconciliation import (
    approve_reconciled_plan,
    check_task_drift,
    create_agent_task,
    start_replanning,
    submit_reconciled_plan,
)

logger = logging.getLogger(__name__)


# ==============================================================================
# 1. Agent A (Provider Workflow)
# ==============================================================================


async def run_agent_a_publish_revision_1(base_dir: Optional[Path] = None) -> dict[str, Any]:
    """Agent A publishes Billing Service Contract Revision 1.0 ('card_token')."""
    root_dir = base_dir or Path(__file__).parent.parent
    billing_repo = root_dir / "repos" / "billing-service"

    commit_sha = get_service_git_commit(billing_repo)
    schema_v1 = extract_pydantic_schema_from_repo(billing_repo, "schemas_v1.py", "ChargeRequest")

    pub_result = await publish_contract_revision(
        service_name="billing-service",
        endpoint_path="/v1/charges",
        http_method="POST",
        revision_number=1,
        schema_json=schema_v1,
        semantic_summary="Process credit card payments using legacy card_token",
        published_by="agent-a-billing-lead",
        source_commit=commit_sha,
    )

    mem_id = await store_contract_semantic_memory(
        contract_revision_id=pub_result["contract_revision_id"],
        service_name="billing-service",
        endpoint_path="/v1/charges",
        http_method="POST",
        summary="Process credit card payments using legacy card_token",
        metadata={"revision": 1, "tier": "production"},
        use_langchain_store=False,
    )
    pub_result["memory_id"] = mem_id
    return pub_result


async def run_agent_a_publish_revision_2(base_dir: Optional[Path] = None) -> dict[str, Any]:
    """Agent A publishes breaking Billing Service Contract Revision 2.0 ('payment_method_id')."""
    root_dir = base_dir or Path(__file__).parent.parent
    billing_repo = root_dir / "repos" / "billing-service"

    commit_sha = get_service_git_commit(billing_repo)
    schema_v2 = extract_pydantic_schema_from_repo(billing_repo, "schemas_v2.py", "ChargeRequest")

    pub_result = await publish_contract_revision(
        service_name="billing-service",
        endpoint_path="/v1/charges",
        http_method="POST",
        revision_number=2,
        schema_json=schema_v2,
        semantic_summary="Process credit card payments using modern payment_method_id with optional description",
        published_by="agent-a-billing-lead",
        source_commit=commit_sha,
    )

    mem_id = await store_contract_semantic_memory(
        contract_revision_id=pub_result["contract_revision_id"],
        service_name="billing-service",
        endpoint_path="/v1/charges",
        http_method="POST",
        summary="Process credit card payments using modern payment_method_id",
        metadata={"revision": 2, "tier": "production"},
        use_langchain_store=False,
    )
    pub_result["memory_id"] = mem_id
    return pub_result


# ==============================================================================
# 2. Agent B (Consumer Workflow with Real Worktree Adaptation & Test Execution)
# ==============================================================================


def create_agent_b_worktree(
    task_id: str,
    base_dir: Optional[Path] = None,
    source_commit: Optional[str] = None,
) -> Path:
    """Create an isolated per-task worktree directory for Agent B from repos/orders-service."""
    root_dir = base_dir or Path(__file__).parent.parent
    src_repo = root_dir / "repos" / "orders-service"
    if not src_repo.exists():
        src_repo = Path(__file__).parent.parent / "repos" / "orders-service"

    worktree_dir = root_dir / "worktrees" / f"task-orders-{task_id}"

    # Strict check: refuse destructive overwrites of active worktree directories
    if worktree_dir.exists():
        raise FileExistsError(
            f"Worktree directory already exists at {worktree_dir}; refusing to destructively overwrite."
        )

    worktree_dir.parent.mkdir(parents=True, exist_ok=True)

    if source_commit and not source_commit.startswith("commit-untracked"):
        from coordinator.deployer import extract_commit_snapshot
        extract_commit_snapshot(src_repo, source_commit, worktree_dir)
        canonical_test = src_repo / "tests" / "test_contract_scenarios.py"
        target_test = worktree_dir / "tests" / "test_contract_scenarios.py"
        if canonical_test.exists() and target_test.parent.exists():
            shutil.copy2(canonical_test, target_test)
    else:
        shutil.copytree(src_repo, worktree_dir)
    return worktree_dir


async def start_agent_b_checkout_task(
    contract_id: str,
    base_dir: Optional[Path] = None,
    task_id: Optional[str] = None,
) -> dict[str, Any]:
    """Agent B registers an in-flight task with an isolated worktree to build checkout assuming Billing Revision 1."""
    root_dir = base_dir or Path(__file__).parent.parent
    src_repo = root_dir / "repos" / "orders-service"
    if not src_repo.exists():
        src_repo = Path(__file__).parent.parent / "repos" / "orders-service"
    commit_sha = get_service_git_commit(src_repo)

    # Server-generated opaque UUID per task to guarantee absolute worktree isolation
    task_uuid = task_id or str(uuid.uuid4())
    worktree_path = create_agent_b_worktree(
        task_id=task_uuid,
        base_dir=root_dir,
        source_commit=commit_sha if commit_sha and not commit_sha.startswith("commit-untracked") else None,
    )


    task = await create_agent_task(
        agent_id="agent-b-orders-engineer",
        service_name="orders-service",
        task_summary="Implement checkout compatibility with Billing Service",
        worktree_path=str(worktree_path),
        base_commit=commit_sha,
        consumer_repository=str(src_repo),
        dependencies=[
            {
                "provider_service": "billing-service",
                "contract_id": contract_id,
                "assumed_revision": 1,
                "dependency_kind": "HTTP_REST",
                "dependency_path": "clients/billing_client.py",
                "http_method": "POST",
                "endpoint_path": "/v1/charges",
                "path_parameters": {},
                "query_parameters": {},
                "declared_headers": {"content-type": "application/json"},
                "request_body_schema": {
                    "type": "object", "required": ["amount", "card_token"],
                    "properties": {"amount": {"type": "integer"}, "currency": {"type": "string"}, "card_token": {"type": "string"}},
                },
                "response_schemas": {
                    "200": {"type": "object", "required": ["charge_id", "status", "amount", "currency", "card_token"]},
                },
                "consumer_source_file": "clients/billing_client.py",
                "consumer_source_evidence": {
                    "source_commit": commit_sha,
                    "content_sha256": hashlib.sha256((src_repo / "clients" / "billing_client.py").read_bytes()).hexdigest(),
                },
                "confirmation_status": "CONFIRMED",
                "confirmed_by": "agent-b-orders-engineer",
            }
        ],
    )
    task["worktree_path"] = str(worktree_path)
    return task



async def execute_agent_b_step_with_drift_check(
    task_id: str,
    step_name: str,
) -> dict[str, Any]:
    """Execute a single agent milestone step and perform checkpoint boundary drift check."""
    # 1. Check for active drift intervention at clean boundary
    drift = await check_task_drift(task_id)
    if drift:
        logger.warning(
            "Agent B intercepted drift intervention at checkpoint boundary '%s': %s",
            step_name, drift["breaking_diff"].get("diff_summary"),
        )
        return {
            "step": step_name,
            "drift_detected": True,
            "drift": drift,
        }

    # 2. Record clean milestone step checkpoint
    await save_agent_checkpoint(
        task_id=task_id,
        plan_revision=1,
        status="OPTIMISTIC_EXECUTING",
        checkpoint_state={"phase": step_name, "summary": f"Milestone {step_name}"},
    )

    return {
        "step": step_name,
        "drift_detected": False,
    }


def adapt_worktree_billing_client(worktree_dir: Path) -> Path:
    """Real code modification: Updates billing client in the worktree to use BillingClientV2 (payment_method_id)."""
    client_file = worktree_dir / "clients" / "billing_client.py"
    if not client_file.exists():
        raise FileNotFoundError(f"Client file {client_file} not found in worktree")

    content = client_file.read_text(encoding="utf-8")
    
    # Ensure default BillingClient alias points to BillingClientV2
    if "BillingClient = BillingClientV2" not in content:
        content += "\n\n# Reconciled default client\nBillingClient = BillingClientV2\n"
        client_file.write_text(content, encoding="utf-8")

    return client_file


def run_worktree_pytest(worktree_dir: Path, timeout_seconds: float = 30.0) -> dict[str, Any]:
    """Execute real pytest command in the worktree with sanitized environment and capture authoritative exit code and output."""
    test_path = worktree_dir / "tests" / "test_contract_scenarios.py"
    cmd = [sys.executable, "-m", "pytest", str(test_path), "-v"]

    from coordinator.deployer import build_isolated_pythonpath, get_sanitized_sandbox_env

    root_dir = Path(__file__).parent.parent
    billing_dir = str(root_dir / "repos" / "billing-service")
    orders_dir = str(worktree_dir)
    py_path = build_isolated_pythonpath(orders_dir, billing_dir, root_dir)

    test_env = get_sanitized_sandbox_env({
        "PYTHONPATH": py_path,
        "BILLING_SERVICE_PATH": str(root_dir / "repos" / "billing-service" / "main.py"),
    })

    res = subprocess.run(
        cmd,
        cwd=str(worktree_dir),
        capture_output=True,
        text=True,
        env=test_env,
        timeout=timeout_seconds,
    )

    return {

        "all_passed": res.returncode == 0,
        "returncode": res.returncode,
        "test_suite": str(test_path),
        "stdout": res.stdout[-2000:] if res.stdout else "",
        "stderr": res.stderr[-2000:] if res.stderr else "",
    }



async def adapt_agent_b_code_and_reconcile(
    task_id: str,
    drift_id: Optional[str] = None,
    worktree_path: Optional[str | Path] = None,
    auto_reconcile: bool = True,
    base_dir: Optional[Path] = None,
) -> dict[str, Any]:
    """Agent B replans, modifies real code in worktree, executes real pytest suite, and submits verified plan."""
    root_dir = base_dir or Path(__file__).parent.parent
    wt_dir = Path(worktree_path) if worktree_path else (root_dir / "worktrees" / "task-orders-checkout")

    # 1. Transition to REPLANNING
    await start_replanning(task_id)

    # 2. Real code modification in worktree
    adapted_file = adapt_worktree_billing_client(wt_dir)
    adapted_files = [str(adapted_file.relative_to(root_dir))]

    # 3. Real test execution in worktree
    test_results = run_worktree_pytest(wt_dir)
    if not test_results["all_passed"]:
        raise RuntimeError(
            f"Reconciliation test execution failed in worktree (exit code {test_results['returncode']}):\n{test_results['stdout']}\n{test_results['stderr']}"
        )

    plan_summary = "Upgraded BillingClient to v2 sending payment_method_id instead of legacy card_token. Verified via pytest."

    # 4. Submit reconciled plan with real test evidence
    submission = await submit_reconciled_plan(
        task_id=task_id,
        drift_id=drift_id,
        adapted_files=adapted_files,
        test_results=test_results,
        plan_summary=plan_summary,
        auto_reconcile=auto_reconcile,
    )

    submission["test_results"] = test_results
    return submission


# ==============================================================================
# 3. Complete End-to-End Multi-Agent Orchestration Flow
# ==============================================================================


async def run_full_reconciliation_workflow(
    auto_reconcile: bool = True,
    base_dir: Optional[Path] = None,
) -> dict[str, Any]:
    """Run the complete end-to-end multi-agent scenario with drift detection, real worktree code modification, and reconciliation."""
    root_dir = base_dir or Path(__file__).parent.parent

    # 1. Agent A publishes Billing v1
    res_a_v1 = await run_agent_a_publish_revision_1(root_dir)
    contract_id = res_a_v1["contract_id"]

    # 2. Agent B starts in-flight task assuming Billing v1 in isolated worktree
    task_b = await start_agent_b_checkout_task(contract_id=contract_id, base_dir=root_dir)
    task_id = task_b["task_id"]
    worktree_path = task_b["worktree_path"]

    # 3. Agent B executes Milestone 1 (clean, no drift)
    step1 = await execute_agent_b_step_with_drift_check(
        task_id=task_id,
        step_name="scaffold_checkout_route",
    )
    assert not step1["drift_detected"]

    # 4. Agent A publishes breaking Billing v2
    res_a_v2 = await run_agent_a_publish_revision_2(root_dir)
    inbox_event_id = str(uuid.uuid4())
    diff_payload = res_a_v2.get("schema_diff") or {
        "is_breaking": True,
        "breaking_changes": [{"field": "payment_token", "change": "required field added"}],
        "diff_summary": "Breaking change: payment_method_id required instead of card_token",
    }
    from coordinator.db import execute_query
    await execute_query(
        """INSERT INTO coordinator_outbox (
               event_id, aggregate_type, aggregate_id, aggregate_revision, source_service, event_type, payload
           ) VALUES (%s, 'CONTRACT_REVISION', %s, 2, 'billing-service', 'CONTRACT_CHANGED', %s::jsonb)
           ON CONFLICT (event_id) DO NOTHING;""",
        (inbox_event_id, str(contract_id), json.dumps({"contract_id": str(contract_id), "revision_number": 2, "schema_diff": diff_payload})),
    )
    from coordinator.drift_worker import ingest_changefeed_event
    await ingest_changefeed_event({
        "event_id": inbox_event_id,
        "event_type": "CONTRACT_CHANGED",
        "aggregate_type": "CONTRACT_REVISION",
        "aggregate_id": res_a_v2.get("contract_revision_id") or contract_id,
        "aggregate_revision": 2,
        "source_service": "billing-service",
        "payload": {
            "contract_id": str(contract_id),
            "revision_number": 2,
            "schema_diff": res_a_v2.get("schema_diff") or {"is_breaking": True, "breaking_changes": [{"field": "payment_token", "change": "required field added"}]},
        },
    })

    # 5. CockroachDB Changefeed / Drift Worker processes outbox event
    events_processed = await process_all_pending_events()

    # 6. Agent B reaches Milestone 2 checkpoint boundary -> Intercepts REPLAN_REQUIRED
    step2 = await execute_agent_b_step_with_drift_check(
        task_id=task_id,
        step_name="integrate_billing_client",
    )
    assert step2["drift_detected"]
    drift_info = step2["drift"]

    # 7. Agent B modifies real code in worktree, executes real pytest, and submits reconciled plan
    reconciliation = await adapt_agent_b_code_and_reconcile(
        task_id=task_id,
        drift_id=drift_info["drift_id"],
        worktree_path=worktree_path,
        auto_reconcile=auto_reconcile,
        base_dir=root_dir,
    )

    return {
        "contract_id": contract_id,
        "task_id": task_id,
        "drift_id": drift_info["drift_id"],
        "events_processed": events_processed,
        "reconciliation_status": reconciliation["status"],
        "auto_reconciled": reconciliation["auto_reconciled"],
        "test_results": reconciliation["test_results"],
    }
