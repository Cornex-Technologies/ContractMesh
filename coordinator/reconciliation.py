"""Checkpoint-Aware Reconciliation Engine & Approval State Machine.

Implements the formal agent reconciliation lifecycle:
  REPLAN_REQUIRED -> REPLANNING -> AWAITING_APPROVAL -> RECONCILED (or FAILED)

Enforces:
1. Checkpoint Milestone Delivery: Delivers structured REPLAN_REQUIRED diff payloads at clean
   step boundaries rather than interrupting mid-tool-call.
2. Fail-Closed Test Evidence Verification: Missing, unverified, or failing test results
   are rejected immediately.
3. Strict Conditional State Machine: Enforces valid state transitions (e.g. only REPLANNING -> AWAITING_APPROVAL/RECONCILED)
   using conditional SQL updates with row count assertions.
4. Human-In-The-Loop Approval Gate: Tasks pause in AWAITING_APPROVAL until approved by an operator,
   or auto-reconcile when DEMO_AUTO_RECONCILE=True for scripted video demos.
5. Transactional Consistency: All state transitions update active_agent_tasks, drift_events,
   and append to contract_audit_history and coordinator_outbox atomically.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Optional
import psycopg
from psycopg.rows import dict_row

from coordinator.config import settings
from coordinator.db import fetch_one, execute_query, run_transaction
from coordinator.memory import save_agent_checkpoint
from coordinator.http_dependencies import persist_http_interface_dependency

logger = logging.getLogger(__name__)


# ==============================================================================
# 1. Task Registration & Dependency Binding
# ==============================================================================


def normalize_task_summary(task_summary: str, *, max_length: int = 200) -> str:
    """Return a bounded operational task summary suitable for durable storage.

    Task intent may be held by the external harness, but CodeClaim persists only this
    short operational description.  There is deliberately no prompt fallback here.
    """
    if not isinstance(task_summary, str):
        raise ValueError("task_summary is required and must be a string")
    normalized = " ".join(task_summary.split())
    if not normalized:
        raise ValueError("task_summary is required and cannot be blank")
    return normalized[:max_length]


async def create_agent_task(
    agent_id: str,
    service_name: str,
    task_summary: str,
    worktree_path: str = "",
    base_commit: str = "",
    dependencies: Optional[list[dict[str, Any]]] = None,
    consumer_repository: Optional[str] = None,
    task_id: Optional[str] = None,
    branch_name: Optional[str] = None,
) -> dict[str, Any]:
    """Register a new optimistic coding task with confirmed contract assumptions."""
    clean_summary = normalize_task_summary(task_summary)
    initial_checkpoint = {
        "phase": "OPTIMISTIC_EXECUTING",
        "plan_revision": 1,
        "files_changed": [],
        "test_status": "NOT_RUN",
        "summary": clean_summary,
    }

    async def _tx(conn: psycopg.AsyncConnection) -> dict[str, Any]:
        async with conn.cursor(row_factory=dict_row) as cur:
            # 1. Insert task into active_agent_tasks
            if task_id:
                await cur.execute(
                    """
                    INSERT INTO active_agent_tasks (
                        task_id, agent_id, service_name, task_summary, worktree_path, branch_name,
                        base_commit, plan_revision, status, checkpoint_state
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, 1, 'OPTIMISTIC_EXECUTING', %s::jsonb)
                    ON CONFLICT (task_id) DO UPDATE SET
                        status = EXCLUDED.status,
                        task_summary = EXCLUDED.task_summary,
                        updated_at = now()
                    RETURNING task_id, status, plan_revision, created_at;
                    """,
                    (
                        task_id,
                        agent_id,
                        service_name,
                        clean_summary,
                        worktree_path,
                        branch_name or "main",
                        base_commit,
                        json.dumps(initial_checkpoint),
                    ),
                )
            else:
                await cur.execute(
                    """
                    INSERT INTO active_agent_tasks (
                        agent_id, service_name, task_summary, worktree_path, branch_name,
                        base_commit, plan_revision, status, checkpoint_state
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, 1, 'OPTIMISTIC_EXECUTING', %s::jsonb)
                    RETURNING task_id, status, plan_revision, created_at;
                    """,
                    (
                        agent_id,
                        service_name,
                        clean_summary,
                        worktree_path,
                        branch_name or "main",
                        base_commit,
                        json.dumps(initial_checkpoint),
                    ),
                )
            task_row = await cur.fetchone()
            if not task_row:
                raise RuntimeError("Failed to create active agent task")
            effective_task_id = str(task_row["task_id"])

            # 2. Insert dependencies
            bound_deps = []
            if dependencies:
                for dep in dependencies:
                    provider = dep["provider_service"]
                    contract_id = dep["contract_id"]
                    assumed_rev = dep.get("assumed_revision", 1)
                    dep_kind = dep.get("dependency_kind", "HTTP_REST")
                    dep_path = dep.get("dependency_path", "clients/billing_client.py")
                    interface_dependency_id = dep.get("interface_dependency_id")
                    if interface_dependency_id is None and "http_method" in dep:
                        if not consumer_repository:
                            raise ValueError("consumer_repository is required for exact HTTP dependency registration")
                        interface_dependency_id = await persist_http_interface_dependency(
                            cur, dependency=dep, consumer_service=service_name,
                            consumer_repository=consumer_repository,
                        )
                    if interface_dependency_id is None:
                        raise ValueError(
                            "Every v1 task dependency must include an exact HTTP interface or an "
                            "existing interface_dependency_id"
                        )

                    await cur.execute(
                        """
                        SELECT confirmation_status, provider_service, consumer_service,
                               contract_id, assumed_provider_revision, http_method, endpoint_path
                        FROM http_interface_dependencies
                        WHERE dependency_id=%s
                        FOR UPDATE;
                        """,
                        (interface_dependency_id,),
                    )
                    conf_row = await cur.fetchone()
                    if not conf_row:
                        raise ValueError(f"Task dependency '{provider}' references an unknown HTTP dependency")
                    if conf_row.get("confirmation_status") != "CONFIRMED":
                        raise ValueError(
                            f"Task dependency '{provider}' cannot be bound: confirmation_status is not CONFIRMED"
                        )
                    if (
                        str(conf_row.get("provider_service")) != str(provider)
                        or str(conf_row.get("consumer_service")) != str(service_name)
                        or str(conf_row.get("contract_id")) != str(contract_id)
                        or int(conf_row.get("assumed_provider_revision")) != int(assumed_rev)
                        or (dep.get("http_method") and str(conf_row.get("http_method")).upper() != str(dep["http_method"]).upper())
                        or (dep.get("endpoint_path") and str(conf_row.get("endpoint_path")) != str(dep["endpoint_path"]))
                    ):
                        raise ValueError("Task dependency does not match its confirmed HTTP interface record")

                    await cur.execute(
                        """
                        INSERT INTO task_contract_dependencies (
                            task_id, provider_service, contract_id, assumed_revision,
                            dependency_kind, dependency_path, interface_dependency_id
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (task_id, provider_service, contract_id) DO UPDATE SET
                            assumed_revision = EXCLUDED.assumed_revision,
                            interface_dependency_id = EXCLUDED.interface_dependency_id
                        RETURNING provider_service, assumed_revision;
                        """,
                        (effective_task_id, provider, contract_id, assumed_rev, dep_kind, dep_path, interface_dependency_id),
                    )
                    bound_deps.append({
                        "provider_service": provider,
                        "contract_id": str(contract_id),
                        "assumed_revision": assumed_rev,
                        "interface_dependency_id": interface_dependency_id,
                    })

            # Outbox and audit events carry structured summaries only, strictly no raw prompts
            await cur.execute(
                """INSERT INTO coordinator_outbox (
                       aggregate_type, aggregate_id, aggregate_revision, source_service, event_type, payload
                   ) VALUES ('TASK_STATE', %s, 1, %s, 'TASK_REGISTERED', %s::jsonb)
                   RETURNING event_id;""",
                (effective_task_id, service_name, json.dumps({
                    "task_id": effective_task_id, "agent_id": agent_id, "service_name": service_name,
                    "task_summary": clean_summary, "branch_name": branch_name or "main",
                    "base_commit": base_commit, "dependencies": bound_deps,
                })),
            )
            outbox_row = await cur.fetchone()
            if not outbox_row or not outbox_row.get("event_id"):
                raise RuntimeError("TASK_REGISTERED outbox event was not created")
            outbox_id = outbox_row["event_id"]
            await cur.execute(
                """INSERT INTO contract_audit_history (
                       event_type, source_service, summary, actor,
                       outbox_event_id, causation_id, correlation_id
                   ) VALUES ('TASK_REGISTERED', %s, %s, %s, %s, %s, %s);""",
                (service_name, f"{agent_id} registered task {effective_task_id}: {clean_summary}", agent_id,
                 outbox_id, outbox_id, outbox_id),
            )

            return {
                "task_id": effective_task_id,
                "agent_id": agent_id,
                "service_name": service_name,
                "task_summary": clean_summary,
                "branch_name": branch_name or "main",
                "status": task_row["status"],
                "plan_revision": task_row["plan_revision"],
                "dependencies": bound_deps,
            }

    return await run_transaction(_tx)


# ==============================================================================
# 2. Checkpoint Boundary Drift Interception
# ==============================================================================


async def check_task_drift(task_id: str) -> Optional[dict[str, Any]]:
    """Query whether an active drift intervention exists for this task at a clean checkpoint boundary."""
    sql = """
    SELECT 
        d.drift_id,
        d.source_service,
        d.target_task_id,
        d.target_service,
        d.old_contract_revision,
        d.new_contract_revision,
        d.breaking_diff,
        d.status AS drift_status,
        t.status AS task_status,
        t.plan_revision
    FROM drift_events d
    JOIN active_agent_tasks t ON d.target_task_id = t.task_id
    WHERE d.target_task_id = %s
      AND d.status = 'ACTIVE_INTERVENTION'
    ORDER BY d.created_at DESC
    LIMIT 1;
    """
    row = await fetch_one(sql, (task_id,))
    if not row:
        return None

    diff = row.get("breaking_diff")
    if isinstance(diff, str):
        try:
            diff = json.loads(diff)
        except Exception:
            pass

    migration_notes = ""
    if isinstance(diff, dict):
        migration_notes = (
            diff.get("migration_note")
            or diff.get("migration_notes")
            or diff.get("diff_summary")
            or diff.get("summary")
            or ""
        )

    return {
        "drift_id": str(row["drift_id"]),
        "task_id": str(row["target_task_id"]),
        "source_service": row["source_service"],
        "target_service": row["target_service"],
        "old_contract_revision": row["old_contract_revision"],
        "new_contract_revision": row["new_contract_revision"],
        "breaking_diff": diff,
        "migration_notes": migration_notes,
        "audit_ids": {
            "drift_id": str(row["drift_id"]),
            "task_id": str(row["target_task_id"]),
            "source_service": row["source_service"],
            "target_service": row["target_service"],
        },
        "drift_status": row["drift_status"],
        "task_status": row["task_status"],
        "plan_revision": row["plan_revision"],
    }


# ==============================================================================
# 3. State Machine Transitions with Conditional SQL
# ==============================================================================


async def start_replanning(task_id: str) -> dict[str, Any]:
    """Transition task to REPLANNING from REPLAN_REQUIRED or OPTIMISTIC_EXECUTING, incrementing plan revision."""
    async def _tx(conn: psycopg.AsyncConnection) -> dict[str, Any]:
        async with conn.cursor(row_factory=dict_row) as cur:
            # 1. Fetch and lock task
            await cur.execute(
                """
                SELECT task_id, status, plan_revision 
                FROM active_agent_tasks 
                WHERE task_id = %s 
                FOR UPDATE;
                """,
                (task_id,),
            )
            task = await cur.fetchone()
            if not task:
                raise ValueError(f"Task {task_id} not found")

            # Must be in an active execution or replan-required status
            if task["status"] not in ("REPLAN_REQUIRED", "OPTIMISTIC_EXECUTING", "REPLANNING"):
                raise ValueError(
                    f"Task {task_id} cannot start replanning from current status '{task['status']}'"
                )

            new_plan_revision = task["plan_revision"] + 1
            new_state = {
                "phase": "REPLANNING",
                "plan_revision": new_plan_revision,
                "summary": "Replanning in response to upstream breaking contract drift",
            }

            # 2. Update task status to REPLANNING conditionally
            await cur.execute(
                """
                UPDATE active_agent_tasks
                SET 
                    status = 'REPLANNING',
                    plan_revision = %s,
                    checkpoint_state = %s::jsonb,
                    updated_at = now()
                WHERE task_id = %s
                RETURNING task_id, status, plan_revision;
                """,
                (new_plan_revision, json.dumps(new_state), task_id),
            )
            updated_task = await cur.fetchone()

            # 3. Emit a durable state event, then link the audit record to it.
            await cur.execute(
                """
                INSERT INTO coordinator_outbox (
                    aggregate_type, aggregate_id, aggregate_revision,
                    source_service, event_type, payload
                ) VALUES ('AGENT_TASK', %s, %s, 'coordinator', 'TASK_REPLAN_STARTED', %s::jsonb)
                RETURNING event_id;
                """,
                (task_id, new_plan_revision, json.dumps({
                    "task_id": task_id,
                    "plan_revision": new_plan_revision,
                    "status": "REPLANNING",
                })),
            )
            outbox_row = await cur.fetchone()
            if not outbox_row or not outbox_row.get("event_id"):
                raise RuntimeError("TASK_REPLAN_STARTED outbox event was not created")
            outbox_id = outbox_row["event_id"]

            await cur.execute(
                """
                INSERT INTO contract_audit_history (
                    event_type, source_service, summary, actor,
                    outbox_event_id, causation_id, correlation_id
                )
                VALUES ('TASK_REPLAN_STARTED', 'coordinator', %s, 'agent-reconciler', %s, %s, %s);
                """,
                (f"Agent started replanning task {task_id} (revision {new_plan_revision})",
                 outbox_id, outbox_id, outbox_id),
            )

            return {
                "task_id": str(task_id),
                "status": updated_task["status"],
                "plan_revision": updated_task["plan_revision"],
            }

    return await run_transaction(_tx)


async def _update_task_dependency_binding(
    cur: psycopg.AsyncCursor,
    task_id: str,
    provider_service: str,
    new_revision: int,
    confirmed_by: str,
) -> None:
    """Atomically record confirmed exact interface dependency for new revision and rebind task dependency."""
    await cur.execute(
        """
        SELECT h.*
        FROM http_interface_dependencies h
        JOIN task_contract_dependencies d ON d.interface_dependency_id = h.dependency_id
        WHERE d.task_id = %s AND d.provider_service = %s
        LIMIT 1;
        """,
        (task_id, provider_service),
    )
    cur_dep = await cur.fetchone()
    if cur_dep:
        await cur.execute(
            """
            INSERT INTO http_interface_dependencies (
                provider_service, consumer_service, contract_id, assumed_provider_revision,
                http_method, endpoint_path, path_parameters, query_parameters, declared_headers,
                request_body_schema, response_schemas, consumer_repository, consumer_source_file,
                consumer_source_evidence, confirmation_status, confirmed_by, confirmed_at
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s::jsonb, %s::jsonb, %s::jsonb,
                %s::jsonb, %s::jsonb, %s, %s,
                %s::jsonb, 'CONFIRMED', %s, now()
            )
            ON CONFLICT (consumer_service, provider_service, contract_id, assumed_provider_revision, consumer_repository, consumer_source_file)
            DO UPDATE SET confirmation_status = 'CONFIRMED', confirmed_by = EXCLUDED.confirmed_by, confirmed_at = EXCLUDED.confirmed_at, updated_at = now()
            RETURNING dependency_id;
            """,
            (
                cur_dep["provider_service"],
                cur_dep["consumer_service"],
                cur_dep["contract_id"],
                new_revision,
                cur_dep["http_method"],
                cur_dep["endpoint_path"],
                json.dumps(cur_dep.get("path_parameters") or {}),
                json.dumps(cur_dep.get("query_parameters") or {}),
                json.dumps(cur_dep.get("declared_headers") or {}),
                json.dumps(cur_dep.get("request_body_schema") or {}),
                json.dumps(cur_dep.get("response_schemas") or {}),
                cur_dep["consumer_repository"],
                cur_dep["consumer_source_file"],
                json.dumps(cur_dep.get("consumer_source_evidence") or {}),
                confirmed_by,
            ),
        )
        new_dep_row = await cur.fetchone()
        new_dep_id = new_dep_row["dependency_id"] if new_dep_row else cur_dep["dependency_id"]
        await cur.execute(
            """
            UPDATE task_contract_dependencies
            SET assumed_revision = %s,
                interface_dependency_id = %s
            WHERE task_id = %s AND provider_service = %s;
            """,
            (new_revision, new_dep_id, task_id, provider_service),
        )


async def submit_reconciled_plan(
    task_id: str,
    drift_id: str,
    adapted_files: list[str],
    test_results: dict[str, Any],
    plan_summary: str,
    auto_reconcile: Optional[bool] = None,
) -> dict[str, Any]:
    """Submit adapted code plan and verified test results.
    
    Enforces fail-closed test validation and strict conditional transition:
    - Rejects if test_results is missing, all_passed is not True, or exit code != 0.
    - Requires task to currently be in REPLANNING status.
    - Requires drift_id to match an active drift event for this task.
    """
    # 1. Fail-closed test evidence verification
    if not test_results or not isinstance(test_results, dict):
        raise ValueError("Reconciliation plan rejected: test_results evidence dictionary is required.")
    if test_results.get("all_passed") is not True:
        raise ValueError("Reconciliation plan rejected: test suite did not report all_passed=True.")
    if test_results.get("returncode", 0) != 0:
        raise ValueError(
            f"Reconciliation plan rejected: test suite exited with non-zero returncode {test_results.get('returncode')}."
        )

    should_auto_reconcile = (
        auto_reconcile if auto_reconcile is not None else settings.demo_auto_reconcile
    )

    target_status = "RECONCILED" if should_auto_reconcile else "AWAITING_APPROVAL"

    reconciled_plan_payload = {
        "drift_id": drift_id,
        "adapted_files": adapted_files,
        "test_results": test_results,
        "plan_summary": plan_summary,
        "auto_approved": should_auto_reconcile,
    }

    async def _tx(conn: psycopg.AsyncConnection) -> dict[str, Any]:
        async with conn.cursor(row_factory=dict_row) as cur:
            # 2. Verify drift event exists for this task
            if drift_id:
                try:
                    await cur.execute(
                        """
                        SELECT drift_id, source_service, new_contract_revision, status
                        FROM drift_events
                        WHERE drift_id = %s AND target_task_id = %s
                        FOR UPDATE;
                        """,
                        (drift_id, task_id),
                    )
                    drift_info = await cur.fetchone()
                except Exception:
                    drift_info = None
            else:
                drift_info = None

            if not drift_info:
                await cur.execute(
                    """
                    SELECT drift_id, source_service, new_contract_revision, status
                    FROM drift_events
                    WHERE target_task_id = %s
                    ORDER BY created_at DESC
                    LIMIT 1
                    FOR UPDATE;
                    """,
                    (task_id,),
                )
                drift_info = await cur.fetchone()

            if not drift_info:
                raise ValueError(f"No drift event found for task {task_id}")

            # 3. Update task status conditionally (only allowed from REPLANNING)
            await cur.execute(
                """
                UPDATE active_agent_tasks
                SET 
                    status = %s,
                    checkpoint_state = jsonb_set(
                        COALESCE(checkpoint_state, '{}'::jsonb),
                        '{reconciled_plan}',
                        %s::jsonb
                    ),
                    last_reconciled_at = CASE WHEN %s THEN now() ELSE last_reconciled_at END,
                    updated_at = now()
                WHERE task_id = %s AND status = 'REPLANNING'
                RETURNING task_id, status, plan_revision;
                """,
                (target_status, json.dumps(reconciled_plan_payload), should_auto_reconcile, task_id),
            )
            task_row = await cur.fetchone()
            if not task_row:
                raise ValueError(
                    f"Task {task_id} must be in REPLANNING status to submit a reconciled plan."
                )

            # 4. Update drift event if auto-reconciled
            if should_auto_reconcile:
                await cur.execute(
                    """
                    UPDATE drift_events
                    SET 
                        status = 'RECONCILED',
                        reconciled_at = now(),
                        resolved_by = 'agent-auto-reconciler',
                        resolution_summary = %s,
                        updated_at = now()
                    WHERE drift_id = %s;
                    """,
                    (plan_summary, drift_id),
                )
                # Rebind confirmed exact http interface dependency and task dependency
                await _update_task_dependency_binding(
                    cur,
                    task_id=task_id,
                    provider_service=drift_info["source_service"],
                    new_revision=drift_info["new_contract_revision"],
                    confirmed_by="agent-auto-reconciler",
                )
            else:
                await cur.execute(
                    """
                    UPDATE drift_events
                    SET 
                        resolution_summary = %s,
                        updated_at = now()
                    WHERE drift_id = %s;
                    """,
                    (plan_summary, drift_id),
                )

            # 5. Emit the plan state event, then write its causally linked audit record.
            event_type = "TASK_RECONCILED" if should_auto_reconcile else "PLAN_AWAITING_APPROVAL"
            await cur.execute(
                """
                INSERT INTO coordinator_outbox (
                    aggregate_type, aggregate_id, aggregate_revision,
                    source_service, event_type, payload
                ) VALUES ('AGENT_TASK', %s, %s, 'coordinator', %s, %s::jsonb)
                RETURNING event_id;
                """,
                (task_id, task_row["plan_revision"], event_type, json.dumps({
                    "task_id": task_id,
                    "drift_id": drift_id,
                    "plan_revision": task_row["plan_revision"],
                    "status": target_status,
                    "plan_summary": plan_summary[:500],
                })),
            )
            outbox_row = await cur.fetchone()
            if not outbox_row or not outbox_row.get("event_id"):
                raise RuntimeError(f"{event_type} outbox event was not created")
            outbox_id = outbox_row["event_id"]

            await cur.execute(
                """
                INSERT INTO contract_audit_history (
                    event_type, source_service, summary, actor,
                    outbox_event_id, causation_id, correlation_id
                )
                VALUES (%s, 'coordinator', %s, %s, %s, %s, %s);
                """,
                (
                    event_type,
                    f"Task {task_id} plan submitted: {plan_summary}",
                    "agent-auto" if should_auto_reconcile else "agent-proposer",
                    outbox_id, outbox_id, outbox_id,
                ),
            )

            return {
                "task_id": str(task_id),
                "drift_id": str(drift_id),
                "status": target_status,
                "plan_revision": task_row["plan_revision"],
                "auto_reconciled": should_auto_reconcile,
                "outbox_event_id": outbox_id,
            }

    return await run_transaction(_tx)


async def approve_reconciled_plan(
    task_id: str,
    approved_by: str = "operator-human",
) -> dict[str, Any]:
    """Human approval action: Transition task from AWAITING_APPROVAL to RECONCILED with conditional SQL."""
    async def _tx(conn: psycopg.AsyncConnection) -> dict[str, Any]:
        async with conn.cursor(row_factory=dict_row) as cur:
            # 1. Update task status conditionally (fails if not in AWAITING_APPROVAL)
            await cur.execute(
                """
                UPDATE active_agent_tasks
                SET 
                    status = 'RECONCILED',
                    last_reconciled_at = now(),
                    updated_at = now()
                WHERE task_id = %s AND status = 'AWAITING_APPROVAL'
                RETURNING task_id, status, plan_revision;
                """,
                (task_id,),
            )
            updated_task = await cur.fetchone()
            if not updated_task:
                raise ValueError(
                    f"Task {task_id} cannot be approved because it is not in AWAITING_APPROVAL status"
                )

            # 2. Mark corresponding drift event as RECONCILED
            await cur.execute(
                """
                UPDATE drift_events
                SET 
                    status = 'RECONCILED',
                    reconciled_at = now(),
                    resolved_by = %s,
                    acknowledged = true,
                    updated_at = now()
                WHERE target_task_id = %s AND status = 'ACTIVE_INTERVENTION'
                RETURNING drift_id, source_service, new_contract_revision;
                """,
                (approved_by, task_id),
            )
            drift_row = await cur.fetchone()

            # 3. Update assumed contract revision
            if drift_row:
                await _update_task_dependency_binding(
                    cur,
                    task_id=task_id,
                    provider_service=drift_row["source_service"],
                    new_revision=drift_row["new_contract_revision"],
                    confirmed_by=approved_by,
                )

            # 4. Emit outbox and log audit history with causal lineage
            await cur.execute(
                """
                INSERT INTO coordinator_outbox (
                    aggregate_type, aggregate_id, aggregate_revision, source_service, event_type, payload
                ) VALUES ('AGENT_TASK', %s, %s, 'coordinator', 'PLAN_APPROVED', %s::jsonb)
                RETURNING event_id;
                """,
                (task_id, updated_task["plan_revision"], json.dumps({"task_id": task_id, "approved_by": approved_by, "status": "RECONCILED"})),
            )
            outbox_row = await cur.fetchone()
            if not outbox_row or not outbox_row.get("event_id"):
                raise RuntimeError("PLAN_APPROVED outbox event was not created")
            outbox_id = str(outbox_row["event_id"])

            await cur.execute(
                """
                INSERT INTO contract_audit_history (
                    event_type, source_service, summary, actor, outbox_event_id, causation_id, correlation_id
                )
                VALUES ('PLAN_APPROVED', 'coordinator', %s, %s, %s, %s, %s);
                """,
                (f"Operator '{approved_by}' approved reconciled plan for task {task_id}", approved_by, outbox_id, outbox_id, outbox_id),
            )

            return {
                "task_id": str(task_id),
                "status": "RECONCILED",
                "plan_revision": updated_task["plan_revision"],
                "approved_by": approved_by,
                "outbox_event_id": outbox_id,
            }

    return await run_transaction(_tx)


async def reject_reconciled_plan(
    task_id: str,
    rejection_reason: str,
    rejected_by: str = "operator-human",
) -> dict[str, Any]:
    """Return a task to REPLANNING with bounded operator feedback."""
    async def _tx(conn: psycopg.AsyncConnection) -> dict[str, Any]:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT task_id, status, plan_revision FROM active_agent_tasks WHERE task_id = %s FOR UPDATE;",
                (task_id,),
            )
            task = await cur.fetchone()
            if not task:
                raise ValueError(f"Task {task_id} not found")
            if task["status"] != "AWAITING_APPROVAL":
                raise ValueError(
                    f"Task {task_id} cannot be rejected from status '{task['status']}'; must be AWAITING_APPROVAL"
                )

            new_plan_rev = task["plan_revision"] + 1
            new_state = {
                "node": "replan",
                "rejection_feedback": rejection_reason,
                "rejection_reason": rejection_reason,
            }

            await cur.execute(
                """
                UPDATE active_agent_tasks
                SET 
                    status = 'REPLANNING',
                    plan_revision = %s,
                    checkpoint_state = %s::jsonb,
                    updated_at = now()
                WHERE task_id = %s AND status = 'AWAITING_APPROVAL'
                RETURNING task_id, status, plan_revision;
                """,
                (new_plan_rev, json.dumps(new_state), task_id),
            )
            updated_task = await cur.fetchone()

            # Emit outbox and log audit history with causal lineage
            await cur.execute(
                """
                INSERT INTO coordinator_outbox (
                    aggregate_type, aggregate_id, aggregate_revision, source_service, event_type, payload
                ) VALUES ('AGENT_TASK', %s, %s, 'coordinator', 'PLAN_REJECTED', %s::jsonb)
                RETURNING event_id;
                """,
                (task_id, new_plan_rev, json.dumps({"task_id": task_id, "rejected_by": rejected_by, "rejection_reason": rejection_reason})),
            )
            outbox_row = await cur.fetchone()
            if not outbox_row or not outbox_row.get("event_id"):
                raise RuntimeError("PLAN_REJECTED outbox event was not created")
            outbox_id = str(outbox_row["event_id"])

            await cur.execute(
                """
                INSERT INTO contract_audit_history (
                    event_type, source_service, summary, actor, outbox_event_id, causation_id, correlation_id
                )
                VALUES ('PLAN_REJECTED', 'coordinator', %s, %s, %s, %s, %s);
                """,
                (f"Plan rejected for task {task_id}: {rejection_reason}", rejected_by, outbox_id, outbox_id, outbox_id),
            )

            return {
                "task_id": str(task_id),
                "status": "REPLANNING",
                "plan_revision": updated_task["plan_revision"],
                "rejection_reason": rejection_reason,
                "outbox_event_id": outbox_id,
            }

    return await run_transaction(_tx)
