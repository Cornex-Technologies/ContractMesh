"""Harness-neutral compatibility work creation, claiming, and evidence recording.

The coordinator owns state transitions; coding harnesses only receive and report work.
Every externally visible action is represented by a durable work item and outbox event.
"""

from __future__ import annotations

import hashlib
import json
import secrets
from typing import Any, Optional

import psycopg
from psycopg.rows import dict_row

from coordinator.db import fetch_one, run_transaction
from coordinator.reconciliation import create_agent_task, normalize_task_summary


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def build_compatibility_coordination_key(
    *, source_contract_id: Any, source_contract_revision: int, interface_dependency_id: Any
) -> str:
    """Return the stable identity of one consumer obligation for one provider revision."""
    return f"compat:{source_contract_id}:{int(source_contract_revision)}:{interface_dependency_id}"


async def _lock_owned_task_with_optional_work(
    cur: Any, *, task_id: str, agent_id: str
) -> dict[str, Any] | None:
    """Lock an owned task and then its optional compatibility work item.

    CockroachDB does not allow ``FOR UPDATE`` on the nullable side of an outer
    join.  Task checkpoints and normal task completion need to lock the task
    while also determining whether it belongs to a compatibility obligation,
    so those reads must be separate statements.  Keeping both statements in
    the caller's transaction preserves the same atomic ownership invariant.
    """
    await cur.execute(
        """
        SELECT task_id, service_name, status, plan_revision
        FROM active_agent_tasks
        WHERE task_id=%s AND agent_id=%s
        FOR UPDATE;
        """,
        (task_id, agent_id),
    )
    task = await cur.fetchone()
    if not task:
        return None

    await cur.execute(
        """
        SELECT work_item_id
        FROM compatibility_work_items
        WHERE task_id=%s
        ORDER BY created_at ASC
        LIMIT 1
        FOR UPDATE;
        """,
        (task_id,),
    )
    work = await cur.fetchone()
    task = dict(task)
    task["work_item_id"] = work.get("work_item_id") if work else None
    return task


async def register_harness(
    *,
    harness_name: str,
    harness_type: str,
    service_name: str,
    repository_url: str,
    dispatch_mode: str = "poll",
    dispatch_url: Optional[str] = None,
    capability_manifest: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Register a single service-owning runner and return its token exactly once."""
    if dispatch_mode not in {"poll", "webhook"}:
        raise ValueError("dispatch_mode must be 'poll' or 'webhook'")
    if dispatch_mode == "webhook" and not dispatch_url:
        raise ValueError("dispatch_url is required for webhook harnesses")

    token = secrets.token_urlsafe(32)

    async def _tx(conn: psycopg.AsyncConnection) -> dict[str, Any]:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                INSERT INTO harness_registrations (
                    harness_name, harness_type, service_name, repository_url,
                    dispatch_mode, dispatch_url, capability_manifest, access_token_hash
                ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                RETURNING harness_id, harness_name, harness_type, service_name,
                          repository_url, dispatch_mode, dispatch_url, status;
                """,
                (harness_name, harness_type, service_name, repository_url, dispatch_mode,
                 dispatch_url, json.dumps(capability_manifest or {}), _token_hash(token)),
            )
            row = await cur.fetchone()
            if not row:
                raise RuntimeError("Failed to register harness")
            await cur.execute(
                """INSERT INTO coordinator_outbox (
                       aggregate_type, aggregate_id, aggregate_revision,
                       source_service, event_type, payload
                   ) VALUES ('HARNESS', %s, 1, %s, 'HARNESS_REGISTERED', %s::jsonb)
                   RETURNING event_id;""",
                (row["harness_id"], service_name, json.dumps({
                    "harness_id": str(row["harness_id"]),
                    "harness_name": harness_name,
                    "harness_type": harness_type,
                    "service_name": service_name,
                    "repository_url": repository_url,
                })),
            )
            outbox = await cur.fetchone()
            if not outbox or not outbox.get("event_id"):
                raise RuntimeError("Failed to create HARNESS_REGISTERED outbox event")
            outbox_id = outbox["event_id"]
            await cur.execute(
                """INSERT INTO contract_audit_history (
                       event_type, source_service, summary, actor,
                       outbox_event_id, causation_id, correlation_id
                   ) VALUES ('HARNESS_REGISTERED', %s, %s, %s, %s, %s, %s);""",
                (service_name, f"Registered {harness_type} harness '{harness_name}'", "operator",
                 outbox_id, outbox_id, outbox_id),
            )
            return {**dict(row), "access_token": token}

    return await run_transaction(_tx)


async def disable_harness(harness_id: str, *, actor: str = "operator") -> dict[str, Any]:
    """Disable a harness token without deleting its durable execution history.

    Harness registrations are security principals, not historical work items.  A
    compromised or rotated token must therefore be invalidated in place while
    retaining the harness identity on completed tasks and audit records.
    """

    async def _tx(conn: psycopg.AsyncConnection) -> dict[str, Any]:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT harness_id, harness_name, harness_type, service_name, status
                FROM harness_registrations
                WHERE harness_id = %s
                FOR UPDATE;
                """,
                (harness_id,),
            )
            harness = await cur.fetchone()
            if not harness:
                raise ValueError("Harness is unknown")
            if harness["status"] == "DISABLED":
                return {**dict(harness), "status": "DISABLED", "already_disabled": True}

            await cur.execute(
                """
                UPDATE harness_registrations
                SET status = 'DISABLED', updated_at = now()
                WHERE harness_id = %s
                RETURNING harness_id, harness_name, harness_type, service_name, status;
                """,
                (harness_id,),
            )
            updated = await cur.fetchone()
            if not updated:
                raise RuntimeError("Failed to disable harness")

            payload = {
                "harness_id": str(updated["harness_id"]),
                "harness_name": updated["harness_name"],
                "harness_type": updated["harness_type"],
                "service_name": updated["service_name"],
                "previous_status": harness["status"],
                "status": "DISABLED",
                "actor": actor,
            }
            await cur.execute(
                """
                INSERT INTO coordinator_outbox (
                    aggregate_type, aggregate_id, aggregate_revision,
                    source_service, event_type, payload
                ) VALUES ('HARNESS', %s, 1, %s, 'HARNESS_DISABLED', %s::jsonb)
                RETURNING event_id;
                """,
                (updated["harness_id"], updated["service_name"], json.dumps(payload)),
            )
            outbox = await cur.fetchone()
            if not outbox or not outbox.get("event_id"):
                raise RuntimeError("Failed to create HARNESS_DISABLED outbox event")

            await cur.execute(
                """
                INSERT INTO contract_audit_history (
                    event_type, source_service, summary, actor,
                    outbox_event_id, causation_id, correlation_id
                ) VALUES ('HARNESS_DISABLED', %s, %s, %s, %s, %s, %s);
                """,
                (
                    updated["service_name"],
                    f"Disabled {updated['harness_type']} harness '{updated['harness_name']}'",
                    actor,
                    outbox["event_id"],
                    outbox["event_id"],
                    outbox["event_id"],
                ),
            )
            return {**dict(updated), "already_disabled": False}

    return await run_transaction(_tx)


async def authenticate_harness(harness_id: str, token: str) -> dict[str, Any]:
    row = await fetch_one(
        """SELECT harness_id, harness_name, harness_type, service_name, repository_url,
                  status, access_token_hash
             FROM harness_registrations WHERE harness_id = %s;""",
        (harness_id,),
    )
    if not row or row["status"] != "ACTIVE":
        raise PermissionError("Harness is unknown or inactive")
    if not secrets.compare_digest(row["access_token_hash"], _token_hash(token)):
        raise PermissionError("Invalid harness token")
    row.pop("access_token_hash", None)
    return row


async def register_harness_task(
    harness: dict[str, Any], *, task_summary: str, worktree_path: str, base_commit: str,
    dependencies: list[dict[str, Any]],
    task_id: Optional[str] = None,
) -> dict[str, Any]:
    """Register a normal optimistic task before the harness begins editing."""
    if not dependencies:
        raise ValueError("At least one explicit contract dependency is required")
    return await create_agent_task(
        agent_id=f"{harness['harness_type']}:{harness['harness_name']}",
        service_name=harness["service_name"], task_summary=task_summary,
        worktree_path=worktree_path, base_commit=base_commit, dependencies=dependencies,
        consumer_repository=harness["repository_url"],
        task_id=task_id,
    )



async def create_compatibility_work_for_contract_change(
    conn: psycopg.AsyncConnection,
    *,
    source_event_id: Any,
    contract_id: Any,
    source_service: str,
    revision_number: int,
    schema_diff: dict[str, Any],
) -> list[dict[str, Any]]:
    """Create idempotent work for registered consumers after a breaking contract change.

    Runs within the contract-event transaction, so a committed change cannot exist without
    its compatibility work and outbox records.
    """
    requires_review = schema_diff.get("classification") == "REVIEW_REQUIRED"
    if not schema_diff.get("is_breaking") and not requires_review:
        return []
    created: list[dict[str, Any]] = []
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """INSERT INTO coordinator_outbox (
                   event_id, aggregate_type, aggregate_id, aggregate_revision, source_service, event_type, payload
               ) VALUES (%s, 'SERVICE_CONTRACT', %s, %s, %s, 'CONTRACT_CHANGED', %s::jsonb)
               ON CONFLICT (event_id) DO NOTHING;""",
            (source_event_id, str(contract_id), revision_number, source_service, json.dumps({"contract_id": str(contract_id), "revision_number": revision_number, "schema_diff": schema_diff})),
        )

        await cur.execute(
            """
            SELECT d.dependency_id, d.assumed_provider_revision, d.consumer_service, d.consumer_repository,
                   d.http_method, d.endpoint_path,
                   h.harness_id, h.harness_name
            FROM http_interface_dependencies d
            LEFT JOIN harness_registrations h
              ON h.service_name = d.consumer_service
             AND h.repository_url = d.consumer_repository
             AND h.status = 'ACTIVE'
            WHERE d.contract_id = %s AND d.confirmation_status = 'CONFIRMED';
            """,
            (contract_id,),
        )
        consumers = await cur.fetchall()
        for consumer in consumers:
            target_service = consumer.get("consumer_service")
            target_repo = consumer.get("consumer_repository")
            interface_dependency_id = consumer.get("dependency_id")
            # Historical coarse rows and incomplete test fixtures are not valid v1 HTTP dependencies.
            if not target_service or not target_repo or not interface_dependency_id:
                continue
            idempotency_key = f"compat:{source_event_id}:{target_service}:{target_repo}:{revision_number}"
            payload = {
                "source_event_id": str(source_event_id),
                "source_service": source_service,
                "contract_id": str(contract_id),
                "source_contract_revision": revision_number,
                "interface_dependency_id": str(interface_dependency_id),
                "http_method": consumer["http_method"],
                "endpoint_path": consumer["endpoint_path"],
                "consumer_assumed_revision": consumer["assumed_provider_revision"],
                "target_service": target_service,
                "target_repository": target_repo,
                "breaking_diff": schema_diff,
                "harness_registered": consumer["harness_id"] is not None,
                "classification": schema_diff.get("classification", "BREAKING"),
            }
            coordination_key = build_compatibility_coordination_key(
                source_contract_id=contract_id,
                source_contract_revision=revision_number,
                interface_dependency_id=interface_dependency_id,
            )
            await cur.execute(
                """
                INSERT INTO compatibility_work_items (
                    source_event_id, source_contract_id, source_contract_revision,
                    target_service, target_repository, harness_id, state, idempotency_key,
                    coordination_key, causation_id, correlation_id, payload
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (coordination_key) DO NOTHING
                RETURNING work_item_id, state, harness_id;
                """,
                (source_event_id, contract_id, revision_number, target_service, target_repo,
                 consumer["harness_id"], "REVIEW_REQUIRED" if requires_review else "PENDING",
                 idempotency_key, coordination_key, source_event_id, source_event_id,
                 json.dumps(payload)),
            )
            row = await cur.fetchone()
            if not row:
                continue
            work_item_id = row["work_item_id"]
            if requires_review:
                await cur.execute(
                    """INSERT INTO compatibility_incidents (
                           work_item_id, incident_type, evidence, requested_resolution
                       ) VALUES (%s, 'REVIEW_REQUIRED', %s::jsonb, %s);""",
                    (work_item_id, json.dumps({"review_reasons": schema_diff.get("review_reasons", []), "schema_diff": schema_diff}),
                     "Human review of publisher-declared or structurally uncertain compatibility change"),
                )
            await cur.execute(
                """
                INSERT INTO coordinator_outbox (
                    aggregate_type, aggregate_id, aggregate_revision, source_service, event_type, payload
                ) VALUES ('COMPATIBILITY_WORK', %s, %s, 'coordinator', 'COMPATIBILITY_WORK_CREATED', %s::jsonb)
                RETURNING event_id;
                """,
                (work_item_id, revision_number, json.dumps({**payload, "work_item_id": str(work_item_id)})),
            )
            outbox_row = await cur.fetchone()
            if not outbox_row or not outbox_row.get("event_id"):
                raise RuntimeError("COMPATIBILITY_WORK_CREATED outbox event was not created")
            outbox_id = outbox_row["event_id"]
            await cur.execute(
                """INSERT INTO contract_audit_history (event_type, source_service, summary, actor, outbox_event_id, causation_id, correlation_id)
                   VALUES ('COMPATIBILITY_WORK_CREATED', %s, %s, 'coordinator', %s, %s, %s);""",
                (source_service, f"Created compatibility work {work_item_id} for {target_service}", outbox_id, source_event_id, source_event_id),
            )
            created.append({"work_item_id": str(work_item_id), "target_service": target_service,
                            "harness_id": str(row["harness_id"]) if row["harness_id"] else None,
                            "state": row["state"]})
    return created


async def claim_next_work_item(harness_id: str, *, worktree_path: str, base_commit: str) -> Optional[dict[str, Any]]:
    """Atomically assign and lease one compatibility item for a harness.

    A work item may have been created before a compatible harness was connected.
    In that case the matching service/repository harness may claim the unassigned
    item.  The harness assignment, task creation, and audit/outbox event all live
    in one serializable transaction, so a racing harness cannot claim the same row.
    """
    async def _tx(conn: psycopg.AsyncConnection) -> Optional[dict[str, Any]]:
        async with conn.cursor(row_factory=dict_row) as cur:
            harness = await _fetch_harness_with_cursor(cur, harness_id)
            if harness.get("status") != "ACTIVE":
                raise ValueError("Harness is not active")
            await cur.execute(
                """
                SELECT * FROM compatibility_work_items
                WHERE state IN ('PENDING', 'DISPATCHED')
                  AND (
                        harness_id = %s
                        OR (
                            harness_id IS NULL
                            AND target_service = %s
                            AND target_repository = %s
                        )
                  )
                ORDER BY created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED;
                """, (harness_id, harness["service_name"], harness["repository_url"]),
            )
            work = await cur.fetchone()
            if not work:
                return None
            payload = work["payload"] if isinstance(work["payload"], dict) else json.loads(work["payload"])
            original_harness_id = work.get("harness_id")
            assignment_mode = "existing_assignment"
            if original_harness_id is None:
                # Keep this conditional even though the row is locked: it makes the
                # ownership invariant explicit and protects future query changes.
                await cur.execute(
                    """UPDATE compatibility_work_items
                       SET harness_id=%s, updated_at=now()
                       WHERE work_item_id=%s AND harness_id IS NULL
                       RETURNING harness_id;""",
                    (harness_id, work["work_item_id"]),
                )
                assigned = await cur.fetchone()
                if not assigned:
                    raise RuntimeError("Compatibility work was claimed by another harness")
                assignment_mode = "late_unassigned"
            task_summary = f"Make {work['target_service']} compatible with {payload['source_service']} contract revision {work['source_contract_revision']}"[:200]
            task = await _create_task_with_cursor(
                cur, agent_id=f"{harness['harness_type']}:{harness['harness_name']}",
                service_name=work["target_service"], task_summary=task_summary,
                worktree_path=worktree_path, base_commit=base_commit,
                dependencies=[{"provider_service": payload["source_service"], "contract_id": payload["contract_id"],
                                "assumed_revision": payload["consumer_assumed_revision"], "dependency_path": "external-harness",
                                "interface_dependency_id": payload["interface_dependency_id"],
                                "http_method": payload.get("http_method"),
                                "endpoint_path": payload.get("endpoint_path")}],
            )
            await cur.execute(
                """UPDATE compatibility_work_items SET state='ACKNOWLEDGED', task_id=%s,
                    lease_expires_at=now() + INTERVAL '30 minutes', updated_at=now()
                    WHERE work_item_id=%s AND harness_id=%s
                      AND state IN ('PENDING', 'DISPATCHED')
                    RETURNING work_item_id;""", (task["task_id"], work["work_item_id"], harness_id),
            )
            updated = await cur.fetchone()
            if not updated:
                raise RuntimeError("Compatibility work changed before the claim could be committed")

            await cur.execute(
                """INSERT INTO coordinator_outbox (
                       aggregate_type, aggregate_id, aggregate_revision,
                       source_service, event_type, payload
                   ) VALUES ('COMPATIBILITY_WORK', %s, %s, 'coordinator',
                             'COMPATIBILITY_WORK_CLAIMED', %s::jsonb)
                   RETURNING event_id;""",
                (work["work_item_id"], work["source_contract_revision"], json.dumps({
                    "work_item_id": str(work["work_item_id"]),
                    "harness_id": str(harness_id),
                    "task_id": task["task_id"],
                    "assignment_mode": assignment_mode,
                    "target_service": work["target_service"],
                    "source_service": payload.get("source_service"),
                    "source_contract_revision": work["source_contract_revision"],
                })),
            )
            outbox = await cur.fetchone()
            if not outbox or not outbox.get("event_id"):
                raise RuntimeError("COMPATIBILITY_WORK_CLAIMED outbox event was not created")
            outbox_id = outbox["event_id"]
            await cur.execute(
                """INSERT INTO contract_audit_history (
                       event_type, source_service, summary, actor,
                       outbox_event_id, causation_id, correlation_id
                   ) VALUES ('COMPATIBILITY_WORK_CLAIMED', 'coordinator', %s, %s, %s, %s, %s);""",
                (
                    f"Harness {harness['harness_name']} claimed compatibility work {work['work_item_id']} ({assignment_mode})",
                    f"{harness['harness_type']}:{harness['harness_name']}",
                    outbox_id, outbox_id, work["correlation_id"],
                ),
            )
            return {"work_item_id": str(work["work_item_id"]), "state": "ACKNOWLEDGED",
                    "task": task, "payload": payload, "assignment_mode": assignment_mode,
                    "outbox_event_id": str(outbox_id)}
    return await run_transaction(_tx)


def validate_checkpoint_payload(checkpoint: dict[str, Any]) -> dict[str, Any]:
    """Allow only operational metadata—never chain-of-thought, thoughts, scratchpad, or raw chat history."""
    allowed = {
        "task_id",
        "plan_revision",
        "phase",
        "files_changed",
        "changed_files",
        "assumed_contract_revisions",
        "test_status",
        "summary",
    }
    unknown = set(checkpoint) - allowed
    if unknown:
        raise ValueError(f"Checkpoint contains unsupported or forbidden fields: {sorted(unknown)}")

    # Strict check against chain-of-thought and scratchpad injection
    forbidden_tokens = ("scratchpad", "thought", "chain_of_thought", "cot", "chat_history", "messages", "prompt", "raw_chat")
    for key in checkpoint.keys():
        if any(token in key.lower() for token in forbidden_tokens):
            raise ValueError(f"Forbidden field '{key}': chain-of-thought and raw chat histories are strictly prohibited.")

    phase = checkpoint.get("phase")
    if phase not in {"PLANNING", "IMPLEMENTING", "TESTING", "REPLANNING"}:
        raise ValueError("Checkpoint phase must be PLANNING, IMPLEMENTING, TESTING, or REPLANNING")

    raw_files = checkpoint.get("files_changed", checkpoint.get("changed_files", []))
    if not isinstance(raw_files, list) or not all(isinstance(path, str) and len(path) <= 500 for path in raw_files):
        raise ValueError("files_changed must be a list of bounded path strings")

    assumed_revs = checkpoint.get("assumed_contract_revisions", {})
    if not isinstance(assumed_revs, dict) or not all(isinstance(k, str) and isinstance(v, int) for k, v in assumed_revs.items()):
        raise ValueError("assumed_contract_revisions must be an object mapping service names to integer revisions")

    test_status = checkpoint.get("test_status", "NOT_RUN")
    if not isinstance(test_status, str):
        raise ValueError("test_status must be a string")

    summary = checkpoint.get("summary", "")
    if not isinstance(summary, str) or len(summary) > 1000:
        raise ValueError("summary must be a string with at most 1000 characters")

    result = {
        "phase": phase,
        "files_changed": raw_files,
        "changed_files": raw_files,
        "assumed_contract_revisions": assumed_revs,
        "test_status": test_status,
        "summary": summary,
    }
    if checkpoint.get("plan_revision") is not None:
        result["plan_revision"] = int(checkpoint["plan_revision"])
    if checkpoint.get("task_id") is not None:
        result["task_id"] = str(checkpoint["task_id"])
    return result


async def record_harness_checkpoint(harness_id: str, task_id: str, checkpoint: dict[str, Any]) -> dict[str, Any]:
    """Persist a typed checkpoint and return a deterministic CONTINUE/REPLAN_REQUIRED instruction."""
    clean = validate_checkpoint_payload(checkpoint)
    harness = await fetch_one(
        "SELECT harness_name, harness_type FROM harness_registrations WHERE harness_id=%s;", (harness_id,)
    )
    if not harness:
        raise ValueError("Harness no longer exists")
    expected_agent_id = f"{harness['harness_type']}:{harness['harness_name']}"
    plan_revision = clean.get("plan_revision")

    async def _tx(conn: psycopg.AsyncConnection) -> dict[str, Any]:
        async with conn.cursor(row_factory=dict_row) as cur:
            task = await _lock_owned_task_with_optional_work(
                cur, task_id=task_id, agent_id=expected_agent_id
            )
            if not task:
                raise ValueError("Task is not owned by this harness")
            effective_plan_rev = plan_revision if plan_revision is not None else task["plan_revision"]
            await cur.execute(
                """UPDATE active_agent_tasks SET checkpoint_state=%s::jsonb, plan_revision=%s, updated_at=now()
                   WHERE task_id=%s;""", (json.dumps(clean), effective_plan_rev, task_id),
            )
            await cur.execute(
                """INSERT INTO agent_checkpoints (task_id, plan_revision, status, checkpoint_state)
                   VALUES (%s, %s, %s, %s::jsonb);""",
                (task_id, effective_plan_rev, task["status"], json.dumps(clean)),
            )
            await cur.execute(
                """UPDATE compatibility_work_items SET state='EXECUTING', updated_at=now()
                   WHERE work_item_id=%s AND state='ACKNOWLEDGED';""", (task["work_item_id"],),
            )
            await cur.execute(
                """INSERT INTO coordinator_outbox (
                       aggregate_type, aggregate_id, aggregate_revision, source_service, event_type, payload
                   ) VALUES ('TASK_STATE', %s, %s, 'coordinator', 'TASK_CHECKPOINTED', %s::jsonb)
                   RETURNING event_id;""",
                (task_id, effective_plan_rev, json.dumps({"task_id": task_id, **clean})),
            )
            outbox_row = await cur.fetchone()
            if not outbox_row or not outbox_row.get("event_id"):
                raise RuntimeError("TASK_CHECKPOINTED outbox event was not created")
            outbox_id = outbox_row["event_id"]
            await cur.execute(
                """INSERT INTO contract_audit_history (
                       event_type, source_service, summary, actor,
                       outbox_event_id, causation_id, correlation_id
                   ) VALUES ('TASK_CHECKPOINTED', 'coordinator', %s, %s, %s, %s, %s);""",
                (f"Task {task_id} checkpointed at plan revision {effective_plan_rev}", expected_agent_id,
                 outbox_id, outbox_id, outbox_id),
            )
            return outbox_id
    checkpoint_outbox_id = await run_transaction(_tx)
    from coordinator.reconciliation import check_task_drift
    drift = await check_task_drift(task_id)
    if drift:
        schema_diff = drift.get("breaking_diff") or {}
        migration_notes = (
            drift.get("migration_notes")
            or (schema_diff.get("migration_note") if isinstance(schema_diff, dict) else "")
            or (schema_diff.get("diff_summary") if isinstance(schema_diff, dict) else "")
            or ""
        )
        audit_ids = drift.get("audit_ids") or {
            "drift_id": str(drift.get("drift_id")),
            "task_id": str(task_id),
            "source_service": drift.get("source_service"),
            "target_service": drift.get("target_service"),
        }
        return {
            "instruction": "REPLAN_REQUIRED",
            "new_contract_revision": drift.get("new_contract_revision"),
            "old_contract_revision": drift.get("old_contract_revision"),
            "schema_diff": schema_diff,
            "migration_notes": migration_notes,
            "audit_ids": audit_ids,
            "checkpoint_outbox_id": checkpoint_outbox_id,
            "drift": drift,
        }
    return {"instruction": "CONTINUE", "checkpoint_outbox_id": checkpoint_outbox_id}


async def complete_harness_task(
    harness_id: str,
    task_id: str,
    *,
    summary: str = "Task completed by harness",
    test_results: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Close a normal harness task while preserving its dependency history.

    This endpoint is intentionally narrower than a generic job queue: it only
    completes an owned task that is not itself a compatibility work item.  A
    compatibility task must use its evidence/approval lifecycle so deployment
    safety cannot be bypassed.
    """
    harness = await fetch_one(
        "SELECT harness_name, harness_type FROM harness_registrations WHERE harness_id=%s AND status='ACTIVE';",
        (harness_id,),
    )
    if not harness:
        raise ValueError("Harness is unknown or inactive")
    expected_agent_id = f"{harness['harness_type']}:{harness['harness_name']}"
    clean_summary = " ".join(str(summary).split())[:500] or "Task completed by harness"
    clean_tests = sanitize_test_results(test_results) if test_results is not None else None
    if clean_tests is not None and (clean_tests["returncode"] != 0 or clean_tests["all_passed"] is not True):
        raise ValueError("Task completion requires passing test evidence")

    async def _tx(conn: psycopg.AsyncConnection) -> dict[str, Any]:
        async with conn.cursor(row_factory=dict_row) as cur:
            task = await _lock_owned_task_with_optional_work(
                cur, task_id=task_id, agent_id=expected_agent_id
            )
            if not task:
                raise ValueError("Task is not owned by this harness")
            if task.get("work_item_id"):
                raise ValueError("Compatibility tasks must complete through evidence and approval")
            if task["status"] not in {"OPTIMISTIC_EXECUTING", "RECONCILED"}:
                raise ValueError(f"Task in state '{task['status']}' cannot be completed")

            completion = {"summary": clean_summary}
            if clean_tests is not None:
                completion["test_results"] = clean_tests
            await cur.execute(
                """UPDATE active_agent_tasks
                   SET status='COMPLETED',
                       checkpoint_state=jsonb_set(COALESCE(checkpoint_state, '{}'::jsonb), '{completion}', %s::jsonb),
                       updated_at=now()
                   WHERE task_id=%s AND status IN ('OPTIMISTIC_EXECUTING', 'RECONCILED')
                   RETURNING task_id, status, plan_revision;""",
                (json.dumps(completion), task_id),
            )
            updated = await cur.fetchone()
            if not updated:
                raise ValueError("Task changed before completion could be committed")
            await cur.execute(
                """INSERT INTO coordinator_outbox (
                       aggregate_type, aggregate_id, aggregate_revision,
                       source_service, event_type, payload
                   ) VALUES ('TASK_STATE', %s, %s, %s, 'TASK_COMPLETED', %s::jsonb)
                   RETURNING event_id;""",
                (task_id, task["plan_revision"], task["service_name"], json.dumps({
                    "task_id": str(task_id), "status": "COMPLETED", "summary": clean_summary,
                    "test_results": clean_tests,
                })),
            )
            outbox = await cur.fetchone()
            if not outbox or not outbox.get("event_id"):
                raise RuntimeError("TASK_COMPLETED outbox event was not created")
            outbox_id = outbox["event_id"]
            await cur.execute(
                """INSERT INTO contract_audit_history (
                       event_type, source_service, summary, actor,
                       outbox_event_id, causation_id, correlation_id
                   ) VALUES ('TASK_COMPLETED', %s, %s, %s, %s, %s, %s);""",
                (task["service_name"], f"Task {task_id} completed: {clean_summary}", expected_agent_id,
                 outbox_id, outbox_id, outbox_id),
            )
            return {"task_id": str(task_id), "status": "COMPLETED", "plan_revision": task["plan_revision"],
                    "outbox_event_id": str(outbox_id)}

    return await run_transaction(_tx)


async def _fetch_harness_with_cursor(cur: Any, harness_id: str) -> dict[str, Any]:
    await cur.execute(
        """SELECT harness_name, harness_type, service_name, repository_url, status
           FROM harness_registrations WHERE harness_id=%s FOR UPDATE;""",
        (harness_id,),
    )
    row = await cur.fetchone()
    if not row:
        raise ValueError("Harness no longer exists")
    return dict(row)


async def _create_task_with_cursor(cur: Any, **kwargs: Any) -> dict[str, Any]:
    """Avoid nested transactions while claiming a work item."""
    dependencies = kwargs.pop("dependencies")
    summary = normalize_task_summary(kwargs.get("task_summary", ""))
    await cur.execute(
        """INSERT INTO active_agent_tasks (agent_id, service_name, task_summary, worktree_path, base_commit,
                   plan_revision, status, checkpoint_state)
           VALUES (%s, %s, %s, %s, %s, 1, 'OPTIMISTIC_EXECUTING', %s::jsonb)
           RETURNING task_id, status, plan_revision;""",
        (kwargs["agent_id"], kwargs["service_name"], summary, kwargs["worktree_path"],
         kwargs["base_commit"], json.dumps({"phase": "PLANNING", "files_changed": [], "assumed_contract_revisions": {}, "test_status": "NOT_RUN", "summary": summary})),
    )
    task = await cur.fetchone()
    for dep in dependencies:
        await cur.execute(
            """
            SELECT confirmation_status, provider_service, consumer_service,
                   contract_id, assumed_provider_revision, http_method, endpoint_path
            FROM http_interface_dependencies
            WHERE dependency_id=%s
            FOR UPDATE;
            """,
            (dep.get("interface_dependency_id"),),
        )
        confirmed = await cur.fetchone()
        if not confirmed:
            raise ValueError("Compatibility work references an unknown HTTP dependency")
        if confirmed.get("confirmation_status") != "CONFIRMED":
            raise ValueError("Compatibility work cannot bind an unconfirmed HTTP dependency")
        if (
            str(confirmed.get("provider_service")) != str(dep["provider_service"])
            or str(confirmed.get("consumer_service")) != str(kwargs["service_name"])
            or str(confirmed.get("contract_id")) != str(dep["contract_id"])
            or int(confirmed.get("assumed_provider_revision")) != int(dep["assumed_revision"])
            or (dep.get("http_method") and str(confirmed.get("http_method")).upper() != str(dep["http_method"]).upper())
            or (dep.get("endpoint_path") and str(confirmed.get("endpoint_path")) != str(dep["endpoint_path"]))
        ):
            raise ValueError("Compatibility work dependency no longer matches its confirmed interface")
        await cur.execute(
            """INSERT INTO task_contract_dependencies (task_id, provider_service, contract_id, assumed_revision,
                   dependency_kind, dependency_path, interface_dependency_id)
               VALUES (%s, %s, %s, %s, 'HTTP_REST', %s, %s);""",
            (task["task_id"], dep["provider_service"], dep["contract_id"], dep["assumed_revision"],
             dep["dependency_path"], dep.get("interface_dependency_id")),
        )
    return {"task_id": str(task["task_id"]), "status": task["status"], "plan_revision": task["plan_revision"]}


def sanitize_test_results(results: dict[str, Any]) -> dict[str, Any]:
    """Validate and sanitize test execution evidence, stripping tracebacks, sources, or raw output."""
    if not isinstance(results, dict):
        raise ValueError("test_results must be an object")
    returncode = results.get("returncode")
    if not isinstance(returncode, int):
        raise ValueError("test_results.returncode must be an integer")
    all_passed = results.get("all_passed")
    if not isinstance(all_passed, bool):
        raise ValueError("test_results.all_passed must be a boolean")

    clean = {
        "returncode": returncode,
        "all_passed": all_passed,
        "passed_count": int(results.get("passed_count", results.get("passed", 0))),
        "failed_count": int(results.get("failed_count", results.get("failed", 0))),
        "duration_seconds": float(results.get("duration_seconds", results.get("duration", 0.0))),
        "framework": str(results.get("framework", "pytest"))[:50],
        "summary": str(results.get("summary", "Test execution completed"))[:500],
    }
    return clean


async def record_compatibility_result(
    work_item_id: str, *, test_results: dict[str, Any], summary: str
) -> dict[str, Any]:
    """Record verified evidence. Deployment remains a separate, human-approved operation."""
    clean_test_results = sanitize_test_results(test_results)
    if clean_test_results.get("all_passed") is not True or clean_test_results.get("returncode") != 0:
        raise ValueError("Compatibility results require passing test evidence")
    clean_summary = str(summary)[:500]

    async def _tx(conn: psycopg.AsyncConnection) -> dict[str, Any]:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("SELECT payload, target_service, state FROM compatibility_work_items WHERE work_item_id=%s FOR UPDATE;", (work_item_id,))
            row = await cur.fetchone()
            if not row:
                raise ValueError("Compatibility work item not found")
            if row["state"] not in {"EXECUTING", "ACKNOWLEDGED", "AWAITING_APPROVAL"}:
                raise ValueError(f"Work item in state '{row['state']}' cannot submit test results")
            payload = row["payload"] if isinstance(row["payload"], dict) else json.loads(row["payload"])
            payload["result"] = {"summary": clean_summary, "test_results": clean_test_results}
            await cur.execute(
                """UPDATE compatibility_work_items SET state='AWAITING_APPROVAL', payload=%s::jsonb,
                   updated_at=now() WHERE work_item_id=%s RETURNING state;""",
                (json.dumps(payload), work_item_id),
            )
            state = await cur.fetchone()
            event_payload = {"work_item_id": work_item_id, "target_service": row["target_service"], "summary": clean_summary, "test_results": clean_test_results}
            
            last_outbox_id = None
            for event_type in ("TESTS_PASSED", "AWAITING_APPROVAL"):
                await cur.execute(
                    """INSERT INTO coordinator_outbox (
                           aggregate_type, aggregate_id, aggregate_revision, source_service, event_type, payload
                       ) VALUES ('COMPATIBILITY_WORK', %s, 1, 'coordinator', %s, %s::jsonb)
                       RETURNING event_id;""",
                    (work_item_id, event_type, json.dumps(event_payload)),
                )
                out_row = await cur.fetchone()
                if not out_row or not out_row.get("event_id"):
                    raise RuntimeError(f"{event_type} outbox event was not created")
                last_outbox_id = str(out_row["event_id"])

            await cur.execute(
                """INSERT INTO contract_audit_history (event_type, source_service, summary, actor, outbox_event_id, causation_id, correlation_id)
                   VALUES ('COMPATIBILITY_RESULT_RECORDED', 'coordinator', %s, 'harness', %s, %s, %s);""",
                (f"Compatibility work {work_item_id} submitted passing test evidence: {clean_summary}", last_outbox_id, last_outbox_id, last_outbox_id),
            )
            return {"work_item_id": work_item_id, "state": state["state"], "outbox_event_id": last_outbox_id}
    return await run_transaction(_tx)


async def record_compatibility_incident(
    work_item_id: str,
    *,
    outcome: str,
    requested_resolution: str,
    missing_requirement: str = "",
    unavailable_required_input: Optional[str] = None,
    reason_code: str = "UNAVAILABLE_REQUIRED_INPUT",
    provider_service: Optional[str] = None,
    provider_contract_revision: Optional[int] = None,
    sources_checked: Optional[list[str]] = None,
    worktree_path: Optional[str] = None,
    source_commit: Optional[str] = None,
    changed_files: Optional[list[str]] = None,
    evidence: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Record BLOCKED/INCOMPATIBLE as a human design decision, preserving worktree/branch/commit without deploying or discarding."""
    if outcome not in {"BLOCKED", "INCOMPATIBLE"}:
        raise ValueError("outcome must be BLOCKED or INCOMPATIBLE")
    if not requested_resolution.strip():
        raise ValueError("requested_resolution is required")
    
    # Normalize missing requirement and unavailable input
    req_summary = missing_requirement.strip()
    if not req_summary and unavailable_required_input:
        req_summary = f"Missing required input: {unavailable_required_input}"
    if not req_summary:
        req_summary = f"Incompatibility [{reason_code}]: contract requirement cannot be satisfied"

    evidence_dict = dict(evidence or {})
    checked_list = list(sources_checked or evidence_dict.get("sources_checked") or [])
    files_list = list(changed_files or evidence_dict.get("changed_files") or evidence_dict.get("files_changed") or [])

    async def _tx(conn: psycopg.AsyncConnection) -> dict[str, Any]:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT target_service, payload, task_id, state FROM compatibility_work_items WHERE work_item_id=%s FOR UPDATE;",
                (work_item_id,),
            )
            row = await cur.fetchone()
            if not row:
                raise ValueError("Compatibility work item not found")
            if row["state"] in {"COMPLETED", "CANCELLED", "FAILED"}:
                raise ValueError(f"Cannot report incident on work item in terminal state '{row['state']}'")

            work_payload = row["payload"] if isinstance(row["payload"], dict) else json.loads(row["payload"])
            target_service = row["target_service"]
            task_id = str(row["task_id"]) if row["task_id"] else None

            # Retrieve active task metadata if worktree/commit not explicitly passed
            task_worktree = worktree_path or work_payload.get("worktree_path")
            task_commit = source_commit or work_payload.get("source_commit") or work_payload.get("base_commit")
            final_files = list(files_list)
            if task_id and (not task_worktree or not task_commit or not final_files):
                await cur.execute(
                    "SELECT worktree_path, base_commit, checkpoint_state FROM active_agent_tasks WHERE task_id=%s;",
                    (task_id,),
                )
                task_row = await cur.fetchone()
                if task_row:
                    task_worktree = task_worktree or task_row.get("worktree_path")
                    task_commit = task_commit or task_row.get("base_commit")
                    if not final_files and task_row.get("checkpoint_state"):
                        cp = task_row["checkpoint_state"] if isinstance(task_row["checkpoint_state"], dict) else json.loads(task_row["checkpoint_state"])
                        final_files = cp.get("files_changed") or cp.get("changed_files") or []

            prov_service = provider_service or work_payload.get("source_service") or "upstream-service"
            prov_rev = provider_contract_revision or work_payload.get("source_contract_revision") or 1

            full_evidence = {
                **evidence_dict,
                "reason_code": reason_code,
                "unavailable_required_input": unavailable_required_input or req_summary,
                "provider_service": prov_service,
                "provider_contract_revision": prov_rev,
                "sources_checked": checked_list,
                "worktree_path": task_worktree,
                "source_commit": task_commit,
                "changed_files": final_files,
                "requested_resolution": requested_resolution,
                "missing_requirement": req_summary,
            }

            # Update work item with preserved metadata
            work_payload["preserved_worktree"] = task_worktree
            work_payload["preserved_commit"] = task_commit
            work_payload["preserved_files"] = final_files
            work_payload["incident_evidence"] = full_evidence

            failure_msg = f"[{reason_code}] {req_summary}"[:1000]
            await cur.execute(
                """UPDATE compatibility_work_items
                   SET state=%s, failure_reason=%s, payload=%s::jsonb, updated_at=now()
                   WHERE work_item_id=%s;""",
                (outcome, failure_msg, json.dumps(work_payload), work_item_id),
            )

            # Insert or update compatibility incident
            await cur.execute(
                """INSERT INTO compatibility_incidents (
                       work_item_id, incident_type, missing_requirement, evidence, requested_resolution, status
                   ) VALUES (%s, %s, %s, %s::jsonb, %s, 'HUMAN_DECISION_REQUIRED')
                   ON CONFLICT (work_item_id) DO UPDATE SET
                       incident_type = EXCLUDED.incident_type,
                       missing_requirement = EXCLUDED.missing_requirement,
                       evidence = EXCLUDED.evidence,
                       requested_resolution = EXCLUDED.requested_resolution,
                       status = 'HUMAN_DECISION_REQUIRED',
                       resolved_at = NULL,
                       resolved_by = NULL;""",
                (work_item_id, outcome, req_summary, json.dumps(full_evidence), requested_resolution),
            )

            # Emit coordinator outbox event for dashboard and notification listeners
            outbox_payload = {
                "work_item_id": work_item_id,
                "target_service": target_service,
                "task_id": task_id,
                "outcome": outcome,
                "reason_code": reason_code,
                "unavailable_required_input": unavailable_required_input or req_summary,
                "provider_service": prov_service,
                "provider_contract_revision": prov_rev,
                "sources_checked": checked_list,
                "worktree_path": task_worktree,
                "source_commit": task_commit,
                "changed_files": files_list,
                "missing_requirement": req_summary,
                "requested_resolution": requested_resolution,
                "evidence": full_evidence,
                "status": "HUMAN_DECISION_REQUIRED",
            }
            await cur.execute(
                """INSERT INTO coordinator_outbox (
                       aggregate_type, aggregate_id, aggregate_revision, source_service, event_type, payload
                   ) VALUES ('COMPATIBILITY_WORK', %s, 1, 'coordinator', %s, %s::jsonb)
                   RETURNING event_id;""",
                (work_item_id, f"COMPATIBILITY_{outcome}", json.dumps(outbox_payload)),
            )
            incident_outbox_row = await cur.fetchone()
            if not incident_outbox_row or not incident_outbox_row.get("event_id"):
                raise RuntimeError("COMPATIBILITY_BLOCKED outbox event was not created")
            incident_outbox_id = incident_outbox_row["event_id"]

            # Create append-only audit event in contract_audit_history with causal lineage
            audit_summary = f"{outcome}: [{reason_code}] {req_summary}. Provider: {prov_service} rev {prov_rev}. Work preserved in '{task_worktree or 'worktree'}' (commit: {task_commit or 'unknown'}). Requires human design decision."
            await cur.execute(
                """INSERT INTO contract_audit_history (event_type, source_service, summary, actor, outbox_event_id, causation_id, correlation_id)
                   VALUES (%s, %s, %s, 'harness', %s, %s, %s);""",
                (f"COMPATIBILITY_{outcome}", target_service, audit_summary, incident_outbox_id, incident_outbox_id, incident_outbox_id),
            )

            return {
                "work_item_id": work_item_id,
                "state": outcome,
                "status": "HUMAN_DECISION_REQUIRED",
                "reason_code": reason_code,
                "unavailable_required_input": unavailable_required_input or req_summary,
                "worktree_preserved": task_worktree,
                "commit_preserved": task_commit,
            }

    return await run_transaction(_tx)


async def approve_compatibility_work(work_item_id: str, approved_by: str) -> dict[str, Any]:
    """Approve evidence and rebind the confirmed dependency to the new revision atomically."""
    async def _tx(conn: psycopg.AsyncConnection) -> dict[str, Any]:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT work_item_id, target_service, source_contract_id,
                          source_contract_revision, task_id, state, payload
                   FROM compatibility_work_items
                   WHERE work_item_id=%s FOR UPDATE;""",
                (work_item_id,),
            )
            work = await cur.fetchone()
            if not work:
                raise ValueError("Compatibility work item not found")
            if work["state"] != "AWAITING_APPROVAL":
                raise ValueError("Work item must be in AWAITING_APPROVAL before approval")
            payload = work["payload"] if isinstance(work["payload"], dict) else json.loads(work["payload"])
            dependency_id = payload.get("interface_dependency_id")
            if not dependency_id:
                raise ValueError("Compatibility work has no confirmed HTTP dependency to rebind")

            await cur.execute(
                """SELECT provider_service, consumer_service, http_method, endpoint_path,
                          path_parameters, query_parameters, declared_headers,
                          request_body_schema, response_schemas, consumer_repository,
                          consumer_source_file, consumer_source_evidence
                   FROM http_interface_dependencies
                   WHERE dependency_id=%s AND consumer_service=%s
                     AND provider_service=%s AND confirmation_status='CONFIRMED'
                   FOR UPDATE;""",
                (dependency_id, work["target_service"], payload.get("source_service")),
            )
            previous_dependency = await cur.fetchone()
            if not previous_dependency:
                raise ValueError("Confirmed HTTP dependency no longer matches compatibility work")

            await cur.execute(
                """INSERT INTO http_interface_dependencies (
                       provider_service, consumer_service, contract_id,
                       assumed_provider_revision, http_method, endpoint_path,
                       path_parameters, query_parameters, declared_headers,
                       request_body_schema, response_schemas, consumer_repository,
                       consumer_source_file, consumer_source_evidence,
                       confirmation_status, confirmed_by, confirmed_at
                   ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb,
                             %s::jsonb, %s::jsonb, %s::jsonb, %s, %s, %s::jsonb,
                             'CONFIRMED', %s, now())
                   ON CONFLICT (consumer_service, provider_service, contract_id,
                                assumed_provider_revision, consumer_repository,
                                consumer_source_file)
                   DO UPDATE SET
                       path_parameters=EXCLUDED.path_parameters,
                       query_parameters=EXCLUDED.query_parameters,
                       declared_headers=EXCLUDED.declared_headers,
                       request_body_schema=EXCLUDED.request_body_schema,
                       response_schemas=EXCLUDED.response_schemas,
                       consumer_source_evidence=EXCLUDED.consumer_source_evidence,
                       confirmation_status='CONFIRMED',
                       confirmed_by=EXCLUDED.confirmed_by,
                       confirmed_at=now(), updated_at=now()
                   RETURNING dependency_id;""",
                (
                    previous_dependency["provider_service"], previous_dependency["consumer_service"],
                    work["source_contract_id"], work["source_contract_revision"],
                    previous_dependency["http_method"], previous_dependency["endpoint_path"],
                    json.dumps(previous_dependency["path_parameters"] or {}),
                    json.dumps(previous_dependency["query_parameters"] or {}),
                    json.dumps(previous_dependency["declared_headers"] or {}),
                    json.dumps(previous_dependency["request_body_schema"] or {}),
                    json.dumps(previous_dependency["response_schemas"] or {}),
                    previous_dependency["consumer_repository"], previous_dependency["consumer_source_file"],
                    json.dumps(previous_dependency["consumer_source_evidence"] or {}), approved_by,
                ),
            )
            rebound_dependency = await cur.fetchone()
            if not rebound_dependency:
                raise RuntimeError("Failed to append the rebound HTTP dependency")
            rebound_dependency_id = rebound_dependency["dependency_id"]

            await cur.execute(
                """UPDATE task_contract_dependencies
                   SET contract_id=%s, assumed_revision=%s,
                       interface_dependency_id=%s
                   WHERE task_id=%s AND interface_dependency_id=%s;""",
                (work["source_contract_id"], work["source_contract_revision"],
                 rebound_dependency_id, work["task_id"], dependency_id),
            )
            payload["rebound_interface_dependency_id"] = str(rebound_dependency_id)
            payload["rebound_contract_revision"] = work["source_contract_revision"]
            await cur.execute(
                """UPDATE compatibility_work_items SET state='VERIFIED', payload=%s::jsonb, updated_at=now()
                   WHERE work_item_id=%s AND state='AWAITING_APPROVAL'
                   RETURNING state;""",
                (json.dumps(payload), work_item_id),
            )
            updated = await cur.fetchone()
            if not updated:
                raise ValueError("Work item changed before approval could be committed")

            event_payload = {
                "work_item_id": str(work_item_id),
                "from_state": "AWAITING_APPROVAL",
                "to_state": "VERIFIED",
                "approved_by": approved_by,
                "dependency_id": str(dependency_id),
                "rebound_dependency_id": str(rebound_dependency_id),
                "rebound_contract_revision": work["source_contract_revision"],
            }
            await cur.execute(
                """INSERT INTO coordinator_outbox (
                       aggregate_type, aggregate_id, aggregate_revision,
                       source_service, event_type, payload
                   ) VALUES ('COMPATIBILITY_WORK', %s, %s, 'coordinator',
                             'COMPATIBILITY_VERIFIED', %s::jsonb)
                   RETURNING event_id;""",
                (work_item_id, work["source_contract_revision"], json.dumps(event_payload)),
            )
            outbox = await cur.fetchone()
            if not outbox or not outbox.get("event_id"):
                raise RuntimeError("COMPATIBILITY_VERIFIED outbox event was not created")
            outbox_id = outbox["event_id"]
            await cur.execute(
                """INSERT INTO contract_audit_history (
                       event_type, source_service, summary, actor,
                       outbox_event_id, causation_id, correlation_id
                   ) VALUES ('COMPATIBILITY_VERIFIED', %s, %s, %s, %s, %s, %s);""",
                (work["target_service"],
                 f"Approved compatibility work {work_item_id}; dependency rebound to contract revision {work['source_contract_revision']}",
                 approved_by, outbox_id, outbox_id, outbox_id),
            )
            await cur.execute(
                """INSERT INTO coordinator_outbox (
                       aggregate_type, aggregate_id, aggregate_revision,
                       source_service, event_type, payload
                   ) VALUES ('COMPATIBILITY_WORK', %s, %s, 'coordinator',
                             'PLAN_APPROVED', %s::jsonb)
                   RETURNING event_id;""",
                (work_item_id, work["source_contract_revision"], json.dumps({
                    **event_payload,
                    "event_type": "PLAN_APPROVED",
                })),
            )
            approval_outbox = await cur.fetchone()
            if not approval_outbox or not approval_outbox.get("event_id"):
                raise RuntimeError("PLAN_APPROVED outbox event was not created")
            approval_outbox_id = approval_outbox["event_id"]
            await cur.execute(
                """INSERT INTO contract_audit_history (
                       event_type, source_service, summary, actor,
                       outbox_event_id, causation_id, correlation_id
                   ) VALUES ('PLAN_APPROVED', %s, %s, %s, %s, %s, %s);""",
                (work["target_service"],
                 f"Operator '{approved_by}' approved compatibility plan for work {work_item_id}",
                 approved_by, approval_outbox_id, outbox_id, approval_outbox_id),
            )
            return {"work_item_id": str(work_item_id), "state": "VERIFIED",
                    "dependency_rebound": True,
                    "dependency_id": str(rebound_dependency_id),
                    "rebound_contract_revision": work["source_contract_revision"],
                    "outbox_event_id": str(outbox_id),
                    "plan_approval_outbox_event_id": str(approval_outbox_id)}
    return await run_transaction(_tx)


async def complete_compatibility_work(work_item_id: str, completed_by: str) -> dict[str, Any]:
    """Mark work item COMPLETED upon successful deployment/release cutover."""
    return await _transition_compatibility_work(work_item_id, "VERIFIED", "COMPLETED", completed_by, "COMPATIBILITY_COMPLETED")


async def cancel_compatibility_work(work_item_id: str, *, reason: str, actor: str) -> dict[str, Any]:
    """Cancel in-flight compatibility work item."""
    async def _tx(conn: psycopg.AsyncConnection) -> dict[str, Any]:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT target_service, state FROM compatibility_work_items WHERE work_item_id=%s FOR UPDATE;""",
                (work_item_id,),
            )
            row = await cur.fetchone()
            if not row:
                raise ValueError("Compatibility work item not found")
            if row["state"] in {"COMPLETED", "CANCELLED"}:
                raise ValueError(f"Cannot cancel work item already in '{row['state']}' state")
            await cur.execute(
                """UPDATE compatibility_work_items SET state='CANCELLED', failure_reason=%s, updated_at=now()
                   WHERE work_item_id=%s RETURNING state;""",
                (reason[:1000], work_item_id),
            )
            await cur.execute(
                """INSERT INTO coordinator_outbox (
                       aggregate_type, aggregate_id, aggregate_revision, source_service, event_type, payload
                   ) VALUES ('COMPATIBILITY_WORK', %s, 1, 'coordinator', 'COMPATIBILITY_WORK_CANCELLED', %s::jsonb)
                   RETURNING event_id;""",
                (work_item_id, json.dumps({"work_item_id": work_item_id, "reason": reason, "cancelled_by": actor})),
            )
            out_row = await cur.fetchone()
            if not out_row or not out_row.get("event_id"):
                raise RuntimeError("COMPATIBILITY_CANCELLED outbox event was not created")
            outbox_id = str(out_row["event_id"])
            await cur.execute(
                """INSERT INTO contract_audit_history (event_type, source_service, summary, actor, outbox_event_id, causation_id, correlation_id)
                   VALUES ('COMPATIBILITY_CANCELLED', %s, %s, %s, %s, %s, %s);""",
                (row["target_service"], f"Cancelled work item {work_item_id}: {reason}", actor, outbox_id, outbox_id, outbox_id),
            )
            return {"work_item_id": work_item_id, "state": "CANCELLED", "outbox_event_id": outbox_id}
    return await run_transaction(_tx)


async def fail_compatibility_work(work_item_id: str, *, failure_reason: str, actor: str = "coordinator") -> dict[str, Any]:
    """Mark work item as FAILED due to unrecoverable errors or retries exceeded."""
    async def _tx(conn: psycopg.AsyncConnection) -> dict[str, Any]:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT target_service, state FROM compatibility_work_items WHERE work_item_id=%s FOR UPDATE;""",
                (work_item_id,),
            )
            row = await cur.fetchone()
            if not row:
                raise ValueError("Compatibility work item not found")
            await cur.execute(
                """UPDATE compatibility_work_items SET state='FAILED', failure_reason=%s, updated_at=now()
                   WHERE work_item_id=%s RETURNING state;""",
                (failure_reason[:1000], work_item_id),
            )
            await cur.execute(
                """INSERT INTO coordinator_outbox (
                       aggregate_type, aggregate_id, aggregate_revision, source_service, event_type, payload
                   ) VALUES ('COMPATIBILITY_WORK', %s, 1, 'coordinator', 'COMPATIBILITY_WORK_FAILED', %s::jsonb)
                   RETURNING event_id;""",
                (work_item_id, json.dumps({"work_item_id": work_item_id, "failure_reason": failure_reason, "actor": actor})),
            )
            out_row = await cur.fetchone()
            if not out_row or not out_row.get("event_id"):
                raise RuntimeError("COMPATIBILITY_FAILED outbox event was not created")
            outbox_id = str(out_row["event_id"])
            await cur.execute(
                """INSERT INTO contract_audit_history (event_type, source_service, summary, actor, outbox_event_id, causation_id, correlation_id)
                   VALUES ('COMPATIBILITY_FAILED', %s, %s, %s, %s, %s, %s);""",
                (row["target_service"], f"Work item {work_item_id} failed: {failure_reason}", actor, outbox_id, outbox_id, outbox_id),
            )
            return {"work_item_id": work_item_id, "state": "FAILED", "outbox_event_id": outbox_id}
    return await run_transaction(_tx)


async def expire_compatibility_work(work_item_id: str, *, reason: str = "Lease expired without progress") -> dict[str, Any]:
    """Transition work item to EXPIRED state."""
    async def _tx(conn: psycopg.AsyncConnection) -> dict[str, Any]:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """UPDATE compatibility_work_items SET state='EXPIRED', failure_reason=%s, updated_at=now()
                   WHERE work_item_id=%s AND state IN ('PENDING', 'DISPATCHED', 'ACKNOWLEDGED', 'EXECUTING')
                   RETURNING target_service, state;""",
                (reason[:1000], work_item_id),
            )
            row = await cur.fetchone()
            if not row:
                raise ValueError("Work item not found or not in expirable state")
            await cur.execute(
                """INSERT INTO coordinator_outbox (
                       aggregate_type, aggregate_id, aggregate_revision, source_service, event_type, payload
                   ) VALUES ('COMPATIBILITY_WORK', %s, 1, 'coordinator', 'COMPATIBILITY_WORK_EXPIRED', %s::jsonb)
                   RETURNING event_id;""",
                (work_item_id, json.dumps({"work_item_id": work_item_id, "reason": reason})),
            )
            out_row = await cur.fetchone()
            if not out_row or not out_row.get("event_id"):
                raise RuntimeError("COMPATIBILITY_EXPIRED outbox event was not created")
            outbox_id = str(out_row["event_id"])
            await cur.execute(
                """INSERT INTO contract_audit_history (event_type, source_service, summary, actor, outbox_event_id, causation_id, correlation_id)
                   VALUES ('COMPATIBILITY_EXPIRED', %s, %s, 'coordinator', %s, %s, %s);""",
                (row["target_service"], f"Work item {work_item_id} expired: {reason}", outbox_id, outbox_id, outbox_id),
            )
            return {"work_item_id": work_item_id, "state": "EXPIRED", "outbox_event_id": outbox_id}
    return await run_transaction(_tx)


async def get_compatibility_work_item(work_item_id: str) -> Optional[dict[str, Any]]:
    """Retrieve full details of a compatibility work item."""
    return await fetch_one(
        """SELECT work_item_id, source_event_id, source_contract_id, source_contract_revision,
                  target_service, target_repository, harness_id, state, idempotency_key,
                  coordination_key,
                  causation_id, correlation_id, hop_count, task_id, payload, dispatch_attempts,
                  lease_expires_at, failure_reason, created_at, updated_at
           FROM compatibility_work_items WHERE work_item_id = %s;""",
        (work_item_id,),
    )


async def _transition_compatibility_work(work_item_id: str, from_state: str, to_state: str, actor: str, event_type: str) -> dict[str, Any]:
    async def _tx(conn: psycopg.AsyncConnection) -> dict[str, Any]:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """UPDATE compatibility_work_items SET state=%s, updated_at=now()
                   WHERE work_item_id=%s AND state=%s RETURNING target_service, state;""",
                (to_state, work_item_id, from_state),
            )
            row = await cur.fetchone()
            if not row:
                raise ValueError(f"Work item must be in {from_state} before moving to {to_state}")
            await cur.execute(
                """INSERT INTO coordinator_outbox (
                       aggregate_type, aggregate_id, aggregate_revision, source_service, event_type, payload
                   ) VALUES ('COMPATIBILITY_WORK', %s, 1, 'coordinator', %s, %s::jsonb)
                   RETURNING event_id;""",
                (work_item_id, event_type, json.dumps({"work_item_id": work_item_id, "from_state": from_state, "to_state": to_state, "actor": actor})),
            )
            out_row = await cur.fetchone()
            if not out_row or not out_row.get("event_id"):
                raise RuntimeError(f"{event_type} outbox event was not created")
            outbox_id = str(out_row["event_id"])
            await cur.execute(
                """INSERT INTO contract_audit_history (event_type, source_service, summary, actor, outbox_event_id, causation_id, correlation_id)
                   VALUES (%s, %s, %s, %s, %s, %s, %s);""", (event_type, row["target_service"], f"Work item {work_item_id} -> {to_state}", actor, outbox_id, outbox_id, outbox_id),
            )
            return {"work_item_id": work_item_id, "state": to_state, "outbox_event_id": outbox_id}
    return await run_transaction(_tx)
