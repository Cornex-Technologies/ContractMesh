"""Asynchronous Drift Worker & Lease Claiming Engine.

Implements:
1. Atomic inbox lease claiming protocol (`FOR UPDATE SKIP LOCKED` / atomic state CAS)
2. Changefeed payload decoding & aggregate revision tracking
3. Relational dependency query over `task_contract_dependencies`
4. Breaking-change gating: only breaking diffs trigger `ACTIVE_INTERVENTION` and `REPLAN_REQUIRED`
5. Derived drift event generation in `ACTIVE_INTERVENTION` state
6. Outbox `TASK_REPLAN_REQUIRED` event emission for checkpoint-aware agents
7. Fail-safe decoupled error recording:
   - Claim transaction increments attempt_count on claim and marks PROCESSED on success.
   - Failures roll back the claim transaction, and a fresh independent transaction increments
     attempt_count and records FAILED status + last_error.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from typing import Any, Optional
import psycopg
from psycopg.rows import dict_row

from coordinator.db import run_transaction, execute_query, fetch_one
from coordinator.differencer import compute_schema_diff
from coordinator.compatibility import create_compatibility_work_for_contract_change

logger = logging.getLogger(__name__)

MAX_ATTEMPT_RETRIES = 3


# ==============================================================================
# 1. Inbox Ingestion & Atomic Lease Claiming
# ==============================================================================


async def ingest_changefeed_event(event_dict: dict[str, Any]) -> str:
    """Idempotently ingest an event record into event_inbox for asynchronous drift processing."""
    event_id = str(event_dict.get("event_id") or uuid.uuid4())
    payload = event_dict.get("payload") or event_dict
    insert_query = """
    INSERT INTO event_inbox (event_id, processing_status, attempt_count, payload)
    VALUES (%s, 'RECEIVED', 0, %s::jsonb)
    ON CONFLICT (event_id) DO NOTHING
    RETURNING event_id;
    """
    await execute_query(insert_query, (event_id, json.dumps(payload)))
    return event_id


async def claim_next_inbox_event(conn: psycopg.AsyncConnection) -> Optional[dict[str, Any]]:
    """Atomically claim the next pending or retryable inbox event for processing."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT
                i.event_id,
                i.processing_status,
                i.attempt_count,
                i.payload AS inbox_payload
            FROM event_inbox i
            WHERE i.processing_status = 'RECEIVED'
               OR (i.processing_status = 'FAILED' AND i.attempt_count < %s)
            ORDER BY i.received_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED;
            """,
            (MAX_ATTEMPT_RETRIES,),
        )
        row = await cur.fetchone()
        if not row:
            return None

        event_id = row["event_id"]

        # Keep the inbox claim lockable on CockroachDB.  A LEFT JOIN followed
        # by FOR UPDATE is rejected because the joined table is nullable.  The
        # outbox row is read separately in the same transaction, preserving the
        # original optional-join semantics without weakening the inbox lease.
        await cur.execute(
            """
            SELECT aggregate_type, aggregate_id, aggregate_revision,
                   source_service, event_type, payload AS outbox_payload
            FROM coordinator_outbox
            WHERE event_id = %s;
            """,
            (event_id,),
        )
        outbox_row = await cur.fetchone()
        if outbox_row:
            row.update(dict(outbox_row))
        else:
            row.update(
                {
                    "aggregate_type": None,
                    "aggregate_id": None,
                    "aggregate_revision": None,
                    "source_service": None,
                    "event_type": None,
                    "outbox_payload": None,
                }
            )

        # Atomically transition to PROCESSING and increment attempt_count in the active transaction
        await cur.execute(
            """
            UPDATE event_inbox
            SET 
                processing_status = 'PROCESSING',
                attempt_count = attempt_count + 1
            WHERE event_id = %s;
            """,
            (event_id,),
        )

        return row


async def process_claimed_event(
    conn: psycopg.AsyncConnection,
    event: dict[str, Any],
) -> dict[str, Any]:
    """Process a claimed inbox event, detect drift on dependent tasks with breaking-change gating, and mark PROCESSED."""
    event_id = event["event_id"]
    
    # Resolve payload: merge outbox_payload and inbox_payload so schema_diff is always available
    inbox_payload = event.get("inbox_payload") or {}
    if isinstance(inbox_payload, str):
        try:
            inbox_payload = json.loads(inbox_payload)
        except Exception:
            inbox_payload = {}
    outbox_payload = event.get("outbox_payload") or {}
    if isinstance(outbox_payload, str):
        try:
            outbox_payload = json.loads(outbox_payload)
        except Exception:
            outbox_payload = {}

    payload = {**inbox_payload, **outbox_payload}
    if not payload.get("schema_diff") and inbox_payload.get("schema_diff"):
        payload["schema_diff"] = inbox_payload["schema_diff"]

    event_type = event.get("event_type") or payload.get("event_type", "CONTRACT_CHANGED")
    source_service = event.get("source_service") or payload.get("service_name", "unknown")

    drift_records_created: list[dict[str, Any]] = []

    async with conn.cursor(row_factory=dict_row) as cur:
        if event_type in {"CONTRACT_CHANGED", "ENDPOINT_RETIRED", "ENDPOINT_RETIREMENT_REVIEW_REQUIRED"}:
            contract_id = payload.get("contract_id")
            new_revision = payload.get("revision_number", 1)
            schema_diff = payload.get("schema_diff")

            if contract_id:
                revision_operator = "<=" if event_type == "ENDPOINT_RETIREMENT_REVIEW_REQUIRED" else "<"
                # Find all in-flight tasks depending on older revisions of this contract
                await cur.execute(
                    f"""
                    SELECT 
                        d.task_id,
                        d.provider_service,
                        d.contract_id,
                        d.assumed_revision,
                        t.service_name AS target_service,
                        t.agent_id,
                        t.status AS task_status
                    FROM task_contract_dependencies d
                    JOIN active_agent_tasks t ON d.task_id = t.task_id
                    WHERE d.contract_id = %s
                      AND d.assumed_revision {revision_operator} %s
                      AND t.status IN ('OPTIMISTIC_EXECUTING', 'REPLANNING', 'AWAITING_APPROVAL');
                    """,
                    (contract_id, new_revision),
                )
                affected_tasks = await cur.fetchall()

                # Check if change is breaking (Breaking-change gate)
                diff_payload = schema_diff or {}
                is_breaking = bool(diff_payload.get("is_breaking", False))
                requires_review = diff_payload.get("classification") == "REVIEW_REQUIRED"
                intervention_required = is_breaking or requires_review

                # Create durable work for registered contract consumers even when no agent task is
                # already running. This is deliberately transactional with the change event.
                if intervention_required:
                    await create_compatibility_work_for_contract_change(
                        conn,
                        source_event_id=event_id,
                        contract_id=contract_id,
                        source_service=source_service,
                        revision_number=new_revision,
                        schema_diff=diff_payload,
                    )

                for task in affected_tasks:
                    task_id = task["task_id"]
                    old_revision = task["assumed_revision"]
                    target_service = task["target_service"]

                    if intervention_required:
                        # 1. Insert drift event in ACTIVE_INTERVENTION state with causal lineage
                        await cur.execute(
                            """
                            INSERT INTO drift_events (
                                outbox_event_id, causation_id, correlation_id,
                                source_service, target_task_id, target_service,
                                old_contract_revision, new_contract_revision,
                                breaking_diff, status
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, 'ACTIVE_INTERVENTION')
                            RETURNING drift_id;
                            """,
                            (
                                event_id,
                                event_id,
                                event_id,
                                source_service,
                                task_id,
                                target_service,
                                old_revision,
                                new_revision,
                                json.dumps(diff_payload),
                            ),
                        )
                        drift_row = await cur.fetchone()
                        drift_id = None
                        if drift_row is not None:
                            if isinstance(drift_row, dict):
                                drift_id = drift_row.get("drift_id") or drift_row.get("event_id") or str(uuid.uuid4())
                            elif isinstance(drift_row, (list, tuple)) and len(drift_row) > 0:
                                drift_id = drift_row[0]
                            else:
                                drift_id = str(uuid.uuid4())

                        # 2. Mark in-flight agent task as REPLAN_REQUIRED
                        await cur.execute(
                            """
                            UPDATE active_agent_tasks
                            SET status = 'REPLAN_REQUIRED', updated_at = now()
                            WHERE task_id = %s;
                            """,
                            (task_id,),
                        )

                        # 3. Emit TASK_REPLAN_REQUIRED outbox event
                        replan_outbox_payload = {
                            "drift_id": str(drift_id),
                            "task_id": str(task_id),
                            "target_service": target_service,
                            "source_service": source_service,
                            "contract_id": str(contract_id),
                            "old_contract_revision": old_revision,
                            "new_contract_revision": new_revision,
                            "breaking_diff": diff_payload,
                        }
                        await cur.execute(
                            """
                            INSERT INTO coordinator_outbox (
                                aggregate_type, aggregate_id, aggregate_revision,
                                source_service, event_type, payload
                            )
                            VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                            RETURNING event_id;
                            """,
                            (
                                "TASK_STATE",
                                task_id,
                                new_revision,
                                target_service,
                                "TASK_REPLAN_REQUIRED",
                                json.dumps(replan_outbox_payload),
                            ),
                        )
                        replan_outbox_row = await cur.fetchone()
                        replan_outbox_id = None
                        if replan_outbox_row is not None:
                            if isinstance(replan_outbox_row, dict):
                                replan_outbox_id = replan_outbox_row.get("event_id") or replan_outbox_row.get("drift_id") or str(uuid.uuid4())
                            elif isinstance(replan_outbox_row, (list, tuple)) and len(replan_outbox_row) > 0:
                                replan_outbox_id = replan_outbox_row[0]
                            else:
                                replan_outbox_id = str(uuid.uuid4())
                        if not replan_outbox_id:
                            raise RuntimeError("TASK_REPLAN_REQUIRED outbox event was not created")

                        # Record causal audit history
                        await cur.execute(
                            """
                            INSERT INTO contract_audit_history (
                                event_type, source_service, target_service, summary, actor,
                                outbox_event_id, causation_id, correlation_id
                            )
                            VALUES ('DRIFT_INTERVENTION_RAISED', %s, %s, %s, 'drift-worker', %s, %s, %s);
                            """,
                            (
                                source_service,
                                target_service,
                                f"Drift detected on {source_service} (rev {old_revision} -> {new_revision}) affecting task {task_id}",
                                replan_outbox_id,
                                event_id,
                                event_id,
                            ),
                        )

                        # Separate human-observability event; the task replan event remains
                        # the authoritative coordinator instruction.
                        await cur.execute(
                            """INSERT INTO coordinator_outbox (
                                   aggregate_type, aggregate_id, aggregate_revision,
                                   source_service, event_type, payload
                               ) VALUES ('DRIFT_EVENT', %s, %s, %s, 'DRIFT_DETECTED', %s::jsonb);""",
                            (drift_id, new_revision, source_service, json.dumps(replan_outbox_payload)),
                        )

                        drift_records_created.append({
                            "drift_id": str(drift_id),
                            "task_id": str(task_id),
                            "target_service": target_service,
                            "old_revision": old_revision,
                            "new_revision": new_revision,
                            "is_breaking": is_breaking,
                            "review_required": requires_review,
                        })
                    else:
                        logger.info(
                            "Non-breaking contract revision %s for %s; task %s continues execution without replan.",
                            new_revision, source_service, task_id,
                        )

        # 4. Mark inbox event as successfully PROCESSED (attempt_count already incremented at claim time)
        await cur.execute(
            """
            UPDATE event_inbox
            SET 
                processing_status = 'PROCESSED',
                processed_at = now(),
                last_error = NULL
            WHERE event_id = %s;
            """,
            (event_id,),
        )

        return {
            "event_id": str(event_id),
            "event_type": event_type,
            "status": "PROCESSED",
            "drift_events": drift_records_created,
        }


async def _record_event_failure(event_id: str, error_message: str) -> None:
    """Record event processing failure in a fresh, independent transaction, durably incrementing attempt_count."""
    async def _fail_tx(conn: psycopg.AsyncConnection) -> None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE event_inbox
                SET 
                    processing_status = 'FAILED',
                    attempt_count = attempt_count + 1,
                    last_error = %s
                WHERE event_id = %s;
                """,
                (error_message[:1000], event_id),
            )

    try:
        await run_transaction(_fail_tx)
    except Exception as record_err:
        logger.error("Failed to record failure state for inbox event %s: %s", event_id, record_err)


# ==============================================================================
# 2. Worker Lifecycle & Public Execution Interfaces
# ==============================================================================


async def process_single_inbox_event() -> Optional[dict[str, Any]]:
    """Claim and process a single inbox event inside a transaction.
    
    If processing encounters a SQL error or exception, the active transaction rolls back
    and failure state + attempt_count increment are durably recorded in a fresh, isolated transaction.
    """
    claimed_event_id: Optional[str] = None

    async def _tx(conn: psycopg.AsyncConnection) -> Optional[dict[str, Any]]:
        nonlocal claimed_event_id
        claimed = await claim_next_inbox_event(conn)
        if not claimed:
            return None

        claimed_event_id = str(claimed["event_id"])
        return await process_claimed_event(conn, claimed)

    try:
        return await run_transaction(_tx)
    except Exception as e:
        logger.error("Error during inbox event processing for %s: %s", claimed_event_id, e)
        if claimed_event_id:
            await _record_event_failure(claimed_event_id, str(e))
            return {
                "event_id": claimed_event_id,
                "status": "FAILED",
                "error": str(e),
            }
        return None


async def process_all_pending_events(max_count: int = 100) -> int:
    """Drain all currently pending inbox events up to max_count."""
    processed = 0
    for _ in range(max_count):
        res = await process_single_inbox_event()
        if not res:
            break
        processed += 1
    return processed


async def run_drift_worker_loop(
    poll_interval: float = 0.5,
    stop_event: Optional[asyncio.Event] = None,
) -> None:
    """Run continuous background drift worker loop until stop_event is set."""
    logger.info("Starting CodeClaim Drift Worker background loop...")
    while stop_event is None or not stop_event.is_set():
        try:
            result = await process_single_inbox_event()
            if not result:
                await asyncio.sleep(poll_interval)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("Drift worker loop encountered error: %s", e)
            await asyncio.sleep(poll_interval)
    logger.info("Drift Worker background loop stopped.")
