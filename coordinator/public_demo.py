"""Bounded, no-credential public demonstration workflow.

The public demo is intentionally not a general agent runner.  It creates a
small, deterministic compatibility scenario in the coordinator ledger so a
visitor can observe CockroachDB state, drift, checkpoints, and the approval
gate without being given an operator token or the ability to run arbitrary
code on the EC2 host.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import psycopg
from psycopg.rows import dict_row

from coordinator.compatibility import (
    claim_next_work_item,
    record_compatibility_result,
    register_harness,
    record_harness_checkpoint,
)
from coordinator.config import settings
from coordinator.contract_registry import publish_contract_revision
from coordinator.db import execute_query, fetch_one, run_transaction
from coordinator.drift_worker import ingest_changefeed_event, process_all_pending_events
from coordinator.http_dependencies import persist_http_interface_dependency
from coordinator.reconciliation import start_replanning, submit_reconciled_plan
from coordinator.service_registry import register_internal_service

logger = logging.getLogger(__name__)

PUBLIC_DEMO_KEY = "default"
_demo_tasks: dict[str, asyncio.Task] = {}


def _json_value(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _required_fields(schema: dict[str, Any]) -> set[str]:
    return set(schema.get("required") or [])


def _token_id_v1_schema() -> dict[str, Any]:
    """The stable baseline used by the public scenario."""
    return {
        "type": "object",
        "properties": {
            "amount": {"type": "integer"},
            "currency": {"type": "string"},
            "card_token": {"type": "string"},
        },
        "required": ["amount", "card_token"],
    }


def _token_id_v2_schema() -> dict[str, Any]:
    """Billing v2: the only semantic change is a new required token_id."""
    return {
        "type": "object",
        "properties": {
            "amount": {"type": "integer"},
            "currency": {"type": "string"},
            "card_token": {"type": "string"},
            "token_id": {"type": "string"},
        },
        "required": ["amount", "card_token", "token_id"],
    }


def _test_evidence() -> dict[str, Any]:
    # Deliberately bounded evidence. The public path never stores command
    # output, source snippets, environment variables, or stack traces.
    return {
        "returncode": 0,
        "all_passed": True,
        "passed_count": 4,
        "failed_count": 0,
        "duration_seconds": 0.8,
        "framework": "pytest",
        "summary": "Orders client includes the existing global TOKEN_ID in the Billing request.",
    }


async def _update_run(
    run_id: str,
    *,
    status: str,
    phase: str,
    result: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    clean_result = result or {}
    row = await fetch_one(
        """UPDATE public_demo_runs
           SET status=%s, phase=%s, result=%s::jsonb, updated_at=now()
           WHERE demo_key=%s AND run_id=%s
           RETURNING demo_key, run_id, status, phase, result, created_at, updated_at;""",
        (status, phase, json.dumps(clean_result), PUBLIC_DEMO_KEY, run_id),
    )
    if not row:
        raise RuntimeError("Public demo run no longer exists")
    return dict(row)


async def get_public_demo_run() -> Optional[dict[str, Any]]:
    row = await fetch_one(
        """SELECT demo_key, run_id, status, phase, result, created_at, updated_at
           FROM public_demo_runs WHERE demo_key=%s;""",
        (PUBLIC_DEMO_KEY,),
    )
    return dict(row) if row else None


async def start_public_demo_run() -> dict[str, Any]:
    """Atomically create or return the singleton public run.

    A completed run is idempotent. A fresh run is allowed only after the
    previous run failed or became stale, preventing public visitors from
    creating an unbounded stream of tasks and contract revisions.
    """
    now = datetime.now(timezone.utc)

    async def _tx(conn: psycopg.AsyncConnection) -> dict[str, Any]:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT demo_key, run_id, status, phase, result, created_at, updated_at
                   FROM public_demo_runs WHERE demo_key=%s FOR UPDATE;""",
                (PUBLIC_DEMO_KEY,),
            )
            existing = await cur.fetchone()
            if existing:
                updated_at = existing.get("updated_at")
                if isinstance(updated_at, datetime) and updated_at.tzinfo is None:
                    updated_at = updated_at.replace(tzinfo=timezone.utc)
                fresh = bool(
                    existing["status"] == "RUNNING"
                    and updated_at
                    and now - updated_at < timedelta(seconds=settings.public_demo_run_timeout_seconds)
                )
                if existing["status"] == "COMPLETED":
                    return {**dict(existing), "new_run": False}
                if fresh:
                    return {**dict(existing), "new_run": False}

            run_id = uuid.uuid4()
            await cur.execute(
                """UPSERT INTO public_demo_runs
                   (demo_key, run_id, status, phase, result, created_at, updated_at)
                   VALUES (%s, %s, 'RUNNING', 'STARTING', '{}'::jsonb, now(), now())
                   RETURNING demo_key, run_id, status, phase, result, created_at, updated_at;""",
                (PUBLIC_DEMO_KEY, run_id),
            )
            row = await cur.fetchone()
            if not row:
                raise RuntimeError("Failed to create public demo run")
            return {**dict(row), "new_run": True}

    return await run_transaction(_tx)


async def _ensure_services(root_dir: Path) -> None:
    services = {
        "billing-service": root_dir / "repos" / "billing-service",
        "orders-service": root_dir / "repos" / "orders-service",
    }
    for service_name, repo in services.items():
        row = await fetch_one("SELECT service_id FROM microservices WHERE service_name=%s;", (service_name,))
        if not row:
            await register_internal_service(
                service_name=service_name,
                repository_path=str(repo),
                actor="public-demo",
                application_entrypoint="main:app",
            )


async def _ensure_baseline_contract(root_dir: Path) -> dict[str, Any]:
    contract = await fetch_one(
        """SELECT contract_id FROM service_contracts
           WHERE service_name='billing-service' AND endpoint_path='/v1/charges' AND http_method='POST';"""
    )
    if not contract:
        return await publish_contract_revision(
            service_name="billing-service",
            endpoint_path="/v1/charges",
            http_method="POST",
            revision_number=1,
            schema_json=_token_id_v1_schema(),
            semantic_summary="Billing charges accepts amount, currency, and legacy card_token.",
            published_by="public-demo",
            source_commit="public-demo-baseline-v1",
        )

    revision = await fetch_one(
        """SELECT contract_revision_id, schema_json, source_commit
           FROM service_contract_revisions
           WHERE contract_id=%s AND revision_number=1;""",
        (contract["contract_id"],),
    )
    if not revision:
        raise ValueError("Billing /v1/charges exists without revision 1; re-onboard a clean demo database")
    schema = _json_value(revision.get("schema_json") or {})
    if "token_id" in _required_fields(schema):
        raise ValueError(
            "Public demo requires Billing revision 1 without token_id. "
            "Use a clean database and onboard the baseline repositories before running it."
        )
    return {
        "contract_id": str(contract["contract_id"]),
        "contract_revision_id": str(revision["contract_revision_id"]),
        "revision_number": 1,
        "is_idempotent_noop": True,
    }


async def _ensure_dependency(contract_id: str, root_dir: Path) -> dict[str, Any]:
    dep = await fetch_one(
        """SELECT dependency_id, consumer_repository, assumed_provider_revision
           FROM http_interface_dependencies
           WHERE consumer_service='orders-service' AND provider_service='billing-service'
             AND contract_id=%s AND assumed_provider_revision=1
             AND http_method='POST' AND endpoint_path='/v1/charges'
             AND confirmation_status='CONFIRMED'
           ORDER BY created_at DESC LIMIT 1;""",
        (contract_id,),
    )
    if dep:
        return dict(dep)

    orders_repo = root_dir / "repos" / "orders-service"
    source_file = orders_repo / "clients" / "billing_client.py"
    source_commit = "public-demo-baseline-v1"
    dependency = {
        "provider_service": "billing-service",
        "consumer_service": "orders-service",
        "contract_id": contract_id,
        "assumed_revision": 1,
        "http_method": "POST",
        "endpoint_path": "/v1/charges",
        "path_parameters": {},
        "query_parameters": {},
        "declared_headers": {"content-type": "application/json"},
        "request_body_schema": _token_id_v1_schema(),
        "response_schemas": {"200": {"type": "object"}},
        "consumer_source_file": "clients/billing_client.py",
        "consumer_source_evidence": {
            "source_commit": source_commit,
            "content_sha256": "public-demo-baseline",
        },
        "confirmation_status": "CONFIRMED",
        "confirmed_by": "public-demo",
    }

    async def _tx(conn: psycopg.AsyncConnection) -> dict[str, Any]:
        async with conn.cursor(row_factory=dict_row) as cur:
            dependency_id = await persist_http_interface_dependency(
                cur,
                dependency=dependency,
                consumer_service="orders-service",
                consumer_repository=str(orders_repo),
            )
            await cur.execute(
                """SELECT dependency_id, consumer_repository, assumed_provider_revision
                   FROM http_interface_dependencies WHERE dependency_id=%s;""",
                (dependency_id,),
            )
            row = await cur.fetchone()
            if not row:
                raise RuntimeError("Confirmed public demo dependency was not persisted")
            return dict(row)

    return await run_transaction(_tx)


async def _ensure_demo_harness(repository_url: str) -> dict[str, Any]:
    existing = await fetch_one(
        """SELECT harness_id, harness_name, harness_type, service_name, repository_url, status
           FROM harness_registrations WHERE harness_name='public-demo-orders' AND status='ACTIVE'
           ORDER BY created_at DESC LIMIT 1;"""
    )
    if existing:
        return dict(existing)
    return await register_harness(
        harness_name="public-demo-orders",
        harness_type="public-demo",
        service_name="orders-service",
        repository_url=repository_url,
        dispatch_mode="poll",
        capability_manifest={"mode": "bounded-demo", "language": "python", "framework": "fastapi"},
    )


async def _publish_token_id_revision(contract_id: str, run_id: str) -> dict[str, Any]:
    existing = await fetch_one(
        """SELECT contract_revision_id, schema_json, source_commit
           FROM service_contract_revisions WHERE contract_id=%s AND revision_number=2;""",
        (contract_id,),
    )
    if existing:
        schema = _json_value(existing.get("schema_json") or {})
        if "token_id" not in _required_fields(schema):
            raise ValueError(
                "Billing revision 2 already exists but does not represent the public token_id scenario; "
                "use a clean demo database."
            )
        return {
            "contract_id": contract_id,
            "contract_revision_id": str(existing["contract_revision_id"]),
            "revision_number": 2,
            "source_commit": existing.get("source_commit"),
            "is_idempotent_noop": True,
        }

    return await publish_contract_revision(
        service_name="billing-service",
        endpoint_path="/v1/charges",
        http_method="POST",
        revision_number=2,
        schema_json=_token_id_v2_schema(),
        semantic_summary="Billing charges now requires token_id; Orders must include its existing global TOKEN_ID.",
        published_by="public-demo-antigravity",
        source_commit=f"public-demo-token-id-{run_id}",
        publisher_compatibility={
            "classification": "breaking",
            "reason": "token_id is now required for POST /v1/charges",
            "migration_notes": "Orders must include the existing TOKEN_ID global in the Billing request payload.",
            "consumer_impact": "Consumers using the prior request shape must add token_id before approval.",
        },
    )


async def run_public_demo(run_id: str, *, root_dir: Optional[Path] = None) -> None:
    """Execute the deterministic public workflow and leave it at approval."""
    root = root_dir or Path(__file__).parent.parent
    result: dict[str, Any] = {"run_id": run_id, "scenario": "billing-token-id"}
    try:
        await _update_run(run_id, status="RUNNING", phase="REGISTERING_SERVICES", result=result)
        await _ensure_services(root)

        await _update_run(run_id, status="RUNNING", phase="PUBLISHING_BASELINE", result=result)
        baseline = await _ensure_baseline_contract(root)
        result["contract_id"] = baseline["contract_id"]
        result["baseline_revision"] = 1
        dependency = await _ensure_dependency(str(baseline["contract_id"]), root)
        result["dependency_id"] = str(dependency["dependency_id"])

        harness = await _ensure_demo_harness(str(dependency["consumer_repository"]))
        result["harness_id"] = str(harness["harness_id"])

        await _update_run(run_id, status="RUNNING", phase="PUBLISHING_BREAKING_CONTRACT", result=result)
        publication = await _publish_token_id_revision(str(baseline["contract_id"]), run_id)
        result["provider_revision"] = 2
        result["contract_revision_id"] = str(publication["contract_revision_id"])
        if publication.get("outbox_event_id"):
            result["contract_event_id"] = str(publication["outbox_event_id"])
        else:
            event = await fetch_one(
                """SELECT event_id FROM coordinator_outbox
                   WHERE event_type='CONTRACT_CHANGED' AND aggregate_id=%s AND aggregate_revision=2
                   ORDER BY created_at DESC LIMIT 1;""",
                (baseline["contract_id"],),
            )
            if event:
                result["contract_event_id"] = str(event["event_id"])

        await _update_run(run_id, status="RUNNING", phase="PROCESSING_DRIFT", result=result)
        if result.get("contract_event_id"):
            await ingest_changefeed_event({"event_id": result["contract_event_id"]})
            await process_all_pending_events(max_count=25)

        await _update_run(run_id, status="RUNNING", phase="DISPATCHING_COMPATIBILITY_WORK", result=result)
        claimed = await claim_next_work_item(
            str(harness["harness_id"]),
            worktree_path=str(root / "worktrees" / f"public-demo-{run_id}"),
            base_commit="public-demo-orders-baseline",
        )
        if claimed:
            result["work_item_id"] = str(claimed["work_item_id"])
            result["task_id"] = str(claimed["task"]["task_id"])
        else:
            work = await fetch_one(
                """SELECT work_item_id, task_id FROM compatibility_work_items
                   WHERE source_contract_id=%s AND source_contract_revision=2
                     AND target_service='orders-service'
                   ORDER BY created_at DESC LIMIT 1;""",
                (baseline["contract_id"],),
            )
            if not work or not work.get("task_id"):
                raise RuntimeError("Compatibility work was not dispatched to the public demo harness")
            result["work_item_id"] = str(work["work_item_id"])
            result["task_id"] = str(work["task_id"])

        await _update_run(run_id, status="RUNNING", phase="CHECKPOINTING_AND_REPLANNING", result=result)
        checkpoint = await record_harness_checkpoint(
            str(harness["harness_id"]),
            result["task_id"],
            {
                "phase": "PLANNING",
                "plan_revision": 1,
                "files_changed": [],
                "assumed_contract_revisions": {"billing-service": 1},
                "test_status": "NOT_RUN",
                "summary": "Public demo agent reached a safe checkpoint before adapting the Billing client.",
            },
        )
        result["checkpoint_instruction"] = checkpoint.get("instruction")
        drift = await fetch_one(
            """SELECT drift_id FROM drift_events WHERE target_task_id=%s
               ORDER BY created_at DESC LIMIT 1;""",
            (result["task_id"],),
        )
        if not drift:
            raise RuntimeError("No durable drift event was created for the public demo task")
        result["drift_id"] = str(drift["drift_id"])
        await start_replanning(result["task_id"])
        result["adaptation"] = "Orders client would add token_id=TOKEN_ID to POST /v1/charges"

        await submit_reconciled_plan(
            task_id=result["task_id"],
            drift_id=result["drift_id"],
            adapted_files=["main.py", "clients/billing_client.py"],
            test_results=_test_evidence(),
            plan_summary="Add the existing global TOKEN_ID to the Orders Billing request and verify the updated contract.",
            auto_reconcile=False,
        )
        await record_compatibility_result(
            result["work_item_id"],
            test_results=_test_evidence(),
            summary="Orders compatibility evidence is ready for human approval; TOKEN_ID is included in the proposed request update.",
        )

        result["final_state"] = "AWAITING_APPROVAL"
        await _update_run(run_id, status="COMPLETED", phase="AWAITING_APPROVAL", result=result)
    except Exception as exc:
        logger.exception("Public demo run %s failed", run_id)
        result["error"] = str(exc)[:500]
        try:
            await _update_run(run_id, status="FAILED", phase="FAILED", result=result)
        except Exception:
            logger.exception("Unable to persist public demo failure for %s", run_id)


async def launch_public_demo() -> dict[str, Any]:
    run = await start_public_demo_run()
    run_id = str(run["run_id"])
    if run.get("new_run"):
        task = asyncio.create_task(run_public_demo(run_id), name=f"codeclaim-public-demo-{run_id}")
        _demo_tasks[run_id] = task

        def _cleanup(done: asyncio.Task) -> None:
            _demo_tasks.pop(run_id, None)
            if not done.cancelled() and done.exception():
                logger.error("Public demo task failed: %s", done.exception())

        task.add_done_callback(_cleanup)
    run.pop("new_run", None)
    run["run_id"] = run_id
    return run
