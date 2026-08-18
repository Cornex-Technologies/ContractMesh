"""Deterministic delivery loop for compatibility work.

It never executes code or asks an LLM to choose a target. It only delivers already
committed work to a registered harness and records delivery evidence.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Optional

import httpx
import psycopg
from psycopg.rows import dict_row

from coordinator.config import settings
from coordinator.db import run_transaction

logger = logging.getLogger(__name__)
MAX_DISPATCH_ATTEMPTS = 3


async def claim_next_dispatch() -> Optional[dict[str, Any]]:
    """Lease one pending work item; network delivery occurs after the transaction commits."""
    async def _tx(conn: psycopg.AsyncConnection) -> Optional[dict[str, Any]]:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT w.work_item_id, w.payload, w.dispatch_attempts, h.dispatch_mode, h.dispatch_url
                FROM compatibility_work_items w
                JOIN harness_registrations h ON h.harness_id = w.harness_id
                WHERE w.state = 'PENDING' AND h.status = 'ACTIVE'
                ORDER BY w.created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED;
                """
            )
            work = await cur.fetchone()
            if not work:
                return None
            attempt = int(work["dispatch_attempts"]) + 1
            await cur.execute(
                """UPDATE compatibility_work_items SET state='DISPATCHED', dispatch_attempts=%s,
                   updated_at=now() WHERE work_item_id=%s;""",
                (attempt, work["work_item_id"]),
            )
            await cur.execute(
                """INSERT INTO compatibility_dispatch_attempts
                   (work_item_id, attempt_number, delivery_status, response_summary)
                   VALUES (%s, %s, %s, %s);""",
                (work["work_item_id"], attempt,
                 "POLL_READY" if work["dispatch_mode"] == "poll" else "DELIVERY_PENDING",
                 "Ready for authenticated polling" if work["dispatch_mode"] == "poll" else "Webhook delivery started"),
            )
            return {**dict(work), "attempt_number": attempt}
    return await run_transaction(_tx)


async def record_webhook_delivery(work_item_id: Any, attempt_number: int, *, status_code: Optional[int], error: Optional[str]) -> None:
    success = status_code is not None and 200 <= status_code < 300
    async def _tx(conn: psycopg.AsyncConnection) -> None:
        async with conn.cursor() as cur:
            await cur.execute(
                """UPDATE compatibility_dispatch_attempts SET delivery_status=%s, response_code=%s,
                   response_summary=%s WHERE work_item_id=%s AND attempt_number=%s;""",
                ("DELIVERED" if success else "RETRYABLE_FAILURE", status_code,
                 (error or "Webhook accepted")[:1000], work_item_id, attempt_number),
            )
            if success:
                await cur.execute(
                    """UPDATE compatibility_work_items SET state='ACKNOWLEDGED', failure_reason=NULL, updated_at=now()
                       WHERE work_item_id=%s AND state='DISPATCHED';""",
                    (work_item_id,),
                )
            else:
                next_state = "FAILED" if attempt_number >= MAX_DISPATCH_ATTEMPTS else "PENDING"
                await cur.execute(
                    """UPDATE compatibility_work_items SET state=%s, failure_reason=%s, updated_at=now()
                       WHERE work_item_id=%s;""", (next_state, (error or "Webhook delivery failed")[:1000], work_item_id),
                )
    await run_transaction(_tx)


async def recover_stale_dispatches(timeout_seconds: int = 60) -> int:
    """Recover orphaned DISPATCHED work items whose harness crashed or timed out."""
    async def _tx(conn: psycopg.AsyncConnection) -> int:
        async with conn.cursor() as cur:
            await cur.execute(
                """UPDATE compatibility_work_items
                   SET state='PENDING', updated_at=now()
                   WHERE state='DISPATCHED'
                     AND updated_at < now() - INTERVAL '1 second' * %s;""",
                (timeout_seconds,),
            )
            return cur.rowcount
    try:
        return await run_transaction(_tx)
    except Exception as ex:
        logger.debug("Could not recover stale dispatches (DB state): %s", ex)
        return 0


async def dispatch_one_pending_work_item() -> Optional[dict[str, Any]]:
    work = await claim_next_dispatch()
    if not work:
        return None
    if work["dispatch_mode"] == "poll":
        return {"work_item_id": str(work["work_item_id"]), "status": "POLL_READY"}

    headers = {"Content-Type": "application/json", "X-CodeClaim-Event": "compatibility_work"}
    if settings.harness_dispatch_webhook_secret:
        headers["X-CodeClaim-Dispatch-Secret"] = settings.harness_dispatch_webhook_secret
    try:
        async with httpx.AsyncClient(timeout=5.0, follow_redirects=False) as client:
            response = await client.post(work["dispatch_url"], json={
                "work_item_id": str(work["work_item_id"]),
                "payload": work["payload"] if isinstance(work["payload"], dict) else json.loads(work["payload"]),
            }, headers=headers)
        await record_webhook_delivery(work["work_item_id"], work["attempt_number"], status_code=response.status_code, error=response.text[:500])
        return {"work_item_id": str(work["work_item_id"]), "status": "DELIVERED" if response.is_success else "RETRYABLE_FAILURE"}
    except Exception as ex:
        await record_webhook_delivery(work["work_item_id"], work["attempt_number"], status_code=None, error=str(ex))
        return {"work_item_id": str(work["work_item_id"]), "status": "RETRYABLE_FAILURE"}


async def run_compatibility_dispatcher_loop(stop_event: asyncio.Event, poll_interval: float = 1.0) -> None:
    loop_count = 0
    while not stop_event.is_set():
        try:
            loop_count += 1
            if loop_count % 30 == 0:
                await recover_stale_dispatches(timeout_seconds=60)
            dispatched = await dispatch_one_pending_work_item()
            if not dispatched:
                await asyncio.wait_for(stop_event.wait(), timeout=poll_interval)
        except asyncio.TimeoutError:
            pass
        except asyncio.CancelledError:
            raise
        except Exception as ex:
            logger.error("Error in compatibility dispatcher loop: %s", ex, exc_info=True)
            await asyncio.sleep(poll_interval)
