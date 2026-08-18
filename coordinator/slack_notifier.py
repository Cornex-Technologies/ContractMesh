"""Asynchronous Slack projection of committed coordinator outbox events.

Slack is never part of a correctness-critical transaction. A failed Slack call only
updates this delivery ledger; it cannot roll back or change coordinator state.
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
MAX_ATTEMPTS = 5

# Hackathon Filter: Notify only (1) Breaking contract published, (2) Compatibility work created / replan required, (3) Compatibility blocked.
NOTIFIABLE_EVENTS = {
    # 1. Breaking contract published
    "CONTRACT_CHANGED",
    "BREAKING_CONTRACT_PUBLISHED",
    "ENDPOINT_RETIRED",
    # 2. Compatibility work created / replan required
    "COMPATIBILITY_WORK_CREATED",
    "DRIFT_DETECTED",
    "REPLAN_REQUIRED",
    # 3. Compatibility blocked
    "COMPATIBILITY_BLOCKED",
    "COMPATIBILITY_INCOMPATIBLE",
}


def format_slack_message(event: dict[str, Any]) -> dict[str, Any]:
    """Format sanitized, structured Slack notification.
    
    Security & Privacy Guarantee: Never sends source code, customer data, secrets,
    prompts, scratchpads, or chain-of-thought to Slack.
    """
    payload = event.get("payload") or {}
    if isinstance(payload, str):
        payload = json.loads(payload)
    event_type = event["event_type"]
    service = payload.get("service_name") or payload.get("target_service") or event.get("source_service", "coordinator")

    # 1. Compatibility Blocked / Incompatible
    if event_type in {"COMPATIBILITY_BLOCKED", "COMPATIBILITY_INCOMPATIBLE"}:
        provider = payload.get("source_service") or payload.get("provider_service") or "upstream-service"
        consumer = payload.get("target_service") or service
        rev = payload.get("source_contract_revision") or payload.get("provider_contract_revision") or "?"
        reason_code = payload.get("reason_code", "UNAVAILABLE_REQUIRED_INPUT")
        missing = payload.get("unavailable_required_input") or payload.get("missing_requirement") or "contract requirement"
        worktree = payload.get("worktree_path") or "worktree"
        commit = payload.get("source_commit") or "HEAD"
        resolution = payload.get("requested_resolution") or "Human API/design decision required"
        diff_summary = payload.get("breaking_change") or (payload.get("breaking_diff", {}).get("diff_summary") if isinstance(payload.get("breaking_diff"), dict) else "") or "Required input missing"

        title = "🚨 Compatibility Blocked — Human Decision Required"
        text = f"*{title}*\nConsumer `{consumer}` cannot satisfy Provider `{provider}` (v{rev})"
        details = [
            f"• *Provider & Consumer:* `{provider}` (v{rev}) → `{consumer}`",
            f"• *Reason Code:* `{reason_code}`",
            f"• *Missing Input:* `{missing}`",
            f"• *Breaking Diff:* {diff_summary}",
            f"• *Preserved State:* Worktree `{worktree}`, Commit `{str(commit)[:8]}`",
            f"• *Status:* `Human decision required`",
            f"• *Requested Resolution:* {resolution}",
        ]

    # 2. Breaking Contract Published
    elif event_type in {"CONTRACT_CHANGED", "BREAKING_CONTRACT_PUBLISHED", "ENDPOINT_RETIRED"}:
        rev = payload.get("revision_number") or payload.get("revision") or "?"
        schema_diff = payload.get("schema_diff") or {}
        diff_summary = schema_diff.get("diff_summary") or payload.get("summary") or "New contract revision published"
        migration_note = payload.get("migration_note") or schema_diff.get("migration_note") or ""

        title = "💥 Breaking Contract Published"
        text = f"*{title}*\nService `{service}` published contract v{rev}"
        details = [
            f"• *Service:* `{service}` (v{rev})",
            f"• *Diff Summary:* {diff_summary}",
        ]
        if migration_note:
            details.append(f"• *Migration Note:* {migration_note}")

    # 3. Compatibility Work Created / Replan Required
    elif event_type in {"COMPATIBILITY_WORK_CREATED", "DRIFT_DETECTED", "REPLAN_REQUIRED"}:
        provider = payload.get("source_service", "upstream-service")
        consumer = payload.get("target_service") or service
        old_rev = payload.get("old_contract_revision", "1")
        new_rev = payload.get("new_contract_revision") or payload.get("source_contract_revision") or "2"
        diff_summary = (payload.get("breaking_diff", {}).get("diff_summary") if isinstance(payload.get("breaking_diff"), dict) else "") or payload.get("summary") or "Contract drift detected"

        title = "⚡ Compatibility Work Created (Replan Required)"
        text = f"*{title}*\nConsumer `{consumer}` requires replanning for `{provider}` (v{old_rev} → v{new_rev})"
        details = [
            f"• *Consumer:* `{consumer}`",
            f"• *Provider Contract:* `{provider}` (v{old_rev} → v{new_rev})",
            f"• *Breaking Change:* {diff_summary}",
            f"• *Action:* Agent dispatched to adapt client code at safe checkpoint",
        ]

    else:
        title = event_type.replace("_", " ").title()
        text = f"CodeClaim: {title} — {service}"
        details = [payload.get("summary", "Outbox notification event")]

    return {
        "text": text,
        "blocks": [
            {"type": "header", "text": {"type": "plain_text", "text": title[:150]}},
            {"type": "section", "text": {"type": "mrkdwn", "text": "\n".join([text, *details])[:2900]}},
        ],
    }


async def claim_next_notification() -> Optional[dict[str, Any]]:
    async def _tx(conn: psycopg.AsyncConnection) -> Optional[dict[str, Any]]:
        async with conn.cursor(row_factory=dict_row) as cur:
            placeholders = ",".join(["%s"] * len(NOTIFIABLE_EVENTS))
            await cur.execute(f"""
                SELECT o.event_id, o.event_type, o.source_service, o.payload,
                       o.created_at
                FROM coordinator_outbox o
                WHERE o.event_type IN ({placeholders})
                  AND (
                      NOT EXISTS (
                          SELECT 1 FROM slack_notification_deliveries d
                          WHERE d.event_id = o.event_id
                      )
                      OR EXISTS (
                          SELECT 1 FROM slack_notification_deliveries d
                          WHERE d.event_id = o.event_id
                            AND (
                                (d.status IN ('PENDING', 'RETRY_READY') AND d.next_attempt_at <= now())
                                OR (d.status = 'DELIVERING' AND d.lease_expires_at <= now())
                            )
                      )
                  )
                ORDER BY o.created_at ASC LIMIT 1
                FOR UPDATE SKIP LOCKED;
            """, tuple(sorted(NOTIFIABLE_EVENTS)))
            event = await cur.fetchone()
            if not event:
                return None

            # Lock/read the optional delivery row separately.  CockroachDB
            # rejects FOR UPDATE on the nullable side of a LEFT JOIN; the
            # outbox row remains the lease anchor in this transaction.
            await cur.execute(
                """
                SELECT attempt_count
                FROM slack_notification_deliveries
                WHERE event_id = %s
                FOR UPDATE;
                """,
                (event["event_id"],),
            )
            delivery = await cur.fetchone()
            event = {**dict(event), "attempt_count": int(delivery["attempt_count"]) if delivery else 0}
            payload = event.get("payload") if isinstance(event.get("payload"), dict) else json.loads(event.get("payload") or "{}")
            if event["event_type"] == "CONTRACT_CHANGED":
                diff = payload.get("schema_diff") or {}
                if not diff.get("is_breaking") and diff.get("classification") != "REVIEW_REQUIRED":
                    # Mark non-breaking change as ignored without spamming Slack
                    await cur.execute("""
                        INSERT INTO slack_notification_deliveries (event_id, status, attempt_count, updated_at)
                        VALUES (%s, 'IGNORED_NON_BREAKING', 0, now())
                        ON CONFLICT (event_id) DO UPDATE SET status='IGNORED_NON_BREAKING', updated_at=now();
                    """, (event["event_id"],))
                    return None
            attempt = int(event["attempt_count"]) + 1
            await cur.execute("""
                INSERT INTO slack_notification_deliveries (event_id, status, attempt_count, lease_expires_at, updated_at)
                VALUES (%s, 'DELIVERING', %s, now() + INTERVAL '30 seconds', now())
                ON CONFLICT (event_id) DO UPDATE SET status='DELIVERING', attempt_count=%s,
                    lease_expires_at=now() + INTERVAL '30 seconds', updated_at=now();
            """, (event["event_id"], attempt, attempt))
            return {**dict(event), "attempt_number": attempt}
    return await run_transaction(_tx)


async def record_delivery(event_id: Any, attempt: int, *, response_code: Optional[int], error: Optional[str]) -> None:
    delivered = response_code is not None and 200 <= response_code < 300
    permanent = response_code is not None and 400 <= response_code < 500 and response_code != 429
    delay = min(300, 2 ** attempt)
    async def _tx(conn: psycopg.AsyncConnection) -> None:
        async with conn.cursor() as cur:
            await cur.execute("""
                INSERT INTO slack_notification_attempts (event_id, attempt_number, status, response_code, response_summary)
                VALUES (%s, %s, %s, %s, %s);
            """, (event_id, attempt, "DELIVERED" if delivered else ("PERMANENT_FAILURE" if permanent else "RETRYABLE_FAILURE"), response_code, (error or "Slack accepted")[:1000]))
            if delivered:
                await cur.execute("""UPDATE slack_notification_deliveries SET status='DELIVERED', delivered_at=now(),
                    lease_expires_at=NULL, last_error=NULL, updated_at=now() WHERE event_id=%s;""", (event_id,))
            else:
                status = "FAILED" if permanent or attempt >= MAX_ATTEMPTS else "RETRY_READY"
                await cur.execute("""UPDATE slack_notification_deliveries SET status=%s,
                    next_attempt_at=now() + (%s * INTERVAL '1 second'), lease_expires_at=NULL,
                    last_error=%s, updated_at=now() WHERE event_id=%s;""", (status, delay, (error or "Slack delivery failed")[:1000], event_id))
    await run_transaction(_tx)


async def notify_one() -> Optional[dict[str, Any]]:
    if not settings.slack_notifications_enabled or not settings.slack_webhook_url:
        return None
    event = await claim_next_notification()
    if not event:
        return None
    try:
        async with httpx.AsyncClient(timeout=5.0, follow_redirects=False) as client:
            response = await client.post(settings.slack_webhook_url, json=format_slack_message(event))
        await record_delivery(event["event_id"], event["attempt_number"], response_code=response.status_code, error=response.text[:500])
        return {"event_id": str(event["event_id"]), "delivered": response.is_success}
    except Exception as ex:
        logger.warning("Slack notification failed for %s: %s", event["event_id"], ex)
        await record_delivery(event["event_id"], event["attempt_number"], response_code=None, error=str(ex))
        return {"event_id": str(event["event_id"]), "delivered": False}


async def run_slack_notifier_loop(stop_event: asyncio.Event, poll_interval: float = 1.0) -> None:
    while not stop_event.is_set():
        try:
            if not await notify_one():
                await asyncio.wait_for(stop_event.wait(), timeout=poll_interval)
        except asyncio.TimeoutError:
            pass
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Slack notifier iteration failed")
            await asyncio.sleep(poll_interval)
