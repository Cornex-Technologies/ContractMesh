"""Section 6 Verification Suite: Changefeed Receiver, Inbox Lease & Drift Worker."""

import json
import uuid
from unittest.mock import AsyncMock, MagicMock, patch
import pytest
from httpx import ASGITransport, AsyncClient

from coordinator.app import app
from coordinator.config import settings
from coordinator.drift_worker import (
    claim_next_inbox_event,
    process_all_pending_events,
    process_claimed_event,
    process_single_inbox_event,
)
from coordinator.db import check_health, close_pool, execute_query, execute_statement, init_db


# ==============================================================================
# 1. Changefeed Webhook Ingestion & Authentication Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_changefeed_webhook_auth_success_and_failure():
    """Verify webhook endpoint enforces secret authentication via headers."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        with patch.object(settings, "changefeed_webhook_secret", "super-secret-token"), \
             patch.object(settings, "is_demo_mode", False), \
             patch("coordinator.app.fetch_one", AsyncMock(return_value={"event_id": "11111111-1111-1111-1111-111111111111"})):

            payload = {
                "event_id": "11111111-1111-1111-1111-111111111111",
                "event_type": "CONTRACT_CHANGED",
                "payload": {"service_name": "billing-service"},
            }

            # 1. Unauthorized request (No Header) -> 401
            resp_no_auth = await client.post("/events/cockroach", json=payload)
            assert resp_no_auth.status_code == 401

            # 2. Unauthorized request (Wrong Header) -> 401
            resp_bad_auth = await client.post(
                "/events/cockroach",
                json=payload,
                headers={"X-Webhook-Secret": "wrong-secret"},
            )
            assert resp_bad_auth.status_code == 401

            # 3. Authorized request via X-Webhook-Secret -> 200
            resp_ok_secret = await client.post(
                "/events/cockroach",
                json=payload,
                headers={"X-Webhook-Secret": "super-secret-token"},
            )
            assert resp_ok_secret.status_code == 200
            data = resp_ok_secret.json()
            assert data["status"] == "received"

            # 4. Authorized request via Bearer Header -> 200
            resp_ok_bearer = await client.post(
                "/events/cockroach",
                json=payload,
                headers={"Authorization": "Bearer super-secret-token"},
            )
            assert resp_ok_bearer.status_code == 200


@pytest.mark.asyncio
async def test_changefeed_webhook_wrapped_payload_array_batch_ingestion():
    """Verify CockroachDB changefeed batched delivery under a payload array is decoded properly."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        inserted_ids = []

        async def mock_fetch_one(query, params=None):
            if params:
                inserted_ids.append(params[0])
                return {"event_id": params[0]}
            return None

        with patch.object(settings, "changefeed_webhook_secret", "test-secret"), \
             patch("coordinator.app.fetch_one", side_effect=mock_fetch_one):

            # Standard CockroachDB changefeed wrapped payload array
            batch_payload = {
                "payload": [
                    {
                        "event_id": "11111111-1111-1111-1111-111111111111",
                        "event_type": "CONTRACT_CHANGED",
                        "payload": {"service_name": "billing-service", "revision_number": 2},
                    },
                    {
                        "event_id": "22222222-2222-2222-2222-222222222222",
                        "event_type": "CONTRACT_CHANGED",
                        "payload": {"service_name": "orders-service", "revision_number": 1},
                    },
                ]
            }

            resp = await client.post(
                "/events/cockroach",
                json=batch_payload,
                headers={"X-Webhook-Secret": "test-secret"},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["count"] == 2
            assert len(data["events"]) == 2
            assert "11111111-1111-1111-1111-111111111111" in inserted_ids
            assert "22222222-2222-2222-2222-222222222222" in inserted_ids


@pytest.mark.asyncio
async def test_changefeed_webhook_db_failure_returns_500():
    """Verify that a database write failure returns HTTP 500 to trigger changefeed delivery retry."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        with patch.object(settings, "changefeed_webhook_secret", "test-secret"), \
             patch("coordinator.app.fetch_one", side_effect=RuntimeError("CockroachDB transaction aborted")):

            payload = {
                "event_id": "11111111-1111-1111-1111-111111111111",
                "event_type": "CONTRACT_CHANGED",
            }

            resp = await client.post(
                "/events/cockroach",
                json=payload,
                headers={"X-Webhook-Secret": "test-secret"},
            )
            assert resp.status_code == 500
            assert "Database failure persisting event" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_changefeed_webhook_idempotent_ingestion():
    """Verify that duplicate changefeed deliveries are safely deduplicated in event_inbox."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        call_count = 0
        async def mock_fetch_one(query, params=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {"event_id": "11111111-1111-1111-1111-111111111111"}
            return None  # ON CONFLICT DO NOTHING returns None

        with patch.object(settings, "changefeed_webhook_secret", "test-secret"), \
             patch("coordinator.app.fetch_one", side_effect=mock_fetch_one):

            payload = {
                "event_id": "11111111-1111-1111-1111-111111111111",
                "event_type": "CONTRACT_CHANGED",
                "payload": {"contract_id": "ctr-1", "revision_number": 2},
            }

            # Delivery 1: Inserted
            res1 = await client.post(
                "/events/cockroach",
                json=payload,
                headers={"X-Webhook-Secret": "test-secret"},
            )
            assert res1.status_code == 200
            assert res1.json()["events"][0]["is_new"] is True

            # Delivery 2: Deduplicated (Idempotent)
            res2 = await client.post(
                "/events/cockroach",
                json=payload,
                headers={"X-Webhook-Secret": "test-secret"},
            )
            assert res2.status_code == 200
            assert res2.json()["events"][0]["is_new"] is False


# ==============================================================================
# 2. Breaking-Change Gating & Drift Generation Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_drift_worker_breaking_change_triggers_replan():
    """Verify that breaking contract changes trigger ACTIVE_INTERVENTION and REPLAN_REQUIRED."""
    mock_cur = AsyncMock()

    claimed_event = {
        "event_id": "evt-101",
        "processing_status": "RECEIVED",
        "attempt_count": 0,
        "event_type": "CONTRACT_CHANGED",
        "source_service": "billing-service",
        "outbox_payload": {
            "contract_id": "ctr-101",
            "revision_number": 2,
            "service_name": "billing-service",
            "schema_diff": {
                "is_breaking": True,
                "breaking_changes": [{"field": "payment_method_id", "change": "new required field"}],
            },
        },
    }

    affected_tasks = [
        {
            "task_id": "task-orders-102",
            "provider_service": "billing-service",
            "contract_id": "ctr-101",
            "assumed_revision": 1,
            "target_service": "orders-service",
            "agent_id": "agent-b",
            "task_status": "OPTIMISTIC_EXECUTING",
        }
    ]

    executed_queries = []

    async def mock_execute(sql, params=None):
        executed_queries.append((sql, params))

    mock_cur.execute = mock_execute
    
    fetch_call = 0
    fetchall_call = 0
    async def mock_fetchall():
        nonlocal fetchall_call
        fetchall_call += 1
        # The breaking-change path first loads in-flight tasks, then loads
        # confirmed consumers while creating durable compatibility work.
        return affected_tasks if fetchall_call == 1 else []

    async def mock_fetchone():
        nonlocal fetch_call
        fetch_call += 1
        if fetch_call == 1:
            return {
                "event_id": claimed_event["event_id"],
                "processing_status": claimed_event["processing_status"],
                "attempt_count": claimed_event["attempt_count"],
                "inbox_payload": claimed_event.get("inbox_payload", {}),
            }
        elif fetch_call == 2:
            return {
                "aggregate_type": "CONTRACT",
                "aggregate_id": "ctr-101",
                "aggregate_revision": 2,
                "source_service": "billing-service",
                "event_type": "CONTRACT_CHANGED",
                "outbox_payload": claimed_event["outbox_payload"],
            }
        elif fetch_call == 3:
            return {"event_id": "evt-101"}
        elif fetch_call == 4:
            return {"drift_id": "drift-901"}
        elif fetch_call == 5:
            return {"event_id": "outbox-901"}
        return None

    mock_cur.fetchone = mock_fetchone
    mock_cur.fetchall = mock_fetchall

    mock_conn = MagicMock()
    mock_cur_ctx = AsyncMock()
    mock_cur_ctx.__aenter__.return_value = mock_cur
    mock_cur_ctx.__aexit__.return_value = None
    mock_conn.cursor.return_value = mock_cur_ctx

    # 1. Test Atomic Claim
    claimed = await claim_next_inbox_event(mock_conn)
    assert claimed is not None

    # 2. Test Processing & Drift Generation
    result = await process_claimed_event(mock_conn, claimed)
    assert result["status"] == "PROCESSED"
    assert len(result["drift_events"]) == 1
    drift = result["drift_events"][0]
    assert drift["target_service"] == "orders-service"
    assert drift["is_breaking"] is True

    # Verify SQL statements executed
    sql_texts = " ".join(q[0] for q in executed_queries)
    all_params = [p for q in executed_queries if q[1] for p in q[1]]
    assert "INSERT INTO drift_events" in sql_texts
    assert "ACTIVE_INTERVENTION" in sql_texts or "ACTIVE_INTERVENTION" in all_params
    assert "UPDATE active_agent_tasks" in sql_texts
    assert "REPLAN_REQUIRED" in sql_texts or "REPLAN_REQUIRED" in all_params
    assert "TASK_REPLAN_REQUIRED" in sql_texts or "TASK_REPLAN_REQUIRED" in all_params

    # The inbox lease query must not use a nullable-side outer join with
    # FOR UPDATE; CockroachDB rejects that statement shape.
    claim_query = executed_queries[0][0]
    assert "LEFT JOIN coordinator_outbox" not in claim_query
    assert "FOR UPDATE SKIP LOCKED" in claim_query
    assert any("FROM coordinator_outbox" in query for query, _ in executed_queries)


@pytest.mark.asyncio
async def test_drift_worker_non_breaking_change_does_not_interrupt_task():
    """Verify that non-breaking changes (is_breaking: False) are marked PROCESSED without interrupting tasks."""
    mock_cur = AsyncMock()

    claimed_event = {
        "event_id": "evt-non-breaking-101",
        "processing_status": "RECEIVED",
        "attempt_count": 0,
        "event_type": "CONTRACT_CHANGED",
        "source_service": "billing-service",
        "outbox_payload": {
            "contract_id": "ctr-101",
            "revision_number": 2,
            "service_name": "billing-service",
            "schema_diff": {
                "is_breaking": False,
                "non_breaking_changes": [{"field": "metadata", "change": "optional field added"}],
            },
        },
    }

    affected_tasks = [
        {
            "task_id": "task-orders-102",
            "provider_service": "billing-service",
            "contract_id": "ctr-101",
            "assumed_revision": 1,
            "target_service": "orders-service",
            "agent_id": "agent-b",
            "task_status": "OPTIMISTIC_EXECUTING",
        }
    ]

    executed_queries = []
    async def mock_execute(sql, params=None):
        executed_queries.append((sql, params))

    mock_cur.execute = mock_execute
    mock_cur.fetchone.return_value = claimed_event
    mock_cur.fetchall.return_value = affected_tasks

    mock_conn = MagicMock()
    mock_cur_ctx = AsyncMock()
    mock_cur_ctx.__aenter__.return_value = mock_cur
    mock_cur_ctx.__aexit__.return_value = None
    mock_conn.cursor.return_value = mock_cur_ctx

    result = await process_claimed_event(mock_conn, claimed_event)
    assert result["status"] == "PROCESSED"
    # Zero drift events created because change is non-breaking!
    assert len(result["drift_events"]) == 0

    # Ensure NO replan or drift insertion was executed
    sql_texts = " ".join(q[0] for q in executed_queries)
    all_params = [p for q in executed_queries if q[1] for p in q[1]]
    assert "INSERT INTO drift_events" not in sql_texts
    assert "ACTIVE_INTERVENTION" not in sql_texts and "ACTIVE_INTERVENTION" not in all_params
    assert "REPLAN_REQUIRED" not in sql_texts and "REPLAN_REQUIRED" not in all_params


# ==============================================================================
# 3. Worker Error Handling & Failure State Persistence Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_drift_worker_persists_failure_state_without_rollback():
    """Verify that worker errors are durably recorded as FAILED in event_inbox without transaction rollback."""
    mock_cur = AsyncMock()

    claimed_event = {
        "event_id": "evt-failing-101",
        "processing_status": "RECEIVED",
        "attempt_count": 0,
        "event_type": "CONTRACT_CHANGED",
        "outbox_payload": {"contract_id": "ctr-1"},
    }

    executed_queries = []
    async def mock_execute(sql, params=None):
        executed_queries.append((sql, params))
        if "active_agent_tasks" in sql:
            raise RuntimeError("CockroachDB connection timeout")

    mock_cur.execute = mock_execute
    mock_cur.fetchone.return_value = claimed_event

    mock_conn = MagicMock()
    mock_cur_ctx = AsyncMock()
    mock_cur_ctx.__aenter__.return_value = mock_cur
    mock_cur_ctx.__aexit__.return_value = None
    mock_conn.cursor.return_value = mock_cur_ctx

    with patch("coordinator.drift_worker.claim_next_inbox_event", AsyncMock(return_value=claimed_event)), \
         patch("coordinator.drift_worker.run_transaction") as mock_run_tx:

        async def run_tx_side_effect(fn):
            return await fn(mock_conn)
        mock_run_tx.side_effect = run_tx_side_effect

        result = await process_single_inbox_event()
        assert result is not None
        assert result["status"] == "FAILED"
        assert "CockroachDB connection timeout" in result["error"]

        # Verify FAILED status and attempt_count increment was written to event_inbox
        sql_texts = " ".join(q[0] for q in executed_queries)
        assert "UPDATE event_inbox" in sql_texts
        assert "FAILED" in sql_texts
        assert "attempt_count = attempt_count + 1" in sql_texts



@pytest.mark.asyncio
async def test_drift_worker_drain_all_pending_events():
    """Verify process_all_pending_events loops until inbox is empty."""
    call_count = 0
    async def mock_process_single():
        nonlocal call_count
        call_count += 1
        if call_count <= 3:
            return {"event_id": f"evt-{call_count}", "status": "PROCESSED"}
        return None

    with patch("coordinator.drift_worker.process_single_inbox_event", side_effect=mock_process_single):
        total_processed = await process_all_pending_events(max_count=10)
        assert total_processed == 3


# ==============================================================================
# 4. Live CockroachDB Integration Test
# ==============================================================================


@pytest.mark.integration
@pytest.mark.asyncio
async def test_live_changefeed_and_drift_worker():
    """Live test: Ingests event into event_inbox and executes drift worker."""
    health = await check_health(timeout_seconds=3.0)
    if health.get("status") != "healthy":
        pytest.skip(f"Live CockroachDB is not reachable: {health.get('error')}")

    try:
        await init_db()

        event_id = str(uuid.uuid4())
        payload = {"service_name": "billing-service", "event_type": "CONTRACT_CHANGED"}
        await execute_statement(
            """
            INSERT INTO event_inbox (event_id, processing_status, payload)
            VALUES (%s, 'RECEIVED', %s::jsonb)
            ON CONFLICT (event_id) DO NOTHING;
            """,
            (event_id, json.dumps(payload)),
        )

        res = await process_single_inbox_event()
        assert res is not None

        row = await execute_query("SELECT processing_status FROM event_inbox WHERE event_id = %s;", (event_id,))
        assert len(row) == 1
        assert row[0]["processing_status"] == "PROCESSED"

    finally:
        await close_pool()
