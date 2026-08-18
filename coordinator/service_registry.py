"""Transactional registration for internal CodeClaim services."""

from __future__ import annotations

import json
from typing import Any

import psycopg
from psycopg.rows import dict_row

from coordinator.db import run_transaction


async def register_internal_service(
    *,
    service_name: str,
    repository_path: str,
    actor: str,
    application_entrypoint: str = "main:app",
) -> dict[str, Any]:
    """Register an internal repository and append audit/outbox records atomically."""
    async def _tx(conn: psycopg.AsyncConnection) -> dict[str, Any]:
        async with conn.cursor(row_factory=dict_row) as cur:
            if ":" not in application_entrypoint:
                raise ValueError("application_entrypoint must use module:app format")
            entrypoint_module, entrypoint_app = application_entrypoint.rsplit(":", 1)
            if not entrypoint_module or not entrypoint_app:
                raise ValueError("application_entrypoint must use module:app format")
            await cur.execute(
                """INSERT INTO microservices (
                       service_name, repository_path, entrypoint_module, entrypoint_app,
                       registration_source, registered_by
                   ) VALUES (%s, %s, %s, %s, 'ONBOARDING_CLI', %s)
                   ON CONFLICT (service_name) DO UPDATE SET
                       repository_path=EXCLUDED.repository_path,
                       entrypoint_module=EXCLUDED.entrypoint_module,
                       entrypoint_app=EXCLUDED.entrypoint_app,
                       registration_source='ONBOARDING_CLI',
                       registered_by=EXCLUDED.registered_by
                   RETURNING service_id, service_name, repository_path,
                             entrypoint_module, entrypoint_app,
                             registration_source, registered_by, registration_event_id;""",
                (service_name, repository_path, entrypoint_module, entrypoint_app, actor),
            )
            service = await cur.fetchone()
            await cur.execute(
                """INSERT INTO coordinator_outbox (aggregate_type, aggregate_id, aggregate_revision, source_service, event_type, payload)
                   VALUES ('MICROSERVICE', %s, 1, %s, 'SERVICE_ONBOARDED', %s::jsonb)
                   RETURNING event_id;""",
                (service["service_id"], service_name, json.dumps({"service_name": service_name, "repository_path": repository_path, "actor": actor})),
            )
            outbox = await cur.fetchone()
            if not outbox or not outbox.get("event_id"):
                raise RuntimeError("Failed to create SERVICE_ONBOARDED outbox event")
            outbox_id = outbox["event_id"]
            await cur.execute(
                """UPDATE microservices
                   SET registration_event_id = %s
                   WHERE service_id = %s;""",
                (outbox_id, service["service_id"]),
            )
            await cur.execute(
                """INSERT INTO contract_audit_history (
                       event_type, source_service, summary, actor,
                       outbox_event_id, causation_id, correlation_id
                   ) VALUES ('SERVICE_ONBOARDED', %s, %s, %s, %s, %s, %s);""",
                (service_name, f"Registered internal FastAPI repository {repository_path}", actor,
                 outbox_id, outbox_id, outbox_id),
            )
            return {
                **dict(service),
                "registration_source": "ONBOARDING_CLI",
                "registered_by": actor,
                "registration_event_id": str(outbox_id),
                "outbox_event_id": str(outbox_id),
            }
    return await run_transaction(_tx)
