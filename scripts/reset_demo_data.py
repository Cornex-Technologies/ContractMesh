"""Safely reset CodeClaim runtime/demo data without changing the schema.

This is intentionally separate from migrations. It clears coordination records
from the configured CockroachDB database while preserving ``schema_migrations``
so the next coordinator startup does not re-run or rewrite migrations.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any

from psycopg.rows import dict_row

from coordinator.db import close_pool, get_pool


# Explicit allowlist: this command must never truncate arbitrary tables supplied
# by a caller. schema_migrations is deliberately excluded.
RESETTABLE_TABLES = (
    "agent_checkpoints",
    "active_agent_tasks",
    "compatibility_dispatch_attempts",
    "compatibility_incidents",
    "compatibility_work_items",
    "contract_audit_history",
    "contract_inventory_findings",
    "contract_inventory_publications",
    "contract_retirements",
    "coordinator_outbox",
    "deployments",
    "drift_events",
    "event_inbox",
    "http_interface_dependencies",
    "harness_registrations",
    "semantic_memory",
    "service_contract_consumers",
    "service_contract_revisions",
    "service_contracts",
    "slack_notification_attempts",
    "slack_notification_deliveries",
    "task_contract_dependencies",
    "microservices",
)

# Child-before-parent order derived from the live migration schema. This avoids
# relying on CockroachDB's cascading TRUNCATE implementation for a demo reset.
RESET_ORDER = (
    "agent_checkpoints",
    "drift_events",
    "task_contract_dependencies",
    "compatibility_dispatch_attempts",
    "compatibility_incidents",
    "compatibility_work_items",
    "contract_inventory_findings",
    "contract_retirements",
    "service_contract_consumers",
    "service_contract_revisions",
    "http_interface_dependencies",
    "slack_notification_attempts",
    "slack_notification_deliveries",
    "coordinator_outbox",
    "active_agent_tasks",
    "harness_registrations",
    "service_contracts",
    "microservices",
    "event_inbox",
    "deployments",
    "semantic_memory",
    "contract_audit_history",
)


async def _existing_tables(cur: Any) -> set[str]:
    await cur.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
        """
    )
    rows = await cur.fetchall()
    return {str(row["table_name"]) for row in rows}


async def _counts(cur: Any, tables: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table in tables:
        await cur.execute(f'SELECT count(*) AS count FROM "{table}";')
        row = await cur.fetchone()
        counts[table] = int(row["count"]) if row else 0
    return counts


async def reset_demo_data(*, expected_database: str, apply: bool) -> dict[str, Any]:
    pool = await get_pool()
    try:
        async with pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute("SELECT current_database() AS database;")
                    database_row = await cur.fetchone()
                    database = str(database_row["database"]) if database_row else ""
                    if database != expected_database:
                        raise RuntimeError(
                            f"Refusing reset: connected to database '{database}', expected '{expected_database}'."
                        )

                    existing = await _existing_tables(cur)
                    tables = [table for table in RESETTABLE_TABLES if table in existing]
                    before = await _counts(cur, tables)

                    if apply:
                        for table in RESET_ORDER:
                            if table in existing:
                                # Table names come only from RESET_ORDER above.
                                await cur.execute(f'DELETE FROM "{table}";')
                        after = await _counts(cur, tables)
                    else:
                        after = before.copy()

                    return {
                        "database": database,
                        "applied": apply,
                        "preserved_tables": ["schema_migrations"],
                        "tables": tables,
                        "before": before,
                        "after": after,
                    }
    finally:
        await close_pool()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Reset CodeClaim runtime/demo rows while preserving schema migrations"
    )
    parser.add_argument(
        "--database-name",
        default="codeclaim_db",
        help="Exact CockroachDB database name required for the reset (default: codeclaim_db)",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Actually truncate the allowlisted runtime tables; without this flag only a dry run is performed",
    )
    args = parser.parse_args(argv)

    result = asyncio.run(reset_demo_data(expected_database=args.database_name, apply=args.yes))
    print(json.dumps(result, indent=2, default=str))
    if not args.yes:
        print("Dry run only. Re-run with --yes after stopping the coordinator to apply the reset.")
    else:
        print("Runtime/demo data cleared. schema_migrations was preserved.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
