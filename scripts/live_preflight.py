"""Read-only preflight for a local coordinator backed by CockroachDB Cloud.

This command is intentionally safe to run against an existing database. It
checks process/database health and the revision-one Billing/Orders baseline but
never onboards, registers, approves, deploys, deletes, or updates records.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from typing import Any, Iterable

import httpx

from coordinator.db import check_health, close_pool, execute_query


UNRESOLVED_WORK_STATES = frozenset(
    {
        "PENDING",
        "DISPATCHED",
        "ACKNOWLEDGED",
        "EXECUTING",
        "AWAITING_APPROVAL",
        "REVIEW_REQUIRED",
        "BLOCKED",
        "INCOMPATIBLE",
    }
)


def _check(name: str, passed: bool, summary: str, details: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "name": name,
        "passed": bool(passed),
        "summary": summary,
        "details": details or {},
    }


def evaluate_baseline(
    contracts: Iterable[dict[str, Any]],
    dependencies: Iterable[dict[str, Any]],
    compatibility_work: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    """Evaluate the exact baseline required by the live late-change scenario."""
    contract_rows = list(contracts)
    dependency_rows = list(dependencies)
    work_rows = list(compatibility_work)

    billing_charge_contracts = [
        row
        for row in contract_rows
        if str(row.get("service_name")) == "billing-service"
        and str(row.get("endpoint_path")) == "/v1/charges"
        and str(row.get("http_method", "")).upper() == "POST"
    ]
    latest_revision = max(
        (int(row.get("latest_revision", 0)) for row in billing_charge_contracts),
        default=0,
    )

    dependency_matches = [
        row
        for row in dependency_rows
        if str(row.get("consumer_service")) == "orders-service"
        and str(row.get("provider_service")) == "billing-service"
        and str(row.get("endpoint_path")) == "/v1/charges"
        and str(row.get("http_method", "")).upper() == "POST"
    ]
    confirmed_revisions = [
        int(row.get("assumed_provider_revision", 0))
        for row in dependency_matches
        if str(row.get("confirmation_status")) == "CONFIRMED"
    ]

    unresolved_work = [
        row
        for row in work_rows
        if str(row.get("provider_service")) == "billing-service"
        and str(row.get("state")) in UNRESOLVED_WORK_STATES
    ]

    checks = [
        _check(
            "billing_revision_one",
            latest_revision == 1,
            "Billing POST /v1/charges is at revision 1"
            if latest_revision == 1
            else "Billing POST /v1/charges must exist at revision 1 before the scenario",
            {"latest_revision": latest_revision, "matching_contracts": billing_charge_contracts},
        ),
        _check(
            "orders_dependency_revision_one",
            bool(confirmed_revisions) and max(confirmed_revisions) == 1,
            "Orders has a confirmed Billing POST /v1/charges revision-1 dependency"
            if confirmed_revisions and max(confirmed_revisions) == 1
            else "Orders must have one confirmed Billing POST /v1/charges dependency at revision 1",
            {"confirmed_revisions": confirmed_revisions, "matches": dependency_matches},
        ),
        _check(
            "no_unresolved_billing_work",
            not unresolved_work,
            "No unresolved Billing compatibility work is blocking the baseline"
            if not unresolved_work
            else "Resolve or isolate existing Billing compatibility work before running the scenario",
            {"work_item_ids": [str(row.get("work_item_id")) for row in unresolved_work]},
        ),
    ]
    return {
        "ready": all(check["passed"] for check in checks),
        "checks": checks,
        "unresolved_work": unresolved_work,
    }


async def _get_json(client: httpx.AsyncClient, url: str, headers: dict[str, str] | None = None) -> dict[str, Any]:
    response = await client.get(url, headers=headers)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected JSON object from {url}")
    return payload


async def run_preflight(
    *,
    base_url: str = "http://127.0.0.1:8000",
    public_base_url: str | None = None,
    operator_token: str | None = None,
) -> dict[str, Any]:
    """Run local/public HTTP and read-only CockroachDB baseline checks."""
    result: dict[str, Any] = {"ready": False, "checks": [], "database": None, "harnesses": []}
    normalized_base_url = base_url.rstrip("/")

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            health = await _get_json(client, f"{normalized_base_url}/health")
            result["checks"].append(
                _check(
                    "local_coordinator_health",
                    health.get("status") == "healthy" and health.get("demo_mode") is False,
                    "Local coordinator is healthy and not in demo mode"
                    if health.get("status") == "healthy" and health.get("demo_mode") is False
                    else "Local coordinator must report status=healthy and demo_mode=false",
                    health,
                )
            )
        except Exception as exc:
            result["checks"].append(
                _check("local_coordinator_health", False, "Cannot reach the local coordinator", {"error": str(exc)})
            )

        if public_base_url:
            normalized_public_url = public_base_url.rstrip("/")
            public_ok = False
            public_details: dict[str, Any] = {}
            try:
                public_health = await _get_json(client, f"{normalized_public_url}/health")
                public_ok = public_health.get("status") == "healthy" and public_health.get("demo_mode") is False
                public_details = public_health
            except Exception as exc:
                public_details = {"error": str(exc)}
            result["checks"].append(
                _check(
                    "public_webhook_reachability",
                    public_ok and normalized_public_url.startswith("https://"),
                    "Public HTTPS tunnel reaches the same live coordinator"
                    if public_ok and normalized_public_url.startswith("https://")
                    else "Public base URL must be HTTPS and reach the live coordinator",
                    public_details,
                )
            )

        if operator_token:
            try:
                await _get_json(
                    client,
                    f"{normalized_base_url}/api/dashboard/state",
                    headers={"X-Operator-Token": operator_token},
                )
                result["checks"].append(
                    _check("operator_authentication", True, "Operator token can read dashboard state")
                )
            except Exception as exc:
                result["checks"].append(
                    _check("operator_authentication", False, "Operator token cannot read dashboard state", {"error": str(exc)})
                )

    try:
        database = await check_health()
        result["database"] = database
        db_ok = database.get("status") == "healthy"
        result["checks"].append(
            _check(
                "cockroach_database_health",
                db_ok,
                "CockroachDB Cloud is reachable"
                if db_ok
                else "CockroachDB Cloud is not reachable",
                database,
            )
        )
        if db_ok:
            contracts = await execute_query(
                """
                SELECT c.service_name, c.endpoint_path, c.http_method,
                       max(r.revision_number) AS latest_revision
                FROM service_contracts c
                JOIN service_contract_revisions r ON r.contract_id = c.contract_id
                WHERE c.service_name IN ('billing-service', 'orders-service')
                GROUP BY c.service_name, c.endpoint_path, c.http_method
                ORDER BY c.service_name, c.endpoint_path;
                """
            )
            dependencies = await execute_query(
                """
                SELECT consumer_service, provider_service, endpoint_path, http_method,
                       assumed_provider_revision, confirmation_status
                FROM http_interface_dependencies
                WHERE consumer_service = 'orders-service'
                  AND provider_service = 'billing-service'
                  AND endpoint_path = '/v1/charges';
                """
            )
            compatibility_work = await execute_query(
                """
                SELECT w.work_item_id, c.service_name AS provider_service,
                       w.target_service, w.source_contract_revision, w.state
                FROM compatibility_work_items w
                JOIN service_contracts c ON c.contract_id = w.source_contract_id
                WHERE c.service_name = 'billing-service';
                """
            )
            harnesses = await execute_query(
                """
                SELECT harness_id, harness_name, harness_type, service_name, status
                FROM harness_registrations
                ORDER BY created_at DESC;
                """
            )
            result["harnesses"] = harnesses
            baseline = evaluate_baseline(contracts, dependencies, compatibility_work)
            result["checks"].extend(baseline["checks"])
            result["unresolved_work"] = baseline["unresolved_work"]
    except Exception as exc:
        result["checks"].append(
            _check("database_baseline_queries", False, "Read-only baseline queries failed", {"error": str(exc)})
        )
    finally:
        await close_pool()

    result["ready"] = all(check["passed"] for check in result["checks"])
    return result


def _print_report(result: dict[str, Any]) -> None:
    print("CodeClaim live preflight")
    print("=" * 26)
    for check in result.get("checks", []):
        marker = "PASS" if check["passed"] else "BLOCKED"
        print(f"[{marker}] {check['name']}: {check['summary']}")
        if not check["passed"] and check.get("details"):
            print(f"         details: {json.dumps(check['details'], default=str)}")
    print()
    print("READY: yes" if result.get("ready") else "READY: no")
    if result.get("harnesses"):
        print(f"Registered harnesses observed: {len(result['harnesses'])}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run read-only local/cloud CodeClaim live preflight checks")
    parser.add_argument("--base-url", default=os.environ.get("CODECLAIM_BASE_URL", "http://127.0.0.1:8000"))
    parser.add_argument(
        "--public-base-url",
        default=os.environ.get("CHANGEFEED_PUBLIC_BASE_URL"),
        help="Optional ngrok/HTTPS URL used by CockroachDB Cloud for /events/cockroach",
    )
    parser.add_argument("--operator-token", default=os.environ.get("COORDINATOR_API_KEY"))
    parser.add_argument("--json", action="store_true", dest="as_json", help="Print machine-readable JSON only")
    args = parser.parse_args(argv)
    result = asyncio.run(
        run_preflight(
            base_url=args.base_url,
            public_base_url=args.public_base_url,
            operator_token=args.operator_token,
        )
    )
    if args.as_json:
        print(json.dumps(result, default=str))
    else:
        _print_report(result)
    return 0 if result["ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
