"""REST-only live harness scenario for the late compatibility demo.

This script is deliberately a thin external harness client.  It does not call
``agent_runner.py`` or coordinator internals for workflow mutations.  A real
Codex/Antigravity harness can replace the scripted checkpoint/evidence calls,
or ``--manual`` can pause after the late claim for that interaction.

Required environment:
    CODECLAIM_BASE_URL
    COORDINATOR_API_KEY
    BILLING_HARNESS_ID / BILLING_HARNESS_TOKEN
    ORDERS_HARNESS_ID / ORDERS_HARNESS_TOKEN

Set CODECLAIM_REGISTER_HARNESSES=true to register both harnesses and print the
one-time tokens instead.  A fresh database with onboarded Billing and Orders
contracts, plus one confirmed Orders -> Billing dependency, is expected.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import httpx


ROOT_DIR = Path(__file__).resolve().parents[1]
BILLING_REPO = ROOT_DIR / "repos" / "billing-service"
ORDERS_REPO = ROOT_DIR / "repos" / "orders-service"


def _env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name, default)
    return value.strip() if isinstance(value, str) else value


def _git_head(path: Path) -> str:
    configured = _env("BILLING_SOURCE_COMMIT" if path.name == "billing-service" else "ORDERS_SOURCE_COMMIT")
    if configured:
        return configured
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=str(path), capture_output=True, text=True, check=True
    )
    return result.stdout.strip()


def _orders_worktree() -> Path:
    configured = _env("ORDERS_WORKTREE_PATH")
    return Path(configured).expanduser().resolve() if configured else ORDERS_REPO.resolve()


def _model_schema(module_filename: str, model_name: str) -> dict[str, Any]:
    """Use the same deterministic static Pydantic extraction as onboarding."""
    from coordinator.contract_registry import extract_pydantic_schema_from_repo

    return extract_pydantic_schema_from_repo(BILLING_REPO, module_filename, model_name)


def _billing_contract_schema(revision: int) -> dict[str, Any]:
    module = "schemas_v1.py" if revision == 1 else "schemas_v2.py"
    request = _model_schema(module, "ChargeRequest")
    response = _model_schema(module, "ChargeResponse")
    return {
        "type": "object",
        "x-codeclaim-http-interface": {
            "http_method": "POST",
            "endpoint_path": "/v1/charges",
            "path_parameters": {},
            "query_parameters": {},
            "declared_headers": {},
            "request_body_schema": request,
            "request_body_schemas_by_media": {"application/json": request},
            "response_schemas": {"200": response},
            "response_schemas_by_media": {"200": {"application/json": response}},
            "security_requirements": [],
            "security_schemes": {},
        },
    }


def _orders_dependency(contract_id: str, source_commit: str) -> dict[str, Any]:
    source = ORDERS_REPO / "clients" / "billing_client.py"
    content = source.read_bytes()
    return {
        "provider_service": "billing-service",
        "contract_id": contract_id,
        "assumed_revision": 1,
        "http_method": "POST",
        "endpoint_path": "/v1/charges",
        "path_parameters": {},
        "query_parameters": {},
        "declared_headers": {},
        "request_body_schema": _billing_contract_schema(1)["x-codeclaim-http-interface"]["request_body_schema"],
        "response_schemas": {"200": _billing_contract_schema(1)["x-codeclaim-http-interface"]["response_schemas"]["200"]},
        "consumer_source_file": "clients/billing_client.py",
        "consumer_source_evidence": {
            "source_commit": source_commit,
            "content_sha256": hashlib.sha256(content).hexdigest(),
            "line": 30,
            "call": "POST /v1/charges",
        },
        "confirmation_status": "CONFIRMED",
        "confirmed_by": "live-orders-codex",
    }


class LiveClient:
    def __init__(self, client: httpx.AsyncClient, operator_token: str):
        self.client = client
        self.operator_token = operator_token

    def operator_headers(self) -> dict[str, str]:
        return {"X-Operator-Token": self.operator_token}

    async def request(self, method: str, path: str, *, headers: dict[str, str] | None = None, **kwargs: Any) -> Any:
        response = await self.client.request(method, path, headers=headers, **kwargs)
        if response.is_error:
            raise RuntimeError(f"{method} {path} -> {response.status_code}: {response.text[:1000]}")
        return response.json() if response.content else {}

    async def harness_request(self, method: str, harness_id: str, token: str, path: str, **kwargs: Any) -> Any:
        headers = {"X-Harness-Token": token}
        return await self.request(method, path, headers=headers, **kwargs)


def _validate_live_environment(base_url: str) -> None:
    """Reject local demo shortcuts before any live workflow mutation occurs."""
    if _env("IS_DEMO_MODE", "false").lower() == "true":
        raise RuntimeError("Live harness scenario refuses IS_DEMO_MODE=true")
    if _env("DEMO_AUTO_RECONCILE", "false").lower() == "true":
        raise RuntimeError("Live harness scenario refuses DEMO_AUTO_RECONCILE=true")
    normalized = base_url.lower().rstrip("/")
    if normalized.startswith("http://") and not any(
        host in normalized for host in ("http://127.0.0.1", "http://localhost", "http://[::1]")
    ):
        raise RuntimeError("Remote live coordinator URLs must use HTTPS")


async def _assert_live_server(client: LiveClient) -> dict[str, Any]:
    """Verify the remote coordinator is healthy and not running demo mode."""
    health = await client.request("GET", "/health")
    if health.get("demo_mode") is True:
        raise RuntimeError("Coordinator reports demo_mode=true; start it with IS_DEMO_MODE=false")
    database = health.get("database") or {}
    if health.get("status") != "healthy" or database.get("status") != "healthy":
        raise RuntimeError(f"Coordinator live preflight failed: {json.dumps(health, default=str)}")
    return health


async def _register_harnesses(client: LiveClient) -> tuple[str, str, str, str]:
    register = _env("CODECLAIM_REGISTER_HARNESSES", "false").lower() == "true"
    billing_id, billing_token = _env("BILLING_HARNESS_ID"), _env("BILLING_HARNESS_TOKEN")
    orders_id, orders_token = _env("ORDERS_HARNESS_ID"), _env("ORDERS_HARNESS_TOKEN")
    if not register and all((billing_id, billing_token, orders_id, orders_token)):
        return billing_id, billing_token, orders_id, orders_token  # type: ignore[return-value]
    if not register:
        raise RuntimeError("Provide harness IDs/tokens or set CODECLAIM_REGISTER_HARNESSES=true")

    registered: list[tuple[str, str]] = []
    for name, service, repo in (
        ("live-antigravity-billing", "billing-service", BILLING_REPO),
        ("live-codex-orders", "orders-service", ORDERS_REPO),
    ):
        result = await client.request(
            "POST", "/harnesses/register", headers=client.operator_headers(),
            json={
                "harness_name": name,
                "harness_type": "antigravity" if service == "billing-service" else "codex",
                "service_name": service,
                "repository_url": str(repo.resolve()),
                "dispatch_mode": "poll",
                "capability_manifest": {"python": True, "fastapi": True, "rest": True},
            },
        )
        registered.append((str(result["harness_id"]), str(result["access_token"])))
        print(f"Registered {service} harness: id={result['harness_id']} token={result['access_token']}")
    return registered[0][0], registered[0][1], registered[1][0], registered[1][1]


async def register_live_harnesses() -> bool:
    """Register fresh Billing and Orders harnesses and print one-time tokens."""
    base_url = _env("CODECLAIM_BASE_URL", "http://127.0.0.1:8000")
    operator_token = _env("COORDINATOR_API_KEY")
    if not operator_token:
        raise RuntimeError("COORDINATOR_API_KEY is required to register live harnesses")
    if _env("CODECLAIM_REGISTER_HARNESSES", "false").lower() != "true":
        raise RuntimeError("Set CODECLAIM_REGISTER_HARNESSES=true for --register-only")
    _validate_live_environment(base_url)
    async with httpx.AsyncClient(base_url=base_url.rstrip("/"), timeout=30.0) as http_client:
        client = LiveClient(http_client, operator_token)
        await _assert_live_server(client)
        await _register_harnesses(client)
    return True


async def run_live_harness_scenario(*, manual: bool = False) -> bool:
    base_url = _env("CODECLAIM_BASE_URL", "http://127.0.0.1:8000")
    operator_token = _env("COORDINATOR_API_KEY")
    if not operator_token:
        raise RuntimeError("COORDINATOR_API_KEY is required for the live REST scenario")
    _validate_live_environment(base_url)

    async with httpx.AsyncClient(base_url=base_url.rstrip("/"), timeout=30.0) as http_client:
        client = LiveClient(http_client, operator_token)
        await _assert_live_server(client)
        billing_id, billing_token, orders_id, orders_token = await _register_harnesses(client)

        state = await client.request("GET", "/api/dashboard/state", headers=client.operator_headers())
        dependency = next(
            (
                row for row in state.get("dependencies", [])
                if row.get("consumer_service") == "orders-service"
                and row.get("provider_service") == "billing-service"
                and row.get("http_method") == "POST"
                and row.get("endpoint_path") == "/v1/charges"
                and int(row.get("assumed_provider_revision", 0)) == 1
            ),
            None,
        )
        if not dependency:
            raise RuntimeError("No confirmed Orders -> Billing POST /v1/charges revision 1 dependency found")

        orders_commit = _git_head(ORDERS_REPO)
        billing_commit = _git_head(BILLING_REPO)
        dependency_payload = _orders_dependency(str(dependency["contract_id"]), orders_commit)

        print("[1/9] Registering Orders task against Billing revision 1 via REST")
        task_result = await client.harness_request(
            "POST", orders_id, orders_token, f"/harnesses/{orders_id}/tasks",
            json={
                "task_summary": "Orders checkout work using Billing revision 1",
                "worktree_path": str(_orders_worktree()),
                "base_commit": orders_commit,
                "dependencies": [dependency_payload],
            },
        )
        initial_task_id = str(task_result["task_id"])

        print("[2/9] Completing the initial Orders task; dependency remains durable history")
        await client.harness_request(
            "POST", orders_id, orders_token, f"/harnesses/{orders_id}/tasks/{initial_task_id}/complete",
            json={
                "summary": "Orders baseline work completed before provider change",
                "test_results": {"returncode": 0, "all_passed": True, "framework": "pytest", "summary": "Orders baseline tests passed"},
            },
        )

        print("[3/9] Publishing Billing revision 2 through the Billing harness REST endpoint")
        publication = await client.harness_request(
            "POST", billing_id, billing_token, f"/harnesses/{billing_id}/contracts/publish",
            json={
                "service_name": "billing-service",
                "endpoint_path": "/v1/charges",
                "http_method": "POST",
                "revision_number": 2,
                "schema_json": _billing_contract_schema(2),
                "source_commit": billing_commit,
                "semantic_summary": "Billing requires payment_method_id; legacy card_token is removed",
            },
        )
        print(f"  Published revision {publication.get('revision_number', 2)}; waiting for claimable work")

        print("[4/9] Orders harness claims late compatibility work")
        claimed: dict[str, Any] | None = None
        for _ in range(30):
            result = await client.harness_request(
                "POST", orders_id, orders_token, f"/harnesses/{orders_id}/compatibility-work/claim",
                json={"worktree_path": str(_orders_worktree()), "base_commit": orders_commit},
            )
            claimed = result.get("work")
            if claimed:
                break
            await asyncio.sleep(1)
        if not claimed:
            raise RuntimeError("No late compatibility work became claimable")
        work_item_id = str(claimed["work_item_id"])
        compatibility_task_id = str(claimed["task"]["task_id"])
        print(f"  Claimed work={work_item_id} task={compatibility_task_id} mode={claimed.get('assignment_mode')}")

        if manual:
            input("Edit Orders with Codex now, run its tests, then press Enter to submit evidence... ")

        print("[5/9] Recording checkpoint and passing compatibility evidence via REST")
        await client.harness_request(
            "POST", orders_id, orders_token,
            f"/harnesses/{orders_id}/tasks/{compatibility_task_id}/checkpoint",
            json={
                "task_id": compatibility_task_id,
                "phase": "TESTING",
                "files_changed": ["clients/billing_client.py"],
                "assumed_contract_revisions": {"billing-service": 2},
                "test_status": "PASSED",
                "summary": "Orders client now sends payment_method_id for Billing v2",
            },
        )
        await client.harness_request(
            "POST", orders_id, orders_token,
            f"/harnesses/{orders_id}/compatibility-work/{work_item_id}/result",
            json={
                "summary": "Orders compatibility tests passed",
                "test_results": {"returncode": 0, "all_passed": True, "framework": "pytest", "summary": "Compatibility suite passed"},
            },
        )

        print("[6/9] Attempting Billing deployment before approval (must be rejected)")
        rejected = await client.request(
            "POST", "/deploy/promote", headers=client.operator_headers(),
            json={"service_name": "billing-service", "source_commit": billing_commit, "health_check_timeout": 3.0},
        )
        if rejected.get("status") != "FAILED" or not rejected.get("compatibility_blockers"):
            raise RuntimeError(f"Deployment gate did not reject unresolved work: {json.dumps(rejected, default=str)}")
        print(f"  Rejected with {len(rejected['compatibility_blockers'])} compatibility blocker(s)")

        print("[7/9] Operator approves work and atomically rebinds Orders to Billing revision 2")
        approved = await client.request(
            "POST", f"/compatibility-work/{work_item_id}/approve", headers=client.operator_headers(),
            json={"actor": "live-demo-operator"},
        )
        if approved.get("state") != "VERIFIED" or approved.get("dependency_rebound") is not True:
            raise RuntimeError(f"Compatibility approval did not rebind dependency: {approved}")

        print("[8/9] Retrying Billing deployment after approval")
        deployed = await client.request(
            "POST", "/deploy/promote", headers=client.operator_headers(),
            json={"service_name": "billing-service", "source_commit": billing_commit, "health_check_timeout": 3.0},
        )
        if deployed.get("status") not in {"HEALTHY", "COMPLETED"}:
            raise RuntimeError(f"Approved deployment did not complete: {json.dumps(deployed, default=str)}")

        await client.request(
            "POST", f"/compatibility-work/{work_item_id}/complete", headers=client.operator_headers(),
            json={"actor": "live-demo-operator"},
        )
        print("[9/9] Live REST scenario complete: task → contract change → late claim → gate → approval → deployment")
        return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run CodeClaim's REST-only live late-compatibility scenario")
    parser.add_argument("--manual", action="store_true", help="Pause after late claim for a real coding harness to edit Orders")
    parser.add_argument(
        "--register-only",
        action="store_true",
        help="Register fresh Billing and Orders harnesses, print one-time tokens, and stop",
    )
    args = parser.parse_args(argv)
    try:
        if args.register_only:
            return 0 if asyncio.run(register_live_harnesses()) else 1
        return 0 if asyncio.run(run_live_harness_scenario(manual=args.manual)) else 1
    except Exception as exc:
        print(f"LIVE SCENARIO FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
