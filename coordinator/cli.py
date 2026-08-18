"""CodeClaim command-line interface. Only ``onboard`` mutates coordinator state."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
import sys
from typing import Any

from coordinator.contract_registry import get_service_git_commit, publish_contract_revision
from coordinator.db import fetch_one
from coordinator.dependency_discovery import (
    confirm_internal_http_dependency,
    find_provider_operation_candidates,
    suggest_python_http_calls,
)
from coordinator.onboarding import OnboardingPlan, make_plan, render_plan
from coordinator.service_registry import register_internal_service


def _prompt(value: str | None, label: str) -> str:
    return value if value else input(f"{label}: ").strip()


def _run(coroutine: Any) -> Any:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    return asyncio.run(coroutine)


async def apply_onboarding(plan: OnboardingPlan, *, actor: str = "codeclaim-onboard") -> dict[str, Any]:
    """Apply an already-confirmed plan; this is intentionally unreachable before confirmation in ``main``."""
    source_commit = get_service_git_commit(plan.repository_path)
    # A one-time onboarding must not silently create a duplicate revision series.
    for contract in plan.contracts:
        contract_key = f"{plan.service_name}:{contract['http_method']}:{contract['endpoint_path']}"
        existing = await fetch_one(
            """SELECT COALESCE(MAX(r.revision_number), 0) AS revision
               FROM service_contracts c JOIN service_contract_revisions r ON r.contract_id=c.contract_id
               WHERE c.contract_key=%s;""", (contract_key,),
        )
        if existing and int(existing["revision"]) > 0:
            raise ValueError(f"Contract {contract['http_method']} {contract['endpoint_path']} is already onboarded")
    application_entrypoint = plan.app_entry or "main:app"
    service = await register_internal_service(
        service_name=plan.service_name,
        repository_path=str(plan.repository_path),
        actor=actor,
        application_entrypoint=application_entrypoint,
    )
    publications = []
    for contract in plan.contracts:
        publications.append(await publish_contract_revision(
            service_name=plan.service_name,
            endpoint_path=contract["endpoint_path"],
            http_method=contract["http_method"],
            revision_number=1,
            schema_json=contract["schema_json"],
            semantic_summary=contract["semantic_summary"],
            published_by=actor,
            source_commit=source_commit,
        ))
    config_dir = plan.repository_path / ".codeclaim"
    config_dir.mkdir(exist_ok=True)
    config_path = config_dir / "service.json"
    config = {
        "version": 1,
        "service_name": plan.service_name,
        "framework": "fastapi",
        "application_entrypoint": application_entrypoint,
        "contract_source": {"app_entry": plan.app_entry, "openapi_url": plan.openapi_url},
        "endpoint_code_dir": str(plan.endpoint_code_dir.relative_to(plan.repository_path)),
        "review_required": plan.review_findings,
    }
    temp_path = config_path.with_suffix(".json.tmp")
    temp_path.write_text(json.dumps(config, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temp_path, config_path)
    return {"service": service, "publications": publications, "config_path": str(config_path)}


async def apply_revision_publication(
    plan: OnboardingPlan,
    *,
    endpoint_path: str,
    http_method: str,
    actor: str = "codeclaim-publish",
    requested_revision: int | None = None,
) -> dict[str, Any]:
    """Publish the next immutable revision for one already-onboarded operation.

    The current FastAPI OpenAPI document is extracted deterministically from the
    repository, while the next revision is resolved from CockroachDB. This is
    intentionally separate from one-time onboarding: it never registers a new
    service and it refuses an operation that is not already in the contract
    ledger.
    """
    normalized_method = http_method.upper()
    matching = [
        contract
        for contract in plan.contracts
        if contract["endpoint_path"] == endpoint_path
        and contract["http_method"].upper() == normalized_method
    ]
    if len(matching) != 1:
        raise ValueError(
            f"Expected exactly one extracted operation for {normalized_method} {endpoint_path}; found {len(matching)}"
        )
    contract = matching[0]
    contract_key = f"{plan.service_name}:{normalized_method}:{endpoint_path}"
    existing = await fetch_one(
        """
        SELECT c.contract_id, COALESCE(MAX(r.revision_number), 0) AS revision
        FROM service_contracts c
        LEFT JOIN service_contract_revisions r ON r.contract_id = c.contract_id
        WHERE c.contract_key = %s
        GROUP BY c.contract_id;
        """,
        (contract_key,),
    )
    if not existing or not existing.get("contract_id"):
        raise ValueError(
            f"Operation {normalized_method} {endpoint_path} is not onboarded for {plan.service_name}; run codeclaim onboard first"
        )
    latest_revision = int(existing.get("revision") or 0)
    revision_number = requested_revision if requested_revision is not None else latest_revision + 1
    if revision_number <= latest_revision:
        raise ValueError(
            f"Requested revision {revision_number} is not newer than the current revision {latest_revision}"
        )
    source_commit = get_service_git_commit(plan.repository_path)
    publication = await publish_contract_revision(
        service_name=plan.service_name,
        endpoint_path=endpoint_path,
        http_method=normalized_method,
        revision_number=revision_number,
        schema_json=contract["schema_json"],
        semantic_summary=contract["semantic_summary"],
        published_by=actor,
        source_commit=source_commit,
    )
    return {"publication": publication, "revision_number": revision_number, "source_commit": source_commit}


def _onboard(args: argparse.Namespace) -> int:
    service_name = _prompt(args.service_name, "Service name")
    repository_raw = _prompt(args.repository_path, "Repository path")
    repository_path = Path(repository_raw).expanduser()
    endpoint_raw = _prompt(args.endpoint_code_dir, "Endpoint-code directory")
    endpoint_code_dir = Path(endpoint_raw).expanduser()
    if not endpoint_code_dir.is_absolute():
        endpoint_code_dir = repository_path / endpoint_code_dir
    app_entry = args.app_entry
    if not args.openapi_url:
        app_entry = _prompt(app_entry, "FastAPI app entry (for example app.main:app)")
    plan = make_plan(
        service_name=service_name, repository_path=repository_path,
        endpoint_code_dir=endpoint_code_dir, app_entry=app_entry, openapi_url=args.openapi_url,
    )
    print(render_plan(plan))
    approved = args.yes or input("Apply this onboarding plan? [y/N] ").strip().lower() in {"y", "yes"}
    if not approved:
        print("Onboarding cancelled. No database or filesystem changes were made.")
        return 0
    result = _run(apply_onboarding(plan))
    print(f"Onboarding complete: {len(result['publications'])} contracts published; config: {result['config_path']}")
    return 0


def _publish_revision(args: argparse.Namespace) -> int:
    service_name = _prompt(args.service_name, "Service name")
    repository_raw = _prompt(args.repository_path, "Repository path")
    repository_path = Path(repository_raw).expanduser()
    endpoint_raw = _prompt(args.endpoint_code_dir, "Endpoint-code directory")
    endpoint_code_dir = Path(endpoint_raw).expanduser()
    if not endpoint_code_dir.is_absolute():
        endpoint_code_dir = repository_path / endpoint_code_dir
    app_entry = args.app_entry
    if not args.openapi_url:
        app_entry = _prompt(app_entry, "FastAPI app entry (for example app.main:app)")
    plan = make_plan(
        service_name=service_name,
        repository_path=repository_path,
        endpoint_code_dir=endpoint_code_dir,
        app_entry=app_entry,
        openapi_url=args.openapi_url,
    )
    method = args.http_method.upper()
    matching = [
        contract
        for contract in plan.contracts
        if contract["http_method"].upper() == method and contract["endpoint_path"] == args.endpoint_path
    ]
    print("CodeClaim contract revision publication plan")
    print(f"  Service: {service_name}")
    print(f"  Repository: {repository_path.resolve()}")
    print(f"  Source: {app_entry or args.openapi_url}")
    print(f"  Operation: {method} {args.endpoint_path}")
    print(f"  Extracted matches: {len(matching)}")
    if plan.review_findings:
        print("  REVIEW_REQUIRED findings:")
        for finding in plan.review_findings:
            print(f"    - {finding}")
    approved = args.yes or input("Publish the next immutable contract revision? [y/N] ").strip().lower() in {"y", "yes"}
    if not approved:
        print("Publication cancelled. No database or filesystem changes were made.")
        return 0
    result = _run(
        apply_revision_publication(
            plan,
            endpoint_path=args.endpoint_path,
            http_method=method,
            actor=args.published_by,
            requested_revision=args.revision,
        )
    )
    publication = result["publication"]
    print(
        f"Contract revision published: {service_name} {method} {args.endpoint_path} "
        f"v{result['revision_number']} (outbox={publication.get('outbox_event_id')})"
    )
    return 0


def _review_dependencies(args: argparse.Namespace) -> int:
    consumer_service = _prompt(args.consumer_service, "Consumer service")
    repository_path = Path(_prompt(args.repository_path, "Consumer repository path")).expanduser().resolve()
    endpoint_dir = Path(_prompt(args.endpoint_code_dir, "Consumer endpoint/client-code directory")).expanduser()
    if not endpoint_dir.is_absolute():
        endpoint_dir = repository_path / endpoint_dir
    if not repository_path.is_dir() or not endpoint_dir.is_dir():
        raise ValueError("Consumer repository and endpoint/client-code directory must exist")
    suggestions = suggest_python_http_calls(repository_path, endpoint_dir)
    if not suggestions:
        print("No deterministic literal Python HTTP client calls found. No dependencies were registered.")
        return 0
    confirmed_by = args.confirmed_by or "operator"
    for suggestion in suggestions:
        provider = args.provider_service or suggestion["possible_provider"]
        candidates = _run(find_provider_operation_candidates(
            http_method=suggestion["http_method"], endpoint_path=suggestion["endpoint_path"], provider_service=provider,
        ))
        print("\nDependency suggestion")
        print(f"  Consumer: {consumer_service}")
        print(f"  Possible provider: {provider or 'unresolved'}")
        print(f"  Operation: {suggestion['http_method']} {suggestion['endpoint_path']}")
        print(f"  Source evidence: {suggestion['source_file']}:{suggestion['source_line']} ({suggestion['source_evidence']['content_sha256'][:12]})")
        print(f"  Confidence: {suggestion['confidence']:.2f}")
        for index, candidate in enumerate(candidates, start=1):
            print(f"    [{index}] {candidate['provider_service']} {candidate['http_method']} {candidate['endpoint_path']} v{candidate['revision_number']}")
        action = input("Action: [c]onfirm internal dependency, [i]gnore, [e]dit: ").strip().lower()
        if action == "i":
            print("Ignored. No dependency was registered.")
            continue
        if action == "e":
            provider = input("Provider service: ").strip()
            method = input("HTTP method: ").strip().upper()
            path = input("Endpoint path: ").strip()
            candidates = _run(find_provider_operation_candidates(http_method=method, endpoint_path=path, provider_service=provider))
            suggestion = {**suggestion, "http_method": method, "endpoint_path": path}
        if action not in {"c", "e"}:
            print("Skipped. No dependency was registered.")
            continue
        if not candidates:
            print("No exact provider operation matched. No dependency was registered.")
            continue
        if len(candidates) > 1:
            selected = input("Choose exact provider operation number, or press Enter to cancel: ").strip()
            if not selected.isdigit() or not 1 <= int(selected) <= len(candidates):
                print("Ambiguous match not confirmed. No dependency was registered.")
                continue
            candidate = candidates[int(selected) - 1]
        else:
            candidate = candidates[0]
        dependency_id = _run(confirm_internal_http_dependency(
            consumer_service=consumer_service, consumer_repository=str(repository_path), candidate=candidate,
            suggestion=suggestion, confirmed_by=confirmed_by,
        ))
        print(f"Confirmed internal dependency {dependency_id}.")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="codeclaim", description="CodeClaim coordination tools")
    commands = parser.add_subparsers(dest="command", required=True)
    onboard = commands.add_parser("onboard", help="Onboard one internal FastAPI service from OpenAPI")
    onboard.add_argument("--service-name")
    onboard.add_argument("--repository-path")
    onboard.add_argument("--endpoint-code-dir")
    onboard.add_argument("--app-entry", help="FastAPI application object, for example app.main:app")
    onboard.add_argument("--openapi-url", help="Loopback URL ending in /openapi.json")
    onboard.add_argument("--yes", action="store_true", help="Apply after printing the plan without interactive confirmation")
    onboard.set_defaults(handler=_onboard)
    publish = commands.add_parser(
        "publish-revision",
        help="Extract one current FastAPI operation and publish its next immutable contract revision",
    )
    publish.add_argument("--service-name")
    publish.add_argument("--repository-path")
    publish.add_argument("--endpoint-code-dir")
    publish.add_argument("--app-entry", help="FastAPI application object, for example app.main:app")
    publish.add_argument("--openapi-url", help="Loopback URL ending in /openapi.json")
    publish.add_argument("--endpoint-path", required=True)
    publish.add_argument("--http-method", required=True)
    publish.add_argument("--revision", type=int, help="Optional explicit revision; otherwise the next DB revision is used")
    publish.add_argument("--published-by", default="codeclaim-publish")
    publish.add_argument("--yes", action="store_true", help="Publish after printing the plan without interactive confirmation")
    publish.set_defaults(handler=_publish_revision)
    dependencies = commands.add_parser("dependencies", help="Review and explicitly confirm Python HTTP client dependencies")
    dependencies.add_argument("--consumer-service")
    dependencies.add_argument("--repository-path")
    dependencies.add_argument("--endpoint-code-dir", help="Directory containing Python HTTP client code")
    dependencies.add_argument("--provider-service", help="Optional provider filter; unresolved calls are never auto-confirmed")
    dependencies.add_argument("--confirmed-by", help="Operator identity recorded with an explicit confirmation")
    dependencies.set_defaults(handler=_review_dependencies)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
