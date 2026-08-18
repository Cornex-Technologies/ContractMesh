"""CodeClaim MCP server for trusted coding harnesses.

This is intentionally separate from CockroachDB Managed MCP: Managed MCP is read-only
audit access, while this server exposes the coordinator's validated workflow operations.
Run it next to a trusted local harness, not as an unauthenticated public endpoint.
"""

from __future__ import annotations

from typing import Any

from mcp.server.fastmcp import FastMCP

from coordinator.compatibility import (
    authenticate_harness,
    claim_next_work_item,
    complete_harness_task,
    record_compatibility_incident,
    record_compatibility_result,
    record_harness_checkpoint,
    register_harness_task,
)
from coordinator.config import settings
from coordinator.db import fetch_one
from coordinator.memory import discover_and_verify_dependencies
from coordinator.reconciliation import check_task_drift
from coordinator.contract_registry import publish_contract_inventory, publish_contract_revision as publish_contract_revision_internal, retire_contract

mcp = FastMCP("CodeClaim Coordination")


async def _configured_harness() -> dict[str, Any]:
    """Authenticate via MCP process environment so credentials are never tool arguments."""
    if not settings.mcp_harness_id or not settings.mcp_harness_token:
        raise RuntimeError("MCP_HARNESS_ID and MCP_HARNESS_TOKEN must be configured for write tools")
    return await authenticate_harness(settings.mcp_harness_id, settings.mcp_harness_token)


async def _require_owned_work(work_item_id: str) -> None:
    harness = await _configured_harness()
    work = await fetch_one("SELECT harness_id FROM compatibility_work_items WHERE work_item_id=%s;", (work_item_id,))
    if not work or str(work["harness_id"]) != str(harness["harness_id"]):
        raise ValueError("Compatibility work is not assigned to this MCP harness")


@mcp.tool()
async def get_harness_identity() -> dict[str, Any]:
    """Verify this MCP process is connected as the expected registered harness.

    This is intentionally read-only and is useful as the first smoke test after
    configuring Codex or another local MCP client.  It does not expose the
    token or database connection details.
    """
    harness = await _configured_harness()
    return {
        "status": "success",
        "summary": f"Authenticated as {harness['harness_type']} harness '{harness['harness_name']}'",
        "next_actions": [
            "Use discover_relevant_contracts before registering consumer work",
            "Use claim_compatibility_work when compatibility work is available",
        ],
        "artifacts": {
            "harness_id": str(harness["harness_id"]),
            "harness_type": harness["harness_type"],
            "service_name": harness["service_name"],
            "repository_url": harness["repository_url"],
        },
    }


@mcp.tool()
async def register_task(
    task_summary: str,
    worktree_path: str,
    base_commit: str,
    dependencies: list[dict[str, Any]],
    task_id: str | None = None,
) -> dict[str, Any]:
    """Register a coding task and its declared contract assumptions with the coordinator."""
    harness = await _configured_harness()
    return await register_harness_task(
        harness,
        task_summary=task_summary,
        worktree_path=worktree_path,
        base_commit=base_commit,
        dependencies=dependencies,
        task_id=task_id,
    )


@mcp.tool()
async def discover_relevant_contracts(service_name: str, task_prompt: str, repository: str) -> list[dict[str, Any]]:
    """Return relationally confirmed contract dependencies for a planned coding task."""
    harness = await _configured_harness()
    if harness["service_name"] != service_name:
        raise ValueError(f"Configured MCP harness is registered for service '{harness['service_name']}', cannot discover for '{service_name}'")
    return await discover_and_verify_dependencies(
        consumer_service=service_name, task_prompt=task_prompt, consumer_repo=repository
    )


@mcp.tool()
async def claim_compatibility_work(worktree_path: str, base_commit: str) -> dict[str, Any] | None:
    """Atomically claim one queued compatibility task for the configured harness."""
    harness = await _configured_harness()
    return await claim_next_work_item(str(harness["harness_id"]), worktree_path=worktree_path, base_commit=base_commit)


@mcp.tool()
async def complete_task(
    task_id: str,
    summary: str = "Task completed by harness",
    test_results: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Complete a normal owned task while retaining its confirmed dependencies as history."""
    harness = await _configured_harness()
    return await complete_harness_task(
        str(harness["harness_id"]), task_id, summary=summary, test_results=test_results
    )


@mcp.tool()
async def get_pending_drift(task_id: str) -> dict[str, Any] | None:
    """Return the current deterministic drift instruction at a checkpoint boundary."""
    harness = await _configured_harness()
    task = await fetch_one("SELECT agent_id FROM active_agent_tasks WHERE task_id=%s;", (task_id,))
    expected_agent_id = f"{harness['harness_type']}:{harness['harness_name']}"
    if not task or task.get("agent_id") != expected_agent_id:
        raise ValueError(f"Task {task_id} is not owned by this MCP harness ({expected_agent_id})")
    return await check_task_drift(task_id)


@mcp.tool()
async def publish_contract_revision(
    service_name: str,
    endpoint_path: str,
    http_method: str,
    revision_number: int,
    schema_json: dict[str, Any],
    source_commit: str,
    semantic_summary: str = "",
    publisher_compatibility: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Publish a versioned OpenAPI contract revision from the authenticated harness."""
    harness = await _configured_harness()
    if harness["service_name"] != service_name:
        raise ValueError(f"Configured MCP harness is registered for service '{harness['service_name']}', cannot publish for '{service_name}'")
    return await publish_contract_revision_internal(
        service_name=service_name,
        endpoint_path=endpoint_path,
        http_method=http_method,
        revision_number=revision_number,
        schema_json=schema_json,
        source_commit=source_commit,
        semantic_summary=semantic_summary,
        published_by=f"{harness['harness_type']}:{harness['harness_name']}",
        publisher_compatibility=publisher_compatibility,
    )


@mcp.tool()
async def checkpoint_task(
    task_id: str,
    phase: str,
    files_changed: list[str],
    assumed_contract_revisions: dict[str, int],
    test_status: str = "NOT_RUN",
    plan_revision: int = 1,
    summary: str = "",
) -> dict[str, Any]:
    """Persist typed operational metadata only and return CONTINUE or REPLAN_REQUIRED."""
    harness = await _configured_harness()
    return await record_harness_checkpoint(str(harness["harness_id"]), task_id, {
        "task_id": task_id,
        "plan_revision": plan_revision,
        "phase": phase,
        "files_changed": files_changed,
        "changed_files": files_changed,
        "assumed_contract_revisions": assumed_contract_revisions,
        "test_status": test_status,
        "summary": summary,
    })


@mcp.tool()
async def submit_compatibility_evidence(work_item_id: str, summary: str, test_results: dict[str, Any]) -> dict[str, Any]:
    """Submit passing test evidence for human approval; this tool never deploys or merges code."""
    await _require_owned_work(work_item_id)
    return await record_compatibility_result(
        work_item_id, summary=summary, test_results=test_results
    )


@mcp.tool()
async def report_incompatible_contract(
    work_item_id: str,
    outcome: str,
    requested_resolution: str,
    missing_requirement: str = "",
    unavailable_required_input: str | None = None,
    reason_code: str = "UNAVAILABLE_REQUIRED_INPUT",
    provider_service: str | None = None,
    provider_contract_revision: int | None = None,
    sources_checked: list[str] | None = None,
    worktree_path: str | None = None,
    source_commit: str | None = None,
    changed_files: list[str] | None = None,
    evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Escalate a BLOCKED or INCOMPATIBLE contract requirement for a human API decision.
    
    Preserves worktree, branch, commit, and file evidence, creates an append-only audit event,
    and blocks automated deployment promotion until resolved by a human engineer.
    """
    await _require_owned_work(work_item_id)
    return await record_compatibility_incident(
        work_item_id=work_item_id,
        outcome=outcome,
        requested_resolution=requested_resolution,
        missing_requirement=missing_requirement,
        unavailable_required_input=unavailable_required_input,
        reason_code=reason_code,
        provider_service=provider_service,
        provider_contract_revision=provider_contract_revision,
        sources_checked=sources_checked,
        worktree_path=worktree_path,
        source_commit=source_commit,
        changed_files=changed_files,
        evidence=evidence,
    )


@mcp.tool()
async def retire_endpoint(service_name: str, endpoint_path: str, http_method: str, source_commit: str, migration_note: str, replacement_contract_key: str | None = None) -> dict[str, Any]:
    """Publish an explicit breaking endpoint tombstone from the authenticated harness."""
    harness = await _configured_harness()
    if harness["service_name"] != service_name:
        raise ValueError("Configured MCP harness may retire only its registered service")
    return await retire_contract(
        service_name=service_name, endpoint_path=endpoint_path, http_method=http_method,
        source_commit=source_commit, migration_note=migration_note,
        retired_by=f"{harness['harness_type']}:{harness['harness_name']}",
        replacement_contract_key=replacement_contract_key,
    )


@mcp.tool()
async def publish_endpoint_inventory(service_name: str, source_commit: str, contracts: list[dict[str, str]]) -> dict[str, Any]:
    """Fail closed if an active endpoint disappears without an explicit retirement tombstone."""
    harness = await _configured_harness()
    if harness["service_name"] != service_name:
        raise ValueError("Configured MCP harness may publish inventory only for its registered service")
    return await publish_contract_inventory(
        service_name=service_name, source_commit=source_commit, contracts=contracts,
        published_by=f"{harness['harness_type']}:{harness['harness_name']}",
    )


if __name__ == "__main__":
    mcp.run()
