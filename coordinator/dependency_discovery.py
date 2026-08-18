"""Deterministic suggestions and explicit confirmation for internal Python HTTP clients."""

from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import psycopg
from psycopg.rows import dict_row

from coordinator.contract_registry import get_service_git_commit
from coordinator.db import execute_query, run_transaction
from coordinator.http_dependencies import persist_http_interface_dependency

_HTTP_METHODS = {"get", "post", "put", "patch", "delete", "head", "options"}


def suggest_python_http_calls(repository_path: Path, endpoint_code_dir: Path) -> list[dict[str, Any]]:
    """Statically extract only literal Python HTTP client calls; ambiguous calls stay suggestions."""
    commit = get_service_git_commit(repository_path)
    suggestions: list[dict[str, Any]] = []
    seen_operations: set[tuple[str, str, str]] = set()
    for source in sorted(endpoint_code_dir.rglob("*.py")):
        content = source.read_bytes()
        try:
            tree = ast.parse(content.decode("utf-8"), filename=str(source))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            method = node.func.attr.lower()
            if method not in _HTTP_METHODS or not node.args:
                continue
            target = node.args[0]
            if not isinstance(target, ast.Constant) or not isinstance(target.value, str):
                continue
            raw_target = target.value
            parsed = urlparse(raw_target)
            path = parsed.path if parsed.scheme else raw_target
            if not path.startswith("/"):
                continue
            source_file = source.relative_to(repository_path).as_posix()
            operation_key = (method.upper(), path, source_file)
            # Multiple transport branches in one client file represent one
            # exact interface binding. Keep the first deterministic source line
            # as evidence rather than prompting the operator repeatedly.
            if operation_key in seen_operations:
                continue
            seen_operations.add(operation_key)
            suggestions.append({
                "http_method": method.upper(),
                "endpoint_path": path,
                "possible_provider": parsed.hostname if parsed.scheme else None,
                "source_file": source_file,
                "source_line": node.lineno,
                "source_evidence": {
                    "source_commit": commit,
                    "content_sha256": hashlib.sha256(content).hexdigest(),
                    "line": node.lineno,
                    "call": f"{method.upper()} {raw_target}",
                },
                "confidence": 0.95 if parsed.hostname else 0.65,
            })
    return suggestions


async def find_provider_operation_candidates(*, http_method: str, endpoint_path: str, provider_service: str | None = None) -> list[dict[str, Any]]:
    """Return exact active provider operations. This is relational lookup, not embedding inference."""
    conditions = ["c.http_method=%s", "c.endpoint_path=%s", "c.lifecycle_state='ACTIVE'", "r.is_active=true"]
    params: list[Any] = [http_method.upper(), endpoint_path]
    if provider_service:
        conditions.append("c.service_name=%s")
        params.append(provider_service)
    return await execute_query(
        f"""SELECT c.contract_id, c.service_name AS provider_service, c.http_method, c.endpoint_path,
                   r.revision_number, r.schema_json
            FROM service_contracts c
            JOIN service_contract_revisions r ON r.contract_id=c.contract_id
            WHERE {' AND '.join(conditions)}
            ORDER BY c.service_name, r.revision_number;""",
        tuple(params),
    )


def candidate_to_confirmed_dependency(*, candidate: dict[str, Any], suggestion: dict[str, Any], confirmed_by: str) -> dict[str, Any]:
    """Build the dependency solely from one chosen exact operation and static consumer evidence."""
    schema = candidate.get("schema_json") or {}
    interface = schema.get("x-codeclaim-http-interface") or {}
    if not interface:
        raise ValueError("Provider contract is not an OpenAPI-normalized HTTP interface; re-onboard the provider first")
    return {
        "provider_service": candidate["provider_service"],
        "contract_id": str(candidate["contract_id"]),
        "assumed_revision": int(candidate["revision_number"]),
        "http_method": candidate["http_method"],
        "endpoint_path": candidate["endpoint_path"],
        "path_parameters": interface.get("path_parameters", {}),
        "query_parameters": interface.get("query_parameters", {}),
        "declared_headers": interface.get("declared_headers", {}),
        "request_body_schema": interface.get("request_body_schema", {}),
        "response_schemas": interface.get("response_schemas", {}),
        "consumer_source_file": suggestion["source_file"],
        "consumer_source_evidence": suggestion["source_evidence"],
        "confirmation_status": "CONFIRMED",
        "confirmed_by": confirmed_by,
    }


async def confirm_internal_http_dependency(*, consumer_service: str, consumer_repository: str, candidate: dict[str, Any], suggestion: dict[str, Any], confirmed_by: str) -> str:
    """Persist one explicitly approved consumer/provider operation link and its audit/outbox events."""
    dependency = candidate_to_confirmed_dependency(candidate=candidate, suggestion=suggestion, confirmed_by=confirmed_by)

    async def _tx(conn: psycopg.AsyncConnection) -> str:
        async with conn.cursor(row_factory=dict_row) as cur:
            dependency_id = await persist_http_interface_dependency(
                cur, dependency=dependency, consumer_service=consumer_service,
                consumer_repository=consumer_repository,
            )
            payload = {
                "dependency_id": dependency_id, "consumer_service": consumer_service,
                "provider_service": dependency["provider_service"], "contract_id": dependency["contract_id"],
                "assumed_revision": dependency["assumed_revision"], "http_method": dependency["http_method"],
                "endpoint_path": dependency["endpoint_path"], "source_file": dependency["consumer_source_file"],
            }
            await cur.execute(
                """INSERT INTO coordinator_outbox (aggregate_type, aggregate_id, aggregate_revision, source_service, event_type, payload)
                   VALUES ('HTTP_INTERFACE_DEPENDENCY', %s, %s, %s, 'INTERNAL_HTTP_DEPENDENCY_CONFIRMED', %s::jsonb)
                   RETURNING event_id;""",
                (dependency_id, dependency["assumed_revision"], consumer_service, json.dumps(payload)),
            )
            outbox = await cur.fetchone()
            if not outbox or not outbox.get("event_id"):
                raise RuntimeError("Failed to create INTERNAL_HTTP_DEPENDENCY_CONFIRMED outbox event")
            outbox_id = outbox["event_id"]
            await cur.execute(
                """INSERT INTO contract_audit_history (
                       event_type, source_service, target_service, summary, actor,
                       outbox_event_id, causation_id, correlation_id
                   ) VALUES ('INTERNAL_HTTP_DEPENDENCY_CONFIRMED', %s, %s, %s, %s, %s, %s, %s);""",
                (consumer_service, dependency["provider_service"],
                 f"Confirmed {consumer_service} -> {dependency['provider_service']} {dependency['http_method']} {dependency['endpoint_path']}",
                 confirmed_by, outbox_id, outbox_id, outbox_id),
            )
            return dependency_id
    return await run_transaction(_tx)
