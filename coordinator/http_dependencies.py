"""Validation and persistence for CodeClaim's exact internal HTTP/JSON dependencies."""

from __future__ import annotations

import json
from typing import Any

_HTTP_METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}
_CONFIRMATION_STATUSES = {"DECLARED", "CONFIRMED", "REJECTED"}
_REQUIRED_FIELDS = {
    "provider_service", "contract_id", "assumed_revision", "http_method", "endpoint_path",
    "path_parameters", "query_parameters", "declared_headers", "request_body_schema",
    "response_schemas", "consumer_source_file", "consumer_source_evidence", "confirmation_status",
}


def validate_http_interface_dependency(dependency: dict[str, Any]) -> dict[str, Any]:
    """Validate the complete observable HTTP boundary and its consumer-side evidence."""
    missing = _REQUIRED_FIELDS - set(dependency)
    if missing:
        raise ValueError(f"Exact HTTP dependency is missing required fields: {sorted(missing)}")

    normalized = dict(dependency)
    normalized["provider_service"] = str(normalized["provider_service"]).strip()
    normalized["http_method"] = str(normalized["http_method"]).strip().upper()
    normalized["endpoint_path"] = str(normalized["endpoint_path"]).strip()
    normalized["consumer_source_file"] = str(normalized["consumer_source_file"]).strip()
    normalized["confirmation_status"] = str(normalized["confirmation_status"]).strip().upper()
    if not normalized["provider_service"] or not normalized["consumer_source_file"]:
        raise ValueError("provider_service and consumer_source_file are required")
    if normalized["http_method"] not in _HTTP_METHODS:
        raise ValueError(f"http_method must be one of {sorted(_HTTP_METHODS)}")
    if not normalized["endpoint_path"].startswith("/"):
        raise ValueError("endpoint_path must start with '/'")
    if not isinstance(normalized["assumed_revision"], int) or normalized["assumed_revision"] < 1:
        raise ValueError("assumed_revision must be a positive integer")
    if normalized["confirmation_status"] not in _CONFIRMATION_STATUSES:
        raise ValueError("confirmation_status must be DECLARED, CONFIRMED, or REJECTED")
    for field in ("path_parameters", "query_parameters", "declared_headers", "request_body_schema", "response_schemas"):
        if not isinstance(normalized[field], dict):
            raise ValueError(f"{field} must be a JSON object")
    if not normalized["response_schemas"]:
        raise ValueError("response_schemas must declare at least one HTTP status-code response")
    if not all(str(status).isdigit() and 100 <= int(status) <= 599 for status in normalized["response_schemas"]):
        raise ValueError("response_schemas keys must be numeric HTTP status codes")
    evidence = normalized["consumer_source_evidence"]
    if not isinstance(evidence, dict) or not evidence.get("source_commit") or not evidence.get("content_sha256"):
        raise ValueError("consumer_source_evidence must include source_commit and content_sha256")
    if normalized["confirmation_status"] == "CONFIRMED":
        if not normalized.get("confirmed_by"):
            raise ValueError("confirmed_by is required when confirmation_status is CONFIRMED")
    return normalized


async def persist_http_interface_dependency(
    cur: Any, *, dependency: dict[str, Any], consumer_service: str, consumer_repository: str
) -> str:
    """Verify the declared route against the provider ledger, then persist its exact consumer binding."""
    dep = validate_http_interface_dependency(dependency)
    await cur.execute(
        """SELECT contract_id FROM service_contracts
           WHERE contract_id=%s AND service_name=%s AND http_method=%s AND endpoint_path=%s
             AND lifecycle_state='ACTIVE';""",
        (dep["contract_id"], dep["provider_service"], dep["http_method"], dep["endpoint_path"]),
    )
    contract = await cur.fetchone()
    if not contract:
        raise ValueError("Exact dependency does not match an active registered provider HTTP contract")
    await cur.execute(
        """SELECT contract_revision_id, schema_json FROM service_contract_revisions
           WHERE contract_id=%s AND revision_number=%s;""",
        (dep["contract_id"], dep["assumed_revision"]),
    )
    rev_row = await cur.fetchone()
    if not rev_row:
        raise ValueError("assumed_revision is not present for the registered provider contract")
    confirmed = dep["confirmation_status"] == "CONFIRMED"
    if confirmed and rev_row.get("schema_json"):
        provider_schema = rev_row["schema_json"] if isinstance(rev_row["schema_json"], dict) else json.loads(rev_row["schema_json"])
        prov_props = provider_schema.get("properties") or provider_schema.get("request_body_schema", {}).get("properties")
        decl_props = dep.get("request_body_schema", {}).get("properties")
        if prov_props and decl_props:
            prov_req = set(provider_schema.get("required") or [])
            decl_props_keys = set(decl_props.keys())
            missing_req = prov_req - decl_props_keys
            if missing_req:
                raise ValueError(f"Declared HTTP dependency is missing required provider fields: {sorted(missing_req)}")
    await cur.execute(
        """INSERT INTO http_interface_dependencies (
               provider_service, consumer_service, contract_id, assumed_provider_revision,
               http_method, endpoint_path, path_parameters, query_parameters, declared_headers,
               request_body_schema, response_schemas, consumer_repository, consumer_source_file,
               consumer_source_evidence, confirmation_status, confirmed_by, confirmed_at
           ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb,
                     %s::jsonb, %s, %s, %s::jsonb, %s, %s, CASE WHEN %s THEN now() ELSE NULL END)
           ON CONFLICT (consumer_service, provider_service, contract_id, assumed_provider_revision,
                        consumer_repository, consumer_source_file)
           DO UPDATE SET path_parameters=EXCLUDED.path_parameters, query_parameters=EXCLUDED.query_parameters,
               declared_headers=EXCLUDED.declared_headers, request_body_schema=EXCLUDED.request_body_schema,
               response_schemas=EXCLUDED.response_schemas, consumer_source_evidence=EXCLUDED.consumer_source_evidence,
               confirmation_status=EXCLUDED.confirmation_status, confirmed_by=EXCLUDED.confirmed_by,
               confirmed_at=EXCLUDED.confirmed_at, updated_at=now()
           RETURNING dependency_id;""",
        (
            dep["provider_service"], consumer_service, dep["contract_id"], dep["assumed_revision"],
            dep["http_method"], dep["endpoint_path"], json.dumps(dep["path_parameters"]),
            json.dumps(dep["query_parameters"]), json.dumps(dep["declared_headers"]),
            json.dumps(dep["request_body_schema"]), json.dumps(dep["response_schemas"]),
            consumer_repository, dep["consumer_source_file"], json.dumps(dep["consumer_source_evidence"]),
            dep["confirmation_status"], dep.get("confirmed_by"), confirmed,
        ),
    )
    row = await cur.fetchone()
    if not row:
        raise RuntimeError("Failed to persist exact HTTP interface dependency")
    return str(row["dependency_id"])
