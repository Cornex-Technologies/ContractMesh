"""Verification for CodeClaim's v1 exact HTTP/JSON dependency boundary."""

from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from coordinator.http_dependencies import persist_http_interface_dependency, validate_http_interface_dependency


def _dependency(**overrides):
    value = {
        "provider_service": "billing-service",
        "contract_id": "11111111-1111-1111-1111-111111111111",
        "assumed_revision": 2,
        "http_method": "POST",
        "endpoint_path": "/v1/charges/{charge_id}",
        "path_parameters": {"charge_id": {"type": "string"}},
        "query_parameters": {"expand": {"type": "boolean"}},
        "declared_headers": {"content-type": "application/json", "x-request-id": {"required": False}},
        "request_body_schema": {"type": "object", "required": ["payment_method_id"]},
        "response_schemas": {"200": {"type": "object"}, "422": {"type": "object"}},
        "consumer_source_file": "clients/billing_client.py",
        "consumer_source_evidence": {"source_commit": "abc123", "content_sha256": "a" * 64},
        "confirmation_status": "CONFIRMED",
        "confirmed_by": "orders-owner",
    }
    value.update(overrides)
    return value


def test_exact_http_dependency_migration_is_forward_only_and_complete():
    migration_dir = Path(__file__).parent.parent / "coordinator" / "migrations"
    migration = (migration_dir / "007_exact_http_interface_dependencies.sql").read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS http_interface_dependencies" in migration
    for field in (
        "provider_service", "consumer_service", "http_method", "endpoint_path",
        "path_parameters", "query_parameters", "declared_headers", "request_body_schema",
        "response_schemas", "assumed_provider_revision", "consumer_source_file",
        "consumer_source_evidence", "confirmation_status",
    ):
        assert field in migration
    assert "007_exact_http_interface_dependencies.sql" in [path.name for path in sorted(migration_dir.glob("*.sql"))]


def test_exact_http_dependency_requires_all_observable_interface_parts_and_evidence():
    clean = validate_http_interface_dependency(_dependency())
    assert clean["http_method"] == "POST"
    assert clean["confirmation_status"] == "CONFIRMED"

    with pytest.raises(ValueError, match="missing required fields"):
        validate_http_interface_dependency({"provider_service": "billing-service"})
    with pytest.raises(ValueError, match="response_schemas"):
        validate_http_interface_dependency(_dependency(response_schemas={}))
    with pytest.raises(ValueError, match="source_commit and content_sha256"):
        validate_http_interface_dependency(_dependency(consumer_source_evidence={"source_commit": "abc"}))
    with pytest.raises(ValueError, match="confirmed_by"):
        validate_http_interface_dependency(_dependency(confirmed_by=None))


@pytest.mark.asyncio
async def test_persisted_dependency_is_verified_against_contract_route_and_revision():
    cursor = AsyncMock()
    executed = []

    async def execute(sql, params=None):
        executed.append((sql, params))

    cursor.execute = execute
    cursor.fetchone.side_effect = [
        {"contract_id": "11111111-1111-1111-1111-111111111111"},
        {"contract_revision_id": "revision-2"},
        {"dependency_id": "dependency-1"},
    ]

    dependency_id = await persist_http_interface_dependency(
        cursor, dependency=_dependency(), consumer_service="orders-service",
        consumer_repository="https://git.example.internal/orders-service.git",
    )

    assert dependency_id == "dependency-1"
    sql = " ".join(statement for statement, _ in executed)
    assert "service_contracts" in sql
    assert "service_contract_revisions" in sql
    assert "http_interface_dependencies" in sql
    assert "confirmation_status" in sql
