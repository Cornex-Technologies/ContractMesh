"""Confirmed-only internal HTTP dependency registration and isolation tests."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from coordinator import cli
from coordinator.compatibility import create_compatibility_work_for_contract_change
from coordinator.dependency_discovery import (
    candidate_to_confirmed_dependency,
    confirm_internal_http_dependency,
    suggest_python_http_calls,
)


def _candidate() -> dict:
    return {
        "contract_id": "billing-charge-contract", "provider_service": "billing-service",
        "http_method": "POST", "endpoint_path": "/v1/charges", "revision_number": 2,
        "schema_json": {"x-codeclaim-http-interface": {
            "path_parameters": {}, "query_parameters": {}, "declared_headers": {"content-type": "application/json"},
            "request_body_schema": {"type": "object"}, "response_schemas": {"200": {"type": "object"}},
        }},
    }


def test_literal_python_http_call_becomes_a_suggestion_with_source_evidence(tmp_path):
    client = tmp_path / "clients" / "billing.py"
    client.parent.mkdir()
    client.write_text("await http_client.post('/v1/charges', json=payload)\n", encoding="utf-8")
    suggestions = suggest_python_http_calls(tmp_path, tmp_path)
    assert len(suggestions) == 1
    suggestion = suggestions[0]
    assert suggestion["http_method"] == "POST"
    assert suggestion["endpoint_path"] == "/v1/charges"
    assert suggestion["possible_provider"] is None
    assert suggestion["source_file"] == "clients/billing.py"
    assert suggestion["source_evidence"]["content_sha256"]


def test_repeated_transport_branches_in_one_client_file_are_one_suggestion(tmp_path):
    client = tmp_path / "clients" / "billing.py"
    client.parent.mkdir()
    client.write_text(
        "await http_client.post('/v1/charges', json=payload)\n"
        "await http_client.post('/v1/charges', json=payload)\n",
        encoding="utf-8",
    )
    suggestions = suggest_python_http_calls(tmp_path, tmp_path)
    assert len(suggestions) == 1
    assert suggestions[0]["source_line"] == 1


def test_chosen_exact_operation_builds_confirmed_dependency_not_a_semantic_guess():
    suggestion = {
        "source_file": "clients/billing.py",
        "source_evidence": {"source_commit": "commit-1", "content_sha256": "a" * 64},
    }
    dependency = candidate_to_confirmed_dependency(candidate=_candidate(), suggestion=suggestion, confirmed_by="orders-owner")
    assert dependency["provider_service"] == "billing-service"
    assert dependency["http_method"] == "POST"
    assert dependency["endpoint_path"] == "/v1/charges"
    assert dependency["assumed_revision"] == 2
    assert dependency["confirmation_status"] == "CONFIRMED"
    assert dependency["consumer_source_file"] == "clients/billing.py"


def test_cli_ignore_never_calls_confirmation(monkeypatch, tmp_path, capsys):
    source = tmp_path / "client.py"
    source.write_text("http_client.post('/v1/charges')\n", encoding="utf-8")
    monkeypatch.setattr(cli, "suggest_python_http_calls", lambda *_: [{
        "http_method": "POST", "endpoint_path": "/v1/charges", "possible_provider": "billing-service",
        "source_file": "client.py", "source_line": 1,
        "source_evidence": {"content_sha256": "a" * 64}, "confidence": 0.95,
    }])
    monkeypatch.setattr(cli, "find_provider_operation_candidates", AsyncMock(return_value=[_candidate()]))
    confirm = AsyncMock()
    monkeypatch.setattr(cli, "confirm_internal_http_dependency", confirm)
    answers = iter(["i"])
    monkeypatch.setattr("builtins.input", lambda _: next(answers))
    assert cli.main([
        "dependencies", "--consumer-service", "orders-service", "--repository-path", str(tmp_path),
        "--endpoint-code-dir", ".", "--provider-service", "billing-service",
    ]) == 0
    assert "Ignored" in capsys.readouterr().out
    confirm.assert_not_awaited()


@pytest.mark.asyncio
async def test_explicit_confirmation_persists_dependency_and_audit_outbox(monkeypatch):
    cursor = AsyncMock()
    statements = []

    async def execute(sql, params=None):
        statements.append(sql)

    cursor.execute = execute
    cursor.fetchone.return_value = {"event_id": "event-dependency-confirmed"}
    conn = MagicMock()
    context = AsyncMock()
    context.__aenter__.return_value = cursor
    context.__aexit__.return_value = None
    conn.cursor.return_value = context

    async def run_in_transaction(fn):
        return await fn(conn)

    monkeypatch.setattr("coordinator.dependency_discovery.run_transaction", run_in_transaction)
    persisted = AsyncMock(return_value="dependency-1")
    monkeypatch.setattr("coordinator.dependency_discovery.persist_http_interface_dependency", persisted)
    dependency_id = await confirm_internal_http_dependency(
        consumer_service="orders-service", consumer_repository="C:/work/orders-service",
        candidate=_candidate(), suggestion={"source_file": "clients/billing.py", "source_evidence": {"source_commit": "commit-1", "content_sha256": "a" * 64}},
        confirmed_by="orders-owner",
    )
    assert dependency_id == "dependency-1"
    persisted.assert_awaited_once()
    sql = " ".join(statements)
    assert "INTERNAL_HTTP_DEPENDENCY_CONFIRMED" in sql
    assert "coordinator_outbox" in sql
    assert "contract_audit_history" in sql


@pytest.mark.asyncio
async def test_unrelated_provider_operation_change_creates_no_work_for_confirmed_charge_consumer():
    cursor = AsyncMock()
    executed = []

    async def execute(sql, params=None):
        executed.append((sql, params))

    cursor.execute = execute
    # The only confirmed dependency in this scenario is orders -> billing POST /v1/charges.
    # A billing POST /v1/refunds event therefore returns no matching dependency rows.
    cursor.fetchall.return_value = []
    conn = MagicMock()
    context = AsyncMock()
    context.__aenter__.return_value = cursor
    context.__aexit__.return_value = None
    conn.cursor.return_value = context

    created = await create_compatibility_work_for_contract_change(
        conn, source_event_id="refund-event", contract_id="billing-refund-contract",
        source_service="billing-service", revision_number=3,
        schema_diff={"is_breaking": True, "classification": "BREAKING"},
    )

    assert created == []
    dependency_query = next(sql for sql, _ in executed if "http_interface_dependencies" in sql)
    dependency_params = next(params for sql, params in executed if "http_interface_dependencies" in sql)
    assert "confirmation_status = 'CONFIRMED'" in dependency_query
    assert dependency_params == ("billing-refund-contract",)
    assert not any("INSERT INTO compatibility_work_items" in sql for sql, _ in executed)
