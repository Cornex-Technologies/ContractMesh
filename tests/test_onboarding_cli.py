"""Tests for the FastAPI-only, confirmation-gated CodeClaim onboarding CLI."""

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from coordinator import cli
from coordinator.onboarding import find_dynamic_behavior, load_openapi_from_local_url, make_plan
from coordinator.service_registry import register_internal_service


FIXTURE_REPO = Path(__file__).parent / "fixtures" / "onboard_fastapi"


def test_fastapi_entry_openapi_is_normalized_to_exact_http_contract():
    plan = make_plan(
        service_name="billing-service", repository_path=FIXTURE_REPO,
        endpoint_code_dir=FIXTURE_REPO / "app", app_entry="app.main:app", openapi_url=None,
    )
    assert len(plan.contracts) == 1
    contract = plan.contracts[0]
    interface = contract["schema_json"]["x-codeclaim-http-interface"]
    assert contract["http_method"] == "POST"
    assert contract["endpoint_path"] == "/charges/{charge_id}"
    assert interface["path_parameters"]["charge_id"]["required"] is True
    assert interface["query_parameters"]["expand"]["required"] is False
    assert interface["declared_headers"]["X-Request-ID"]["required"] is True
    assert "payment_method_id" in interface["request_body_schema"]["properties"]
    assert "200" in interface["response_schemas"]
    assert "422" in interface["response_schemas"]
    assert interface["security_requirements"]


def test_dynamic_route_and_header_access_are_review_required(tmp_path):
    source = tmp_path / "routes.py"
    source.write_text("app.get(prefix + '/items')(handler)\nvalue = request.headers[header_name]\nHeader(alias=header_name)\n", encoding="utf-8")
    findings = find_dynamic_behavior(tmp_path)
    assert any("dynamic route path" in finding for finding in findings)
    assert any("dynamic request header access" in finding for finding in findings)
    assert any("dynamic declared header alias" in finding for finding in findings)


def test_loopback_openapi_url_is_supported_and_non_loopback_is_rejected():
    response = MagicMock()
    response.status = 200
    response.read.return_value = b'{"openapi":"3.1.0","paths":{}}'
    context = MagicMock()
    context.__enter__.return_value = response
    context.__exit__.return_value = None
    with patch("coordinator.onboarding.urllib.request.urlopen", return_value=context):
        assert load_openapi_from_local_url("http://127.0.0.1:8000/openapi.json")["openapi"] == "3.1.0"
    try:
        load_openapi_from_local_url("https://example.com/openapi.json")
    except ValueError as exc:
        assert "locally running" in str(exc)
    else:
        raise AssertionError("non-loopback URL should be rejected")


def test_cli_prints_plan_and_cancellation_performs_no_writes(monkeypatch, capsys):
    apply = AsyncMock()
    monkeypatch.setattr(cli, "apply_onboarding", apply)
    monkeypatch.setattr("builtins.input", lambda _: "n")
    exit_code = cli.main([
        "onboard", "--service-name", "billing-service", "--repository-path", str(FIXTURE_REPO),
        "--endpoint-code-dir", "app", "--app-entry", "app.main:app",
    ])
    rendered = capsys.readouterr().out
    assert exit_code == 0
    assert "CodeClaim FastAPI onboarding plan" in rendered
    assert "POST /charges/{charge_id}" in rendered
    assert "No database or filesystem changes" in rendered
    apply.assert_not_awaited()


def test_yes_mode_still_prints_plan_before_apply(monkeypatch, capsys):
    apply = AsyncMock(return_value={"publications": [{"contract_id": "contract-1"}], "config_path": "/tmp/.codeclaim/service.json"})
    monkeypatch.setattr(cli, "apply_onboarding", apply)
    exit_code = cli.main([
        "onboard", "--service-name", "billing-service", "--repository-path", str(FIXTURE_REPO),
        "--endpoint-code-dir", "app", "--app-entry", "app.main:app", "--yes",
    ])
    rendered = capsys.readouterr().out
    assert exit_code == 0
    assert "CodeClaim FastAPI onboarding plan" in rendered
    assert "Onboarding complete" in rendered
    apply.assert_awaited_once()


def test_publish_revision_prints_plan_and_cancellation_performs_no_writes(monkeypatch, capsys):
    publish = AsyncMock()
    monkeypatch.setattr(cli, "apply_revision_publication", publish)
    monkeypatch.setattr("builtins.input", lambda _: "n")
    exit_code = cli.main([
        "publish-revision",
        "--service-name", "billing-service",
        "--repository-path", str(FIXTURE_REPO),
        "--endpoint-code-dir", "app",
        "--app-entry", "app.main:app",
        "--endpoint-path", "/charges/{charge_id}",
        "--http-method", "post",
    ])
    rendered = capsys.readouterr().out
    assert exit_code == 0
    assert "CodeClaim contract revision publication plan" in rendered
    assert "POST /charges/{charge_id}" in rendered
    assert "No database or filesystem changes" in rendered
    publish.assert_not_awaited()


def test_apply_onboarding_writes_only_codeclaim_config_after_database_calls(tmp_path, monkeypatch):
    app_dir = tmp_path / "app"
    app_dir.mkdir()
    plan = make_plan(
        service_name="billing-service", repository_path=FIXTURE_REPO,
        endpoint_code_dir=FIXTURE_REPO / "app", app_entry="app.main:app", openapi_url=None,
    )
    isolated_plan = plan.__class__(
        plan.service_name, tmp_path, app_dir, plan.app_entry, plan.openapi_url, plan.contracts, plan.review_findings,
    )
    monkeypatch.setattr(cli, "register_internal_service", AsyncMock(return_value={"service_id": "service-1"}))
    monkeypatch.setattr(cli, "fetch_one", AsyncMock(return_value=None))
    monkeypatch.setattr(cli, "publish_contract_revision", AsyncMock(return_value={"contract_id": "contract-1"}))
    monkeypatch.setattr(cli, "get_service_git_commit", lambda _: "commit-1")
    result = asyncio.run(cli.apply_onboarding(isolated_plan))
    config = tmp_path / ".codeclaim" / "service.json"
    assert config.exists()
    config_data = json.loads(config.read_text(encoding="utf-8"))
    assert config_data["application_entrypoint"] == "app.main:app"
    assert result["config_path"] == str(config)
    assert not list(tmp_path.glob("*.py"))


def test_apply_onboarding_rejects_an_already_onboarded_contract_before_writes(tmp_path, monkeypatch):
    plan = make_plan(
        service_name="billing-service", repository_path=FIXTURE_REPO,
        endpoint_code_dir=FIXTURE_REPO / "app", app_entry="app.main:app", openapi_url=None,
    )
    isolated_plan = plan.__class__(plan.service_name, tmp_path, tmp_path, plan.app_entry, plan.openapi_url, plan.contracts, plan.review_findings)
    register = AsyncMock()
    monkeypatch.setattr(cli, "register_internal_service", register)
    monkeypatch.setattr(cli, "fetch_one", AsyncMock(return_value={"revision": 1}))
    monkeypatch.setattr(cli, "get_service_git_commit", lambda _: "commit-1")
    try:
        asyncio.run(cli.apply_onboarding(isolated_plan))
    except ValueError as exc:
        assert "already onboarded" in str(exc)
    else:
        raise AssertionError("existing contract must stop one-time onboarding")
    register.assert_not_awaited()


async def test_service_registration_appends_outbox_and_audit_records(monkeypatch):
    cursor = AsyncMock()
    statements = []

    async def execute(sql, params=None):
        statements.append(sql)

    cursor.execute = execute
    cursor.fetchone.side_effect = [
        {
            "service_id": "service-1", "service_name": "billing-service",
            "repository_path": "C:/work/billing", "registration_source": "ONBOARDING_CLI",
        },
        {"event_id": "event-service-onboarded"},
    ]
    conn = MagicMock()
    context = AsyncMock()
    context.__aenter__.return_value = cursor
    context.__aexit__.return_value = None
    conn.cursor.return_value = context

    async def run_in_transaction(fn):
        return await fn(conn)

    monkeypatch.setattr("coordinator.service_registry.run_transaction", run_in_transaction)
    registered = await register_internal_service(service_name="billing-service", repository_path="C:/work/billing", actor="codeclaim-onboard")
    assert registered["service_name"] == "billing-service"
    assert registered["registration_source"] == "ONBOARDING_CLI"
    assert registered["registered_by"] == "codeclaim-onboard"
    assert registered["registration_event_id"] == "event-service-onboarded"
    sql = " ".join(statements)
    assert "microservices" in sql
    assert "SERVICE_ONBOARDED" in sql
    assert "registration_source" in sql
    assert "registration_event_id" in sql
    assert "contract_audit_history" in sql
