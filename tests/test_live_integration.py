"""Offline tests for live harness setup and client integration boundaries."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from coordinator.app import app
from coordinator.mcp_server import mcp
from scripts.live_harness_scenario import _validate_live_environment
from scripts.register_agents import _harness_specs, build_mcp_config


def test_live_harness_mapping_matches_demo_roles():
    specs = {spec["config_name"]: spec for spec in _harness_specs()}

    assert specs["antigravity"]["harness_type"] == "antigravity"
    assert specs["antigravity"]["service_name"] == "billing-service"
    assert specs["codex"]["harness_type"] == "codex"
    assert specs["codex"]["service_name"] == "orders-service"


def test_mcp_config_redacts_tokens_and_database_url_by_default(tmp_path):
    config = build_mcp_config(
        python_executable="python",
        project_root=tmp_path,
        harness_id="harness-1",
        access_token="one-time-secret",
    )
    env = config["mcpServers"]["codeclaim"]["env"]

    assert env["MCP_HARNESS_ID"] == "harness-1"
    assert env["MCP_HARNESS_TOKEN"] != "one-time-secret"
    assert env["COCKROACH_DATABASE_URL"] != "postgresql://secret"
    assert "inject" in env["MCP_HARNESS_TOKEN"]
    assert "inject" in env["COCKROACH_DATABASE_URL"]


def test_mcp_config_can_explicitly_include_secrets_only_when_requested(monkeypatch, tmp_path):
    monkeypatch.setenv("COCKROACH_DATABASE_URL", "postgresql://trusted-secret")
    config = build_mcp_config(
        python_executable="python",
        project_root=tmp_path,
        harness_id="harness-1",
        access_token="one-time-secret",
        include_secrets=True,
    )
    env = config["mcpServers"]["codeclaim"]["env"]

    assert env["MCP_HARNESS_TOKEN"] == "one-time-secret"
    assert env["COCKROACH_DATABASE_URL"] == "postgresql://trusted-secret"


def test_checked_in_mcp_configs_are_redacted():
    root = Path(__file__).parents[1]
    for name, expected_service in (
        ("mcp_antigravity.json", "<billing-harness-id>"),
        ("mcp_codex.json", "<orders-harness-id>"),
    ):
        payload = json.loads((root / name).read_text(encoding="utf-8"))
        env = payload["mcpServers"]["codeclaim"]["env"]
        assert env["MCP_HARNESS_ID"] == expected_service
        assert env["MCP_HARNESS_TOKEN"].startswith("<inject-")
        assert env["COCKROACH_DATABASE_URL"].startswith("<inject-")


def test_live_environment_rejects_demo_and_remote_http(monkeypatch):
    monkeypatch.setenv("IS_DEMO_MODE", "true")
    with pytest.raises(RuntimeError, match="IS_DEMO_MODE=true"):
        _validate_live_environment("https://coordinator.example")

    monkeypatch.setenv("IS_DEMO_MODE", "false")
    monkeypatch.setenv("DEMO_AUTO_RECONCILE", "false")
    with pytest.raises(RuntimeError, match="HTTPS"):
        _validate_live_environment("http://coordinator.example")


def test_mcp_server_exposes_identity_smoke_tool():
    tool_names = [tool.name for tool in mcp._tool_manager.list_tools()]
    assert "get_harness_identity" in tool_names


@pytest.mark.asyncio
async def test_harness_disable_endpoint_is_operator_authenticated():
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        with patch("coordinator.app.verify_operator_auth") as verify, patch(
            "coordinator.app.disable_harness",
            AsyncMock(return_value={"harness_id": "h-1", "status": "DISABLED"}),
        ) as disable:
            response = await client.post(
                "/harnesses/h-1/disable",
                json={"actor": "rotation-test"},
                headers={"X-Operator-Token": "operator-token"},
            )

    assert response.status_code == 200
    assert response.json()["status"] == "DISABLED"
    verify.assert_called_once()
    disable.assert_awaited_once_with("h-1", actor="rotation-test")
