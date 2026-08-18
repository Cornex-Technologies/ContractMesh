"""Section 0 Verification Suite: Environment, Pinned Dependencies & Configuration."""

import importlib.metadata
from pathlib import Path
import pytest
from pydantic import ValidationError

from coordinator.config import Settings


# ==============================================================================
# 1. Third-Party Dependency Import Smoke Tests
# ==============================================================================


def test_core_package_imports():
    """Verify that all pinned runtime dependencies can be imported cleanly."""
    import fastapi
    import psycopg
    import psycopg_pool
    import langchain_cockroachdb
    import langchain_core
    import langgraph
    import boto3
    import jinja2
    import pydantic
    import httpx

    assert fastapi.__version__ is not None
    assert psycopg.__version__ is not None
    assert langchain_cockroachdb.__version__ is not None
    assert importlib.metadata.version("langgraph") is not None
    assert boto3.__version__ is not None
    assert jinja2.__version__ is not None
    assert pydantic.__version__ is not None
    assert httpx.__version__ is not None


# ==============================================================================
# 2. Configuration Defaults & Safe State Tests
# ==============================================================================


def test_settings_safe_defaults():
    """Verify safe default values (auto-reconciliation and demo mode disabled)."""
    config = Settings(
        _env_file=None,
        COCKROACH_DATABASE_URL="postgresql://root@localhost:26257/defaultdb?sslmode=disable",
        CHANGEFEED_WEBHOOK_SECRET="test-secret-key",
    )
    assert config.demo_auto_reconcile is False, "demo_auto_reconcile must default to False for safety"
    assert config.is_demo_mode is False, "is_demo_mode must default to False for safety"
    assert config.coordinator_port == 8000
    assert config.aws_region == "us-east-1"
    assert config.bedrock_model_id == "anthropic.claude-3-5-sonnet-20241022-v2:0"
    assert config.bedrock_embedding_model_id == "amazon.titan-embed-text-v1"


def test_settings_env_override(monkeypatch):
    """Verify configuration overrides via environment variables."""
    monkeypatch.setenv("COCKROACH_DATABASE_URL", "postgresql://custom_user:pwd@cloud.cockroachdb.com:26257/meshdb")
    monkeypatch.setenv("DEMO_AUTO_RECONCILE", "true")
    monkeypatch.setenv("IS_DEMO_MODE", "true")
    monkeypatch.setenv("COORDINATOR_PORT", "9000")
    monkeypatch.setenv("MCP_CLUSTER_ID", "mcp-cluster-abc-123")
    monkeypatch.setenv("CHANGEFEED_WEBHOOK_SECRET", "custom-secret-key-123")

    config = Settings(_env_file=None)
    assert config.cockroach_database_url == "postgresql://custom_user:pwd@cloud.cockroachdb.com:26257/meshdb"
    assert config.demo_auto_reconcile is True
    assert config.is_demo_mode is True
    assert config.coordinator_port == 9000
    assert config.mcp_cluster_id == "mcp-cluster-abc-123"
    assert config.changefeed_webhook_secret == "custom-secret-key-123"


# ==============================================================================
# 3. Configuration Validation & Security Tests
# ==============================================================================


def test_invalid_port_validation():
    """Verify port validation rejects out-of-range port numbers."""
    with pytest.raises(ValidationError):
        Settings(_env_file=None, COORDINATOR_PORT=0)

    with pytest.raises(ValidationError):
        Settings(_env_file=None, COORDINATOR_PORT=70000)

    with pytest.raises(ValidationError):
        Settings(_env_file=None, COORDINATOR_PORT=-80)


def test_validate_runtime_missing_database_url():
    """Verify validate_runtime aggregates error if database URL is absent."""
    config = Settings(_env_file=None, COCKROACH_DATABASE_URL=None, CHANGEFEED_WEBHOOK_SECRET="test-secret")
    with pytest.raises(ValueError, match="Missing 'COCKROACH_DATABASE_URL'"):
        config.validate_runtime(require_database=True)


def test_validate_runtime_missing_webhook_secret_outside_demo():
    """Verify validate_runtime aggregates error if webhook secret is missing in non-demo mode."""
    config = Settings(
        _env_file=None,
        COCKROACH_DATABASE_URL="postgresql://root@localhost:26257/db",
        CHANGEFEED_WEBHOOK_SECRET=None,
        IS_DEMO_MODE=False,
    )
    with pytest.raises(ValueError, match="Missing 'CHANGEFEED_WEBHOOK_SECRET'"):
        config.validate_runtime()


def test_validate_runtime_aggregates_multiple_errors():
    """Verify validate_runtime reports multiple missing fields simultaneously."""
    config = Settings(
        _env_file=None,
        COCKROACH_DATABASE_URL=None,
        CHANGEFEED_WEBHOOK_SECRET=None,
        IS_DEMO_MODE=False,
    )
    with pytest.raises(ValueError) as exc_info:
        config.validate_runtime()
    msg = str(exc_info.value)
    assert "Missing 'COCKROACH_DATABASE_URL'" in msg
    assert "Missing 'CHANGEFEED_WEBHOOK_SECRET'" in msg


def test_validate_runtime_passes_in_demo_mode():
    """Verify validate_runtime allows missing webhook secret in demo mode."""
    config = Settings(
        _env_file=None,
        COCKROACH_DATABASE_URL="postgresql://root@localhost:26257/db",
        CHANGEFEED_WEBHOOK_SECRET=None,
        IS_DEMO_MODE=True,
    )
    # Should not raise
    config.validate_runtime()


# ==============================================================================
# 4. Project Structure & Scaffolding Tests
# ==============================================================================


def test_project_structure_exists():
    """Verify standard project directories are present."""
    base_dir = Path(__file__).parent.parent
    expected_dirs = [
        base_dir / "coordinator",
        base_dir / "coordinator" / "templates",
        base_dir / "coordinator" / "static",
        base_dir / "infra" / "cockroach",
        base_dir / "infra" / "ccloud",
        base_dir / "infra" / "skills",
        base_dir / "repos",
        base_dir / "worktrees",
        base_dir / "live",
        base_dir / "tests",
    ]
    for d in expected_dirs:
        assert d.exists() and d.is_dir(), f"Directory {d} must exist"


def test_project_templates_and_docs_exist():
    """Verify essential project files exist (.env.example, .gitignore, pyproject.toml, README)."""
    base_dir = Path(__file__).parent.parent
    assert (base_dir / "pyproject.toml").exists()
    assert (base_dir / ".env.example").exists()
    assert (base_dir / ".gitignore").exists()
    assert (base_dir / "README.md").exists()
    assert (base_dir / "IMPLEMENTATION_PLAN.md").exists()
    assert (base_dir / "SECTION_PLAN.md").exists()
