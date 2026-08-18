"""Section 4 Verification Suite: Atomic Contract Publication & CDC Outbox Engine."""

import json
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from coordinator.contract_registry import (
    _canonical_schema_json,
    extract_pydantic_schema_from_git_commit,
    extract_pydantic_schema_from_repo,
    get_service_git_commit,
    publish_contract_revision,
)
from coordinator.db import check_health, close_pool, execute_query, init_db


def test_canonical_schema_preserves_referenced_definitions_and_ignores_metadata_only_changes():
    old_schema = {
        "title": "Old title",
        "$defs": {"Charge": {"type": "object", "properties": {"amount": {"type": "integer"}}}},
        "$ref": "#/$defs/Charge",
    }
    same_contract = {
        "description": "Updated prose",
        "$ref": "#/$defs/Charge",
        "$defs": {"Charge": {"properties": {"amount": {"type": "integer"}}, "type": "object"}},
    }
    changed_contract = {
        "$ref": "#/$defs/Charge",
        "$defs": {"Charge": {"type": "object", "properties": {"amount": {"type": "number"}}}},
    }

    assert _canonical_schema_json(old_schema) == _canonical_schema_json(same_contract)
    assert _canonical_schema_json(old_schema) != _canonical_schema_json(changed_contract)


# ==============================================================================
# 1. Provenance & Dynamic Model Extraction Unit Tests
# ==============================================================================


def test_extract_service_git_commit():
    """Verify extracting actual Git commit SHA from initialized sibling repositories."""
    base_dir = Path(__file__).parent.parent
    billing_repo = base_dir / "repos" / "billing-service"
    orders_repo = base_dir / "repos" / "orders-service"

    commit_billing = get_service_git_commit(billing_repo)
    commit_orders = get_service_git_commit(orders_repo)

    assert len(commit_billing) == 40, f"Expected 40-char SHA, got: {commit_billing}"
    assert len(commit_orders) == 40, f"Expected 40-char SHA, got: {commit_orders}"
    assert commit_billing != "commit-untracked-dev"
    assert commit_orders != "commit-untracked-dev"


def test_extract_pydantic_schema_from_repo():
    """Verify dynamic schema extraction from service repo files without global namespace pollution."""
    base_dir = Path(__file__).parent.parent
    billing_repo = base_dir / "repos" / "billing-service"

    schema_v1 = extract_pydantic_schema_from_repo(billing_repo, "schemas_v1.py", "ChargeRequest")
    schema_v2 = extract_pydantic_schema_from_repo(billing_repo, "schemas_v2.py", "ChargeRequest")

    assert "card_token" in schema_v1["properties"]
    assert "payment_method_id" in schema_v2["properties"]
    assert "card_token" not in schema_v2["properties"]
    assert "amount" in schema_v1["properties"]
    assert "amount" in schema_v2["properties"]


def test_extract_pydantic_schema_from_git_commit():
    """Verify extracting schema from a specific Git commit SHA (commit-bound extraction)."""
    base_dir = Path(__file__).parent.parent
    billing_repo = base_dir / "repos" / "billing-service"
    commit_sha = get_service_git_commit(billing_repo)

    # 1. Valid commit extraction
    schema = extract_pydantic_schema_from_git_commit(
        billing_repo,
        source_commit=commit_sha,
        module_filename="schemas_v1.py",
        model_name="ChargeRequest",
    )
    assert "card_token" in schema["properties"]
    assert "amount" in schema["properties"]

    # 2. Extracting non-existent module in that commit should fail
    with pytest.raises(FileNotFoundError):
        extract_pydantic_schema_from_git_commit(
            billing_repo,
            source_commit=commit_sha,
            module_filename="non_existent_file.py",
            model_name="ChargeRequest",
        )


def test_ast_schema_extractor_sandboxed_against_code_execution():
    """Verify that AST schema extraction never executes arbitrary Python code in class definitions."""
    from coordinator.contract_registry import extract_pydantic_schema_from_source

    malicious_code = '''
from pydantic import BaseModel, Field

# Side effect that would fail or execute if evaluated dynamically
raise RuntimeError("Malicious code executed on coordinator!")

class MaliciousModel(BaseModel):
    account_id: str = Field(..., description="Target account")
    balance: int = 0
'''

    # If the parser executed the code, it would raise RuntimeError("Malicious code executed on coordinator!")
    # Because it is a static AST parser, it extracts the schema with zero execution!
    schema = extract_pydantic_schema_from_source(malicious_code, "MaliciousModel")
    assert schema["title"] == "MaliciousModel"
    assert "account_id" in schema["properties"]
    assert "balance" in schema["properties"]
    assert schema["properties"]["balance"]["default"] == 0
    assert "account_id" in schema["required"]
    assert "balance" not in schema["required"]


def test_ast_annotated_and_enum_and_alias_extraction():
    """Verify AST extractor parses Annotated types, Enum definitions, aliases, and default factories."""
    from coordinator.contract_registry import extract_pydantic_schema_from_source

    code = '''
from enum import Enum
from typing import Annotated, Optional
from pydantic import BaseModel, Field

class Currency(str, Enum):
    USD = "usd"
    EUR = "eur"
    GBP = "gbp"

class AdvancedPaymentRequest(BaseModel):
    amount: Annotated[int, Field(gt=0, description="Amount in cents")]
    currency: Currency = Currency.USD
    recipient_token: str = Field(..., alias="recipientToken")
    tags: list[str] = Field(default_factory=list)
'''

    schema = extract_pydantic_schema_from_source(code, "AdvancedPaymentRequest")
    assert schema["title"] == "AdvancedPaymentRequest"
    
    # 1. Check Annotated type and constraints
    assert schema["properties"]["amount"]["type"] == "integer"
    assert schema["properties"]["amount"]["gt"] == 0
    assert "amount" in schema["required"]

    # 2. Check Enum type resolution
    assert schema["properties"]["currency"]["type"] == "string"
    assert "enum" in schema["properties"]["currency"]
    assert "usd" in schema["properties"]["currency"]["enum"]

    # 3. Check alias resolution
    assert schema["properties"]["recipient_token"]["alias"] == "recipientToken"
    assert "recipient_token" in schema["required"]

    # 4. Check default_factory resolution
    assert schema["properties"]["tags"]["type"] == "array"
    assert schema["properties"]["tags"].get("default_factory") is True
    assert "tags" not in schema["required"]



# ==============================================================================
# 2. Transaction Atomicity, Outbox & Immutability Mocked Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_atomic_publication_executes_all_invariants_in_single_tx():
    """Verify that contract publication executes contract, revision, outbox, and audit in one transaction."""
    executed_statements = []

    mock_cur = AsyncMock()

    async def mock_execute(sql, params=None):
        executed_statements.append((sql, params))

    mock_cur.execute = mock_execute
    
    call_count = 0

    async def mock_fetchone():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return {"contract_id": "11111111-1111-1111-1111-111111111111"}
        elif call_count == 2:
            return None  # No existing revision (brand new)
        elif call_count == 3:
            return {"contract_revision_id": "22222222-2222-2222-2222-222222222222"}
        elif call_count == 4:
            return {"event_id": "33333333-3333-3333-3333-333333333333"}
        elif call_count == 5:
            return {"history_id": "44444444-4444-4444-4444-444444444444"}
        return None

    mock_cur.fetchone = mock_fetchone

    mock_conn = MagicMock()
    mock_cur_ctx = AsyncMock()
    mock_cur_ctx.__aenter__.return_value = mock_cur
    mock_cur_ctx.__aexit__.return_value = None
    mock_conn.cursor.return_value = mock_cur_ctx

    mock_tx_ctx = AsyncMock()
    mock_conn.transaction.return_value.__aenter__ = AsyncMock(return_value=mock_tx_ctx)
    mock_conn.transaction.return_value.__aexit__ = AsyncMock(return_value=None)

    mock_pool = MagicMock()
    mock_pool.closed = False
    mock_pool_conn_ctx = AsyncMock()
    mock_pool_conn_ctx.__aenter__.return_value = mock_conn
    mock_pool_conn_ctx.__aexit__.return_value = None
    mock_pool.connection.return_value = mock_pool_conn_ctx

    with patch("coordinator.db.get_pool", AsyncMock(return_value=mock_pool)):
        result = await publish_contract_revision(
            service_name="billing-service",
            endpoint_path="/v1/charges",
            http_method="POST",
            revision_number=1,
            schema_json={"properties": {"card_token": {"type": "string"}}},
            semantic_summary="Billing charge endpoint",
            published_by="agent-a",
            source_commit="2ff89c5123456789012345678901234567890123",
        )

        assert result["contract_id"] == "11111111-1111-1111-1111-111111111111"
        assert result["contract_revision_id"] == "22222222-2222-2222-2222-222222222222"
        assert result["outbox_event_id"] == "33333333-3333-3333-3333-333333333333"
        assert result["history_id"] == "44444444-4444-4444-4444-444444444444"
        assert result["revision_number"] == 1

        # Check that operations executed
        assert len(executed_statements) >= 5
        sql_texts = " ".join(s[0] for s in executed_statements)
        assert "service_contracts" in sql_texts
        assert "service_contract_revisions" in sql_texts
        assert "coordinator_outbox" in sql_texts
        assert "contract_audit_history" in sql_texts


@pytest.mark.asyncio
async def test_publish_contract_revision_immutability_conflict():
    """Verify that attempting to overwrite an existing revision with a conflicting schema raises ValueError."""
    mock_cur = AsyncMock()

    call_count = 0
    async def mock_fetchone():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return {"contract_id": "11111111-1111-1111-1111-111111111111"}
        elif call_count == 2:
            # Existing revision 1 exists with card_token schema
            return {
                "contract_revision_id": "22222222-2222-2222-2222-222222222222",
                "source_commit": "commit-v1",
                "schema_json": {"properties": {"card_token": {"type": "string"}}},
                "semantic_summary": "v1 summary",
                "is_active": True,
            }
        return None

    mock_cur.fetchone = mock_fetchone

    mock_conn = MagicMock()
    mock_cur_ctx = AsyncMock()
    mock_cur_ctx.__aenter__.return_value = mock_cur
    mock_cur_ctx.__aexit__.return_value = None
    mock_conn.cursor.return_value = mock_cur_ctx

    mock_tx_ctx = AsyncMock()
    mock_conn.transaction.return_value.__aenter__ = AsyncMock(return_value=mock_tx_ctx)
    mock_conn.transaction.return_value.__aexit__ = AsyncMock(return_value=None)

    mock_pool = MagicMock()
    mock_pool.closed = False
    mock_pool_conn_ctx = AsyncMock()
    mock_pool_conn_ctx.__aenter__.return_value = mock_conn
    mock_pool_conn_ctx.__aexit__.return_value = None
    mock_pool.connection.return_value = mock_pool_conn_ctx

    with patch("coordinator.db.get_pool", AsyncMock(return_value=mock_pool)):
        # Attempt to publish revision 1 with a DIFFERENT schema (e.g. payment_method_id)
        with pytest.raises(ValueError, match="is immutable and already exists with a different schema"):
            await publish_contract_revision(
                service_name="billing-service",
                endpoint_path="/v1/charges",
                http_method="POST",
                revision_number=1,
                schema_json={"properties": {"payment_method_id": {"type": "string"}}},
                semantic_summary="Conflicting revision 1",
                published_by="agent-a",
                source_commit="commit-v2-different",
            )


@pytest.mark.asyncio
async def test_publish_contract_revision_idempotent_noop():
    """Verify that re-publishing identical schema for an existing revision is a safe idempotent no-op."""
    mock_cur = AsyncMock()

    call_count = 0
    async def mock_fetchone():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return {"contract_id": "11111111-1111-1111-1111-111111111111"}
        elif call_count == 2:
            return {
                "contract_revision_id": "22222222-2222-2222-2222-222222222222",
                "source_commit": "commit-v1-identical",
                "schema_json": {"properties": {"card_token": {"type": "string"}}},
                "semantic_summary": "v1 summary",
                "is_active": True,
            }
        return None

    mock_cur.fetchone = mock_fetchone

    mock_conn = MagicMock()
    mock_cur_ctx = AsyncMock()
    mock_cur_ctx.__aenter__.return_value = mock_cur
    mock_cur_ctx.__aexit__.return_value = None
    mock_conn.cursor.return_value = mock_cur_ctx

    mock_tx_ctx = AsyncMock()
    mock_conn.transaction.return_value.__aenter__ = AsyncMock(return_value=mock_tx_ctx)
    mock_conn.transaction.return_value.__aexit__ = AsyncMock(return_value=None)

    mock_pool = MagicMock()
    mock_pool.closed = False
    mock_pool_conn_ctx = AsyncMock()
    mock_pool_conn_ctx.__aenter__.return_value = mock_conn
    mock_pool_conn_ctx.__aexit__.return_value = None
    mock_pool.connection.return_value = mock_pool_conn_ctx

    with patch("coordinator.db.get_pool", AsyncMock(return_value=mock_pool)):
        result = await publish_contract_revision(
            service_name="billing-service",
            endpoint_path="/v1/charges",
            http_method="POST",
            revision_number=1,
            schema_json={"properties": {"card_token": {"type": "string"}}},
            semantic_summary="v1 summary",
            published_by="agent-a",
            source_commit="commit-v1-identical",
        )

        assert result["contract_revision_id"] == "22222222-2222-2222-2222-222222222222"
        assert result.get("is_idempotent_noop") is True


@pytest.mark.asyncio
async def test_atomic_publication_computes_authoritative_diff_for_subsequent_revisions():
    """A legacy caller diff cannot downgrade the coordinator's deterministic result."""
    mock_cur = AsyncMock()
    
    call_count = 0
    async def mock_fetchone():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return {"contract_id": "11111111-1111-1111-1111-111111111111"}
        elif call_count == 2:
            return None  # Revision 2 does not exist yet
        elif call_count == 3:
            # Return previous revision 1 schema
            return {
                "schema_json": {
                    "type": "object",
                    "properties": {"card_token": {"type": "string"}},
                    "required": ["card_token"],
                }
            }
        elif call_count == 4:
            return {"contract_revision_id": "22222222-2222-2222-2222-222222222222"}
        elif call_count == 5:
            return {"event_id": "33333333-3333-3333-3333-333333333333"}
        elif call_count == 6:
            return {"history_id": "44444444-4444-4444-4444-444444444444"}
        elif call_count == 7:
            return {"event_id": "33333333-3333-3333-3333-333333333333"}
        return None

    mock_cur.fetchone = mock_fetchone
    mock_cur.fetchall.return_value = []

    mock_conn = MagicMock()
    mock_cur_ctx = AsyncMock()
    mock_cur_ctx.__aenter__.return_value = mock_cur
    mock_cur_ctx.__aexit__.return_value = None
    mock_conn.cursor.return_value = mock_cur_ctx

    mock_tx_ctx = AsyncMock()
    mock_conn.transaction.return_value.__aenter__ = AsyncMock(return_value=mock_tx_ctx)
    mock_conn.transaction.return_value.__aexit__ = AsyncMock(return_value=None)

    mock_pool = MagicMock()
    mock_pool.closed = False
    mock_pool_conn_ctx = AsyncMock()
    mock_pool_conn_ctx.__aenter__.return_value = mock_conn
    mock_pool_conn_ctx.__aexit__.return_value = None
    mock_pool.connection.return_value = mock_pool_conn_ctx

    with patch("coordinator.db.get_pool", AsyncMock(return_value=mock_pool)):
        result = await publish_contract_revision(
            service_name="billing-service",
            endpoint_path="/v1/charges",
            http_method="POST",
            revision_number=2,
            schema_json={
                "type": "object",
                "properties": {"payment_method_id": {"type": "string"}},
                "required": ["payment_method_id"],
            },
            semantic_summary="Billing charge endpoint v2",
            published_by="agent-a",
            schema_diff={"is_breaking": False, "classification": "NON_BREAKING"},
        )

        assert result["schema_diff"] is not None
        assert result["schema_diff"]["is_breaking"] is True
        assert result["schema_diff"]["old_revision"] == 1
        assert result["schema_diff"]["new_revision"] == 2
        assert result["schema_diff"]["legacy_caller_diff"]["classification"] == "NON_BREAKING"
        breaking_field_names = [f["field"] for f in result["schema_diff"]["breaking_changes"]]
        assert "card_token" in breaking_field_names
        assert "payment_method_id" in breaking_field_names


@pytest.mark.asyncio
async def test_atomic_publication_aborts_on_failure():
    """Verify that a failure in any part of the publication flow causes the transaction to abort."""
    mock_cur = AsyncMock()

    # Simulate database crash during outbox insert
    async def mock_execute(sql, params=None):
        if "coordinator_outbox" in sql:
            raise RuntimeError("Database disk full during outbox write")

    mock_cur.execute = mock_execute
    
    call_count = 0
    async def mock_fetchone():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return {"contract_id": "11111111-1111-1111-1111-111111111111"}
        elif call_count == 2:
            return None
        elif call_count == 3:
            return {"contract_revision_id": "22222222-2222-2222-2222-222222222222"}
        return None

    mock_cur.fetchone = mock_fetchone

    mock_conn = MagicMock()
    mock_cur_ctx = AsyncMock()
    mock_cur_ctx.__aenter__.return_value = mock_cur
    mock_cur_ctx.__aexit__.return_value = None
    mock_conn.cursor.return_value = mock_cur_ctx

    mock_tx_ctx = AsyncMock()
    mock_conn.transaction.return_value.__aenter__ = AsyncMock(return_value=mock_tx_ctx)
    mock_conn.transaction.return_value.__aexit__ = AsyncMock(return_value=None)

    mock_pool = MagicMock()
    mock_pool.closed = False
    mock_pool_conn_ctx = AsyncMock()
    mock_pool_conn_ctx.__aenter__.return_value = mock_conn
    mock_pool_conn_ctx.__aexit__.return_value = None
    mock_pool.connection.return_value = mock_pool_conn_ctx

    with patch("coordinator.db.get_pool", AsyncMock(return_value=mock_pool)):
        with pytest.raises(RuntimeError, match="Database disk full"):
            await publish_contract_revision(
                service_name="billing-service",
                endpoint_path="/v1/charges",
                http_method="POST",
                revision_number=1,
                schema_json={"properties": {}},
                semantic_summary="Test summary",
                published_by="agent-a",
            )


# ==============================================================================
# 3. Live CockroachDB Integration Test
# ==============================================================================


@pytest.mark.integration
@pytest.mark.asyncio
async def test_live_atomic_publication_and_outbox():
    """Live test: Publishes contract revision to live CockroachDB and verifies atomic state."""
    health = await check_health(timeout_seconds=3.0)
    if health.get("status") != "healthy":
        pytest.skip(f"Live CockroachDB is not reachable: {health.get('error')}")

    try:
        await init_db()

        # Publish v1.0
        unique_endpoint = f"/v1/charges-test-{uuid.uuid4().hex[:8]}"
        res_v1 = await publish_contract_revision(
            service_name="billing-service",
            endpoint_path=unique_endpoint,
            http_method="POST",
            revision_number=1,
            schema_json={"properties": {"card_token": {"type": "string"}}},
            semantic_summary="Billing charge endpoint v1",
            published_by="agent-a-v1",
            source_commit="commit-billing-v1-sha",
        )
        assert res_v1["revision_number"] == 1
        assert res_v1["outbox_event_id"] is not None

        # Verify outbox event written
        outbox_events = await execute_query(
            "SELECT * FROM coordinator_outbox WHERE aggregate_id = %s;",
            (res_v1["contract_id"],),
        )
        assert len(outbox_events) >= 1
        event = outbox_events[-1]
        assert event["event_type"] == "CONTRACT_CHANGED"
        assert event["source_service"] == "billing-service"

        # Verify audit history written
        audit_records = await execute_query(
            "SELECT * FROM contract_audit_history WHERE source_service = 'billing-service';"
        )
        assert len(audit_records) >= 1

    finally:
        await close_pool()
