"""Section 1 Verification Suite: CockroachDB Schema, Vector Indexing & Connection Pool."""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
import pytest
from psycopg.errors import SerializationFailure

from coordinator.db import (
    check_health,
    close_pool,
    execute_query,
    execute_statement,
    fetch_one,
    init_db,
    run_transaction,
)


# ==============================================================================
# Unit Tests (Offline / Mocked)
# ==============================================================================


def test_schema_sql_contains_native_vector_indexes():
    """Verify that schema.sql uses native CREATE VECTOR INDEX syntax."""
    schema_path = Path(__file__).parent.parent / "coordinator" / "schema.sql"
    assert schema_path.exists(), "schema.sql must exist"
    
    content = schema_path.read_text(encoding="utf-8")
    
    # 1. Check all required tables exist
    expected_tables = [
        "microservices",
        "service_contracts",
        "service_contract_revisions",
        "semantic_memory",
        "service_contract_consumers",
        "active_agent_tasks",
        "task_contract_dependencies",
        "drift_events",
        "coordinator_outbox",
        "event_inbox",
        "contract_audit_history",
        "deployments",
    ]
    for table in expected_tables:
        assert f"CREATE TABLE IF NOT EXISTS {table}" in content, f"Missing table: {table}"

    # 2. Check all required MCP views exist
    expected_views = [
        "contract_drift_audit",
        "contract_publication_audit",
    ]
    for view in expected_views:
        assert f"CREATE OR REPLACE VIEW {view}" in content, f"Missing view: {view}"

    # 3. Check native CockroachDB VECTOR INDEX syntax
    assert "CREATE VECTOR INDEX IF NOT EXISTS idx_contract_summary_embedding ON service_contract_revisions (summary_embedding);" in content
    assert "CREATE VECTOR INDEX IF NOT EXISTS idx_semantic_memory_embedding ON semantic_memory (embedding);" in content


def test_service_registration_migrations_remove_implicit_seeds_and_track_provenance():
    """Migration 012 remains immutable while later migrations remove seed-only rows safely."""
    migrations_dir = Path(__file__).parent.parent / "coordinator" / "migrations"
    migration_012 = (migrations_dir / "012_enforce_service_boundaries_and_entrypoints.sql").read_text(encoding="utf-8")
    migration_017 = (migrations_dir / "017_service_registration_provenance.sql").read_text(encoding="utf-8")
    migration_018 = (migrations_dir / "018_remove_implicit_service_seeds.sql").read_text(encoding="utf-8")

    assert "INSERT INTO microservices" in migration_012
    assert "registration_source" in migration_017
    assert "registered_by" in migration_017
    assert "registration_event_id" in migration_017
    assert "MIGRATION_SEED" in migration_018
    assert "SERVICE_ONBOARDED" in migration_018
    assert "NOT EXISTS" in migration_018
    assert "DELETE FROM microservices" in migration_018


def test_changefeed_sql_validity():
    """Verify that changefeed.sql is well-formed with outbox table reference and valid authentication options."""
    changefeed_path = Path(__file__).parent.parent / "infra" / "cockroach" / "changefeed.sql"
    assert changefeed_path.exists(), "changefeed.sql must exist"
    
    content = changefeed_path.read_text(encoding="utf-8")
    assert "CREATE CHANGEFEED FOR TABLE coordinator_outbox" in content
    assert "webhook-" in content
    assert "envelope = 'wrapped'" in content
    assert "Basic" in content or "extra_headers" in content


@pytest.mark.asyncio
async def test_run_transaction_retry_on_serialization_failure():
    """Verify that run_transaction catches SerializationFailure (40001) and retries successfully."""
    attempts = 0

    async def mock_tx_fn(conn):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            # Simulate CockroachDB Serialization Failure (SQLSTATE 40001)
            err = SerializationFailure("restart transaction: TransactionRetryWithNewSnapshotError")
            raise err
        return {"status": "success", "attempts": attempts}

    # Mock connection and pool
    mock_conn = MagicMock()
    mock_tx_context = AsyncMock()
    mock_conn.transaction.return_value.__aenter__ = AsyncMock(return_value=mock_tx_context)
    mock_conn.transaction.return_value.__aexit__ = AsyncMock(return_value=None)

    mock_pool = MagicMock()
    mock_pool.closed = False
    mock_pool_conn_ctx = AsyncMock()
    mock_pool_conn_ctx.__aenter__.return_value = mock_conn
    mock_pool_conn_ctx.__aexit__.return_value = None
    mock_pool.connection.return_value = mock_pool_conn_ctx

    with patch("coordinator.db.get_pool", AsyncMock(return_value=mock_pool)):
        result = await run_transaction(mock_tx_fn, max_retries=5, base_backoff=0.001)
        assert result["status"] == "success"
        assert result["attempts"] == 3
        assert attempts == 3


@pytest.mark.asyncio
async def test_run_transaction_exhausts_retries():
    """Verify that run_transaction raises when max_retries are exhausted."""
    attempts = 0

    async def always_failing_tx(conn):
        nonlocal attempts
        attempts += 1
        raise SerializationFailure("persistent conflict")

    mock_conn = MagicMock()
    mock_tx_context = AsyncMock()
    mock_conn.transaction.return_value.__aenter__ = AsyncMock(return_value=mock_tx_context)
    mock_conn.transaction.return_value.__aexit__ = AsyncMock(return_value=None)

    mock_pool = MagicMock()
    mock_pool.closed = False
    mock_pool_conn_ctx = AsyncMock()
    mock_pool_conn_ctx.__aenter__.return_value = mock_conn
    mock_pool_conn_ctx.__aexit__.return_value = None
    mock_pool.connection.return_value = mock_pool_conn_ctx

    with patch("coordinator.db.get_pool", AsyncMock(return_value=mock_pool)):
        with pytest.raises(SerializationFailure):
            await run_transaction(always_failing_tx, max_retries=3, base_backoff=0.001)
        assert attempts == 3


@pytest.mark.asyncio
async def test_versioned_migrations_discovery_and_execution(tmp_path):
    """Verify versioned migration runner executes pending migrations and records checksums."""
    from coordinator.db import migrate_db

    # Create temporary mock migration directory with two ordered SQL files
    mig_dir = tmp_path / "migrations"
    mig_dir.mkdir()
    
    mig1 = mig_dir / "001_create_test_table.sql"
    mig1.write_text("CREATE TABLE test_table_1 (id INT PRIMARY KEY);", encoding="utf-8")
    
    mig2 = mig_dir / "002_add_column.sql"
    mig2.write_text("ALTER TABLE test_table_1 ADD COLUMN name STRING;", encoding="utf-8")

    executed_sql = []
    
    mock_cur = AsyncMock()
    async def mock_execute(sql, params=None):
        executed_sql.append((sql, params))

    mock_cur.execute = mock_execute
    mock_cur.fetchall = AsyncMock(return_value=[])  # No migrations previously applied

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
        applied = await migrate_db(mig_dir)
        
        assert applied == ["001", "002"]
        all_sql = " ".join(s[0] for s in executed_sql)
        assert "CREATE TABLE IF NOT EXISTS schema_migrations" in all_sql
        assert "CREATE TABLE test_table_1" in all_sql
        assert "ALTER TABLE test_table_1 ADD COLUMN name" in all_sql
        assert "INSERT INTO schema_migrations" in all_sql



@pytest.mark.asyncio
async def test_versioned_migrations_checksum_mismatch_rejection(tmp_path):
    """Verify migrate_db raises ValueError when an already-applied migration file has been altered on disk."""
    from coordinator.db import migrate_db

    mig_dir = tmp_path / "migrations"
    mig_dir.mkdir()
    
    mig1 = mig_dir / "001_initial.sql"
    mig1.write_text("CREATE TABLE initial (id INT);", encoding="utf-8")

    # Simulate database recording a different historical checksum
    mock_cur = AsyncMock()
    mock_cur.execute = AsyncMock()
    mock_cur.fetchall = AsyncMock(return_value=[
        {
            "version": "001",
            "name": "001_initial",
            "checksum": "different_historical_sha256_hash_value",
        }
    ])

    mock_conn = MagicMock()
    mock_cur_ctx = AsyncMock()
    mock_cur_ctx.__aenter__.return_value = mock_cur
    mock_cur_ctx.__aexit__.return_value = None
    mock_conn.cursor.return_value = mock_cur_ctx

    mock_pool = MagicMock()
    mock_pool.closed = False
    mock_pool_conn_ctx = AsyncMock()
    mock_pool_conn_ctx.__aenter__.return_value = mock_conn
    mock_pool_conn_ctx.__aexit__.return_value = None
    mock_pool.connection.return_value = mock_pool_conn_ctx

    with patch("coordinator.db.get_pool", AsyncMock(return_value=mock_pool)):
        with pytest.raises(ValueError, match="Checksum mismatch for already-applied migration 001"):
            await migrate_db(mig_dir)




# ==============================================================================
# Live Database Integration Tests (Marked with @pytest.mark.integration)
# ==============================================================================


@pytest.mark.integration
@pytest.mark.asyncio
async def test_live_cockroach_schema_and_vector_operations():
    """Live test: Connects to CockroachDB, initializes schema, tests vector insertion & distance query."""
    health = await check_health(timeout_seconds=3.0)
    if health.get("status") != "healthy":
        pytest.skip(f"Live CockroachDB is not reachable: {health.get('error')}")

    try:
        # 1. Initialize schema
        await init_db()

        # 2. Insert test service & contract
        await execute_statement(
            """
            INSERT INTO microservices (service_name, repository_path, primary_region)
            VALUES ('billing-service', 'repos/billing-service', 'us-east-1')
            ON CONFLICT (service_name) DO NOTHING;
            """
        )

        contract = await fetch_one(
            """
            INSERT INTO service_contracts (service_name, endpoint_path, http_method, contract_key)
            VALUES ('billing-service', '/v1/charges', 'POST', 'billing-service:POST:/v1/charges')
            ON CONFLICT (contract_key) DO UPDATE SET service_name = EXCLUDED.service_name
            RETURNING contract_id;
            """
        )
        assert contract is not None
        contract_id = contract["contract_id"]

        # 3. Create 1536-dimensional mock vector
        mock_embedding = [0.05] * 1536
        vector_str = "[" + ",".join(str(x) for x in mock_embedding) + "]"

        # 4. Insert contract revision with vector embedding
        rev = await fetch_one(
            """
            INSERT INTO service_contract_revisions (
                contract_id, revision_number, source_commit, schema_json, semantic_summary, summary_embedding, published_by
            )
            VALUES (
                %s, 1, 'commit-test-001', '{"type": "object"}'::jsonb, 'Process credit card charges', %s::VECTOR(1536), 'test-agent'
            )
            ON CONFLICT (contract_id, revision_number) DO UPDATE SET semantic_summary = EXCLUDED.semantic_summary, summary_embedding = EXCLUDED.summary_embedding
            RETURNING contract_revision_id;
            """,
            (contract_id, vector_str),
        )
        assert rev is not None

        # 5. Execute native vector nearest-neighbor query with cosine distance (<=>)
        query_vector_str = "[" + ",".join(str(x) for x in [0.049] * 1536) + "]"
        nearest = await fetch_one(
            """
            SELECT contract_revision_id, summary_embedding <=> %s::VECTOR(1536) AS distance
            FROM service_contract_revisions
            WHERE contract_id = %s AND summary_embedding IS NOT NULL
            ORDER BY distance ASC
            LIMIT 1;
            """,
            (query_vector_str, contract_id),
        )
        assert nearest is not None
        assert nearest["distance"] is not None
        assert float(nearest["distance"]) < 0.1

        # 6. Verify transactional outbox insertion
        outbox_row = await fetch_one(
            """
            INSERT INTO coordinator_outbox (
                aggregate_type, aggregate_id, aggregate_revision, source_service, event_type, payload
            )
            VALUES ('SERVICE_CONTRACT', %s, 1, 'billing-service', 'CONTRACT_CHANGED', '{"version": 1}'::jsonb)
            RETURNING event_id;
            """,
            (contract_id,),
        )
        assert outbox_row is not None
        assert "event_id" in outbox_row

        # 7. Verify MCP read-only views queryable
        audit_records = await execute_query("SELECT * FROM contract_drift_audit LIMIT 5;")
        assert isinstance(audit_records, list)

    finally:
        await close_pool()
