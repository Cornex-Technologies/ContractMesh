-- ==============================================================================
-- CodeClaim: Migration 001 - Authoritative Core Schema Specification
-- ==============================================================================

-- 1. Microservice Registry
CREATE TABLE IF NOT EXISTS microservices (
    service_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    service_name STRING UNIQUE NOT NULL,
    repository_path STRING NOT NULL,
    primary_region STRING NOT NULL DEFAULT 'us-east-1',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 2. Stable Service Contract Identity
CREATE TABLE IF NOT EXISTS service_contracts (
    contract_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    service_name STRING NOT NULL,
    endpoint_path STRING NOT NULL,
    http_method STRING NOT NULL,
    contract_key STRING NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 3. Immutable Contract Revisions + Native Vector Memory
CREATE TABLE IF NOT EXISTS service_contract_revisions (
    contract_revision_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    contract_id UUID NOT NULL REFERENCES service_contracts(contract_id) ON DELETE CASCADE,
    revision_number INT8 NOT NULL,
    source_commit STRING NOT NULL,
    schema_json JSONB NOT NULL,
    semantic_summary STRING NOT NULL,
    summary_embedding VECTOR(1536),
    embedding_model STRING,
    embedding_dimension INT8,
    is_active BOOL NOT NULL DEFAULT true,
    published_by STRING NOT NULL,
    published_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_contract_revision UNIQUE (contract_id, revision_number)
);

-- Native CockroachDB Vector Index for Fast Nearest-Neighbor Search
CREATE VECTOR INDEX IF NOT EXISTS idx_contract_summary_embedding ON service_contract_revisions (summary_embedding);

-- 4. Dedicated LangChain Semantic Memory Vector Table
CREATE TABLE IF NOT EXISTS semantic_memory (
    memory_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    text STRING NOT NULL,
    embedding VECTOR(1536),
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Native CockroachDB Vector Index on Semantic Memory
CREATE VECTOR INDEX IF NOT EXISTS idx_semantic_memory_embedding ON semantic_memory (embedding);

-- 5. Explicit Relational Dependency Graph
CREATE TABLE IF NOT EXISTS service_contract_consumers (
    consumer_service STRING NOT NULL,
    provider_service STRING NOT NULL,
    contract_id UUID NOT NULL REFERENCES service_contracts(contract_id) ON DELETE CASCADE,
    consumer_repository STRING NOT NULL,
    consumer_file_path STRING,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_service, provider_service, contract_id, consumer_repository)
);

-- 6. Active In-Flight Tasks & Intent
CREATE TABLE IF NOT EXISTS active_agent_tasks (
    task_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id STRING NOT NULL,
    service_name STRING NOT NULL,
    task_prompt STRING NOT NULL,
    worktree_path STRING NOT NULL,
    base_commit STRING NOT NULL,
    plan_revision INT8 NOT NULL DEFAULT 1,
    status STRING NOT NULL DEFAULT 'OPTIMISTIC_EXECUTING', -- OPTIMISTIC_EXECUTING, REPLAN_REQUIRED, REPLANNING, AWAITING_APPROVAL, RECONCILED, COMPLETED, FAILED
    checkpoint_state JSONB,
    heartbeat_at TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ,
    last_reconciled_at TIMESTAMPTZ,
    failure_reason STRING,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 6b. Granular Agent Checkpoint History (LangGraph Checkpoint Saver)
CREATE TABLE IF NOT EXISTS agent_checkpoints (
    checkpoint_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id UUID NOT NULL REFERENCES active_agent_tasks(task_id) ON DELETE CASCADE,
    plan_revision INT8 NOT NULL,
    status STRING NOT NULL,
    checkpoint_state JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 7. Multi-Service Task Dependencies
CREATE TABLE IF NOT EXISTS task_contract_dependencies (
    task_id UUID NOT NULL REFERENCES active_agent_tasks(task_id) ON DELETE CASCADE,
    provider_service STRING NOT NULL,
    contract_id UUID NOT NULL REFERENCES service_contracts(contract_id) ON DELETE CASCADE,
    assumed_revision INT8 NOT NULL,
    dependency_kind STRING NOT NULL DEFAULT 'HTTP_REST', -- HTTP_REST, GRPC, EVENT_PAYLOAD
    dependency_path STRING,
    PRIMARY KEY (task_id, provider_service, contract_id)
);

-- 8. Cross-Service Drift Events
CREATE TABLE IF NOT EXISTS drift_events (
    drift_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_service STRING NOT NULL,
    target_task_id UUID NOT NULL REFERENCES active_agent_tasks(task_id) ON DELETE CASCADE,
    target_service STRING NOT NULL,
    old_contract_revision INT8 NOT NULL,
    new_contract_revision INT8 NOT NULL,
    breaking_diff JSONB NOT NULL,
    status STRING NOT NULL DEFAULT 'ACTIVE_INTERVENTION', -- ACTIVE_INTERVENTION, RECONCILED, DISMISSED
    acknowledged BOOL NOT NULL DEFAULT false,
    resolved_by STRING,
    resolution_summary STRING,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    reconciled_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 9. Transactional Outbox (CDC Changefeed Spine)
CREATE TABLE IF NOT EXISTS coordinator_outbox (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type STRING NOT NULL,       -- 'SERVICE_CONTRACT', 'TASK_STATE', 'DEPLOYMENT'
    aggregate_id UUID NOT NULL,
    aggregate_revision INT8 NOT NULL,
    source_service STRING NOT NULL,
    event_type STRING NOT NULL,           -- 'CONTRACT_CHANGED', 'DRIFT_DETECTED', 'TASK_REPLAN_REQUIRED', 'DEPLOYMENT_COMPLETED'
    payload JSONB NOT NULL,
    event_version INT8 NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 10. Idempotent Ingestion Inbox
CREATE TABLE IF NOT EXISTS event_inbox (
    event_id UUID PRIMARY KEY,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    processing_status STRING NOT NULL DEFAULT 'RECEIVED', -- RECEIVED, PROCESSING, PROCESSED, FAILED
    attempt_count INT8 NOT NULL DEFAULT 0,
    payload JSONB,
    last_error STRING,
    processed_at TIMESTAMPTZ
);

-- 11. Append-Only Contract Audit History
CREATE TABLE IF NOT EXISTS contract_audit_history (
    history_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type STRING NOT NULL,
    source_service STRING NOT NULL,
    target_service STRING,
    summary STRING NOT NULL,
    schema_diff JSONB,
    actor STRING NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 12. Deployment State and Browser Reload Version
CREATE TABLE IF NOT EXISTS deployments (
    deployment_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    service_name STRING NOT NULL,
    source_commit STRING NOT NULL,
    status STRING NOT NULL, -- VALIDATING, DEPLOYING, HEALTHY, FAILED, ROLLED_BACK
    reload_version INT8 NOT NULL UNIQUE,
    health_check JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);


-- 13. Dedicated Read-Only Views for CockroachDB Cloud Managed MCP Server
CREATE OR REPLACE VIEW contract_drift_audit AS
SELECT 
    d.drift_id,
    d.source_service,
    d.target_service,
    d.old_contract_revision,
    d.new_contract_revision,
    d.breaking_diff,
    d.status,
    d.created_at,
    d.reconciled_at
FROM drift_events d;

CREATE OR REPLACE VIEW contract_publication_audit AS
SELECT 
    history_id,
    event_type,
    source_service,
    target_service,
    summary,
    schema_diff,
    actor,
    created_at
FROM contract_audit_history;
