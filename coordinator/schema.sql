-- ==============================================================================
-- CodeClaim: Consolidated CockroachDB Schema (Derived from Migrations 001-012)
-- NOTE: The versioned SQL migration files in coordinator/migrations/ are the
-- authoritative source of truth for runtime database schema evolution.
-- ==============================================================================

-- 1. Microservice Registry
CREATE TABLE IF NOT EXISTS microservices (
    service_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    service_name STRING UNIQUE NOT NULL,
    repository_path STRING NOT NULL,
    entrypoint_module STRING NOT NULL DEFAULT 'main',
    entrypoint_app STRING NOT NULL DEFAULT 'app',
    primary_region STRING NOT NULL DEFAULT 'us-east-1',
    registration_source STRING NOT NULL DEFAULT 'LEGACY_UNKNOWN',
    registered_by STRING,
    registration_event_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 2. Stable Service Contract Identity
CREATE TABLE IF NOT EXISTS service_contracts (
    contract_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    service_name STRING NOT NULL REFERENCES microservices(service_name) ON DELETE CASCADE,
    endpoint_path STRING NOT NULL,
    http_method STRING NOT NULL,
    contract_key STRING NOT NULL UNIQUE,
    lifecycle_state STRING NOT NULL DEFAULT 'ACTIVE',
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

CREATE VECTOR INDEX IF NOT EXISTS idx_contract_summary_embedding ON service_contract_revisions (summary_embedding);

-- 4. Dedicated LangChain Semantic Memory Vector Table
CREATE TABLE IF NOT EXISTS semantic_memory (
    memory_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    text STRING NOT NULL,
    embedding VECTOR(1536),
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

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

-- 6. Active Agent Tasks & In-Flight Intentions
CREATE TABLE IF NOT EXISTS active_agent_tasks (
    task_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id STRING NOT NULL,
    service_name STRING NOT NULL,
    task_summary STRING NOT NULL,
    worktree_path STRING NOT NULL,
    base_commit STRING,
    status STRING NOT NULL DEFAULT 'OPTIMISTIC_EXECUTING',
    plan_revision INT8 NOT NULL DEFAULT 1,
    checkpoint_state JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 6. Task Contract Dependencies
CREATE TABLE IF NOT EXISTS task_contract_dependencies (
    dependency_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id UUID NOT NULL REFERENCES active_agent_tasks(task_id) ON DELETE CASCADE,
    provider_service STRING NOT NULL,
    contract_id UUID NOT NULL REFERENCES service_contracts(contract_id),
    assumed_revision INT8 NOT NULL,
    dependency_kind STRING NOT NULL DEFAULT 'HTTP_REST',
    dependency_path STRING NOT NULL,
    interface_dependency_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_task_contract_dependency UNIQUE (task_id, provider_service, contract_id)
);

-- 7. Exact HTTP Interface Dependencies
CREATE TABLE IF NOT EXISTS http_interface_dependencies (
    dependency_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    provider_service STRING NOT NULL,
    consumer_service STRING NOT NULL,
    contract_id UUID NOT NULL REFERENCES service_contracts(contract_id),
    assumed_provider_revision INT8 NOT NULL,
    http_method STRING NOT NULL,
    endpoint_path STRING NOT NULL,
    path_parameters JSONB NOT NULL DEFAULT '{}'::jsonb,
    query_parameters JSONB NOT NULL DEFAULT '{}'::jsonb,
    declared_headers JSONB NOT NULL DEFAULT '{}'::jsonb,
    request_body_schema JSONB NOT NULL DEFAULT '{}'::jsonb,
    response_schemas JSONB NOT NULL DEFAULT '{}'::jsonb,
    consumer_repository STRING NOT NULL,
    consumer_source_file STRING NOT NULL,
    consumer_source_evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    confirmation_status STRING NOT NULL DEFAULT 'UNCONFIRMED',
    confirmed_by STRING,
    confirmed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_exact_http_interface UNIQUE (consumer_service, provider_service, contract_id, assumed_provider_revision, consumer_repository, consumer_source_file)
);

-- 8. Unconfirmed Dependency Candidates
CREATE TABLE IF NOT EXISTS unconfirmed_dependency_candidates (
    candidate_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    consumer_service STRING NOT NULL,
    provider_service STRING NOT NULL,
    endpoint_path STRING NOT NULL,
    http_method STRING NOT NULL,
    source_file STRING NOT NULL,
    matched_by STRING NOT NULL,
    confidence_score FLOAT8 NOT NULL,
    status STRING NOT NULL DEFAULT 'UNCONFIRMED',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 9. Transactional Coordinator Outbox
CREATE TABLE IF NOT EXISTS coordinator_outbox (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type STRING NOT NULL,
    aggregate_id STRING NOT NULL,
    aggregate_revision INT8 NOT NULL,
    source_service STRING NOT NULL,
    event_type STRING NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 10. CDC Event Inbox
CREATE TABLE IF NOT EXISTS event_inbox (
    inbox_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id UUID UNIQUE NOT NULL,
    event_type STRING NOT NULL,
    source_service STRING NOT NULL,
    payload JSONB NOT NULL,
    processing_status STRING NOT NULL DEFAULT 'PENDING',
    attempt_count INT8 NOT NULL DEFAULT 0,
    lease_expires_at TIMESTAMPTZ,
    last_error STRING,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at TIMESTAMPTZ
);

-- 11. Drift Interventions
CREATE TABLE IF NOT EXISTS drift_events (
    drift_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    outbox_event_id UUID REFERENCES coordinator_outbox(event_id),
    causation_id UUID,
    correlation_id UUID,
    source_service STRING NOT NULL,
    target_task_id UUID NOT NULL REFERENCES active_agent_tasks(task_id) ON DELETE CASCADE,
    target_service STRING NOT NULL,
    old_contract_revision INT8 NOT NULL,
    new_contract_revision INT8 NOT NULL,
    breaking_diff JSONB NOT NULL,
    status STRING NOT NULL DEFAULT 'ACTIVE_INTERVENTION',
    resolved_by STRING,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at TIMESTAMPTZ
);

-- 12. Harness Neutral Registrations & Compatibility Work
CREATE TABLE IF NOT EXISTS harness_registrations (
    harness_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    harness_name STRING NOT NULL UNIQUE,
    harness_type STRING NOT NULL,
    service_name STRING NOT NULL REFERENCES microservices(service_name) ON DELETE CASCADE,
    repository_url STRING NOT NULL,
    dispatch_mode STRING NOT NULL DEFAULT 'poll',
    dispatch_url STRING,
    capability_manifest JSONB NOT NULL DEFAULT '{}'::JSONB,
    access_token_hash STRING NOT NULL,
    status STRING NOT NULL DEFAULT 'ACTIVE',
    last_seen_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS compatibility_work_items (
    work_item_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_event_id UUID NOT NULL REFERENCES coordinator_outbox(event_id),
    source_contract_id UUID NOT NULL REFERENCES service_contracts(contract_id),
    source_contract_revision INT8 NOT NULL,
    target_service STRING NOT NULL,
    target_repository STRING NOT NULL,
    harness_id UUID REFERENCES harness_registrations(harness_id),
    state STRING NOT NULL DEFAULT 'PENDING',
    idempotency_key STRING NOT NULL UNIQUE,
    coordination_key STRING NOT NULL UNIQUE,
    causation_id UUID NOT NULL,
    correlation_id UUID NOT NULL,
    hop_count INT8 NOT NULL DEFAULT 0,
    task_id UUID REFERENCES active_agent_tasks(task_id),
    payload JSONB NOT NULL,
    dispatch_attempts INT8 NOT NULL DEFAULT 0,
    lease_expires_at TIMESTAMPTZ,
    failure_reason STRING,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_compatibility_work_claimable_target
    ON compatibility_work_items (target_service, target_repository, state, created_at);

CREATE INDEX IF NOT EXISTS idx_compatibility_work_provider_gate
    ON compatibility_work_items (source_contract_id, state, created_at);

CREATE TABLE IF NOT EXISTS compatibility_incidents (
    incident_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    work_item_id UUID NOT NULL REFERENCES compatibility_work_items(work_item_id),
    task_id UUID REFERENCES active_agent_tasks(task_id),
    outcome STRING NOT NULL,
    reason_code STRING NOT NULL,
    missing_requirement STRING NOT NULL,
    unavailable_required_input STRING,
    provider_service STRING,
    provider_contract_revision INT8,
    sources_checked JSONB NOT NULL DEFAULT '[]'::jsonb,
    worktree_path STRING,
    source_commit STRING,
    changed_files JSONB NOT NULL DEFAULT '[]'::jsonb,
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    requested_resolution STRING NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 13. Slack Notification Deliveries
CREATE TABLE IF NOT EXISTS slack_notification_deliveries (
    event_id UUID PRIMARY KEY REFERENCES coordinator_outbox(event_id) ON DELETE CASCADE,
    status STRING NOT NULL DEFAULT 'PENDING',
    attempt_count INT8 NOT NULL DEFAULT 0,
    lease_expires_at TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error STRING,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS slack_notification_attempts (
    attempt_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id UUID NOT NULL REFERENCES coordinator_outbox(event_id) ON DELETE CASCADE,
    attempt_number INT8 NOT NULL,
    status STRING NOT NULL,
    response_code INT8,
    response_summary STRING,
    attempted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 14. Audit History
CREATE TABLE IF NOT EXISTS contract_audit_history (
    audit_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type STRING NOT NULL,
    source_service STRING NOT NULL,
    summary STRING NOT NULL,
    actor STRING NOT NULL,
    outbox_event_id UUID,
    causation_id UUID,
    correlation_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 15. Deployment State and Browser Reload Version
CREATE TABLE IF NOT EXISTS deployments (
    deployment_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    service_name STRING NOT NULL,
    source_commit STRING NOT NULL,
    status STRING NOT NULL,
    reload_version INT8 NOT NULL UNIQUE,
    health_check JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);

-- 16. Schema Migrations Ledger
CREATE TABLE IF NOT EXISTS schema_migrations (
    version STRING PRIMARY KEY,
    name STRING NOT NULL,
    checksum STRING NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 17. Dedicated Read-Only Views for CockroachDB Cloud Managed MCP Server
CREATE OR REPLACE VIEW contract_drift_audit AS
SELECT 
    d.drift_id,
    d.source_service,
    d.target_service,
    d.old_contract_revision,
    d.new_contract_revision,
    d.breaking_diff,
    d.status,
    d.created_at
FROM drift_events d;

CREATE OR REPLACE VIEW contract_publication_audit AS
SELECT 
    audit_id,
    event_type,
    source_service,
    summary,
    actor,
    created_at
FROM contract_audit_history;
