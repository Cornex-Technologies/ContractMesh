-- ==============================================================================
-- CodeClaim: Migration 003 - Harness-neutral compatibility workflow
-- ==============================================================================

CREATE TABLE IF NOT EXISTS harness_registrations (
    harness_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    harness_name STRING NOT NULL UNIQUE,
    harness_type STRING NOT NULL,
    service_name STRING NOT NULL,
    repository_url STRING NOT NULL,
    dispatch_mode STRING NOT NULL DEFAULT 'poll', -- poll | webhook
    dispatch_url STRING,
    capability_manifest JSONB NOT NULL DEFAULT '{}'::JSONB,
    access_token_hash STRING NOT NULL,
    status STRING NOT NULL DEFAULT 'ACTIVE', -- ACTIVE | PAUSED | DISABLED
    last_seen_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT valid_harness_dispatch_mode CHECK (dispatch_mode IN ('poll', 'webhook')),
    CONSTRAINT valid_harness_status CHECK (status IN ('ACTIVE', 'PAUSED', 'DISABLED'))
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
    causation_id UUID NOT NULL,
    correlation_id UUID NOT NULL,
    hop_count INT8 NOT NULL DEFAULT 0,
    task_id UUID REFERENCES active_agent_tasks(task_id),
    payload JSONB NOT NULL,
    dispatch_attempts INT8 NOT NULL DEFAULT 0,
    lease_expires_at TIMESTAMPTZ,
    failure_reason STRING,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT valid_compatibility_work_state CHECK (state IN (
        'PENDING', 'DISPATCHED', 'ACKNOWLEDGED', 'EXECUTING', 'AWAITING_APPROVAL',
        'VERIFIED', 'COMPLETED', 'FAILED', 'EXPIRED', 'CANCELLED'
    )),
    CONSTRAINT nonnegative_hop_count CHECK (hop_count >= 0 AND hop_count <= 5)
);

CREATE INDEX IF NOT EXISTS idx_compatibility_work_dispatch
    ON compatibility_work_items (state, created_at);
CREATE INDEX IF NOT EXISTS idx_compatibility_work_target
    ON compatibility_work_items (target_service, state, created_at);

CREATE TABLE IF NOT EXISTS compatibility_dispatch_attempts (
    dispatch_attempt_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    work_item_id UUID NOT NULL REFERENCES compatibility_work_items(work_item_id) ON DELETE CASCADE,
    attempt_number INT8 NOT NULL,
    delivery_status STRING NOT NULL, -- DELIVERED | RETRYABLE_FAILURE | PERMANENT_FAILURE | POLL_READY
    response_code INT8,
    response_summary STRING,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (work_item_id, attempt_number)
);
