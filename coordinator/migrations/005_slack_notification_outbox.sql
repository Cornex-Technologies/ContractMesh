-- ==============================================================================
-- CodeClaim: Migration 005 - Durable Slack projection of coordinator_outbox
-- ==============================================================================

CREATE TABLE IF NOT EXISTS slack_notification_deliveries (
    event_id UUID PRIMARY KEY REFERENCES coordinator_outbox(event_id) ON DELETE CASCADE,
    status STRING NOT NULL DEFAULT 'PENDING', -- PENDING | DELIVERING | DELIVERED | RETRY_READY | FAILED
    attempt_count INT8 NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_expires_at TIMESTAMPTZ,
    last_error STRING,
    delivered_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT valid_slack_delivery_status CHECK (status IN ('PENDING', 'DELIVERING', 'DELIVERED', 'RETRY_READY', 'FAILED'))
);

CREATE TABLE IF NOT EXISTS slack_notification_attempts (
    attempt_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id UUID NOT NULL REFERENCES coordinator_outbox(event_id) ON DELETE CASCADE,
    attempt_number INT8 NOT NULL,
    status STRING NOT NULL, -- DELIVERED | RETRYABLE_FAILURE | PERMANENT_FAILURE
    response_code INT8,
    response_summary STRING,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(event_id, attempt_number)
);

CREATE INDEX IF NOT EXISTS idx_slack_notification_deliveries_pending
    ON slack_notification_deliveries (status, next_attempt_at);
