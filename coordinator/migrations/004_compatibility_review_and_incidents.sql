-- ==============================================================================
-- CodeClaim: Migration 004 - Review-required compatibility and human incidents
-- ==============================================================================

ALTER TABLE compatibility_work_items
    DROP CONSTRAINT IF EXISTS valid_compatibility_work_state;
ALTER TABLE compatibility_work_items
    ADD CONSTRAINT valid_compatibility_work_state CHECK (state IN (
        'PENDING', 'DISPATCHED', 'ACKNOWLEDGED', 'EXECUTING', 'AWAITING_APPROVAL',
        'VERIFIED', 'COMPLETED', 'REVIEW_REQUIRED', 'BLOCKED', 'INCOMPATIBLE',
        'FAILED', 'EXPIRED', 'CANCELLED'
    ));

CREATE TABLE IF NOT EXISTS compatibility_incidents (
    incident_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    work_item_id UUID NOT NULL UNIQUE REFERENCES compatibility_work_items(work_item_id) ON DELETE CASCADE,
    incident_type STRING NOT NULL, -- BLOCKED | INCOMPATIBLE | REVIEW_REQUIRED
    missing_requirement STRING,
    evidence JSONB NOT NULL,
    requested_resolution STRING NOT NULL,
    status STRING NOT NULL DEFAULT 'HUMAN_DECISION_REQUIRED',
    resolved_by STRING,
    resolution_summary STRING,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at TIMESTAMPTZ,
    CONSTRAINT valid_compatibility_incident_type CHECK (incident_type IN ('BLOCKED', 'INCOMPATIBLE', 'REVIEW_REQUIRED')),
    CONSTRAINT valid_compatibility_incident_status CHECK (status IN ('HUMAN_DECISION_REQUIRED', 'RESOLVED', 'DISMISSED'))
);

CREATE INDEX IF NOT EXISTS idx_compatibility_incidents_status
    ON compatibility_incidents (status, created_at);
