-- ============================================================================
-- CodeClaim: Migration 014 - Semantic compatibility-work identity
-- ============================================================================
-- Expand-only schema change. Data backfill and duplicate reconciliation are
-- intentionally kept in the following migration.

ALTER TABLE compatibility_work_items
    ADD COLUMN IF NOT EXISTS coordination_key STRING;

ALTER TABLE compatibility_work_items
    DROP CONSTRAINT IF EXISTS valid_compatibility_work_state;

ALTER TABLE compatibility_work_items
    ADD CONSTRAINT valid_compatibility_work_state CHECK (state IN (
        'PENDING', 'DISPATCHED', 'ACKNOWLEDGED', 'EXECUTING', 'AWAITING_APPROVAL',
        'VERIFIED', 'COMPLETED', 'REVIEW_REQUIRED', 'BLOCKED', 'INCOMPATIBLE',
        'FAILED', 'EXPIRED', 'CANCELLED', 'SUPERSEDED'
    ));
