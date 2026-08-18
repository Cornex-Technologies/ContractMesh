-- ============================================================================
-- CodeClaim: Migration 016 - Enforce semantic compatibility-work identity
-- ============================================================================

ALTER TABLE compatibility_work_items
    ALTER COLUMN coordination_key SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_compatibility_work_coordination_key
    ON compatibility_work_items (coordination_key);

CREATE INDEX IF NOT EXISTS idx_compatibility_work_active_coordination
    ON compatibility_work_items (coordination_key, state, updated_at);
