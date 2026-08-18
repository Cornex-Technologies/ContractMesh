-- ==============================================================================
-- CodeClaim: Migration 008 - Structured Incompatibility Incidents & Evidence
-- ==============================================================================

ALTER TABLE compatibility_incidents
    ADD COLUMN IF NOT EXISTS reason_code STRING NOT NULL DEFAULT 'UNAVAILABLE_REQUIRED_INPUT';

ALTER TABLE compatibility_incidents
    ADD COLUMN IF NOT EXISTS unavailable_required_input STRING;

ALTER TABLE compatibility_incidents
    ADD COLUMN IF NOT EXISTS provider_service STRING;

ALTER TABLE compatibility_incidents
    ADD COLUMN IF NOT EXISTS provider_contract_revision INT8;

ALTER TABLE compatibility_incidents
    ADD COLUMN IF NOT EXISTS sources_checked JSONB NOT NULL DEFAULT '[]'::JSONB;

ALTER TABLE compatibility_incidents
    ADD COLUMN IF NOT EXISTS worktree_path STRING;

ALTER TABLE compatibility_incidents
    ADD COLUMN IF NOT EXISTS source_commit STRING;

ALTER TABLE compatibility_incidents
    ADD COLUMN IF NOT EXISTS changed_files JSONB NOT NULL DEFAULT '[]'::JSONB;

CREATE INDEX IF NOT EXISTS idx_compatibility_incidents_reason
    ON compatibility_incidents (reason_code, status);
