-- ============================================================================
-- CodeClaim: Migration 017 - Explicit Service Registration Provenance
--
-- Migration 012 seeded demo service names before the onboarding CLI existed.
-- Applied migrations are immutable, so this forward migration adds provenance
-- without editing 012. The cleanup of unreferenced seed rows is separate in
-- migration 018.
-- ============================================================================

ALTER TABLE microservices
    ADD COLUMN IF NOT EXISTS registration_source STRING NOT NULL DEFAULT 'LEGACY_UNKNOWN';

ALTER TABLE microservices
    ADD COLUMN IF NOT EXISTS registered_by STRING;

ALTER TABLE microservices
    ADD COLUMN IF NOT EXISTS registration_event_id UUID;

ALTER TABLE microservices
    ADD CONSTRAINT IF NOT EXISTS chk_microservices_registration_source
    CHECK (registration_source IN ('LEGACY_UNKNOWN', 'MIGRATION_SEED', 'ONBOARDING_CLI'));
