-- ============================================================================
-- CodeClaim: Migration 013 - Late compatibility claims and provider safety lookup
-- ============================================================================

-- Work is created before a harness necessarily exists. These indexes keep the
-- service/repository claim path and provider deployment gate bounded as the
-- compatibility ledger grows.
CREATE INDEX IF NOT EXISTS idx_compatibility_work_claimable_target
    ON compatibility_work_items (target_service, target_repository, state, created_at);

CREATE INDEX IF NOT EXISTS idx_compatibility_work_provider_gate
    ON compatibility_work_items (source_contract_id, state, created_at);
