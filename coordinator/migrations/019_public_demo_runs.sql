-- ============================================================================
-- CodeClaim: Migration 019 - Bounded Public Demo Run State
--
-- This is a singleton coordinator record for the optional public demo button.
-- The durable audit/outbox tables remain the historical source of truth; this
-- table only prevents duplicate public runs and exposes the current phase.
-- ============================================================================

CREATE TABLE IF NOT EXISTS public_demo_runs (
    demo_key STRING PRIMARY KEY,
    run_id UUID NOT NULL,
    status STRING NOT NULL,
    phase STRING NOT NULL,
    result JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_public_demo_run_status
        CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED'))
);

CREATE INDEX IF NOT EXISTS idx_public_demo_runs_updated_at
    ON public_demo_runs (updated_at DESC);
