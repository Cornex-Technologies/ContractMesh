-- ==============================================================================
-- CodeClaim: Migration 002 - Idempotent Schema Evolution Check
-- ==============================================================================

-- 1. Ensure agent_checkpoints ledger exists
CREATE TABLE IF NOT EXISTS agent_checkpoints (
    checkpoint_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id UUID NOT NULL REFERENCES active_agent_tasks(task_id) ON DELETE CASCADE,
    plan_revision INT8 NOT NULL,
    status STRING NOT NULL,
    checkpoint_state JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 2. Ensure checkpoint_state column exists on active_agent_tasks
ALTER TABLE active_agent_tasks ADD COLUMN IF NOT EXISTS checkpoint_state JSONB;

-- 3. Ensure payload column exists on event_inbox
ALTER TABLE event_inbox ADD COLUMN IF NOT EXISTS payload JSONB;
