-- ==============================================================================
-- CodeClaim Migration 011: Complete prompt purge from active_agent_tasks
-- Eliminates legacy prompt storage column to ensure zero-trust compliance
-- ==============================================================================

ALTER TABLE active_agent_tasks DROP COLUMN IF EXISTS task_prompt;
