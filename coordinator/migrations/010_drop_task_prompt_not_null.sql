-- ==============================================================================
-- CodeClaim Migration 010: Drop NOT NULL constraint on task_prompt
-- Enforces Charter Zero-Trust Architecture: task_summary is the sole authorized field
-- ==============================================================================

ALTER TABLE active_agent_tasks ALTER COLUMN task_prompt DROP NOT NULL;
ALTER TABLE active_agent_tasks ALTER COLUMN task_prompt SET DEFAULT '';
