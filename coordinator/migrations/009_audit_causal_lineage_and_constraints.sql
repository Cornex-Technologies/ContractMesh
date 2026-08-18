-- ==============================================================================
-- CodeClaim: Migration 009 - Causal Lineage, Branch Metadata & Task Summary
-- ==============================================================================

-- 1. Active Tasks: Add bounded summary and branch metadata
ALTER TABLE active_agent_tasks
    ADD COLUMN IF NOT EXISTS task_summary STRING DEFAULT 'Agent Task';

ALTER TABLE active_agent_tasks
    ADD COLUMN IF NOT EXISTS branch_name STRING;

-- 2. Compatibility Work & Incidents: Add branch metadata
ALTER TABLE compatibility_work_items
    ADD COLUMN IF NOT EXISTS branch_name STRING;

ALTER TABLE compatibility_incidents
    ADD COLUMN IF NOT EXISTS branch_name STRING;

-- 3. Drift Events: Add causal links
ALTER TABLE drift_events
    ADD COLUMN IF NOT EXISTS outbox_event_id UUID;

ALTER TABLE drift_events
    ADD COLUMN IF NOT EXISTS causation_id UUID;

ALTER TABLE drift_events
    ADD COLUMN IF NOT EXISTS correlation_id UUID;

-- 4. Contract Audit History: Add causal links and immutable tracking
ALTER TABLE contract_audit_history
    ADD COLUMN IF NOT EXISTS outbox_event_id UUID;

ALTER TABLE contract_audit_history
    ADD COLUMN IF NOT EXISTS causation_id UUID;

ALTER TABLE contract_audit_history
    ADD COLUMN IF NOT EXISTS correlation_id UUID;

CREATE INDEX IF NOT EXISTS idx_contract_audit_causation
    ON contract_audit_history (causation_id, correlation_id);

CREATE INDEX IF NOT EXISTS idx_drift_events_outbox
    ON drift_events (outbox_event_id);
