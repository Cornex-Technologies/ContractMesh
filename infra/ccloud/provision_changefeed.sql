-- ==============================================================================
-- CodeClaim: CockroachDB Managed MCP Audit Role & CDC Changefeed Provisioning
-- ==============================================================================
-- Parameterized Deployment Template.
-- Injected environment variables required:
--   1. ${MCP_AUDIT_PASSWORD}       : Strong audit role password
--   2. ${COORDINATOR_WEBHOOK_URL}  : Public HTTPS coordinator ingress for CDC changefeeds
--   3. ${CHANGEFEED_WEBHOOK_SECRET}: Authenticated Bearer token for webhook validation
-- ==============================================================================

-- 1. Create Cluster-Scoped Read-Only MCP Audit User
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mcp_audit_agent') THEN
        CREATE ROLE mcp_audit_agent WITH LOGIN;
    END IF;
END $$;

-- Securely set password from environment variable
ALTER ROLE mcp_audit_agent WITH PASSWORD '${MCP_AUDIT_PASSWORD}';

-- 2. Grant Database & Schema Usage
GRANT CONNECT ON DATABASE codeclaim_db TO mcp_audit_agent;
GRANT USAGE ON SCHEMA public TO mcp_audit_agent;

-- 3. Least-Privilege Access: Grant SELECT on Vector Discovery & Curated Audit Views ONLY
GRANT SELECT ON TABLE service_contracts TO mcp_audit_agent;
GRANT SELECT ON TABLE service_contract_revisions TO mcp_audit_agent;
GRANT SELECT ON TABLE semantic_memory TO mcp_audit_agent;
GRANT SELECT ON TABLE contract_drift_audit TO mcp_audit_agent;
GRANT SELECT ON TABLE contract_publication_audit TO mcp_audit_agent;

-- 4. Defense-in-Depth: Explicitly Revoke Any Mutating Permissions
REVOKE INSERT, UPDATE, DELETE, TRUNCATE, DROP, ALTER ON ALL TABLES IN SCHEMA public FROM mcp_audit_agent;

-- 5. Active Transactional Changefeed on Outbox with Authenticated Webhook
-- Streams transactional outbox events to the Coordinator webhook
CREATE CHANGEFEED FOR TABLE coordinator_outbox 
INTO '${COORDINATOR_WEBHOOK_URL}'
WITH 
    format = json,
    updated,
    envelope = wrapped,
    protect_data_from_gc_on_sink_failure,
    webhook_auth_header = 'Bearer ${CHANGEFEED_WEBHOOK_SECRET}';
