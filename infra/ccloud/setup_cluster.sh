#!/usr/bin/env bash
# ==============================================================================
# CodeClaim - CockroachDB Cloud (ccloud) Cluster Setup & Configuration
# ==============================================================================
set -euo pipefail

CLUSTER_NAME="${1:-codeclaim-prod}"
CLOUD_PROVIDER="${2:-aws}"
REGION="${3:-us-east-1}"
DATABASE_NAME="codeclaim_db"

echo "======================================================================"
echo "🚀 Initializing CockroachDB Dedicated Cluster: ${CLUSTER_NAME}"
echo "Provider: ${CLOUD_PROVIDER} | Region: ${REGION}"
echo "======================================================================"

# 1. Verify ccloud CLI authentication
if ! command -v ccloud &> /dev/null; then
    echo "❌ ccloud CLI is not installed. Please install from https://cockroachlabs.cloud/docs"
    exit 1
fi

echo "✓ Checking ccloud authentication..."
ccloud auth whoami

# 2. Provision CockroachDB Dedicated Cluster (Multi-AZ with Vector Search)
echo "✓ Provisioning cluster '${CLUSTER_NAME}'..."
ccloud cluster create "${CLUSTER_NAME}" \
    --cloud "${CLOUD_PROVIDER}" \
    --region "${REGION}" \
    --nodes 3 \
    --plan DEDICATED

echo "✓ Waiting for cluster '${CLUSTER_NAME}' to reach READY state..."
while true; do
    STATUS=$(ccloud cluster describe "${CLUSTER_NAME}" --format json | jq -r '.status // .state')
    echo "  Current status: ${STATUS}"
    if [[ "${STATUS}" == "READY" || "${STATUS}" == "RUNNING" ]]; then
        break
    fi
    sleep 10
done

# 3. Create database and apply baseline schema
echo "✓ Creating database '${DATABASE_NAME}' and establishing secure connection string..."
ccloud sql "${CLUSTER_NAME}" --database defaultdb --command "CREATE DATABASE IF NOT EXISTS ${DATABASE_NAME};"

echo "✓ Applying authoritative versioned migrations (coordinator/migrations/*.sql)..."
for migration in coordinator/migrations/*.sql; do
    echo "  Applying $(basename "${migration}")..."
    ccloud sql "${CLUSTER_NAME}" --database "${DATABASE_NAME}" < "${migration}"
done

# 4. Prepare and inject deployment credentials for MCP Audit role & Changefeed
echo "✓ Validating environment configuration for MCP role and changefeed..."
export MCP_AUDIT_PASSWORD="${MCP_AUDIT_PASSWORD:-$(openssl rand -base64 24)}"
export COORDINATOR_WEBHOOK_URL="${COORDINATOR_WEBHOOK_URL:-}"
export CHANGEFEED_WEBHOOK_SECRET="${CHANGEFEED_WEBHOOK_SECRET:-}"

if [[ -z "${COORDINATOR_WEBHOOK_URL}" || "${COORDINATOR_WEBHOOK_URL}" == *".internal"* ]]; then
    echo "⚠️ COORDINATOR_WEBHOOK_URL is not configured with a public HTTPS ingress."
    echo "   Provisioning MCP audit role only. Changefeed creation deferred until webhook ingress is live."
    envsubst < infra/ccloud/provision_changefeed.sql | grep -v "CREATE CHANGEFEED" | ccloud sql "${CLUSTER_NAME}" --database "${DATABASE_NAME}"
else
    echo "✓ Applying full provisioning script with authenticated changefeed sink..."
    envsubst < infra/ccloud/provision_changefeed.sql | ccloud sql "${CLUSTER_NAME}" --database "${DATABASE_NAME}"
fi

echo "======================================================================"
echo "✅ CodeClaim CockroachDB Cloud cluster successfully provisioned!"
echo "Database: ${DATABASE_NAME}"
echo "MCP Audit Role: mcp_audit_agent (Read-Only Cluster Scoped)"
echo "MCP Audit Password has been configured via environment."
echo "======================================================================"
