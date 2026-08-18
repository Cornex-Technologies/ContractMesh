-- ==============================================================================
-- CodeClaim: CockroachDB CDC Changefeed Configuration
-- ==============================================================================
--
-- Creates a transactional changefeed on the `coordinator_outbox` table.
-- When contract mutations and task replan events are committed atomically,
-- CockroachDB streams them to the Coordinator Webhook (`POST /events/cockroach`).
--
-- Deployment Instructions:
-- Replace ${COORDINATOR_ENDPOINT_URL} and ${CHANGEFEED_WEBHOOK_SECRET} with
-- values configured in your coordinator environment.
--
-- Reference: https://www.cockroachlabs.com/docs/stable/create-changefeed

-- Pattern A: Bearer Token Authentication (Recommended for CodeClaim Coordinator)
-- Webhook endpoint validates: Authorization: Bearer <CHANGEFEED_WEBHOOK_SECRET>
--
-- Example SQL invocation on CockroachDB cluster:
CREATE CHANGEFEED FOR TABLE coordinator_outbox
INTO 'webhook-https://${COORDINATOR_HOST}:${COORDINATOR_PORT}/events/cockroach'
WITH
    initial_scan = 'no',
    envelope = 'wrapped',
    updated,
    extra_headers = '{"Authorization": "Bearer ${CHANGEFEED_WEBHOOK_SECRET}"}';

-- Pattern B: Basic Authentication Header (CockroachDB native webhook_auth_header)
-- Webhook endpoint validates: Authorization: Basic <base64(user:secret)>
--
-- CREATE CHANGEFEED FOR TABLE coordinator_outbox
-- INTO 'webhook-https://${COORDINATOR_HOST}:${COORDINATOR_PORT}/events/cockroach'
-- WITH
--     initial_scan = 'no',
--     envelope = 'wrapped',
--     updated,
--     webhook_auth_header = 'Basic ${BASE64_USER_PASSWORD}';
