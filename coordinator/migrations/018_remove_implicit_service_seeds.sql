-- ============================================================================
-- CodeClaim: Migration 018 - Remove Unreferenced Implicit Service Seeds
--
-- This data migration is intentionally conservative. It marks the exact demo
-- rows introduced by migration 012 as MIGRATION_SEED, then removes only seed
-- rows that have no service contracts or harness registrations. Existing
-- dependent data is preserved for operator review and explicit re-onboarding.
-- ============================================================================

UPDATE microservices AS m
SET registration_source = 'MIGRATION_SEED'
WHERE m.registration_source = 'LEGACY_UNKNOWN'
  AND (
      (m.service_name = 'billing-service' AND m.repository_path = 'repos/billing-service')
      OR (m.service_name = 'orders-service' AND m.repository_path = 'repos/orders-service')
  )
  AND NOT EXISTS (
      SELECT 1
      FROM coordinator_outbox AS o
      WHERE o.aggregate_type = 'MICROSERVICE'
        AND o.aggregate_id = m.service_id
        AND o.event_type = 'SERVICE_ONBOARDED'
  );

DELETE FROM microservices AS m
WHERE m.registration_source = 'MIGRATION_SEED'
  AND NOT EXISTS (
      SELECT 1 FROM service_contracts AS c
      WHERE c.service_name = m.service_name
  )
  AND NOT EXISTS (
      SELECT 1 FROM harness_registrations AS h
      WHERE h.service_name = m.service_name
  );
