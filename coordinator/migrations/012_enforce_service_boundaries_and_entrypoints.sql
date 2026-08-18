-- ==============================================================================
-- CodeClaim: Migration 012 - Service Boundaries, Entrypoints & Harness Foreign Keys
-- ==============================================================================

-- 1. Add application entrypoint metadata to microservices
ALTER TABLE microservices ADD COLUMN IF NOT EXISTS entrypoint_module STRING NOT NULL DEFAULT 'main';
ALTER TABLE microservices ADD COLUMN IF NOT EXISTS entrypoint_app STRING NOT NULL DEFAULT 'app';

-- 2. Seed authoritative microservice registrations
INSERT INTO microservices (service_name, repository_path)
VALUES 
    ('billing-service', 'repos/billing-service'),
    ('orders-service', 'repos/orders-service')
ON CONFLICT (service_name) DO UPDATE SET 
    repository_path = EXCLUDED.repository_path;

-- 3. Enforce foreign key constraints from harness_registrations to microservices
ALTER TABLE harness_registrations
ADD CONSTRAINT IF NOT EXISTS fk_harness_service_name
FOREIGN KEY (service_name) REFERENCES microservices(service_name) ON DELETE CASCADE;

-- 4. Enforce foreign key constraints from service_contracts to microservices
ALTER TABLE service_contracts
ADD CONSTRAINT IF NOT EXISTS fk_contracts_service_name
FOREIGN KEY (service_name) REFERENCES microservices(service_name) ON DELETE CASCADE;
