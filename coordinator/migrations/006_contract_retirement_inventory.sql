-- ==============================================================================
-- CodeClaim: Migration 006 - Explicit endpoint retirement and inventory review
-- ==============================================================================

ALTER TABLE service_contracts ADD COLUMN IF NOT EXISTS lifecycle_state STRING NOT NULL DEFAULT 'ACTIVE';
ALTER TABLE service_contracts ADD COLUMN IF NOT EXISTS retired_at TIMESTAMPTZ;
ALTER TABLE service_contracts ADD COLUMN IF NOT EXISTS retired_by STRING;
ALTER TABLE service_contracts ADD COLUMN IF NOT EXISTS retirement_reason STRING;
ALTER TABLE service_contracts ADD COLUMN IF NOT EXISTS replacement_contract_id UUID REFERENCES service_contracts(contract_id);

CREATE TABLE IF NOT EXISTS contract_retirements (
    retirement_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    contract_id UUID NOT NULL REFERENCES service_contracts(contract_id),
    retirement_revision INT8 NOT NULL,
    source_commit STRING NOT NULL,
    migration_note STRING NOT NULL,
    replacement_contract_id UUID REFERENCES service_contracts(contract_id),
    retired_by STRING NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(contract_id, retirement_revision)
);

CREATE TABLE IF NOT EXISTS contract_inventory_publications (
    inventory_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    service_name STRING NOT NULL,
    source_commit STRING NOT NULL,
    contract_keys JSONB NOT NULL,
    published_by STRING NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(service_name, source_commit)
);

CREATE TABLE IF NOT EXISTS contract_inventory_findings (
    finding_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    inventory_id UUID NOT NULL REFERENCES contract_inventory_publications(inventory_id) ON DELETE CASCADE,
    contract_id UUID NOT NULL REFERENCES service_contracts(contract_id),
    status STRING NOT NULL DEFAULT 'REVIEW_REQUIRED',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(inventory_id, contract_id)
);
