-- ==============================================================================
-- CodeClaim: Migration 007 - Confirmed exact HTTP/JSON consumer dependencies
-- ==============================================================================
-- Forward-only. Legacy dependency rows remain historical evidence but cannot satisfy
-- the exact-interface gate for new compatibility work.

CREATE TABLE IF NOT EXISTS http_interface_dependencies (
    dependency_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    provider_service STRING NOT NULL,
    consumer_service STRING NOT NULL,
    contract_id UUID NOT NULL REFERENCES service_contracts(contract_id),
    assumed_provider_revision INT8 NOT NULL,
    http_method STRING NOT NULL,
    endpoint_path STRING NOT NULL,
    path_parameters JSONB NOT NULL,
    query_parameters JSONB NOT NULL,
    declared_headers JSONB NOT NULL,
    request_body_schema JSONB NOT NULL,
    response_schemas JSONB NOT NULL,
    consumer_repository STRING NOT NULL,
    consumer_source_file STRING NOT NULL,
    consumer_source_evidence JSONB NOT NULL,
    confirmation_status STRING NOT NULL DEFAULT 'DECLARED',
    confirmed_by STRING,
    confirmed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT exact_http_dependency_method CHECK (http_method IN ('GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HEAD', 'OPTIONS')),
    CONSTRAINT exact_http_dependency_path CHECK (endpoint_path LIKE '/%'),
    CONSTRAINT exact_http_dependency_revision CHECK (assumed_provider_revision > 0),
    CONSTRAINT exact_http_dependency_confirmation CHECK (confirmation_status IN ('DECLARED', 'CONFIRMED', 'REJECTED')),
    CONSTRAINT confirmed_http_dependency_has_actor CHECK (
        confirmation_status <> 'CONFIRMED' OR (confirmed_by IS NOT NULL AND confirmed_at IS NOT NULL)
    ),
    CONSTRAINT uq_exact_http_dependency UNIQUE (
        consumer_service, provider_service, contract_id, assumed_provider_revision,
        consumer_repository, consumer_source_file
    ),
    CONSTRAINT uq_http_interface_dependency_binding UNIQUE (
        dependency_id, contract_id, assumed_provider_revision
    )
);

CREATE INDEX IF NOT EXISTS idx_http_interface_dependencies_provider_confirmed
    ON http_interface_dependencies (contract_id, confirmation_status, consumer_service);

ALTER TABLE task_contract_dependencies
    ADD COLUMN IF NOT EXISTS interface_dependency_id UUID REFERENCES http_interface_dependencies(dependency_id);

ALTER TABLE task_contract_dependencies
    ADD CONSTRAINT task_dependency_exact_http_binding
    FOREIGN KEY (interface_dependency_id, contract_id, assumed_revision)
    REFERENCES http_interface_dependencies (dependency_id, contract_id, assumed_provider_revision);

CREATE INDEX IF NOT EXISTS idx_task_contract_dependencies_interface
    ON task_contract_dependencies (interface_dependency_id)
    WHERE interface_dependency_id IS NOT NULL;
