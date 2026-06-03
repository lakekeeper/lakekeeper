CREATE TABLE idempotency_record (
    idempotency_key     UUID            NOT NULL,
    warehouse_id        UUID            NOT NULL REFERENCES warehouse(warehouse_id) ON DELETE CASCADE,

    -- Reuses existing api_endpoints enum (sqlx-mapped via EndpointFlat)
    operation           api_endpoints   NOT NULL,

    -- HTTP status code of the finalized response (200, 201, 204).
    -- Always set: the key is inserted atomically inside the mutation transaction,
    -- so it only exists if the mutation committed successfully.
    http_status         INTEGER         NOT NULL,

    created_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),

    PRIMARY KEY (warehouse_id, idempotency_key),

    -- Only mutation endpoints are valid for idempotency
    CONSTRAINT idempotency_operation_check CHECK (operation IN (
        'catalog-v1-create-namespace',
        'catalog-v1-update-namespace-properties',
        'catalog-v1-drop-namespace',
        'catalog-v1-create-table',
        'catalog-v1-update-table',
        'catalog-v1-drop-table',
        'catalog-v1-rename-table',
        'catalog-v1-register-table',
        'catalog-v1-create-view',
        'catalog-v1-replace-view',
        'catalog-v1-drop-view',
        'catalog-v1-rename-view',
        'catalog-v1-commit-transaction'
    ))
);

-- Index for cleanup (expire old records)
CREATE INDEX idx_idempotency_created_at ON idempotency_record (created_at);
