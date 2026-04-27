-- The two new generic-table values were just added to the enum; PG won't let us use
-- them as literals in the same tx, hence the ::text cast.
ALTER TABLE idempotency_record
    DROP CONSTRAINT idempotency_operation_check,
    ADD CONSTRAINT idempotency_operation_check CHECK (
        operation IN (
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
        )
        OR operation::text IN (
            'generic-table-v1-create-generic-table',
            'generic-table-v1-drop-generic-table'
        )
    );
