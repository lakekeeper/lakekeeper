-- Rewrite CHECK now that 'generic-table' enum value is committed.
ALTER TABLE tabular DROP CONSTRAINT tabular_check;
ALTER TABLE tabular ADD CONSTRAINT tabular_metadata_location_check
    CHECK (
        (typ = 'view' AND metadata_location IS NOT NULL)
        OR typ = 'table'
        OR typ = 'generic-table'
    );
