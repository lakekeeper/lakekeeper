-- 'generic-table' was just added to the enum; PG won't let us use it as a literal here yet.
ALTER TABLE tabular DROP CONSTRAINT tabular_check;
ALTER TABLE tabular ADD CONSTRAINT tabular_metadata_location_check
    CHECK (typ <> 'view' OR metadata_location IS NOT NULL);
