-- Split "location" column into "protocol" and "location_in_fs" columns
-- to support multiple protocols for the same filesystem, such as "s3a" and "s3".

ALTER TABLE tabular ADD COLUMN fs_protocol TEXT;
ALTER TABLE tabular ADD COLUMN fs_location TEXT;

-- Update new columns with split values from the "location" column
UPDATE tabular
SET fs_protocol = split_part(location, '://', 1),
    fs_location = split_part(location, '://', 2);

-- Ensure new columns are not null
ALTER TABLE tabular ALTER COLUMN fs_protocol SET NOT NULL;
ALTER TABLE tabular ALTER COLUMN fs_location SET NOT NULL;

-- Modify dependant views
DROP VIEW IF EXISTS active_tables;
DROP VIEW IF EXISTS active_views;
DROP VIEW IF EXISTS active_tabulars;

CREATE VIEW active_tabulars
AS SELECT t.tabular_id,
    t.namespace_id,
    t.name,
    t.typ,
    t.metadata_location,
    t.fs_protocol,
    t.fs_location,
    w.warehouse_id,
    n.namespace_name
   FROM tabular t
     JOIN namespace n ON t.namespace_id = n.namespace_id
     JOIN warehouse w ON n.warehouse_id = w.warehouse_id AND w.status = 'active'::warehouse_status;

CREATE VIEW active_tables
AS SELECT tabular_id AS table_id,
    namespace_id,
    name,
    metadata_location,
    fs_protocol,
    fs_location
   FROM active_tabulars t
  WHERE typ = 'table'::tabular_type;

CREATE VIEW active_views
AS SELECT tabular_id AS view_id,
    namespace_id,
    name,
    metadata_location,
    fs_protocol,
    fs_location
   FROM active_tabulars t
  WHERE typ = 'view'::tabular_type;

-- Drop old index - old column is going to be dropped after
-- migration period.
drop index tabular_namespace_id_location_idx;
ALTER TABLE tabular drop column location;

-- Create a new index on the new columns
create index tabular_namespace_id_location_idx on tabular (namespace_id, fs_location);
