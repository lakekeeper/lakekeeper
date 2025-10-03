-- namespace_name was added as FK to tabular, so no need to join namespace anymore
CREATE OR REPLACE VIEW active_tabulars AS
SELECT t.tabular_id,
    t.namespace_id,
    t.name,
    t.typ,
    t.metadata_location,
    t.fs_protocol,
    t.fs_location,
    t.warehouse_id,
    t.tabular_namespace_name as namespace_name
   FROM tabular t
     JOIN warehouse w ON t.warehouse_id = w.warehouse_id AND w.status = 'active'::warehouse_status;
