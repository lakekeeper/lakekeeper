ALTER TABLE warehouse ADD CONSTRAINT unique_warehouse_name_in_project UNIQUE (project_id, warehouse_name);

CREATE INDEX "warehouse_project_id_name" ON warehouse (
    project_id,
    warehouse_name collate "case_insensitive"
);
