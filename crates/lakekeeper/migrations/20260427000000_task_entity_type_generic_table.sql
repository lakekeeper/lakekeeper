-- Allow tasks (e.g. tabular-expiration, tabular-purge) to target generic
-- tables. Mirrors the 'table' / 'view' values on the same enum and the
-- accompanying check constraints introduced in
-- `20251228101923_enable_tasks_on_project_level`.
ALTER TYPE entity_type ADD VALUE IF NOT EXISTS 'generic-table';
