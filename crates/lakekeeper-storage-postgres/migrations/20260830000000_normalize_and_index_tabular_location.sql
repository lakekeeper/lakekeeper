-- Serves the descendant half of the collision check in
-- `ensure_location_available`, which without this index scans every tabular in the
-- warehouse on each create. `text_pattern_ops` because that half compares bytes
-- (`~>=~`, `~<~`) to read locations literally, and only this operator class serves
-- those -- so the index is unreachable if that comparison ever changes. Keep the
-- two together. (The equality half has been served by
-- `tabular_warehouse_id_location_idx` since 20250904142650; this index happens to
-- serve it too, which makes the older one largely redundant.)
CREATE INDEX tabular_warehouse_fs_location_pattern_idx ON tabular (
    warehouse_id,
    fs_location text_pattern_ops
);

-- A trailing slash on a stored location hides every collision against it: the
-- check compares this column by equality and by byte range, and neither matches
-- one, so a tabular could be created inside such a location.
--
-- `NOT VALID` attaches the constraint without reading the existing rows, which is
-- what lets it live here. Enforcement of new writes starts immediately; the
-- `normalize_fs_location` hook, which runs after this file in the same
-- transaction, trims whatever is already stored and then validates it. The
-- trimming belongs to the hook and not here because a shipped `.sql` file cannot
-- be changed, and a later migration may need to run it again.
ALTER TABLE tabular
    ADD CONSTRAINT tabular_fs_location_no_trailing_slash
    CHECK (fs_location NOT LIKE '%/') NOT VALID;
