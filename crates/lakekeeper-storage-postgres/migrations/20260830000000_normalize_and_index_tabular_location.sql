-- The location-collision check reads this column as bytes (`~>=~`, `~<~`) so that
-- locations are compared literally. Only `text_pattern_ops` serves those
-- operators; without this index every create scans the whole warehouse.
CREATE INDEX tabular_warehouse_fs_location_pattern_idx ON tabular (
    warehouse_id,
    fs_location text_pattern_ops
);

-- A stored location ending in a slash matches neither the equality nor the range
-- comparison, so it would hide every collision against it and a tabular could be
-- created inside it.
--
-- `NOT VALID` enforces new writes at once without reading the existing rows. The
-- rows already stored are trimmed and the constraint validated by this
-- migration's data step -- which lives there, not here, because a shipped file
-- can never change and a later migration may need to trim again.
ALTER TABLE tabular
    ADD CONSTRAINT tabular_fs_location_no_trailing_slash
    CHECK (fs_location NOT LIKE '%/') NOT VALID;
