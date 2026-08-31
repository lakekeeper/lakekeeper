-- Serves the descendant half of `ensure_location_available`, which without it
-- scans every tabular in the warehouse on each create. `text_pattern_ops`
-- because that check compares bytes (`~>=~`, `~<~`) to read locations literally,
-- and only an index in this operator class serves those. Reachable only through
-- that comparison -- keep the two together.
CREATE INDEX tabular_warehouse_fs_location_pattern_idx ON tabular (
    warehouse_id,
    fs_location text_pattern_ops
);
