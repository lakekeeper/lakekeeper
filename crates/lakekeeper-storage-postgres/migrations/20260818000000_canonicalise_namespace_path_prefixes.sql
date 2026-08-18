-- Repair namespace path prefixes that were stored with the caller's casing.
--
-- `create_namespace` resolved a namespace's parent under the `case_insensitive` ICU collation but
-- inserted the caller's full path verbatim, so creating `a/b/c` under a parent stored as `a/B`
-- stored the child as {a,b,c} while its parent row holds {a,B}. Nothing in the database is broken
-- by that -- the parent id is right and every SQL comparison is collated -- but the namespace cache
-- compares a child's path minus its leaf against its parent's ident byte-wise (`is_parent_ident`),
-- so such a row and every descendant of it can never be served from cache: each lookup
-- invalidates, reloads the same bytes and fails the same comparison. Both write paths now take the
-- ancestor segments from the locked parent row, so no new rows can drift; this repairs the ones
-- already stored.
--
-- One statement per depth, ascending, rather than one recursive statement. That is a performance
-- requirement, not a style choice. The natural recursive form joins on
-- `child.namespace_name[1:parent.depth] = parent.stored`, whose left operand references columns
-- from *both* relations, so Postgres can never use it as a join key -- it demotes to a per-row
-- `Join Filter` over the cross product of each depth level with the next, which is quadratic in
-- the number of namespaces. Measured on this schema with nothing to repair: 4.4 s at 28k
-- namespaces, 18.6 s at 66k (2.35x the rows, 4.2x the time). Migrations run inside one
-- transaction under `SET LOCAL statement_timeout = '60min'` while holding the advisory lock, and
-- `serve` waits for them, so at a few hundred thousand namespaces in one warehouse that statement
-- would abort the upgrade and the server would not start.
--
-- Pinning the depth to a literal per statement makes the slice bound constant, so the left operand
-- references only the child relation and the planner gets a real join key:
-- `Hash Cond: ((c.warehouse_id = p.warehouse_id) AND (c.namespace_name[1:2] = p.namespace_name))`.
-- Same fixture, same result: 79 ms for the whole loop against 18.6 s -- and the gap widens
-- quadratically. This is the same non-indexable-slice trap that `move_namespace`'s `has_children`
-- guard already documents.
--
-- Ascending order is what makes it correct: each statement reads parents one level up that the
-- previous statement has already canonicalised, so a fix at depth 2 propagates into depth 3 and on
-- down. A single pass keyed on the pre-statement snapshot would repair {a,b,c} and leave
-- {a,b,c,D} still mismatched against the new {a,B,c}.
--
-- Bounded by the deepest row actually present rather than by MAX_NAMESPACE_DEPTH, so rows that
-- predate that limit -- or were written while it was higher -- are still repaired.
--
-- Collision-free by construction: every rewritten value is collation-equal to the value it
-- replaces, and `unique_namespace_per_warehouse` is over the `case_insensitive` collation, so no
-- other row can already hold the target path.
--
-- The `ON UPDATE CASCADE` on the tabular foreign key propagates the new spelling to
-- `tabular.tabular_namespace_name` automatically.
--
-- `version` and `updated_at` are deliberately left alone. The
-- `set_updated_at_and_increment_version` trigger's WHEN clause compares `namespace_name`, which is
-- `text[] collate "case_insensitive"`, so a case-only rewrite compares NOT DISTINCT and the trigger
-- does not fire. Replicas do not need the bump: an affected row could never be served from cache in
-- the first place, and `namespace_cache_insert` accepts an equal version, so the first lookup after
-- this migration replaces the stale entry.
--
-- Rows whose ancestor row is missing are left untouched (the loop starts at depth 2 and joins to an
-- existing parent). Re-running this migration is a no-op.
DO $$
DECLARE
    d int;
    max_depth int;
BEGIN
    SELECT max(depth) INTO max_depth FROM namespace;

    FOR d IN 2..coalesce(max_depth, 1) LOOP
        -- `format` inlines the depth so the slice bounds are literals and the join stays hashable;
        -- passing it as a parameter would leave the planner a generic plan.
        EXECUTE format($stmt$
            UPDATE namespace c
            SET namespace_name = p.namespace_name || c.namespace_name[%1$s:]
            FROM namespace p
            WHERE p.warehouse_id = c.warehouse_id
              AND c.depth = %1$s
              AND p.depth = %1$s - 1
              -- Collated equality: this is how the parent row is found, case-insensitively.
              AND p.namespace_name = c.namespace_name[1:%1$s - 1]
              -- Byte inequality: only rows whose stored prefix actually differs are rewritten,
              -- which is what makes the statement idempotent. On the collated column the two
              -- compare equal, so this must force the C collation.
              AND (c.namespace_name[1:%1$s - 1]::text) COLLATE "C"
                  <> (p.namespace_name::text) COLLATE "C"
        $stmt$, d);
    END LOOP;
END $$;
