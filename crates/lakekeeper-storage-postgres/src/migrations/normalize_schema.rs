use anyhow::Context;
use futures::{FutureExt, future::BoxFuture};
use sqlx::Postgres;

use super::MigrationHook;
use crate::tabular::table::{SchemaFieldBatch, normalized_schema::flatten_schema};

// Flush the accumulated batch once it reaches this many field rows. Bounds statement size /
// memory independent of the 500-row read page — wide or deeply nested schemas emit many field
// rows per schema, so the read page alone is not a safe write cap.
const FIELD_FLUSH_THRESHOLD: usize = 10_000;

pub(crate) struct NormalizeSchemaHook;

impl MigrationHook for NormalizeSchemaHook {
    fn apply<'c>(
        &self,
        trx: &'c mut sqlx::Transaction<'_, Postgres>,
    ) -> BoxFuture<'c, anyhow::Result<()>> {
        run(trx).boxed()
    }

    fn name(&self) -> &'static str {
        "normalize_schema"
    }

    fn version() -> i64
    where
        Self: Sized,
    {
        20_260_625_000_000
    }
}

async fn run(txn: &mut sqlx::Transaction<'_, Postgres>) -> anyhow::Result<()> {
    // Cap every lock wait (below: two SHARE LOCKs, two ALTER DROP NOT NULL) so a busy catalog fails
    // fast and the migration rolls back to retry next boot, rather than queueing behind an unbounded
    // wait. SET LOCAL reverts at txn end. The backfills take no contended lock, so 5s never clips
    // them — they are bounded only by the migrator's per-statement timeout.
    sqlx::query("SET LOCAL lock_timeout = '5s'")
        .execute(&mut **txn)
        .await?;
    // Backfill both under SHARE locks first so no in-flight JSONB write is missed; defer the
    // read-blocking DROP NOT NULLs to the tail so that window is brief. table_schema goes last.
    sqlx::query("LOCK TABLE table_schema IN SHARE MODE")
        .execute(&mut **txn)
        .await
        .context(
            "acquire SHARE lock on table_schema for schema backfill \
             (5s lock_timeout; migration retries on the next boot if writers are active)",
        )?;
    backfill(txn).await?;
    sqlx::query("LOCK TABLE view_schema IN SHARE MODE")
        .execute(&mut **txn)
        .await
        .context(
            "acquire SHARE lock on view_schema for schema backfill \
             (5s lock_timeout; migration retries on the next boot if writers are active)",
        )?;
    backfill_view_schemas(txn).await?;

    // Reject any write setting a non-null `schema` (an old-pod write); new NULL anchors, and blanking
    // an existing `schema` to NULL, are both allowed so a later migration can clear the column before
    // dropping it. Shared by both freeze triggers below.
    sqlx::query(
        r#"CREATE FUNCTION reject_schema_write() RETURNS trigger LANGUAGE plpgsql AS $f$
           BEGIN
             IF (TG_OP = 'INSERT' AND NEW.schema IS NOT NULL)
                OR (TG_OP = 'UPDATE' AND NEW.schema IS NOT NULL
                    AND NEW.schema IS DISTINCT FROM OLD.schema) THEN
               RAISE EXCEPTION 'schema JSONB writes are frozen after the normalized-schema migration'
                 USING ERRCODE = 'object_not_in_prerequisite_state';
             END IF;
             RETURN NEW;
           END $f$;"#,
    )
    .execute(&mut **txn)
    .await?;

    // Views: allow NULL anchors (drop NOT NULL) then install the freeze trigger.
    sqlx::query("ALTER TABLE view_schema ALTER COLUMN schema DROP NOT NULL")
        .execute(&mut **txn)
        .await
        .context(
            "drop NOT NULL on view_schema.schema \
             (5s lock_timeout; ACCESS EXCLUSIVE contends with reads; migration retries on the next boot)",
        )?;
    sqlx::query(
        "CREATE TRIGGER view_schema_freeze_jsonb BEFORE INSERT OR UPDATE ON view_schema
         FOR EACH ROW EXECUTE FUNCTION reject_schema_write()",
    )
    .execute(&mut **txn)
    .await?;

    // table_schema last: its ACCESS EXCLUSIVE (which blocks reads) is held for the shortest window
    // before commit, after all backfill work — table and view — is done.
    sqlx::query("ALTER TABLE table_schema ALTER COLUMN schema DROP NOT NULL")
        .execute(&mut **txn)
        .await
        .context(
            "drop NOT NULL on table_schema.schema \
             (5s lock_timeout; ACCESS EXCLUSIVE contends with reads; migration retries on the next boot)",
        )?;
    sqlx::query(
        "CREATE TRIGGER table_schema_freeze_jsonb BEFORE INSERT OR UPDATE ON table_schema
         FOR EACH ROW EXECUTE FUNCTION reject_schema_write()",
    )
    .execute(&mut **txn)
    .await?;
    Ok(())
}

pub(crate) async fn backfill(txn: &mut sqlx::Transaction<'_, Postgres>) -> anyhow::Result<()> {
    const BATCH: i64 = 500;
    let (mut last_wh, mut last_tbl, mut last_sid) =
        (uuid::Uuid::nil(), uuid::Uuid::nil(), i32::MIN);
    let mut batch = SchemaFieldBatch::default();
    // Progress logging: a single-transaction backfill of a large catalog runs for minutes;
    // emit a throttled INFO line per flush so operators can see it advancing.
    let started = std::time::Instant::now();
    let (mut schemas_done, mut fields_done): (u64, u64) = (0, 0);
    loop {
        // Keyset pagination (bounded memory) — one page at a time. Fetch the legacy JSONB as a raw
        // Value and deserialize per row below, so a single corrupt blob names its
        // (warehouse, table, schema) instead of aborting the migration with a context-free error.
        let rows = sqlx::query!(
            r#"SELECT warehouse_id, table_id, schema_id,
                      schema as "schema!: sqlx::types::Json<serde_json::Value>"
               FROM table_schema
               WHERE schema IS NOT NULL
                 AND (warehouse_id, table_id, schema_id) > ($1, $2, $3)
               ORDER BY warehouse_id, table_id, schema_id
               LIMIT $4"#,
            last_wh,
            last_tbl,
            last_sid,
            BATCH
        )
        .fetch_all(&mut **txn)
        .await?;
        let Some(l) = rows.last() else { break };
        (last_wh, last_tbl, last_sid) = (l.warehouse_id, l.table_id, l.schema_id);
        for r in rows {
            let (wh, tbl, sid, schema_json) = (r.warehouse_id, r.table_id, r.schema_id, r.schema.0);
            let schema: iceberg::spec::Schema = serde_json::from_value(schema_json)
                .map_err(|e| anyhow::anyhow!("deserialize {wh}/{tbl} schema {sid}: {e}"))?;
            // The anchor's schema_id column is the store's identity (table_current_schema references
            // it); the JSONB carries its own embedded schema-id. Lakekeeper always writes them equal,
            // so a mismatch is corrupt source data — fail loud before this migration drops the JSONB.
            if schema.schema_id() != sid {
                anyhow::bail!(
                    "schema-id mismatch for {wh}/{tbl}: anchor row schema_id={sid}, embedded schema-id={}",
                    schema.schema_id()
                );
            }
            let flat = flatten_schema(&schema)
                .map_err(|e| anyhow::anyhow!("flatten {wh}/{tbl} schema {sid}: {e}"))?;
            batch.push_schema(wh, tbl, sid, &flat);
            schemas_done += 1;
            fields_done += flat.len() as u64;
            if batch.field_count() >= FIELD_FLUSH_THRESHOLD {
                batch
                    .flush(txn)
                    .await
                    .map_err(|e| anyhow::anyhow!("write schema_field: {e}"))?;
                tracing::info!(
                    schemas = schemas_done,
                    field_rows = fields_done,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "table schema backfill progress"
                );
            }
        }
    }
    batch
        .flush(txn)
        .await
        .map_err(|e| anyhow::anyhow!("write schema_field: {e}"))?;
    if schemas_done > 0 {
        tracing::info!(
            schemas = schemas_done,
            field_rows = fields_done,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "table schema backfill complete"
        );
    }
    Ok(())
}

pub(crate) async fn backfill_view_schemas(
    txn: &mut sqlx::Transaction<'_, Postgres>,
) -> anyhow::Result<()> {
    const BATCH: i64 = 500;
    let (mut last_wh, mut last_view, mut last_sid) =
        (uuid::Uuid::nil(), uuid::Uuid::nil(), i32::MIN);
    let mut batch = SchemaFieldBatch::default();
    let started = std::time::Instant::now();
    let (mut schemas_done, mut fields_done): (u64, u64) = (0, 0);
    loop {
        // Keyset pagination (bounded memory). Raw-Value fetch + per-row deserialize so a corrupt
        // blob names its (warehouse, view, schema).
        let rows = sqlx::query!(
            r#"SELECT warehouse_id, view_id, schema_id,
                      schema as "schema!: sqlx::types::Json<serde_json::Value>"
               FROM view_schema
               WHERE schema IS NOT NULL
                 AND (warehouse_id, view_id, schema_id) > ($1, $2, $3)
               ORDER BY warehouse_id, view_id, schema_id
               LIMIT $4"#,
            last_wh,
            last_view,
            last_sid,
            BATCH
        )
        .fetch_all(&mut **txn)
        .await?;
        let Some(l) = rows.last() else { break };
        (last_wh, last_view, last_sid) = (l.warehouse_id, l.view_id, l.schema_id);
        for r in rows {
            let (wh, view, sid, schema_json) = (r.warehouse_id, r.view_id, r.schema_id, r.schema.0);
            let schema: iceberg::spec::Schema = serde_json::from_value(schema_json)
                .map_err(|e| anyhow::anyhow!("deserialize view {wh}/{view} schema {sid}: {e}"))?;
            // Reject a blob whose embedded schema-id disagrees with its anchor row.
            if schema.schema_id() != sid {
                anyhow::bail!(
                    "schema-id mismatch for view {wh}/{view}: anchor row schema_id={sid}, embedded schema-id={}",
                    schema.schema_id()
                );
            }
            let flat = flatten_schema(&schema)
                .map_err(|e| anyhow::anyhow!("flatten view {wh}/{view} schema {sid}: {e}"))?;
            batch.push_schema(wh, view, sid, &flat);
            schemas_done += 1;
            fields_done += flat.len() as u64;
            if batch.field_count() >= FIELD_FLUSH_THRESHOLD {
                batch
                    .flush(txn)
                    .await
                    .map_err(|e| anyhow::anyhow!("write schema_field view: {e}"))?;
                tracing::info!(
                    schemas = schemas_done,
                    field_rows = fields_done,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "view schema backfill progress"
                );
            }
        }
    }
    batch
        .flush(txn)
        .await
        .map_err(|e| anyhow::anyhow!("write schema_field view: {e}"))?;
    if schemas_done > 0 {
        tracing::info!(
            schemas = schemas_done,
            field_rows = fields_done,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "view schema backfill complete"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use iceberg::{
        NamespaceIdent,
        spec::{NestedField, PrimitiveType, Schema, Type as IcebergType},
    };
    use lakekeeper_io::Location;
    use serde_json::json;
    use sqlx::PgPool;
    use uuid::Uuid;

    use super::NormalizeSchemaHook;
    use crate::{
        CatalogState, migrations::MigrationHook, namespace::tests::initialize_namespace,
        tabular::view::load_view, warehouse::test::initialize_warehouse,
    };

    // ── E. View backfill ─────────────────────────────────────────────────────

    /// Seed a legacy `view_schema` JSONB row (no schema_field yet), run the backfill,
    /// assert schema_field is populated and load_view reconstructs the schema exactly.
    #[sqlx::test]
    async fn view_backfill_reproduces_schema_from_jsonb(pool: PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, wh) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["ns_backfill".to_string()]).unwrap();
        initialize_namespace(state.clone(), wh, &namespace, None).await;
        let namespace_id =
            crate::tabular::table::tests::get_namespace_id(state.clone(), wh, &namespace).await;

        let view_uuid = Uuid::now_v7();
        let location = format!("s3://bucket/view_{view_uuid}/data")
            .parse::<Location>()
            .unwrap();
        let meta_loc: Location = format!("s3://bucket/view_{view_uuid}/meta/v1.json")
            .parse()
            .unwrap();

        // The view_request fixture: schema_id=0 (fields 0,1), schema_id=1 (field 0).
        let request = crate::tabular::view::tests::view_request(Some(view_uuid), &location);
        let mut tx = pool.begin().await.unwrap();
        crate::tabular::view::create_view(
            wh,
            namespace_id,
            &meta_loc,
            &mut tx,
            "bf_view",
            &request,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();

        // Wipe schema_field rows (simulate pre-migration state) and restore JSONB on view_schema.
        let mut tx = pool.begin().await.unwrap();
        sqlx::query("DELETE FROM schema_field WHERE warehouse_id=$1 AND tabular_id=$2")
            .bind(*wh)
            .bind(view_uuid)
            .execute(&mut *tx)
            .await
            .unwrap();
        // Restore the JSONB for each schema version so the backfill can read it.
        for s in request.schemas_iter() {
            let jsonb = serde_json::to_value(s.as_ref()).unwrap();
            sqlx::query(
                "UPDATE view_schema SET schema=$3 WHERE warehouse_id=$1 AND view_id=$2 AND schema_id=$4",
            )
            .bind(*wh)
            .bind(view_uuid)
            .bind(&jsonb)
            .bind(s.schema_id())
            .execute(&mut *tx)
            .await
            .unwrap();
        }
        // Run only the view backfill (not the table one, to keep scope narrow).
        super::backfill_view_schemas(&mut tx).await.unwrap();
        tx.commit().await.unwrap();

        // schema_field must now exist.
        let sf_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM schema_field WHERE warehouse_id=$1 AND tabular_id=$2",
        )
        .bind(*wh)
        .bind(view_uuid)
        .fetch_one(&pool)
        .await
        .unwrap();
        // view_request fixture: schema 1 has 1 field, schema 0 has 2 fields → 3 rows.
        assert_eq!(
            sf_count, 3,
            "backfill must reproduce exactly the fixture's schema_field rows"
        );

        // load_view must reconstruct both schemas exactly.
        let mut tx = pool.begin().await.unwrap();
        let loaded = load_view(wh, view_uuid.into(), false, &mut tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        // ViewMetadata is Eq with HashMap-backed versions+schemas, so struct equality is
        // order-insensitive; comparing serialized JSON would be flaky on HashMap iteration order.
        assert_eq!(
            loaded.metadata.as_ref(),
            &request,
            "load_view after backfill must equal original metadata"
        );
    }

    // ── F. View freeze ───────────────────────────────────────────────────────

    /// After NormalizeSchemaHook runs, a NULL-anchor view_schema INSERT is allowed
    /// but a non-null `schema` INSERT is rejected with SQLSTATE 55000.
    #[sqlx::test]
    async fn view_freeze_blocks_jsonb_but_allows_null_anchor(pool: PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, wh) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["ns_freeze".to_string()]).unwrap();
        initialize_namespace(state.clone(), wh, &namespace, None).await;
        let namespace_id =
            crate::tabular::table::tests::get_namespace_id(state.clone(), wh, &namespace).await;

        let view_uuid = Uuid::now_v7();
        let location = format!("s3://bucket/view_{view_uuid}/data")
            .parse::<Location>()
            .unwrap();
        let meta_loc: Location = format!("s3://bucket/view_{view_uuid}/meta/v1.json")
            .parse()
            .unwrap();

        let request = crate::tabular::view::tests::view_request(Some(view_uuid), &location);
        let mut tx = pool.begin().await.unwrap();
        crate::tabular::view::create_view(
            wh,
            namespace_id,
            &meta_loc,
            &mut tx,
            "freeze_view",
            &request,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();

        // Seed a legacy JSONB schema row (pre-freeze, column still nullable) so we can verify below
        // that the freeze permits blanking it back to NULL.
        sqlx::query("INSERT INTO view_schema(warehouse_id, view_id, schema_id, schema) VALUES ($1,$2,$3,$4)")
            .bind(*wh)
            .bind(view_uuid)
            .bind(500_i32)
            .bind(serde_json::json!({
                "type": "struct", "schema-id": 500,
                "fields": [{"id": 1, "name": "c", "required": false, "type": "int"}]
            }))
            .execute(&pool)
            .await
            .expect("pre-freeze JSONB insert should be allowed");

        // Install the freeze (drops NOT NULL + installs trigger).
        let mut tx = pool.begin().await.unwrap();
        NormalizeSchemaHook.apply(&mut tx).await.unwrap();
        tx.commit().await.unwrap();

        // NULL-anchor insert is allowed.
        sqlx::query("INSERT INTO view_schema(warehouse_id, view_id, schema_id) VALUES ($1,$2,$3)")
            .bind(*wh)
            .bind(view_uuid)
            .bind(998_i32)
            .execute(&pool)
            .await
            .expect("NULL-schema anchor insert on view_schema must be allowed under freeze");

        // JSONB schema write is rejected with SQLSTATE 55000.
        let err = sqlx::query(
            "INSERT INTO view_schema(warehouse_id, view_id, schema_id, schema) VALUES ($1,$2,$3,$4)",
        )
        .bind(*wh)
        .bind(view_uuid)
        .bind(999_i32)
        .bind(serde_json::json!({"type":"struct","schema-id":999,"fields":[]}))
        .execute(&pool)
        .await
        .unwrap_err();

        assert_eq!(
            err.as_database_error().and_then(|e| e.code()).as_deref(),
            Some("55000"),
            "legacy JSONB view_schema write must fail with SQLSTATE 55000"
        );

        // Blanking an existing JSONB schema back to NULL is allowed (so a later cleanup migration can
        // clear the column before dropping it) — the freeze rejects only writes that SET a non-null.
        let affected = sqlx::query(
            "UPDATE view_schema SET schema = NULL WHERE warehouse_id = $1 AND view_id = $2 AND schema_id = $3",
        )
        .bind(*wh)
        .bind(view_uuid)
        .bind(500_i32)
        .execute(&pool)
        .await
        .expect("blanking a JSONB schema to NULL must be allowed under the freeze")
        .rows_affected();
        assert_eq!(
            affected, 1,
            "expected exactly one JSONB schema row blanked to NULL"
        );
    }

    // ── G. >threshold batched backfill ───────────────────────────────────────

    /// Seed more than FIELD_FLUSH_THRESHOLD (10_000) total field rows across
    /// multiple table_schema entries, run the backfill hook, assert every field
    /// round-trips. This exercises the mid-loop flush + SchemaFieldBatch array-clear
    /// reuse path.
    ///
    /// Strategy: 1 seed field + 100 schemas × 100 fields = 10_001 field rows (> FIELD_FLUSH_THRESHOLD).
    #[sqlx::test]
    async fn batched_backfill_exceeds_flush_threshold(pool: PgPool) {
        // We need a real table row as a FK anchor. Use `create_table_with_schema` to
        // create one seed table, then insert extra schema versions directly.
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, wh) = initialize_warehouse(state.clone(), None, None, None, true).await;

        let seed_schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "seed", IcebergType::Primitive(PrimitiveType::Long))
                    .into(),
            ])
            .build()
            .unwrap();
        let (table_id, _) =
            crate::tabular::table::tests::create_table_with_schema(state.clone(), wh, seed_schema)
                .await;

        // Wipe schema_field rows for the seed schema so the backfill re-populates them cleanly.
        let mut tx = pool.begin().await.unwrap();
        sqlx::query("DELETE FROM schema_field WHERE warehouse_id=$1 AND tabular_id=$2")
            .bind(*wh)
            .bind(*table_id)
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        // Restore the seed schema's JSONB and add 100 extra schema versions, each with 100 fields.
        // Total field rows: seed(1) + 100×100 = 10_001, which exceeds FIELD_FLUSH_THRESHOLD=10_000.
        let seed_jsonb = serde_json::json!({"type":"struct","schema-id":0,"fields":[
            {"id":1,"name":"seed","required":true,"type":"long"}
        ]});
        let mut tx = pool.begin().await.unwrap();
        sqlx::query("UPDATE table_schema SET schema=$3 WHERE warehouse_id=$1 AND table_id=$2 AND schema_id=0")
            .bind(*wh)
            .bind(*table_id)
            .bind(&seed_jsonb)
            .execute(&mut *tx)
            .await
            .unwrap();

        for schema_ver in 1_i32..=100 {
            let fields: Vec<serde_json::Value> = (0_i32..100)
                .map(|i| {
                    let fid = schema_ver * 100 + i + 2; // unique field_id across all schemas
                    json!({"id": fid, "name": format!("f{fid}"), "required": false, "type": "long"})
                })
                .collect();
            let schema_jsonb = json!({
                "type": "struct",
                "schema-id": schema_ver,
                "fields": fields
            });
            sqlx::query(
                "INSERT INTO table_schema(warehouse_id, table_id, schema_id, schema) VALUES ($1,$2,$3,$4)",
            )
            .bind(*wh)
            .bind(*table_id)
            .bind(schema_ver)
            .bind(&schema_jsonb)
            .execute(&mut *tx)
            .await
            .unwrap();
        }
        tx.commit().await.unwrap();

        // Run the backfill. This must exercise at least one mid-loop flush.
        let mut tx = pool.begin().await.unwrap();
        super::backfill(&mut tx).await.unwrap();
        tx.commit().await.unwrap();

        // schema_field must have exactly 10_001 rows: 1 (seed) + 100 schemas × 100 fields.
        let total: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM schema_field WHERE warehouse_id=$1 AND tabular_id=$2",
        )
        .bind(*wh)
        .bind(*table_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(total, 10_001, "backfill must persist all 10_001 field rows");

        // Spot-check: the seed field (field_id=1) round-trips.
        let sf_rows = sqlx::query!(
            r#"SELECT schema_id, field_id, name FROM schema_field
               WHERE warehouse_id=$1 AND tabular_id=$2 AND schema_id=0"#,
            *wh,
            *table_id,
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            sf_rows.len(),
            1,
            "seed schema must have exactly 1 field row"
        );
        assert_eq!(sf_rows[0].field_id, 1);
        assert_eq!(sf_rows[0].name, "seed");

        // Spot-check: schema_ver=100 must have 100 field rows.
        let last_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM schema_field WHERE warehouse_id=$1 AND tabular_id=$2 AND schema_id=100",
        )
        .bind(*wh)
        .bind(*table_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            last_count, 100,
            "last schema version must have 100 field rows"
        );
    }

    // ── G2. schema-id integrity ──────────────────────────────────────────────

    /// A legacy JSONB whose embedded schema-id disagrees with its anchor `schema_id` column is
    /// corrupt source data. The backfill must reject it loud (naming both ids) rather than silently
    /// normalize under the column id, because the migration then drops the JSONB irreversibly.
    #[sqlx::test]
    async fn backfill_rejects_schema_id_mismatch(pool: PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, wh) = initialize_warehouse(state.clone(), None, None, None, true).await;

        let seed_schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "seed", IcebergType::Primitive(PrimitiveType::Long))
                    .into(),
            ])
            .build()
            .unwrap();
        let (table_id, _) =
            crate::tabular::table::tests::create_table_with_schema(state.clone(), wh, seed_schema)
                .await;

        // Pre-migration state, but with a corrupt JSONB whose embedded schema-id (999) disagrees
        // with the anchor row's schema_id column (0).
        let mut tx = pool.begin().await.unwrap();
        sqlx::query("DELETE FROM schema_field WHERE warehouse_id=$1 AND tabular_id=$2")
            .bind(*wh)
            .bind(*table_id)
            .execute(&mut *tx)
            .await
            .unwrap();
        let corrupt = json!({"type":"struct","schema-id":999,"fields":[
            {"id":1,"name":"seed","required":true,"type":"long"}
        ]});
        sqlx::query(
            "UPDATE table_schema SET schema=$3 WHERE warehouse_id=$1 AND table_id=$2 AND schema_id=0",
        )
        .bind(*wh)
        .bind(*table_id)
        .bind(&corrupt)
        .execute(&mut *tx)
        .await
        .unwrap();

        let err = super::backfill(&mut tx).await.unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("schema-id mismatch")
                && msg.contains("schema_id=0")
                && msg.contains("embedded schema-id=999"),
            "expected a contextual schema-id mismatch error naming anchor 0 and embedded 999, got: {msg}"
        );
    }

    // ── H. Benchmark: 100k tables ────────────────────────────────────────────

    /// Benchmark the backfill over ~100k tables with a realistic 15-field schema.
    ///
    /// Marked `#[ignore]` — run explicitly with:
    ///   cargo nextest run -p lakekeeper-storage-postgres bench_backfill_100k --run-ignored all --no-capture
    ///
    /// Seeding uses set-based SQL (generate_series) to avoid 100k round-trips.
    #[ignore]
    #[sqlx::test]
    async fn bench_backfill_100k(pool: PgPool) {
        use std::time::Instant;

        // Surface the backfill's INFO progress logs live (run with --no-capture).
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .try_init();

        const N_TABLES: i64 = 100_000;
        const FIELDS_PER_SCHEMA: usize = 15;

        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, wh) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["ns_bench".to_string()]).unwrap();
        initialize_namespace(state.clone(), wh, &namespace, None).await;
        let namespace_id =
            crate::tabular::table::tests::get_namespace_id(state.clone(), wh, &namespace).await;

        // Build one realistic Iceberg schema with FIELDS_PER_SCHEMA fields:
        // 10 primitives + 1 struct (with 2 inner fields) + 1 list + 1 map + 1 timestamp + 1 binary
        // We serialize it once and reuse the JSON for all 100k rows.
        let schema_json = serde_json::json!({
            "type": "struct",
            "schema-id": 0,
            "fields": [
                {"id": 1,  "name": "id",         "required": true,  "type": "long"},
                {"id": 2,  "name": "name",        "required": false, "type": "string"},
                {"id": 3,  "name": "created_at",  "required": false, "type": "timestamptz"},
                {"id": 4,  "name": "updated_at",  "required": false, "type": "timestamp"},
                {"id": 5,  "name": "amount",      "required": false, "type": "double"},
                {"id": 6,  "name": "count",       "required": false, "type": "int"},
                {"id": 7,  "name": "is_active",   "required": false, "type": "boolean"},
                {"id": 8,  "name": "score",       "required": false, "type": "float"},
                {"id": 9,  "name": "event_date",  "required": false, "type": "date"},
                {"id": 10, "name": "payload",     "required": false, "type": "binary"},
                {"id": 11, "name": "address", "required": false, "type": {
                    "type": "struct",
                    "fields": [
                        {"id": 12, "name": "street", "required": false, "type": "string"},
                        {"id": 13, "name": "city",   "required": false, "type": "string"}
                    ]
                }},
                {"id": 14, "name": "tags", "required": false, "type": {
                    "type": "list",
                    "element-id": 15,
                    "element": "string",
                    "element-required": false
                }},
                {"id": 16, "name": "properties", "required": false, "type": {
                    "type": "map",
                    "key-id": 17,
                    "key": "string",
                    "value-id": 18,
                    "value": "string",
                    "value-required": false
                }},
                {"id": 19, "name": "uuid_col",   "required": false, "type": "uuid"},
                {"id": 20, "name": "decimal_col", "required": false, "type": "decimal(18,6)"}
            ]
        });

        // Seed: bulk-insert N_TABLES rows into tabular + "table" + table_schema using generate_series.
        // Use a single connection (acquire from pool) so the temp table is visible to all queries.
        let seed_start = Instant::now();

        let mut conn = pool.acquire().await.unwrap();

        // Temp table of UUIDs — one per synthetic table. Visible only within this connection.
        sqlx::query("CREATE TEMP TABLE _bench_ids (tabular_id uuid NOT NULL)")
            .execute(&mut *conn)
            .await
            .unwrap();
        sqlx::query("INSERT INTO _bench_ids SELECT gen_random_uuid() FROM generate_series(1, $1)")
            .bind(N_TABLES)
            .execute(&mut *conn)
            .await
            .unwrap();

        // Insert into tabular. Required columns: warehouse_id, tabular_id, namespace_id, name, typ,
        // fs_protocol, fs_location, and tabular_namespace_name (text[], NOT NULL, FK to
        // namespace.namespace_name — looked up below so the FK is satisfied).
        sqlx::query(
            "INSERT INTO tabular (warehouse_id, tabular_id, namespace_id, name, typ, fs_protocol, fs_location, tabular_namespace_name)
             SELECT $1, b.tabular_id, $2, 'bench_tbl_' || b.tabular_id, 'table',
                    's3',
                    'bench-bucket/' || b.tabular_id,
                    n.namespace_name
             FROM _bench_ids b
             CROSS JOIN namespace n
             WHERE n.warehouse_id = $1 AND n.namespace_id = $2",
        )
        .bind(*wh)
        .bind(*namespace_id)
        .execute(&mut *conn)
        .await
        .unwrap();

        // Insert into "table". table_format_version, last_column_id, last_sequence_number,
        // last_updated_ms, last_partition_id, and next_row_id are all NOT NULL.
        sqlx::query(
            "INSERT INTO \"table\" (warehouse_id, table_id, table_format_version, last_column_id, last_sequence_number, last_updated_ms, last_partition_id, next_row_id)
             SELECT $1, b.tabular_id, '2', 20, 0, 0, 999, 0 FROM _bench_ids b",
        )
        .bind(*wh)
        .execute(&mut *conn)
        .await
        .unwrap();

        // Insert into table_schema (warehouse_id, table_id, schema_id, schema).
        sqlx::query(
            "INSERT INTO table_schema (warehouse_id, table_id, schema_id, schema)
             SELECT $1, b.tabular_id, 0, $2::jsonb FROM _bench_ids b",
        )
        .bind(*wh)
        .bind(&schema_json)
        .execute(&mut *conn)
        .await
        .unwrap();

        drop(conn); // return connection to pool; temp table is session-local and goes away

        let seed_elapsed = seed_start.elapsed();

        // Backfill: measure only the backfill call.
        let backfill_start = Instant::now();
        let mut txn = pool.begin().await.unwrap();
        super::backfill(&mut txn).await.unwrap();
        txn.commit().await.unwrap();
        let backfill_elapsed = backfill_start.elapsed();

        // Count resulting schema_field rows.
        let sf_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM schema_field WHERE warehouse_id = $1")
                .bind(*wh)
                .fetch_one(&pool)
                .await
                .unwrap();

        // Expected: N_TABLES × FIELDS_PER_SCHEMA (flat leaf count).
        // The schema has 15 user-level fields but the struct children (12, 13) + list element (15)
        // + map key (17) + map value (18) are also emitted as separate schema_field rows.
        // Total = 20 field IDs in the JSON above.
        let expected_rows = N_TABLES * 20;

        let rows_per_sec = sf_count as f64 / backfill_elapsed.as_secs_f64();
        let schemas_per_sec = N_TABLES as f64 / backfill_elapsed.as_secs_f64();
        let extrap_500k = backfill_elapsed.as_secs_f64() * (500_000.0 / N_TABLES as f64);

        println!();
        println!("=== bench_backfill_100k results ===");
        println!("  N tables:              {N_TABLES}");
        println!(
            "  Fields per schema:     {FIELDS_PER_SCHEMA} top-level ({} total field ids)",
            20
        );
        println!("  schema_field rows:     {sf_count}");
        println!(
            "  Seed duration:         {:.3}s",
            seed_elapsed.as_secs_f64()
        );
        println!(
            "  Backfill duration:     {:.3}s",
            backfill_elapsed.as_secs_f64()
        );
        println!("  Rows/sec:              {rows_per_sec:.0}");
        println!("  Schemas/sec:           {schemas_per_sec:.0}");
        println!("  Extrapolation 500k:    {extrap_500k:.1}s");
        println!("===================================");
        println!();

        assert_eq!(
            sf_count, expected_rows,
            "schema_field count must equal N_TABLES × field_ids_per_schema (got {sf_count}, want {expected_rows})"
        );
    }
}
