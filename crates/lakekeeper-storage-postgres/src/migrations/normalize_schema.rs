use futures::{FutureExt, future::BoxFuture};
use sqlx::Postgres;

use super::MigrationHook;
use crate::tabular::table::{SchemaFieldBatch, normalized_schema::flatten_schema};

// Flush the accumulated batch once it reaches this many field rows. Bounds statement size /
// memory independent of the 500-row read page — wide or deeply nested schemas emit many field
// rows per schema, so the read page alone is not a safe write cap (GUARD 3).
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
    // The FKs created by the DDL already hold SHARE ROW EXCLUSIVE on table_schema for the whole
    // transaction (writes blocked, reads allowed). This explicit lock is intent-only/redundant.
    sqlx::query("LOCK TABLE table_schema IN SHARE MODE")
        .execute(&mut **txn)
        .await?;
    backfill(txn).await?;
    // Allow the normalized write path (which inserts anchors with schema = NULL): drop the legacy
    // column's NOT NULL. This takes ACCESS EXCLUSIVE, but only briefly and *after* the long backfill,
    // so reads stall only for the migration tail.
    sqlx::query("ALTER TABLE table_schema ALTER COLUMN schema DROP NOT NULL")
        .execute(&mut **txn)
        .await?;
    // Freeze legacy JSONB schema writes: reject any write that sets a non-null `schema` (an old-pod
    // write during the brief roll-over) while permitting the new NULL-anchor writes. Rejected writes
    // fail loud (SQLSTATE object_not_in_prerequisite_state) and are retried against a new pod.
    sqlx::query(
        r#"CREATE FUNCTION reject_schema_write() RETURNS trigger LANGUAGE plpgsql AS $f$
           BEGIN
             IF (TG_OP = 'INSERT' AND NEW.schema IS NOT NULL)
                OR (TG_OP = 'UPDATE' AND NEW.schema IS DISTINCT FROM OLD.schema) THEN
               RAISE EXCEPTION 'schema JSONB writes are frozen after the normalized-schema migration'
                 USING ERRCODE = 'object_not_in_prerequisite_state';
             END IF;
             RETURN NEW;
           END $f$;"#,
    )
    .execute(&mut **txn)
    .await?;
    sqlx::query(
        "CREATE TRIGGER table_schema_freeze_jsonb BEFORE INSERT OR UPDATE ON table_schema
         FOR EACH ROW EXECUTE FUNCTION reject_schema_write()",
    )
    .execute(&mut **txn)
    .await?;

    // Same choreography for views: backfill JSONB schemas into schema_field, drop the NOT NULL so
    // the normalized write path can insert NULL anchors, then freeze legacy JSONB writes. Reuses
    // reject_schema_write() created above.
    sqlx::query("LOCK TABLE view_schema IN SHARE MODE")
        .execute(&mut **txn)
        .await?;
    backfill_view_schemas(txn).await?;
    sqlx::query("ALTER TABLE view_schema ALTER COLUMN schema DROP NOT NULL")
        .execute(&mut **txn)
        .await?;
    sqlx::query(
        "CREATE TRIGGER view_schema_freeze_jsonb BEFORE INSERT OR UPDATE ON view_schema
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
    loop {
        // Keyset pagination (bounded memory) — one batch at a time, not the whole table.
        let rows = sqlx::query!(
            r#"SELECT warehouse_id, table_id, schema_id,
                      schema as "schema!: sqlx::types::Json<iceberg::spec::Schema>"
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
        if rows.is_empty() {
            break;
        }
        for r in &rows {
            let schema = &r.schema.0;
            let flat = flatten_schema(schema).map_err(|e| {
                anyhow::anyhow!(
                    "flatten {}/{} schema {}: {e}",
                    r.warehouse_id,
                    r.table_id,
                    r.schema_id
                )
            })?;
            batch.push_schema(r.warehouse_id, r.table_id, r.schema_id, &flat);
            if batch.field_count() >= FIELD_FLUSH_THRESHOLD {
                batch
                    .flush(txn)
                    .await
                    .map_err(|e| anyhow::anyhow!("write schema_field: {e}"))?;
            }
        }
        let l = rows.last().unwrap();
        (last_wh, last_tbl, last_sid) = (l.warehouse_id, l.table_id, l.schema_id);
    }
    batch
        .flush(txn)
        .await
        .map_err(|e| anyhow::anyhow!("write schema_field: {e}"))?;
    Ok(())
}

pub(crate) async fn backfill_view_schemas(
    txn: &mut sqlx::Transaction<'_, Postgres>,
) -> anyhow::Result<()> {
    const BATCH: i64 = 500;
    let (mut last_wh, mut last_view, mut last_sid) =
        (uuid::Uuid::nil(), uuid::Uuid::nil(), i32::MIN);
    let mut batch = SchemaFieldBatch::default();
    loop {
        // Keyset pagination (bounded memory) — one batch at a time, not the whole table.
        let rows = sqlx::query!(
            r#"SELECT warehouse_id, view_id, schema_id,
                      schema as "schema!: sqlx::types::Json<iceberg::spec::Schema>"
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
        if rows.is_empty() {
            break;
        }
        for r in &rows {
            let schema = &r.schema.0;
            let flat = flatten_schema(schema).map_err(|e| {
                anyhow::anyhow!(
                    "flatten view {}/{} schema {}: {e}",
                    r.warehouse_id,
                    r.view_id,
                    r.schema_id
                )
            })?;
            batch.push_schema(r.warehouse_id, r.view_id, r.schema_id, &flat);
            if batch.field_count() >= FIELD_FLUSH_THRESHOLD {
                batch
                    .flush(txn)
                    .await
                    .map_err(|e| anyhow::anyhow!("write schema_field view: {e}"))?;
            }
        }
        let l = rows.last().unwrap();
        (last_wh, last_view, last_sid) = (l.warehouse_id, l.view_id, l.schema_id);
    }
    batch
        .flush(txn)
        .await
        .map_err(|e| anyhow::anyhow!("write schema_field view: {e}"))?;
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
}
