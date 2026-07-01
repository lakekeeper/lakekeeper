use futures::{FutureExt, future::BoxFuture};
use sqlx::Postgres;

use super::MigrationHook;

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
    // Freeze future JSONB schema writes (SHARE ROW EXCLUSIVE — reads still allowed). No DROP NOT NULL
    // here; that is a later release.
    sqlx::query(
        r#"CREATE FUNCTION reject_schema_write() RETURNS trigger LANGUAGE plpgsql AS $f$
           BEGIN
             IF TG_OP = 'INSERT' OR (TG_OP = 'UPDATE' AND NEW.schema IS DISTINCT FROM OLD.schema) THEN
               RAISE EXCEPTION 'schema writes are frozen during the normalized-schema migration'
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
    Ok(())
}

pub(crate) async fn backfill(txn: &mut sqlx::Transaction<'_, Postgres>) -> anyhow::Result<()> {
    const BATCH: i64 = 500;
    let (mut last_wh, mut last_tbl, mut last_sid) =
        (uuid::Uuid::nil(), uuid::Uuid::nil(), i32::MIN);
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
            let flat =
                crate::tabular::table::normalized_schema::flatten_schema(schema).map_err(|e| {
                    anyhow::anyhow!(
                        "flatten {}/{} schema {}: {e}",
                        r.warehouse_id,
                        r.table_id,
                        r.schema_id
                    )
                })?;
            crate::tabular::table::write_normalized_schema(
                txn,
                r.warehouse_id.into(),
                r.table_id.into(),
                r.schema_id,
                &flat,
            )
            .await
            .map_err(|e| {
                anyhow::anyhow!("write schema_field {}/{}: {e}", r.warehouse_id, r.table_id)
            })?;
        }
        let l = rows.last().unwrap();
        (last_wh, last_tbl, last_sid) = (l.warehouse_id, l.table_id, l.schema_id);
    }
    Ok(())
}
