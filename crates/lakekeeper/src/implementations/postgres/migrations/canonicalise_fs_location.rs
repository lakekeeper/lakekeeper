use std::str::FromStr;

use anyhow::{Context, anyhow};
use futures::{FutureExt, future::BoxFuture};
use lakekeeper_io::Location;
use sqlx::Postgres;

use crate::implementations::postgres::migrations::MigrationHook;

pub(super) struct CanonicaliseFsLocationHook;

/// How many rows to bundle into a single batch UPDATE. Chunks bound peak
/// memory, lock duration on `tabular`, and WAL volume per statement.
const UPDATE_CHUNK: usize = 5_000;

impl MigrationHook for CanonicaliseFsLocationHook {
    fn apply<'c>(
        &self,
        trx: &'c mut sqlx::Transaction<'_, Postgres>,
    ) -> BoxFuture<'c, anyhow::Result<()>> {
        canonicalise_fs_location(trx).boxed()
    }

    fn name(&self) -> &'static str {
        "canonicalise_fs_location"
    }

    fn version() -> i64
    where
        Self: Sized,
    {
        20_260_508_120_000
    }
}

/// Row shape decoded from the canonicalisation cursor. `FromRow` decodes
/// at runtime, so the historical-migration code stays robust against
/// future schema drift on `tabular` columns: as long as the column types
/// at *this* migration's runtime match the struct field types, the hook
/// works regardless of what later migrations do.
///
/// In contrast, `sqlx::query!` would bake column types from the dev DB
/// (which has every migration applied, including future ones) into the
/// generated Rust code — a future type change to `fs_protocol` or
/// `fs_location` would silently shift our compiled assumption out of
/// sync with what runs in production at this migration's slot.
#[derive(sqlx::FromRow)]
struct ScannedRow {
    tabular_id: uuid::Uuid,
    fs_protocol: String,
    fs_location: String,
}

async fn canonicalise_fs_location(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
) -> anyhow::Result<()> {
    // Server-side cursor walks `tabular` once with a stable MVCC snapshot
    // taken at DECLARE time. NO SCROLL = forward-only — required for the
    // safety property below.
    //
    // We `FETCH FORWARD N` a chunk, canonicalise it, run an UPDATE on the
    // changed rows, then continue. Modifications by the same transaction
    // are *not* visible to the cursor (the cursor walks its snapshot, not
    // the live heap), so the UPDATEs we issue cannot cause the cursor to
    // skip or revisit a row. The outer transaction also holds ACCESS
    // EXCLUSIVE on `tabular` (from the `ALTER COLUMN TYPE` in the .sql),
    // so no other transaction can mutate the table concurrently either.
    //
    // Memory is bounded to one chunk at a time, instead of the whole row
    // set materialised eagerly.
    sqlx::query(
        "DECLARE canonicalise_fs_location_cur NO SCROLL CURSOR FOR
         SELECT tabular_id, fs_protocol, fs_location FROM tabular",
    )
    .execute(&mut **transaction)
    .await
    .context("failed to declare canonicalisation cursor")?;

    let mut total_scanned = 0usize;
    let mut total_updates = 0usize;

    // PostgreSQL's `FETCH` does not accept bind parameters for the row
    // count — the count must be inlined into the SQL. `UPDATE_CHUNK` is a
    // compile-time constant, so the inlined literal is safe.
    let fetch_sql = format!("FETCH FORWARD {UPDATE_CHUNK} FROM canonicalise_fs_location_cur");

    loop {
        // `query_as` (runtime FromRow) rather than `query_as!` because the
        // cursor isn't declared at macro-compile time, so the macro can't
        // see the row shape.
        let chunk: Vec<ScannedRow> = sqlx::query_as(&fetch_sql)
            .fetch_all(&mut **transaction)
            .await
            .context("failed to fetch chunk from canonicalisation cursor")?;

        if chunk.is_empty() {
            break;
        }
        total_scanned += chunk.len();

        // Each chunk-local change set is bounded by `UPDATE_CHUNK`; we
        // pass three array parameters to the UPDATE regardless of size,
        // well under PostgreSQL's 65535-parameter ceiling.
        let mut ids: Vec<uuid::Uuid> = Vec::with_capacity(chunk.len());
        let mut new_protocols: Vec<String> = Vec::with_capacity(chunk.len());
        let mut new_locations: Vec<String> = Vec::with_capacity(chunk.len());

        for row in &chunk {
            let raw = format!("{}://{}", row.fs_protocol, row.fs_location);
            let canonical = Location::from_str(&raw).map_err(|e| {
                anyhow!(
                    "tabular_id {}: stored location `{}` cannot be canonicalised: {}. \
                     Reconcile the row manually before retrying the upgrade.",
                    row.tabular_id,
                    raw,
                    e.reason
                )
            })?;

            let new_protocol = canonical.scheme();
            let new_location = canonical.authority_and_path();
            if new_protocol == row.fs_protocol && new_location == row.fs_location {
                continue;
            }
            ids.push(row.tabular_id);
            new_protocols.push(new_protocol.to_string());
            new_locations.push(new_location.to_string());
        }
        // Drop the read chunk before issuing UPDATE so peak resident is
        // either the read buffer or the change vectors, not both.
        drop(chunk);

        if ids.is_empty() {
            continue;
        }
        let chunk_updates = ids.len();
        sqlx::query!(
            "UPDATE tabular SET fs_protocol = u.fs_protocol, fs_location = u.fs_location
             FROM unnest($1::uuid[], $2::text[], $3::text[])
                  AS u(id, fs_protocol, fs_location)
             WHERE tabular.tabular_id = u.id",
            &ids,
            &new_protocols,
            &new_locations,
        )
        .execute(&mut **transaction)
        .await
        .with_context(|| {
            format!("failed to rewrite canonical fs_location chunk of {chunk_updates} rows")
        })?;
        total_updates += chunk_updates;
    }

    sqlx::query("CLOSE canonicalise_fs_location_cur")
        .execute(&mut **transaction)
        .await
        .context("failed to close canonicalisation cursor")?;

    tracing::info!(
        scanned = total_scanned,
        updated = total_updates,
        chunk_size = UPDATE_CHUNK,
        "Canonicalised tabular.fs_location values"
    );

    // The unique index was created by the .sql migration that runs before
    // this hook. Any chunk UPDATE above that would create two rows with
    // the same canonical (warehouse_id, fs_location) has already failed
    // with a unique-violation, rolling back the outer transaction with the
    // conflicting key in the error. The constraint covers soft-deleted
    // rows too — see the index comment in the .sql.
    Ok(())
}

#[cfg(test)]
mod tests {
    use iceberg_ext::NamespaceIdent;
    use sqlx::PgPool;
    use uuid::Uuid;

    use super::*;
    use crate::implementations::postgres::{
        CatalogState, namespace::tests::initialize_namespace, warehouse::test::initialize_warehouse,
    };

    /// Set up a warehouse + namespace and return the IDs needed to satisfy
    /// the FK constraints when raw-inserting tabular rows. Bypassing the
    /// normal `create_tabular` path is the point — those go through
    /// `Location::from_str` and would canonicalise the input before insert.
    async fn seed_warehouse(pool: &PgPool) -> (Uuid, Uuid, Vec<String>) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let ns_name = "test_ns";
        let namespace = NamespaceIdent::from_vec(vec![ns_name.to_string()]).unwrap();
        let resp = initialize_namespace(state, warehouse_id, &namespace, None).await;
        (
            *warehouse_id,
            *resp.namespace_id(),
            vec![ns_name.to_string()],
        )
    }

    async fn raw_insert_tabular(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        warehouse_id: Uuid,
        namespace_id: Uuid,
        ns_name: &[String],
        name: &str,
        fs_protocol: &str,
        fs_location: &str,
    ) -> Uuid {
        let tabular_id = Uuid::now_v7();
        let metadata_location = format!("{fs_protocol}://{fs_location}/metadata.json");
        sqlx::query!(
            "INSERT INTO tabular (
                tabular_id, name, namespace_id, tabular_namespace_name,
                warehouse_id, typ, metadata_location, fs_protocol, fs_location
            ) VALUES ($1, $2, $3, $4, $5, 'table', $6, $7, $8)",
            tabular_id,
            name,
            namespace_id,
            ns_name,
            warehouse_id,
            metadata_location,
            fs_protocol,
            fs_location,
        )
        .execute(&mut **tx)
        .await
        .unwrap();
        tabular_id
    }

    #[sqlx::test]
    async fn canonicalise_rewrites_non_canonical_row(pool: PgPool) {
        let (warehouse_id, namespace_id, ns_name) = seed_warehouse(&pool).await;

        let mut tx = pool.begin().await.unwrap();
        let tabular_id = raw_insert_tabular(
            &mut tx,
            warehouse_id,
            namespace_id,
            &ns_name,
            "t",
            "s3",
            // `%2D` decodes to `-` (unreserved), `%2f` is uppercased to `%2F`-
            // wait, `%2F` itself is rejected; use a benign mixed-hex case.
            "test-bucket/Foo%2Dbar",
        )
        .await;

        canonicalise_fs_location(&mut tx).await.unwrap();

        let row = sqlx::query!(
            "SELECT fs_protocol, fs_location FROM tabular WHERE tabular_id = $1",
            tabular_id,
        )
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        assert_eq!(row.fs_protocol, "s3");
        assert_eq!(row.fs_location, "test-bucket/Foo-bar");
    }

    #[sqlx::test]
    async fn canonicalise_lowercases_scheme(pool: PgPool) {
        let (warehouse_id, namespace_id, ns_name) = seed_warehouse(&pool).await;

        let mut tx = pool.begin().await.unwrap();
        // Pre-canonicalisation rows could have mixed-case scheme.
        let tabular_id = raw_insert_tabular(
            &mut tx,
            warehouse_id,
            namespace_id,
            &ns_name,
            "t",
            "S3A",
            "test-bucket/foo",
        )
        .await;

        canonicalise_fs_location(&mut tx).await.unwrap();

        let row = sqlx::query!(
            "SELECT fs_protocol FROM tabular WHERE tabular_id = $1",
            tabular_id,
        )
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        assert_eq!(row.fs_protocol, "s3a");
    }

    #[sqlx::test]
    async fn canonical_row_is_no_op(pool: PgPool) {
        let (warehouse_id, namespace_id, ns_name) = seed_warehouse(&pool).await;

        let mut tx = pool.begin().await.unwrap();
        let tabular_id = raw_insert_tabular(
            &mut tx,
            warehouse_id,
            namespace_id,
            &ns_name,
            "t",
            "s3",
            "test-bucket/already-canonical",
        )
        .await;

        canonicalise_fs_location(&mut tx).await.unwrap();

        let row = sqlx::query!(
            "SELECT fs_protocol, fs_location FROM tabular WHERE tabular_id = $1",
            tabular_id,
        )
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        // Direct byte-equality assertion: an already-canonical input must
        // round-trip verbatim through the hook.
        assert_eq!(row.fs_protocol, "s3");
        assert_eq!(row.fs_location, "test-bucket/already-canonical");
    }

    #[sqlx::test]
    async fn empty_table_is_no_op(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        canonicalise_fs_location(&mut tx).await.unwrap();
        tx.commit().await.unwrap();
    }

    #[sqlx::test]
    async fn collision_returns_unique_violation(pool: PgPool) {
        let (warehouse_id, namespace_id, ns_name) = seed_warehouse(&pool).await;

        let mut tx = pool.begin().await.unwrap();
        // Two rows whose canonical forms collapse together: encoded `%2D`
        // and literal `-`. Both are byte-distinct now, but
        // canonicalisation rewrites the first to match the second.
        raw_insert_tabular(
            &mut tx,
            warehouse_id,
            namespace_id,
            &ns_name,
            "t1",
            "s3",
            "test-bucket/Foo%2Dbar",
        )
        .await;
        raw_insert_tabular(
            &mut tx,
            warehouse_id,
            namespace_id,
            &ns_name,
            "t2",
            "s3",
            "test-bucket/Foo-bar",
        )
        .await;

        let err = canonicalise_fs_location(&mut tx)
            .await
            .expect_err("two rows that canonicalise to the same value must collide");
        // Pin the failure mode to the exact PostgreSQL unique-violation
        // error code (SQLSTATE 23505). A different SQLSTATE means the
        // assertion is masking a different failure (e.g. parse error,
        // FK violation) and the collision path isn't actually exercised.
        let db_err = err
            .chain()
            .find_map(|e| e.downcast_ref::<sqlx::Error>())
            .and_then(|e| match e {
                sqlx::Error::Database(db) => Some(db),
                _ => None,
            })
            .unwrap_or_else(|| panic!("expected sqlx database error in chain, got: {err:?}"));
        assert_eq!(
            db_err.code().as_deref(),
            Some("23505"),
            "expected unique_violation (23505), got code {:?} message {}",
            db_err.code(),
            db_err.message()
        );
    }

    /// Drive `UPDATE_CHUNK` * 3 rows through the migration to confirm the
    /// chunked UPDATE works.
    #[sqlx::test]
    async fn batch_update_handles_many_rows(pool: PgPool) {
        const N: usize = 15_000;

        let (warehouse_id, namespace_id, ns_name) = seed_warehouse(&pool).await;
        let mut tx = pool.begin().await.unwrap();

        // Bulk-insert via UNNEST so the test setup itself doesn't dominate
        // runtime. Every row is non-canonical (`%2Dbar`) so every row
        // produces an UPDATE.
        let mut ids: Vec<Uuid> = Vec::with_capacity(N);
        let mut names: Vec<String> = Vec::with_capacity(N);
        let mut metadata_locations: Vec<String> = Vec::with_capacity(N);
        let mut fs_locations: Vec<String> = Vec::with_capacity(N);
        for i in 0..N {
            let id = Uuid::now_v7();
            ids.push(id);
            names.push(format!("t{i}"));
            metadata_locations.push(format!("s3://test-bucket/row{i}/Foo%2Dbar/metadata.json"));
            fs_locations.push(format!("test-bucket/row{i}/Foo%2Dbar"));
        }
        sqlx::query!(
            "INSERT INTO tabular (
                tabular_id, name, namespace_id, tabular_namespace_name,
                warehouse_id, typ, metadata_location, fs_protocol, fs_location
            )
            SELECT u.id, u.name, $1, $2, $3, 'table', u.metadata_location, 's3', u.fs_location
            FROM unnest($4::uuid[], $5::text[], $6::text[], $7::text[])
                 AS u(id, name, metadata_location, fs_location)",
            namespace_id,
            &ns_name,
            warehouse_id,
            &ids,
            &names,
            &metadata_locations,
            &fs_locations,
        )
        .execute(&mut *tx)
        .await
        .unwrap();

        canonicalise_fs_location(&mut tx).await.unwrap();

        // Spot-check: first, middle, last rows all canonicalised.
        for &i in &[0usize, N / 2, N - 1] {
            let row = sqlx::query!(
                "SELECT fs_location FROM tabular WHERE tabular_id = $1",
                ids[i],
            )
            .fetch_one(&mut *tx)
            .await
            .unwrap();
            assert_eq!(row.fs_location, format!("test-bucket/row{i}/Foo-bar"));
        }
    }

    #[sqlx::test]
    async fn invalid_stored_location_aborts_with_row_id(pool: PgPool) {
        let (warehouse_id, namespace_id, ns_name) = seed_warehouse(&pool).await;

        let mut tx = pool.begin().await.unwrap();
        // Trailing `.` in a path segment is rejected by PR1's
        // canonicalisation (Azure Blob aliasing). A pre-PR1 row with such a
        // value cannot be canonicalised — operator must reconcile.
        let tabular_id = raw_insert_tabular(
            &mut tx,
            warehouse_id,
            namespace_id,
            &ns_name,
            "t",
            "s3",
            "test-bucket/foo./bar",
        )
        .await;

        let err = canonicalise_fs_location(&mut tx)
            .await
            .expect_err("uncanonicalisable input must abort the migration");
        let msg = format!("{err:?}");
        assert!(
            msg.contains(&tabular_id.to_string()),
            "error must name the failing row id; got: {msg}"
        );
    }
}
