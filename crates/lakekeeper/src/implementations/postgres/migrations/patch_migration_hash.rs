use std::borrow::Cow;

use sqlx::{AssertSqlSafe, Postgres};

/// Rewrite the checksum stored in `table_name` for `version` from
/// `stored_in_db` to `new_in_file`. Used to recover when a previously-shipped
/// migration's content was edited without bumping the version.
///
/// `table_name` must come from `ExtensionMigrations::tracker_table()` or
/// `CORE_MIGRATIONS_TABLE` — both are validated against `[a-z_][a-z0-9_]*`,
/// so the interpolation below is SQL-safe.
// Historical note: prior to the rename, the parameters were called
// `new_checksum`/`old_checksum` and the bind order didn't match the
// caller's argument order, so the `UPDATE` matched zero rows. The
// `sha_patches.remove()` call in the caller still absorbed the
// `VersionMismatch`, so the bug was silent. This version fixes both
// the names and the bind order, leaving the caller's positional order
// unchanged.
pub(crate) async fn patch(
    trx: &mut sqlx::Transaction<'_, Postgres>,
    table_name: &str,
    stored_in_db: Cow<'static, [u8]>,
    new_in_file: Cow<'static, [u8]>,
    version: i64,
) -> anyhow::Result<()> {
    tracing::info!(
        "Fixing checksum in {table_name} for version {version}: {stored_in_db:?} -> {new_in_file:?}",
    );
    let q = sqlx::query(AssertSqlSafe(format!(
        "UPDATE {table_name}
           SET checksum = $1
           WHERE version = $2 AND checksum = $3",
    )))
    .bind(new_in_file.as_ref())
    .bind(version)
    .bind(stored_in_db.as_ref())
    .execute(&mut **trx)
    .await?;
    if q.rows_affected() > 1 {
        tracing::error!(
            "More than one row was updated in {table_name} by the checksum patch — this is a bug; please report it."
        );
        return Err(anyhow::anyhow!(
            "More than one row was updated in {table_name} by the checksum patch — this is a bug; please report it."
        ));
    }
    if q.rows_affected() == 1 {
        tracing::info!("Patched checksum in {table_name} for version {version}");
    } else {
        tracing::info!("No rows were updated in {table_name} for version {version}");
    }
    Ok(())
}
