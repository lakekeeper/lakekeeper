use std::borrow::Cow;

use sqlx::Postgres;

pub(crate) async fn patch(
    trx: &mut sqlx::Transaction<'_, Postgres>,
    new_checksum: Cow<'static, [u8]>,
    old_checksum: Cow<'static, [u8]>,
    version: i64,
) -> anyhow::Result<()> {
    tracing::info!(
        "Fixing checksum in _sqlx_migrations for version {}: {:?} -> {:?}",
        version,
        old_checksum,
        new_checksum
    );
    let q = sqlx::query!(
        r#"UPDATE _sqlx_migrations
           SET checksum = $1
           WHERE version = $2 AND checksum = $3"#,
        new_checksum.as_ref(),
        version,
        old_checksum.as_ref()
    )
    .execute(&mut **trx)
    .await?;
    if q.rows_affected() > 1 {
        tracing::error!("More than one row was updated in _sqlx_migrations by the fix_sequence_metadata_hash migration, this is a bug please report it to the Lakekeeper developers.");
        return Err(anyhow::anyhow!(
            "More than one row was updated in _sqlx_migrations by the fix_sequence_metadata_hash migration, this is a bug please report it to the Lakekeeper developers."
        ));
    }
    if q.rows_affected() == 1 {
        tracing::info!("Patched fix sequence metadata hash in _sqlx_migrations");
    } else {
        tracing::info!("No rows were updated in _sqlx_migrations");
    }
    Ok(())
}
