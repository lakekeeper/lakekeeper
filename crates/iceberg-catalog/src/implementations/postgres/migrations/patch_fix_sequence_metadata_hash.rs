use futures::{future::BoxFuture, FutureExt};
use sqlx::Postgres;

use crate::implementations::postgres::migrations::MigrationHook;

pub(super) struct FixSequenceMetadataHashHook;

impl MigrationHook for FixSequenceMetadataHashHook {
    fn apply<'c>(
        &self,
        trx: &'c mut sqlx::Transaction<'_, Postgres>,
    ) -> BoxFuture<'c, anyhow::Result<()>> {
        async move {
            let q = sqlx::query!(r#"UPDATE _sqlx_migrations
                                                   SET checksum = '\xb2e25f1775c5b8d27b7f681cf1ae34bb8b0fa4ecd7e86dc3d5fa3687519c9c23b2842f3bcb831ad7b55a6938b067e661'
                                                   WHERE checksum = '\x9e77063e97355a4bb0738dace6cb1fe57bcd74feab2e657eeb8f00136af74c573308458fd37ad02cb4e96e96fb9169af'
                                                     AND version = '20250328131139'"#).execute(&mut **trx).await?;
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
        .boxed()
    }

    fn version() -> i64
    where
        Self: Sized,
    {
        20_250_326_153_233
    }
}
