use futures::{future::BoxFuture, FutureExt};
use iceberg_ext::catalog::rest::ErrorModel;
use sqlx::Postgres;

use crate::implementations::postgres::migrations::MigrationHook;

pub(super) struct SplitTableMetadataHook;

impl MigrationHook for SplitTableMetadataHook {
    fn apply<'c>(
        &self,
        trx: &'c mut sqlx::Transaction<'_, Postgres>,
    ) -> BoxFuture<'c, anyhow::Result<()>> {
        split_table_metadata(trx).boxed()
    }

    fn name(&self) -> &'static str {
        "split_table_metadata"
    }

    fn version() -> i64
    where
        Self: Sized,
    {
        20_241_106_201_139
    }
}

async fn split_table_metadata(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
) -> anyhow::Result<()> {
    let num_projects: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM project")
        .fetch_one(&mut **transaction)
        .await
        .map_err(|e| {
            tracing::error!("Failed to count projects during migration: {:?}", e);
            ErrorModel::internal(
                "Failed to count projects during migration",
                "FailedToCountProjects",
                Some(Box::new(e)),
            )
        })?;
    if num_projects == 0 {
        return Ok(());
    }

    Err(ErrorModel::failed_dependency(
        "Please update to Lakekeeper Version 0.7.X first!",
        "RequiredUpdateSkipped",
        None,
    )
    .into())
}
