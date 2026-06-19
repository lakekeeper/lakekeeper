use lakekeeper::{tokio, tracing};
use lakekeeper_storage_postgres::{
    config::CONFIG as PG_CONFIG,
    get_reader_pool,
    migrations::{MigrationState, check_migration_status},
};

use crate::healthcheck::db_health_check;

pub(crate) async fn wait_for_db(
    check_migrations: bool,
    retries: u32,
    backoff: u64,
    check_db: bool,
) -> anyhow::Result<()> {
    if check_db {
        let mut counter = 0;

        loop {
            let Err(details) = db_health_check().await else {
                tracing::info!("Database is healthy.");
                break;
            };
            counter += 1;
            if counter > retries {
                tracing::error!("DB is not up.");
                anyhow::bail!("DB is not up.");
            }
            tracing::info!(
                ?details,
                "DB not up yet, sleeping for {backoff}s before next retry. Retry: {counter}/{retries}",
            );
            tokio::time::sleep(std::time::Duration::from_secs(backoff)).await;
        }
    }

    if check_migrations {
        let mut counter = 0;
        loop {
            let read_pool = get_reader_pool(PG_CONFIG.to_pool_opts()).await?;
            let migrations = check_migration_status(&read_pool).await;
            match migrations {
                Ok(MigrationState::Complete) => {
                    tracing::info!("Database is up to date with binary.");
                    break;
                }
                Ok(MigrationState::Ahead) => {
                    // The DB was migrated by a newer Lakekeeper. Retrying never
                    // resolves this (the DB won't get older), so fail fast
                    // instead of looping until the retry budget is exhausted.
                    tracing::error!(
                        "Database has been migrated by a NEWER Lakekeeper than this binary. \
                         Refusing to start to avoid running against an incompatible schema. \
                         Use a binary at least as new as the one that last migrated the database. \
                         To start anyway (e.g. an emergency rollback, accepting the risk of \
                         schema incompatibility), run `serve --force-start`."
                    );
                    anyhow::bail!(
                        "Database is newer than this binary (migrated by a newer Lakekeeper); refusing to start."
                    );
                }
                unready => {
                    tracing::info!(?unready, "Database is not up to date with binary.");
                }
            }

            counter += 1;
            if counter > retries {
                tracing::error!(
                    "Database is not up to date with binary, make sure to run the migrate command before starting the server."
                );
                anyhow::bail!(
                    "Database is not up to date with binary, make sure to run the migrate command before starting the server."
                );
            }
            tracing::info!(
                "DB not up to date with binary yet, sleeping for {backoff}s before next retry. Retry: {counter}/{retries}",
            );
            tokio::time::sleep(std::time::Duration::from_secs(backoff)).await;
        }
    }
    Ok(())
}
