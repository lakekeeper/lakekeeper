use crate::healthcheck::db_health_check;
use iceberg_catalog::implementations::postgres::{get_reader_pool, MigrationState};
use iceberg_catalog::CONFIG;

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
            tracing::info!(?details,
                        "DB not up yet, sleeping for {backoff}s before next retry. Retry: {counter}/{retries}",
                    );
            tokio::time::sleep(std::time::Duration::from_secs(backoff)).await;
        }
    }

    if check_migrations {
        let mut counter = 0;
        loop {
            let opts = CONFIG
                .to_pool_opts()
                .acquire_timeout(std::time::Duration::from_secs(1));

            let read_pool = get_reader_pool(opts).await?;
            let migrations =
                iceberg_catalog::implementations::postgres::check_migration_status(&read_pool)
                    .await;
            match migrations {
                Ok(MigrationState::Complete) => {
                    tracing::info!("Database is up to date with binary.");
                    break;
                }
                unready => {
                    tracing::info!(?unready, "Database is not up to date with binary.");
                }
            }

            counter += 1;
            if counter > retries {
                tracing::error!("Database is not up to date with binary, make sure to run the migrate command before starting the server.");
                anyhow::bail!("Database is not up to date with binary, make sure to run the migrate command before starting the server.");
            }
            tracing::info!(
                        "DB not up to date with binary yet, sleeping for {backoff}s before next retry. Retry: {counter}/{retries}",
                    );
            tokio::time::sleep(std::time::Duration::from_secs(backoff)).await;
        }
    }
    Ok(())
}
