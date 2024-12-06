use anyhow::Context;
use iceberg_catalog::implementations::postgres::{get_reader_pool, get_writer_pool, ReadWrite};
use iceberg_catalog::service::health::{HealthExt, HealthState, HealthStatus};
use iceberg_catalog::CONFIG;

pub async fn health(check_db: bool, check_server: bool) -> anyhow::Result<()> {
    tracing::info!("Checking health...");
    if check_db {
        match db_health_check().await {
            Ok(_) => {
                tracing::info!("Database is healthy.");
            }
            Err(details) => {
                tracing::info!(?details, "Database is not healthy.");
                std::process::exit(1);
            }
        };
    };

    if check_server {
        let client = reqwest::Client::new();
        let response = client
            .get(format!("http://localhost:{}/health", CONFIG.listen_port))
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            tracing::info!("Server is not healthy: StatusCode: '{}'", status);
            std::process::exit(1);
        }
        let body = response.json::<HealthState>().await?;
        // Fail with an error if the server is not healthy
        if !matches!(body.health, HealthStatus::Healthy) {
            tracing::info!(?body, "Server is not healthy: StatusCode: '{}'", status,);
            std::process::exit(1);
        } else {
            tracing::info!("Server is healthy.");
        }
    }
    Ok(())
}

pub(crate) async fn db_health_check() -> anyhow::Result<()> {
    let reader = get_reader_pool(
        CONFIG
            .to_pool_opts()
            .acquire_timeout(std::time::Duration::from_secs(1))
            .max_connections(1),
    )
    .await
    .with_context(|| "Read pool failed.")?;
    let writer = get_writer_pool(
        CONFIG
            .to_pool_opts()
            .acquire_timeout(std::time::Duration::from_secs(1))
            .max_connections(1),
    )
    .await
    .with_context(|| "Write pool failed.")?;

    let db = ReadWrite::from_pools(reader.clone(), writer.clone());
    db.update_health().await;
    db.health().await;
    let mut db_healthy = true;

    for h in db.health().await {
        tracing::info!("{:?}", h);
        db_healthy = db_healthy && matches!(h.status(), HealthStatus::Healthy);
    }
    if db_healthy {
        Ok(())
    } else {
        Err(anyhow::anyhow!("Database is not healthy."))
    }
}
