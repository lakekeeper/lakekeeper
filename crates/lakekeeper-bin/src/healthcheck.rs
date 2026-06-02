use anyhow::Context;
use lakekeeper::{
    CONFIG,
    implementations::postgres::{ReadWrite, get_reader_pool, get_writer_pool},
    service::health::{HealthExt, HealthState, HealthStatus},
    tracing,
};

pub(crate) async fn health(check_db: bool, check_server: bool) -> anyhow::Result<()> {
    tracing::info!("Checking health...");
    if check_db {
        match db_health_check().await {
            Ok(()) => {
                tracing::info!("Database is healthy.");
            }
            Err(details) => {
                tracing::info!(?details, "Database is not healthy.");
                std::process::exit(1);
            }
        }
    }

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
        if matches!(body.health, HealthStatus::Healthy) {
            tracing::info!("Server is healthy.");
        } else {
            tracing::info!(?body, "Server is not healthy: StatusCode: '{}'", status,);
            std::process::exit(1);
        }
    }
    Ok(())
}

pub(crate) fn normalize_checks(
    check_all: bool,
    check_db: bool,
    check_server: bool,
) -> (bool, bool) {
    let check_db = check_db || check_all;
    let check_server = check_server || check_all;

    if !check_db && !check_server {
        (false, true)
    } else {
        (check_db, check_server)
    }
}

pub(crate) async fn db_health_check() -> anyhow::Result<()> {
    let reader = get_reader_pool(CONFIG.to_pool_opts().max_connections(1))
        .await
        .with_context(|| "Read pool failed.")?;
    let writer = get_writer_pool(CONFIG.to_pool_opts().max_connections(1))
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

#[cfg(test)]
mod tests {
    #[test]
    fn bare_healthcheck_checks_server() {
        assert_eq!(super::normalize_checks(false, false, false), (false, true));
    }

    #[test]
    fn check_all_checks_db_and_server() {
        assert_eq!(super::normalize_checks(true, false, false), (true, true));
    }

    #[test]
    fn explicit_checks_are_preserved() {
        assert_eq!(super::normalize_checks(false, true, false), (true, false));
        assert_eq!(super::normalize_checks(false, false, true), (false, true));
    }
}
