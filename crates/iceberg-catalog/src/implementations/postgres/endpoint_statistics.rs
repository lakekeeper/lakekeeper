use std::{collections::HashMap, sync::Arc, time::Duration};

use http::StatusCode;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::{
    api::endpoints::Endpoints,
    implementations::postgres::dbutils::DBErrorHandler,
    service::endpoint_statistics::{EndpointIdentifier, EndpointStatisticsSink},
    ProjectIdent, WarehouseIdent,
};

#[derive(Debug)]
pub struct PostgresStatisticsSink {
    pool: sqlx::PgPool,
}

impl PostgresStatisticsSink {
    #[must_use]
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    async fn process_stats(
        &self,
        stats: Arc<HashMap<Option<ProjectIdent>, HashMap<EndpointIdentifier, i64>>>,
    ) -> crate::api::Result<()> {
        let mut conn = self.pool.begin().await.map_err(|e| {
            tracing::error!("Failed to start transaction: {e}, lost stats: {stats:?}");
            e.into_error_model("failed to start transaction")
        })?;

        for (project, endpoints) in stats.iter() {
            tracing::info!("Consuming stats for project: {project:?}, counts: {endpoints:?}",);
            for (
                EndpointIdentifier {
                    uri,
                    status_code,
                    warehouse,
                    warehouse_name,
                },
                count,
            ) in endpoints
            {
                insert_statistics(
                    &mut conn,
                    *project,
                    uri,
                    status_code,
                    *count,
                    *warehouse,
                    warehouse_name.as_deref(),
                )
                .await?;
            }
        }
        conn.commit().await.map_err(|e| {
            tracing::error!("Failed to commit: {e}");
            e.into_error_model("failed to commit")
        })?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl EndpointStatisticsSink for PostgresStatisticsSink {
    async fn consume_endpoint_statistics(
        &self,
        stats: HashMap<Option<ProjectIdent>, HashMap<EndpointIdentifier, i64>>,
    ) -> crate::api::Result<()> {
        let stats = Arc::new(stats);

        tryhard::retry_fn(async || {
            self.process_stats(stats.clone()).await.inspect_err(|e| {
                tracing::error!(
                    "Failed to consume stats: {}, will retry up to 5 times.",
                    e.error
                );
            })
        })
        .retries(5)
        .exponential_backoff(Duration::from_millis(125))
        .await
    }

    fn sink_id(&self) -> &'static str {
        "postgres"
    }
}

async fn insert_statistics(
    conn: &mut Transaction<'_, Postgres>,
    project: Option<ProjectIdent>,
    uri: &Endpoints,
    status_code: &StatusCode,
    count: i64,
    ident: Option<WarehouseIdent>,
    warehouse_name: Option<&str>,
) -> Result<(), crate::api::ErrorModel> {
    let _ = sqlx::query!(
                    r#"
                    WITH warehouse_id AS (SELECT CASE
                                 WHEN $2::uuid IS NULL
                                     THEN (SELECT warehouse_id FROM warehouse WHERE warehouse_name = $3)
                                 ELSE $2::uuid
                                 END AS warehouse_id)
                    INSERT
                    INTO endpoint_statistics (project_id, warehouse_id, matched_path, status_code, count)
                    SELECT $1,
                           (SELECT warehouse_id from warehouse_id),
                           $4,
                           $5,
                           COALESCE((SELECT count
                                     FROM endpoint_statistics
                                     WHERE project_id = $1
                                       AND warehouse_id = (select warehouse_id from warehouse_id)
                                       AND matched_path = $4
                                       AND status_code = $5
                                       AND valid_until = get_stats_date_default()), 0) + $6
                    ON CONFLICT (project_id, warehouse_id, matched_path, status_code, valid_until)
                        DO UPDATE SET count = EXCLUDED.count
                    "#,
        project.map(|p| *p),
        ident.as_deref().copied() as Option<Uuid>,
        warehouse_name,
        uri as _,
        i32::from(status_code.as_u16()),
        count
    )
    .execute(&mut **conn)
    .await
    .map_err(|e| {
        tracing::error!("Failed to insert stats: {e}");
        e.into_error_model("failed to insert stats")
    })
    ?;
    Ok(())
}

#[cfg(test)]
mod test {
    use strum::IntoEnumIterator;

    use crate::ProjectIdent;

    #[sqlx::test]
    async fn test_can_insert_all_variants(pool: sqlx::PgPool) {
        let mut conn = pool.begin().await.unwrap();

        let project = Some(ProjectIdent::default());
        let status_code = http::StatusCode::OK;
        let count = 1;
        let ident = None;
        let warehouse_name = None;
        for uri in crate::api::endpoints::Endpoints::iter() {
            super::insert_statistics(
                &mut conn,
                project,
                &uri,
                &status_code,
                count,
                ident,
                warehouse_name,
            )
            .await
            .unwrap();
        }

        conn.commit().await.unwrap();
    }
}
