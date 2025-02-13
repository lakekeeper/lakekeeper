use std::collections::HashMap;

use crate::{
    api::endpoints::Endpoints,
    implementations::postgres::dbutils::DBErrorHandler,
    service::stats::endpoint::{EndpointIdentifier, StatsSink},
    ProjectIdent, WarehouseIdent,
};
use http::StatusCode;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

#[derive(Debug)]
pub struct PostgresStatsSink {
    pool: sqlx::PgPool,
}

impl PostgresStatsSink {
    #[must_use]
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl StatsSink for PostgresStatsSink {
    async fn consume_endpoint_stats(
        &self,
        stats: HashMap<Option<ProjectIdent>, HashMap<EndpointIdentifier, i64>>,
    ) {
        let mut conn = self.pool.begin().await.unwrap();

        for (project, endpoints) in stats {
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
                tracing::info!("Consuming stats for endpoint: {uri:?}, count: {count:?}",);

                insert_stats(
                    &mut conn,
                    project,
                    uri,
                    status_code,
                    count,
                    warehouse,
                    warehouse_name,
                )
                .await;
            }
        }
        conn.commit()
            .await
            .inspect_err(|e| tracing::error!("Failed to commit: {e}"))
            .unwrap();
    }
}

async fn insert_stats(
    conn: &mut Transaction<'_, Postgres>,
    project: Option<ProjectIdent>,
    uri: Endpoints,
    status_code: StatusCode,
    count: i64,
    ident: Option<WarehouseIdent>,
    warehouse_name: Option<String>,
) {
    tracing::debug!("Inserting stats for project: {project:?}, uri: {uri:?}, status_code: {status_code:?}, count: {count:?}",);
    let _ = sqlx::query!(
                    r#"
                    WITH warehouse_id AS (SELECT CASE
                                 WHEN $2::uuid IS NULL
                                     THEN (SELECT warehouse_id FROM warehouse WHERE warehouse_name = $3)
                                 ELSE $2::uuid
                                 END AS warehouse_id)
                    INSERT
                    INTO endpoint_stats (project_id, warehouse_id, matched_path, status_code, count)
                    SELECT $1,
                           (SELECT warehouse_id from warehouse_id),
                           $4,
                           $5,
                           COALESCE((SELECT count
                                     FROM endpoint_stats
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
    .inspect(|r| tracing::debug!("Inserted stats: {r:?}"))
    .map_err(|e| {
        tracing::error!("Failed to insert stats: {e}");
        e.into_error_model("failed to insert stats")
    })
    .unwrap();
}
