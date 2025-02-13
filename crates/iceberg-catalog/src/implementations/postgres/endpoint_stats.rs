use std::collections::HashMap;

use http::StatusCode;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::{
    api::endpoints::Endpoints,
    implementations::postgres::dbutils::DBErrorHandler,
    service::stats::endpoint::{EndpointIdentifier, StatsSink, WarehouseIdentOrPrefix},
    ProjectIdent, WarehouseIdent,
};

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
                },
                count,
            ) in endpoints
            {
                tracing::info!("Consuming stats for endpoint: {uri:?}, count: {count:?}",);
                let (ident, prefix) = warehouse
                    .map(|w| match w {
                        WarehouseIdentOrPrefix::Ident(ident) => (Some(ident), None),
                        WarehouseIdentOrPrefix::Prefix(prefix) => (None, Some(prefix)),
                    })
                    .unzip();
                let ident = ident.flatten();
                let prefix = prefix.flatten();

                insert_stats(&mut conn, project, uri, status_code, count, ident, prefix).await;
            }
        }
        conn.commit().await.unwrap();
    }
}

async fn insert_stats(
    conn: &mut Transaction<'_, Postgres>,
    project: Option<ProjectIdent>,
    uri: Endpoints,
    status_code: StatusCode,
    count: i64,
    ident: Option<WarehouseIdent>,
    prefix: Option<String>,
) {
    let _ = sqlx::query!(
                    r#"
                    WITH warehouse_id AS (
                        SELECT CASE
                            WHEN $2::uuid IS NULL THEN (SELECT warehouse_id FROM warehouse WHERE warehouse_name = $3)
                            ELSE $2::uuid
                        END AS warehouse_id
                    )
                    INSERT INTO endpoint_stats (project_id, warehouse_id, matched_path, status_code, count, valid_until)
                    SELECT $1, (SELECT warehouse_id from warehouse_id), $4, $5, count + $6, get_stats_date_default() FROM endpoint_stats
                    WHERE project_id = $1
                      AND warehouse_id = (select warehouse_id from warehouse_id)
                      AND matched_path = $4
                      AND status_code = $5
                      AND valid_until = get_stats_date_default()
                    ON CONFLICT (project_id, warehouse_id, matched_path, status_code, valid_until)
                    DO UPDATE SET count = EXCLUDED.count
                    "#,
                    project.map(|p| *p),
                    ident.as_deref().copied() as Option<Uuid>,
                    prefix,
                    uri as _,
                    i32::from(status_code.as_u16()),
                    count
                )
        .execute(&mut **conn)
        .await
        .map_err(|e| e.into_error_model("failed to consume stats")).unwrap();
}
