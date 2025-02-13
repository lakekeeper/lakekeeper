use crate::api::endpoints::Endpoints;
use crate::implementations::postgres::dbutils::DBErrorHandler;
use crate::service::stats::endpoint::{EndpointIdentifier, StatsSink, WarehouseIdentOrPrefix};
use crate::{ProjectIdent, WarehouseIdent};
use http::{Method, StatusCode};
use sqlx::{Postgres, Transaction};
use std::collections::HashMap;
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
                    method,
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

                insert_stats(
                    &mut conn,
                    project,
                    uri,
                    status_code,
                    method,
                    count,
                    ident,
                    prefix,
                )
                .await;
            }
        }
        conn.commit().await.unwrap();
    }
}

async fn insert_stats(
    conn: &mut Transaction<Postgres>,
    project: Option<ProjectIdent>,
    uri: Endpoints,
    status_code: StatusCode,
    method: Method,
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
                    INSERT INTO endpoint_stats (project_id, warehouse_id, matched_path, status_code, method, count)
                    VALUES ($1, (SELECT warehouse_id from warehouse_id), $4, $5, $6, $7)
                    ON CONFLICT (project_id, warehouse_id, matched_path, method, status_code, valid_until)
                    DO UPDATE SET count = endpoint_stats.count + $7
                    "#,
                    project.map(|p| *p),
                    ident.as_deref().copied() as Option<Uuid>,
                    prefix,
                    uri as _,
                    status_code.as_u16() as i32,
                    method.as_str(),
                    count
                )
        .execute(&mut *conn)
        .await
        .map_err(|e| e.into_error_model("failed to consume stats")).unwrap();
}
