use crate::implementations::postgres::dbutils::DBErrorHandler;
use sqlx::PgPool;
// use crate::service::stats::endpoint::StatsSink;
use crate::api::management::v1::warehouse::{WarehouseStatistics, WarehouseStatsResponse};
use crate::service::ListFlags;
use crate::WarehouseIdent;

pub(crate) async fn get_warehouse_stats(
    conn: PgPool,
    warehouse_ident: WarehouseIdent,
) -> crate::api::Result<WarehouseStatsResponse> {
    // TODO: pagination
    let stats = sqlx::query!(
        r#"
        SELECT number_of_views, number_of_tables, created_at
        FROM warehouse_statistics ws
        WHERE ws.warehouse_id = $1
        ORDER BY created_at DESC
        "#,
        warehouse_ident.0
    )
    .fetch_all(&conn)
    .await
    .map_err(|e| e.into_error_model("failed to get stats"))?;
    let stats = stats
        .into_iter()
        .map(|s| WarehouseStatistics {
            number_of_tables: s.number_of_tables,
            number_of_views: s.number_of_views,
            taken_at: s.created_at,
        })
        .collect();
    Ok(WarehouseStatsResponse {
        warehouse_ident: *warehouse_ident,
        stats,
    })
}

pub(crate) async fn update_stats(
    conn: PgPool,
    warehouse_ident: WarehouseIdent,
    // TODO: use list_flags for filtering
    _list_flags: ListFlags,
) -> crate::api::Result<WarehouseStatistics> {
    let stats = sqlx::query!(
        r#"
        WITH update_tables AS (
            SELECT count(*) AS value FROM "table" t
                INNER JOIN tabular ti ON t.table_id = ti.tabular_id
                INNER JOIN namespace n ON ti.namespace_id = n.namespace_id
                INNER JOIN warehouse w ON n.warehouse_id = w.warehouse_id
            WHERE w.warehouse_id = $1 AND w.status = 'active'
        ),
        update_views AS (
            SELECT count(*) AS value FROM "view" v
                INNER JOIN tabular vi ON v.view_id = vi.tabular_id
                INNER JOIN namespace n ON vi.namespace_id = n.namespace_id
                INNER JOIN warehouse w ON n.warehouse_id = w.warehouse_id
            WHERE w.warehouse_id = $1 AND w.status = 'active'
        )
        INSERT INTO warehouse_statistics (warehouse_id, number_of_views, number_of_tables) VALUES ($1, (SELECT value FROM update_views), (SELECT value FROM update_tables))
        RETURNING (SELECT value as "number_of_views!" FROM update_views), (SELECT value as "number_of_tables!" FROM update_tables), created_at as "taken_at"
        "#,
        warehouse_ident.0
    )
        .fetch_one(&conn)
        .await
        .map_err(|e| e.into_error_model("failed to collect stats"))?;

    Ok(WarehouseStatistics {
        number_of_tables: stats.number_of_tables,
        number_of_views: stats.number_of_views,
        taken_at: stats.taken_at,
    })
}

// pub struct PostgresStatsSink {
//     pool: sqlx::PgPool,
// }
//
// impl PostgresStatsSink {
//     pub fn new(pool: sqlx::PgPool) -> Self {
//         Self { pool }
//     }
// }

// #[async_trait::async_trait]
// impl StatsSink for PostgresStatsSink {
//     async fn consume_endpoint_stats(
//         &self,
//         stats_id: Uuid,
//         stats: std::collections::HashMap<String, i64>,
//     ) {
//         let mut conn = self.pool.begin().await.unwrap();
//         let _ = sqlx::query!(
//             r#"
//             INSERT INTO statistics (statistics_id)
//             VALUES ($1)
//             "#,
//             stats_id
//         )
//         for (endpoint, count) in stats {
//             let _ = sqlx::query!(
//                 r#"
//                 INSERT INTO endpoint_stats (stats_id, endpoint, count)
//                 VALUES ($1, $2)
//                 ON CONFLICT (endpoint) DO UPDATE SET count = endpoint_stats.count + $2
//                 "#,
//                 endpoint,
//                 count
//             )
//             .execute(&mut conn)
//             .await;
//         }
//     }
// }
