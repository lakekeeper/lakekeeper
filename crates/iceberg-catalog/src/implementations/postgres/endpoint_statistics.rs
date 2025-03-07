use std::{collections::HashMap, sync::Arc, time::Duration};

use chrono::Utc;
use iceberg_ext::catalog::rest::ErrorModel;
use itertools::{izip, Itertools};
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    api::{
        endpoints::Endpoints,
        management::v1::project::{
            EndpointStatistic, EndpointStatisticsResponse, RangeSpecifier, WarehouseFilter,
        },
    },
    implementations::postgres::{
        dbutils::DBErrorHandler,
        pagination::{PaginateToken, RoundTrippableDuration, V1PaginateToken},
    },
    service::endpoint_statistics::{EndpointIdentifier, EndpointStatisticsSink},
    ProjectId,
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

    #[allow(clippy::too_many_lines)]
    async fn process_stats(
        &self,
        stats: Arc<HashMap<ProjectId, HashMap<EndpointIdentifier, i64>>>,
    ) -> crate::api::Result<()> {
        let mut conn = self.pool.begin().await.map_err(|e| {
            tracing::error!("Failed to start transaction: {e}");
            e.into_error_model("failed to start transaction")
        })?;

        let (projects, warehouse_idents): (Vec<Uuid>, Vec<_>) = stats
            .iter()
            .flat_map(|(p, e)| {
                e.keys().filter_map(|epi| {
                    epi.warehouse_name
                        .as_ref()
                        .map(|warehouse| (**p, warehouse.to_string()))
                })
            })
            .unique()
            .unzip();

        let warehouse_ids = sqlx::query!(
            r#"SELECT project_id, warehouse_name, warehouse_id
               FROM warehouse
               WHERE (project_id, warehouse_name) IN (
                   SELECT unnest($1::uuid[]), unnest($2::text[])
               )"#,
            &projects,
            &warehouse_idents
        )
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| {
            tracing::error!("Failed to fetch warehouse ids: {e}");
            e.into_error_model("failed to fetch warehouse ids")
        })?
        .into_iter()
        .map(|w| ((w.project_id, w.warehouse_name), w.warehouse_id))
        .collect::<HashMap<_, _>>();
        let n_eps = stats.iter().map(|(_, eps)| eps.len()).sum::<usize>();

        tracing::debug!(
            "Preparing to insert {n_eps} endpoint statistic datapoints across {} projects",
            stats.len()
        );

        let mut uris = Vec::with_capacity(n_eps);
        let mut status_codes = Vec::with_capacity(n_eps);
        let mut warehouses = Vec::with_capacity(n_eps);
        let mut counts = Vec::with_capacity(n_eps);
        let mut projects = Vec::with_capacity(n_eps);
        for (project, endpoints) in stats.iter() {
            tracing::trace!("Processing stats for project: {project}");
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
                projects.push(**project);
                uris.push(*uri);
                status_codes.push(i32::from(status_code.as_u16()));
                let warehouse = warehouse
                    .as_deref()
                    .or_else(|| {
                        warehouse_name
                            .as_deref()
                            .and_then(|wn| warehouse_ids.get(&(**project, wn.to_string())))
                    })
                    .copied();
                warehouses.push(warehouse);
                counts.push(*count);
            }
        }

        tracing::debug!("Inserting stats batch");

        // TODO: when to start batching the inserts?
        sqlx::query!(r#"INSERT INTO endpoint_statistics (project_id, warehouse_id, matched_path, status_code, count, timestamp)
                        SELECT
                            project_id,
                            warehouse,
                            uri,
                            status_code,
                            cnt,
                            get_stats_date_default()
                        FROM (
                            SELECT
                                unnest($1::UUID[]) as project_id,
                                unnest($2::UUID[]) as warehouse,
                                unnest($3::api_endpoints[]) as uri,
                                unnest($4::INT[]) as status_code,
                                unnest($5::BIGINT[]) as cnt
                        ) t
                        ON CONFLICT (project_id, warehouse_id, matched_path, status_code, timestamp)
                            DO UPDATE SET count = endpoint_statistics.count + EXCLUDED.count"#,
                projects.as_slice(),
                warehouses.as_slice() as _,
                &uris as _,
                &status_codes,
                &counts
            ).execute(&mut *conn).await.map_err(|e| {
            tracing::error!("Failed to insert stats: {e}, lost stats: {stats:?}");
            e.into_error_model("failed to insert stats")
        })?;

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
        stats: HashMap<ProjectId, HashMap<EndpointIdentifier, i64>>,
    ) -> crate::api::Result<()> {
        let stats = Arc::new(stats);

        tryhard::retry_fn(async || {
            self.process_stats(stats.clone()).await.inspect_err(|e| {
                tracing::error!(
                    "Failed to consume stats: {:?}, will retry up to 5 times.",
                    e.error
                );
            })
        })
        .retries(5)
        .exponential_backoff(Duration::from_millis(125))
        .await
        .inspect(|()| {
            tracing::debug!("Successfully consumed stats");
        })
        .inspect_err(|e| {
            tracing::error!(
                "Failed to consume stats: {:?}, lost stats: {stats:?}",
                e.error
            );
        })
    }

    fn sink_id(&self) -> &'static str {
        "postgres"
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn list_statistics(
    project: ProjectId,
    warehouse_filter: WarehouseFilter,
    status_codes: Option<&[u16]>,
    range_specifier: RangeSpecifier,
    conn: &PgPool,
) -> crate::api::Result<EndpointStatisticsResponse> {
    let (end, interval) = match range_specifier {
        RangeSpecifier::Range {
            end_of_range,
            interval,
        } => (end_of_range, interval),
        RangeSpecifier::PageToken { token } => parse_token(token.as_str())?,
    };

    let start = end - interval;

    let get_all = matches!(warehouse_filter, WarehouseFilter::All);
    let warehouse_filter = match warehouse_filter {
        WarehouseFilter::Ident { id } => Some(id),
        _ => None,
    };
    let status_codes = status_codes.map(|s| s.iter().map(|i| i32::from(*i)).collect_vec());

    tracing::info!(
        "Listing stats for project: {project:?}, get_all: {get_all}, warehouse_filter: {warehouse_filter:?}, status_codes: {status_codes:?}, interval: {interval:?}, start: {start:?}, end: {end:?}",
        project = project,
        warehouse_filter = warehouse_filter,
        status_codes = status_codes,
        interval = interval,
        start = start,
        end = end,
    );
    // 2025-03-07 09:34:45+00
    // 2025-03-07 09:34:46+00
    let row = sqlx::query!(
        r#"
        SELECT timestamp,
               array_agg(matched_path) as "matched_path!: Vec<Endpoints>",
               array_agg(status_code) as "status_code!",
               array_agg(count) as "count!",
               array_agg(es.warehouse_id) as "warehouse_id!: Vec<Option<Uuid>>",
               array_agg(warehouse_name) as "warehouse_name!: Vec<Option<String>>",
               array_agg(es.created_at) as "created_at!",
               array_agg(es.updated_at) as "updated_at!: Vec<Option<chrono::DateTime<Utc>>>"
        FROM endpoint_statistics es
        LEFT JOIN warehouse w ON es.warehouse_id = w.warehouse_id
        WHERE es.project_id = $1
            AND (es.warehouse_id = $2 OR $3)
            AND (status_code = ANY($4) OR $4 IS NULL)
            AND timestamp >  (date_trunc(get_stats_interval_unit(), $5::timestamptz) + get_stats_interval())
            AND timestamp <= (date_trunc(get_stats_interval_unit(), $6::timestamptz) + get_stats_interval())
        group by timestamp
        order by timestamp desc
        "#,
        *project,
        warehouse_filter,
        get_all,
        status_codes.as_deref(),
        start,
        end,
    )
    .fetch_all(conn)
    .await
    .map_err(|e| {
        tracing::error!("Failed to list stats: {e}");
        e.into_error_model("failed to list stats")
    })?;

    let (timestamps, called_endpoints): (Vec<_>, Vec<_>) = row
        .into_iter()
        .map(|r| {
            let ts = r.timestamp;
            let row_stats: Vec<_> = izip!(
                r.matched_path,
                r.status_code,
                r.count,
                r.warehouse_id,
                r.warehouse_name,
                r.created_at,
                r.updated_at
            )
            .map(
                |(
                    uri,
                    status_code,
                    count,
                    warehouse_id,
                    warehouse_name,
                    created_at,
                    updated_at,
                )| EndpointStatistic {
                    count,
                    http_route: uri.as_http_route().to_string(),
                    status_code: status_code
                        .clamp(i32::from(u16::MIN), i32::from(u16::MAX))
                        .try_into()
                        .expect("status code is valid since we just clamped it"),
                    warehouse_id,
                    warehouse_name,
                    created_at,
                    updated_at,
                },
            )
            .collect();

            (ts, row_stats)
        })
        .unzip();

    Ok(EndpointStatisticsResponse {
        timestamps,
        called_endpoints,
        previous_page_token: PaginateToken::V1(V1PaginateToken {
            created_at: start,
            id: interval,
        })
        .to_string(),
        next_page_token: PaginateToken::V1(V1PaginateToken {
            created_at: end + interval,
            id: interval,
        })
        .to_string(),
    })
}

fn parse_token(token: &str) -> Result<(chrono::DateTime<Utc>, chrono::Duration), ErrorModel> {
    // ... don't get me started.. we have a quite flexible token format. We can pass arbitrary data
    // through the id field as long as it implements Display and TryFrom<&str>. Now, we'd really love
    // to pass a chrono::Duration through here. But chrono::Duration doesn't implement FromStr or
    // TryFrom<&str>. Bummer. So we resort to iso8601::Duration which offers a FromStr implementation.
    // Their Error type is String and incompatible with our TryFrom implementation which requires an
    // std::error::Error, so we now end up having our own Duration type RoundTrippableDuration which
    // wraps the iso8601::Duration and implements TryFrom<&str> and Display.
    // Funfunfun.
    let PaginateToken::V1(V1PaginateToken { created_at, id }): PaginateToken<
        RoundTrippableDuration,
    > = PaginateToken::try_from(token)?;

    match id.0 {
        iso8601::Duration::YMDHMS {
            year,
            month,
            day,
            hour,
            minute,
            second,
            millisecond,
        } => {
            if year != 0 || month != 0 {
                return Err(ErrorModel::bad_request(
                    "Invalid paginate token".to_string(),
                    "PaginateTokenParseError".to_string(),
                    None,
                ));
            }
            Ok((
                created_at,
                chrono::Duration::days(i64::from(day))
                    + chrono::Duration::hours(i64::from(hour))
                    + chrono::Duration::minutes(i64::from(minute))
                    + chrono::Duration::seconds(i64::from(second))
                    + chrono::Duration::milliseconds(i64::from(millisecond)),
            ))
        }
        iso8601::Duration::Weeks(w) => Ok((created_at, chrono::Duration::weeks(i64::from(w)))),
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, sync::Arc};

    use strum::IntoEnumIterator;

    use crate::{
        api::management::v1::warehouse::TabularDeleteProfile,
        implementations::postgres::PostgresStatisticsSink, service::authz::AllowAllAuthorizer,
        DEFAULT_PROJECT_ID,
    };

    #[sqlx::test]
    async fn test_can_insert_all_variants(pool: sqlx::PgPool) {
        let conn = pool.begin().await.unwrap();
        let (_api, warehouse) = crate::tests::setup(
            pool.clone(),
            crate::tests::test_io_profile(),
            None,
            AllowAllAuthorizer,
            TabularDeleteProfile::Hard {},
            None,
            None,
        )
        .await;

        let sink = PostgresStatisticsSink::new(pool);

        let project = DEFAULT_PROJECT_ID.unwrap();
        let status_code = http::StatusCode::OK;
        let count = 1;
        let ident = None;
        let warehouse_name = Some(warehouse.warehouse_name);
        let mut stats = HashMap::default();
        stats.insert(project, HashMap::default());
        let s = stats.get_mut(&project).unwrap();
        for uri in crate::api::endpoints::Endpoints::iter() {
            s.insert(
                crate::service::endpoint_statistics::EndpointIdentifier {
                    uri,
                    status_code,
                    warehouse: ident,
                    warehouse_name: warehouse_name.clone(),
                },
                count,
            );
        }
        sink.process_stats(Arc::new(stats)).await.unwrap();
        conn.commit().await.unwrap();
    }
}
