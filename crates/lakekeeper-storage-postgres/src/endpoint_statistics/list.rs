use std::{
    collections::HashSet,
    sync::{LazyLock, Mutex},
};

use chrono::Utc;
use iceberg_ext::catalog::rest::ErrorModel;
use itertools::{Itertools, izip};
use lakekeeper::{
    api::{
        endpoints::ServedEndpoint,
        management::v1::project::{
            EndpointStatistic, EndpointStatisticsResponse, TimeWindowSelector, WarehouseFilter,
        },
    },
    service::ArcProjectId,
    utils::time_conversion::iso_8601_duration_to_chrono,
};
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    dbutils::DBErrorHandler,
    pagination::{PaginateToken, V1PaginateToken},
};

pub(crate) async fn list_statistics(
    project: ArcProjectId,
    warehouse_filter: WarehouseFilter,
    status_codes: Option<&[u16]>,
    range_specifier: TimeWindowSelector,
    conn: &PgPool,
) -> lakekeeper::api::Result<EndpointStatisticsResponse> {
    let (until, interval) = match range_specifier {
        TimeWindowSelector::Window { end, interval } => (end, interval),
        TimeWindowSelector::PageToken { token } => parse_token(token.as_str())?,
    };

    let from = until - interval;

    let (warehouse_id, get_all, unmapped) = match warehouse_filter {
        WarehouseFilter::WarehouseId { id } => (Some(id), false, false),
        WarehouseFilter::Unmapped => (None, false, true),
        WarehouseFilter::All => (None, true, false),
    };
    let status_codes = status_codes.map(|s| s.iter().map(|i| i32::from(*i)).collect_vec());

    tracing::trace!(
        "Listing stats for project: '{project:?}', warehouse_id: '{warehouse_id:?}', returning full stats: '{get_all}', unmapped only: '{unmapped}', interval: {interval:?}, start: {from:?}, end: {until:?}",
    );

    let row = sqlx::query!(
        r#"
        SELECT es.timestamp,
               array_agg(es.matched_path) as "matched_path!: Vec<String>",
               array_agg(es.status_code) as "status_code!",
               array_agg(es.count) as "count!",
               array_agg(es.warehouse_id) as "warehouse_id!: Vec<Option<Uuid>>",
               array_agg(w.warehouse_name) as "warehouse_name!: Vec<Option<String>>",
               array_agg(es.created_at) as "created_at!",
               array_agg(es.updated_at) as "updated_at!: Vec<Option<chrono::DateTime<Utc>>>"
        FROM endpoint_statistics es
        LEFT JOIN warehouse w ON es.warehouse_id = w.warehouse_id
        WHERE es.project_id = $1
            AND (es.warehouse_id = $2 OR $3 OR (es.warehouse_id IS NULL AND $7))
            AND (es.status_code = ANY($4) OR $4 IS NULL)
            -- The interval functions are volatile. A scalar subquery is evaluated once, so the
            -- bounds become index conditions.
            AND es.timestamp >  (SELECT date_trunc(get_stats_interval_unit(), $5::timestamptz) + get_stats_interval())
            AND es.timestamp <= (SELECT date_trunc(get_stats_interval_unit(), $6::timestamptz) + get_stats_interval())
        group by es.timestamp
        order by es.timestamp desc
        "#,
        project.as_str(),
        warehouse_id,
        get_all,
        status_codes.as_deref(),
        from,
        until,
        unmapped,
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
            .filter_map(endpoint_statistic)
            .collect();

            (ts, row_stats)
        })
        .unzip();

    Ok(EndpointStatisticsResponse {
        timestamps,
        called_endpoints,
        previous_page_token: PaginateToken::V1(V1PaginateToken {
            created_at: from,
            id: interval,
        })
        .to_string(),
        next_page_token: PaginateToken::V1(V1PaginateToken {
            created_at: until + interval,
            id: interval,
        })
        .to_string(),
    })
}

type StatisticRow = (
    String,
    i32,
    i64,
    Option<Uuid>,
    Option<String>,
    chrono::DateTime<Utc>,
    Option<chrono::DateTime<Utc>>,
);

fn endpoint_statistic(
    (name, status_code, count, warehouse_id, warehouse_name, created_at, updated_at): StatisticRow,
) -> Option<EndpointStatistic> {
    let Some(endpoint) = ServedEndpoint::from_name(&name) else {
        warn_unknown_endpoint_name(&name);
        return None;
    };
    Some(EndpointStatistic {
        count,
        http_route: endpoint.http_route().into_owned(),
        status_code: status_code
            .clamp(i32::from(u16::MIN), i32::from(u16::MAX))
            .try_into()
            .expect("status code is valid since we just clamped it"),
        warehouse_id,
        warehouse_name,
        created_at,
        updated_at,
    })
}

/// Stored names come from newer servers after a rolling downgrade, or from endpoints this
/// server does not serve. They are left out of the listing; each is logged once.
fn warn_unknown_endpoint_name(name: &str) {
    static LOGGED: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(Mutex::default);
    let first = LOGGED
        .lock()
        .map_or(true, |mut logged| logged.insert(name.to_string()));
    if first {
        tracing::warn!(
            endpoint = name,
            "Endpoint statistics contain an endpoint this server does not serve. It is left out of the listing."
        );
    }
}

fn parse_token(token: &str) -> Result<(chrono::DateTime<Utc>, chrono::Duration), ErrorModel> {
    // We have a flexible token format that allows passing arbitrary data through the id field as long as it
    // implements Display and TryFrom<&str>. Ideally, we would like to pass a chrono::Duration through here.
    // However, chrono::Duration does not implement FromStr or TryFrom<&str>. Therefore, we use iso8601::Duration,
    // which offers a FromStr implementation. The Error type of iso8601::Duration is String and is incompatible
    // with our TryFrom implementation, which requires an std::error::Error. As a result, we created our own
    // Duration type, RoundTrippableDuration, which wraps iso8601::Duration and implements TryFrom<&str> and Display.
    // This approach ensures compatibility and functionality.
    let parsed: PaginateToken<iso8601::Duration> = PaginateToken::try_from(token)?;
    let (created_at, id) = parsed.v1_parts()?;
    let created_at = *created_at;

    Ok((
        created_at,
        iso_8601_duration_to_chrono(id).inspect_err(|e| {
            tracing::error!("Failed to parse duration from statistics page token: {e}");
        })?,
    ))
}
