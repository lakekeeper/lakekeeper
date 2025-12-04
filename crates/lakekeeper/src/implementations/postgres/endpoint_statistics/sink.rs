use std::{collections::HashMap, sync::Arc, time::Duration};

use fxhash::FxHashSet;
use itertools::Itertools;
use uuid::Uuid;

use crate::{
    ProjectId,
    api::endpoints::EndpointFlat,
    implementations::postgres::dbutils::DBErrorHandler,
    service::endpoint_statistics::{EndpointIdentifier, EndpointStatisticsSink},
};

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

#[derive(Debug)]
pub struct PostgresStatisticsSink {
    read_pool: sqlx::PgPool,
    write_pool: sqlx::PgPool,
}

impl PostgresStatisticsSink {
    #[must_use]
    pub fn new(read_pool: sqlx::PgPool, write_pool: sqlx::PgPool) -> Self {
        Self {
            read_pool,
            write_pool,
        }
    }

    #[allow(clippy::too_many_lines)]
    pub(super) async fn process_stats(
        &self,
        stats: Arc<HashMap<ProjectId, HashMap<EndpointIdentifier, i64>>>,
    ) -> crate::api::Result<()> {
        tracing::debug!(
            "Resolving projects and warehouses for '{}' recorded unique project ids.",
            stats.len()
        );

        let resolved_projects = resolve_projects(&stats, &self.read_pool).await?;
        let warehouse_ids = resolve_warehouses(&stats, &self.read_pool).await?;

        let endpoint_calls_total = stats
            .iter()
            .filter_map(|(p, eps)| resolved_projects.contains(p).then_some(eps.len()))
            .sum::<usize>();

        tracing::debug!(
            "Processing up to '{endpoint_calls_total}' endpoint statistic datapoints across '{}' resolved projects, discarding stats from '{}' not existing projects.",
            resolved_projects.len(),
            stats.len() - resolved_projects.len()
        );

        // Aggregate stats to prevent duplicate constraint violations
        // Key: (project_id, warehouse_id, matched_path, status_code)
        let mut aggregated_stats: HashMap<(String, Option<Uuid>, EndpointFlat, i32), i64> =
            HashMap::new();

        for (project, endpoints) in stats.iter() {
            if !resolved_projects.contains(project) {
                tracing::warn!(
                    "Skipping recording stats for project: '{project}' since we couldn't resolve it."
                );
                continue;
            }
            tracing::trace!("Processing stats for project: {project}");

            let project_str = project.to_string();
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
                let uri_flat = EndpointFlat::from(*uri);
                let status_code_i32 = i32::from(status_code.as_u16());

                let resolved_warehouse = warehouse.as_ref().map(|w| **w).or_else(|| {
                    warehouse_name
                        .as_deref()
                        .and_then(|wn| warehouse_ids.get(&(project_str.clone(), wn.to_string())))
                        .copied()
                });

                let key = (
                    project_str.clone(),
                    resolved_warehouse,
                    uri_flat,
                    status_code_i32,
                );
                *aggregated_stats.entry(key).or_insert(0) += count;
            }
        }

        let final_count = aggregated_stats.len();
        let mut uris = Vec::with_capacity(final_count);
        let mut status_codes = Vec::with_capacity(final_count);
        let mut warehouses = Vec::with_capacity(final_count);
        let mut counts = Vec::with_capacity(final_count);
        let mut projects = Vec::with_capacity(final_count);

        for ((project, warehouse, uri, status_code), count) in aggregated_stats {
            projects.push(project);
            warehouses.push(warehouse);
            uris.push(uri);
            status_codes.push(status_code);
            counts.push(count);
        }

        tracing::debug!(
            "Inserting '{final_count}' aggregated stats records (reduced from '{endpoint_calls_total}' raw datapoints)"
        );

        sqlx::query!(r#"INSERT INTO endpoint_statistics (project_id, warehouse_id, matched_path, status_code, count, timestamp)
                        SELECT
                            project_id,
                            warehouse,
                            uri,
                            status_code,
                            cnt,
                            get_stats_date_default()
                        FROM unnest(
                                $1::text[],
                                $2::UUID[],
                                $3::api_endpoints[],
                                $4::INT[],
                                $5::BIGINT[]
                            ) AS u(project_id, warehouse, uri, status_code, cnt)
                        ON CONFLICT (project_id, warehouse_id, matched_path, status_code, timestamp)
                            DO UPDATE SET count = endpoint_statistics.count + EXCLUDED.count"#,
                projects.as_slice(),
                warehouses.as_slice() as _,
                &uris as _,
                &status_codes,
                &counts
            ).execute(&self.write_pool).await.map_err(|e| {
            tracing::error!("Failed to insert stats: {e}, lost stats: {stats:?}");
            e.into_error_model("failed to insert stats")
        })?;

        Ok(())
    }
}

async fn resolve_projects<'c, 'e: 'c, E: sqlx::Executor<'c, Database = sqlx::Postgres>>(
    stats: &Arc<HashMap<ProjectId, HashMap<EndpointIdentifier, i64>>>,
    conn: E,
) -> crate::api::Result<FxHashSet<ProjectId>> {
    let projects = stats.keys().map(ToString::to_string).collect_vec();
    tracing::debug!("Resolving '{}' project ids.", projects.len());
    let resolved_projects: FxHashSet<ProjectId> = sqlx::query!(
        r#"SELECT true as "exists!", project_id
               FROM project
               WHERE project_id = ANY($1::text[])"#,
        &projects
    )
    .fetch_all(conn)
    .await
    .map_err(|e| {
        tracing::error!("Failed to fetch project ids: {e}");
        e.into_error_model("failed to fetch project ids")
    })?
    .into_iter()
    .filter_map(|p| {
        p.exists
            .then_some(ProjectId::try_new(p.project_id))
            .transpose()
            .inspect_err(|e| {
                tracing::error!("Failed to parse project id from db: {e}");
            })
            .ok()
            .flatten()
    })
    .collect::<_>();

    tracing::debug!("Resolved '{}' project ids.", resolved_projects.len());

    Ok(resolved_projects)
}

async fn resolve_warehouses<'c, 'e: 'c, E: sqlx::Executor<'c, Database = sqlx::Postgres>>(
    stats: &Arc<HashMap<ProjectId, HashMap<EndpointIdentifier, i64>>>,
    conn: E,
) -> crate::api::Result<HashMap<(String, String), Uuid>> {
    let (projects, warehouse_idents): (Vec<_>, Vec<_>) = stats
        .iter()
        .flat_map(|(p, e)| {
            e.keys().filter_map(|epi| {
                epi.warehouse_name
                    .as_ref()
                    .map(|warehouse| (p.to_string(), warehouse.clone()))
            })
        })
        .unique()
        .unzip();

    Ok(sqlx::query!(
        r#"SELECT project_id, warehouse_name, warehouse_id
               FROM warehouse
               WHERE (project_id, warehouse_name) IN (
                   Select * FROM unnest($1::text[], $2::text[]) as u(project_id, warehouse_name)
               )"#,
        &projects,
        &warehouse_idents
    )
    .fetch_all(conn)
    .await
    .map_err(|e| {
        tracing::error!("Failed to fetch warehouse ids: {e}");
        e.into_error_model("failed to fetch warehouse ids")
    })?
    .into_iter()
    .map(|w| ((w.project_id, w.warehouse_name), w.warehouse_id))
    .collect::<HashMap<_, _>>())
}
