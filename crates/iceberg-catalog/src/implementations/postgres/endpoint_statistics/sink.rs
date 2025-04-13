use std::{collections::HashMap, sync::Arc, time::Duration};

use fxhash::FxHashSet;
use itertools::Itertools;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::{
    api::endpoints::EndpointFlat,
    implementations::postgres::dbutils::DBErrorHandler,
    service::endpoint_statistics::{EndpointIdentifier, EndpointStatisticsSink},
    ProjectId,
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
    pool: sqlx::PgPool,
}

impl PostgresStatisticsSink {
    #[must_use]
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    #[allow(clippy::too_many_lines)]
    pub(super) async fn process_stats(
        &self,
        stats: Arc<HashMap<ProjectId, HashMap<EndpointIdentifier, i64>>>,
    ) -> crate::api::Result<()> {
        let mut conn = self.pool.begin().await.map_err(|e| {
            tracing::error!("Failed to start transaction: {e}");
            e.into_error_model("failed to start transaction")
        })?;

        tracing::debug!(
            "Resolving projects and warehouses for '{}' recorded unique project ids.",
            stats.len()
        );

        let resolved_projects = resolve_projects(&stats, &mut conn).await?;
        let warehouse_ids = resolve_warehouses(&stats, &mut conn).await?;

        let endpoint_calls_total = stats
            .iter()
            .filter_map(|(p, eps)| resolved_projects.contains(p).then_some(eps.len()))
            .sum::<usize>();

        tracing::debug!(
            "Preparing to insert '{endpoint_calls_total}' endpoint statistic datapoints across '{}' resolved projects, discarding stats from '{}' not existing projects.",
            resolved_projects.len(),
            stats.len() - resolved_projects.len()
        );

        let mut uris = Vec::with_capacity(endpoint_calls_total);
        let mut status_codes = Vec::with_capacity(endpoint_calls_total);
        let mut warehouses = Vec::with_capacity(endpoint_calls_total);
        let mut counts = Vec::with_capacity(endpoint_calls_total);
        let mut projects = Vec::with_capacity(endpoint_calls_total);

        for (project, endpoints) in stats.iter() {
            if !resolved_projects.contains(project) {
                tracing::debug!(
                    "Skipping recording stats for project: '{project}' since we couldn't resolve it."
                );
                continue;
            }
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
                projects.push(project.to_string());
                uris.push(EndpointFlat::from(*uri));
                status_codes.push(i32::from(status_code.as_u16()));
                counts.push(*count);

                let warehouse = warehouse
                    .as_deref()
                    .or_else(|| {
                        warehouse_name.as_deref().and_then(|wn| {
                            warehouse_ids.get(&(project.to_string(), wn.to_string()))
                        })
                    })
                    .copied();
                warehouses.push(warehouse);
            }
        }

        tracing::debug!("Inserting stats batch");

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
                                unnest($1::text[]) as project_id,
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

async fn resolve_projects(
    stats: &Arc<HashMap<ProjectId, HashMap<EndpointIdentifier, i64>>>,
    conn: &mut Transaction<'_, Postgres>,
) -> crate::api::Result<FxHashSet<ProjectId>> {
    let projects = stats.keys().map(ToString::to_string).collect_vec();
    tracing::debug!("Resolving '{}' project ids.", projects.len());
    let resolved_projects: FxHashSet<ProjectId> = sqlx::query!(
        r#"SELECT true as "exists!", project_id
               FROM project
               WHERE project_id = ANY($1::text[])"#,
        &projects
    )
    .fetch_all(&mut **conn)
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
                tracing::error!("Failed to parse project id from db: {:?}", e.error);
            })
            .ok()
            .flatten()
    })
    .collect::<_>();

    tracing::debug!("Resolved '{}' project ids.", resolved_projects.len());

    Ok(resolved_projects)
}

async fn resolve_warehouses(
    stats: &Arc<HashMap<ProjectId, HashMap<EndpointIdentifier, i64>>>,
    conn: &mut Transaction<'_, Postgres>,
) -> crate::api::Result<HashMap<(String, String), Uuid>> {
    let (projects, warehouse_idents): (Vec<_>, Vec<_>) = stats
        .iter()
        .flat_map(|(p, e)| {
            e.keys().filter_map(|epi| {
                epi.warehouse_name
                    .as_ref()
                    .map(|warehouse| (p.to_string(), warehouse.to_string()))
            })
        })
        .unique()
        .unzip();

    Ok(sqlx::query!(
        r#"SELECT project_id, warehouse_name, warehouse_id
               FROM warehouse
               WHERE (project_id, warehouse_name) IN (
                   SELECT unnest($1::text[]), unnest($2::text[])
               )"#,
        &projects,
        &warehouse_idents
    )
    .fetch_all(&mut **conn)
    .await
    .map_err(|e| {
        tracing::error!("Failed to fetch warehouse ids: {e}");
        e.into_error_model("failed to fetch warehouse ids")
    })?
    .into_iter()
    .map(|w| ((w.project_id, w.warehouse_name), w.warehouse_id))
    .collect::<HashMap<_, _>>())
}
