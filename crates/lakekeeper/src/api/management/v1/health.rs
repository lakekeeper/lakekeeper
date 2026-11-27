//! Table health: sizes and findings of the Iceberg tables in a warehouse.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures::FutureExt as _;
use serde::{Deserialize, Serialize};

use super::{
    ApiServer,
    tasks::{ListTasksRequest, TaskStatus, WarehouseTaskEntityFilter},
};
use crate::{
    WarehouseId,
    api::{
        ApiContext, RequestMetadata, Result,
        iceberg::{types::PageToken, v1::PaginationQuery},
    },
    server::UnfilteredPage,
    service::{
        ArcResolvedWarehouse, CatalogNamespaceOps, CatalogStore, CatalogTableHealthOps,
        CatalogTabularOps, CatalogTaskOps, CatalogWarehouseOps, NamespaceHierarchy, SecretStore,
        State, TableId, TableInfo, TabularId, TabularListFlags, Transaction, ViewOrTableInfo,
        authz::{
            ActionOnTable, AuthZError, AuthZTableOps, Authorizer, AuthzWarehouseOps,
            CatalogTableAction, CatalogWarehouseAction,
        },
        events::{
            APIEventContext,
            context::{
                TableActionGetTableHealth, WarehouseActionListTableHealth, authz_to_error_no_audit,
            },
        },
        tasks::{TaskFilter, TaskId, TaskInfo, table_health_queue::QUEUE_NAME},
    },
};

/// Sort order of the table health ranking.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub enum TableHealthOrderBy {
    /// Highest severity first, then most live data bytes.
    #[default]
    Severity,
}

#[derive(Debug, Deserialize, Default)]
#[cfg_attr(feature = "open-api", derive(utoipa::IntoParams))]
#[serde(rename_all = "camelCase")]
pub struct ListTableHealthQuery {
    /// Sort order. Default: `severity`
    #[serde(default)]
    pub order_by: TableHealthOrderBy,
    /// Include soft-deleted tables. Default: false
    #[serde(default)]
    pub include_soft_deleted: bool,
    /// Next page token
    #[serde(default)]
    pub page_token: Option<String>,
    /// Signals an upper bound of the number of results that a client will receive.
    /// Default: 100
    #[serde(default)]
    pub page_size: Option<i64>,
}

impl ListTableHealthQuery {
    #[must_use]
    pub fn pagination_query(&self) -> PaginationQuery {
        PaginationQuery {
            page_token: self
                .page_token
                .clone()
                .map_or(PageToken::Empty, PageToken::Present),
            page_size: self.page_size,
        }
    }
}

/// Whether the last run measured the table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub enum TableHealthStatus {
    /// The table was measured. Sizes and findings are set.
    Measured,
    /// The table cannot be measured. `not-measurable-reason` says why. Sizes are not set.
    NotMeasurable,
    /// The table has no result yet. Only the table health detail returns this status.
    NotMeasured,
}

/// Why a table cannot be measured.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case", tag = "reason")]
pub enum NotMeasurableReason {
    /// The table is encrypted.
    Encrypted,
    /// The scan ran over the run time limit.
    TimeLimit,
    /// A data or delete file in the current snapshot has no file size.
    MissingFileSize {
        /// The file without a size
        path: String,
    },
    /// A manifest list or manifest cannot be read.
    MetadataUnreadable {
        /// The file that cannot be read
        path: String,
    },
    /// The metadata file, the manifest list or a manifest does not exist.
    MetadataMissing {
        /// The missing file
        path: String,
    },
}

/// Severity of a finding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub enum FindingSeverity {
    Info,
    Warning,
    Critical,
}

/// One table in the health ranking of a warehouse.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct TableHealthSummary {
    #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
    pub table_id: TableId,
    /// Namespace parts of the table
    pub namespace: Vec<String>,
    /// Name of the table
    pub name: String,
    /// `measured` or `not-measurable`
    pub status: TableHealthStatus,
    /// Set if `status` is `not-measurable`
    pub not_measurable_reason: Option<NotMeasurableReason>,
    /// Highest severity of the table's findings. `warning` for a table that cannot be measured.
    /// Not set if the table has no findings.
    pub max_severity: Option<FindingSeverity>,
    pub finding_count: i32,
    /// Bytes of the data files of the current snapshot. Not set if `status` is `not-measurable`.
    pub live_data_bytes: Option<i64>,
    /// Bytes of the delete files of the current snapshot. Not set if `status` is
    /// `not-measurable`.
    pub live_delete_bytes: Option<i64>,
    /// Bytes of the metadata files of the current snapshot. Not set if `status` is
    /// `not-measurable`.
    pub live_metadata_bytes: Option<i64>,
    /// End of the last run
    pub measured_at: DateTime<Utc>,
    /// Version of the measurement code that produced these values
    pub measurement_version: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ListTableHealthResponse {
    pub tables: Vec<TableHealthSummary>,
    /// Token to fetch the next page
    pub next_page_token: Option<String>,
}

/// A problem health found in a table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct TableHealthFinding {
    /// Kind of the finding, e.g. `manifest-length-mismatch`
    pub kind: String,
    pub severity: FindingSeverity,
    /// Kind-specific data that shows the problem
    #[serde(default)]
    pub evidence: serde_json::Value,
    /// The values the decision used
    #[serde(default)]
    pub thresholds: serde_json::Value,
}

/// The `table_health` task of a table.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct TableHealthTask {
    /// The active task. Not set if no task is scheduled or running.
    #[cfg_attr(feature = "open-api", schema(value_type = Option<uuid::Uuid>))]
    pub task_id: Option<TaskId>,
    /// Status of the active task
    pub status: Option<TaskStatus>,
    /// When the active task runs
    pub scheduled_for: Option<DateTime<Utc>>,
    /// Outcome of the last finished attempt
    pub last_outcome: Option<TaskStatus>,
}

impl TableHealthTask {
    /// Reads the active task and the last finished attempt from the `table_health` tasks of one
    /// table, newest first.
    fn from_tasks(tasks: &[TaskInfo]) -> Self {
        let is_active = |t: &&TaskInfo| t.status.split().0.is_some();
        let active = tasks.iter().find(is_active);
        let finished = tasks.iter().find(|t| !is_active(t));
        Self {
            task_id: active.map(TaskInfo::task_id),
            status: active.map(|t| t.status),
            scheduled_for: active.map(|t| t.task_metadata.scheduled_for),
            last_outcome: finished.map(|t| t.status),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct GetTableHealthResponse {
    #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
    pub table_id: TableId,
    /// Namespace parts of the table
    pub namespace: Vec<String>,
    /// Name of the table
    pub name: String,
    pub status: TableHealthStatus,
    /// Set if `status` is `not-measurable`
    pub not_measurable_reason: Option<NotMeasurableReason>,
    /// Highest severity of the table's findings. `warning` for a table that cannot be measured.
    /// Not set if the table has no findings.
    pub max_severity: Option<FindingSeverity>,
    pub finding_count: i32,
    /// Bytes of the data files of the current snapshot. Set if `status` is `measured`.
    pub live_data_bytes: Option<i64>,
    /// Bytes of the delete files of the current snapshot. Set if `status` is `measured`.
    pub live_delete_bytes: Option<i64>,
    /// Bytes of the metadata files of the current snapshot. Set if `status` is `measured`.
    pub live_metadata_bytes: Option<i64>,
    /// End of the last run. Not set if `status` is `not-measured`.
    pub measured_at: Option<DateTime<Utc>>,
    /// Version of the measurement code that produced these values. Not set if `status` is
    /// `not-measured`.
    pub measurement_version: Option<i32>,
    pub findings: Vec<TableHealthFinding>,
    /// Duration of the last run
    pub scan_duration_ms: Option<i64>,
    /// Metadata file that was measured
    pub metadata_location: Option<String>,
    /// Current snapshot that was measured. Not set for a table without a snapshot or a table
    /// that cannot be measured.
    pub snapshot_id: Option<i64>,
    /// Number of manifests read. Set if `status` is `measured`.
    pub manifests_read: Option<i64>,
    /// Earliest time of the next run. Not set if `status` is `not-measured`.
    pub next_not_before: Option<DateTime<Utc>>,
    /// Only set if the caller may see the table's tasks
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task: Option<TableHealthTask>,
}

impl GetTableHealthResponse {
    /// The response for a table without a result.
    fn not_measured(table: &TableInfo) -> Self {
        Self {
            table_id: table.tabular_id,
            namespace: table.tabular_ident.namespace.clone().inner(),
            name: table.tabular_ident.name.clone(),
            status: TableHealthStatus::NotMeasured,
            not_measurable_reason: None,
            max_severity: None,
            finding_count: 0,
            live_data_bytes: None,
            live_delete_bytes: None,
            live_metadata_bytes: None,
            measured_at: None,
            measurement_version: None,
            findings: vec![],
            scan_duration_ms: None,
            metadata_location: None,
            snapshot_id: None,
            manifests_read: None,
            next_not_before: None,
            task: None,
        }
    }
}

/// The part of the health totals of a warehouse that comes from soft-deleted tables.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct SoftDeletedHealthTotals {
    /// Number of soft-deleted Iceberg tables
    pub tables: i64,
    pub live_data_bytes: i64,
    pub live_delete_bytes: i64,
    pub live_metadata_bytes: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct MeasurementVersionCount {
    pub version: i32,
    pub tables: i64,
}

/// Health totals of the Iceberg tables of a warehouse. Sums include soft-deleted tables.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct WarehouseHealthTotalsResponse {
    /// Number of Iceberg tables, soft-deleted tables included
    pub tables: i64,
    pub tables_measured: i64,
    pub tables_not_measurable: i64,
    /// Tables never measured or not measurable
    pub tables_without_value: i64,
    /// Sum over the measured tables
    pub live_data_bytes: i64,
    /// Sum over the measured tables
    pub live_delete_bytes: i64,
    /// Sum over the measured tables
    pub live_metadata_bytes: i64,
    /// The part of the counts and sums that comes from soft-deleted tables
    pub soft_deleted: SoftDeletedHealthTotals,
    /// Measurement versions in use, with the number of tables measured by each
    pub measurement_versions: Vec<MeasurementVersionCount>,
}

impl<C: CatalogStore, A: Authorizer + Clone, S: SecretStore> TableHealthService<C, A, S>
    for ApiServer<C, A, S>
{
}

#[async_trait::async_trait]
pub trait TableHealthService<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>
where
    Self: Send + Sync + 'static,
{
    async fn list_table_health(
        warehouse_id: WarehouseId,
        query: ListTableHealthQuery,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ListTableHealthResponse> {
        // ------------------- AuthZ -------------------
        let catalog = context.v1_state.catalog;
        let authorizer = context.v1_state.authz;

        let event_ctx = APIEventContext::for_warehouse(
            Arc::new(request_metadata),
            context.v1_state.events.clone(),
            warehouse_id,
            WarehouseActionListTableHealth {},
        );
        let warehouse = C::get_active_warehouse_by_id(warehouse_id, catalog.clone()).await;
        let authz_result = authorizer
            .require_warehouse_action(
                event_ctx.request_metadata(),
                warehouse_id,
                warehouse,
                CatalogWarehouseAction::Use,
            )
            .await;
        let (event_ctx, warehouse) = event_ctx.emit_authz(authz_result)?;

        // ------------- Business Logic & table level AuthZ filter -------------
        let ListTableHealthQuery {
            order_by,
            include_soft_deleted,
            ..
        } = query;
        let pagination_query = query.pagination_query();
        let request_metadata = event_ctx.request_metadata().clone();
        let (tables, _, next_page_token) = crate::server::fetch_until_full_page::<_, _, _, C>(
            pagination_query.page_size,
            pagination_query.page_token,
            catalog.clone(),
            |page_size, page_token, t| {
                async move {
                    C::list_table_health(
                        warehouse_id,
                        order_by,
                        include_soft_deleted,
                        PaginationQuery {
                            page_size: Some(page_size),
                            page_token: page_token.into(),
                        },
                        t.transaction(),
                    )
                    .await
                }
                .boxed()
            },
            |page_size, page| {
                let authorizer = authorizer.clone();
                let request_metadata = request_metadata.clone();
                let warehouse = warehouse.clone();
                let catalog = catalog.clone();
                async move {
                    let (ids, summaries, tokens): (Vec<_>, Vec<_>, Vec<_>) =
                        itertools::multiunzip(page.into_iter_with_page_tokens());
                    let allowed = readable_tables::<C, A>(
                        &authorizer,
                        &request_metadata,
                        &warehouse,
                        &ids,
                        include_soft_deleted,
                        catalog,
                    )
                    .await?;
                    Ok(UnfilteredPage::new(
                        summaries,
                        ids,
                        tokens,
                        allowed,
                        page_size
                            .clamp(0, i64::MAX)
                            .try_into()
                            .expect("We clamped."),
                    ))
                }
                .boxed()
            },
        )
        .await?;

        Ok(ListTableHealthResponse {
            tables,
            next_page_token,
        })
    }

    async fn get_table_health(
        warehouse_id: WarehouseId,
        table_id: TableId,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<GetTableHealthResponse> {
        // ------------------- AuthZ -------------------
        let catalog = context.v1_state.catalog;
        let authorizer = context.v1_state.authz;

        let event_ctx = APIEventContext::for_table(
            Arc::new(request_metadata),
            context.v1_state.events.clone(),
            warehouse_id,
            table_id,
            TableActionGetTableHealth {},
        );
        let authz_result =
            authorize_get_table_health::<C, A>(&authorizer, &event_ctx, catalog.clone()).await;
        let (_event_ctx, (warehouse, table, can_see_tasks)) = event_ctx.emit_authz(authz_result)?;

        // ------------------- Business Logic -------------------
        let health = C::get_table_health(warehouse_id, table_id, catalog.clone()).await?;
        let mut response = health.unwrap_or_else(|| GetTableHealthResponse::not_measured(&table));
        if can_see_tasks {
            let filter = TaskFilter::WarehouseId {
                warehouse_id,
                project_id: warehouse.project_id.clone(),
            };
            // A table has at most one active task per queue, and it is the newest. The list has
            // one finished row per task, so two rows hold the active task and the last finished
            // attempt.
            let query = ListTasksRequest::builder()
                .queue_name(Some(vec![QUEUE_NAME.clone()]))
                .entities(Some(vec![WarehouseTaskEntityFilter::Table { table_id }]))
                .page_size(Some(2))
                .build();
            let mut t = C::Transaction::begin_read(catalog).await?;
            let tasks = C::list_tasks(&filter, &query, t.transaction()).await?;
            t.commit().await?;
            response.task = Some(TableHealthTask::from_tasks(&tasks.tasks));
        }
        Ok(response)
    }
}

/// Which of `table_ids` the caller may read the metadata of. The health of a table shows its
/// sizes and problems, so the ranking checks `GetMetadata`, not `IncludeInList`, and
/// `ListEverything` on the warehouse doesn't skip the check. A table that is gone, or whose
/// namespace is gone, is not readable.
async fn readable_tables<C: CatalogStore, A: Authorizer>(
    authorizer: &A,
    request_metadata: &RequestMetadata,
    warehouse: &ArcResolvedWarehouse,
    table_ids: &[TableId],
    include_soft_deleted: bool,
    catalog: C::State,
) -> Result<Vec<bool>> {
    let flags = if include_soft_deleted {
        TabularListFlags::all()
    } else {
        TabularListFlags::active()
    };
    let tabular_ids = table_ids
        .iter()
        .copied()
        .map(TabularId::Table)
        .collect::<Vec<_>>();
    let tables =
        C::get_tabular_infos_by_id(warehouse.warehouse_id, &tabular_ids, flags, catalog.clone())
            .await?
            .into_iter()
            .filter_map(ViewOrTableInfo::into_table_info)
            .map(|info| (info.tabular_id, info))
            .collect::<std::collections::HashMap<_, _>>();
    let namespace_ids = tables
        .values()
        .map(|table| table.namespace_id)
        .collect::<Vec<_>>();
    let namespaces =
        C::get_namespaces_by_id(warehouse.warehouse_id, &namespace_ids, catalog).await?;

    // Index into `table_ids` of every table that can be checked.
    let (indices, actions): (Vec<_>, Vec<_>) = table_ids
        .iter()
        .enumerate()
        .filter_map(|(index, table_id)| {
            let table = tables.get(table_id)?;
            let namespace = namespaces.get(&table.namespace_id)?;
            Some((
                index,
                (
                    namespace,
                    ActionOnTable {
                        info: table,
                        action: CatalogTableAction::GetMetadata,
                        user: None,
                        is_delegated_execution: false,
                    },
                ),
            ))
        })
        .unzip();
    let decisions = authorizer
        .are_allowed_table_actions_vec(request_metadata, warehouse, &namespaces, &actions)
        .await
        .map_err(authz_to_error_no_audit)?
        .into_allowed();

    let mut allowed = vec![false; table_ids.len()];
    for (index, decision) in indices.into_iter().zip(decisions) {
        allowed[index] = decision;
    }
    Ok(allowed)
}

/// Requires `GetMetadata` on the table, and says whether the caller may also see its tasks:
/// `GetTasks` on the table or `GetAllTasks` on the warehouse, as for the task details.
async fn authorize_get_table_health<C: CatalogStore, A: Authorizer>(
    authorizer: &A,
    event_ctx: &APIEventContext<
        crate::service::events::context::UserProvidedTable,
        crate::service::events::context::Unresolved,
        TableActionGetTableHealth,
    >,
    catalog: C::State,
) -> std::result::Result<(ArcResolvedWarehouse, TableInfo, bool), AuthZError> {
    let (warehouse, namespace, table): (ArcResolvedWarehouse, NamespaceHierarchy, TableInfo) =
        authorizer
            .load_and_authorize_table_operation::<C>(
                event_ctx.request_metadata(),
                event_ctx.user_provided_entity(),
                TabularListFlags::all(),
                CatalogTableAction::GetMetadata,
                catalog,
            )
            .await?;

    let can_get_all_tasks = authorizer
        .is_allowed_warehouse_action(
            event_ctx.request_metadata(),
            None,
            &warehouse,
            CatalogWarehouseAction::GetAllTasks,
        )
        .await?
        .into_inner();
    let can_see_tasks = can_get_all_tasks
        || authorizer
            .is_allowed_table_action(
                event_ctx.request_metadata(),
                None,
                &warehouse,
                &namespace,
                &table,
                CatalogTableAction::GetTasks,
            )
            .await?
            .into_inner();
    Ok((warehouse, table, can_see_tasks))
}
