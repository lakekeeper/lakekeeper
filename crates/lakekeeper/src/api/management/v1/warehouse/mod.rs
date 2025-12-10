mod undrop;

use std::sync::Arc;

use futures::FutureExt;
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use super::{DeleteWarehouseQuery, ProtectionResponse};
pub use crate::service::{
    WarehouseStatus,
    storage::{
        AdlsProfile, AzCredential, GcsCredential, GcsProfile, GcsServiceKey, S3Credential,
        S3Profile, StorageCredential, StorageProfile,
    },
};
use crate::{
    ProjectId, WarehouseId,
    api::{
        ApiContext, Result,
        iceberg::v1::{PageToken, PaginationQuery},
        management::v1::{
            ApiServer, DeletedTabularResponse, GetWarehouseStatisticsQuery,
            ListDeletedTabularsResponse,
        },
    },
    request_metadata::RequestMetadata,
    server::UnfilteredPage,
    service::{
        CachePolicy, CatalogNamespaceOps, CatalogStore, CatalogTabularOps, CatalogTaskOps,
        CatalogWarehouseOps, NamespaceId, State, TabularId, TabularListFlags, Transaction,
        ViewOrTableDeletionInfo,
        authz::{
            AuthZCannotUseWarehouseId, AuthZProjectOps, AuthZTableOps,
            AuthZWarehouseActionForbidden, Authorizer, AuthzNamespaceOps, AuthzWarehouseOps,
            CatalogNamespaceAction, CatalogProjectAction, CatalogTableAction, CatalogViewAction,
            CatalogWarehouseAction,
        },
        require_namespace_for_tabular,
        secrets::SecretStore,
        tasks::{TaskFilter, TaskQueueName, tabular_expiration_queue::TabularExpirationTask},
    },
};

#[derive(Debug, Deserialize, Default)]
#[cfg_attr(feature = "open-api", derive(utoipa::IntoParams))]
#[serde(rename_all = "camelCase")]
pub struct ListDeletedTabularsQuery {
    /// Filter by Namespace ID
    #[serde(default)]
    #[cfg_attr(feature = "open-api", param(value_type=uuid::Uuid))]
    pub namespace_id: Option<NamespaceId>,
    /// Next page token
    #[serde(default)]
    pub page_token: Option<String>,
    /// Signals an upper bound of the number of results that a client will receive.
    /// Default: 100
    #[serde(default)]
    pub page_size: Option<i64>,
}

impl ListDeletedTabularsQuery {
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, TypedBuilder)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct CreateWarehouseRequest {
    /// Name of the warehouse to create. Must be unique
    /// within a project and may not contain "/"
    pub warehouse_name: String,
    /// Project ID in which to create the warehouse.
    /// Deprecated: Please use the `x-project-id` header instead.
    #[cfg_attr(feature = "open-api", schema(value_type=Option::<String>))]
    #[builder(default, setter(strip_option))]
    pub project_id: Option<ProjectId>,
    /// Storage profile to use for the warehouse.
    pub storage_profile: StorageProfile,
    /// Optional storage credential to use for the warehouse.
    #[builder(default, setter(strip_option))]
    pub storage_credential: Option<StorageCredential>,
    /// Profile to determine behavior upon dropping of tabulars. Default: hard deletion.
    #[serde(default)]
    #[builder(default)]
    pub delete_profile: TabularDeleteProfile,
}

#[derive(Debug, Clone, PartialEq, Eq, Copy, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case", tag = "type")]
pub enum TabularDeleteProfile {
    #[cfg_attr(feature = "open-api", schema(title = "TabularDeleteProfileHard"))]
    Hard {},
    #[cfg_attr(feature = "open-api", schema(title = "TabularDeleteProfileSoft"))]
    #[serde(rename_all = "kebab-case")]
    Soft {
        #[serde(
            deserialize_with = "seconds_to_duration",
            serialize_with = "duration_to_seconds",
            alias = "expiration_seconds"
        )]
        #[cfg_attr(feature = "open-api", schema(value_type=i64))]
        expiration_seconds: chrono::Duration,
    },
}

fn seconds_to_duration<'de, D>(deserializer: D) -> Result<chrono::Duration, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let buf = i64::deserialize(deserializer)?;

    Ok(chrono::Duration::seconds(buf))
}

fn duration_to_seconds<S>(duration: &chrono::Duration, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_i64(duration.num_seconds())
}

impl TabularDeleteProfile {
    #[must_use]
    pub fn expiration_seconds(&self) -> Option<chrono::Duration> {
        match self {
            Self::Soft { expiration_seconds } => Some(*expiration_seconds),
            Self::Hard {} => None,
        }
    }
}

impl Default for TabularDeleteProfile {
    fn default() -> Self {
        Self::Hard {}
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(transparent)]
pub struct CreateWarehouseResponse(GetWarehouseResponse);
impl CreateWarehouseResponse {
    #[must_use]
    pub fn warehouse_id(&self) -> WarehouseId {
        self.0.warehouse_id
    }

    #[must_use]
    pub fn project_id(&self) -> ProjectId {
        self.0.project_id.clone()
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct UpdateWarehouseStorageRequest {
    /// Storage profile to use for the warehouse.
    /// The new profile must point to the same location as the existing profile
    /// to avoid data loss. For S3 this means that you may not change the
    /// bucket, key prefix, or region.
    pub storage_profile: StorageProfile,
    /// Optional storage credential to use for the warehouse.
    /// The existing credential is not re-used. If no credential is
    /// provided, we assume that this storage does not require credentials.
    #[serde(default)]
    pub storage_credential: Option<StorageCredential>,
}

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::IntoParams))]
#[serde(rename_all = "camelCase")]
pub struct ListWarehousesRequest {
    /// Optional filter to return only warehouses
    /// with the specified status.
    /// If not provided, only active warehouses are returned.
    #[serde(default)]
    #[cfg_attr(feature = "open-api", param(nullable = false, required = false))]
    pub warehouse_status: Option<Vec<WarehouseStatus>>,
    /// The project ID to list warehouses for.
    /// Deprecated: Please use the `x-project-id` header instead.
    #[serde(default)]
    #[cfg_attr(feature = "open-api", param(value_type=Option::<String>))]
    pub project_id: Option<ProjectId>,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct RenameWarehouseRequest {
    /// New name for the warehouse.
    pub new_name: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct UpdateWarehouseDeleteProfileRequest {
    pub delete_profile: TabularDeleteProfile,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct RenameProjectRequest {
    /// New name for the project.
    pub new_name: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct GetWarehouseResponse {
    /// ID of the warehouse.
    #[cfg_attr(feature = "open-api", schema(value_type=uuid::Uuid))]
    #[deprecated(since = "0.11.0", note = "Please use 'warehouse_id' field instead.")]
    pub id: WarehouseId,
    /// ID of the warehouse.
    #[cfg_attr(feature = "open-api", schema(value_type=uuid::Uuid))]
    pub warehouse_id: WarehouseId,
    /// Name of the warehouse.
    pub name: String,
    /// Project ID in which the warehouse was created.
    #[cfg_attr(feature = "open-api", schema(value_type=String))]
    pub project_id: ProjectId,
    /// Storage profile used for the warehouse.
    pub storage_profile: StorageProfile,
    /// Delete profile used for the warehouse.
    pub delete_profile: TabularDeleteProfile,
    /// Whether the warehouse is active.
    pub status: WarehouseStatus,
    /// Whether the warehouse is protected from being deleted.
    pub protected: bool,
    /// Last updated timestamp.
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ListWarehousesResponse {
    /// List of warehouses in the project.
    pub warehouses: Vec<GetWarehouseResponse>,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct UpdateWarehouseCredentialRequest {
    /// New storage credential to use for the warehouse.
    /// If not specified, the existing credential is removed.
    pub new_storage_credential: Option<StorageCredential>,
}

impl axum::response::IntoResponse for CreateWarehouseResponse {
    fn into_response(self) -> axum::http::Response<axum::body::Body> {
        (http::StatusCode::CREATED, axum::Json(self)).into_response()
    }
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct WarehouseStatistics {
    /// Timestamp of when these statistics are valid until
    ///
    /// We lazily create a new statistics entry every hour, in between hours, the existing entry
    /// is being updated. If there's a change at `created_at` + 1 hour, a new entry is created. If
    /// there's no change, no new entry is created.
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Number of tables in the warehouse.
    pub number_of_tables: i64, // silly but necessary due to sqlx wanting i64, not usize
    /// Number of views in the warehouse.
    pub number_of_views: i64,
    /// Timestamp of when these statistics were last updated
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct WarehouseStatisticsResponse {
    /// ID of the warehouse for which the stats were collected.
    pub warehouse_ident: uuid::Uuid,
    /// Ordered list of warehouse statistics.
    pub stats: Vec<WarehouseStatistics>,
    /// Next page token
    pub next_page_token: Option<String>,
}

#[derive(Deserialize, Debug)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct UndropTabularsRequest {
    /// Tabulars to undrop
    pub targets: Vec<TabularId>,
}

impl<C: CatalogStore, A: Authorizer + Clone, S: SecretStore> Service<C, A, S>
    for ApiServer<C, A, S>
{
}

#[async_trait::async_trait]
pub trait Service<C: CatalogStore, A: Authorizer, S: SecretStore> {
    async fn create_warehouse(
        request: CreateWarehouseRequest,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<CreateWarehouseResponse> {
        let CreateWarehouseRequest {
            warehouse_name,
            project_id,
            mut storage_profile,
            storage_credential,
            delete_profile,
        } = request;
        let project_id = request_metadata.require_project_id(project_id)?;

        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;
        authorizer
            .require_project_action(
                &request_metadata,
                &project_id,
                CatalogProjectAction::CreateWarehouse,
            )
            .await?;

        // ------------------- Business Logic -------------------
        validate_warehouse_name(&warehouse_name)?;
        storage_profile.normalize(storage_credential.as_ref())?;

        // Run validation and overlap check in parallel
        let validation_future =
            storage_profile.validate_access(storage_credential.as_ref(), None, &request_metadata);
        let overlap_check_future = async {
            let warehouses =
                C::list_warehouses(&project_id, None, context.v1_state.catalog.clone()).await?;

            for w in &warehouses {
                if storage_profile.is_overlapping_location(&w.storage_profile) {
                    return Err::<_, IcebergErrorResponse>(
                        ErrorModel::bad_request(
                            format!(
                                "Storage profile overlaps with existing warehouse {}",
                                w.name
                            ),
                            "CreateWarehouseStorageProfileOverlap",
                            None,
                        )
                        .into(),
                    );
                }
            }

            Ok(())
        };

        let (validation_result, overlap_result) =
            tokio::join!(validation_future, overlap_check_future);

        // Check results from both operations
        validation_result?;
        overlap_result?;

        let mut transaction = C::Transaction::begin_write(context.v1_state.catalog).await?;
        let secret_id = if let Some(storage_credential) = storage_credential {
            Some(
                context
                    .v1_state
                    .secrets
                    .create_storage_secret(storage_credential)
                    .await?,
            )
        } else {
            None
        };

        let resolved_warehouse = C::create_warehouse(
            warehouse_name,
            &project_id,
            storage_profile,
            delete_profile,
            secret_id,
            transaction.transaction(),
        )
        .await?;
        authorizer
            .create_warehouse(
                &request_metadata,
                resolved_warehouse.warehouse_id,
                &project_id,
            )
            .await?;

        transaction.commit().await?;

        context
            .v1_state
            .hooks
            .create_warehouse(resolved_warehouse.clone(), Arc::new(request_metadata))
            .await;

        Ok(CreateWarehouseResponse(
            (*resolved_warehouse).clone().into(),
        ))
    }

    async fn list_warehouses(
        request: ListWarehousesRequest,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ListWarehousesResponse> {
        // ------------------- AuthZ -------------------
        let project_id = request_metadata.require_project_id(request.project_id)?;

        let authorizer = context.v1_state.authz;
        authorizer
            .require_project_action(
                &request_metadata,
                &project_id,
                CatalogProjectAction::ListWarehouses,
            )
            .await?;

        // ------------------- Business Logic -------------------
        let warehouses = C::list_warehouses(
            &project_id,
            request.warehouse_status,
            context.v1_state.catalog,
        )
        .await?;

        let warehouses = authorizer
            .are_allowed_warehouse_actions_vec(
                &request_metadata,
                None,
                &warehouses
                    .iter()
                    .map(|w| (&**w, CatalogWarehouseAction::IncludeInList))
                    .collect::<Vec<_>>(),
            )
            .await?
            .into_inner()
            .into_iter()
            .zip(warehouses)
            .filter_map(|(allowed, warehouse)| {
                if allowed {
                    Some((*warehouse).clone().into())
                } else {
                    None
                }
            })
            .collect();

        Ok(ListWarehousesResponse { warehouses })
    }

    async fn get_warehouse(
        warehouse_id: WarehouseId,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<GetWarehouseResponse> {
        let authorizer = context.v1_state.authz;

        let warehouse = C::get_warehouse_by_id_cache_aware(
            warehouse_id,
            WarehouseStatus::active_and_inactive(),
            CachePolicy::Skip,
            context.v1_state.catalog,
        )
        .await;
        let warehouse = authorizer
            .require_warehouse_action(
                &request_metadata,
                warehouse_id,
                warehouse,
                CatalogWarehouseAction::GetMetadata,
            )
            .await?;
        Ok((*warehouse).clone().into())
    }

    async fn get_warehouse_statistics(
        warehouse_id: WarehouseId,
        query: GetWarehouseStatisticsQuery,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<WarehouseStatisticsResponse> {
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;
        let warehouse = C::get_warehouse_by_id_cache_aware(
            warehouse_id,
            WarehouseStatus::active_and_inactive(),
            CachePolicy::Use,
            context.v1_state.catalog.clone(),
        )
        .await;
        authorizer
            .require_warehouse_action(
                &request_metadata,
                warehouse_id,
                warehouse,
                CatalogWarehouseAction::GetMetadata,
            )
            .await?;

        // ------------------- Business Logic -------------------
        C::get_warehouse_stats(
            warehouse_id,
            query.to_pagination_query(),
            context.v1_state.catalog,
        )
        .await
    }

    async fn delete_warehouse(
        warehouse_id: WarehouseId,
        query: DeleteWarehouseQuery,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;

        let warehouse = C::get_warehouse_by_id_cache_aware(
            warehouse_id,
            WarehouseStatus::active_and_inactive(),
            CachePolicy::Skip,
            context.v1_state.catalog.clone(),
        )
        .await;
        authorizer
            .require_warehouse_action(
                &request_metadata,
                warehouse_id,
                warehouse,
                CatalogWarehouseAction::Delete,
            )
            .await?;

        // ------------------- Business Logic -------------------
        let mut transaction = C::Transaction::begin_write(context.v1_state.catalog).await?;
        C::delete_warehouse(warehouse_id, query, transaction.transaction()).await?;
        authorizer
            .delete_warehouse(&request_metadata, warehouse_id)
            .await?;
        transaction.commit().await?;

        context
            .v1_state
            .hooks
            .delete_warehouse(warehouse_id, Arc::new(request_metadata))
            .await;

        Ok(())
    }

    async fn set_warehouse_protection(
        warehouse_id: WarehouseId,
        protection: bool,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ProtectionResponse> {
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;

        let warehouse = C::get_warehouse_by_id_cache_aware(
            warehouse_id,
            WarehouseStatus::active_and_inactive(),
            CachePolicy::Use,
            context.v1_state.catalog.clone(),
        )
        .await;
        authorizer
            .require_warehouse_action(
                &request_metadata,
                warehouse_id,
                warehouse,
                CatalogWarehouseAction::SetProtection,
            )
            .await?;

        // ------------------- Business Logic -------------------
        let mut transaction = C::Transaction::begin_write(context.v1_state.catalog).await?;
        tracing::debug!("Setting protection for warehouse {warehouse_id} to {protection}");
        let resolved_warehouse =
            C::set_warehouse_protected(warehouse_id, protection, transaction.transaction()).await?;
        transaction.commit().await?;

        context
            .v1_state
            .hooks
            .set_warehouse_protection(
                protection,
                resolved_warehouse.clone(),
                Arc::new(request_metadata),
            )
            .await;

        Ok(ProtectionResponse {
            protected: resolved_warehouse.protected,
            updated_at: resolved_warehouse.updated_at,
        })
    }

    async fn rename_warehouse(
        warehouse_id: WarehouseId,
        request: RenameWarehouseRequest,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<GetWarehouseResponse> {
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;

        let warehouse = C::get_warehouse_by_id_cache_aware(
            warehouse_id,
            WarehouseStatus::active_and_inactive(),
            CachePolicy::Use,
            context.v1_state.catalog.clone(),
        )
        .await;
        authorizer
            .require_warehouse_action(
                &request_metadata,
                warehouse_id,
                warehouse,
                CatalogWarehouseAction::Rename,
            )
            .await?;

        // ------------------- Business Logic -------------------
        validate_warehouse_name(&request.new_name)?;
        let mut transaction = C::Transaction::begin_write(context.v1_state.catalog).await?;

        let updated_warehouse =
            C::rename_warehouse(warehouse_id, &request.new_name, transaction.transaction()).await?;

        transaction.commit().await?;

        context
            .v1_state
            .hooks
            .rename_warehouse(
                Arc::new(request),
                updated_warehouse.clone(),
                Arc::new(request_metadata),
            )
            .await;

        Ok((*updated_warehouse).clone().into())
    }

    async fn update_warehouse_delete_profile(
        warehouse_id: WarehouseId,
        request: UpdateWarehouseDeleteProfileRequest,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<GetWarehouseResponse> {
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;

        let warehouse =
            C::get_active_warehouse_by_id(warehouse_id, context.v1_state.catalog.clone()).await;
        authorizer
            .require_warehouse_action(
                &request_metadata,
                warehouse_id,
                warehouse,
                CatalogWarehouseAction::ModifySoftDeletion,
            )
            .await?;

        // ------------------- Business Logic -------------------
        let mut transaction = C::Transaction::begin_write(context.v1_state.catalog).await?;
        let updated_warehouse = C::set_warehouse_deletion_profile(
            warehouse_id,
            &request.delete_profile,
            transaction.transaction(),
        )
        .await?;
        transaction.commit().await?;

        context
            .v1_state
            .hooks
            .update_warehouse_delete_profile(
                Arc::new(request),
                updated_warehouse.clone(),
                Arc::new(request_metadata),
            )
            .await;

        Ok((*updated_warehouse).clone().into())
    }

    async fn deactivate_warehouse(
        warehouse_id: WarehouseId,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;

        let warehouse = C::get_warehouse_by_id_cache_aware(
            warehouse_id,
            WarehouseStatus::active_and_inactive(),
            CachePolicy::Skip,
            context.v1_state.catalog.clone(),
        )
        .await;
        authorizer
            .require_warehouse_action(
                &request_metadata,
                warehouse_id,
                warehouse,
                CatalogWarehouseAction::Deactivate,
            )
            .await?;

        // ------------------- Business Logic -------------------
        let mut transaction = C::Transaction::begin_write(context.v1_state.catalog).await?;

        C::set_warehouse_status(
            warehouse_id,
            WarehouseStatus::Inactive,
            transaction.transaction(),
        )
        .await?;

        transaction.commit().await?;

        Ok(())
    }

    async fn activate_warehouse(
        warehouse_id: WarehouseId,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;

        let warehouse = C::get_warehouse_by_id_cache_aware(
            warehouse_id,
            WarehouseStatus::active_and_inactive(),
            CachePolicy::Skip,
            context.v1_state.catalog.clone(),
        )
        .await;
        authorizer
            .require_warehouse_action(
                &request_metadata,
                warehouse_id,
                warehouse,
                CatalogWarehouseAction::Activate,
            )
            .await?;

        // ------------------- Business Logic -------------------
        let mut transaction = C::Transaction::begin_write(context.v1_state.catalog).await?;

        C::set_warehouse_status(
            warehouse_id,
            WarehouseStatus::Active,
            transaction.transaction(),
        )
        .await?;

        transaction.commit().await?;

        Ok(())
    }

    async fn update_storage(
        warehouse_id: WarehouseId,
        request: UpdateWarehouseStorageRequest,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<GetWarehouseResponse> {
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;

        let warehouse = C::get_warehouse_by_id_cache_aware(
            warehouse_id,
            WarehouseStatus::active(),
            CachePolicy::Skip,
            context.v1_state.catalog.clone(),
        )
        .await;
        let warehouse = authorizer
            .require_warehouse_action(
                &request_metadata,
                warehouse_id,
                warehouse,
                CatalogWarehouseAction::UpdateStorage,
            )
            .await?;

        // ------------------- Business Logic -------------------
        let request_for_hook = Arc::new(request.clone());
        let UpdateWarehouseStorageRequest {
            mut storage_profile,
            storage_credential,
        } = request;

        storage_profile.normalize(storage_credential.as_ref())?;
        Box::pin(storage_profile.validate_access(
            storage_credential.as_ref(),
            None,
            &request_metadata,
        ))
        .await?;

        let mut transaction = C::Transaction::begin_write(context.v1_state.catalog).await?;
        let storage_profile = warehouse
            .storage_profile
            .clone()
            .update_with(storage_profile)?;
        let old_secret_id = warehouse.storage_secret_id;

        let secret_id = if let Some(storage_credential) = storage_credential {
            Some(
                context
                    .v1_state
                    .secrets
                    .create_storage_secret(storage_credential)
                    .await?,
            )
        } else {
            None
        };

        let updated_warehouse = C::update_storage_profile(
            warehouse_id,
            storage_profile,
            secret_id,
            transaction.transaction(),
        )
        .await?;

        transaction.commit().await?;

        context
            .v1_state
            .hooks
            .update_warehouse_storage(
                request_for_hook,
                updated_warehouse.clone(),
                Arc::new(request_metadata),
            )
            .await;

        // Delete the old secret if it exists - never fail the request if the deletion fails
        if let Some(old_secret_id) = old_secret_id {
            context
                .v1_state
                .secrets
                .delete_secret(&old_secret_id)
                .await
                .map_err(|e| {
                    tracing::warn!(error=?e.error, "Failed to delete old storage secret");
                })
                .ok();
        }

        Ok((*updated_warehouse).clone().into())
    }

    async fn update_storage_credential(
        warehouse_id: WarehouseId,
        request: UpdateWarehouseCredentialRequest,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<GetWarehouseResponse> {
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;

        let warehouse = C::get_warehouse_by_id_cache_aware(
            warehouse_id,
            WarehouseStatus::active(),
            CachePolicy::Skip,
            context.v1_state.catalog.clone(),
        )
        .await;
        let warehouse = authorizer
            .require_warehouse_action(
                &request_metadata,
                warehouse_id,
                warehouse,
                CatalogWarehouseAction::UpdateStorage,
            )
            .await?;

        // ------------------- Business Logic -------------------
        let request_for_hook = Arc::new(request.clone());
        let UpdateWarehouseCredentialRequest {
            new_storage_credential,
        } = request;
        let mut transaction = C::Transaction::begin_write(context.v1_state.catalog).await?;
        let old_secret_id = warehouse.storage_secret_id;

        Box::pin(warehouse.storage_profile.validate_access(
            new_storage_credential.as_ref(),
            None,
            &request_metadata,
        ))
        .await?;

        let secret_id = if let Some(new_storage_credential) = new_storage_credential {
            Some(
                context
                    .v1_state
                    .secrets
                    .create_storage_secret(new_storage_credential)
                    .await?,
            )
        } else {
            None
        };

        let updated_warehouse = C::update_storage_profile(
            warehouse_id,
            warehouse.storage_profile.clone(),
            secret_id,
            transaction.transaction(),
        )
        .await?;

        transaction.commit().await?;

        context
            .v1_state
            .hooks
            .update_warehouse_storage_credential(
                request_for_hook,
                old_secret_id,
                updated_warehouse.clone(),
                Arc::new(request_metadata),
            )
            .await;

        // Delete the old secret if it exists - never fail the request if the deletion fails
        if let Some(old_secret_id) = old_secret_id {
            context
                .v1_state
                .secrets
                .delete_secret(&old_secret_id)
                .await
                .map_err(|e| {
                    tracing::warn!(error=?e.error, "Failed to delete old storage secret");
                })
                .ok();
        }

        Ok((*updated_warehouse).clone().into())
    }

    async fn undrop_tabulars(
        warehouse_id: WarehouseId,
        request_metadata: RequestMetadata,
        request: UndropTabularsRequest,
        context: ApiContext<State<A, C, S>>,
    ) -> Result<()> {
        if request.targets.is_empty() {
            return Ok(());
        }
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;

        let warehouse = C::get_warehouse_by_id_cache_aware(
            warehouse_id,
            WarehouseStatus::active(),
            CachePolicy::Skip,
            context.v1_state.catalog.clone(),
        )
        .await;
        let warehouse = authorizer
            .require_warehouse_action(
                &request_metadata,
                warehouse_id,
                warehouse,
                CatalogWarehouseAction::Use,
            )
            .await?;

        undrop::require_undrop_permissions::<A, C>(
            &warehouse,
            &request,
            &authorizer,
            context.v1_state.catalog.clone(),
            &request_metadata,
        )
        .await?;

        // ------------------- Business Logic -------------------
        let catalog = context.v1_state.catalog;
        let mut transaction = C::Transaction::begin_write(catalog.clone()).await?;
        let tabular_ids = &request.targets;
        let undrop_tabular_responses =
            C::clear_tabular_deleted_at(tabular_ids, warehouse_id, transaction.transaction())
                .await?;
        TabularExpirationTask::cancel_scheduled_tasks::<C>(
            TaskFilter::TaskIds(
                undrop_tabular_responses
                    .iter()
                    .filter_map(|r| {
                        if r.expiration_task().is_none() {
                            tracing::warn!(
                                "No expiration task found for tabular '{}' with soft deletion marker set.",
                                r.tabular_ident()
                            );
                        }
                        r.expiration_task().map(|t| t.task_id)})
                    .collect(),
            ),
            transaction.transaction(),
            false,
        )
        .await?;
        transaction.commit().await?;

        context
            .v1_state
            .hooks
            .undrop_tabular(
                warehouse_id,
                Arc::new(request),
                Arc::new(
                    undrop_tabular_responses
                        .into_iter()
                        .map(ViewOrTableDeletionInfo::into_table_or_view_info)
                        .collect(),
                ),
                Arc::new(request_metadata),
            )
            .await;

        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn list_soft_deleted_tabulars(
        warehouse_id: WarehouseId,
        query: ListDeletedTabularsQuery,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ListDeletedTabularsResponse> {
        // ------------------- AuthZ -------------------
        let catalog = context.v1_state.catalog;
        let authorizer = context.v1_state.authz;

        let warehouse = C::get_active_warehouse_by_id(warehouse_id, catalog.clone()).await;
        let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;

        let [can_use, can_list_deleted_tabulars, can_list_everything] = authorizer
            .are_allowed_warehouse_actions_arr(
                &request_metadata,
                None,
                &[
                    (&warehouse, CatalogWarehouseAction::Use),
                    (&warehouse, CatalogWarehouseAction::ListDeletedTabulars),
                    (&warehouse, CatalogWarehouseAction::ListEverything),
                ],
            )
            .await?
            .into_inner();

        if !can_use {
            return Err(AuthZCannotUseWarehouseId::new(warehouse_id).into());
        }
        if !can_list_deleted_tabulars {
            return Err(AuthZWarehouseActionForbidden::new(
                warehouse_id,
                CatalogWarehouseAction::ListDeletedTabulars,
                request_metadata.actor().clone(),
            )
            .into());
        }

        let can_list_everything = if can_list_everything {
            can_list_everything
        } else if let Some(namespace_id) = query.namespace_id {
            let namespace = C::get_namespace(warehouse_id, namespace_id, catalog.clone()).await;
            let namespace =
                authorizer.require_namespace_presence(warehouse_id, namespace_id, namespace)?;
            authorizer
                .is_allowed_namespace_action(
                    &request_metadata,
                    None,
                    &warehouse,
                    &namespace,
                    CatalogNamespaceAction::ListEverything,
                )
                .await?
                .into_inner()
        } else {
            can_list_everything
        };

        // ------------------- Business Logic -------------------
        let pagination_query = query.pagination_query();
        let namespace_id = query.namespace_id;
        let mut t = C::Transaction::begin_read(catalog.clone()).await?;
        let (tabulars, ids, next_page_token) = crate::server::fetch_until_full_page::<_, _, _, C>(
            pagination_query.page_size,
            pagination_query.page_token,
            |page_size, page_token, t| {
                let authorizer = authorizer.clone();
                let request_metadata = request_metadata.clone();
                let warehouse = warehouse.clone();
                async move {
                    let query = PaginationQuery {
                        page_size: Some(page_size),
                        page_token: page_token.into(),
                    };

                    let page = C::list_tabulars(
                        warehouse_id,
                        namespace_id,
                        TabularListFlags::only_deleted(),
                        t.transaction(),
                        None,
                        query,
                    )
                    .await?;
                    let (ids, items, tokens): (Vec<_>, Vec<_>, Vec<_>) =
                        page.into_iter_with_page_tokens().multiunzip();

                    let authz_decisions = if can_list_everything {
                        vec![true; ids.len()]
                    } else {
                        let namespaces = C::get_namespaces_by_id(
                            warehouse_id,
                            &items
                                .iter()
                                .map(ViewOrTableDeletionInfo::namespace_id)
                                .collect_vec(),
                            t.transaction(),
                        )
                        .await?;
                        let actions = items
                            .iter()
                            .map(|t| {
                                Ok::<_, ErrorModel>((
                                    require_namespace_for_tabular(&namespaces, t)?,
                                    t.as_action_request(
                                        CatalogViewAction::IncludeInList,
                                        CatalogTableAction::IncludeInList,
                                    ),
                                ))
                            })
                            .collect::<Result<Vec<_>, _>>()?;

                        authorizer
                            .are_allowed_tabular_actions_vec(
                                &request_metadata,
                                None,
                                &warehouse,
                                &namespaces,
                                &actions,
                            )
                            .await?
                            .into_inner()
                    };

                    let (next_idents, next_uuids, next_page_tokens, mask): (
                        Vec<_>,
                        Vec<_>,
                        Vec<_>,
                        Vec<bool>,
                    ) = authz_decisions
                        .into_iter()
                        .zip(items.into_iter().zip(ids.into_iter()))
                        .zip(tokens.into_iter())
                        .map(|((allowed, namespace), token)| {
                            (namespace.0, namespace.1, token, allowed)
                        })
                        .multiunzip();
                    Ok(UnfilteredPage::new(
                        next_idents,
                        next_uuids,
                        next_page_tokens,
                        mask,
                        page_size
                            .clamp(0, i64::MAX)
                            .try_into()
                            .expect("We clamped."),
                    ))
                }
                .boxed()
            },
            &mut t,
        )
        .await?;

        let tabulars = ids
            .into_iter()
            .zip(tabulars)
            .filter_map(|(k, info)| {
                let deleted_at = info.deleted_at()?;
                let Some(expiration_task) = info.expiration_task() else {
                    tracing::error!(
                        "Did not find expiration task for soft-deleted tabular with id '{k}'"
                    );
                    return None;
                };
                let tabular_ident = info.tabular_ident().clone();
                Some(DeletedTabularResponse {
                    id: *k,
                    name: tabular_ident.name,
                    namespace: tabular_ident.namespace.inner(),
                    typ: k.into(),
                    warehouse_id,
                    created_at: info.created_at(),
                    deleted_at,
                    expiration_date: expiration_task.expiration_date,
                })
            })
            .collect::<Vec<_>>();

        t.commit().await?;

        Ok(ListDeletedTabularsResponse {
            tabulars,
            next_page_token,
        })
    }

    async fn set_task_queue_config(
        project_id: Option<ProjectId>,
        warehouse_id: Option<WarehouseId>,
        queue_name: &TaskQueueName,
        request: SetTaskQueueConfigRequest,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        if let Some(warehouse_id) = warehouse_id {
            // ------------------- AuthZ -------------------
            let authorizer = context.v1_state.authz;

            let warehouse =
                C::get_active_warehouse_by_id(warehouse_id, context.v1_state.catalog.clone()).await;
            let warehouse_resolved = authorizer
                .require_warehouse_action(
                    &request_metadata,
                    warehouse_id,
                    warehouse,
                    CatalogWarehouseAction::ModifyTaskQueueConfig,
                )
                .await?;

            // ------------------- Business Logic -------------------
            let task_queues = context.v1_state.registered_task_queues;

            if let Some(validate_config_fn) = task_queues.validate_config_fn(queue_name).await {
                validate_config_fn(request.queue_config.0.clone()).map_err(|e| {
                ErrorModel::bad_request(
                    format!(
                        "Failed to deserialize queue config for queue-name '{queue_name}': '{e}'"
                    ),
                    "InvalidQueueConfig",
                    Some(Box::new(e)),
                )
            })?;
            } else {
                let mut existing_queue_names = task_queues.queue_names().await;
                existing_queue_names.sort_unstable();
                let existing_queue_names = existing_queue_names.iter().join(", ");
                return Err(ErrorModel::bad_request(
                    format!(
                        "Queue '{queue_name}' not found! Existing queues: [{existing_queue_names}]"
                    ),
                    "QueueNotFound",
                    None,
                )
                .into());
            }
            let project_id = warehouse_resolved.project_id.clone();
            let mut transaction = C::Transaction::begin_write(context.v1_state.catalog).await?;
            C::set_task_queue_config(
                project_id,
                Some(warehouse_id),
                queue_name,
                request,
                transaction.transaction(),
            )
            .await?;
            transaction.commit().await?;
            Ok(())
        } else {
            if let Some(_project_id) = project_id {
                return Err(ErrorModel::internal(
                    "Project-level tasks are not yet supported",
                    "ProjectLevelTasksNotSupported",
                    None,
                )
                .into());
            } else {
                return Err(ErrorModel::bad_request(
                    "Either ProjectId or WarehouseId must be provided.",
                    "MissingNecessaryId",
                    None,
                )
                .into());
            }
        }
    }

    async fn get_task_queue_config(
        project_id: Option<ProjectId>,
        warehouse_id: Option<WarehouseId>,
        queue_name: &TaskQueueName,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<GetTaskQueueConfigResponse> {
        if let Some(warehouse_id) = warehouse_id {
            // ------------------- AuthZ -------------------
            let authorizer = context.v1_state.authz;

            let warehouse =
                C::get_active_warehouse_by_id(warehouse_id, context.v1_state.catalog.clone()).await;
            let warehouse_resolved = authorizer
                .require_warehouse_action(
                    &request_metadata,
                    warehouse_id,
                    warehouse,
                    CatalogWarehouseAction::GetTaskQueueConfig,
                )
                .await?;

            // ------------------- Business Logic -------------------
            let project_id = warehouse_resolved.project_id.clone();

            let config =
                C::get_task_queue_config(Some(project_id), Some(warehouse_id), queue_name, context.v1_state.catalog)
                    .await?
                    .unwrap_or_else(|| GetTaskQueueConfigResponse {
                        queue_config: QueueConfigResponse {
                            config: serde_json::json!({}),
                            queue_name: queue_name.clone(),
                        },
                        max_seconds_since_last_heartbeat: None,
                    });
            Ok(config)
        } else {
            if let Some(_project_id) = project_id {
                return Err(ErrorModel::internal(
                    "Project-level tasks are not yet supported",
                    "ProjectLevelTasksNotSupported",
                    None,
                )
                .into());
            } else {
                return Err(ErrorModel::bad_request(
                    "Either ProjectId or WarehouseId must be provided.",
                    "MissingNecessaryId",
                    None,
                )
                .into());
            }
        }
    }
}

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct SetTaskQueueConfigRequest {
    pub queue_config: QueueConfig,
    pub max_seconds_since_last_heartbeat: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(transparent)]
pub struct QueueConfig(pub(crate) serde_json::Value);

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct GetTaskQueueConfigResponse {
    pub queue_config: QueueConfigResponse,
    pub max_seconds_since_last_heartbeat: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct QueueConfigResponse {
    #[serde(flatten)]
    pub(crate) config: serde_json::Value,
    #[cfg_attr(feature = "open-api", schema(value_type=String))]
    pub(crate) queue_name: TaskQueueName,
}

impl axum::response::IntoResponse for GetTaskQueueConfigResponse {
    fn into_response(self) -> axum::http::Response<axum::body::Body> {
        (http::StatusCode::OK, axum::Json(self)).into_response()
    }
}

impl axum::response::IntoResponse for ListWarehousesResponse {
    fn into_response(self) -> axum::http::Response<axum::body::Body> {
        axum::Json(self).into_response()
    }
}

impl axum::response::IntoResponse for GetWarehouseResponse {
    fn into_response(self) -> axum::http::Response<axum::body::Body> {
        axum::Json(self).into_response()
    }
}

impl From<crate::service::ResolvedWarehouse> for GetWarehouseResponse {
    fn from(warehouse: crate::service::ResolvedWarehouse) -> Self {
        Self {
            warehouse_id: warehouse.warehouse_id,
            id: warehouse.warehouse_id,
            name: warehouse.name,
            project_id: warehouse.project_id,
            storage_profile: warehouse.storage_profile,
            status: warehouse.status,
            delete_profile: warehouse.tabular_delete_profile,
            protected: warehouse.protected,
            updated_at: warehouse.updated_at,
        }
    }
}

fn validate_warehouse_name(warehouse_name: &str) -> Result<()> {
    if warehouse_name.is_empty() {
        return Err(ErrorModel::bad_request(
            "Warehouse name cannot be empty",
            "EmptyWarehouseName",
            None,
        )
        .into());
    }

    if warehouse_name.len() > 128 {
        return Err(ErrorModel::bad_request(
            "Warehouse must be shorter than 128 chars",
            "WarehouseNameTooLong",
            None,
        )
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod test {
    #[test]
    fn test_de_create_warehouse_request() {
        let request = serde_json::json!({
            "warehouse-name": "test_warehouse",
            "project-id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
            "storage-profile": {
                "type": "s3",
                "bucket": "test",
                "region": "dummy",
                "path-style-access": true,
                "endpoint": "http://localhost:9000",
                "sts-enabled": true,
            },
            "storage-credential": {
                "type": "s3",
                "credential-type": "access-key",
                "aws-access-key-id": "test-access-key-id",
                "aws-secret-access-key": "test-secret-access-key",
            },
        });

        let request: super::CreateWarehouseRequest = serde_json::from_value(request).unwrap();
        assert_eq!(request.warehouse_name, "test_warehouse");
        assert_eq!(
            request.project_id,
            Some(
                uuid::Uuid::parse_str("f47ac10b-58cc-4372-a567-0e02b2c3d479")
                    .unwrap()
                    .into()
            )
        );
        let s3_profile = request.storage_profile.try_into_s3().unwrap();
        assert_eq!(s3_profile.bucket, "test");
        assert_eq!(s3_profile.region, "dummy");
        assert_eq!(s3_profile.path_style_access, Some(true));
    }

    use iceberg::TableIdent;
    use itertools::Itertools;
    use sqlx::PgPool;

    use crate::{
        WarehouseId,
        api::{
            ApiContext,
            iceberg::{
                types::Prefix,
                v1::{
                    DataAccess, DropParams, NamespaceParameters, ViewParameters, views::ViewService,
                },
            },
            management::v1::{
                ApiServer,
                warehouse::{ListDeletedTabularsQuery, Service as _, TabularDeleteProfile},
            },
        },
        implementations::postgres::{PostgresBackend, SecretsState},
        request_metadata::RequestMetadata,
        server::{CatalogServer, test::impl_pagination_tests},
        service::{State, UserId, authz::tests::HidingAuthorizer},
        tests::create_view_request,
    };

    async fn setup_pagination_test(
        pool: sqlx::PgPool,
        n_tabulars: usize,
        hidden_ranges: &[(usize, usize)],
    ) -> (
        ApiContext<State<HidingAuthorizer, PostgresBackend, SecretsState>>,
        WarehouseId,
    ) {
        let prof = crate::server::test::memory_io_profile();

        let authz = HidingAuthorizer::new();
        authz.block_can_list_everything();

        let (ctx, warehouse) = crate::server::test::setup(
            pool.clone(),
            prof,
            None,
            authz.clone(),
            TabularDeleteProfile::Soft {
                expiration_seconds: chrono::Duration::seconds(10),
            },
            Some(UserId::new_unchecked("oidc", "test-user-id")),
        )
        .await;
        let ns = crate::server::test::create_ns(
            ctx.clone(),
            warehouse.warehouse_id.to_string(),
            "ns1".to_string(),
        )
        .await;
        let ns_params = NamespaceParameters {
            prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
            namespace: ns.namespace.clone(),
        };
        // create 10 staged tables
        for i in 0..n_tabulars {
            let v = CatalogServer::create_view(
                ns_params.clone(),
                create_view_request(Some(&format!("{i}")), None),
                ctx.clone(),
                DataAccess {
                    vended_credentials: true,
                    remote_signing: false,
                },
                RequestMetadata::new_unauthenticated(),
            )
            .await
            .unwrap();

            CatalogServer::drop_view(
                ViewParameters {
                    prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
                    view: TableIdent {
                        name: format!("{i}"),
                        namespace: ns.namespace.clone(),
                    },
                },
                DropParams {
                    purge_requested: true,
                    force: true,
                },
                ctx.clone(),
                RequestMetadata::new_unauthenticated(),
            )
            .await
            .unwrap();
            if hidden_ranges
                .iter()
                .any(|(start, end)| i >= *start && i < *end)
            {
                authz.hide(&format!(
                    "view:{}/{}",
                    warehouse.warehouse_id,
                    v.metadata.uuid()
                ));
            }
        }

        (ctx, warehouse.warehouse_id)
    }

    impl_pagination_tests!(
        soft_deleted_tabular,
        setup_pagination_test,
        ApiServer,
        ListDeletedTabularsQuery,
        tabulars,
        |tid| { tid.name }
    );

    #[sqlx::test]
    async fn test_deleted_tabulars_pagination(pool: sqlx::PgPool) {
        let prof = crate::server::test::memory_io_profile();

        let authz = HidingAuthorizer::new();
        authz.block_can_list_everything();

        let (ctx, warehouse) = crate::server::test::setup(
            pool.clone(),
            prof,
            None,
            authz.clone(),
            TabularDeleteProfile::Soft {
                expiration_seconds: chrono::Duration::seconds(10),
            },
            Some(UserId::new_unchecked("oidc", "test-user-id")),
        )
        .await;
        let ns = crate::server::test::create_ns(
            ctx.clone(),
            warehouse.warehouse_id.to_string(),
            "ns1".to_string(),
        )
        .await;
        let ns_params = NamespaceParameters {
            prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
            namespace: ns.namespace.clone(),
        };
        for i in 0..10 {
            let _ = CatalogServer::create_view(
                ns_params.clone(),
                create_view_request(Some(&format!("view-{i}")), None),
                ctx.clone(),
                DataAccess {
                    vended_credentials: true,
                    remote_signing: false,
                },
                RequestMetadata::new_unauthenticated(),
            )
            .await
            .unwrap();
            CatalogServer::drop_view(
                ViewParameters {
                    prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
                    view: TableIdent {
                        name: format!("view-{i}"),
                        namespace: ns.namespace.clone(),
                    },
                },
                DropParams {
                    purge_requested: true,
                    force: false,
                },
                ctx.clone(),
                RequestMetadata::new_unauthenticated(),
            )
            .await
            .unwrap();
        }

        // list 1 more than existing tables
        let all = ApiServer::list_soft_deleted_tabulars(
            warehouse.warehouse_id,
            ListDeletedTabularsQuery {
                namespace_id: None,
                page_size: Some(11),
                page_token: None,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        assert_eq!(all.tabulars.len(), 10);

        // list exactly amount of existing tables
        let all = ApiServer::list_soft_deleted_tabulars(
            warehouse.warehouse_id,
            ListDeletedTabularsQuery {
                namespace_id: None,
                page_size: Some(10),
                page_token: None,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        assert_eq!(all.tabulars.len(), 10);

        // next page is empty
        let next = ApiServer::list_soft_deleted_tabulars(
            warehouse.warehouse_id,
            ListDeletedTabularsQuery {
                namespace_id: None,
                page_size: Some(10),
                page_token: all.next_page_token,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        assert_eq!(next.tabulars.len(), 0);
        assert!(next.next_page_token.is_none());

        let first_six = ApiServer::list_soft_deleted_tabulars(
            warehouse.warehouse_id,
            ListDeletedTabularsQuery {
                namespace_id: None,
                page_size: Some(6),
                page_token: None,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        assert_eq!(first_six.tabulars.len(), 6);
        assert!(first_six.next_page_token.is_some());
        let first_six_items = first_six
            .tabulars
            .iter()
            .map(|i| i.name.clone())
            .sorted()
            .collect::<Vec<_>>();

        for (i, item) in first_six_items.iter().enumerate().take(6) {
            assert_eq!(item, &format!("view-{i}"));
        }

        let next_four = ApiServer::list_soft_deleted_tabulars(
            warehouse.warehouse_id,
            ListDeletedTabularsQuery {
                namespace_id: None,
                page_size: Some(6),
                page_token: first_six.next_page_token,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        assert_eq!(next_four.tabulars.len(), 4);
        // page-size > number of items left -> no next page
        assert!(next_four.next_page_token.is_none());

        let next_four_items = next_four
            .tabulars
            .iter()
            .map(|i| i.name.clone())
            .sorted()
            .collect::<Vec<_>>();

        for (idx, i) in (6..10).enumerate() {
            assert_eq!(next_four_items[idx], format!("view-{i}"));
        }

        let mut ids = all.tabulars;
        ids.sort_by_key(|e| e.id);
        for t in ids.iter().take(6).skip(4) {
            authz.hide(&format!("view:{}/{}", warehouse.warehouse_id, t.id));
        }

        let page = ApiServer::list_soft_deleted_tabulars(
            warehouse.warehouse_id,
            ListDeletedTabularsQuery {
                namespace_id: None,
                page_size: Some(5),
                page_token: None,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        assert_eq!(page.tabulars.len(), 5);
        assert!(page.next_page_token.is_some());
        let page_items = page
            .tabulars
            .iter()
            .map(|i| i.name.clone())
            .sorted()
            .collect::<Vec<_>>();
        for (i, item) in page_items.iter().enumerate() {
            let tab_id = if i > 3 { i + 2 } else { i };
            assert_eq!(item, &format!("view-{tab_id}"));
        }

        let next_page = ApiServer::list_soft_deleted_tabulars(
            warehouse.warehouse_id,
            ListDeletedTabularsQuery {
                namespace_id: None,
                page_size: Some(6),
                page_token: page.next_page_token,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        assert_eq!(next_page.tabulars.len(), 3);

        let next_page_items = next_page
            .tabulars
            .iter()
            .map(|i| i.name.clone())
            .sorted()
            .collect::<Vec<_>>();

        for (idx, i) in (7..10).enumerate() {
            assert_eq!(next_page_items[idx], format!("view-{i}"));
        }
    }
}
