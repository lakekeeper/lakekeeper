use crate::api::management::v1::{ApiServer, DeletedTabularResponse, ListDeletedTabularsResponse};
use crate::api::{ApiContext, Result};
use crate::request_metadata::RequestMetadata;
use crate::service::authz::{CatalogProjectAction, CatalogWarehouseAction};
pub use crate::service::storage::{
    AdlsProfile, AzCredential, GcsCredential, GcsProfile, GcsServiceKey, S3Credential, S3Profile,
    StorageCredential, StorageProfile,
};

use crate::api::iceberg::v1::{PaginatedTabulars, PaginationQuery};

use super::TabularType;
use crate::api::management::v1::role::require_project_id;
pub use crate::service::WarehouseStatus;
use crate::service::{
    authz::Authorizer, secrets::SecretStore, Catalog, ListFlags, State, Transaction,
};
use crate::{ProjectIdent, WarehouseIdent, CONFIG, DEFAULT_PROJECT_ID};
use iceberg_ext::catalog::rest::ErrorModel;
use serde::Deserialize;
use utoipa::ToSchema;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct CreateWarehouseRequest {
    /// Name of the warehouse to create. Must be unique
    /// within a project and may not contain "/"
    pub warehouse_name: String,
    /// Project ID in which to create the warehouse.
    /// If no default project is set for this server, this field is required.
    pub project_id: Option<uuid::Uuid>,
    /// Storage profile to use for the warehouse.
    pub storage_profile: StorageProfile,
    /// Optional storage credential to use for the warehouse.
    pub storage_credential: Option<StorageCredential>,
    /// Profile to determine behavior upon dropping of tabulars, defaults to soft-deletion with
    /// 7 days expiration.
    #[serde(default)]
    pub delete_profile: TabularDeleteProfile,
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case", tag = "type")]
pub enum TabularDeleteProfile {
    #[schema(title = "TabularDeleteProfileHard")]
    Hard {},
    #[schema(title = "TabularDeleteProfileSoft")]
    Soft {
        #[serde(
            deserialize_with = "crate::config::seconds_to_duration",
            serialize_with = "crate::config::duration_to_seconds"
        )]
        expiration_seconds: chrono::Duration,
    },
}

impl TabularDeleteProfile {
    pub(crate) fn expiration_seconds(&self) -> Option<chrono::Duration> {
        match self {
            Self::Soft { expiration_seconds } => Some(*expiration_seconds),
            Self::Hard {} => None,
        }
    }
}

impl Default for TabularDeleteProfile {
    fn default() -> Self {
        Self::Soft {
            expiration_seconds: CONFIG.default_tabular_expiration_delay_seconds,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct CreateWarehouseResponse {
    /// ID of the created warehouse.
    pub warehouse_id: uuid::Uuid,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
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

#[derive(Debug, Deserialize, ToSchema, utoipa::IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct ListWarehousesRequest {
    /// Optional filter to return only warehouses
    /// with the specified status.
    /// If not provided, only active warehouses are returned.
    #[serde(default)]
    pub warehouse_status: Option<Vec<WarehouseStatus>>,
    /// The project ID to list warehouses for.
    /// Setting a warehouse is required.
    #[serde(default)]
    #[param(value_type=Option::<uuid::Uuid>)]
    pub project_id: Option<ProjectIdent>,
}

#[derive(Debug, Clone, serde::Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct RenameWarehouseRequest {
    /// New name for the warehouse.
    pub new_name: String,
}

#[derive(Debug, Clone, serde::Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct UpdateWarehouseDeleteProfileRequest {
    pub delete_profile: TabularDeleteProfile,
}

#[derive(Debug, Clone, serde::Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct RenameProjectRequest {
    /// New name for the project.
    pub new_name: String,
}

#[derive(Debug, Clone, serde::Serialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct GetWarehouseResponse {
    /// ID of the warehouse.
    pub id: uuid::Uuid,
    /// Name of the warehouse.
    pub name: String,
    /// Project ID in which the warehouse is created.
    pub project_id: uuid::Uuid,
    /// Storage profile used for the warehouse.
    pub storage_profile: StorageProfile,
    /// Whether the warehouse is active.
    pub status: WarehouseStatus,
}

#[derive(Debug, Clone, serde::Serialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct ListWarehousesResponse {
    /// List of warehouses in the project.
    pub warehouses: Vec<GetWarehouseResponse>,
}

#[derive(Debug, Clone, serde::Deserialize, ToSchema)]
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

impl<C: Catalog, A: Authorizer + Clone, S: SecretStore> Service<C, A, S> for ApiServer<C, A, S> {}

#[async_trait::async_trait]
pub(super) trait Service<C: Catalog, A: Authorizer, S: SecretStore> {
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
        let project_id = project_id
            .map(ProjectIdent::from)
            .or(*DEFAULT_PROJECT_ID)
            .ok_or(ErrorModel::bad_request(
                "project_id must be specified",
                "CreateWarehouseProjectIdMissing",
                None,
            ))?;

        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;
        authorizer
            .require_project_action(
                &request_metadata,
                project_id,
                &CatalogProjectAction::CanCreateWarehouse,
            )
            .await?;

        // ------------------- Business Logic -------------------
        validate_warehouse_name(&warehouse_name)?;
        storage_profile.normalize()?;
        storage_profile
            .validate_access(storage_credential.as_ref(), None)
            .await?;

        let mut transaction = C::Transaction::begin_write(context.v1_state.catalog).await?;
        let secret_id = if let Some(storage_credential) = storage_credential {
            Some(
                context
                    .v1_state
                    .secrets
                    .create_secret(storage_credential)
                    .await?,
            )
        } else {
            None
        };

        let warehouse_id = C::create_warehouse(
            warehouse_name,
            project_id,
            storage_profile,
            delete_profile,
            secret_id,
            transaction.transaction(),
        )
        .await?;
        authorizer
            .create_warehouse(&request_metadata, warehouse_id, project_id)
            .await?;

        transaction.commit().await?;

        Ok(CreateWarehouseResponse {
            warehouse_id: *warehouse_id,
        })
    }

    async fn list_warehouses(
        request: ListWarehousesRequest,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ListWarehousesResponse> {
        // ------------------- AuthZ -------------------
        let project_id = require_project_id(request.project_id, &request_metadata)?;

        let authorizer = context.v1_state.authz;
        authorizer
            .require_project_action(
                &request_metadata,
                project_id,
                &CatalogProjectAction::CanListWarehouses,
            )
            .await?;

        // ------------------- Business Logic -------------------
        let warehouses = C::list_warehouses(
            project_id,
            request.warehouse_status,
            context.v1_state.catalog,
        )
        .await?;

        let warehouses = futures::future::try_join_all(warehouses.iter().map(|w| {
            authorizer.is_allowed_warehouse_action(
                &request_metadata,
                w.id,
                &CatalogWarehouseAction::CanIncludeInList,
            )
        }))
        .await?
        .into_iter()
        .zip(warehouses.into_iter())
        .filter_map(|(allowed, warehouse)| {
            if allowed {
                Some(warehouse.into())
            } else {
                None
            }
        })
        .collect();

        Ok(ListWarehousesResponse { warehouses })
    }

    async fn get_warehouse(
        warehouse_id: WarehouseIdent,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<GetWarehouseResponse> {
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;
        authorizer
            .require_warehouse_action(
                &request_metadata,
                warehouse_id,
                &CatalogWarehouseAction::CanGetMetadata,
            )
            .await?;

        // ------------------- Business Logic -------------------
        let mut transaction = C::Transaction::begin_read(context.v1_state.catalog).await?;
        let warehouses = C::require_warehouse(warehouse_id, transaction.transaction()).await?;

        Ok(warehouses.into())
    }

    async fn delete_warehouse(
        warehouse_id: WarehouseIdent,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;
        authorizer
            .require_warehouse_action(
                &request_metadata,
                warehouse_id,
                &CatalogWarehouseAction::CanDelete,
            )
            .await?;

        // ------------------- Business Logic -------------------
        let mut transaction = C::Transaction::begin_write(context.v1_state.catalog).await?;
        C::delete_warehouse(warehouse_id, transaction.transaction()).await?;
        authorizer
            .delete_warehouse(&request_metadata, warehouse_id)
            .await?;
        transaction.commit().await?;

        Ok(())
    }
    async fn rename_warehouse(
        warehouse_id: WarehouseIdent,
        request: RenameWarehouseRequest,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;
        authorizer
            .require_warehouse_action(
                &request_metadata,
                warehouse_id,
                &CatalogWarehouseAction::CanRename,
            )
            .await?;

        // ------------------- Business Logic -------------------
        validate_warehouse_name(&request.new_name)?;
        let mut transaction = C::Transaction::begin_write(context.v1_state.catalog).await?;

        C::rename_warehouse(warehouse_id, &request.new_name, transaction.transaction()).await?;

        transaction.commit().await?;

        Ok(())
    }

    async fn update_warehouse_delete_profile(
        warehouse_id: WarehouseIdent,
        request: UpdateWarehouseDeleteProfileRequest,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;
        authorizer
            .require_warehouse_action(
                &request_metadata,
                warehouse_id,
                &CatalogWarehouseAction::CanModifySoftDeletion,
            )
            .await?;

        // ------------------- Business Logic -------------------
        let mut transaction = C::Transaction::begin_write(context.v1_state.catalog).await?;
        C::set_warehouse_deletion_profile(
            warehouse_id,
            &request.delete_profile,
            transaction.transaction(),
        )
        .await?;
        transaction.commit().await?;

        Ok(())
    }

    async fn deactivate_warehouse(
        warehouse_id: WarehouseIdent,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;
        authorizer
            .require_warehouse_action(
                &request_metadata,
                warehouse_id,
                &CatalogWarehouseAction::CanDeactivate,
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
        warehouse_id: WarehouseIdent,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;
        authorizer
            .require_warehouse_action(
                &request_metadata,
                warehouse_id,
                &CatalogWarehouseAction::CanActivate,
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
        warehouse_id: WarehouseIdent,
        request: UpdateWarehouseStorageRequest,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;
        authorizer
            .require_warehouse_action(
                &request_metadata,
                warehouse_id,
                &CatalogWarehouseAction::CanUpdateStorage,
            )
            .await?;

        // ------------------- Business Logic -------------------
        let UpdateWarehouseStorageRequest {
            mut storage_profile,
            storage_credential,
        } = request;

        storage_profile.normalize()?;
        storage_profile
            .validate_access(storage_credential.as_ref(), None)
            .await?;

        let mut transaction = C::Transaction::begin_write(context.v1_state.catalog).await?;
        let warehouse = C::require_warehouse(warehouse_id, transaction.transaction()).await?;
        warehouse
            .storage_profile
            .can_be_updated_with(&storage_profile)?;
        let old_secret_id = warehouse.storage_secret_id;

        let secret_id = if let Some(storage_credential) = storage_credential {
            Some(
                context
                    .v1_state
                    .secrets
                    .create_secret(storage_credential)
                    .await?,
            )
        } else {
            None
        };

        C::update_storage_profile(
            warehouse_id,
            storage_profile,
            secret_id,
            transaction.transaction(),
        )
        .await?;

        transaction.commit().await?;

        // Delete the old secret if it exists - never fail the request if the deletion fails
        if let Some(old_secret_id) = old_secret_id {
            context
                .v1_state
                .secrets
                .delete_secret(&old_secret_id)
                .await
                .map_err(|e| {
                    tracing::warn!("Failed to delete old secret: {:?}", e.error);
                })
                .ok();
        }

        Ok(())
    }

    async fn update_storage_credential(
        warehouse_id: WarehouseIdent,
        request: UpdateWarehouseCredentialRequest,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;
        authorizer
            .require_warehouse_action(
                &request_metadata,
                warehouse_id,
                &CatalogWarehouseAction::CanUpdateStorageCredential,
            )
            .await?;

        // ------------------- Business Logic -------------------
        let UpdateWarehouseCredentialRequest {
            new_storage_credential,
        } = request;

        let mut transaction = C::Transaction::begin_write(context.v1_state.catalog).await?;
        let warehouse = C::require_warehouse(warehouse_id, transaction.transaction()).await?;
        let old_secret_id = warehouse.storage_secret_id;
        let storage_profile = warehouse.storage_profile;

        storage_profile
            .validate_access(new_storage_credential.as_ref(), None)
            .await?;

        let secret_id = if let Some(new_storage_credential) = new_storage_credential {
            Some(
                context
                    .v1_state
                    .secrets
                    .create_secret(new_storage_credential)
                    .await?,
            )
        } else {
            None
        };

        C::update_storage_profile(
            warehouse_id,
            storage_profile,
            secret_id,
            transaction.transaction(),
        )
        .await?;

        transaction.commit().await?;

        // Delete the old secret if it exists - never fail the request if the deletion fails
        if let Some(old_secret_id) = old_secret_id {
            context
                .v1_state
                .secrets
                .delete_secret(&old_secret_id)
                .await
                .map_err(|e| {
                    tracing::warn!("Failed to delete old secret: {:?}", e.error);
                })
                .ok();
        }

        Ok(())
    }

    async fn list_soft_deleted_tabulars(
        request_metadata: RequestMetadata,
        warehouse_id: WarehouseIdent,
        context: ApiContext<State<A, C, S>>,
        pagination_query: PaginationQuery,
    ) -> Result<ListDeletedTabularsResponse> {
        // ------------------- AuthZ -------------------
        let authorizer = context.v1_state.authz;
        authorizer
            .require_warehouse_action(
                &request_metadata,
                warehouse_id,
                &CatalogWarehouseAction::CanListDeletedTabulars,
            )
            .await?;

        // ------------------- Business Logic -------------------
        let PaginatedTabulars {
            tabulars,
            next_page_token,
        } = C::list_tabulars(
            warehouse_id,
            ListFlags::only_deleted(),
            context.v1_state.catalog,
            pagination_query,
        )
        .await?;

        let tabulars = tabulars
            .into_iter()
            .map(|(k, (ident, delete_opts))| {
                let i = ident.into_inner();
                let deleted = delete_opts.ok_or(ErrorModel::internal(
                    "Expected delete options to be Some, but found None",
                    "InternalDatabaseError",
                    None,
                ))?;

                Ok(DeletedTabularResponse {
                    id: *k,
                    name: i.name,
                    namespace: i.namespace.inner(),
                    typ: k.into(),
                    warehouse_id: *warehouse_id,
                    created_at: deleted.created_at,
                    deleted_at: deleted.deleted_at,
                    expiration_date: deleted.expiration_date,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let tabulars = futures::future::try_join_all(tabulars.iter().map(|t| match t.typ {
            TabularType::View => authorizer.is_allowed_view_action(
                &request_metadata,
                warehouse_id,
                t.id.into(),
                &crate::service::authz::CatalogViewAction::CanIncludeInList,
            ),
            TabularType::Table => authorizer.is_allowed_table_action(
                &request_metadata,
                warehouse_id,
                t.id.into(),
                &crate::service::authz::CatalogTableAction::CanIncludeInList,
            ),
        }))
        .await?
        .into_iter()
        .zip(tabulars.into_iter())
        .filter_map(|(allowed, tabular)| if allowed { Some(tabular) } else { None })
        .collect();

        // ToDo: Better pagination with non-empty pages
        Ok(ListDeletedTabularsResponse {
            tabulars,
            next_page_token,
        })
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

impl From<crate::service::GetWarehouseResponse> for GetWarehouseResponse {
    fn from(warehouse: crate::service::GetWarehouseResponse) -> Self {
        Self {
            id: warehouse.id.to_uuid(),
            name: warehouse.name,
            project_id: *warehouse.project_id,
            storage_profile: warehouse.storage_profile,
            status: warehouse.status,
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
            Some(uuid::Uuid::parse_str("f47ac10b-58cc-4372-a567-0e02b2c3d479").unwrap())
        );
        let s3_profile = request.storage_profile.try_into_s3().unwrap();
        assert_eq!(s3_profile.bucket, "test");
        assert_eq!(s3_profile.region, "dummy");
        assert_eq!(s3_profile.path_style_access, Some(true));
    }
}
