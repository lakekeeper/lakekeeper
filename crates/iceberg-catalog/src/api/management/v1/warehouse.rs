use crate::api::management::v1::{ApiServer, DeletedTabularResponse, ListDeletedTabularsResponse};
use crate::api::{ApiContext, Result};
use crate::request_metadata::RequestMetadata;
use crate::service::authz::{CatalogProjectAction, CatalogWarehouseAction};
pub use crate::service::storage::{
    AdlsProfile, AzCredential, GcsCredential, GcsProfile, GcsServiceKey, S3Credential, S3Profile,
    StorageCredential, StorageProfile,
};
use futures::FutureExt;
use itertools::Itertools;

use crate::api::iceberg::v1::{PageToken, PaginationQuery};
use crate::service::NamespaceIdentUuid;

use super::default_page_size;
use crate::api::management::v1::role::require_project_id;
use crate::catalog::PageStatus;
pub use crate::service::WarehouseStatus;
use crate::service::{
    authz::Authorizer, secrets::SecretStore, Catalog, ListFlags, State, TabularIdentUuid,
    Transaction,
};
use crate::{ProjectIdent, WarehouseIdent, DEFAULT_PROJECT_ID};
use iceberg_ext::catalog::rest::ErrorModel;
use serde::Deserialize;
use utoipa::ToSchema;

#[derive(Debug, Deserialize, utoipa::IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct ListDeletedTabularsQuery {
    /// Filter by Namespace ID
    #[serde(default)]
    #[param(value_type=uuid::Uuid)]
    pub namespace_id: Option<NamespaceIdentUuid>,
    /// Next page token
    #[serde(default)]
    pub page_token: Option<String>,
    /// Signals an upper bound of the number of results that a client will receive.
    /// Default: 100
    #[serde(default = "default_page_size")]
    pub page_size: i64,
}

impl ListDeletedTabularsQuery {
    #[must_use]
    pub fn pagination_query(&self) -> PaginationQuery {
        PaginationQuery {
            page_token: self
                .page_token
                .clone()
                .map_or(PageToken::Empty, PageToken::Present),
            page_size: Some(self.page_size),
        }
    }
}

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
    #[serde(rename_all = "kebab-case")]
    Soft {
        #[serde(
            deserialize_with = "seconds_to_duration",
            serialize_with = "duration_to_seconds",
            alias = "expiration_seconds"
        )]
        #[schema(value_type=i32)]
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
    pub(crate) fn expiration_seconds(&self) -> Option<chrono::Duration> {
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
    /// Delete profile used for the warehouse.
    pub delete_profile: TabularDeleteProfile,
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
pub trait Service<C: Catalog, A: Authorizer, S: SecretStore> {
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
        let mut trx = C::Transaction::begin_read(context.v1_state.catalog).await?;

        let warehouses =
            C::list_warehouses(project_id, request.warehouse_status, trx.transaction()).await?;
        trx.commit().await?;

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

    #[allow(clippy::too_many_lines)]
    async fn list_soft_deleted_tabulars(
        request_metadata: RequestMetadata,
        warehouse_id: WarehouseIdent,
        context: ApiContext<State<A, C, S>>,
        query: ListDeletedTabularsQuery,
    ) -> Result<ListDeletedTabularsResponse> {
        // ------------------- AuthZ -------------------
        let catalog = context.v1_state.catalog;
        let authorizer = context.v1_state.authz;
        authorizer
            .require_warehouse_action(
                &request_metadata,
                warehouse_id,
                &CatalogWarehouseAction::CanListDeletedTabulars,
            )
            .await?;

        // ------------------- Business Logic -------------------
        let pagination_query = query.pagination_query();
        let namespace_id = query.namespace_id;
        let mut t = C::Transaction::begin_read(catalog.clone()).await?;
        let (tabulars, idents, next_page_token) =
            crate::catalog::fetch_until_full_page::<_, _, _, C>(
                pagination_query.page_size,
                pagination_query.page_token,
                |page_size, page_token, _| {
                    let catalog = catalog.clone();
                    let authorizer = authorizer.clone();
                    let request_metadata = request_metadata.clone();
                    async move {
                        let query = PaginationQuery {
                            page_size: Some(page_size),
                            page_token: page_token.into(),
                        };

                        let page = C::list_tabulars(
                            warehouse_id,
                            namespace_id,
                            ListFlags::only_deleted(),
                            catalog,
                            query,
                        )
                        .await?;
                        let (ids, idents, tokens): (Vec<_>, Vec<_>, Vec<_>) =
                            page.into_iter_with_page_tokens().multiunzip();
                        let before_filter_len = ids.len();

                        let (next_idents, next_uuids, next_page_tokens): (Vec<_>, Vec<_>, Vec<_>) =
                            futures::future::try_join_all(ids.iter().map(|tid| match tid {
                                TabularIdentUuid::View(id) => authorizer.is_allowed_view_action(
                                    &request_metadata,
                                    warehouse_id,
                                    (*id).into(),
                                    &crate::service::authz::CatalogViewAction::CanIncludeInList,
                                ),
                                TabularIdentUuid::Table(id) => authorizer.is_allowed_table_action(
                                    &request_metadata,
                                    warehouse_id,
                                    (*id).into(),
                                    &crate::service::authz::CatalogTableAction::CanIncludeInList,
                                ),
                            }))
                            .await?
                            .into_iter()
                            .zip(idents.into_iter().zip(ids.into_iter()))
                            .zip(tokens.into_iter())
                            .filter_map(|((allowed, namespace), token)| {
                                allowed.then_some((namespace.0, namespace.1, token))
                            })
                            .multiunzip();

                        let p = if before_filter_len == next_idents.len() {
                            if before_filter_len
                                == usize::try_from(page_size).expect("we sanitize page size")
                            {
                                PageStatus::Full
                            } else {
                                PageStatus::Partial
                            }
                        } else {
                            PageStatus::AuthFiltered
                        };
                        Ok((next_idents, next_uuids, next_page_tokens, p))
                    }
                    .boxed()
                },
                &mut t,
            )
            .await?;

        let tabulars = idents
            .into_iter()
            .zip(tabulars.into_iter())
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
            delete_profile: warehouse.tabular_delete_profile,
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

    #[needs_env_var::needs_env_var(TEST_MINIO = 1)]
    mod minio {
        use crate::api::iceberg::types::Prefix;
        use crate::api::iceberg::v1::{
            DataAccess, DropParams, NamespaceParameters, ViewParameters,
        };
        use crate::catalog::test::random_request_metadata;
        use crate::catalog::CatalogServer;
        use crate::service::authz::implementations::openfga::tests::ObjectHidingMock;
        use iceberg::TableIdent;

        use crate::api::iceberg::v1::views::Service;
        use crate::api::management::v1::warehouse::{
            ListDeletedTabularsQuery, Service as _, TabularDeleteProfile,
        };
        use crate::api::management::v1::ApiServer;
        use itertools::Itertools;

        #[sqlx::test]
        async fn test_deleted_tabulars_pagination(pool: sqlx::PgPool) {
            let (prof, cred) = crate::catalog::test::minio_profile();

            let hiding_mock = ObjectHidingMock::new();
            let authz = hiding_mock.to_authorizer();

            let (ctx, warehouse) = crate::catalog::test::setup(
                pool.clone(),
                prof,
                Some(cred),
                authz,
                TabularDeleteProfile::Soft {
                    expiration_seconds: chrono::Duration::seconds(10),
                },
            )
            .await;
            let ns = crate::catalog::test::create_ns(
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
            for i in 0..10 {
                let _ = CatalogServer::create_view(
                    ns_params.clone(),
                    crate::catalog::views::create::test::create_view_request(
                        Some(&format!("view-{i}")),
                        None,
                    ),
                    ctx.clone(),
                    DataAccess {
                        vended_credentials: true,
                        remote_signing: false,
                    },
                    random_request_metadata(),
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
                        purge_requested: None,
                    },
                    ctx.clone(),
                    random_request_metadata(),
                )
                .await
                .unwrap();
            }

            // list 1 more than existing tables
            let all = ApiServer::list_soft_deleted_tabulars(
                random_request_metadata(),
                warehouse.warehouse_id.into(),
                ctx.clone(),
                ListDeletedTabularsQuery {
                    namespace_id: None,
                    page_size: 11,
                    page_token: None,
                },
            )
            .await
            .unwrap();
            assert_eq!(all.tabulars.len(), 10);

            // list exactly amount of existing tables
            let all = ApiServer::list_soft_deleted_tabulars(
                random_request_metadata(),
                warehouse.warehouse_id.into(),
                ctx.clone(),
                ListDeletedTabularsQuery {
                    namespace_id: None,
                    page_size: 10,
                    page_token: None,
                },
            )
            .await
            .unwrap();
            assert_eq!(all.tabulars.len(), 10);

            // next page is empty
            let next = ApiServer::list_soft_deleted_tabulars(
                random_request_metadata(),
                warehouse.warehouse_id.into(),
                ctx.clone(),
                ListDeletedTabularsQuery {
                    namespace_id: None,
                    page_size: 10,
                    page_token: all.next_page_token,
                },
            )
            .await
            .unwrap();
            assert_eq!(next.tabulars.len(), 0);
            assert!(next.next_page_token.is_none());

            let first_six = ApiServer::list_soft_deleted_tabulars(
                random_request_metadata(),
                warehouse.warehouse_id.into(),
                ctx.clone(),
                ListDeletedTabularsQuery {
                    namespace_id: None,
                    page_size: 6,
                    page_token: None,
                },
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
                random_request_metadata(),
                warehouse.warehouse_id.into(),
                ctx.clone(),
                ListDeletedTabularsQuery {
                    namespace_id: None,
                    page_size: 6,
                    page_token: first_six.next_page_token,
                },
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
                hiding_mock.hide(&format!("view:{}", t.id));
            }

            let page = ApiServer::list_soft_deleted_tabulars(
                random_request_metadata(),
                warehouse.warehouse_id.into(),
                ctx.clone(),
                ListDeletedTabularsQuery {
                    namespace_id: None,
                    page_size: 5,
                    page_token: None,
                },
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
                random_request_metadata(),
                warehouse.warehouse_id.into(),
                ctx.clone(),
                ListDeletedTabularsQuery {
                    namespace_id: None,
                    page_size: 6,
                    page_token: page.next_page_token,
                },
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
}
