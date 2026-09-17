mod create;
mod credentials;
mod drop;
mod import;
pub(crate) use import::{ImportParams, run_import};
mod list;
mod load;
mod rename;
mod versioning;

use async_trait::async_trait;
use iceberg_ext::catalog::rest::ErrorModel;

use crate::{
    api::{
        ApiContext,
        data::v1::datasets::{
            CommitDatasetRequest, CreateDatasetRefRequest, CreateDatasetRequest, DatasetParameters,
            DatasetRefParameters, DatasetRefResponse, DatasetService, ListDatasetFilesQuery,
            ListDatasetFilesResponse, ListDatasetRefsResponse, ListDatasetsQuery,
            ListDatasetsResponse, LoadDatasetResponse, MoveDatasetRefRequest,
            SetDatasetRefProtectionRequest, SnapshotResponse,
        },
        iceberg::{types::DropParams, v1::namespace::NamespaceParameters},
    },
    request_metadata::RequestMetadata,
    server::CatalogServer,
    service::{
        CatalogBackendError, CatalogStore, IcebergErrorResponse, Result, SecretStore, State,
        authz::{AuthZError, Authorizer, RequireDatasetActionError},
    },
};

fn iceberg_err_to_authz(e: impl Into<IcebergErrorResponse>) -> AuthZError {
    let err_model = ErrorModel::from(e.into());
    AuthZError::RequireDatasetActionError(RequireDatasetActionError::CatalogBackendError(
        CatalogBackendError::new_unexpected(err_model),
    ))
}

/// Fetches and authorizes a dataset operation in one call.

#[async_trait]
impl<C: CatalogStore, A: Authorizer + Clone, S: SecretStore> DatasetService<State<A, C, S>>
    for CatalogServer<C, A, S>
{
    async fn create_dataset(
        parameters: NamespaceParameters,
        request: CreateDatasetRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadDatasetResponse> {
        create::create_dataset::<C, A, S>(parameters, request, state, request_metadata).await
    }

    async fn load_dataset(
        parameters: DatasetParameters,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadDatasetResponse> {
        load::load_dataset::<C, A, S>(parameters, state, request_metadata).await
    }

    async fn list_datasets(
        parameters: NamespaceParameters,
        query: ListDatasetsQuery,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ListDatasetsResponse> {
        list::list_datasets::<C, A, S>(parameters, query, state, request_metadata).await
    }

    async fn drop_dataset(
        parameters: DatasetParameters,
        drop_params: DropParams,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        drop::drop_dataset::<C, A, S>(parameters, drop_params, state, request_metadata).await
    }

    async fn commit_dataset(
        parameters: DatasetRefParameters,
        request: CommitDatasetRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<SnapshotResponse> {
        versioning::commit_dataset::<C, A, S>(parameters, request, state, request_metadata).await
    }

    async fn list_dataset_refs(
        parameters: DatasetParameters,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ListDatasetRefsResponse> {
        versioning::list_dataset_refs::<C, A, S>(parameters, state, request_metadata).await
    }

    async fn create_dataset_ref(
        parameters: DatasetParameters,
        request: CreateDatasetRefRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<DatasetRefResponse> {
        versioning::create_dataset_ref::<C, A, S>(parameters, request, state, request_metadata)
            .await
    }

    async fn move_dataset_ref(
        parameters: DatasetRefParameters,
        request: MoveDatasetRefRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<DatasetRefResponse> {
        versioning::move_dataset_ref::<C, A, S>(parameters, request, state, request_metadata).await
    }

    async fn delete_dataset_ref(
        parameters: DatasetRefParameters,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        versioning::delete_dataset_ref::<C, A, S>(parameters, state, request_metadata).await
    }

    async fn list_dataset_files(
        parameters: DatasetRefParameters,
        query: ListDatasetFilesQuery,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ListDatasetFilesResponse> {
        versioning::list_dataset_files::<C, A, S>(parameters, query, state, request_metadata).await
    }

    async fn set_dataset_ref_protection(
        parameters: DatasetRefParameters,
        request: SetDatasetRefProtectionRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<DatasetRefResponse> {
        versioning::set_dataset_ref_protection::<C, A, S>(
            parameters,
            request,
            state,
            request_metadata,
        )
        .await
    }

    async fn rename_dataset(
        prefix: Option<crate::api::iceberg::types::Prefix>,
        request: crate::api::data::v1::datasets::RenameDatasetRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        rename::rename_dataset(prefix, request, state, request_metadata).await
    }

    async fn load_dataset_credentials(
        parameters: crate::api::data::v1::datasets::DatasetParameters,
        request: crate::api::data::v1::datasets::LoadDatasetCredentialsRequest,
        data_access: crate::api::iceberg::v1::DataAccess,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<crate::api::data::v1::datasets::LoadDatasetCredentialsResponse> {
        credentials::load_dataset_credentials(
            parameters,
            request,
            data_access,
            state,
            request_metadata,
        )
        .await
    }

    async fn import_dataset(
        parameters: crate::api::data::v1::datasets::DatasetParameters,
        request: crate::api::data::v1::datasets::ImportDatasetRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<crate::api::data::v1::datasets::ImportDatasetResponse> {
        import::import_dataset(parameters, request, state, request_metadata).await
    }
}
