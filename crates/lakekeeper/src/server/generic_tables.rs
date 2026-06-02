mod create;
mod credentials;
mod drop;
mod list;
mod load;
mod rename;

use std::sync::Arc;

use async_trait::async_trait;
use iceberg::{NamespaceIdent, TableIdent};
use iceberg_ext::catalog::rest::ErrorModel;

use crate::{
    api::{
        ApiContext,
        data::v1::generic_tables::{
            CreateGenericTableRequest, GenericTableParameters, GenericTableService,
            ListGenericTablesQuery, ListGenericTablesResponse, LoadGenericTableCredentialsRequest,
            LoadGenericTableCredentialsResponse, LoadGenericTableResponse,
            RenameGenericTableRequest,
        },
        iceberg::{
            types::{DropParams, Prefix},
            v1::{DataAccess, DataAccessMode, namespace::NamespaceParameters},
        },
    },
    request_metadata::RequestMetadata,
    server::CatalogServer,
    service::{
        CatalogBackendError, CatalogGenericTableOps, CatalogNamespaceOps, CatalogStore,
        CatalogWarehouseOps, GenericTableInfo, IcebergErrorResponse, LoadGenericTableError,
        NamespaceHierarchy, ResolvedWarehouse, Result, SecretStore, State, Transaction,
        WarehouseId,
        authz::{
            AuthZCannotSeeGenericTable, AuthZError, AuthZGenericTableOps, Authorizer,
            AuthzNamespaceOps, AuthzWarehouseOps, RequireGenericTableActionError,
            refresh_warehouse_and_namespace_if_needed,
        },
    },
};

/// Fetches and authorizes a generic table operation in one call.
async fn load_and_authorize_generic_table_operation<C: CatalogStore, A: Authorizer + Clone>(
    authorizer: &A,
    request_metadata: &RequestMetadata,
    warehouse_id: WarehouseId,
    namespace: NamespaceIdent,
    table_name: &str,
    action: impl Into<A::GenericTableAction> + Send,
    catalog_state: C::State,
) -> std::result::Result<(Arc<ResolvedWarehouse>, NamespaceHierarchy, GenericTableInfo), AuthZError>
{
    let (warehouse_result, namespace_result) = tokio::join!(
        C::get_active_warehouse_by_id(warehouse_id, catalog_state.clone()),
        C::get_namespace(warehouse_id, namespace.clone(), catalog_state.clone()),
    );

    let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse_result)?;
    let namespace_hierarchy =
        authorizer.require_namespace_presence(warehouse_id, namespace.clone(), namespace_result)?;

    let namespace_id = namespace_hierarchy.namespace.namespace_id();

    let table_ident = TableIdent::new(namespace.clone(), table_name.to_string());

    let mut t = C::Transaction::begin_read(catalog_state.clone())
        .await
        .map_err(iceberg_err_to_authz)?;
    let info = match C::load_generic_table(warehouse_id, namespace_id, table_name, t.transaction())
        .await
    {
        Ok(info) => info,
        Err(LoadGenericTableError::GenericTableNotFound(_)) => {
            return Err(
                AuthZCannotSeeGenericTable::new_not_found(warehouse_id, table_ident).into(),
            );
        }
        Err(e) => return Err(iceberg_err_to_authz(e)),
    };
    t.commit().await.map_err(iceberg_err_to_authz)?;

    let (warehouse, namespace_hierarchy) = refresh_warehouse_and_namespace_if_needed::<C, _, _>(
        &warehouse,
        namespace_hierarchy,
        &info,
        AuthZCannotSeeGenericTable::new_not_found(warehouse_id, table_ident.clone()),
        authorizer,
        catalog_state,
    )
    .await?;

    let info = authorizer
        .require_generic_table_action(
            request_metadata,
            &warehouse,
            &namespace_hierarchy,
            table_ident,
            Ok::<_, RequireGenericTableActionError>(Some(info)),
            action,
        )
        .await?;

    Ok((warehouse, namespace_hierarchy, info))
}

/// Convert a catalog error into `AuthZError` via `CatalogBackendError`.
fn iceberg_err_to_authz(e: impl Into<IcebergErrorResponse>) -> AuthZError {
    let err_model = ErrorModel::from(e.into());
    AuthZError::RequireGenericTableActionError(RequireGenericTableActionError::CatalogBackendError(
        CatalogBackendError::new_unexpected(err_model),
    ))
}

#[async_trait]
impl<C: CatalogStore, A: Authorizer + Clone, S: SecretStore> GenericTableService<State<A, C, S>>
    for CatalogServer<C, A, S>
{
    async fn create_generic_table(
        parameters: NamespaceParameters,
        request: CreateGenericTableRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadGenericTableResponse> {
        create::create_generic_table::<C, A, S>(parameters, request, state, request_metadata).await
    }

    async fn load_generic_table(
        parameters: GenericTableParameters,
        state: ApiContext<State<A, C, S>>,
        data_access: impl Into<DataAccessMode> + Send,
        request_metadata: RequestMetadata,
    ) -> Result<LoadGenericTableResponse> {
        load::load_generic_table::<C, A, S>(parameters, state, data_access, request_metadata).await
    }

    async fn load_generic_table_credentials(
        parameters: GenericTableParameters,
        request: LoadGenericTableCredentialsRequest,
        data_access: DataAccess,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadGenericTableCredentialsResponse> {
        credentials::load_generic_table_credentials::<C, A, S>(
            parameters,
            request,
            data_access,
            state,
            request_metadata,
        )
        .await
    }

    async fn list_generic_tables(
        parameters: NamespaceParameters,
        query: ListGenericTablesQuery,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ListGenericTablesResponse> {
        list::list_generic_tables::<C, A, S>(parameters, query, state, request_metadata).await
    }

    async fn drop_generic_table(
        parameters: GenericTableParameters,
        drop_params: DropParams,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        drop::drop_generic_table::<C, A, S>(parameters, drop_params, state, request_metadata).await
    }

    async fn rename_generic_table(
        prefix: Option<Prefix>,
        request: RenameGenericTableRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        rename::rename_generic_table::<C, A, S>(prefix, request, state, request_metadata).await
    }
}
