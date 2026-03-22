mod create;
mod drop;
mod list;
mod load;

use async_trait::async_trait;

use crate::{
    api::{
        ApiContext,
        iceberg::v1::{DataAccessMode, namespace::NamespaceParameters},
        v1::generic_tables::{
            CreateGenericTableRequest, GenericTableParameters, GenericTableService,
            ListGenericTablesQuery, ListGenericTablesResponse, LoadGenericTableResponse,
        },
    },
    request_metadata::RequestMetadata,
    server::CatalogServer,
    service::{CatalogStore, Result, SecretStore, State, authz::Authorizer},
};

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
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        drop::drop_generic_table::<C, A, S>(parameters, state, request_metadata).await
    }
}
