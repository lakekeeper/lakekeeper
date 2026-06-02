pub mod commit;
pub mod create;
pub mod drop;
pub mod exists;
pub mod list;
pub mod load;
pub mod rename;

use iceberg_ext::catalog::rest::ViewUpdate;

use super::{CatalogServer, tables::validate_table_properties};
use crate::{
    api::iceberg::{
        types::DropParams,
        v1::{
            ApiContext, CommitViewRequest, CreateViewRequest, DataAccessMode, ListTablesQuery,
            ListTablesResponse, LoadViewResult, NamespaceParameters, Prefix, RenameTableRequest,
            Result, ViewParameters, views::LoadViewRequest,
        },
    },
    request_metadata::RequestMetadata,
    service::{CatalogStore, SecretStore, State, authz::Authorizer},
};

#[async_trait::async_trait]
impl<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>
    crate::api::iceberg::v1::views::ViewService<State<A, C, S>> for CatalogServer<C, A, S>
{
    /// List all view identifiers underneath a given namespace
    async fn list_views(
        parameters: NamespaceParameters,
        query: ListTablesQuery,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ListTablesResponse> {
        list::list_views(parameters, query, state, request_metadata).await
    }

    /// Create a view in the given namespace
    async fn create_view(
        parameters: NamespaceParameters,
        request: CreateViewRequest,
        state: ApiContext<State<A, C, S>>,
        data_access: impl Into<DataAccessMode> + Send,
        request_metadata: RequestMetadata,
    ) -> Result<LoadViewResult> {
        create::create_view(parameters, request, state, data_access, request_metadata).await
    }

    /// Load a view from the catalog
    async fn load_view(
        parameters: ViewParameters,
        request: LoadViewRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadViewResult> {
        load::load_view(parameters, request, state, request_metadata).await
    }

    /// Commit updates to a view
    async fn commit_view(
        parameters: ViewParameters,
        request: CommitViewRequest,
        state: ApiContext<State<A, C, S>>,
        data_access: impl Into<DataAccessMode> + Send,
        request_metadata: RequestMetadata,
    ) -> Result<LoadViewResult> {
        commit::commit_view(parameters, request, state, data_access, request_metadata).await
    }

    /// Drop a view from the catalog
    async fn drop_view(
        parameters: ViewParameters,
        drop_params: DropParams,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        drop::drop_view(parameters, drop_params, state, request_metadata).await
    }

    /// Check if a view exists
    async fn view_exists(
        parameters: ViewParameters,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        exists::view_exists(parameters, state, request_metadata).await
    }

    /// Rename a view
    async fn rename_view(
        prefix: Option<Prefix>,
        request: RenameTableRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        rename::rename_view(prefix, request, state, request_metadata).await
    }
}

pub fn validate_view_properties<'a, I>(properties: I) -> Result<()>
where
    I: IntoIterator<Item = &'a String>,
{
    validate_table_properties(properties)
}

fn validate_view_updates(updates: &Vec<ViewUpdate>) -> Result<()> {
    for update in updates {
        match update {
            ViewUpdate::SetProperties { updates } => {
                validate_view_properties(updates.keys())?;
            }
            ViewUpdate::RemoveProperties { removals } => {
                validate_view_properties(removals.iter())?;
            }
            _ => {}
        }
    }
    Ok(())
}
