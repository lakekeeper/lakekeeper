mod commit;
pub(crate) mod create;
mod drop;
mod exists;
mod list;
mod load;
mod rename;

use super::tables::validate_table_properties;
use super::CatalogServer;
use crate::api::iceberg::types::DropParams;
use crate::api::iceberg::v1::{
    ApiContext, CommitViewRequest, CreateViewRequest, DataAccess, ListTablesQuery,
    ListTablesResponse, LoadViewResult, NamespaceParameters, Prefix, RenameTableRequest, Result,
    ViewParameters,
};
use crate::request_metadata::RequestMetadata;
use crate::service::authz::Authorizer;
use crate::service::{Catalog, SecretStore, State};
pub(crate) use exists::authorized_view_ident_to_id;
use iceberg_ext::catalog::rest::{ErrorModel, ViewUpdate};
use iceberg_ext::configs::Location;
use std::str::FromStr;

#[async_trait::async_trait]
impl<C: Catalog, A: Authorizer + Clone, S: SecretStore>
    crate::api::iceberg::v1::views::Service<State<A, C, S>> for CatalogServer<C, A, S>
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
        data_access: DataAccess,
        request_metadata: RequestMetadata,
    ) -> Result<LoadViewResult> {
        create::create_view(parameters, request, state, data_access, request_metadata).await
    }

    /// Load a view from the catalog
    async fn load_view(
        parameters: ViewParameters,
        state: ApiContext<State<A, C, S>>,
        data_access: DataAccess,
        request_metadata: RequestMetadata,
    ) -> Result<LoadViewResult> {
        load::load_view(parameters, state, data_access, request_metadata).await
    }

    /// Commit updates to a view
    async fn commit_view(
        parameters: ViewParameters,
        request: CommitViewRequest,
        state: ApiContext<State<A, C, S>>,
        data_access: DataAccess,
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

fn validate_view_properties<'a, I>(properties: I) -> Result<()>
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

fn parse_view_location(location: &str) -> Result<Location> {
    Ok(Location::from_str(location).map_err(|e| {
        ErrorModel::internal(
            format!("Invalid view location in DB: {e}"),
            "InvalidViewLocation",
            Some(Box::new(e)),
        )
    })?)
}

#[cfg(test)]
mod test {
    use crate::api::ApiContext;

    use crate::implementations::postgres::warehouse::test::initialize_warehouse;
    use crate::implementations::postgres::{PostgresCatalog, SecretsState};
    use crate::service::authz::AllowAllAuthorizer;
    use crate::service::storage::{StorageProfile, TestProfile};
    use crate::service::State;
    use crate::WarehouseIdent;

    use iceberg::NamespaceIdent;

    use crate::catalog::views::validate_view_properties;
    use crate::implementations::postgres::namespace::tests::initialize_namespace;
    use sqlx::PgPool;
    use uuid::Uuid;

    pub(crate) async fn setup(
        pool: PgPool,
        namespace_name: Option<Vec<String>>,
    ) -> (
        ApiContext<State<AllowAllAuthorizer, PostgresCatalog, SecretsState>>,
        NamespaceIdent,
        WarehouseIdent,
    ) {
        let api_context = get_api_context(pool, AllowAllAuthorizer);
        let state = api_context.v1_state.catalog.clone();
        let warehouse_id = initialize_warehouse(
            state.clone(),
            Some(StorageProfile::Test(TestProfile::default())),
            None,
            None,
            true,
        )
        .await;

        let namespace = initialize_namespace(
            state,
            warehouse_id,
            &NamespaceIdent::from_vec(namespace_name.unwrap_or(vec![Uuid::now_v7().to_string()]))
                .unwrap(),
            None,
        )
        .await
        .1
        .namespace;
        (api_context, namespace, warehouse_id)
    }

    pub(crate) use crate::catalog::test::get_api_context;

    #[test]
    fn test_mixed_case_properties() {
        let properties = ["a".to_string(), "B".to_string()];
        assert!(validate_view_properties(properties.iter()).is_ok());
    }
}
