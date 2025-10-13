use itertools::Itertools as _;
use serde::{Deserialize, Serialize};

use super::ApiServer;
use crate::{
    api::{ApiContext, RequestMetadata, Result},
    service::{
        authz::{
            AuthZCannotUseWarehouseId, Authorizer, AuthzWarehouseOps, CatalogTableAction,
            CatalogViewAction, CatalogWarehouseAction,
        },
        CatalogStore, SecretStore, State, TabularId,
    },
    WarehouseId,
};

impl<C: CatalogStore, A: Authorizer + Clone, S: SecretStore> TabularManagementService<C, A, S>
    for ApiServer<C, A, S>
{
}

#[async_trait::async_trait]
pub trait TabularManagementService<C: CatalogStore, A: Authorizer, S: SecretStore>
where
    Self: Send + Sync + 'static,
{
    async fn search_tabular(
        warehouse_id: WarehouseId,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        request: SearchTabularRequest,
    ) -> Result<SearchTabularResponse> {
        // -------------------- AUTHZ --------------------
        let authorizer = context.v1_state.authz;

        let [authz_can_use, authz_list_all] = authorizer
            .are_allowed_warehouse_actions_arr(
                &request_metadata,
                &[
                    (warehouse_id, CatalogWarehouseAction::CanUse),
                    (warehouse_id, CatalogWarehouseAction::CanListEverything),
                ],
            )
            .await?
            .into_inner();

        if !authz_can_use {
            return Err(AuthZCannotUseWarehouseId::new(warehouse_id).into());
        }

        // -------------------- Business Logic & Tabular level AuthZ filters --------------------
        let mut search = request.search;
        if search.chars().count() > 64 {
            search = search.chars().take(64).collect();
        }
        let all_matches = C::search_tabular(warehouse_id, &search, context.v1_state.catalog)
            .await?
            .tabulars;

        // Untangle tables and views as they must be checked for authz separately.
        // `search_tabular` returns only a small number of results, so we're rather trying
        // to keep this simple + readable instead of maximizing efficiency.
        let (table_checks, view_checks): (Vec<_>, Vec<_>) =
            all_matches
                .into_iter()
                .partition_map(|search_result| match search_result.tabular_id {
                    TabularId::Table(id) => itertools::Either::Left((
                        id,
                        CatalogTableAction::CanIncludeInList,
                        search_result,
                    )),
                    TabularId::View(id) => itertools::Either::Right((
                        id,
                        CatalogViewAction::CanIncludeInList,
                        search_result,
                    )),
                });
        let authorized_tables = if authz_list_all {
            table_checks.into_iter().map(|(_, _, sr)| sr).collect()
        } else {
            let table_checks_authz = table_checks
                .iter()
                .map(|(id, action, _)| (*id, *action))
                .collect_vec();
            authorizer
                .are_allowed_table_actions(&request_metadata, warehouse_id, table_checks_authz)
                .await?
                .into_inner()
                .into_iter()
                .zip(table_checks)
                .filter_map(|(is_allowed, (_, _, sr))| is_allowed.then_some(sr))
                .collect_vec()
        };

        let authorized_views = if authz_list_all {
            view_checks.into_iter().map(|(_, _, sr)| sr).collect()
        } else {
            let view_checks_authz = view_checks
                .iter()
                .map(|(id, action, _)| (*id, *action))
                .collect_vec();
            authorizer
                .are_allowed_view_actions(&request_metadata, warehouse_id, view_checks_authz)
                .await?
                .into_inner()
                .into_iter()
                .zip(view_checks)
                .filter_map(|(is_allowed, (_, _, sr))| is_allowed.then_some(sr))
                .collect_vec()
        };

        // Merge authorized tables and views and show best matches first.
        let mut authorized_tabulars = authorized_tables
            .into_iter()
            .chain(authorized_views)
            .collect::<Vec<_>>();
        // sort `f32` by treating NaN as greater than any number
        authorized_tabulars.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(std::cmp::Ordering::Greater)
        });

        Ok(SearchTabularResponse {
            tabulars: authorized_tabulars,
        })
    }
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct SearchTabularRequest {
    /// Search string for fuzzy search.
    /// Length is truncated to 64 characters.
    #[schema(max_length = 64)]
    pub search: String,
}

/// Search result for tabulars
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct SearchTabularResponse {
    /// List of tabulars matching the search criteria
    pub tabulars: Vec<SearchTabular>,
}

#[derive(Debug, Serialize, utoipa::ToSchema, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct SearchTabular {
    /// Namespace name
    pub namespace_name: Vec<String>,
    /// Tabular name
    pub tabular_name: String,
    /// ID of the tabular
    pub tabular_id: TabularId,
    /// Better matches have a lower distance
    pub distance: Option<f32>,
}
