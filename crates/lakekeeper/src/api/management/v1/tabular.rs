use super::{ApiServer, ProtectionResponse};
use crate::{
    api::{ApiContext, RequestMetadata, Result},
    service::{
        authz::{Authorizer, CatalogTableAction, CatalogViewAction, CatalogWarehouseAction},
        Catalog, SecretStore, State, TableId, TabularId, Transaction, ViewId,
    },
    WarehouseId,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::api::management::v1::TabularType;

impl<C: Catalog, A: Authorizer + Clone, S: SecretStore> TabularManagementService<C, A, S>
    for ApiServer<C, A, S>
{
}

#[async_trait::async_trait]
pub trait TabularManagementService<C: Catalog, A: Authorizer, S: SecretStore>
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

        let (authz_can_use, authz_list_all) = tokio::join!(
            authorizer.require_warehouse_action(
                &request_metadata,
                warehouse_id,
                CatalogWarehouseAction::CanUse,
            ),
            authorizer.is_allowed_warehouse_action(
                &request_metadata,
                warehouse_id,
                CatalogWarehouseAction::CanListEverything,
            )
        );
        authz_can_use
            .map_err(|e| e.append_detail("Not authorized to search tabulars in the Warehouse."))?;
        let authz_list_all = authz_list_all?.into_inner();

        // -------------------- Business Logic --------------------
        let all_matches =
            C::search_tabular(warehouse_id, &request.search, context.v1_state.catalog)
                .await?
                .tabulars;

        // Untangle tables and views as they must be checked for authz separately.
        // `search_tabular` returns only a small number of results, so we're rather trying
        // to keep this simple + readable instead of maximizing efficiency.
        let tables = all_matches
            .iter()
            .filter_map(|res| match res.tabular_type {
                TabularType::Table => Some(res.clone()),
                TabularType::View => None,
            })
            .collect::<Vec<_>>();
        let table_checks = tables
            .iter()
            .map(|res| (TableId::from(res.id), CatalogTableAction::CanIncludeInList))
            .collect::<Vec<_>>();
        let table_masks = if authz_list_all {
            vec![true; tables.len()]
        } else {
            authorizer
                .are_allowed_table_actions(&request_metadata, warehouse_id, table_checks)
                .await?
                .into_inner()
        };
        let mut authorized_tables = Vec::with_capacity(tables.len());
        for (i, t) in tables.into_iter().enumerate() {
            if table_masks.get(i).copied().unwrap_or(false) {
                authorized_tables.push(t);
            }
        }

        let views = all_matches
            .iter()
            .filter_map(|res| match res.tabular_type {
                TabularType::Table => None,
                TabularType::View => Some(res.clone()),
            })
            .collect::<Vec<_>>();
        let view_checks = views
            .iter()
            .map(|res| (ViewId::from(res.id), CatalogViewAction::CanIncludeInList))
            .collect::<Vec<_>>();
        let view_masks = if authz_list_all {
            vec![true; views.len()]
        } else {
            authorizer
                .are_allowed_view_actions(&request_metadata, warehouse_id, view_checks)
                .await?
                .into_inner()
        };
        let mut authorized_views = Vec::with_capacity(views.len());
        for (i, v) in views.into_iter().enumerate() {
            if view_masks.get(i).copied().unwrap_or(false) {
                authorized_views.push(v);
            }
        }

        // Merge authorized tables and views and show best matches first.
        let mut authorized_tabulars = authorized_tables
            .into_iter()
            .chain(authorized_views)
            .collect::<Vec<_>>();
        // sort `f32` by treating NaN as greater than any number
        authorized_tabulars.sort_by(|a, b| {
            a.dist
                .partial_cmp(&b.dist)
                .unwrap_or(std::cmp::Ordering::Greater)
        });

        Ok(SearchTabularResponse {
            tabulars: authorized_tabulars,
        })

        // TODO(mooori) handle search_term is uuid
    }
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct SearchTabularRequest {
    /// Search string for fuzzy search.
    /// Length is truncated to 64 characters.
    pub search: String,
}

/// Search result for users
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct SearchTabularResponse {
    /// List of users matching the search criteria
    pub tabulars: Vec<SearchTabular>,
}

#[derive(Debug, Serialize, utoipa::ToSchema, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct SearchTabular {
    /// Namespace name
    pub namespace_name: Vec<String>,
    /// Tabular name
    pub tabular_name: String,
    /// ID of the user
    #[schema(value_type=String)]
    pub id: Uuid,
    /// Type of the tabular
    pub tabular_type: TabularType,
    /// Better matches have a lower distanc
    pub dist: Option<f32>,
}
