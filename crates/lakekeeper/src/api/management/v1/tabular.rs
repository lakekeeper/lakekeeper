use std::sync::Arc;

use iceberg_ext::catalog::rest::ErrorModel;
use itertools::Itertools as _;
use serde::{Deserialize, Serialize};

use super::ApiServer;
use crate::{
    WarehouseId,
    api::{ApiContext, RequestMetadata, Result},
    service::{
        CatalogNamespaceOps, CatalogStore, CatalogTabularOps, CatalogWarehouseOps,
        ResolvedWarehouse, SecretStore, State, TabularId,
        authz::{
            AuthZCannotUseWarehouseId, AuthZTableOps, Authorizer, AuthzWarehouseOps,
            CatalogTableAction, CatalogViewAction, CatalogWarehouseAction,
            RequireWarehouseActionError,
        },
        events::{
            APIEventContext,
            context::{WarehouseActionSearchTabulars, authz_to_error_no_audit},
        },
        require_namespace_for_tabular,
    },
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

        let event_ctx = APIEventContext::for_warehouse(
            Arc::new(request_metadata),
            context.v1_state.events.clone(),
            warehouse_id,
            WarehouseActionSearchTabulars {},
        );

        let authz_result = authorize_search_tabular::<C, A>(
            event_ctx.request_metadata(),
            warehouse_id,
            &authorizer,
            context.v1_state.catalog.clone(),
        )
        .await;
        let (
            event_ctx,
            AuthorizeSearchTabularResult {
                warehouse,
                authz_list_all,
            },
        ) = event_ctx.emit_authz(authz_result)?;

        // -------------------- Business Logic & Tabular level AuthZ filters --------------------
        let mut search = request.search;
        if search.chars().count() > 64 {
            search = search.chars().take(64).collect();
        }
        let all_matches =
            C::search_tabular(warehouse_id, &search, context.v1_state.catalog.clone())
                .await?
                .search_results;
        let namespace_ids = all_matches
            .iter()
            .map(|t| t.tabular.namespace_id())
            .collect_vec();
        let namespaces =
            C::get_namespaces_by_id(warehouse_id, &namespace_ids, context.v1_state.catalog).await?;

        let actions = all_matches
            .iter()
            .map(|t| {
                Ok::<_, ErrorModel>((
                    require_namespace_for_tabular(&namespaces, t)
                        .map_err(authz_to_error_no_audit)?,
                    t.tabular.as_action_request(
                        CatalogViewAction::IncludeInList,
                        CatalogTableAction::IncludeInList,
                    ),
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let authz_decisions = if authz_list_all {
            vec![true; actions.len()]
        } else {
            authorizer
                .are_allowed_tabular_actions_vec(
                    event_ctx.request_metadata(),
                    None,
                    &warehouse,
                    &namespaces,
                    &actions,
                )
                .await
                .map_err(authz_to_error_no_audit)?
                .into_inner()
        };

        // Merge authorized tables and views and show best matches first.
        let mut authorized_tabulars = all_matches
            .into_iter()
            .zip(authz_decisions)
            .filter_map(|(t, allowed)| {
                if allowed {
                    let table_ident = t.tabular.tabular_ident().clone();
                    Some(SearchTabular {
                        namespace_name: table_ident.namespace.to_vec(),
                        tabular_name: table_ident.name.clone(),
                        tabular_id: t.tabular.tabular_id(),
                        distance: t.distance,
                    })
                } else {
                    None
                }
            })
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

struct AuthorizeSearchTabularResult {
    warehouse: Arc<ResolvedWarehouse>,
    authz_list_all: bool,
}

async fn authorize_search_tabular<C: CatalogStore, A: Authorizer>(
    request_metadata: &RequestMetadata,
    warehouse_id: WarehouseId,
    authorizer: &A,
    state: C::State,
) -> Result<AuthorizeSearchTabularResult, RequireWarehouseActionError> {
    let warehouse = C::get_active_warehouse_by_id(warehouse_id, state.clone()).await;
    let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;

    let [authz_can_use, authz_list_all] = authorizer
        .are_allowed_warehouse_actions_arr(
            request_metadata,
            None,
            &[
                (&warehouse, CatalogWarehouseAction::Use),
                (&warehouse, CatalogWarehouseAction::ListEverything),
            ],
        )
        .await?
        .into_inner();

    if !authz_can_use {
        return Err(AuthZCannotUseWarehouseId::new_access_denied(warehouse_id).into());
    }

    Ok(AuthorizeSearchTabularResult {
        warehouse,
        authz_list_all,
    })
}

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
pub struct SearchTabularRequest {
    /// Search string for fuzzy search.
    /// Length is truncated to 64 characters.
    #[cfg_attr(feature = "open-api", schema(max_length = 64))]
    pub search: String,
}

/// Search result for tabulars
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
pub struct SearchTabularResponse {
    /// List of tabulars matching the search criteria
    pub tabulars: Vec<SearchTabular>,
}

#[derive(Debug, Serialize, Clone)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
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
