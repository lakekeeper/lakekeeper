use super::{ApiServer, ProtectionResponse};
use crate::{
    api::{ApiContext, RequestMetadata, Result},
    service::{
        authz::{Authorizer, CatalogViewAction},
        Catalog, SecretStore, State, TabularId, Transaction, ViewId,
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
        // TODO(mooori) authz
        C::search_tabular(warehouse_id, &request.search, context.v1_state.catalog).await
        // TODO(mooori) mask tabulars that user cannot see
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
}
