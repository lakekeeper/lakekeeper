use std::sync::Arc;

use crate::{
    api::{
        ApiContext,
        data::v1::datasets::{DatasetData, DatasetParameters, LoadDatasetResponse},
    },
    request_metadata::RequestMetadata,
    server::require_warehouse_id,
    service::{
        CatalogDatasetOps, CatalogStore, Result, SecretStore, State, TabularListFlags, Transaction,
        authz::{AuthZDatasetOps, Authorizer, CatalogDatasetAction},
        events::{
            APIEventContext,
            context::{ResolvedDataset, UserProvidedDataset},
        },
    },
};

pub(super) async fn load_dataset<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: DatasetParameters,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<LoadDatasetResponse> {
    let DatasetParameters {
        prefix,
        namespace,
        dataset_name,
    } = parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    let authorizer = &state.v1_state.authz;

    // ------------------- AUTHZ -------------------
    let dataset_ident = iceberg::TableIdent::new(namespace.clone(), dataset_name.clone());
    let event_ctx = APIEventContext::for_dataset(
        Arc::new(request_metadata.clone()),
        state.v1_state.events.clone(),
        warehouse_id,
        dataset_ident.clone(),
        CatalogDatasetAction::GetMetadata,
    );

    let (event_ctx, (warehouse, _ns_hierarchy, tabular)) = event_ctx.emit_authz(
        authorizer
            .load_and_authorize_dataset_operation::<C>(
                &request_metadata,
                &UserProvidedDataset::new(
                    warehouse_id,
                    iceberg::TableIdent::new(namespace, dataset_name.clone()),
                ),
                TabularListFlags::active(),
                CatalogDatasetAction::GetMetadata,
                state.v1_state.catalog.clone(),
            )
            .await,
    )?;

    // By id, not by name: the authorized identity is the id, and a concurrent
    // rename plus create-with-the-same-name would otherwise return a different
    // dataset than the one authorized.
    let mut t = C::Transaction::begin_read(state.v1_state.catalog.clone()).await?;
    let info = C::load_dataset_by_id(warehouse_id, tabular.tabular_id, t.transaction()).await?;
    t.commit().await?;

    let info = Arc::new(info);
    let response = LoadDatasetResponse {
        dataset: DatasetData {
            name: info.name.clone(),
            id: info.dataset_id,
            location: info.location.to_string(),
            ownership: info.ownership,
            protected: info.protected,
            constraints: (!info.constraints.is_empty()).then(|| info.constraints.clone()),
        },
    };

    let event_ctx = event_ctx.resolve(ResolvedDataset {
        warehouse,
        dataset: info,
    });
    event_ctx.emit_dataset_loaded_async();

    Ok(response)
}
