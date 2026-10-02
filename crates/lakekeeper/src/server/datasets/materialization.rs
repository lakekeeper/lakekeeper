//! What the last materialization check found for a snapshot.
use std::sync::Arc;

use iceberg::TableIdent;

use crate::{
    CONFIG,
    api::{
        ApiContext,
        data::v1::datasets::{
            DatasetSnapshotParameters, SnapshotMaterializationQuery,
            SnapshotMaterializationResponse,
        },
        iceberg::v1::Result,
    },
    request_metadata::RequestMetadata,
    server::require_warehouse_id,
    service::{
        CatalogDatasetOps, CatalogStore, SecretStore, State, TabularListFlags, Transaction,
        authz::{AuthZDatasetOps, Authorizer, CatalogDatasetAction},
        events::{APIEventContext, context::UserProvidedDataset},
    },
};

pub(super) async fn get_dataset_snapshot_materialization<
    C: CatalogStore,
    A: Authorizer + Clone,
    S: SecretStore,
>(
    parameters: DatasetSnapshotParameters,
    query: SnapshotMaterializationQuery,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<SnapshotMaterializationResponse> {
    let DatasetSnapshotParameters {
        prefix,
        namespace,
        dataset_name,
        snapshot_id,
    } = parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;

    // The missing files are file names, which reading the files would show: the
    // same action. A snapshot is not a ref, so no ref narrows it.
    let action = CatalogDatasetAction::ReadData {
        target_refs: Arc::default(),
    };
    let dataset_ident = TableIdent::new(namespace, dataset_name);
    let event_ctx = APIEventContext::for_dataset(
        Arc::new(request_metadata.clone()),
        state.v1_state.events.clone(),
        warehouse_id,
        dataset_ident.clone(),
        action.clone(),
    );
    let (_event_ctx, (_warehouse, _ns, info)) = event_ctx.emit_authz(
        state
            .v1_state
            .authz
            .load_and_authorize_dataset_operation::<C>(
                &request_metadata,
                &UserProvidedDataset::new(warehouse_id, dataset_ident),
                TabularListFlags::active(),
                action,
                state.v1_state.catalog.clone(),
            )
            .await,
    )?;

    let page_size = CONFIG.page_size_or_pagination_default(query.page_size);
    let mut t = C::Transaction::begin_read(state.v1_state.catalog).await?;
    let checked = C::get_snapshot_materialization(
        warehouse_id,
        info.tabular_id,
        snapshot_id,
        query.page_token.as_deref(),
        page_size,
        t.transaction(),
    )
    .await?;
    t.commit().await?;

    Ok(checked.into())
}
