//! Bringing back a snapshot retention expired, before it is purged, and expiring
//! one by hand.
use std::sync::Arc;

use iceberg::TableIdent;

use crate::{
    api::{
        ApiContext,
        data::v1::datasets::{
            DatasetSnapshotParameters, ExpireDatasetSnapshotResponse, SnapshotResponse,
        },
        iceberg::v1::Result,
    },
    request_metadata::RequestMetadata,
    server::require_warehouse_id,
    service::{
        CatalogDatasetOps, CatalogStore, NamedEntity, SecretStore, State, TabularListFlags,
        Transaction,
        authz::{AuthZDatasetOps, Authorizer, CatalogDatasetAction},
        events::{APIEventContext, context::UserProvidedDataset},
        tasks::{
            ScheduleTaskMetadata, TaskEntity, WarehouseTaskEntityId,
            dataset_snapshot_expiry_queue::{EffectiveRetention, purge_after, warehouse_retention},
            dataset_snapshot_purge_queue::{DatasetSnapshotPurgePayload, DatasetSnapshotPurgeTask},
        },
    },
};

pub(super) async fn restore_dataset_snapshot<
    C: CatalogStore,
    A: Authorizer + Clone,
    S: SecretStore,
>(
    parameters: DatasetSnapshotParameters,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<SnapshotResponse> {
    let DatasetSnapshotParameters {
        prefix,
        namespace,
        dataset_name,
        snapshot_id,
    } = parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;

    let action = CatalogDatasetAction::RestoreSnapshots;
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

    let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
    let snapshot =
        C::restore_dataset_snapshot(warehouse_id, info.tabular_id, snapshot_id, t.transaction())
            .await?;
    t.commit().await?;

    Ok(snapshot.into())
}

pub(super) async fn expire_dataset_snapshot<
    C: CatalogStore,
    A: Authorizer + Clone,
    S: SecretStore,
>(
    parameters: DatasetSnapshotParameters,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<ExpireDatasetSnapshotResponse> {
    let DatasetSnapshotParameters {
        prefix,
        namespace,
        dataset_name,
        snapshot_id,
    } = parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;

    let action = CatalogDatasetAction::ExpireSnapshots;
    let dataset_ident = TableIdent::new(namespace, dataset_name);
    let event_ctx = APIEventContext::for_dataset(
        Arc::new(request_metadata.clone()),
        state.v1_state.events.clone(),
        warehouse_id,
        dataset_ident.clone(),
        action.clone(),
    );
    let (_event_ctx, (warehouse, _ns, info)) = event_ctx.emit_authz(
        state
            .v1_state
            .authz
            .load_and_authorize_dataset_operation::<C>(
                &request_metadata,
                &UserProvidedDataset::new(warehouse_id, dataset_ident.clone()),
                TabularListFlags::active(),
                action,
                state.v1_state.catalog.clone(),
            )
            .await,
    )?;

    // The grace period of the dataset's own policy, or of the warehouse's.
    let warehouse_config =
        warehouse_retention::<C>(warehouse_id, state.v1_state.catalog.clone()).await?;
    let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
    let dataset = C::load_dataset_by_id(warehouse_id, info.tabular_id, t.transaction()).await?;
    let policy = EffectiveRetention::resolve(&dataset.retention, &warehouse_config);
    let purge_after = C::expire_dataset_snapshot(
        warehouse_id,
        info.tabular_id,
        snapshot_id,
        purge_after(chrono::Utc::now(), policy.grace_period),
        t.transaction(),
    )
    .await?;
    // A purge already pending keeps its time: the snapshot waits for it if due
    // before, and the purge reschedules itself for what is left. `purge_after` is
    // the earliest the snapshot is deleted, not when.
    DatasetSnapshotPurgeTask::schedule_task::<C>(
        ScheduleTaskMetadata {
            project_id: warehouse.project_id.clone(),
            parent_task_id: None,
            scheduled_for: Some(purge_after),
            entity: TaskEntity::EntityInWarehouse {
                entity_name: dataset_ident.into_name_parts(),
                warehouse_id,
                entity_id: WarehouseTaskEntityId::Dataset {
                    dataset_id: info.tabular_id,
                },
            },
        },
        DatasetSnapshotPurgePayload::default(),
        t.transaction(),
    )
    .await?;
    t.commit().await?;

    Ok(ExpireDatasetSnapshotResponse {
        snapshot_id,
        purge_after,
    })
}
