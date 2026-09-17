//! Changing a dataset's settings after it was created.
use std::sync::Arc;

use chrono::Duration;
use iceberg::TableIdent;

use super::validate_constraints;
use crate::{
    api::{
        ApiContext, ErrorModel,
        data::v1::datasets::{
            DatasetData, DatasetParameters, LoadDatasetResponse, UpdateDatasetSettingsRequest,
        },
        iceberg::v1::Result,
    },
    request_metadata::RequestMetadata,
    server::require_warehouse_id,
    service::{
        CatalogDatasetOps, CatalogStore, DatasetRetention, DatasetSettingsUpdate, NamedEntity,
        SecretStore, State, TabularListFlags, Transaction,
        authz::{
            AuthZDatasetOps, AuthZError, Authorizer, CatalogDatasetAction,
            RequireDatasetActionError,
        },
        events::{
            APIEventContext,
            context::{ResolvedDataset, UserProvidedDataset},
        },
        tasks::{
            TaskEntity, WarehouseTaskEntityId,
            dataset_snapshot_expiry_queue::schedule_snapshot_expiry,
        },
    },
};

/// The longest duration a retention policy may name: a hundred years.
const MAX_RETENTION_DURATION: Duration = Duration::days(36_525);
const DURATIONS_OUT_OF_RANGE: &str = "durations must be between zero and 100 years";
const KEEPS_NO_HEAD: &str = "a policy must keep at least each branch's head: 1 or more";

/// Refuse a policy with a duration out of range, or one that keeps less than a
/// branch's head.
fn validate_retention(retention: &DatasetRetention) -> Result<()> {
    let out_of_range = |d: Option<&Duration>| {
        d.is_some_and(|d| *d < Duration::zero() || *d > MAX_RETENTION_DURATION)
    };
    let invalid = match retention {
        DatasetRetention::Inherit {} => None,
        DatasetRetention::Manual { grace_period } => {
            out_of_range(grace_period.as_ref()).then_some(DURATIONS_OUT_OF_RANGE)
        }
        DatasetRetention::Ttl {
            max_snapshot_age,
            min_snapshots_to_keep,
            grace_period,
        } => {
            if *min_snapshots_to_keep == Some(0) {
                Some(KEEPS_NO_HEAD)
            } else {
                (out_of_range(Some(max_snapshot_age)) || out_of_range(grace_period.as_ref()))
                    .then_some(DURATIONS_OUT_OF_RANGE)
            }
        }
        DatasetRetention::MaxCount {
            max_snapshots,
            grace_period,
        } => {
            if *max_snapshots == 0 {
                Some(KEEPS_NO_HEAD)
            } else {
                out_of_range(grace_period.as_ref()).then_some(DURATIONS_OUT_OF_RANGE)
            }
        }
    };
    match invalid {
        Some(reason) => Err(ErrorModel::bad_request(reason, "InvalidRetention", None).into()),
        None => Ok(()),
    }
}

pub(super) async fn update_dataset_settings<
    C: CatalogStore,
    A: Authorizer + Clone,
    S: SecretStore,
>(
    parameters: DatasetParameters,
    request: UpdateDatasetSettingsRequest,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<LoadDatasetResponse> {
    let DatasetParameters {
        prefix,
        namespace,
        dataset_name,
    } = parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    if let Some(constraints) = &request.constraints {
        validate_constraints(constraints)?;
    }
    if let Some(retention) = &request.retention {
        validate_retention(retention)?;
    }

    // Each setting changes under its own authority. Constraints bound what may be
    // committed: a write. A retention policy expires snapshots from then on, which
    // takes the authority expiring them by hand does. A request that names
    // neither is a settings write.
    let mut actions = Vec::new();
    if request.constraints.is_some() || request.retention.is_none() {
        actions.push(CatalogDatasetAction::UpdateSettings);
    }
    if request.retention.is_some() {
        actions.push(CatalogDatasetAction::UpdateRetention);
    }
    let dataset_ident = TableIdent::new(namespace, dataset_name);
    let event_ctx = APIEventContext::for_dataset(
        Arc::new(request_metadata.clone()),
        state.v1_state.events.clone(),
        warehouse_id,
        dataset_ident.clone(),
        actions.clone(),
    );
    let authorizer = state.v1_state.authz.clone();
    let authz_result = async {
        let (warehouse, namespace, info) = authorizer
            .load_and_authorize_dataset_operation::<C>(
                &request_metadata,
                &UserProvidedDataset::new(warehouse_id, dataset_ident.clone()),
                TabularListFlags::active(),
                actions[0].clone(),
                state.v1_state.catalog.clone(),
            )
            .await?;
        for action in &actions[1..] {
            authorizer
                .require_dataset_action(
                    &request_metadata,
                    &warehouse,
                    &namespace,
                    dataset_ident.clone(),
                    Ok::<_, RequireDatasetActionError>(Some(info.clone())),
                    action.clone(),
                )
                .await?;
        }
        Ok::<_, AuthZError>((warehouse, info))
    }
    .await;
    let (event_ctx, (warehouse, info)) = event_ctx.emit_authz(authz_result)?;

    let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
    C::update_dataset_settings(
        warehouse_id,
        info.tabular_id,
        DatasetSettingsUpdate {
            constraints: request.constraints.clone(),
            retention: request.retention.clone(),
        },
        t.transaction(),
    )
    .await?;
    // A policy applies from the next retention pass; queuing one here, as a commit
    // does, reaches a dataset nobody writes to as well.
    if request.retention.is_some() {
        schedule_snapshot_expiry::<C>(
            warehouse.project_id.clone(),
            TaskEntity::EntityInWarehouse {
                entity_name: dataset_ident.into_name_parts(),
                warehouse_id,
                entity_id: WarehouseTaskEntityId::Dataset {
                    dataset_id: info.tabular_id,
                },
            },
            &mut t,
        )
        .await?;
    }
    // Read back in the same transaction: a replica may not have the write yet.
    let updated = C::load_dataset_by_id(warehouse_id, info.tabular_id, t.transaction()).await?;
    t.commit().await?;

    let response = LoadDatasetResponse {
        dataset: DatasetData::from(&updated),
    };
    event_ctx
        .resolve(ResolvedDataset {
            warehouse,
            dataset: Arc::new(updated),
        })
        .emit_dataset_settings_updated_async(Arc::new(request));
    Ok(response)
}
