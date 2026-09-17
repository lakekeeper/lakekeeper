use std::{
    collections::{HashMap, HashSet},
    str::FromStr as _,
    sync::Arc,
};

use http::StatusCode;
use iceberg::TableIdent;
use lakekeeper_io::check_unsafe_chars;
use uuid::Uuid;

use super::{
    access::access_mode, authorize_replay, idempotency_key_reused, physical_location,
    resolves_elsewhere, schedule_after_publish, target_ref, validate_key,
};
use crate::{
    api::{
        ApiContext, ErrorModel,
        data::v1::datasets::{
            CommitDatasetRequest, CommitFile, CreateDatasetRefRequest, DatasetParameters,
            DatasetRefParameters, DatasetRefResponse, DatasetRefSource, ListDatasetFilesQuery,
            ListDatasetFilesResponse, ListDatasetRefsResponse, MoveDatasetRefRequest,
            RefMaterialization, SetDatasetRefProtectionRequest, SnapshotResponse,
        },
        endpoints::EndpointFlat,
    },
    request_metadata::RequestMetadata,
    server::{require_warehouse_id, tabular::claim_idempotency_key, validate_blob_size},
    service::{
        CatalogDatasetOps, CatalogIdempotencyOps, CatalogStore, DEFAULT_DATASET_BRANCH,
        DatasetCommit, DatasetCommitOutcome, DatasetId, DatasetRef, DatasetSnapshotId, Location,
        ManifestEntry, NamedEntity, RecordedCommit, Result, SecretStore, State, TabularListFlags,
        Transaction, WarehouseId,
        authz::{AuthZDatasetOps, Authorizer, CatalogDatasetAction},
        events::{
            APIEventContext, DatasetPublished,
            context::{ResolvedDataset, UserProvidedDataset},
        },
        idempotency::IdempotencyKey,
        tasks::{
            TaskEntity, WarehouseTaskEntityId,
            dataset_snapshot_expiry_queue::schedule_snapshot_expiry,
        },
    },
};

fn to_ref_response(r: DatasetRef) -> DatasetRefResponse {
    DatasetRefResponse {
        name: r.name,
        typ: r.typ,
        snapshot_id: r.snapshot_id,
        protected: r.protected,
        materialization: None,
    }
}

/// A commit names each key once: a key both added and removed, or added twice,
/// has no single meaning.
fn validate_commit_keys(request: &CommitDatasetRequest) -> Result<()> {
    let mut seen = HashSet::with_capacity(request.added.len() + request.removed.len());
    for file in &request.added {
        validate_key(&file.logical_key, "logical-key")?;
        // A full URI is checked against the location once it is known.
        if let Some(path) = file.physical_path.as_deref().filter(|p| !p.contains("://")) {
            validate_key(path, "physical-path")?;
        }
    }
    for key in &request.removed {
        validate_key(key, "removed key")?;
    }
    let keys = request
        .added
        .iter()
        .map(|f| f.logical_key.as_str())
        .chain(request.removed.iter().map(String::as_str));
    for key in keys {
        if !seen.insert(key) {
            return Err(ErrorModel::bad_request(
                format!("A commit names each key once; '{key}' appears more than once"),
                "DuplicateKey",
                None,
            )
            .into());
        }
    }
    Ok(())
}

/// Where a file's bytes are, its physical path or else its key, is relative to the
/// dataset or a full URI inside it. One outside would point readers at objects
/// this dataset does not govern, and be signed with the warehouse's credential.
fn validate_physical_paths(added: &[CommitFile], location: &Location) -> Result<()> {
    for file in added {
        let path = file.physical_path.as_deref().unwrap_or(&file.logical_key);
        // Below the location, not the location itself, and read back as storage
        // will parse it: one it refuses could not be signed.
        let readable = !resolves_elsewhere(path)
            && physical_location(location, path).is_ok_and(|uri| {
                uri.is_prefix_within(location) && Location::from_str(uri.as_str()).is_ok()
            });
        if !readable {
            return Err(ErrorModel::bad_request(
                format!("physical-path must lie within the dataset location, got '{path}'"),
                "PhysicalPathOutsideDataset",
                None,
            )
            .into());
        }
    }
    Ok(())
}

/// Ref names appear as a path segment, so anything that would make a ref
/// unaddressable is refused at creation.
fn validate_ref_name(name: &str) -> Result<()> {
    const MAX_REF_NAME_LEN: usize = 255;

    if name.is_empty() {
        return Err(
            ErrorModel::bad_request("Ref name must not be empty", "InvalidRefName", None).into(),
        );
    }
    if name.len() > MAX_REF_NAME_LEN {
        return Err(ErrorModel::bad_request(
            format!("Ref name must be at most {MAX_REF_NAME_LEN} characters"),
            "InvalidRefName",
            None,
        )
        .into());
    }
    // `/` would split the path segment; `%`, `?` and `#` change how the URL parses.
    // Control characters and whitespace make a ref that cannot be typed back.
    if let Some(bad) = name
        .chars()
        .find(|c| matches!(c, '/' | '%' | '?' | '#') || c.is_control() || c.is_whitespace())
    {
        return Err(ErrorModel::bad_request(
            format!("Ref name must not contain '{bad}'"),
            "InvalidRefName",
            None,
        )
        .into());
    }
    // A format character renders as nothing: `main` with a zero-width space in it
    // would look like `main`.
    if let Err(reason) = check_unsafe_chars(name) {
        return Err(ErrorModel::bad_request(
            format!("Ref name must not hold a {reason}"),
            "InvalidRefName",
            None,
        )
        .into());
    }
    // A name that is only dots resolves as a path traversal segment.
    if name.chars().all(|c| c == '.') {
        return Err(ErrorModel::bad_request(
            format!("Ref name '{name}' is reserved"),
            "InvalidRefName",
            None,
        )
        .into());
    }
    Ok(())
}

// A sequence of phases; the parts worth naming are already extracted.
#[allow(clippy::too_many_lines)]
pub(super) async fn commit_dataset<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: DatasetRefParameters,
    request: CommitDatasetRequest,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<SnapshotResponse> {
    let DatasetRefParameters {
        prefix,
        namespace,
        dataset_name,
        ref_name: branch,
    } = parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    let authorizer = &state.v1_state.authz;

    validate_commit_keys(&request)?;
    validate_blob_size("Dataset commit summary", request.summary.as_ref())?;

    let idempotency_key = request_metadata.idempotency_key().copied();
    let action = CatalogDatasetAction::Commit {
        target_refs: target_ref(&branch),
    };
    let dataset_ident = TableIdent::new(namespace.clone(), dataset_name.clone());
    // Both are consumed below — the ident by the event context, the branch by the
    // commit — but a checkpoint task may still need them afterwards.
    let dataset_ident_for_task = dataset_ident.clone();
    let branch_for_task = branch.clone();
    let event_ctx = APIEventContext::for_dataset(
        Arc::new(request_metadata.clone()),
        state.v1_state.events.clone(),
        warehouse_id,
        dataset_ident,
        action.clone(),
    );

    // A retry is answered with the snapshot the commit published, not the head
    // at retry time: a commit may have landed on top of it since.
    if let Some(key) = idempotency_key
        && C::check_idempotency_key(
            warehouse_id,
            &key,
            EndpointFlat::DatasetV1CommitDataset,
            state.v1_state.catalog.clone(),
        )
        .await?
        .is_replay()
    {
        return replay_commit::<C, A, S>(
            warehouse_id,
            TableIdent::new(namespace, dataset_name),
            action,
            key,
            state,
            &request_metadata,
        )
        .await;
    }

    let (event_ctx, (warehouse, _ns, info)) = event_ctx.emit_authz(
        authorizer
            .load_and_authorize_dataset_operation::<C>(
                &request_metadata,
                &UserProvidedDataset::new(
                    warehouse_id,
                    TableIdent::new(namespace, dataset_name.clone()),
                ),
                TabularListFlags::active(),
                action,
                state.v1_state.catalog.clone(),
            )
            .await,
    )?;

    validate_physical_paths(&request.added, &info.location)?;

    let commit = DatasetCommit {
        warehouse_id,
        dataset_id: info.tabular_id,
        branch,
        expected_snapshot_id: request.parent_snapshot_id,
        snapshot_id: DatasetSnapshotId::from(Uuid::now_v7()),
        location: info.location.to_string(),
        added: request.added.into_iter().map(ManifestEntry::from).collect(),
        // The catalog commit API takes adds and removes; a caller that rewrote a
        // file sends it as an add, and newest-wins resolves it the same way.
        modified: Vec::new(),
        removed: request.removed,
        summary: request.summary,
        idempotency_key,
        on_constraint_violation: request.on_constraint_violation.unwrap_or_default(),
    };

    // Claimed before the commit, so a concurrent request with the same key waits
    // on it and then fails as in progress: the change is committed once.
    let t = C::Transaction::begin_write(state.v1_state.catalog).await?;
    let mut t = claim_idempotency_key::<C>(
        t,
        warehouse_id,
        idempotency_key,
        EndpointFlat::DatasetV1CommitDataset,
        StatusCode::OK,
    )
    .await?;
    // The key's record lapses before the snapshot that carries the key: a retry
    // after that is answered from the snapshot.
    if let Some(key) = idempotency_key
        && let Some(recorded) =
            C::get_dataset_snapshot_by_idempotency_key(warehouse_id, key, t.transaction()).await?
    {
        let response = recorded_commit(recorded, info.tabular_id)?;
        t.commit().await?;
        return Ok(response);
    }
    let outcome = C::commit_dataset(commit, t.transaction()).await?;
    let dataset = C::load_dataset_by_id(warehouse_id, info.tabular_id, t.transaction()).await?;

    schedule_after_publish::<C>(
        warehouse.project_id.clone(),
        TaskEntity::EntityInWarehouse {
            entity_name: dataset_ident_for_task.into_name_parts(),
            warehouse_id,
            entity_id: WarehouseTaskEntityId::Dataset {
                dataset_id: info.tabular_id,
            },
        },
        &branch_for_task,
        outcome.checkpoint_due,
        &mut t,
    )
    .await?;

    t.commit().await?;

    let DatasetCommitOutcome {
        snapshot, staged, ..
    } = outcome;
    event_ctx
        .resolve(ResolvedDataset {
            warehouse,
            dataset: Arc::new(dataset),
        })
        .emit_dataset_committed_async(DatasetPublished {
            branch: branch_for_task,
            snapshot_id: snapshot.snapshot_id,
            parent_snapshot_id: snapshot.parent_snapshot_id,
            summary: snapshot.summary.clone(),
            changes: staged.staged,
        });
    Ok(SnapshotResponse {
        skipped: staged.skipped,
        ..SnapshotResponse::from(snapshot)
    })
}

/// The snapshot a replayed commit published, authorized as the commit was: the
/// answer tells what it published, which no read of the dataset shows.
async fn replay_commit<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    warehouse_id: WarehouseId,
    dataset: TableIdent,
    action: CatalogDatasetAction,
    key: IdempotencyKey,
    state: ApiContext<State<A, C, S>>,
    request_metadata: &RequestMetadata,
) -> Result<SnapshotResponse> {
    let (_warehouse, info) =
        authorize_replay::<C, A, S>(warehouse_id, dataset, action, &state, request_metadata)
            .await?;
    // The primary: the record was read there, and a replica may lag it.
    let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
    let recorded =
        C::get_dataset_snapshot_by_idempotency_key(warehouse_id, key, t.transaction()).await?;
    t.commit().await?;
    match recorded {
        Some(recorded) => recorded_commit(recorded, info.tabular_id),
        None => Err(ErrorModel::not_found(
            "The snapshot this Idempotency-Key's commit published has been purged",
            "DatasetSnapshotNotFound",
            None,
        )
        .into()),
    }
}

/// The answer to a commit's retry, if `recorded` is that commit's.
fn recorded_commit(recorded: RecordedCommit, dataset_id: DatasetId) -> Result<SnapshotResponse> {
    if recorded.dataset_id != dataset_id {
        return Err(idempotency_key_reused().into());
    }
    Ok(SnapshotResponse {
        skipped: recorded.skipped,
        ..SnapshotResponse::from(recorded.snapshot)
    })
}

/// The ref a replayed create or move names, as it stands now: a ref operation
/// records no row of its own, so its replay reports the current state.
async fn load_ref_for_replay<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    warehouse_id: WarehouseId,
    dataset: TableIdent,
    ref_name: &str,
    state: ApiContext<State<A, C, S>>,
    request_metadata: &RequestMetadata,
) -> Result<DatasetRefResponse> {
    let (_warehouse, info) = authorize_replay::<C, A, S>(
        warehouse_id,
        dataset,
        CatalogDatasetAction::GetMetadata,
        &state,
        request_metadata,
    )
    .await?;
    let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
    let found =
        C::get_dataset_ref(warehouse_id, info.tabular_id, ref_name, t.transaction()).await?;
    t.commit().await?;
    Ok(to_ref_response(found))
}

pub(super) async fn list_dataset_refs<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: DatasetParameters,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<ListDatasetRefsResponse> {
    let DatasetParameters {
        prefix,
        namespace,
        dataset_name,
    } = parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;

    let dataset_ident = TableIdent::new(namespace.clone(), dataset_name.clone());
    let event_ctx = APIEventContext::for_dataset(
        Arc::new(request_metadata.clone()),
        state.v1_state.events.clone(),
        warehouse_id,
        dataset_ident,
        CatalogDatasetAction::GetMetadata,
    );

    let (_event_ctx, (_warehouse, _ns, info)) = event_ctx.emit_authz(
        state
            .v1_state
            .authz
            .load_and_authorize_dataset_operation::<C>(
                &request_metadata,
                &UserProvidedDataset::new(
                    warehouse_id,
                    TableIdent::new(namespace, dataset_name.clone()),
                ),
                TabularListFlags::active(),
                CatalogDatasetAction::GetMetadata,
                state.v1_state.catalog.clone(),
            )
            .await,
    )?;

    let mut t = C::Transaction::begin_read(state.v1_state.catalog).await?;
    let refs = C::list_dataset_refs(warehouse_id, info.tabular_id, t.transaction()).await?;
    let snapshots: Vec<DatasetSnapshotId> = refs.iter().filter_map(|r| r.snapshot_id).collect();
    let checked: HashMap<DatasetSnapshotId, RefMaterialization> =
        C::get_snapshot_materialization_statuses(warehouse_id, &snapshots, t.transaction())
            .await?
            .into_iter()
            .map(|checked| (checked.snapshot_id, RefMaterialization::from(checked)))
            .collect();
    t.commit().await?;

    Ok(ListDatasetRefsResponse {
        refs: refs
            .into_iter()
            .map(|r| {
                let materialization = r.snapshot_id.and_then(|id| checked.get(&id).cloned());
                DatasetRefResponse {
                    materialization,
                    ..to_ref_response(r)
                }
            })
            .collect(),
    })
}

pub(super) async fn create_dataset_ref<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: DatasetParameters,
    request: CreateDatasetRefRequest,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<DatasetRefResponse> {
    let DatasetParameters {
        prefix,
        namespace,
        dataset_name,
    } = parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    validate_ref_name(&request.name)?;

    let idempotency_key = request_metadata.idempotency_key().copied();
    let action = CatalogDatasetAction::ManageRefs {
        target_refs: target_ref(&request.name),
    };
    let dataset_ident = TableIdent::new(namespace.clone(), dataset_name.clone());
    let event_ctx = APIEventContext::for_dataset(
        Arc::new(request_metadata.clone()),
        state.v1_state.events.clone(),
        warehouse_id,
        dataset_ident,
        action.clone(),
    );

    if let Some(key) = idempotency_key
        && C::check_idempotency_key(
            warehouse_id,
            &key,
            EndpointFlat::DatasetV1CreateDatasetRef,
            state.v1_state.catalog.clone(),
        )
        .await?
        .is_replay()
    {
        return load_ref_for_replay::<C, A, S>(
            warehouse_id,
            TableIdent::new(namespace, dataset_name),
            &request.name,
            state,
            &request_metadata,
        )
        .await;
    }

    let (event_ctx, (warehouse, _ns, info)) = event_ctx.emit_authz(
        state
            .v1_state
            .authz
            .load_and_authorize_dataset_operation::<C>(
                &request_metadata,
                &UserProvidedDataset::new(
                    warehouse_id,
                    TableIdent::new(namespace, dataset_name.clone()),
                ),
                TabularListFlags::active(),
                action,
                state.v1_state.catalog.clone(),
            )
            .await,
    )?;

    // Resolved in the write transaction: a replica may not have seen the
    // source's latest commit yet.
    let t = C::Transaction::begin_write(state.v1_state.catalog).await?;
    let mut t = claim_idempotency_key::<C>(
        t,
        warehouse_id,
        idempotency_key,
        EndpointFlat::DatasetV1CreateDatasetRef,
        StatusCode::OK,
    )
    .await?;
    let snapshot_id = match request.source {
        DatasetRefSource::Snapshot { snapshot_id } => snapshot_id,
        DatasetRefSource::Ref { name } => {
            C::get_dataset_ref(warehouse_id, info.tabular_id, &name, t.transaction())
                .await?
                .snapshot_id
                .ok_or_else(|| {
                    ErrorModel::conflict(
                        format!("Ref '{name}' has no commits to branch or tag from"),
                        "DatasetRefHasNoSnapshot",
                        None,
                    )
                })?
        }
    };
    let created = C::create_dataset_ref(
        warehouse_id,
        info.tabular_id,
        &request.name,
        request.typ,
        snapshot_id,
        t.transaction(),
    )
    .await?;
    let dataset = C::load_dataset_by_id(warehouse_id, info.tabular_id, t.transaction()).await?;
    t.commit().await?;

    let response = to_ref_response(created.clone());
    event_ctx
        .resolve(ResolvedDataset {
            warehouse,
            dataset: Arc::new(dataset),
        })
        .emit_dataset_ref_created_async(created);
    Ok(response)
}

pub(super) async fn move_dataset_ref<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: DatasetRefParameters,
    request: MoveDatasetRefRequest,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<DatasetRefResponse> {
    let DatasetRefParameters {
        prefix,
        namespace,
        dataset_name,
        ref_name,
    } = parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;

    // A fast-forward only ever extends history, so it is the publish step and
    // takes `Promote`. A reset can abandon commits; it takes `Reset`, which ref
    // management grants, as it does creating, deleting and protecting refs.
    let target_refs = target_ref(&ref_name);
    let action = if request.fast_forward {
        CatalogDatasetAction::Promote { target_refs }
    } else {
        CatalogDatasetAction::Reset { target_refs }
    };

    let idempotency_key = request_metadata.idempotency_key().copied();
    let dataset_ident = TableIdent::new(namespace.clone(), dataset_name.clone());
    let event_ctx = APIEventContext::for_dataset(
        Arc::new(request_metadata.clone()),
        state.v1_state.events.clone(),
        warehouse_id,
        dataset_ident,
        action.clone(),
    );

    if let Some(key) = idempotency_key
        && C::check_idempotency_key(
            warehouse_id,
            &key,
            EndpointFlat::DatasetV1MoveDatasetRef,
            state.v1_state.catalog.clone(),
        )
        .await?
        .is_replay()
    {
        return load_ref_for_replay::<C, A, S>(
            warehouse_id,
            TableIdent::new(namespace, dataset_name),
            &ref_name,
            state,
            &request_metadata,
        )
        .await;
    }

    let (event_ctx, (warehouse, _ns, info)) = event_ctx.emit_authz(
        state
            .v1_state
            .authz
            .load_and_authorize_dataset_operation::<C>(
                &request_metadata,
                &UserProvidedDataset::new(
                    warehouse_id,
                    TableIdent::new(namespace, dataset_name.clone()),
                ),
                TabularListFlags::active(),
                action,
                state.v1_state.catalog.clone(),
            )
            .await,
    )?;

    let t = C::Transaction::begin_write(state.v1_state.catalog).await?;
    let mut t = claim_idempotency_key::<C>(
        t,
        warehouse_id,
        idempotency_key,
        EndpointFlat::DatasetV1MoveDatasetRef,
        StatusCode::OK,
    )
    .await?;
    let moved = C::move_dataset_ref(
        warehouse_id,
        info.tabular_id,
        &ref_name,
        request.snapshot_id,
        request.expected_snapshot_id,
        request.fast_forward,
        t.transaction(),
    )
    .await?;
    let dataset = C::load_dataset_by_id(warehouse_id, info.tabular_id, t.transaction()).await?;
    t.commit().await?;

    let response = to_ref_response(moved.clone());
    event_ctx
        .resolve(ResolvedDataset {
            warehouse,
            dataset: Arc::new(dataset),
        })
        .emit_dataset_ref_moved_async(moved, request.fast_forward);
    Ok(response)
}

pub(super) async fn set_dataset_ref_protection<
    C: CatalogStore,
    A: Authorizer + Clone,
    S: SecretStore,
>(
    parameters: DatasetRefParameters,
    request: SetDatasetRefProtectionRequest,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<DatasetRefResponse> {
    let DatasetRefParameters {
        prefix,
        namespace,
        dataset_name,
        ref_name,
    } = parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;

    let action = CatalogDatasetAction::ManageRefs {
        target_refs: target_ref(&ref_name),
    };
    let dataset_ident = TableIdent::new(namespace.clone(), dataset_name.clone());
    let event_ctx = APIEventContext::for_dataset(
        Arc::new(request_metadata.clone()),
        state.v1_state.events.clone(),
        warehouse_id,
        dataset_ident,
        action.clone(),
    );

    let (_event_ctx, (_warehouse, _ns, info)) = event_ctx.emit_authz(
        state
            .v1_state
            .authz
            .load_and_authorize_dataset_operation::<C>(
                &request_metadata,
                &UserProvidedDataset::new(
                    warehouse_id,
                    TableIdent::new(namespace, dataset_name.clone()),
                ),
                TabularListFlags::active(),
                action,
                state.v1_state.catalog.clone(),
            )
            .await,
    )?;

    let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
    let updated = C::set_dataset_ref_protection(
        warehouse_id,
        info.tabular_id,
        &ref_name,
        request.protected,
        t.transaction(),
    )
    .await?;
    t.commit().await?;

    Ok(to_ref_response(updated))
}

pub(super) async fn delete_dataset_ref<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: DatasetRefParameters,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<()> {
    let DatasetRefParameters {
        prefix,
        namespace,
        dataset_name,
        ref_name,
    } = parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;

    let idempotency_key = request_metadata.idempotency_key().copied();
    let action = CatalogDatasetAction::ManageRefs {
        target_refs: target_ref(&ref_name),
    };
    let dataset_ident = TableIdent::new(namespace.clone(), dataset_name.clone());
    let event_ctx = APIEventContext::for_dataset(
        Arc::new(request_metadata.clone()),
        state.v1_state.events.clone(),
        warehouse_id,
        dataset_ident.clone(),
        action.clone(),
    );

    if let Some(key) = idempotency_key
        && C::check_idempotency_key(
            warehouse_id,
            &key,
            EndpointFlat::DatasetV1DeleteDatasetRef,
            state.v1_state.catalog.clone(),
        )
        .await?
        .is_replay()
    {
        event_ctx.emit_idempotent_replay(key);
        return Ok(());
    }

    let (event_ctx, (warehouse, _ns, info)) = event_ctx.emit_authz(
        state
            .v1_state
            .authz
            .load_and_authorize_dataset_operation::<C>(
                &request_metadata,
                &UserProvidedDataset::new(
                    warehouse_id,
                    TableIdent::new(namespace, dataset_name.clone()),
                ),
                TabularListFlags::active(),
                action,
                state.v1_state.catalog.clone(),
            )
            .await,
    )?;

    // `main` is the branch every other ref is cut from and the default commit
    // target; deleting it would leave the dataset addressable only by snapshot id.
    if ref_name == DEFAULT_DATASET_BRANCH {
        return Err(ErrorModel::conflict(
            "The 'main' branch cannot be deleted.",
            "CannotDeleteMainBranch",
            None,
        )
        .into());
    }

    let t = C::Transaction::begin_write(state.v1_state.catalog).await?;
    let mut t = claim_idempotency_key::<C>(
        t,
        warehouse_id,
        idempotency_key,
        EndpointFlat::DatasetV1DeleteDatasetRef,
        StatusCode::NO_CONTENT,
    )
    .await?;
    C::delete_dataset_ref(warehouse_id, info.tabular_id, &ref_name, t.transaction()).await?;
    let dataset = C::load_dataset_by_id(warehouse_id, info.tabular_id, t.transaction()).await?;
    // What only this ref held becomes retention's to expire.
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
    t.commit().await?;

    event_ctx
        .resolve(ResolvedDataset {
            warehouse,
            dataset: Arc::new(dataset),
        })
        .emit_dataset_ref_deleted_async(ref_name);
    Ok(())
}

pub(super) async fn list_dataset_files<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: DatasetRefParameters,
    query: ListDatasetFilesQuery,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<ListDatasetFilesResponse> {
    let DatasetRefParameters {
        prefix,
        namespace,
        dataset_name,
        ref_name,
    } = parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;

    let action = CatalogDatasetAction::ReadData {
        target_refs: target_ref(&ref_name),
    };
    let dataset_ident = TableIdent::new(namespace.clone(), dataset_name.clone());
    let event_ctx = APIEventContext::for_dataset(
        Arc::new(request_metadata.clone()),
        state.v1_state.events.clone(),
        warehouse_id,
        dataset_ident,
        action.clone(),
    );

    let (_event_ctx, (warehouse, _ns, info)) = event_ctx.emit_authz(
        state
            .v1_state
            .authz
            .load_and_authorize_dataset_operation::<C>(
                &request_metadata,
                &UserProvidedDataset::new(
                    warehouse_id,
                    TableIdent::new(namespace, dataset_name.clone()),
                ),
                TabularListFlags::active(),
                action,
                state.v1_state.catalog.clone(),
            )
            .await,
    )?;

    let mut t = C::Transaction::begin_read(state.v1_state.catalog).await?;
    let ownership =
        C::load_dataset_ownership(warehouse_id, info.tabular_id, t.transaction()).await?;
    let (snapshot_id, entries, next_page_token) = C::list_dataset_files(
        warehouse_id,
        info.tabular_id,
        &ref_name,
        query.content_type.as_deref(),
        query.page_size,
        query.page_token.as_deref(),
        t.transaction(),
    )
    .await?;
    t.commit().await?;

    Ok(ListDatasetFilesResponse {
        snapshot_id,
        files: entries.into_iter().map(Into::into).collect(),
        next_page_token,
        access_mode: access_mode(ownership, &warehouse.storage_profile),
    })
}
