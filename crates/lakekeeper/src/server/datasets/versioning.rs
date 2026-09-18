use std::sync::Arc;

use uuid::Uuid;

use crate::{
    api::{
        ApiContext, ErrorModel,
        data::v1::datasets::{
            CommitDatasetRequest, CreateDatasetRefRequest, DatasetFile, DatasetParameters,
            DatasetRefParameters, DatasetRefResponse, DatasetRefSource, ListDatasetFilesQuery,
            ListDatasetFilesResponse, ListDatasetRefsResponse, MoveDatasetRefRequest,
            SetDatasetRefProtectionRequest, SnapshotResponse,
        },
    },
    request_metadata::RequestMetadata,
    server::require_warehouse_id,
    service::{
        CatalogDatasetOps, CatalogStore, DatasetCommit, DatasetRef, DatasetSnapshotId,
        DatasetTabularInfo, ManifestEntry, NamedEntity, Result, SecretStore, State,
        TabularListFlags, Transaction, WarehouseId,
        authz::{AuthZDatasetOps, Authorizer, CatalogDatasetAction},
        events::{APIEventContext, context::UserProvidedDataset},
        tasks::{
            ScheduleTaskMetadata, TaskEntity, WarehouseTaskEntityId,
            dataset_checkpoint_queue::{DatasetCheckpointPayload, DatasetCheckpointTask},
        },
    },
};

fn to_ref_response(r: DatasetRef) -> DatasetRefResponse {
    DatasetRefResponse {
        name: r.name,
        typ: r.typ,
        snapshot_id: r.snapshot_id,
        protected: r.protected,
    }
}

/// Reject anything that would place a file outside the dataset's own location.
///
/// Keys are relative and must stay inside the prefix: overlap checks and
/// prefix-scoped credential vending are only sound if every manifest entry
/// belongs to exactly one dataset.
fn validate_key(key: &str, field: &str) -> Result<()> {
    if key.is_empty() {
        return Err(ErrorModel::bad_request(
            format!("{field} must not be empty"),
            "InvalidKey",
            None,
        )
        .into());
    }
    if key.starts_with('/') {
        return Err(ErrorModel::bad_request(
            format!("{field} must be relative, got '{key}'"),
            "InvalidKey",
            None,
        )
        .into());
    }
    if key.split('/').any(|segment| segment == "..") {
        return Err(ErrorModel::bad_request(
            format!("{field} must not escape the dataset location, got '{key}'"),
            "InvalidKey",
            None,
        )
        .into());
    }
    Ok(())
}

/// Ref names appear as a path segment, so anything that would make a ref
/// unaddressable is refused at creation rather than discovered later.
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

    for file in &request.added {
        validate_key(&file.logical_key, "logical-key")?;
        if let Some(path) = &file.physical_path {
            validate_key(path, "physical-path")?;
        }
    }
    for key in &request.removed {
        validate_key(key, "removed key")?;
    }

    let dataset_ident = iceberg::TableIdent::new(namespace.clone(), dataset_name.clone());
    // Both are consumed below -- the ident by the event context, the branch by the
    // commit -- but a checkpoint task may still need them afterwards.
    let dataset_ident_for_task = dataset_ident.clone();
    let branch_for_task = branch.clone();
    let event_ctx = APIEventContext::for_dataset(
        Arc::new(request_metadata.clone()),
        state.v1_state.events.clone(),
        warehouse_id,
        dataset_ident,
        CatalogDatasetAction::Commit,
    );

    let (_event_ctx, (warehouse, _ns, info)) = event_ctx.emit_authz(
        authorizer
            .load_and_authorize_dataset_operation::<C>(
                &request_metadata,
                &UserProvidedDataset::new(
                    warehouse_id,
                    iceberg::TableIdent::new(namespace, dataset_name.clone()),
                ),
                TabularListFlags::active(),
                CatalogDatasetAction::Commit,
                state.v1_state.catalog.clone(),
            )
            .await,
    )?;

    let added: Vec<ManifestEntry> = request
        .added
        .into_iter()
        .map(|f| ManifestEntry {
            physical_path: f.physical_path.unwrap_or_else(|| f.logical_key.clone()),
            logical_key: f.logical_key,
            etag: f.etag,
            size: f.size,
            content_type: f.content_type,
            checksum: f.checksum,
            version_id: f.version_id,
            last_modified: f.last_modified,
        })
        .collect();

    let commit = DatasetCommit {
        warehouse_id,
        dataset_id: info.tabular_id,
        branch,
        expected_snapshot_id: request.parent_snapshot_id,
        snapshot_id: DatasetSnapshotId::from(Uuid::now_v7()),
        location: info.location.to_string(),
        added,
        // The catalog commit API takes adds and removes; a caller that rewrote a
        // file sends it as an add, and newest-wins resolves it the same way.
        modified: Vec::new(),
        removed: request.removed,
        summary: request.summary,
    };

    let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
    let snapshot = C::commit_dataset(commit, t.transaction()).await?;

    // The chain has outgrown the checkpoint interval, so hand the fold to the
    // queue rather than making this writer wait for it. Enqueued inside the
    // commit's transaction so a rolled-back commit cannot leave a task behind.
    //
    // Deduplication is the task table's: one active task per (entity, queue), so
    // a hundred commits past the threshold produce one fold, not a hundred.
    // `schedule_task` returning None is that conflict, and is the expected case.
    if snapshot.checkpoint_due {
        DatasetCheckpointTask::schedule_task::<C>(
            ScheduleTaskMetadata {
                project_id: warehouse.project_id.clone(),
                parent_task_id: None,
                scheduled_for: None,
                entity: TaskEntity::EntityInWarehouse {
                    entity_name: dataset_ident_for_task.into_name_parts(),
                    warehouse_id,
                    entity_id: WarehouseTaskEntityId::Dataset {
                        dataset_id: info.tabular_id,
                    },
                },
            },
            DatasetCheckpointPayload::new(branch_for_task),
            t.transaction(),
        )
        .await?;
    }

    t.commit().await?;

    Ok(SnapshotResponse {
        snapshot_id: snapshot.snapshot_id,
        parent_snapshot_id: snapshot.parent_snapshot_id,
        location: snapshot.location,
        summary: snapshot.summary,
        created_at: snapshot.created_at,
    })
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

    let dataset_ident = iceberg::TableIdent::new(namespace.clone(), dataset_name.clone());
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
                    iceberg::TableIdent::new(namespace, dataset_name.clone()),
                ),
                TabularListFlags::active(),
                CatalogDatasetAction::GetMetadata,
                state.v1_state.catalog.clone(),
            )
            .await,
    )?;

    let mut t = C::Transaction::begin_read(state.v1_state.catalog).await?;
    let refs = C::list_dataset_refs(warehouse_id, info.tabular_id, t.transaction()).await?;
    t.commit().await?;

    Ok(ListDatasetRefsResponse {
        refs: refs.into_iter().map(to_ref_response).collect(),
    })
}

/// Resolve a ref-or-snapshot source to the snapshot it names right now.
async fn resolve_source<C: CatalogStore>(
    warehouse_id: WarehouseId,
    info: &DatasetTabularInfo,
    source: DatasetRefSource,
    catalog_state: C::State,
) -> Result<DatasetSnapshotId> {
    match source {
        DatasetRefSource::Snapshot { snapshot_id } => Ok(snapshot_id),
        DatasetRefSource::Ref { name } => {
            let mut t = C::Transaction::begin_read(catalog_state).await?;
            let source_ref =
                C::get_dataset_ref(warehouse_id, info.tabular_id, &name, t.transaction()).await?;
            t.commit().await?;
            source_ref.snapshot_id.ok_or_else(|| {
                ErrorModel::bad_request(
                    format!("Ref '{name}' has no commits to branch or tag from"),
                    "DatasetRefHasNoSnapshot",
                    None,
                )
                .into()
            })
        }
    }
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

    let dataset_ident = iceberg::TableIdent::new(namespace.clone(), dataset_name.clone());
    let event_ctx = APIEventContext::for_dataset(
        Arc::new(request_metadata.clone()),
        state.v1_state.events.clone(),
        warehouse_id,
        dataset_ident,
        CatalogDatasetAction::ManageRefs,
    );

    let (_event_ctx, (_warehouse, _ns, info)) = event_ctx.emit_authz(
        state
            .v1_state
            .authz
            .load_and_authorize_dataset_operation::<C>(
                &request_metadata,
                &UserProvidedDataset::new(
                    warehouse_id,
                    iceberg::TableIdent::new(namespace, dataset_name.clone()),
                ),
                TabularListFlags::active(),
                CatalogDatasetAction::ManageRefs,
                state.v1_state.catalog.clone(),
            )
            .await,
    )?;

    let snapshot_id = resolve_source::<C>(
        warehouse_id,
        &info,
        request.source,
        state.v1_state.catalog.clone(),
    )
    .await?;

    let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
    let created = C::create_dataset_ref(
        warehouse_id,
        info.tabular_id,
        &request.name,
        request.typ,
        snapshot_id,
        t.transaction(),
    )
    .await?;
    t.commit().await?;

    Ok(to_ref_response(created))
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
    // takes `Promote`. A reset can abandon commits, so it takes `Reset`, which
    // derives from grant authority rather than plain write access -- otherwise
    // anyone who can commit could bypass the protected-branch guarantee.
    let action = if request.fast_forward {
        CatalogDatasetAction::Promote
    } else {
        CatalogDatasetAction::Reset
    };

    let dataset_ident = iceberg::TableIdent::new(namespace.clone(), dataset_name.clone());
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
                    iceberg::TableIdent::new(namespace, dataset_name.clone()),
                ),
                TabularListFlags::active(),
                action,
                state.v1_state.catalog.clone(),
            )
            .await,
    )?;

    let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
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
    t.commit().await?;

    Ok(to_ref_response(moved))
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

    let dataset_ident = iceberg::TableIdent::new(namespace.clone(), dataset_name.clone());
    let event_ctx = APIEventContext::for_dataset(
        Arc::new(request_metadata.clone()),
        state.v1_state.events.clone(),
        warehouse_id,
        dataset_ident,
        CatalogDatasetAction::SetProtection,
    );

    let (_event_ctx, (_warehouse, _ns, info)) = event_ctx.emit_authz(
        state
            .v1_state
            .authz
            .load_and_authorize_dataset_operation::<C>(
                &request_metadata,
                &UserProvidedDataset::new(
                    warehouse_id,
                    iceberg::TableIdent::new(namespace, dataset_name.clone()),
                ),
                TabularListFlags::active(),
                CatalogDatasetAction::SetProtection,
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

    let dataset_ident = iceberg::TableIdent::new(namespace.clone(), dataset_name.clone());
    let event_ctx = APIEventContext::for_dataset(
        Arc::new(request_metadata.clone()),
        state.v1_state.events.clone(),
        warehouse_id,
        dataset_ident,
        CatalogDatasetAction::ManageRefs,
    );

    let (_event_ctx, (_warehouse, _ns, info)) = event_ctx.emit_authz(
        state
            .v1_state
            .authz
            .load_and_authorize_dataset_operation::<C>(
                &request_metadata,
                &UserProvidedDataset::new(
                    warehouse_id,
                    iceberg::TableIdent::new(namespace, dataset_name.clone()),
                ),
                TabularListFlags::active(),
                CatalogDatasetAction::ManageRefs,
                state.v1_state.catalog.clone(),
            )
            .await,
    )?;

    // `main` is the branch every other ref is cut from and the default commit
    // target; deleting it would leave the dataset addressable only by snapshot id.
    if ref_name == "main" {
        return Err(ErrorModel::bad_request(
            "The 'main' branch cannot be deleted.",
            "CannotDeleteMainBranch",
            None,
        )
        .into());
    }

    let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
    C::delete_dataset_ref(warehouse_id, info.tabular_id, &ref_name, t.transaction()).await?;
    t.commit().await?;

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

    let dataset_ident = iceberg::TableIdent::new(namespace.clone(), dataset_name.clone());
    let event_ctx = APIEventContext::for_dataset(
        Arc::new(request_metadata.clone()),
        state.v1_state.events.clone(),
        warehouse_id,
        dataset_ident,
        CatalogDatasetAction::ReadData,
    );

    let (_event_ctx, (_warehouse, _ns, info)) = event_ctx.emit_authz(
        state
            .v1_state
            .authz
            .load_and_authorize_dataset_operation::<C>(
                &request_metadata,
                &UserProvidedDataset::new(
                    warehouse_id,
                    iceberg::TableIdent::new(namespace, dataset_name.clone()),
                ),
                TabularListFlags::active(),
                CatalogDatasetAction::ReadData,
                state.v1_state.catalog.clone(),
            )
            .await,
    )?;

    let mut t = C::Transaction::begin_read(state.v1_state.catalog).await?;
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
        files: entries
            .into_iter()
            .map(|e| DatasetFile {
                logical_key: e.logical_key,
                physical_path: e.physical_path,
                etag: e.etag,
                size: e.size,
                content_type: e.content_type,
                checksum: e.checksum,
                version_id: e.version_id,
                last_modified: e.last_modified,
            })
            .collect(),
        next_page_token,
    })
}
