mod access;
mod create;
mod credentials;
mod diff;
mod drop;
mod import;
pub(crate) use import::{
    ABANDONED_STAGING_AFTER, IMPORT_CANCELLED, ImportHeartbeat, ImportParams, announce_import,
    run_import,
};
mod list;
mod load;
mod manifest;
mod materialization;
mod rename;
mod restore;
mod settings;
mod versioning;

use std::{collections::BTreeSet, str::FromStr as _, sync::Arc};

use async_trait::async_trait;
use iceberg::TableIdent;
use iceberg_ext::catalog::rest::ErrorModel;
use lakekeeper_io::{Location, LocationParseError, check_unsafe_chars};

use crate::{
    api::{
        ApiContext,
        data::v1::datasets::{
            CommitDatasetRequest, CreateDatasetAccessGrantRequest, CreateDatasetRefRequest,
            CreateDatasetRequest, DatasetAccessGrantParameters, DatasetAccessGrantResponse,
            DatasetParameters, DatasetRefParameters, DatasetRefResponse, DatasetService,
            DatasetSnapshotParameters, DiffDatasetQuery, DiffDatasetResponse,
            ExpireDatasetSnapshotResponse, ImportDatasetRequest, ImportDatasetResponse,
            ListDatasetFilesQuery, ListDatasetFilesResponse, ListDatasetRefsResponse,
            ListDatasetsQuery, ListDatasetsResponse, LoadDatasetCredentialsRequest,
            LoadDatasetCredentialsResponse, LoadDatasetResponse, MoveDatasetRefRequest,
            RenameDatasetRequest, SetDatasetRefProtectionRequest, SignDatasetFilesRequest,
            SignDatasetFilesResponse, SnapshotMaterializationQuery,
            SnapshotMaterializationResponse, SnapshotResponse, UpdateDatasetSettingsRequest,
        },
        iceberg::{
            types::{DropParams, Prefix},
            v1::{DataAccess, namespace::NamespaceParameters},
        },
    },
    request_metadata::RequestMetadata,
    server::CatalogServer,
    service::{
        ArcProjectId, CatalogBackendError, CatalogStore, DatasetConstraints, DatasetTabularInfo,
        IcebergErrorResponse, ResolvedWarehouse, Result, SecretStore, State, TabularListFlags,
        Transaction, WarehouseId,
        authz::{
            AuthZDatasetOps, AuthZError, Authorizer, CatalogDatasetAction,
            RequireDatasetActionError,
        },
        events::{APIEventContext, context::UserProvidedDataset},
        tasks::{
            ScheduleTaskMetadata, TaskEntity,
            dataset_checkpoint_queue::{DatasetCheckpointPayload, DatasetCheckpointTask},
            dataset_snapshot_expiry_queue::schedule_snapshot_expiry,
        },
    },
};

fn iceberg_err_to_authz(e: impl Into<IcebergErrorResponse>) -> AuthZError {
    let err_model = ErrorModel::from(e.into());
    AuthZError::RequireDatasetActionError(RequireDatasetActionError::CatalogBackendError(
        CatalogBackendError::new_unexpected(err_model),
    ))
}

/// The longest key a file may have, in characters: the limit object stores share.
const MAX_KEY_LEN: usize = 1024;

/// Reject a key that would not name exactly one object inside the dataset's own
/// location.
///
/// Keys are relative and must stay inside the prefix: overlap checks and
/// prefix-scoped credential vending are only sound if every manifest entry
/// belongs to exactly one dataset. A file is read where its key resolves under the
/// location, without empty segments, so `a//b` would read `a/b`; and a signer that
/// builds a URL from it resolves `%2e%2e` as `..` and `\` as `/`. `#` and `?`
/// would end the URL's path.
fn validate_key(key: &str, field: &str) -> Result<()> {
    let refused = |why: &str| -> Result<()> {
        Err(ErrorModel::bad_request(format!("{field} {why}"), "InvalidKey", None).into())
    };
    if key.is_empty() {
        return refused("must not be empty");
    }
    if key.chars().count() > MAX_KEY_LEN {
        return refused(&format!("must be at most {MAX_KEY_LEN} characters"));
    }
    if key.starts_with('/') || key.contains("://") {
        return refused(&format!("must be relative, got '{key}'"));
    }
    if key.split('/').any(str::is_empty) || resolves_elsewhere(key) {
        return refused(&format!(
            "must not hold an empty, `.` or `..` segment, or a `\\`, got '{key}'"
        ));
    }
    if let Some(c) = key.chars().find(|c| matches!(c, '#' | '?')) {
        return refused(&format!("must not hold '{c}', got '{key}'"));
    }
    if let Err(reason) = check_unsafe_chars(key) {
        return refused(&format!("must not hold a {reason}"));
    }
    Ok(())
}

/// Whether a path reads as somewhere else once a URL is built from it: a `.` or
/// `..` segment, percent-encoded or not, or a `\`, which a URL parser takes for a
/// separator.
fn resolves_elsewhere(path: &str) -> bool {
    path.contains('\\')
        || path.split('/').any(|segment| {
            let decoded = segment.to_ascii_lowercase().replace("%2e", ".");
            decoded == "." || decoded == ".."
        })
}

/// Refuse constraints no file could meet.
fn validate_constraints(constraints: &DatasetConstraints) -> Result<()> {
    if constraints.max_file_size.is_some_and(|max| max <= 0) {
        return Err(ErrorModel::bad_request(
            "max-file-size must be greater than zero.",
            "InvalidConstraints",
            None,
        )
        .into());
    }
    if constraints
        .allowed_content_types
        .as_ref()
        .is_some_and(Vec::is_empty)
    {
        return Err(ErrorModel::bad_request(
            "allowed-content-types must name at least one content type; omit it to allow any.",
            "InvalidConstraints",
            None,
        )
        .into());
    }
    Ok(())
}

/// A manifest's physical path as a location: a full URI as written, or a path
/// relative to the dataset's location.
fn physical_location(
    dataset_location: &Location,
    physical_path: &str,
) -> std::result::Result<Location, LocationParseError> {
    if physical_path.contains("://") {
        return Location::from_str(physical_path);
    }
    let mut location = dataset_location.clone();
    location.extend(physical_path.split('/').filter(|s| !s.is_empty()));
    Ok(location)
}

/// The dataset a replayed request names, authorized for `action`: whatever shows
/// what the replay answers with, which a caller who cannot see the dataset must
/// not learn.
async fn authorize_replay<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    warehouse_id: WarehouseId,
    dataset: TableIdent,
    action: CatalogDatasetAction,
    state: &ApiContext<State<A, C, S>>,
    request_metadata: &RequestMetadata,
) -> Result<(Arc<ResolvedWarehouse>, DatasetTabularInfo)> {
    let event_ctx = APIEventContext::for_dataset(
        Arc::new(request_metadata.clone()),
        state.v1_state.events.clone(),
        warehouse_id,
        dataset.clone(),
        action.clone(),
    );
    let (_event_ctx, (warehouse, _namespace, info)) = event_ctx.emit_authz(
        state
            .v1_state
            .authz
            .load_and_authorize_dataset_operation::<C>(
                request_metadata,
                &UserProvidedDataset::new(warehouse_id, dataset),
                TabularListFlags::active(),
                action,
                state.v1_state.catalog.clone(),
            )
            .await,
    )?;
    Ok((warehouse, info))
}

/// A key whose recorded row belongs to another request: another dataset's, or
/// another caller's.
fn idempotency_key_reused() -> ErrorModel {
    ErrorModel::bad_request(
        "Idempotency-Key was already used for a different request. Keys must be unique \
         per request and must not be reused.",
        "IdempotencyKeyReused",
        None,
    )
}

/// Queue what a published snapshot calls for: a fold once the chain has outgrown
/// the checkpoint interval, and a retention pass. Enqueued inside the publishing
/// transaction, so a rolled-back publish leaves no task behind, and the publisher
/// never waits for either.
///
/// Deduplication is the task table's: one active task per (entity, queue), so a
/// hundred publishes past the threshold produce one fold, not a hundred.
async fn schedule_after_publish<C: CatalogStore>(
    project_id: ArcProjectId,
    entity: TaskEntity,
    branch: &str,
    checkpoint_due: bool,
    transaction: &mut C::Transaction,
) -> Result<()> {
    if checkpoint_due {
        DatasetCheckpointTask::schedule_task::<C>(
            ScheduleTaskMetadata {
                project_id: project_id.clone(),
                parent_task_id: None,
                scheduled_for: None,
                entity: entity.clone(),
            },
            DatasetCheckpointPayload::new(branch.to_string()),
            transaction.transaction(),
        )
        .await?;
    }
    schedule_snapshot_expiry::<C>(project_id, entity, transaction).await
}

/// `target_refs` naming the one ref a versioning action acts on.
fn target_ref(name: &str) -> Arc<BTreeSet<String>> {
    Arc::new(BTreeSet::from([name.to_string()]))
}

#[async_trait]
impl<C: CatalogStore, A: Authorizer + Clone, S: SecretStore> DatasetService<State<A, C, S>>
    for CatalogServer<C, A, S>
{
    async fn create_dataset(
        parameters: NamespaceParameters,
        request: CreateDatasetRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadDatasetResponse> {
        create::create_dataset::<C, A, S>(parameters, request, state, request_metadata).await
    }

    async fn load_dataset(
        parameters: DatasetParameters,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadDatasetResponse> {
        load::load_dataset::<C, A, S>(parameters, state, request_metadata).await
    }

    async fn list_datasets(
        parameters: NamespaceParameters,
        query: ListDatasetsQuery,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ListDatasetsResponse> {
        list::list_datasets::<C, A, S>(parameters, query, state, request_metadata).await
    }

    async fn drop_dataset(
        parameters: DatasetParameters,
        drop_params: DropParams,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        drop::drop_dataset::<C, A, S>(parameters, drop_params, state, request_metadata).await
    }

    async fn commit_dataset(
        parameters: DatasetRefParameters,
        request: CommitDatasetRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<SnapshotResponse> {
        versioning::commit_dataset::<C, A, S>(parameters, request, state, request_metadata).await
    }

    async fn list_dataset_refs(
        parameters: DatasetParameters,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ListDatasetRefsResponse> {
        versioning::list_dataset_refs::<C, A, S>(parameters, state, request_metadata).await
    }

    async fn create_dataset_ref(
        parameters: DatasetParameters,
        request: CreateDatasetRefRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<DatasetRefResponse> {
        versioning::create_dataset_ref::<C, A, S>(parameters, request, state, request_metadata)
            .await
    }

    async fn move_dataset_ref(
        parameters: DatasetRefParameters,
        request: MoveDatasetRefRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<DatasetRefResponse> {
        versioning::move_dataset_ref::<C, A, S>(parameters, request, state, request_metadata).await
    }

    async fn delete_dataset_ref(
        parameters: DatasetRefParameters,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        versioning::delete_dataset_ref::<C, A, S>(parameters, state, request_metadata).await
    }

    async fn list_dataset_files(
        parameters: DatasetRefParameters,
        query: ListDatasetFilesQuery,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ListDatasetFilesResponse> {
        versioning::list_dataset_files::<C, A, S>(parameters, query, state, request_metadata).await
    }

    async fn diff_dataset(
        parameters: DatasetParameters,
        query: DiffDatasetQuery,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<DiffDatasetResponse> {
        diff::diff_dataset::<C, A, S>(parameters, query, state, request_metadata).await
    }

    async fn restore_dataset_snapshot(
        parameters: DatasetSnapshotParameters,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<SnapshotResponse> {
        restore::restore_dataset_snapshot::<C, A, S>(parameters, state, request_metadata).await
    }

    async fn expire_dataset_snapshot(
        parameters: DatasetSnapshotParameters,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ExpireDatasetSnapshotResponse> {
        restore::expire_dataset_snapshot::<C, A, S>(parameters, state, request_metadata).await
    }

    async fn get_dataset_snapshot_materialization(
        parameters: DatasetSnapshotParameters,
        query: SnapshotMaterializationQuery,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<SnapshotMaterializationResponse> {
        materialization::get_dataset_snapshot_materialization::<C, A, S>(
            parameters,
            query,
            state,
            request_metadata,
        )
        .await
    }

    async fn update_dataset_settings(
        parameters: DatasetParameters,
        request: UpdateDatasetSettingsRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadDatasetResponse> {
        settings::update_dataset_settings::<C, A, S>(parameters, request, state, request_metadata)
            .await
    }

    async fn set_dataset_ref_protection(
        parameters: DatasetRefParameters,
        request: SetDatasetRefProtectionRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<DatasetRefResponse> {
        versioning::set_dataset_ref_protection::<C, A, S>(
            parameters,
            request,
            state,
            request_metadata,
        )
        .await
    }

    async fn create_dataset_access_grant(
        parameters: DatasetRefParameters,
        request: CreateDatasetAccessGrantRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<DatasetAccessGrantResponse> {
        access::create_dataset_access_grant::<C, A, S>(parameters, request, state, request_metadata)
            .await
    }

    async fn revoke_dataset_access_grant(
        parameters: DatasetAccessGrantParameters,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        access::revoke_dataset_access_grant::<C, A, S>(parameters, state, request_metadata).await
    }

    async fn sign_dataset_files(
        parameters: DatasetSnapshotParameters,
        request: SignDatasetFilesRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<SignDatasetFilesResponse> {
        access::sign_dataset_files::<C, A, S>(parameters, request, state, request_metadata).await
    }

    async fn rename_dataset(
        prefix: Option<Prefix>,
        request: RenameDatasetRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        rename::rename_dataset(prefix, request, state, request_metadata).await
    }

    async fn load_dataset_credentials(
        parameters: DatasetParameters,
        request: LoadDatasetCredentialsRequest,
        data_access: DataAccess,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadDatasetCredentialsResponse> {
        credentials::load_dataset_credentials(
            parameters,
            request,
            data_access,
            state,
            request_metadata,
        )
        .await
    }

    async fn import_dataset(
        parameters: DatasetParameters,
        request: ImportDatasetRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ImportDatasetResponse> {
        import::import_dataset(parameters, request, state, request_metadata).await
    }
}
