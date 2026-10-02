//! Reading a version's bytes: access grants, and signed URLs for its files.
//!
//! A grant is authorized once, when it is issued, against the ref it names; the
//! calls that sign files check the grant and never run policy again. So a long
//! read costs one authorization, and revoking the grant stops it at once.
use std::{
    collections::HashMap,
    sync::{Arc, LazyLock},
    time::Instant,
};

use axum_prometheus::metrics;
use http::StatusCode;
use iceberg::TableIdent;

use super::{
    authorize_replay, idempotency_key_reused, physical_location, resolves_elsewhere, target_ref,
};
use crate::{
    CONFIG,
    api::{
        ApiContext, ErrorModel,
        data::v1::datasets::{
            CreateDatasetAccessGrantRequest, DatasetAccessGrantParameters,
            DatasetAccessGrantResponse, DatasetAccessMode, DatasetRefParameters,
            DatasetSnapshotParameters, SignDatasetFilesRequest, SignDatasetFilesResponse,
            SignedDatasetFile,
        },
        endpoints::EndpointFlat,
        iceberg::v1::Result,
    },
    request_metadata::RequestMetadata,
    server::{maybe_get_secret, require_warehouse_id, tabular::claim_idempotency_key},
    service::{
        CatalogDatasetOps, CatalogIdempotencyOps, CatalogStore, CatalogTabularOps,
        CatalogWarehouseOps, DatasetAccessGrant, DatasetAccessGrantCreation, DatasetAccessGrantId,
        DatasetId, DatasetOwnership, DatasetSnapshotId, ManifestEntry, SecretStore, State,
        TabularListFlags, Transaction, WarehouseId,
        authn::Actor,
        authz::{
            AuthZDatasetOps, AuthZError, Authorizer, CatalogDatasetAction,
            RequireDatasetActionError,
        },
        events::{
            APIEventContext,
            backends::audit::{AuditOutcome, DatasetFilesSignedContext, dataset_files_signed},
            context::UserProvidedDataset,
        },
        idempotency::IdempotencyKey,
        storage::{ReadTarget, StorageProfile},
    },
};

const METRIC_SIGN_REQUESTS_TOTAL: &str = "lakekeeper_dataset_sign_requests_total";
const METRIC_SIGNED_FILES_TOTAL: &str = "lakekeeper_dataset_signed_files_total";
const METRIC_SIGN_BATCH_SIZE: &str = "lakekeeper_dataset_sign_batch_size";
const METRIC_GRANT_VALIDATION_SECONDS: &str = "lakekeeper_dataset_grant_validation_seconds";

/// How many out-of-scope keys a refusal names; the rest are counted.
const REFUSED_KEYS_SHOWN: usize = 5;

static METRICS_INITIALIZED: LazyLock<()> = LazyLock::new(|| {
    metrics::describe_counter!(
        METRIC_SIGN_REQUESTS_TOTAL,
        "Calls signing dataset files, by outcome"
    );
    metrics::describe_counter!(
        METRIC_SIGNED_FILES_TOTAL,
        "Dataset files signed for reading"
    );
    metrics::describe_histogram!(
        METRIC_SIGN_BATCH_SIZE,
        "Keys asked for per call signing dataset files"
    );
    metrics::describe_histogram!(
        METRIC_GRANT_VALIDATION_SECONDS,
        "Time to check an access grant and the keys it is asked to sign"
    );
});

/// How a dataset's bytes are read. Prefix credentials would expose everything
/// under the prefix, so they are offered only for a managed dataset, whose prefix
/// holds nothing but its own files, on a profile that vends them.
pub(super) fn access_mode(
    ownership: DatasetOwnership,
    profile: &StorageProfile,
) -> DatasetAccessMode {
    if ownership == DatasetOwnership::Managed && profile.credential_vending_enabled() {
        DatasetAccessMode::StorageCredentials
    } else {
        DatasetAccessMode::Presigned
    }
}

/// Whom a grant belongs to: the principal, and the role it acts as by id, so a
/// role rename keeps the grant.
fn actor_key(request_metadata: &RequestMetadata) -> String {
    match request_metadata.actor() {
        Actor::Anonymous => "anonymous".to_string(),
        Actor::Principal(user) => user.to_string(),
        Actor::Role {
            principal,
            assumed_role,
        } => format!("{principal}/role:{}", assumed_role.id),
    }
}

pub(super) async fn create_dataset_access_grant<
    C: CatalogStore,
    A: Authorizer + Clone,
    S: SecretStore,
>(
    parameters: DatasetRefParameters,
    request: CreateDatasetAccessGrantRequest,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<DatasetAccessGrantResponse> {
    let DatasetRefParameters {
        prefix,
        namespace,
        dataset_name,
        ref_name,
    } = parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;

    let idempotency_key = request_metadata.idempotency_key().copied();
    let action = CatalogDatasetAction::ReadData {
        target_refs: target_ref(&ref_name),
    };
    let dataset_ident = TableIdent::new(namespace, dataset_name);
    let event_ctx = APIEventContext::for_dataset(
        Arc::new(request_metadata.clone()),
        state.v1_state.events.clone(),
        warehouse_id,
        dataset_ident.clone(),
        action.clone(),
    );

    // A retry is answered with the grant the request issued: a second one would be
    // live, and its caller would never learn its id to revoke it.
    if let Some(key) = idempotency_key
        && C::check_idempotency_key(
            warehouse_id,
            &key,
            EndpointFlat::DatasetV1CreateDatasetAccessGrant,
            state.v1_state.catalog.clone(),
        )
        .await?
        .is_replay()
    {
        return replay_grant::<C, A, S>(warehouse_id, dataset_ident, key, state, &request_metadata)
            .await;
    }

    let (_event_ctx, (warehouse, _namespace, info)) = event_ctx.emit_authz(
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

    let t = C::Transaction::begin_write(state.v1_state.catalog).await?;
    let mut t = claim_idempotency_key::<C>(
        t,
        warehouse_id,
        idempotency_key,
        EndpointFlat::DatasetV1CreateDatasetAccessGrant,
        StatusCode::OK,
    )
    .await?;
    // The key's record lapses before the grant that carries the key: a retry after
    // that is answered from the grant.
    if let Some(key) = idempotency_key
        && let Some(grant) =
            C::get_dataset_access_grant_by_idempotency_key(warehouse_id, key, t.transaction())
                .await?
    {
        let grant = recorded_grant(grant, info.tabular_id, &request_metadata)?;
        let ownership =
            C::load_dataset_ownership(warehouse_id, info.tabular_id, t.transaction()).await?;
        t.commit().await?;
        return Ok(grant_response(grant, ownership, &warehouse.storage_profile));
    }
    let Some(snapshot_id) =
        C::get_dataset_ref(warehouse_id, info.tabular_id, &ref_name, t.transaction())
            .await?
            .snapshot_id
    else {
        return Err(ErrorModel::conflict(
            format!("Ref '{ref_name}' has no commits, so there are no files to read"),
            "DatasetRefHasNoSnapshot",
            None,
        )
        .into());
    };
    let ownership =
        C::load_dataset_ownership(warehouse_id, info.tabular_id, t.transaction()).await?;
    let grant = C::create_dataset_access_grant(
        DatasetAccessGrantCreation {
            grant_id: DatasetAccessGrantId::new_random(),
            warehouse_id,
            dataset_id: info.tabular_id,
            snapshot_id,
            ref_name,
            actor: actor_key(&request_metadata),
            content_type: request.content_type,
            expires_at: chrono::Utc::now() + CONFIG.dataset_access_grant_validity_seconds,
            idempotency_key,
        },
        t.transaction(),
    )
    .await?;
    t.commit().await?;

    Ok(grant_response(grant, ownership, &warehouse.storage_profile))
}

/// The grant a replayed request issued, if the caller is the one it was issued to.
async fn replay_grant<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    warehouse_id: WarehouseId,
    dataset: TableIdent,
    key: IdempotencyKey,
    state: ApiContext<State<A, C, S>>,
    request_metadata: &RequestMetadata,
) -> Result<DatasetAccessGrantResponse> {
    let (warehouse, info) = authorize_replay::<C, A, S>(
        warehouse_id,
        dataset,
        CatalogDatasetAction::GetMetadata,
        &state,
        request_metadata,
    )
    .await?;
    let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
    let grant = C::get_dataset_access_grant_by_idempotency_key(warehouse_id, key, t.transaction())
        .await?
        // Swept once it expired.
        .ok_or_else(|| {
            ErrorModel::not_found(
                "The access grant this Idempotency-Key created has expired",
                "DatasetAccessGrantNotFound",
                None,
            )
        })?;
    let grant = recorded_grant(grant, info.tabular_id, request_metadata)?;
    let ownership =
        C::load_dataset_ownership(warehouse_id, info.tabular_id, t.transaction()).await?;
    t.commit().await?;
    Ok(grant_response(grant, ownership, &warehouse.storage_profile))
}

/// `grant`, if the request it answers is the one that obtained it: on this dataset,
/// by this caller. Another caller's grant is not theirs to be handed.
fn recorded_grant(
    grant: DatasetAccessGrant,
    dataset_id: DatasetId,
    request_metadata: &RequestMetadata,
) -> Result<DatasetAccessGrant> {
    if grant.dataset_id == dataset_id && grant.actor == actor_key(request_metadata) {
        Ok(grant)
    } else {
        Err(idempotency_key_reused().into())
    }
}

fn grant_response(
    grant: DatasetAccessGrant,
    ownership: DatasetOwnership,
    storage_profile: &StorageProfile,
) -> DatasetAccessGrantResponse {
    DatasetAccessGrantResponse {
        grant_id: grant.grant_id,
        snapshot_id: grant.snapshot_id,
        expires_at: grant.expires_at,
        content_type: grant.content_type,
        access_mode: access_mode(ownership, storage_profile),
        max_keys_per_request: CONFIG.dataset_sign_max_keys,
        url_validity_seconds: CONFIG.dataset_signed_url_validity_seconds.num_seconds(),
    }
}

pub(super) async fn revoke_dataset_access_grant<
    C: CatalogStore,
    A: Authorizer + Clone,
    S: SecretStore,
>(
    parameters: DatasetAccessGrantParameters,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<()> {
    let DatasetAccessGrantParameters {
        prefix,
        namespace,
        dataset_name,
        grant_id,
    } = parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    let authorizer = state.v1_state.authz.clone();

    let dataset_ident = TableIdent::new(namespace, dataset_name);
    let mut event_ctx = APIEventContext::for_dataset(
        Arc::new(request_metadata.clone()),
        state.v1_state.events.clone(),
        warehouse_id,
        dataset_ident.clone(),
        CatalogDatasetAction::GetMetadata,
    );

    // Seeing the dataset is enough to revoke one's own grant; another caller's
    // takes `RevokeAccessGrants`. A grant of another dataset reads as absent. The
    // record names the action the revoke took.
    let mut anothers = false;
    let authz_result = async {
        let (warehouse, namespace, info) = authorizer
            .load_and_authorize_dataset_operation::<C>(
                event_ctx.request_metadata(),
                &UserProvidedDataset::new(warehouse_id, dataset_ident.clone()),
                TabularListFlags::active(),
                CatalogDatasetAction::GetMetadata,
                state.v1_state.catalog.clone(),
            )
            .await?;
        // On the primary: a grant issued a moment ago may not have reached a replica.
        let mut t = C::Transaction::begin_write(state.v1_state.catalog.clone())
            .await
            .map_err(super::iceberg_err_to_authz)?;
        let grant = C::get_dataset_access_grant(warehouse_id, grant_id, t.transaction())
            .await
            .map_err(super::iceberg_err_to_authz)?
            .filter(|grant| grant.dataset_id == info.tabular_id);
        t.commit().await.map_err(super::iceberg_err_to_authz)?;
        if let Some(grant) = &grant
            && grant.actor != actor_key(event_ctx.request_metadata())
        {
            anothers = true;
            authorizer
                .require_dataset_action(
                    event_ctx.request_metadata(),
                    &warehouse,
                    &namespace,
                    dataset_ident.clone(),
                    Ok::<_, RequireDatasetActionError>(Some(info.clone())),
                    CatalogDatasetAction::RevokeAccessGrants,
                )
                .await?;
        }
        Ok::<_, AuthZError>((info, grant))
    }
    .await;
    if anothers {
        event_ctx.override_action(CatalogDatasetAction::RevokeAccessGrants);
    }
    let (_event_ctx, (info, grant)) = event_ctx.emit_authz(authz_result)?;
    if grant.is_none() {
        return Err(grant_not_found().into());
    }

    let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
    C::revoke_dataset_access_grant(warehouse_id, info.tabular_id, grant_id, t.transaction())
        .await?
        .ok_or_else(grant_not_found)?;
    t.commit().await?;
    Ok(())
}

fn grant_not_found() -> ErrorModel {
    ErrorModel::not_found("Access grant not found", "DatasetAccessGrantNotFound", None)
}

pub(super) async fn sign_dataset_files<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: DatasetSnapshotParameters,
    request: SignDatasetFilesRequest,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<SignDatasetFilesResponse> {
    let () = &*METRICS_INITIALIZED;
    let DatasetSnapshotParameters {
        prefix,
        namespace,
        dataset_name,
        snapshot_id,
    } = parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    let SignDatasetFilesRequest { grant_id, keys } = request;
    if keys.is_empty() || keys.len() > CONFIG.dataset_sign_max_keys {
        return Err(ErrorModel::bad_request(
            format!(
                "Sign between 1 and {} keys per request, got {}",
                CONFIG.dataset_sign_max_keys,
                keys.len()
            ),
            "InvalidSignBatch",
            None,
        )
        .into());
    }
    metrics::histogram!(METRIC_SIGN_BATCH_SIZE)
        .record(f64::from(u32::try_from(keys.len()).unwrap_or(u32::MAX)));

    let mut audit = DatasetFilesSignedContext {
        warehouse_id,
        dataset_id: None,
        snapshot_id,
        access_grant_id: grant_id,
        key_count: keys.len(),
    };
    let signed = sign_with_grant::<C, S>(
        &state,
        &request_metadata,
        TableIdent::new(namespace, dataset_name),
        &keys,
        &mut audit,
    )
    .await;
    let outcome = match &signed {
        Ok(_) => AuditOutcome::Success,
        Err(e) if e.error.code >= 500 => AuditOutcome::Failed,
        Err(_) => AuditOutcome::Forbidden,
    };
    dataset_files_signed(&request_metadata, outcome, &audit);
    metrics::counter!(METRIC_SIGN_REQUESTS_TOTAL, "outcome" => outcome.as_str()).increment(1);
    if signed.is_ok() {
        metrics::counter!(METRIC_SIGNED_FILES_TOTAL).increment(audit.key_count as u64);
    }
    signed.map(|files| SignDatasetFilesResponse { files })
}

/// Check the grant and the keys, then sign. No policy runs: the grant is the
/// authorization, checked here — live, held by the caller, covering these keys.
async fn sign_with_grant<C: CatalogStore, S: SecretStore>(
    state: &ApiContext<State<impl Authorizer, C, S>>,
    request_metadata: &RequestMetadata,
    dataset_ident: TableIdent,
    keys: &[String],
    audit: &mut DatasetFilesSignedContext,
) -> Result<Vec<SignedDatasetFile>> {
    let started = Instant::now();
    let catalog = state.v1_state.catalog.clone();
    let warehouse = C::get_active_warehouse_by_id(audit.warehouse_id, catalog.clone())
        .await?
        .ok_or_else(grant_not_found)?;
    let info = C::get_dataset_info(
        audit.warehouse_id,
        dataset_ident,
        TabularListFlags::active(),
        catalog.clone(),
    )
    .await?
    .ok_or_else(grant_not_found)?;
    audit.dataset_id = Some(info.tabular_id);

    // On the primary: a revocation must stop the very next call, and a replica
    // may not have seen it yet.
    let mut t = C::Transaction::begin_write(catalog).await?;
    let grant =
        C::get_dataset_access_grant(audit.warehouse_id, audit.access_grant_id, t.transaction())
            .await?;
    let grant = check_grant(
        grant,
        info.tabular_id,
        audit.snapshot_id,
        &actor_key(request_metadata),
    )?;
    let files = C::get_snapshot_files(
        audit.warehouse_id,
        info.tabular_id,
        grant.snapshot_id,
        keys,
        t.transaction(),
    )
    .await?;
    t.commit().await?;
    let files = files_in_scope(&grant, keys, files)?;
    metrics::histogram!(METRIC_GRANT_VALIDATION_SECONDS).record(started.elapsed().as_secs_f64());

    // A file that records an object version is read at it: what the snapshot
    // pinned, not whatever was written to the key since. Signed with the
    // warehouse's credential, so nothing outside the dataset's location is.
    let targets = files
        .iter()
        .map(|file| {
            let location = physical_location(&info.location, &file.physical_path)
                .ok()
                .filter(|location| {
                    location.is_prefix_within(&info.location)
                        && !resolves_elsewhere(&file.physical_path)
                })
                .ok_or_else(|| {
                    ErrorModel::internal(
                        format!(
                            "A manifest records a physical path outside the dataset: {}",
                            file.physical_path
                        ),
                        "InvalidPhysicalPath",
                        None,
                    )
                })?;
            Ok(ReadTarget {
                location,
                version: file.version_id.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let validity = CONFIG
        .dataset_signed_url_validity_seconds
        .to_std()
        .unwrap_or_default();
    let secret = maybe_get_secret(warehouse.storage_secret_id, &state.v1_state.secrets).await?;
    let urls = warehouse
        .storage_profile
        .presign_reads(secret.as_deref(), &info.location, &info, &targets, validity)
        .await?;
    let expires_at = chrono::Utc::now() + CONFIG.dataset_signed_url_validity_seconds;
    // What the reader compares a download's `ETag` with, where the two can agree.
    let etags = warehouse.storage_profile.has_one_etag_per_object();
    Ok(files
        .into_iter()
        .zip(urls)
        .map(|(file, url)| SignedDatasetFile {
            logical_key: file.logical_key,
            url,
            expires_at,
            etag: file.etag.filter(|_| etags),
        })
        .collect())
}

/// The grant, if it lets the caller sign `snapshot_id` of `dataset_id` now.
/// Someone else's grant reads as absent, so a leaked id confirms nothing.
fn check_grant(
    grant: Option<DatasetAccessGrant>,
    dataset_id: DatasetId,
    snapshot_id: DatasetSnapshotId,
    actor: &str,
) -> Result<DatasetAccessGrant> {
    let grant = grant
        .filter(|grant| grant.dataset_id == dataset_id && grant.actor == actor)
        .ok_or_else(grant_not_found)?;
    if grant.revoked_at.is_some() {
        return Err(ErrorModel::forbidden(
            "The access grant was revoked",
            "DatasetAccessGrantRevoked",
            None,
        )
        .into());
    }
    if grant.expires_at <= chrono::Utc::now() {
        return Err(ErrorModel::forbidden(
            "The access grant expired; obtain a new one",
            "DatasetAccessGrantExpired",
            None,
        )
        .into());
    }
    if grant.snapshot_id != snapshot_id {
        return Err(ErrorModel::forbidden(
            format!(
                "The access grant covers snapshot {}, not {snapshot_id}",
                grant.snapshot_id
            ),
            "DatasetAccessGrantSnapshotMismatch",
            None,
        )
        .into());
    }
    Ok(grant)
}

/// The snapshot's files for `keys`, in the order asked, or a refusal naming the
/// keys the grant does not cover: absent from the snapshot, or outside its
/// content-type filter.
fn files_in_scope(
    grant: &DatasetAccessGrant,
    keys: &[String],
    files: Vec<ManifestEntry>,
) -> Result<Vec<ManifestEntry>> {
    let by_key: HashMap<String, ManifestEntry> = files
        .into_iter()
        .filter(|file| {
            grant
                .content_type
                .as_deref()
                .is_none_or(|wanted| file.content_type.as_deref() == Some(wanted))
        })
        .map(|file| (file.logical_key.clone(), file))
        .collect();
    let mut outside = Vec::new();
    let mut in_scope = Vec::with_capacity(keys.len());
    for key in keys {
        match by_key.get(key) {
            Some(file) => in_scope.push(file.clone()),
            None => outside.push(key.as_str()),
        }
    }
    if outside.is_empty() {
        return Ok(in_scope);
    }
    let shown = outside
        .iter()
        .take(REFUSED_KEYS_SHOWN)
        .map(|k| format!("'{k}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let more = outside.len().saturating_sub(REFUSED_KEYS_SHOWN);
    Err(ErrorModel::forbidden(
        if more > 0 {
            format!("The access grant does not cover {shown} and {more} more")
        } else {
            format!("The access grant does not cover {shown}")
        },
        "DatasetFilesOutsideGrant",
        None,
    )
    .into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::storage::{S3Flavor, S3Profile};

    fn s3_profile(sts_enabled: bool) -> StorageProfile {
        S3Profile::builder()
            .bucket("test-bucket".to_string())
            .region("local".to_string())
            .sts_enabled(sts_enabled)
            .flavor(S3Flavor::S3Compat)
            .build()
            .into()
    }

    /// Prefix credentials go only to a managed dataset on a vending profile; every
    /// other combination reads through signed URLs.
    #[test]
    fn test_access_mode_offers_credentials_only_for_a_managed_vending_dataset() {
        for (ownership, sts_enabled, expected) in [
            (
                DatasetOwnership::Managed,
                true,
                DatasetAccessMode::StorageCredentials,
            ),
            (
                DatasetOwnership::Managed,
                false,
                DatasetAccessMode::Presigned,
            ),
            (
                DatasetOwnership::Imported,
                true,
                DatasetAccessMode::Presigned,
            ),
            (
                DatasetOwnership::Imported,
                false,
                DatasetAccessMode::Presigned,
            ),
        ] {
            assert_eq!(
                access_mode(ownership, &s3_profile(sts_enabled)),
                expected,
                "{ownership:?} with sts_enabled={sts_enabled}"
            );
        }
    }
}
