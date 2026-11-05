use std::{
    collections::{BTreeMap, HashSet},
    str::FromStr as _,
    sync::Arc,
};

use http::StatusCode;
use iceberg::spec::{ViewFormatVersion, ViewMetadata, ViewMetadataBuilder};
use iceberg_ext::catalog::{ViewRequirement, rest::ViewUpdate};
use lakekeeper_io::{LakekeeperStorage, Location};
use uuid::Uuid;

use crate::{
    SecretId,
    api::{
        endpoints::EndpointFlat,
        iceberg::v1::{
            ApiContext, CommitViewRequest, DataAccessMode, ErrorModel, LoadViewResult, Result,
            ViewParameters, views::LoadViewRequest,
        },
    },
    request_metadata::RequestMetadata,
    server::{
        compression_codec::CompressionCodec,
        io::{cleanup_before_response, delete_on_failure, remove_all, write_file},
        maybe_get_secret, require_warehouse_id,
        tables::{
            MAX_RETRIES_ON_CONCURRENT_UPDATE, concurrent_update_backoff, determine_table_ident,
            extract_count_from_metadata_location, validate_table_or_view_ident,
        },
        views::{
            owner_property::{self, PropertyWrites},
            validate_view_updates,
        },
    },
    service::{
        AuthZViewInfo, CONCURRENT_UPDATE_ERROR_TYPE, CatalogIdempotencyOps, CatalogStore,
        CatalogView, CatalogViewOps, InternalParseLocationError, State, TabularListFlags,
        Transaction, ViewCommit, ViewId,
        authz::{AuthZViewOps, Authorizer, CatalogViewAction},
        contract_verification::ContractVerification,
        events::{
            APIEventContext, ViewEventTransition,
            context::{AuthzChecked, Resolved, ResolvedView, UserProvidedView},
        },
        idempotency::{IdempotencyInfo, IdempotencyKey},
        secrets::SecretStore,
        storage::StorageProfile,
    },
};

/// Commit updates to a view
pub async fn commit_view<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: ViewParameters,
    request: CommitViewRequest,
    state: ApiContext<State<A, C, S>>,
    data_access: impl Into<DataAccessMode>,
    request_metadata: RequestMetadata,
) -> Result<LoadViewResult> {
    let data_access = data_access.into();
    // ------------------- VALIDATIONS -------------------
    let warehouse_id = require_warehouse_id(parameters.prefix.as_ref())?;

    let view_ident = determine_table_ident(&parameters.view, request.identifier.as_ref())?;
    validate_table_or_view_ident(&view_ident)?;
    validate_view_updates(&request.updates)?;

    // ------------------- IDEMPOTENCY CHECK -------------------
    let idempotency_key = request_metadata.idempotency_key().copied();
    if let Some(ref key) = idempotency_key {
        let check = C::check_idempotency_key(
            warehouse_id,
            key,
            EndpointFlat::CatalogV1ReplaceView,
            state.v1_state.catalog.clone(),
        )
        .await?;
        if check.is_replay() {
            return super::load::load_view::<C, A, S>(
                parameters,
                LoadViewRequest {
                    data_access,
                    referenced_by: None,
                },
                state,
                request_metadata,
            )
            .await;
        }
    }

    // ------------------- AUTHZ + BUSINESS LOGIC -------------------
    let authorizer = state.v1_state.authz.clone();

    let (updated_properties, removed_properties) = parse_view_property_updates(&request.updates);
    let (updated_properties, removed_properties) =
        (Arc::new(updated_properties), Arc::new(removed_properties));
    let event_ctx = APIEventContext::for_view(
        Arc::new(request_metadata),
        state.v1_state.events.clone(),
        warehouse_id,
        parameters.view,
        CatalogViewAction::Commit {
            updated_properties: updated_properties.clone(),
            removed_properties: removed_properties.clone(),
        },
    );
    let property_writes = PropertyWrites {
        updated: &updated_properties,
        removed: &removed_properties,
    };
    let protected_properties = owner_property::protected_properties(state.v1_state.authentication);

    // Security: an engine owner property (e.g. `trino.run-as-owner`) names the user an engine
    // runs the view as. Only a trusted engine that owns the property may set or remove it.
    owner_property::check_property_writes(
        property_writes.keys(),
        protected_properties,
        event_ctx.request_metadata().engines(),
    )
    .map_err(|e| event_ctx.emit_early_authz_failure(e))?;

    let authz_result = authorizer
        .load_and_authorize_view_operation::<C>(
            event_ctx.request_metadata(),
            event_ctx.user_provided_entity(),
            TabularListFlags::active(),
            event_ctx.action().clone(),
            state.v1_state.catalog.clone(),
        )
        .await;

    let (event_ctx, (warehouse, _namespace, view_info)) = event_ctx.emit_authz(authz_result)?;

    let view_id = view_info.view_id();
    let event_ctx = event_ctx.resolve(ResolvedView {
        warehouse: warehouse.clone(),
        view: Arc::new(view_info),
    });

    // ------------------- BUSINESS LOGIC -------------------
    // Verify assertions
    check_requirements(request.requirements.as_ref(), view_id)?;

    let request = Arc::new(request);
    let ctx = CommitViewContext {
        event_ctx: &event_ctx,
        storage_profile: &warehouse.storage_profile,
        storage_secret_id: warehouse.storage_secret_id,
        request: &request,
        property_writes,
        protected_properties,
    };
    let (result, commit, committed_request) =
        commit_with_retries::<C, A, S>(&ctx, &state, idempotency_key.as_ref()).await?;
    event_ctx.emit_view_committed_async(Arc::new(commit), data_access, committed_request);
    Ok(result)
}

/// Runs [`try_commit_view`], and runs it again after a concurrent update.
async fn commit_with_retries<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    ctx: &CommitViewContext<'_>,
    state: &ApiContext<State<A, C, S>>,
    idempotency_key: Option<&IdempotencyKey>,
) -> Result<(LoadViewResult, ViewEventTransition, Arc<CommitViewRequest>)> {
    let mut attempt = 0;
    loop {
        match try_commit_view::<C, A, S>(ctx, state, idempotency_key).await {
            Err(e)
                if e.error.r#type == CONCURRENT_UPDATE_ERROR_TYPE
                    && attempt < MAX_RETRIES_ON_CONCURRENT_UPDATE =>
            {
                attempt += 1;
                tracing::info!(
                    "Concurrent update detected (attempt {attempt}/{MAX_RETRIES_ON_CONCURRENT_UPDATE}), retrying view commit operation",
                );
                concurrent_update_backoff(attempt).await;
            }
            result => return result,
        }
    }
}

type CommitViewEventContext =
    APIEventContext<UserProvidedView, Resolved<ResolvedView>, CatalogViewAction, AuthzChecked>;

// Context structure to hold static parameters for retry function
struct CommitViewContext<'a> {
    event_ctx: &'a CommitViewEventContext,
    storage_profile: &'a StorageProfile,
    storage_secret_id: Option<SecretId>,
    request: &'a Arc<CommitViewRequest>,
    /// The properties the authorized commit sets and removes.
    property_writes: PropertyWrites<'a>,
    /// `None` if the owner property rules are off.
    protected_properties: Option<&'static HashSet<String>>,
}

// Core commit logic that may be retried. Also returns the request the commit applied, which
// can carry a `remove-properties` update that Lakekeeper adds.
#[allow(clippy::too_many_lines)]
async fn try_commit_view<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    ctx: &CommitViewContext<'_>,
    state: &ApiContext<State<A, C, S>>,
    idempotency_key: Option<&IdempotencyKey>,
) -> Result<(LoadViewResult, ViewEventTransition, Arc<CommitViewRequest>)> {
    let request_metadata = ctx.event_ctx.request_metadata();
    let view_info = &*ctx.event_ctx.resolved().view;
    let warehouse_id = view_info.warehouse_id;
    let mut t = C::Transaction::begin_write(state.v1_state.catalog.clone()).await?;

    // These operations need fresh data on each retry
    let previous_view = C::load_view(
        view_info.warehouse_id,
        view_info.tabular_id,
        false,
        t.transaction(),
    )
    .await?;

    let previous_view_location = Location::from_str(previous_view.metadata.location())
        .map_err(InternalParseLocationError::from)?;
    let previous_metadata_location = previous_view.metadata_location.clone();

    let CommittedMetadata {
        request,
        metadata: new_metadata,
        delete_old_location,
    } = build_committed_metadata(ctx, &previous_view.metadata, &previous_view_location)?;
    let new_metadata = Arc::new(new_metadata);

    state
        .v1_state
        .contract_verifiers
        .check_view_updates(&request.updates, &previous_view.metadata)
        .await?
        .into_result()?;

    let new_location =
        Location::from_str(new_metadata.location()).map_err(InternalParseLocationError::from)?;
    let metadata_location = ctx.storage_profile.default_metadata_location(
        &new_location,
        &CompressionCodec::try_from_properties(new_metadata.properties())?,
        Uuid::now_v7(),
        extract_count_from_metadata_location(&previous_metadata_location).map_or(0, |v| v + 1),
    );

    if delete_old_location.is_some() {
        ctx.storage_profile
            .require_allowed_location(&new_location)?;
    }

    let new_view = CatalogView {
        metadata: new_metadata.clone(),
        metadata_location,
        location: new_location,
        warehouse_version: previous_view.warehouse_version,
    };

    C::commit_view(
        ViewCommit {
            previous_view: &previous_view,
            namespace_id: view_info.namespace_id,
            warehouse_id: view_info.warehouse_id,
            new_view: &new_view,
        },
        t.transaction(),
    )
    .await?;

    let storage_secret = maybe_get_secret(ctx.storage_secret_id, &state.v1_state.secrets).await?;
    let storage_secret_ref = storage_secret.as_deref();

    // Write the metadata file and commit
    let file_io = ctx.storage_profile.file_io(storage_secret_ref).await?;
    delete_on_failure(&file_io, &[&new_view.metadata_location], async {
        write_file(
            &file_io,
            &new_view.metadata_location,
            &new_metadata,
            CompressionCodec::try_from_metadata(&new_metadata)?,
        )
        .await?;

        tracing::debug!(
            "Wrote new view metadata file to: '{}'",
            new_view.metadata_location
        );

        // Insert idempotency key in the same transaction.
        if let Some(key) = idempotency_key
            && !C::try_insert_idempotency_key(
                warehouse_id,
                &IdempotencyInfo::builder()
                    .key(*key)
                    .endpoint(EndpointFlat::CatalogV1ReplaceView)
                    .http_status(StatusCode::OK)
                    .build(),
                t.transaction(),
            )
            .await?
        {
            t.rollback()
                .await
                .inspect_err(|e| {
                    tracing::warn!("Rollback failed after idempotency conflict: {e}");
                })
                .ok();
            return Err(ErrorModel::request_in_progress().into());
        }

        t.commit().await
    })
    .await?;

    // The removal ends before the response: the client's next commit may move the view back
    // into this location.
    if let Some(DeleteLocation(before_update_view_location)) = delete_old_location
        && !cleanup_before_response(
            request_metadata.received_at(),
            remove_old_view_location(&file_io, before_update_view_location),
        )
        .await
    {
        tracing::warn!(
            "Stopped deleting old view location '{before_update_view_location}' at the request deadline. Its remaining files stay in storage."
        );
    }

    Ok((
        LoadViewResult {
            metadata_location: new_view.metadata_location.to_string(),
            metadata: new_metadata.clone(),
            // Views carry no storage access: engines load the tables behind a view with loadTable.
            config: None,
        },
        ViewEventTransition {
            old_metadata: previous_view.metadata,
            new_metadata,
            old_metadata_location: previous_metadata_location,
            new_metadata_location: new_view.metadata_location,
        },
        request,
    ))
}

/// The metadata a commit produces from the stored view, and the request it is built from.
struct CommittedMetadata<'l> {
    /// The client's request, with a `remove-properties` update appended if the owner property
    /// rules remove owner properties.
    request: Arc<CommitViewRequest>,
    metadata: ViewMetadata,
    delete_old_location: Option<DeleteLocation<'l>>,
}

/// Builds the metadata a commit produces from the stored view and applies the owner property
/// rules of [`owner_property::check_version_change`]. A refusal follows the request's
/// authorization record. If the rules remove owner properties, the metadata is built again from
/// the stored view, with the removal appended to the request.
fn build_committed_metadata<'l>(
    ctx: &CommitViewContext<'_>,
    previous_metadata: &ViewMetadata,
    previous_location: &'l Location,
) -> Result<CommittedMetadata<'l>> {
    let (metadata, delete_old_location) = build_new_metadata(
        &ctx.request.updates,
        previous_metadata.clone(),
        previous_location,
    )?;
    let stale_owner_properties = owner_property::check_version_change(
        previous_metadata,
        &metadata,
        ctx.property_writes,
        ctx.protected_properties,
        ctx.event_ctx.request_metadata().engines(),
    )
    .map_err(|e| ctx.event_ctx.emit_late_authz_failure(e))?;
    if stale_owner_properties.is_empty() {
        return Ok(CommittedMetadata {
            request: ctx.request.clone(),
            metadata,
            delete_old_location,
        });
    }

    let mut request = CommitViewRequest::clone(ctx.request);
    request.updates.push(ViewUpdate::RemoveProperties {
        removals: stale_owner_properties,
    });
    let (metadata, delete_old_location) = build_new_metadata(
        &request.updates,
        previous_metadata.clone(),
        previous_location,
    )?;
    Ok(CommittedMetadata {
        request: Arc::new(request),
        metadata,
        delete_old_location,
    })
}

/// Removes the location a view moved away from, and logs a failure.
async fn remove_old_view_location(file_io: &impl LakekeeperStorage, location: &Location) {
    tracing::debug!("Deleting old view location at: '{location}'");
    let _ = remove_all(file_io, location)
        .await
        .inspect(|()| tracing::debug!("Deleted old view location {location}"))
        .inspect_err(|e| tracing::error!("Failed to delete old view location '{location}': {e:?}"));
}

fn check_requirements(requirements: Option<&Vec<ViewRequirement>>, view_id: ViewId) -> Result<()> {
    if let Some(requirements) = requirements {
        for assertion in requirements {
            match assertion {
                ViewRequirement::AssertViewUuid(req) => {
                    if req.uuid != *view_id {
                        return Err(ErrorModel::bad_request(
                            "View UUID does not match",
                            "ViewUuidMismatch",
                            None,
                        )
                        .into());
                    }
                }
            }
        }
    }

    Ok(())
}

struct DeleteLocation<'c>(&'c Location);

fn build_new_metadata<'l>(
    updates: &[ViewUpdate],
    before_update_metadata: ViewMetadata,
    before_location: &'l Location,
) -> Result<(ViewMetadata, Option<DeleteLocation<'l>>)> {
    let previous_location = before_update_metadata.location().to_string();
    // Snapshot persisted schemas before the builder consumes the metadata, to reject a commit that
    // recycles a schema id onto different content (defense-in-depth: views have no RemoveSchema
    // update today, so this cannot currently trip — but the storage store diffs schemas by id).
    let previous_schemas: Vec<_> = before_update_metadata.schemas_iter().cloned().collect();

    let mut m = ViewMetadataBuilder::new_from_metadata(before_update_metadata);
    let mut delete_old_location = None;
    for upd in updates {
        m = match upd {
            ViewUpdate::AssignUuid { .. } => {
                return Err(ErrorModel::bad_request(
                    "Assigning UUIDs is not supported",
                    "AssignUuidNotSupported",
                    None,
                )
                .into());
            }
            ViewUpdate::SetLocation { location } => {
                // Compared without trailing slashes, because that is the only
                // difference the builder erases: `set_location` trims before
                // storing, so `s3://b/v/` and `s3://b/v` become the same location.
                // Comparing raw, a client that sends the location it already has
                // with a slash reads as a move to itself -- and `delete_old_location`
                // then purges the directory this commit just wrote its metadata into.
                if location.trim_end_matches('/') != previous_location.trim_end_matches('/') {
                    delete_old_location = Some(DeleteLocation(before_location));
                }
                m.set_location(location.clone())
            }

            ViewUpdate::UpgradeFormatVersion { format_version } => match *format_version {
                ViewFormatVersion::V1 => m,
            },
            ViewUpdate::AddSchema {
                schema,
                last_column_id: _,
            } => m.add_schema(schema.clone()),
            ViewUpdate::SetProperties { updates } => {
                m.set_properties(updates.clone()).map_err(|e| {
                    ErrorModel::bad_request(
                        format!("Error setting properties: {e}"),
                        "AddSchemaError",
                        Some(Box::new(e)),
                    )
                })?
            }
            ViewUpdate::RemoveProperties { removals } => m.remove_properties(removals),
            ViewUpdate::AddViewVersion { view_version } => {
                m.add_version(view_version.clone()).map_err(|e| {
                    ErrorModel::bad_request(
                        format!("Error appending view version: {e}"),
                        "AppendViewVersionError".to_string(),
                        Some(Box::new(e)),
                    )
                })?
            }
            ViewUpdate::SetCurrentViewVersion { view_version_id } => {
                m.set_current_version_id(*view_version_id).map_err(|e| {
                    ErrorModel::bad_request(
                        format!("Error setting current view version: {e}"),
                        "SetCurrentViewVersionError",
                        Some(Box::new(e)),
                    )
                })?
            }
        }
    }

    let requested_update_metadata = m.build().map_err(|e| {
        ErrorModel::bad_request(
            format!("Error building metadata: {e}"),
            "BuildMetadataError",
            Some(Box::new(e)),
        )
    })?;
    crate::server::commit_tables::ensure_schema_content_stable(
        previous_schemas.iter(),
        requested_update_metadata.metadata.schemas_iter(),
    )?;
    Ok((requested_update_metadata.metadata, delete_old_location))
}

pub(crate) fn parse_view_property_updates(
    updates: &[ViewUpdate],
) -> (BTreeMap<String, String>, Vec<String>) {
    let mut property_updates = BTreeMap::new();
    let mut property_removals = Vec::new();

    for update in updates {
        match update {
            ViewUpdate::SetProperties { updates } => {
                property_updates.extend(updates.clone());
            }
            ViewUpdate::RemoveProperties { removals } => {
                property_removals.extend(removals.clone());
            }
            _ => {}
        }
    }

    (property_updates, property_removals)
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, str::FromStr as _};

    use iceberg::{
        NamespaceIdent,
        spec::{
            NestedField, PrimitiveType, Schema, SqlViewRepresentation, Type as IcebergType,
            ViewFormatVersion, ViewMetadata, ViewMetadataBuilder, ViewRepresentation,
            ViewRepresentations, ViewVersion,
        },
    };
    use iceberg_ext::catalog::rest::{CommitViewRequest, ViewUpdate};
    use lakekeeper_io::Location;

    use super::build_new_metadata;

    fn view_metadata(location: &str) -> ViewMetadata {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "id", IcebergType::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();
        let representations = ViewRepresentations::builder()
            .add_representation(ViewRepresentation::Sql(SqlViewRepresentation {
                sql: "select 1".to_string(),
                dialect: "ansi".to_string(),
            }))
            .build()
            .unwrap();
        let version = ViewVersion::builder()
            .with_schema_id(1)
            .with_timestamp_ms(0)
            .with_default_namespace(NamespaceIdent::from_vec(vec!["ns".to_string()]).unwrap())
            .with_representations(representations)
            .build();

        ViewMetadataBuilder::new(
            location.to_string(),
            schema,
            version,
            ViewFormatVersion::V1,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata
    }

    fn set_location(location: &str) -> CommitViewRequest {
        CommitViewRequest {
            identifier: None,
            requirements: None,
            updates: vec![ViewUpdate::SetLocation {
                location: location.to_string(),
            }],
        }
    }

    /// Committing a view's own location back with a trailing slash is not a move,
    /// so the old location must not be scheduled for deletion.
    ///
    /// It is the same directory: `ViewMetadataBuilder::set_location` trims, so the
    /// new location is byte-identical to the old one. Treating it as a move purges
    /// the prefix the commit has just written its metadata into, which leaves the
    /// view unloadable and its data gone.
    #[test]
    fn recommitting_the_same_location_with_a_slash_deletes_nothing() {
        let before = Location::from_str("s3://bucket/view").unwrap();
        let (metadata, delete_old) = build_new_metadata(
            &set_location("s3://bucket/view/").updates,
            view_metadata("s3://bucket/view"),
            &before,
        )
        .unwrap();

        assert_eq!(metadata.location(), "s3://bucket/view");
        assert!(
            delete_old.is_none(),
            "a view's own location was scheduled for deletion"
        );
    }

    /// A genuine relocation still schedules the old location for deletion.
    #[test]
    fn relocating_a_view_deletes_the_old_location() {
        let before = Location::from_str("s3://bucket/view").unwrap();
        let (metadata, delete_old) = build_new_metadata(
            &set_location("s3://bucket/elsewhere").updates,
            view_metadata("s3://bucket/view"),
            &before,
        )
        .unwrap();

        assert_eq!(metadata.location(), "s3://bucket/elsewhere");
        assert!(delete_old.is_some(), "the vacated location was left behind");
    }
}
