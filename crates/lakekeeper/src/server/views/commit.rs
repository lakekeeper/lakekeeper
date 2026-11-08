use std::{
    collections::{BTreeMap, HashSet},
    str::FromStr as _,
    sync::Arc,
};

use http::StatusCode;
use iceberg::spec::{ViewFormatVersion, ViewMetadata, ViewMetadataBuilder};
use iceberg_ext::catalog::{
    ViewRequirement,
    rest::{IcebergErrorResponse, ViewUpdate},
};
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
    config::MatchedEngines,
    request_metadata::RequestMetadata,
    server::{
        compression_codec::CompressionCodec,
        io::{cleanup_before_response, delete_on_failure, remove_all, write_file},
        maybe_get_secret, require_warehouse_id,
        tables::{
            MAX_RETRIES_ON_CONCURRENT_UPDATE, concurrent_update_backoff, determine_table_ident,
            extract_count_from_metadata_location, validate_table_or_view_ident,
        },
        tabular::require_empty_location,
        views::{
            owner_property::{self, PropertyWrites},
            security_mode::{self, SecurityModeSwitches, SwitchRecords, SwitchTarget},
            validate_view_updates,
        },
    },
    service::{
        ArcResolvedWarehouse, AuthZViewInfo, CONCURRENT_UPDATE_ERROR_TYPE, CatalogBackendError,
        CatalogIdempotencyOps, CatalogStore, CatalogView, CatalogViewOps,
        InternalParseLocationError, ReadTransaction, State, TabularListFlags, Transaction,
        ViewCommit, ViewId, ViewInfo,
        authz::{AuthZError, AuthZViewOps, Authorizer, CatalogViewAction, RequireViewActionError},
        contract_verification::ContractVerification,
        events::{
            APIEventContext, ViewEventTransition,
            context::{
                AuthzChecked, Resolved, ResolvedView, Unresolved, UserProvidedView,
                authz_to_error_no_audit,
            },
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
    let request = Arc::new(request);

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
    let request_metadata = event_ctx.request_metadata_arc();
    let rules = OwnerPropertyRules {
        property_writes: PropertyWrites {
            updated: &updated_properties,
            removed: &removed_properties,
        },
        protected_properties: owner_property::protected_properties(state.v1_state.authentication),
        engines: request_metadata.engines(),
    };

    // Security: an engine owner property (e.g. `trino.run-as-owner`) names the user an engine
    // runs the view as. Only a trusted engine that owns the property may set or remove it.
    owner_property::check_property_writes(
        rules.property_writes.keys(),
        rules.protected_properties,
        rules.engines,
    )
    .map_err(|e| event_ctx.emit_early_authz_failure(e))?;

    let (event_ctx, warehouse, view_info, security_mode_switches) = authorize_commit::<C, A>(
        &authorizer,
        event_ctx,
        &request,
        rules,
        state.v1_state.catalog.clone(),
    )
    .await?;

    let view_id = view_info.view_id();
    let event_ctx = event_ctx.resolve(ResolvedView {
        warehouse: warehouse.clone(),
        view: Arc::new(view_info),
    });

    // ------------------- BUSINESS LOGIC -------------------
    // Verify assertions
    check_requirements(request.requirements.as_ref(), view_id)?;

    let ctx = CommitViewContext {
        event_ctx: &event_ctx,
        storage_profile: &warehouse.storage_profile,
        storage_secret_id: warehouse.storage_secret_id,
        request: &request,
        rules,
        security_mode_switches: &security_mode_switches,
    };
    let (result, commit, committed_request) =
        commit_with_retries::<C, A, S>(&ctx, &state, idempotency_key.as_ref()).await?;
    event_ctx.emit_view_committed_async(Arc::new(commit), data_access, committed_request);
    Ok(result)
}

/// Authorizes the commit and, if it switches the view's security mode, the switch. Which
/// further checks the commit needs depends on the switch, so it is decided on the stored view
/// before the commit is recorded.
async fn authorize_commit<C: CatalogStore, A: Authorizer>(
    authorizer: &A,
    event_ctx: APIEventContext<UserProvidedView, Unresolved, CatalogViewAction>,
    request: &Arc<CommitViewRequest>,
    rules: OwnerPropertyRules<'_>,
    catalog_state: C::State,
) -> Result<(
    APIEventContext<UserProvidedView, Unresolved, CatalogViewAction, AuthzChecked>,
    ArcResolvedWarehouse,
    ViewInfo,
    SecurityModeSwitches,
)> {
    let authz_result = authorizer
        .load_and_authorize_view_operation::<C>(
            event_ctx.request_metadata(),
            event_ctx.user_provided_entity(),
            TabularListFlags::active(),
            event_ctx.action().clone(),
            catalog_state.clone(),
        )
        .await;
    let authz_result = match authz_result {
        Ok((warehouse, namespace, view_info)) => {
            decide_security_mode_switches::<C>(request, rules, &view_info, catalog_state)
                .await
                .map(|switches| (warehouse, namespace, view_info, switches))
        }
        Err(e) => Err(e),
    };

    let (event_ctx, (warehouse, namespace, view_info, switches)) =
        event_ctx.emit_authz(authz_result)?;
    security_mode::authorize_security_mode_switches(
        authorizer,
        SwitchRecords {
            request_metadata: &event_ctx.request_metadata_arc(),
            dispatcher: event_ctx.dispatcher(),
            view: &event_ctx.user_provided_entity_arc(),
        },
        SwitchTarget {
            warehouse: &warehouse,
            namespace: &namespace,
            view: &view_info,
        },
        &switches,
    )
    .await?;
    Ok((event_ctx, warehouse, view_info, switches))
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
    rules: OwnerPropertyRules<'a>,
    /// The security mode switches decided and authorized before the commit transaction.
    security_mode_switches: &'a SecurityModeSwitches,
}

/// What the owner property rules of a view commit depend on.
#[derive(Clone, Copy)]
struct OwnerPropertyRules<'a> {
    /// The properties the authorized commit sets and removes.
    property_writes: PropertyWrites<'a>,
    /// `None` if the owner property rules are off.
    protected_properties: Option<&'static HashSet<String>>,
    engines: &'a MatchedEngines,
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
    } = build_committed_metadata(ctx, &previous_view.metadata)?;
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

    let storage_secret = maybe_get_secret(ctx.storage_secret_id, &state.v1_state.secrets).await?;
    let storage_secret_ref = storage_secret.as_deref();
    let file_io = ctx.storage_profile.file_io(storage_secret_ref).await?;
    let moved = check_view_location(ctx, &file_io, &previous_view_location, &new_location).await?;

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

    // Write the metadata file and commit
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
    if moved
        && !cleanup_before_response(
            request_metadata.received_at(),
            remove_old_view_location(&file_io, &previous_view_location),
        )
        .await
    {
        tracing::warn!(
            "Stopped deleting old view location '{previous_view_location}' at the request deadline. Its remaining files stay in storage."
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
struct CommittedMetadata {
    /// The client's request, with a `remove-properties` update appended if the owner property
    /// rules remove owner properties.
    request: Arc<CommitViewRequest>,
    metadata: ViewMetadata,
}

/// Builds the metadata a commit produces from the stored view, applies the owner property rules
/// and requires the security mode switches that were decided before the transaction. A
/// refusal of the rules follows the request's authorization record.
fn build_committed_metadata(
    ctx: &CommitViewContext<'_>,
    previous_metadata: &ViewMetadata,
) -> Result<CommittedMetadata> {
    let committed = apply_owner_property_rules(ctx.request, previous_metadata, ctx.rules, |e| {
        ctx.event_ctx.emit_late_authz_failure(e)
    })?;
    if let Some(protected_properties) = ctx.rules.protected_properties {
        let switches = security_mode::security_mode_switches(
            previous_metadata,
            &committed.metadata,
            protected_properties,
        );
        security_mode::require_decided_switches(ctx.security_mode_switches, &switches)
            .map_err(ErrorModel::from)?;
    }
    Ok(committed)
}

/// Builds the metadata a commit produces from `previous_metadata` and applies the owner property
/// rules of [`owner_property::check_version_change`]; `on_refusal` reports a refusal. If the
/// rules remove owner properties, the metadata is built again from `previous_metadata`, with the
/// removal appended to the request.
fn apply_owner_property_rules(
    request: &Arc<CommitViewRequest>,
    previous_metadata: &ViewMetadata,
    rules: OwnerPropertyRules<'_>,
    on_refusal: impl FnOnce(owner_property::ProtectedViewVersionChange) -> ErrorModel,
) -> Result<CommittedMetadata> {
    let metadata = build_new_metadata(&request.updates, previous_metadata.clone())?;
    let stale_owner_properties = owner_property::check_version_change(
        previous_metadata,
        &metadata,
        rules.property_writes,
        rules.protected_properties,
        rules.engines,
    )
    .map_err(on_refusal)?;
    if stale_owner_properties.is_empty() {
        return Ok(CommittedMetadata {
            request: request.clone(),
            metadata,
        });
    }

    let mut request = CommitViewRequest::clone(request);
    request.updates.push(ViewUpdate::RemoveProperties {
        removals: stale_owner_properties,
    });
    let metadata = build_new_metadata(&request.updates, previous_metadata.clone())?;
    Ok(CommittedMetadata {
        request: Arc::new(request),
        metadata,
    })
}

/// The security mode switches a commit makes, decided on the stored view. Only a trusted engine
/// that owns an owner property can switch the mode, and only on a view or with a request that
/// has one, so the stored view is read only then. A commit the owner property rules refuse
/// switches nothing; the commit transaction reports the refusal. If the stored view changes
/// before that transaction, it refuses a commit that then switches otherwise.
async fn decide_security_mode_switches<C: CatalogStore>(
    request: &Arc<CommitViewRequest>,
    rules: OwnerPropertyRules<'_>,
    view_info: &ViewInfo,
    catalog_state: C::State,
) -> std::result::Result<SecurityModeSwitches, AuthZError> {
    let Some(protected_properties) = rules.protected_properties else {
        return Ok(SecurityModeSwitches::new());
    };
    let engine_owns_one = protected_properties
        .iter()
        .any(|p| rules.engines.owns_property(p));
    let view_or_request_has_one = protected_properties.iter().any(|p| {
        view_info.properties.contains_key(p) || rules.property_writes.updated.contains_key(p)
    });
    if !engine_owns_one || !view_or_request_has_one {
        return Ok(SecurityModeSwitches::new());
    }

    let mut t = ReadTransaction::<C>::begin(catalog_state)
        .await
        .map_err(catalog_error)?;
    let stored = C::load_view(
        view_info.warehouse_id,
        view_info.tabular_id,
        false,
        t.transaction(),
    )
    .await
    .map_err(catalog_error)?;
    t.commit().await.map_err(catalog_error)?;

    Ok(
        apply_owner_property_rules(request, &stored.metadata, rules, authz_to_error_no_audit)
            .map(|committed| {
                security_mode::security_mode_switches(
                    &stored.metadata,
                    &committed.metadata,
                    protected_properties,
                )
            })
            .unwrap_or_default(),
    )
}

/// A catalog error while deciding the authorization of a commit.
fn catalog_error(e: impl Into<IcebergErrorResponse>) -> AuthZError {
    RequireViewActionError::CatalogBackendError(CatalogBackendError::new_unexpected(
        ErrorModel::from(e.into()),
    ))
    .into()
}

/// Checks the location a commit gives the view, and returns whether the view moves away from
/// `previous`. The old location is then removed with all files in it, so the view moves only
/// to an empty location, as on create.
async fn check_view_location(
    ctx: &CommitViewContext<'_>,
    file_io: &impl LakekeeperStorage,
    previous: &Location,
    new: &Location,
) -> Result<bool> {
    if new == previous {
        return Ok(false);
    }
    ctx.storage_profile.require_allowed_location(new)?;
    let moved = is_move(previous, new)?;
    if moved {
        require_empty_location(file_io, new).await?;
    }
    Ok(moved)
}

/// Whether `new` is another directory than `previous`. Compared as the catalog stores
/// locations, without scheme and trailing slash: `s3a://b/v` is the directory of `s3://b/v`.
/// Fails if one is inside the other: removing the old location would remove the new metadata.
fn is_move(previous: &Location, new: &Location) -> Result<bool> {
    let previous = previous.authority_and_path().trim_end_matches('/');
    let new = new.authority_and_path().trim_end_matches('/');
    let inside = |inner: &str, outer: &str| {
        inner
            .strip_prefix(outer)
            .is_some_and(|rest| rest.starts_with('/'))
    };
    if previous == new {
        return Ok(false);
    }
    if inside(new, previous) || inside(previous, new) {
        return Err(ErrorModel::bad_request(
            "Cannot move a view into its own location or into a parent of it",
            "SetLocationNotAllowed",
            None,
        )
        .into());
    }
    Ok(true)
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

fn build_new_metadata(
    updates: &[ViewUpdate],
    before_update_metadata: ViewMetadata,
) -> Result<ViewMetadata> {
    // Snapshot persisted schemas before the builder consumes the metadata, to reject a commit that
    // recycles a schema id onto different content (defense-in-depth: views have no RemoveSchema
    // update today, so this cannot currently trip — but the storage store diffs schemas by id).
    let previous_schemas: Vec<_> = before_update_metadata.schemas_iter().cloned().collect();

    let mut m = ViewMetadataBuilder::new_from_metadata(before_update_metadata);
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
            ViewUpdate::SetLocation { location } => m.set_location(location.clone()),

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
    Ok(requested_update_metadata.metadata)
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
            // No `_` arm: a new kind of update is authorized here only once it is listed.
            ViewUpdate::AssignUuid { .. }
            | ViewUpdate::UpgradeFormatVersion { .. }
            | ViewUpdate::AddSchema { .. }
            | ViewUpdate::SetLocation { .. }
            | ViewUpdate::AddViewVersion { .. }
            | ViewUpdate::SetCurrentViewVersion { .. } => {}
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

    use super::{build_new_metadata, is_move};

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

    fn location(location: &str) -> Location {
        Location::from_str(location).unwrap()
    }

    /// The builder stores a location without its trailing slash, so a client that sends the
    /// view's own location with one leaves the location unchanged, and nothing is removed.
    #[test]
    fn the_own_location_with_a_trailing_slash_is_unchanged() {
        let metadata = build_new_metadata(
            &set_location("s3://bucket/view/").updates,
            view_metadata("s3://bucket/view"),
        )
        .unwrap();

        assert_eq!(metadata.location(), "s3://bucket/view");
    }

    /// An alternative scheme names the same directory: the view stays, nothing is removed.
    #[test]
    fn another_spelling_of_the_own_location_is_no_move() {
        let previous = location("s3://bucket/view");
        assert!(!is_move(&previous, &location("s3a://bucket/view")).unwrap());
        assert!(!is_move(&previous, &location("s3a://bucket/view/")).unwrap());
    }

    #[test]
    fn another_directory_is_a_move() {
        let previous = location("s3://bucket/ns/view");
        assert!(is_move(&previous, &location("s3://bucket/ns/elsewhere")).unwrap());
        assert!(is_move(&previous, &location("s3://bucket/ns/view2")).unwrap());
        assert!(is_move(&previous, &location("s3://other-bucket/ns/view")).unwrap());
    }

    /// Removing the old location after the move would remove the new metadata inside it.
    #[test]
    fn a_move_into_or_above_the_own_location_is_refused() {
        let previous = location("s3://bucket/ns/view");
        for new in [
            "s3://bucket/ns/view/sub",
            "s3a://bucket/ns/view/sub",
            "s3://bucket/ns",
        ] {
            let err = is_move(&previous, &location(new)).unwrap_err();
            assert_eq!(err.error.r#type, "SetLocationNotAllowed", "{new}");
        }
    }
}
