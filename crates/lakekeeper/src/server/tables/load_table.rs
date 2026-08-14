use std::{collections::HashMap, sync::Arc};

use http::StatusCode;
use iceberg_ext::catalog::rest::ETag;

use crate::{
    WarehouseId,
    api::iceberg::v1::{
        ApiContext, LoadTableResult, LoadTableResultOrNotModified, Result, TableIdent,
        TableParameters,
        tables::{LoadTableFilters, LoadTableRequest},
    },
    request_metadata::RequestMetadata,
    server::{
        maybe_get_secret, require_warehouse_id,
        tables::{
            authorize_load_table,
            etag::{StorageAccess, TableETag, TableResponseShape},
            parse_location, validate_table_or_view_ident,
        },
    },
    service::{
        AuthZTableInfo as _, CachePolicy, CatalogStore, CatalogTableOps, CatalogWarehouseOps,
        LoadTableResponse as CatalogLoadTableResult, State, TableId, TableIdentOrId,
        TabularListFlags, TabularNotFound, Transaction, WarehouseStatus,
        authz::{Authorizer, AuthzWarehouseOps, CatalogTableAction},
        events::{
            APIEventContext,
            context::{ResolvedTable, authz_to_error_no_audit},
        },
        secrets::SecretStore,
        storage::{credential_revalidate_after_ms, now_epoch_ms},
    },
};

/// Load a table from the catalog.
pub async fn load_table<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: TableParameters,
    request: LoadTableRequest,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<LoadTableResultOrNotModified> {
    load_table_with_flags(
        parameters,
        request,
        state,
        request_metadata,
        TabularListFlags::active(),
    )
    .await
}

/// [`load_table`], but with control over whether a staged table is visible.
///
/// `loadTable` itself must never serve a staged table — a client that sees one
/// would treat an uncommitted table as real. The idempotency replay path is the
/// exception: a `createTable` with `stage_create` returns a staged
/// [`LoadTableResult`], and replaying that key has to reproduce it rather than
/// 404. Staged-ness is gated twice — once when resolving the table for authz and
/// once on the loaded row — so both have to agree.
///
/// # Panics
/// May panic if internal invariants are violated (e.g., an entry expected to
/// exist in a pre-resolved map is missing).
#[allow(clippy::too_many_lines)]
pub(crate) async fn load_table_with_flags<
    C: CatalogStore,
    A: Authorizer + Clone,
    S: SecretStore,
>(
    parameters: TableParameters,
    request: LoadTableRequest,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
    list_flags: TabularListFlags,
) -> Result<LoadTableResultOrNotModified> {
    let LoadTableRequest {
        data_access,
        filters,
        etags,
        referenced_by,
    } = request;

    // ------------------- VALIDATIONS -------------------
    let TableParameters { prefix, table } = parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    // It is important to throw a 404 if a table cannot be found,
    // because spark might check if `table`.`branch` exists, which should return 404.
    // Only then will it treat it as a branch.
    if let Err(mut e) = validate_table_or_view_ident(&table) {
        if e.error.r#type == *"NamespaceDepthExceeded" {
            e.error.code = StatusCode::NOT_FOUND.into();
        }
        return Err(e);
    }

    // ------------------- AUTHZ -------------------
    let authorizer = state.v1_state.authz;
    let catalog_state = state.v1_state.catalog;

    let event_ctx = APIEventContext::for_table(
        Arc::new(request_metadata.clone()),
        state.v1_state.events,
        warehouse_id,
        table.clone(),
        CatalogTableAction::GetMetadata,
    );

    let (event_ctx, (warehouse, table_info, storage_permissions)) = event_ctx.emit_authz(
        authorize_load_table::<C, A>(
            &request_metadata,
            table,
            warehouse_id,
            list_flags,
            authorizer.clone(),
            catalog_state.clone(),
            referenced_by.as_deref(),
        )
        .await,
    )?;

    let mut event_ctx = event_ctx.resolve(ResolvedTable {
        warehouse,
        table: Arc::new(table_info),
        storage_permissions,
    });

    // ------------------- ETAG CHECK -------------------
    // The 304 decision rides on the client-echoed ETag's revalidation point; this
    // flag only governs the cases where the ETag carries none (metadata-only /
    // wildcard). Not the raw `vended-credentials` flag, since backends vend
    // expiring credentials even for the default request (S3 auto-promotes;
    // GCS/Azure vend for any delegated access).
    let vends_credentials = storage_permissions.is_some()
        && event_ctx
            .resolved()
            .warehouse
            .storage_profile
            .vends_expiring_credentials(data_access);
    // The warehouse version is a parameter rather than read from `event_ctx`,
    // because the two call sites see different versions: the 304 check below
    // runs before the table is loaded, while the response tag must name the
    // version the body was actually generated from — which the refetch further
    // down may have advanced.
    let shape_at = |warehouse_version| {
        TableResponseShape::for_load(
            &filters,
            // Both per-request inputs to `generate_table_config`. Without storage
            // access there is no `config` at all — a load made before access was
            // granted must not answer one made after; and credentials are
            // policy-scoped per permission level, so a read-scoped body must not
            // answer a request from a caller who can now write.
            match storage_permissions {
                None => StorageAccess::NoConfig,
                Some(permissions) => StorageAccess::Config {
                    delegation: data_access,
                    permissions,
                    warehouse_version,
                },
            },
        )
    };
    if let Some(etag) = match_not_modified(
        &etags,
        warehouse_id,
        event_ctx
            .resolved()
            .table
            .metadata_location
            .as_ref()
            .map(lakekeeper_io::Location::as_str),
        shape_at(event_ctx.resolved().warehouse.version),
        now_epoch_ms(),
        vends_credentials,
    ) {
        return Ok(LoadTableResultOrNotModified::NotModifiedResponse(etag));
    }

    // ------------------- BUSINESS LOGIC -------------------
    let mut t = C::Transaction::begin_read(catalog_state.clone()).await?;
    let CatalogLoadTableResult {
        table_id: _,
        namespace_id: _,
        table_metadata,
        metadata_location,
        warehouse_version,
    } = load_table_inner::<C>(
        warehouse_id,
        event_ctx.resolved().table.table_id(),
        event_ctx.resolved().table.table_ident(),
        false,
        list_flags.include_staged,
        &filters,
        &mut t,
    )
    .await?;
    t.commit().await?;

    // Refetch warehouse if version is stale
    if event_ctx.resolved().warehouse.version < warehouse_version {
        let warehouse = C::get_warehouse_by_id_cache_aware(
            warehouse_id,
            WarehouseStatus::active(),
            CachePolicy::RequireMinimumVersion(*warehouse_version),
            catalog_state.clone(),
        )
        .await;
        let fresh_warehouse = authorizer
            .require_warehouse_presence(warehouse_id, warehouse)
            .map_err(authz_to_error_no_audit)?;
        event_ctx.resolved_mut().warehouse = fresh_warehouse;
    }
    let warehouse = &event_ctx.resolved().warehouse;
    // Bound to the version `generate_table_config` below reads the profile from,
    // which the refetch above may have advanced past the one the 304 check used.
    // Reusing that earlier shape would let one tag stand for two different bodies.
    let response_shape = shape_at(warehouse.version);

    let table_location =
        parse_location(table_metadata.location(), StatusCode::INTERNAL_SERVER_ERROR)?;

    let storage_config = if let Some(storage_permissions) = storage_permissions {
        let storage_secret =
            maybe_get_secret(warehouse.storage_secret_id, &state.v1_state.secrets).await?;
        let storage_secret_ref = storage_secret.as_deref();
        Some(
            warehouse
                .storage_profile
                .generate_table_config(
                    data_access,
                    storage_secret_ref,
                    &table_location,
                    storage_permissions,
                    &request_metadata,
                    &*event_ctx.resolved().table,
                )
                .await?,
        )
    } else {
        None
    };

    let storage_credentials = storage_config
        .as_ref()
        .and_then(|c| c.storage_credentials(&table_location));
    let credentials_revalidate_after_ms = storage_config
        .as_ref()
        .and_then(|c| c.credentials_expiration_ms)
        .map(credential_revalidate_after_ms);

    let metadata_ref = Arc::new(table_metadata);
    let metadata_location_ref = metadata_location.map(Arc::new);

    event_ctx.emit_table_loaded_async(metadata_ref.clone(), metadata_location_ref.clone());

    let remote_signing_config = storage_config
        .as_ref()
        .and_then(|c| c.remote_signing.clone());
    let load_table_result = LoadTableResult {
        metadata_location: metadata_location_ref.as_ref().map(ToString::to_string),
        metadata: metadata_ref,
        config: storage_config.map(|c| c.config.into()),
        storage_credentials,
        remote_signing_config,
        etag: metadata_location_ref.as_ref().map(|loc| {
            TableETag::new(
                warehouse_id,
                loc.as_str(),
                response_shape,
                credentials_revalidate_after_ms,
            )
            .into_etag()
        }),
    };

    Ok(LoadTableResultOrNotModified::LoadTableResult(
        load_table_result,
    ))
}

/// Load a table from the catalog, by default rejecting a staged one.
///
/// # Errors
/// Returns an error if the table is staged and `include_staged` is false, if it
/// cannot be found, or if a DB error occurs.
async fn load_table_inner<C: CatalogStore>(
    warehouse_id: WarehouseId,
    table_id: TableId,
    table_ident: &TableIdent,
    include_deleted: bool,
    include_staged: bool,
    load_table_filters: &LoadTableFilters,
    t: &mut C::Transaction,
) -> Result<CatalogLoadTableResult> {
    let mut metadatas = C::load_tables(
        warehouse_id,
        [table_id],
        include_deleted,
        load_table_filters,
        t.transaction(),
    )
    .await?
    .into_iter()
    .map(|r| (r.table_id, r))
    .collect::<HashMap<_, _>>();
    let result = metadatas.remove(&table_id).ok_or_else(|| {
        TabularNotFound::new(warehouse_id, TableIdentOrId::from(table_ident.clone()))
            .append_detail("Table metadata not returned from table load".to_string())
    })?;
    if !metadatas.is_empty() {
        tracing::error!(
            "Unexpected extra table metadatas returned when loading table {:?} in warehouse {:?}: {:?}",
            table_ident,
            warehouse_id,
            metadatas.keys()
        );
    }
    if !include_staged {
        require_not_staged(
            warehouse_id,
            table_ident.clone(),
            result.metadata_location.as_ref(),
        )?;
    }
    Ok(result)
}

fn require_not_staged<T>(
    warehouse_id: WarehouseId,
    table_ident: impl Into<TableIdentOrId>,
    metadata_location: Option<&T>,
) -> std::result::Result<(), TabularNotFound> {
    if metadata_location.is_none() {
        return Err(TabularNotFound::new(warehouse_id, table_ident.into())
            .append_detail("Table is in staged state; operation requires active table"));
    }

    Ok(())
}

/// Decide whether a conditional `loadTable` may return `304 Not Modified`,
/// returning the [`ETag`] to echo back if so.
///
/// When the client-echoed [`ETag`] carries a revalidation point (it cached a
/// credential-bearing response), a 304 is served only while `now` is before it.
/// When it carries none (metadata-only / wildcard), a 304 is served only if this
/// load also vends no expiring credentials (`!vends_credentials`). Anything we
/// can't parse isn't matched, so the client reloads — never a 304 with stale
/// credentials.
///
/// `shape` describes the body *this* request would produce. It is folded into
/// the comparison tag, so a client that cached one shape and revalidates for
/// another — a different `snapshots` filter, or different access delegation —
/// misses and gets a full response instead of a 304 for content it never
/// received.
fn match_not_modified(
    client_etags: &[ETag],
    warehouse_id: WarehouseId,
    metadata_location: Option<&str>,
    shape: TableResponseShape,
    now_ms: i64,
    vends_credentials: bool,
) -> Option<ETag> {
    let metadata_location = metadata_location?;
    let current = TableETag::new(warehouse_id, metadata_location, shape, None);

    for client in client_etags {
        let value = client.as_str();

        // Wildcard matches the metadata, but carries no revalidation point.
        if value == "*" {
            if vends_credentials {
                continue;
            }
            return Some(current.clone().into_etag());
        }

        // Not parseable as one of our ETags → reload.
        let Some(parsed) = TableETag::parse(value) else {
            continue;
        };
        if parsed.hash() != current.hash() {
            continue;
        }
        match parsed.revalidate_after_ms() {
            // Client holds credentials: 304 only while still within their window.
            Some(revalidate_after) => {
                if now_ms < revalidate_after {
                    return Some(parsed.into_etag());
                }
            }
            // Metadata-only cached response: 304 only if we'd add no creds now.
            None => {
                if !vends_credentials {
                    return Some(parsed.into_etag());
                }
            }
        }
    }
    None
}

#[cfg(test)]
mod etag_tests {
    use super::*;
    use crate::{
        api::iceberg::v1::tables::{DataAccess, DataAccessMode, SnapshotsQuery},
        service::{WarehouseVersion, storage::StoragePermissions},
    };

    const LOC: &str = "s3://bucket/table/metadata.json";
    const NOW: i64 = 1_750_000_000_000;
    /// Default shape: all snapshots, default (unspecified) delegation.
    fn wv() -> WarehouseVersion {
        WarehouseVersion::new(7)
    }

    fn delegated(vended_credentials: bool, remote_signing: bool) -> DataAccessMode {
        DataAccessMode::ServerDelegated(DataAccess {
            vended_credentials,
            remote_signing,
        })
    }

    /// A config-bearing shape with the given snapshot filter.
    fn shape_of(
        snapshots: SnapshotsQuery,
        delegation: DataAccessMode,
        permissions: StoragePermissions,
    ) -> TableResponseShape {
        TableResponseShape::new(
            snapshots,
            StorageAccess::Config {
                delegation,
                permissions,
                warehouse_version: wv(),
            },
        )
    }

    /// A default delegated load: all snapshots, unspecified delegation, writer.
    fn all() -> TableResponseShape {
        shape_of(
            SnapshotsQuery::All,
            delegated(false, false),
            StoragePermissions::ReadWriteDelete,
        )
    }

    /// The same, refs-filtered.
    fn refs() -> TableResponseShape {
        shape_of(
            SnapshotsQuery::Refs,
            delegated(false, false),
            StoragePermissions::ReadWriteDelete,
        )
    }

    /// Fixed: the tag is scoped to a warehouse, and every helper here uses the
    /// same one so a mismatch means a real shape difference.
    fn wh() -> WarehouseId {
        WarehouseId::new(uuid::uuid!("019bbf1a-0000-7000-8000-0000000000a1"))
    }

    /// Build a client-supplied `ETag` for a given shape, in the form
    /// `parse_etags` yields. `revalidate_after` = `None` for a metadata-only
    /// cached response.
    fn client_etag_for(
        loc: &str,
        shape: TableResponseShape,
        revalidate_after: Option<i64>,
    ) -> ETag {
        as_client_etag(&TableETag::new(wh(), loc, shape, revalidate_after).into_etag())
    }

    /// The shape a client echoes back: the wire value with the weak marker and
    /// quotes stripped, exactly as the HTTP layer's `parse_etags` produces.
    fn as_client_etag(etag: &ETag) -> ETag {
        ETag::from(etag.as_str().trim_start_matches("W/").trim_matches('"'))
    }

    /// Default-shape client [`ETag`].
    fn client_etag(loc: &str, revalidate_after: Option<i64>) -> ETag {
        client_etag_for(loc, all(), revalidate_after)
    }

    fn matches_repr(etags: &[ETag], shape: TableResponseShape, vends_credentials: bool) -> bool {
        match_not_modified(etags, wh(), Some(LOC), shape, NOW, vends_credentials).is_some()
    }

    fn matches(etags: &[ETag], vends_credentials: bool) -> bool {
        matches_repr(etags, all(), vends_credentials)
    }

    #[test]
    fn metadata_only_load_returns_304_for_matching_etags() {
        // A metadata-only ETag and the wildcard match when this load vends no creds.
        assert!(matches(&[client_etag(LOC, None)], false));
        assert!(matches(&[ETag::from("*")], false));
    }

    #[test]
    fn unparseable_etag_triggers_reload() {
        // A pre-upgrade bare-hash ETag (or any non-`lk2` value) can't be parsed,
        // so it never yields a 304. The client reloads once and re-primes.
        let legacy = ETag::from(TableETag::new(wh(), LOC, all(), None).hash());
        assert!(!matches(&[legacy], false));
        assert!(!matches(&[ETag::from("not-our-etag")], false));
    }

    #[test]
    fn no_match_when_metadata_differs() {
        let other = client_etag("s3://bucket/table/metadata-2.json", Some(NOW + 60_000));
        assert!(!matches(std::slice::from_ref(&other), false));
        assert!(!matches(&[other], true));
    }

    #[test]
    fn no_match_when_metadata_location_absent() {
        assert!(match_not_modified(&[ETag::from("*")], wh(), None, all(), NOW, false).is_none());
    }

    #[test]
    fn never_304s_at_or_after_credential_expiry() {
        // The safety invariant, end-to-end: compose the producer
        // (`revalidate_after_at`, including its clamp) with the checker. Whatever
        // revalidation point we mint for a credential, a conditional request at or
        // after the real expiry must never be answered with a 304.
        use crate::service::storage::revalidate_after_at;
        for (expiry, vend_now) in [
            (NOW + 600_000, NOW),       // 10-min credential
            (NOW + 4 * 3_600_000, NOW), // long credential (1h cap)
            (NOW + 1, NOW),             // about to expire
            (NOW, NOW),                 // already at expiry
        ] {
            let etag = client_etag(LOC, Some(revalidate_after_at(expiry, vend_now)));
            for check_now in [expiry, expiry + 1, expiry + 60_000] {
                assert!(
                    match_not_modified(
                        std::slice::from_ref(&etag),
                        wh(),
                        Some(LOC),
                        all(),
                        check_now,
                        true
                    )
                    .is_none(),
                    "served a 304 at/after expiry (expiry={expiry}, check_now={check_now})"
                );
            }
        }
    }

    #[test]
    fn credential_load_honors_embedded_revalidate_after() {
        // Revalidation point still in the future → 304.
        assert!(matches(&[client_etag(LOC, Some(NOW + 1))], true));
        // Reached/passed → must re-vend (200).
        assert!(!matches(&[client_etag(LOC, Some(NOW))], true));
        assert!(!matches(&[client_etag(LOC, Some(NOW - 60_000))], true));
        // No revalidation point (client cached a metadata-only response) while we
        // now vend creds → must re-vend so the client gets them.
        assert!(!matches(&[client_etag(LOC, None)], true));
    }

    #[test]
    fn future_revalidate_after_serves_304_even_for_metadata_only_load() {
        // The decision rides on the echoed ETag, not the current load's flag.
        assert!(matches(&[client_etag(LOC, Some(NOW + 60_000))], false));
    }

    #[test]
    fn credential_load_rejects_unparseable_and_wildcard() {
        // Unparseable ETag and wildcard carry no revalidation point → reload.
        let legacy = ETag::from(TableETag::new(wh(), LOC, all(), None).hash());
        assert!(!matches(&[legacy], true));
        assert!(!matches(&[ETag::from("*")], true));
    }

    #[test]
    fn no_304_across_snapshot_representations() {
        // A client caches `snapshots=refs` (a truncated snapshot list), then
        // revalidates asking for `snapshots=all`. When both sides hashed only the
        // metadata location this matched, and the client kept using the truncated
        // body as if it were complete.
        let cached_refs = client_etag_for(LOC, refs(), None);
        assert!(
            !matches_repr(std::slice::from_ref(&cached_refs), all(), false),
            "304'd an `all` request from a `refs` ETag: client would reuse a truncated snapshot list"
        );

        // And the reverse: a full body cached, then a `refs` request.
        let cached_all = client_etag_for(LOC, all(), None);
        assert!(
            !matches_repr(std::slice::from_ref(&cached_all), refs(), false),
            "304'd a `refs` request from an `all` ETag"
        );

        // Each still 304s against its own representation — the fix must not
        // disable conditional requests, only stop them crossing representations.
        assert!(matches_repr(&[cached_refs], refs(), false));
        assert!(matches_repr(&[cached_all], all(), false));
    }

    #[test]
    fn no_304_across_delegation_modes() {
        // Storage config depends on the delegation the client asked for, so a
        // tag minted under one mode must not satisfy a request under another.
        // These cases cover the revalidate-less branch, reachable when the load
        // vends no expiring credentials — client-managed access, or the in-memory
        // test profile. See `credential_load_rejects_cross_delegation_304` for
        // the branch that real S3/GCS/ADLS traffic takes.
        let signing = shape_of(
            SnapshotsQuery::All,
            delegated(false, true),
            StoragePermissions::ReadWriteDelete,
        );
        let client_managed = shape_of(
            SnapshotsQuery::All,
            DataAccessMode::ClientManaged,
            StoragePermissions::ReadWriteDelete,
        );

        let cached_signing = client_etag_for(LOC, signing, None);
        assert!(
            !matches_repr(std::slice::from_ref(&cached_signing), all(), false),
            "304'd an unspecified-delegation request from a remote-signing ETag"
        );
        assert!(
            !matches_repr(std::slice::from_ref(&cached_signing), client_managed, false),
            "304'd a client-managed request from a remote-signing ETag"
        );
        // Still 304s its own shape.
        assert!(matches_repr(&[cached_signing], signing, false));

        // And the reverse direction: a plain cached body must not satisfy a
        // request that asks for signing.
        let cached_default = client_etag_for(LOC, all(), None);
        assert!(
            !matches_repr(std::slice::from_ref(&cached_default), signing, false),
            "304'd a remote-signing request from an unspecified-delegation ETag: \
             the client would get no signer config"
        );
    }

    #[test]
    fn credential_load_rejects_cross_delegation_304() {
        // The branch real traffic takes. On S3/GCS/ADLS `vends_expiring_credentials`
        // is true for *any* server-delegated request — it never inspects
        // `remote_signing` — so a delegated load always carries a revalidation
        // point and the `vends_credentials` gate cannot distinguish modes. Inside
        // that window only the shape stops the 304.
        let vended = shape_of(
            SnapshotsQuery::All,
            delegated(true, false),
            StoragePermissions::ReadWriteDelete,
        );
        let signing = shape_of(
            SnapshotsQuery::All,
            delegated(false, true),
            StoragePermissions::ReadWriteDelete,
        );
        // Cached a credential-bearing response, still well inside its window.
        let cached = client_etag_for(LOC, vended, Some(NOW + 60_000));

        assert!(
            !matches_repr(std::slice::from_ref(&cached), signing, true),
            "304'd a remote-signing request from a vended-credentials ETag while \
             the credential window was still open"
        );
        // Its own shape still 304s inside the window — the gate is unchanged.
        assert!(matches_repr(&[cached], vended, true));
    }

    #[test]
    fn no_storage_access_is_its_own_shape() {
        // A load the caller has no storage access for returns `config: null`.
        // That body must not satisfy a later load made after access is granted.
        let no_config = TableResponseShape::no_storage_config();
        let cached = client_etag_for(LOC, no_config, None);
        assert!(
            !matches_repr(std::slice::from_ref(&cached), all(), false),
            "304'd a config-bearing load from a config-less ETag"
        );
        assert!(matches_repr(&[cached], no_config, false));
    }

    #[test]
    fn no_304_across_storage_permission_levels() {
        // Vended credentials are policy-scoped per level, so a body built for a
        // read-only caller must not answer a request from one who can now write.
        // `create_table`/`register_table` always scope to ReadWriteDelete, so
        // without this a read-only caller's load could match a create tag.
        let read = shape_of(
            SnapshotsQuery::All,
            delegated(false, false),
            StoragePermissions::Read,
        );
        let cached_read = client_etag_for(LOC, read, Some(NOW + 60_000));
        assert!(
            !matches_repr(std::slice::from_ref(&cached_read), all(), true),
            "304'd a read-write-delete load from a read-scoped ETag: the client \
             would keep credentials that cannot write"
        );
        assert!(matches_repr(&[cached_read], read, true));
    }

    #[test]
    fn commit_etag_does_not_satisfy_a_delegated_load() {
        // A commit response carries `config: None`, which no load reproduces, so
        // its tag must not 304 a load that expects storage config.
        let commit = as_client_etag(&super::super::etag::commit_etag(wh(), LOC));
        assert!(!matches(std::slice::from_ref(&commit), false));
        assert!(!matches_repr(&[commit], refs(), false));
    }

    #[test]
    fn wildcard_echoes_the_requested_representation() {
        // `If-None-Match: *` carries no representation, so the tag we echo must be
        // the one for the body this request would have produced. Echoing the
        // default here would hand the client an `all` tag for a `refs` body.
        let echoed = match_not_modified(&[ETag::from("*")], wh(), Some(LOC), refs(), NOW, false)
            .expect("wildcard should 304 when no credentials are vended");
        // Compare against the quoted wire form — `client_etag_for` yields the
        // unquoted shape a client echoes back, which is not what we emit.
        assert_eq!(echoed, TableETag::new(wh(), LOC, refs(), None).into_etag());
        assert_ne!(
            echoed,
            TableETag::new(wh(), LOC, all(), None).into_etag(),
            "wildcard echoed the default representation instead of the requested one"
        );
        // That echoed tag must then be accepted for `refs` and rejected for `all`.
        let echoed_bare = as_client_etag(&echoed);
        assert!(matches_repr(
            std::slice::from_ref(&echoed_bare),
            refs(),
            false
        ));
        assert!(!matches_repr(&[echoed_bare], all(), false));
    }

    /// The defect the warehouse-id axis exists for, asserted at the decision
    /// level rather than only on the hash. Every other helper here shares one
    /// warehouse — correctly, so that a differing id cannot mask a shape bug —
    /// which is exactly why this case needs its own test.
    ///
    /// `vends_credentials` is true with an open revalidation window on purpose:
    /// that neuters the credential gate, so only the warehouse axis can refuse.
    #[test]
    fn no_304_across_warehouses_at_the_same_location() {
        let other = WarehouseId::new(uuid::uuid!("f1000000-0000-7000-8000-00000000000f"));
        let cached_a =
            as_client_etag(&TableETag::new(wh(), LOC, all(), Some(NOW + 60_000)).into_etag());

        assert!(
            match_not_modified(
                std::slice::from_ref(&cached_a),
                other,
                Some(LOC),
                all(),
                NOW,
                true
            )
            .is_none(),
            "304'd warehouse B's load from warehouse A's ETag"
        );
        // Same tag, its own warehouse: still a 304, so the refusal above is the
        // warehouse axis and not the window.
        assert!(
            match_not_modified(&[cached_a], wh(), Some(LOC), all(), NOW, true).is_some(),
            "the tag must still match its own warehouse"
        );
    }

    #[test]
    fn credential_load_picks_valid_etag_among_several() {
        let etags = vec![
            client_etag("s3://other/metadata.json", Some(NOW + 60_000)),
            client_etag(LOC, Some(NOW - 1)),      // passed
            client_etag(LOC, Some(NOW + 60_000)), // valid
        ];
        assert!(matches(&etags, true));
    }
}
