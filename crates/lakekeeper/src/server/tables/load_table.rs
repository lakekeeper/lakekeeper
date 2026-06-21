use std::{collections::HashMap, sync::Arc};

use http::StatusCode;
use iceberg_ext::catalog::rest::{ETag, StorageCredential, TableETag};

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
        tables::{authorize_load_table, parse_location, validate_table_or_view_ident},
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
///
/// # Panics
/// May panic if internal invariants are violated (e.g., an entry expected to
/// exist in a pre-resolved map is missing).
#[allow(clippy::too_many_lines)]
pub async fn load_table<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: TableParameters,
    request: LoadTableRequest,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
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
            TabularListFlags::active(),
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
    if let Some(etag) = match_not_modified(
        &etags,
        event_ctx
            .resolved()
            .table
            .metadata_location
            .as_ref()
            .map(lakekeeper_io::Location::as_str),
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

    let storage_credentials = storage_config.as_ref().and_then(|c| {
        (!c.creds.inner().is_empty()).then(|| {
            vec![StorageCredential {
                prefix: table_location.to_string(),
                config: c.creds.clone().into(),
            }]
        })
    });
    let credentials_revalidate_after_ms = storage_config
        .as_ref()
        .and_then(|c| c.credentials_expiration_ms)
        .map(credential_revalidate_after_ms);

    let metadata_ref = Arc::new(table_metadata);
    let metadata_location_ref = metadata_location.map(Arc::new);

    event_ctx.emit_table_loaded_async(metadata_ref.clone(), metadata_location_ref.clone());

    let load_table_result = LoadTableResult {
        metadata_location: metadata_location_ref.as_ref().map(ToString::to_string),
        metadata: metadata_ref,
        config: storage_config.map(|c| c.config.into()),
        storage_credentials,
        credentials_revalidate_after_ms,
    };

    Ok(LoadTableResultOrNotModified::LoadTableResult(
        load_table_result,
    ))
}

/// Load a table from the catalog, ensuring that it is not staged
///
/// # Errors
/// Returns an error if the table is staged, if it cannot be found, or if a DB error occurs.
async fn load_table_inner<C: CatalogStore>(
    warehouse_id: WarehouseId,
    table_id: TableId,
    table_ident: &TableIdent,
    include_deleted: bool,
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
    require_not_staged(
        warehouse_id,
        table_ident.clone(),
        result.metadata_location.as_ref(),
    )?;
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
fn match_not_modified(
    client_etags: &[ETag],
    metadata_location: Option<&str>,
    now_ms: i64,
    vends_credentials: bool,
) -> Option<ETag> {
    let metadata_location = metadata_location?;
    let current = TableETag::new(metadata_location, None);

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
        if parsed.metadata_hash() != current.metadata_hash() {
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

    const LOC: &str = "s3://bucket/table/metadata.json";
    const NOW: i64 = 1_750_000_000_000;
    /// Build a client-supplied `ETag` (quotes stripped, as `parse_etags` yields).
    /// `revalidate_after` = `None` for a metadata-only cached response.
    fn client_etag(loc: &str, revalidate_after: Option<i64>) -> ETag {
        let quoted = TableETag::new(loc, revalidate_after).into_etag();
        ETag::from(quoted.as_str().trim_matches('"'))
    }

    fn matches(etags: &[ETag], vends_credentials: bool) -> bool {
        match_not_modified(etags, Some(LOC), NOW, vends_credentials).is_some()
    }

    #[test]
    fn metadata_only_load_returns_304_for_matching_etags() {
        // A metadata-only ETag and the wildcard match when this load vends no creds.
        assert!(matches(&[client_etag(LOC, None)], false));
        assert!(matches(&[ETag::from("*")], false));
    }

    #[test]
    fn unparseable_etag_triggers_reload() {
        // A pre-upgrade bare-hash ETag (or any non-`lk1` value) can't be parsed,
        // so it never yields a 304. The client reloads once and re-primes.
        let legacy = ETag::from(TableETag::new(LOC, None).metadata_hash());
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
        assert!(match_not_modified(&[ETag::from("*")], None, NOW, false).is_none());
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
                    match_not_modified(std::slice::from_ref(&etag), Some(LOC), check_now, true)
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
        let legacy = ETag::from(TableETag::new(LOC, None).metadata_hash());
        assert!(!matches(&[legacy], true));
        assert!(!matches(&[ETag::from("*")], true));
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
