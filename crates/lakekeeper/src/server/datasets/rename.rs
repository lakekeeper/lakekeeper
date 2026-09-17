use std::{collections::BTreeMap, sync::Arc};

use crate::{
    WarehouseId,
    api::{
        data::v1::datasets::{RenameDatasetRequest, RenameDatasetTarget},
        endpoints::EndpointFlat,
        iceberg::v1::{ApiContext, ErrorModel, Prefix, Result, TableIdent},
    },
    request_metadata::RequestMetadata,
    server::{
        require_warehouse_id,
        tables::validate_table_or_view_ident,
        tabular::{
            claim_rename_idempotency_key, commit_rename_with_reparent,
            ensure_authorized_destination,
        },
    },
    service::{
        CachePolicy, CatalogDatasetOps, CatalogIdempotencyOps, CatalogNamespaceOps, CatalogStore,
        CatalogTabularOps, CatalogWarehouseOps, DatasetInfo, LoadDatasetError, NamespaceHierarchy,
        ResolvedWarehouse, SecretStore, State, TabularId, Transaction,
        authz::{
            AuthZCannotSeeDataset, AuthZDatasetOps, AuthZError, Authorizer, AuthzNamespaceOps,
            AuthzWarehouseOps, CatalogDatasetAction, CatalogNamespaceAction,
            RequireDatasetActionError, refresh_warehouse_and_namespace_if_needed,
        },
        events::{APIEventContext, context::ResolvedDataset},
    },
};

pub(super) async fn rename_dataset<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    prefix: Option<Prefix>,
    request: RenameDatasetRequest,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<()> {
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    let to_table_ident = |t: RenameDatasetTarget, code: &'static str| -> Result<TableIdent> {
        t.try_into().map_err(|e: iceberg::Error| {
            ErrorModel::bad_request(format!("Invalid {code}: {e}"), code, None).into()
        })
    };
    let source = to_table_ident(request.source.clone(), "InvalidSourceIdent")?;
    let destination = to_table_ident(request.destination.clone(), "InvalidDestinationIdent")?;
    validate_table_or_view_ident(&source)?;
    validate_table_or_view_ident(&destination)?;

    // Built before the idempotency check so a served replay can be audited.
    let idempotency_key = request_metadata.idempotency_key().copied();
    let event_ctx = APIEventContext::for_dataset(
        Arc::new(request_metadata.clone()),
        state.v1_state.events.clone(),
        warehouse_id,
        source.clone(),
        CatalogDatasetAction::Rename,
    );

    if let Some(ref key) = idempotency_key {
        let check = C::check_idempotency_key(
            warehouse_id,
            key,
            EndpointFlat::DatasetV1RenameDataset,
            state.v1_state.catalog.clone(),
        )
        .await?;
        if check.is_replay() {
            event_ctx.emit_idempotent_replay(*key);
            return Ok(());
        }
    }

    let authorizer = &state.v1_state.authz;

    let authz_result = authorize_rename_dataset::<C, A>(
        &request_metadata,
        warehouse_id,
        &source,
        &destination,
        authorizer,
        state.v1_state.catalog.clone(),
    )
    .await;

    let (event_ctx, (warehouse, destination_namespace, source_info)) =
        event_ctx.emit_authz(authz_result)?;

    let source_id = source_info.dataset_id;
    let source_namespace_id = source_info.namespace_id;
    let destination_namespace_id = destination_namespace.namespace_id();
    let event_ctx = event_ctx.resolve(ResolvedDataset {
        warehouse: warehouse.clone(),
        dataset: Arc::new(source_info),
    });

    if source == destination {
        return Ok(());
    }

    let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
    let renamed = C::rename_tabular(
        warehouse_id,
        TabularId::Dataset(source_id),
        source_namespace_id,
        destination_namespace_id,
        &source,
        &destination,
        t.transaction(),
    )
    .await?;
    ensure_authorized_destination(destination_namespace_id, renamed.namespace_id())?;

    let t = claim_rename_idempotency_key::<C>(
        t,
        warehouse_id,
        idempotency_key,
        EndpointFlat::DatasetV1RenameDataset,
    )
    .await?;

    // ------------------- AUTHZ HIERARCHY -------------------
    // Consumes the transaction: a cross-namespace rename moves the parent edge, and
    // the ordering around the commit is what keeps that fail-closed.
    commit_rename_with_reparent::<C, A>(
        t,
        authorizer,
        event_ctx.request_metadata(),
        warehouse_id,
        TabularId::Dataset(source_id),
        source_namespace_id,
        destination_namespace_id,
    )
    .await?;

    event_ctx.emit_dataset_renamed_async(destination_namespace.namespace, Arc::new(request));

    Ok(())
}

async fn authorize_rename_dataset<C: CatalogStore, A: Authorizer + Clone>(
    request_metadata: &RequestMetadata,
    warehouse_id: WarehouseId,
    source: &TableIdent,
    destination: &TableIdent,
    authorizer: &A,
    catalog_state: C::State,
) -> std::result::Result<(Arc<ResolvedWarehouse>, NamespaceHierarchy, DatasetInfo), AuthZError> {
    let (warehouse, destination_namespace, source_namespace) = tokio::join!(
        C::get_active_warehouse_by_id(warehouse_id, catalog_state.clone()),
        // Read uncached: the destination has no version anchor to detect staleness
        // against, and a stale `ident -> id` entry would outlive the request.
        // `rename_tabular` pins the destination by id regardless.
        C::get_namespace_cache_aware(
            warehouse_id,
            &destination.namespace,
            CachePolicy::Skip,
            catalog_state.clone(),
        ),
        C::get_namespace(warehouse_id, &source.namespace, catalog_state.clone()),
    );

    let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;
    let source_namespace = authorizer.require_namespace_presence(
        warehouse_id,
        source.namespace.clone(),
        source_namespace,
    )?;

    let source_namespace_id = source_namespace.namespace.namespace_id();

    let mut t = C::Transaction::begin_read(catalog_state.clone())
        .await
        .map_err(super::iceberg_err_to_authz)?;
    let source_info = match C::load_dataset(
        warehouse_id,
        source_namespace_id,
        &source.name,
        t.transaction(),
    )
    .await
    {
        Ok(info) => info,
        Err(LoadDatasetError::DatasetNotFound(_)) => {
            return Err(AuthZCannotSeeDataset::new_not_found(warehouse_id, source.clone()).into());
        }
        Err(e) => return Err(super::iceberg_err_to_authz(e)),
    };
    t.commit().await.map_err(super::iceberg_err_to_authz)?;

    let (warehouse, source_namespace) = refresh_warehouse_and_namespace_if_needed::<C, _, _>(
        &warehouse,
        source_namespace,
        &source_info,
        AuthZCannotSeeDataset::new_not_found(warehouse_id, source.clone()),
        authorizer,
        catalog_state,
    )
    .await?;

    let create_action = CatalogNamespaceAction::CreateDataset {
        name: Some(destination.name.clone()),
        dataset_id: Some(source_info.dataset_id),
        location: Some(source_info.location.to_string()),
        managed: Some(source_info.ownership.is_managed()),
        properties: Arc::new(
            source_info
                .properties
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<BTreeMap<_, _>>(),
        ),
    };

    // A rename is a create in the destination and a rename on the source; both are
    // required, so moving a dataset cannot bypass the destination's permissions.
    let user_provided_namespace = &destination.namespace;
    let (destination_namespace, source_info) = tokio::join!(
        authorizer.require_namespace_action(
            request_metadata,
            &warehouse,
            user_provided_namespace.clone(),
            destination_namespace,
            create_action,
        ),
        authorizer.require_dataset_action(
            request_metadata,
            &warehouse,
            &source_namespace,
            source.clone(),
            Ok::<_, RequireDatasetActionError>(Some(source_info)),
            CatalogDatasetAction::Rename,
        ),
    );

    let destination_namespace = destination_namespace?;
    let source_info = source_info?;

    Ok((warehouse, destination_namespace, source_info))
}
