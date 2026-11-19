use std::{collections::BTreeMap, sync::Arc};

use http::StatusCode;
use iceberg::{TableIdent, spec::ViewMetadata};
use iceberg_ext::catalog::rest::{LoadViewResult, RegisterViewRequest};
use lakekeeper_io::Location;

use crate::{
    api::{
        ApiContext,
        endpoints::EndpointFlat,
        iceberg::v1::{
            DataAccessMode, NamespaceParameters, ViewParameters, views::LoadViewRequest,
        },
    },
    request_metadata::RequestMetadata,
    server::{
        io::read_metadata_file,
        maybe_get_secret, require_warehouse_id,
        tables::{parse_location, require_active_warehouse, validate_table_or_view_ident_creation},
        tabular::claim_idempotency_key,
        views::{create::commit_created_view, owner_property, validate_view_properties},
    },
    service::{
        CatalogIdempotencyOps, CatalogNamespaceOps, CatalogStore, CatalogViewOps,
        CatalogWarehouseOps, ResolvedWarehouse, Result, SecretStore, State, Transaction, ViewId,
        authz::{
            Authorizer, AuthzNamespaceOps, AuthzWarehouseOps, CatalogNamespaceAction,
            CatalogWarehouseAction, emit_bootstrap_grants_async,
        },
        events::{APIEventContext, context::ResolvedNamespace},
        idempotency::IdempotencyInfo,
    },
};

/// Register a view in the given namespace from an existing view metadata file. The view is
/// tracked at that file; no metadata file is written.
pub async fn register_view<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: NamespaceParameters,
    request: RegisterViewRequest,
    state: ApiContext<State<A, C, S>>,
    data_access: impl Into<DataAccessMode>,
    request_metadata: RequestMetadata,
) -> Result<LoadViewResult> {
    // ------------------- VALIDATIONS -------------------
    let NamespaceParameters {
        namespace: provided_ns,
        prefix,
    } = &parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    let view = TableIdent::new(provided_ns.clone(), request.name.clone());
    validate_table_or_view_ident_creation(&view)?;
    let metadata_location = parse_location(&request.metadata_location, StatusCode::BAD_REQUEST)?;

    // ------------------- IDEMPOTENCY CHECK -------------------
    let idempotency_key = request_metadata.idempotency_key().copied();
    if let Some(ref key) = idempotency_key {
        let check = C::check_idempotency_key(
            warehouse_id,
            key,
            EndpointFlat::CatalogV1RegisterView,
            state.v1_state.catalog.clone(),
        )
        .await?;
        if check.is_replay() {
            return super::load::load_view::<C, A, S>(
                ViewParameters {
                    prefix: parameters.prefix.clone(),
                    view,
                },
                LoadViewRequest {
                    data_access: data_access.into(),
                    referenced_by: None,
                },
                state,
                request_metadata,
            )
            .await;
        }
    }

    // ------------------- AUTHZ -------------------
    let authorizer = state.v1_state.authz.clone();
    let protected_properties = owner_property::protected_properties(state.v1_state.authentication);
    let event_ctx = APIEventContext::for_namespace(
        Arc::new(request_metadata),
        state.v1_state.events.clone(),
        warehouse_id,
        provided_ns.clone(),
        // Preliminary: the view id and properties are only known from the metadata file.
        CatalogNamespaceAction::CreateView {
            name: Some(request.name.clone()),
            view_id: None,
            properties: Arc::new(BTreeMap::new()),
        },
    );

    let (warehouse, namespace) = tokio::join!(
        C::get_active_warehouse_by_id(warehouse_id, state.v1_state.catalog.clone()),
        C::get_namespace(warehouse_id, provided_ns, state.v1_state.catalog.clone())
    );
    let warehouse = authorizer
        .require_warehouse_action(
            event_ctx.request_metadata(),
            warehouse_id,
            warehouse,
            CatalogWarehouseAction::Use,
        )
        .await
        .map_err(|e| event_ctx.emit_early_authz_failure(e))?;
    let namespace = authorizer
        .require_namespace_action(
            event_ctx.request_metadata(),
            &warehouse,
            provided_ns,
            namespace,
            CatalogNamespaceAction::GetMetadata,
        )
        .await
        .map_err(|e| event_ctx.emit_early_authz_failure(e))?;

    // ------------------- READ THE METADATA FILE -------------------
    require_active_warehouse(warehouse.status)?;
    let metadata =
        read_registered_metadata(&warehouse, &metadata_location, &state.v1_state.secrets).await?;
    let view_id = ViewId::from(metadata.uuid());

    // Checked before the file's contents are validated, so a caller who may not create here
    // learns only that the file exists and parses.
    let action = CatalogNamespaceAction::CreateView {
        name: Some(request.name.clone()),
        view_id: Some(view_id),
        properties: Arc::new(
            metadata
                .properties()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        ),
    };
    let mut event_ctx = event_ctx;
    event_ctx.override_action(action.clone());
    let authz_result = authorizer
        .require_namespace_action(
            event_ctx.request_metadata(),
            &warehouse,
            provided_ns,
            Ok(Some(namespace)),
            action,
        )
        .await;
    let (event_ctx, namespace) = event_ctx.emit_authz(authz_result)?;

    // Security: an engine owner property (e.g. `trino.run-as-owner`) names the user an engine
    // runs the view as. Registering writes the file's properties as a create writes the
    // request's, so the same rule applies.
    owner_property::check_property_writes(
        metadata.properties().keys().map(String::as_str),
        protected_properties,
        event_ctx.request_metadata().engines(),
    )
    .map_err(|e| event_ctx.emit_late_authz_failure(e))?;

    // ------------------- VALIDATE THE FILE -------------------
    let view_location = parse_location(metadata.location(), StatusCode::BAD_REQUEST)?;
    validate_view_properties(metadata.properties().keys())?;
    warehouse
        .storage_profile
        .require_allowed_location(&view_location)?;

    let event_ctx = event_ctx.resolve(ResolvedNamespace {
        warehouse: warehouse.clone(),
        namespace: namespace.namespace.clone(),
    });
    let request_metadata = event_ctx.request_metadata();
    let namespace_id = namespace.namespace_id();

    // ------------------- INSERT -------------------
    let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
    C::create_view(
        warehouse_id,
        namespace_id,
        &view,
        &metadata,
        &metadata_location,
        t.transaction(),
    )
    .await?;
    let idempotency = idempotency_key.map(|key| {
        IdempotencyInfo::builder()
            .key(key)
            .endpoint(EndpointFlat::CatalogV1RegisterView)
            .http_status(StatusCode::OK)
            .build()
    });
    let t = claim_idempotency_key::<C>(t, warehouse_id, idempotency.as_ref()).await?;

    authorizer
        .create_view(request_metadata, warehouse_id, view_id, namespace_id)
        .await?;
    let bootstrap_grants =
        commit_created_view::<C, A>(&authorizer, request_metadata, warehouse_id, view_id, t)
            .await?;

    emit_bootstrap_grants_async(
        event_ctx.dispatcher(),
        event_ctx.request_metadata_arc(),
        bootstrap_grants,
    );

    Ok(LoadViewResult {
        metadata_location: metadata_location.to_string(),
        metadata: Arc::new(metadata),
        // Views carry no storage access: engines load the tables behind a view with loadTable.
        config: None,
        labels: None,
    })
}

/// Reads the view metadata file at `metadata_location`, which must be inside the warehouse's
/// storage. It may be anywhere there, also outside the namespace's location.
async fn read_registered_metadata<S: SecretStore>(
    warehouse: &ResolvedWarehouse,
    metadata_location: &Location,
    secrets: &S,
) -> Result<ViewMetadata> {
    let storage_profile = &warehouse.storage_profile;
    storage_profile.require_allowed_location(metadata_location)?;
    let storage_secret = maybe_get_secret(warehouse.storage_secret_id, secrets).await?;
    let file_io = storage_profile.file_io(storage_secret.as_deref()).await?;
    Ok(read_metadata_file(&file_io, metadata_location).await?)
}
