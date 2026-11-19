use std::{collections::BTreeMap, sync::Arc};

use http::StatusCode;
use iceberg::{NamespaceIdent, TableIdent, spec::ViewMetadata};
use iceberg_ext::catalog::rest::{LoadViewResult, RegisterViewRequest};
use lakekeeper_io::Location;

use crate::{
    WarehouseId,
    api::{
        ApiContext,
        endpoints::EndpointFlat,
        iceberg::v1::{
            DataAccessMode, NamespaceParameters, ViewParameters, views::LoadViewRequest,
        },
    },
    request_metadata::{ArcRequestMetadata, RequestMetadata},
    server::{
        io::read_metadata_file,
        maybe_get_secret, require_warehouse_id,
        tables::{parse_location, require_active_warehouse, validate_table_or_view_ident_creation},
        tabular::{authorize_register_on_warehouse, claim_idempotency_key},
        views::{create::commit_created_view, owner_property, validate_view_properties},
    },
    service::{
        ArcResolvedWarehouse, CatalogIdempotencyOps, CatalogNamespaceOps, CatalogStore,
        CatalogViewOps, CatalogWarehouseOps, NamespaceHierarchy, NamespaceId, ResolvedWarehouse,
        Result, SecretStore, State, Transaction, ViewId,
        authz::{
            Authorizer, AuthzNamespaceOps, AuthzWarehouseOps, CatalogNamespaceAction,
            CatalogWarehouseAction, GrantSpec, emit_bootstrap_grants_async,
        },
        events::{
            APIEventContext,
            context::{AuthzChecked, ResolvedNamespace, Unresolved, UserProvidedNamespace},
        },
        idempotency::{IdempotencyInfo, IdempotencyKey},
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
    let AuthorizedFile {
        event_ctx,
        warehouse,
        namespace,
        metadata,
    } = authorize_registration(
        &state,
        RegistrationTarget {
            request_metadata: Arc::new(request_metadata),
            warehouse_id,
            namespace: provided_ns,
            name: &request.name,
            metadata_location: &metadata_location,
        },
    )
    .await?;

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

    // ------------------- INSERT -------------------
    let bootstrap_grants = insert_registered_view::<C, A>(
        state.v1_state.catalog,
        &state.v1_state.authz,
        event_ctx.request_metadata(),
        RegisteredView {
            warehouse_id,
            namespace_id: namespace.namespace_id(),
            view: &view,
            metadata: &metadata,
            metadata_location: &metadata_location,
            idempotency_key,
        },
    )
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

/// A registered view to insert, at the metadata file it was registered from.
struct RegisteredView<'a> {
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    view: &'a TableIdent,
    metadata: &'a ViewMetadata,
    metadata_location: &'a Location,
    idempotency_key: Option<IdempotencyKey>,
}

/// Inserts the view and the idempotency key in one write transaction, adds the view to the
/// authorizer and commits with the bootstrap grants, which it returns. No metadata file is
/// written.
async fn insert_registered_view<C: CatalogStore, A: Authorizer>(
    catalog: C::State,
    authorizer: &A,
    request_metadata: &RequestMetadata,
    registered: RegisteredView<'_>,
) -> Result<Vec<GrantSpec>> {
    let RegisteredView {
        warehouse_id,
        namespace_id,
        view,
        metadata,
        metadata_location,
        idempotency_key,
    } = registered;
    let mut t = C::Transaction::begin_write(catalog).await?;
    C::create_view(
        warehouse_id,
        namespace_id,
        view,
        metadata,
        metadata_location,
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

    let view_id = ViewId::from(metadata.uuid());
    authorizer
        .create_view(request_metadata, warehouse_id, view_id, namespace_id)
        .await?;
    commit_created_view::<C, A>(authorizer, request_metadata, warehouse_id, view_id, t).await
}

/// The view a register call names, and the file it registers it from.
struct RegistrationTarget<'a> {
    request_metadata: ArcRequestMetadata,
    warehouse_id: WarehouseId,
    namespace: &'a NamespaceIdent,
    name: &'a str,
    metadata_location: &'a Location,
}

/// A metadata file the caller may register, with the context that recorded the allowed
/// `CreateView`.
struct AuthorizedFile {
    event_ctx:
        APIEventContext<UserProvidedNamespace, Unresolved, CatalogNamespaceAction, AuthzChecked>,
    warehouse: ArcResolvedWarehouse,
    namespace: NamespaceHierarchy,
    metadata: ViewMetadata,
}

/// Authorizes a register and reads its metadata file. `RegisterView` on the warehouse is asked
/// before the file is read. `CreateView` on the namespace is asked after it, with the view id
/// and properties from the file, and before the file's contents are validated, so a caller who
/// may not create here learns only that the file exists and parses.
async fn authorize_registration<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    state: &ApiContext<State<A, C, S>>,
    target: RegistrationTarget<'_>,
) -> Result<AuthorizedFile> {
    let authorizer = &state.v1_state.authz;
    let mut event_ctx = APIEventContext::for_namespace(
        target.request_metadata,
        state.v1_state.events.clone(),
        target.warehouse_id,
        target.namespace.clone(),
        // Preliminary: the view id and properties are only known from the metadata file.
        CatalogNamespaceAction::CreateView {
            name: Some(target.name.to_string()),
            view_id: None,
            properties: Arc::new(BTreeMap::new()),
        },
    );

    let (warehouse, namespace) = tokio::join!(
        C::get_active_warehouse_by_id(target.warehouse_id, state.v1_state.catalog.clone()),
        C::get_namespace(
            target.warehouse_id,
            target.namespace,
            state.v1_state.catalog.clone()
        )
    );
    let warehouse = authorizer
        .require_warehouse_action(
            event_ctx.request_metadata(),
            target.warehouse_id,
            warehouse,
            CatalogWarehouseAction::Use,
        )
        .await
        .map_err(|e| event_ctx.emit_early_authz_failure(e))?;
    let namespace = authorizer
        .require_namespace_action(
            event_ctx.request_metadata(),
            &warehouse,
            target.namespace,
            namespace,
            CatalogNamespaceAction::GetMetadata,
        )
        .await
        .map_err(|e| event_ctx.emit_early_authz_failure(e))?;
    authorize_register_on_warehouse(
        authorizer,
        &event_ctx,
        &warehouse,
        CatalogWarehouseAction::RegisterView,
    )
    .await?;

    require_active_warehouse(warehouse.status)?;
    let metadata = read_registered_metadata(
        &warehouse,
        target.metadata_location,
        &state.v1_state.secrets,
    )
    .await?;

    let action = CatalogNamespaceAction::CreateView {
        name: Some(target.name.to_string()),
        view_id: Some(ViewId::from(metadata.uuid())),
        properties: Arc::new(
            metadata
                .properties()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        ),
    };
    event_ctx.override_action(action.clone());
    let authz_result = authorizer
        .require_namespace_action(
            event_ctx.request_metadata(),
            &warehouse,
            target.namespace,
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
        owner_property::protected_properties(state.v1_state.authentication),
        event_ctx.request_metadata().engines(),
    )
    .map_err(|e| event_ctx.emit_late_authz_failure(e))?;

    Ok(AuthorizedFile {
        event_ctx,
        warehouse,
        namespace,
        metadata,
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
