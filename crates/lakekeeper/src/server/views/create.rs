use std::sync::Arc;

use iceberg::{
    TableIdent, ViewCreation,
    spec::{ViewMetadata, ViewMetadataBuilder},
};
use iceberg_ext::catalog::rest::{CreateViewRequest, ErrorModel, LoadViewResult};
use lakekeeper_io::Location;

use crate::{
    api::{ApiContext, iceberg::v1::NamespaceParameters},
    request_metadata::RequestMetadata,
    server::{
        compression_codec::CompressionCodec,
        io::{delete_on_failure, write_file},
        maybe_get_secret, require_warehouse_id,
        tables::{require_active_warehouse, validate_table_or_view_ident_creation},
        tabular::{determine_tabular_location, file_io_if_empty},
        views::{owner_property, validate_view_properties},
    },
    service::{
        COMMIT_STATE_UNKNOWN_ERROR_TYPE, CachePolicy, CatalogStore, CatalogViewOps, Result,
        SecretStore, State, TabularId, Transaction, ViewId, WarehouseId,
        authz::{
            Authorizer, AuthzNamespaceOps, CatalogNamespaceAction, GrantResource, GrantSpec,
            emit_bootstrap_grants_async, write_bootstrap_grants,
        },
        contract_verification::ContractVerification,
        events::{
            APIEventContext,
            context::{ResolvedNamespace, UserProvidedNamespace},
        },
    },
};

// TODO: split up into smaller functions
#[allow(clippy::too_many_lines)]
/// Create a view in the given namespace.
pub async fn create_view<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: NamespaceParameters,
    request: CreateViewRequest,
    state: ApiContext<State<A, C, S>>,
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
    validate_view_properties(request.properties.keys())?;

    if request.view_version.representations().is_empty() {
        return Err(ErrorModel::bad_request(
            "View must have at least one representation.",
            "EmptyView",
            None,
        )
        .into());
    }

    let action = CatalogNamespaceAction::CreateView {
        name: Some(request.name.clone()),
        properties: Arc::new(request.properties.clone().into_iter().collect()),
    };
    let event_ctx = APIEventContext::for_namespace(
        Arc::new(request_metadata.clone()),
        state.v1_state.events,
        warehouse_id,
        provided_ns.clone(),
        action.clone(),
    );

    // Security: an engine owner property (e.g. `trino.run-as-owner`) names the user an engine
    // runs the view as. Only a trusted engine that owns the property may set it.
    owner_property::check_property_writes(
        request.properties.keys().map(String::as_str),
        owner_property::protected_properties(state.v1_state.authentication),
        request_metadata.engines(),
    )
    .map_err(|e| event_ctx.emit_early_authz_failure(e))?;

    // ------------------- AUTHZ -------------------
    let authorizer = &state.v1_state.authz;
    let authz_result = authorizer
        .load_and_authorize_namespace_action::<C>(
            &request_metadata,
            UserProvidedNamespace::new(warehouse_id, provided_ns.clone()),
            action.clone(),
            CachePolicy::Use,
            state.v1_state.catalog.clone(),
        )
        .await;

    let (event_ctx, (warehouse, ns_hierarchy)) = event_ctx.emit_authz(authz_result)?;

    let event_ctx = event_ctx.resolve(ResolvedNamespace {
        warehouse: warehouse.clone(),
        namespace: ns_hierarchy.namespace.clone(),
    });

    // ------------------- BUSINESS LOGIC -------------------
    require_active_warehouse(event_ctx.resolved().warehouse.status)?;

    let view_id: TabularId = TabularId::View(uuid::Uuid::now_v7().into());

    let view_location = determine_tabular_location(
        &ns_hierarchy,
        request.location.clone(),
        view_id,
        &view,
        &warehouse.storage_profile,
    )?;

    let metadata_location = warehouse.storage_profile.default_metadata_location(
        &view_location,
        &CompressionCodec::try_from_properties(&request.properties)?,
        *view_id,
        0,
    );

    let metadata = build_view_metadata(&view, &view_location, view_id, &request)?;

    state
        .v1_state
        .contract_verifiers
        .check_create_view(&request, &metadata)
        .await?
        .into_result()?;

    let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
    C::create_view(
        warehouse_id,
        ns_hierarchy.namespace_id(),
        &view,
        &metadata,
        &metadata_location,
        t.transaction(),
    )
    .await?;

    // We don't commit the transaction yet, first we need to write the metadata file.
    let storage_secret =
        maybe_get_secret(warehouse.storage_secret_id, &state.v1_state.secrets).await?;
    let storage_secret_ref = storage_secret.as_deref();

    let file_io = file_io_if_empty(&warehouse, storage_secret_ref, &view_location).await?;
    let bootstrap_grants = delete_on_failure(&file_io, &[&metadata_location], async {
        let compression_codec = CompressionCodec::try_from_metadata(&metadata)?;
        write_file(&file_io, &metadata_location, &metadata, compression_codec).await?;
        tracing::debug!("Wrote new metadata file to: '{}'", metadata_location);

        let view_id = ViewId::from(metadata.uuid());
        authorizer
            .create_view(
                &request_metadata,
                warehouse_id,
                view_id,
                ns_hierarchy.namespace_id(),
            )
            .await?;

        commit_created_view::<C, A>(authorizer, &request_metadata, warehouse_id, view_id, t).await
    })
    .await?;

    // Held across the create event below, which consumes the context.
    let grant_dispatcher = event_ctx.dispatcher().clone();
    let grant_request_metadata = event_ctx.request_metadata_arc();

    let view_metadata = Arc::new(metadata);
    let metadata_location_str = metadata_location.to_string();
    let metadata_location = Arc::new(metadata_location);

    // Emit success event using the event context
    event_ctx.emit_view_created_async(
        view_metadata.clone(),
        metadata_location.clone(),
        view.name,
        Arc::new(CreateViewRequest {
            location: Some(view_location.to_string()),
            ..request
        }),
    );

    emit_bootstrap_grants_async(&grant_dispatcher, grant_request_metadata, bootstrap_grants);

    let load_view_result = LoadViewResult {
        metadata_location: metadata_location_str,
        metadata: view_metadata,
        // Views carry no storage access: engines load the tables behind a view with loadTable.
        config: None,
        labels: None,
    };

    Ok(load_view_result)
}

/// Writes the bootstrap grants of a created view and commits its transaction. If that fails,
/// removes the authorizer entry of the view, unless the commit may have been applied.
async fn commit_created_view<C: CatalogStore, A: Authorizer>(
    authorizer: &A,
    request_metadata: &RequestMetadata,
    warehouse_id: WarehouseId,
    view_id: ViewId,
    mut t: C::Transaction,
) -> Result<Vec<GrantSpec>> {
    let committed: Result<_> = async {
        let bootstrap_grants = write_bootstrap_grants::<C, A>(
            authorizer,
            request_metadata,
            &GrantResource::View {
                warehouse_id,
                view_id,
            },
            t.transaction(),
        )
        .await?;
        t.commit().await?;
        Ok(bootstrap_grants)
    }
    .await;
    // The view entry outlives a rolled-back transaction; keep it if the commit may have
    // been applied.
    if let Err(e) = &committed
        && e.error.r#type != COMMIT_STATE_UNKNOWN_ERROR_TYPE
        && let Err(e) = authorizer.delete_view(warehouse_id, view_id).await
    {
        tracing::warn!(
            %view_id,
            "Failed to remove the authorizer entry of a view that was not created: {e}"
        );
    }
    committed
}

/// Build the metadata of a new view.
///
/// # Panics
/// Panics if the resolved metadata builder fails an internal invariant (e.g.
/// uuid assignment).
fn build_view_metadata(
    view: &TableIdent,
    view_location: &Location,
    view_id: TabularId,
    request: &CreateViewRequest,
) -> Result<ViewMetadata> {
    let view_creation = ViewMetadataBuilder::from_view_creation(ViewCreation {
        name: view.name.clone(),
        location: view_location.to_string(),
        representations: request.view_version.representations().clone(),
        schema: request.schema.clone(),
        properties: request.properties.clone(),
        default_namespace: request.view_version.default_namespace().clone(),
        default_catalog: request.view_version.default_catalog().cloned(),
        summary: request.view_version.summary().clone(),
    })
    .unwrap()
    .assign_uuid(*view_id.as_ref());

    view_creation
        .build()
        .map(|result| result.metadata)
        .map_err(|e| {
            ErrorModel::bad_request(
                format!("Failed to create view metadata: {e}"),
                "ViewMetadataCreationFailed",
                Some(Box::new(e)),
            )
            .into()
        })
}
