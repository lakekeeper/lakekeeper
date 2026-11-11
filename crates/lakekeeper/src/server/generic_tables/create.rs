use std::{collections::BTreeMap, sync::Arc};

use http::StatusCode;
use iceberg::TableIdent;
use lakekeeper_io::Location;
use uuid::Uuid;

use crate::{
    api::{
        ApiContext, ErrorModel, IcebergErrorResponse,
        data::v1::generic_tables::{
            CreateGenericTableRequest, GenericTableData, GenericTableParameters,
            LoadGenericTableResponse,
        },
        endpoints::EndpointFlat,
        iceberg::v1::namespace::NamespaceParameters,
    },
    request_metadata::{ArcRequestMetadata, RequestMetadata},
    server::{
        maybe_get_secret, require_warehouse_id,
        tables::validate_new_tabular_name,
        tabular::{claim_idempotency_key, determine_tabular_location, location_is_empty},
    },
    service::{
        COMMIT_STATE_UNKNOWN_ERROR_TYPE, CachePolicy, CatalogGenericTableOps,
        CatalogIdempotencyOps, CatalogStore, CatalogTabularOps, GenericTableAlreadyExists,
        GenericTableCreation, GenericTableId, ResolvedWarehouse, Result, SecretStore, State,
        TabularId, TabularIdentBorrowed, TabularListFlags, Transaction, WarehouseId,
        authz::{
            Authorizer, AuthzNamespaceOps, CatalogNamespaceAction, CatalogWarehouseAction,
            GrantResource, emit_bootstrap_grants_async, require_file_takeover,
            write_bootstrap_grants,
        },
        events::{
            APIEventContext, EventDispatcher,
            context::{ResolvedNamespace, UserProvidedNamespace},
        },
        idempotency::IdempotencyInfo,
    },
};

const MAX_FORMAT_LEN: usize = 64;
const MAX_BLOB_BYTES: usize = 1024 * 1024;

fn validate_create_request(request: &CreateGenericTableRequest) -> Result<()> {
    if request.name.is_empty() {
        return Err(ErrorModel::bad_request(
            "Generic table name cannot be empty",
            "InvalidName",
            None,
        )
        .into());
    }
    validate_new_tabular_name(&request.name)?;
    validate_format(request.format.as_str())?;
    validate_blob_size("schema", request.schema.as_ref())?;
    validate_blob_size("statistics", request.statistics.as_ref())?;
    Ok(())
}

fn validate_format(format: &str) -> Result<()> {
    let mut chars = format.chars();
    let first_ok = chars.next().is_some_and(|c| c.is_ascii_lowercase());
    let rest_ok =
        chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-');
    if !first_ok || !rest_ok || format.len() > MAX_FORMAT_LEN {
        return Err(ErrorModel::bad_request(
            format!(
                "Generic table format must start with a lowercase letter, contain only lowercase \
                 letters, digits, '_' or '-', and be at most {MAX_FORMAT_LEN} characters."
            ),
            "InvalidFormat",
            None,
        )
        .into());
    }
    Ok(())
}

fn validate_blob_size(field: &str, value: Option<&serde_json::Value>) -> Result<()> {
    let Some(value) = value else { return Ok(()) };
    let len = serde_json::to_string(value).map_or(usize::MAX, |s| s.len());
    if len > MAX_BLOB_BYTES {
        return Err(ErrorModel::bad_request(
            format!(
                "Generic table {field} payload of {len} bytes exceeds the {MAX_BLOB_BYTES}-byte limit."
            ),
            "PayloadTooLarge",
            None,
        )
        .into());
    }
    Ok(())
}

/// Guard to ensure cleanup of authorizer resources if generic table creation fails.
struct GenericTableCreationGuard<A: Authorizer> {
    authorizer: A,
    warehouse_id: WarehouseId,
    generic_table_id: GenericTableId,
    authorizer_created: bool,
}

impl<A: Authorizer> GenericTableCreationGuard<A> {
    fn new(authorizer: A, warehouse_id: WarehouseId, generic_table_id: GenericTableId) -> Self {
        Self {
            authorizer,
            warehouse_id,
            generic_table_id,
            authorizer_created: false,
        }
    }

    fn mark_authorizer_created(&mut self) {
        self.authorizer_created = true;
    }

    fn success(&mut self) {
        self.authorizer_created = false;
    }

    /// Remove what was prepared before `error`. Keeps it if the commit may have been applied.
    async fn cleanup(&mut self, error: &IcebergErrorResponse) {
        if error.error.r#type == COMMIT_STATE_UNKNOWN_ERROR_TYPE {
            tracing::warn!(
                "Creation of generic table {} in warehouse {} may have been committed, skipping cleanup",
                self.generic_table_id,
                self.warehouse_id
            );
            return;
        }

        if self.authorizer_created
            && let Err(e) = self
                .authorizer
                .delete_generic_table(self.warehouse_id, self.generic_table_id)
                .await
        {
            tracing::warn!(
                "Failed to cleanup authorizer generic table {} in warehouse {} after failed transaction: {e}",
                self.generic_table_id,
                self.warehouse_id
            );
        }
    }
}

#[allow(clippy::too_many_lines)]
pub(super) async fn create_generic_table<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: NamespaceParameters,
    request: CreateGenericTableRequest,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<LoadGenericTableResponse> {
    let NamespaceParameters { namespace, prefix } = &parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    validate_create_request(&request)?;

    // ------------------- IDEMPOTENCY CHECK -------------------
    let idempotency_key = request_metadata.idempotency_key().copied();
    if let Some(ref key) = idempotency_key {
        let check = C::check_idempotency_key(
            warehouse_id,
            key,
            EndpointFlat::GenericTableV1CreateGenericTable,
            state.v1_state.catalog.clone(),
        )
        .await?;
        if check.is_replay() {
            return super::load::load_generic_table::<C, A, S>(
                GenericTableParameters {
                    prefix: prefix.clone(),
                    namespace: namespace.clone(),
                    table_name: request.name.clone(),
                },
                state,
                crate::api::iceberg::v1::DataAccessMode::ClientManaged,
                request_metadata,
            )
            .await;
        }
    }

    let authorizer = state.v1_state.authz.clone();
    let generic_table_id = GenericTableId::from(Uuid::now_v7());
    let mut guard =
        GenericTableCreationGuard::new(authorizer.clone(), warehouse_id, generic_table_id);

    match create_generic_table_inner::<C, A, S>(
        namespace,
        &request,
        &state,
        &request_metadata,
        idempotency_key.as_ref(),
        &mut guard,
    )
    .await
    {
        Ok(result) => {
            guard.success();
            Ok(result)
        }
        Err(e) => {
            guard.cleanup(&e).await;
            Err(e)
        }
    }
}

/// Creating on a location that already holds files brings them into the catalog, as registering
/// a table does, so it needs `RegisterTable` on the warehouse. Asked before the write
/// transaction opens, because an authorizer may read the catalog.
async fn authorize_location_with_files<C: CatalogStore, A: Authorizer, S: SecretStore>(
    state: &ApiContext<State<A, C, S>>,
    (request_metadata, dispatcher): (ArcRequestMetadata, EventDispatcher),
    warehouse: &ResolvedWarehouse,
    location: &Location,
    table_ident: &TableIdent,
) -> Result<()> {
    let storage_secret =
        maybe_get_secret(warehouse.storage_secret_id, &state.v1_state.secrets).await?;
    let file_io = warehouse
        .storage_profile
        .file_io(storage_secret.as_deref())
        .await?;
    if location_is_empty(&file_io, location).await? {
        return Ok(());
    }
    let event_ctx = APIEventContext::for_warehouse(
        request_metadata,
        dispatcher,
        warehouse.warehouse_id,
        CatalogWarehouseAction::RegisterTable,
    );
    let authz_result = require_file_takeover(
        &state.v1_state.authz,
        event_ctx.request_metadata(),
        warehouse,
    )
    .await;
    let Err(refusal) = event_ctx.emit_authz(authz_result) else {
        return Ok(());
    };
    // The insert refuses a name in use with 409 whatever the decision, so a re-create gets that.
    let same_name = [
        TabularIdentBorrowed::Table(table_ident),
        TabularIdentBorrowed::View(table_ident),
        TabularIdentBorrowed::GenericTable(table_ident),
    ];
    let name_in_use = C::get_tabular_infos_by_ident(
        warehouse.warehouse_id,
        &same_name,
        TabularListFlags::active_and_staged(),
        state.v1_state.catalog.clone(),
    )
    .await
    .is_ok_and(|found| !found.is_empty());
    if name_in_use {
        return Err(ErrorModel::from(GenericTableAlreadyExists::new()).into());
    }
    Err(refusal.into())
}

#[allow(clippy::too_many_lines)]
async fn create_generic_table_inner<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    namespace: &iceberg::NamespaceIdent,
    request: &CreateGenericTableRequest,
    state: &ApiContext<State<A, C, S>>,
    request_metadata: &RequestMetadata,
    idempotency_key: Option<&crate::service::idempotency::IdempotencyKey>,
    guard: &mut GenericTableCreationGuard<A>,
) -> Result<LoadGenericTableResponse> {
    let warehouse_id = guard.warehouse_id;
    let generic_table_id = guard.generic_table_id;
    let authorizer = &state.v1_state.authz;

    // ------------------- AUTHZ: namespace-level CreateGenericTable -------------------
    let action = CatalogNamespaceAction::CreateGenericTable {
        name: Some(request.name.clone()),
        generic_table_id: Some(generic_table_id),
        format: Some(request.format.to_string()),
        base_location: request.base_location.clone(),
        properties: Arc::new(
            request
                .properties
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<BTreeMap<_, _>>(),
        ),
    };

    let event_ctx = APIEventContext::for_namespace(
        Arc::new(request_metadata.clone()),
        state.v1_state.events.clone(),
        warehouse_id,
        namespace.clone(),
        action.clone(),
    );

    let (event_ctx, (warehouse, ns_hierarchy)) = event_ctx.emit_authz(
        authorizer
            .load_and_authorize_namespace_action::<C>(
                request_metadata,
                UserProvidedNamespace::new(warehouse_id, namespace.clone()),
                action,
                CachePolicy::Use,
                state.v1_state.catalog.clone(),
            )
            .await,
    )?;

    let event_ctx = event_ctx.resolve(ResolvedNamespace {
        warehouse: warehouse.clone(),
        namespace: ns_hierarchy.namespace.clone(),
    });

    let namespace_id = ns_hierarchy.namespace.namespace_id();

    // ------------------- BUSINESS LOGIC -------------------
    let tabular_id = TabularId::GenericTable(generic_table_id);
    let table_ident = TableIdent::new(namespace.clone(), request.name.clone());

    let location = determine_tabular_location(
        &ns_hierarchy,
        request.base_location.clone(),
        tabular_id,
        &table_ident,
        &warehouse.storage_profile,
    )?;
    authorize_location_with_files(
        state,
        (
            event_ctx.request_metadata_arc(),
            event_ctx.dispatcher().clone(),
        ),
        &warehouse,
        &location,
        &table_ident,
    )
    .await?;

    let creation = GenericTableCreation {
        generic_table_id,
        namespace_id,
        warehouse_id: warehouse.warehouse_id,
        name: request.name.clone(),
        format: request.format.clone(),
        location,
        doc: request.doc.clone(),
        schema: request.schema.clone(),
        statistics: request.statistics.clone(),
        properties: request.properties.clone(),
    };

    let mut t = C::Transaction::begin_write(state.v1_state.catalog.clone()).await?;
    let info = C::create_generic_table(creation, t.transaction()).await?;

    // Create in authorizer
    authorizer
        .create_generic_table(
            request_metadata,
            warehouse.warehouse_id,
            info.generic_table_id,
            namespace_id,
        )
        .await?;
    guard.mark_authorizer_created();

    let bootstrap_grants = write_bootstrap_grants::<C, A>(
        authorizer,
        request_metadata,
        &GrantResource::GenericTable {
            warehouse_id,
            generic_table_id: info.generic_table_id,
        },
        t.transaction(),
    )
    .await?;

    let idempotency = idempotency_key.map(|key| {
        IdempotencyInfo::builder()
            .key(*key)
            .endpoint(EndpointFlat::GenericTableV1CreateGenericTable)
            .http_status(StatusCode::OK)
            .build()
    });
    let t = claim_idempotency_key::<C>(t, warehouse_id, idempotency.as_ref()).await?;
    t.commit().await?;

    // Held across the create event below, which consumes the context.
    let grant_dispatcher = event_ctx.dispatcher().clone();
    let grant_request_metadata = event_ctx.request_metadata_arc();

    let info = Arc::new(info);
    let response = LoadGenericTableResponse {
        table: GenericTableData {
            name: info.name.clone(),
            format: info.format.clone(),
            base_location: info.location.to_string(),
            protected: info.protected,
            doc: info.doc.clone(),
            properties: info.properties.clone(),
            schema: info.schema.clone(),
            statistics: info.statistics.clone(),
        },
        config: None,
        storage_credentials: None,
    };

    event_ctx.emit_generic_table_created_async(info, Arc::new(request.clone()));

    emit_bootstrap_grants_async(&grant_dispatcher, grant_request_metadata, bootstrap_grants);

    Ok(response)
}
