use std::{collections::BTreeMap, sync::Arc};

use http::StatusCode;
use iceberg::TableIdent;
use uuid::Uuid;

use crate::{
    api::{
        ApiContext, ErrorModel,
        endpoints::EndpointFlat,
        iceberg::v1::namespace::NamespaceParameters,
        v1::generic_tables::{
            CreateGenericTableRequest, GenericTableData, GenericTableParameters,
            LoadGenericTableResponse,
        },
    },
    request_metadata::RequestMetadata,
    server::{require_warehouse_id, tabular::determine_tabular_location},
    service::{
        CachePolicy, CatalogGenericTableOps, CatalogIdempotencyOps, CatalogStore,
        GenericTableCreation, GenericTableId, Result, SecretStore, State, TabularId, Transaction,
        WarehouseId,
        authz::{Authorizer, AuthzNamespaceOps, CatalogNamespaceAction},
        events::{
            APIEventContext,
            context::{ResolvedNamespace, UserProvidedNamespace},
        },
        idempotency::IdempotencyInfo,
    },
};

fn validate_create_request(request: &CreateGenericTableRequest) -> Result<()> {
    if request.name.is_empty() {
        return Err(ErrorModel::bad_request(
            "Generic table name cannot be empty",
            "InvalidName",
            None,
        )
        .into());
    }
    if request.name.contains('+') {
        return Err(ErrorModel::bad_request(
            "Generic table name cannot contain '+' character.",
            "InvalidName",
            None,
        )
        .into());
    }
    if request.format.as_str().is_empty() {
        return Err(ErrorModel::bad_request(
            "Generic table format cannot be empty",
            "InvalidFormat",
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

    async fn cleanup(&mut self) {
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
        let check =
            C::check_idempotency_key(warehouse_id, key, state.v1_state.catalog.clone()).await?;
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
            guard.cleanup().await;
            Err(e)
        }
    }
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

    let _event_ctx = event_ctx.resolve(ResolvedNamespace {
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

    // Create in authorizer BEFORE commit (with guard for rollback)
    authorizer
        .create_generic_table(
            request_metadata,
            warehouse.warehouse_id,
            info.generic_table_id,
            namespace_id,
        )
        .await?;
    guard.mark_authorizer_created();

    // Insert idempotency key in the same transaction.
    if let Some(key) = idempotency_key
        && !C::try_insert_idempotency_key(
            warehouse_id,
            &IdempotencyInfo::builder()
                .key(*key)
                .endpoint(EndpointFlat::GenericTableV1CreateGenericTable)
                .http_status(StatusCode::OK)
                .build(),
            t.transaction(),
        )
        .await?
    {
        t.rollback()
            .await
            .inspect_err(|e| tracing::warn!("Rollback after idempotency conflict: {e}"))
            .ok();
        return Err(ErrorModel::request_in_progress().into());
    }

    t.commit().await?;

    Ok(LoadGenericTableResponse {
        table: GenericTableData {
            name: info.name,
            format: info.format,
            base_location: info.location.to_string(),
            doc: info.doc,
            properties: info.properties,
            schema: info.schema,
            statistics: info.statistics,
        },
        config: None,
        storage_credentials: None,
    })
}
