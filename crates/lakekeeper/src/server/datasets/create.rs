use std::{collections::BTreeMap, sync::Arc};

use http::StatusCode;
use iceberg::TableIdent;
use uuid::Uuid;

use crate::{
    api::{
        ApiContext, ErrorModel,
        data::v1::datasets::{
            CreateDatasetRequest, DatasetData, DatasetParameters, LoadDatasetResponse,
        },
        endpoints::EndpointFlat,
        iceberg::v1::namespace::NamespaceParameters,
    },
    request_metadata::RequestMetadata,
    server::{require_warehouse_id, tabular::determine_tabular_location},
    service::{
        CachePolicy, CatalogDatasetOps, CatalogIdempotencyOps, CatalogStore, DatasetCreation,
        DatasetId, DatasetOwnership, Result, SecretStore, State, TabularId, Transaction,
        WarehouseId,
        authz::{Authorizer, AuthzNamespaceOps, CatalogNamespaceAction},
        events::{
            APIEventContext,
            context::{ResolvedNamespace, UserProvidedNamespace},
        },
        idempotency::IdempotencyInfo,
    },
};

fn validate_create_request(request: &CreateDatasetRequest) -> Result<()> {
    if request.name.is_empty() {
        return Err(
            ErrorModel::bad_request("Dataset name cannot be empty", "InvalidName", None).into(),
        );
    }
    if request.name.contains('+') {
        return Err(ErrorModel::bad_request(
            "Dataset name cannot contain '+' character.",
            "InvalidName",
            None,
        )
        .into());
    }
    if let Some(max_file_size) = request.constraints.as_ref().and_then(|c| c.max_file_size)
        && max_file_size <= 0
    {
        return Err(ErrorModel::bad_request(
            "max-file-size must be greater than zero.",
            "InvalidConstraints",
            None,
        )
        .into());
    }
    Ok(())
}

/// Guard to ensure cleanup of authorizer resources if dataset creation fails.
///
/// Without it, a failure after the authorizer write leaves a dataset that exists in
/// the authorization store but not the catalog — invisible and ungovernable.
struct DatasetCreationGuard<A: Authorizer> {
    authorizer: A,
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    authorizer_created: bool,
}

impl<A: Authorizer> DatasetCreationGuard<A> {
    fn new(authorizer: A, warehouse_id: WarehouseId, dataset_id: DatasetId) -> Self {
        Self {
            authorizer,
            warehouse_id,
            dataset_id,
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
                .delete_dataset(self.warehouse_id, self.dataset_id)
                .await
        {
            tracing::warn!(
                "Failed to cleanup authorizer dataset {} in warehouse {} after failed transaction: {e}",
                self.dataset_id,
                self.warehouse_id
            );
        }
    }
}

pub(super) async fn create_dataset<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: NamespaceParameters,
    request: CreateDatasetRequest,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<LoadDatasetResponse> {
    let NamespaceParameters { namespace, prefix } = &parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    validate_create_request(&request)?;

    // ------------------- IDEMPOTENCY CHECK -------------------
    let idempotency_key = request_metadata.idempotency_key().copied();
    if let Some(ref key) = idempotency_key {
        let check = C::check_idempotency_key(
            warehouse_id,
            key,
            EndpointFlat::DatasetV1CreateDataset,
            state.v1_state.catalog.clone(),
        )
        .await?;
        if check.is_replay() {
            return super::load::load_dataset::<C, A, S>(
                DatasetParameters {
                    prefix: prefix.clone(),
                    namespace: namespace.clone(),
                    dataset_name: request.name.clone(),
                },
                state,
                request_metadata,
            )
            .await;
        }
    }

    let authorizer = state.v1_state.authz.clone();
    let dataset_id = DatasetId::from(Uuid::now_v7());
    let mut guard = DatasetCreationGuard::new(authorizer.clone(), warehouse_id, dataset_id);

    match create_dataset_inner::<C, A, S>(
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
async fn create_dataset_inner<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    namespace: &iceberg::NamespaceIdent,
    request: &CreateDatasetRequest,
    state: &ApiContext<State<A, C, S>>,
    request_metadata: &RequestMetadata,
    idempotency_key: Option<&crate::service::idempotency::IdempotencyKey>,
    guard: &mut DatasetCreationGuard<A>,
) -> Result<LoadDatasetResponse> {
    let warehouse_id = guard.warehouse_id;
    let dataset_id = guard.dataset_id;
    let authorizer = &state.v1_state.authz;

    // A caller-supplied location means the dataset borrows a prefix that already
    // holds data; without one Lakekeeper allocates and owns the prefix.
    let ownership = if request.location.is_some() {
        DatasetOwnership::Imported
    } else {
        DatasetOwnership::Managed
    };

    // ------------------- AUTHZ: namespace-level CreateDataset -------------------
    let action = CatalogNamespaceAction::CreateDataset {
        name: Some(request.name.clone()),
        dataset_id: Some(dataset_id),
        location: request.location.clone(),
        managed: Some(ownership.is_managed()),
        properties: Arc::new(BTreeMap::new()),
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
    let tabular_id = TabularId::Dataset(dataset_id);
    let table_ident = TableIdent::new(namespace.clone(), request.name.clone());

    let location = determine_tabular_location(
        &ns_hierarchy,
        request.location.clone(),
        tabular_id,
        &table_ident,
        &warehouse.storage_profile,
    )?;

    let creation = DatasetCreation {
        dataset_id,
        namespace_id,
        warehouse_id: warehouse.warehouse_id,
        name: request.name.clone(),
        location,
        ownership,
        constraints: request.constraints.clone().unwrap_or_default(),
    };

    let mut t = C::Transaction::begin_write(state.v1_state.catalog.clone()).await?;
    let info = C::create_dataset(creation, t.transaction()).await?;

    // Create in authorizer
    authorizer
        .create_dataset(
            request_metadata,
            warehouse.warehouse_id,
            info.dataset_id,
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
                .endpoint(EndpointFlat::DatasetV1CreateDataset)
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

    let info = Arc::new(info);
    let response = LoadDatasetResponse {
        dataset: DatasetData {
            name: info.name.clone(),
            id: info.dataset_id,
            location: info.location.to_string(),
            ownership: info.ownership,
            protected: info.protected,
            constraints: (!info.constraints.is_empty()).then(|| info.constraints.clone()),
        },
    };

    event_ctx.emit_dataset_created_async(info, Arc::new(request.clone()));

    Ok(response)
}
