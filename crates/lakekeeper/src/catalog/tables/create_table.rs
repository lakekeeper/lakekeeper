use std::sync::Arc;

use iceberg::spec::{
    FormatVersion, SortOrder, TableMetadata, TableMetadataBuilder, UnboundPartitionSpec,
    PROPERTY_FORMAT_VERSION,
};
use iceberg_ext::catalog::rest::StorageCredential;
use lakekeeper_io::{InvalidLocationError, LakekeeperStorage as _, Location, StorageBackend};
use uuid::Uuid;

use super::{
    super::{io::write_file, namespace::authorized_namespace_ident_to_id, require_warehouse_id},
    require_active_warehouse, validate_table_or_view_ident, validate_table_properties,
};
use crate::{
    api::iceberg::v1::{
        tables::DataAccessMode, ApiContext, CreateTableRequest, ErrorModel, LoadTableResult,
        NamespaceParameters, Result, TableIdent,
    },
    catalog::{compression_codec::CompressionCodec, tabular::determine_tabular_location},
    request_metadata::RequestMetadata,
    service::{
        authz::{Authorizer, CatalogNamespaceAction},
        secrets::SecretStore,
        storage::{StorageLocations as _, StoragePermissions, ValidationError},
        Catalog, CreateTableResponse, State, TableCreation, TableId, TabularId, Transaction,
    },
    WarehouseId,
};

/// Guard to ensure cleanup of resources if table creation fails
struct TableCreationGuard<A: Authorizer> {
    authorizer: A,
    warehouse_id: WarehouseId,
    table_id: TableId,
    metadata_location: Option<(StorageBackend, Location)>,
    authorizer_created: bool,
}

impl<A: Authorizer> TableCreationGuard<A> {
    fn new(authorizer: A, warehouse_id: WarehouseId, table_id: TableId) -> Self {
        Self {
            authorizer,
            warehouse_id,
            table_id,
            metadata_location: None,
            authorizer_created: false,
        }
    }

    fn mark_metadata_written(&mut self, io: StorageBackend, location: Location) {
        self.metadata_location = Some((io, location));
    }

    fn mark_authorizer_created(&mut self) {
        self.authorizer_created = true;
    }

    fn success(&mut self) {
        self.metadata_location = None;
        self.authorizer_created = false;
    }

    async fn cleanup(&mut self) {
        if self.authorizer_created {
            if let Err(e) = self
                .authorizer
                .delete_table(self.warehouse_id, self.table_id)
                .await
            {
                tracing::warn!("Failed to cleanup authorizer table {} in warehouse {} after failed transaction: {e}", self.table_id, self.warehouse_id);
            }
        }

        if let Some((io, metadata_location)) = self.metadata_location.take() {
            if let Err(e) = io.delete(&metadata_location).await {
                tracing::warn!(
                    "Failed to cleanup metadata file at {metadata_location} after failed transaction: {e}",
                );
            }
        }
    }
}

/// Load a table from the catalog
pub(super) async fn create_table<C: Catalog, A: Authorizer + Clone, S: SecretStore>(
    parameters: NamespaceParameters,
    // mut because we need to change location
    request: CreateTableRequest,
    data_access: impl Into<DataAccessMode> + Send,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<LoadTableResult> {
    let authorizer = state.v1_state.authz.clone();
    let warehouse_id = require_warehouse_id(parameters.prefix.clone())?;
    let table_id = TableId::from(Uuid::now_v7());

    let mut guard = TableCreationGuard::new(authorizer.clone(), warehouse_id, table_id);

    match create_table_inner(
        parameters,
        request,
        data_access,
        state,
        request_metadata,
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

/// Inner function that performs the actual table creation logic
#[allow(clippy::too_many_lines)]
async fn create_table_inner<C: Catalog, A: Authorizer + Clone, S: SecretStore>(
    parameters: NamespaceParameters,
    // mut because we need to change location
    mut request: CreateTableRequest,
    data_access: impl Into<DataAccessMode> + Send,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
    guard: &mut TableCreationGuard<A>,
) -> Result<LoadTableResult> {
    let data_access = data_access.into();
    // ------------------- VALIDATIONS -------------------
    let NamespaceParameters { namespace, prefix } = parameters.clone();
    let warehouse_id = require_warehouse_id(prefix.clone())?;
    let table = TableIdent::new(namespace.clone(), request.name.clone());
    validate_table_or_view_ident(&table)?;

    if let Some(properties) = &request.properties {
        validate_table_properties(properties.keys())?;
    }

    // ------------------- AUTHZ -------------------
    let authorizer = state.v1_state.authz.clone();
    let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
    let namespace_id = authorized_namespace_ident_to_id::<C, _>(
        authorizer.clone(),
        &request_metadata,
        &warehouse_id,
        &namespace,
        CatalogNamespaceAction::CanCreateTable,
        t.transaction(),
    )
    .await?;

    // ------------------- BUSINESS LOGIC -------------------
    let id = Uuid::now_v7();
    let tabular_id = TabularId::Table(id.into());
    let table_id = TableId::from(id);

    let namespace = C::get_namespace(warehouse_id, namespace_id, t.transaction()).await?;
    let warehouse = C::require_warehouse(warehouse_id, t.transaction()).await?;
    let storage_profile = &warehouse.storage_profile;
    require_active_warehouse(warehouse.status)?;

    let table_location = determine_tabular_location(
        &namespace,
        request.location.clone(),
        tabular_id,
        storage_profile,
    )?;

    // Update the request for event
    request.location = Some(table_location.to_string());
    let request = request; // Make it non-mutable again for our sanity

    // If stage-create is true, we should not create the metadata file
    let metadata_location = if request.stage_create.unwrap_or(false) {
        None
    } else {
        let metadata_id = Uuid::now_v7();
        Some(storage_profile.default_metadata_location(
            &table_location,
            &CompressionCodec::try_from_maybe_properties(request.properties.as_ref())?,
            metadata_id,
            0,
        ))
    };

    let table_metadata = create_table_request_into_table_metadata(table_id, request.clone())?;

    let CreateTableResponse {
        table_metadata,
        staged_table_id,
    } = C::create_table(
        TableCreation {
            warehouse_id: warehouse.id,
            namespace_id: namespace.namespace_id,
            table_ident: &table,
            table_metadata,
            metadata_location: metadata_location.as_ref(),
        },
        t.transaction(),
    )
    .await?;

    // We don't commit the transaction yet, first we need to write the metadata file.
    let storage_secret = if let Some(secret_id) = warehouse.storage_secret_id {
        let secret_state = state.v1_state.secrets;
        Some(secret_state.get_secret_by_id(secret_id).await?.secret)
    } else {
        None
    };

    let file_io = storage_profile.file_io(storage_secret.as_ref()).await?;
    if !crate::service::storage::is_empty(&file_io, &table_location).await? {
        return Err(ValidationError::from(InvalidLocationError::new(
            table_location.to_string(),
            "Unexpected files in location, tabular locations have to be empty",
        ))
        .into());
    }

    if let Some(metadata_location) = &metadata_location {
        let compression_codec = CompressionCodec::try_from_metadata(&table_metadata)?;
        write_file(
            &file_io,
            metadata_location,
            &table_metadata,
            compression_codec,
        )
        .await?;

        guard.mark_metadata_written(file_io, metadata_location.clone());
    }

    // This requires the storage secret
    // because the table config might contain vended-credentials based
    // on the `data_access` parameter.
    let config = storage_profile
        .generate_table_config(
            data_access,
            storage_secret.as_ref(),
            &table_location,
            StoragePermissions::ReadWriteDelete,
            &request_metadata,
            warehouse_id,
            table_id.into(),
        )
        .await?;

    let storage_credentials = (!config.creds.inner().is_empty()).then(|| {
        vec![StorageCredential {
            prefix: table_location.to_string(),
            config: config.creds.into(),
        }]
    });

    let load_table_result = LoadTableResult {
        metadata_location: metadata_location.as_ref().map(ToString::to_string),
        metadata: table_metadata.clone(),
        config: Some(config.config.into()),
        storage_credentials,
    };

    // Create table in authorizer
    authorizer
        .create_table(&request_metadata, warehouse_id, table_id, namespace_id)
        .await?;

    guard.mark_authorizer_created();

    // Commit transaction
    t.commit().await?;

    // If a staged table was overwritten, delete it from authorizer
    if let Some(staged_table_id) = staged_table_id {
        authorizer
            .delete_table(warehouse_id, staged_table_id)
            .await
            .ok();
    }

    state
        .v1_state
        .hooks
        .create_table(
            warehouse_id,
            parameters,
            Arc::new(request),
            Arc::new(table_metadata),
            metadata_location.map(Arc::new),
            data_access,
            Arc::new(request_metadata),
        )
        .await;

    Ok(load_table_result)
}

pub(crate) fn create_table_request_into_table_metadata(
    table_id: TableId,
    request: CreateTableRequest,
) -> Result<TableMetadata> {
    let CreateTableRequest {
        name: _,
        location,
        schema,
        partition_spec,
        write_order,
        // Stage-create is already handled in the catalog service.
        // If stage-create is true, the metadata_location is None,
        // otherwise, it is the location of the metadata file.
        stage_create: _,
        mut properties,
    } = request;

    let location = location.ok_or_else(|| {
        ErrorModel::conflict(
            "Table location is required",
            "CreateTableLocationRequired",
            None,
        )
    })?;

    let format_version = properties
        .as_mut()
        .and_then(|props| props.remove(PROPERTY_FORMAT_VERSION))
        .map(|s| match s.as_str() {
            "v1" | "1" => Ok(FormatVersion::V1),
            "v2" | "2" => Ok(FormatVersion::V2),
            "v3" | "3" => Ok(FormatVersion::V3),
            _ => Err(ErrorModel::bad_request(
                format!("Invalid format version specified in table_properties: {s}"),
                "InvalidFormatVersion",
                None,
            )),
        })
        .transpose()?
        .unwrap_or(FormatVersion::V2);

    let table_metadata = TableMetadataBuilder::new(
        schema,
        partition_spec.unwrap_or(UnboundPartitionSpec::builder().build()),
        write_order.unwrap_or(SortOrder::unsorted_order()),
        location,
        format_version,
        properties.unwrap_or_default(),
    )
    .map_err(|e| {
        let msg = e.message().to_string();
        ErrorModel::bad_request(msg, "CreateTableMetadataError", Some(Box::new(e)))
    })?
    .assign_uuid(*table_id)
    .build()
    .map_err(|e| {
        let msg = e.message().to_string();
        ErrorModel::bad_request(msg, "BuildTableMetadataError", Some(Box::new(e)))
    })?
    .metadata;

    Ok(table_metadata)
}
