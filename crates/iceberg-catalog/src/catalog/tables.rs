use std::collections::{HashMap, HashSet};
use std::str::FromStr as _;

use super::commit_tables::apply_commit;
use super::{
    io::write_metadata_file, maybe_get_secret, namespace::validate_namespace_ident,
    require_warehouse_id, CatalogServer,
};

use crate::api::iceberg::types::DropParams;
use crate::api::iceberg::v1::{
    ApiContext, CommitTableRequest, CommitTableResponse, CommitTransactionRequest,
    CreateTableRequest, DataAccess, ErrorModel, ListTablesQuery, ListTablesResponse,
    LoadTableResult, NamespaceParameters, Prefix, RegisterTableRequest, RenameTableRequest, Result,
    TableIdent, TableParameters,
};
use crate::api::management::v1::warehouse::TabularDeleteProfile;
use crate::api::management::v1::TabularType;
use crate::api::set_not_found_status_code;
use crate::catalog::compression_codec::CompressionCodec;
use crate::request_metadata::RequestMetadata;
use crate::service::authz::{CatalogNamespaceAction, CatalogTableAction, CatalogWarehouseAction};
use crate::service::contract_verification::{ContractVerification, ContractVerificationOutcome};
use crate::service::event_publisher::{CloudEventsPublisher, EventMetadata};
use crate::service::storage::{StorageLocations as _, StoragePermissions, StorageProfile};
use crate::service::task_queue::tabular_expiration_queue::TabularExpirationInput;
use crate::service::task_queue::tabular_purge_queue::TabularPurgeInput;
use crate::service::TabularIdentUuid;
use crate::service::{
    authz::Authorizer, secrets::SecretStore, Catalog, CreateTableResponse, ListFlags,
    LoadTableResponse as CatalogLoadTableResult, State, Transaction,
};
use crate::service::{
    GetNamespaceResponse, TableCommit, TableCreation, TableIdentUuid, WarehouseStatus,
};

use http::StatusCode;
use iceberg::spec::{
    MetadataLog, TableMetadataBuildResult, PROPERTY_METADATA_PREVIOUS_VERSIONS_MAX,
};
use iceberg::{NamespaceIdent, TableUpdate};
use iceberg_ext::configs::namespace::NamespaceProperties;
use iceberg_ext::configs::Location;
use serde::Serialize;
use uuid::Uuid;

const PROPERTY_METADATA_DELETE_AFTER_COMMIT_ENABLED: &str =
    "write.metadata.delete-after-commit.enabled";
const PROPERTY_METADATA_DELETE_AFTER_COMMIT_ENABLED_DEFAULT: bool = false;

#[async_trait::async_trait]
impl<C: Catalog, A: Authorizer + Clone, S: SecretStore>
    crate::api::iceberg::v1::tables::Service<State<A, C, S>> for CatalogServer<C, A, S>
{
    /// List all table identifiers underneath a given namespace
    async fn list_tables(
        parameters: NamespaceParameters,
        query: ListTablesQuery,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ListTablesResponse> {
        let return_uuids = query.return_uuids;
        // ------------------- VALIDATIONS -------------------
        let NamespaceParameters { namespace, prefix } = parameters;
        let warehouse_id = require_warehouse_id(prefix)?;
        validate_namespace_ident(&namespace)?;

        // ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz;
        authorizer
            .require_warehouse_action(
                &request_metadata,
                warehouse_id,
                &CatalogWarehouseAction::CanUse,
            )
            .await?;
        let mut t: <C as Catalog>::Transaction =
            Transaction::begin_read(state.v1_state.catalog).await?;
        let namespace_id = C::namespace_to_id(warehouse_id, &namespace, t.transaction()).await; // We can't fail before AuthZ.

        authorizer
            .require_namespace_action(
                &request_metadata,
                warehouse_id,
                namespace_id,
                &CatalogNamespaceAction::CanListTables,
            )
            .await?;

        // ------------------- BUSINESS LOGIC -------------------
        let include_staged = false;
        let include_deleted = false;
        let include_active = true;

        let tables = C::list_tables(
            warehouse_id,
            &namespace,
            ListFlags {
                include_active,
                include_staged,
                include_deleted,
            },
            t.transaction(),
            query.into(),
        )
        .await?;

        // ToDo: Better pagination with non-empty pages
        let next_page_token = tables.next_page_token;
        let (table_uuids, identifiers): (Vec<_>, Vec<_>) =
            futures::future::try_join_all(tables.tabulars.iter().map(|t| {
                authorizer.is_allowed_table_action(
                    &request_metadata,
                    warehouse_id,
                    *t.0,
                    &CatalogTableAction::CanIncludeInList,
                )
            }))
            .await?
            .into_iter()
            .zip(tables.tabulars.into_iter())
            .filter_map(|(allowed, table)| allowed.then_some((*table.0, table.1)))
            .collect();

        Ok(ListTablesResponse {
            next_page_token,
            identifiers,
            table_uuids: return_uuids.then_some(table_uuids),
        })
    }

    #[allow(clippy::too_many_lines)]
    /// Create a table in the given namespace
    async fn create_table(
        parameters: NamespaceParameters,
        // mut because we need to change location
        mut request: CreateTableRequest,
        data_access: DataAccess,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadTableResult> {
        // ------------------- VALIDATIONS -------------------
        let NamespaceParameters { namespace, prefix } = parameters;
        let warehouse_id = require_warehouse_id(prefix.clone())?;
        let table = TableIdent::new(namespace.clone(), request.name.clone());
        validate_table_or_view_ident(&table)?;

        if let Some(properties) = &request.properties {
            validate_table_properties(properties.keys())?;
        }

        // ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz;
        authorizer
            .require_warehouse_action(
                &request_metadata,
                warehouse_id,
                &CatalogWarehouseAction::CanUse,
            )
            .await?;
        let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
        let namespace_id = C::namespace_to_id(warehouse_id, &namespace, t.transaction()).await; // We can't fail before AuthZ.
        let namespace_id = authorizer
            .require_namespace_action(
                &request_metadata,
                warehouse_id,
                namespace_id,
                &CatalogNamespaceAction::CanCreateTable,
            )
            .await?;

        // ------------------- BUSINESS LOGIC -------------------
        let table_id: TabularIdentUuid = TabularIdentUuid::Table(uuid::Uuid::now_v7());

        let namespace = C::get_namespace(warehouse_id, namespace_id, t.transaction()).await?;
        let warehouse = C::require_warehouse(warehouse_id, t.transaction()).await?;
        let storage_profile = &warehouse.storage_profile;
        require_active_warehouse(warehouse.status)?;

        let table_location = determine_tabular_location(
            &namespace,
            request.location.clone(),
            TabularIdentUuid::Table(*table_id),
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
            ))
        };

        // serialize body before moving it
        let body = maybe_body_to_json(&request);

        let CreateTableResponse { table_metadata } = C::create_table(
            TableCreation {
                namespace_id: namespace.namespace_id,
                table_ident: &table,
                table_id: (*table_id).into(),
                table_location: &table_location,
                table_schema: request.schema,
                table_partition_spec: request.partition_spec,
                table_write_order: request.write_order,
                table_properties: request.properties,
                metadata_location: metadata_location.as_ref(),
            },
            t.transaction(),
        )
        .await?;

        // We don't commit the transaction yet, first we need to write the metadata file.
        let storage_secret = if let Some(secret_id) = &warehouse.storage_secret_id {
            let secret_state = state.v1_state.secrets;
            Some(secret_state.get_secret_by_id(secret_id).await?.secret)
        } else {
            None
        };

        let file_io = storage_profile.file_io(storage_secret.as_ref())?;

        crate::service::storage::check_location_is_empty(
            &file_io,
            &table_location,
            storage_profile,
            || crate::service::storage::ValidationError::InvalidLocation {
                reason: "Unexpected files in location, tabular locations have to be empty"
                    .to_string(),
                location: table_location.to_string(),
                source: None,
                storage_type: storage_profile.storage_type(),
            },
        )
        .await?;

        if let Some(metadata_location) = &metadata_location {
            let compression_codec = CompressionCodec::try_from_metadata(&table_metadata)?;
            write_metadata_file(
                metadata_location,
                &table_metadata,
                compression_codec,
                &file_io,
            )
            .await?;
        };

        // This requires the storage secret
        // because the table config might contain vended-credentials based
        //
        // on the `data_access` parameter.
        let config = storage_profile
            .generate_table_config(
                &data_access,
                storage_secret.as_ref(),
                &table_location,
                StoragePermissions::ReadWriteDelete,
            )
            .await?;
        let load_table_result = LoadTableResult {
            metadata_location: metadata_location.map(|l| l.to_string()),
            metadata: table_metadata,
            config: Some(config.into()),
        };

        // Tables are the odd one out: If a staged table was created before,
        // we must be able to overwrite it.
        authorizer
            .delete_table(TableIdentUuid::from(*table_id))
            .await?;
        authorizer
            .create_table(
                &request_metadata,
                TableIdentUuid::from(*table_id),
                namespace_id,
            )
            .await?;

        // Metadata file written, now we can commit the transaction
        t.commit().await?;

        emit_change_event(
            EventMetadata {
                tabular_id: TabularIdentUuid::Table(*table_id),
                warehouse_id: *warehouse_id,
                name: table.name.clone(),
                namespace: table.namespace.to_url_string(),
                prefix: prefix.map(Prefix::into_string).unwrap_or_default(),
                num_events: 1,
                sequence_number: 0,
                trace_id: request_metadata.request_id,
            },
            body,
            "createTable",
            state.v1_state.publisher.clone(),
        )
        .await;
        Ok(load_table_result)
    }

    /// Register a table in the given namespace using given metadata file location
    #[allow(clippy::too_many_lines)]
    async fn register_table(
        _parameters: NamespaceParameters,
        _request: RegisterTableRequest,
        _state: ApiContext<State<A, C, S>>,
        _request_metadata: RequestMetadata,
    ) -> Result<LoadTableResult> {
        Err(ErrorModel::not_implemented(
            "Registering tables is not supported",
            "RegisterTableNotSupported",
            None,
        )
        .into())
    }

    /// Load a table from the catalog
    #[allow(clippy::too_many_lines)]
    async fn load_table(
        parameters: TableParameters,
        data_access: DataAccess,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadTableResult> {
        // ------------------- VALIDATIONS -------------------
        let TableParameters { prefix, table } = parameters;
        let warehouse_id = require_warehouse_id(prefix)?;
        // It is important to throw a 404 if a table cannot be found,
        // because spark might check if `table`.`branch` exists, which should return 404.
        // Only then will it treat it as a branch.
        match validate_table_or_view_ident(&table) {
            Ok(()) => {}
            Err(mut e) => {
                if e.error.r#type == *"NamespaceDepthExceeded" {
                    e.error.code = StatusCode::NOT_FOUND.into();
                }
                return Err(e);
            }
        }

        // ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz;
        authorizer
            .require_warehouse_action(
                &request_metadata,
                warehouse_id,
                &CatalogWarehouseAction::CanUse,
            )
            .await?;
        let include_staged: bool = false;
        let include_deleted = false;
        let include_active = true;

        let mut t = C::Transaction::begin_read(state.v1_state.catalog).await?;
        let table_id = C::table_to_id(
            warehouse_id,
            &table,
            ListFlags {
                include_active,
                include_staged,
                include_deleted,
            },
            t.transaction(),
        )
        .await; // We can't fail before AuthZ.
        let table_id = authorizer
            .require_table_action(
                &request_metadata,
                warehouse_id,
                table_id,
                &CatalogTableAction::CanGetMetadata,
            )
            .await
            .map_err(set_not_found_status_code)?;

        let (read_access, write_access) = futures::try_join!(
            authorizer.is_allowed_table_action(
                &request_metadata,
                warehouse_id,
                table_id,
                &CatalogTableAction::CanReadData,
            ),
            authorizer.is_allowed_table_action(
                &request_metadata,
                warehouse_id,
                table_id,
                &CatalogTableAction::CanWriteData,
            ),
        )?;

        let storage_permissions = if write_access {
            Some(StoragePermissions::ReadWriteDelete)
        } else if read_access {
            Some(StoragePermissions::Read)
        } else {
            None
        };

        // ------------------- BUSINESS LOGIC -------------------
        let mut metadatas = C::load_tables(
            warehouse_id,
            vec![table_id],
            include_deleted,
            t.transaction(),
        )
        .await?;
        let CatalogLoadTableResult {
            table_id: _,
            namespace_id: _,
            table_metadata,
            metadata_location,
            storage_secret_ident,
            storage_profile,
        } = remove_table(&table_id, &table, &mut metadatas)?;
        require_not_staged(&metadata_location)?;

        let table_location =
            parse_location(&table_metadata.location, StatusCode::INTERNAL_SERVER_ERROR)?;

        // ToDo: This is a small inefficiency: We fetch the secret even if it might
        // not be required based on the `data_access` parameter.
        let storage_config = if let Some(storage_permissions) = storage_permissions {
            let storage_secret =
                maybe_get_secret(storage_secret_ident, &state.v1_state.secrets).await?;
            Some(
                storage_profile
                    .generate_table_config(
                        &data_access,
                        storage_secret.as_ref(),
                        &table_location,
                        storage_permissions,
                    )
                    .await?,
            )
        } else {
            None
        };

        let load_table_result = LoadTableResult {
            metadata_location: metadata_location.as_ref().map(ToString::to_string),
            metadata: table_metadata,
            config: storage_config.map(Into::into),
        };

        Ok(load_table_result)
    }

    /// Commit updates to a table
    #[allow(clippy::too_many_lines)]
    async fn commit_table(
        parameters: TableParameters,
        mut request: CommitTableRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<CommitTableResponse> {
        // ------------------- VALIDATIONS -------------------
        let warehouse_id = require_warehouse_id(parameters.prefix.clone())?;
        let TableParameters { table, prefix } = parameters;

        let table_ident = determine_table_ident(table, &request.identifier)?;
        // Fix identifier in request for emitted event
        request.identifier = Some(table_ident.clone());
        validate_table_or_view_ident(&table_ident)?;
        validate_table_updates(&request.updates)?;

        // ------------------- AUTHZ -------------------
        let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
        let authorizer = state.v1_state.authz;
        authorizer
            .require_warehouse_action(
                &request_metadata,
                warehouse_id,
                &CatalogWarehouseAction::CanUse,
            )
            .await?;

        let include_staged = true;
        let include_deleted = false;
        let include_active = true;

        let table_id = C::table_to_id(
            warehouse_id,
            &table_ident,
            ListFlags {
                include_active,
                include_staged,
                include_deleted,
            },
            t.transaction(),
        )
        .await; // We can't fail before AuthZ.

        let table_id = authorizer
            .require_table_action(
                &request_metadata,
                warehouse_id,
                table_id,
                &CatalogTableAction::CanCommit,
            )
            .await?;

        // ------------------- BUSINESS LOGIC -------------------
        // serialize body before moving it
        let body = maybe_body_to_json(&request);
        let CommitTableRequest {
            identifier: _,
            requirements,
            updates,
        } = request;

        let mut previous_table = C::load_tables(
            warehouse_id,
            vec![table_id],
            include_deleted,
            t.transaction(),
        )
        .await?;
        let previous_table = remove_table(&table_id, &table_ident, &mut previous_table)?;
        let warehouse = C::require_warehouse(warehouse_id, t.transaction()).await?;

        // Contract verification
        state
            .v1_state
            .contract_verifiers
            .check_table_updates(&updates, &previous_table.table_metadata)
            .await?
            .into_result()?;

        // Apply changes
        let TableMetadataBuildResult {
            metadata: new_metadata,
            changes: _,
            expired_metadata_logs,
        } = apply_commit(
            previous_table.table_metadata,
            &previous_table.metadata_location,
            &requirements,
            updates,
        )?;
        let new_table_location =
            parse_location(new_metadata.location(), StatusCode::INTERNAL_SERVER_ERROR)?;
        let new_compression_codec = CompressionCodec::try_from_metadata(&new_metadata)?;
        let new_metadata_location = previous_table.storage_profile.default_metadata_location(
            &new_table_location,
            &new_compression_codec,
            uuid::Uuid::now_v7(),
        );
        let commit = TableCommit {
            new_metadata,
            new_metadata_location,
        };
        C::commit_table_transaction(warehouse_id, vec![commit.clone()], t.transaction()).await?;

        // We don't commit the transaction yet, first we need to write the metadata file.
        let storage_secret =
            maybe_get_secret(warehouse.storage_secret_id, &state.v1_state.secrets).await?;

        let file_io = warehouse.storage_profile.file_io(storage_secret.as_ref())?;

        // Write metadata file
        write_metadata_file(
            &commit.new_metadata_location,
            &commit.new_metadata,
            new_compression_codec,
            &file_io,
        )
        .await?;

        t.commit().await?;

        // Delete files in parallel - if one delete fails, we still want to delete the rest
        if get_delete_after_commit_enabled(commit.new_metadata.properties()) {
            let _ = futures::future::join_all(
                expired_metadata_logs
                    .into_iter()
                    .map(|expired_metadata_log| file_io.delete(expired_metadata_log.metadata_file))
                    .collect::<Vec<_>>(),
            )
            .await
            .into_iter()
            .map(|r| {
                r.map_err(|e| tracing::warn!("Failed to delete metadata file: {:?}", e))
                    .ok()
            });
        }

        emit_change_event(
            EventMetadata {
                tabular_id: TabularIdentUuid::Table(*previous_table.table_id),
                warehouse_id: *warehouse_id,
                name: table_ident.name,
                namespace: table_ident.namespace.to_url_string(),
                prefix: prefix
                    .map(crate::api::iceberg::types::Prefix::into_string)
                    .unwrap_or_default(),

                num_events: 1,
                sequence_number: 0,
                trace_id: request_metadata.request_id,
            },
            body,
            "updateTable",
            state.v1_state.publisher.clone(),
        )
        .await;

        Ok(CommitTableResponse {
            metadata_location: commit.new_metadata_location.to_string(),
            metadata: commit.new_metadata,
            config: None,
        })
    }

    #[allow(clippy::too_many_lines)]
    /// Drop a table from the catalog
    async fn drop_table(
        parameters: TableParameters,
        DropParams { purge_requested }: DropParams,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- VALIDATIONS -------------------
        let TableParameters { prefix, table } = parameters;
        let warehouse_id = require_warehouse_id(prefix.clone())?;
        validate_table_or_view_ident(&table)?;

        // ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz;
        authorizer
            .require_warehouse_action(
                &request_metadata,
                warehouse_id,
                &CatalogWarehouseAction::CanUse,
            )
            .await?;

        let include_staged = true;
        let include_deleted = false;
        let include_active = true;

        let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
        let table_id = C::table_to_id(
            warehouse_id,
            &table,
            ListFlags {
                include_active,
                include_staged,
                include_deleted,
            },
            t.transaction(),
        )
        .await; // We can't fail before AuthZ

        let table_id = authorizer
            .require_table_action(
                &request_metadata,
                warehouse_id,
                table_id,
                &CatalogTableAction::CanDrop,
            )
            .await?;

        // ------------------- BUSINESS LOGIC -------------------
        let purge = purge_requested.unwrap_or(false);

        let warehouse = C::require_warehouse(warehouse_id, t.transaction()).await?;

        state
            .v1_state
            .contract_verifiers
            .check_drop(TabularIdentUuid::Table(*table_id))
            .await?
            .into_result()?;

        match warehouse.tabular_delete_profile {
            TabularDeleteProfile::Hard {} => {
                let location = C::drop_table(table_id, t.transaction()).await?;
                // committing here means maybe dangling data if queue_tabular_purge fails
                // commiting after queuing means we may end up with a table pointing nowhere
                // I feel that some undeleted files are less bad than a table that's there but can't be loaded
                t.commit().await?;

                if purge {
                    state
                        .v1_state
                        .queues
                        .queue_tabular_purge(TabularPurgeInput {
                            tabular_id: *table_id,
                            tabular_location: location,
                            warehouse_ident: warehouse_id,
                            tabular_type: TabularType::Table,
                            parent_id: None,
                        })
                        .await?;

                    tracing::debug!("Queued purge task for dropped table '{table_id}'.");
                }
                authorizer.delete_table(table_id).await?;
            }
            TabularDeleteProfile::Soft { expiration_seconds } => {
                C::mark_tabular_as_deleted(TabularIdentUuid::Table(*table_id), t.transaction())
                    .await?;
                t.commit().await?;

                state
                    .v1_state
                    .queues
                    .queue_tabular_expiration(TabularExpirationInput {
                        tabular_id: table_id.into(),
                        warehouse_ident: warehouse_id,
                        tabular_type: TabularType::Table,
                        purge,
                        expire_at: chrono::Utc::now() + expiration_seconds,
                    })
                    .await?;
                tracing::debug!("Queued expiration task for dropped table '{table_id}'.");
            }
        }

        emit_change_event(
            EventMetadata {
                tabular_id: TabularIdentUuid::Table(*table_id),
                warehouse_id: *warehouse_id,
                name: table.name,
                namespace: table.namespace.to_url_string(),
                prefix: prefix
                    .map(crate::api::iceberg::types::Prefix::into_string)
                    .unwrap_or_default(),
                num_events: 1,
                sequence_number: 0,
                trace_id: request_metadata.request_id,
            },
            serde_json::Value::Null,
            "dropTable",
            state.v1_state.publisher,
        )
        .await;

        Ok(())
    }

    /// Check if a table exists
    async fn table_exists(
        parameters: TableParameters,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- VALIDATIONS -------------------
        let TableParameters { prefix, table } = parameters;
        let warehouse_id = require_warehouse_id(prefix.clone())?;
        validate_table_or_view_ident(&table)?;

        // ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz;
        authorizer
            .require_warehouse_action(
                &request_metadata,
                warehouse_id,
                &CatalogWarehouseAction::CanUse,
            )
            .await?;

        let include_staged = false;
        let include_deleted = false;
        let include_active = true;

        let mut t = C::Transaction::begin_read(state.v1_state.catalog).await?;
        let table_id = C::table_to_id(
            warehouse_id,
            &table,
            ListFlags {
                include_active,
                include_staged,
                include_deleted,
            },
            t.transaction(),
        )
        .await; // We can't fail before AuthZ
        authorizer
            .require_table_action(
                &request_metadata,
                warehouse_id,
                table_id,
                &CatalogTableAction::CanGetMetadata,
            )
            .await
            .map_err(set_not_found_status_code)?;

        // ------------------- BUSINESS LOGIC -------------------
        Ok(())
    }

    /// Rename a table
    async fn rename_table(
        prefix: Option<Prefix>,
        request: RenameTableRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- VALIDATIONS -------------------
        let warehouse_id = require_warehouse_id(prefix.clone())?;
        let body = maybe_body_to_json(&request);
        let RenameTableRequest {
            source,
            destination,
        } = request;
        validate_table_or_view_ident(&source)?;
        validate_table_or_view_ident(&destination)?;

        // ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz;
        authorizer
            .require_warehouse_action(
                &request_metadata,
                warehouse_id,
                &CatalogWarehouseAction::CanUse,
            )
            .await?;

        let include_staged = false;
        let include_deleted = false;
        let include_active = true;

        let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
        let source_table_id = C::table_to_id(
            warehouse_id,
            &source,
            ListFlags {
                include_active,
                include_staged,
                include_deleted,
            },
            t.transaction(),
        )
        .await; // We can't fail before AuthZ;
        let source_table_id = authorizer
            .require_table_action(
                &request_metadata,
                warehouse_id,
                source_table_id,
                &CatalogTableAction::CanRename,
            )
            .await?;
        let namespace_id =
            C::namespace_to_id(warehouse_id, &source.namespace, t.transaction()).await; // We can't fail before AuthZ

        // We need to be allowed to delete the old table and create the new one
        authorizer
            .require_namespace_action(
                &request_metadata,
                warehouse_id,
                namespace_id,
                &CatalogNamespaceAction::CanCreateTable,
            )
            .await?;

        // ------------------- BUSINESS LOGIC -------------------
        if source == destination {
            return Ok(());
        }

        C::rename_table(
            warehouse_id,
            source_table_id,
            &source,
            &destination,
            t.transaction(),
        )
        .await?;

        state
            .v1_state
            .contract_verifiers
            .check_rename(TabularIdentUuid::Table(*source_table_id), &destination)
            .await?
            .into_result()?;

        t.commit().await?;

        emit_change_event(
            EventMetadata {
                tabular_id: TabularIdentUuid::Table(*source_table_id),
                warehouse_id: *warehouse_id,
                name: source.name,
                namespace: source.namespace.to_url_string(),
                prefix: prefix.map(Prefix::into_string).unwrap_or_default(),
                num_events: 1,
                sequence_number: 0,
                trace_id: request_metadata.request_id,
            },
            body,
            "renameTable",
            state.v1_state.publisher.clone(),
        )
        .await;

        Ok(())
    }

    /// Commit updates to multiple tables in an atomic operation
    #[allow(clippy::too_many_lines)]
    // ToDo: Split some of this into helper functions
    async fn commit_transaction(
        prefix: Option<Prefix>,
        request: CommitTransactionRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- VALIDATIONS -------------------
        let warehouse_id = require_warehouse_id(prefix.clone())?;
        for change in &request.table_changes {
            validate_table_updates(&change.updates)?;
            change
                .identifier
                .as_ref()
                .map(validate_table_or_view_ident)
                .transpose()?;

            if change.identifier.is_none() {
                return Err(ErrorModel::builder()
                        .code(StatusCode::BAD_REQUEST.into())
                        .message(
                            "Table identifier is required for each change in the CommitTransactionRequest"
                                .to_string(),
                        )
                        .r#type("TableIdentifierRequiredForCommitTransaction".to_string())
                        .build()
                        .into());
            };
        }

        // ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz;
        authorizer
            .require_warehouse_action(
                &request_metadata,
                warehouse_id,
                &CatalogWarehouseAction::CanUse,
            )
            .await?;

        let include_staged = true;
        let include_deleted = false;
        let include_active = true;

        let identifiers = request
            .table_changes
            .iter()
            .filter_map(|change| change.identifier.as_ref())
            .collect::<HashSet<_>>();

        let table_ids = C::table_idents_to_ids(
            warehouse_id,
            identifiers,
            ListFlags {
                include_active,
                include_staged,
                include_deleted,
            },
            state.v1_state.catalog.clone(),
        )
        .await
        .map_err(|e| {
            ErrorModel::internal("Error fetching table ids", "TableIdsFetchError", None)
                .append_details(vec![e.error.message, e.error.r#type])
                .append_details(e.error.stack)
        })?;

        let authz_checks = table_ids
            .values()
            .map(|table_id| {
                authorizer.require_table_action(
                    &request_metadata,
                    warehouse_id,
                    Ok(*table_id),
                    &CatalogTableAction::CanCommit,
                )
            })
            .collect::<Vec<_>>();

        let table_uuids = futures::future::try_join_all(authz_checks).await?;
        let table_ids = table_ids
            .into_iter()
            .zip(table_uuids)
            .map(|((table_ident, _), table_uuid)| (table_ident, table_uuid))
            .collect::<HashMap<_, _>>();

        // ------------------- BUSINESS LOGIC -------------------

        let mut transaction = C::Transaction::begin_write(state.v1_state.catalog).await?;
        let warehouse = C::require_warehouse(warehouse_id, transaction.transaction()).await?;

        // Store data for events before it is moved
        let mut events = vec![];
        let mut event_table_ids: Vec<(TableIdent, TableIdentUuid)> = vec![];
        let mut updates = vec![];
        for commit_table_request in &request.table_changes {
            if let Some(id) = &commit_table_request.identifier {
                if let Some(uuid) = table_ids.get(id) {
                    events.push(maybe_body_to_json(commit_table_request));
                    event_table_ids.push((id.clone(), *uuid));
                    updates.push(commit_table_request.updates.clone());
                }
            }
        }

        // Load old metadata
        let mut previous_metadatas = C::load_tables(
            warehouse_id,
            table_ids.values().copied(),
            include_deleted,
            transaction.transaction(),
        )
        .await?;

        let mut expired_metadata_logs: Vec<MetadataLog> = vec![];

        // Apply changes
        let commits = request
            .table_changes
            .into_iter()
            .map(|change| {
                let table_ident = change.identifier.ok_or_else(||
                    // This should never happen due to validation
                    ErrorModel::internal(
                        "Change without Identifier",
                        "ChangeWithoutIdentifier",
                        None,
                    ))?;
                let table_id =
                    require_table_id(&table_ident, table_ids.get(&table_ident).copied())?;
                let previous_table =
                    remove_table(&table_id, &table_ident, &mut previous_metadatas)?;
                let TableMetadataBuildResult {
                    metadata: new_metadata,
                    changes: _,
                    expired_metadata_logs: this_expired,
                } = apply_commit(
                    previous_table.table_metadata.clone(),
                    &previous_table.metadata_location,
                    &change.requirements,
                    change.updates.clone(),
                )?;
                if get_delete_after_commit_enabled(new_metadata.properties()) {
                    expired_metadata_logs.extend(this_expired);
                }
                let new_table_location =
                    parse_location(new_metadata.location(), StatusCode::INTERNAL_SERVER_ERROR)?;
                let new_compression_codec = CompressionCodec::try_from_metadata(&new_metadata)?;
                let new_metadata_location =
                    previous_table.storage_profile.default_metadata_location(
                        &new_table_location,
                        &new_compression_codec,
                        uuid::Uuid::now_v7(),
                    );
                Ok(CommitContext {
                    new_metadata,
                    new_metadata_location,
                    new_compression_codec,
                    updates: change.updates,
                    previous_metadata: previous_table.table_metadata,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        // Commit changes in DB
        C::commit_table_transaction(
            warehouse_id,
            commits.iter().map(CommitContext::commit),
            transaction.transaction(),
        )
        .await?;

        // Check contract verification
        let futures = commits.iter().map(|c| {
            state
                .v1_state
                .contract_verifiers
                .check_table_updates(&c.updates, &c.previous_metadata)
        });

        futures::future::try_join_all(futures)
            .await?
            .into_iter()
            .map(ContractVerificationOutcome::into_result)
            .collect::<Result<Vec<()>, ErrorModel>>()?;

        // We don't commit the transaction yet, first we need to write the metadata file.
        let storage_secret =
            maybe_get_secret(warehouse.storage_secret_id, &state.v1_state.secrets).await?;

        // Write metadata files
        let file_io = warehouse.storage_profile.file_io(storage_secret.as_ref())?;

        let write_futures: Vec<_> = commits
            .iter()
            .map(|commit| {
                write_metadata_file(
                    &commit.new_metadata_location,
                    &commit.new_metadata,
                    commit.new_compression_codec,
                    &file_io,
                )
            })
            .collect();
        futures::future::try_join_all(write_futures).await?;

        transaction.commit().await?;

        // Delete files in parallel - if one delete fails, we still want to delete the rest
        let _ = futures::future::join_all(
            expired_metadata_logs
                .into_iter()
                .map(|expired_metadata_log| file_io.delete(expired_metadata_log.metadata_file))
                .collect::<Vec<_>>(),
        )
        .await
        .into_iter()
        .map(|r| {
            r.map_err(|e| tracing::warn!("Failed to delete metadata file: {:?}", e))
                .ok()
        });

        let number_of_events = events.len();

        for (event_sequence_number, (body, (table_ident, table_id))) in
            events.into_iter().zip(event_table_ids).enumerate()
        {
            emit_change_event(
                EventMetadata {
                    tabular_id: TabularIdentUuid::Table(*table_id),
                    warehouse_id: *warehouse_id,
                    name: table_ident.name,
                    namespace: table_ident.namespace.to_url_string(),
                    prefix: prefix
                        .clone()
                        .map(|p| p.as_str().to_string())
                        .unwrap_or_default(),
                    num_events: number_of_events,
                    sequence_number: event_sequence_number,
                    trace_id: request_metadata.request_id,
                },
                body,
                "updateTable",
                state.v1_state.publisher.clone(),
            )
            .await;
        }

        Ok(())
    }
}

struct CommitContext {
    pub new_metadata: iceberg::spec::TableMetadata,
    pub new_metadata_location: Location,
    pub previous_metadata: iceberg::spec::TableMetadata,
    pub updates: Vec<TableUpdate>,
    pub new_compression_codec: CompressionCodec,
}

impl CommitContext {
    fn commit(&self) -> TableCommit {
        TableCommit {
            new_metadata: self.new_metadata.clone(),
            new_metadata_location: self.new_metadata_location.clone(),
        }
    }
}

pub(crate) fn determine_table_ident(
    parameters_ident: TableIdent,
    request_ident: &Option<TableIdent>,
) -> Result<TableIdent> {
    let Some(identifier) = request_ident else {
        return Ok(parameters_ident);
    };

    if identifier == &parameters_ident {
        return Ok(identifier.clone());
    }

    // Below is for the tricky case: We have a conflict.
    // When querying a branch, spark sends something like the following as part of the `parameters`:
    // namespace: (<my>, <namespace>, <table_name>)
    // table_name: branch_<branch_name>
    let ns_parts = parameters_ident.namespace.clone().inner();
    let table_name_candidate = if ns_parts.len() >= 2 {
        NamespaceIdent::from_vec(ns_parts.iter().take(ns_parts.len() - 1).cloned().collect())
            .ok()
            .map(|n| TableIdent::new(n, ns_parts.last().cloned().unwrap_or_default()))
    } else {
        None
    };

    if table_name_candidate != Some(identifier.clone()) {
        return Err(ErrorModel::bad_request(
            "Table identifier in path does not match the one in the request body",
            "TableIdentifierMismatch",
            None,
        )
        .into());
    }

    Ok(identifier.clone())
}

pub(super) fn parse_location(location: &str, code: StatusCode) -> Result<Location> {
    Location::from_str(location)
        .map_err(|e| {
            ErrorModel::builder()
                .code(code.into())
                .message(format!("Invalid location: {e}"))
                .r#type("InvalidTableLocation".to_string())
                .build()
        })
        .map_err(Into::into)
}

pub(super) fn determine_tabular_location(
    namespace: &GetNamespaceResponse,
    request_table_location: Option<String>,
    table_id: TabularIdentUuid,
    storage_profile: &StorageProfile,
) -> Result<Location> {
    let request_table_location = request_table_location
        .map(|l| parse_location(&l, StatusCode::BAD_REQUEST))
        .transpose()?;

    let mut location = if let Some(location) = request_table_location {
        if !storage_profile.is_allowed_location(&location) {
            return Err(ErrorModel::bad_request(
                format!("Specified table location is not allowed: {location}"),
                "InvalidTableLocation",
                None,
            )
            .into());
        }
        location
    } else {
        let namespace_props = NamespaceProperties::from_props_unchecked(
            namespace.properties.clone().unwrap_or_default(),
        );

        let namespace_location = match namespace_props.get_location() {
            Some(location) => location,
            None => storage_profile
                .default_namespace_location(namespace.namespace_id)
                .map_err(|e| {
                    ErrorModel::internal(
                        "Failed to generate default namespace location",
                        "InvalidDefaultNamespaceLocaiton",
                        Some(Box::new(e)),
                    )
                })?,
        };

        storage_profile.default_tabular_location(&namespace_location, table_id)
    };
    // all locations are without a trailing slash
    location.without_trailing_slash();
    Ok(location)
}

fn require_table_id(
    table_ident: &TableIdent,
    table_id: Option<TableIdentUuid>,
) -> Result<TableIdentUuid> {
    table_id.ok_or_else(|| {
        ErrorModel::not_found(
            format!(
                "Table '{}.{}' does not exist.",
                table_ident.namespace.to_url_string(),
                table_ident.name
            ),
            "TableNotFound",
            None,
        )
        .into()
    })
}

fn require_not_staged<T>(metadata_location: &Option<T>) -> Result<()> {
    if metadata_location.is_none() {
        return Err(ErrorModel::not_found(
            "Table not found or staged.",
            "TableNotFoundOrStaged",
            None,
        )
        .into());
    }

    Ok(())
}

fn remove_table<T>(
    table_id: &TableIdentUuid,
    table_ident: &TableIdent,
    metadatas: &mut HashMap<TableIdentUuid, T>,
) -> Result<T> {
    metadatas
        .remove(table_id)
        .ok_or_else(|| {
            ErrorModel::not_found(
                format!(
                    "Table '{}.{}' does not exist.",
                    table_ident.namespace.to_url_string(),
                    table_ident.name
                ),
                "TableNotFound",
                None,
            )
        })
        .map_err(Into::into)
}

pub(crate) fn require_active_warehouse(status: WarehouseStatus) -> Result<()> {
    if status != WarehouseStatus::Active {
        return Err(ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message("Warehouse is not active".to_string())
            .r#type("WarehouseNotActive".to_string())
            .build()
            .into());
    }
    Ok(())
}

async fn emit_change_event(
    parameters: EventMetadata,
    body: serde_json::Value,
    operation_id: &str,
    publisher: CloudEventsPublisher,
) {
    let _ = publisher
        .publish(Uuid::now_v7(), operation_id, body, parameters)
        .await;
}

// Quick validation of properties for early fails.
// Full validation is performed when changes are applied.
fn validate_table_updates(updates: &Vec<TableUpdate>) -> Result<()> {
    for update in updates {
        match update {
            TableUpdate::SetProperties { updates } => {
                validate_table_properties(updates.keys())?;
            }
            TableUpdate::RemoveProperties { removals } => {
                validate_table_properties(removals)?;
            }
            _ => {}
        }
    }
    Ok(())
}

pub(crate) fn validate_lowercase_property(prop: &str) -> Result<()> {
    if prop != prop.to_lowercase() {
        return Err(ErrorModel::builder()
            .code(StatusCode::BAD_REQUEST.into())
            .message(format!("The property '{prop}' is not all lowercase."))
            .r#type("PropertyNotLowercase")
            .build()
            .into());
    }
    Ok(())
}

pub(crate) fn get_delete_after_commit_enabled(properties: &HashMap<String, String>) -> bool {
    properties
        .get(PROPERTY_METADATA_DELETE_AFTER_COMMIT_ENABLED)
        .map_or(PROPERTY_METADATA_DELETE_AFTER_COMMIT_ENABLED_DEFAULT, |v| {
            v == "true"
        })
}

pub(crate) fn validate_table_properties<'a, I>(properties: I) -> Result<()>
where
    I: IntoIterator<Item = &'a String>,
{
    for prop in properties {
        if (prop.starts_with("write.metadata")
            && ![
                PROPERTY_METADATA_PREVIOUS_VERSIONS_MAX,
                PROPERTY_METADATA_DELETE_AFTER_COMMIT_ENABLED,
                "write.metadata.compression-codec",
            ]
            .contains(&prop.as_str()))
            || prop.starts_with("write.data.path")
        {
            return Err(ErrorModel::conflict(
                format!("Properties contain unsupported property: '{prop}'"),
                "FailedToSetProperties",
                None,
            )
            .into());
        }
        validate_lowercase_property(prop)?;
    }

    Ok(())
}

pub(crate) fn validate_table_or_view_ident(table: &TableIdent) -> Result<()> {
    let TableIdent {
        ref namespace,
        ref name,
    } = &table;
    validate_namespace_ident(namespace)?;

    if name.is_empty() {
        return Err(ErrorModel::bad_request(
            "name of the identifier cannot be empty",
            "IdentifierNameEmpty",
            None,
        )
        .into());
    }
    Ok(())
}

// This function does not return a result but serde_json::Value::Null if serialization
// fails. This follows the rationale that we'll likely end up ignoring the error in the API handler
// anyway since we already effected the change and only the event emission about the change failed.
// Given that we are serializing stuff we've received as a json body and also successfully
// processed, it's unlikely to cause issues.
pub(crate) fn maybe_body_to_json(request: impl Serialize) -> serde_json::Value {
    if let Ok(body) = serde_json::to_value(&request) {
        body
    } else {
        tracing::warn!("Serializing the request body to json failed, this is very unexpected. It will not be part of any emitted Event.");
        serde_json::Value::Null
    }
}
