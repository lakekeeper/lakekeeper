use super::commit_tables::apply_commit;
use super::io::delete_file;
use super::namespace::authorized_namespace_ident_to_id;
use super::{
    io::write_metadata_file, maybe_get_secret, namespace::validate_namespace_ident,
    require_warehouse_id, CatalogServer,
};
use crate::api::iceberg::types::DropParams;
use crate::api::iceberg::v1::{
    ApiContext, CommitTableRequest, CommitTableResponse, CommitTransactionRequest,
    CreateTableRequest, DataAccess, ErrorModel, ListTablesQuery, ListTablesResponse,
    LoadTableResult, NamespaceParameters, PaginationQuery, Prefix, RegisterTableRequest,
    RenameTableRequest, Result, TableIdent, TableParameters,
};
use crate::api::management::v1::warehouse::TabularDeleteProfile;
use crate::api::management::v1::TabularType;
use crate::api::set_not_found_status_code;
use crate::catalog::compression_codec::CompressionCodec;
use crate::request_metadata::RequestMetadata;
use crate::service::authz::{CatalogNamespaceAction, CatalogTableAction, CatalogWarehouseAction};
use crate::service::contract_verification::{ContractVerification, ContractVerificationOutcome};
use crate::service::event_publisher::{CloudEventsPublisher, EventMetadata};
use crate::service::storage::{
    StorageLocations as _, StoragePermissions, StorageProfile, ValidationError,
};
use crate::service::task_queue::tabular_expiration_queue::TabularExpirationInput;
use crate::service::task_queue::tabular_purge_queue::TabularPurgeInput;
use crate::service::TabularIdentUuid;
use crate::service::{
    authz::Authorizer, secrets::SecretStore, Catalog, CreateTableResponse, ListFlags,
    LoadTableResponse as CatalogLoadTableResult, State, TabularDetails, Transaction,
};
use crate::service::{
    GetNamespaceResponse, TableCommit, TableCreation, TableIdentUuid, WarehouseStatus,
};
use futures::FutureExt;
use fxhash::FxHashSet;
use std::collections::{HashMap, HashSet};
use std::str::FromStr as _;

use crate::catalog::tabular::list_entities;
use crate::retry::retry_fn;
use crate::{catalog, WarehouseIdent};
use http::StatusCode;
use iceberg::spec::{
    FormatVersion, MetadataLog, SchemaId, SortOrder, TableMetadata, TableMetadataBuildResult,
    TableMetadataBuilder, UnboundPartitionSpec, PROPERTY_FORMAT_VERSION,
    PROPERTY_METADATA_PREVIOUS_VERSIONS_MAX,
};
use iceberg::{NamespaceIdent, TableUpdate};
use iceberg_ext::catalog::rest::{LoadCredentialsResponse, StorageCredential};
use iceberg_ext::configs::namespace::NamespaceProperties;
use iceberg_ext::configs::{Location, ParseFromStr};
use itertools::Itertools;
use serde::Serialize;
use uuid::Uuid;

const PROPERTY_METADATA_DELETE_AFTER_COMMIT_ENABLED: &str =
    "write.metadata.delete-after-commit.enabled";
const PROPERTY_METADATA_DELETE_AFTER_COMMIT_ENABLED_DEFAULT: bool = false;

#[async_trait::async_trait]
impl<C: Catalog, A: Authorizer + Clone, S: SecretStore>
    crate::api::iceberg::v1::tables::TablesService<State<A, C, S>> for CatalogServer<C, A, S>
{
    #[allow(clippy::too_many_lines)]
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
        let mut t = C::Transaction::begin_read(state.v1_state.catalog).await?;
        let _namespace_id = authorized_namespace_ident_to_id::<C, _>(
            authorizer.clone(),
            &request_metadata,
            &warehouse_id,
            &namespace,
            &CatalogNamespaceAction::CanListTables,
            t.transaction(),
        )
        .await?;
        // ------------------- BUSINESS LOGIC -------------------

        let (identifiers, table_uuids, next_page_token) =
            catalog::fetch_until_full_page::<_, _, _, C>(
                query.page_size,
                query.page_token,
                list_entities!(
                    Table,
                    list_tables,
                    table_action,
                    namespace,
                    authorizer,
                    request_metadata,
                    warehouse_id
                ),
                &mut t,
            )
            .await?;
        t.commit().await?;

        Ok(ListTablesResponse {
            next_page_token,
            identifiers,
            table_uuids: return_uuids.then_some(table_uuids.into_iter().map(|u| *u).collect()),
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
        let authorizer = state.v1_state.authz.clone();
        let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
        let namespace_id = authorized_namespace_ident_to_id::<C, _>(
            authorizer.clone(),
            &request_metadata,
            &warehouse_id,
            &namespace,
            &CatalogNamespaceAction::CanCreateTable,
            t.transaction(),
        )
        .await?;

        // ------------------- BUSINESS LOGIC -------------------
        let id = Uuid::now_v7();
        let tabular_id = TabularIdentUuid::Table(id);
        let table_id = TableIdentUuid::from(id);

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

        // serialize body before moving it
        let body = maybe_body_to_json(&request);

        let table_metadata = create_table_request_into_table_metadata(table_id, request)?;

        let CreateTableResponse {
            table_metadata,
            staged_table_id,
        } = C::create_table(
            TableCreation {
                namespace_id: namespace.namespace_id,
                table_ident: &table,
                table_metadata,
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
        retry_fn(|| async {
            match crate::service::storage::check_location_is_empty(
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
            .await
            {
                Err(e @ ValidationError::IoOperationFailed(_, _)) => {
                    tracing::warn!(
                        "Error while checking location is empty: {e}, retrying up to three times.."
                    );
                    Err(e)
                }
                Ok(()) => {
                    tracing::debug!("Location is empty");
                    Ok(Ok(()))
                }
                Err(other) => {
                    tracing::error!("Unrecoverable error: {other:?}");
                    Ok(Err(other))
                }
            }
        })
        .await??;

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

        let storage_credentials = (!config.creds.inner().is_empty()).then(|| {
            vec![StorageCredential {
                prefix: table_location.to_string(),
                config: config.creds.into(),
            }]
        });

        let load_table_result = LoadTableResult {
            metadata_location: metadata_location.map(|l| l.to_string()),
            metadata: table_metadata,
            config: Some(config.config.into()),
            storage_credentials,
        };

        authorizer
            .create_table(
                &request_metadata,
                TableIdentUuid::from(*tabular_id),
                namespace_id,
            )
            .await?;

        // Metadata file written, now we can commit the transaction
        t.commit().await?;

        if let Some(staged_table_id) = staged_table_id {
            authorizer.delete_table(staged_table_id).await.ok();
        }

        emit_change_event(
            EventMetadata {
                tabular_id: TabularIdentUuid::Table(*tabular_id),
                warehouse_id,
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
        if let Err(mut e) = validate_table_or_view_ident(&table) {
            if e.error.r#type == *"NamespaceDepthExceeded" {
                e.error.code = StatusCode::NOT_FOUND.into();
            }
            return Err(e);
        }

        let list_flags = ListFlags {
            include_active: true,
            include_staged: false,
            include_deleted: false,
        };

        // ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz;
        let catalog = state.v1_state.catalog;
        let mut t = C::Transaction::begin_read(catalog).await?;

        let (table_id, storage_permissions) = Self::resolve_and_authorize_table_access(
            &request_metadata,
            &table,
            warehouse_id,
            list_flags,
            authorizer,
            t.transaction(),
        )
        .await?;

        // ------------------- BUSINESS LOGIC -------------------
        let mut metadatas = C::load_tables(
            warehouse_id,
            vec![table_id.ident],
            list_flags.include_deleted,
            t.transaction(),
        )
        .await?;
        t.commit().await?;
        let CatalogLoadTableResult {
            table_id: _,
            namespace_id: _,
            table_metadata,
            metadata_location,
            storage_secret_ident,
            storage_profile,
        } = remove_table(&table_id.ident, &table, &mut metadatas)?;
        require_not_staged(metadata_location.as_ref())?;

        let table_location =
            parse_location(table_metadata.location(), StatusCode::INTERNAL_SERVER_ERROR)?;

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

        let storage_credentials = storage_config.as_ref().and_then(|c| {
            (!c.creds.inner().is_empty()).then(|| {
                vec![StorageCredential {
                    prefix: table_location.to_string(),
                    config: c.creds.clone().into(),
                }]
            })
        });

        let load_table_result = LoadTableResult {
            metadata_location: metadata_location.as_ref().map(ToString::to_string),
            metadata: table_metadata,
            config: storage_config.map(|c| c.config.into()),
            storage_credentials,
        };

        Ok(load_table_result)
    }

    async fn load_table_credentials(
        parameters: TableParameters,
        data_access: DataAccess,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadCredentialsResponse> {
        // ------------------- VALIDATIONS -------------------
        let TableParameters { prefix, table } = parameters;
        let warehouse_id = require_warehouse_id(prefix)?;

        let mut t = C::Transaction::begin_read(state.v1_state.catalog).await?;
        let (table_id, storage_permissions) = Self::resolve_and_authorize_table_access(
            &request_metadata,
            &table,
            warehouse_id,
            ListFlags {
                include_active: true,
                include_staged: false,
                include_deleted: false,
            },
            state.v1_state.authz,
            t.transaction(),
        )
        .await?;
        let storage_permission = storage_permissions.ok_or(ErrorModel::unauthorized(
            "No storage permissions for table",
            "NoStoragePermissions",
            None,
        ))?;

        let (storage_secret_ident, storage_profile) =
            C::load_storage_profile(warehouse_id, table_id.ident, t.transaction()).await?;
        let storage_secret =
            maybe_get_secret(storage_secret_ident, &state.v1_state.secrets).await?;
        let storage_config = storage_profile
            .generate_table_config(
                &data_access,
                storage_secret.as_ref(),
                &parse_location(
                    table_id.location.as_str(),
                    StatusCode::INTERNAL_SERVER_ERROR,
                )?,
                storage_permission,
            )
            .await?;

        let storage_credentials = if storage_config.creds.inner().is_empty() {
            vec![]
        } else {
            vec![StorageCredential {
                prefix: table_id.location.clone(),
                config: storage_config.creds.into(),
            }]
        };

        Ok(LoadCredentialsResponse {
            storage_credentials,
        })
    }

    /// Commit updates to a table
    #[allow(clippy::too_many_lines)]
    async fn commit_table(
        parameters: TableParameters,
        mut request: CommitTableRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<CommitTableResponse> {
        request.identifier = Some(determine_table_ident(
            parameters.table,
            request.identifier.as_ref(),
        )?);
        let t = commit_tables_internal(
            parameters.prefix,
            CommitTransactionRequest {
                table_changes: vec![request],
            },
            state,
            request_metadata,
        )
        .await?;
        let Some(item) = t.into_iter().next() else {
            return Err(ErrorModel::internal(
                "No new metadata returned by backend",
                "NoNewMetadataReturned",
                None,
            )
            .into());
        };

        Ok(CommitTableResponse {
            metadata_location: item.new_metadata_location.to_string(),
            metadata: item.new_metadata,
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
            .require_table_action(&request_metadata, table_id, &CatalogTableAction::CanDrop)
            .await?;

        // ------------------- BUSINESS LOGIC -------------------
        let purge = purge_requested.unwrap_or(true);

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
                warehouse_id,
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
        let mut t = C::Transaction::begin_read(state.v1_state.catalog).await?;
        let list_flags = ListFlags {
            include_staged: false,
            include_deleted: false,
            include_active: true,
        };
        let _table_id = authorized_table_ident_to_id::<C, _>(
            authorizer,
            &request_metadata,
            warehouse_id,
            &table,
            list_flags,
            &CatalogTableAction::CanGetMetadata,
            t.transaction(),
        )
        .await?;
        t.commit().await?;

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
        let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
        let list_flags = ListFlags {
            include_staged: false,
            include_deleted: false,
            include_active: true,
        };
        let source_table_id = authorized_table_ident_to_id::<C, _>(
            authorizer.clone(),
            &request_metadata,
            warehouse_id,
            &source,
            list_flags,
            &CatalogTableAction::CanRename,
            t.transaction(),
        )
        .await?;

        let namespace_id =
            C::namespace_to_id(warehouse_id, &source.namespace, t.transaction()).await; // We can't fail before AuthZ

        // We need to be allowed to delete the old table and create the new one
        authorizer
            .require_namespace_action(
                &request_metadata,
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
                warehouse_id,
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
        let _ = commit_tables_internal(prefix, request, state, request_metadata).await?;
        Ok(())
    }
}

impl<C: Catalog, A: Authorizer + Clone, S: SecretStore> CatalogServer<C, A, S> {
    async fn resolve_and_authorize_table_access(
        request_metadata: &RequestMetadata,
        table: &TableIdent,
        warehouse_id: WarehouseIdent,
        list_flags: ListFlags,
        authorizer: A,
        transaction: <C::Transaction as Transaction<C::State>>::Transaction<'_>,
    ) -> Result<(TabularDetails, Option<StoragePermissions>)> {
        authorizer
            .require_warehouse_action(
                request_metadata,
                warehouse_id,
                &CatalogWarehouseAction::CanUse,
            )
            .await?;

        // We can't fail before AuthZ.
        let table_id = C::resolve_table_ident(warehouse_id, table, list_flags, transaction).await;

        let table_id = authorizer
            .require_table_action(
                request_metadata,
                table_id,
                &CatalogTableAction::CanGetMetadata,
            )
            .await
            .map_err(set_not_found_status_code)?;

        let (read_access, write_access) = futures::try_join!(
            authorizer.is_allowed_table_action(
                request_metadata,
                table_id.ident,
                &CatalogTableAction::CanReadData,
            ),
            authorizer.is_allowed_table_action(
                request_metadata,
                table_id.ident,
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
        Ok((table_id, storage_permissions))
    }
}

// TODO: split this into smaller functions
#[allow(clippy::too_many_lines)]
async fn commit_tables_internal<C: Catalog, A: Authorizer + Clone, S: SecretStore>(
    prefix: Option<Prefix>,
    request: CommitTransactionRequest,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<Vec<CommitContext>> {
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
            return Err(ErrorModel::bad_request(
                "Table identifier is required for each change in the CommitTransactionRequest",
                "TableIdentifierRequiredForCommitTransaction",
                None,
            )
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
    let n_identifiers = identifiers.len();
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

    if n_identifiers != request.table_changes.len() {
        return Err(ErrorModel::bad_request(
            "Table identifiers must be unique in the CommitTransactionRequest",
            "UniqueTableIdentifiersRequiredForCommitTransaction",
            None,
        )
        .into());
    }

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
            let table_id = require_table_id(&table_ident, table_ids.get(&table_ident).copied())?;
            let previous_table = remove_table(&table_id, &table_ident, &mut previous_metadatas)?;
            let TableMetadataBuildResult {
                metadata: new_metadata,
                changes: _,
                expired_metadata_logs: mut this_expired,
            } = apply_commit(
                previous_table.table_metadata.clone(),
                previous_table.metadata_location.as_ref(),
                &change.requirements,
                change.updates.clone(),
            )?;

            let number_expired_metadata_log_entries = this_expired.len();

            if get_delete_after_commit_enabled(new_metadata.properties()) {
                expired_metadata_logs.extend(this_expired);
            } else {
                this_expired.clear();
            }

            let next_metadata_count = previous_table
                .metadata_location
                .as_ref()
                .and_then(extract_count_from_metadata_location)
                .map_or(0, |v| v + 1);

            let new_table_location =
                parse_location(new_metadata.location(), StatusCode::INTERNAL_SERVER_ERROR)?;
            let new_compression_codec = CompressionCodec::try_from_metadata(&new_metadata)?;
            let new_metadata_location = previous_table.storage_profile.default_metadata_location(
                &new_table_location,
                &new_compression_codec,
                Uuid::now_v7(),
                next_metadata_count,
            );

            let number_added_metadata_log_entries = (new_metadata.metadata_log().len()
                + number_expired_metadata_log_entries)
                .saturating_sub(previous_table.table_metadata.metadata_log().len());

            Ok(CommitContext {
                new_metadata,
                new_metadata_location,
                new_compression_codec,
                updates: change.updates,
                previous_metadata: previous_table.table_metadata,
                number_expired_metadata_log_entries,
                number_added_metadata_log_entries,
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
    let expired_locations = expired_metadata_logs
        .into_iter()
        .filter_map(|expired_metadata_log| {
            Location::parse_value(&expired_metadata_log.metadata_file)
                .map_err(|e| {
                    tracing::warn!(
                        "Failed to parse expired metadata file location {}: {:?}",
                        expired_metadata_log.metadata_file,
                        e
                    );
                })
                .ok()
        })
        .collect::<Vec<_>>();
    let _ = futures::future::join_all(
        expired_locations
            .iter()
            .map(|location| delete_file(&file_io, location))
            .collect::<Vec<_>>(),
    )
    .await
    .into_iter()
    .map(|r| {
        r.map_err(|e| tracing::warn!("Failed to delete expired metadata file: {:?}", e))
            .ok()
    });

    let number_of_events = events.len();

    for (event_sequence_number, (body, (table_ident, table_id))) in
        events.into_iter().zip(event_table_ids).enumerate()
    {
        emit_change_event(
            EventMetadata {
                tabular_id: TabularIdentUuid::Table(*table_id),
                warehouse_id,
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
    Ok(commits)
}

pub(crate) async fn authorized_table_ident_to_id<C: Catalog, A: Authorizer>(
    authorizer: A,
    metadata: &RequestMetadata,
    warehouse_id: WarehouseIdent,
    table_ident: &TableIdent,
    list_flags: ListFlags,
    action: impl From<&CatalogTableAction> + std::fmt::Display + Send,
    transaction: <C::Transaction as Transaction<C::State>>::Transaction<'_>,
) -> Result<TableIdentUuid> {
    authorizer
        .require_warehouse_action(metadata, warehouse_id, &CatalogWarehouseAction::CanUse)
        .await?;
    let table_id = C::table_to_id(warehouse_id, table_ident, list_flags, transaction).await; // We can't fail before AuthZ
    authorizer
        .require_table_action(metadata, table_id, action)
        .await
        .map_err(set_not_found_status_code)
}

pub(crate) fn extract_count_from_metadata_location(location: &Location) -> Option<usize> {
    let last_segment = location
        .as_str()
        .trim_end_matches('/')
        .split('/')
        .last()
        .unwrap_or(location.as_str());

    if let Some((_whole, version, _metadata_id)) = lazy_regex::regex_captures!(
        r"^(\d+)-([\w-]{36})(?:\.\w+)?\.metadata\.json",
        last_segment
    ) {
        version.parse().ok()
    } else {
        None
    }
}

struct CommitContext {
    pub new_metadata: iceberg::spec::TableMetadata,
    pub new_metadata_location: Location,
    pub previous_metadata: iceberg::spec::TableMetadata,
    pub updates: Vec<TableUpdate>,
    pub new_compression_codec: CompressionCodec,
    pub number_expired_metadata_log_entries: usize,
    pub number_added_metadata_log_entries: usize,
}

impl CommitContext {
    fn commit(&self) -> TableCommit {
        let diffs = calculate_diffs(
            &self.new_metadata,
            &self.previous_metadata,
            self.number_added_metadata_log_entries,
            self.number_expired_metadata_log_entries,
        );

        TableCommit {
            diffs,
            new_metadata: self.new_metadata.clone(),
            new_metadata_location: self.new_metadata_location.clone(),
            updates: self.updates.clone(),
        }
    }
}

#[allow(clippy::too_many_lines)]
fn calculate_diffs(
    new_metadata: &TableMetadata,
    previous_metadata: &TableMetadata,
    added_metadata_log: usize,
    expired_metadata_logs: usize,
) -> TableMetadataDiffs {
    let new_snaps = new_metadata
        .snapshots()
        .map(|s| s.snapshot_id())
        .collect::<FxHashSet<i64>>();
    let old_snaps = previous_metadata
        .snapshots()
        .map(|s| s.snapshot_id())
        .collect::<FxHashSet<i64>>();
    let removed_snaps = old_snaps
        .difference(&new_snaps)
        .copied()
        .collect::<Vec<i64>>();
    let added_snapshots = new_snaps
        .difference(&old_snaps)
        .copied()
        .collect::<Vec<i64>>();

    let old_schemas = previous_metadata
        .schemas_iter()
        .map(|s| s.schema_id())
        .collect::<FxHashSet<SchemaId>>();
    let new_schemas = new_metadata
        .schemas_iter()
        .map(|s| s.schema_id())
        .collect::<FxHashSet<SchemaId>>();
    let removed_schemas = old_schemas
        .difference(&new_schemas)
        .copied()
        .collect::<Vec<SchemaId>>();
    let added_schemas = new_schemas
        .difference(&old_schemas)
        .copied()
        .collect::<Vec<SchemaId>>();
    let new_current_schema_id = (previous_metadata.current_schema_id()
        != new_metadata.current_schema_id())
    .then_some(new_metadata.current_schema_id());

    let old_specs = previous_metadata
        .partition_specs_iter()
        .map(|s| s.spec_id())
        .collect::<FxHashSet<i32>>();
    let new_specs = new_metadata
        .partition_specs_iter()
        .map(|s| s.spec_id())
        .collect::<FxHashSet<i32>>();
    let removed_specs = old_specs
        .difference(&new_specs)
        .copied()
        .collect::<Vec<i32>>();
    let added_partition_specs = new_specs
        .difference(&old_specs)
        .copied()
        .collect::<Vec<i32>>();
    let default_partition_spec_id = (previous_metadata.default_partition_spec_id()
        != new_metadata.default_partition_spec_id())
    .then_some(new_metadata.default_partition_spec_id());

    let old_sort_orders = previous_metadata
        .sort_orders_iter()
        .map(|s| s.order_id)
        .collect::<FxHashSet<i64>>();
    let new_sort_orders = new_metadata
        .sort_orders_iter()
        .map(|s| s.order_id)
        .collect::<FxHashSet<i64>>();
    let removed_sort_orders = old_sort_orders
        .difference(&new_sort_orders)
        .copied()
        .collect::<Vec<i64>>();
    let added_sort_orders = new_sort_orders
        .difference(&old_sort_orders)
        .copied()
        .collect::<Vec<i64>>();
    let default_sort_order_id = (previous_metadata.default_sort_order_id()
        != new_metadata.default_sort_order_id())
    .then_some(new_metadata.default_sort_order_id());

    let head_of_snapshot_log_changed =
        previous_metadata.history().last() != new_metadata.history().last();

    let n_removed_snapshot_log = previous_metadata.history().len().saturating_sub(
        new_metadata
            .history()
            .len()
            .saturating_sub(usize::from(head_of_snapshot_log_changed)),
    );

    let old_stats = previous_metadata
        .statistics_iter()
        .map(|s| s.snapshot_id)
        .collect::<FxHashSet<_>>();
    let new_stats = new_metadata
        .statistics_iter()
        .map(|s| s.snapshot_id)
        .collect::<FxHashSet<_>>();
    let removed_stats = old_stats
        .difference(&new_stats)
        .copied()
        .collect::<Vec<_>>();
    let added_stats = new_stats
        .difference(&old_stats)
        .copied()
        .collect::<Vec<_>>();

    let old_partition_stats = previous_metadata
        .partition_statistics_iter()
        .map(|s| s.snapshot_id)
        .collect::<FxHashSet<_>>();
    let new_partition_stats = new_metadata
        .partition_statistics_iter()
        .map(|s| s.snapshot_id)
        .collect::<FxHashSet<_>>();
    let removed_partition_stats = old_partition_stats
        .difference(&new_partition_stats)
        .copied()
        .collect::<Vec<_>>();
    let added_partition_stats = new_partition_stats
        .difference(&old_partition_stats)
        .copied()
        .collect::<Vec<_>>();

    TableMetadataDiffs {
        removed_snapshots: removed_snaps,
        added_snapshots,
        removed_schemas,
        added_schemas,
        new_current_schema_id,
        removed_partition_specs: removed_specs,
        added_partition_specs,
        default_partition_spec_id,
        removed_sort_orders,
        added_sort_orders,
        default_sort_order_id,
        head_of_snapshot_log_changed,
        n_removed_snapshot_log,
        expired_metadata_logs,
        added_metadata_log,
        added_stats,
        removed_stats,
        added_partition_stats,
        removed_partition_stats,
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TableMetadataDiffs {
    pub(crate) removed_snapshots: Vec<i64>,
    pub(crate) added_snapshots: Vec<i64>,
    pub(crate) removed_schemas: Vec<i32>,
    pub(crate) added_schemas: Vec<i32>,
    pub(crate) new_current_schema_id: Option<i32>,
    pub(crate) removed_partition_specs: Vec<i32>,
    pub(crate) added_partition_specs: Vec<i32>,
    pub(crate) default_partition_spec_id: Option<i32>,
    pub(crate) removed_sort_orders: Vec<i64>,
    pub(crate) added_sort_orders: Vec<i64>,
    pub(crate) default_sort_order_id: Option<i64>,
    pub(crate) head_of_snapshot_log_changed: bool,
    pub(crate) n_removed_snapshot_log: usize,
    pub(crate) expired_metadata_logs: usize,
    pub(crate) added_metadata_log: usize,
    pub(crate) added_stats: Vec<i64>,
    pub(crate) removed_stats: Vec<i64>,
    pub(crate) added_partition_stats: Vec<i64>,
    pub(crate) removed_partition_stats: Vec<i64>,
}

pub(crate) fn determine_table_ident(
    parameters_ident: TableIdent,
    request_ident: Option<&TableIdent>,
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

fn require_not_staged<T>(metadata_location: Option<&T>) -> Result<()> {
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

pub(crate) fn create_table_request_into_table_metadata(
    table_id: TableIdentUuid,
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

#[cfg(test)]
mod test {
    use crate::api::iceberg::types::{PageToken, Prefix};
    use crate::api::iceberg::v1::tables::TablesService as _;
    use crate::api::iceberg::v1::{
        DataAccess, ListTablesQuery, NamespaceParameters, TableParameters,
    };
    use crate::api::management::v1::warehouse::TabularDeleteProfile;
    use crate::api::ApiContext;
    use crate::catalog::test::random_request_metadata;
    use crate::catalog::CatalogServer;
    use crate::implementations::postgres::{PostgresCatalog, SecretsState};
    use crate::service::authz::implementations::openfga::tests::ObjectHidingMock;
    use crate::service::authz::AllowAllAuthorizer;
    use crate::service::{State, UserId};

    use http::StatusCode;
    use iceberg::spec::{
        NestedField, Operation, PrimitiveType, Schema, Snapshot, SnapshotReference,
        SnapshotRetention, Summary, Transform, Type, UnboundPartitionField, UnboundPartitionSpec,
        MAIN_BRANCH, PROPERTY_METADATA_PREVIOUS_VERSIONS_MAX,
    };
    use iceberg::TableIdent;
    use iceberg_ext::catalog::rest::{
        CommitTableRequest, CreateNamespaceResponse, CreateTableRequest, LoadTableResult,
    };
    use itertools::Itertools;
    use sqlx::PgPool;
    use std::collections::HashMap;
    use uuid::Uuid;

    use crate::catalog::tables::validate_table_properties;
    use crate::catalog::test::impl_pagination_tests;
    use crate::service::authz::implementations::openfga::OpenFGAAuthorizer;
    use iceberg_ext::configs::Location;
    use std::str::FromStr;

    #[test]
    fn test_mixed_case_properties() {
        let properties = ["a".to_string(), "B".to_string()];
        assert!(validate_table_properties(properties.iter()).is_ok());
    }

    #[test]
    fn test_extract_count_from_metadata_location() {
        let location = Location::from_str("s3://path/to/table/metadata/00000-d0407fb2-1112-4944-bb88-c68ae697e2b4.gz.metadata.json").unwrap();
        let count = super::extract_count_from_metadata_location(&location).unwrap();
        assert_eq!(count, 0);

        let location = Location::from_str("s3://path/to/table/metadata/00010-d0407fb2-1112-4944-bb88-c68ae697e2b4.gz.metadata.json").unwrap();
        let count = super::extract_count_from_metadata_location(&location).unwrap();
        assert_eq!(count, 10);

        let location = Location::from_str(
            "s3://path/to/table/metadata/1-d0407fb2-1112-4944-bb88-c68ae697e2b4.gz.metadata.json",
        )
        .unwrap();
        let count = super::extract_count_from_metadata_location(&location).unwrap();
        assert_eq!(count, 1);

        let location = Location::from_str(
            "s3://path/to/table/metadata/10000010-d0407fb2-1112-4944-bb88-c68ae697e2b4.gz.metadata.json",
        )
            .unwrap();
        let count = super::extract_count_from_metadata_location(&location).unwrap();
        assert_eq!(count, 10_000_010);

        let location = Location::from_str(
            "s3://path/to/table/metadata/10000010-d0407fb2-1112-4944-bb88-c68ae697e2b4.metadata.json",
        )
            .unwrap();
        let count = super::extract_count_from_metadata_location(&location).unwrap();
        assert_eq!(count, 10_000_010);

        let location = Location::from_str(
            "s3://path/to/table/metadata/d0407fb2-1112-4944-bb88-c68ae697e2b4.metadata.json",
        )
        .unwrap();
        let count = super::extract_count_from_metadata_location(&location);
        assert!(count.is_none());
    }

    fn create_request(table_name: Option<String>) -> CreateTableRequest {
        CreateTableRequest {
            name: table_name.unwrap_or("my_table".to_string()),
            location: None,
            schema: Schema::builder()
                .with_fields(vec![
                    NestedField::required(
                        1,
                        "id",
                        iceberg::spec::Type::Primitive(PrimitiveType::Int),
                    )
                    .into(),
                    NestedField::required(
                        2,
                        "name",
                        iceberg::spec::Type::Primitive(PrimitiveType::String),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
            partition_spec: Some(UnboundPartitionSpec::builder().build()),
            write_order: None,
            stage_create: Some(false),
            properties: None,
        }
    }

    fn partition_spec() -> UnboundPartitionSpec {
        UnboundPartitionSpec::builder()
            .with_spec_id(0)
            .add_partition_field(2, "y", Transform::Identity)
            .unwrap()
            .build()
    }

    #[sqlx::test]
    async fn test_set_properties_commit_table(pool: sqlx::PgPool) {
        let (ctx, ns, ns_params, table) = commit_test_setup(pool).await;

        let table_metadata = table
            .metadata
            .into_builder(table.metadata_location)
            .set_properties(HashMap::from([
                ("p1".into(), "v2".into()),
                ("p2".into(), "v2".into()),
            ]))
            .unwrap()
            .build()
            .unwrap();
        let updates = table_metadata.changes;
        let _ = super::commit_tables_internal(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(TableIdent {
                        namespace: ns.namespace.clone(),
                        name: "tab-1".to_string(),
                    }),
                    requirements: vec![],
                    updates,
                }],
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
        .new_metadata;

        let tab = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix,
                table: TableIdent {
                    namespace: ns.namespace.clone(),
                    name: "tab-1".to_string(),
                },
            },
            DataAccess::none(),
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();
        assert_eq!(tab.metadata, table_metadata.metadata);
    }

    fn schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "x", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "y", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(3, "z", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap()
    }

    #[sqlx::test]
    async fn test_add_partition_spec_commit_table(pool: sqlx::PgPool) {
        let (ctx, ns, ns_params, table) = commit_test_setup(pool).await;

        let added_spec = UnboundPartitionSpec::builder()
            .with_spec_id(10)
            .add_partition_fields(vec![
                UnboundPartitionField {
                    // The previous field - has field_id set
                    name: "y".to_string(),
                    transform: Transform::Identity,
                    source_id: 2,
                    field_id: Some(1000),
                },
                UnboundPartitionField {
                    // A new field without field id - should still be without field id in changes
                    name: "z".to_string(),
                    transform: Transform::Identity,
                    source_id: 3,
                    field_id: None,
                },
            ])
            .unwrap()
            .build();

        let table_metadata = table
            .metadata
            .into_builder(table.metadata_location)
            .add_schema(schema())
            .set_current_schema(-1)
            .unwrap()
            .add_partition_spec(partition_spec())
            .unwrap()
            .add_partition_spec(added_spec.clone())
            .unwrap()
            .build()
            .unwrap();

        let updates = table_metadata.changes;
        let _ = super::commit_tables_internal(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(TableIdent {
                        namespace: ns.namespace.clone(),
                        name: "tab-1".to_string(),
                    }),
                    requirements: vec![],
                    updates,
                }],
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        let tab = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix,
                table: TableIdent {
                    namespace: ns.namespace.clone(),
                    name: "tab-1".to_string(),
                },
            },
            DataAccess::none(),
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        assert_eq!(tab.metadata, table_metadata.metadata);
    }

    #[sqlx::test]
    async fn test_set_default_partition_spec(pool: PgPool) {
        let (ctx, ns, ns_params, table) = commit_test_setup(pool).await;

        let added_spec = UnboundPartitionSpec::builder()
            .with_spec_id(10)
            .add_partition_field(1, "y_bucket[2]", Transform::Bucket(2))
            .unwrap()
            .build();

        let table_metadata = table
            .metadata
            .into_builder(table.metadata_location)
            .add_partition_spec(added_spec)
            .unwrap()
            .set_default_partition_spec(-1)
            .unwrap()
            .build()
            .unwrap();
        let updates = table_metadata.changes;

        let _ = super::commit_tables_internal(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(TableIdent {
                        namespace: ns.namespace.clone(),
                        name: "tab-1".to_string(),
                    }),
                    requirements: vec![],
                    updates,
                }],
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
        .new_metadata;

        let tab = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix,
                table: TableIdent {
                    namespace: ns.namespace.clone(),
                    name: "tab-1".to_string(),
                },
            },
            DataAccess::none(),
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();
        assert_eq!(tab.metadata, table_metadata.metadata);
    }

    #[sqlx::test]
    async fn test_set_ref(pool: PgPool) {
        let (ctx, ns, ns_params, table) = commit_test_setup(pool).await;
        let last_updated = table.metadata.last_updated_ms();
        let builder = table.metadata.into_builder(table.metadata_location);

        let snapshot = Snapshot::builder()
            .with_snapshot_id(1)
            .with_timestamp_ms(last_updated + 1)
            .with_sequence_number(0)
            .with_schema_id(0)
            .with_manifest_list("/snap-1.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::from_iter(vec![
                    (
                        "spark.app.id".to_string(),
                        "local-1662532784305".to_string(),
                    ),
                    ("added-data-files".to_string(), "4".to_string()),
                    ("added-records".to_string(), "4".to_string()),
                    ("added-files-size".to_string(), "6001".to_string()),
                ]),
            })
            .build();

        let builder = builder
            .add_snapshot(snapshot.clone())
            .unwrap()
            .set_ref(
                MAIN_BRANCH,
                SnapshotReference {
                    snapshot_id: 1,
                    retention: SnapshotRetention::Branch {
                        min_snapshots_to_keep: Some(10),
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                },
            )
            .unwrap()
            .build()
            .unwrap();
        let updates = builder.changes;

        let _ = super::commit_tables_internal(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(TableIdent {
                        namespace: ns.namespace.clone(),
                        name: "tab-1".to_string(),
                    }),
                    requirements: vec![],
                    updates,
                }],
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        let tab = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix,
                table: TableIdent {
                    namespace: ns.namespace.clone(),
                    name: "tab-1".to_string(),
                },
            },
            DataAccess::none(),
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();
        assert_eq!(tab.metadata, builder.metadata);
    }

    #[sqlx::test]
    async fn test_expire_metadata_log(pool: PgPool) {
        let (ctx, ns, ns_params, table) = commit_test_setup(pool).await;
        let table_ident = TableIdent {
            namespace: ns.namespace.clone(),
            name: "tab-1".to_string(),
        };
        let builder = table
            .metadata
            .into_builder(table.metadata_location)
            .set_properties(HashMap::from_iter([(
                PROPERTY_METADATA_PREVIOUS_VERSIONS_MAX.to_string(),
                "2".to_string(),
            )]))
            .unwrap()
            .build()
            .unwrap();
        let _ = super::commit_tables_internal(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(table_ident.clone()),
                    requirements: vec![],
                    updates: builder.changes,
                }],
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        let tab = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: table_ident.clone(),
            },
            DataAccess::none(),
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();
        assert_eq!(tab.metadata, builder.metadata);

        let builder = builder
            .metadata
            .into_builder(tab.metadata_location)
            .set_properties(HashMap::from_iter(vec![(
                "change_nr".to_string(),
                "1".to_string(),
            )]))
            .unwrap()
            .build()
            .unwrap();

        let committed = super::commit_tables_internal(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(table_ident.clone()),
                    requirements: vec![],
                    updates: builder.changes,
                }],
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

        let tab = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: table_ident.clone(),
            },
            DataAccess::none(),
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        assert_eq!(tab.metadata, builder.metadata);

        let builder = committed
            .new_metadata
            .into_builder(tab.metadata_location)
            .set_properties(HashMap::from_iter(vec![(
                "change_nr".to_string(),
                "2".to_string(),
            )]))
            .unwrap()
            .build()
            .unwrap();

        let _ = super::commit_tables_internal(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(table_ident.clone()),
                    requirements: vec![],
                    updates: builder.changes,
                }],
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

        let tab = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix,
                table: table_ident.clone(),
            },
            DataAccess::none(),
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        assert_eq!(tab.metadata, builder.metadata);
    }

    #[sqlx::test]
    async fn test_remove_snapshot_commit(pg_pool: PgPool) {
        let (ctx, ns, ns_params, table) = commit_test_setup(pg_pool).await;
        let table_ident = TableIdent {
            namespace: ns.namespace.clone(),
            name: "tab-1".to_string(),
        };
        let last_updated = table.metadata.last_updated_ms();
        let builder = table.metadata.into_builder(table.metadata_location);

        let snap = Snapshot::builder()
            .with_snapshot_id(1)
            .with_timestamp_ms(last_updated + 1)
            .with_sequence_number(0)
            .with_schema_id(0)
            .with_manifest_list("/snap-1.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::from_iter(vec![
                    (
                        "spark.app.id".to_string(),
                        "local-1662532784305".to_string(),
                    ),
                    ("added-data-files".to_string(), "4".to_string()),
                    ("added-records".to_string(), "4".to_string()),
                    ("added-files-size".to_string(), "6001".to_string()),
                ]),
            })
            .build();

        let builder = builder
            .add_snapshot(snap)
            .unwrap()
            .set_ref(
                MAIN_BRANCH,
                SnapshotReference {
                    snapshot_id: 1,
                    retention: SnapshotRetention::Branch {
                        min_snapshots_to_keep: Some(10),
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                },
            )
            .unwrap()
            .build()
            .unwrap();

        let _ = super::commit_tables_internal(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(table_ident.clone()),
                    requirements: vec![],
                    updates: builder.changes,
                }],
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        let tab = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: table_ident.clone(),
            },
            DataAccess::none(),
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();
        assert_eq!(tab.metadata.history(), builder.metadata.history());
        assert_eq!(tab.metadata, builder.metadata);

        assert_json_diff::assert_json_eq!(
            serde_json::to_value(tab.metadata.clone()).unwrap(),
            serde_json::to_value(builder.metadata.clone()).unwrap()
        );

        let last_updated = tab.metadata.last_updated_ms();
        let builder = builder.metadata.into_builder(tab.metadata_location);

        let snap = Snapshot::builder()
            .with_snapshot_id(2)
            .with_parent_snapshot_id(Some(1))
            .with_timestamp_ms(last_updated + 1)
            .with_sequence_number(1)
            .with_schema_id(0)
            .with_manifest_list("/snap-2.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::from_iter(vec![
                    (
                        "spark.app.id".to_string(),
                        "local-1662532784305".to_string(),
                    ),
                    ("added-data-files".to_string(), "4".to_string()),
                    ("added-records".to_string(), "4".to_string()),
                    ("added-files-size".to_string(), "6001".to_string()),
                ]),
            })
            .build();

        let builder = builder.add_snapshot(snap).unwrap().build().unwrap();

        let updates = builder.changes;

        let _ = super::commit_tables_internal(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(table_ident.clone()),
                    requirements: vec![],
                    updates,
                }],
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        let tab = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: table_ident.clone(),
            },
            DataAccess::none(),
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        assert_eq!(tab.metadata, builder.metadata);

        let last_updated = tab.metadata.last_updated_ms();
        let builder = builder.metadata.into_builder(tab.metadata_location);

        let snap = Snapshot::builder()
            .with_snapshot_id(3)
            .with_timestamp_ms(last_updated + 1)
            .with_parent_snapshot_id(Some(2))
            .with_sequence_number(2)
            .with_schema_id(0)
            .with_manifest_list("/snap-2.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::from_iter(vec![
                    (
                        "spark.app.id".to_string(),
                        "local-1662532784305".to_string(),
                    ),
                    ("added-data-files".to_string(), "4".to_string()),
                    ("added-records".to_string(), "4".to_string()),
                    ("added-files-size".to_string(), "6001".to_string()),
                ]),
            })
            .build();

        let builder = builder.add_snapshot(snap).unwrap().build().unwrap();

        let updates = builder.changes;

        let _ = super::commit_tables_internal(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(table_ident.clone()),
                    requirements: vec![],
                    updates,
                }],
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        let tab = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: table_ident.clone(),
            },
            DataAccess::none(),
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        assert_eq!(tab.metadata, builder.metadata);

        let builder = builder
            .metadata
            .into_builder(tab.metadata_location)
            .remove_snapshots(&[2])
            .build()
            .unwrap();

        let updates = builder.changes;

        let _ = super::commit_tables_internal(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(table_ident.clone()),
                    requirements: vec![],
                    updates,
                }],
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        let tab = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: table_ident.clone(),
            },
            DataAccess::none(),
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();
        assert_eq!(tab.metadata.history(), builder.metadata.history());
        assert_eq!(
            tab.metadata
                .snapshots()
                .sorted_by_key(|s| s.snapshot_id())
                .collect_vec(),
            builder
                .metadata
                .snapshots()
                .sorted_by_key(|s| s.snapshot_id())
                .collect_vec()
        );
        assert_eq!(tab.metadata, builder.metadata);
    }

    async fn commit_test_setup(
        pool: PgPool,
    ) -> (
        ApiContext<State<AllowAllAuthorizer, PostgresCatalog, SecretsState>>,
        CreateNamespaceResponse,
        NamespaceParameters,
        LoadTableResult,
    ) {
        let (ctx, ns, ns_params, _) = table_test_setup(pool).await;
        let table = CatalogServer::create_table(
            ns_params.clone(),
            create_request(Some("tab-1".to_string())),
            DataAccess {
                vended_credentials: true,
                remote_signing: false,
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();
        (ctx, ns, ns_params, table)
    }

    async fn table_test_setup(
        pool: PgPool,
    ) -> (
        ApiContext<State<AllowAllAuthorizer, PostgresCatalog, SecretsState>>,
        CreateNamespaceResponse,
        NamespaceParameters,
        String,
    ) {
        let prof = crate::catalog::test::test_io_profile();
        let base_loc = prof.base_location().unwrap().to_string();
        let (ctx, warehouse) = crate::catalog::test::setup(
            pool.clone(),
            prof,
            None,
            AllowAllAuthorizer,
            TabularDeleteProfile::Hard {},
            None,
        )
        .await;
        let ns = crate::catalog::test::create_ns(
            ctx.clone(),
            warehouse.warehouse_id.to_string(),
            "ns1".to_string(),
        )
        .await;
        let ns_params = NamespaceParameters {
            prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
            namespace: ns.namespace.clone(),
        };
        (ctx, ns, ns_params, base_loc)
    }

    #[sqlx::test]
    async fn test_cannot_create_table_at_same_location(pool: PgPool) {
        let (ctx, _, ns_params, base_location) = table_test_setup(pool).await;
        let tmp_id = Uuid::now_v7();
        let mut create_request_1 = create_request(Some("tab-1".to_string()));
        create_request_1.location = Some(format!("{base_location}/{tmp_id}/bucket/"));
        let mut create_request_2 = create_request(Some("tab-2".to_string()));
        create_request_2.location = Some(format!("{base_location}/{tmp_id}/bucket/"));

        let _ = CatalogServer::create_table(
            ns_params.clone(),
            create_request_1,
            DataAccess::none(),
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        let e = CatalogServer::create_table(
            ns_params.clone(),
            create_request_2,
            DataAccess::none(),
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .expect_err("Table was created at same location which should not be possible");
        assert_eq!(e.error.code, StatusCode::BAD_REQUEST, "{e:?}");
        assert_eq!(e.error.r#type.as_str(), "LocationAlreadyTaken");
    }

    #[sqlx::test]
    async fn test_cannot_create_staged_tables_at_sublocations(pool: PgPool) {
        let (ctx, _, ns_params, base_location) = table_test_setup(pool).await;
        let tmp_id = Uuid::now_v7();
        let mut create_request_1 = create_request(Some("tab-1".to_string()));
        create_request_1.stage_create = Some(true);
        create_request_1.location = Some(format!("{base_location}/{tmp_id}/bucket/inner"));
        let mut create_request_2 = create_request(Some("tab-2".to_string()));
        create_request_2.stage_create = Some(true);
        create_request_2.location = Some(format!("{base_location}/{tmp_id}/bucket/"));
        let _ = CatalogServer::create_table(
            ns_params.clone(),
            create_request_1,
            DataAccess::none(),
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        let e = CatalogServer::create_table(
            ns_params.clone(),
            create_request_2,
            DataAccess::none(),
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .expect_err("Staged table could be created at sublocation which should not be possible");
        assert_eq!(e.error.code, StatusCode::BAD_REQUEST, "{e:?}");
        assert_eq!(e.error.r#type.as_str(), "LocationAlreadyTaken");
    }

    #[sqlx::test]
    async fn test_cannot_create_tables_at_sublocations(pool: PgPool) {
        let (ctx, _, ns_params, base_location) = table_test_setup(pool).await;
        let tmp_id = Uuid::now_v7();

        let mut create_request_1 = create_request(Some("tab-1".to_string()));
        create_request_1.location = Some(format!("{base_location}/{tmp_id}/bucket/"));
        let mut create_request_2 = create_request(Some("tab-2".to_string()));
        create_request_2.location = Some(format!("{base_location}/{tmp_id}/bucket/sublocation"));
        let _ = CatalogServer::create_table(
            ns_params.clone(),
            create_request_1,
            DataAccess::none(),
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        let e = CatalogServer::create_table(
            ns_params.clone(),
            create_request_2,
            DataAccess::none(),
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .expect_err("Staged table could be created at sublocation which should not be possible");
        assert_eq!(e.error.code, StatusCode::BAD_REQUEST, "{e:?}");
        assert_eq!(e.error.r#type.as_str(), "LocationAlreadyTaken");
    }

    async fn pagination_test_setup(
        pool: PgPool,
        n_tables: usize,
        hidden_ranges: &[(usize, usize)],
    ) -> (
        ApiContext<State<OpenFGAAuthorizer, PostgresCatalog, SecretsState>>,
        NamespaceParameters,
    ) {
        let prof = crate::catalog::test::test_io_profile();
        let base_location = prof.base_location().unwrap();
        let hiding_mock = ObjectHidingMock::new();
        let authz = hiding_mock.to_authorizer();

        let (ctx, warehouse) = crate::catalog::test::setup(
            pool.clone(),
            prof,
            None,
            authz,
            TabularDeleteProfile::Hard {},
            Some(UserId::OIDC("test-user-id".to_string())),
        )
        .await;
        let ns = crate::catalog::test::create_ns(
            ctx.clone(),
            warehouse.warehouse_id.to_string(),
            "ns1".to_string(),
        )
        .await;
        let ns_params = NamespaceParameters {
            prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
            namespace: ns.namespace.clone(),
        };
        for i in 0..n_tables {
            let mut create_request = create_request(Some(format!("{i}")));
            create_request.location = Some(format!("{base_location}/bucket/{i}"));
            let tab = CatalogServer::create_table(
                ns_params.clone(),
                create_request,
                DataAccess::none(),
                ctx.clone(),
                random_request_metadata(),
            )
            .await
            .unwrap();
            for (start, end) in hidden_ranges.iter().copied() {
                if i >= start && i < end {
                    hiding_mock.hide(&format!("table:{}", tab.metadata.uuid()));
                }
            }
        }

        (ctx, ns_params)
    }

    impl_pagination_tests!(
        table,
        pagination_test_setup,
        CatalogServer,
        ListTablesQuery,
        identifiers,
        |tid| { tid.name }
    );

    #[sqlx::test]
    async fn test_table_pagination(pool: sqlx::PgPool) {
        let prof = crate::catalog::test::test_io_profile();

        let hiding_mock = ObjectHidingMock::new();
        let authz = hiding_mock.to_authorizer();

        let (ctx, warehouse) = crate::catalog::test::setup(
            pool.clone(),
            prof,
            None,
            authz,
            TabularDeleteProfile::Hard {},
            Some(UserId::OIDC("test-user-id".to_string())),
        )
        .await;
        let ns = crate::catalog::test::create_ns(
            ctx.clone(),
            warehouse.warehouse_id.to_string(),
            "ns1".to_string(),
        )
        .await;
        let ns_params = NamespaceParameters {
            prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
            namespace: ns.namespace.clone(),
        };
        // create 10 staged tables
        for i in 0..10 {
            let _ = CatalogServer::create_table(
                ns_params.clone(),
                create_request(Some(format!("tab-{i}"))),
                DataAccess {
                    vended_credentials: true,
                    remote_signing: false,
                },
                ctx.clone(),
                random_request_metadata(),
            )
            .await
            .unwrap();
        }

        // list 1 more than existing tables
        let all = CatalogServer::list_tables(
            ns_params.clone(),
            ListTablesQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(11),
                return_uuids: true,
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();
        assert_eq!(all.identifiers.len(), 10);

        // list exactly amount of existing tables
        let all = CatalogServer::list_tables(
            ns_params.clone(),
            ListTablesQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(10),
                return_uuids: true,
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();
        assert_eq!(all.identifiers.len(), 10);

        // next page is empty
        let next = CatalogServer::list_tables(
            ns_params.clone(),
            ListTablesQuery {
                page_token: PageToken::Present(all.next_page_token.unwrap()),
                page_size: Some(10),
                return_uuids: true,
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        assert_eq!(next.identifiers.len(), 0);
        assert!(next.next_page_token.is_none());

        let first_six = CatalogServer::list_tables(
            ns_params.clone(),
            ListTablesQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(6),
                return_uuids: true,
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();
        assert_eq!(first_six.identifiers.len(), 6);
        assert!(first_six.next_page_token.is_some());
        let first_six_items = first_six
            .identifiers
            .iter()
            .map(|i| i.name.clone())
            .sorted()
            .collect::<Vec<_>>();

        for (i, item) in first_six_items.iter().enumerate().take(6) {
            assert_eq!(item, &format!("tab-{i}"));
        }

        let next_four = CatalogServer::list_tables(
            ns_params.clone(),
            ListTablesQuery {
                page_token: PageToken::Present(first_six.next_page_token.unwrap()),
                page_size: Some(6),
                return_uuids: true,
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();
        assert_eq!(next_four.identifiers.len(), 4);
        // page-size > number of items left -> no next page
        assert!(next_four.next_page_token.is_none());

        let next_four_items = next_four
            .identifiers
            .iter()
            .map(|i| i.name.clone())
            .sorted()
            .collect::<Vec<_>>();

        for (idx, i) in (6..10).enumerate() {
            assert_eq!(next_four_items[idx], format!("tab-{i}"));
        }

        let mut ids = all.table_uuids.unwrap();
        ids.sort();
        for t in ids.iter().take(6).skip(4) {
            hiding_mock.hide(&format!("table:{t}"));
        }

        let page = CatalogServer::list_tables(
            ns_params.clone(),
            ListTablesQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(5),
                return_uuids: true,
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        assert_eq!(page.identifiers.len(), 5);
        assert!(page.next_page_token.is_some());
        let page_items = page
            .identifiers
            .iter()
            .map(|i| i.name.clone())
            .sorted()
            .collect::<Vec<_>>();
        for (i, item) in page_items.iter().enumerate() {
            let tab_id = if i > 3 { i + 2 } else { i };
            assert_eq!(item, &format!("tab-{tab_id}"));
        }

        let next_page = CatalogServer::list_tables(
            ns_params.clone(),
            ListTablesQuery {
                page_token: PageToken::Present(page.next_page_token.unwrap()),
                page_size: Some(6),
                return_uuids: true,
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        assert_eq!(next_page.identifiers.len(), 3);

        let next_page_items = next_page
            .identifiers
            .iter()
            .map(|i| i.name.clone())
            .sorted()
            .collect::<Vec<_>>();

        for (idx, i) in (7..10).enumerate() {
            assert_eq!(next_page_items[idx], format!("tab-{i}"));
        }
    }
}
