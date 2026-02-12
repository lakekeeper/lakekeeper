use std::{
    collections::{BTreeMap, HashMap, HashSet},
    str::FromStr as _,
    sync::Arc,
};

use futures::FutureExt;
use http::StatusCode;
use iceberg::{
    NamespaceIdent, TableUpdate,
    spec::{
        MetadataLog, SchemaId, TableMetadata, TableMetadataBuildResult, TableMetadataRef,
        TableProperties,
    },
};
use iceberg_ext::{
    catalog::rest::{ETag, IcebergErrorResponse, LoadCredentialsResponse, StorageCredential},
    configs::ParseFromStr,
};
use itertools::Itertools;
use lakekeeper_io::Location;
use serde::Serialize;
use uuid::Uuid;
pub(crate) mod create_table;
mod load_table;
mod rename_table;
use super::{
    CatalogServer,
    commit_tables::apply_commit,
    io::{delete_file, read_metadata_file, write_file},
    maybe_get_secret,
    namespace::validate_namespace_ident,
    require_warehouse_id,
};
use crate::{
    WarehouseId, XXHashSet,
    api::{
        iceberg::{
            types::DropParams,
            v1::{
                ApiContext, CommitTableRequest, CommitTableResponse, CommitTransactionRequest,
                CreateTableRequest, DataAccess, ErrorModel, ListTablesQuery, ListTablesResponse,
                LoadTableResult, LoadTableResultOrNotModified, NamespaceParameters, Prefix,
                RegisterTableRequest, RenameTableRequest, Result, TableIdent, TableParameters,
                tables::{DataAccessMode, LoadTableFilters},
            },
        },
        management::v1::{DeleteKind, warehouse::TabularDeleteProfile},
    },
    request_metadata::RequestMetadata,
    server::{
        self,
        compression_codec::{CompressionCodec, PROPERTY_METADATA_COMPRESSION_CODEC},
        tabular::list_entities,
    },
    service::{
        AuthZTableInfo as _, CONCURRENT_UPDATE_ERROR_TYPE, CachePolicy, CatalogNamespaceOps,
        CatalogStore, CatalogTableOps, CatalogTabularOps, CatalogWarehouseOps, NamedEntity,
        ResolvedWarehouse, State, TableCommit, TableCreation, TableId, TableIdentOrId, TableInfo,
        TabularId, TabularInfo, TabularListFlags, TabularNotFound, Transaction, WarehouseStatus,
        authz::{
            AuthZCannotSeeNamespace, AuthZCannotSeeTable, AuthZTableActionForbidden, AuthZTableOps,
            Authorizer, AuthzNamespaceOps, AuthzWarehouseOps, CatalogNamespaceAction,
            CatalogTableAction, CatalogWarehouseAction, AuthZError,
            RequireNamespaceActionError, RequireTableActionError,
            refresh_warehouse_and_namespace_if_needed,
        },
        contract_verification::{ContractVerification, ContractVerificationOutcome},
        events::{
            APIEventCommitContext, APIEventContext, CommitTransactionEvent,
            context::{ResolvedNamespace, ResolvedTable},
        },
        require_namespace_for_tabular,
        secrets::SecretStore,
        storage::{StorageLocations as _, StoragePermissions},
        tasks::{
            ScheduleTaskMetadata, TaskEntity, WarehouseTaskEntityId,
            tabular_expiration_queue::{TabularExpirationPayload, TabularExpirationTask},
            tabular_purge_queue::{TabularPurgePayload, TabularPurgeTask},
        },
    },
};

const PROPERTY_METADATA_DELETE_AFTER_COMMIT_ENABLED: &str =
    "write.metadata.delete-after-commit.enabled";
const PROPERTY_METADATA_DELETE_AFTER_COMMIT_ENABLED_DEFAULT: bool = true;

pub(crate) const MAX_RETRIES_ON_CONCURRENT_UPDATE: usize = 2;

#[async_trait::async_trait]
impl<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>
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
        let NamespaceParameters {
            namespace: provided_namespace,
            prefix,
        } = parameters;
        let warehouse_id = require_warehouse_id(prefix.as_ref())?;
        validate_namespace_ident(&provided_namespace)?;

        // ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz;

        let event_ctx = APIEventContext::for_namespace(
            Arc::new(request_metadata),
            state.v1_state.events,
            warehouse_id,
            provided_namespace.clone(),
            CatalogNamespaceAction::ListTables,
        );

        let authz_result = authorizer
            .load_and_authorize_namespace_action::<C>(
                event_ctx.request_metadata(),
                event_ctx.user_provided_entity().clone(),
                event_ctx.action().clone(),
                CachePolicy::Use,
                state.v1_state.catalog.clone(),
            )
            .await;

        let (event_ctx, (warehouse, namespace)) = event_ctx.emit_authz(authz_result)?;

        let event_ctx = Arc::new(event_ctx.resolve(ResolvedNamespace {
            warehouse: warehouse.clone(),
            namespace: namespace.namespace.clone(),
        }));

        // ------------------- BUSINESS LOGIC -------------------
        let mut t = C::Transaction::begin_read(state.v1_state.catalog).await?;
        let (table_infos, table_uuids, next_page_token) =
            server::fetch_until_full_page::<_, _, _, C>(
                query.page_size,
                query.page_token,
                list_entities!(
                    Table,
                    list_tables,
                    warehouse,
                    namespace,
                    authorizer,
                    event_ctx
                ),
                &mut t,
            )
            .await?;
        t.commit().await?;
        let mut identifiers = Vec::with_capacity(table_infos.len());
        let mut protection_status = Vec::with_capacity(table_infos.len());
        for table_info in table_infos {
            identifiers.push(table_info.tabular.tabular_ident);
            protection_status.push(table_info.tabular.protected);
        }

        Ok(ListTablesResponse {
            next_page_token,
            identifiers,
            table_uuids: return_uuids.then_some(table_uuids.into_iter().map(|u| *u).collect()),
            protection_status: query.return_protection_status.then_some(protection_status),
        })
    }

    #[allow(clippy::too_many_lines)]
    /// Create a table in the given namespace
    async fn create_table(
        parameters: NamespaceParameters,
        request: CreateTableRequest,
        data_access: impl Into<DataAccessMode> + Send,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadTableResult> {
        create_table::create_table(parameters, request, data_access, state, request_metadata).await
    }

    /// Register a table in the given namespace using given metadata file location
    #[allow(clippy::too_many_lines)]
    async fn register_table(
        parameters: NamespaceParameters,
        request: RegisterTableRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadTableResult> {
        // ------------------- VALIDATIONS -------------------
        let NamespaceParameters {
            namespace: provided_ns,
            prefix,
        } = &parameters;
        let warehouse_id = require_warehouse_id(prefix.as_ref())?;
        let table_ident = TableIdent::new(provided_ns.clone(), request.name.clone());
        validate_table_or_view_ident_creation(&table_ident)?;
        let metadata_location =
            parse_location(&request.metadata_location, StatusCode::BAD_REQUEST)?;

        // ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz.clone();

        let event_ctx = APIEventContext::for_namespace(
            Arc::new(request_metadata),
            state.v1_state.events,
            warehouse_id,
            parameters.namespace.clone(),
            // Prelimineray action, updated after Metadata is read
            CatalogNamespaceAction::CreateTable {
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

        // ------------------- BUSINESS LOGIC -------------------
        let storage_profile = &warehouse.storage_profile;

        require_active_warehouse(warehouse.status)?;
        storage_profile.require_allowed_location(&metadata_location)?;

        let storage_secret =
            maybe_get_secret(warehouse.storage_secret_id, &state.v1_state.secrets).await?;
        let storage_secret_ref = storage_secret.as_deref();
        let file_io = storage_profile.file_io(storage_secret_ref).await?;
        let table_metadata = read_metadata_file(&file_io, &metadata_location).await?;
        let table_location = parse_location(table_metadata.location(), StatusCode::BAD_REQUEST)?;
        validate_table_properties(table_metadata.properties().keys())?;
        storage_profile.require_allowed_location(&table_location)?;

        let action = CatalogNamespaceAction::CreateTable {
            properties: Arc::new(
                table_metadata
                    .properties()
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect::<BTreeMap<_, _>>(),
            ),
        };

        let mut event_ctx = event_ctx;
        event_ctx.action_mut().clone_from(&action);

        let authz_result = authorizer
            .require_namespace_action(
                event_ctx.request_metadata(),
                &warehouse,
                provided_ns,
                namespace,
                action,
            )
            .await;
        let (event_ctx, namespace) = event_ctx.emit_authz(authz_result)?;

        let namespace_id = namespace.namespace_id();
        let table_metadata = Arc::new(table_metadata);

        let event_ctx = event_ctx.resolve(ResolvedNamespace {
            warehouse: warehouse.clone(),
            namespace: namespace.namespace.clone(),
        });
        let request_metadata = event_ctx.request_metadata();

        // Check if we need to handle overwrite
        // Drop the existing table to overwrite it
        // We don't drop the files for the previous table on overwrite
        let mut previous_table_to_drop = None;
        if request.overwrite {
            // Check if table exists
            let previous_table_info = C::get_table_info(
                warehouse_id,
                table_ident.clone(),
                TabularListFlags::active_and_staged(),
                state.v1_state.catalog.clone(),
            )
            .await;

            if let Ok(Some(_)) = &previous_table_info {
                let mut drop_tbl_event_ctx = APIEventContext::for_table(
                    event_ctx.request_metadata_arc(),
                    event_ctx.dispatcher().clone(),
                    warehouse_id,
                    table_ident.clone(),
                    CatalogTableAction::Drop,
                );
                drop_tbl_event_ctx
                    .push_stack("Dropping existing table to overwrite it during register_table");

                let authz_result = authorizer
                    .require_table_action(
                        drop_tbl_event_ctx.request_metadata(),
                        &warehouse,
                        &namespace,
                        drop_tbl_event_ctx.user_provided_entity().table.clone(),
                        previous_table_info,
                        drop_tbl_event_ctx.action().clone(),
                    )
                    .await;
                let (drop_tbl_event_ctx, previous_table_info) =
                    drop_tbl_event_ctx.emit_authz(authz_result)?;

                // Verify authorization to drop the table first
                previous_table_to_drop = Some(previous_table_info);

                tracing::debug!(
                    "Register Table: Dropping existing table '{}' of warehouse '{}' for overwrite operation",
                    drop_tbl_event_ctx.user_provided_entity().table.to_string(),
                    warehouse.name
                );
            }
        }
        let mut t_write = C::Transaction::begin_write(state.v1_state.catalog).await?;
        if let Some(previous_table_to_drop) = &previous_table_to_drop {
            let _previous_table_location = C::drop_tabular(
                warehouse_id,
                previous_table_to_drop.table_id(),
                false,
                t_write.transaction(),
            )
            .await?;
        }

        let tabular_id = TableId::from(table_metadata.uuid());

        let (table_info, staged_table_id) = C::create_table(
            TableCreation {
                warehouse_id: warehouse.warehouse_id,
                namespace_id,
                table_ident: &table_ident,
                table_metadata: &table_metadata,
                metadata_location: Some(&metadata_location),
            },
            t_write.transaction(),
        )
        .await?;

        let config = storage_profile
            .generate_table_config(
                DataAccess::not_specified().into(),
                storage_secret_ref,
                &table_location,
                StoragePermissions::ReadWriteDelete,
                request_metadata,
                &table_info,
            )
            .await?;

        let mut auth_needs_delete = false;
        // Delete the previous table from authorizer if it exists and differs from the new one
        if let Some(previous_table_to_drop) = &previous_table_to_drop {
            if previous_table_to_drop.tabular_id != tabular_id {
                auth_needs_delete = true;
                // Only create authorization for the new table if it's different
                authorizer
                    .create_table(&request_metadata, warehouse_id, tabular_id, namespace_id)
                    .await?;
            }
        } else {
            // No previous table, need to create authorization
            authorizer
                .create_table(&request_metadata, warehouse_id, tabular_id, namespace_id)
                .await?;
        }

        // Commit the transaction
        t_write.commit().await?;

        // If we need to delete the previous table from authorizer
        if auth_needs_delete && let Some(previous_table) = &previous_table_to_drop {
            authorizer.delete_table(warehouse_id, previous_table.tabular_id).await.map_err({
                    |e| {
                        tracing::warn!(
                            "Failed to delete previous table {} from authorizer on overwrite via table register endpoint: {}",
                            previous_table.tabular_id, e.error
                        );
                    }
                }).ok();
        }

        // If a staged table was overwritten, delete it from authorizer
        if let Some(staged_table_id) = staged_table_id {
            authorizer
                .delete_table(warehouse_id, staged_table_id.0)
                .await
                .ok();
        }

        // Fire hooks
        let metadata_location_str = metadata_location.to_string();
        event_ctx.emit_table_registered_async(
            Arc::new(request),
            table_metadata.clone(),
            Arc::new(metadata_location),
        );

        Ok(LoadTableResult {
            metadata_location: Some(metadata_location_str),
            metadata: table_metadata,
            config: Some(config.config.into()),
            storage_credentials: None,
        })
    }

    /// Load a table from the catalog
    #[allow(clippy::too_many_lines)]
    async fn load_table(
        parameters: TableParameters,
        data_access: impl Into<DataAccessMode> + Send,
        filters: LoadTableFilters,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        etags: Vec<ETag>,
    ) -> Result<LoadTableResultOrNotModified> {
        load_table::load_table(
            parameters,
            data_access,
            filters,
            state,
            request_metadata,
            etags,
        )
        .await
    }

    async fn load_table_credentials(
        parameters: TableParameters,
        data_access: DataAccess,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadCredentialsResponse> {
        // ------------------- VALIDATIONS -------------------
        let TableParameters { prefix, table } = parameters;
        let warehouse_id = require_warehouse_id(prefix.as_ref())?;

        let event_ctx = APIEventContext::for_table(
            Arc::new(request_metadata),
            state.v1_state.events,
            warehouse_id,
            table.clone(),
            CatalogTableAction::GetMetadata,
        );

        let authz_result = match authorize_load_table::<C, A>(
            event_ctx.request_metadata(),
            table.clone(),
            warehouse_id,
            TabularListFlags::active_and_staged(),
            state.v1_state.authz,
            state.v1_state.catalog.clone(),
        )
        .await
        {
            Err(e) => Err(e),
            Ok((_, _, None)) => Err(AuthZTableActionForbidden::new(
                warehouse_id,
                table.clone(),
                &CatalogTableAction::ReadData,
            )
            .into()),
            Ok((a, b, Some(c))) => Ok((a, b, c)),
        };

        let (event_ctx, (warehouse, tabular_info, storage_permissions)) =
            event_ctx.emit_authz(authz_result)?;

        let event_ctx = event_ctx.resolve(ResolvedTable {
            warehouse,
            table: Arc::new(tabular_info),
            storage_permissions: Some(storage_permissions),
        });
        let warehouse = &event_ctx.resolved().warehouse;
        let tabular_info = &*event_ctx.resolved().table;

        let storage_secret =
            maybe_get_secret(warehouse.storage_secret_id, &state.v1_state.secrets).await?;
        let storage_secret_ref = storage_secret.as_deref();
        let storage_config = warehouse
            .storage_profile
            .generate_table_config(
                data_access.into(),
                storage_secret_ref,
                &parse_location(
                    tabular_info.location.as_str(),
                    StatusCode::INTERNAL_SERVER_ERROR,
                )?,
                storage_permissions,
                event_ctx.request_metadata(),
                tabular_info,
            )
            .await?;

        let storage_credentials = if storage_config.creds.inner().is_empty() {
            vec![]
        } else {
            vec![StorageCredential {
                prefix: tabular_info.location.to_string(),
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
            &parameters.table,
            request.identifier.as_ref(),
        )?);
        let t = commit_tables_with_authz(
            parameters.prefix,
            CommitTransactionRequest {
                table_changes: vec![request],
            },
            state,
            request_metadata,
        )
        .await?;
        let mut it = t.iter();
        let Some(item) = it.next() else {
            return Err(ErrorModel::internal(
                "No new metadata returned by backend",
                "NoNewMetadataReturned",
                None,
            )
            .into());
        };
        debug_assert!(
            it.next().is_none(),
            "commit_table must return exactly one CommitContext"
        );

        Ok(CommitTableResponse {
            metadata_location: item.new_metadata_location.to_string(),
            metadata: item.new_metadata.clone(),
            config: None,
        })
    }

    #[allow(clippy::too_many_lines)]
    /// Drop a table from the catalog
    async fn drop_table(
        parameters: TableParameters,
        DropParams {
            purge_requested,
            force,
        }: DropParams,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- VALIDATIONS -------------------
        let TableParameters { prefix, table } = &parameters;
        let warehouse_id = require_warehouse_id(prefix.as_ref())?;

        // Deny a "+" in in table name, since some clients (spark, trino) encode space as "+" in URLs and supporting
        // space is more important. Other clients properly encode space as "%20".
        if table.name.contains('+') {
            return Err(ErrorModel::bad_request(
                "Table name cannot contain '+' character.",
                "InvalidTableName",
                None,
            )
            .into());
        }

        validate_table_or_view_ident(table)?;

        // ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz;

        let event_ctx = APIEventContext::for_table(
            Arc::new(request_metadata),
            state.v1_state.events,
            warehouse_id,
            table.clone(),
            CatalogTableAction::Drop,
        );

        let authz_result = authorizer
            .load_and_authorize_table_operation::<C>(
                event_ctx.request_metadata(),
                event_ctx.user_provided_entity(),
                TabularListFlags::active(),
                event_ctx.action().clone(),
                state.v1_state.catalog.clone(),
            )
            .await;
        let (event_ctx, (warehouse, _ns, table_info)) = event_ctx.emit_authz(authz_result)?;

        let table_id = table_info.table_id();
        let event_ctx = event_ctx.resolve(ResolvedTable {
            warehouse: warehouse.clone(),
            table: Arc::new(table_info),
            storage_permissions: None,
        });

        // ------------------- BUSINESS LOGIC -------------------
        state
            .v1_state
            .contract_verifiers
            .check_drop(table_id.into())
            .await?
            .into_result()?;

        let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;

        let delete_profile = if force {
            TabularDeleteProfile::Hard {}
        } else {
            warehouse.tabular_delete_profile
        };
        let project_id = &warehouse.project_id;

        match delete_profile {
            TabularDeleteProfile::Hard {} => {
                let location =
                    C::drop_tabular(warehouse_id, table_id, force, t.transaction()).await?;

                if purge_requested {
                    TabularPurgeTask::schedule_task::<C>(
                        ScheduleTaskMetadata {
                            project_id: project_id.clone(),

                            parent_task_id: None,
                            scheduled_for: None,
                            entity: TaskEntity::EntityInWarehouse {
                                entity_name: table.clone().into_name_parts(),
                                warehouse_id,
                                entity_id: WarehouseTaskEntityId::Table { table_id },
                            },
                        },
                        TabularPurgePayload {
                            tabular_location: location.to_string(),
                        },
                        t.transaction(),
                    )
                    .await?;

                    tracing::debug!("Queued purge task for dropped table '{table_id}'.");
                }
                t.commit().await?;
                authorizer
                    .delete_table(warehouse_id, table_id)
                    .await
                    .inspect_err(|e| {
                        tracing::error!(?e, "Failed to delete table from authorizer: {}", e.error);
                    })
                    .ok();
            }
            TabularDeleteProfile::Soft { expiration_seconds } => {
                let _ = TabularExpirationTask::schedule_task::<C>(
                    ScheduleTaskMetadata {
                        project_id: project_id.clone(),
                        parent_task_id: None,
                        scheduled_for: Some(chrono::Utc::now() + expiration_seconds),
                        entity: TaskEntity::EntityInWarehouse {
                            entity_name: table.clone().into_name_parts(),
                            entity_id: WarehouseTaskEntityId::Table { table_id },
                            warehouse_id,
                        },
                    },
                    TabularExpirationPayload {
                        deletion_kind: if purge_requested {
                            DeleteKind::Purge
                        } else {
                            DeleteKind::Default
                        },
                    },
                    t.transaction(),
                )
                .await?;

                C::mark_tabular_as_deleted(
                    warehouse_id,
                    TabularId::Table(table_id),
                    force,
                    t.transaction(),
                )
                .await?;

                tracing::debug!("Queued expiration task for dropped table '{table_id}'.");
                t.commit().await?;
            }
        }

        event_ctx.emit_table_dropped_async(DropParams {
            purge_requested,
            force,
        });

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
        let warehouse_id = require_warehouse_id(prefix.as_ref())?;
        validate_table_or_view_ident(&table)?;

        // ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz;

        let event_ctx = APIEventContext::for_table(
            Arc::new(request_metadata),
            state.v1_state.events,
            warehouse_id,
            table.clone(),
            CatalogTableAction::GetMetadata,
        );

        let authz_result = authorizer
            .load_and_authorize_table_operation::<C>(
                event_ctx.request_metadata(),
                event_ctx.user_provided_entity(),
                TabularListFlags::active(),
                event_ctx.action().clone(),
                state.v1_state.catalog.clone(),
            )
            .await;
        let _ = event_ctx.emit_authz(authz_result)?;

        Ok(())
    }

    /// Rename a table
    async fn rename_table(
        prefix: Option<Prefix>,
        request: RenameTableRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        rename_table::rename_table(prefix, request, state, request_metadata).await
    }

    /// Commit updates to multiple tables in an atomic operation
    #[allow(clippy::too_many_lines)]
    async fn commit_transaction(
        prefix: Option<Prefix>,
        request: CommitTransactionRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        let contexts = commit_tables_with_authz(prefix, request, state, request_metadata).await?;
        tracing::debug!("Successfully committed {} table(s)", contexts.len());
        Ok(())
    }
}

async fn authorize_load_table<C: CatalogStore, A: Authorizer + Clone>(
    request_metadata: &RequestMetadata,
    table: TableIdent,
    warehouse_id: WarehouseId,
    list_flags: TabularListFlags,
    authorizer: A,
    state: C::State,
) -> Result<
    (
        Arc<ResolvedWarehouse>,
        TableInfo,
        Option<StoragePermissions>,
    ),
    AuthZError,
> {
    let (warehouse, namespace, table_info) = tokio::join!(
        C::get_active_warehouse_by_id(warehouse_id, state.clone()),
        C::get_namespace(warehouse_id, table.namespace.clone(), state.clone()),
        C::get_table_info(warehouse_id, table.clone(), list_flags, state.clone())
    );
    let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;
    let table_info = authorizer.require_table_presence(warehouse_id, table.clone(), table_info)?;
    let namespace =
        authorizer.require_namespace_presence(warehouse_id, table.namespace.clone(), namespace)?;

    // Refresh warehouse and namespace if required

    let (warehouse, namespace) = refresh_warehouse_and_namespace_if_needed::<C, _, _>(
        &warehouse,
        namespace,
        &table_info,
        AuthZCannotSeeTable::new_not_found(warehouse_id, table.clone()),
        &authorizer,
        state.clone(),
    )
    .await?;

    let [can_get_metadata, can_read, can_write] = authorizer
        .are_allowed_table_actions_arr(
            request_metadata,
            None,
            &warehouse,
            &namespace,
            &table_info,
            &[
                CatalogTableAction::GetMetadata,
                CatalogTableAction::ReadData,
                CatalogTableAction::WriteData,
            ],
        )
        .await?
        .into_inner();

    if !can_get_metadata {
        return Err(AuthZCannotSeeTable::new_forbidden(warehouse_id, table).into());
    }

    let storage_permissions = if can_write {
        Some(StoragePermissions::ReadWriteDelete)
    } else if can_read {
        Some(StoragePermissions::Read)
    } else {
        None
    };
    Ok((warehouse, table_info, storage_permissions))
}

/// Validate commit table requests
///
/// # Errors
/// Returns an error if any validation fails.
fn commit_tables_validate(request: &CommitTransactionRequest) -> Result<()> {
    for change in &request.table_changes {
        validate_table_updates(&change.updates)?;
        change
            .identifier
            .as_ref()
            .map(validate_table_or_view_ident)
            .transpose()?;

        if change.identifier.is_none() {
            return Err(ErrorModel::bad_request(
                "Table identifier is required for each change in the CommitTransactionRequest (one of the changes was missing an identifier)",
                "TableIdentifierRequiredForCommitTransaction",
                None,
            )
            .into());
        }
    }

    // Check table identifier uniqueness
    let identifiers = request
        .table_changes
        .iter()
        .filter_map(|change| change.identifier.as_ref())
        .collect::<HashSet<_>>();
    let n_identifiers = identifiers.len();

    if n_identifiers != request.table_changes.len() {
        let mut counts = std::collections::HashMap::<&TableIdent, usize>::new();
        for ident in request
            .table_changes
            .iter()
            .filter_map(|c| c.identifier.as_ref())
        {
            *counts.entry(ident).or_default() += 1;
        }
        let dups = counts
            .into_iter()
            .filter(|&(_i, c)| c > 1)
            .map(|(i, _c)| i.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(ErrorModel::bad_request(
            format!("Table identifiers must be unique; duplicates: [{dups}]"),
            "TableIdentifiersNotUnique",
            None,
        )
        .into());
    }

    Ok(())
}

#[allow(clippy::too_many_lines)]
/// Commit updates to multiple tables without authorization checks
///
/// # Errors
/// Returns an error if the commit fails or if a DB error occurs.
/// This function will retry on concurrent update errors up to a maximum number of retries.
async fn commit_tables_inner<C: CatalogStore, A: Authorizer, S: SecretStore>(
    warehouse: Arc<ResolvedWarehouse>,
    request: CommitTransactionRequest,
    event_ctx: APIEventCommitContext,
    state: ApiContext<State<A, C, S>>,
) -> Result<Arc<Vec<CommitContext>>> {
    let include_deleted = false;
    let warehouse_id = event_ctx.user_provided_entity().warehouse_id;

    // Start the retry loop
    let mut attempt = 0;
    loop {
        let result =
            try_commit_tables::<C, A, S>(&request, &warehouse, &event_ctx, &state, include_deleted)
                .await;

        match result {
            Ok(commits) => {
                state
                    .v1_state
                    .events
                    .transaction_committed(CommitTransactionEvent {
                        warehouse_id,
                        request: Arc::new(request),
                        commits: commits.clone(),
                        request_metadata: event_ctx.request_metadata_arc(),
                    })
                    .await;
                return Ok(commits);
            }
            Err(e)
                if e.error.r#type == CONCURRENT_UPDATE_ERROR_TYPE
                    && attempt < MAX_RETRIES_ON_CONCURRENT_UPDATE =>
            {
                attempt += 1;
                tracing::info!(
                    warehouse_id = %warehouse_id,
                    n_tables = %event_ctx.user_provided_entity().tables.len(),
                    attempt = attempt,
                    max_attempts = MAX_RETRIES_ON_CONCURRENT_UPDATE,
                    "Concurrent update detected, retrying commit operation"
                );
                // Short jittered exponential backoff to reduce contention
                // First delay: 50ms, then 100ms, 200ms, ..., up to 3200ms (50*2^6)
                let exp = u32::try_from(attempt.saturating_sub(1).min(6)).unwrap_or(6); // cap growth explicitly
                let base = 50u64.saturating_mul(1u64 << exp);
                let jitter = fastrand::u64(..base / 2);
                tracing::debug!(attempt, base, jitter, "Concurrent update backoff");
                tokio::time::sleep(std::time::Duration::from_millis(base + jitter)).await;
            }
            Err(e) => {
                if attempt > 0 {
                    tracing::warn!(
                        warehouse_id = %warehouse_id,
                        n_tables = %event_ctx.user_provided_entity().tables.len(),
                        attempt = attempt,
                        "Table commit operation failed after {} attempts. Operation was retried due to concurrent updates. {e}",
                        attempt + 1
                    );
                }
                return Err(e);
            }
        }
    }
}

#[allow(clippy::too_many_lines)]
/// Commit updates to multiple tables in an atomic operation
///
/// # Errors
/// Returns an error if the commit fails, if the table identifiers are not unique,
/// or if the table identifiers are not provided for each change.
/// This function will retry on concurrent update errors up to a maximum number of retries.
async fn commit_tables_with_authz<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    prefix: Option<Prefix>,
    request: CommitTransactionRequest,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<Arc<Vec<CommitContext>>> {
    // ------------------- VALIDATIONS -------------------
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    commit_tables_validate(&request)?;

    // ------------------- AUTHZ -------------------
    let authorizer = state.v1_state.authz.clone();
    let request_metadata = Arc::new(request_metadata);

    let (identifiers, actions): (Vec<_>, Vec<_>) = request
        .table_changes
        .iter()
        .filter_map(|c| {
            c.identifier.as_ref().map(|ti| {
                let (updated_properties, removed_properties) =
                    parse_table_property_updates(&c.updates);
                let action = CatalogTableAction::Commit {
                    updated_properties: Arc::new(updated_properties),
                    removed_properties: Arc::new(removed_properties),
                };

                (ti.clone(), action)
            })
        })
        .multiunzip();

    let event_ctx = APIEventContext::for_tables_by_ident(
        request_metadata,
        state.v1_state.events.clone(),
        warehouse_id,
        identifiers,
        actions.clone(),
    );

    let authz_result = commit_tables_authz::<A, C>(
        authorizer,
        warehouse_id,
        &event_ctx.user_provided_entity().tables,
        &actions,
        state.v1_state.catalog.clone(),
        event_ctx.request_metadata(),
    )
    .await;
    let (event_ctx, authz_result) = event_ctx.emit_authz(authz_result)?;
    let warehouse = authz_result.warehouse;
    let table_infos = authz_result
        .table_infos_with_actions
        .into_iter()
        .map(|(ident, (info, _action))| (ident, info))
        .collect::<HashMap<_, _>>();
    let event_ctx = event_ctx.resolve(table_infos);

    // ------------------- BUSINESS LOGIC -------------------
    commit_tables_inner::<C, _, _>(warehouse, request, event_ctx, state).await
}

struct CommitAuthorizationResult<'a> {
    table_infos_with_actions:
        HashMap<TableIdent, (Arc<TabularInfo<TableId>>, &'a CatalogTableAction)>,
    warehouse: Arc<ResolvedWarehouse>,
}

async fn commit_tables_authz<'a, A: Authorizer + Clone, C: CatalogStore>(
    authorizer: A,
    warehouse_id: WarehouseId,
    identifiers: &Vec<TableIdent>,
    actions: &'a Vec<CatalogTableAction>,
    catalog_state: C::State,
    request_metadata: &RequestMetadata,
) -> Result<CommitAuthorizationResult<'a>, AuthZError> {
    let warehouse = C::get_active_warehouse_by_id(warehouse_id, catalog_state.clone()).await;
    let mut warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;

    let (idents_ref, ns_ref): (Vec<_>, Vec<_>) =
        identifiers.iter().map(|i| (i, &i.namespace)).unzip();

    let (table_infos, namespaces) = tokio::join!(
        C::get_table_infos_by_ident(
            warehouse_id,
            &idents_ref,
            TabularListFlags::active_and_staged(),
            catalog_state.clone(),
        ),
        C::get_namespaces_by_ident(warehouse_id, &ns_ref, catalog_state.clone())
    );

    // Don't map anymore
    let table_infos = table_infos.map_err(RequireTableActionError::from)?;

    let mut table_ident_to_info = table_infos
        .into_iter()
        .map(|ti| (ti.tabular_ident.clone(), Arc::new(ti)))
        .collect::<HashMap<_, _>>();

    let table_infos_with_actions = identifiers
        .iter()
        .zip(actions)
        .map(|(ti, action)| {
            let table_info = table_ident_to_info
                .remove(ti)
                .ok_or_else(|| AuthZCannotSeeTable::new_not_found(warehouse_id, (*ti).clone()))?;
            Ok(((*ti).clone(), (table_info, action)))
        })
        .collect::<Result<HashMap<_, _>, AuthZCannotSeeTable>>()?;

    drop(table_ident_to_info); // No longer needed

    let namespaces = namespaces.map_err(RequireNamespaceActionError::from)?;

    // Refresh warehouse if required
    if let Some(required_version) = table_infos_with_actions
        .values()
        .map(|ti| ti.0.warehouse_version)
        .max()
        && warehouse.version < required_version
    {
        let refreshed_warehouse = C::get_warehouse_by_id_cache_aware(
            warehouse_id,
            WarehouseStatus::active(),
            CachePolicy::RequireMinimumVersion(*required_version),
            catalog_state.clone(),
        )
        .await;
        warehouse = authorizer.require_warehouse_presence(warehouse_id, refreshed_warehouse)?;
    }

    authorizer
        .require_table_actions(
            request_metadata,
            &warehouse,
            &namespaces,
            &table_infos_with_actions
                .values()
                .map(|(ti, a)| {
                    Ok::<_, AuthZCannotSeeNamespace>((
                        require_namespace_for_tabular(&namespaces, &**ti)?,
                        &**ti,
                        (**a).clone(),
                    ))
                })
                .collect::<Result<Vec<_>, _>>()?,
        )
        .await?;

    Ok(CommitAuthorizationResult {
        table_infos_with_actions,
        warehouse,
    })
}

// Extract the core commit logic to a separate function for retry purposes
#[allow(clippy::too_many_lines)]
async fn try_commit_tables<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    request: &CommitTransactionRequest,
    warehouse: &ResolvedWarehouse,
    event_ctx: &APIEventCommitContext,
    state: &ApiContext<State<A, C, S>>,
    include_deleted: bool,
) -> Result<Arc<Vec<CommitContext>>> {
    let warehouse_id = warehouse.warehouse_id;
    let mut transaction = C::Transaction::begin_write(state.v1_state.catalog.clone()).await?;

    // Load old metadata
    let previous_metadatas = C::load_tables(
        warehouse_id,
        event_ctx.resolved().values().map(|ti| ti.table_id()),
        include_deleted,
        &LoadTableFilters::default(),
        transaction.transaction(),
    )
    .await?;
    let mut previous_metadatas = previous_metadatas
        .into_iter()
        .map(|tm| (tm.table_id, tm))
        .collect::<HashMap<_, _>>();

    transaction.commit().await?;

    let mut expired_metadata_logs: Vec<MetadataLog> = vec![];

    // Apply changes
    let commits = request
        .table_changes
        .iter()
        .map(|change| {
            let table_ident = change.identifier.as_ref().ok_or_else(||
                    // This should never happen due to validation
                    ErrorModel::internal(
                        "Change without Identifier",
                        "ChangeWithoutIdentifier",
                        None,
                    ))?;
            let table_info = event_ctx
                .resolved()
                .get(table_ident)
                .ok_or_else(|| {
                    ErrorModel::internal(
                        "Event context not found for table identifier",
                        "EventContextNotFoundForTableIdentifier",
                        None,
                    )
                })?
                .clone();
            let table_id = table_info.table_id();
            let previous_table_metadata =
                previous_metadatas.remove(&table_id).ok_or_else(|| {
                    TabularNotFound::new(warehouse_id, TableIdentOrId::from(table_ident.clone()))
                        .append_detail("Table metadata not returned from table load".to_string())
                })?;
            let TableMetadataBuildResult {
                metadata: new_metadata,
                changes: _,
                expired_metadata_logs: mut this_expired,
            } = apply_commit(
                previous_table_metadata.table_metadata.clone(),
                previous_table_metadata.metadata_location.as_ref(),
                &change.requirements,
                change.updates.clone(),
            )?;

            let number_expired_metadata_log_entries = this_expired.len();

            if delete_after_commit_enabled(new_metadata.properties()) {
                expired_metadata_logs.extend(this_expired);
            } else {
                this_expired.clear();
            }

            let next_metadata_count = previous_table_metadata
                .metadata_location
                .as_ref()
                .and_then(extract_count_from_metadata_location)
                .map_or(0, |v| v + 1);

            let new_table_location =
                parse_location(new_metadata.location(), StatusCode::INTERNAL_SERVER_ERROR)?;
            if new_metadata.location() != previous_table_metadata.table_metadata.location() {
                warehouse
                    .storage_profile
                    .require_allowed_location(&new_table_location)?;
            }
            let new_compression_codec = CompressionCodec::try_from_metadata(&new_metadata)?;
            let new_metadata_location = warehouse.storage_profile.default_metadata_location(
                &new_table_location,
                &new_compression_codec,
                Uuid::now_v7(),
                next_metadata_count,
            );

            let number_added_metadata_log_entries = (new_metadata.metadata_log().len()
                + number_expired_metadata_log_entries)
                .saturating_sub(previous_table_metadata.table_metadata.metadata_log().len());

            Ok(CommitContext {
                new_metadata: Arc::new(new_metadata),
                new_metadata_location,
                table_info,
                new_compression_codec,
                previous_metadata_location: previous_table_metadata.metadata_location,
                updates: Arc::new(change.updates.clone()),
                previous_metadata: Arc::new(previous_table_metadata.table_metadata),
                number_expired_metadata_log_entries,
                number_added_metadata_log_entries,
            })
        })
        .collect::<Result<Vec<_>>>()?;

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
    let storage_secret_ref = storage_secret.as_deref();

    // Write metadata files
    let file_io = warehouse
        .storage_profile
        .file_io(storage_secret_ref)
        .await?;

    let write_futures: Vec<_> = commits
        .iter()
        .map(|commit| {
            write_file(
                &file_io,
                &commit.new_metadata_location,
                &commit.new_metadata,
                commit.new_compression_codec,
            )
        })
        .collect();
    futures::future::try_join_all(write_futures).await?;

    // Make changes in DB
    let transaction_result = async {
        let mut transaction = C::Transaction::begin_write(state.v1_state.catalog.clone()).await?;
        C::commit_table_transaction(
            warehouse_id,
            commits.iter().map(CommitContext::commit),
            transaction.transaction(),
        )
        .await?;

        transaction.commit().await?;
        Result::<_, IcebergErrorResponse>::Ok(())
    }
    .await;

    // If transaction fails, delete the metadata files we just wrote (best-effort), then
    // return the original error.
    if let Err(e) = transaction_result {
        let delete_result = futures::future::join_all(
            commits
                .iter()
                .map(|commit| delete_file(&file_io, &commit.new_metadata_location))
                .collect::<Vec<_>>(),
        )
        .await;
        // Log any delete errors, but return the original error
        for r in delete_result {
            if let Err(e) = r {
                tracing::warn!("Failed to delete metadata file after failed commit: {e:?}");
            }
        }
        return Err(e);
    }

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

    Ok(Arc::new(commits))
}

pub(crate) fn extract_count_from_metadata_location(location: &Location) -> Option<usize> {
    let last_segment = location
        .as_str()
        .trim_end_matches('/')
        .split('/')
        .next_back()
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

#[derive(Clone, Debug)]
pub struct CommitContext {
    pub new_metadata: TableMetadataRef,
    pub new_metadata_location: Location,
    pub previous_metadata: TableMetadataRef,
    pub previous_metadata_location: Option<Location>,
    pub updates: Arc<Vec<TableUpdate>>,
    pub new_compression_codec: CompressionCodec,
    pub number_expired_metadata_log_entries: usize,
    pub number_added_metadata_log_entries: usize,
    pub table_info: Arc<TableInfo>,
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
            previous_metadata_location: self.previous_metadata_location.clone(),
            updates: self.updates.clone(),
        }
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn calculate_diffs(
    new_metadata: &TableMetadata,
    previous_metadata: &TableMetadata,
    added_metadata_log: usize,
    expired_metadata_logs: usize,
) -> TableMetadataDiffs {
    let new_snaps = new_metadata
        .snapshots()
        .map(|s| s.snapshot_id())
        .collect::<XXHashSet<i64>>();
    let old_snaps = previous_metadata
        .snapshots()
        .map(|s| s.snapshot_id())
        .collect::<XXHashSet<i64>>();
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
        .collect::<XXHashSet<SchemaId>>();
    let new_schemas = new_metadata
        .schemas_iter()
        .map(|s| s.schema_id())
        .collect::<XXHashSet<SchemaId>>();
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
        .collect::<XXHashSet<i32>>();
    let new_specs = new_metadata
        .partition_specs_iter()
        .map(|s| s.spec_id())
        .collect::<XXHashSet<i32>>();
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
        .collect::<XXHashSet<i64>>();
    let new_sort_orders = new_metadata
        .sort_orders_iter()
        .map(|s| s.order_id)
        .collect::<XXHashSet<i64>>();
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
        .collect::<XXHashSet<_>>();
    let new_stats = new_metadata
        .statistics_iter()
        .map(|s| s.snapshot_id)
        .collect::<XXHashSet<_>>();
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
        .collect::<XXHashSet<_>>();
    let new_partition_stats = new_metadata
        .partition_statistics_iter()
        .map(|s| s.snapshot_id)
        .collect::<XXHashSet<_>>();
    let removed_partition_stats = old_partition_stats
        .difference(&new_partition_stats)
        .copied()
        .collect::<Vec<_>>();
    let added_partition_stats = new_partition_stats
        .difference(&old_partition_stats)
        .copied()
        .collect::<Vec<_>>();

    let old_encryption_keys = previous_metadata
        .encryption_keys_iter()
        .map(|k| k.key_id().to_string())
        .collect::<XXHashSet<_>>();
    let new_encryption_keys = new_metadata
        .encryption_keys_iter()
        .map(|k| k.key_id().to_string())
        .collect::<XXHashSet<_>>();
    let removed_encryption_keys = old_encryption_keys
        .difference(&new_encryption_keys)
        .cloned()
        .collect::<Vec<_>>();
    let added_encryption_keys = new_encryption_keys
        .difference(&old_encryption_keys)
        .cloned()
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
        removed_encryption_keys,
        added_encryption_keys,
    }
}

#[derive(Debug, Clone)]
pub struct TableMetadataDiffs {
    pub removed_snapshots: Vec<i64>,
    pub added_snapshots: Vec<i64>,
    pub removed_schemas: Vec<i32>,
    pub added_schemas: Vec<i32>,
    pub new_current_schema_id: Option<i32>,
    pub removed_partition_specs: Vec<i32>,
    pub added_partition_specs: Vec<i32>,
    pub default_partition_spec_id: Option<i32>,
    pub removed_sort_orders: Vec<i64>,
    pub added_sort_orders: Vec<i64>,
    pub default_sort_order_id: Option<i64>,
    pub head_of_snapshot_log_changed: bool,
    pub n_removed_snapshot_log: usize,
    pub expired_metadata_logs: usize,
    pub added_metadata_log: usize,
    pub added_stats: Vec<i64>,
    pub removed_stats: Vec<i64>,
    pub added_partition_stats: Vec<i64>,
    pub removed_partition_stats: Vec<i64>,
    pub added_encryption_keys: Vec<String>,
    pub removed_encryption_keys: Vec<String>,
}

pub(crate) fn determine_table_ident(
    parameters_ident: &TableIdent,
    request_ident: Option<&TableIdent>,
) -> Result<TableIdent> {
    let Some(identifier) = request_ident else {
        return Ok(parameters_ident.clone());
    };

    if identifier == parameters_ident {
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

// Quick validation of properties for early fails.
// Full validation is performed when changes are applied.
fn validate_table_updates(updates: &[TableUpdate]) -> Result<()> {
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

pub(crate) fn delete_after_commit_enabled(properties: &HashMap<String, String>) -> bool {
    properties
        .get(PROPERTY_METADATA_DELETE_AFTER_COMMIT_ENABLED)
        .map_or(PROPERTY_METADATA_DELETE_AFTER_COMMIT_ENABLED_DEFAULT, |v| {
            matches!(v.to_lowercase().as_str(), "true" | "yes" | "1")
        })
}

pub(crate) fn validate_table_properties<'a, I>(properties: I) -> Result<()>
where
    I: IntoIterator<Item = &'a String>,
{
    for prop in properties {
        // Only allow explicitly supported write.metadata properties to prevent
        // future properties from being silently ignored, which could mislead users.
        if ((prop.starts_with("write.metadata")
            && ![
                TableProperties::PROPERTY_METADATA_PREVIOUS_VERSIONS_MAX,
                PROPERTY_METADATA_DELETE_AFTER_COMMIT_ENABLED,
                PROPERTY_METADATA_COMPRESSION_CODEC,
            ]
            .contains(&prop.as_str()))
            || prop.starts_with("write.data.path"))
            && !prop.starts_with("write.metadata.metrics.")
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
    let TableIdent { namespace, name } = &table;
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

pub(crate) fn validate_table_or_view_ident_creation(table: &TableIdent) -> Result<()> {
    validate_table_or_view_ident(table)?;
    // Deny a "+" in names, since some clients (spark, trino) encode space as "+" in URLs and supporting
    // space is more important. Other clients properly encode space as "%20".
    if table.name.contains('+') {
        return Err(ErrorModel::bad_request(
            "Table name cannot contain '+' character.",
            "InvalidTableName",
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
        tracing::warn!(
            "Serializing the request body to json failed, this is very unexpected. It will not be part of any emitted Event."
        );
        serde_json::Value::Null
    }
}

/// Parse property updates and removals from a list of table updates
///
/// Returns a tuple of (updates, removals) where:
/// - updates: `BtreeMap` of property key-value pairs to set
/// - removals: `Vec` of property keys to remove
pub(crate) fn parse_table_property_updates(
    updates: &[TableUpdate],
) -> (BTreeMap<String, String>, Vec<String>) {
    let mut property_updates = BTreeMap::new();
    let mut property_removals = Vec::new();

    for update in updates {
        match update {
            TableUpdate::SetProperties { updates } => {
                property_updates.extend(updates.clone());
            }
            TableUpdate::RemoveProperties { removals } => {
                property_removals.extend(removals.clone());
            }
            _ => {}
        }
    }

    (property_updates, property_removals)
}

#[cfg(test)]
pub(crate) mod test {
    use std::{collections::HashMap, str::FromStr};

    use http::StatusCode;
    use iceberg::{
        NamespaceIdent, TableIdent, TableUpdate,
        spec::{
            EncryptedKey, FormatVersion, MAIN_BRANCH, NestedField, Operation, PrimitiveType,
            Schema, Snapshot, SnapshotReference, SnapshotRetention, Summary, TableMetadata,
            Transform, Type, UnboundPartitionField, UnboundPartitionSpec,
        },
    };
    use iceberg_ext::catalog::rest::{
        CommitTableRequest, CreateNamespaceResponse, CreateTableRequest, LoadTableResult,
        RenameTableRequest,
    };
    use itertools::Itertools;
    use lakekeeper_io::Location;
    use sqlx::PgPool;
    use uuid::Uuid;

    use super::*;
    use crate::{
        WarehouseId,
        api::{
            ApiContext,
            iceberg::{
                types::{PageToken, Prefix},
                v1::{
                    DataAccess, DropParams, ListTablesQuery, LoadTableResultOrNotModified,
                    NamespaceParameters, TableParameters,
                    tables::{LoadTableFilters, TablesService as _},
                },
            },
            management::v1::{
                ApiServer as ManagementApiServer, table::TableManagementService,
                warehouse::TabularDeleteProfile,
            },
        },
        implementations::postgres::{
            PostgresBackend, SecretsState, tabular::table::tests::initialize_table,
        },
        request_metadata::RequestMetadata,
        server::{
            CatalogServer, CatalogStore,
            test::{impl_pagination_tests, tabular_test_multi_warehouse_setup},
        },
        service::{
            SecretStore, State, TableId, TabularListFlags, UserId,
            authz::{
                AllowAllAuthorizer, CatalogNamespaceAction, CatalogTableAction,
                tests::HidingAuthorizer,
            },
        },
        tests::{create_table_request as create_request, random_request_metadata},
    };

    #[test]
    fn test_parse_table_property_updates() {
        // Test empty updates
        let updates = vec![];
        let (property_updates, property_removals) = parse_table_property_updates(&updates);
        assert!(property_updates.is_empty());
        assert!(property_removals.is_empty());

        // Test only SetProperties
        let updates = vec![TableUpdate::SetProperties {
            updates: HashMap::from([
                ("key1".to_string(), "value1".to_string()),
                ("key2".to_string(), "value2".to_string()),
            ]),
        }];
        let (property_updates, property_removals) = parse_table_property_updates(&updates);
        assert_eq!(property_updates.len(), 2);
        assert_eq!(property_updates.get("key1"), Some(&"value1".to_string()));
        assert_eq!(property_updates.get("key2"), Some(&"value2".to_string()));
        assert!(property_removals.is_empty());

        // Test only RemoveProperties
        let updates = vec![TableUpdate::RemoveProperties {
            removals: vec!["key1".to_string(), "key2".to_string()],
        }];
        let (property_updates, property_removals) = parse_table_property_updates(&updates);
        assert!(property_updates.is_empty());
        assert_eq!(property_removals.len(), 2);
        assert!(property_removals.contains(&"key1".to_string()));
        assert!(property_removals.contains(&"key2".to_string()));

        // Test mixed updates
        let updates = vec![
            TableUpdate::SetProperties {
                updates: HashMap::from([
                    ("key1".to_string(), "value1".to_string()),
                    ("key2".to_string(), "value2".to_string()),
                ]),
            },
            TableUpdate::RemoveProperties {
                removals: vec!["key3".to_string(), "key4".to_string()],
            },
            TableUpdate::SetProperties {
                updates: HashMap::from([("key5".to_string(), "value5".to_string())]),
            },
        ];
        let (property_updates, property_removals) = parse_table_property_updates(&updates);
        assert_eq!(property_updates.len(), 3);
        assert_eq!(property_updates.get("key1"), Some(&"value1".to_string()));
        assert_eq!(property_updates.get("key2"), Some(&"value2".to_string()));
        assert_eq!(property_updates.get("key5"), Some(&"value5".to_string()));
        assert_eq!(property_removals.len(), 2);
        assert!(property_removals.contains(&"key3".to_string()));
        assert!(property_removals.contains(&"key4".to_string()));

        // Test with other update types (should be ignored)
        let updates = vec![
            TableUpdate::SetProperties {
                updates: HashMap::from([("key1".to_string(), "value1".to_string())]),
            },
            TableUpdate::AssignUuid {
                uuid: Uuid::now_v7(),
            },
            TableUpdate::RemoveProperties {
                removals: vec!["key2".to_string()],
            },
            TableUpdate::UpgradeFormatVersion {
                format_version: FormatVersion::V2,
            },
        ];
        let (property_updates, property_removals) = parse_table_property_updates(&updates);
        assert_eq!(property_updates.len(), 1);
        assert_eq!(property_updates.get("key1"), Some(&"value1".to_string()));
        assert_eq!(property_removals.len(), 1);
        assert!(property_removals.contains(&"key2".to_string()));

        // Test property override (later SetProperties should override earlier ones)
        let updates = vec![
            TableUpdate::SetProperties {
                updates: HashMap::from([("key1".to_string(), "value1".to_string())]),
            },
            TableUpdate::SetProperties {
                updates: HashMap::from([("key1".to_string(), "value2".to_string())]),
            },
        ];
        let (property_updates, property_removals) = parse_table_property_updates(&updates);
        assert_eq!(property_updates.len(), 1);
        assert_eq!(property_updates.get("key1"), Some(&"value2".to_string()));
        assert!(property_removals.is_empty());
    }

    #[test]
    fn test_mixed_case_properties() {
        let properties = ["a".to_string(), "B".to_string()];
        assert!(validate_table_properties(properties.iter()).is_ok());
    }

    #[test]
    fn test_allow_metrics_properties() {
        let properties = [
            "write.metadata.metrics.max-inferred-column-defaults".to_string(),
            "write.metadata.metrics.default".to_string(),
            "write.metadata.metrics.column.col1".to_string(),
        ];
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

    fn partition_spec() -> UnboundPartitionSpec {
        UnboundPartitionSpec::builder()
            .with_spec_id(0)
            .add_partition_field(2, "y", Transform::Identity)
            .unwrap()
            .build()
    }

    // Helper functions to reduce repetitive code in tests

    /// Creates a standard test schema with id and name fields
    fn create_test_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", iceberg::spec::Type::Primitive(PrimitiveType::Int))
                    .into(),
                NestedField::required(
                    2,
                    "name",
                    iceberg::spec::Type::Primitive(PrimitiveType::String),
                )
                .into(),
            ])
            .build()
            .unwrap()
    }

    /// Creates a `CreateTableRequest` with the given name and format version
    fn create_table_request(
        name: &str,
        format_version: Option<FormatVersion>,
    ) -> CreateTableRequest {
        let mut properties = None;
        if let Some(version) = format_version {
            properties = Some(HashMap::from([(
                TableProperties::PROPERTY_FORMAT_VERSION.to_string(),
                match version {
                    FormatVersion::V1 => "1".to_string(),
                    FormatVersion::V2 => "2".to_string(),
                    FormatVersion::V3 => "3".to_string(),
                },
            )]));
        }

        CreateTableRequest {
            name: name.to_string(),
            location: None,
            schema: create_test_schema(),
            partition_spec: Some(UnboundPartitionSpec::builder().build()),
            write_order: None,
            stage_create: Some(false),
            properties,
        }
    }

    /// Helper to load a table using `CatalogServer`
    async fn load_table(
        ctx: &ApiContext<
            State<impl crate::service::authz::Authorizer, impl CatalogStore, impl SecretStore>,
        >,
        ns_params: &NamespaceParameters,
        table_name: &str,
    ) -> LoadTableResult {
        let table_ident = TableIdent {
            namespace: ns_params.namespace.clone(),
            name: table_name.to_string(),
        };

        let load_table_result = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: table_ident,
            },
            DataAccess::not_specified(),
            LoadTableFilters::default(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
            Vec::new(),
        )
        .await
        .unwrap();

        let LoadTableResultOrNotModified::LoadTableResult(load_table_result) = load_table_result
        else {
            panic!("Expected LoadTableResult, got NotModified");
        };
        load_table_result
    }

    /// Helper to commit table changes
    async fn commit_table_changes(
        ctx: &ApiContext<
            State<impl crate::service::authz::Authorizer, impl CatalogStore, impl SecretStore>,
        >,
        ns_params: &NamespaceParameters,
        table_ident: &TableIdent,
        updates: Vec<TableUpdate>,
    ) -> super::CommitContext {
        super::commit_tables_with_authz(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(table_ident.clone()),
                    requirements: vec![],
                    updates,
                }],
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
    }

    /// Helper to create a standard snapshot for testing
    fn create_test_snapshot_v3(
        snapshot_id: i64,
        timestamp_ms: i64,
        sequence_number: i64,
        manifest_list: &str,
        row_range: Option<(u64, u64)>,
        added_records: u64,
        key_id: &str,
    ) -> Snapshot {
        let base_builder = Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_timestamp_ms(timestamp_ms)
            .with_sequence_number(sequence_number)
            .with_schema_id(0)
            .with_manifest_list(manifest_list)
            .with_encryption_key_id(Some(key_id.to_string()))
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::from_iter(vec![
                    ("added-data-files".to_string(), "1".to_string()),
                    ("added-records".to_string(), added_records.to_string()),
                ]),
            });

        if let Some((first_row_id, added_rows_count)) = row_range {
            base_builder
                .with_row_range(first_row_id, added_rows_count)
                .build()
        } else {
            base_builder.build()
        }
    }

    /// Helper to create a snapshot reference
    fn create_snapshot_reference(snapshot_id: i64) -> SnapshotReference {
        SnapshotReference {
            snapshot_id,
            retention: SnapshotRetention::Branch {
                min_snapshots_to_keep: Some(10),
                max_snapshot_age_ms: None,
                max_ref_age_ms: None,
            },
        }
    }

    #[sqlx::test]
    async fn test_set_properties_commit_table(pool: sqlx::PgPool) {
        let (ctx, ns, ns_params, table) = commit_test_setup(pool).await;

        let table_metadata = (*table.metadata)
            .clone()
            .into_builder(table.metadata_location)
            .set_properties(HashMap::from([
                ("p1".into(), "v2".into()),
                ("p2".into(), "v2".into()),
            ]))
            .unwrap()
            .build()
            .unwrap();
        let updates = table_metadata.changes;
        let _ = super::commit_tables_with_authz(
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
            RequestMetadata::new_unauthenticated(),
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
            DataAccess::not_specified(),
            LoadTableFilters::default(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
            Vec::new(),
        )
        .await
        .unwrap();

        let LoadTableResultOrNotModified::LoadTableResult(tab) = tab else {
            panic!("Expected LoadTableResult, got NotModified");
        };

        assert_table_metadata_are_equal(&table_metadata.metadata, &tab.metadata);
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

    fn assert_table_metadata_are_equal(expected: &TableMetadata, actual: &TableMetadata) {
        assert_eq!(actual.location(), expected.location());
        assert_eq!(actual.properties(), expected.properties());
        assert_eq!(
            actual
                .snapshots()
                .sorted_by_key(|s| s.snapshot_id())
                .collect_vec(),
            expected
                .snapshots()
                .sorted_by_key(|s| s.snapshot_id())
                .collect_vec()
        );
        assert_eq!(
            actual
                .partition_specs_iter()
                .sorted_by_key(|ps| ps.spec_id())
                .collect_vec(),
            expected
                .partition_specs_iter()
                .sorted_by_key(|ps| ps.spec_id())
                .collect_vec()
        );
        assert_eq!(
            actual
                .partition_statistics_iter()
                .sorted_by_key(|s| (s.snapshot_id, &s.statistics_path))
                .collect_vec(),
            expected
                .partition_statistics_iter()
                .sorted_by_key(|s| (s.snapshot_id, &s.statistics_path))
                .collect_vec()
        );
        assert_eq!(
            actual
                .sort_orders_iter()
                .sorted_by_key(|s| s.order_id)
                .collect_vec(),
            expected
                .sort_orders_iter()
                .sorted_by_key(|s| s.order_id)
                .collect_vec()
        );
        assert_eq!(
            actual
                .statistics_iter()
                .sorted_by_key(|s| (s.snapshot_id, &s.statistics_path))
                .collect_vec(),
            expected
                .statistics_iter()
                .sorted_by_key(|s| (s.snapshot_id, &s.statistics_path))
                .collect_vec()
        );
        assert_eq!(actual.history(), expected.history());
        assert_eq!(actual.current_schema_id(), expected.current_schema_id());
        assert_eq!(actual.current_snapshot_id(), expected.current_snapshot_id());
        assert_eq!(
            actual.default_partition_spec(),
            expected.default_partition_spec()
        );
        assert_eq!(actual.default_sort_order(), expected.default_sort_order());
        assert_eq!(actual.format_version(), expected.format_version());
        assert_eq!(actual.last_column_id(), expected.last_column_id());
        assert_eq!(
            actual.last_sequence_number(),
            expected.last_sequence_number()
        );
        assert_eq!(actual.last_partition_id(), expected.last_partition_id());
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

        let table_metadata = (*table.metadata)
            .clone()
            .into_builder(table.metadata_location)
            .add_schema(schema())
            .unwrap()
            .set_current_schema(-1)
            .unwrap()
            .add_partition_spec(partition_spec())
            .unwrap()
            .add_partition_spec(added_spec.clone())
            .unwrap()
            .build()
            .unwrap();

        let updates = table_metadata.changes;
        let _ = super::commit_tables_with_authz(
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
            RequestMetadata::new_unauthenticated(),
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
            DataAccess::not_specified(),
            LoadTableFilters::default(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
            Vec::new(),
        )
        .await
        .unwrap();

        let LoadTableResultOrNotModified::LoadTableResult(tab) = tab else {
            panic!("Expected LoadTableResult, got NotModified");
        };

        assert_table_metadata_are_equal(&table_metadata.metadata, &tab.metadata);
    }

    #[sqlx::test]
    async fn test_set_default_partition_spec(pool: PgPool) {
        let (ctx, ns, ns_params, table) = commit_test_setup(pool).await;

        let added_spec = UnboundPartitionSpec::builder()
            .with_spec_id(10)
            .add_partition_field(1, "y_bucket[2]", Transform::Bucket(2))
            .unwrap()
            .build();

        let table_metadata = (*table.metadata)
            .clone()
            .into_builder(table.metadata_location)
            .add_partition_spec(added_spec)
            .unwrap()
            .set_default_partition_spec(-1)
            .unwrap()
            .build()
            .unwrap();
        let updates = table_metadata.changes;

        let _ = super::commit_tables_with_authz(
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
            RequestMetadata::new_unauthenticated(),
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
            DataAccess::not_specified(),
            LoadTableFilters::default(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
            Vec::new(),
        )
        .await
        .unwrap();

        let LoadTableResultOrNotModified::LoadTableResult(tab) = tab else {
            panic!("Expected LoadTableResult, got NotModified");
        };

        assert_table_metadata_are_equal(&table_metadata.metadata, &tab.metadata);
    }

    #[sqlx::test]
    async fn test_set_ref(pool: PgPool) {
        let (ctx, ns, ns_params, table) = commit_test_setup(pool).await;
        let last_updated = table.metadata.last_updated_ms();
        let builder = (*table.metadata)
            .clone()
            .into_builder(table.metadata_location);

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

        let _ = super::commit_tables_with_authz(
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
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        let tab = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: TableIdent {
                    namespace: ns.namespace.clone(),
                    name: "tab-1".to_string(),
                },
            },
            DataAccess::not_specified(),
            LoadTableFilters::default(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
            Vec::new(),
        )
        .await
        .unwrap();

        let LoadTableResultOrNotModified::LoadTableResult(tab) = tab else {
            panic!("Expected LoadTableResult, got NotModified");
        };

        assert_eq!(&*tab.metadata, &builder.metadata);
    }

    #[sqlx::test]
    async fn test_expire_metadata_log(pool: PgPool) {
        let (ctx, ns, ns_params, table) = commit_test_setup(pool).await;
        let table_ident = TableIdent {
            namespace: ns.namespace.clone(),
            name: "tab-1".to_string(),
        };
        let builder = (*table.metadata)
            .clone()
            .into_builder(table.metadata_location)
            .set_properties(HashMap::from_iter([(
                TableProperties::PROPERTY_METADATA_PREVIOUS_VERSIONS_MAX.to_string(),
                "2".to_string(),
            )]))
            .unwrap()
            .build()
            .unwrap();
        let _ = super::commit_tables_with_authz(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(table_ident.clone()),
                    requirements: vec![],
                    updates: builder.changes,
                }],
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        let tab = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: table_ident.clone(),
            },
            DataAccess::not_specified(),
            LoadTableFilters::default(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
            Vec::new(),
        )
        .await
        .unwrap();

        let LoadTableResultOrNotModified::LoadTableResult(tab) = tab else {
            panic!("Expected LoadTableResult, got NotModified");
        };

        assert_table_metadata_are_equal(&builder.metadata, &tab.metadata);

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

        let committed = super::commit_tables_with_authz(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(table_ident.clone()),
                    requirements: vec![],
                    updates: builder.changes,
                }],
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
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
            DataAccess::not_specified(),
            LoadTableFilters::default(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
            Vec::new(),
        )
        .await
        .unwrap();

        let LoadTableResultOrNotModified::LoadTableResult(tab) = tab else {
            panic!("Expected LoadTableResult, got NotModified");
        };

        assert_table_metadata_are_equal(&builder.metadata, &tab.metadata);

        let builder = (*committed.new_metadata)
            .clone()
            .into_builder(tab.metadata_location)
            .set_properties(HashMap::from_iter(vec![(
                "change_nr".to_string(),
                "2".to_string(),
            )]))
            .unwrap()
            .build()
            .unwrap();

        let _ = super::commit_tables_with_authz(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(table_ident.clone()),
                    requirements: vec![],
                    updates: builder.changes,
                }],
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
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
            DataAccess::not_specified(),
            LoadTableFilters::default(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
            Vec::new(),
        )
        .await
        .unwrap();

        let LoadTableResultOrNotModified::LoadTableResult(tab) = tab else {
            panic!("Expected LoadTableResult, got NotModified");
        };

        assert_table_metadata_are_equal(&builder.metadata, &tab.metadata);
    }

    #[sqlx::test]
    async fn test_default_format_version_is_v2(pg_pool: PgPool) {
        let (ctx, _ns, ns_params, _) = table_test_setup(pg_pool).await;
        let create_request = create_table_request("my_table", None);
        let table = CatalogServer::create_table(
            ns_params.clone(),
            create_request,
            DataAccess {
                vended_credentials: true,
                remote_signing: false,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        assert_eq!(table.metadata.format_version(), FormatVersion::V2);
    }

    #[sqlx::test]
    #[allow(clippy::too_many_lines)]
    async fn test_table_v3(pg_pool: PgPool) {
        let (ctx, ns, ns_params, _) = table_test_setup(pg_pool).await;
        let create_request = create_table_request("my_table", Some(FormatVersion::V3));
        let table = CatalogServer::create_table(
            ns_params.clone(),
            create_request,
            DataAccess {
                vended_credentials: true,
                remote_signing: false,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        assert_eq!(table.metadata.format_version(), FormatVersion::V3);
        assert_eq!(table.metadata.next_row_id(), 0);

        // Create table identifier for commits
        let table_ident = TableIdent {
            namespace: ns.namespace.clone(),
            name: "my_table".to_string(),
        };

        // Add a snapshot with row_range (0, 100)
        let last_updated = table.metadata.last_updated_ms();

        let snapshot1 = create_test_snapshot_v3(
            1,
            last_updated + 1,
            1,
            "/snap-1.avro",
            Some((0, 100)),
            100,
            "key-1",
        );

        // Commit using Catalog
        let encryption_key = EncryptedKey::builder()
            .key_id("key-1")
            .encrypted_key_metadata("key-metadata".as_bytes().to_vec())
            .encrypted_by_id("my-vault".to_string())
            .build();

        commit_table_changes(
            &ctx,
            &ns_params,
            &table_ident,
            vec![
                TableUpdate::AddSnapshot {
                    snapshot: snapshot1,
                },
                TableUpdate::SetSnapshotRef {
                    ref_name: MAIN_BRANCH.to_string(),
                    reference: create_snapshot_reference(1),
                },
                TableUpdate::AddEncryptionKey {
                    encryption_key: encryption_key.clone(),
                },
            ],
        )
        .await;

        // Load using Catalog and assert next_row_id = 100
        let loaded_table = load_table(&ctx, &ns_params, "my_table").await;
        assert_eq!(loaded_table.metadata.next_row_id(), 100);
        let current_snapshot = loaded_table
            .metadata
            .current_snapshot()
            .expect("There should be a current snapshot");
        assert_eq!(current_snapshot.snapshot_id(), 1);
        assert_eq!(current_snapshot.row_range(), Some((0, 100)));
        assert_eq!(
            loaded_table.metadata.encryption_key("key-1"),
            Some(&encryption_key)
        );
        assert_eq!(current_snapshot.encryption_key_id(), Some("key-1"));

        let snapshot2_invalid = create_test_snapshot_v3(
            2,
            last_updated + 2,
            2,
            "/snap-2-invalid.avro",
            Some((50, 100)),
            100,
            "key-1",
        );

        // This commit should fail due to row range overlap
        let invalid_commit_result = super::commit_tables_with_authz(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(table_ident.clone()),
                    requirements: vec![],
                    updates: vec![TableUpdate::AddSnapshot {
                        snapshot: snapshot2_invalid,
                    }],
                }],
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await;

        // Assert that the commit fails
        assert!(invalid_commit_result.is_err());
        let err_string = invalid_commit_result.as_ref().unwrap_err().to_string();
        assert!(
            err_string.contains("first-row-id is behind table next-row-id"),
            "The error message `{err_string}` did not contain the expected text",
        );

        // Add another snapshot with row_range (100, 50) - this should succeed
        // because it doesn't overlap (rows 100-149)
        let loaded_table2 = load_table(&ctx, &ns_params, "my_table").await;

        assert_eq!(loaded_table2.metadata.next_row_id(), 100);
        assert_eq!(loaded_table2.metadata.format_version(), FormatVersion::V3);

        let snapshot3_valid = create_test_snapshot_v3(
            3,
            last_updated + 3,
            2,
            "/snap-3-valid.avro",
            Some((100, 50)), // first_row_id: 100, added_rows_count: 50
            50,              // added_records: 50
            "key-1",
        );

        // This commit should succeed
        commit_table_changes(
            &ctx,
            &ns_params,
            &table_ident,
            vec![TableUpdate::AddSnapshot {
                snapshot: snapshot3_valid,
            }],
        )
        .await;

        // Load again and check next_row_id should now be 150
        let final_table = load_table(&ctx, &ns_params, "my_table").await;

        assert_eq!(final_table.metadata.next_row_id(), 150);
        println!(
            "Available snapshot ids: {:?}",
            final_table
                .metadata
                .snapshots()
                .map(|s| s.snapshot_id())
                .collect::<Vec<_>>()
        );
        let snapshot = final_table.metadata.snapshot_by_id(3).unwrap();
        assert_eq!(snapshot.row_range(), Some((100, 50)));
        assert_eq!(snapshot.manifest_list(), "/snap-3-valid.avro");
    }

    #[sqlx::test]
    async fn test_v2_to_v3_migration(pg_pool: PgPool) {
        let (ctx, ns, ns_params, _) = table_test_setup(pg_pool).await;

        // Create a v2 table (default version)
        let create_request = CreateTableRequest {
            name: "my_migration_table".to_string(),
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
            properties: None, // No format version specified, should default to V2
        };

        let table = CatalogServer::create_table(
            ns_params.clone(),
            create_request,
            DataAccess {
                vended_credentials: true,
                remote_signing: false,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        // Verify it's a V2 table
        assert_eq!(table.metadata.format_version(), FormatVersion::V2);

        // Create table identifier for commits
        let table_ident = TableIdent {
            namespace: ns.namespace.clone(),
            name: "my_migration_table".to_string(),
        };

        // Add a snapshot to the V2 table (without row_range)
        let last_updated = table.metadata.last_updated_ms();
        let builder = (*table.metadata)
            .clone()
            .into_builder(table.metadata_location);

        let snapshot1 = Snapshot::builder()
            .with_snapshot_id(1)
            .with_timestamp_ms(last_updated + 1)
            .with_sequence_number(1)
            .with_schema_id(0)
            .with_manifest_list("/snap-1.avro")
            // No row_range for V2 table
            .with_row_range(0, 50) // row_range is ignored in V2
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::from_iter(vec![
                    ("added-data-files".to_string(), "1".to_string()),
                    ("added-records".to_string(), "100".to_string()),
                ]),
            })
            .build();

        let builder = builder
            .add_snapshot(snapshot1)
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

        // Commit the snapshot
        super::commit_tables_with_authz(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(table_ident.clone()),
                    requirements: vec![],
                    updates: builder.changes,
                }],
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        // Load table and verify it's still V2
        let loaded_table_v2 = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: table_ident.clone(),
            },
            DataAccess::not_specified(),
            LoadTableFilters::default(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
            Vec::new(),
        )
        .await
        .unwrap();

        let LoadTableResultOrNotModified::LoadTableResult(loaded_table_v2) = loaded_table_v2 else {
            panic!("Expected LoadTableResult, got NotModified");
        };

        assert_eq!(loaded_table_v2.metadata.format_version(), FormatVersion::V2);

        // Upgrade to V3 using TableUpdate::UpgradeFormatVersion
        super::commit_tables_with_authz(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(table_ident.clone()),
                    requirements: vec![],
                    updates: vec![TableUpdate::UpgradeFormatVersion {
                        format_version: FormatVersion::V3,
                    }],
                }],
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        // Load table again -> should be V3 and next_row_id should be 0 (NULL equivalent)
        let loaded_table_v3 = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: table_ident.clone(),
            },
            DataAccess::not_specified(),
            LoadTableFilters::default(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
            Vec::new(),
        )
        .await
        .unwrap();

        let LoadTableResultOrNotModified::LoadTableResult(loaded_table_v3) = loaded_table_v3 else {
            panic!("Expected LoadTableResult, got NotModified");
        };

        assert_eq!(loaded_table_v3.metadata.format_version(), FormatVersion::V3);
        assert_eq!(loaded_table_v3.metadata.next_row_id(), 0); // Should be 0 after migration

        // Add a snapshot with row_range to the V3 table
        let snapshot2 = Snapshot::builder()
            .with_snapshot_id(2)
            .with_timestamp_ms(last_updated + 2)
            .with_sequence_number(2)
            .with_schema_id(0)
            .with_manifest_list("/snap-2.avro")
            .with_row_range(0, 50) // first_row_id: 0, added_rows_count: 50
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::from_iter(vec![
                    ("added-data-files".to_string(), "1".to_string()),
                    ("added-records".to_string(), "50".to_string()),
                ]),
            })
            .build();

        super::commit_tables_with_authz(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(table_ident.clone()),
                    requirements: vec![],
                    updates: vec![TableUpdate::AddSnapshot {
                        snapshot: snapshot2,
                    }],
                }],
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        // Load table -> next_row_id should now be increased to 50
        let final_table = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: table_ident.clone(),
            },
            DataAccess::not_specified(),
            LoadTableFilters::default(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
            Vec::new(),
        )
        .await
        .unwrap();

        let LoadTableResultOrNotModified::LoadTableResult(final_table) = final_table else {
            panic!("Expected LoadTableResult, got NotModified");
        };

        assert_eq!(final_table.metadata.format_version(), FormatVersion::V3);
        assert_eq!(final_table.metadata.next_row_id(), 50);
    }

    #[sqlx::test]
    async fn test_remove_snapshot_commit(pg_pool: PgPool) {
        let (ctx, ns, ns_params, table) = commit_test_setup(pg_pool).await;
        let table_ident = TableIdent {
            namespace: ns.namespace.clone(),
            name: "tab-1".to_string(),
        };
        let last_updated = table.metadata.last_updated_ms();
        let builder = (*table.metadata)
            .clone()
            .into_builder(table.metadata_location);

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

        let _ = super::commit_tables_with_authz(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(table_ident.clone()),
                    requirements: vec![],
                    updates: builder.changes,
                }],
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        let tab = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: table_ident.clone(),
            },
            DataAccess::not_specified(),
            LoadTableFilters::default(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
            Vec::new(),
        )
        .await
        .unwrap();

        let LoadTableResultOrNotModified::LoadTableResult(tab) = tab else {
            panic!("Expected LoadTableResult, got NotModified");
        };

        assert_eq!(tab.metadata.history(), builder.metadata.history());
        assert_eq!(&*tab.metadata, &builder.metadata);

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

        let _ = super::commit_tables_with_authz(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(table_ident.clone()),
                    requirements: vec![],
                    updates,
                }],
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        let tab = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: table_ident.clone(),
            },
            DataAccess::not_specified(),
            LoadTableFilters::default(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
            Vec::new(),
        )
        .await
        .unwrap();

        let LoadTableResultOrNotModified::LoadTableResult(tab) = tab else {
            panic!("Expected LoadTableResult, got NotModified");
        };

        assert_eq!(&*tab.metadata, &builder.metadata);

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

        let _ = super::commit_tables_with_authz(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(table_ident.clone()),
                    requirements: vec![],
                    updates,
                }],
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        let tab = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: table_ident.clone(),
            },
            DataAccess::not_specified(),
            LoadTableFilters::default(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
            Vec::new(),
        )
        .await
        .unwrap();

        let LoadTableResultOrNotModified::LoadTableResult(tab) = tab else {
            panic!("Expected LoadTableResult, got NotModified");
        };

        assert_eq!(&*tab.metadata, &builder.metadata);

        let builder = builder
            .metadata
            .into_builder(tab.metadata_location)
            .remove_snapshots(&[2])
            .build()
            .unwrap();

        let updates = builder.changes;

        let _ = super::commit_tables_with_authz(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(table_ident.clone()),
                    requirements: vec![],
                    updates,
                }],
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        let tab = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: table_ident.clone(),
            },
            DataAccess::not_specified(),
            LoadTableFilters::default(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
            Vec::new(),
        )
        .await
        .unwrap();

        let LoadTableResultOrNotModified::LoadTableResult(tab) = tab else {
            panic!("Expected LoadTableResult, got NotModified");
        };

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
        assert_table_metadata_are_equal(&builder.metadata, &tab.metadata);
    }

    async fn commit_test_setup(
        pool: PgPool,
    ) -> (
        ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>,
        CreateNamespaceResponse,
        NamespaceParameters,
        LoadTableResult,
    ) {
        let (ctx, ns, ns_params, _) = table_test_setup(pool).await;
        let table = CatalogServer::create_table(
            ns_params.clone(),
            create_request(Some("tab-1".to_string()), Some(false)),
            DataAccess {
                vended_credentials: true,
                remote_signing: false,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        (ctx, ns, ns_params, table)
    }

    async fn table_test_setup(
        pool: PgPool,
    ) -> (
        ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>,
        CreateNamespaceResponse,
        NamespaceParameters,
        String,
    ) {
        let prof = crate::server::test::memory_io_profile();
        let base_loc = prof.base_location().unwrap().to_string();
        let (ctx, warehouse) = crate::server::test::setup(
            pool.clone(),
            prof,
            None,
            AllowAllAuthorizer::default(),
            TabularDeleteProfile::Hard {},
            None,
        )
        .await;
        let ns = crate::server::test::create_ns(
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
    async fn test_can_create_tables_with_same_prefix_1(pool: PgPool) {
        let (ctx, _, ns_params, base_location) = table_test_setup(pool).await;
        let tmp_id = Uuid::now_v7();
        let mut create_request_1 = create_request(Some("tab-1".to_string()), Some(false));
        create_request_1.location = Some(format!("{base_location}/{tmp_id}/my-table-2"));
        let mut create_request_2 = create_request(Some("tab-2".to_string()), Some(false));
        create_request_2.location = Some(format!("{base_location}/{tmp_id}/my-table"));

        let _ = CatalogServer::create_table(
            ns_params.clone(),
            create_request_1,
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        CatalogServer::create_table(
            ns_params.clone(),
            create_request_2,
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
    }

    #[sqlx::test]
    async fn test_can_create_tables_with_same_prefix_2(pool: PgPool) {
        let (ctx, _, ns_params, base_location) = table_test_setup(pool).await;
        let tmp_id = Uuid::now_v7();
        let mut create_request_1 = create_request(Some("tab-1".to_string()), Some(false));
        create_request_1.location = Some(format!("{base_location}/{tmp_id}/my-table"));
        let mut create_request_2 = create_request(Some("tab-2".to_string()), Some(false));
        create_request_2.location = Some(format!("{base_location}/{tmp_id}/my-table-2"));

        let _ = CatalogServer::create_table(
            ns_params.clone(),
            create_request_1,
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        CatalogServer::create_table(
            ns_params.clone(),
            create_request_2,
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
    }

    #[sqlx::test]
    async fn test_cannot_create_table_at_same_location(pool: PgPool) {
        let (ctx, _, ns_params, base_location) = table_test_setup(pool).await;
        let tmp_id = Uuid::now_v7();
        let mut create_request_1 = create_request(Some("tab-1".to_string()), Some(false));
        create_request_1.location = Some(format!("{base_location}/{tmp_id}/bucket"));
        let mut create_request_2 = create_request(Some("tab-2".to_string()), Some(false));
        create_request_2.location = Some(format!("{base_location}/{tmp_id}/bucket"));

        let _ = CatalogServer::create_table(
            ns_params.clone(),
            create_request_1,
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        let e = CatalogServer::create_table(
            ns_params.clone(),
            create_request_2,
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .expect_err("Table was created at same location which should not be possible");
        assert_eq!(e.error.code, StatusCode::CONFLICT, "{e:?}");
        assert_eq!(e.error.r#type.as_str(), "LocationAlreadyTaken");
    }

    #[sqlx::test]
    async fn test_cannot_create_staged_tables_at_sublocations_1(pool: PgPool) {
        let (ctx, _, ns_params, base_location) = table_test_setup(pool).await;
        let tmp_id = Uuid::now_v7();
        let mut create_request_1 = create_request(Some("tab-1".to_string()), Some(false));
        create_request_1.stage_create = Some(true);
        create_request_1.location = Some(format!("{base_location}/{tmp_id}/bucket/inner"));
        let mut create_request_2 = create_request(Some("tab-2".to_string()), Some(false));
        create_request_2.stage_create = Some(true);
        create_request_2.location = Some(format!("{base_location}/{tmp_id}/bucket"));
        let _ = CatalogServer::create_table(
            ns_params.clone(),
            create_request_1,
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        let e = CatalogServer::create_table(
            ns_params.clone(),
            create_request_2,
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .expect_err("Staged table could be created at sublocation which should not be possible");
        assert_eq!(e.error.code, StatusCode::CONFLICT, "{e:?}");
        assert_eq!(e.error.r#type.as_str(), "LocationAlreadyTaken");
    }

    #[sqlx::test]
    async fn test_cannot_create_staged_tables_at_sublocations_2(pool: PgPool) {
        let (ctx, _, ns_params, base_location) = table_test_setup(pool).await;
        let tmp_id = Uuid::now_v7();
        let mut create_request_1 = create_request(Some("tab-1".to_string()), Some(false));
        create_request_1.stage_create = Some(true);
        create_request_1.location = Some(format!("{base_location}/{tmp_id}/bucket"));
        let mut create_request_2 = create_request(Some("tab-2".to_string()), Some(false));
        create_request_2.stage_create = Some(true);
        create_request_2.location = Some(format!("{base_location}/{tmp_id}/bucket/inner"));
        let _ = CatalogServer::create_table(
            ns_params.clone(),
            create_request_1,
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        let e = CatalogServer::create_table(
            ns_params.clone(),
            create_request_2,
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .expect_err("Staged table could be created at sublocation which should not be possible");
        assert_eq!(e.error.code, StatusCode::CONFLICT, "{e:?}");
        assert_eq!(e.error.r#type.as_str(), "LocationAlreadyTaken");
    }

    #[sqlx::test]
    async fn test_cannot_create_tables_at_sublocations_1(pool: PgPool) {
        let (ctx, _, ns_params, base_location) = table_test_setup(pool).await;
        let tmp_id = Uuid::now_v7();

        let mut create_request_1 = create_request(Some("tab-1".to_string()), Some(false));
        create_request_1.location = Some(format!("{base_location}/{tmp_id}/bucket"));
        let mut create_request_2 = create_request(Some("tab-2".to_string()), Some(false));
        create_request_2.location = Some(format!("{base_location}/{tmp_id}/bucket/sublocation"));
        let _ = CatalogServer::create_table(
            ns_params.clone(),
            create_request_1,
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        let e = CatalogServer::create_table(
            ns_params.clone(),
            create_request_2,
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .expect_err("Staged table could be created at sublocation which should not be possible");
        assert_eq!(e.error.code, StatusCode::CONFLICT, "{e:?}");
        assert_eq!(e.error.r#type.as_str(), "LocationAlreadyTaken");
    }

    async fn pagination_test_setup(
        pool: PgPool,
        n_tables: usize,
        hidden_ranges: &[(usize, usize)],
    ) -> (
        ApiContext<State<HidingAuthorizer, PostgresBackend, SecretsState>>,
        NamespaceParameters,
    ) {
        let prof = crate::server::test::memory_io_profile();
        let base_location = prof.base_location().unwrap();
        let authz = HidingAuthorizer::new();
        // Prevent hidden tables from becoming visible through `can_list_everything`.
        authz.block_can_list_everything();

        let (ctx, warehouse) = crate::server::test::setup(
            pool.clone(),
            prof,
            None,
            authz.clone(),
            TabularDeleteProfile::Hard {},
            Some(UserId::new_unchecked("oidc", "test-user-id")),
        )
        .await;
        let ns = crate::server::test::create_ns(
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
            let mut create_request = create_request(Some(format!("{i}")), Some(false));
            create_request.location = Some(format!("{base_location}/bucket/{i}"));
            let tab = CatalogServer::create_table(
                ns_params.clone(),
                create_request,
                DataAccess::not_specified(),
                ctx.clone(),
                RequestMetadata::new_unauthenticated(),
            )
            .await
            .unwrap();
            for (start, end) in hidden_ranges.iter().copied() {
                if i >= start && i < end {
                    authz.hide(&format!(
                        "table:{}/{}",
                        warehouse.warehouse_id,
                        tab.metadata.uuid()
                    ));
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
        let prof = crate::server::test::memory_io_profile();

        let authz = HidingAuthorizer::new();
        // Prevent hidden tables from becoming visible through `can_list_everything`.
        authz.block_can_list_everything();

        let (ctx, warehouse) = crate::server::test::setup(
            pool.clone(),
            prof,
            None,
            authz.clone(),
            TabularDeleteProfile::Hard {},
            Some(UserId::new_unchecked("oidc", "test-user-id")),
        )
        .await;
        let ns = crate::server::test::create_ns(
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
                create_request(Some(format!("tab-{i}")), Some(false)),
                DataAccess {
                    vended_credentials: true,
                    remote_signing: false,
                },
                ctx.clone(),
                RequestMetadata::new_unauthenticated(),
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
                return_protection_status: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
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
                return_protection_status: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
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
                return_protection_status: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
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
                return_protection_status: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
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
                return_protection_status: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
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
            authz.hide(&format!("table:{}/{t}", warehouse.warehouse_id));
        }

        let page = CatalogServer::list_tables(
            ns_params.clone(),
            ListTablesQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(5),
                return_uuids: true,
                return_protection_status: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
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
                return_protection_status: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
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

    #[sqlx::test]
    async fn test_list_tables(pool: sqlx::PgPool) {
        let prof = crate::server::test::memory_io_profile();

        let authz = HidingAuthorizer::new();

        let (ctx, warehouse) = crate::server::test::setup(
            pool.clone(),
            prof,
            None,
            authz.clone(),
            TabularDeleteProfile::Hard {},
            Some(UserId::new_unchecked("oidc", "test-user-id")),
        )
        .await;
        let ns = crate::server::test::create_ns(
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
                create_request(Some(format!("tab-{i}")), Some(false)),
                DataAccess {
                    vended_credentials: true,
                    remote_signing: false,
                },
                ctx.clone(),
                RequestMetadata::new_unauthenticated(),
            )
            .await
            .unwrap();
        }

        // By default `HidingAuthorizer` allows everything, meaning the quick check path in
        // `list_tables` will be hit since `can_list_everything: true`.
        let all = CatalogServer::list_tables(
            ns_params.clone(),
            ListTablesQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(11),
                return_uuids: true,
                return_protection_status: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        assert_eq!(all.identifiers.len(), 10);

        // Block `can_list_everything` to hit alternative code path.
        ctx.v1_state.authz.block_can_list_everything();
        let all = CatalogServer::list_tables(
            ns_params.clone(),
            ListTablesQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(11),
                return_uuids: true,
                return_protection_status: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        assert_eq!(all.identifiers.len(), 10);
    }

    #[sqlx::test]
    async fn test_cannot_drop_protected_table(pool: PgPool) {
        let (ctx, _, ns_params, _) = table_test_setup(pool).await;
        let table_ident = TableIdent {
            namespace: ns_params.namespace.clone(),
            name: "tab-1".to_string(),
        };
        let tab = CatalogServer::create_table(
            ns_params.clone(),
            create_request(Some("tab-1".to_string()), Some(false)),
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        ManagementApiServer::set_table_protection(
            tab.metadata.uuid().into(),
            WarehouseId::from_str_or_internal(ns_params.prefix.clone().unwrap().as_str()).unwrap(),
            true,
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        let e = CatalogServer::drop_table(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: table_ident.clone(),
            },
            DropParams {
                purge_requested: true,
                force: false,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .expect_err("Table was dropped which should not be possible");
        assert_eq!(e.error.code, StatusCode::CONFLICT, "{e:?}");

        ManagementApiServer::set_table_protection(
            tab.metadata.uuid().into(),
            WarehouseId::from_str_or_internal(ns_params.prefix.clone().unwrap().as_str()).unwrap(),
            false,
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        CatalogServer::drop_table(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: table_ident.clone(),
            },
            DropParams {
                purge_requested: true,
                force: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
    }

    #[sqlx::test]
    async fn test_can_force_drop_protected_table(pool: PgPool) {
        let (ctx, _, ns_params, _) = table_test_setup(pool).await;
        let table_ident = TableIdent {
            namespace: ns_params.namespace.clone(),
            name: "tab-1".to_string(),
        };
        let tab = CatalogServer::create_table(
            ns_params.clone(),
            create_request(Some("tab-1".to_string()), Some(false)),
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        ManagementApiServer::set_table_protection(
            tab.metadata.uuid().into(),
            WarehouseId::from_str_or_internal(ns_params.prefix.clone().unwrap().as_str()).unwrap(),
            true,
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        CatalogServer::drop_table(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: table_ident.clone(),
            },
            DropParams {
                purge_requested: true,
                force: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .expect("Table couldn't be force dropped which should be possible");
    }

    #[sqlx::test]
    async fn test_rename_table_without_can_rename(pool: sqlx::PgPool) {
        let prof = crate::server::test::memory_io_profile();
        let authz = HidingAuthorizer::new();
        let (ctx, warehouse) = crate::server::test::setup(
            pool.clone(),
            prof,
            None,
            authz.clone(),
            TabularDeleteProfile::Hard {},
            Some(UserId::new_unchecked("oidc", "test-user-id")),
        )
        .await;

        let from_ns = crate::server::test::create_ns(
            ctx.clone(),
            warehouse.warehouse_id.to_string(),
            "from_ns".to_string(),
        )
        .await;
        let to_ns = crate::server::test::create_ns(
            ctx.clone(),
            warehouse.warehouse_id.to_string(),
            "to_ns".to_string(),
        )
        .await;
        let ns_params = NamespaceParameters {
            prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
            namespace: from_ns.namespace.clone(),
        };
        let prefix = Some(Prefix(warehouse.warehouse_id.to_string()));
        let table_name = "from_table".to_string();
        CatalogServer::create_table(
            ns_params.clone(),
            create_request(Some(table_name.clone()), Some(false)),
            DataAccess {
                vended_credentials: true,
                remote_signing: false,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        // Not authorized to rename the source table
        authz.block_action(format!("table:{:?}", CatalogTableAction::Rename).as_str());
        let rename_table_request = RenameTableRequest {
            source: TableIdent {
                namespace: ns_params.namespace.clone(),
                name: table_name.clone(),
            },
            destination: TableIdent {
                namespace: to_ns.namespace.clone(),
                name: table_name.clone(),
            },
        };
        let response = CatalogServer::rename_table(
            prefix.clone(),
            rename_table_request.clone(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap_err();

        assert_eq!(response.error.code, StatusCode::FORBIDDEN, "{response:?}");

        // If we also block the get_metadata_action, the user is not allowed to know if the table exists.
        // thus, we should get a 404 instead.
        authz.block_action(format!("table:{:?}", CatalogTableAction::GetMetadata).as_str());
        let response = CatalogServer::rename_table(
            prefix,
            rename_table_request,
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap_err();
        assert_eq!(response.error.code, StatusCode::NOT_FOUND, "{response:?}");
    }

    #[sqlx::test]
    async fn test_rename_table_without_can_create(pool: sqlx::PgPool) {
        let prof = crate::server::test::memory_io_profile();
        let authz = HidingAuthorizer::new();
        let (ctx, warehouse) = crate::server::test::setup(
            pool.clone(),
            prof,
            None,
            authz.clone(),
            TabularDeleteProfile::Hard {},
            Some(UserId::new_unchecked("oidc", "test-user-id")),
        )
        .await;

        let from_ns = crate::server::test::create_ns(
            ctx.clone(),
            warehouse.warehouse_id.to_string(),
            "from_ns".to_string(),
        )
        .await;
        let to_ns = crate::server::test::create_ns(
            ctx.clone(),
            warehouse.warehouse_id.to_string(),
            "to_ns".to_string(),
        )
        .await;
        let ns_params = NamespaceParameters {
            prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
            namespace: from_ns.namespace.clone(),
        };
        let prefix = Some(Prefix(warehouse.warehouse_id.to_string()));
        let table_name = "from_table".to_string();
        CatalogServer::create_table(
            ns_params.clone(),
            create_request(Some(table_name.clone()), Some(false)),
            DataAccess {
                vended_credentials: true,
                remote_signing: false,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        // Not authorized to create a table in the destination namepsace
        authz.block_action(
            format!(
                "namespace:{:?}",
                CatalogNamespaceAction::CreateTable {
                    properties: Arc::default(),
                }
            )
            .as_str(),
        );
        let response = CatalogServer::rename_table(
            prefix,
            RenameTableRequest {
                source: TableIdent {
                    namespace: ns_params.namespace.clone(),
                    name: table_name.clone(),
                },
                destination: TableIdent {
                    namespace: to_ns.namespace.clone(),
                    name: table_name,
                },
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap_err();

        assert_eq!(response.error.code, StatusCode::FORBIDDEN);
        assert_eq!(response.error.r#type, "NamespaceActionForbidden");
    }

    #[sqlx::test]
    async fn test_rename_table_without_target_namespace(pool: sqlx::PgPool) {
        let prof = crate::server::test::memory_io_profile();
        let (ctx, warehouse) = crate::server::test::setup(
            pool.clone(),
            prof,
            None,
            AllowAllAuthorizer::default(),
            TabularDeleteProfile::Hard {},
            Some(UserId::new_unchecked("oidc", "test-user-id")),
        )
        .await;

        let from_ns = crate::server::test::create_ns(
            ctx.clone(),
            warehouse.warehouse_id.to_string(),
            "from_ns".to_string(),
        )
        .await;
        let ns_params = NamespaceParameters {
            prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
            namespace: from_ns.namespace.clone(),
        };
        let prefix = Some(Prefix(warehouse.warehouse_id.to_string()));
        let table_name = "from_table".to_string();
        CatalogServer::create_table(
            ns_params.clone(),
            create_request(Some(table_name.clone()), Some(false)),
            DataAccess {
                vended_credentials: true,
                remote_signing: false,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        // All actions are allowed but the target namespace does not exist
        let response = CatalogServer::rename_table(
            prefix,
            RenameTableRequest {
                source: TableIdent {
                    namespace: ns_params.namespace.clone(),
                    name: table_name.clone(),
                },
                destination: TableIdent {
                    namespace: NamespaceIdent::new("to_ns".to_string()),
                    name: table_name,
                },
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap_err();

        assert_eq!(response.error.code, StatusCode::NOT_FOUND);
        assert_eq!(response.error.r#type, "NoSuchNamespaceException");
    }

    #[sqlx::test]
    async fn test_rename_table_without_source_table(pool: sqlx::PgPool) {
        let prof = crate::server::test::memory_io_profile();
        let (ctx, warehouse) = crate::server::test::setup(
            pool.clone(),
            prof,
            None,
            AllowAllAuthorizer::default(),
            TabularDeleteProfile::Hard {},
            Some(UserId::new_unchecked("oidc", "test-user-id")),
        )
        .await;

        let from_ns = crate::server::test::create_ns(
            ctx.clone(),
            warehouse.warehouse_id.to_string(),
            "from_ns".to_string(),
        )
        .await;
        let to_ns = crate::server::test::create_ns(
            ctx.clone(),
            warehouse.warehouse_id.to_string(),
            "to_ns".to_string(),
        )
        .await;
        let prefix = Some(Prefix(warehouse.warehouse_id.to_string()));
        let table_name = "from_table".to_string();

        // All actions are allowed but the origin table does not exist
        let response = CatalogServer::rename_table(
            prefix,
            RenameTableRequest {
                source: TableIdent {
                    namespace: from_ns.namespace.clone(),
                    name: table_name.clone(),
                },
                destination: TableIdent {
                    namespace: to_ns.namespace.clone(),
                    name: table_name,
                },
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap_err();

        assert_eq!(response.error.code, StatusCode::NOT_FOUND);
    }

    #[sqlx::test]
    async fn test_register_table_with_overwrite(pool: PgPool) {
        let (ctx, ns, ns_params, _) = table_test_setup(pool).await;

        // Create a table first
        let initial_table = CatalogServer::create_table(
            ns_params.clone(),
            create_request(Some("test_overwrite".to_string()), Some(false)),
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        // Verify the table exists
        let table_ident = TableIdent {
            namespace: ns.namespace.clone(),
            name: "test_overwrite".to_string(),
        };

        CatalogServer::table_exists(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: table_ident.clone(),
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        // Now create a second table to use for the overwrite test
        let second_table = CatalogServer::create_table(
            ns_params.clone(),
            create_request(Some("second_table".to_string()), Some(false)),
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        // Read table metadata

        // Drop second table, keep data
        CatalogServer::drop_table(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: TableIdent {
                    namespace: ns.namespace.clone(),
                    name: "second_table".to_string(),
                },
            },
            DropParams {
                purge_requested: false,
                force: false,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .expect("Failed to drop second table");

        // Test without overwrite flag - should fail
        let register_request = iceberg_ext::catalog::rest::RegisterTableRequest::builder()
            .name("test_overwrite".to_string())
            .metadata_location(second_table.metadata_location.as_ref().unwrap().clone())
            .build();

        CatalogServer::register_table(
            ns_params.clone(),
            register_request.clone(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .expect_err("Registration should fail without overwrite flag");

        // Test with overwrite flag - should succeed
        let register_request_with_overwrite =
            iceberg_ext::catalog::rest::RegisterTableRequest::builder()
                .name("test_overwrite".to_string())
                .metadata_location(second_table.metadata_location.as_ref().unwrap().clone())
                .overwrite(true)
                .build();

        let result = CatalogServer::register_table(
            ns_params.clone(),
            register_request_with_overwrite,
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await;

        assert!(
            result.is_ok(),
            "Registration with overwrite flag should succeed, but failed with: {:?}",
            result.err().map(|e| e.error.message)
        );

        // Verify the table exists and has the new metadata
        let loaded_table = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix,
                table: table_ident,
            },
            DataAccess::not_specified(),
            LoadTableFilters::default(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
            Vec::new(),
        )
        .await
        .unwrap();

        let LoadTableResultOrNotModified::LoadTableResult(loaded_table) = loaded_table else {
            panic!("Expected LoadTableResult, got NotModified");
        };

        // The loaded table should have the UUID and content of the second table
        assert_eq!(loaded_table.metadata.uuid(), second_table.metadata.uuid());
        assert_ne!(loaded_table.metadata.uuid(), initial_table.metadata.uuid());
    }

    // Reasons for using a mix of PostgresCatalog and CatalogServer:
    //
    // - PostgresCatalog: required for specifying id of table to be created
    // - CatalogServer: required for taking TabularDeleteProfile into account
    #[sqlx::test]
    async fn test_reuse_table_ids_hard_delete(pool: PgPool) {
        let delete_profile = TabularDeleteProfile::Hard {};
        let (ctx, mut wh_ns_data, _base_loc) =
            tabular_test_multi_warehouse_setup(pool.clone(), 3, delete_profile).await;

        let t_id = TableId::new_random();
        let t_name = "t1".to_string();
        let list_flags = TabularListFlags::all();

        // Create tables with the same table ID across different warehouses.
        for (wh_id, _ns_id, ns_params) in &wh_ns_data {
            let _inited_table = initialize_table(
                *wh_id,
                ctx.v1_state.catalog.clone(),
                false,
                Some(ns_params.namespace.clone()),
                Some(t_id),
                Some(t_name.clone()),
            )
            .await;

            // Verify table creation.
            let _meta = PostgresBackend::get_table_info(
                *wh_id,
                t_id,
                list_flags,
                ctx.v1_state.catalog.clone(),
            )
            .await
            .unwrap()
            .expect("table and metadata should exist");
        }

        // Hard delete one of the tables.
        let deleted_table_data = wh_ns_data.pop().unwrap();
        CatalogServer::drop_table(
            TableParameters {
                prefix: deleted_table_data.2.prefix.clone(),
                table: TableIdent {
                    namespace: deleted_table_data.2.namespace.clone(),
                    name: t_name.clone(),
                },
            },
            DropParams {
                purge_requested: false,
                force: false,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        // Deleted table cannot be accessed anymore.
        let deleted_res = PostgresBackend::get_table_info(
            deleted_table_data.0,
            t_id,
            list_flags,
            ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap();
        assert!(deleted_res.is_none(), "Table should be deleted");

        // Tables in other warehouses are still there.
        assert!(!wh_ns_data.is_empty());
        for (wh_id, _ns_id, _ns_params) in &wh_ns_data {
            PostgresBackend::get_table_info(*wh_id, t_id, list_flags, ctx.v1_state.catalog.clone())
                .await
                .unwrap()
                .expect("table and metadata should still exist");
        }

        // As the delete was hard, the table can be recreated in the warehouse.
        let _inited_table = initialize_table(
            deleted_table_data.0,
            ctx.v1_state.catalog.clone(),
            false,
            Some(deleted_table_data.2.namespace.clone()),
            Some(t_id),
            Some(t_name.clone()),
        )
        .await;
        let _meta = PostgresBackend::get_table_info(
            deleted_table_data.0,
            t_id,
            list_flags,
            ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap()
        .expect("table and metadata should exist");
    }

    // Reasons for using a mix of PostgresCatalog and CatalogServer:
    //
    // - PostgresCatalog: required for specifying id of table to be created
    // - CatalogServer: required for taking TabularDeleteProfile into account
    #[sqlx::test]
    async fn test_reuse_table_ids_soft_delete(pool: PgPool) {
        let delete_profile = TabularDeleteProfile::Soft {
            expiration_seconds: chrono::Duration::seconds(10),
        };
        let (ctx, mut wh_ns_data, _base_loc) =
            tabular_test_multi_warehouse_setup(pool.clone(), 3, delete_profile).await;

        let t_id = TableId::new_random();
        let t_name = "t1".to_string();
        let list_flags_active = TabularListFlags::active();

        // Create tables with the same table ID across different warehouses.
        for (wh_id, _ns_id, ns_params) in &wh_ns_data {
            let _inited_table = initialize_table(
                *wh_id,
                ctx.v1_state.catalog.clone(),
                false,
                Some(ns_params.namespace.clone()),
                Some(t_id),
                Some(t_name.clone()),
            )
            .await;

            // Verify table creation.
            let _meta = PostgresBackend::get_table_info(
                *wh_id,
                t_id,
                list_flags_active,
                ctx.v1_state.catalog.clone(),
            )
            .await
            .unwrap()
            .expect("table and metadata should exist");
        }

        // Soft delete one of the tables.
        let deleted_table_data = wh_ns_data.pop().unwrap();
        CatalogServer::drop_table(
            TableParameters {
                prefix: deleted_table_data.2.prefix.clone(),
                table: TableIdent {
                    namespace: deleted_table_data.2.namespace.clone(),
                    name: t_name.clone(),
                },
            },
            DropParams {
                purge_requested: false,
                force: false,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        // Check availability depending on list flags.
        let deleted_res = PostgresBackend::get_table_info(
            deleted_table_data.0,
            t_id,
            list_flags_active,
            ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap();
        assert!(deleted_res.is_none(), "Table should be soft deleted");
        let deleted_res = PostgresBackend::get_table_info(
            deleted_table_data.0,
            t_id,
            TabularListFlags::all(), // include soft deleted
            ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap();
        assert!(deleted_res.is_some(), "Table should be only soft deleted");

        // Tables in other warehouses are still there.
        assert!(!wh_ns_data.is_empty());
        for (wh_id, _ns_id, _ns_params) in &wh_ns_data {
            PostgresBackend::get_table_info(
                *wh_id,
                t_id,
                list_flags_active,
                ctx.v1_state.catalog.clone(),
            )
            .await
            .unwrap()
            .expect("table and metadata should still exist");
        }
    }
}
