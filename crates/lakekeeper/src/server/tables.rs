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
    catalog::rest::{IcebergErrorResponse, LoadCredentialsResponse, StorageCredential},
    configs::ParseFromStr,
};
use itertools::Itertools;
use lakekeeper_io::Location;
use serde::Serialize;
use uuid::Uuid;
pub mod authorize_load;
pub mod create_table;
pub mod load_table;
mod rename_table;

pub(crate) use authorize_load::*;

use super::{
    CatalogServer,
    commit_tables::{apply_commit, ensure_format_version_upgrades_allowed},
    io::{delete_file, read_metadata_file, write_file},
    maybe_get_secret,
    namespace::validate_namespace_ident,
    require_warehouse_id,
};
use crate::{
    WarehouseId, XXHashSet,
    api::{
        endpoints::EndpointFlat,
        iceberg::{
            types::DropParams,
            v1::{
                ApiContext, CommitTableRequest, CommitTableResponse, CommitTransactionRequest,
                CreateTableRequest, DataAccess, ErrorModel, ListTablesQuery, ListTablesResponse,
                LoadTableResult, LoadTableResultOrNotModified, NamespaceParameters, Prefix,
                ReferencingView, RegisterTableRequest, RenameTableRequest, Result, TableIdent,
                TableParameters,
                tables::{
                    DataAccessMode, LoadTableCredentialsRequest, LoadTableFilters, LoadTableRequest,
                },
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
        AuthZTableInfo, CONCURRENT_UPDATE_ERROR_TYPE, CachePolicy, CatalogIdempotencyOps,
        CatalogNamespaceOps, CatalogStore, CatalogTableOps, CatalogTabularOps, CatalogWarehouseOps,
        NamedEntity, ResolvedWarehouse, State, TableCommit, TableCreation, TableId, TableIdentOrId,
        TableInfo, TabularId, TabularIdentBorrowed, TabularInfo, TabularListFlags, TabularNotFound,
        Transaction, WarehouseStatus,
        authz::{
            ActionOnTableOrView, AuthZCannotSeeNamespace, AuthZCannotSeeTable, AuthZCannotSeeView,
            AuthZError, AuthZTableActionForbidden, AuthZTableOps, AuthorizationCountMismatch,
            Authorizer, AuthzNamespaceOps, AuthzWarehouseOps, BackendUnavailableOrCountMismatch,
            CatalogNamespaceAction, CatalogTableAction, CatalogWarehouseAction,
            RequireNamespaceActionError, RequireTableActionError,
        },
        build_namespace_hierarchy,
        contract_verification::{ContractVerification, ContractVerificationOutcome},
        events::{
            APIEventCommitContext, APIEventContext, CommitTransactionEvent,
            context::{ResolvedNamespace, ResolvedTable},
        },
        idempotency::{IdempotencyCheck, IdempotencyInfo},
        require_namespace_for_tabular,
        secrets::SecretStore,
        storage::StoragePermissions,
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

/// Replay a load-table operation for idempotency.
///
/// Used when an idempotency check detects a replay for operations that
/// return a `LoadTableResult` (e.g. `createTable`, `registerTable`).
async fn replay_load_table<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: TableParameters,
    data_access: DataAccessMode,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
    operation_name: &str,
) -> Result<LoadTableResult> {
    let load_result = load_table::load_table::<C, A, S>(
        parameters,
        LoadTableRequest::builder().data_access(data_access).build(),
        state,
        request_metadata,
    )
    .await
    .map_err(|e| {
        ErrorModel::internal(
            format!("Failed to replay idempotent {operation_name}: {e}"),
            "IdempotencyReplayFailed",
            None,
        )
    })?;
    match load_result {
        LoadTableResultOrNotModified::LoadTableResult(r) => Ok(r),
        LoadTableResultOrNotModified::NotModifiedResponse(_) => {
            // Should not happen: replay uses LoadTableRequest::default() with no
            // If-None-Match header. If it does, treat as an internal error.
            Err(ErrorModel::internal(
                "Unexpected NotModified during idempotency replay",
                "IdempotencyReplayFailed",
                None,
            )
            .into())
        }
    }
}

/// Replay a commit-table operation for idempotency.
///
/// Used when an idempotency check detects a replay for `updateTable`.
async fn replay_commit_table<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: TableParameters,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<CommitTableResponse> {
    // CommitTableResponse doesn't include storage credentials, so default access mode is fine.
    let r = replay_load_table::<C, A, S>(
        parameters,
        DataAccessMode::default(),
        state,
        request_metadata,
        "updateTable",
    )
    .await?;
    let metadata_location = r.metadata_location.ok_or_else(|| {
        ErrorModel::internal(
            "Missing metadata_location during idempotency replay",
            "IdempotencyReplayFailed",
            None,
        )
    })?;
    Ok(CommitTableResponse {
        metadata_location,
        metadata: r.metadata,
        config: None,
    })
}

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
            identifiers: Arc::new(identifiers),
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

        // ------------------- IDEMPOTENCY CHECK -------------------
        let idempotency_key = request_metadata.idempotency_key().copied();
        if let Some(ref key) = idempotency_key {
            let check =
                C::check_idempotency_key(warehouse_id, key, state.v1_state.catalog.clone()).await?;
            if check.is_replay() {
                let load_params = TableParameters {
                    prefix: parameters.prefix.clone(),
                    table: table_ident,
                };
                return replay_load_table::<C, A, S>(
                    load_params,
                    DataAccessMode::default(),
                    state,
                    request_metadata,
                    "registerTable",
                )
                .await;
            }
        }

        // ------------------- AUTHZ + BUSINESS LOGIC -------------------
        let authorizer = state.v1_state.authz.clone();

        let event_ctx = APIEventContext::for_namespace(
            Arc::new(request_metadata),
            state.v1_state.events,
            warehouse_id,
            parameters.namespace.clone(),
            // Preliminary action, updated after Metadata is read
            CatalogNamespaceAction::CreateTable {
                name: Some(request.name.clone()),
                table_id: None,
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
            name: Some(request.name.clone()),
            table_id: Some(TableId::from(table_metadata.uuid())),
            properties: Arc::new(
                table_metadata
                    .properties()
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect::<BTreeMap<_, _>>(),
            ),
        };

        let mut event_ctx = event_ctx;
        event_ctx.override_action(action.clone());

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
                drop_tbl_event_ctx.push_extra_context("invoked-by", "register_table_overwrite");

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

        // Insert idempotency key in the same transaction.
        if let Some(ref key) = idempotency_key
            && !C::try_insert_idempotency_key(
                warehouse_id,
                &IdempotencyInfo::builder()
                    .key(*key)
                    .endpoint(EndpointFlat::CatalogV1RegisterTable)
                    .http_status(StatusCode::OK)
                    .build(),
                t_write.transaction(),
            )
            .await?
        {
            // Concurrent request committed the same key.
            t_write
                .rollback()
                .await
                .inspect_err(|e| {
                    tracing::warn!("Rollback failed after idempotency conflict: {e}");
                })
                .ok();
            return Err(ErrorModel::request_in_progress().into());
        }

        let mut auth_needs_delete = false;
        // Delete the previous table from authorizer if it exists and differs from the new one
        if let Some(previous_table_to_drop) = &previous_table_to_drop {
            if previous_table_to_drop.tabular_id != tabular_id {
                auth_needs_delete = true;
                authorizer
                    .create_table(request_metadata, warehouse_id, tabular_id, namespace_id)
                    .await?;
            }
        } else {
            authorizer
                .create_table(request_metadata, warehouse_id, tabular_id, namespace_id)
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
        request: LoadTableRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadTableResultOrNotModified> {
        load_table::load_table(parameters, request, state, request_metadata).await
    }

    async fn load_table_credentials(
        parameters: TableParameters,
        request: LoadTableCredentialsRequest,
        data_access: DataAccess,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadCredentialsResponse> {
        let LoadTableCredentialsRequest { referenced_by } = request;

        // ------------------- VALIDATIONS -------------------
        let TableParameters { prefix, table } = parameters;
        let warehouse_id = require_warehouse_id(prefix.as_ref())?;

        let event_ctx = APIEventContext::for_table(
            Arc::new(request_metadata),
            state.v1_state.events,
            warehouse_id,
            table.clone(),
            CatalogTableAction::ReadData,
        );

        let authz_result = match authorize_load_table::<C, A>(
            event_ctx.request_metadata(),
            table.clone(),
            warehouse_id,
            TabularListFlags::active_and_staged(),
            state.v1_state.authz,
            state.v1_state.catalog.clone(),
            referenced_by.as_deref(),
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
        // ------------------- VALIDATIONS -------------------
        request.identifier = Some(determine_table_ident(
            &parameters.table,
            request.identifier.as_ref(),
        )?);

        // ------------------- AUTHZ + BUSINESS LOGIC -------------------
        let idempotency_key = request_metadata.idempotency_key().copied();
        let idempotency = idempotency_key.map(|key| IdempotencyInfo {
            key,
            endpoint: EndpointFlat::CatalogV1UpdateTable,
            http_status: StatusCode::OK,
        });
        let result = commit_tables_with_authz(
            parameters.prefix.clone(),
            CommitTransactionRequest {
                table_changes: vec![request],
            },
            state.clone(),
            request_metadata.clone(),
            idempotency.as_ref(),
        )
        .await?;

        match result {
            CommitTablesResult::Replay => {
                replay_commit_table::<C, A, S>(parameters, state, request_metadata).await
            }
            CommitTablesResult::Committed(t) => {
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
        }
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

        // ------------------- IDEMPOTENCY CHECK -------------------
        let idempotency_key = request_metadata.idempotency_key().copied();
        if let Some(ref key) = idempotency_key {
            let check =
                C::check_idempotency_key(warehouse_id, key, state.v1_state.catalog.clone()).await?;
            if check.is_replay() {
                return Ok(());
            }
        }

        // ------------------- AUTHZ + BUSINESS LOGIC -------------------
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
            }
        }

        // Insert idempotency key and commit — shared across both delete profiles.
        if let Some(ref key) = idempotency_key
            && !C::try_insert_idempotency_key(
                warehouse_id,
                &IdempotencyInfo::builder()
                    .key(*key)
                    .endpoint(EndpointFlat::CatalogV1DropTable)
                    .http_status(StatusCode::NO_CONTENT)
                    .build(),
                t.transaction(),
            )
            .await?
        {
            t.rollback()
                .await
                .inspect_err(|e| {
                    tracing::warn!("Rollback failed after idempotency conflict: {e}");
                })
                .ok();
            return Err(ErrorModel::request_in_progress().into());
        }
        t.commit().await?;

        // Post-commit: best-effort authz cleanup for hard deletes
        if matches!(delete_profile, TabularDeleteProfile::Hard {}) {
            authorizer
                .delete_table(warehouse_id, table_id)
                .await
                .inspect_err(|e| {
                    tracing::error!(?e, "Failed to delete table from authorizer: {}", e.error);
                })
                .ok();
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
        // ------------------- AUTHZ + BUSINESS LOGIC -------------------
        let idempotency_key = request_metadata.idempotency_key().copied();
        let idempotency = idempotency_key.map(|key| IdempotencyInfo {
            key,
            endpoint: EndpointFlat::CatalogV1CommitTransaction,
            http_status: StatusCode::NO_CONTENT,
        });
        let result = commit_tables_with_authz(
            prefix,
            request,
            state,
            request_metadata,
            idempotency.as_ref(),
        )
        .await?;
        match result {
            CommitTablesResult::Replay => Ok(()),
            CommitTablesResult::Committed(contexts) => {
                tracing::debug!("Successfully committed {} table(s)", contexts.len());
                Ok(())
            }
        }
    }
}

async fn authorize_load_table<C: CatalogStore, A: Authorizer + Clone>(
    request_metadata: &RequestMetadata,
    table: TableIdent,
    warehouse_id: WarehouseId,
    list_flags: TabularListFlags,
    authorizer: A,
    state: C::State,
    referenced_by: Option<&[ReferencingView]>,
) -> Result<
    (
        Arc<ResolvedWarehouse>,
        TableInfo,
        Option<StoragePermissions>,
    ),
    AuthZError,
> {
    let engines = request_metadata.engines();
    let referenced_by = effective_referenced_by(referenced_by, engines);

    // 1. Collect all relevant namespace idents
    let user_provided_namespaces = get_relevant_namespaces_to_authorize_load_tabular(
        &TabularIdentBorrowed::Table(&table),
        referenced_by,
    );

    // 2. Collect all relevant tabular idents
    let user_provided_tabulars = get_relevant_tabulars_to_authorize_load_tabular(
        TabularIdentBorrowed::Table(&table),
        referenced_by,
    );

    // 3. Load objects concurrently
    let AuthorizeLoadTabularObjects {
        warehouse,
        namespaces,
        tabulars,
    } = load_objects_to_authorize_load_tabular::<C>(
        warehouse_id,
        user_provided_namespaces.clone().into_iter().collect(),
        user_provided_tabulars.clone().into_iter().collect(),
        list_flags,
        state.clone(),
    )
    .await;

    // 4. Check objects presence
    let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;
    let tabulars =
        check_required_tabulars(warehouse_id, user_provided_tabulars, tabulars, &authorizer)?;
    let namespaces =
        check_required_namespaces(warehouse_id, &user_provided_namespaces, namespaces)?;

    // 5. Build NamespaceHierarchy
    let namespaces_with_hierarchy = namespaces
        .iter()
        .map(|(namespace_id, namespace)| {
            (
                *namespace_id,
                build_namespace_hierarchy(namespace, &namespaces),
            )
        })
        .collect::<HashMap<_, _>>();

    // 6. Sort tabulars by comparing initial referenced_by list plus appended table/view
    let sorted_tabulars =
        sort_tabulars_for_authorize_load_tabular(&tabulars, referenced_by, &table);

    // 7. Connect tabular with namespaces by using namespace_id
    let sorted_tabulars = add_namespace_to_tabulars_for_authorize_load_tabular(
        warehouse_id,
        sorted_tabulars,
        &namespaces_with_hierarchy,
    )?;

    // 8. Resolve owners and assign the current user for each tabular in the chain.
    //    DEFINER views switch current_user to the view owner for subsequent tabulars.
    let token_idp_id = request_metadata
        .authentication()
        .and_then(|a| a.subject().idp_id())
        .map(String::as_str);
    let sorted_tabulars_with_full_info = resolve_users_for_authorize_load_tabular(
        &sorted_tabulars,
        request_metadata.actor(),
        engines,
        token_idp_id,
    )?;

    // 9. Build actions and check all authorizations in batch.
    let actions = build_actions_from_sorted_tabulars_for_authorize_load_tabular(
        &sorted_tabulars_with_full_info,
    );
    let authz_results = authorizer
        .are_allowed_tabular_actions_vec(request_metadata, &warehouse, &namespaces, &actions)
        .await?
        .into_allowed();

    // 10. Interpret authorization results.
    let (table_info, storage_permissions) =
        interpret_authz_results_for_load_table(&actions, &authz_results, warehouse_id, &table)?;

    Ok((warehouse, table_info, storage_permissions))
}

/// Interpret the flat `Vec<bool>` authorization results by matching each result
/// to its corresponding action. This avoids relying on positional indices.
///
/// Returns `(TableInfo, Option<StoragePermissions>)` for the target table.
pub fn interpret_authz_results_for_load_table(
    actions: &[TabularAuthzAction<'_>],
    authz_results: &[bool],
    warehouse_id: WarehouseId,
    table: &TableIdent,
) -> Result<(TableInfo, Option<StoragePermissions>), AuthZError> {
    if actions.len() != authz_results.len() {
        return Err(
            BackendUnavailableOrCountMismatch::from(AuthorizationCountMismatch::new(
                actions.len(),
                authz_results.len(),
                "load_table",
            ))
            .into(),
        );
    }

    let mut table_info: Option<TableInfo> = None;
    let mut table_is_delegated = false;
    let mut can_get_metadata = None;
    let mut can_read = None;
    let mut can_write = None;

    for ((_ns, action), &allowed) in actions.iter().zip(authz_results) {
        match action {
            ActionOnTableOrView::Table(table_action) => {
                if let Some(existing) = &table_info {
                    if existing.tabular_id != table_action.info.tabular_id {
                        return Err(BackendUnavailableOrCountMismatch::from(
                            AuthorizationCountMismatch::new(1, 2, "tables_in_chain"),
                        )
                        .into());
                    }
                } else {
                    table_info = Some(table_action.info.clone());
                    table_is_delegated = table_action.is_delegated_execution;
                }
                match &table_action.action {
                    CatalogTableAction::GetMetadata => can_get_metadata = Some(allowed),
                    CatalogTableAction::ReadData => can_read = Some(allowed),
                    CatalogTableAction::WriteData => can_write = Some(allowed),
                    _ => {}
                }
            }
            ActionOnTableOrView::View(view_action) => {
                if !allowed {
                    return Err(AuthZCannotSeeView::new_forbidden(
                        warehouse_id,
                        view_action.info.tabular_ident.clone(),
                    )
                    .with_delegated_execution(view_action.is_delegated_execution)
                    .into());
                }
            }
            ActionOnTableOrView::GenericTable(_) => {
                // Unreachable: loadTable authz chain only resolves tables and
                // intermediate views. Fail closed if the invariant breaks in
                // release — silent fall-through would let an unexpected
                // entry bypass authorization checks.
                return Err(BackendUnavailableOrCountMismatch::from(
                    AuthorizationCountMismatch::new(0, 0, "generic_table_in_load_table_chain"),
                )
                .into());
            }
        }
    }

    let table_info = table_info
        .ok_or_else(|| AuthZCannotSeeTable::new_not_found(warehouse_id, table.clone()))?;

    if !can_get_metadata.unwrap_or(false) {
        return Err(
            AuthZCannotSeeTable::new_forbidden(warehouse_id, table.clone())
                .with_delegated_execution(table_is_delegated)
                .into(),
        );
    }

    let storage_permissions = if can_write.unwrap_or(false) {
        Some(StoragePermissions::ReadWriteDelete)
    } else if can_read.unwrap_or(false) {
        Some(StoragePermissions::Read)
    } else {
        None
    };

    Ok((table_info, storage_permissions))
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
    idempotency: Option<&IdempotencyInfo>,
) -> Result<Arc<Vec<CommitContext>>> {
    let include_deleted = false;
    let warehouse_id = event_ctx.user_provided_entity().warehouse_id;

    // Start the retry loop
    let mut attempt = 0;
    loop {
        let result = try_commit_tables::<C, A, S>(
            &request,
            &warehouse,
            &event_ctx,
            &state,
            include_deleted,
            idempotency,
        )
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
pub async fn commit_tables_with_authz<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    prefix: Option<Prefix>,
    request: CommitTransactionRequest,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
    idempotency: Option<&IdempotencyInfo>,
) -> Result<CommitTablesResult> {
    // ------------------- VALIDATIONS -------------------
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    commit_tables_validate(&request)?;

    // ------------------- AUTHZ (+ parallel idempotency check) -------------------
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

    // Run authz and idempotency check in parallel.
    let (authz_result, idempotency_check) = tokio::join!(
        commit_tables_authz::<A, C>(
            authorizer,
            warehouse_id,
            &event_ctx.user_provided_entity().tables,
            &actions,
            state.v1_state.catalog.clone(),
            event_ctx.request_metadata(),
        ),
        async {
            if let Some(info) = idempotency {
                C::check_idempotency_key(warehouse_id, &info.key, state.v1_state.catalog.clone())
                    .await
            } else {
                Ok(IdempotencyCheck::NewRequest)
            }
        }
    );
    let idempotency_check = idempotency_check?;
    let (event_ctx, authz_result) = event_ctx.emit_authz(authz_result)?;

    // If the idempotency check determined this is a replay, return early.
    if idempotency_check.is_replay() {
        return Ok(CommitTablesResult::Replay);
    }

    let warehouse = authz_result.warehouse;
    let table_infos = authz_result
        .table_infos_with_actions
        .into_iter()
        .map(|(ident, (info, _action))| (ident, info))
        .collect::<HashMap<_, _>>();
    let event_ctx = event_ctx.resolve(table_infos);

    // ------------------- BUSINESS LOGIC -------------------
    let commits =
        commit_tables_inner::<C, _, _>(warehouse, request, event_ctx, state, idempotency).await?;
    Ok(CommitTablesResult::Committed(commits))
}

struct CommitAuthorizationResult<'a> {
    table_infos_with_actions:
        HashMap<TableIdent, (Arc<TabularInfo<TableId>>, &'a CatalogTableAction)>,
    warehouse: Arc<ResolvedWarehouse>,
}

/// Result of `commit_tables_with_authz`, indicating whether the operation
/// was committed or is a replay of a previously committed idempotent request.
#[derive(Debug)]
pub enum CommitTablesResult {
    /// The commit was executed successfully.
    Committed(Arc<Vec<CommitContext>>),
    /// An idempotency check determined this is a replay of an already-committed
    /// request. The caller must reconstruct the response appropriately.
    Replay,
}

impl CommitTablesResult {
    /// Unwrap the committed result, panicking if this is a replay.
    /// Only intended for use in tests; gated behind `test-utils` so production
    /// callers can't accidentally panic on the replay branch.
    ///
    /// # Panics
    /// Panics if the result is `Replay`.
    #[cfg(any(test, feature = "test-utils"))]
    #[must_use]
    pub fn unwrap_committed(self) -> Arc<Vec<CommitContext>> {
        match self {
            Self::Committed(c) => c,
            Self::Replay => panic!("Expected CommitTablesResult::Committed, got Replay"),
        }
    }
}

async fn commit_tables_authz<'a, A: Authorizer + Clone, C: CatalogStore>(
    authorizer: A,
    warehouse_id: WarehouseId,
    identifiers: &[TableIdent],
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
    idempotency: Option<&IdempotencyInfo>,
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
            ensure_format_version_upgrades_allowed(
                &change.updates,
                &warehouse.allowed_format_versions,
            )?;
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

        // Insert idempotency key in the same transaction.
        if let Some(info) = idempotency
            && !C::try_insert_idempotency_key(warehouse_id, info, transaction.transaction()).await?
        {
            transaction
                .rollback()
                .await
                .inspect_err(|e| {
                    tracing::warn!("Rollback failed after idempotency conflict: {e}");
                })
                .ok();
            return Err(ErrorModel::request_in_progress().into());
        }

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

pub fn extract_count_from_metadata_location(location: &Location) -> Option<usize> {
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
pub fn calculate_diffs(
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

pub fn validate_table_properties<'a, I>(properties: I) -> Result<()>
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
pub fn parse_table_property_updates(
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
mod unit_tests {
    use std::{collections::HashMap, str::FromStr};

    use iceberg::{TableUpdate, spec::FormatVersion};
    use lakekeeper_io::Location;
    use uuid::Uuid;

    use super::*;
    use crate::{
        WarehouseId,
        service::{Actor, NamespaceHierarchy, UserId, ViewInfo, ViewOrTableInfo},
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
        let count = extract_count_from_metadata_location(&location).unwrap();
        assert_eq!(count, 0);

        let location = Location::from_str("s3://path/to/table/metadata/00010-d0407fb2-1112-4944-bb88-c68ae697e2b4.gz.metadata.json").unwrap();
        let count = extract_count_from_metadata_location(&location).unwrap();
        assert_eq!(count, 10);

        let location = Location::from_str(
            "s3://path/to/table/metadata/1-d0407fb2-1112-4944-bb88-c68ae697e2b4.gz.metadata.json",
        )
        .unwrap();
        let count = extract_count_from_metadata_location(&location).unwrap();
        assert_eq!(count, 1);

        let location = Location::from_str(
            "s3://path/to/table/metadata/10000010-d0407fb2-1112-4944-bb88-c68ae697e2b4.gz.metadata.json",
        )
            .unwrap();
        let count = extract_count_from_metadata_location(&location).unwrap();
        assert_eq!(count, 10_000_010);

        let location = Location::from_str(
            "s3://path/to/table/metadata/10000010-d0407fb2-1112-4944-bb88-c68ae697e2b4.metadata.json",
        )
            .unwrap();
        let count = extract_count_from_metadata_location(&location).unwrap();
        assert_eq!(count, 10_000_010);

        let location = Location::from_str(
            "s3://path/to/table/metadata/d0407fb2-1112-4944-bb88-c68ae697e2b4.metadata.json",
        )
        .unwrap();
        let count = extract_count_from_metadata_location(&location);
        assert!(count.is_none());
    }

    // ---- interpret_authz_results tests ----

    fn resolved(
        tabular: ViewOrTableInfo,
        actor: &Actor,
        namespace: NamespaceHierarchy,
    ) -> ResolvedTabular {
        ResolvedTabular {
            tabular,
            user: actor.to_user_or_role(),
            is_delegated_execution: false,
            namespace,
        }
    }

    #[test]
    fn test_interpret_authz_results_table_all_allowed() {
        let warehouse_id = WarehouseId::new_random();
        let table = TableInfo::new_random(warehouse_id);
        let namespace = NamespaceHierarchy::new_with_id(warehouse_id, table.namespace_id);
        let actor = Actor::Principal(UserId::new_unchecked("test", "user"));

        let tabulars = vec![resolved(table.clone().into(), &actor, namespace)];
        let actions = build_actions_from_sorted_tabulars_for_authorize_load_tabular(&tabulars);
        let results = vec![true, true, true];

        let (info, perms) = interpret_authz_results_for_load_table(
            &actions,
            &results,
            warehouse_id,
            &table.tabular_ident,
        )
        .unwrap();

        assert_eq!(info.tabular_id, table.tabular_id);
        assert_eq!(perms, Some(StoragePermissions::ReadWriteDelete));
    }

    #[test]
    fn test_interpret_authz_results_table_read_only() {
        let warehouse_id = WarehouseId::new_random();
        let table = TableInfo::new_random(warehouse_id);
        let namespace = NamespaceHierarchy::new_with_id(warehouse_id, table.namespace_id);
        let actor = Actor::Principal(UserId::new_unchecked("test", "user"));

        let tabulars = vec![resolved(table.clone().into(), &actor, namespace)];
        let actions = build_actions_from_sorted_tabulars_for_authorize_load_tabular(&tabulars);
        let results = vec![true, true, false];

        let (_, perms) = interpret_authz_results_for_load_table(
            &actions,
            &results,
            warehouse_id,
            &table.tabular_ident,
        )
        .unwrap();

        assert_eq!(perms, Some(StoragePermissions::Read));
    }

    #[test]
    fn test_interpret_authz_results_table_no_read_no_write() {
        let warehouse_id = WarehouseId::new_random();
        let table = TableInfo::new_random(warehouse_id);
        let namespace = NamespaceHierarchy::new_with_id(warehouse_id, table.namespace_id);
        let actor = Actor::Principal(UserId::new_unchecked("test", "user"));

        let tabulars = vec![resolved(table.clone().into(), &actor, namespace)];
        let actions = build_actions_from_sorted_tabulars_for_authorize_load_tabular(&tabulars);
        let results = vec![true, false, false];

        let (_, perms) = interpret_authz_results_for_load_table(
            &actions,
            &results,
            warehouse_id,
            &table.tabular_ident,
        )
        .unwrap();

        assert_eq!(perms, None);
    }

    #[test]
    fn test_interpret_authz_results_table_not_visible() {
        let warehouse_id = WarehouseId::new_random();
        let table = TableInfo::new_random(warehouse_id);
        let namespace = NamespaceHierarchy::new_with_id(warehouse_id, table.namespace_id);
        let actor = Actor::Principal(UserId::new_unchecked("test", "user"));

        let tabulars = vec![resolved(table.clone().into(), &actor, namespace)];
        let actions = build_actions_from_sorted_tabulars_for_authorize_load_tabular(&tabulars);
        let results = vec![false, false, false];

        let result = interpret_authz_results_for_load_table(
            &actions,
            &results,
            warehouse_id,
            &table.tabular_ident,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_interpret_authz_results_view_denied_in_chain() {
        let warehouse_id = WarehouseId::new_random();
        let view = ViewInfo::new_random(warehouse_id);
        let view_ns = NamespaceHierarchy::new_with_id(warehouse_id, view.namespace_id);
        let table = TableInfo::new_random(warehouse_id);
        let table_ns = NamespaceHierarchy::new_with_id(warehouse_id, table.namespace_id);
        let actor = Actor::Principal(UserId::new_unchecked("test", "user"));

        let tabulars = vec![
            resolved(view.into(), &actor, view_ns),
            resolved(table.clone().into(), &actor, table_ns),
        ];
        let actions = build_actions_from_sorted_tabulars_for_authorize_load_tabular(&tabulars);
        let results = vec![false, true, true, true];

        let result = interpret_authz_results_for_load_table(
            &actions,
            &results,
            warehouse_id,
            &table.tabular_ident,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_interpret_authz_results_count_mismatch() {
        let warehouse_id = WarehouseId::new_random();
        let table = TableInfo::new_random(warehouse_id);
        let namespace = NamespaceHierarchy::new_with_id(warehouse_id, table.namespace_id);
        let actor = Actor::Principal(UserId::new_unchecked("test", "user"));

        let tabulars = vec![resolved(table.clone().into(), &actor, namespace)];
        let actions = build_actions_from_sorted_tabulars_for_authorize_load_tabular(&tabulars);
        let results = vec![true, true]; // Only 2 results for 3 actions

        let result = interpret_authz_results_for_load_table(
            &actions,
            &results,
            warehouse_id,
            &table.tabular_ident,
        );
        assert!(result.is_err());
    }
}
