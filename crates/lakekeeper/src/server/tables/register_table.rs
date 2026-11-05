use std::{collections::HashMap, sync::Arc};

use iceberg::spec::TableMetadata;
use lakekeeper_io::Location;

use crate::{
    api::{
        ErrorModel,
        iceberg::v1::{
            LoadTableResult, Result,
            tables::{DataAccessMode, SnapshotsQuery},
        },
    },
    request_metadata::RequestMetadata,
    server::tables::{
        etag::{TableETag, TableResponseShape},
        load_response_config,
        load_table::storage_access_for,
    },
    service::{
        ArcResolvedWarehouse, NamespaceHierarchy, ResolvedWarehouse, TableInfo,
        authz::{
            ActionOnTable, AuthZTableOps, Authorizer, AuthzWarehouseOps, CatalogTableAction,
            CatalogWarehouseAction,
        },
        events::{
            APIEventContext, AuthorizationFailureReason, AuthorizationFailureSource,
            context::{
                APIEventActions, ResolutionState, UserProvidedEntity, authz_to_error_no_audit,
            },
        },
        storage::{StorageCredential, StoragePermissions, credential_revalidate_after_ms},
    },
};

/// Registering adopts files that are already in storage, so it needs `RegisterTable` on the
/// warehouse as well as `CreateTable` on the namespace. Asked before the metadata file is read,
/// and recorded on its own. A refusal also refuses the call's `CreateTable`, recorded first.
pub(super) async fn authorize_register_on_warehouse<A, P, R, Act>(
    authorizer: &A,
    event_ctx: &APIEventContext<P, R, Act>,
    warehouse: &ArcResolvedWarehouse,
) -> Result<()>
where
    A: Authorizer,
    P: UserProvidedEntity,
    R: ResolutionState,
    Act: APIEventActions,
{
    let action = CatalogWarehouseAction::RegisterTable;
    let register_ctx = APIEventContext::for_warehouse(
        event_ctx.request_metadata_arc(),
        event_ctx.dispatcher().clone(),
        warehouse.warehouse_id,
        action.clone(),
    );
    let authz_result = authorizer
        .require_warehouse_action(
            register_ctx.request_metadata(),
            warehouse.warehouse_id,
            Ok(Some(Arc::clone(warehouse))),
            action,
        )
        .await
        .map_err(|refusal| {
            let failure_reason = refusal.to_failure_reason();
            let error = event_ctx.emit_early_authz_failure(refusal);
            RecordedFailure {
                failure_reason,
                error,
            }
        });
    let (_register_ctx, _) = register_ctx.emit_authz(authz_result)?;
    Ok(())
}

/// A failure already recorded on one context, to record it on another.
struct RecordedFailure {
    failure_reason: AuthorizationFailureReason,
    error: ErrorModel,
}

impl AuthorizationFailureSource for RecordedFailure {
    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        self.failure_reason.clone()
    }

    fn into_error_model(self) -> ErrorModel {
        self.error
    }
}

/// A table that a register has committed, and what its response is built from.
#[derive(typed_builder::TypedBuilder)]
pub(super) struct RegisteredTable<'a, A> {
    authorizer: &'a A,
    request_metadata: &'a RequestMetadata,
    warehouse: &'a ResolvedWarehouse,
    namespace: &'a NamespaceHierarchy,
    table_info: &'a TableInfo,
    table_metadata: Arc<TableMetadata>,
    metadata_location: String,
    table_location: &'a Location,
    data_access: DataAccessMode,
    storage_secret: Option<&'a StorageCredential>,
}

/// The response to a register. It carries the storage access `loadTable` would give the
/// caller: credentials by the caller's read and write privileges on the registered table.
/// A failure here leaves the table registered; the error is what `loadTable` would return.
pub(super) async fn registered_table_result<A: Authorizer>(
    table: RegisteredTable<'_, A>,
) -> Result<LoadTableResult> {
    let storage_permissions = storage_permissions_on_registered_table(&table).await?;
    let storage_config = match storage_permissions {
        Some(permissions) => Some(
            table
                .warehouse
                .storage_profile
                .generate_table_config(
                    table.data_access,
                    table.storage_secret,
                    table.table_location,
                    permissions,
                    table.request_metadata,
                    table.table_info,
                )
                .await?,
        ),
        None => None,
    };

    let storage_credentials = storage_config
        .as_ref()
        .and_then(|c| c.storage_credentials(table.table_location));
    let credentials_revalidate_after_ms = storage_config
        .as_ref()
        .and_then(|c| c.credentials_expiration_ms)
        .map(credential_revalidate_after_ms);
    // The full snapshot list of the metadata file, tagged with the storage access the
    // config was built for, so a later `loadTable` by the same caller matches it.
    let etag = TableETag::new(
        table.warehouse.warehouse_id,
        &table.metadata_location,
        TableResponseShape::new(
            SnapshotsQuery::All,
            storage_access_for(
                storage_permissions,
                table.data_access,
                table.warehouse.version,
            ),
        ),
        credentials_revalidate_after_ms,
    )
    .into_etag();

    Ok(LoadTableResult {
        metadata_location: Some(table.metadata_location),
        metadata: table.table_metadata,
        remote_signing_config: storage_config
            .as_ref()
            .and_then(|c| c.remote_signing.clone()),
        config: Some(load_response_config(storage_config.map(|c| c.config))),
        storage_credentials,
        etag: Some(etag),
    })
}

/// Asked once the table is committed, so the authorizer sees what creating it conferred.
/// The register itself was authorized before; these checks decide no access, only scope.
async fn storage_permissions_on_registered_table<A: Authorizer>(
    table: &RegisteredTable<'_, A>,
) -> Result<Option<StoragePermissions>> {
    let parent_namespaces: HashMap<_, _> = table
        .namespace
        .parents
        .iter()
        .map(|ns| (ns.namespace_id(), ns.clone()))
        .collect();
    let on_table = |action| {
        (
            &table.namespace.namespace,
            ActionOnTable {
                info: table.table_info,
                action,
                user: None,
                is_delegated_execution: false,
            },
        )
    };
    let [can_read, can_write] = table
        .authorizer
        .are_allowed_table_actions_arr(
            table.request_metadata,
            table.warehouse,
            &parent_namespaces,
            &[
                on_table(CatalogTableAction::ReadData),
                on_table(CatalogTableAction::WriteData),
            ],
        )
        .await
        .map_err(authz_to_error_no_audit)?
        .into_inner();
    Ok(StoragePermissions::from_data_privileges(
        can_read, can_write,
    ))
}
