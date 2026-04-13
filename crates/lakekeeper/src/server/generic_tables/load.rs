use std::sync::Arc;

use iceberg_ext::catalog::rest::StorageCredential;

use crate::{
    api::{
        ApiContext,
        iceberg::v1::DataAccessMode,
        v1::generic_tables::{GenericTableData, GenericTableParameters, LoadGenericTableResponse},
    },
    request_metadata::RequestMetadata,
    server::{maybe_get_secret, require_warehouse_id},
    service::{
        CatalogStore, Result, SecretStore, State,
        authz::{AuthZGenericTableOps, Authorizer, CatalogGenericTableAction},
        events::{APIEventContext, AuthorizationFailureSource, context::ResolvedNamespace},
        storage::StoragePermissions,
    },
};

pub(super) async fn load_generic_table<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: GenericTableParameters,
    state: ApiContext<State<A, C, S>>,
    data_access: impl Into<DataAccessMode>,
    request_metadata: RequestMetadata,
) -> Result<LoadGenericTableResponse> {
    let data_access = data_access.into();

    let GenericTableParameters {
        prefix,
        namespace,
        table_name,
    } = parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    let authorizer = &state.v1_state.authz;

    // ------------------- AUTHZ: GetMetadata -------------------
    let event_ctx = APIEventContext::for_generic_table(
        Arc::new(request_metadata.clone()),
        state.v1_state.events.clone(),
        warehouse_id,
        iceberg::TableIdent::new(namespace.clone(), table_name.clone()),
        CatalogGenericTableAction::GetMetadata,
    );

    let (event_ctx, (warehouse, ns_hierarchy, info)) = event_ctx.emit_authz(
        super::load_and_authorize_generic_table_operation::<C, A>(
            authorizer,
            &request_metadata,
            warehouse_id,
            namespace.clone(),
            &table_name,
            CatalogGenericTableAction::GetMetadata,
            state.v1_state.catalog.clone(),
        )
        .await,
    )?;

    let _event_ctx = event_ctx.resolve(ResolvedNamespace {
        warehouse: warehouse.clone(),
        namespace: ns_hierarchy.namespace.clone(),
    });

    // ------------------- Check ReadData + WriteData for storage permissions -------------------
    let [can_read, can_write] = authorizer
        .are_allowed_generic_table_actions_arr(
            &request_metadata,
            None,
            &warehouse,
            &ns_hierarchy,
            &info,
            &[
                CatalogGenericTableAction::ReadData,
                CatalogGenericTableAction::WriteData,
            ],
        )
        .await
        .map_err(AuthorizationFailureSource::into_error_model)?
        .into_inner();

    let storage_permissions = if can_write {
        Some(StoragePermissions::ReadWriteDelete)
    } else if can_read {
        Some(StoragePermissions::Read)
    } else {
        None
    };

    let (config, storage_credentials) = if let Some(storage_permissions) = storage_permissions {
        let storage_secret =
            maybe_get_secret(warehouse.storage_secret_id, &state.v1_state.secrets).await?;
        let storage_secret_ref = storage_secret.as_deref();

        let table_config = warehouse
            .storage_profile
            .generate_table_config(
                data_access,
                storage_secret_ref,
                &info.location,
                storage_permissions,
                &request_metadata,
                &info,
            )
            .await?;

        let base_location = info.location.to_string();
        let creds = (!table_config.creds.inner().is_empty()).then(|| {
            vec![StorageCredential {
                prefix: base_location,
                config: table_config.creds.clone().into(),
            }]
        });

        (Some(table_config.config.into()), creds)
    } else {
        (None, None)
    };

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
        config,
        storage_credentials,
    })
}
