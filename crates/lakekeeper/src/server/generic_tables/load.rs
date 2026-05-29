use std::sync::Arc;

use iceberg::TableIdent;
use iceberg_ext::catalog::rest::StorageCredential;

use crate::{
    api::{
        ApiContext,
        data::v1::generic_tables::{
            GenericTableData, GenericTableParameters, LoadGenericTableResponse,
        },
        iceberg::v1::DataAccessMode,
    },
    request_metadata::RequestMetadata,
    server::{maybe_get_secret, require_warehouse_id},
    service::{
        CatalogGenericTableOps, CatalogStore, Result, SecretStore, State, TabularListFlags,
        Transaction,
        authz::{Authorizer, CatalogGenericTableAction},
        events::{APIEventContext, context::ResolvedGenericTable},
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

    let table_ident = TableIdent::new(namespace.clone(), table_name.clone());

    let event_ctx = APIEventContext::for_generic_table(
        Arc::new(request_metadata.clone()),
        state.v1_state.events.clone(),
        warehouse_id,
        table_ident.clone(),
        CatalogGenericTableAction::GetMetadata,
    );

    // load() doesn't yet expose `referenced_by`; reuse the chain-aware helper with None
    // so the topology matches credentials and the three actions batch into one round-trip.
    let (event_ctx, (warehouse, _ns, gt_tabular, storage_permissions)) = event_ctx.emit_authz(
        super::credentials::authorize_load_generic_table::<C, A>(
            &request_metadata,
            table_ident.clone(),
            warehouse_id,
            TabularListFlags::active(),
            state.v1_state.authz.clone(),
            state.v1_state.catalog.clone(),
            None,
        )
        .await,
    )?;

    // Load by the authz-resolved id, not by (namespace_id, name). Closes the
    // TOCTOU window where a concurrent rename + create-with-same-name between
    // authz and load would substitute a different row than the one the
    // caller's grant applied to.
    let mut t = C::Transaction::begin_read(state.v1_state.catalog.clone()).await?;
    let info =
        C::load_generic_table_by_id(warehouse_id, gt_tabular.tabular_id, t.transaction()).await?;
    t.commit().await?;

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
        config,
        storage_credentials,
    };

    let event_ctx = event_ctx.resolve(ResolvedGenericTable {
        warehouse,
        generic_table: info,
        storage_permissions,
    });
    event_ctx.emit_generic_table_loaded_async(data_access);

    Ok(response)
}
