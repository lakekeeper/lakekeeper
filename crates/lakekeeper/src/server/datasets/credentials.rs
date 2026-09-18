use std::sync::Arc;

use crate::{
    api::{
        ApiContext,
        data::v1::datasets::{
            DatasetParameters, LoadDatasetCredentialsRequest, LoadDatasetCredentialsResponse,
        },
        iceberg::v1::{DataAccess, Result},
    },
    request_metadata::RequestMetadata,
    server::{maybe_get_secret, require_warehouse_id},
    service::{
        CatalogStore, SecretStore, State, TabularListFlags,
        authz::{
            ActionOnDataset, AuthZCannotSeeDataset, AuthZDatasetOps, Authorizer,
            CatalogDatasetAction,
        },
        events::{APIEventContext, context::UserProvidedDataset},
        storage::StoragePermissions,
    },
};

/// Vend storage credentials scoped to the dataset's prefix.
///
/// Read and write are separate permissions, and the weaker one is vended when only
/// it is held: a caller who may read but not commit still gets credentials, just
/// read-only ones. A caller with neither is refused rather than handed a token that
/// would fail at the object store.
pub(super) async fn load_dataset_credentials<
    C: CatalogStore,
    A: Authorizer + Clone,
    S: SecretStore,
>(
    parameters: DatasetParameters,
    _request: LoadDatasetCredentialsRequest,
    data_access: DataAccess,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<LoadDatasetCredentialsResponse> {
    let DatasetParameters {
        prefix,
        namespace,
        dataset_name,
    } = parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    let authorizer = state.v1_state.authz.clone();

    let dataset_ident = iceberg::TableIdent::new(namespace, dataset_name);
    let event_ctx = APIEventContext::for_dataset(
        Arc::new(request_metadata.clone()),
        state.v1_state.events.clone(),
        warehouse_id,
        dataset_ident.clone(),
        // The gate below enforces GetMetadata; a denial is audited as that.
        CatalogDatasetAction::GetMetadata,
    );

    let authz_result = async {
        let (warehouse, namespace, info) = authorizer
            .load_and_authorize_dataset_operation::<C>(
                event_ctx.request_metadata(),
                &UserProvidedDataset::new(warehouse_id, dataset_ident.clone()),
                TabularListFlags::active_and_staged(),
                CatalogDatasetAction::GetMetadata,
                state.v1_state.catalog.clone(),
            )
            .await?;

        let parents_map = namespace
            .parents
            .iter()
            .map(|ns| (ns.namespace_id(), ns.clone()))
            .collect();
        let wanted = [CatalogDatasetAction::ReadData, CatalogDatasetAction::Commit];
        let allowed = authorizer
            .are_allowed_dataset_actions_vec(
                event_ctx.request_metadata(),
                &warehouse,
                &parents_map,
                &wanted
                    .iter()
                    .map(|action| {
                        (
                            &namespace.namespace,
                            ActionOnDataset {
                                info: &info,
                                action: action.clone(),
                                user: None,
                            },
                        )
                    })
                    .collect::<Vec<_>>(),
            )
            .await?
            .into_allowed();

        let storage_permissions = match (allowed.first(), allowed.get(1)) {
            // Commit implies the writer places objects under the prefix; delete is
            // included so a failed write can clean up after itself.
            (_, Some(true)) => StoragePermissions::ReadWriteDelete,
            (Some(true), _) => StoragePermissions::Read,
            _ => {
                return Err(crate::service::authz::AuthZError::from(
                    AuthZCannotSeeDataset::new_forbidden(warehouse_id, dataset_ident.clone()),
                ));
            }
        };

        Ok((warehouse, info, storage_permissions))
    }
    .await;

    let (_event_ctx, (warehouse, info, storage_permissions)) =
        event_ctx.emit_authz(authz_result)?;

    let storage_secret =
        maybe_get_secret(warehouse.storage_secret_id, &state.v1_state.secrets).await?;

    let storage_config = warehouse
        .storage_profile
        .generate_table_config(
            data_access.into(),
            storage_secret.as_deref(),
            &info.location,
            storage_permissions,
            &request_metadata,
            &info,
        )
        .await?;

    let storage_credentials = storage_config
        .storage_credentials(&info.location)
        .unwrap_or_default();

    Ok(LoadDatasetCredentialsResponse {
        storage_credentials,
    })
}
