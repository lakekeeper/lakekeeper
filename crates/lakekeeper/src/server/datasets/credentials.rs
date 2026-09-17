use std::sync::Arc;

use iceberg::TableIdent;
use uuid::Uuid;

use super::load_ownership;
use crate::{
    api::{
        ApiContext, ErrorModel,
        data::v1::datasets::{
            DatasetParameters, LoadDatasetCredentialsRequest, LoadDatasetCredentialsResponse,
        },
        iceberg::v1::{DataAccess, Result},
    },
    request_metadata::RequestMetadata,
    server::{maybe_get_secret, require_warehouse_id},
    service::{
        CatalogStore, DatasetOwnership, DatasetTabularInfo, ResolvedWarehouse, SecretStore, State,
        TabularListFlags,
        authz::{
            ActionOnDataset, AuthZCannotSeeDataset, AuthZDatasetOps, AuthZError, Authorizer,
            CatalogDatasetAction,
        },
        events::{APIEventContext, context::UserProvidedDataset},
        storage::StoragePermissions,
    },
};

/// Below a dataset's location, the folder each writer gets a fresh child of.
pub(crate) const WRITE_DIR: &str = "data";

/// Vend storage credentials for a dataset. A reader gets read-only credentials for
/// the dataset's prefix; a writer gets credentials for a fresh, empty folder below
/// it, named in `write-prefix`, and reaches nothing outside that folder.
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

    let dataset_ident = TableIdent::new(namespace, dataset_name);
    let event_ctx = APIEventContext::for_dataset(
        Arc::new(request_metadata.clone()),
        state.v1_state.events.clone(),
        warehouse_id,
        dataset_ident.clone(),
        CatalogDatasetAction::ReadData {
            target_refs: Arc::default(),
        },
    );

    let authz_result = async {
        let (warehouse, namespace, info) = authorizer
            .load_and_authorize_dataset_operation::<C>(
                event_ctx.request_metadata(),
                &UserProvidedDataset::new(warehouse_id, dataset_ident.clone()),
                TabularListFlags::active(),
                CatalogDatasetAction::GetMetadata,
                state.v1_state.catalog.clone(),
            )
            .await?;

        let parents_map = namespace
            .parents
            .iter()
            .map(|ns| (ns.namespace_id(), ns.clone()))
            .collect();
        // Credentials cover every branch, so the commit check names none.
        let wanted = [
            CatalogDatasetAction::ReadData {
                target_refs: Arc::default(),
            },
            CatalogDatasetAction::Commit {
                target_refs: Arc::default(),
            },
        ];
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

        let may_read = allowed.first().copied().unwrap_or(false);
        let may_commit = allowed.get(1).copied().unwrap_or(false);
        if !may_read && !may_commit {
            return Err(AuthZError::from(AuthZCannotSeeDataset::new_forbidden(
                warehouse_id,
                dataset_ident.clone(),
            )));
        }

        Ok((warehouse, info, may_read, may_commit))
    }
    .await;

    let (_event_ctx, (warehouse, info, may_read, may_commit)) =
        event_ctx.emit_authz(authz_result)?;

    let catalog = state.v1_state.catalog.clone();
    // An imported dataset borrows its prefix, which may hold objects that are not the
    // dataset's: a credential for the prefix would reach them too.
    if load_ownership::<C>(warehouse_id, info.tabular_id, catalog).await?
        == DatasetOwnership::Imported
    {
        return Err(ErrorModel::conflict(
            "An imported dataset's prefix is borrowed, so no storage credentials are vended \
             for it. Read its files through an access grant.",
            "CannotVendCredentialsForImportedDataset",
            None,
        )
        .into());
    }

    vend(
        &warehouse,
        &info,
        may_read,
        may_commit,
        data_access,
        &state.v1_state.secrets,
        &request_metadata,
    )
    .await
}

/// Read credentials for the dataset's prefix when `may_read`, and a fresh write folder
/// with credentials for it alone when `may_commit`.
async fn vend<S: SecretStore>(
    warehouse: &ResolvedWarehouse,
    info: &DatasetTabularInfo,
    may_read: bool,
    may_commit: bool,
    data_access: DataAccess,
    secrets: &S,
    request_metadata: &RequestMetadata,
) -> Result<LoadDatasetCredentialsResponse> {
    let storage_secret = maybe_get_secret(warehouse.storage_secret_id, secrets).await?;

    let mut storage_credentials = Vec::new();
    if may_read {
        let config = warehouse
            .storage_profile
            .generate_table_config(
                data_access.into(),
                storage_secret.as_deref(),
                &info.location,
                StoragePermissions::Read,
                request_metadata,
                info,
            )
            .await?;
        storage_credentials.extend(
            config
                .storage_credentials(&info.location)
                .unwrap_or_default(),
        );
    }

    // A folder no snapshot can record yet. Delete is included so a writer can remove
    // its own uploads when its commit fails.
    let write_prefix = if may_commit {
        let id = Uuid::now_v7().to_string();
        let mut folder = info.location.clone();
        folder
            .without_trailing_slash()
            .extend([WRITE_DIR, id.as_str()]);
        let config = warehouse
            .storage_profile
            .generate_table_config(
                data_access.into(),
                storage_secret.as_deref(),
                &folder,
                StoragePermissions::ReadWriteDelete,
                request_metadata,
                info,
            )
            .await?;
        storage_credentials.extend(config.storage_credentials(&folder).unwrap_or_default());
        Some(folder.to_string())
    } else {
        None
    };

    Ok(LoadDatasetCredentialsResponse {
        storage_credentials,
        write_prefix,
    })
}
