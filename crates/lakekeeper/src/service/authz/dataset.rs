use std::{collections::HashMap, sync::Arc};

use iceberg_ext::catalog::rest::ErrorModel;

use crate::{
    WarehouseId,
    api::RequestMetadata,
    service::{
        AuthZDatasetInfo, CachePolicy, CatalogBackendError, CatalogNamespaceOps, CatalogStore,
        CatalogTabularOps, CatalogWarehouseOps, DatasetId, DatasetIdentOrId, DatasetTabularInfo,
        GetTabularInfoError, InternalParseLocationError, InvalidNamespaceIdentifier,
        NamespaceHierarchy, NamespaceId, NamespaceWithParent, ResolvedWarehouse,
        SerializationError, TabularId, TabularListFlags, TabularNotFound,
        UnexpectedTabularInResponse, ViewOrTableInfo,
        authz::{
            ActionOnDataset, AuthZError, AuthorizationBackendUnavailable,
            AuthorizationCountMismatch, AuthorizationDecision, Authorizer, AuthzBadRequest,
            AuthzNamespaceOps, AuthzWarehouseOps, BackendUnavailableOrCountMismatch,
            CannotInspectPermissions, CatalogAction, CatalogDatasetAction, IsAllowedActionError,
            MustUse, UserOrRole,
        },
        events::{
            AuthorizationFailureReason, AuthorizationFailureSource, context::UserProvidedDataset,
            delegate_authorization_failure_source,
        },
    },
};

pub trait DatasetAction
where
    Self: CatalogAction + Clone + PartialEq + Eq + From<CatalogDatasetAction>,
{
    /// Whether this action reads or writes the files of a dataset (as opposed to
    /// its metadata or catalog operations). Used to exclude data-plane actions
    /// from the instance-admin bypass.
    fn is_data_plane(&self) -> bool;
}

impl DatasetAction for CatalogDatasetAction {
    fn is_data_plane(&self) -> bool {
        // A commit only writes catalog rows -- the bytes are written directly to
        // storage by the client, under separately authorized credentials -- but it
        // is what makes files visible to readers, so it belongs to the data plane
        // alongside reads.
        matches!(self, Self::ReadData | Self::Commit)
    }
}

// ------------------ Cannot See Error ------------------
#[derive(Debug, PartialEq, Eq)]
pub struct AuthZCannotSeeDataset {
    warehouse_id: WarehouseId,
    dataset: DatasetIdentOrId,
    internal_resource_not_found: bool,
}
impl AuthZCannotSeeDataset {
    #[must_use]
    pub fn new(
        warehouse_id: WarehouseId,
        dataset: impl Into<DatasetIdentOrId>,
        resource_not_found: bool,
    ) -> Self {
        Self {
            warehouse_id,
            dataset: dataset.into(),
            internal_resource_not_found: resource_not_found,
        }
    }

    #[must_use]
    pub fn new_not_found(warehouse_id: WarehouseId, dataset: impl Into<DatasetIdentOrId>) -> Self {
        Self::new(warehouse_id, dataset, true)
    }

    #[must_use]
    pub fn new_forbidden(warehouse_id: WarehouseId, dataset: impl Into<DatasetIdentOrId>) -> Self {
        Self::new(warehouse_id, dataset, false)
    }
}
impl AuthorizationFailureSource for AuthZCannotSeeDataset {
    fn into_error_model(self) -> ErrorModel {
        let AuthZCannotSeeDataset {
            warehouse_id,
            dataset,
            internal_resource_not_found: _,
        } = self;
        TabularNotFound::new(warehouse_id, dataset).into()
    }

    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        if self.internal_resource_not_found {
            AuthorizationFailureReason::ResourceNotFound
        } else {
            AuthorizationFailureReason::CannotSeeResource
        }
    }
}

// ------------------ Action Forbidden Error ------------------
#[derive(Debug, PartialEq, Eq)]
pub struct AuthZDatasetActionForbidden {
    warehouse_id: WarehouseId,
    dataset: DatasetIdentOrId,
    action: String,
}
impl AuthZDatasetActionForbidden {
    #[must_use]
    pub fn new(
        warehouse_id: WarehouseId,
        dataset: impl Into<DatasetIdentOrId>,
        action: &impl DatasetAction,
    ) -> Self {
        Self {
            warehouse_id,
            dataset: dataset.into(),
            action: action.as_log_str(),
        }
    }
}
impl AuthorizationFailureSource for AuthZDatasetActionForbidden {
    fn into_error_model(self) -> ErrorModel {
        let AuthZDatasetActionForbidden {
            warehouse_id,
            dataset,
            action,
        } = self;
        ErrorModel::forbidden(
            format!(
                "Dataset action `{action}` forbidden on dataset {dataset} in warehouse `{warehouse_id}`"
            ),
            "DatasetActionForbidden",
            None,
        )
    }

    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        AuthorizationFailureReason::ActionForbidden
    }
}

// ------------------ Require Action Error ------------------
#[derive(Debug, derive_more::From)]
pub enum RequireDatasetActionError {
    AuthZDatasetActionForbidden(AuthZDatasetActionForbidden),
    AuthorizationBackendUnavailable(AuthorizationBackendUnavailable),
    AuthorizationCountMismatch(AuthorizationCountMismatch),
    CannotInspectPermissions(CannotInspectPermissions),
    AuthorizerValidationFailed(AuthzBadRequest),
    AuthZCannotSeeDataset(AuthZCannotSeeDataset),
    CatalogBackendError(CatalogBackendError),
    InvalidNamespaceIdentifier(InvalidNamespaceIdentifier),
    SerializationError(SerializationError),
    UnexpectedTabularInResponse(UnexpectedTabularInResponse),
    InternalParseLocationError(InternalParseLocationError),
}

impl From<BackendUnavailableOrCountMismatch> for RequireDatasetActionError {
    fn from(err: BackendUnavailableOrCountMismatch) -> Self {
        match err {
            BackendUnavailableOrCountMismatch::AuthorizationBackendUnavailable(e) => e.into(),
            BackendUnavailableOrCountMismatch::AuthorizationCountMismatch(e) => e.into(),
        }
    }
}

impl From<IsAllowedActionError> for RequireDatasetActionError {
    fn from(err: IsAllowedActionError) -> Self {
        match err {
            IsAllowedActionError::AuthorizationBackendUnavailable(e) => e.into(),
            IsAllowedActionError::CannotInspectPermissions(e) => e.into(),
            IsAllowedActionError::BadRequest(e) => e.into(),
            IsAllowedActionError::CountMismatch(e) => e.into(),
        }
    }
}

impl From<GetTabularInfoError> for RequireDatasetActionError {
    fn from(err: GetTabularInfoError) -> Self {
        match err {
            GetTabularInfoError::CatalogBackendError(e) => e.into(),
            GetTabularInfoError::SerializationError(e) => e.into(),
            GetTabularInfoError::InvalidNamespaceIdentifier(e) => e.into(),
            GetTabularInfoError::UnexpectedTabularInResponse(e) => e.into(),
            GetTabularInfoError::InternalParseLocationError(e) => e.into(),
        }
    }
}

delegate_authorization_failure_source!(RequireDatasetActionError => {
    AuthZDatasetActionForbidden,
    AuthorizationBackendUnavailable,
    AuthorizationCountMismatch,
    CannotInspectPermissions,
    AuthZCannotSeeDataset,
    CatalogBackendError,
    InvalidNamespaceIdentifier,
    SerializationError,
    UnexpectedTabularInResponse,
    InternalParseLocationError,
    AuthorizerValidationFailed
});

/// The permission that decides whether a dataset is visible at all. A caller
/// lacking it gets "not found" rather than "forbidden", so absence of access
/// cannot be used to probe for existence.
const CAN_SEE_PERMISSION: CatalogDatasetAction = CatalogDatasetAction::GetMetadata;

#[async_trait::async_trait]
pub trait AuthZDatasetOps: Authorizer {
    fn require_dataset_presence<T: AuthZDatasetInfo>(
        &self,
        warehouse_id: WarehouseId,
        user_provided: impl Into<DatasetIdentOrId> + Send,
        result: Result<Option<T>, impl Into<RequireDatasetActionError> + Send>,
    ) -> Result<T, RequireDatasetActionError> {
        let info = result.map_err(Into::into)?;
        let Some(info) = info else {
            return Err(AuthZCannotSeeDataset::new_not_found(warehouse_id, user_provided).into());
        };
        Ok(info)
    }

    fn require_dataset_action<T: AuthZDatasetInfo>(
        &self,
        metadata: &RequestMetadata,
        warehouse: &ResolvedWarehouse,
        namespace: &NamespaceHierarchy,
        user_provided: impl Into<DatasetIdentOrId> + Send,
        result: Result<Option<T>, impl Into<RequireDatasetActionError> + Send>,
        action: impl Into<Self::DatasetAction> + Send,
    ) -> impl std::future::Future<Output = Result<T, RequireDatasetActionError>> + Send {
        async move {
            let warehouse_id = warehouse.warehouse_id;
            let user_provided = user_provided.into();
            let info =
                self.require_dataset_presence(warehouse_id, user_provided.clone(), result)?;
            let ident = info.dataset_ident().clone();

            let cant_see_err =
                AuthZCannotSeeDataset::new_forbidden(warehouse_id, user_provided).into();
            let action = action.into();

            if action == CAN_SEE_PERMISSION.into() {
                let [is_allowed] = self
                    .are_allowed_dataset_actions_arr(
                        metadata,
                        None,
                        warehouse,
                        namespace,
                        &info,
                        &[action],
                    )
                    .await?
                    .into_inner();
                return is_allowed.then_some(info).ok_or(cant_see_err);
            }

            let [can_see, is_allowed] = self
                .are_allowed_dataset_actions_arr(
                    metadata,
                    None,
                    warehouse,
                    namespace,
                    &info,
                    &[CAN_SEE_PERMISSION.into(), action.clone()],
                )
                .await?
                .into_inner();
            if can_see {
                is_allowed.then_some(info).ok_or_else(|| {
                    AuthZDatasetActionForbidden::new(warehouse_id, ident.clone(), &action).into()
                })
            } else {
                Err(cant_see_err)
            }
        }
    }

    fn are_allowed_dataset_actions_arr<
        const N: usize,
        A: DatasetAction + Into<Self::DatasetAction> + Send + Clone + Sync,
    >(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        warehouse: &ResolvedWarehouse,
        namespace_hierarchy: &NamespaceHierarchy,
        info: &impl AuthZDatasetInfo,
        actions: &[A; N],
    ) -> impl std::future::Future<Output = Result<MustUse<[bool; N]>, IsAllowedActionError>> + Send
    {
        async move {
            let wrapped = actions
                .iter()
                .map(|a| {
                    (
                        &namespace_hierarchy.namespace,
                        ActionOnDataset {
                            info,
                            action: a.clone(),
                            user: for_user,
                        },
                    )
                })
                .collect::<Vec<_>>();
            let result = self
                .are_allowed_dataset_actions_vec(
                    metadata,
                    warehouse,
                    &namespace_hierarchy
                        .parents
                        .iter()
                        .map(|ns| (ns.namespace_id(), ns.clone()))
                        .collect(),
                    &wrapped,
                )
                .await?
                .into_allowed();
            let n_returned = result.len();
            let arr: [bool; N] = result
                .try_into()
                .map_err(|_| AuthorizationCountMismatch::new(N, n_returned, "dataset"))?;
            Ok(MustUse::from(arr))
        }
    }

    /// Batch-authorize dataset actions, applying the same normalization the other
    /// tabular subtypes use: a warehouse mismatch denies outright, acting-as-self
    /// is normalized to `None`, and the control-plane bypass is applied for
    /// non-data-plane actions only.
    fn are_allowed_dataset_actions_vec<
        A: DatasetAction + Into<Self::DatasetAction> + Send + Clone + Sync,
    >(
        &self,
        metadata: &RequestMetadata,
        warehouse: &ResolvedWarehouse,
        parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
        actions: &[(
            &NamespaceWithParent,
            ActionOnDataset<'_, '_, impl AuthZDatasetInfo, A>,
        )],
    ) -> impl std::future::Future<
        Output = Result<MustUse<Vec<AuthorizationDecision>>, IsAllowedActionError>,
    > + Send {
        async move {
            #[cfg(debug_assertions)]
            {
                let namespaces: Vec<&NamespaceWithParent> =
                    actions.iter().map(|(ns, _)| *ns).collect();
                super::table::validate_namespace_hierarchy(&namespaces, parent_namespaces);
            }

            let internal = metadata.is_lakekeeper_internal();

            let mut auto_approved: Vec<Option<bool>> = Vec::with_capacity(actions.len());
            let mut actions_to_check = Vec::new();

            for (ns, action) in actions {
                let same_warehouse = action.info.warehouse_id() == warehouse.warehouse_id;
                if !same_warehouse {
                    tracing::warn!(
                        "Dataset warehouse_id `{}` does not match provided warehouse_id `{}`. Denying access.",
                        action.info.warehouse_id(),
                        warehouse.warehouse_id
                    );
                    auto_approved.push(Some(false));
                    continue;
                }

                // Normalize user: if it's the actor itself, treat as None (acting as self).
                let normalized_user = if metadata.actor().to_user_or_role().as_ref() == action.user
                {
                    None
                } else {
                    action.user
                };

                // `LakekeeperInternal` bypasses all actions including data-plane.
                // Instance admins bypass only non-data-plane actions -- notably not
                // `Commit`, which makes files visible to every reader of a ref.
                let bypass = metadata.bypasses_control_plane_authz(normalized_user)
                    && (internal || !action.action.is_data_plane());
                if bypass {
                    auto_approved.push(Some(true));
                } else {
                    auto_approved.push(None);
                    let mut normalized_action = action.clone();
                    normalized_action.user = normalized_user;
                    actions_to_check.push((*ns, normalized_action));
                }
            }

            if actions_to_check.is_empty() {
                Ok(auto_approved
                    .into_iter()
                    .map(|v| AuthorizationDecision::from(v.unwrap()))
                    .collect())
            } else {
                let decisions = self
                    .are_allowed_dataset_actions_impl(
                        metadata,
                        warehouse,
                        parent_namespaces,
                        &actions_to_check,
                    )
                    .await?;

                if decisions.len() != actions_to_check.len() {
                    return Err(AuthorizationCountMismatch::new(
                        actions_to_check.len(),
                        decisions.len(),
                        "dataset",
                    )
                    .into());
                }

                // Merge auto-approved decisions (warehouse-mismatch / bypass) with the
                // authorizer's checked decisions, preserving each one's `determined_by`.
                let mut decision_iter = decisions.into_iter();
                let final_decisions: Vec<AuthorizationDecision> = auto_approved
                    .into_iter()
                    .map(|auto| {
                        auto.map_or_else(
                            || decision_iter.next().unwrap(),
                            AuthorizationDecision::from,
                        )
                    })
                    .collect();

                Ok(final_decisions)
            }
            .map(MustUse::from)
        }
    }

    /// Resolve a dataset by ident or id, check consistency, then authorize.
    ///
    /// The single entry point for every dataset route. The catalog API arrives
    /// with a namespace and name, the management API with an id; both are
    /// `DatasetIdentOrId`, so neither caller reimplements resolution and neither
    /// can accidentally skip the TOCTOU refresh below.
    ///
    /// # Errors
    /// Returns `AuthZError` if the warehouse, namespace, or dataset is not found,
    /// identifiers are inconsistent, the user is not authorized, or a
    /// catalog/authorization backend error occurs.
    async fn load_and_authorize_dataset_operation<C: CatalogStore>(
        &self,
        request_metadata: &RequestMetadata,
        user_provided: &UserProvidedDataset,
        table_flags: TabularListFlags,
        action: impl Into<Self::DatasetAction> + Send,
        catalog_state: C::State,
    ) -> Result<
        (
            Arc<ResolvedWarehouse>,
            NamespaceHierarchy,
            DatasetTabularInfo,
        ),
        AuthZError,
    > {
        let warehouse_id = user_provided.warehouse_id;
        let action = action.into();

        let (warehouse, namespace, info) = match &user_provided.dataset {
            DatasetIdentOrId::Id(dataset_id) => {
                fetch_warehouse_namespace_dataset_by_id::<C, _>(
                    self,
                    warehouse_id,
                    *dataset_id,
                    table_flags,
                    catalog_state.clone(),
                )
                .await?
            }
            DatasetIdentOrId::Ident(ident) => {
                // Warehouse, namespace and dataset are independent lookups, so
                // they go out together rather than in series.
                let (warehouse_result, namespace_result, info_result) = tokio::join!(
                    C::get_active_warehouse_by_id(warehouse_id, catalog_state.clone()),
                    C::get_namespace(warehouse_id, ident.namespace.clone(), catalog_state.clone()),
                    C::get_dataset_info(
                        warehouse_id,
                        ident.clone(),
                        table_flags,
                        catalog_state.clone()
                    )
                );

                let warehouse = self.require_warehouse_presence(warehouse_id, warehouse_result)?;
                let namespace = self.require_namespace_presence(
                    warehouse_id,
                    ident.namespace.clone(),
                    namespace_result,
                )?;
                let info =
                    self.require_dataset_presence(warehouse_id, ident.clone(), info_result)?;

                (warehouse, namespace, info)
            }
        };

        // Guards against the dataset having been moved between the lookups above.
        let (warehouse, namespace) =
            super::table::refresh_warehouse_and_namespace_if_needed::<C, _, _>(
                &warehouse,
                namespace,
                &info,
                AuthZCannotSeeDataset::new_not_found(warehouse_id, user_provided.dataset.clone()),
                self,
                catalog_state,
            )
            .await?;

        let info = self
            .require_dataset_action(
                request_metadata,
                &warehouse,
                &namespace,
                user_provided.dataset.clone(),
                Ok::<_, RequireDatasetActionError>(Some(info)),
                action,
            )
            .await?;

        Ok((warehouse, namespace, info))
    }
}

impl<T> AuthZDatasetOps for T where T: Authorizer {}

/// Id-addressed resolution: the tabular row carries the namespace, so the
/// namespace is fetched from it rather than supplied by the caller.
pub(crate) async fn fetch_warehouse_namespace_dataset_by_id<C, A>(
    authorizer: &A,
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    table_flags: TabularListFlags,
    catalog_state: C::State,
) -> Result<
    (
        Arc<ResolvedWarehouse>,
        NamespaceHierarchy,
        DatasetTabularInfo,
    ),
    AuthZError,
>
where
    C: CatalogStore,
    A: AuthzWarehouseOps + AuthzNamespaceOps,
{
    let lookup_ids = [TabularId::Dataset(dataset_id)];
    let (warehouse_result, info_result) = tokio::join!(
        C::get_active_warehouse_by_id(warehouse_id, catalog_state.clone()),
        C::get_tabular_infos_by_id(
            warehouse_id,
            &lookup_ids,
            table_flags,
            catalog_state.clone(),
        ),
    );

    let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse_result)?;

    let infos = info_result.map_err(|e| {
        AuthZError::RequireDatasetActionError(RequireDatasetActionError::CatalogBackendError(
            CatalogBackendError::new_unexpected(ErrorModel::from(
                iceberg_ext::catalog::rest::IcebergErrorResponse::from(e),
            )),
        ))
    })?;
    let info = infos
        .into_iter()
        .find_map(|i| match i {
            ViewOrTableInfo::Dataset(d) => Some(d),
            _ => None,
        })
        .ok_or_else(|| AuthZCannotSeeDataset::new_not_found(warehouse_id, dataset_id))?;

    let namespace_id = info.namespace_id;
    let namespace_result = C::get_namespace_cache_aware(
        warehouse_id,
        namespace_id,
        CachePolicy::RequireMinimumVersion(*info.namespace_version),
        catalog_state,
    )
    .await;
    let namespace =
        authorizer.require_namespace_presence(warehouse_id, namespace_id, namespace_result)?;

    Ok((warehouse, namespace, info))
}
