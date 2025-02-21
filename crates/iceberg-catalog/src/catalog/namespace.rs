use std::{collections::HashMap, ops::Deref};

use futures::FutureExt;
use http::StatusCode;
use iceberg::NamespaceIdent;
use iceberg_ext::configs::{namespace::NamespaceProperties, ConfigProperty as _, Location};
use itertools::Itertools;

use super::{require_warehouse_id, CatalogServer, UnfilteredPage};
use crate::{
    api::{
        iceberg::v1::{
            namespace::GetNamespacePropertiesQuery, ApiContext, CreateNamespaceRequest,
            CreateNamespaceResponse, ErrorModel, GetNamespaceResponse, ListNamespacesQuery,
            ListNamespacesResponse, NamespaceParameters, Prefix, Result,
            UpdateNamespacePropertiesRequest, UpdateNamespacePropertiesResponse,
        },
        set_not_found_status_code,
    },
    catalog,
    request_metadata::RequestMetadata,
    service::{
        authz::{Authorizer, CatalogNamespaceAction, CatalogWarehouseAction, NamespaceParent},
        secrets::SecretStore,
        Catalog, GetWarehouseResponse, NamespaceIdentUuid, State, Transaction,
    },
    WarehouseIdent, CONFIG,
};

pub const UNSUPPORTED_NAMESPACE_PROPERTIES: &[&str] = &[];
// If this is increased, we need to modify namespace creation and deletion
// to take care of the hierarchical structure.
pub const MAX_NAMESPACE_DEPTH: i32 = 5;
pub const NAMESPACE_ID_PROPERTY: &str = "namespace_id";
pub(crate) const MANAGED_ACCESS_PROPERTY: &str = "managed_access";

#[async_trait::async_trait]
impl<C: Catalog, A: Authorizer + Clone, S: SecretStore>
    crate::api::iceberg::v1::namespace::Service<State<A, C, S>> for CatalogServer<C, A, S>
{
    async fn list_namespaces(
        prefix: Option<Prefix>,
        query: ListNamespacesQuery,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ListNamespacesResponse> {
        // ------------------- VALIDATIONS -------------------
        let warehouse_id = require_warehouse_id(prefix)?;
        let ListNamespacesQuery {
            page_token: _,
            page_size: _,
            parent,
            return_uuids,
        } = &query;
        parent.as_ref().map(validate_namespace_ident).transpose()?;
        let return_uuids = *return_uuids;

        // ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz;
        authorizer
            .require_warehouse_action(
                &request_metadata,
                warehouse_id,
                &CatalogWarehouseAction::CanListNamespaces,
            )
            .await?;
        let mut t = C::Transaction::begin_read(state.v1_state.catalog.clone()).await?;

        if let Some(parent) = parent {
            let namespace_id = C::namespace_to_id(warehouse_id, parent, t.transaction()).await; // Cannot fail before authz
            authorizer
                .require_namespace_action(
                    &request_metadata,
                    namespace_id,
                    &CatalogNamespaceAction::CanListNamespaces,
                )
                .await?;
        };

        // ------------------- BUSINESS LOGIC -------------------
        let (idents, ids, next_page_token) = catalog::fetch_until_full_page::<_, _, _, C>(
            query.page_size,
            query.page_token.clone(),
            |ps, page_token, trx| {
                let parent = parent.clone();
                let authorizer = authorizer.clone();
                let request_metadata = request_metadata.clone();
                async move {
                    let query = ListNamespacesQuery {
                        page_size: Some(ps),
                        page_token: page_token.into(),
                        parent,
                        return_uuids: true,
                    };

                    // list_namespaces gives us a HashMap<Id, Ident> and a Vec<(Id, Token)>, in order
                    // to do sane pagination, we need to rely on the order of the Vec<(Id, Token)> to
                    // return the correct next page token which is why we do these unholy things here.
                    let list_namespaces =
                        C::list_namespaces(warehouse_id, &query, trx.transaction()).await?;
                    let (ids, idents, tokens): (Vec<_>, Vec<_>, Vec<_>) =
                        list_namespaces.into_iter_with_page_tokens().multiunzip();

                    let (next_namespaces, next_uuids, next_page_tokens, mask): (
                        Vec<_>,
                        Vec<_>,
                        Vec<_>,
                        Vec<bool>,
                    ) = futures::future::try_join_all(ids.iter().map(|n| {
                        authorizer.is_allowed_namespace_action(
                            &request_metadata,
                            *n,
                            &CatalogNamespaceAction::CanGetMetadata,
                        )
                    }))
                    .await?
                    .into_iter()
                    .zip(idents.into_iter().zip(ids.into_iter()))
                    .zip(tokens.into_iter())
                    .map(|((allowed, namespace), token)| (namespace.0, namespace.1, token, allowed))
                    .multiunzip();

                    Ok(UnfilteredPage::new(
                        next_namespaces,
                        next_uuids,
                        next_page_tokens,
                        mask,
                        ps.clamp(0, i64::MAX).try_into().expect("We clamped it"),
                    ))
                }
                .boxed()
            },
            &mut t,
        )
        .await?;
        t.commit().await?;

        Ok(ListNamespacesResponse {
            next_page_token,
            namespaces: idents,
            namespace_uuids: return_uuids.then_some(ids.into_iter().map(|s| *s).collect()),
        })
    }

    async fn create_namespace(
        prefix: Option<Prefix>,
        request: CreateNamespaceRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<CreateNamespaceResponse> {
        // ------------------- VALIDATIONS -------------------
        let warehouse_id = require_warehouse_id(prefix)?;
        let CreateNamespaceRequest {
            namespace,
            properties,
        } = &request;

        tracing::debug!("Creating namespace: {:?}", namespace);

        validate_namespace_ident(namespace)?;

        properties
            .as_ref()
            .map(|p| validate_namespace_properties_keys(p.keys()))
            .transpose()?;

        if CONFIG
            .reserved_namespaces
            .contains(&namespace.as_ref()[0].to_lowercase())
        {
            tracing::debug!("Denying reserved namespace: '{}'", &namespace.as_ref()[0]);
            return Err(ErrorModel::bad_request(
                "Namespace is reserved for internal use.",
                "ReservedNamespace",
                None,
            )
            .into());
        }

        // ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz;
        let namespace_parent = namespace.parent();

        if namespace_parent.is_some() {
            // Pre-check before fetching the parent namespace id
            authorizer
                .require_warehouse_action(
                    &request_metadata,
                    warehouse_id,
                    &CatalogWarehouseAction::CanUse,
                )
                .await?;
        }

        let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
        let parent_id = if let Some(namespace_parent) = namespace.parent() {
            let parent_namespace_id =
                C::namespace_to_id(warehouse_id, &namespace_parent, t.transaction()).await;
            let parent_namespace_id = authorizer
                .require_namespace_action(
                    &request_metadata,
                    parent_namespace_id,
                    &CatalogNamespaceAction::CanCreateNamespace,
                )
                .await?;
            Some(parent_namespace_id)
        } else {
            authorizer
                .require_warehouse_action(
                    &request_metadata,
                    warehouse_id,
                    &CatalogWarehouseAction::CanCreateNamespace,
                )
                .await?;
            None
        };

        // ------------------- BUSINESS LOGIC -------------------
        let namespace_id = NamespaceIdentUuid::default();
        let warehouse = C::require_warehouse(warehouse_id, t.transaction()).await?;

        let mut namespace_props = NamespaceProperties::try_from_maybe_props(properties.clone())
            .map_err(|e| ErrorModel::bad_request(e.to_string(), e.err_type(), None))?;
        // Set location if not specified - validate location if specified
        set_namespace_location_property(&mut namespace_props, &warehouse, namespace_id)?;
        remove_managed_namespace_properties(&mut namespace_props);

        let mut request = request;
        request.properties = Some(namespace_props.into());

        let mut r =
            C::create_namespace(warehouse_id, namespace_id, request, t.transaction()).await?;
        let authz_parent = if let Some(parent_id) = parent_id {
            NamespaceParent::Namespace(parent_id)
        } else {
            NamespaceParent::Warehouse(warehouse_id)
        };
        authorizer
            .create_namespace(&request_metadata, namespace_id, authz_parent)
            .await?;
        t.commit().await?;
        r.properties
            .as_mut()
            .map(|p| p.insert(NAMESPACE_ID_PROPERTY.to_string(), namespace_id.to_string()));
        Ok(r)
    }

    /// Return all stored metadata properties for a given namespace
    async fn load_namespace_metadata(
        parameters: NamespaceParameters,
        query: GetNamespacePropertiesQuery,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<GetNamespaceResponse> {
        let GetNamespacePropertiesQuery { return_uuid } = query;
        // ------------------- VALIDATIONS -------------------
        let warehouse_id = require_warehouse_id(parameters.prefix)?;
        validate_namespace_ident(&parameters.namespace)?;

        // ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz.clone();
        let mut t = C::Transaction::begin_read(state.v1_state.catalog).await?;
        let namespace_id = authorized_namespace_ident_to_id::<C, _>(
            authorizer,
            &request_metadata,
            &warehouse_id,
            &parameters.namespace,
            &CatalogNamespaceAction::CanGetMetadata,
            t.transaction(),
        )
        .await?;

        // ------------------- BUSINESS LOGIC -------------------
        let mut r = C::get_namespace(warehouse_id, namespace_id, t.transaction()).await?;
        r.properties
            .as_mut()
            .map(|p| p.insert(NAMESPACE_ID_PROPERTY.to_string(), namespace_id.to_string()));
        t.commit().await?;
        Ok(GetNamespaceResponse {
            properties: r.properties,
            namespace: r.namespace,
            namespace_uuid: return_uuid.then_some(*namespace_id),
        })
    }

    /// Check if a namespace exists
    async fn namespace_exists(
        parameters: NamespaceParameters,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        //  ------------------- VALIDATIONS -------------------
        let warehouse_id = require_warehouse_id(parameters.prefix)?;

        //  ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz.clone();
        let mut t = C::Transaction::begin_read(state.v1_state.catalog).await?;
        let _namespace_id = authorized_namespace_ident_to_id::<C, _>(
            authorizer,
            &request_metadata,
            &warehouse_id,
            &parameters.namespace,
            &CatalogNamespaceAction::CanGetMetadata,
            t.transaction(),
        )
        .await?;

        t.commit().await?;
        Ok(())
    }

    /// Drop a namespace from the catalog. Namespace must be empty.
    async fn drop_namespace(
        parameters: NamespaceParameters,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        //  ------------------- VALIDATIONS -------------------
        let warehouse_id = require_warehouse_id(parameters.prefix)?;
        validate_namespace_ident(&parameters.namespace)?;

        if CONFIG
            .reserved_namespaces
            .contains(&parameters.namespace.as_ref()[0].to_lowercase())
        {
            return Err(ErrorModel::builder()
                .code(StatusCode::BAD_REQUEST.into())
                .message("Cannot drop namespace which is reserved for internal use.".to_owned())
                .r#type("ReservedNamespace".to_owned())
                .build()
                .into());
        }

        //  ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz.clone();
        let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
        let namespace_id = authorized_namespace_ident_to_id::<C, _>(
            authorizer.clone(),
            &request_metadata,
            &warehouse_id,
            &parameters.namespace,
            &CatalogNamespaceAction::CanDelete,
            t.transaction(),
        )
        .await?;

        //  ------------------- BUSINESS LOGIC -------------------
        C::drop_namespace(warehouse_id, namespace_id, t.transaction()).await?;
        authorizer
            .delete_namespace(&request_metadata, namespace_id)
            .await?;
        t.commit().await?;
        Ok(())
    }

    /// Set or remove properties on a namespace
    async fn update_namespace_properties(
        parameters: NamespaceParameters,
        request: UpdateNamespacePropertiesRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<UpdateNamespacePropertiesResponse> {
        //  ------------------- VALIDATIONS -------------------
        let warehouse_id = require_warehouse_id(parameters.prefix)?;
        validate_namespace_ident(&parameters.namespace)?;
        let UpdateNamespacePropertiesRequest { removals, updates } = request;
        updates
            .as_ref()
            .map(|p| validate_namespace_properties_keys(p.keys()))
            .transpose()?;
        removals
            .as_ref()
            .map(validate_namespace_properties_keys)
            .transpose()?;

        namespace_location_may_not_change(updates.as_ref(), removals.as_ref())?;
        let mut updates = NamespaceProperties::try_from_maybe_props(updates.clone())
            .map_err(|e| ErrorModel::bad_request(e.to_string(), e.err_type(), None))?;
        remove_managed_namespace_properties(&mut updates);
        //  ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz.clone();
        let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
        let namespace_id = authorized_namespace_ident_to_id::<C, _>(
            authorizer,
            &request_metadata,
            &warehouse_id,
            &parameters.namespace,
            &CatalogNamespaceAction::CanUpdateProperties,
            t.transaction(),
        )
        .await?;

        //  ------------------- BUSINESS LOGIC -------------------
        let previous_properties =
            C::get_namespace(warehouse_id, namespace_id, t.transaction()).await?;
        let (new_properties, r) =
            update_namespace_properties(previous_properties.properties, updates, removals);
        C::update_namespace_properties(warehouse_id, namespace_id, new_properties, t.transaction())
            .await?;
        t.commit().await?;
        Ok(r)
    }
}

pub(crate) async fn authorized_namespace_ident_to_id<C: Catalog, A: Authorizer + Clone>(
    authorizer: A,
    metadata: &RequestMetadata,
    warehouse_id: &WarehouseIdent,
    namespace: &NamespaceIdent,
    action: impl From<&CatalogNamespaceAction> + std::fmt::Display + Send,
    transaction: <C::Transaction as Transaction<C::State>>::Transaction<'_>,
) -> Result<NamespaceIdentUuid> {
    validate_namespace_ident(namespace)?;
    authorizer
        .require_warehouse_action(metadata, *warehouse_id, &CatalogWarehouseAction::CanUse)
        .await?;
    let namespace_id = C::namespace_to_id(*warehouse_id, namespace, transaction).await; // Cannot fail before authz
    authorizer
        .require_namespace_action(metadata, namespace_id, action)
        .await
        .map_err(set_not_found_status_code)
}

pub(crate) fn uppercase_first_letter(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        None => String::new(),
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
    }
}

pub(crate) fn validate_namespace_properties_keys<'a, I>(properties: I) -> Result<()>
where
    I: IntoIterator<Item = &'a String>,
{
    for prop in properties {
        if UNSUPPORTED_NAMESPACE_PROPERTIES.contains(&prop.as_str()) {
            return Err(ErrorModel::builder()
                .code(StatusCode::BAD_REQUEST.into())
                .message(format!(
                    "Specifying the '{prop}' property for Namespaces is not supported. '{prop}' is managed by the catalog.",
                ))
                .r#type(format!("{}PropertyNotSupported", uppercase_first_letter(prop)))
                .build()
                .into());
        } else if prop != &prop.to_lowercase() {
            return Err(ErrorModel::builder()
                .code(StatusCode::BAD_REQUEST.into())
                .message(format!("The property '{prop}' is not all lowercase."))
                .r#type(format!("{}NotLowercase", uppercase_first_letter(prop)))
                .build()
                .into());
        }
    }
    Ok(())
}

pub(crate) fn validate_namespace_ident(namespace: &NamespaceIdent) -> Result<()> {
    if namespace.len() > MAX_NAMESPACE_DEPTH as usize {
        return Err(ErrorModel::bad_request(
            format!("Namespace exceeds maximum depth of {MAX_NAMESPACE_DEPTH}",),
            "NamespaceDepthExceeded".to_string(),
            None,
        )
        .into());
    }

    if namespace.deref().iter().any(|s| s.contains('.')) {
        return Err(ErrorModel::bad_request(
            "Namespace parts cannot contain '.'".to_string(),
            "NamespacePartContainsDot".to_string(),
            None,
        )
        .append_detail(format!("Namespace: {namespace:?}"))
        .into());
    }

    if namespace.iter().any(String::is_empty) {
        return Err(ErrorModel::bad_request(
            "Namespace parts cannot be empty".to_string(),
            "NamespacePartEmpty".to_string(),
            None,
        )
        .append_detail(format!("Namespace: {namespace:?}"))
        .into());
    }

    Ok(())
}

fn remove_managed_namespace_properties(namespace_props: &mut NamespaceProperties) {
    namespace_props.remove_untyped(NAMESPACE_ID_PROPERTY);
    namespace_props.remove_untyped(MANAGED_ACCESS_PROPERTY);
}

fn set_namespace_location_property(
    namespace_props: &mut NamespaceProperties,
    warehouse: &GetWarehouseResponse,
    namespace_id: NamespaceIdentUuid,
) -> Result<()> {
    let mut location = namespace_props.get_location();

    // NS locations should always have a trailing slash
    location.as_mut().map(Location::with_trailing_slash);

    // For customer specified location, we need to check if we can write to the location.
    // If no location is specified, we use our default location.
    let location = if let Some(location) = location {
        warehouse
            .storage_profile
            .require_allowed_location(&location)?;
        location
    } else {
        warehouse
            .storage_profile
            .default_namespace_location(namespace_id)?
    };

    namespace_props.insert(&location);
    Ok(())
}

fn update_namespace_properties(
    previous_properties: Option<HashMap<String, String>>,
    updates: NamespaceProperties,
    removals: Option<Vec<String>>,
) -> (HashMap<String, String>, UpdateNamespacePropertiesResponse) {
    let mut properties = previous_properties.unwrap_or_default();

    let mut changes_updated = vec![];
    let mut changes_removed = vec![];
    let mut changes_missing = vec![];

    for key in removals.unwrap_or_default() {
        if properties.remove(&key).is_some() {
            changes_removed.push(key.clone());
        } else {
            changes_missing.push(key.clone());
        }
    }

    for (key, value) in updates {
        // Push to updated if the value for the key is different.
        // Also push on insert

        if properties.insert(key.clone(), value.clone()) != Some(value) {
            changes_updated.push(key);
        }
    }

    // Remove managed property namespace_id
    properties.remove(NAMESPACE_ID_PROPERTY);

    (
        properties,
        UpdateNamespacePropertiesResponse {
            updated: changes_updated,
            removed: changes_removed,
            missing: if changes_missing.is_empty() {
                None
            } else {
                Some(changes_missing)
            },
        },
    )
}

fn namespace_location_may_not_change(
    updates: Option<&HashMap<String, String>>,
    removals: Option<&Vec<String>>,
) -> Result<()> {
    if removals
        .as_ref()
        .is_some_and(|r| r.contains(&Location::KEY.to_string()))
    {
        return Err(ErrorModel::bad_request(
            "Namespace property `location` cannot be removed.",
            "LocationCannotBeRemoved",
            None,
        )
        .into());
    }

    if let Some(location) = updates.as_ref().and_then(|u| u.get(Location::KEY)) {
        return Err(ErrorModel::bad_request(
            "Namespace property `location` cannot be updated.",
            "LocationCannotBeUpdated",
            None,
        )
        .append_detail(format!("Location: {location:?}"))
        .into());
    }

    Ok(())
}

#[cfg(test)]
mod tests {

    use std::{collections::HashSet, hash::RandomState};

    use iceberg::NamespaceIdent;
    use iceberg_ext::catalog::rest::CreateNamespaceRequest;
    use sqlx::PgPool;

    use crate::{
        api::{
            iceberg::{
                types::{PageToken, Prefix},
                v1::namespace::Service,
            },
            management::v1::warehouse::TabularDeleteProfile,
            ApiContext,
        },
        catalog::{test::impl_pagination_tests, CatalogServer},
        implementations::postgres::{
            namespace::namespace_to_id, PostgresCatalog, PostgresTransaction, SecretsState,
        },
        request_metadata::RequestMetadata,
        service::{
            authz::implementations::openfga::{tests::ObjectHidingMock, OpenFGAAuthorizer},
            ListNamespacesQuery, State, Transaction, UserId,
        },
    };

    async fn ns_paginate_test_setup(
        pool: PgPool,
        number_of_namespaces: usize,
        hide_ranges: &[(usize, usize)],
    ) -> (
        ApiContext<State<OpenFGAAuthorizer, PostgresCatalog, SecretsState>>,
        Option<Prefix>,
    ) {
        let prof = crate::catalog::test::test_io_profile();

        let hiding_mock = ObjectHidingMock::new();
        let authz = hiding_mock.to_authorizer();

        let (ctx, warehouse) = crate::catalog::test::setup(
            pool.clone(),
            prof,
            None,
            authz,
            TabularDeleteProfile::Hard {},
            Some(UserId::new_unchecked("oidc", "test-user-id")),
        )
        .await;

        for n in 0..number_of_namespaces {
            let ns = format!("{n}");
            let ns = CatalogServer::create_namespace(
                Some(Prefix(warehouse.warehouse_id.to_string())),
                CreateNamespaceRequest {
                    namespace: NamespaceIdent::new(ns),
                    properties: None,
                },
                ctx.clone(),
                RequestMetadata::new_unauthenticated(),
            )
            .await
            .unwrap();
            let mut trx = PostgresTransaction::begin_read(ctx.v1_state.catalog.clone())
                .await
                .unwrap();
            for (range_start, range_end) in hide_ranges {
                if n >= *range_start && n < *range_end {
                    hiding_mock.hide(&format!(
                        "namespace:{}",
                        *namespace_to_id(warehouse.warehouse_id, &ns.namespace, trx.transaction(),)
                            .await
                            .unwrap()
                            .unwrap()
                    ));
                }
            }
            trx.commit().await.unwrap();
        }
        (ctx, Some(Prefix(warehouse.warehouse_id.to_string())))
    }

    impl_pagination_tests!(
        namespace,
        ns_paginate_test_setup,
        CatalogServer,
        ListNamespacesQuery,
        namespaces,
        |ns| ns.inner()[0].to_string()
    );

    #[sqlx::test]
    async fn test_ns_pagination(pool: sqlx::PgPool) {
        let prof = crate::catalog::test::test_io_profile();

        let hiding_mock = ObjectHidingMock::new();
        let authz = hiding_mock.to_authorizer();

        let (ctx, warehouse) = crate::catalog::test::setup(
            pool.clone(),
            prof,
            None,
            authz,
            TabularDeleteProfile::Hard {},
            Some(UserId::new_unchecked("oidc", "test-user-id")),
        )
        .await;
        for n in 0..10 {
            let ns = format!("ns-{n}");
            let _ = CatalogServer::create_namespace(
                Some(Prefix(warehouse.warehouse_id.to_string())),
                CreateNamespaceRequest {
                    namespace: NamespaceIdent::new(ns),
                    properties: None,
                },
                ctx.clone(),
                RequestMetadata::new_unauthenticated(),
            )
            .await
            .unwrap();
        }

        let all = CatalogServer::list_namespaces(
            Some(Prefix(warehouse.warehouse_id.to_string())),
            ListNamespacesQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(11),
                parent: None,
                return_uuids: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        assert_eq!(all.namespaces.len(), 10);

        let _ = CatalogServer::list_namespaces(
            Some(Prefix(warehouse.warehouse_id.to_string())),
            ListNamespacesQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(10),
                parent: None,
                return_uuids: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        assert_eq!(all.namespaces.len(), 10);

        let first_six = CatalogServer::list_namespaces(
            Some(Prefix(warehouse.warehouse_id.to_string())),
            ListNamespacesQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(6),
                parent: None,
                return_uuids: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        assert_eq!(first_six.namespaces.len(), 6);
        let first_six_items: HashSet<String, RandomState> = first_six
            .namespaces
            .into_iter()
            .map(|ns| ns.to_url_string())
            .collect();
        for i in 0..6 {
            assert!(first_six_items.contains(&format!("ns-{i}")));
        }

        let next_four = CatalogServer::list_namespaces(
            Some(Prefix(warehouse.warehouse_id.to_string())),
            ListNamespacesQuery {
                page_token: PageToken::Present(first_six.next_page_token.unwrap()),
                page_size: Some(6),
                parent: None,
                return_uuids: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        let next_four_items: HashSet<String, RandomState> = next_four
            .namespaces
            .into_iter()
            .map(|ns| ns.to_url_string())
            .collect();
        for i in 6..10 {
            assert!(next_four_items.contains(&format!("ns-{i}")));
        }

        let mut ids = all.namespace_uuids.unwrap();
        ids.sort();
        for i in ids.iter().take(6).skip(4) {
            hiding_mock.hide(&format!("namespace:{i}"));
        }

        let page = CatalogServer::list_namespaces(
            Some(Prefix(warehouse.warehouse_id.to_string())),
            ListNamespacesQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(5),
                parent: None,
                return_uuids: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        assert_eq!(page.namespaces.len(), 5);
        assert!(page.next_page_token.is_some());

        let page_items: HashSet<String, RandomState> = page
            .namespaces
            .into_iter()
            .map(|ns| ns.to_url_string())
            .collect();

        for i in 0..5 {
            let ns_id = if i > 3 { i + 2 } else { i };
            assert!(page_items.contains(&format!("ns-{ns_id}")));
        }
        let next_page = CatalogServer::list_namespaces(
            Some(Prefix(warehouse.warehouse_id.to_string())),
            ListNamespacesQuery {
                page_token: PageToken::Present(page.next_page_token.unwrap()),
                page_size: Some(5),
                parent: None,
                return_uuids: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        assert_eq!(next_page.namespaces.len(), 3);

        let next_page_items: HashSet<String, RandomState> = next_page
            .namespaces
            .into_iter()
            .map(|ns| ns.to_url_string())
            .collect();

        for i in 7..10 {
            assert!(next_page_items.contains(&format!("ns-{i}")));
        }
    }

    #[test]
    fn test_update_ns_properties() {
        use super::*;
        let previous_properties = HashMap::from_iter(vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
            ("key3".to_string(), "value3".to_string()),
            ("key5".to_string(), "value5".to_string()),
        ]);

        let updates = NamespaceProperties::from_props_unchecked(vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value12".to_string()),
        ]);

        let removals = Some(vec!["key3".to_string(), "key4".to_string()]);

        let (new_props, result) =
            update_namespace_properties(Some(previous_properties), updates, removals);
        assert_eq!(result.updated, vec!["key2".to_string()]);
        assert_eq!(result.removed, vec!["key3".to_string()]);
        assert_eq!(result.missing, Some(vec!["key4".to_string()]));
        assert_eq!(
            new_props,
            HashMap::from_iter(vec![
                ("key1".to_string(), "value1".to_string()),
                ("key2".to_string(), "value12".to_string()),
                ("key5".to_string(), "value5".to_string()),
            ])
        );
    }

    #[test]
    fn test_update_ns_properties_empty_removal() {
        use super::*;
        let previous_properties = HashMap::from_iter(vec![]);
        let updates = NamespaceProperties::from_props_unchecked(vec![]);
        let removals = Some(vec![]);

        let (new_props, result) =
            update_namespace_properties(Some(previous_properties), updates, removals);
        assert!(result.updated.is_empty());
        assert!(result.removed.is_empty());
        assert!(result.missing.is_none());
        assert!(new_props.is_empty());
    }
}
