use std::sync::Arc;

use iceberg::NamespaceIdent;

use crate::{
    WarehouseId,
    api::{Result, endpoints::EndpointFlat},
    request_metadata::RequestMetadata,
    server::tables::parse_location,
    service::{
        CatalogIdempotencyOps, CatalogStore, NamespaceHierarchy, NamespaceId, ResolvedWarehouse,
        TabularId, Transaction,
        authz::{AuthZError, Authorizer, AuthzNamespaceOps, CatalogNamespaceAction},
        idempotency::{IdempotencyInfo, IdempotencyKey},
        is_same_namespace_path_ignoring_ascii_case,
        storage::{
            StorageProfile,
            storage_layout::{NamespaceNameContext, NamespacePath, TabularNameContext},
        },
    },
};

/// Claim the request's idempotency key inside the rename transaction.
///
/// Returns the transaction so the caller can go on to commit it. A key another in-flight
/// request already holds is not an error the caller can recover from, so the transaction is
/// rolled back here and the request fails; `None` is a no-op. Every rename endpoint answers
/// 204, which is the status recorded against the key.
pub(crate) async fn claim_rename_idempotency_key<C: CatalogStore>(
    mut transaction: C::Transaction,
    warehouse_id: WarehouseId,
    key: Option<IdempotencyKey>,
    endpoint: EndpointFlat,
) -> Result<C::Transaction> {
    let Some(key) = key else {
        return Ok(transaction);
    };

    let claimed = C::try_insert_idempotency_key(
        warehouse_id,
        &IdempotencyInfo::builder()
            .key(key)
            .endpoint(endpoint)
            .http_status(StatusCode::NO_CONTENT)
            .build(),
        transaction.transaction(),
    )
    .await?;
    if claimed {
        return Ok(transaction);
    }

    transaction
        .rollback()
        .await
        .inspect_err(|e| {
            tracing::warn!("Rollback failed after idempotency conflict: {e}");
        })
        .ok();
    Err(ErrorModel::request_in_progress().into())
}

/// Fail a rename whose row landed in a different namespace than authorization was
/// evaluated against.
///
/// A backstop, not the defence. The destination namespace is resolved once, before the
/// transaction, and `rename_tabular` pins the row to that id rather than resolving the
/// destination name a second time — so this holds by construction and should never fire.
/// It stays because the alternative is silent: were the statement to resolve the
/// destination itself again, authorization would have been checked against one namespace
/// while the row moved into another, and the authorizer would be re-pointed at the wrong
/// one — or, when the two happen to coincide with the source, not re-pointed at all.
/// Nothing about that reaches the caller or shows up in an assignment listing.
///
/// Called before the commit, so returning here rolls the rename back.
pub(crate) fn ensure_authorized_destination(
    authorized: NamespaceId,
    committed: NamespaceId,
) -> Result<()> {
    if authorized == committed {
        return Ok(());
    }
    tracing::warn!(
        "Rename destination namespace changed under the request: authorized {authorized}, \
         landed in {committed}. Refusing the rename."
    );
    Err(destination_namespace_changed().into())
}

fn destination_namespace_changed() -> ErrorModel {
    ErrorModel::conflict(
        "The destination namespace changed while the request was in flight. Please retry.",
        "DestinationNamespaceChanged",
        None,
    )
}

/// Fail a rename whose paths name one namespace but which resolved to two.
///
/// Such a rename is recorded and authorized as `rename` on the strength of its paths, so it
/// must not move the tabular. The catalog's collation rules this out; failing keeps the
/// record and the checks in agreement should it ever happen.
pub(crate) fn ensure_rename_stays_in_namespace(
    move_destination: Option<&Arc<Vec<String>>>,
    source: NamespaceId,
    destination: NamespaceId,
) -> Result<()> {
    if move_destination.is_some() || source == destination {
        return Ok(());
    }
    tracing::warn!(
        "Rename paths name one namespace but resolved to {source} and {destination}. \
         Refusing the rename."
    );
    Err(destination_namespace_changed().into())
}

/// The destination path of a rename that moves a tabular into another namespace; `None` for
/// a rename within its namespace.
///
/// Decided on the request's paths, so the audit record names the action the checks ask
/// (`move` or `rename`) before anything is resolved, a served replay included. Paths equal
/// ignoring ASCII case name one namespace; any other difference counts as a move, which can
/// only ask more than needed (see [`is_same_namespace_path_ignoring_ascii_case`]).
pub(crate) fn tabular_move_destination(
    source: &NamespaceIdent,
    destination: &NamespaceIdent,
) -> Option<Arc<Vec<String>>> {
    (!is_same_namespace_path_ignoring_ascii_case(source.as_ref(), destination.as_ref()))
        .then(|| Arc::new(destination.as_ref().clone()))
}

/// The action a rename is recorded and checked under: `move` into another namespace,
/// `rename` within one.
pub(crate) fn rename_or_move<A>(
    move_destination: Option<&Arc<Vec<String>>>,
    rename: A,
    move_to: impl FnOnce(Arc<Vec<String>>) -> A,
) -> A {
    move_destination.map_or(rename, |destination| move_to(destination.clone()))
}

/// The namespaces at both ends of a rename, as [`authorize_tabular_move`] checks them.
pub(crate) struct TabularRenameEnds<'a> {
    pub(crate) source_namespace: &'a NamespaceHierarchy,
    pub(crate) destination_namespace: NamespaceHierarchy,
    /// From [`tabular_move_destination`].
    pub(crate) move_destination: Option<Arc<Vec<String>>>,
}

/// The move half of a rename into another namespace: `AcceptMovedTabular` on the destination
/// namespace and, through `require_move`, the tabular's own `Move`. A rename within one
/// namespace asks neither; [`ensure_rename_stays_in_namespace`] holds it to that.
///
/// A moved tabular carries its direct grants and inherits the destination's, so, as for a
/// namespace move, the actor must be able to grant at both ends.
pub(crate) async fn authorize_tabular_move<A, T, E, Fut>(
    authorizer: &A,
    request_metadata: &RequestMetadata,
    warehouse: &ResolvedWarehouse,
    ends: TabularRenameEnds<'_>,
    tabular: T,
    require_move: impl FnOnce(T, Arc<Vec<String>>) -> Fut,
) -> std::result::Result<(NamespaceHierarchy, T), AuthZError>
where
    A: Authorizer,
    Fut: Future<Output = std::result::Result<T, E>>,
    AuthZError: From<E>,
{
    let TabularRenameEnds {
        source_namespace,
        destination_namespace,
        move_destination,
    } = ends;
    let Some(destination_path) = move_destination else {
        return Ok((destination_namespace, tabular));
    };
    let destination_id = destination_namespace.namespace_id();
    let source_path = Arc::new(source_namespace.namespace_ident().as_ref().clone());
    let (destination_namespace, tabular) = tokio::join!(
        authorizer.require_namespace_action(
            request_metadata,
            warehouse,
            destination_id,
            Ok(Some(destination_namespace)),
            CatalogNamespaceAction::AcceptMovedTabular {
                source: source_path,
            },
        ),
        require_move(tabular, destination_path),
    );
    Ok((destination_namespace?, tabular?))
}

/// Commit a rename, re-pointing the tabular's authorizer hierarchy when it crosses
/// namespaces.
///
/// A tabular inherits its permissions from the namespace it hangs under, so a rename that
/// changes the namespace has to move that edge or the tabular keeps inheriting from the
/// namespace it left.
///
/// Namespaces are compared by id, not ident, because namespace idents are case-insensitive:
/// `a.t -> A.t2` names one namespace and is not a move. Re-pointing a tabular onto the
/// namespace it already has would still converge here — the attach rewrites what the detach
/// removed — but it would spend two pointless OpenFGA round trips on the common in-place
/// rename, and open a window in which a crash between them leaves the tabular with no parent
/// at all.
///
/// Detach-then-commit-then-attach, so that every failure mode leaves the authorizer
/// *missing* an edge rather than holding an extra one; see the hook docs on
/// [`Authorizer::detach_tabular_parent`] and [`Authorizer::attach_tabular_parent`] for the
/// full ordering contract. Consumes the transaction: nothing may run between the detach and
/// the commit.
///
/// Three residual windows, all requiring reconciliation rather than a retry:
///
/// * An **ambiguous commit** — the connection dies after Postgres commits but before the
///   ack — surfaces as `Err`, and the compensation below then re-attaches a namespace the
///   tabular has actually left. That is the one path that can leave a surplus edge. It is
///   the same trade the namespace move makes, and `--mode add-and-delete-drift` is its
///   repair.
/// * A **failed post-commit attach** leaves the tabular parentless, and no retry repairs
///   it. With an idempotency key the replay short-circuits before reaching this function,
///   because the key was inserted in the committed transaction; without one the storage
///   layer rejects the retry, because the tabular is no longer in the namespace the caller
///   names as its source. Reconciliation is the repair path, not retry — and its default
///   additive mode suffices, because the missing edge is one the catalog implies.
/// * **Two renames of the same tabular in flight** serialize in the catalog, but their
///   OpenFGA writes are not ordered against each other: the first rename's post-commit
///   attach can land after the second's pre-commit detach of the same namespace, leaving
///   that namespace attached to a tabular that has since moved on. Only
///   `--mode add-and-delete-drift` retracts the survivor.
pub(crate) async fn commit_rename_with_reparent<C: CatalogStore, A: Authorizer>(
    transaction: C::Transaction,
    authorizer: &A,
    metadata: &RequestMetadata,
    warehouse_id: WarehouseId,
    tabular_id: TabularId,
    source_namespace_id: NamespaceId,
    destination_namespace_id: NamespaceId,
) -> Result<()> {
    let reparented = source_namespace_id != destination_namespace_id;

    // Pre-commit: retire the old edge. Hard error — nothing is committed yet, so failing
    // here leaves both systems as they were.
    if reparented {
        authorizer
            .detach_tabular_parent(metadata, warehouse_id, tabular_id, source_namespace_id)
            .await?;
    }

    if let Err(err) = transaction.commit().await {
        // The rename did not happen, so put the old edge back. Best effort: if this also
        // fails the tabular is left parentless, which is fail-closed and repairable by an
        // additive reconcile.
        if reparented {
            authorizer
                .attach_tabular_parent(metadata, warehouse_id, tabular_id, source_namespace_id)
                .await
                .inspect_err(|e| {
                    tracing::error!(
                        ?e,
                        "Failed to restore the parent of {} {tabular_id} in the authorizer \
                         after a failed commit: {}",
                        tabular_id.typ_str(),
                        e.error
                    );
                })
                .ok();
        }
        return Err(err);
    }

    // Post-commit: publish the new edge, now that the catalog has accepted the rename. Its
    // failure cannot be reported — the rename happened — so it is logged and left to
    // reconciliation, per the contract on `attach_tabular_parent`.
    if reparented {
        authorizer
            .attach_tabular_parent(metadata, warehouse_id, tabular_id, destination_namespace_id)
            .await
            .inspect_err(|e| {
                tracing::error!(
                    ?e,
                    "Failed to re-parent {} {tabular_id} in the authorizer: {}",
                    tabular_id.typ_str(),
                    e.error
                );
            })
            .ok();
    }

    Ok(())
}

pub(super) fn determine_tabular_location(
    namespace_hierarchy: &NamespaceHierarchy,
    request_table_location: Option<String>,
    table_id: TabularId,
    table_ident: &TableIdent,
    storage_profile: &StorageProfile,
) -> Result<Location, ErrorModel> {
    let namespace = &namespace_hierarchy.namespace;
    let request_table_location = request_table_location
        .map(|l| parse_location(&l, StatusCode::BAD_REQUEST))
        .transpose()?;

    let mut location = if let Some(location) = request_table_location {
        storage_profile.require_allowed_location(&location)?;
        location
    } else {
        let namespace_props = NamespaceProperties::from_props_unchecked(
            namespace.namespace.properties.clone().unwrap_or_default(),
        );

        let namespace_location = if let Some(location) = namespace_props.get_location() {
            location
        } else {
            let mut namespace_name_contexts = vec![NamespaceNameContext::try_from(namespace)?];
            for ancestor in &namespace_hierarchy.parents {
                namespace_name_contexts.push(NamespaceNameContext::try_from(ancestor)?);
            }
            namespace_name_contexts.reverse();
            let namespace_path = NamespacePath::new(namespace_name_contexts);
            storage_profile
                .default_namespace_location(&namespace_path)
                .map_err(|e| {
                    ErrorModel::internal(
                        "Failed to generate default namespace location",
                        "InvalidDefaultNamespaceLocation",
                        Some(Box::new(e)),
                    )
                })?
        };

        let table_name_context = TabularNameContext {
            name: table_ident.name.clone(),
            uuid: *table_id,
        };

        storage_profile.default_tabular_location(&namespace_location, &table_name_context)
    };
    // all locations are without a trailing slash
    location.without_trailing_slash();
    Ok(location)
}

/// Reads a page of tabulars, with their namespaces unless `$can_list_everything`.
macro_rules! fetch_entities {
    ($list_fn:ident, $namespace_response:ident, $can_list_everything:ident) => {
        |ps, page_token, trx: &mut _| {
            use crate::service::{BasicTabularInfo, TabularListFlags};

            let warehouse_id = $namespace_response.warehouse_id();
            let namespace_id = $namespace_response.namespace_id();
            let can_list_everything = $can_list_everything;

            async move {
                let query = crate::api::iceberg::v1::PaginationQuery {
                    page_size: Some(ps),
                    page_token: page_token.into(),
                };
                let entities = C::$list_fn(
                    warehouse_id,
                    Some(namespace_id),
                    TabularListFlags::active(),
                    trx.transaction(),
                    query,
                )
                .await?;

                let (ids, idents, tokens): (Vec<_>, Vec<_>, Vec<_>) =
                    entities.into_iter_with_page_tokens().multiunzip();

                let namespaces = if can_list_everything {
                    // No need to check individual permissions if everything in namespace can
                    // be listed.
                    None
                } else {
                    let requested_namespace_ids = idents
                        .iter()
                        .map(|id| BasicTabularInfo::namespace_id(&id.tabular))
                        .collect::<Vec<_>>();
                    Some(
                        C::get_namespaces_by_id(
                            warehouse_id,
                            &requested_namespace_ids,
                            trx.transaction(),
                        )
                        .await?,
                    )
                };
                Ok((ids, idents, tokens, namespaces))
            }
            .boxed()
        }
    };
}

/// Authorizes a page read by `fetch_entities!`.
macro_rules! authorize_entities {
    ($entity:ident, $resolved_warehouse:ident, $authorizer:ident, $event_ctx:ident) => {
        |ps, (ids, idents, tokens, namespaces)| {
            use ::pastey::paste;

            #[allow(unused)]
            use crate::{
                server::UnfilteredPage,
                service::{
                    require_namespace_for_tabular,
                    authz::ActionOnTable,
                    authz::ActionOnView,
                    events::context::authz_to_error_no_audit,
                },
            };

            let authorizer = $authorizer.clone();
            let request_metadata = $event_ctx.request_metadata().clone();
            let resolved_warehouse = $resolved_warehouse.clone();

            async move {
                let masks = if let Some(namespaces) = namespaces {
                    paste! {
                        authorizer.[<are_allowed_ $entity:lower _actions_vec>](
                            &request_metadata,
                            &resolved_warehouse,
                            &namespaces,
                            &idents.iter().map(|t| Ok::<_, crate::service::authz::AuthZCannotSeeNamespace>((
                                require_namespace_for_tabular(&namespaces, &t.tabular)?,
                                [<ActionOn $entity>] {
                                    info: t,
                                    action: [<Catalog $entity Action>]::IncludeInList,
                                    user: None,
                                    is_delegated_execution: false,
                                }
                            )
                            )).collect::<Result<Vec<_>, _>>()
                            .map_err(authz_to_error_no_audit)?,
                        ).await
                        .map_err(authz_to_error_no_audit)?
                        .into_allowed()
                    }
                } else {
                    vec![true; ids.len()]
                };

                let (next_idents, next_uuids, next_page_tokens, mask): (
                    Vec<_>,
                    Vec<_>,
                    Vec<_>,
                    Vec<bool>,
                ) = masks
                    .into_iter()
                    .zip(idents.into_iter().zip(ids.into_iter()))
                    .zip(tokens.into_iter())
                    .map(|((allowed, namespace), token)| (namespace.0, namespace.1, token, allowed))
                    .multiunzip();

                Ok(UnfilteredPage::new(
                    next_idents,
                    next_uuids,
                    next_page_tokens,
                    mask,
                    ps.clamp(0, i64::MAX).try_into().expect("we clamped it"),
                ))
            }
            .boxed()
        }
    };
}

pub(crate) use authorize_entities;
pub(crate) use fetch_entities;
use http::StatusCode;
use iceberg::TableIdent;
use iceberg_ext::{catalog::rest::ErrorModel, configs::namespace::NamespaceProperties};
use lakekeeper_io::Location;

#[cfg(test)]
mod tests {
    use super::*;

    /// The whole point is to fail closed, so the two directions are pinned separately.
    #[test]
    fn authorized_destination_must_be_the_one_the_row_landed_in() {
        let authorized = NamespaceId::new_random();
        assert!(ensure_authorized_destination(authorized, authorized).is_ok());

        let landed_elsewhere = NamespaceId::new_random();
        let err = ensure_authorized_destination(authorized, landed_elsewhere)
            .expect_err("a destination that moved under the request must not be accepted");
        assert_eq!(err.error.code, StatusCode::CONFLICT.as_u16());
        assert_eq!(err.error.r#type, "DestinationNamespaceChanged");
    }

    /// Paths that name one namespace must resolve to one; a move is not held to that.
    #[test]
    fn a_rename_recorded_as_in_place_must_stay_in_its_namespace() {
        let source = NamespaceId::new_random();
        let other = NamespaceId::new_random();
        assert!(ensure_rename_stays_in_namespace(None, source, source).is_ok());
        let destination = Arc::new(vec!["other".to_string()]);
        assert!(ensure_rename_stays_in_namespace(Some(&destination), source, other).is_ok());

        let err = ensure_rename_stays_in_namespace(None, source, other)
            .expect_err("an in-place rename that resolved to two namespaces must fail");
        assert_eq!(err.error.code, StatusCode::CONFLICT.as_u16());
        assert_eq!(err.error.r#type, "DestinationNamespaceChanged");
    }
}
