use crate::api::iceberg::types::PageToken;
use crate::api::iceberg::v1::PaginationQuery;
use crate::api::management::v1::warehouse::{UndropTabularsRequest, Undroppable};
use crate::request_metadata::RequestMetadata;
use crate::service::authz::Authorizer;
use crate::service::{Catalog, ListFlags, TableIdentUuid, TabularIdentUuid, Transaction};
use crate::{api, WarehouseIdent};
use iceberg_ext::catalog::rest::ErrorModel;
use itertools::Itertools;

pub(crate) async fn collect_tabular_ids<C: Catalog>(
    warehouse_ident: WarehouseIdent,
    request: UndropTabularsRequest,
    transaction: <C::Transaction as Transaction<C::State>>::Transaction<'_>,
) -> api::Result<Vec<TableIdentUuid>> {
    Ok(match request.undrop {
        Undroppable::Tabulars { tabulars: tabs } => {
            tabs.into_iter().map(|i| TableIdentUuid::from(*i)).collect()
        }
        Undroppable::Namespace { namespace: ns } =>
        // TODO: do we want to paginate here?
        {
            C::list_tabulars(
                warehouse_ident,
                Some(ns.into()),
                ListFlags::only_deleted(),
                transaction,
                PaginationQuery {
                    page_token: PageToken::NotSpecified,
                    page_size: Some(1000),
                },
            )
            .await?
            .into_iter()
            .map(|t| TableIdentUuid::from(*t.0))
            .collect_vec()
        }
    })
}

pub(crate) async fn require_undrop_permissions<A: Authorizer>(
    request: &UndropTabularsRequest,
    authorizer: &A,
    request_metadata: &RequestMetadata,
    warehouse_ident: WarehouseIdent,
) -> api::Result<()> {
    match &request.undrop {
        Undroppable::Tabulars { tabulars: tabs } => {
            let all_allowed = can_undrop_all_specified_tabulars(
                request_metadata,
                warehouse_ident,
                authorizer,
                tabs,
            )
            .await?;
            if !all_allowed {
                return Err(ErrorModel::forbidden(
                    "Not allowed to undrop at least one specified tabular.",
                    "NotAuthorized",
                    None,
                )
                .into());
            };
        }
        Undroppable::Namespace { namespace: ns } => {
            let false = authorizer
                .is_allowed_namespace_action(
                    request_metadata,
                    warehouse_ident,
                    (*ns).into(),
                    &crate::service::authz::CatalogNamespaceAction::CanUndropAll,
                )
                .await?
            else {
                return Err(ErrorModel::forbidden(
                    "Not allowed to undrop all tabulars in the specified namespace.",
                    "NotAuthorized",
                    None,
                )
                .into());
            };
        }
    }
    Ok(())
}

async fn can_undrop_all_specified_tabulars<A: Authorizer>(
    request_metadata: &RequestMetadata,
    warehouse_ident: WarehouseIdent,
    authorizer: &A,
    tabs: &[TabularIdentUuid],
) -> api::Result<bool> {
    let mut futs = vec![];

    for t in tabs {
        match t {
            TabularIdentUuid::View(id) => {
                futs.push(authorizer.is_allowed_view_action(
                    request_metadata,
                    warehouse_ident,
                    (*id).into(),
                    &crate::service::authz::CatalogViewAction::CanUndrop,
                ));
            }
            TabularIdentUuid::Table(id) => {
                futs.push(authorizer.is_allowed_table_action(
                    request_metadata,
                    warehouse_ident,
                    (*id).into(),
                    &crate::service::authz::CatalogTableAction::CanUndrop,
                ));
            }
        }
    }
    let all_allowed = futures::future::try_join_all(futs)
        .await?
        .into_iter()
        .all(|t| t);
    Ok(all_allowed)
}
