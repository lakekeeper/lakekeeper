use iceberg_ext::catalog::rest::ErrorModel;

use crate::{
    api::{self, management::v1::warehouse::UndropTabularsRequest},
    request_metadata::RequestMetadata,
    service::{
        authz::{Authorizer, MustUse},
        TabularId,
    },
};

pub(crate) async fn require_undrop_permissions<A: Authorizer>(
    request: &UndropTabularsRequest,
    authorizer: &A,
    request_metadata: &RequestMetadata,
) -> api::Result<()> {
    let all_allowed =
        can_undrop_all_specified_tabulars(request_metadata, authorizer, request.targets.as_slice())
            .await?;
    if !all_allowed {
        return Err(ErrorModel::forbidden(
            "Not allowed to undrop at least one specified tabular.",
            "NotAuthorized",
            None,
        )
        .into());
    }
    Ok(())
}

async fn can_undrop_all_specified_tabulars<A: Authorizer>(
    request_metadata: &RequestMetadata,
    authorizer: &A,
    tabs: &[TabularId],
) -> api::Result<bool> {
    let mut futs = vec![];

    for t in tabs {
        match t {
            TabularId::View(id) => {
                futs.push(authorizer.is_allowed_view_action(
                    request_metadata,
                    (*id).into(),
                    crate::service::authz::CatalogViewAction::CanUndrop,
                ));
            }
            TabularId::Table(id) => {
                futs.push(authorizer.is_allowed_table_action(
                    request_metadata,
                    (*id).into(),
                    crate::service::authz::CatalogTableAction::CanUndrop,
                ));
            }
        }
    }
    let all_allowed = futures::future::try_join_all(futs)
        .await?
        .into_iter()
        .all(MustUse::into_inner);
    Ok(all_allowed)
}
