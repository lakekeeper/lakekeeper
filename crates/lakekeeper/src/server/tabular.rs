use crate::{
    server::tables::parse_location,
    service::{
        storage::{StorageLocations as _, StorageProfile},
        GetNamespaceResponse, TabularId, TabularListFlags,
    },
};

pub(crate) fn default_view_flags() -> bool {
    false
}

pub(crate) fn default_table_flags() -> TabularListFlags {
    TabularListFlags::active()
}

pub(super) fn determine_tabular_location(
    namespace: &GetNamespaceResponse,
    request_table_location: Option<String>,
    table_id: TabularId,
    storage_profile: &StorageProfile,
) -> Result<Location, ErrorModel> {
    let request_table_location = request_table_location
        .map(|l| parse_location(&l, StatusCode::BAD_REQUEST))
        .transpose()?;

    let mut location = if let Some(location) = request_table_location {
        storage_profile.require_allowed_location(&location)?;
        location
    } else {
        let namespace_props = NamespaceProperties::from_props_unchecked(
            namespace
                .properties
                .as_ref()
                .map(|arc| (**arc).clone())
                .unwrap_or_default(),
        );

        let namespace_location = match namespace_props.get_location() {
            Some(location) => location,
            None => storage_profile
                .default_namespace_location(namespace.namespace_id)
                .map_err(|e| {
                    ErrorModel::internal(
                        "Failed to generate default namespace location",
                        "InvalidDefaultNamespaceLocation",
                        Some(Box::new(e)),
                    )
                })?,
        };

        storage_profile.default_tabular_location(&namespace_location, table_id)
    };
    // all locations are without a trailing slash
    location.without_trailing_slash();
    Ok(location)
}

macro_rules! list_entities {
    ($entity:ident, $list_fn:ident, $namespace_response:ident, $authorizer:ident, $request_metadata:ident) => {
        |ps, page_token, trx| {
            use ::paste::paste;
            paste! {
                use crate::server::tabular::[<default_ $entity:snake _flags>] as default_flags;
            }
            use crate::server::UnfilteredPage;
            // let namespace = $namespace.clone();
            let authorizer = $authorizer.clone();
            let request_metadata = $request_metadata.clone();
            let warehouse_id = $namespace_response.warehouse_id;
            let namespace_response = $namespace_response.clone();
            async move {
                let query = crate::api::iceberg::v1::PaginationQuery {
                    page_size: Some(ps),
                    page_token: page_token.into(),
                };
                let entities = C::$list_fn(
                    &namespace_response,
                    default_flags(),
                    trx.transaction(),
                    query,
                )
                .await?;
                let can_list_everything = authorizer
                    .is_allowed_namespace_action(
                        &request_metadata,
                        &namespace_response,
                        CatalogNamespaceAction::CanListEverything,
                    )
                    .await?
                    .into_inner();

                let (ids, idents, tokens): (Vec<_>, Vec<_>, Vec<_>) =
                    entities.into_iter_with_page_tokens().multiunzip();

                let masks = if can_list_everything {
                    // No need to check individual permissions if everything in namespace can
                    // be listed.
                    vec![true; ids.len()]
                } else {
                    paste! {
                        authorizer.[<are_allowed_ $entity:lower _actions>](
                            &request_metadata,
                            warehouse_id,
                            ids.iter().map(|id| (
                                *id,
                                [<Catalog $entity Action>]::CanIncludeInList)
                            ).collect(),
                        ).await?.into_inner()
                    }
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

use http::StatusCode;
use iceberg_ext::{catalog::rest::ErrorModel, configs::namespace::NamespaceProperties};
use lakekeeper_io::Location;
pub(crate) use list_entities;
