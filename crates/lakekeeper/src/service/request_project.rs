//! The project a catalog request addresses when it names none.
//!
//! A request names its project with `x-project-id`. A catalog request that sends no
//! header still addresses one warehouse — through the `{prefix}` of its route, or the
//! `warehouse=<project>/<name>` argument of `GET /catalog/v1/config` — and so one
//! project. [`warehouse_reference`] finds that reference by looking up the request's
//! method and matched path in the endpoint registry
//! ([`Endpoint::from_method_and_matched_path`]) and classifying the resulting
//! [`Endpoint`]; the auth middleware resolves it before anything reads the request's
//! project.

use std::str::FromStr;

#[cfg(feature = "router")]
use http::Method;

use crate::ProjectId;
#[cfg(feature = "router")]
use crate::{
    WarehouseId,
    api::endpoints::{
        CatalogV1Endpoint, DatasetV1Endpoint, Endpoint, GenericTableV1Endpoint, SignEndpoint,
    },
};

/// Where a header-less catalog request says which warehouse it is about.
#[cfg(feature = "router")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WarehouseReference {
    /// The warehouse the route's `{prefix}` names.
    Warehouse(WarehouseId),
    /// The project `GET /catalog/v1/config?warehouse=<project>/<name>` names.
    Project(ProjectId),
}

/// Where an endpoint's project comes from, for a request that sent no `x-project-id`.
#[cfg(feature = "router")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProjectSource {
    /// The route's `{prefix}` path parameter names the warehouse.
    WarehousePrefix,
    /// The `warehouse` query argument of `GET /catalog/v1/config` names the project.
    ConfigArgument,
    /// The endpoint carries no warehouse or project of its own.
    Nothing,
}

/// Classify where `endpoint`'s project comes from. Every catalog, generic-table,
/// dataset and signer endpoint is named explicitly, with no `_` arm in those groups: a
/// new endpoint added to any of them fails to compile here until it is classified.
#[cfg(feature = "router")]
fn project_source(endpoint: Endpoint) -> ProjectSource {
    match endpoint {
        Endpoint::CatalogV1(CatalogV1Endpoint::GetConfig) => ProjectSource::ConfigArgument,
        Endpoint::CatalogV1(
            CatalogV1Endpoint::ListNamespaces
            | CatalogV1Endpoint::NamespaceExists
            | CatalogV1Endpoint::CreateNamespace
            | CatalogV1Endpoint::LoadNamespaceMetadata
            | CatalogV1Endpoint::DropNamespace
            | CatalogV1Endpoint::UpdateNamespaceProperties
            | CatalogV1Endpoint::ListTables
            | CatalogV1Endpoint::CreateTable
            | CatalogV1Endpoint::LoadTable
            | CatalogV1Endpoint::UpdateTable
            | CatalogV1Endpoint::DropTable
            | CatalogV1Endpoint::TableExists
            | CatalogV1Endpoint::LoadCredentials
            | CatalogV1Endpoint::RenameTable
            | CatalogV1Endpoint::RegisterTable
            | CatalogV1Endpoint::ReportMetrics
            | CatalogV1Endpoint::CommitTransaction
            | CatalogV1Endpoint::CreateView
            | CatalogV1Endpoint::ListViews
            | CatalogV1Endpoint::LoadView
            | CatalogV1Endpoint::ReplaceView
            | CatalogV1Endpoint::DropView
            | CatalogV1Endpoint::ViewExists
            | CatalogV1Endpoint::RenameView
            | CatalogV1Endpoint::CancelPlanning
            | CatalogV1Endpoint::FetchPlanningResult
            | CatalogV1Endpoint::PlanTableScan
            | CatalogV1Endpoint::FetchScanTasks,
        )
        | Endpoint::GenericTableV1(
            GenericTableV1Endpoint::CreateGenericTable
            | GenericTableV1Endpoint::ListGenericTables
            | GenericTableV1Endpoint::LoadGenericTable
            | GenericTableV1Endpoint::DropGenericTable
            | GenericTableV1Endpoint::RenameGenericTable
            | GenericTableV1Endpoint::LoadGenericTableCredentials,
        )
        | Endpoint::DatasetV1(
            DatasetV1Endpoint::CreateDataset
            | DatasetV1Endpoint::ListDatasets
            | DatasetV1Endpoint::LoadDataset
            | DatasetV1Endpoint::DropDataset
            | DatasetV1Endpoint::ListDatasetRefs
            | DatasetV1Endpoint::CreateDatasetRef
            | DatasetV1Endpoint::MoveDatasetRef
            | DatasetV1Endpoint::DeleteDatasetRef
            | DatasetV1Endpoint::ListDatasetFiles
            | DatasetV1Endpoint::DiffDataset
            | DatasetV1Endpoint::SetDatasetRefProtection
            | DatasetV1Endpoint::CreateDatasetAccessGrant
            | DatasetV1Endpoint::RevokeDatasetAccessGrant
            | DatasetV1Endpoint::SignDatasetFiles
            | DatasetV1Endpoint::RestoreDatasetSnapshot
            | DatasetV1Endpoint::GetDatasetSnapshotMaterialization
            | DatasetV1Endpoint::ExpireDatasetSnapshot
            | DatasetV1Endpoint::CommitDataset
            | DatasetV1Endpoint::ImportDataset
            | DatasetV1Endpoint::LoadDatasetCredentials
            | DatasetV1Endpoint::UpdateDatasetSettings
            | DatasetV1Endpoint::RenameDataset,
        )
        | Endpoint::Sign(
            SignEndpoint::S3RequestPrefix
            | SignEndpoint::S3RequestTabular
            | SignEndpoint::S3RequestByTableName,
        ) => ProjectSource::WarehousePrefix,
        // Neither group ever carries a `{prefix}`: every route here is addressed by
        // `x-project-id` (or, for `PermissionV1`, proxied whole to an
        // authorizer-defined implementation with no warehouse of its own).
        Endpoint::Sign(SignEndpoint::S3RequestGlobal)
        | Endpoint::ManagementV1(_)
        | Endpoint::PermissionV1(_) => ProjectSource::Nothing,
    }
}

/// The warehouse or project a request addresses, from its method, its matched route,
/// its `{prefix}` path parameter and its query string. `None` for a method/route pair
/// the endpoint registry does not know, and for a prefix or argument that does not
/// parse: the handler then reports the request as it always does.
#[cfg(feature = "router")]
pub(crate) fn warehouse_reference(
    method: &Method,
    matched_path: &str,
    prefix: Option<&str>,
    query: Option<&str>,
) -> Option<WarehouseReference> {
    let endpoint = Endpoint::from_method_and_matched_path(method, matched_path)?;
    match project_source(endpoint) {
        ProjectSource::ConfigArgument => {
            let argument = url::form_urlencoded::parse(query?.as_bytes())
                .find(|(key, _)| key == "warehouse")?
                .1;
            let (project_id, _name) = parse_warehouse_arg(&argument);
            project_id.map(WarehouseReference::Project)
        }
        // Parsed exactly as the handlers parse it (`require_warehouse_id`).
        ProjectSource::WarehousePrefix => WarehouseId::from_str_or_bad_request(prefix?)
            .ok()
            .map(WarehouseReference::Warehouse),
        ProjectSource::Nothing => None,
    }
}

pub(crate) fn parse_warehouse_arg(arg: &str) -> (Option<ProjectId>, String) {
    // structure of the argument is <(optional project id)>/<warehouse_name>
    // Warehouse names cannot include /

    // Split arg at first /
    let parts: Vec<&str> = arg.splitn(2, '/').collect();
    match parts.len() {
        1 => {
            // No project_id provided
            let warehouse_name = parts[0].to_string();
            (None, warehouse_name)
        }
        2 => {
            // Maybe project_id and warehouse_id provided
            // If parts[0] is a valid project id, it is a project_id, otherwise the whole thing is a warehouse_id
            match ProjectId::from_str(parts[0]) {
                Ok(project_id) => {
                    let warehouse_name = parts[1].to_string();
                    (Some(project_id), warehouse_name)
                }
                Err(_) => (None, arg.to_string()),
            }
        }
        // Because of the splitn(2, ..) there can't be more than 2 parts
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "router")]
    use strum::IntoEnumIterator;

    use super::*;
    #[cfg(feature = "router")]
    use crate::api::endpoints::ManagementV1Endpoint;

    #[cfg(feature = "router")]
    const WAREHOUSE: &str = "01970000-0000-7000-8000-00000000000a";
    const PROJECT: &str = "01970000-0000-7000-8000-00000000000b";

    #[cfg(feature = "router")]
    fn warehouse() -> WarehouseReference {
        WarehouseReference::Warehouse(WarehouseId::from(uuid::Uuid::parse_str(WAREHOUSE).unwrap()))
    }

    #[cfg(feature = "router")]
    #[test]
    fn a_prefixed_catalog_route_addresses_its_warehouse() {
        for endpoint in [
            Endpoint::CatalogV1(CatalogV1Endpoint::ListNamespaces),
            Endpoint::CatalogV1(CatalogV1Endpoint::LoadTable),
            Endpoint::CatalogV1(CatalogV1Endpoint::ReportMetrics),
            Endpoint::Sign(SignEndpoint::S3RequestPrefix),
            Endpoint::Sign(SignEndpoint::S3RequestTabular),
            Endpoint::Sign(SignEndpoint::S3RequestByTableName),
            Endpoint::GenericTableV1(GenericTableV1Endpoint::ListGenericTables),
            Endpoint::DatasetV1(DatasetV1Endpoint::ListDatasets),
            Endpoint::DatasetV1(DatasetV1Endpoint::SignDatasetFiles),
            Endpoint::DatasetV1(DatasetV1Endpoint::DiffDataset),
            Endpoint::DatasetV1(DatasetV1Endpoint::RenameDataset),
        ] {
            assert_eq!(
                warehouse_reference(&endpoint.method(), endpoint.path(), Some(WAREHOUSE), None),
                Some(warehouse()),
                "{}",
                endpoint.as_http_route()
            );
        }
    }

    #[cfg(feature = "router")]
    #[test]
    fn config_addresses_the_project_its_argument_names() {
        let method = CatalogV1Endpoint::GetConfig.method();
        let path = CatalogV1Endpoint::GetConfig.path();
        assert_eq!(
            warehouse_reference(
                &method,
                path,
                None,
                Some(&format!("warehouse={PROJECT}%2Fmy-warehouse"))
            ),
            Some(WarehouseReference::Project(
                ProjectId::from_str(PROJECT).unwrap()
            ))
        );
        assert_eq!(
            warehouse_reference(
                &method,
                path,
                None,
                Some(&format!("warehouse={PROJECT}/my-warehouse"))
            ),
            Some(WarehouseReference::Project(
                ProjectId::from_str(PROJECT).unwrap()
            ))
        );
        // A project id is not required to look like a UUID.
        assert_eq!(
            warehouse_reference(
                &method,
                path,
                None,
                Some("warehouse=my-project%2Fmy-warehouse")
            ),
            Some(WarehouseReference::Project(
                ProjectId::from_str("my-project").unwrap()
            ))
        );
    }

    #[cfg(feature = "router")]
    #[test]
    fn config_without_a_project_in_its_argument_addresses_nothing() {
        let method = CatalogV1Endpoint::GetConfig.method();
        let path = CatalogV1Endpoint::GetConfig.path();
        for query in [
            "warehouse=my-warehouse",
            "warehouse=not.a.project/my-warehouse",
            "other=1",
        ] {
            assert_eq!(
                warehouse_reference(&method, path, None, Some(query)),
                None,
                "{query}"
            );
        }
        assert_eq!(warehouse_reference(&method, path, None, None), None);
    }

    #[cfg(feature = "router")]
    #[test]
    fn routes_that_address_no_warehouse_address_nothing() {
        for (endpoint, prefix) in [
            (
                Endpoint::ManagementV1(ManagementV1Endpoint::GetWarehouse),
                Some(WAREHOUSE),
            ),
            (
                Endpoint::ManagementV1(ManagementV1Endpoint::ListProjects),
                None,
            ),
            (Endpoint::Sign(SignEndpoint::S3RequestGlobal), None),
            (
                Endpoint::CatalogV1(CatalogV1Endpoint::ListNamespaces),
                Some("not-a-uuid"),
            ),
            (Endpoint::CatalogV1(CatalogV1Endpoint::ListNamespaces), None),
        ] {
            assert_eq!(
                warehouse_reference(&endpoint.method(), endpoint.path(), prefix, None),
                None,
                "{}",
                endpoint.as_http_route()
            );
        }

        // A matched path the registry does not know.
        assert_eq!(
            warehouse_reference(&Method::GET, "/not/a/real/route", None, None),
            None
        );
        // A known path with a method the registry does not have for it.
        assert_eq!(
            warehouse_reference(
                &Method::DELETE,
                CatalogV1Endpoint::GetConfig.path(),
                None,
                None
            ),
            None
        );
    }

    #[test]
    fn the_warehouse_argument_keeps_its_parse() {
        assert_eq!(
            parse_warehouse_arg(&format!("{PROJECT}/my-warehouse")),
            (
                Some(ProjectId::from_str(PROJECT).unwrap()),
                "my-warehouse".to_string()
            )
        );
        assert_eq!(
            parse_warehouse_arg("my-warehouse"),
            (None, "my-warehouse".to_string())
        );
        // A project id is not required to look like a UUID.
        assert_eq!(
            parse_warehouse_arg("my-project/my-warehouse"),
            (
                Some(ProjectId::from_str("my-project").unwrap()),
                "my-warehouse".to_string()
            )
        );
        assert_eq!(
            parse_warehouse_arg("not.a.project/my-warehouse"),
            (None, "not.a.project/my-warehouse".to_string())
        );
    }

    /// Every catalog, generic-table and signer endpoint whose path carries `{prefix}`
    /// must classify as `WarehousePrefix`, every one that doesn't must not — and
    /// `GetConfig` alone reads the query argument.
    #[cfg(feature = "router")]
    #[test]
    fn project_source_follows_the_prefix_and_only_get_config_reads_the_argument() {
        for endpoint in Endpoint::iter() {
            let source = project_source(endpoint);
            let has_prefix = endpoint.path().contains("{prefix}");
            let is_get_config =
                matches!(endpoint, Endpoint::CatalogV1(CatalogV1Endpoint::GetConfig));
            assert_eq!(
                source == ProjectSource::WarehousePrefix,
                has_prefix,
                "{} classified as {source:?}",
                endpoint.as_http_route()
            );
            assert_eq!(
                source == ProjectSource::ConfigArgument,
                is_get_config,
                "{} classified as {source:?}",
                endpoint.as_http_route()
            );
        }
    }
}
