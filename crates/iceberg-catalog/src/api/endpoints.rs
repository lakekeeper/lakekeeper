use std::{collections::HashMap, string::ToString, sync::LazyLock};

use http::Method;
use strum::IntoEnumIterator;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, strum_macros::EnumIter, strum::Display, sqlx::Type,
)]
#[strum(serialize_all = "kebab-case")]
#[sqlx(type_name = "api_endpoints", rename_all = "kebab-case")]
pub enum Endpoints {
    // Signer
    CatalogPostAwsS3Sign,
    CatalogPostPrefixAwsS3Sign,
    // Catalog
    CatalogGetConfig,
    CatalogGetNamespaces,
    CatalogPostNamespaces,
    CatalogGetNamespace,
    CatalogHeadNamespace,
    CatalogDeleteNamespace,
    CatalogPostNamespaceProperties,
    CatalogGetNamespaceTables,
    CatalogPostNamespaceTables,
    CatalogGetNamespaceTable,
    CatalogPostNamespaceTable,
    CatalogDeleteNamespaceTable,
    CatalogHeadNamespaceTable,
    CatalogGetNamespaceTableCredentials,
    CatalogPostTablesRename,
    CatalogPostNamespaceRegister,
    CatalogPostNamespaceTableMetrics,
    CatalogPostTransactionsCommit,
    CatalogPostNamespaceViews,
    CatalogGetNamespaceViews,
    CatalogGetNamespaceView,
    CatalogPostNamespaceView,
    CatalogDeleteNamespaceView,
    CatalogHeadNamespaceView,
    CatalogPostViewsRename,
    // Not yet implemented
    CatalogDeletePlan,
    CatalogGetPlanId,
    CatalogPostPlanId,
    CatalogPostTasks,
    // Management
    ManagementGetInfo,
    ManagementGetEndpointStatisticsDeprecated,
    ManagementPostBootstrap,
    ManagementPostRole,
    ManagementGetRole,
    ManagementPostRoleID,
    ManagementGetRoleID,
    ManagementDeleteRoleID,
    ManagementPostSearchRole,
    ManagementGetWhoami,
    ManagementPostSearchUser,
    ManagementGetUserID,
    ManagementDeleteUserID,
    ManagementPostUser,
    ManagementGetUser,
    ManagementPostProject,
    ManagementGetDefaultProjectDeprecated,
    ManagementGetDefaultProject,
    ManagementDeleteDefaultProjectDeprecated,
    ManagementDeleteDefaultProject,
    ManagementPostRenameProjectDeprecated,
    ManagementPostRenameProject,
    ManagementGetProjectID,
    ManagementDeleteProjectID,
    ManagementPostWarehouse,
    ManagementGetWarehouse,
    ManagementGetProjectList,
    ManagementGetWarehouseID,
    ManagementDeleteWarehouseID,
    ManagementPostWarehouseRename,
    ManagementPostWarehouseDeactivate,
    ManagementPostWarehouseActivate,
    ManagementPostWarehouseStorage,
    ManagementPostWarehouseStorageCredential,
    ManagementGetWarehouseStatistics,
    ManagementGetWarehouseDeletedTabulars,
    ManagementPostWarehouseDeletedTabularsUndrop1,
    ManagementPostWarehouseDeletedTabularsUndrop2,
    ManagementPostWarehouseDeleteProfile,
    ManagementPostWarehouseProtection,
    ManagementPostWarehouseNamespaceProtection,
    ManagementPostWarehouseTableProtection,
    ManagementPostWarehouseViewProtection,
    ManagementGetWarehouseNamespaceProtection,
    ManagementGetWarehouseTableProtection,
    ManagementGetWarehouseViewProtection,
    ManagementDeleteUserId,
    ManagementGetUserId,
    ManagementPutUserId,
    ManagementPostEndpointStatistics,
    ManagementPostProjectRenameId,
    // authz, we don't resolve single endpoints since every authorizer may have their own set
    ManagementGetPermissions,
    ManagementPostPermissions,
    ManagementHeadPermissions,
    ManagementDeletePermissions,
}

static ROUTE_MAP: LazyLock<HashMap<(Method, &'static str), Endpoints>> = LazyLock::new(|| {
    Endpoints::iter()
        .filter(|e| {
            !matches!(
                e,
                // see comment above in the endpoints enum, these are grouped endpoints due to them
                // potentially being different for every authorizer
                Endpoints::ManagementGetPermissions
                    | Endpoints::ManagementPostPermissions
                    | Endpoints::ManagementHeadPermissions
                    | Endpoints::ManagementDeletePermissions
            )
        })
        .map(|e| ((e.method(), e.path()), e))
        .collect()
});

impl Endpoints {
    pub fn catalog() -> Vec<Self> {
        Endpoints::iter().filter(|e| Self::is_catalog(*e)).collect()
    }

    pub fn is_catalog(self) -> bool {
        self.to_string().starts_with("catalog")
    }

    pub fn is_management(self) -> bool {
        self.to_string().starts_with("management")
    }

    pub fn is_real_endpoint(self) -> bool {
        !matches!(
            self,
            Endpoints::ManagementGetPermissions
                | Endpoints::ManagementPostPermissions
                | Endpoints::ManagementHeadPermissions
                | Endpoints::ManagementDeletePermissions
        )
    }

    pub fn is_grouped_endpoint(self) -> bool {
        !self.is_real_endpoint()
    }

    pub fn from_method_and_matched_path(method: &Method, inp: &str) -> Option<Self> {
        if inp.starts_with("/management/v1/permissions") {
            return match *method {
                Method::GET => Some(Endpoints::ManagementGetPermissions),
                Method::POST => Some(Endpoints::ManagementPostPermissions),
                Method::HEAD => Some(Endpoints::ManagementHeadPermissions),
                Method::DELETE => Some(Endpoints::ManagementDeletePermissions),
                _ => None,
            };
        }
        ROUTE_MAP
            .get(&(
                match method {
                    &Method::GET => Method::GET,
                    &Method::POST => Method::POST,
                    &Method::HEAD => Method::HEAD,
                    &Method::DELETE => Method::DELETE,
                    x => x.clone(),
                },
                inp,
            ))
            .copied()
    }

    pub fn method(self) -> Method {
        match self
            .as_http_route()
            .split_once(' ')
            .expect("as_http_route needs to contain a whitespace separated method and path")
            .0
        {
            "GET" => Method::GET,
            "POST" => Method::POST,
            "HEAD" => Method::HEAD,
            "DELETE" => Method::DELETE,
            "PUT" => Method::PUT,
            x => panic!("unsupported method: '{x}', if you add a route to the enum, you need to add the method to this match"),
        }
    }

    pub fn path(self) -> &'static str {
        self.as_http_route()
            .split_once(' ')
            .expect("as_http_route needs to contain a whitespace separated method and path")
            .1
    }

    #[allow(clippy::too_many_lines)]
    pub fn as_http_route(self) -> &'static str {
        match self {
            Endpoints::CatalogPostAwsS3Sign => "POST /catalog/v1/aws/s3/sign",
            Endpoints::CatalogPostPrefixAwsS3Sign => "POST /catalog/v1/{prefix}/v1/aws/s3/sign",
            Endpoints::CatalogGetConfig => "GET /catalog/v1/config",
            Endpoints::CatalogGetNamespaces => "GET /catalog/v1/{prefix}/namespaces",
            Endpoints::CatalogHeadNamespace => "HEAD /catalog/v1/{prefix}/namespaces/{namespace}",
            Endpoints::CatalogPostNamespaces => "POST /catalog/v1/{prefix}/namespaces",
            Endpoints::CatalogGetNamespace => "GET /catalog/v1/{prefix}/namespaces/{namespace}",
            Endpoints::CatalogDeleteNamespace => {
                "DELETE /catalog/v1/{prefix}/namespaces/{namespace}"
            }
            Endpoints::CatalogPostNamespaceProperties => {
                "POST /catalog/v1/{prefix}/namespaces/{namespace}/properties"
            }
            Endpoints::CatalogGetNamespaceTables => {
                "GET /catalog/v1/{prefix}/namespaces/{namespace}/tables"
            }
            Endpoints::CatalogPostNamespaceTables => {
                "POST /catalog/v1/{prefix}/namespaces/{namespace}/tables"
            }
            Endpoints::CatalogGetNamespaceTable => {
                "GET /catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}"
            }
            Endpoints::CatalogPostNamespaceTable => {
                "POST /catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}"
            }
            Endpoints::CatalogDeleteNamespaceTable => {
                "DELETE /catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}"
            }
            Endpoints::CatalogHeadNamespaceTable => {
                "HEAD /catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}"
            }
            Endpoints::CatalogGetNamespaceTableCredentials => {
                "GET /catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials"
            }
            Endpoints::CatalogPostTablesRename => "POST /catalog/v1/{prefix}/tables/rename",
            Endpoints::CatalogPostNamespaceRegister => {
                "POST /catalog/v1/{prefix}/namespaces/{namespace}/register"
            }
            Endpoints::CatalogPostNamespaceTableMetrics => {
                "POST /catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics"
            }
            Endpoints::CatalogPostTransactionsCommit => {
                "POST /catalog/v1/{prefix}/transactions/commit"
            }
            Endpoints::CatalogPostNamespaceViews => {
                "POST /catalog/v1/{prefix}/namespaces/{namespace}/views"
            }
            Endpoints::CatalogGetNamespaceViews => {
                "GET /catalog/v1/{prefix}/namespaces/{namespace}/views"
            }
            Endpoints::CatalogGetNamespaceView => {
                "GET /catalog/v1/{prefix}/namespaces/{namespace}/views/{view}"
            }
            Endpoints::CatalogPostNamespaceView => {
                "POST /catalog/v1/{prefix}/namespaces/{namespace}/views/{view}"
            }
            Endpoints::CatalogDeleteNamespaceView => {
                "DELETE /catalog/v1/{prefix}/namespaces/{namespace}/views/{view}"
            }
            Endpoints::CatalogHeadNamespaceView => {
                "HEAD /catalog/v1/{prefix}/namespaces/{namespace}/views/{view}"
            }
            Endpoints::CatalogPostViewsRename => "POST /catalog/v1/{prefix}/views/rename",
            Endpoints::CatalogDeletePlan => {
                "DELETE catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}"
            }
            Endpoints::CatalogGetPlanId => {
                "GET catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}"
            }
            Endpoints::CatalogPostPlanId => {
                "POST catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}/plan"
            }
            Endpoints::CatalogPostTasks => {
                "POST /catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks"
            }
            Endpoints::ManagementGetInfo => "GET /management/v1/info",
            Endpoints::ManagementGetEndpointStatisticsDeprecated => {
                "GET /management/v1/endpoint-statistics"
            }
            Endpoints::ManagementPostBootstrap => "POST /management/v1/bootstrap",
            Endpoints::ManagementPostRole => "POST /management/v1/role",
            Endpoints::ManagementGetRole => "GET /management/v1/role",
            Endpoints::ManagementPostRoleID => "POST /management/v1/role/{id}",
            Endpoints::ManagementGetRoleID => "GET /management/v1/role/{id}",
            Endpoints::ManagementDeleteRoleID => "DELETE /management/v1/role/{id}",
            Endpoints::ManagementPostSearchRole => "POST /management/v1/search/role",
            Endpoints::ManagementGetWhoami => "GET /management/v1/whoami",
            Endpoints::ManagementPostSearchUser => "POST /management/v1/search/user",
            Endpoints::ManagementGetUserID => "GET /management/v1/user/{user_id}",
            Endpoints::ManagementDeleteUserID => "DELETE /management/v1/user/{user_id}",
            Endpoints::ManagementPostUser => "POST /management/v1/user",
            Endpoints::ManagementGetUser => "GET /management/v1/user",
            Endpoints::ManagementPostProject => "POST /management/v1/project",
            Endpoints::ManagementGetDefaultProjectDeprecated => {
                "GET /management/v1/default-project"
            }
            Endpoints::ManagementDeleteDefaultProjectDeprecated => {
                "DELETE /management/v1/default-project"
            }
            Endpoints::ManagementPostRenameProjectDeprecated => {
                "POST /management/v1/default-project/rename"
            }
            Endpoints::ManagementGetDefaultProject => "GET /management/v1/project",
            Endpoints::ManagementDeleteDefaultProject => "DELETE /management/v1/project",
            Endpoints::ManagementPostRenameProject => "POST /management/v1/project/rename",
            Endpoints::ManagementGetProjectID => "GET /management/v1/project/{project_id}",
            Endpoints::ManagementDeleteProjectID => "DELETE /management/v1/project/{project_id}",
            Endpoints::ManagementPostWarehouse => "POST /management/v1/warehouse",
            Endpoints::ManagementGetWarehouse => "GET /management/v1/warehouse",
            Endpoints::ManagementGetProjectList => "GET /management/v1/project-list",
            Endpoints::ManagementGetWarehouseID => "GET /management/v1/warehouse/{warehouse_id}",
            Endpoints::ManagementDeleteWarehouseID => {
                "DELETE /management/v1/warehouse/{warehouse_id}"
            }
            Endpoints::ManagementPostWarehouseRename => {
                "POST /management/v1/warehouse/{warehouse_id}/rename"
            }
            Endpoints::ManagementPostWarehouseDeactivate => {
                "POST /management/v1/warehouse/{warehouse_id}/deactivate"
            }
            Endpoints::ManagementPostWarehouseActivate => {
                "POST /management/v1/warehouse/{warehouse_id}/activate"
            }
            Endpoints::ManagementPostWarehouseStorage => {
                "POST /management/v1/warehouse/{warehouse_id}/storage"
            }
            Endpoints::ManagementPostWarehouseStorageCredential => {
                "POST /management/v1/warehouse/{warehouse_id}/storage-credential"
            }
            Endpoints::ManagementGetWarehouseStatistics => {
                "GET /management/v1/warehouse/{warehouse_id}/statistics"
            }
            Endpoints::ManagementGetWarehouseDeletedTabulars => {
                "GET /management/v1/warehouse/{warehouse_id}/deleted-tabulars"
            }
            Endpoints::ManagementPostWarehouseDeletedTabularsUndrop1 => {
                "POST /management/v1/warehouse/{warehouse_id}/deleted_tabulars/undrop"
            }
            Endpoints::ManagementPostWarehouseDeletedTabularsUndrop2 => {
                "POST /management/v1/warehouse/{warehouse_id}/deleted-tabulars/undrop"
            }
            Endpoints::ManagementPostWarehouseDeleteProfile => {
                "POST /management/v1/warehouse/{warehouse_id}/delete-profile"
            }
            Endpoints::ManagementGetPermissions => "GET /management/v1/permissions",
            Endpoints::ManagementPostPermissions => "POST /management/v1/permissions",
            Endpoints::ManagementHeadPermissions => "HEAD /management/v1/permissions",
            Endpoints::ManagementDeletePermissions => "DELETE /management/v1/permissions",
            Endpoints::ManagementPostWarehouseProtection => {
                "POST /management/v1/warehouse/{warehouse_id}/protection"
            }
            Endpoints::ManagementPostWarehouseNamespaceProtection => {
                "POST /management/v1/warehouse/{warehouse_id}/namespace/{namespace_id}/protection"
            }
            Endpoints::ManagementPostWarehouseTableProtection => {
                "POST /management/v1/warehouse/{warehouse_id}/table/{table_id}/protection"
            }
            Endpoints::ManagementPostWarehouseViewProtection => {
                "POST /management/v1/warehouse/{warehouse_id}/view/{view_id}/protection"
            }
            Endpoints::ManagementGetWarehouseNamespaceProtection => {
                "GET /management/v1/warehouse/{warehouse_id}/namespace/{namespace_id}/protection"
            }
            Endpoints::ManagementGetWarehouseTableProtection => {
                "GET /management/v1/warehouse/{warehouse_id}/table/{table_id}/protection"
            }
            Endpoints::ManagementGetWarehouseViewProtection => {
                "GET /management/v1/warehouse/{warehouse_id}/view/{view_id}/protection"
            }
            Endpoints::ManagementDeleteUserId => "DELETE /management/v1/user/{user_id}",
            Endpoints::ManagementGetUserId => "GET /management/v1/user/{user_id}",
            Endpoints::ManagementPutUserId => "PUT /management/v1/user/{user_id}",
            Endpoints::ManagementPostEndpointStatistics => {
                "POST /management/v1/endpoint-statistics"
            }
            Endpoints::ManagementPostProjectRenameId => {
                "POST /management/v1/project/{project_id}/rename"
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::api::endpoints::Endpoints;
    use itertools::Itertools;
    use strum::IntoEnumIterator;

    #[test]
    fn test_all_management_endpoints_present() {
        use crate::api::endpoints::Endpoints;
        use itertools::Itertools;
        use serde_yaml::Value;
        use std::collections::HashSet;
        use strum::IntoEnumIterator;

        // Load YAML files
        let management_yaml = include_str!("../../../../docs/docs/api/management-open-api.yaml");
        let catalog_yaml = include_str!("../../../../docs/docs/api/rest-catalog-open-api.yaml");

        // Parse YAML files
        let management: Value =
            serde_yaml::from_str(management_yaml).expect("Failed to parse management YAML");
        let catalog: Value =
            serde_yaml::from_str(catalog_yaml).expect("Failed to parse catalog YAML");

        // Extract endpoints from management YAML
        let mut expected_endpoints = HashSet::new();

        // Process management YAML paths
        if let Value::Mapping(paths) = &management["paths"] {
            for (path, methods) in paths {
                let path_str = path.as_str().expect("Path is not a string");
                if let Value::Mapping(methods_map) = methods {
                    for (method, _) in methods_map {
                        let method_str = method.as_str().expect("Method is not a string");
                        // Skip parameters entry which isn't an HTTP method
                        if method_str != "parameters" {
                            let normalized_path = path_str.trim_start_matches('/');
                            expected_endpoints
                                .insert((method_str.to_uppercase(), normalized_path.to_string()));
                        }
                    }
                }
            }
        }

        // Process catalog YAML paths
        if let Value::Mapping(paths) = &catalog["paths"] {
            for (path, methods) in paths {
                let path_str = path.as_str().expect("Path is not a string");
                if let Value::Mapping(methods_map) = methods {
                    for (method, _) in methods_map {
                        let method_str = method.as_str().expect("Method is not a string");
                        // Skip parameters entry which isn't an HTTP method
                        if method_str != "parameters" {
                            let normalized_path = format!("catalog{}", path_str);
                            expected_endpoints.insert((method_str.to_uppercase(), normalized_path));
                        }
                    }
                }
            }
        }

        // Extract endpoints from Endpoints enum
        let mut actual_endpoints = HashSet::new();
        for endpoint in Endpoints::iter() {
            if endpoint.is_grouped_endpoint() {
                // Skip grouped endpoints as they're handled specially
                continue;
            }

            let http_route = endpoint.as_http_route();
            let (method, path) = http_route
                .split_once(' ')
                .expect("HTTP route should have a space separating method and path");

            // Remove leading "/" to match normalized paths from YAML
            let normalized_path = path.trim_start_matches('/');
            actual_endpoints.insert((method.to_string(), normalized_path.to_string()));
        }

        // Find missing endpoints
        let missing_endpoints: Vec<_> = expected_endpoints.difference(&actual_endpoints).collect();

        let missing_endpoints = missing_endpoints
            .iter()
            // Remove all endpoints that start with /management/v1/permissions/ from the missing endpoints.
            // Those are handled separately by the middleware as they are provided by the authorizer.
            .filter(|(_method, path)| !path.starts_with("management/v1/permissions"))
            // Remove deprecated oauth endpoints
            .filter(|(_method, path)| !path.starts_with("catalog/v1/oauth/tokens"))
            .collect::<Vec<_>>();

        if !missing_endpoints.is_empty() {
            let missing_formatted = missing_endpoints
                .iter()
                .sorted()
                .map(|(method, path)| format!("{} /{}", method, path))
                .join("\n");

            panic!("The following endpoints are in the OpenAPI YAML but missing from the Endpoints enum:\n{}", missing_formatted);
        }

        // Find extra endpoints
        let extra_endpoints: Vec<_> = actual_endpoints.difference(&expected_endpoints).collect();
        let extra_endpoints = extra_endpoints
            .iter()
            // Remove all endpoints that start with /management/v1/permissions/ from the extra endpoints.
            // Those are handled separately by the middleware as they are provided by the authorizer.
            .filter(|(_method, path)| !path.starts_with("management/v1/permissions"))
            // S3 sign endpoints are not in the OpenAPI YAML
            .filter(|(_method, path)| !path.starts_with("catalog/v1/aws/s3/sign"))
            // Filter deprecated /management/v1/default-project endpoints
            .filter(|(_method, path)| !path.starts_with("management/v1/default-project"))
            // Filter deprecated get /management/v1/endpoint-statistics endpoints
            .filter(|(method, path)| {
                !(path.starts_with("management/v1/endpoint-statistics") && *method == "GET")
            })
            // Signing endpoints are not in the OpenAPI YAML
            .filter(|(_method, path)| !(path.starts_with("catalog/v1/{prefix}/v1/aws/s3/sign")))
            .collect::<Vec<_>>();
        if !extra_endpoints.is_empty() {
            let extra_formatted = extra_endpoints
                .iter()
                .sorted()
                .map(|(method, path)| format!("{} /{}", method, path))
                .join("\n");

            panic!("The following endpoints are in the Endpoints enum but missing from the OpenAPI YAML:\n{}", extra_formatted);
        }
    }

    #[test]
    fn test_catalog_is_not_empty() {
        assert!(!Endpoints::catalog().is_empty());
    }

    #[test]
    fn test_can_get_all_paths() {
        let _ = Endpoints::iter().map(Endpoints::path).collect_vec();
    }

    #[test]
    fn test_can_get_all_methods() {
        let _ = Endpoints::iter().map(Endpoints::method).collect_vec();
    }

    #[test]
    fn test_can_resolve_all_tuples() {
        let paths = Endpoints::iter().map(Endpoints::path).collect_vec();
        let methods = Endpoints::iter().map(Endpoints::method).collect_vec();
        for (method, path) in methods.iter().zip(paths.into_iter()) {
            let endpoint = Endpoints::from_method_and_matched_path(method, path);
            assert_eq!(
                endpoint.unwrap().as_http_route(),
                format!("{method} {path}")
            );
        }
    }
}
