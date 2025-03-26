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
    CatalogPostNamespace,
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
    // Management
    ManagementGetInfo,
    ManagementGetEndpointStatistics,
    ManagementPostBootstrap,
    ManagementPostRole,
    ManagementGetRole,
    ManagementPostRoleID,
    ManagementGetRoleID,
    ManagementDeleteRoleID,
    ManagementPostSearchRole,
    ManagementGetWhoami,
    ManagementPostSearchUser,
    ManagementPostUserID,
    ManagementGetUserID,
    ManagementDeleteUserID,
    ManagementPostUser,
    ManagementGetUser,
    ManagementPostProject,
    ManagementGetDefaultProject,
    ManagementDeleteDefaultProject,
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
            Endpoints::CatalogPostNamespace => "POST /catalog/v1/{prefix}/namespaces/{namespace}",
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
            Endpoints::ManagementGetInfo => "GET /management/v1/info",
            Endpoints::ManagementGetEndpointStatistics => "GET /management/v1/endpoint-statistics",
            Endpoints::ManagementPostBootstrap => "POST /management/v1/bootstrap",
            Endpoints::ManagementPostRole => "POST /management/v1/role",
            Endpoints::ManagementGetRole => "GET /management/v1/role",
            Endpoints::ManagementPostRoleID => "POST /management/v1/role/{id}",
            Endpoints::ManagementGetRoleID => "GET /management/v1/role/{id}",
            Endpoints::ManagementDeleteRoleID => "DELETE /management/v1/role/{id}",
            Endpoints::ManagementPostSearchRole => "POST /management/v1/search/role",
            Endpoints::ManagementGetWhoami => "GET /management/v1/whoami",
            Endpoints::ManagementPostSearchUser => "POST /management/v1/search/user",
            Endpoints::ManagementPostUserID => "POST /management/v1/user/{user_id}",
            Endpoints::ManagementGetUserID => "GET /management/v1/user/{user_id}",
            Endpoints::ManagementDeleteUserID => "DELETE /management/v1/user/{user_id}",
            Endpoints::ManagementPostUser => "POST /management/v1/user",
            Endpoints::ManagementGetUser => "GET /management/v1/user",
            Endpoints::ManagementPostProject => "POST /management/v1/project",
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
        }
    }
}

#[cfg(test)]
mod test {
    use itertools::Itertools;
    use strum::IntoEnumIterator;

    use crate::api::endpoints::Endpoints;

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
