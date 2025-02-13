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

static MAP: LazyLock<HashMap<&str, Endpoints>> = LazyLock::new(|| {
    Endpoints::iter()
        .filter(|e| {
            !matches!(
                e,
                Endpoints::ManagementGetPermissions
                    | Endpoints::ManagementPostPermissions
                    | Endpoints::ManagementHeadPermissions
                    | Endpoints::ManagementDeletePermissions
            )
        })
        .map(|e| (e.to_http_string(), e))
        .collect()
});

impl Endpoints {
    pub fn catalog() -> Vec<Self> {
        Endpoints::iter().filter(|e| Self::is_catalog(*e)).collect()
    }

    pub fn is_catalog(self) -> bool {
        self.to_string().starts_with("Catalog")
    }

    pub fn is_management(self) -> bool {
        self.to_string().starts_with("Management")
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
        if inp.starts_with("/v1/management/permissions") {
            return match method.as_str() {
                "GET" => Some(Endpoints::ManagementGetPermissions),
                "POST" => Some(Endpoints::ManagementPostPermissions),
                "HEAD" => Some(Endpoints::ManagementHeadPermissions),
                "DELETE" => Some(Endpoints::ManagementDeletePermissions),
                _ => None,
            };
        }
        MAP.get(inp).copied()
    }

    #[allow(clippy::too_many_lines)]
    pub fn to_http_string(self) -> &'static str {
        match self {
            Endpoints::CatalogPostAwsS3Sign => "POST /v1/catalog/aws/s3/sign",
            Endpoints::CatalogPostPrefixAwsS3Sign => "POST /v1/catalog/{prefix}/aws/s3/sign",
            Endpoints::CatalogGetConfig => "GET /v1/config",
            Endpoints::CatalogGetNamespaces => "GET /v1/{prefix}/namespaces",
            Endpoints::CatalogPostNamespaces => "POST /v1/{prefix}/namespaces",
            Endpoints::CatalogGetNamespace => "GET /v1/{prefix}/namespaces/{namespace}",
            Endpoints::CatalogPostNamespace => "POST /v1/{prefix}/namespaces/{namespace}",
            Endpoints::CatalogDeleteNamespace => "DELETE /v1/{prefix}/namespaces/{namespace}",
            Endpoints::CatalogPostNamespaceProperties => {
                "POST /v1/{prefix}/namespaces/{namespace}/properties"
            }
            Endpoints::CatalogGetNamespaceTables => {
                "GET /v1/{prefix}/namespaces/{namespace}/tables"
            }
            Endpoints::CatalogPostNamespaceTables => {
                "POST /v1/{prefix}/namespaces/{namespace}/tables"
            }
            Endpoints::CatalogGetNamespaceTable => {
                "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}"
            }
            Endpoints::CatalogPostNamespaceTable => {
                "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}"
            }
            Endpoints::CatalogDeleteNamespaceTable => {
                "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}"
            }
            Endpoints::CatalogHeadNamespaceTable => {
                "Head /v1/{prefix}/namespaces/{namespace}/tables/{table}"
            }
            Endpoints::CatalogGetNamespaceTableCredentials => {
                "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials"
            }
            Endpoints::CatalogPostTablesRename => "POST /v1/{prefix}/tables/rename",
            Endpoints::CatalogPostNamespaceRegister => {
                "POST /v1/{prefix}/namespaces/{namespace}/register"
            }
            Endpoints::CatalogPostNamespaceTableMetrics => {
                "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics"
            }
            Endpoints::CatalogPostTransactionsCommit => "POST /v1/{prefix}/transactions/commit",
            Endpoints::CatalogPostNamespaceViews => {
                "POST /v1/{prefix}/namespaces/{namespace}/views"
            }
            Endpoints::CatalogGetNamespaceViews => "Get /v1/{prefix}/namespaces/{namespace}/views",
            Endpoints::CatalogGetNamespaceView => {
                "GET /v1/{prefix}/namespaces/{namespace}/views/{view}"
            }
            Endpoints::CatalogPostNamespaceView => {
                "POST /v1/{prefix}/namespaces/{namespace}/views/{view}"
            }
            Endpoints::CatalogDeleteNamespaceView => {
                "DELETE /v1/{prefix}/namespaces/{namespace}/views/{view}"
            }
            Endpoints::CatalogHeadNamespaceView => {
                "HEAD /v1/{prefix}/namespaces/{namespace}/views/{view}"
            }
            Endpoints::CatalogPostViewsRename => "POST /v1/{prefix}/views/rename",
            Endpoints::ManagementGetInfo => "GET /v1/management/info",
            Endpoints::ManagementPostBootstrap => "POST /v1/management/bootstrap",
            Endpoints::ManagementPostRole => "POST /v1/management/role",
            Endpoints::ManagementGetRole => "GET /v1/management/role",
            Endpoints::ManagementPostRoleID => "POST /v1/management/role/{id}",
            Endpoints::ManagementGetRoleID => "GET /v1/management/role/{id}",
            Endpoints::ManagementDeleteRoleID => "DELETE /v1/management/role/{id}",
            Endpoints::ManagementPostSearchRole => "POST /v1/management/search/role",
            Endpoints::ManagementGetWhoami => "GET /v1/management/whoami",
            Endpoints::ManagementPostSearchUser => "POST /v1/management/search/user",
            Endpoints::ManagementPostUserID => "POST /v1/management/user/{user_id}",
            Endpoints::ManagementGetUserID => "GET /v1/management/user/{user_id}",
            Endpoints::ManagementDeleteUserID => "DELETE /v1/management/user/{user_id}",
            Endpoints::ManagementPostUser => "POST /v1/management/user",
            Endpoints::ManagementGetUser => "GET /v1/management/user",
            Endpoints::ManagementPostProject => "POST /v1/management/project",
            Endpoints::ManagementGetDefaultProject => "GET /v1/management/project",
            Endpoints::ManagementDeleteDefaultProject => "DELETE /v1/management/project",
            Endpoints::ManagementPostRenameProject => "POST /v1/management/project/rename",
            Endpoints::ManagementGetProjectID => "GET /v1/management/project/{project_id}",
            Endpoints::ManagementDeleteProjectID => "DELETE /v1/management/project/{project_id}",
            Endpoints::ManagementPostWarehouse => "POST /v1/management/warehouse",
            Endpoints::ManagementGetWarehouse => "GET /v1/management/warehouse",
            Endpoints::ManagementGetProjectList => "GET /v1/management/project-list",
            Endpoints::ManagementGetWarehouseID => "GET /v1/management/warehouse/{warehouse_id}",
            Endpoints::ManagementDeleteWarehouseID => {
                "DELETE /v1/management/warehouse/{warehouse_id}"
            }
            Endpoints::ManagementPostWarehouseRename => {
                "POST /v1/management/warehouse/{warehouse_id}/rename"
            }
            Endpoints::ManagementPostWarehouseDeactivate => {
                "POST /v1/management/warehouse/{warehouse_id}/deactivate"
            }
            Endpoints::ManagementPostWarehouseActivate => {
                "POST /v1/management/warehouse/{warehouse_id}/activate"
            }
            Endpoints::ManagementPostWarehouseStorage => {
                "POST /v1/management/warehouse/{warehouse_id}/storage"
            }
            Endpoints::ManagementPostWarehouseStorageCredential => {
                "POST /v1/management/warehouse/{warehouse_id}/storage-credential"
            }
            Endpoints::ManagementGetWarehouseStatistics => {
                "GET /v1/management/warehouse/{warehouse_id}/statistics"
            }
            Endpoints::ManagementGetWarehouseDeletedTabulars => {
                "GET /v1/management/warehouse/{warehouse_id}/deleted-tabulars"
            }
            Endpoints::ManagementPostWarehouseDeletedTabularsUndrop1 => {
                "POST /v1/management/warehouse/{warehouse_id}/deleted_tabulars/undrop"
            }
            Endpoints::ManagementPostWarehouseDeletedTabularsUndrop2 => {
                "POST /v1/management/warehouse/{warehouse_id}/deleted-tabulars/undrop"
            }
            Endpoints::ManagementPostWarehouseDeleteProfile => {
                "POST /v1/management/warehouse/{warehouse_id}/delete-profile"
            }

            Endpoints::ManagementGetPermissions => "GET /v1/management/permissions",
            Endpoints::ManagementPostPermissions => "POST /v1/management/permissions",
            Endpoints::ManagementHeadPermissions => "HEAD /v1/management/permissions",
            Endpoints::ManagementDeletePermissions => "DELETE /v1/management/permissions",
        }
    }
}
