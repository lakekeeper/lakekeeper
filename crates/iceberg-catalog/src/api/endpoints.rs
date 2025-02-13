use http::{Method, Uri};
use iceberg_ext::catalog::rest::ErrorModel;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::LazyLock;
use strum::{EnumString, IntoEnumIterator, VariantIterator, VariantMetadata, VariantNames};

static PATHS: LazyLock<Vec<String>> = LazyLock::new(|| {
    let mut paths = Vec::new();
    for endpoint in Endpoints::iter() {
        paths.push(endpoint.to_string());
    }
    paths
});

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    strum_macros::EnumString,
    strum_macros::EnumIter,
    strum_macros::VariantNames,
    strum::Display,
    strum::EnumDiscriminants,
)]
#[strum_discriminants(derive(strum::Display, strum::EnumIterAdditionalDerive))]
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
    // authz, for now all of these are openfga
    ManagementGetPermissionsRoleAccess,
    ManagementGetPermissionsServerAccess,
    ManagementGetPermissionsProjectAccess,
    ManagementGetPermissionsWarehouseAccess,
    ManagementGetPermissionsWarehouse,
    ManagementPostPermissionsWarehouseManagedAccess,
    ManagementGetPermissionsProjectIDAccess,
    ManagementGetPermissionsNamespaceAccess,
    ManagementGetPermissionsNamespace,
    ManagementPostPermissionsNamespaceManagedAccess,
    ManagementGetPermissionsTableAccess,
    ManagementGetPermissionsViewAccess,
    ManagementGetPermissionsRoleAssignments,
    ManagementPostPermissionsRoleAssignments,
    ManagementGetPermissionsServerAssignments,
    ManagementPostPermissionsServerAssignments,
    ManagementGetPermissionsProjectAssignments,
    ManagementPostPermissionsProjectAssignments,
    ManagementGetPermissionsProjectIDAssignments,
    ManagementPostPermissionsProjectIDAssignments,
    ManagementGetPermissionsWarehouseAssignments,
    ManagementPostPermissionsWarehouseAssignments,
    ManagementGetPermissionsNamespaceAssignments,
    ManagementPostPermissionsNamespaceAssignments,
    ManagementGetPermissionsTableAssignments,
    ManagementPostPermissionsTableAssignments,
    ManagementGetPermissionsViewAssignments,
    ManagementPostPermissionsViewAssignments,
    ManagementPostPermissionsCheck,
}

static MAP: LazyLock<HashMap<&str, Endpoints>> =
    LazyLock::new(|| Endpoints::iter().map(|e| (e.to_http_string(), e)).collect());

impl Endpoints {
    pub fn catalog() -> Vec<Self> {
        Endpoints::iter().filter(Self::is_catalog).collect()
    }

    pub fn is_catalog(&self) -> bool {
        self.to_string().starts_with("Catalog")
    }

    pub fn is_management(&self) -> bool {
        self.to_string().starts_with("Management")
    }

    pub fn from_http_string(inp: &str) -> Option<Self> {
        MAP.get(inp).copied()
    }

    pub fn to_http_string(&self) -> &'static str {
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
            Endpoints::CatalogPostViewsRename => "POST /v1/{prefix}/tables/rename",
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
            Endpoints::ManagementGetPermissionsRoleAccess => {
                "GET /v1/management/permissions/role/{role_id}/access"
            }
            Endpoints::ManagementGetPermissionsServerAccess => {
                "GET /v1/management/permissions/server/access"
            }
            Endpoints::ManagementGetPermissionsProjectAccess => {
                "GET /v1/management/permissions/project/access"
            }
            Endpoints::ManagementGetPermissionsWarehouseAccess => {
                "GET /v1/management/permissions/warehouse/{warehouse_id}/access"
            }
            Endpoints::ManagementGetPermissionsWarehouse => {
                "GET /v1/management/permissions/warehouse/{warehouse_id}"
            }
            Endpoints::ManagementPostPermissionsWarehouseManagedAccess => {
                "POST /v1/management/permissions/warehouse/{warehouse_id}/managed-access"
            }
            Endpoints::ManagementGetPermissionsProjectIDAccess => {
                "GET /v1/management/permissions/project/{project_id}/access"
            }
            Endpoints::ManagementGetPermissionsNamespaceAccess => {
                "GET /permissions/namespace/{namespace_id}/access"
            }
            Endpoints::ManagementGetPermissionsNamespace => {
                "GET /v1/management/permissions/namespace/{namespace_id}"
            }
            Endpoints::ManagementPostPermissionsNamespaceManagedAccess => {
                "POST /v1/management/permissions/namespace/{namespace_id}/managed-access"
            }
            Endpoints::ManagementGetPermissionsTableAccess => {
                "GET /v1/management/permissions/table/{table_id}/access"
            }
            Endpoints::ManagementGetPermissionsViewAccess => {
                "GET /v1/management/permissions/view/{table_id}/access"
            }
            Endpoints::ManagementGetPermissionsRoleAssignments => {
                "GET /v1/management/permissions/role/{role_id}/assignments"
            }
            Endpoints::ManagementPostPermissionsRoleAssignments => {
                "POST /v1/management/permissions/role/{role_id}/assignments"
            }
            Endpoints::ManagementGetPermissionsServerAssignments => {
                "GET /v1/management/permissions/server/assignments"
            }
            Endpoints::ManagementPostPermissionsServerAssignments => {
                "POST /v1/management/permissions/server/assignments"
            }
            Endpoints::ManagementGetPermissionsProjectAssignments => {
                "GET /v1/management/permissions/project/assignments"
            }
            Endpoints::ManagementPostPermissionsProjectAssignments => {
                "POST /v1/management/permissions/project/assignments"
            }
            Endpoints::ManagementGetPermissionsProjectIDAssignments => {
                "GET /v1/management/permissions/project/{project_id}/assignments"
            }
            Endpoints::ManagementPostPermissionsProjectIDAssignments => {
                "POST /v1/management/permissions/project/{project_id}/assignments"
            }
            Endpoints::ManagementGetPermissionsWarehouseAssignments => {
                "GET /v1/management/permissions/warehouse/{warehouse_id}/assignments"
            }
            Endpoints::ManagementPostPermissionsWarehouseAssignments => {
                "POST /v1/management/permissions/warehouse/{warehouse_id}/assignments"
            }
            Endpoints::ManagementGetPermissionsNamespaceAssignments => {
                "GET /v1/management/permissions/namespace/{namespace_id}/assignments"
            }
            Endpoints::ManagementPostPermissionsNamespaceAssignments => {
                "POST /v1/management/permissions/namespace/{namespace_id}/assignments"
            }
            Endpoints::ManagementGetPermissionsTableAssignments => {
                "GET /v1/management/permissions/table/{table_id}/assignments"
            }
            Endpoints::ManagementPostPermissionsTableAssignments => {
                "POST /v1/management/permissions/table/{table_id}/assignments"
            }
            Endpoints::ManagementGetPermissionsViewAssignments => {
                "GET /v1/management/permissions/view/{view_id}/assignments"
            }
            Endpoints::ManagementPostPermissionsViewAssignments => {
                "POST /v1/management/permissions/view/{view_id}/assignments"
            }
            Endpoints::ManagementPostPermissionsCheck => "POST /v1/management/permissions/check",
        }
    }
}
