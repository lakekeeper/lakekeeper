use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{LazyLock, OnceLock},
};

use http::Method;
use strum::IntoEnumIterator;

#[cfg(feature = "router")]
use crate::api::management::v1::extension::ManagementApiExtensionError;

macro_rules! generate_endpoints {
    (
        $(
            enum $enum_name:ident {
                $(
                    $variant:ident($method:ident, $path:expr)
                ),* $(,)?
            }
        )*
    ) => {
        $(
            pastey::paste! {
                #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum_macros::EnumIter)]
                #[allow(clippy::enum_variant_names)]
                pub enum [<$enum_name Endpoint>] {
                    $($variant),*
                }

                impl [<$enum_name Endpoint>] {
                    pub fn as_http_route(self) -> &'static str {
                        match self {
                            $([<$enum_name Endpoint>]::$variant => concat!(stringify!($method), " ", $path)),*
                        }
                    }

                    pub fn method(self) -> http::Method {
                        match self {
                            $([<$enum_name Endpoint>]::$variant => http::Method::$method),*
                        }
                    }

                    pub fn path(self) -> &'static str {
                        match self {
                            $([<$enum_name Endpoint>]::$variant => $path),*
                        }
                    }
                }
            }
        )*

        pastey::paste! {
            /// Every endpoint under its stored name (see [`EndpointFlat::name`]).
            #[derive(
                Debug,
                Clone,
                Copy,
                PartialEq,
                Eq,
                Hash,
                strum_macros::EnumIter,
                strum::Display,
                strum::IntoStaticStr,
                strum::EnumString,
            )]
            #[strum(serialize_all = "kebab-case")]
            pub enum EndpointFlat {
                $(
                    $(
                        [<$enum_name $variant>],
                    )*
                )*
            }

            impl From<EndpointFlat> for Endpoint {
                fn from(endpoint: EndpointFlat) -> Self {
                    match endpoint {
                        $(
                            $(
                                EndpointFlat::[<$enum_name $variant>] => Endpoint::$enum_name([<$enum_name Endpoint>]::$variant),
                            )*
                        )*
                    }
                }
            }

            impl From<Endpoint> for EndpointFlat {
                fn from(endpoint: Endpoint) -> Self {
                    match endpoint {
                        $(
                            $(
                                Endpoint::$enum_name([<$enum_name Endpoint>]::$variant) => EndpointFlat::[<$enum_name $variant>],
                            )*
                        )*
                    }
                }
            }

            #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, derive_more::From)]
            pub enum Endpoint {
                $($enum_name([<$enum_name Endpoint>])),*
            }

            impl strum::IntoEnumIterator for Endpoint {
                type Iterator = std::vec::IntoIter<Self>;

                fn iter() -> Self::Iterator {
                    // Chain iterators from all inner enums
                    [
                        $([<$enum_name Endpoint>]::iter().map(Endpoint::$enum_name).collect::<Vec<_>>()),*
                    ]
                    .concat()
                    .into_iter()
                }
            }

            impl Endpoint {
                pub fn as_http_route(self) -> &'static str {
                    match self {
                        $(Endpoint::$enum_name(e) => e.as_http_route()),*
                    }
                }

                pub fn method(self) -> http::Method {
                    match self {
                        $(Endpoint::$enum_name(e) => e.method()),*
                    }
                }

                pub fn path(self) -> &'static str {
                    match self {
                        $(Endpoint::$enum_name(e) => e.path()),*
                    }
                }
            }
        }
    };
}

impl CatalogV1Endpoint {
    #[must_use]
    pub fn unimplemented(self) -> bool {
        matches!(
            self,
            CatalogV1Endpoint::PlanTableScan
                | CatalogV1Endpoint::FetchPlanningResult
                | CatalogV1Endpoint::CancelPlanning
                | CatalogV1Endpoint::FetchScanTasks
        )
    }
}

// A literal segment beside a path parameter is resolved in the literal's favour, with
// no warning. That is only safe where the parameter is UUID-typed and so can never
// equal the literal — never beside a name-typed parameter (`{project_id}`, `{user_id}`,
// `{tag_name}`, `{queue_name}`, `{namespace}`, `{table}`, `{view}`), where it would make
// resources with that name unaddressable. Use a `by-id/` style disambiguator or a query
// parameter instead. The catalog `{prefix}` is safe only because it must parse as a
// warehouse id.
//
// `/management/v1/project/...` is the standing exception: `grants`, `actions`, `rename`
// and the rest sit beside the deprecated `/project/{project_id}` routes and shadow a
// project literally named for one of them. Those routes are superseded by the
// `x-project-id` header and are not extended; new project-scoped endpoints go under the
// header-addressed form, which has no parameter to shadow.
generate_endpoints! {
    enum CatalogV1 {
        GetConfig(GET, "/catalog/v1/config"),
        ListNamespaces(GET, "/catalog/v1/{prefix}/namespaces"),
        NamespaceExists(HEAD, "/catalog/v1/{prefix}/namespaces/{namespace}"),
        CreateNamespace(POST, "/catalog/v1/{prefix}/namespaces"),
        LoadNamespaceMetadata(GET, "/catalog/v1/{prefix}/namespaces/{namespace}"),
        DropNamespace(DELETE, "/catalog/v1/{prefix}/namespaces/{namespace}"),
        UpdateNamespaceProperties(POST, "/catalog/v1/{prefix}/namespaces/{namespace}/properties"),
        ListTables(GET, "/catalog/v1/{prefix}/namespaces/{namespace}/tables"),
        CreateTable(POST, "/catalog/v1/{prefix}/namespaces/{namespace}/tables"),
        LoadTable(GET, "/catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}"),
        UpdateTable(POST, "/catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}"),
        DropTable(DELETE, "/catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}"),
        TableExists(HEAD, "/catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}"),
        LoadCredentials(GET, "/catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials"),
        RenameTable(POST, "/catalog/v1/{prefix}/tables/rename"),
        RegisterTable(POST, "/catalog/v1/{prefix}/namespaces/{namespace}/register"),
        ReportMetrics(POST, "/catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics"),
        CommitTransaction(POST, "/catalog/v1/{prefix}/transactions/commit"),
        CreateView(POST, "/catalog/v1/{prefix}/namespaces/{namespace}/views"),
        ListViews(GET, "/catalog/v1/{prefix}/namespaces/{namespace}/views"),
        LoadView(GET, "/catalog/v1/{prefix}/namespaces/{namespace}/views/{view}"),
        ReplaceView(POST, "/catalog/v1/{prefix}/namespaces/{namespace}/views/{view}"),
        DropView(DELETE, "/catalog/v1/{prefix}/namespaces/{namespace}/views/{view}"),
        ViewExists(HEAD, "/catalog/v1/{prefix}/namespaces/{namespace}/views/{view}"),
        RenameView(POST, "/catalog/v1/{prefix}/views/rename"),
        CancelPlanning(DELETE, "/catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}"),
        FetchPlanningResult(GET, "/catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}"),
        PlanTableScan(POST, "/catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}/plan"),
        FetchScanTasks(POST, "/catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks"),
    }

    enum GenericTableV1 {
        CreateGenericTable(POST, "/lakekeeper/v1/{prefix}/namespaces/{namespace}/generic-tables"),
        ListGenericTables(GET, "/lakekeeper/v1/{prefix}/namespaces/{namespace}/generic-tables"),
        LoadGenericTable(GET, "/lakekeeper/v1/{prefix}/namespaces/{namespace}/generic-tables/{table}"),
        DropGenericTable(DELETE, "/lakekeeper/v1/{prefix}/namespaces/{namespace}/generic-tables/{table}"),
        RenameGenericTable(POST, "/lakekeeper/v1/{prefix}/generic-tables/rename"),
        LoadGenericTableCredentials(GET, "/lakekeeper/v1/{prefix}/namespaces/{namespace}/generic-tables/{table}/credentials"),
    }

    enum Sign {
        S3RequestGlobal(POST, "/catalog/v1/aws/s3/sign"),
        S3RequestPrefix(POST, "/catalog/v1/{prefix}/v1/aws/s3/sign"),
        S3RequestTabular(POST, "/catalog/v1/signer/{prefix}/tabular-id/{tabular_id}/v1/aws/s3/sign"),
        S3RequestByTableName(POST, "/catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}/sign"),
    }

    enum ManagementV1 {
        ServerInfo(GET, "/management/v1/info"),
        GetServerActions(GET, "/management/v1/server/actions"),
        Bootstrap(POST, "/management/v1/bootstrap"),
        SetMaintenanceMode(POST, "/management/v1/server/maintenance-mode"),
        CreateUser(POST, "/management/v1/user"),
        SearchUser(POST, "/management/v1/search/user"),
        GetUser(GET, "/management/v1/user/{user_id}"),
        Whoami(GET, "/management/v1/whoami"),
        UpdateUser(PUT, "/management/v1/user/{user_id}"),
        ListUser(GET, "/management/v1/user"),
        DeleteUser(DELETE, "/management/v1/user/{user_id}"),
        GetUserActions(GET, "/management/v1/user/{user_id}/actions"),
        CreateRole(POST, "/management/v1/role"),
        SearchRole(POST, "/management/v1/search/role"),
        ListRole(GET, "/management/v1/role"),
        DeleteRole(DELETE, "/management/v1/role/{role_id}"),
        GetRole(GET, "/management/v1/role/{role_id}"),
        UpdateRole(POST, "/management/v1/role/{role_id}"),
        UpdateRoleSourceSystem(PUT, "/management/v1/role/{role_id}/source-system"),
        GetRoleActions(GET, "/management/v1/role/{role_id}/actions"),
        GetRoleMetadata(GET, "/management/v1/role/{role_id}/metadata"),
        ListRoleMembers(GET, "/management/v1/role/{role_id}/members"),
        AddRoleMembers(POST, "/management/v1/role/{role_id}/members"),
        RemoveRoleMember(DELETE, "/management/v1/role/{role_id}/members/{member_type}/{member_id}"),
        ListRoleMemberOf(GET, "/management/v1/role/{role_id}/member-of"),
        ListUserRoles(GET, "/management/v1/user/{user_id}/roles"),
        ListRoleTransitiveMembers(GET, "/management/v1/role/{role_id}/members/transitive"),
        ListUserTransitiveRoles(GET, "/management/v1/user/{user_id}/roles/transitive"),
        ListRoleTransitiveMemberOf(GET, "/management/v1/role/{role_id}/member-of/transitive"),
        CreateTagDefinition(POST, "/management/v1/tag-definition"),
        ListTagDefinitions(GET, "/management/v1/tag-definition"),
        GetTagDefinition(GET, "/management/v1/tag-definition/{tag_definition_id}"),
        UpdateTagDefinition(POST, "/management/v1/tag-definition/{tag_definition_id}"),
        DeleteTagDefinition(DELETE, "/management/v1/tag-definition/{tag_definition_id}"),
        ListTagAttachments(GET, "/management/v1/tag-definition/{tag_definition_id}/attachments"),
        GetTagActions(GET, "/management/v1/tag-definition/{tag_definition_id}/actions"),
        SetWarehouseTag(PUT, "/management/v1/warehouse/{warehouse_id}/tags/{tag_name}"),
        DeleteWarehouseTag(DELETE, "/management/v1/warehouse/{warehouse_id}/tags/{tag_name}"),
        ListWarehouseTags(GET, "/management/v1/warehouse/{warehouse_id}/tags"),
        SetNamespaceTag(PUT, "/management/v1/warehouse/{warehouse_id}/namespace/{namespace_id}/tags/{tag_name}"),
        DeleteNamespaceTag(DELETE, "/management/v1/warehouse/{warehouse_id}/namespace/{namespace_id}/tags/{tag_name}"),
        ListNamespaceTags(GET, "/management/v1/warehouse/{warehouse_id}/namespace/{namespace_id}/tags"),
        SetTableTag(PUT, "/management/v1/warehouse/{warehouse_id}/table/{table_id}/tags/{tag_name}"),
        DeleteTableTag(DELETE, "/management/v1/warehouse/{warehouse_id}/table/{table_id}/tags/{tag_name}"),
        ListTableTags(GET, "/management/v1/warehouse/{warehouse_id}/table/{table_id}/tags"),
        SetTableColumnTag(PUT, "/management/v1/warehouse/{warehouse_id}/table/{table_id}/column/{column_name}/tags/{tag_name}"),
        DeleteTableColumnTag(DELETE, "/management/v1/warehouse/{warehouse_id}/table/{table_id}/column/{column_name}/tags/{tag_name}"),
        ListTableColumnTags(GET, "/management/v1/warehouse/{warehouse_id}/table/{table_id}/column/{column_name}/tags"),
        ListColumnTags(GET, "/management/v1/warehouse/{warehouse_id}/table/{table_id}/column-tags"),
        SetViewTag(PUT, "/management/v1/warehouse/{warehouse_id}/view/{view_id}/tags/{tag_name}"),
        DeleteViewTag(DELETE, "/management/v1/warehouse/{warehouse_id}/view/{view_id}/tags/{tag_name}"),
        ListViewTags(GET, "/management/v1/warehouse/{warehouse_id}/view/{view_id}/tags"),
        SetGenericTableTag(PUT, "/management/v1/warehouse/{warehouse_id}/generic-table/{generic_table_id}/tags/{tag_name}"),
        DeleteGenericTableTag(DELETE, "/management/v1/warehouse/{warehouse_id}/generic-table/{generic_table_id}/tags/{tag_name}"),
        ListGenericTableTags(GET, "/management/v1/warehouse/{warehouse_id}/generic-table/{generic_table_id}/tags"),
        // Two endpoints per resource level, plus the project-wide listing and the
        // vocabulary. This enum is checked against the generated OpenAPI, so a
        // declaration without a route fails the build.
        ListGrants(GET, "/management/v1/grants"),
        GetGrantablePrivileges(GET, "/management/v1/grants/grantable-privileges"),
        ListServerGrants(GET, "/management/v1/server/grants"),
        ApplyServerGrants(POST, "/management/v1/server/grants"),
        ListProjectGrants(GET, "/management/v1/project/grants"),
        ApplyProjectGrants(POST, "/management/v1/project/grants"),
        ListWarehouseGrants(GET, "/management/v1/warehouse/{warehouse_id}/grants"),
        ApplyWarehouseGrants(POST, "/management/v1/warehouse/{warehouse_id}/grants"),
        ListNamespaceGrants(GET, "/management/v1/warehouse/{warehouse_id}/namespace/{namespace_id}/grants"),
        ApplyNamespaceGrants(POST, "/management/v1/warehouse/{warehouse_id}/namespace/{namespace_id}/grants"),
        ListTagGrants(GET, "/management/v1/tag-definition/{tag_definition_id}/grants"),
        ApplyTagGrants(POST, "/management/v1/tag-definition/{tag_definition_id}/grants"),
        ListTableGrants(GET, "/management/v1/warehouse/{warehouse_id}/table/{table_id}/grants"),
        ApplyTableGrants(POST, "/management/v1/warehouse/{warehouse_id}/table/{table_id}/grants"),
        ListViewGrants(GET, "/management/v1/warehouse/{warehouse_id}/view/{view_id}/grants"),
        ApplyViewGrants(POST, "/management/v1/warehouse/{warehouse_id}/view/{view_id}/grants"),
        ListGenericTableGrants(GET, "/management/v1/warehouse/{warehouse_id}/generic-table/{generic_table_id}/grants"),
        ApplyGenericTableGrants(POST, "/management/v1/warehouse/{warehouse_id}/generic-table/{generic_table_id}/grants"),
        // Reading and clearing a whole subtree at once. Separate routes rather than a
        // recursive flag on the listings above: those are gated on the level's grant-read
        // action, which is satisfiable directly and does not inherit, so a flag would
        // widen an existing gate's reach to every descendant.
        ListNamespaceSubtreeGrants(GET, "/management/v1/warehouse/{warehouse_id}/namespace/{namespace_id}/grants/subtree"),
        RevokeNamespaceSubtreeGrants(POST, "/management/v1/warehouse/{warehouse_id}/namespace/{namespace_id}/grants/subtree/revoke"),
        ListWarehouseSubtreeGrants(GET, "/management/v1/warehouse/{warehouse_id}/grants/subtree"),
        RevokeWarehouseSubtreeGrants(POST, "/management/v1/warehouse/{warehouse_id}/grants/subtree/revoke"),
        // "Which privileges may I grant here" — one per level, same final segment as the
        // deployment-wide vocabulary above: the prefix carries the scope.
        GetServerGrantablePrivileges(GET, "/management/v1/server/grants/grantable-privileges"),
        GetProjectGrantablePrivileges(GET, "/management/v1/project/grants/grantable-privileges"),
        GetWarehouseGrantablePrivileges(GET, "/management/v1/warehouse/{warehouse_id}/grants/grantable-privileges"),
        GetNamespaceGrantablePrivileges(GET, "/management/v1/warehouse/{warehouse_id}/namespace/{namespace_id}/grants/grantable-privileges"),
        GetTableGrantablePrivileges(GET, "/management/v1/warehouse/{warehouse_id}/table/{table_id}/grants/grantable-privileges"),
        GetViewGrantablePrivileges(GET, "/management/v1/warehouse/{warehouse_id}/view/{view_id}/grants/grantable-privileges"),
        GetGenericTableGrantablePrivileges(GET, "/management/v1/warehouse/{warehouse_id}/generic-table/{generic_table_id}/grants/grantable-privileges"),
        GetTagGrantablePrivileges(GET, "/management/v1/tag-definition/{tag_definition_id}/grants/grantable-privileges"),
        CreateWarehouse(POST, "/management/v1/warehouse"),
        ValidateWarehouse(POST, "/management/v1/warehouse-creation-validation"),
        ListProjects(GET, "/management/v1/project-list"),
        CreateProject(POST, "/management/v1/project"),
        GetProject(GET, "/management/v1/project"),
        DeleteProject(DELETE, "/management/v1/project"),
        RenameProject(POST, "/management/v1/project/rename"),
        GetProjectActions(GET, "/management/v1/project/actions"),
        ListWarehouses(GET, "/management/v1/warehouse"),
        GetWarehouse(GET, "/management/v1/warehouse/{warehouse_id}"),
        GetWarehouseActions(GET, "/management/v1/warehouse/{warehouse_id}/actions"),
        DeleteWarehouse(DELETE, "/management/v1/warehouse/{warehouse_id}"),
        RenameWarehouse(POST, "/management/v1/warehouse/{warehouse_id}/rename"),
        UpdateWarehouseDeleteProfile(POST, "/management/v1/warehouse/{warehouse_id}/delete-profile"),
        UpdateWarehouseFormatVersionPolicy(POST, "/management/v1/warehouse/{warehouse_id}/format-version-policy"),
        DeactivateWarehouse(POST, "/management/v1/warehouse/{warehouse_id}/deactivate"),
        ActivateWarehouse(POST, "/management/v1/warehouse/{warehouse_id}/activate"),
        UpdateStorageProfile(POST, "/management/v1/warehouse/{warehouse_id}/storage"),
        UpdateStorageCredential(POST, "/management/v1/warehouse/{warehouse_id}/storage-credential"),
        ValidateStorageProfile(POST, "/management/v1/warehouse/{warehouse_id}/storage/validate-profile"),
        ValidateStorageCredential(POST, "/management/v1/warehouse/{warehouse_id}/storage/validate-credential"),
        ValidateStorageAccess(POST, "/management/v1/warehouse/{warehouse_id}/storage/validate-access"),
        GetWarehouseStatistics(GET, "/management/v1/warehouse/{warehouse_id}/statistics"),
        LoadEndpointStatistics(POST, "/management/v1/endpoint-statistics"),
        SearchTabular(POST, "/management/v1/warehouse/{warehouse_id}/search-tabular"),
        ListDeletedTabulars(GET, "/management/v1/warehouse/{warehouse_id}/deleted-tabulars"),
        UndropTabulars(POST, "/management/v1/warehouse/{warehouse_id}/deleted-tabulars/undrop"),
        GetTableProtection(GET, "/management/v1/warehouse/{warehouse_id}/table/{table_id}/protection"),
        SetTableProtection(POST, "/management/v1/warehouse/{warehouse_id}/table/{table_id}/protection"),
        GetTableActions(GET, "/management/v1/warehouse/{warehouse_id}/table/{table_id}/actions"),
        GetViewProtection(GET, "/management/v1/warehouse/{warehouse_id}/view/{view_id}/protection"),
        SetViewProtection(POST, "/management/v1/warehouse/{warehouse_id}/view/{view_id}/protection"),
        GetViewActions(GET, "/management/v1/warehouse/{warehouse_id}/view/{view_id}/actions"),
        GetGenericTableActions(GET, "/management/v1/warehouse/{warehouse_id}/generic-table/{generic_table_id}/actions"),
        GetGenericTableProtection(GET, "/management/v1/warehouse/{warehouse_id}/generic-table/{generic_table_id}/protection"),
        SetGenericTableProtection(POST, "/management/v1/warehouse/{warehouse_id}/generic-table/{generic_table_id}/protection"),
        SetNamespaceProtection(POST, "/management/v1/warehouse/{warehouse_id}/namespace/{namespace_id}/protection"),
        GetNamespaceProtection(GET, "/management/v1/warehouse/{warehouse_id}/namespace/{namespace_id}/protection"),
        GetNamespaceActions(GET, "/management/v1/warehouse/{warehouse_id}/namespace/{namespace_id}/actions"),
        MoveNamespace(POST, "/management/v1/warehouse/{warehouse_id}/namespace/{namespace_id}/move"),
        SetWarehouseProtection(POST, "/management/v1/warehouse/{warehouse_id}/protection"),
        SetWarehouseManagedBy(POST, "/management/v1/warehouse/{warehouse_id}/managed-by"),
        SetTaskQueueConfig(POST, "/management/v1/warehouse/{warehouse_id}/task-queue/{queue_name}/config"),
        GetTaskQueueConfig(GET, "/management/v1/warehouse/{warehouse_id}/task-queue/{queue_name}/config"),
        ScheduleTask(POST, "/management/v1/warehouse/{warehouse_id}/task-queue/{queue_name}/schedule"),
        ListTasks(POST, "/management/v1/warehouse/{warehouse_id}/task/list"),
        GetTaskDetails(GET, "/management/v1/warehouse/{warehouse_id}/task/by-id/{task_id}"),
        ControlTasks(POST, "/management/v1/warehouse/{warehouse_id}/task/control"),
        SetProjectTaskQueueConfig(POST, "/management/v1/project/task-queue/{queue_name}/config"),
        GetProjectTaskQueueConfig(GET, "/management/v1/project/task-queue/{queue_name}/config"),
        ListProjectTasks(POST, "/management/v1/project/task/list"),
        GetProjectTaskDetails(GET, "/management/v1/project/task/by-id/{task_id}"),
        ControlProjectTasks(POST, "/management/v1/project/task/control"),
        BatchCheckActions(POST, "/management/v1/action/batch-check"),
        // --------- Deprecated endpoints ---------
        GetDefaultProjectDeprecated(GET, "/management/v1/default-project"),
        DeleteDefaultProjectDeprecated(DELETE, "/management/v1/default-project"),
        RenameDefaultProjectDeprecated(POST, "/management/v1/default-project/rename"),
        RenameProjectByIdDeprecated(POST, "/management/v1/project/{project_id}/rename"),
        DeleteProjectByIdDeprecated(DELETE, "/management/v1/project/{project_id}"),
        GetProjectByIdDeprecated(GET, "/management/v1/project/{project_id}"),
        UndropTabularsDeprecated(POST, "/management/v1/warehouse/{warehouse_id}/deleted_tabulars/undrop"),
    }

    enum PermissionV1 {
        Get(GET, "/management/v1/permissions"),
        Post(POST, "/management/v1/permissions"),
        Head(HEAD, "/management/v1/permissions"),
        Delete(DELETE, "/management/v1/permissions"),
        Put(PUT, "/management/v1/permissions"),
    }
}

impl ManagementV1Endpoint {
    #[must_use]
    pub fn path_in_management_v1(self) -> &'static str {
        &self.path()["/management/v1".len()..]
    }
}

impl EndpointFlat {
    /// The name this endpoint is stored under, e.g. in endpoint statistics. A stored name
    /// never changes and never disappears; `test_stored_names_are_pinned` enforces that.
    #[must_use]
    pub fn name(self) -> &'static str {
        self.into()
    }
}

impl Endpoint {
    /// See [`EndpointFlat::name`].
    #[must_use]
    pub fn name(self) -> &'static str {
        EndpointFlat::from(self).name()
    }

    /// The endpoint stored under `name`, if this server knows it.
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        name.parse::<EndpointFlat>().ok().map(Into::into)
    }

    /// The Lakekeeper endpoint axum matched for a request. Use
    /// [`ServedEndpoint::from_method_and_matched_path`] to include extension endpoints.
    pub(crate) fn from_method_and_matched_path(method: &Method, inp: &str) -> Option<Self> {
        if inp.starts_with(PERMISSIONS_PREFIX) {
            return match *method {
                Method::GET => Some(PermissionV1Endpoint::Get.into()),
                Method::POST => Some(PermissionV1Endpoint::Post.into()),
                Method::HEAD => Some(PermissionV1Endpoint::Head.into()),
                Method::DELETE => Some(PermissionV1Endpoint::Delete.into()),
                Method::PUT => Some(PermissionV1Endpoint::Put.into()),
                _ => None,
            };
        }
        head_served_by_get(method, |method| {
            ROUTE_MAP.get(&(method.clone(), inp)).copied()
        })
    }
}

/// The authorizer's management routes. Every path starting with this is a permission endpoint.
pub(crate) const PERMISSIONS_PREFIX: &str = "/management/v1/permissions";

/// Looks up `method`, and for a HEAD request without its own endpoint the GET endpoint:
/// axum serves such a request with the route's GET handler.
fn head_served_by_get<T>(method: &Method, lookup: impl Fn(&Method) -> Option<T>) -> Option<T> {
    lookup(method).or_else(|| {
        if method == Method::HEAD {
            lookup(&Method::GET)
        } else {
            None
        }
    })
}

/// An endpoint that a management API extension serves.
///
/// Endpoint statistics store it under [`name`](Self::name), which starts with `ext-`. Like
/// Lakekeeper's own names, a released name never changes.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ExtensionEndpoint {
    name: &'static str,
    method: Method,
    path: &'static str,
}

impl ExtensionEndpoint {
    /// `path` is the full route, starting with `/management/v1/`, as axum reports it as the
    /// matched path. Declare the endpoint as a `static`: `Method` has drop glue, so a
    /// reference to a constant is not `'static`.
    #[must_use]
    pub const fn new(name: &'static str, method: Method, path: &'static str) -> Self {
        Self { name, method, path }
    }

    #[must_use]
    pub fn name(&self) -> &'static str {
        self.name
    }

    #[must_use]
    pub fn method(&self) -> &Method {
        &self.method
    }

    #[must_use]
    pub fn path(&self) -> &'static str {
        self.path
    }
}

/// An endpoint this server serves: one of Lakekeeper's own, or one a management API extension
/// adds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ServedEndpoint {
    Lakekeeper(Endpoint),
    Extension(&'static ExtensionEndpoint),
}

impl From<Endpoint> for ServedEndpoint {
    fn from(endpoint: Endpoint) -> Self {
        Self::Lakekeeper(endpoint)
    }
}

impl From<&'static ExtensionEndpoint> for ServedEndpoint {
    fn from(endpoint: &'static ExtensionEndpoint) -> Self {
        Self::Extension(endpoint)
    }
}

impl ServedEndpoint {
    /// The endpoint axum matched for a request. A HEAD request to a route without a HEAD
    /// endpoint resolves to the route's GET endpoint, whose handler serves it.
    #[must_use]
    pub(crate) fn from_method_and_matched_path(
        method: &Method,
        matched_path: &str,
    ) -> Option<Self> {
        Self::from_method_and_matched_path_in(EXTENSION_ENDPOINTS.get(), method, matched_path)
    }

    fn from_method_and_matched_path_in(
        extensions: Option<&ExtensionEndpoints>,
        method: &Method,
        matched_path: &str,
    ) -> Option<Self> {
        Endpoint::from_method_and_matched_path(method, matched_path)
            .map(Self::Lakekeeper)
            .or_else(|| {
                extensions?
                    .served(method, matched_path)
                    .map(Self::Extension)
            })
    }

    /// The endpoint stored under `name`, if this server serves it.
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        Self::from_name_in(EXTENSION_ENDPOINTS.get(), name)
    }

    fn from_name_in(extensions: Option<&ExtensionEndpoints>, name: &str) -> Option<Self> {
        Endpoint::from_name(name)
            .map(Self::Lakekeeper)
            .or_else(|| extensions?.by_name.get(name).copied().map(Self::Extension))
    }

    /// The name this endpoint is stored under (see [`EndpointFlat::name`]).
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Lakekeeper(endpoint) => endpoint.name(),
            Self::Extension(endpoint) => endpoint.name(),
        }
    }

    /// `METHOD /path`, e.g. `GET /management/v1/info`.
    #[must_use]
    pub fn http_route(self) -> Cow<'static, str> {
        match self {
            Self::Lakekeeper(endpoint) => Cow::Borrowed(endpoint.as_http_route()),
            Self::Extension(endpoint) => {
                Cow::Owned(format!("{} {}", endpoint.method(), endpoint.path()))
            }
        }
    }
}

/// The extension endpoints this process routes.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct ExtensionEndpoints {
    routed: HashMap<(Method, &'static str), &'static ExtensionEndpoint>,
    by_name: HashMap<&'static str, &'static ExtensionEndpoint>,
}

impl ExtensionEndpoints {
    /// Validation rejects a repeated name, and axum panics on a repeated route.
    pub(crate) fn new(routed: impl IntoIterator<Item = &'static ExtensionEndpoint>) -> Self {
        let mut endpoints = Self::default();
        for endpoint in routed {
            endpoints
                .routed
                .insert((endpoint.method().clone(), endpoint.path()), endpoint);
            endpoints.by_name.insert(endpoint.name(), endpoint);
        }
        endpoints
    }

    fn served(&self, method: &Method, matched_path: &str) -> Option<&'static ExtensionEndpoint> {
        head_served_by_get(method, |method| {
            self.routed.get(&(method.clone(), matched_path)).copied()
        })
    }
}

/// Process-global like [`ROUTE_MAP`]: the statistics listing resolves stored names without
/// access to the server's state.
static EXTENSION_ENDPOINTS: OnceLock<ExtensionEndpoints> = OnceLock::new();

/// Installs the extension endpoints of this process. Installing an equal set again succeeds,
/// since a process can build its router more than once; an empty set installs nothing.
///
/// # Errors
/// A different set is already installed.
#[cfg(feature = "router")]
pub(crate) fn install_extension_endpoints(
    endpoints: ExtensionEndpoints,
) -> Result<(), ManagementApiExtensionError> {
    install_extension_endpoints_into(&EXTENSION_ENDPOINTS, endpoints)
}

#[cfg(feature = "router")]
fn install_extension_endpoints_into(
    installed: &OnceLock<ExtensionEndpoints>,
    endpoints: ExtensionEndpoints,
) -> Result<(), ManagementApiExtensionError> {
    if endpoints.by_name.is_empty() {
        return Ok(());
    }
    match installed.set(endpoints) {
        Ok(()) => Ok(()),
        Err(rejected) if installed.get() == Some(&rejected) => Ok(()),
        Err(_) => Err(ManagementApiExtensionError::DifferentEndpointsInstalled),
    }
}

static ROUTE_MAP: LazyLock<HashMap<(Method, &'static str), Endpoint>> = LazyLock::new(|| {
    Endpoint::iter()
        .filter(|e| {
            !matches!(
                e,
                // see comment above in the endpoints enum, these are grouped endpoints due to them
                // potentially being different for every authorizer
                Endpoint::PermissionV1(_)
            )
        })
        .map(|e| ((e.method(), e.path()), e))
        .collect()
});

#[cfg(test)]
mod test {
    use itertools::Itertools;
    use strum::IntoEnumIterator;

    use super::*;

    #[test]
    fn test_as_http_route_is_unique() {
        let mut routes = Endpoint::iter().map(Endpoint::as_http_route).collect_vec();
        routes.sort_unstable();
        routes.dedup();
        assert_eq!(routes.len(), Endpoint::iter().count());
    }

    #[test]
    fn test_method_and_path_is_unique() {
        let routes = Endpoint::iter()
            .map(|e| (e.method(), e.path()))
            .collect_vec();
        assert_eq!(
            routes.len(),
            routes
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
        );
    }

    #[test]
    fn test_endpoint_iter_contains_all_variants() {
        let mut all_variants: Vec<Endpoint> = Vec::new();

        let variants: Vec<Endpoint> = CatalogV1Endpoint::iter().map(Into::into).collect_vec();
        all_variants.extend(variants);

        let variants: Vec<Endpoint> = SignEndpoint::iter().map(Into::into).collect_vec();
        all_variants.extend(variants);

        let variants: Vec<Endpoint> = ManagementV1Endpoint::iter().map(Into::into).collect_vec();
        all_variants.extend(variants);

        let variants: Vec<Endpoint> = PermissionV1Endpoint::iter().map(Into::into).collect_vec();
        all_variants.extend(variants);

        let variants: Vec<Endpoint> = GenericTableV1Endpoint::iter().map(Into::into).collect_vec();
        all_variants.extend(variants);

        let endpoint_variants = Endpoint::iter().collect_vec();

        // Check no duplicates in all_variants
        assert_eq!(
            all_variants.len(),
            all_variants
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
        );

        // Check no duplicates in endpoint_variants
        assert_eq!(
            endpoint_variants.len(),
            endpoint_variants
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
        );

        // Check hashsets are equal
        assert_eq!(
            all_variants
                .iter()
                .collect::<std::collections::HashSet<_>>(),
            endpoint_variants
                .iter()
                .collect::<std::collections::HashSet<_>>()
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_endpoint_completeness() {
        use std::collections::HashSet;

        use itertools::Itertools;
        use serde_norway::Value;
        use strum::IntoEnumIterator;

        use crate::api::endpoints::Endpoint;
        let exempt_config_paths = [
            "management/v1/warehouse/{warehouse_id}/task-queue/soft_deletion/config",
            "management/v1/warehouse/{warehouse_id}/task-queue/tabular_purge/config",
            "management/v1/project/task-queue/task_log_cleanup/config",
        ];
        // Load YAML files
        let management_yaml = include_str!("../../../../docs/docs/api/management-open-api.yaml");
        let catalog_yaml = include_str!("../../../../docs/docs/api/rest-catalog-open-api.yaml");
        let generic_table_yaml =
            include_str!("../../../../docs/docs/api/generic-table-open-api.yaml");

        // Parse YAML files
        let management: Value =
            serde_norway::from_str(management_yaml).expect("Failed to parse management YAML");
        let catalog: Value =
            serde_norway::from_str(catalog_yaml).expect("Failed to parse catalog YAML");
        let generic_table: Value =
            serde_norway::from_str(generic_table_yaml).expect("Failed to parse generic-table YAML");

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
                            let normalized_path = format!("catalog{path_str}");
                            expected_endpoints.insert((method_str.to_uppercase(), normalized_path));
                        }
                    }
                }
            }
        }

        // Process generic-table YAML paths (already prefixed with /lakekeeper/v1)
        if let Value::Mapping(paths) = &generic_table["paths"] {
            for (path, methods) in paths {
                let path_str = path.as_str().expect("Path is not a string");
                if let Value::Mapping(methods_map) = methods {
                    for (method, _) in methods_map {
                        let method_str = method.as_str().expect("Method is not a string");
                        if method_str != "parameters" {
                            let normalized_path = path_str.trim_start_matches('/');
                            expected_endpoints
                                .insert((method_str.to_uppercase(), normalized_path.to_string()));
                        }
                    }
                }
            }
        }

        // Extract endpoints from Endpoints enum
        let mut actual_endpoints = HashSet::new();
        for endpoint in Endpoint::iter() {
            if matches!(endpoint, Endpoint::PermissionV1(_))
                || matches!(endpoint, Endpoint::Sign(_))
            {
                continue;
            }

            // Deprecated endpoints
            if matches!(
                endpoint,
                Endpoint::ManagementV1(
                    ManagementV1Endpoint::DeleteDefaultProjectDeprecated
                        | ManagementV1Endpoint::GetDefaultProjectDeprecated
                        | ManagementV1Endpoint::RenameDefaultProjectDeprecated
                        | ManagementV1Endpoint::UndropTabularsDeprecated
                )
            ) {
                continue;
            }

            let method = endpoint.method().to_string();
            let path = endpoint.path();

            // Remove leading "/" to match normalized paths from YAML
            assert!(path.starts_with('/'), "Path should start with '/'");
            let normalized_path = path.trim_start_matches('/');
            actual_endpoints.insert((method.clone(), normalized_path.to_string()));
        }

        // Find missing endpoints
        let missing_endpoints: Vec<_> = expected_endpoints.difference(&actual_endpoints).collect();

        let missing_endpoints = missing_endpoints
            .iter()
            // Remove deprecated oauth endpoints
            .filter(|(_method, path)| !path.starts_with("catalog/v1/oauth/tokens"))
            // Filter anything that starts with /management/v1/permissions, as these are grouped
            // endpoints that are different for every authorizer
            .filter(|(_method, path)| !path.starts_with("management/v1/permissions"))
            // We remove the parameterized endpoints with {queue_name} and expand them using actually
            // registered queues
            .filter(|(_method, path)| !exempt_config_paths.contains(&path.as_str()))
            .collect::<Vec<_>>();

        if !missing_endpoints.is_empty() {
            let missing_formatted = missing_endpoints
                .iter()
                .sorted()
                .map(|(method, path)| format!("{method} /{path}"))
                .join("\n");

            panic!(
                "The following endpoints are in the OpenAPI YAML but missing from the Endpoints enum:\n{missing_formatted}"
            );
        }

        // Find extra endpoints
        let extra_endpoints: Vec<_> = actual_endpoints.difference(&expected_endpoints).collect();
        let extra_endpoints = extra_endpoints
            .iter()
            .filter(|(_m, path)| {
                // We filter out the parameterized endpoint here since we expand them using actually
                // registered queues
                !path.starts_with(
                    "management/v1/warehouse/{warehouse_id}/task-queue/{queue_name}/config",
                ) && !path.starts_with("management/v1/project/task-queue/{queue_name}/config")
                    && !path.starts_with(
                        "management/v1/warehouse/{warehouse_id}/task-queue/{queue_name}/schedule",
                    )
            })
            .collect_vec();
        if !extra_endpoints.is_empty() {
            let extra_formatted = extra_endpoints
                .iter()
                .sorted()
                .map(|(method, path)| format!("{method} /{path}"))
                .join("\n");

            panic!(
                "The following endpoints are in the Endpoints enum but missing from the OpenAPI YAML:\n{extra_formatted}"
            );
        }
    }

    #[test]
    fn test_can_get_all_paths() {
        let _ = Endpoint::iter().map(Endpoint::path).collect_vec();
    }

    #[test]
    fn test_can_get_all_methods() {
        let _ = Endpoint::iter().map(Endpoint::method).collect_vec();
    }

    #[test]
    fn test_can_resolve_all_tuples() {
        let paths = Endpoint::iter().map(Endpoint::path).collect_vec();
        let methods = Endpoint::iter().map(Endpoint::method).collect_vec();
        for (method, path) in methods.iter().zip(paths) {
            let endpoint = Endpoint::from_method_and_matched_path(method, path);
            assert_eq!(
                endpoint.unwrap().as_http_route(),
                format!("{method} {path}")
            );
        }
    }

    /// Every stored endpoint name, oldest first. Stored data keeps these names, so entries
    /// are only ever appended: a new endpoint adds its name at the end, and a removed route
    /// keeps its endpoint variant.
    const PINNED_STORED_NAMES: &[&str] = &[
        "sign-s3-request-global",
        "sign-s3-request-prefix",
        "catalog-v1-get-config",
        "catalog-v1-list-namespaces",
        "catalog-v1-create-namespace",
        "catalog-v1-load-namespace-metadata",
        "catalog-v1-namespace-exists",
        "catalog-v1-fetch-scan-tasks",
        "catalog-v1-drop-namespace",
        "catalog-v1-update-namespace-properties",
        "catalog-v1-list-tables",
        "catalog-v1-create-table",
        "catalog-v1-load-table",
        "catalog-v1-update-table",
        "catalog-v1-drop-table",
        "catalog-v1-table-exists",
        "catalog-v1-load-credentials",
        "catalog-v1-rename-table",
        "catalog-v1-register-table",
        "catalog-v1-report-metrics",
        "catalog-v1-commit-transaction",
        "catalog-v1-create-view",
        "catalog-v1-list-views",
        "catalog-v1-load-view",
        "catalog-v1-replace-view",
        "catalog-v1-drop-view",
        "catalog-v1-view-exists",
        "catalog-v1-rename-view",
        "management-v1-server-info",
        "management-v1-bootstrap",
        "management-v1-create-role",
        "management-v1-list-role",
        "management-v1-update-role",
        "management-v1-get-role",
        "management-v1-delete-role",
        "management-v1-search-role",
        "management-v1-whoami",
        "management-v1-search-user",
        "management-v1-update-user",
        "management-v1-get-user",
        "management-v1-delete-user",
        "management-v1-create-user",
        "management-v1-list-user",
        "management-v1-create-project",
        "management-v1-get-project",
        "management-v1-delete-project",
        "management-v1-rename-project",
        "management-v1-get-project-by-id-deprecated",
        "management-v1-load-endpoint-statistics",
        "management-v1-delete-project-by-id-deprecated",
        "management-v1-create-warehouse",
        "management-v1-list-warehouses",
        "management-v1-list-projects",
        "management-v1-get-warehouse",
        "management-v1-delete-warehouse",
        "management-v1-rename-warehouse",
        "management-v1-deactivate-warehouse",
        "management-v1-activate-warehouse",
        "management-v1-update-storage-profile",
        "management-v1-update-storage-credential",
        "management-v1-get-warehouse-statistics",
        "management-v1-list-deleted-tabulars",
        "management-v1-undrop-tabulars-deprecated",
        "management-v1-undrop-tabulars",
        "management-v1-update-warehouse-delete-profile",
        "permission-v1-get",
        "permission-v1-post",
        "permission-v1-head",
        "permission-v1-delete",
        "management-v1-set-warehouse-protection",
        "management-v1-set-namespace-protection",
        "management-v1-set-table-protection",
        "management-v1-set-view-protection",
        "catalog-v1-cancel-planning",
        "catalog-v1-fetch-planning-result",
        "catalog-v1-plan-table-scan",
        "management-v1-get-view-protection",
        "management-v1-get-table-protection",
        "management-v1-get-namespace-protection",
        "management-v1-rename-default-project-deprecated",
        "management-v1-get-default-project-deprecated",
        "management-v1-delete-default-project-deprecated",
        "permission-v1-put",
        "management-v1-rename-project-by-id-deprecated",
        "sign-s3-request-tabular",
        "management-v1-set-task-queue-config",
        "management-v1-get-task-queue-config",
        "management-v1-control-tasks",
        "management-v1-get-task-details",
        "management-v1-list-tasks",
        "management-v1-search-tabular",
        "management-v1-get-server-actions",
        "management-v1-get-user-actions",
        "management-v1-get-role-actions",
        "management-v1-get-warehouse-actions",
        "management-v1-get-project-actions",
        "management-v1-get-namespace-actions",
        "management-v1-get-table-actions",
        "management-v1-get-view-actions",
        "management-v1-batch-check-actions",
        "management-v1-get-role-metadata",
        "management-v1-update-role-source-system",
        "management-v1-set-project-task-queue-config",
        "management-v1-get-project-task-queue-config",
        "management-v1-control-project-tasks",
        "management-v1-get-project-task-details",
        "management-v1-list-project-tasks",
        "management-v1-schedule-task",
        "generic-table-v1-create-generic-table",
        "generic-table-v1-list-generic-tables",
        "generic-table-v1-load-generic-table",
        "generic-table-v1-drop-generic-table",
        "generic-table-v1-rename-generic-table",
        "generic-table-v1-load-generic-table-credentials",
        "management-v1-get-generic-table-actions",
        "management-v1-get-generic-table-protection",
        "management-v1-set-generic-table-protection",
        "management-v1-update-warehouse-format-version-policy",
        "management-v1-set-warehouse-managed-by",
        "management-v1-list-role-members",
        "management-v1-add-role-members",
        "management-v1-remove-role-member",
        "management-v1-list-role-member-of",
        "management-v1-list-user-roles",
        "management-v1-list-role-transitive-members",
        "management-v1-list-user-transitive-roles",
        "management-v1-list-role-transitive-member-of",
        "management-v1-create-tag-definition",
        "management-v1-list-tag-definitions",
        "management-v1-get-tag-definition",
        "management-v1-update-tag-definition",
        "management-v1-delete-tag-definition",
        "management-v1-list-tag-attachments",
        "management-v1-set-warehouse-tag",
        "management-v1-delete-warehouse-tag",
        "management-v1-list-warehouse-tags",
        "management-v1-set-namespace-tag",
        "management-v1-delete-namespace-tag",
        "management-v1-list-namespace-tags",
        "management-v1-set-table-tag",
        "management-v1-delete-table-tag",
        "management-v1-list-table-tags",
        "management-v1-set-table-column-tag",
        "management-v1-delete-table-column-tag",
        "management-v1-list-table-column-tags",
        "management-v1-list-column-tags",
        "management-v1-set-view-tag",
        "management-v1-delete-view-tag",
        "management-v1-list-view-tags",
        "management-v1-set-generic-table-tag",
        "management-v1-delete-generic-table-tag",
        "management-v1-list-generic-table-tags",
        "management-v1-validate-warehouse",
        "management-v1-validate-storage-profile",
        "management-v1-validate-storage-credential",
        "management-v1-validate-storage-access",
        "management-v1-list-grants",
        "management-v1-get-grantable-privileges",
        "management-v1-list-warehouse-grants",
        "management-v1-apply-warehouse-grants",
        "management-v1-list-server-grants",
        "management-v1-apply-server-grants",
        "management-v1-list-project-grants",
        "management-v1-apply-project-grants",
        "management-v1-list-namespace-grants",
        "management-v1-apply-namespace-grants",
        "management-v1-list-tag-grants",
        "management-v1-apply-tag-grants",
        "management-v1-list-table-grants",
        "management-v1-apply-table-grants",
        "management-v1-list-view-grants",
        "management-v1-apply-view-grants",
        "management-v1-list-generic-table-grants",
        "management-v1-apply-generic-table-grants",
        "management-v1-get-tag-actions",
        "management-v1-get-server-grantable-privileges",
        "management-v1-get-project-grantable-privileges",
        "management-v1-get-warehouse-grantable-privileges",
        "management-v1-get-namespace-grantable-privileges",
        "management-v1-get-table-grantable-privileges",
        "management-v1-get-view-grantable-privileges",
        "management-v1-get-generic-table-grantable-privileges",
        "management-v1-get-tag-grantable-privileges",
        "management-v1-move-namespace",
        "sign-s3-request-by-table-name",
        "management-v1-list-namespace-subtree-grants",
        "management-v1-revoke-namespace-subtree-grants",
        "management-v1-list-warehouse-subtree-grants",
        "management-v1-revoke-warehouse-subtree-grants",
        "management-v1-set-maintenance-mode",
    ];

    #[test]
    fn test_stored_names_are_pinned() {
        let pinned = PINNED_STORED_NAMES
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            pinned.len(),
            PINNED_STORED_NAMES.len(),
            "duplicate pinned name"
        );

        let current = Endpoint::iter()
            .map(Endpoint::name)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            current.len(),
            Endpoint::iter().count(),
            "duplicate stored name"
        );

        let missing = pinned.difference(&current).collect_vec();
        assert!(
            missing.is_empty(),
            "stored names no endpoint has any more (renamed or removed): {missing:?}"
        );
        let unpinned = current.difference(&pinned).collect_vec();
        assert!(
            unpinned.is_empty(),
            "new endpoints; append their names to PINNED_STORED_NAMES: {unpinned:?}"
        );
    }

    #[test]
    fn test_names_round_trip() {
        for endpoint in Endpoint::iter() {
            assert_eq!(Endpoint::from_name(endpoint.name()), Some(endpoint));
        }
        assert_eq!(Endpoint::from_name("ext-not-an-endpoint"), None);
    }

    #[test]
    fn test_no_name_uses_the_extension_prefix() {
        let reserved = Endpoint::iter()
            .map(Endpoint::name)
            .filter(|name| name.starts_with("ext-"))
            .collect_vec();
        assert!(
            reserved.is_empty(),
            "`ext-` is reserved for extensions: {reserved:?}"
        );
    }

    #[test]
    fn test_head_to_a_get_only_route_resolves_to_the_get_endpoint() {
        let server_info = Endpoint::from(ManagementV1Endpoint::ServerInfo);
        assert_eq!(
            ServedEndpoint::from_method_and_matched_path(&Method::HEAD, server_info.path()),
            Some(ServedEndpoint::Lakekeeper(server_info))
        );
        // A route's own HEAD endpoint wins over its GET endpoint.
        let table_exists = Endpoint::from(CatalogV1Endpoint::TableExists);
        assert_eq!(
            Endpoint::from_method_and_matched_path(&Method::HEAD, table_exists.path()),
            Some(table_exists)
        );
        // Only HEAD falls back, and only to GET.
        assert_eq!(
            Endpoint::from_method_and_matched_path(
                &Method::HEAD,
                ManagementV1Endpoint::Bootstrap.path()
            ),
            None
        );
        assert_eq!(
            Endpoint::from_method_and_matched_path(
                &Method::DELETE,
                CatalogV1Endpoint::GetConfig.path()
            ),
            None
        );
    }

    static HEALTH: ExtensionEndpoint = ExtensionEndpoint::new(
        "ext-table-health",
        Method::GET,
        "/management/v1/warehouse/{warehouse_id}/table/{table_id}/health",
    );
    static RECHECK: ExtensionEndpoint = ExtensionEndpoint::new(
        "ext-table-health-recheck",
        Method::POST,
        "/management/v1/warehouse/{warehouse_id}/table/{table_id}/health",
    );

    fn extension_endpoints() -> ExtensionEndpoints {
        ExtensionEndpoints::new([&HEALTH, &RECHECK])
    }

    #[test]
    fn test_extension_endpoints_resolve_from_method_and_matched_path() {
        let extensions = extension_endpoints();
        let lookup = |method: &Method, path: &str| {
            ServedEndpoint::from_method_and_matched_path_in(Some(&extensions), method, path)
        };
        assert_eq!(
            lookup(&Method::GET, HEALTH.path()),
            Some(ServedEndpoint::Extension(&HEALTH))
        );
        assert_eq!(
            lookup(&Method::HEAD, HEALTH.path()),
            Some(ServedEndpoint::Extension(&HEALTH))
        );
        assert_eq!(
            lookup(&Method::POST, RECHECK.path()),
            Some(ServedEndpoint::Extension(&RECHECK))
        );
        assert_eq!(lookup(&Method::DELETE, HEALTH.path()), None);
        assert_eq!(
            lookup(&Method::GET, ManagementV1Endpoint::ServerInfo.path()),
            Some(ServedEndpoint::Lakekeeper(
                ManagementV1Endpoint::ServerInfo.into()
            ))
        );
        assert_eq!(
            ServedEndpoint::from_method_and_matched_path_in(None, &Method::GET, HEALTH.path()),
            None
        );
    }

    #[test]
    fn test_from_name_knows_lakekeeper_and_extension_endpoints() {
        let extensions = extension_endpoints();
        let from_name = |name: &str| ServedEndpoint::from_name_in(Some(&extensions), name);
        assert_eq!(
            from_name("management-v1-server-info"),
            Some(ServedEndpoint::Lakekeeper(
                ManagementV1Endpoint::ServerInfo.into()
            ))
        );
        assert_eq!(
            from_name("ext-table-health"),
            Some(ServedEndpoint::Extension(&HEALTH))
        );
        assert_eq!(from_name("ext-unknown"), None);
        assert_eq!(ServedEndpoint::from_name_in(None, "ext-table-health"), None);
    }

    #[test]
    fn test_served_endpoint_name_and_http_route() {
        let lakekeeper = ServedEndpoint::from(Endpoint::from(ManagementV1Endpoint::ServerInfo));
        assert_eq!(lakekeeper.name(), "management-v1-server-info");
        assert_eq!(lakekeeper.http_route(), "GET /management/v1/info");

        let extension = ServedEndpoint::from(&RECHECK);
        assert_eq!(extension.name(), "ext-table-health-recheck");
        assert_eq!(
            extension.http_route(),
            "POST /management/v1/warehouse/{warehouse_id}/table/{table_id}/health"
        );
    }

    #[cfg(feature = "router")]
    #[test]
    fn test_installing_extension_endpoints() {
        let installed = OnceLock::new();
        // An empty set installs nothing.
        assert_eq!(
            install_extension_endpoints_into(&installed, ExtensionEndpoints::default()),
            Ok(())
        );
        assert_eq!(installed.get(), None);

        assert_eq!(
            install_extension_endpoints_into(&installed, extension_endpoints()),
            Ok(())
        );
        assert_eq!(
            install_extension_endpoints_into(&installed, extension_endpoints()),
            Ok(())
        );
        assert_eq!(
            install_extension_endpoints_into(&installed, ExtensionEndpoints::new([&HEALTH])),
            Err(ManagementApiExtensionError::DifferentEndpointsInstalled)
        );
        assert_eq!(installed.get(), Some(&extension_endpoints()));
    }
}
