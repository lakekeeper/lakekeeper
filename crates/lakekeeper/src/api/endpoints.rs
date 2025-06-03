use std::{collections::HashMap, sync::LazyLock};

use http::Method;
use strum::IntoEnumIterator;

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
            paste::paste! {
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

        paste::paste! {
            #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum_macros::EnumIter, sqlx::Type, strum::Display)]
            #[strum(serialize_all = "kebab-case")]
            #[sqlx(type_name = "api_endpoints", rename_all = "kebab-case")]
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

    enum Sign {
        S3RequestGlobal(POST, "/catalog/v1/aws/s3/sign"),
        S3RequestPrefix(POST, "/catalog/v1/{prefix}/v1/aws/s3/sign"),
        S3RequestTabular(POST, "/catalog/v1/signer/{prefix}/tabular-id/{tabular_id}/v1/aws/s3/sign"),
    }

    enum ManagementV1 {
        ServerInfo(GET, "/management/v1/info"),
        Bootstrap(POST, "/management/v1/bootstrap"),
        CreateUser(POST, "/management/v1/user"),
        SearchUser(POST, "/management/v1/search/user"),
        GetUser(GET, "/management/v1/user/{user_id}"),
        Whoami(GET, "/management/v1/whoami"),
        UpdateUser(PUT, "/management/v1/user/{user_id}"),
        ListUser(GET, "/management/v1/user"),
        DeleteUser(DELETE, "/management/v1/user/{user_id}"),
        CreateRole(POST, "/management/v1/role"),
        SearchRole(POST, "/management/v1/search/role"),
        ListRole(GET, "/management/v1/role"),
        DeleteRole(DELETE, "/management/v1/role/{role_id}"),
        GetRole(GET, "/management/v1/role/{role_id}"),
        UpdateRole(POST, "/management/v1/role/{role_id}"),
        CreateWarehouse(POST, "/management/v1/warehouse"),
        ListProjects(GET, "/management/v1/project-list"),
        CreateProject(POST, "/management/v1/project"),
        GetDefaultProject(GET, "/management/v1/project"),
        GetDefaultProjectById(GET, "/management/v1/project/{project_id}"),
        DeleteDefaultProject(DELETE, "/management/v1/project"),
        DeleteProjectById(DELETE, "/management/v1/project/{project_id}"),
        RenameDefaultProject(POST, "/management/v1/project/rename"),
        RenameProjectById(POST, "/management/v1/project/{project_id}/rename"),
        ListWarehouses(GET, "/management/v1/warehouse"),
        GetWarehouse(GET, "/management/v1/warehouse/{warehouse_id}"),
        DeleteWarehouse(DELETE, "/management/v1/warehouse/{warehouse_id}"),
        RenameWarehouse(POST, "/management/v1/warehouse/{warehouse_id}/rename"),
        UpdateWarehouseDeleteProfile(POST, "/management/v1/warehouse/{warehouse_id}/delete-profile"),
        DeactivateWarehouse(POST, "/management/v1/warehouse/{warehouse_id}/deactivate"),
        ActivateWarehouse(POST, "/management/v1/warehouse/{warehouse_id}/activate"),
        UpdateStorageProfile(POST, "/management/v1/warehouse/{warehouse_id}/storage"),
        UpdateStorageCredential(POST, "/management/v1/warehouse/{warehouse_id}/storage-credential"),
        GetWarehouseStatistics(GET, "/management/v1/warehouse/{warehouse_id}/statistics"),
        LoadEndpointStatistics(POST, "/management/v1/endpoint-statistics"),
        ListDeletedTabulars(GET, "/management/v1/warehouse/{warehouse_id}/deleted-tabulars"),
        UndropTabularsDeprecated(POST, "/management/v1/warehouse/{warehouse_id}/deleted_tabulars/undrop"),
        UndropTabulars(POST, "/management/v1/warehouse/{warehouse_id}/deleted-tabulars/undrop"),
        GetTableProtection(GET, "/management/v1/warehouse/{warehouse_id}/table/{table_id}/protection"),
        SetTableProtection(POST, "/management/v1/warehouse/{warehouse_id}/table/{table_id}/protection"),
        GetViewProtection(GET, "/management/v1/warehouse/{warehouse_id}/view/{view_id}/protection"),
        SetViewProtection(POST, "/management/v1/warehouse/{warehouse_id}/view/{view_id}/protection"),
        SetNamespaceProtection(POST, "/management/v1/warehouse/{warehouse_id}/namespace/{namespace_id}/protection"),
        GetNamespaceProtection(GET, "/management/v1/warehouse/{warehouse_id}/namespace/{namespace_id}/protection"),
        SetWarehouseProtection(POST, "/management/v1/warehouse/{warehouse_id}/protection"),
        GetDefaultProjectDeprecated(GET, "/management/v1/default-project"),
        DeleteDefaultProjectDeprecated(DELETE, "/management/v1/default-project"),
        RenameDefaultProjectDeprecated(POST, "/management/v1/default-project/rename"),
        SetTaskQueueConfig(POST, "/management/v1/{warehouse_id}/task-queue/{queue_name}/config"),
        GetTaskQueueConfig(GET, "/management/v1/{warehouse_id}/task-queue/{queue_name}/config")
    }

    enum PermissionV1 {
        Get(GET, "/management/v1/permissions"),
        Post(POST, "/management/v1/permissions"),
        Head(HEAD, "/management/v1/permissions"),
        Delete(DELETE, "/management/v1/permissions"),
        Put(PUT, "/management/v1/permissions"),
    }
}

impl Endpoint {
    pub fn from_method_and_matched_path(method: &Method, inp: &str) -> Option<Self> {
        if inp.starts_with("/management/v1/permissions") {
            return match *method {
                Method::GET => Some(PermissionV1Endpoint::Get.into()),
                Method::POST => Some(PermissionV1Endpoint::Post.into()),
                Method::HEAD => Some(PermissionV1Endpoint::Head.into()),
                Method::DELETE => Some(PermissionV1Endpoint::Delete.into()),
                Method::PUT => Some(PermissionV1Endpoint::Put.into()),
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
                    &Method::PUT => Method::PUT,
                    x => x.clone(),
                },
                inp,
            ))
            .copied()
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
    fn test_endpoint_completeness() {
        use std::collections::HashSet;

        use itertools::Itertools;
        use serde_yaml::Value;
        use strum::IntoEnumIterator;

        use crate::api::endpoints::Endpoint;
        let exempt_config_paths = [
            "management/v1/{warehouse_id}/task-queue/tabular_expiration/config",
            "management/v1/{warehouse_id}/task-queue/tabular_purge/config",
        ];
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
                            let normalized_path = format!("catalog{path_str}");
                            expected_endpoints.insert((method_str.to_uppercase(), normalized_path));
                        }
                    }
                }
            }
        }

        // Extract endpoints from Endpoints enum
        let mut actual_endpoints = HashSet::new();
        for endpoint in Endpoint::iter() {
            // Only catalog and management endpoints are relevant for this test
            if matches!(endpoint, Endpoint::PermissionV1(_))
                || matches!(endpoint, Endpoint::Sign(_))
            {
                continue;
            }

            let method = endpoint.method().to_string();
            let path = endpoint.path();

            // Remove leading "/" to match normalized paths from YAML
            assert!(path.starts_with('/'), "Path should start with '/'");
            let normalized_path = path.trim_start_matches('/');
            actual_endpoints.insert((method.to_string(), normalized_path.to_string()));
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

            panic!("The following endpoints are in the OpenAPI YAML but missing from the Endpoints enum:\n{missing_formatted}");
        }

        // Find extra endpoints
        let extra_endpoints: Vec<_> = actual_endpoints.difference(&expected_endpoints).collect();
        let extra_endpoints = extra_endpoints
            .iter()
            .filter(|(_m, path)| {
                // We filter out the parameterized endpoint here since we expand them using actually
                // registered queues
                !path.starts_with("management/v1/{warehouse_id}/task-queue/{queue_name}/config")
            })
            .collect_vec();
        if !extra_endpoints.is_empty() {
            let extra_formatted = extra_endpoints
                .iter()
                .sorted()
                .map(|(method, path)| format!("{method} /{path}"))
                .join("\n");

            panic!("The following endpoints are in the Endpoints enum but missing from the OpenAPI YAML:\n{extra_formatted}");
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
        for (method, path) in methods.iter().zip(paths.into_iter()) {
            let endpoint = Endpoint::from_method_and_matched_path(method, path);
            assert_eq!(
                endpoint.unwrap().as_http_route(),
                format!("{method} {path}")
            );
        }
    }
}
