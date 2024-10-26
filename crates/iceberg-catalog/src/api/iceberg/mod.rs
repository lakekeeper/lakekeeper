pub mod types;

pub mod v1 {
    use crate::api::ThreadSafe;
    use axum::Router;
    use std::collections::HashMap;
    use std::fmt::Debug;

    pub mod config;
    pub mod metrics;
    pub mod namespace;
    pub mod oauth;
    pub mod s3_signer;
    pub mod tables;
    pub mod views;

    pub use iceberg_ext::catalog::{NamespaceIdent, TableIdent};

    pub use self::namespace::{ListNamespacesQuery, NamespaceParameters, PaginationQuery};
    pub use self::tables::{DataAccess, ListTablesQuery, TableParameters};
    pub use self::views::ViewParameters;
    pub use crate::api::iceberg::types::*;

    pub use crate::api::{
        ApiContext, CatalogConfig, CommitTableRequest, CommitTableResponse,
        CommitTransactionRequest, CommitViewRequest, CreateNamespaceRequest,
        CreateNamespaceResponse, CreateTableRequest, CreateViewRequest, ErrorModel,
        GetNamespaceResponse, IcebergErrorResponse, ListNamespacesResponse, ListTablesResponse,
        LoadTableResult, LoadViewResult, OAuthTokenRequest, OAuthTokenResponse,
        RegisterTableRequest, RenameTableRequest, Result, UpdateNamespacePropertiesRequest,
        UpdateNamespacePropertiesResponse,
    };
    pub use crate::request_metadata::RequestMetadata;

    // according to crates/iceberg-ext/src/catalog/rest/namespace.rs:115 we should
    // return everything - in order to block malicious requests, we still cap to 1000
    pub const MAX_PAGE_SIZE: i64 = 1000;

    pub fn new_v1_full_router<
        #[cfg(feature = "s3-signer")] T: config::Service<S>
            + namespace::Service<S>
            + tables::Service<S>
            + metrics::Service<S>
            + s3_signer::Service<S>
            + views::Service<S>,
        #[cfg(not(feature = "s3-signer"))] T: config::Service<S>
            + namespace::Service<S>
            + tables::Service<S>
            + metrics::Service<S>
            + views::Service<S>,
        S: ThreadSafe,
    >() -> Router<ApiContext<S>> {
        let router = Router::new()
            .merge(config::router::<T, S>())
            .merge(namespace::router::<T, S>())
            .merge(tables::router::<T, S>())
            .merge(views::router::<T, S>())
            .merge(metrics::router::<T, S>());

        #[cfg(feature = "s3-signer")]
        let router = router.merge(s3_signer::router::<T, S>());

        router
    }

    pub fn new_v1_config_router<C: config::Service<S>, S: ThreadSafe>() -> Router<ApiContext<S>> {
        config::router::<C, S>()
    }

    #[derive(Debug)]
    pub struct PaginatedTabulars<T, Z>
    where
        T: std::hash::Hash + Eq + Debug,
        Z: Debug,
    {
        pub tabulars: HashMap<T, Z>,
        pub next_page_token: Option<String>,
    }

    impl<T, Z> PaginatedTabulars<T, Z>
    where
        T: std::hash::Hash + Eq + Debug,
        Z: Debug,
    {
        #[must_use]
        pub fn len(&self) -> usize {
            self.tabulars.len()
        }

        #[must_use]
        pub fn is_empty(&self) -> bool {
            self.tabulars.is_empty()
        }

        pub fn get(&self, key: &T) -> Option<&Z> {
            self.tabulars.get(key)
        }
    }

    impl<T, Z> IntoIterator for PaginatedTabulars<T, Z>
    where
        T: std::hash::Hash + Eq + Debug,
        Z: Debug,
    {
        type Item = (T, Z);
        type IntoIter = std::collections::hash_map::IntoIter<T, Z>;

        fn into_iter(self) -> Self::IntoIter {
            self.tabulars.into_iter()
        }
    }
}
