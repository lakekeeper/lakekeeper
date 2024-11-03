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

    #[derive(Debug, Default)]
    pub struct PaginatedMapping<T, Z>
    where
        T: std::hash::Hash + Eq + Debug + Clone,
        Z: Debug,
    {
        entities: HashMap<T, Z>,
        next_page_tokens: Vec<String>,
        ordering: Vec<T>,
    }

    impl<T, Z> PaginatedMapping<T, Z>
    where
        T: std::hash::Hash + Eq + Debug,
        Z: Debug,
    {
        #[must_use]
        pub fn map<NewKey: std::hash::Hash + Eq + Debug + Clone, NEW_VAL: Debug>(
            mut self,
            key_map: impl Fn(T) -> Result<NewKey>,
            value_map: impl Fn(Z) -> Result<NEW_VAL>,
        ) -> Result<PaginatedMapping<NewKey, NEW_VAL>> {
            let mut new_mapping = PaginatedMapping::with_capacity(self.len());
            for (key, value, token) in self.into_iter_with_page_tokens() {
                let k = key_map(key)?;
                new_mapping.insert(k, value_map(value)?, token);
            }
            Ok(new_mapping)
        }

        pub fn into_hashmap(self) -> HashMap<T, Z> {
            self.entities
        }

        pub fn next_token(&self) -> Option<&str> {
            self.next_page_tokens.last().map(|s| s.as_str())
        }

        #[must_use]
        pub fn with_capacity(capacity: usize) -> Self {
            Self {
                entities: HashMap::with_capacity(capacity),
                next_page_tokens: Vec::with_capacity(capacity),
                ordering: Vec::with_capacity(capacity),
            }
        }

        #[must_use]
        pub fn insert(&mut self, key: T, value: Z, next_page_token: String) {
            self.entities.insert(key.clone(), value);
            self.ordering.push(key);
            self.next_page_tokens.push(next_page_token);
        }

        #[must_use]
        pub fn len(&self) -> usize {
            self.entities.len()
        }

        #[must_use]
        pub fn is_empty(&self) -> bool {
            self.entities.is_empty()
        }

        pub fn get(&self, key: &T) -> Option<&Z> {
            self.entities.get(key)
        }

        pub fn into_iter_with_page_tokens(mut self) -> impl Iterator<Item = (T, Z, String)> {
            self.ordering
                .into_iter()
                .zip(self.next_page_tokens.into_iter())
                // we can unwrap here since the only way of adding items is via insert which ensures that every
                // entry in self.ordering is also a key into self.tabulars.
                .map(|(key, next_p)| {
                    (
                        key,
                        self.entities
                            .remove(&key)
                            .expect("keys have to be in tabulars if they are in self.ordering"),
                        next_p,
                    )
                })
        }
    }

    impl<T, Z, X> IntoIterator for PaginatedMapping<T, Z>
    where
        T: std::hash::Hash + Eq + Debug,
        Z: Debug,
        X: Iterator<Item = (T, Z)>,
    {
        type Item = (T, Z);
        type IntoIter = X;

        fn into_iter(mut self) -> Self::IntoIter {
            self.ordering
                .into_iter()
                // we can unwrap here since the only way of adding items is via insert which ensures that every
                // entry in self.ordering is also a key into self.tabulars.
                .map(|key| {
                    (
                        key,
                        self.entities
                            .remove(&key)
                            .expect("keys have to be in tabulars if they are in self.ordering"),
                    )
                })
        }
    }
}
