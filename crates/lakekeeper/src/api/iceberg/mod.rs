use std::sync::LazyLock;

use strum::IntoEnumIterator as _;

pub mod types;

pub mod v1 {
    use std::{collections::HashMap, fmt::Debug};

    use axum::Router;
    use itertools::Itertools;

    use crate::api::ThreadSafe;

    pub mod config;
    pub mod metrics;
    pub mod namespace;
    pub mod oauth;
    pub mod s3_signer;
    pub mod tables;
    pub mod views;

    pub use iceberg_ext::catalog::{NamespaceIdent, TableIdent};

    pub use self::{
        namespace::{ListNamespacesQuery, NamespaceParameters, PaginationQuery},
        tables::{
            DataAccess, DataAccessMode, ListTablesQuery, LoadTableResultOrNotModified,
            TableParameters,
        },
        views::ViewParameters,
    };
    pub use crate::{
        api::{
            ApiContext, CatalogConfig, CommitTableRequest, CommitTableResponse,
            CommitTransactionRequest, CommitViewRequest, CreateNamespaceRequest,
            CreateNamespaceResponse, CreateTableRequest, CreateViewRequest, ErrorModel,
            GetNamespaceResponse, IcebergErrorResponse, ListNamespacesResponse, ListTablesResponse,
            LoadTableResult, LoadViewResult, OAuthTokenRequest, OAuthTokenResponse,
            RegisterTableRequest, RenameTableRequest, Result, UpdateNamespacePropertiesRequest,
            UpdateNamespacePropertiesResponse, iceberg::types::*,
        },
        request_metadata::RequestMetadata,
    };

    pub fn new_v1_full_router<
        #[cfg(feature = "s3-signer")] T: config::Service<S>
            + namespace::NamespaceService<S>
            + tables::TablesService<S>
            + metrics::Service<S>
            + s3_signer::Service<S>
            + views::ViewService<S>,
        #[cfg(not(feature = "s3-signer"))] T: config::Service<S>
            + namespace::NamespaceService<S>
            + tables::TablesService<S>
            + metrics::Service<S>
            + views::ViewService<S>,
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

    /// Iterator over references to key-value pairs in insertion order.
    #[derive(Debug)]
    pub struct Iter<'a, T, V>
    where
        T: std::hash::Hash + Eq + Debug + Clone,
        V: Debug,
    {
        ordering: &'a [T],
        entities: &'a HashMap<T, V>,
        index: usize,
    }

    impl<'a, T, V> Iterator for Iter<'a, T, V>
    where
        T: std::hash::Hash + Eq + Debug + Clone,
        V: Debug,
    {
        type Item = (&'a T, &'a V);

        fn next(&mut self) -> Option<Self::Item> {
            if self.index < self.ordering.len() {
                let key = &self.ordering[self.index];
                self.index += 1;
                // Safe to unwrap: ordering only contains keys that exist in entities
                let value = self
                    .entities
                    .get(key)
                    .expect("keys have to be in entities if they are in ordering");
                Some((key, value))
            } else {
                None
            }
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            let remaining = self.ordering.len() - self.index;
            (remaining, Some(remaining))
        }
    }

    impl<T, V> ExactSizeIterator for Iter<'_, T, V>
    where
        T: std::hash::Hash + Eq + Debug + Clone,
        V: Debug,
    {
    }

    impl<T, V> PaginatedMapping<T, V>
    where
        T: std::hash::Hash + Eq + Debug + Clone + 'static,
        V: Debug + 'static,
    {
        /// Creates a new `PaginatedMapping` with the specified capacity.
        ///
        /// # Errors
        /// If the provided `key_map` or `value_map` functions return an error
        pub fn map<
            NewKey: std::hash::Hash + Eq + Debug + Clone + 'static,
            NewVal: Debug + 'static,
            E,
        >(
            self,
            key_map: impl Fn(T) -> std::result::Result<NewKey, E>,
            value_map: impl Fn(V) -> std::result::Result<NewVal, E>,
        ) -> std::result::Result<PaginatedMapping<NewKey, NewVal>, E> {
            let mut new_mapping = PaginatedMapping::with_capacity(self.len());
            for (key, value, token) in self.into_iter_with_page_tokens() {
                let k = key_map(key)?;
                new_mapping.insert(k, value_map(value)?, token);
            }
            Ok(new_mapping)
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
        pub fn new() -> Self {
            Self {
                entities: HashMap::new(),
                next_page_tokens: Vec::new(),
                ordering: Vec::new(),
            }
        }

        pub fn insert(&mut self, key: T, value: V, next_page_token: String) {
            if self.entities.insert(key.clone(), value).is_some() {
                let position = self
                    .ordering
                    .iter()
                    .find_position(|item| **item == key)
                    .map(|(idx, _)| idx);
                if let Some(idx) = position {
                    self.ordering.remove(idx);
                    self.next_page_tokens.remove(idx);
                }
            }
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

        pub fn get(&self, key: &T) -> Option<&V> {
            self.entities.get(key)
        }

        #[allow(clippy::missing_panics_doc)]
        pub fn into_iter_with_page_tokens(mut self) -> impl Iterator<Item = (T, V, String)> {
            self.ordering
                .into_iter()
                .zip(self.next_page_tokens)
                // we can unwrap here since the only way of adding items is via insert which ensures that every
                // entry in self.ordering is also a key into self.tabulars.
                .map(move |(key, next_p)| {
                    let v = self
                        .entities
                        .remove(&key)
                        .expect("keys have to be in tabulars if they are in self.ordering");
                    (key, v, next_p)
                })
        }

        #[cfg(test)]
        pub fn remove(&mut self, key: &T) -> Option<V> {
            let (idx, _) = self.ordering.iter().find_position(|item| **item == *key)?;
            self.ordering.remove(idx);
            self.next_page_tokens.remove(idx);
            self.entities.remove(key)
        }

        #[cfg(test)]
        pub(crate) fn into_hashmap(self) -> HashMap<T, V> {
            self.entities
        }

        #[cfg(test)]
        pub(crate) fn next_token(&self) -> Option<&str> {
            self.next_page_tokens.last().map(String::as_str)
        }

        /// Returns an iterator over the key-value pairs in insertion order.
        #[must_use]
        pub fn iter(&self) -> Iter<'_, T, V> {
            Iter {
                ordering: &self.ordering,
                entities: &self.entities,
                index: 0,
            }
        }
    }

    impl<'a, T, V> IntoIterator for &'a PaginatedMapping<T, V>
    where
        T: std::hash::Hash + Eq + Debug + Clone + 'static,
        V: Debug + 'static,
    {
        type Item = (&'a T, &'a V);
        type IntoIter = Iter<'a, T, V>;
        fn into_iter(self) -> Self::IntoIter {
            self.iter()
        }
    }

    impl<T, Z> IntoIterator for PaginatedMapping<T, Z>
    where
        T: std::hash::Hash + Eq + Debug + Clone + 'static,
        Z: Debug + 'static,
    {
        type Item = (T, Z);
        type IntoIter = Box<dyn Iterator<Item = (T, Z)>>;

        fn into_iter(mut self) -> Self::IntoIter {
            Box::new(
                self.ordering
                    .into_iter()
                    // we can unwrap here since the only way of adding items is via insert which ensures that every
                    // entry in self.ordering is also a key into self.tabulars.
                    .map(move |key| {
                        let v = self
                            .entities
                            .remove(&key)
                            .expect("keys have to be in tabulars if they are in self.ordering");
                        (key, v)
                    }),
            )
        }
    }
}

static SUPPORTED_ENDPOINTS: LazyLock<Vec<String>> = LazyLock::new(|| {
    crate::api::endpoints::CatalogV1Endpoint::iter()
        .filter(|s| !s.unimplemented())
        .map(|s| s.as_http_route().replace(" /catalog/", " /"))
        .collect()
});

pub(crate) fn supported_endpoints() -> &'static [String] {
    &SUPPORTED_ENDPOINTS
}

#[cfg(test)]
mod test {
    use uuid::Uuid;

    use crate::api::iceberg::v1::PaginatedMapping;

    #[test]
    fn test_supported_endpoints() {
        let openapi = include_str!("../../../../../docs/docs/api/rest-catalog-open-api.yaml");
        let s: serde_json::Value = serde_norway::from_str(openapi).unwrap();
        let paths = s["paths"].as_object().unwrap();
        let unsupported = &[
            "/v1/oauth/tokens",
            "/v1/{prefix}/namespaces/{namespace}/tables/{table}/plan",
            "/v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}",
            "/v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks",
        ];
        // Check that openapi endpoints are in the supported endpoints
        paths
            .into_iter()
            .filter(|(path, _)| !unsupported.contains(&path.as_str()))
            .for_each(|(path, vals)| {
                let methods = vals.as_object().unwrap();
                methods
                    .keys()
                    .filter(|m| *m != "parameters")
                    .for_each(|method| {
                        let route = format!("{} {}", method.to_uppercase(), path);
                        assert!(super::supported_endpoints().contains(&route), "{route}");
                    });
            });

        // Check that none of the unsupported endpoint strings is contained in any of the supported endpoints
        for endpoint in super::supported_endpoints() {
            assert!(
                !unsupported.iter().any(|s| endpoint.contains(s)),
                "endpoint {endpoint} is unsupported endpoint"
            );
        }
    }

    #[test]
    fn iteration_with_page_token_is_in_insertion_order() {
        let mut map = PaginatedMapping::with_capacity(3);
        let k1 = Uuid::now_v7();
        let k2 = Uuid::now_v7();
        let k3 = Uuid::now_v7();

        map.insert(k1, String::from("v1"), String::from("t1"));
        map.insert(k2, String::from("v2"), String::from("t2"));
        map.insert(k3, String::from("v3"), String::from("t3"));

        let r = map.into_iter_with_page_tokens().collect::<Vec<_>>();
        assert_eq!(
            r,
            vec![
                (k1, String::from("v1"), String::from("t1")),
                (k2, String::from("v2"), String::from("t2")),
                (k3, String::from("v3"), String::from("t3")),
            ]
        );
    }

    #[test]
    fn into_iter_is_in_insertion_order() {
        let mut map = PaginatedMapping::with_capacity(3);
        let k1 = Uuid::now_v7();
        let k2 = Uuid::now_v7();
        let k3 = Uuid::now_v7();

        map.insert(k1, String::from("v1"), String::from("t1"));
        map.insert(k2, String::from("v2"), String::from("t2"));
        map.insert(k3, String::from("v3"), String::from("t3"));

        let r = map.into_iter().collect::<Vec<_>>();
        assert_eq!(
            r,
            vec![
                (k1, String::from("v1")),
                (k2, String::from("v2")),
                (k3, String::from("v3")),
            ]
        );
    }

    #[test]
    fn reinserts_dont_panic() {
        let mut map = PaginatedMapping::with_capacity(3);
        let k1 = Uuid::now_v7();
        let k2 = Uuid::now_v7();
        let k3 = Uuid::now_v7();

        map.insert(k1, String::from("v1"), String::from("t1"));
        map.insert(k2, String::from("v2"), String::from("t2"));
        map.insert(k3, String::from("v3"), String::from("t3"));

        map.insert(k1, String::from("v1"), String::from("t1"));

        let r = map.into_iter_with_page_tokens().collect::<Vec<_>>();
        assert_eq!(
            r,
            vec![
                (k2, String::from("v2"), String::from("t2")),
                (k3, String::from("v3"), String::from("t3")),
                (k1, String::from("v1"), String::from("t1"))
            ]
        );
    }

    #[test]
    fn iter_is_in_insertion_order() {
        let mut map = PaginatedMapping::with_capacity(3);
        let k1 = Uuid::now_v7();
        let k2 = Uuid::now_v7();
        let k3 = Uuid::now_v7();

        map.insert(k1, String::from("v1"), String::from("t1"));
        map.insert(k2, String::from("v2"), String::from("t2"));
        map.insert(k3, String::from("v3"), String::from("t3"));

        let r = map.iter().collect::<Vec<_>>();
        assert_eq!(
            r,
            vec![
                (&k1, &String::from("v1")),
                (&k2, &String::from("v2")),
                (&k3, &String::from("v3")),
            ]
        );

        // Verify we can still use map after iter
        assert_eq!(map.len(), 3);
    }

    #[test]
    fn iter_size_hint_is_correct() {
        let mut map = PaginatedMapping::with_capacity(3);
        let k1 = Uuid::now_v7();
        let k2 = Uuid::now_v7();
        let k3 = Uuid::now_v7();

        map.insert(k1, String::from("v1"), String::from("t1"));
        map.insert(k2, String::from("v2"), String::from("t2"));
        map.insert(k3, String::from("v3"), String::from("t3"));

        let mut iter = map.iter();
        assert_eq!(iter.size_hint(), (3, Some(3)));
        assert_eq!(iter.len(), 3);

        iter.next();
        assert_eq!(iter.size_hint(), (2, Some(2)));
        assert_eq!(iter.len(), 2);

        iter.next();
        iter.next();
        assert_eq!(iter.size_hint(), (0, Some(0)));
        assert_eq!(iter.len(), 0);
    }
}
