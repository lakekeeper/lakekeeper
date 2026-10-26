//! Receivers of the change-log rows of cache domains that extensions own.

use std::{collections::HashMap, sync::Arc};

use super::CacheDomain;

/// Evicts the entries of a cache domain that an extension owns. The poller hands it the
/// keys recorded for [`Self::domain`] when it reads them, and again after the grace period.
/// A read that cannot list the changes since the previous read makes it evict every entry.
///
/// Each call runs in a task of its own and is cancelled after 10 s; it returns promptly
/// and never waits on the database. Calls may run at the same time. Cached entries must
/// expire within two hours: the log keeps its rows at least that long, and a poller that
/// cannot read the log for longer misses the changes pruned meanwhile.
#[async_trait::async_trait]
pub trait CacheChangeReceiver: std::fmt::Debug + Send + Sync + 'static {
    /// The domain this receiver evicts, as passed to `lakekeeper_record_cache_change`. A
    /// prefix such as `ext_` keeps it apart from the domains Lakekeeper adds.
    fn domain(&self) -> &str;

    /// Evict the entries of `keys` from this process's cache.
    async fn evict(&self, keys: &[String]);

    /// Evict every entry from this process's cache.
    async fn evict_all(&self);
}

/// Reference to a [`CacheChangeReceiver`] that can be cheaply cloned and shared.
pub type ArcCacheChangeReceiver = Arc<dyn CacheChangeReceiver>;

/// A receiver that cannot be registered.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum CacheChangeReceiverError {
    #[error("the cache domain `{0}` belongs to Lakekeeper and takes no receiver")]
    BuiltInDomain(String),
    #[error("more than one receiver for the cache domain `{0}`")]
    DuplicateDomain(String),
}

/// The receivers of extension domains, at most one per domain.
#[derive(Debug, Clone, Default)]
pub struct CacheChangeReceivers(Arc<HashMap<String, ArcCacheChangeReceiver>>);

impl CacheChangeReceivers {
    /// Index `receivers` by domain.
    ///
    /// # Errors
    /// If a receiver names a domain of Lakekeeper, or two name the same domain.
    pub fn new(receivers: Vec<ArcCacheChangeReceiver>) -> Result<Self, CacheChangeReceiverError> {
        let mut by_domain = HashMap::with_capacity(receivers.len());
        for receiver in receivers {
            let domain = receiver.domain().to_string();
            if CacheDomain::from_name(&domain).is_some() {
                return Err(CacheChangeReceiverError::BuiltInDomain(domain));
            }
            if by_domain.contains_key(&domain) {
                return Err(CacheChangeReceiverError::DuplicateDomain(domain));
            }
            by_domain.insert(domain, receiver);
        }
        Ok(Self(Arc::new(by_domain)))
    }

    pub(super) fn get(&self, domain: &str) -> Option<&ArcCacheChangeReceiver> {
        self.0.get(domain)
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &ArcCacheChangeReceiver> {
        self.0.values()
    }
}

/// A receiver for tests.
#[cfg(test)]
pub(super) mod testing {
    use std::sync::{Arc, Mutex};

    use super::{ArcCacheChangeReceiver, CacheChangeReceiver, CacheChangeReceivers};

    /// Records what it is asked to evict: the keys, or `None` for every entry.
    #[derive(Debug)]
    pub(crate) struct Recording {
        domain: &'static str,
        evicted: Mutex<Vec<Option<Vec<String>>>>,
    }

    impl Recording {
        pub(crate) fn new(domain: &'static str) -> Arc<Self> {
            Arc::new(Self {
                domain,
                evicted: Mutex::default(),
            })
        }

        pub(crate) fn evicted(&self) -> Vec<Option<Vec<String>>> {
            self.evicted.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl CacheChangeReceiver for Recording {
        fn domain(&self) -> &str {
            self.domain
        }

        async fn evict(&self, keys: &[String]) {
            self.evicted.lock().unwrap().push(Some(keys.to_vec()));
        }

        async fn evict_all(&self) {
            self.evicted.lock().unwrap().push(None);
        }
    }

    pub(crate) fn receivers(receivers: &[&Arc<Recording>]) -> CacheChangeReceivers {
        CacheChangeReceivers::new(
            receivers
                .iter()
                .map(|receiver| Arc::clone(*receiver) as ArcCacheChangeReceiver)
                .collect(),
        )
        .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::{testing::Recording, *};

    fn domains(names: &[&'static str]) -> Result<Vec<String>, CacheChangeReceiverError> {
        let receivers = CacheChangeReceivers::new(
            names
                .iter()
                .map(|name| Recording::new(name) as ArcCacheChangeReceiver)
                .collect(),
        )?;
        let mut domains: Vec<String> = receivers
            .iter()
            .map(|receiver| receiver.domain().to_string())
            .collect();
        domains.sort();
        Ok(domains)
    }

    #[test]
    fn a_receiver_takes_an_extension_domain_once() {
        assert_eq!(
            (
                domains(&["extension_a", "extension_b"]),
                domains(&["extension_a", "role"]),
                domains(&["extension_a", "extension_a"]),
            ),
            (
                Ok(vec!["extension_a".to_string(), "extension_b".to_string()]),
                Err(CacheChangeReceiverError::BuiltInDomain("role".to_string())),
                Err(CacheChangeReceiverError::DuplicateDomain(
                    "extension_a".to_string()
                )),
            )
        );
    }
}
