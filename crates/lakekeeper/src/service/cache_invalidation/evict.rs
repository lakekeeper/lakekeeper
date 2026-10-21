//! Evicting the cache entries that change-log rows name from this instance's caches.

use std::{collections::BTreeMap, time::Duration};

use strum::IntoEnumIterator;
use tracing::Instrument;

use super::{CacheChangeReceivers, CacheDomain, CacheKey, ChangeSet, LoggedChange};
use crate::service::{
    catalog_store::{
        namespace_cache::{
            namespace_cache_invalidate, namespace_cache_invalidate_all,
            namespace_cache_invalidate_ident,
        },
        role_assignments_cache::{
            role_ancestors_cache_invalidate_all, user_assignments_cache_invalidate,
            user_assignments_cache_invalidate_all,
        },
        role_cache::{
            role_cache_invalidate, role_cache_invalidate_all, role_cache_invalidate_ident,
        },
        warehouse_cache::{
            warehouse_cache_invalidate, warehouse_cache_invalidate_all,
            warehouse_cache_invalidate_name,
        },
    },
    secrets::{secrets_cache_invalidate, secrets_cache_invalidate_all},
};

/// The longest a receiver may take to evict.
const RECEIVER_TIMEOUT: Duration = Duration::from_secs(10);

/// The cache entries that a batch of changes makes stale.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct Evictions {
    /// The changes of Lakekeeper's domains.
    changes: ChangeSet,
    /// The keys of the other domains, by domain; `None` for every entry of the domain.
    extension_keys: BTreeMap<String, Option<Vec<String>>>,
    all: bool,
}

impl Evictions {
    /// Every entry of every domain.
    pub(super) fn all() -> Self {
        Self {
            all: true,
            ..Self::default()
        }
    }

    /// The entries that `changes` name. A key that does not decode makes its whole domain
    /// stale.
    pub(super) fn of(changes: &[LoggedChange]) -> Self {
        let mut evictions = Self::default();
        for change in changes {
            let Some(domain) = CacheDomain::from_name(&change.domain) else {
                evictions.add_extension_keys(&change.domain, &change.keys);
                continue;
            };
            if change.keys.is_empty() {
                evictions.changes.insert_whole_domain(domain);
            }
            for key in &change.keys {
                match CacheKey::decode(domain, key) {
                    Ok(key) => {
                        evictions.changes.insert(key);
                    }
                    Err(error) => {
                        tracing::warn!(%error, "Evicting the whole cache domain of a key that does not decode");
                        evictions.changes.insert_whole_domain(domain);
                    }
                }
            }
        }
        evictions
    }

    fn add_extension_keys(&mut self, domain: &str, keys: &[String]) {
        let entry = self
            .extension_keys
            .entry(domain.to_string())
            .or_insert_with(|| Some(Vec::new()));
        match entry {
            Some(_) if keys.is_empty() => *entry = None,
            Some(entry_keys) => entry_keys.extend_from_slice(keys),
            None => {}
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        !self.all && self.changes.is_empty() && self.extension_keys.is_empty()
    }

    /// Evict the entries from this process's caches.
    pub(super) async fn apply(&self) {
        if self.all {
            CacheDomain::iter().for_each(invalidate_domain);
        } else {
            evict_locally(&self.changes, &[]).await;
        }
    }

    /// Evict the entries of extension domains through their receivers, each in a task of its
    /// own that is cancelled after 10 s, so a slow or failing receiver holds up no other.
    /// The changes of a domain without a receiver are skipped.
    pub(super) async fn apply_to(&self, receivers: &CacheChangeReceivers) {
        let calls: Vec<_> = if self.all {
            receivers
                .iter()
                .map(|receiver| (receiver.clone(), None))
                .collect()
        } else {
            self.extension_keys
                .iter()
                .filter_map(|(domain, keys)| {
                    let receiver = receivers.get(domain);
                    if receiver.is_none() {
                        tracing::debug!(
                            domain,
                            "Skipping a cache change of a domain this server does not cache"
                        );
                    }
                    receiver.map(|receiver| (receiver.clone(), keys.clone()))
                })
                .collect()
        };
        let tasks = calls.into_iter().map(|(receiver, keys)| {
            let domain = receiver.domain().to_string();
            let task = tokio::spawn(
                async move {
                    let eviction = async {
                        match &keys {
                            Some(keys) => receiver.evict(keys).await,
                            None => receiver.evict_all().await,
                        }
                    };
                    if tokio::time::timeout(RECEIVER_TIMEOUT, eviction)
                        .await
                        .is_err()
                    {
                        tracing::warn!(
                            domain = receiver.domain(),
                            "A cache change receiver did not evict within {RECEIVER_TIMEOUT:?}"
                        );
                    }
                }
                .in_current_span(),
            );
            async move { (domain, task.await) }
        });
        for (domain, result) in futures::future::join_all(tasks).await {
            if let Err(error) = result {
                tracing::warn!(domain, %error, "A cache change receiver failed");
            }
        }
    }
}

/// Evict the entries `changes` name from this process's caches, except the keys in `kept`.
pub(crate) async fn evict_locally(changes: &ChangeSet, kept: &[CacheKey]) {
    for (domain, keys) in changes.keys() {
        match keys {
            None => invalidate_domain(domain),
            Some(keys) => {
                for key in keys.filter(|key| !kept.contains(key)) {
                    invalidate_key(key).await;
                }
            }
        }
    }
}

fn invalidate_domain(domain: CacheDomain) {
    match domain {
        CacheDomain::Warehouse => warehouse_cache_invalidate_all(),
        CacheDomain::Namespace => namespace_cache_invalidate_all(),
        CacheDomain::Role => role_cache_invalidate_all(),
        CacheDomain::UserAssignments => user_assignments_cache_invalidate_all(),
        CacheDomain::RoleAncestors => role_ancestors_cache_invalidate_all(),
        CacheDomain::Secret => secrets_cache_invalidate_all(),
    }
}

async fn invalidate_key(key: &CacheKey) {
    match key {
        CacheKey::Warehouse(id) => warehouse_cache_invalidate(*id).await,
        CacheKey::WarehouseName { project_id, name } => {
            warehouse_cache_invalidate_name(project_id, name).await;
        }
        CacheKey::Namespace(id) => namespace_cache_invalidate(*id).await,
        CacheKey::NamespaceIdent {
            warehouse_id,
            ident,
        } => namespace_cache_invalidate_ident(*warehouse_id, ident).await,
        CacheKey::Role(id) => role_cache_invalidate(*id).await,
        CacheKey::RoleIdent { project_id, ident } => {
            role_cache_invalidate_ident(project_id, ident).await;
        }
        CacheKey::UserAssignments(user_id) => user_assignments_cache_invalidate(user_id).await,
        CacheKey::Secret(id) => secrets_cache_invalidate(*id).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ProjectId, WarehouseId,
        service::{
            SecretId,
            cache_invalidation::{
                ArcCacheChangeReceiver, CacheChangeReceiver,
                receiver::testing::{Recording, receivers},
            },
        },
    };

    fn row(domain: &str, keys: &[&str]) -> LoggedChange {
        LoggedChange {
            domain: domain.to_string(),
            keys: keys.iter().map(ToString::to_string).collect(),
        }
    }

    #[test]
    fn keys_are_decoded_and_empty_rows_name_their_domain() {
        let warehouse = WarehouseId::new_random();
        let name = CacheKey::WarehouseName {
            project_id: ProjectId::new_random(),
            name: "w".to_string(),
        };
        let evictions = Evictions::of(&[
            row("warehouse", &[&warehouse.to_string(), &name.encode()]),
            row("role_ancestors", &[]),
        ]);

        let mut changes = ChangeSet::new();
        changes
            .insert(CacheKey::Warehouse(warehouse))
            .insert(name)
            .insert_whole_domain(CacheDomain::RoleAncestors);
        assert_eq!(
            evictions,
            Evictions {
                changes,
                ..Evictions::default()
            }
        );
    }

    #[test]
    fn a_key_that_does_not_decode_names_its_domain_and_covers_its_keys() {
        let secret = SecretId::from(uuid::Uuid::now_v7());
        let evictions = Evictions::of(&[
            row("secret", &[&secret.to_string()]),
            row("secret", &["not-a-uuid"]),
        ]);

        let mut changes = ChangeSet::new();
        changes.insert_whole_domain(CacheDomain::Secret);
        assert_eq!(
            evictions,
            Evictions {
                changes,
                ..Evictions::default()
            }
        );
    }

    #[test]
    fn rows_of_other_domains_are_kept_by_domain() {
        let evictions = Evictions::of(&[
            row("extension_a", &["key-1"]),
            row("extension_a", &["key-2"]),
            row("extension_b", &[]),
            row("extension_b", &["key-3"]),
        ]);

        assert_eq!(
            evictions,
            Evictions {
                extension_keys: BTreeMap::from([
                    (
                        "extension_a".to_string(),
                        Some(vec!["key-1".to_string(), "key-2".to_string()])
                    ),
                    ("extension_b".to_string(), None),
                ]),
                ..Evictions::default()
            }
        );
    }

    #[tokio::test]
    async fn receivers_evict_their_keys_their_whole_domain_or_everything() {
        let keyed = Recording::new("extension_a");
        let whole = Recording::new("extension_b");
        let receivers = receivers(&[&keyed, &whole]);

        Evictions::of(&[
            row("extension_a", &["key-1"]),
            row("extension_b", &[]),
            row("extension_c", &["key-2"]),
        ])
        .apply_to(&receivers)
        .await;
        Evictions::all().apply_to(&receivers).await;

        assert_eq!(
            (keyed.evicted(), whole.evicted()),
            (
                vec![Some(vec!["key-1".to_string()]), None],
                vec![None, None]
            )
        );
    }

    /// Panics in [`CacheChangeReceiver::evict_all`] and never returns from
    /// [`CacheChangeReceiver::evict`].
    #[derive(Debug)]
    struct Broken(&'static str);

    #[async_trait::async_trait]
    impl CacheChangeReceiver for Broken {
        fn domain(&self) -> &str {
            self.0
        }

        async fn evict(&self, _keys: &[String]) {
            std::future::pending::<()>().await;
        }

        async fn evict_all(&self) {
            panic!("evict_all of a broken receiver");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_failing_or_hanging_receiver_holds_up_no_other() {
        // The hanging receiver's domain sorts first, so it is called first.
        let working = Recording::new("extension_b");
        let receivers = CacheChangeReceivers::new(vec![
            std::sync::Arc::new(Broken("extension_a")),
            working.clone() as ArcCacheChangeReceiver,
        ])
        .unwrap();

        let started = tokio::time::Instant::now();
        let evicting = tokio::spawn({
            let receivers = receivers.clone();
            async move {
                Evictions::of(&[
                    row("extension_a", &["key-1"]),
                    row("extension_b", &["key-2"]),
                ])
                .apply_to(&receivers)
                .await;
            }
        });
        tokio::time::sleep(Duration::from_millis(1)).await;
        let while_hanging = (working.evicted(), evicting.is_finished());
        tokio::time::timeout(2 * RECEIVER_TIMEOUT, evicting)
            .await
            .expect("a hanging receiver is cancelled")
            .unwrap();
        let waited = started.elapsed();
        Evictions::all().apply_to(&receivers).await;

        assert_eq!(
            (while_hanging, working.evicted(), waited),
            (
                (vec![Some(vec!["key-2".to_string()])], false),
                vec![Some(vec!["key-2".to_string()]), None],
                RECEIVER_TIMEOUT
            )
        );
    }
}
