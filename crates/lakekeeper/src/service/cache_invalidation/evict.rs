//! Evicting the cache entries that change-log rows name from this instance's caches.

use std::collections::BTreeSet;

use strum::IntoEnumIterator;

use super::{CacheDomain, CacheKey, ChangeSet, LoggedChange};
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

/// The cache entries that a batch of changes makes stale.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct Evictions {
    keys: Vec<CacheKey>,
    domains: BTreeSet<CacheDomain>,
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

    /// The entries that `changes` name. A row of a domain this server does not cache is
    /// skipped; a key that does not decode makes its whole domain stale.
    pub(super) fn of(changes: &[LoggedChange]) -> Self {
        let mut evictions = Self::default();
        for change in changes {
            let Some(domain) = CacheDomain::from_name(&change.domain) else {
                tracing::debug!(
                    domain = change.domain,
                    "Skipping a cache change of a domain this server does not cache"
                );
                continue;
            };
            if change.keys.is_empty() {
                evictions.domains.insert(domain);
            }
            for key in &change.keys {
                match CacheKey::decode(domain, key) {
                    Ok(key) => evictions.keys.push(key),
                    Err(error) => {
                        tracing::warn!(%error, "Evicting the whole cache domain of a key that does not decode");
                        evictions.domains.insert(domain);
                    }
                }
            }
        }
        let domains = &evictions.domains;
        evictions
            .keys
            .retain(|key| !domains.contains(&key.domain()));
        evictions
    }

    /// The entries that `changes` name.
    pub(super) fn of_set(changes: &ChangeSet) -> Self {
        let mut evictions = Self::default();
        for (domain, keys) in changes.keys() {
            match keys {
                Some(keys) => evictions.keys.extend(keys.cloned()),
                None => {
                    evictions.domains.insert(domain);
                }
            }
        }
        evictions
    }

    pub(super) fn is_empty(&self) -> bool {
        !self.all && self.domains.is_empty() && self.keys.is_empty()
    }

    /// Evict the entries from this process's caches.
    pub(super) async fn apply(&self) {
        if self.all {
            CacheDomain::iter().for_each(invalidate_domain);
            return;
        }
        self.domains.iter().copied().for_each(invalidate_domain);
        for key in &self.keys {
            invalidate_key(key).await;
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
    use crate::{ProjectId, WarehouseId, service::SecretId};

    fn row(domain: &str, keys: &[&str]) -> LoggedChange {
        LoggedChange {
            domain: domain.to_string(),
            keys: keys.iter().map(ToString::to_string).collect(),
            version: None,
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

        assert_eq!(
            evictions,
            Evictions {
                keys: vec![CacheKey::Warehouse(warehouse), name],
                domains: BTreeSet::from([CacheDomain::RoleAncestors]),
                all: false,
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

        assert_eq!(
            evictions,
            Evictions {
                keys: vec![],
                domains: BTreeSet::from([CacheDomain::Secret]),
                all: false,
            }
        );
    }

    #[test]
    fn rows_of_other_domains_are_skipped() {
        let evictions = Evictions::of(&[row("extension_domain", &["key-1"])]);

        assert_eq!(evictions, Evictions::default());
        assert!(evictions.is_empty());
    }
}
