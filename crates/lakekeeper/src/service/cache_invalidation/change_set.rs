use std::collections::BTreeMap;

use iceberg::NamespaceIdent;
use serde::de::DeserializeOwned;

use crate::{
    ProjectId, WarehouseId,
    service::{NamespaceId, RoleId, RoleIdent, SecretId, UserId},
};

/// A group of cached entries that change-log rows name. The snake-case variant name is
/// stored in the `domain` column of the change log.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    strum::EnumIter,
    strum_macros::IntoStaticStr,
    strum_macros::EnumString,
)]
#[strum(serialize_all = "snake_case")]
pub enum CacheDomain {
    /// Warehouses by id and by name.
    Warehouse,
    /// Namespaces by id and by ident.
    Namespace,
    /// Roles by id and by ident.
    Role,
    /// The roles assigned to each user.
    UserAssignments,
    /// The roles each role is transitively a member of. Changes always name the whole domain.
    RoleAncestors,
    /// Storage secrets by id.
    Secret,
}

impl CacheDomain {
    /// The name stored in the `domain` column of the change log.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        self.into()
    }

    /// The domain stored under `name`, or `None` for a domain this crate does not own.
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        name.parse().ok()
    }
}

/// One cached entry that a write makes stale.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheKey {
    /// A warehouse by id.
    Warehouse(WarehouseId),
    /// The entry that resolves a warehouse name to its id.
    WarehouseName { project_id: ProjectId, name: String },
    /// A namespace by id.
    Namespace(NamespaceId),
    /// The entry that resolves a namespace ident to its id.
    NamespaceIdent {
        warehouse_id: WarehouseId,
        ident: NamespaceIdent,
    },
    /// A role by id.
    Role(RoleId),
    /// The entry that resolves a role ident to its id.
    RoleIdent {
        project_id: ProjectId,
        ident: RoleIdent,
    },
    /// The roles assigned to a user.
    UserAssignments(UserId),
    /// A storage secret by id.
    Secret(SecretId),
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("Cannot decode cache key {key:?} of domain {domain:?}")]
pub struct CacheKeyDecodeError {
    pub domain: CacheDomain,
    pub key: String,
}

impl CacheKey {
    /// The domain this key belongs to.
    #[must_use]
    pub fn domain(&self) -> CacheDomain {
        match self {
            Self::Warehouse(_) | Self::WarehouseName { .. } => CacheDomain::Warehouse,
            Self::Namespace(_) | Self::NamespaceIdent { .. } => CacheDomain::Namespace,
            Self::Role(_) | Self::RoleIdent { .. } => CacheDomain::Role,
            Self::UserAssignments(_) => CacheDomain::UserAssignments,
            Self::Secret(_) => CacheDomain::Secret,
        }
    }

    /// The text stored in the `keys` column. An id is stored as is, a compound key as a
    /// JSON array.
    #[must_use]
    pub fn encode(&self) -> String {
        match self {
            Self::Warehouse(id) => id.to_string(),
            Self::Namespace(id) => id.to_string(),
            Self::Role(id) => id.to_string(),
            Self::UserAssignments(id) => id.to_string(),
            Self::Secret(id) => id.to_string(),
            Self::WarehouseName { project_id, name } => compound(&(project_id, name)),
            Self::NamespaceIdent {
                warehouse_id,
                ident,
            } => compound(&(warehouse_id, ident)),
            Self::RoleIdent { project_id, ident } => compound(&(project_id, ident)),
        }
    }

    /// Parse a key that [`Self::encode`] produced for `domain`.
    ///
    /// # Errors
    /// Returns [`CacheKeyDecodeError`] if `key` is not a key of `domain`.
    pub fn decode(domain: CacheDomain, key: &str) -> Result<Self, CacheKeyDecodeError> {
        let error = || CacheKeyDecodeError {
            domain,
            key: key.to_string(),
        };
        // Warehouse, namespace and role ids are UUIDs, so a leading `[` marks their
        // compound keys.
        let compound = key.starts_with('[');
        let uuid = || uuid::Uuid::parse_str(key).ok();
        let decoded = match domain {
            CacheDomain::Warehouse if compound => {
                parse_json(key).map(|(project_id, name)| Self::WarehouseName { project_id, name })
            }
            CacheDomain::Warehouse => uuid().map(|id| Self::Warehouse(id.into())),
            CacheDomain::Namespace if compound => {
                parse_json(key).map(|(warehouse_id, ident)| Self::NamespaceIdent {
                    warehouse_id,
                    ident,
                })
            }
            CacheDomain::Namespace => uuid().map(|id| Self::Namespace(id.into())),
            CacheDomain::Role if compound => {
                parse_json(key).map(|(project_id, ident)| Self::RoleIdent { project_id, ident })
            }
            CacheDomain::Role => uuid().map(|id| Self::Role(id.into())),
            CacheDomain::UserAssignments => UserId::try_from(key).ok().map(Self::UserAssignments),
            CacheDomain::Secret => uuid().map(|id| Self::Secret(id.into())),
            CacheDomain::RoleAncestors => None,
        };
        decoded.ok_or_else(error)
    }
}

/// A compound key as a JSON array.
fn compound(value: &impl serde::Serialize) -> String {
    serde_json::to_string(value).expect("a tuple of ids, strings and string lists serializes")
}

fn parse_json<T: DeserializeOwned>(key: &str) -> Option<T> {
    serde_json::from_str(key).ok()
}

/// The most keys of one domain a change set lists; more make it a whole-domain change.
const FANOUT_CAP: usize = 50_000;

/// The cache entries a write makes stale.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeSet {
    domains: BTreeMap<CacheDomain, DomainChange>,
    fanout_cap: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DomainChange {
    /// Keyed by the encoded key, which sorts and de-duplicates them.
    Keys(BTreeMap<String, CacheKey>),
    Whole,
}

impl Default for ChangeSet {
    fn default() -> Self {
        Self::new()
    }
}

impl ChangeSet {
    /// An empty set. A domain that collects more than 50 000 keys becomes a whole-domain
    /// change.
    #[must_use]
    pub fn new() -> Self {
        Self::with_cap(FANOUT_CAP)
    }

    /// An empty set with a fan-out cap of `fanout_cap` keys.
    #[cfg(any(test, feature = "test-utils"))]
    #[must_use]
    pub fn with_fanout_cap(fanout_cap: usize) -> Self {
        Self::with_cap(fanout_cap)
    }

    fn with_cap(fanout_cap: usize) -> Self {
        Self {
            domains: BTreeMap::new(),
            fanout_cap,
        }
    }

    /// Add one changed key. Its domain becomes a whole-domain change once it holds more
    /// keys than the fan-out cap.
    pub fn insert(&mut self, key: CacheKey) -> &mut Self {
        let change = self
            .domains
            .entry(key.domain())
            .or_insert_with(|| DomainChange::Keys(BTreeMap::new()));
        if let DomainChange::Keys(keys) = change {
            keys.insert(key.encode(), key);
            if keys.len() > self.fanout_cap {
                *change = DomainChange::Whole;
            }
        }
        self
    }

    /// Mark every entry of `domain` as changed.
    pub fn insert_whole_domain(&mut self, domain: CacheDomain) -> &mut Self {
        self.domains.insert(domain, DomainChange::Whole);
        self
    }

    /// Add every change of `other`, under this set's fan-out cap.
    pub fn extend(&mut self, other: ChangeSet) -> &mut Self {
        for (domain, change) in other.domains {
            match change {
                DomainChange::Whole => {
                    self.insert_whole_domain(domain);
                }
                DomainChange::Keys(keys) => {
                    for key in keys.into_values() {
                        self.insert(key);
                    }
                }
            }
        }
        self
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.domains.is_empty()
    }

    /// Each changed domain with its encoded keys in sorted order, or `None` when the whole
    /// domain changed.
    pub fn encoded(
        &self,
    ) -> impl Iterator<Item = (CacheDomain, Option<impl Iterator<Item = &str>>)> {
        self.domains.iter().map(|(&domain, change)| match change {
            DomainChange::Keys(keys) => (domain, Some(keys.keys().map(String::as_str))),
            DomainChange::Whole => (domain, None),
        })
    }
}

#[cfg(test)]
mod tests {
    use strum::IntoEnumIterator;

    use super::*;

    fn warehouse(n: u128) -> CacheKey {
        CacheKey::Warehouse(WarehouseId::from(uuid::Uuid::from_u128(n)))
    }

    #[test]
    fn every_domain_name_parses_back() {
        for domain in CacheDomain::iter() {
            assert_eq!(CacheDomain::from_name(domain.as_str()), Some(domain));
        }
        assert_eq!(CacheDomain::from_name("cedar_scope"), None);
    }

    #[test]
    fn domain_names_are_the_stored_contract() {
        let names: Vec<_> = CacheDomain::iter().map(CacheDomain::as_str).collect();
        assert_eq!(
            names,
            [
                "warehouse",
                "namespace",
                "role",
                "user_assignments",
                "role_ancestors",
                "secret"
            ]
        );
    }

    #[test]
    fn every_key_kind_decodes_to_itself() {
        let project_id = ProjectId::new(uuid::Uuid::from_u128(7));
        let warehouse_id = WarehouseId::from(uuid::Uuid::from_u128(8));
        let keys = [
            warehouse(1),
            CacheKey::WarehouseName {
                project_id: project_id.clone(),
                name: "Sales [eu], \"main\"".to_string(),
            },
            CacheKey::Namespace(NamespaceId::from(uuid::Uuid::from_u128(2))),
            CacheKey::NamespaceIdent {
                warehouse_id,
                ident: NamespaceIdent::from_vec(vec!["a".into(), "b.c".into()]).unwrap(),
            },
            CacheKey::Role(RoleId::from(uuid::Uuid::from_u128(3))),
            CacheKey::RoleIdent {
                project_id,
                ident: RoleIdent::new_unchecked("oidc", "group-1"),
            },
            CacheKey::UserAssignments(UserId::try_from("oidc~user-1").unwrap()),
            CacheKey::UserAssignments(UserId::try_from("oidc~[user]").unwrap()),
            CacheKey::Secret(SecretId::from(uuid::Uuid::from_u128(4))),
        ];
        for key in keys {
            let encoded = key.encode();
            assert_eq!(CacheKey::decode(key.domain(), &encoded).unwrap(), key);
        }
    }

    #[test]
    fn ids_are_stored_as_is() {
        assert_eq!(
            warehouse(1).encode(),
            "00000000-0000-0000-0000-000000000001"
        );
        assert_eq!(
            CacheKey::UserAssignments(UserId::try_from("oidc~user-1").unwrap()).encode(),
            "oidc~user-1"
        );
    }

    #[test]
    fn a_key_of_another_shape_does_not_decode() {
        for (domain, key) in [
            (CacheDomain::Warehouse, "not-a-uuid"),
            (CacheDomain::Warehouse, "[\"unterminated"),
            (CacheDomain::Role, "[\"only-one\"]"),
            (CacheDomain::Secret, "[\"p\", \"n\"]"),
            (CacheDomain::UserAssignments, "[\"p\", \"n\"]"),
            (CacheDomain::UserAssignments, "no-separator"),
            (CacheDomain::RoleAncestors, "x"),
        ] {
            assert_eq!(
                CacheKey::decode(domain, key),
                Err(CacheKeyDecodeError {
                    domain,
                    key: key.to_string()
                })
            );
        }
    }

    fn encoded(set: &ChangeSet) -> Vec<(CacheDomain, Option<Vec<String>>)> {
        set.encoded()
            .map(|(domain, keys)| (domain, keys.map(|keys| keys.map(String::from).collect())))
            .collect()
    }

    #[test]
    fn an_empty_set_has_no_changes() {
        let set = ChangeSet::with_fanout_cap(10);
        assert!(set.is_empty());
        assert_eq!(encoded(&set), vec![]);
    }

    #[test]
    fn keys_are_sorted_and_de_duplicated_per_domain() {
        let mut set = ChangeSet::with_fanout_cap(10);
        set.insert(warehouse(2))
            .insert(warehouse(1))
            .insert(warehouse(2))
            .insert(CacheKey::Secret(SecretId::from(uuid::Uuid::from_u128(9))));
        assert!(!set.is_empty());
        assert_eq!(
            encoded(&set),
            vec![
                (
                    CacheDomain::Warehouse,
                    Some(vec![warehouse(1).encode(), warehouse(2).encode()])
                ),
                (
                    CacheDomain::Secret,
                    Some(vec!["00000000-0000-0000-0000-000000000009".to_string()])
                ),
            ]
        );
    }

    #[test]
    fn more_keys_than_the_cap_change_the_whole_domain() {
        let mut set = ChangeSet::with_fanout_cap(3);
        for n in 0..3 {
            set.insert(warehouse(n));
        }
        assert_eq!(encoded(&set)[0].1.as_ref().map(Vec::len), Some(3));
        set.insert(warehouse(3));
        assert_eq!(encoded(&set), vec![(CacheDomain::Warehouse, None)]);
        set.insert(warehouse(4));
        assert_eq!(encoded(&set), vec![(CacheDomain::Warehouse, None)]);
    }

    #[test]
    fn a_cap_of_zero_records_every_change_as_the_whole_domain() {
        let mut set = ChangeSet::with_fanout_cap(0);
        set.insert(warehouse(1));
        assert_eq!(encoded(&set), vec![(CacheDomain::Warehouse, None)]);
    }

    #[test]
    fn the_whole_domain_absorbs_its_keys() {
        let mut set = ChangeSet::with_fanout_cap(10);
        set.insert(CacheKey::Role(RoleId::from(uuid::Uuid::from_u128(1))))
            .insert_whole_domain(CacheDomain::Role)
            .insert(CacheKey::Role(RoleId::from(uuid::Uuid::from_u128(2))));
        assert_eq!(encoded(&set), vec![(CacheDomain::Role, None)]);
    }

    #[test]
    fn extend_merges_both_sets_under_the_receivers_cap() {
        let mut a = ChangeSet::with_fanout_cap(2);
        a.insert(warehouse(1));
        let mut b = ChangeSet::with_fanout_cap(10);
        b.insert(warehouse(2))
            .insert_whole_domain(CacheDomain::RoleAncestors);
        a.extend(b);
        assert_eq!(
            encoded(&a),
            vec![
                (
                    CacheDomain::Warehouse,
                    Some(vec![warehouse(1).encode(), warehouse(2).encode()])
                ),
                (CacheDomain::RoleAncestors, None),
            ]
        );

        let mut c = ChangeSet::with_fanout_cap(10);
        c.insert(warehouse(3));
        a.extend(c);
        assert_eq!(encoded(&a)[0], (CacheDomain::Warehouse, None));
    }
}
