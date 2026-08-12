//! Construction of the `ETag` that lets a conditional `loadTable` be answered
//! with `304 Not Modified`.
//!
//! This is Lakekeeper policy rather than Iceberg wire format — the `lk1.`
//! encoding is ours, and the inputs include our authorization model — so it
//! lives here and not in `iceberg-ext`. That crate keeps only the [`ETag`]
//! newtype and emits whatever tag it is handed.

use iceberg_ext::catalog::rest::ETag;
use xxhash_rust::xxh3::Xxh3Default;

use crate::{
    api::iceberg::v1::tables::{DataAccessMode, LoadTableFilters, SnapshotsQuery},
    service::{WarehouseVersion, storage::StoragePermissions},
};

/// Version prefix for structured `loadTable` [`ETag`]s. Anything not parsing
/// under this prefix (pre-upgrade or future-version values) isn't matched, so
/// the client reloads. Bump the suffix on incompatible encoding changes.
const ETAG_PREFIX: &str = "lk1";

/// One axis of a [`TableResponseShape`].
///
/// Implemented on the real domain types rather than on mirrors of them, so a
/// new variant is a compile error here instead of a silently untagged value.
///
/// The tags are written out by hand, not derived. [`std::hash::Hash`] would be
/// shorter but guarantees no stability across Rust releases, and these tags live
/// in client caches across deploys — a toolchain upgrade must not silently
/// change the wire format, and the pinned-value tests could not exist.
trait EtagAxis {
    /// Stable discriminant mixed into the hash. Changing an existing string
    /// invalidates every [`ETag`] carrying it.
    fn as_tag(&self) -> &'static str;
}

impl EtagAxis for SnapshotsQuery {
    fn as_tag(&self) -> &'static str {
        match self {
            SnapshotsQuery::All => "all",
            SnapshotsQuery::Refs => "refs",
        }
    }
}

impl EtagAxis for DataAccessMode {
    fn as_tag(&self) -> &'static str {
        match self {
            DataAccessMode::ClientManaged => "cm",
            DataAccessMode::ServerDelegated(access) => {
                match (access.vended_credentials, access.remote_signing) {
                    (false, false) => "d--",
                    (true, false) => "dv-",
                    (false, true) => "d-r",
                    (true, true) => "dvr",
                }
            }
        }
    }
}

impl EtagAxis for StoragePermissions {
    fn as_tag(&self) -> &'static str {
        match self {
            StoragePermissions::Read => "r",
            StoragePermissions::ReadWrite => "rw",
            StoragePermissions::ReadWriteDelete => "rwd",
        }
    }
}

/// The storage-config content of a response: either absent, or characterised by
/// the per-request inputs that shape it.
///
/// A sum rather than parallel fields, so "no config" cannot be paired with a
/// delegation or permission level that would mean nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StorageAccess {
    /// No `config` in the body at all — a commit response, or a caller with no
    /// storage access.
    NoConfig,
    /// Config present, scoped to this delegation and permission level, and
    /// generated from this revision of the warehouse's storage profile.
    ///
    /// The revision belongs here rather than alongside `snapshots`: a warehouse
    /// edit changes region, endpoint, KMS key or the STS toggle, none of which
    /// can reach a body that carries no config at all.
    Config {
        delegation: DataAccessMode,
        permissions: StoragePermissions,
        warehouse_version: WarehouseVersion,
    },
}

/// Identifies which response body a request would produce, at an unchanged
/// table metadata version.
///
/// Covers the inputs that vary the body per request: the snapshot filter and
/// the storage access the config was generated for. It is deliberately *not* a
/// claim to cover everything — `request_metadata` also reaches the body through
/// signer and credential-refresh URIs, which are bounded by the credential
/// window rather than by the tag.
///
/// Predicted, never measured: the conditional-request check runs before the
/// metadata is read and before any storage config is generated, so there is no
/// body to inspect. The same value is then attached to the response that gets
/// built, which is what keeps the two sides symmetric.
///
/// It is therefore an over-approximation. Two shapes may yield byte-identical
/// bodies, which costs only a cache miss; the requirement runs the other way —
/// one shape must never cover two different bodies. When adding an axis, prefer
/// splitting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TableResponseShape {
    snapshots: SnapshotsQuery,
    storage: StorageAccess,
}

impl TableResponseShape {
    pub(crate) fn new(snapshots: SnapshotsQuery, storage: StorageAccess) -> Self {
        Self { snapshots, storage }
    }

    /// Shape of a `loadTable` response.
    ///
    /// `filters` is destructured on purpose: a new field on [`LoadTableFilters`]
    /// must not compile until it has been considered here, since a body-affecting
    /// filter missing from the tag reintroduces the cross-shape 304.
    pub(crate) fn for_load(filters: &LoadTableFilters, storage: StorageAccess) -> Self {
        let LoadTableFilters { snapshots } = filters;
        Self::new(*snapshots, storage)
    }

    /// Shape of a response carrying the full metadata and no storage config.
    ///
    /// A commit response, and equally a load by a caller with no storage access —
    /// those bodies are genuinely equivalent, so sharing a tag is correct.
    pub(crate) fn no_storage_config() -> Self {
        Self::new(SnapshotsQuery::All, StorageAccess::NoConfig)
    }
}

/// Structured contents of a `loadTable` [`ETag`].
///
/// Wire form (inside the quotes): `lk1.<hash>`, or `lk1.<hash>.<revalidate_hex>`
/// when credentials are vended (revalidate-after as epoch-ms in hex). Embedding
/// the revalidation point lets the server decide, from the client-echoed tag
/// alone, whether the held credentials are still within their serve window —
/// i.e. fresh enough for a 304.
///
/// Emitted as a *weak* validator (`W/`): two responses sharing a tag are not
/// byte-identical, since each vends freshly minted credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableETag {
    hash: String,
    revalidate_after_ms: Option<i64>,
}

impl TableETag {
    pub(crate) fn new(
        metadata_location: &str,
        shape: TableResponseShape,
        revalidate_after_ms: Option<i64>,
    ) -> Self {
        // Destructured so a new axis cannot be added without being hashed here.
        let TableResponseShape { snapshots, storage } = shape;

        // NUL-separated: each axis occupies a fixed slot, so no combination of an
        // arbitrary metadata location and the fixed tags can be re-split into a
        // different combination that hashes the same.
        let mut hasher = Xxh3Default::new();
        hasher.update(metadata_location.as_bytes());
        hasher.update(&[0]);
        hasher.update(snapshots.as_tag().as_bytes());
        hasher.update(&[0]);
        match storage {
            StorageAccess::NoConfig => hasher.update(b"nc"),
            StorageAccess::Config {
                delegation,
                permissions,
                warehouse_version,
            } => {
                hasher.update(delegation.as_tag().as_bytes());
                hasher.update(&[0]);
                hasher.update(permissions.as_tag().as_bytes());
                hasher.update(&[0]);
                hasher.update(&warehouse_version.to_le_bytes());
            }
        }
        let hash = hasher.digest();

        Self {
            hash: format!("{hash:x}"),
            // A non-positive value carries no information; drop it.
            revalidate_after_ms: revalidate_after_ms.filter(|ms| *ms > 0),
        }
    }

    pub(crate) fn hash(&self) -> &str {
        &self.hash
    }

    pub(crate) fn revalidate_after_ms(&self) -> Option<i64> {
        self.revalidate_after_ms
    }

    /// Parse a client-supplied [`ETag`] value (quotes and any `W/` already
    /// stripped by `parse_etags`). Returns `None` for unrecognized values so
    /// callers reload.
    pub(crate) fn parse(value: &str) -> Option<Self> {
        let mut parts = value.split('.');
        if parts.next()? != ETAG_PREFIX {
            return None;
        }
        let hash = parts.next().filter(|s| !s.is_empty())?.to_string();
        let revalidate_after_ms = parts
            .next()
            .map(|s| i64::from_str_radix(s, 16))
            .transpose()
            .ok()?;
        // Reject trailing junk so an unexpected shape falls back to a reload.
        if parts.next().is_some() {
            return None;
        }
        Some(Self {
            hash,
            revalidate_after_ms,
        })
    }

    /// Render the wire [`ETag`] value, quoted and weak per HTTP syntax.
    pub(crate) fn into_etag(self) -> ETag {
        let inner = match self.revalidate_after_ms {
            Some(ms) => format!("{ETAG_PREFIX}.{}.{ms:x}", self.hash),
            None => format!("{ETAG_PREFIX}.{}", self.hash),
        };
        format!("W/\"{inner}\"").into()
    }
}

/// Mint the [`ETag`] for a commit response, which carries no storage config.
pub(crate) fn commit_etag(metadata_location: &str) -> ETag {
    TableETag::new(
        metadata_location,
        TableResponseShape::no_storage_config(),
        None,
    )
    .into_etag()
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::api::iceberg::v1::tables::DataAccess;

    const LOC: &str = "s3://bucket/table/metadata.json";
    fn wv() -> WarehouseVersion {
        WarehouseVersion::new(7)
    }

    fn delegated(vended_credentials: bool, remote_signing: bool) -> DataAccessMode {
        DataAccessMode::ServerDelegated(DataAccess {
            vended_credentials,
            remote_signing,
        })
    }

    /// Every shape the server can emit, for exhaustive distinctness checks.
    fn all_shapes() -> Vec<TableResponseShape> {
        let mut shapes = Vec::new();
        for snapshots in [SnapshotsQuery::All, SnapshotsQuery::Refs] {
            shapes.push(TableResponseShape::new(snapshots, StorageAccess::NoConfig));
            for delegation in [
                DataAccessMode::ClientManaged,
                delegated(false, false),
                delegated(true, false),
                delegated(false, true),
                delegated(true, true),
            ] {
                for permissions in [
                    StoragePermissions::Read,
                    StoragePermissions::ReadWrite,
                    StoragePermissions::ReadWriteDelete,
                ] {
                    shapes.push(TableResponseShape::new(
                        snapshots,
                        StorageAccess::Config {
                            delegation,
                            permissions,
                            warehouse_version: wv(),
                        },
                    ));
                }
            }
        }
        shapes
    }

    #[test]
    fn every_shape_gets_a_distinct_etag() {
        // The property the whole mechanism rests on, checked over the full
        // product rather than one axis at a time: two bodies that differ must
        // never share a tag. Catches a new variant copy-pasting an existing tag.
        let shapes = all_shapes();
        let mut seen = HashSet::new();
        for shape in &shapes {
            assert!(
                seen.insert(TableETag::new(LOC, *shape, None).into_etag()),
                "duplicate ETag for {shape:?}"
            );
        }
        assert_eq!(seen.len(), shapes.len());
        // 2 snapshot modes x (1 no-config + 5 delegations x 3 permission levels)
        assert_eq!(shapes.len(), 32);
    }

    #[test]
    fn warehouse_version_is_part_of_the_tag() {
        // A storage-profile edit bumps the warehouse row's version, which changes
        // region/endpoint/KMS key/STS toggle in the config body without touching
        // the metadata location. Without this axis a client-managed caller — whose
        // tag carries no revalidation point — would 304 onto the old config
        // indefinitely.
        let shape = |warehouse_version| {
            TableResponseShape::new(
                SnapshotsQuery::All,
                StorageAccess::Config {
                    delegation: delegated(false, false),
                    permissions: StoragePermissions::ReadWriteDelete,
                    warehouse_version,
                },
            )
        };
        assert_ne!(
            TableETag::new(LOC, shape(WarehouseVersion::new(1)), None),
            TableETag::new(LOC, shape(WarehouseVersion::new(2)), None)
        );
    }

    #[test]
    fn etag_round_trips_through_the_wire_form() {
        for revalidate in [None, Some(1_750_000_000_123)] {
            let etag = TableETag::new(LOC, all_shapes()[0], revalidate);
            let wire = etag.clone().into_etag();
            // `parse_etags` strips the quotes and the weak marker before we see it.
            let bare = wire.as_str().trim_start_matches("W/").trim_matches('"');
            assert_eq!(TableETag::parse(bare).as_ref(), Some(&etag));
            assert_eq!(etag.revalidate_after_ms(), revalidate);
        }
    }

    #[test]
    fn etag_is_emitted_as_a_weak_validator() {
        // Two responses sharing a tag are not byte-identical — each vends freshly
        // minted credentials — so the tag is weak per RFC 9110 8.8.1.
        let wire = TableETag::new(LOC, all_shapes()[0], None).into_etag();
        assert!(wire.as_str().starts_with("W/\""), "{}", wire.as_str());
        assert!(wire.as_str().ends_with('"'));
    }

    #[test]
    fn parse_rejects_legacy_and_junk() {
        // Legacy bare hash, wrong prefix, empty hash, trailing junk, non-hex expiry.
        assert!(TableETag::parse("e34615aade2e6333").is_none());
        assert!(TableETag::parse("lk2.abc").is_none());
        assert!(TableETag::parse("lk1.").is_none());
        assert!(TableETag::parse("lk1.abc.def.ghi").is_none());
        assert!(TableETag::parse("lk1.abc.zzz").is_none());
    }

    #[test]
    fn axis_tags_are_suffix_free_within_each_slot() {
        // What makes the NUL separators load-bearing. Without them the hash input
        // is a bare concatenation, and an arbitrary metadata location abutting a
        // fixed tag can be re-split: location `X` + tag `abc` and location `Xa` +
        // tag `bc` produce identical bytes. That needs one tag in the *same* slot
        // to be a suffix of another; across slots it cannot happen. Fails on a
        // future rename rather than silently allowing a collision.
        let slots: [(&str, Vec<&'static str>); 3] = [
            (
                "snapshots",
                vec![SnapshotsQuery::All.as_tag(), SnapshotsQuery::Refs.as_tag()],
            ),
            (
                "storage",
                vec![
                    "nc",
                    DataAccessMode::ClientManaged.as_tag(),
                    delegated(false, false).as_tag(),
                    delegated(true, false).as_tag(),
                    delegated(false, true).as_tag(),
                    delegated(true, true).as_tag(),
                ],
            ),
            (
                "permissions",
                vec![
                    StoragePermissions::Read.as_tag(),
                    StoragePermissions::ReadWrite.as_tag(),
                    StoragePermissions::ReadWriteDelete.as_tag(),
                ],
            ),
        ];
        for (slot, tags) in slots {
            for (i, a) in tags.iter().enumerate() {
                for (j, b) in tags.iter().enumerate() {
                    assert!(
                        i == j || !a.ends_with(b),
                        "{slot} tag {a:?} ends with {b:?}; without the NUL \
                         separators these could collide"
                    );
                }
            }
        }
    }

    #[test]
    fn commit_etag_matches_a_load_with_no_storage_access() {
        // A commit body and a no-storage-access load body are both metadata with
        // no config, so they are the same shape and sharing a tag is correct.
        // Every config-bearing load differs and must not match.
        let commit = commit_etag(LOC);
        assert_eq!(
            commit,
            TableETag::new(LOC, TableResponseShape::no_storage_config(), None).into_etag()
        );
        let delegated_load = TableETag::new(
            LOC,
            TableResponseShape::new(
                SnapshotsQuery::All,
                StorageAccess::Config {
                    delegation: delegated(false, false),
                    permissions: StoragePermissions::ReadWriteDelete,
                    warehouse_version: wv(),
                },
            ),
            None,
        )
        .into_etag();
        assert_ne!(commit, delegated_load);
    }
}
