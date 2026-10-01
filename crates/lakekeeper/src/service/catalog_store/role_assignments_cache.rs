use std::{
    collections::HashMap,
    fmt::Display,
    hash::{DefaultHasher, Hash, Hasher},
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use moka::{
    future::Cache,
    ops::compute::{CompResult, Op},
};

use crate::{
    CONFIG,
    service::{
        ArcProjectId, ArcRoleIdent, CatalogBackendError, RoleId,
        authn::UserId,
        cache_metrics,
        cache_ttl::JitteredTtl,
        catalog_store::role_assignment::{
            AssignedRole, ListRoleMembersResult, ListUserRoleAssignmentsResult,
        },
    },
};

// ============================================================================
// Counted caches: writes under the key lock, striped invalidation counts
// ============================================================================

/// Invalidation counts, striped by key hash into 256 stripes. Every invalidation
/// of a key bumps that key's stripe; an invalidation of another key bumps the same
/// stripe only when both keys hash to it.
struct Invalidations([AtomicU64; 256]);

impl Invalidations {
    const fn new() -> Self {
        Self([const { AtomicU64::new(0) }; 256])
    }

    fn stripe_index<K: Hash + ?Sized>(key: &K) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        usize::from(hasher.finish().to_le_bytes()[0])
    }

    fn stripe<K: Hash + ?Sized>(&self, key: &K) -> &AtomicU64 {
        &self.0[Self::stripe_index(key)]
    }

    fn read<K: Hash + ?Sized>(&self, key: &K) -> u64 {
        self.stripe(key).load(Ordering::Acquire)
    }

    fn bump<K: Hash + ?Sized>(&self, key: &K) {
        self.stripe(key).fetch_add(1, Ordering::AcqRel);
    }

    fn snapshot(&self) -> [u64; 256] {
        std::array::from_fn(|index| self.0[index].load(Ordering::Acquire))
    }

    /// Bump each stripe that holds one of `keys`, once.
    fn bump_many<K: Hash>(&self, keys: &[K]) {
        let mut bumped = [false; 256];
        for key in keys {
            let index = Self::stripe_index(key);
            if !bumped[index] {
                bumped[index] = true;
                self.0[index].fetch_add(1, Ordering::AcqRel);
            }
        }
    }
}

/// One key's invalidation count, read before a sync's transaction and passed to
/// [`CountedCache::commit_and_cache`]. Typed by the key, so a count only reaches a
/// cache with the same key type.
pub(super) struct InvalidationCount<K>(u64, PhantomData<fn(&K)>);

/// Every stripe of a [`CountedCache`]'s invalidation counts, for a writer that
/// learns its key only inside its transaction.
pub(super) struct InvalidationsSnapshot<K>([u64; 256], PhantomData<fn(&K)>);

impl<K: Hash> InvalidationsSnapshot<K> {
    /// `key`'s count when the snapshot was taken.
    pub(super) fn read(&self, key: &K) -> InvalidationCount<K> {
        InvalidationCount(self.0[Invalidations::stripe_index(key)], PhantomData)
    }
}

/// A moka cache that is written only under the per-key compute lock, together with
/// its striped invalidation counts.
///
/// Three kinds of write take the key lock: [`Self::commit_and_cache`] (a sync that
/// commits under the lock), the loaders ([`Self::get_or_load`],
/// [`Self::get_or_load_optional`]) and [`Self::invalidate`] (`Op::Remove`). A hit
/// takes no lock.
pub(super) struct CountedCache<K, V> {
    cache: Cache<K, V>,
    invalidations: Invalidations,
    enabled: bool,
    /// The `cache_type` label of the cache metrics.
    cache_type: &'static str,
    /// What an entry holds, for log messages.
    noun: &'static str,
}

impl<K, V> CountedCache<K, V>
where
    K: Hash + Eq + Clone + Display + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn new(
        cache_type: &'static str,
        noun: &'static str,
        enabled: bool,
        cache: Cache<K, V>,
    ) -> Self {
        Self {
            cache,
            invalidations: Invalidations::new(),
            enabled,
            cache_type,
            noun,
        }
    }

    /// `key`'s invalidation count, read before a sync's transaction and passed to
    /// [`Self::commit_and_cache`].
    pub(super) fn invalidations(&self, key: &K) -> InvalidationCount<K> {
        InvalidationCount(self.invalidations.read(key), PhantomData)
    }

    /// Every invalidation count, read before a sync's transaction when the sync
    /// resolves its key inside the transaction.
    pub(super) fn invalidations_snapshot(&self) -> InvalidationsSnapshot<K> {
        InvalidationsSnapshot(self.invalidations.snapshot(), PhantomData)
    }

    /// Run `commit`, then write `value` under `key`, both under the key's compute
    /// lock.
    ///
    /// Loaders and invalidations of the key take the same lock, so a load that read
    /// before this commit writes first, or skips its write, and this write replaces
    /// it; an invalidation that counts after the check below removes this write.
    /// `invalidations_before` is the key's count, read before the caller's
    /// transaction began. A writer that commits after this transaction's read
    /// invalidates the key after its commit, which bumps that stripe. So if the
    /// stripe moved, the writer may already have removed the key: the entry is
    /// removed and the next read loads it. A key sharing the stripe causes the same
    /// reload. A failed commit leaves the entry unchanged and returns its error.
    /// With the cache disabled this only commits.
    ///
    /// Lock order is database locks, then this key lock. Code run under the key lock
    /// must not wait for a database lock or a write-pool connection; the loaders only
    /// read, through the read pool.
    pub(super) async fn commit_and_cache<Fut, E>(
        &self,
        key: &K,
        value: V,
        invalidations_before: InvalidationCount<K>,
        commit: Fut,
    ) -> Result<(), E>
    where
        Fut: std::future::Future<Output = Result<(), E>> + Send,
        E: Send + Sync + 'static,
    {
        if !self.enabled {
            return commit.await;
        }
        let stripe = self.invalidations.stripe(key);
        let outcome = self
            .cache
            .entry(key.clone())
            .and_try_compute_with(|_| async move {
                commit.await?;
                if stripe.load(Ordering::Acquire) == invalidations_before.0 {
                    Ok(Op::Put(value))
                } else {
                    Ok(Op::Remove)
                }
            })
            .await?;
        if matches!(
            outcome,
            CompResult::Inserted(_) | CompResult::ReplacedWith(_)
        ) {
            tracing::debug!("Inserted {} for {key} into cache", self.noun);
        } else {
            tracing::debug!(
                "Left {} for {key} uncached after an overlapping invalidation",
                self.noun
            );
        }
        self.update_size_metric();
        Ok(())
    }

    async fn get(&self, key: &K) -> Option<V> {
        if !self.enabled {
            return None;
        }
        self.update_size_metric();
        if let Some(value) = self.cache.get(key).await {
            tracing::debug!("Found {} for {key} in cache", self.noun);
            cache_metrics::record_cache_hit(self.cache_type);
            Some(value)
        } else {
            cache_metrics::record_cache_miss(self.cache_type);
            None
        }
    }

    /// Count an invalidation of `key`, then remove its entry.
    ///
    /// The removal runs through the loader's per-key compute lock (`Op::Remove`), not
    /// a bare `invalidate()`: a bare invalidate is a different moka lock domain, so
    /// one landing mid-load is a no-op and the loader's later insert resurrects the
    /// revoked entry until TTL. `Op::Remove` orders this post-commit removal after
    /// any in-flight load's insert. See [`Self::get_or_load_optional`].
    async fn invalidate(&self, key: &K) {
        if self.enabled {
            tracing::debug!("Invalidating {} for {key} from cache", self.noun);
            self.invalidations.bump(key);
            self.remove(key).await;
            self.update_size_metric();
        }
    }

    /// [`Self::invalidate`] for every key in `keys`, bumping each distinct stripe
    /// once.
    async fn invalidate_many(&self, keys: &[K]) {
        if !self.enabled || keys.is_empty() {
            return;
        }
        self.invalidations.bump_many(keys);
        for key in keys {
            tracing::debug!("Invalidating {} for {key} from cache", self.noun);
            self.remove(key).await;
        }
        self.update_size_metric();
    }

    async fn remove(&self, key: &K) {
        self.cache
            .entry(key.clone())
            .and_compute_with(|_| async { Op::Remove })
            .await;
    }

    /// [`Self::get_or_load_optional`] for a loader that always finds its entry.
    pub(super) async fn get_or_load<Fut>(
        &self,
        key: &K,
        load: Fut,
    ) -> Result<V, CatalogBackendError>
    where
        Fut: std::future::Future<Output = Result<V, CatalogBackendError>> + Send,
    {
        self.get_or_load_optional(key, async move { load.await.map(Some) })
            .await?
            .ok_or_else(|| {
                CatalogBackendError::new_unexpected(std::io::Error::other(format!(
                    "{} cache compute returned no entry",
                    self.cache_type
                )))
            })
    }

    /// Single-flight read-through. A `None` from `load` is returned and never
    /// cached, so the entry stays absent.
    ///
    /// Concurrent misses for the same key coalesce onto one loader run that finds its
    /// entry, unless an invalidation counted during the load (see below). Otherwise
    /// each queued caller loads again. Hit/miss metrics and the `enabled` flag are
    /// preserved; with the cache disabled `load` runs directly. Errors are never
    /// cached: each serialized caller re-runs a failing load.
    ///
    /// Uses `and_try_compute_with`: moka holds the per-key compute lock across the
    /// `load` await, and [`Self::invalidate`] removes through that same lock
    /// (`Op::Remove`). So a removal racing an in-flight load runs after this
    /// loader's insert or before the load starts.
    ///
    /// The loader also reads the key's invalidation count before `load` and caches
    /// the result only if the count is unchanged. So an invalidation that counts
    /// during the load leaves the result uncached, also when its `Op::Remove` never
    /// runs. One that counts after that check and whose request is cancelled before
    /// its `Op::Remove` runs leaves this insert cached until the TTL.
    pub(super) async fn get_or_load_optional<Fut>(
        &self,
        key: &K,
        load: Fut,
    ) -> Result<Option<V>, CatalogBackendError>
    where
        Fut: std::future::Future<Output = Result<Option<V>, CatalogBackendError>> + Send,
    {
        if !self.enabled {
            return load.await;
        }
        // Fast path: a hit returns the stored value.
        if let Some(cached) = self.get(key).await {
            return Ok(Some(cached));
        }

        // Miss (already counted by the get above).
        let stripe = self.invalidations.stripe(key);
        let mut uncached = None;
        let uncached_slot = &mut uncached;
        let outcome = self
            .cache
            .entry(key.clone())
            .and_try_compute_with(|maybe_entry| async move {
                if maybe_entry.is_some() {
                    // Populated by another caller while we waited on the key lock.
                    return Ok::<_, CatalogBackendError>(Op::Nop);
                }
                let invalidations_before = stripe.load(Ordering::Acquire);
                match load.await? {
                    Some(value) if stripe.load(Ordering::Acquire) == invalidations_before => {
                        Ok(Op::Put(value))
                    }
                    Some(value) => {
                        *uncached_slot = Some(value);
                        Ok(Op::Nop)
                    }
                    None => Ok(Op::Nop),
                }
            })
            .await?;
        self.update_size_metric();
        if let Some(loaded) = uncached {
            return Ok(Some(loaded));
        }

        Ok(match outcome {
            CompResult::Inserted(entry)
            | CompResult::ReplacedWith(entry)
            | CompResult::Unchanged(entry) => Some(entry.into_value()),
            // `StillNone`: the loader returned `None`. `Removed` is unreachable, since
            // the closure returns only `Nop` or `Put`.
            CompResult::StillNone(_) | CompResult::Removed(_) => None,
        })
    }

    fn update_size_metric(&self) {
        cache_metrics::set_cache_size(self.cache_type, self.cache.entry_count());
    }
}

// ============================================================================
// User assignments cache  (UserId → Arc<ListUserRoleAssignmentsResult>)
// ============================================================================

/// Hot path: one entry per active user.
///
/// Value is `Arc`-wrapped, so every caller receives an O(1) pointer clone of the
/// `Vec<AssignedRole>`.
pub(super) static USER_ASSIGNMENTS_CACHE: std::sync::LazyLock<
    CountedCache<UserId, Arc<ListUserRoleAssignmentsResult>>,
> = std::sync::LazyLock::new(|| {
    CountedCache::new(
        "user_assignments",
        "user assignments",
        CONFIG.cache.user_assignments.enabled,
        Cache::builder()
            .max_capacity(CONFIG.cache.user_assignments.capacity)
            .initial_capacity(1_000)
            .time_to_live(Duration::from_secs(
                CONFIG.cache.user_assignments.time_to_live_secs,
            ))
            .expire_after(JitteredTtl::with_default_jitter(Duration::from_secs(
                CONFIG.cache.user_assignments.time_to_live_secs,
            )))
            .build(),
    )
});

#[allow(dead_code)] // Not required for all features
pub(crate) async fn user_assignments_cache_invalidate(user_id: &UserId) {
    USER_ASSIGNMENTS_CACHE.invalidate(user_id).await;
}

/// Invalidate the user-assignments cache entry for every user in `user_ids`.
///
/// Convenience over calling [`user_assignments_cache_invalidate`] in a loop —
/// used when a single mutation (e.g. a `role_membership` edge change) makes the
/// effective-role list of a whole set of users stale at once.
pub(crate) async fn user_assignments_cache_invalidate_many(user_ids: &[UserId]) {
    USER_ASSIGNMENTS_CACHE.invalidate_many(user_ids).await;
}

// ============================================================================
// Shared identity pools — dedup Arc<RoleIdent>/Arc<ProjectId> across cached entries
// ============================================================================
//
// The effective-roles loader allocates a fresh `Arc` for each (user, role) row,
// so without sharing a role held by N users keeps N copies of its identity alive.
// Sharing collapses those to one `Arc`. Content-addressed: the key is the `Arc`
// itself, whose `Hash`/`Eq` delegate to the inner value, so a rename produces a
// new ident → new key → self-heals, and stale (renamed/deleted) idents age out by
// idle-eviction — no invalidation hook required. The key clone and the stored
// value share one allocation. The shared `Arc` is strong: the canonical instance
// stays alive via cached entries even if the pool evicts the key, so eviction
// only transiently reduces sharing, never correctness.
//
// Sizing is INTERNAL, not an operator knob, and deliberately NOT tied to the
// role-by-id cache (`cache.role`): the pools serve `USER_ASSIGNMENTS_CACHE`, so
// their idle-TTL tracks *that* cache's TTL, and their capacity is a fixed bound on
// distinct identities in play. Above the capacity, dedup degrades (cold idents
// are LRU-evicted and re-allocated on the next load) but is never incorrect. We
// keep `moka` (sharded, lock-free reads) rather than a hand-rolled weak-value map
// precisely so this stays uncontended under the lazy per-user role-provider sync
// load. `share_identities` is gated on `user_assignments.enabled`, so when that
// cache is off the pools are never populated.
//
// Future option (deferred): if true self-sizing (no fixed cap) is ever wanted,
// swap these for weak-value pools whose canonical `Arc`s are kept alive by the
// cached entries. That design needs a periodic dead-`Weak` sweep under a lock —
// if that sweep (or the lock) ever becomes a bottleneck, shard it (an array of
// locked maps keyed by hash, or a `DashMap`). Not needed now: `moka` is already
// sharded/concurrent, and the fixed cap only degrades dedup, never correctness.

/// Upper bound on distinct shared identities. Generous — far above any realistic
/// distinct-role count (the role-by-id cache defaults to 10k). Exceeding it only
/// degrades dedup, never correctness, so it is a fixed internal constant rather
/// than an operator-facing knob.
const MAX_SHARED_IDENTITIES: u64 = 100_000;

const CACHE_TYPE_SHARED_ROLE_IDENTS: &str = "shared_role_idents";
const CACHE_TYPE_SHARED_PROJECT_IDS: &str = "shared_project_ids";

static SHARED_ROLE_IDENTS: std::sync::LazyLock<Cache<ArcRoleIdent, ArcRoleIdent>> =
    std::sync::LazyLock::new(|| {
        Cache::builder()
            .max_capacity(MAX_SHARED_IDENTITIES)
            .time_to_idle(Duration::from_secs(
                CONFIG.cache.user_assignments.time_to_live_secs,
            ))
            .build()
    });

static SHARED_PROJECT_IDS: std::sync::LazyLock<Cache<ArcProjectId, ArcProjectId>> =
    std::sync::LazyLock::new(|| {
        Cache::builder()
            .max_capacity(MAX_SHARED_IDENTITIES)
            .time_to_idle(Duration::from_secs(
                CONFIG.cache.user_assignments.time_to_live_secs,
            ))
            .build()
    });

async fn share_role_ident(ident: ArcRoleIdent) -> ArcRoleIdent {
    SHARED_ROLE_IDENTS
        .get_with(Arc::clone(&ident), async move { ident })
        .await
}

async fn share_project_id(project_id: ArcProjectId) -> ArcProjectId {
    SHARED_PROJECT_IDS
        .get_with(Arc::clone(&project_id), async move { project_id })
        .await
}

/// Replace the per-row `Arc<RoleIdent>` / `Arc<ProjectId>` in a freshly-loaded
/// user-assignments result with shared `Arc`s before it is cached, so a
/// role/project referenced by many users is stored once in memory. No-op when the
/// user-assignments cache is disabled (nothing is cached → nothing to dedup).
pub(super) async fn share_identities(result: &mut ListUserRoleAssignmentsResult) {
    if !CONFIG.cache.user_assignments.enabled {
        return;
    }
    for role in &mut result.roles {
        role.role_ident = share_role_ident(Arc::clone(&role.role_ident)).await;
        role.project_id = share_project_id(Arc::clone(&role.project_id)).await;
    }
    for sync in &mut result.provider_sync_times {
        sync.project_id = share_project_id(Arc::clone(&sync.project_id)).await;
    }
    update_shared_identity_metrics();
}

/// Gauge the shared-identity pools' entry counts so dedup can be confirmed in
/// prod: compare these against `cache_size{cache_type="user_assignments"}` — a
/// small pool size relative to UA entries means a role/project held by many users
/// is stored once. Reuses the shared `lakekeeper_cache_size` gauge with dedicated
/// `cache_type` labels. `entry_count()` is approximate until moka drains pending
/// tasks (same caveat the UA/RM gauges already accept).
#[inline]
fn update_shared_identity_metrics() {
    cache_metrics::set_cache_size(
        CACHE_TYPE_SHARED_ROLE_IDENTS,
        SHARED_ROLE_IDENTS.entry_count(),
    );
    cache_metrics::set_cache_size(
        CACHE_TYPE_SHARED_PROJECT_IDS,
        SHARED_PROJECT_IDS.entry_count(),
    );
}

// ============================================================================
// Role members cache  (RoleId → Arc<ListRoleMembersResult>)
// ============================================================================

/// Cold path: one entry per queried role. `RoleId` is `Copy` (UUID).
///
/// Value is `Arc`-wrapped because each entry may hold an arbitrarily large
/// `Vec<AssignedUser>`.
pub(super) static ROLE_MEMBERS_CACHE: std::sync::LazyLock<
    CountedCache<RoleId, Arc<ListRoleMembersResult>>,
> = std::sync::LazyLock::new(|| {
    CountedCache::new(
        "role_members",
        "role members",
        CONFIG.cache.role_members.enabled,
        Cache::builder()
            .max_capacity(CONFIG.cache.role_members.capacity)
            .initial_capacity(100)
            .time_to_live(Duration::from_secs(
                CONFIG.cache.role_members.time_to_live_secs,
            ))
            .expire_after(JitteredTtl::with_default_jitter(Duration::from_secs(
                CONFIG.cache.role_members.time_to_live_secs,
            )))
            .build(),
    )
});

#[allow(dead_code)] // Not required for all features
pub(crate) async fn role_members_cache_invalidate(role_id: RoleId) {
    ROLE_MEMBERS_CACHE.invalidate(&role_id).await;
}

// ============================================================================
// Role ancestors cache  (RoleId → Arc<Vec<AssignedRole>>)
// ============================================================================

const CACHE_TYPE_RA: &str = "role_ancestors";

/// One entry per role an authorization request has named, holding at most
/// `CONFIG.role.max_nesting_depth` levels — the write path rejects an edge that would
/// exceed it. Most roles are nested in nothing, so the common entry is an empty `Vec`:
/// cheap to hold, and worth holding, since it saves the round-trip that proves it.
pub(crate) static ROLE_ANCESTORS_CACHE: std::sync::LazyLock<Cache<RoleId, Arc<Vec<AssignedRole>>>> =
    std::sync::LazyLock::new(|| {
        Cache::builder()
            .max_capacity(CONFIG.cache.role_ancestors.capacity)
            .initial_capacity(100)
            .time_to_live(Duration::from_secs(
                CONFIG.cache.role_ancestors.time_to_live_secs,
            ))
            .expire_after(JitteredTtl::with_default_jitter(Duration::from_secs(
                CONFIG.cache.role_ancestors.time_to_live_secs,
            )))
            .build()
    });

async fn role_ancestors_cache_get(role_id: RoleId) -> Option<Arc<Vec<AssignedRole>>> {
    if !CONFIG.cache.role_ancestors.enabled {
        return None;
    }
    update_ra_size_metric();
    if let Some(result) = ROLE_ANCESTORS_CACHE.get(&role_id).await {
        tracing::debug!("Role ancestors for {role_id} found in cache");
        cache_metrics::record_cache_hit(CACHE_TYPE_RA);
        Some(result)
    } else {
        cache_metrics::record_cache_miss(CACHE_TYPE_RA);
        None
    }
}

/// Drop every entry, because a single membership edge changes the ancestor set of the
/// member *and* of everything nested beneath it.
///
/// Deliberately blunt. Evicting only the affected keys would mean walking the member's
/// descendant closure — a second recursive query on a write path that already runs one to
/// find affected users — and clearing more than necessary is never wrong, only wasteful.
/// Role-membership edits are rare administrative writes, so the lost hit rate is a
/// worthwhile trade for not having a second closure walk to keep correct. Call it wherever
/// the membership graph, or the identity of any role in it, changes.
///
/// Local to this process. Other replicas keep serving their own entries until TTL, and the
/// staleness is not symmetric: a *removed* edge stays visible, so a policy written against
/// the parent role keeps applying to the former member for up to one TTL. That direction is
/// permissive, which is why the TTL is short rather than generous.
/// Bumped on every clear, so a load that started before it cannot cache what it read.
///
/// `invalidate_all` stamps a validity time rather than removing, so an insert with a later
/// timestamp survives the clear -- and a bare invalidate is a different moka lock domain
/// from the loader, so no per-key lock orders the two. `Op::Remove` through the compute
/// lock is how the user-assignments cache solves this; it does not compose with a batched
/// loader, which would make the batch the unit of locking and serialise unrelated roles.
static ROLE_ANCESTORS_EPOCH: AtomicU64 = AtomicU64::new(0);

pub(crate) fn role_ancestors_cache_invalidate_all() {
    if CONFIG.cache.role_ancestors.enabled {
        tracing::debug!("Invalidating all role ancestors from cache");
        // Before the clear: a load finishing in between still sees a changed epoch.
        ROLE_ANCESTORS_EPOCH.fetch_add(1, Ordering::AcqRel);
        ROLE_ANCESTORS_CACHE.invalidate_all();
        update_ra_size_metric();
    }
}

#[inline]
fn update_ra_size_metric() {
    cache_metrics::set_cache_size(CACHE_TYPE_RA, ROLE_ANCESTORS_CACHE.entry_count());
}

/// Read-through for the role-ancestors cache, over any number of roles.
///
/// `load` is called once with the roles that missed, and must return an entry for **every**
/// one it was given. A short result is a backend error, not an answer: an absent role cannot
/// be told apart from one nested in nothing, and reading it as nothing silently unscopes
/// every policy written for a parent role. Checked on the uncached path too -- the setting an
/// operator disables because they distrust the cache must not remove the guarantee.
///
/// Not single-flight: concurrent misses on one role each run a loader rather than coalescing.
/// [`ROLE_ANCESTORS_EPOCH`] is what keeps that safe -- a load whose result is already stale
/// answers its own caller but is not cached, so neither a clear nor a faster loader can be
/// undone by a slower one.
pub(super) async fn role_ancestors_cache_get_or_load<F, Fut>(
    role_ids: &[RoleId],
    load: F,
) -> Result<HashMap<RoleId, Arc<Vec<AssignedRole>>>, CatalogBackendError>
where
    F: FnOnce(Vec<RoleId>) -> Fut,
    Fut: std::future::Future<
            Output = Result<HashMap<RoleId, Arc<Vec<AssignedRole>>>, CatalogBackendError>,
        > + Send,
{
    let enabled = CONFIG.cache.role_ancestors.enabled;
    let mut resolved: HashMap<RoleId, Arc<Vec<AssignedRole>>> =
        HashMap::with_capacity(role_ids.len());
    let mut missing = Vec::new();
    if enabled {
        for role_id in role_ids {
            match role_ancestors_cache_get(*role_id).await {
                Some(cached) => {
                    resolved.insert(*role_id, cached);
                }
                None => missing.push(*role_id),
            }
        }
    } else {
        missing.extend_from_slice(role_ids);
    }
    if missing.is_empty() {
        return Ok(resolved);
    }

    let epoch_before = ROLE_ANCESTORS_EPOCH.load(Ordering::Acquire);
    let loaded = load(missing.clone()).await?;
    let mut cached_any = false;

    for role_id in missing {
        let ancestors = loaded.get(&role_id).ok_or_else(|| {
            CatalogBackendError::new_unexpected(std::io::Error::other(format!(
                "role-ancestors load returned no entry for {role_id}"
            )))
        })?;
        // Re-read per insert, not once for the batch: this loop awaits, so a
        // clear can land between two inserts, and a later insert would then
        // outlive it and keep revoked nesting for a TTL.
        if enabled && ROLE_ANCESTORS_EPOCH.load(Ordering::Acquire) == epoch_before {
            ROLE_ANCESTORS_CACHE
                .insert(role_id, Arc::clone(ancestors))
                .await;
            cached_any = true;
        }
        resolved.insert(role_id, Arc::clone(ancestors));
    }
    if cached_any {
        update_ra_size_metric();
    }

    Ok(resolved)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{
        ProjectId,
        service::{
            ArcProjectId, RoleId, RoleIdent, RoleProviderId,
            authn::UserId,
            catalog_store::role_assignment::{
                AssignedRole, AssignedUser, ListRoleMembersResult, ListUserRoleAssignmentsResult,
                UserProviderSyncInfo,
            },
            identifier::role::{ArcRoleIdent, RoleSourceId},
        },
    };

    type UaCache = CountedCache<UserId, Arc<ListUserRoleAssignmentsResult>>;
    type RmCache = CountedCache<RoleId, Arc<ListRoleMembersResult>>;

    /// A user-assignments cache of the calling test's own, so other tests' entries and
    /// invalidations cannot reach it.
    fn ua_cache() -> Arc<UaCache> {
        Arc::new(CountedCache::new(
            "test_user_assignments",
            "user assignments",
            true,
            Cache::new(1_000),
        ))
    }

    /// A role-members cache of the calling test's own; see [`ua_cache`].
    fn rm_cache() -> Arc<RmCache> {
        Arc::new(CountedCache::new(
            "test_role_members",
            "role members",
            true,
            Cache::new(1_000),
        ))
    }

    /// A sync of `key`: read its invalidation count, then commit and cache `value`.
    async fn sync_for_test<K, V>(
        cache: &CountedCache<K, V>,
        key: &K,
        value: V,
        commit: impl std::future::Future<Output = Result<(), CatalogBackendError>> + Send,
    ) -> Result<(), CatalogBackendError>
    where
        K: Hash + Eq + Clone + Display + Send + Sync + 'static,
        V: Clone + Send + Sync + 'static,
    {
        let invalidations_before = cache.invalidations(key);
        cache
            .commit_and_cache(key, value, invalidations_before, commit)
            .await
    }

    async fn insert_for_test<K, V>(cache: &CountedCache<K, V>, key: &K, value: V)
    where
        K: Hash + Eq + Clone + Display + Send + Sync + 'static,
        V: Clone + Send + Sync + 'static,
    {
        sync_for_test(cache, key, value, async { Ok(()) })
            .await
            .expect("a no-op commit succeeds");
    }

    fn test_user_id(s: &str) -> UserId {
        serde_json::from_str(&format!(r#""oidc~{s}""#)).unwrap()
    }

    fn test_role_ident(provider: &str, source: &str) -> ArcRoleIdent {
        Arc::new(RoleIdent::new(
            RoleProviderId::try_new(provider).unwrap(),
            RoleSourceId::try_new(source).unwrap(),
        ))
    }

    fn empty_user_result() -> Arc<ListUserRoleAssignmentsResult> {
        Arc::new(ListUserRoleAssignmentsResult {
            roles: vec![],
            provider_sync_times: vec![],
        })
    }

    fn user_result_with_role(
        role_id: RoleId,
        project_id: ArcProjectId,
        role_ident: ArcRoleIdent,
    ) -> Arc<ListUserRoleAssignmentsResult> {
        Arc::new(ListUserRoleAssignmentsResult {
            roles: vec![AssignedRole {
                role_id,
                role_ident,
                project_id,
            }],
            provider_sync_times: vec![],
        })
    }

    fn empty_role_result(role_id: RoleId) -> Arc<ListRoleMembersResult> {
        Arc::new(ListRoleMembersResult {
            role_id,
            project_id: Arc::new(ProjectId::new_random()),
            role_ident: test_role_ident("lakekeeper", "empty"),
            members: vec![],
            last_synced_at: None,
        })
    }

    fn role_result_with_members(
        role_id: RoleId,
        user_ids: Vec<UserId>,
    ) -> Arc<ListRoleMembersResult> {
        Arc::new(ListRoleMembersResult {
            role_id,
            project_id: Arc::new(ProjectId::new_random()),
            role_ident: test_role_ident("lakekeeper", "with-members"),
            members: user_ids
                .into_iter()
                .map(|user_id| AssignedUser {
                    user_id: Arc::new(user_id),
                })
                .collect(),
            last_synced_at: Some(chrono::Utc::now()),
        })
    }

    // ── User assignments ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_user_assignments_insert_and_get() {
        let cache = ua_cache();
        let user_id = test_user_id("insert-get");
        insert_for_test(&cache, &user_id, empty_user_result()).await;

        let cached = cache.get(&user_id).await;
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().roles.len(), 0);
    }

    #[tokio::test]
    async fn test_user_assignments_miss() {
        let cache = ua_cache();
        let user_id = test_user_id("never-inserted-ua");
        assert!(cache.get(&user_id).await.is_none());
    }

    #[tokio::test]
    async fn test_user_assignments_invalidate() {
        let cache = ua_cache();
        let user_id = test_user_id("invalidate-ua");
        insert_for_test(&cache, &user_id, empty_user_result()).await;
        assert!(cache.get(&user_id).await.is_some());

        cache.invalidate(&user_id).await;
        assert!(cache.get(&user_id).await.is_none());
    }

    /// Regression: a revocation racing an in-flight loader must win — no resurrecting
    /// the revoked entry. Deterministic: the loader holds the key's compute lock
    /// across its `await`, so the invalidate's `Op::Remove` is ordered after the
    /// loader's insert and removes it. (Before the fix the loader used `try_get_with`
    /// and the invalidate a bare, different-lock-domain `invalidate()` — a no-op
    /// mid-load, and the insert resurrected the grant until TTL.)
    #[tokio::test]
    async fn invalidate_wins_over_in_flight_user_assignments_loader() {
        use tokio::sync::oneshot;

        let cache = ua_cache();
        let user_id = test_user_id("race-invalidate-vs-loader");

        let (started_tx, started_rx) = oneshot::channel::<()>();
        let (release_tx, release_rx) = oneshot::channel::<()>();

        let stale = user_result_with_role(
            RoleId::new_random(),
            Arc::new(ProjectId::new_random()),
            test_role_ident("lakekeeper", "stale-grant"),
        );
        let stale_for_loader = Arc::clone(&stale);

        // The loader runs inside the compute closure (holding the key lock). It
        // signals once mid-flight, then blocks until the test releases it before
        // returning the now-stale snapshot.
        let uid = user_id.clone();
        let cache_task = Arc::clone(&cache);
        let loader = tokio::spawn(async move {
            let load = async move {
                started_tx.send(()).unwrap();
                release_rx.await.unwrap();
                Ok(stale_for_loader)
            };
            cache_task.get_or_load(&uid, load).await
        });

        // Once the loader holds the key lock, fire the revocation. Its `Op::Remove`
        // queues behind the loader on the same key lock.
        started_rx.await.unwrap();
        let inv_uid = user_id.clone();
        let cache_task = Arc::clone(&cache);
        let invalidate = tokio::spawn(async move {
            cache_task.invalidate(&inv_uid).await;
        });

        release_tx.send(()).unwrap();
        let returned = loader.await.unwrap().expect("loader succeeds");
        invalidate.await.unwrap();

        // The loader still returns its snapshot to *its* caller (a read that raced a
        // write — acceptable) ...
        assert_eq!(returned.roles.len(), 1);
        // ... but the cache must NOT retain the revoked grant: the invalidate won.
        assert!(
            cache.get(&user_id).await.is_none(),
            "revoked grant was resurrected by the racing loader"
        );
    }

    /// A load that read before a sync committed cannot overwrite the sync's result: the
    /// sync's commit waits for the load's write, then replaces it.
    #[tokio::test]
    async fn a_sync_racing_a_load_leaves_the_synced_entry() {
        use std::sync::atomic::{AtomicBool, Ordering};

        use tokio::sync::oneshot;

        let cache = ua_cache();
        let user_id = test_user_id("race-sync-vs-loader");

        let (started_tx, started_rx) = oneshot::channel::<()>();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let before_sync = user_result_with_role(
            RoleId::new_random(),
            Arc::new(ProjectId::new_random()),
            test_role_ident("lakekeeper", "before-sync"),
        );
        let after_sync = user_result_with_role(
            RoleId::new_random(),
            Arc::new(ProjectId::new_random()),
            test_role_ident("lakekeeper", "after-sync"),
        );

        let uid = user_id.clone();
        let cache_task = Arc::clone(&cache);
        let loader = tokio::spawn(async move {
            let load = async move {
                started_tx.send(()).unwrap();
                release_rx.await.unwrap();
                Ok(before_sync)
            };
            cache_task.get_or_load(&uid, load).await
        });
        started_rx.await.unwrap();

        let committed = Arc::new(AtomicBool::new(false));
        let cache_task = Arc::clone(&cache);
        let sync = tokio::spawn({
            let uid = user_id.clone();
            let committed = Arc::clone(&committed);
            let after_sync = Arc::clone(&after_sync);
            async move {
                sync_for_test(&cache_task, &uid, after_sync, async move {
                    committed.store(true, Ordering::SeqCst);
                    Ok::<_, CatalogBackendError>(())
                })
                .await
            }
        });
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        assert!(
            !committed.load(Ordering::SeqCst),
            "the commit waits for the in-flight load"
        );

        release_tx.send(()).unwrap();
        loader.await.unwrap().expect("loader succeeds");
        sync.await.unwrap().expect("sync succeeds");

        let cached = cache.get(&user_id).await.expect("cached");
        assert!(
            Arc::ptr_eq(&cached, &after_sync),
            "the synced state stays cached"
        );
    }

    /// Two syncs of one user write the cache in commit order, so the later one stays.
    #[tokio::test]
    async fn two_syncs_leave_the_later_commit_cached() {
        use std::sync::atomic::{AtomicBool, Ordering};

        use tokio::sync::oneshot;

        let cache = ua_cache();
        let user_id = test_user_id("race-sync-vs-sync");

        let (started_tx, started_rx) = oneshot::channel::<()>();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let first = user_result_with_role(
            RoleId::new_random(),
            Arc::new(ProjectId::new_random()),
            test_role_ident("lakekeeper", "first-sync"),
        );
        let second = user_result_with_role(
            RoleId::new_random(),
            Arc::new(ProjectId::new_random()),
            test_role_ident("lakekeeper", "second-sync"),
        );

        let cache_task = Arc::clone(&cache);
        let first_sync = tokio::spawn({
            let uid = user_id.clone();
            async move {
                sync_for_test(&cache_task, &uid, first, async move {
                    started_tx.send(()).unwrap();
                    release_rx.await.unwrap();
                    Ok::<_, CatalogBackendError>(())
                })
                .await
            }
        });
        started_rx.await.unwrap();

        let second_committed = Arc::new(AtomicBool::new(false));
        let cache_task = Arc::clone(&cache);
        let second_sync = tokio::spawn({
            let uid = user_id.clone();
            let second_committed = Arc::clone(&second_committed);
            let second = Arc::clone(&second);
            async move {
                sync_for_test(&cache_task, &uid, second, async move {
                    second_committed.store(true, Ordering::SeqCst);
                    Ok::<_, CatalogBackendError>(())
                })
                .await
            }
        });
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        assert!(
            !second_committed.load(Ordering::SeqCst),
            "the second commit waits for the first sync's cache write"
        );

        release_tx.send(()).unwrap();
        first_sync.await.unwrap().expect("first sync succeeds");
        second_sync.await.unwrap().expect("second sync succeeds");

        let cached = cache.get(&user_id).await.expect("cached");
        assert!(
            Arc::ptr_eq(&cached, &second),
            "the later commit stays cached"
        );
    }

    /// A failed commit returns its error and leaves the cached entry as it was.
    #[tokio::test]
    async fn a_failed_commit_caches_nothing() {
        let cache = ua_cache();
        let user_id = test_user_id("failed-commit");
        let existing = empty_user_result();
        insert_for_test(&cache, &user_id, Arc::clone(&existing)).await;

        let err = sync_for_test(
            &cache,
            &user_id,
            user_result_with_role(
                RoleId::new_random(),
                Arc::new(ProjectId::new_random()),
                test_role_ident("lakekeeper", "uncommitted"),
            ),
            async {
                Err(CatalogBackendError::new_unexpected(std::io::Error::other(
                    "boom",
                )))
            },
        )
        .await;
        assert!(err.is_err(), "the commit error reaches the caller");

        let cached = cache.get(&user_id).await.expect("cached");
        assert!(
            Arc::ptr_eq(&cached, &existing),
            "the uncommitted state is not cached"
        );
    }

    /// A role-assignment removal that commits between a sync's read and its commit
    /// invalidates before the sync takes the key lock. The sync's snapshot still holds
    /// the removed role, so the sync removes the entry and the next read loads.
    #[tokio::test]
    async fn an_invalidation_during_a_sync_removes_its_entry() {
        let cache = ua_cache();
        let user_id = test_user_id("invalidation-during-sync");
        let removed_role = user_result_with_role(
            RoleId::new_random(),
            Arc::new(ProjectId::new_random()),
            test_role_ident("lakekeeper", "removed-during-sync"),
        );
        insert_for_test(&cache, &user_id, Arc::clone(&removed_role)).await;

        let invalidations_before = cache.invalidations(&user_id);
        // The removal commits and invalidates while the sync's transaction is open.
        cache.invalidate(&user_id).await;

        cache
            .commit_and_cache(
                &user_id,
                Arc::clone(&removed_role),
                invalidations_before,
                async { Ok::<_, CatalogBackendError>(()) },
            )
            .await
            .expect("sync succeeds");
        assert!(
            cache.get(&user_id).await.is_none(),
            "the sync's pre-removal snapshot is not cached"
        );
    }

    /// Without an invalidation since its read, a sync caches its result.
    #[tokio::test]
    async fn a_sync_without_an_invalidation_caches_its_entry() {
        let cache = ua_cache();
        let user_id = test_user_id("sync-without-invalidation");
        let synced = empty_user_result();

        sync_for_test(&cache, &user_id, Arc::clone(&synced), async { Ok(()) })
            .await
            .expect("sync succeeds");
        let entry = cache.get(&user_id).await.expect("cached");
        assert!(Arc::ptr_eq(&entry, &synced));
    }

    /// Role members: a load that read before a sync committed cannot overwrite the
    /// sync's result.
    #[tokio::test]
    async fn a_role_members_sync_racing_a_load_leaves_the_synced_entry() {
        use std::sync::atomic::{AtomicBool, Ordering};

        use tokio::sync::oneshot;

        let cache = rm_cache();
        let role_id = RoleId::new_random();
        let (started_tx, started_rx) = oneshot::channel::<()>();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let before_sync = role_result_with_members(role_id, vec![test_user_id("rm-before-sync")]);
        let after_sync = role_result_with_members(role_id, vec![test_user_id("rm-after-sync")]);

        let cache_task = Arc::clone(&cache);
        let loader = tokio::spawn(async move {
            let load = async move {
                started_tx.send(()).unwrap();
                release_rx.await.unwrap();
                Ok(Some(before_sync))
            };
            cache_task.get_or_load_optional(&role_id, load).await
        });
        started_rx.await.unwrap();

        let committed = Arc::new(AtomicBool::new(false));
        let cache_task = Arc::clone(&cache);
        let sync = tokio::spawn({
            let committed = Arc::clone(&committed);
            let after_sync = Arc::clone(&after_sync);
            async move {
                sync_for_test(&cache_task, &role_id, after_sync, async move {
                    committed.store(true, Ordering::SeqCst);
                    Ok(())
                })
                .await
            }
        });
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        assert!(
            !committed.load(Ordering::SeqCst),
            "the commit waits for the in-flight load"
        );

        release_tx.send(()).unwrap();
        loader.await.unwrap().expect("loader succeeds");
        sync.await.unwrap().expect("sync succeeds");

        let cached = cache.get(&role_id).await.expect("cached");
        assert!(
            Arc::ptr_eq(&cached, &after_sync),
            "the synced member list stays cached"
        );
    }

    /// Role members: an invalidation between a sync's read and its commit makes the
    /// sync remove the entry.
    #[tokio::test]
    async fn an_invalidation_during_a_role_members_sync_removes_its_entry() {
        let cache = rm_cache();
        let role_id = RoleId::new_random();
        let stale = role_result_with_members(role_id, vec![test_user_id("rm-removed-member")]);
        insert_for_test(&cache, &role_id, Arc::clone(&stale)).await;

        let invalidations_before = cache.invalidations_snapshot();
        cache.invalidate(&role_id).await;

        cache
            .commit_and_cache(
                &role_id,
                Arc::clone(&stale),
                invalidations_before.read(&role_id),
                async { Ok::<_, CatalogBackendError>(()) },
            )
            .await
            .expect("sync succeeds");
        assert!(
            cache.get(&role_id).await.is_none(),
            "the sync's pre-invalidation member list is not cached"
        );
    }

    /// `invalidate_many` bumps the stripe of each of its keys once, so a sync of any of
    /// them that overlaps it removes its entry.
    #[tokio::test]
    async fn invalidate_many_bumps_each_key_and_an_overlapping_sync_removes() {
        let cache = ua_cache();
        let first = test_user_id("invalidate-many-first");
        let second = test_user_id("invalidate-many-second");
        let first_before = cache.invalidations(&first).0;
        let second_before = cache.invalidations(&second);

        cache
            .invalidate_many(&[first.clone(), first.clone(), second.clone()])
            .await;
        assert!(cache.invalidations(&first).0 > first_before);
        assert!(cache.invalidations(&second).0 > second_before.0);
        let first_after = cache.invalidations(&first).0;
        cache.invalidate_many(&[first.clone(), first.clone()]).await;
        assert_eq!(
            cache.invalidations(&first).0,
            first_after + 1,
            "a stripe is bumped once per call"
        );

        cache
            .commit_and_cache(&second, empty_user_result(), second_before, async {
                Ok::<_, CatalogBackendError>(())
            })
            .await
            .expect("sync succeeds");
        assert!(
            cache.get(&second).await.is_none(),
            "the overlapping sync removes its entry"
        );
    }

    /// The production functions count in the process's caches: each invalidation
    /// increases its key's stripe in its own cache's counts. Only increases are
    /// checked, so the test holds whatever else the process invalidates.
    #[tokio::test]
    async fn the_invalidate_functions_count_in_the_process_caches() {
        let user_id = test_user_id("process-counters");
        let before = USER_ASSIGNMENTS_CACHE.invalidations(&user_id).0;
        user_assignments_cache_invalidate(&user_id).await;
        assert!(USER_ASSIGNMENTS_CACHE.invalidations(&user_id).0 > before);

        let other = test_user_id("process-counters-many");
        let before = USER_ASSIGNMENTS_CACHE.invalidations(&other).0;
        user_assignments_cache_invalidate_many(std::slice::from_ref(&other)).await;
        assert!(USER_ASSIGNMENTS_CACHE.invalidations(&other).0 > before);

        let role_id = RoleId::new_random();
        let before = ROLE_MEMBERS_CACHE.invalidations(&role_id).0;
        role_members_cache_invalidate(role_id).await;
        assert!(ROLE_MEMBERS_CACHE.invalidations(&role_id).0 > before);
    }

    /// An invalidation of a key in another stripe leaves an overlapping sync's write.
    #[tokio::test]
    async fn an_invalidation_in_another_stripe_leaves_the_sync_entry() {
        let cache = ua_cache();
        let user_id = test_user_id("stripe-own");
        let other = (0..1024)
            .map(|n| test_user_id(&format!("stripe-other-{n:04}")))
            .find(|other| {
                Invalidations::stripe_index(other) != Invalidations::stripe_index(&user_id)
            })
            .expect("some id hashes to another stripe");
        let invalidations_before = cache.invalidations(&user_id);

        cache.invalidate(&other).await;

        let synced = empty_user_result();
        cache
            .commit_and_cache(&user_id, Arc::clone(&synced), invalidations_before, async {
                Ok::<_, CatalogBackendError>(())
            })
            .await
            .expect("sync succeeds");
        let cached = cache.get(&user_id).await.expect("cached");
        assert!(Arc::ptr_eq(&cached, &synced));
    }

    /// An empty `invalidate_many` bumps nothing, so an overlapping sync still caches.
    #[tokio::test]
    async fn an_empty_invalidate_many_bumps_nothing() {
        let cache = ua_cache();
        let user_id = test_user_id("invalidate-many-empty");
        let before = cache.invalidations_snapshot();
        let invalidations_before = cache.invalidations(&user_id);

        cache.invalidate_many(&[]).await;
        assert_eq!(cache.invalidations_snapshot().0, before.0);

        let synced = empty_user_result();
        cache
            .commit_and_cache(&user_id, Arc::clone(&synced), invalidations_before, async {
                Ok::<_, CatalogBackendError>(())
            })
            .await
            .expect("sync succeeds");
        let cached = cache.get(&user_id).await.expect("cached");
        assert!(Arc::ptr_eq(&cached, &synced));
    }

    /// An invalidation that counts during a load leaves no entry, also when its
    /// `Op::Remove` never runs: here its request is dropped while it waits for the
    /// key lock.
    #[tokio::test]
    async fn an_invalidation_during_a_load_leaves_no_entry() {
        use tokio::sync::oneshot;

        let cache = ua_cache();
        let user_id = test_user_id("invalidation-during-load");

        let (started_tx, started_rx) = oneshot::channel::<()>();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let stale = user_result_with_role(
            RoleId::new_random(),
            Arc::new(ProjectId::new_random()),
            test_role_ident("lakekeeper", "revoked-during-load"),
        );
        let cache_task = Arc::clone(&cache);
        let loader = tokio::spawn({
            let uid = user_id.clone();
            let stale = Arc::clone(&stale);
            async move {
                let load = async move {
                    started_tx.send(()).unwrap();
                    release_rx.await.unwrap();
                    Ok(stale)
                };
                cache_task.get_or_load(&uid, load).await
            }
        });
        started_rx.await.unwrap();

        let before = cache.invalidations(&user_id).0;
        let cache_task = Arc::clone(&cache);
        let invalidate = tokio::spawn({
            let uid = user_id.clone();
            async move { cache_task.invalidate(&uid).await }
        });
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        assert!(
            cache.invalidations(&user_id).0 > before,
            "the invalidation counted and now waits for the key lock"
        );
        invalidate.abort();
        assert!(invalidate.await.unwrap_err().is_cancelled());

        release_tx.send(()).unwrap();
        let returned = loader.await.unwrap().expect("loader succeeds");
        assert!(
            Arc::ptr_eq(&returned, &stale),
            "the loader answers its caller"
        );
        assert!(
            cache.get(&user_id).await.is_none(),
            "the loaded state is not cached"
        );
    }

    /// Role members: an invalidation that counts during a load leaves no entry, also
    /// when its `Op::Remove` never runs.
    #[tokio::test]
    async fn an_invalidation_during_a_role_members_load_leaves_no_entry() {
        use tokio::sync::oneshot;

        let cache = rm_cache();
        let role_id = RoleId::new_random();

        let (started_tx, started_rx) = oneshot::channel::<()>();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let stale = role_result_with_members(role_id, vec![test_user_id("rm-removed-during-load")]);
        let cache_task = Arc::clone(&cache);
        let loader = tokio::spawn({
            let stale = Arc::clone(&stale);
            async move {
                let load = async move {
                    started_tx.send(()).unwrap();
                    release_rx.await.unwrap();
                    Ok(Some(stale))
                };
                cache_task.get_or_load_optional(&role_id, load).await
            }
        });
        started_rx.await.unwrap();

        let before = cache.invalidations(&role_id).0;
        let cache_task = Arc::clone(&cache);
        let invalidate = tokio::spawn(async move { cache_task.invalidate(&role_id).await });
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        assert!(
            cache.invalidations(&role_id).0 > before,
            "the invalidation counted and now waits for the key lock"
        );
        invalidate.abort();
        assert!(invalidate.await.unwrap_err().is_cancelled());

        release_tx.send(()).unwrap();
        let returned = loader
            .await
            .unwrap()
            .expect("loader succeeds")
            .expect("the role exists");
        assert!(
            Arc::ptr_eq(&returned, &stale),
            "the loader answers its caller"
        );
        assert!(
            cache.get(&role_id).await.is_none(),
            "the loaded member list is not cached"
        );
    }

    /// Same race, for `ROLE_MEMBERS`: its loader was already compute-based, so this
    /// guards that the invalidate change (`Op::Remove`) serializes with it.
    #[tokio::test]
    async fn invalidate_wins_over_in_flight_role_members_loader() {
        use tokio::sync::oneshot;

        let cache = rm_cache();
        let role_id = RoleId::new_random();

        let (started_tx, started_rx) = oneshot::channel::<()>();
        let (release_tx, release_rx) = oneshot::channel::<()>();

        let stale = role_result_with_members(role_id, vec![test_user_id("stale-member")]);
        let stale_for_loader = Arc::clone(&stale);

        let cache_task = Arc::clone(&cache);
        let loader = tokio::spawn(async move {
            let load = async move {
                started_tx.send(()).unwrap();
                release_rx.await.unwrap();
                Ok(Some(stale_for_loader))
            };
            cache_task.get_or_load_optional(&role_id, load).await
        });

        started_rx.await.unwrap();
        let cache_task = Arc::clone(&cache);
        let invalidate = tokio::spawn(async move {
            cache_task.invalidate(&role_id).await;
        });

        release_tx.send(()).unwrap();
        let returned = loader.await.unwrap().expect("loader succeeds");
        invalidate.await.unwrap();

        assert_eq!(returned.unwrap().members.len(), 1);
        assert!(
            cache.get(&role_id).await.is_none(),
            "removed role-members entry was resurrected by the racing loader"
        );
    }

    #[tokio::test]
    async fn test_user_assignments_get_returns_same_arc() {
        let cache = ua_cache();
        let user_id = test_user_id("arc-check-ua");
        let role_id = RoleId::new_random();
        let project_id = Arc::new(ProjectId::new_random());
        let role_ident = test_role_ident("lakekeeper", "arc-source");
        let result = user_result_with_role(role_id, project_id, role_ident);

        insert_for_test(&cache, &user_id, Arc::clone(&result)).await;
        let cached = cache.get(&user_id).await.unwrap();

        // Only the Arc counter was bumped — no heap allocation.
        assert!(Arc::ptr_eq(&result, &cached));
    }

    /// A result with `provider_sync_times` populated but no roles must survive
    /// a cache round-trip intact — this is the "synced but no assignments" shape.
    /// A batch that is partly cached loads only what it missed, and answers for all of it.
    #[tokio::test]
    async fn a_partly_cached_batch_loads_only_the_misses() {
        let cached = RoleId::new_random();
        let missed = RoleId::new_random();
        let parent = Arc::new(vec![AssignedRole {
            role_id: RoleId::new_random(),
            role_ident: test_role_ident("lakekeeper", "parent"),
            project_id: Arc::new(ProjectId::new_random()),
        }]);
        ROLE_ANCESTORS_CACHE
            .insert(cached, Arc::clone(&parent))
            .await;

        let resolved = role_ancestors_cache_get_or_load(&[cached, missed], |missing| async move {
            assert_eq!(
                missing,
                vec![missed],
                "only the uncached role is loaded, so a warm batch costs one read for the rest"
            );
            Ok(HashMap::from([(missed, Arc::new(vec![]))]))
        })
        .await
        .expect("both roles resolve");

        assert_eq!(resolved.len(), 2, "the answer covers the whole batch");
        assert_eq!(
            resolved[&cached].len(),
            1,
            "the cached entry is reused as-is"
        );
        assert_eq!(resolved[&missed].len(), 0);
    }

    #[tokio::test]
    async fn test_user_assignments_sync_without_roles() {
        let cache = ua_cache();
        let user_id = test_user_id("sync-no-roles");
        let provider_id = RoleProviderId::try_new("oidc").unwrap();
        let project_id = Arc::new(ProjectId::new_random());
        let synced_at = chrono::Utc::now();

        let result = Arc::new(ListUserRoleAssignmentsResult {
            roles: vec![],
            provider_sync_times: vec![UserProviderSyncInfo {
                project_id: Arc::clone(&project_id),
                provider_id: provider_id.clone(),
                synced_at,
            }],
        });

        insert_for_test(&cache, &user_id, Arc::clone(&result)).await;
        let cached = cache.get(&user_id).await.unwrap();

        assert_eq!(cached.roles.len(), 0, "no roles");
        assert_eq!(
            cached.provider_sync_times.len(),
            1,
            "sync record must survive cache round-trip"
        );
        assert_eq!(cached.provider_sync_times[0].provider_id, provider_id);
        assert_eq!(cached.provider_sync_times[0].synced_at, synced_at);
    }

    /// A clear landing while a load runs must not be undone by that load's insert.
    ///
    /// `invalidate_all` stamps a validity time rather than removing, so the later insert
    /// would survive the clear and serve the pre-change ancestors for a full TTL -- with
    /// nothing left to clear, since the write already did its clearing. That is the
    /// permissive direction: the member keeps the parent's privileges after the edge is gone.
    #[tokio::test]
    async fn a_clear_during_a_load_is_not_undone_by_it() {
        let nested = RoleId::new_random();
        let stale = Arc::new(vec![AssignedRole {
            role_id: RoleId::new_random(),
            role_ident: test_role_ident("lakekeeper", "former-parent"),
            project_id: Arc::new(ProjectId::new_random()),
        }]);

        let resolved = role_ancestors_cache_get_or_load(&[nested], |_| async {
            // The membership edge is removed and the cache cleared while this load runs.
            role_ancestors_cache_invalidate_all();
            Ok(HashMap::from([(nested, Arc::clone(&stale))]))
        })
        .await
        .expect("the load answers this caller");

        assert_eq!(
            resolved[&nested].len(),
            1,
            "the caller that ran the load still gets what it read"
        );
        ROLE_ANCESTORS_CACHE.run_pending_tasks().await;
        assert!(
            role_ancestors_cache_get(nested).await.is_none(),
            "but nothing is cached, so the next request reads the post-removal state"
        );
    }

    /// A slower load must not overwrite a fresher entry.
    ///
    /// Without single-flight there is no ordering between two loaders, so the stale one can
    /// write last and take the cache from correct back to wrong -- with no further write to
    /// trigger another clear.
    #[tokio::test]
    async fn a_stale_load_does_not_overwrite_a_fresh_entry() {
        let nested = RoleId::new_random();
        let stale = Arc::new(vec![AssignedRole {
            role_id: RoleId::new_random(),
            role_ident: test_role_ident("lakekeeper", "former-parent"),
            project_id: Arc::new(ProjectId::new_random()),
        }]);

        // The slow loader read before the edge was removed; the clear happens while it runs,
        // and the fresh (empty) answer is cached in between.
        let resolved = role_ancestors_cache_get_or_load(&[nested], |_| async {
            role_ancestors_cache_invalidate_all();
            ROLE_ANCESTORS_CACHE.insert(nested, Arc::new(vec![])).await;
            Ok(HashMap::from([(nested, Arc::clone(&stale))]))
        })
        .await
        .expect("the load answers this caller");
        assert_eq!(resolved[&nested].len(), 1);

        ROLE_ANCESTORS_CACHE.run_pending_tasks().await;
        assert_eq!(
            role_ancestors_cache_get(nested)
                .await
                .expect("the fresh entry is still there")
                .len(),
            0,
            "the fresh empty answer survives; the stale load did not write over it"
        );
    }

    #[tokio::test]
    async fn test_user_assignments_overwrite() {
        let cache = ua_cache();
        let user_id = test_user_id("overwrite-ua");
        let role_id = RoleId::new_random();
        let project_id = Arc::new(ProjectId::new_random());
        let role_ident = test_role_ident("lakekeeper", "overwrite-src");

        insert_for_test(&cache, &user_id, empty_user_result()).await;
        let rich = user_result_with_role(role_id, project_id, role_ident);
        insert_for_test(&cache, &user_id, Arc::clone(&rich)).await;

        let cached = cache.get(&user_id).await.unwrap();
        assert_eq!(cached.roles.len(), 1);
    }

    // ── Role ancestors ────────────────────────────────────────────────────────

    /// The whole-cache clear is what bounds this cache's staleness, so it has to clear
    /// every key, not the one whose role was named.
    ///
    /// One membership edge changes the ancestor set of the member and of everything nested
    /// beneath it, and the write path does not know which roles those are without a second
    /// closure walk — which is exactly why the clear is blunt. A clear that only removed
    /// some keys would leave a removed edge visible, and a policy written against the
    /// former parent still applying.
    /// A loader that omits a role it was asked about is a backend error, not an answer.
    ///
    /// The two are indistinguishable downstream — an empty closure and an unread one look
    /// identical — and guessing empty is the permissive guess: every policy written against a
    /// parent role silently stops applying. So the read-through refuses to cache a gap.
    #[tokio::test]
    async fn a_loader_that_skips_a_requested_role_errors() {
        let asked = RoleId::new_random();
        let skipped = RoleId::new_random();

        let err = role_ancestors_cache_get_or_load(&[asked, skipped], |missing| async move {
            assert_eq!(missing.len(), 2, "neither role is cached yet");
            Ok(HashMap::from([(asked, Arc::new(vec![]))]))
        })
        .await
        .expect_err("a missing entry is an error");
        assert!(
            err.to_string().contains(&skipped.to_string()),
            "the error names the role that went unanswered, got: {err}"
        );

        assert!(
            role_ancestors_cache_get(skipped).await.is_none(),
            "nothing is cached for the role the loader skipped"
        );
    }

    #[tokio::test]
    async fn role_ancestors_invalidate_all_clears_every_entry() {
        let nested = RoleId::new_random();
        let unrelated = RoleId::new_random();
        let ancestors = Arc::new(vec![AssignedRole {
            role_id: RoleId::new_random(),
            role_ident: test_role_ident("lakekeeper", "parent"),
            project_id: Arc::new(ProjectId::new_random()),
        }]);

        ROLE_ANCESTORS_CACHE.insert(nested, ancestors).await;
        ROLE_ANCESTORS_CACHE
            .insert(unrelated, Arc::new(vec![]))
            .await;
        assert_eq!(
            role_ancestors_cache_get(nested)
                .await
                .expect("just inserted")
                .len(),
            1,
        );
        assert!(role_ancestors_cache_get(unrelated).await.is_some());

        role_ancestors_cache_invalidate_all();
        ROLE_ANCESTORS_CACHE.run_pending_tasks().await;

        assert!(
            role_ancestors_cache_get(nested).await.is_none(),
            "the named role's ancestors are gone"
        );
        assert!(
            role_ancestors_cache_get(unrelated).await.is_none(),
            "so are every other role's — an edge change is not scoped to one key"
        );
    }

    /// Two independently-loaded results referencing the same role/project value
    /// (distinct `Arc` allocations) must, after sharing, collapse to ONE canonical
    /// `Arc` — the dedup that stops a role held by many users from storing its
    /// identity once per user.
    #[tokio::test]
    async fn share_identities_dedups_shared_identity_across_results() {
        let role_id = RoleId::new_random();
        let project = ProjectId::new_random();
        let mk = || ListUserRoleAssignmentsResult {
            roles: vec![AssignedRole {
                role_id,
                role_ident: test_role_ident("lakekeeper", "share-dedup-src"),
                project_id: Arc::new(project.clone()),
            }],
            provider_sync_times: vec![],
        };
        let mut a = mk();
        let mut b = mk();
        // Independently allocated before sharing.
        assert!(!Arc::ptr_eq(&a.roles[0].role_ident, &b.roles[0].role_ident));
        assert!(!Arc::ptr_eq(&a.roles[0].project_id, &b.roles[0].project_id));

        share_identities(&mut a).await;
        share_identities(&mut b).await;

        assert!(
            Arc::ptr_eq(&a.roles[0].role_ident, &b.roles[0].role_ident),
            "same role-ident value must dedup to one shared Arc"
        );
        assert!(
            Arc::ptr_eq(&a.roles[0].project_id, &b.roles[0].project_id),
            "same project-id value must dedup to one shared Arc"
        );
    }

    /// Distinct ident VALUES must not merge — a renamed role (new ident) gets a
    /// fresh canonical, so the content-addressed pool self-heals across renames.
    #[tokio::test]
    async fn share_identities_keeps_distinct_idents_separate() {
        let role_id = RoleId::new_random();
        let project = Arc::new(ProjectId::new_random());
        let mut before = ListUserRoleAssignmentsResult {
            roles: vec![AssignedRole {
                role_id,
                role_ident: test_role_ident("lakekeeper", "share-rename-before"),
                project_id: Arc::clone(&project),
            }],
            provider_sync_times: vec![],
        };
        let mut after = ListUserRoleAssignmentsResult {
            roles: vec![AssignedRole {
                role_id,
                role_ident: test_role_ident("lakekeeper", "share-rename-after"),
                project_id: Arc::clone(&project),
            }],
            provider_sync_times: vec![],
        };
        share_identities(&mut before).await;
        share_identities(&mut after).await;
        assert!(
            !Arc::ptr_eq(&before.roles[0].role_ident, &after.roles[0].role_ident),
            "different ident values must not be deduped together"
        );
    }

    /// `provider_sync_times` project ids are shared too, collapsing to the canonical
    /// `Arc` of a role carrying the same project value.
    #[tokio::test]
    async fn share_identities_dedups_provider_sync_project_id() {
        let project = ProjectId::new_random();
        let mut result = ListUserRoleAssignmentsResult {
            roles: vec![AssignedRole {
                role_id: RoleId::new_random(),
                role_ident: test_role_ident("lakekeeper", "share-sync-proj"),
                project_id: Arc::new(project.clone()),
            }],
            provider_sync_times: vec![UserProviderSyncInfo {
                project_id: Arc::new(project.clone()),
                provider_id: RoleProviderId::try_new("oidc").unwrap(),
                synced_at: chrono::Utc::now(),
            }],
        };
        assert!(!Arc::ptr_eq(
            &result.roles[0].project_id,
            &result.provider_sync_times[0].project_id
        ));
        share_identities(&mut result).await;
        assert!(
            Arc::ptr_eq(
                &result.roles[0].project_id,
                &result.provider_sync_times[0].project_id
            ),
            "provider_sync_times project_id must share the same canonical Arc"
        );
    }

    /// Single-flight: N concurrent misses for the same key run the loader
    /// **exactly once**, and every caller receives the same `Arc`. Guards against
    /// regressing to the per-caller get-load-insert path (a per-replica
    /// thundering herd on hot keys).
    #[tokio::test]
    async fn user_assignments_get_or_load_coalesces_concurrent_misses() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let cache = ua_cache();
        let user_id = test_user_id("single-flight-coalesce");

        let loads = Arc::new(AtomicUsize::new(0));
        let value = user_result_with_role(
            RoleId::new_random(),
            Arc::new(ProjectId::new_random()),
            test_role_ident("lakekeeper", "single-flight"),
        );

        let mut handles = Vec::new();
        for _ in 0..32 {
            let loads = Arc::clone(&loads);
            let uid = user_id.clone();
            let value = Arc::clone(&value);
            let cache_task = Arc::clone(&cache);
            handles.push(tokio::spawn(async move {
                cache_task
                    .get_or_load(&uid, async move {
                        loads.fetch_add(1, Ordering::SeqCst);
                        // Widen the miss window so every caller races in before the
                        // first load completes — without coalescing this forces N
                        // loader runs (the behaviour this guards against).
                        for _ in 0..100 {
                            tokio::task::yield_now().await;
                        }
                        Ok::<_, CatalogBackendError>(value)
                    })
                    .await
            }));
        }

        let mut results = Vec::new();
        for h in handles {
            results.push(h.await.unwrap().expect("loader succeeds"));
        }

        assert_eq!(
            loads.load(Ordering::SeqCst),
            1,
            "concurrent misses must coalesce to a single loader run"
        );
        for r in &results[1..] {
            assert!(
                Arc::ptr_eq(&results[0], r),
                "every caller must receive the same coalesced Arc"
            );
        }
    }

    /// A failed load must not poison the entry: every caller observes the error and
    /// nothing is cached, so a later success still populates. Errors are NOT
    /// coalesced — `and_try_compute_with` inserts nothing on `Err`, so each
    /// serialized caller re-runs the load (consistent with every compute-based cache;
    /// the old `try_get_with` shared one failing load — traded away to serialize the
    /// loader against invalidation).
    #[tokio::test]
    async fn user_assignments_get_or_load_does_not_cache_errors() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        const CALLERS: usize = 16;

        let cache = ua_cache();
        let user_id = test_user_id("single-flight-error");

        let loads = Arc::new(AtomicUsize::new(0));

        // Concurrent callers whose loader fails. They serialize on the key's compute
        // lock; since `Err` caches nothing, each one re-runs the failing load.
        let mut handles = Vec::new();
        for _ in 0..CALLERS {
            let loads = Arc::clone(&loads);
            let uid = user_id.clone();
            let cache_task = Arc::clone(&cache);
            handles.push(tokio::spawn(async move {
                cache_task
                    .get_or_load(&uid, async move {
                        loads.fetch_add(1, Ordering::SeqCst);
                        for _ in 0..100 {
                            tokio::task::yield_now().await;
                        }
                        Err::<Arc<ListUserRoleAssignmentsResult>, _>(
                            CatalogBackendError::new_unexpected(std::io::Error::other("boom")),
                        )
                    })
                    .await
            }));
        }

        for h in handles {
            assert!(
                h.await.unwrap().is_err(),
                "every caller observes the failure"
            );
        }
        assert_eq!(
            loads.load(Ordering::SeqCst),
            CALLERS,
            "errors are not negative-cached, so each serialized caller re-runs the failing load"
        );
        assert!(
            cache.get(&user_id).await.is_none(),
            "a failed load must not poison the entry"
        );

        // A subsequent successful load populates the cache as usual.
        let value = user_result_with_role(
            RoleId::new_random(),
            Arc::new(ProjectId::new_random()),
            test_role_ident("lakekeeper", "after-error"),
        );
        let loaded = cache
            .get_or_load(&user_id, {
                let value = Arc::clone(&value);
                async move { Ok::<_, CatalogBackendError>(value) }
            })
            .await
            .expect("loader succeeds after a prior failure");
        assert!(Arc::ptr_eq(&loaded, &value));
        assert!(cache.get(&user_id).await.is_some());
    }

    /// `get_or_load_optional` must coalesce concurrent misses for the
    /// same role into ONE loader run, with every caller receiving the same `Arc`.
    /// Mirrors the user-assignments single-flight guard, but this read-through
    /// returns `Option` — a present role coalesces; a non-existent one must not
    /// be negative-cached (covered separately below).
    #[tokio::test]
    async fn role_members_get_or_load_coalesces_concurrent_misses() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let cache = rm_cache();
        let role_id = RoleId::new_random();

        let loads = Arc::new(AtomicUsize::new(0));
        let value = role_result_with_members(role_id, vec![test_user_id("rm-coalesce")]);

        let mut handles = Vec::new();
        for _ in 0..32 {
            let loads = Arc::clone(&loads);
            let value = Arc::clone(&value);
            let cache_task = Arc::clone(&cache);
            handles.push(tokio::spawn(async move {
                cache_task
                    .get_or_load_optional(&role_id, async move {
                        loads.fetch_add(1, Ordering::SeqCst);
                        for _ in 0..100 {
                            tokio::task::yield_now().await;
                        }
                        Ok::<_, CatalogBackendError>(Some(value))
                    })
                    .await
            }));
        }

        let mut results = Vec::new();
        for h in handles {
            results.push(
                h.await
                    .unwrap()
                    .expect("loader succeeds")
                    .expect("role exists"),
            );
        }

        assert_eq!(
            loads.load(Ordering::SeqCst),
            1,
            "concurrent misses must coalesce to a single loader run"
        );
        for r in &results[1..] {
            assert!(
                Arc::ptr_eq(&results[0], r),
                "every caller must receive the same coalesced Arc"
            );
        }
    }

    /// A non-existent role (loader returns `None`) must NOT be negative-cached:
    /// after a `None` load the entry stays absent, so a later real insert is
    /// visible immediately rather than shadowed until TTL.
    #[tokio::test]
    async fn role_members_get_or_load_does_not_negative_cache() {
        let cache = rm_cache();
        let role_id = RoleId::new_random();

        let missing = cache
            .get_or_load_optional(&role_id, async { Ok(None) })
            .await
            .expect("loader succeeds");
        assert!(missing.is_none(), "non-existent role resolves to None");
        assert!(
            cache.get(&role_id).await.is_none(),
            "None must not be cached"
        );

        // A subsequent successful load populates the cache as usual.
        let value = role_result_with_members(role_id, vec![test_user_id("rm-late")]);
        let loaded = cache
            .get_or_load_optional(&role_id, {
                let value = Arc::clone(&value);
                async move { Ok::<_, CatalogBackendError>(Some(value)) }
            })
            .await
            .expect("loader succeeds")
            .expect("role now exists");
        assert!(Arc::ptr_eq(&loaded, &value));
        assert!(cache.get(&role_id).await.is_some());
    }

    // ── Role members ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_role_members_insert_and_get() {
        let cache = rm_cache();
        let role_id = RoleId::new_random();
        insert_for_test(&cache, &role_id, empty_role_result(role_id)).await;

        let cached = cache.get(&role_id).await;
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().members.len(), 0);
    }

    #[tokio::test]
    async fn test_role_members_miss() {
        let cache = rm_cache();
        let role_id = RoleId::new_random();
        assert!(cache.get(&role_id).await.is_none());
    }

    #[tokio::test]
    async fn test_role_members_invalidate() {
        let cache = rm_cache();
        let role_id = RoleId::new_random();
        insert_for_test(&cache, &role_id, empty_role_result(role_id)).await;
        assert!(cache.get(&role_id).await.is_some());

        cache.invalidate(&role_id).await;
        assert!(cache.get(&role_id).await.is_none());
    }

    #[tokio::test]
    async fn test_role_members_get_returns_same_arc() {
        let cache = rm_cache();
        let role_id = RoleId::new_random();
        let result = role_result_with_members(
            role_id,
            vec![test_user_id("member-1"), test_user_id("member-2")],
        );

        insert_for_test(&cache, &role_id, Arc::clone(&result)).await;
        let cached = cache.get(&role_id).await.unwrap();

        assert!(Arc::ptr_eq(&result, &cached));
    }

    /// A result with `last_synced_at: Some(...)` but no members must survive
    /// a cache round-trip intact — this is the "synced but no members" shape.
    #[tokio::test]
    async fn test_role_members_sync_without_members() {
        let cache = rm_cache();
        let role_id = RoleId::new_random();
        let synced_at = chrono::Utc::now();

        let result = Arc::new(ListRoleMembersResult {
            role_id,
            project_id: Arc::new(ProjectId::new_random()),
            role_ident: test_role_ident("ldap", "empty-group"),
            members: vec![],
            last_synced_at: Some(synced_at),
        });

        insert_for_test(&cache, &role_id, Arc::clone(&result)).await;
        let cached = cache.get(&role_id).await.unwrap();

        assert_eq!(cached.members.len(), 0, "no members");
        assert_eq!(
            cached.last_synced_at,
            Some(synced_at),
            "last_synced_at must survive cache round-trip even with no members"
        );
    }

    #[tokio::test]
    async fn test_role_members_different_roles_are_independent() {
        let cache = rm_cache();
        let role_a = RoleId::new_random();
        let role_b = RoleId::new_random();

        insert_for_test(
            &cache,
            &role_a,
            role_result_with_members(role_a, vec![test_user_id("user-a")]),
        )
        .await;
        insert_for_test(
            &cache,
            &role_b,
            role_result_with_members(role_b, vec![test_user_id("user-b"), test_user_id("user-c")]),
        )
        .await;

        assert_eq!(cache.get(&role_a).await.unwrap().members.len(), 1);
        assert_eq!(cache.get(&role_b).await.unwrap().members.len(), 2);

        cache.invalidate(&role_a).await;
        assert!(cache.get(&role_a).await.is_none());
        assert!(cache.get(&role_b).await.is_some());
    }
}
