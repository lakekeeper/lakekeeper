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
use tracing::Instrument;

use crate::{
    CONFIG,
    service::{
        ArcProjectId, ArcRoleIdent, CatalogBackendError, RoleId,
        authn::UserId,
        cache_metrics,
        cache_ttl::JitteredTtl,
        catalog_store::role_assignment::{AssignedRole, ListUserRoleAssignmentsResult},
    },
};

// ============================================================================
// Counted caches: writes under the key lock, striped invalidation counts
// ============================================================================

/// Number of invalidation stripes per [`CountedCache`], 8 bytes each. Every
/// sync and every invalidation bumps its key's stripe, so keys sharing a stripe
/// fence each other; more stripes make that rarer.
const STRIPES: usize = 4096;

/// Invalidation counts, striped by key hash into [`STRIPES`] stripes. Every
/// invalidation and every sync of a key bumps that key's stripe; one of another key
/// bumps the same stripe only when both keys hash to it.
struct Invalidations(Box<[AtomicU64]>);

impl Invalidations {
    fn new() -> Self {
        Self((0..STRIPES).map(|_| AtomicU64::new(0)).collect())
    }

    fn stripe_index<K: Hash + ?Sized>(key: &K) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let [low, high, ..] = hasher.finish().to_le_bytes();
        usize::from(u16::from_le_bytes([low, high])) % STRIPES
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

    #[cfg(test)]
    fn snapshot(&self) -> Vec<u64> {
        self.0
            .iter()
            .map(|stripe| stripe.load(Ordering::Acquire))
            .collect()
    }

    /// Bump each stripe that holds one of `keys`, once.
    fn bump_many<K: Hash>(&self, keys: &[K]) {
        let mut bumped = [false; STRIPES];
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
/// [`CountedCache::cache_after_commit`]. Typed by the key, so a count only reaches a
/// cache with the same key type.
pub(super) struct InvalidationCount<K>(u64, PhantomData<fn(&K)>);

/// A moka cache that is written only under the per-key compute lock, together with
/// its striped invalidation counts.
///
/// Three kinds of write take the key lock: [`Self::cache_after_commit`] (a sync's
/// result, after its commit), the loaders ([`Self::get_or_load`],
/// [`Self::get_or_load_optional`]) and [`Self::invalidate`] (`Op::Remove`). A hit
/// takes no lock.
///
/// No code waits for a key lock while it holds a database transaction: a sync
/// commits before it takes the lock, and an invalidation runs after its writer's
/// commit. A loader holds the key lock across its read, and that read can wait
/// behind a pending `ALTER TABLE`; a transaction waiting for the same key lock could
/// close a cycle that Postgres cannot see.
pub(super) struct CountedCache<K, V> {
    cache: Cache<K, V>,
    invalidations: Invalidations,
    enabled: bool,
    /// The `cache_type` label of the cache metrics.
    cache_type: &'static str,
    /// What an entry holds, for log messages: at the start of a sentence, and inside
    /// one.
    noun: (&'static str, &'static str),
}

impl<K, V> CountedCache<K, V>
where
    K: Hash + Eq + Clone + Display + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn new(
        cache_type: &'static str,
        noun: (&'static str, &'static str),
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
    /// [`Self::cache_after_commit`].
    pub(super) fn invalidations(&self, key: &K) -> InvalidationCount<K> {
        InvalidationCount(self.invalidations.read(key), PhantomData)
    }

    /// Cache `value`, the state a sync read inside its transaction, under `key`.
    /// Call it after that transaction committed, inside [`run_to_completion`]
    /// together with the commit.
    ///
    /// It bumps the key's stripe, then, under the key lock, puts `value` if the
    /// stripe moved by exactly that bump since `invalidations_before`, and removes the
    /// entry otherwise. `invalidations_before` is the key's count, read before the
    /// transaction began.
    /// - A writer that commits after the transaction's read invalidates the key after
    ///   its commit. If its bump lands before the check, the stripe moved by more and
    ///   the entry is removed. Otherwise its `Op::Remove` takes the lock after this put.
    /// - Of two syncs of one key, the database orders the commits. The later one bumps
    ///   inside the earlier one's window unless the earlier one already wrote, so the
    ///   earlier value never replaces the later one. Both may remove the entry.
    /// - A load that read before the commit either sees the bump and skips its put,
    ///   or put before this write, which replaces it.
    /// - An invalidation or a sync of another key in the same stripe also removes the
    ///   entry.
    ///
    /// A removed entry is read again on the next request, through the read pool. The
    /// value cached here comes from the primary inside the committing transaction,
    /// and the reload can come from a read replica that has not caught up. With the
    /// cache disabled this returns at once and caches nothing.
    pub(super) async fn cache_after_commit(
        &self,
        key: &K,
        value: V,
        invalidations_before: InvalidationCount<K>,
    ) {
        if !self.enabled {
            return;
        }
        self.invalidations.bump(key);
        let stripe = self.invalidations.stripe(key);
        let after_own_bump = invalidations_before.0 + 1;
        let outcome = self
            .cache
            .entry(key.clone())
            .and_compute_with(|_| async move {
                if stripe.load(Ordering::Acquire) == after_own_bump {
                    Op::Put(value)
                } else {
                    Op::Remove
                }
            })
            .await;
        if matches!(
            outcome,
            CompResult::Inserted(_) | CompResult::ReplacedWith(_)
        ) {
            tracing::debug!("Inserting {} for {key} into cache", self.noun.1);
        } else {
            tracing::debug!(
                "Leaving {} for {key} uncached after an overlapping invalidation or sync",
                self.noun.1
            );
            cache_metrics::record_cache_fenced(self.cache_type);
        }
        self.update_size_metric();
    }

    async fn get(&self, key: &K) -> Option<V> {
        if !self.enabled {
            return None;
        }
        self.update_size_metric();
        if let Some(value) = self.cache.get(key).await {
            tracing::debug!("{} for {key} found in cache", self.noun.0);
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
            tracing::debug!("Invalidating {} for {key} from cache", self.noun.1);
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
            tracing::debug!("Invalidating {} for {key} from cache", self.noun.1);
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
    /// the result only if the count is unchanged. So an invalidation or a sync that
    /// counts during the load leaves the result uncached, also when its `Op::Remove`
    /// or its write waits behind this load. Writers run their post-commit cache steps
    /// in [`run_to_completion`], so a cancelled request still finishes them.
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
            tracing::debug!(
                "Leaving {} for {key} uncached after an overlapping invalidation or sync",
                self.noun.1
            );
            cache_metrics::record_cache_fenced(self.cache_type);
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

/// Run `step` on a task of its own, in the caller's tracing span, and wait for it.
/// A caller dropped while it waits leaves the task running, so a cancelled request
/// cannot separate a commit from the cache updates and events that follow it.
pub(crate) async fn run_to_completion<T: Send + 'static>(
    step: impl std::future::Future<Output = T> + Send + 'static,
) -> T {
    match tokio::spawn(step.in_current_span()).await {
        Ok(output) => output,
        Err(error) => match error.try_into_panic() {
            Ok(panic) => std::panic::resume_unwind(panic),
            // Only a runtime shutdown cancels the task, and it drops this caller too.
            Err(error) => panic!("post-commit step did not finish: {error}"),
        },
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
        ("User assignments", "user assignments"),
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
                AssignedRole, ListUserRoleAssignmentsResult, UserProviderSyncInfo,
            },
            identifier::role::{ArcRoleIdent, RoleSourceId},
        },
    };

    type UaCache = CountedCache<UserId, Arc<ListUserRoleAssignmentsResult>>;

    /// A user-assignments cache of the calling test's own, so other tests' entries and
    /// invalidations cannot reach it.
    fn ua_cache() -> Arc<UaCache> {
        Arc::new(CountedCache::new(
            "test_user_assignments",
            ("User assignments", "user assignments"),
            true,
            Cache::new(1_000),
        ))
    }

    /// A sync of `key`: read its invalidation count, then commit and cache `value` in
    /// [`run_to_completion`], as the production syncs do. A commit error removes the
    /// entry, because the commit may have landed.
    async fn sync_for_test<K, V>(
        cache: &Arc<CountedCache<K, V>>,
        key: &K,
        value: V,
        commit: impl std::future::Future<Output = Result<(), CatalogBackendError>> + Send + 'static,
    ) -> Result<(), CatalogBackendError>
    where
        K: Hash + Eq + Clone + Display + Send + Sync + 'static,
        V: Clone + Send + Sync + 'static,
    {
        let invalidations_before = cache.invalidations(key);
        let cache = Arc::clone(cache);
        let key = key.clone();
        run_to_completion(async move {
            let committed = commit.await;
            if committed.is_ok() {
                cache
                    .cache_after_commit(&key, value, invalidations_before)
                    .await;
            } else {
                cache.invalidate(&key).await;
            }
            committed
        })
        .await
    }

    async fn insert_for_test<K, V>(cache: &Arc<CountedCache<K, V>>, key: &K, value: V)
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

    /// A sync commits while a load of the same key holds the key lock, so no sync
    /// waits for that lock with its transaction open. The load leaves its result
    /// uncached, and the sync's result is cached once the load releases the lock.
    #[tokio::test]
    async fn a_sync_commits_while_a_load_holds_the_key_lock() {
        use std::sync::atomic::{AtomicBool, Ordering};

        use tokio::sync::oneshot;

        let cache = ua_cache();
        let user_id = test_user_id("sync-commits-during-load");

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
        let loader = tokio::spawn({
            let before_sync = Arc::clone(&before_sync);
            async move {
                let load = async move {
                    started_tx.send(()).unwrap();
                    release_rx.await.unwrap();
                    Ok(before_sync)
                };
                cache_task.get_or_load(&uid, load).await
            }
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
                    Ok(())
                })
                .await
            }
        });
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        assert!(
            committed.load(Ordering::SeqCst),
            "the commit runs while the load holds the key lock"
        );
        assert!(!sync.is_finished(), "the cache write waits for the load");

        release_tx.send(()).unwrap();
        let returned = loader.await.unwrap().expect("loader succeeds");
        assert!(
            Arc::ptr_eq(&returned, &before_sync),
            "the load answers its caller"
        );
        sync.await.unwrap().expect("sync succeeds");

        let cached = cache.get(&user_id).await.expect("cached");
        assert!(
            Arc::ptr_eq(&cached, &after_sync),
            "the synced state stays cached"
        );
    }

    /// Two syncs of one key whose windows overlap leave no entry, in either order of
    /// their cache writes, so the earlier commit never replaces the later one.
    #[tokio::test]
    async fn two_overlapping_syncs_leave_no_entry() {
        for later_writes_first in [false, true] {
            let cache = ua_cache();
            let user_id = test_user_id("overlapping-syncs");
            let earlier = user_result_with_role(
                RoleId::new_random(),
                Arc::new(ProjectId::new_random()),
                test_role_ident("lakekeeper", "earlier-sync"),
            );
            let later = user_result_with_role(
                RoleId::new_random(),
                Arc::new(ProjectId::new_random()),
                test_role_ident("lakekeeper", "later-sync"),
            );
            // Both read their count before either commits. The database then orders
            // the commits, and each sync bumps after its own.
            let earlier_before = cache.invalidations(&user_id);
            let later_before = cache.invalidations(&user_id);
            if later_writes_first {
                cache
                    .cache_after_commit(&user_id, later, later_before)
                    .await;
                cache
                    .cache_after_commit(&user_id, earlier, earlier_before)
                    .await;
            } else {
                cache
                    .cache_after_commit(&user_id, earlier, earlier_before)
                    .await;
                cache
                    .cache_after_commit(&user_id, later, later_before)
                    .await;
            }
            assert!(
                cache.get(&user_id).await.is_none(),
                "neither result is cached (later wrote first: {later_writes_first})"
            );
        }
    }

    /// A sync that reads its count after an earlier sync's write caches the later
    /// commit.
    #[tokio::test]
    async fn a_sync_after_another_sync_caches_the_later_commit() {
        let cache = ua_cache();
        let user_id = test_user_id("sequential-syncs");
        let later = user_result_with_role(
            RoleId::new_random(),
            Arc::new(ProjectId::new_random()),
            test_role_ident("lakekeeper", "later-sync"),
        );
        sync_for_test(&cache, &user_id, empty_user_result(), async { Ok(()) })
            .await
            .expect("first sync succeeds");
        sync_for_test(&cache, &user_id, Arc::clone(&later), async { Ok(()) })
            .await
            .expect("second sync succeeds");
        let cached = cache.get(&user_id).await.expect("cached");
        assert!(
            Arc::ptr_eq(&cached, &later),
            "the later commit stays cached"
        );
    }

    /// A sync's request dropped after its commit reached the database still finishes
    /// the cache step: the committed result replaces the pre-sync entry.
    #[tokio::test]
    async fn a_dropped_sync_still_caches_its_committed_result() {
        use std::sync::atomic::{AtomicBool, Ordering};

        use tokio::sync::oneshot;

        let cache = ua_cache();
        let user_id = test_user_id("dropped-sync");
        let before_sync = empty_user_result();
        insert_for_test(&cache, &user_id, Arc::clone(&before_sync)).await;
        let after_sync = user_result_with_role(
            RoleId::new_random(),
            Arc::new(ProjectId::new_random()),
            test_role_ident("lakekeeper", "after-dropped-sync"),
        );

        let (committed_tx, committed_rx) = oneshot::channel::<()>();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let committed = Arc::new(AtomicBool::new(false));
        let cache_task = Arc::clone(&cache);
        let request = tokio::spawn({
            let uid = user_id.clone();
            let committed = Arc::clone(&committed);
            let after_sync = Arc::clone(&after_sync);
            async move {
                sync_for_test(&cache_task, &uid, after_sync, async move {
                    // The database has committed; its reply has not arrived yet.
                    committed.store(true, Ordering::SeqCst);
                    committed_tx.send(()).unwrap();
                    release_rx.await.unwrap();
                    Ok(())
                })
                .await
            }
        });
        committed_rx.await.unwrap();
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        // The send fails only if the commit was dropped with the request.
        let commit_still_running = release_tx.send(()).is_ok();

        let mut cached = None;
        for _ in 0..100 {
            match cache.get(&user_id).await {
                Some(entry) if !Arc::ptr_eq(&entry, &before_sync) => {
                    cached = Some(entry);
                    break;
                }
                _ => tokio::task::yield_now().await,
            }
        }
        assert!(committed.load(Ordering::SeqCst));
        assert!(
            commit_still_running,
            "the commit outlives the dropped request"
        );
        assert!(
            cached.is_some_and(|entry| Arc::ptr_eq(&entry, &after_sync)),
            "the committed result replaces the pre-sync entry"
        );
    }

    /// The post-commit step logs in the caller's span, so its lines carry the
    /// request's context. Worker threads run the step, outside the test's own thread.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[tracing_test::traced_test]
    async fn the_post_commit_step_logs_in_the_callers_span() {
        let cache = ua_cache();
        let user_id = test_user_id("span-carried");
        sync_for_test(&cache, &user_id, empty_user_result(), async { Ok(()) })
            .await
            .expect("sync succeeds");
        assert!(logs_contain(
            "Inserting user assignments for oidc~span-carried into cache"
        ));
    }

    /// A failed commit returns its error, caches nothing and removes the entry, so the
    /// next read loads whatever the database holds.
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

        assert!(
            cache.get(&user_id).await.is_none(),
            "the entry is removed and the uncommitted state is not cached"
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
            .cache_after_commit(&user_id, Arc::clone(&removed_role), invalidations_before)
            .await;
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
            .cache_after_commit(&second, empty_user_result(), second_before)
            .await;
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
            .cache_after_commit(&user_id, Arc::clone(&synced), invalidations_before)
            .await;
        let cached = cache.get(&user_id).await.expect("cached");
        assert!(Arc::ptr_eq(&cached, &synced));
    }

    /// An invalidation of another key in the sync's stripe moves the stripe past the
    /// sync's own bump, so the sync removes its entry.
    #[tokio::test]
    async fn an_invalidation_in_the_same_stripe_removes_the_sync_entry() {
        let cache = ua_cache();
        let user_id = test_user_id("stripe-shared-own");
        let other = (0..100_000)
            .map(|n| test_user_id(&format!("stripe-shared-other-{n:06}")))
            .find(|other| {
                other != &user_id
                    && Invalidations::stripe_index(other) == Invalidations::stripe_index(&user_id)
            })
            .expect("some id hashes to the same stripe");
        insert_for_test(&cache, &user_id, empty_user_result()).await;
        let invalidations_before = cache.invalidations(&user_id);

        cache.invalidate(&other).await;

        cache
            .cache_after_commit(
                &user_id,
                user_result_with_role(
                    RoleId::new_random(),
                    Arc::new(ProjectId::new_random()),
                    test_role_ident("lakekeeper", "same-stripe"),
                ),
                invalidations_before,
            )
            .await;
        assert!(
            cache.get(&user_id).await.is_none(),
            "the sync removes its entry and the next read loads"
        );
    }

    /// An empty `invalidate_many` bumps nothing, so an overlapping sync still caches.
    #[tokio::test]
    async fn an_empty_invalidate_many_bumps_nothing() {
        let cache = ua_cache();
        let user_id = test_user_id("invalidate-many-empty");
        let before = cache.invalidations.snapshot();
        let invalidations_before = cache.invalidations(&user_id);

        cache.invalidate_many(&[]).await;
        assert_eq!(cache.invalidations.snapshot(), before);

        let synced = empty_user_result();
        cache
            .cache_after_commit(&user_id, Arc::clone(&synced), invalidations_before)
            .await;
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

    /// A `None` from the loader is not cached: after a `None` load the entry stays
    /// absent, so a later real insert is visible immediately.
    #[tokio::test]
    async fn get_or_load_optional_does_not_cache_none() {
        let cache = ua_cache();
        let user_id = test_user_id("optional-none");

        let missing = cache
            .get_or_load_optional(&user_id, async { Ok(None) })
            .await
            .expect("loader succeeds");
        assert!(missing.is_none(), "the loader's None is returned");
        assert!(
            cache.get(&user_id).await.is_none(),
            "None must not be cached"
        );

        // A subsequent successful load populates the cache as usual.
        let value = empty_user_result();
        let loaded = cache
            .get_or_load_optional(&user_id, {
                let value = Arc::clone(&value);
                async move { Ok::<_, CatalogBackendError>(Some(value)) }
            })
            .await
            .expect("loader succeeds")
            .expect("the entry now exists");
        assert!(Arc::ptr_eq(&loaded, &value));
        assert!(cache.get(&user_id).await.is_some());
    }
}
