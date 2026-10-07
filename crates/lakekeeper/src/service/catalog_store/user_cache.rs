//! The user cache: `UserId → the user's email`, read when audit records carry emails.
//!
//! Unlike every other cache, this one keeps absence: a user without a row and a user
//! without an email are cached like an email, with the same TTL. Audit records name
//! the same principals over and over, and many of them have no row or no email,
//! service accounts above all. Without negative entries each of their records would
//! cost a database read. An email on an audit record is best-effort metadata, so an
//! answer up to one TTL old is acceptable.
//!
//! The user lifecycle events keep this replica's entries current. Another replica's
//! writes reach this one only through the TTL: a user created or deleted there can
//! keep its old entry here until it expires.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, LazyLock},
    time::Duration,
};

use moka::future::Cache;

use super::role_assignments_cache::CountedCache;
#[cfg(feature = "router")]
use crate::service::events::{self, EventListener};
use crate::{
    CONFIG,
    service::{CatalogBackendError, CatalogStore, authn::UserId, cache_ttl::JitteredTtl},
};

/// What the catalog knows about a user's email.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserEmail {
    /// The user's email.
    Email(Arc<str>),
    /// The user exists and has no email.
    NoEmail,
    /// No user with this id, or a deleted one.
    NoUser,
}

impl UserEmail {
    /// The email, if the user has one.
    #[must_use]
    pub fn email(&self) -> Option<&str> {
        match self {
            Self::Email(email) => Some(email),
            Self::NoEmail | Self::NoUser => None,
        }
    }

    fn of(email: Option<&str>) -> Self {
        email.map_or(Self::NoEmail, |email| Self::Email(Arc::from(email)))
    }
}

pub(super) static USER_CACHE: LazyLock<CountedCache<UserId, UserEmail>> = LazyLock::new(|| {
    CountedCache::new(
        "user",
        ("User", "user"),
        CONFIG.cache.user.enabled,
        Cache::builder()
            .max_capacity(CONFIG.cache.user.capacity)
            .initial_capacity(1_000)
            .time_to_live(Duration::from_secs(CONFIG.cache.user.time_to_live_secs))
            .expire_after(JitteredTtl::with_default_jitter(Duration::from_secs(
                CONFIG.cache.user.time_to_live_secs,
            )))
            .build(),
    )
});

/// The users read in one statement.
const LOAD_CHUNK: usize = 500;

/// What the catalog knows about the email of each of `user_ids`, from the cache and
/// the read pool.
///
/// One id is loaded single-flight: concurrent misses for it share one read. Several
/// ids are loaded in one read for all misses; concurrent misses for the same id then
/// each read it once. A read error is returned and nothing is cached.
pub async fn user_emails<C: CatalogStore>(
    user_ids: &[UserId],
    catalog_state: C::State,
) -> Result<HashMap<UserId, UserEmail>, CatalogBackendError> {
    let mut seen = HashSet::new();
    let unique: Vec<UserId> = user_ids
        .iter()
        .filter(|id| seen.insert(*id))
        .cloned()
        .collect();

    if let [user_id] = unique.as_slice() {
        let email = USER_CACHE
            .get_or_load(user_id, async {
                let mut loaded = load::<C>(std::slice::from_ref(user_id), catalog_state).await?;
                Ok(loaded.remove(user_id).unwrap_or(UserEmail::NoUser))
            })
            .await?;
        return Ok(HashMap::from([(user_id.clone(), email)]));
    }

    let mut emails = HashMap::with_capacity(unique.len());
    let mut misses = Vec::new();
    for user_id in unique {
        match USER_CACHE.get(&user_id).await {
            Some(email) => {
                emails.insert(user_id, email);
            }
            None => misses.push(user_id),
        }
    }
    if misses.is_empty() {
        return Ok(emails);
    }

    let counts: Vec<_> = misses
        .iter()
        .map(|id| USER_CACHE.invalidations(id))
        .collect();
    let mut loaded = load::<C>(&misses, catalog_state).await?;
    for (user_id, count) in misses.into_iter().zip(counts) {
        let email = loaded.remove(&user_id).unwrap_or(UserEmail::NoUser);
        USER_CACHE
            .put_unless_invalidated(&user_id, email.clone(), count)
            .await;
        emails.insert(user_id, email);
    }
    Ok(emails)
}

/// Read the emails of `user_ids`. A user the read does not return has no row or is
/// deleted, and is left out.
async fn load<C: CatalogStore>(
    user_ids: &[UserId],
    catalog_state: C::State,
) -> Result<HashMap<UserId, UserEmail>, CatalogBackendError> {
    let mut emails = HashMap::with_capacity(user_ids.len());
    for chunk in user_ids.chunks(LOAD_CHUNK) {
        let users = C::list_user_membership_entries(chunk, catalog_state.clone())
            .await
            .map_err(|e| {
                CatalogBackendError::new_unexpected(std::io::Error::other(e.error.message))
            })?;
        emails.extend(
            users
                .into_iter()
                .map(|user| (user.user_id, UserEmail::of(user.email.as_deref()))),
        );
    }
    Ok(emails)
}

/// Write `email` for `user_id` after a committed user write. Fences any load that read
/// before the write, so it cannot put the old answer back.
async fn cache_user_write(user_id: &UserId, email: UserEmail) {
    let before = USER_CACHE.invalidations(user_id);
    USER_CACHE.cache_after_commit(user_id, email, before).await;
}

/// Keeps the user cache current from the user lifecycle events of this replica.
#[cfg(feature = "router")]
#[derive(Debug, Clone)]
pub struct UserCacheEventListener;

#[cfg(feature = "router")]
impl std::fmt::Display for UserCacheEventListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UserCacheEventListener")
    }
}

#[cfg(feature = "router")]
#[async_trait::async_trait]
impl EventListener for UserCacheEventListener {
    async fn user_created(&self, event: events::UserCreatedEvent) -> anyhow::Result<()> {
        cache_user_write(&event.user.id, UserEmail::of(event.user.email.as_deref())).await;
        Ok(())
    }

    async fn user_updated(&self, event: events::UserUpdatedEvent) -> anyhow::Result<()> {
        cache_user_write(&event.user.id, UserEmail::of(event.user.email.as_deref())).await;
        Ok(())
    }

    async fn user_deleted(&self, event: events::UserDeletedEvent) -> anyhow::Result<()> {
        cache_user_write(&event.user.id, UserEmail::NoUser).await;
        Ok(())
    }
}
