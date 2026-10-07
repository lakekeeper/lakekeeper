//! Read access to the catalog for event listeners.
//!
//! A listener is a trait object, registered as `Arc<dyn EventListener>`, so it is not generic
//! over the catalog store and cannot call it directly. A listener that needs to read the
//! catalog holds an `Arc<dyn EventCatalog>` instead, given at startup.
//!
//! Add the read you need as a method here, with a default body, and implement it on
//! [`CatalogStoreReader`]. A crate outside Lakekeeper can put its own extension trait on
//! [`CatalogStoreReader`] through [`CatalogStoreReader::state`].

use std::collections::HashMap;

use crate::service::{
    CatalogBackendError, CatalogStore, UserId,
    user_cache::{self, UserEmail},
};

/// Reads from the catalog that event listeners need.
///
/// Every method has a default body that reports the read as unsupported, so a test double
/// implements only the reads it is used for. [`CatalogStoreReader`] implements all of them.
#[async_trait::async_trait]
pub trait EventCatalog: Send + Sync + std::fmt::Debug {
    /// What the catalog knows about the email of each of `user_ids`, through the user
    /// cache.
    async fn user_emails(
        &self,
        user_ids: &[UserId],
    ) -> Result<HashMap<UserId, UserEmail>, CatalogBackendError> {
        let _ = user_ids;
        Err(unsupported("user_emails"))
    }
}

fn unsupported(read: &str) -> CatalogBackendError {
    CatalogBackendError::new_unexpected(std::io::Error::other(format!(
        "this event catalog does not support `{read}`"
    )))
}

/// The catalog store, as event listeners read it.
pub struct CatalogStoreReader<C: CatalogStore> {
    state: C::State,
}

impl<C: CatalogStore> CatalogStoreReader<C> {
    #[must_use]
    pub fn new(state: C::State) -> Self {
        Self { state }
    }

    /// The store's state, for an extension trait outside Lakekeeper.
    #[must_use]
    pub fn state(&self) -> &C::State {
        &self.state
    }
}

impl<C: CatalogStore> std::fmt::Debug for CatalogStoreReader<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CatalogStoreReader")
    }
}

#[async_trait::async_trait]
impl<C: CatalogStore> EventCatalog for CatalogStoreReader<C> {
    async fn user_emails(
        &self,
        user_ids: &[UserId],
    ) -> Result<HashMap<UserId, UserEmail>, CatalogBackendError> {
        user_cache::user_emails::<C>(user_ids, self.state.clone()).await
    }
}
