//! The emails of the user principals an audit record names: its actor, the subjects of its
//! decisions, the recipients of its grants.
//!
//! Best-effort. A principal's email comes from the request's token when the token is that
//! principal's and carries one; otherwise from the user cache and the database, in one
//! lookup per record. A lookup that fails or times out leaves the emails out; it never
//! fails, delays or blocks a request or a record. Off unless the operator enabled emails.

use std::{
    collections::{HashMap, HashSet},
    sync::LazyLock,
    time::Duration,
};

use axum_prometheus::metrics;

use super::parts::{claims_email, include_user_email};
use crate::{
    request_metadata::RequestMetadata,
    service::{UserId, events::EventCatalog},
};

/// The longest a record waits for its emails.
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(2);

const METRIC_EMAIL_LOOKUPS_TOTAL: &str = "lakekeeper_audit_email_lookups_total";

static METRICS_INITIALIZED: LazyLock<()> = LazyLock::new(|| {
    metrics::describe_counter!(
        METRIC_EMAIL_LOOKUPS_TOTAL,
        "User principals whose email an audit record looked up, by outcome: `found`, \
         `missing` (no user, or no email), `error`, `timeout`"
    );
});

fn record_lookups(outcome: &'static str, count: usize) {
    LazyLock::force(&METRICS_INITIALIZED);
    metrics::counter!(METRIC_EMAIL_LOOKUPS_TOTAL, "outcome" => outcome).increment(count as u64);
}

/// The emails of the user principals one record names, by principal.
#[derive(Debug, Default)]
pub(crate) struct Emails(HashMap<UserId, String>);

impl Emails {
    pub(crate) fn get(&self, user_id: &UserId) -> Option<String> {
        self.0.get(user_id).cloned()
    }

    /// The emails of the request's caller and of `principals`, from `catalog` where the
    /// token does not cover them. Empty with emails disabled or without a catalog.
    pub(crate) async fn resolve<'a>(
        catalog: Option<&dyn EventCatalog>,
        request_metadata: &'a RequestMetadata,
        principals: impl IntoIterator<Item = &'a UserId>,
    ) -> Self {
        let Some(catalog) = catalog.filter(|_| include_user_email()) else {
            return Self::default();
        };
        let caller = request_metadata.user_id();
        let from_token = caller.zip(claims_email(request_metadata));

        let mut seen = HashSet::new();
        let to_look_up: Vec<UserId> = caller
            .filter(|_| from_token.is_none())
            .into_iter()
            .chain(principals)
            .filter(|id| from_token.is_none_or(|(caller, _)| caller != *id))
            .filter(|id| seen.insert(*id))
            .cloned()
            .collect();

        let mut emails = lookup(catalog, &to_look_up).await;
        if let Some((caller, email)) = from_token {
            emails.insert(caller.clone(), email.to_owned());
        }
        Self(emails)
    }
}

/// Look up `user_ids`, best-effort: an error or a timeout gives no emails, logged at debug.
async fn lookup(catalog: &dyn EventCatalog, user_ids: &[UserId]) -> HashMap<UserId, String> {
    if user_ids.is_empty() {
        return HashMap::new();
    }
    match tokio::time::timeout(LOOKUP_TIMEOUT, catalog.user_emails(user_ids)).await {
        Ok(Ok(found)) => {
            let emails: HashMap<UserId, String> = found
                .into_iter()
                .filter_map(|(id, email)| email.email().map(|email| (id, email.to_owned())))
                .collect();
            record_lookups("found", emails.len());
            record_lookups("missing", user_ids.len() - emails.len());
            emails
        }
        Ok(Err(error)) => {
            tracing::debug!("Audit records carry no email: looking up users failed: {error}");
            record_lookups("error", user_ids.len());
            HashMap::new()
        }
        Err(_) => {
            tracing::debug!(
                "Audit records carry no email: looking up users took longer than {LOOKUP_TIMEOUT:?}"
            );
            record_lookups("timeout", user_ids.len());
            HashMap::new()
        }
    }
}

/// Look up the email of one user, best-effort, for a record raised outside a request: the
/// actor of a role-provider record, say. `None` with emails disabled.
pub async fn user_email(catalog: &dyn EventCatalog, user_id: &UserId) -> Option<String> {
    if !include_user_email() {
        return None;
    }
    lookup(catalog, std::slice::from_ref(user_id))
        .await
        .remove(user_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::{CatalogBackendError, user_cache::UserEmail};

    /// A catalog that must not be read.
    #[derive(Debug)]
    struct Unreachable;

    #[async_trait::async_trait]
    impl EventCatalog for Unreachable {
        async fn user_emails(
            &self,
            _user_ids: &[UserId],
        ) -> Result<HashMap<UserId, UserEmail>, CatalogBackendError> {
            panic!("emails are disabled, so nothing may be looked up");
        }
    }

    /// With emails disabled, the default, a record looks nothing up, even with a catalog.
    #[tokio::test]
    async fn disabled_emails_look_nothing_up() {
        let bob = UserId::new_unchecked("oidc", "bob");
        let caller = RequestMetadata::test_user(UserId::new_unchecked("oidc", "alice"));
        let emails = Emails::resolve(Some(&Unreachable), &caller, [&bob]).await;
        assert!(emails.0.is_empty());
    }
}
