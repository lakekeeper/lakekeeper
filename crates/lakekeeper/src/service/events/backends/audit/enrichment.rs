//! What an audit record adds about the principals it names, beyond their ids: the email of
//! a user, and the provider and source id of a role.
//!
//! Best-effort. A user's email comes from the request's token when the token is that user's
//! and carries one; otherwise from the user cache. A role's provider and source come from the
//! role cache. Each kind is looked up once per record. A lookup that fails or times out
//! leaves its fields out; it never fails, delays or blocks a request or a record.
//!
//! Emails are off unless the operator enabled them. Roles are always looked up; their source
//! id is shown unless the operator turned it off.

use std::{
    collections::{HashMap, HashSet},
    sync::LazyLock,
    time::Duration,
};

use axum_prometheus::metrics;

use super::parts::{ActorRecord, claims_email, include_user_email};
use crate::{
    request_metadata::RequestMetadata,
    service::{ArcRoleIdent, RoleId, UserId, events::EventCatalog, user_cache::cached_user_email},
};

/// The longest a record waits for one kind of lookup.
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(2);

const METRIC_EMAIL_LOOKUPS_TOTAL: &str = "lakekeeper_audit_email_lookups_total";
const METRIC_ROLE_LOOKUPS_TOTAL: &str = "lakekeeper_audit_role_lookups_total";

static METRICS_INITIALIZED: LazyLock<()> = LazyLock::new(|| {
    metrics::describe_counter!(
        METRIC_EMAIL_LOOKUPS_TOTAL,
        "User principals whose email an audit record looked up, by outcome: `found`, \
         `missing` (no user, or no email), `error`, `timeout`"
    );
    metrics::describe_counter!(
        METRIC_ROLE_LOOKUPS_TOTAL,
        "Roles whose provider and source an audit record looked up, by outcome: `found`, \
         `missing` (no such role), `error`, `timeout`"
    );
});

fn record_lookups(metric: &'static str, outcome: &'static str, count: usize) {
    LazyLock::force(&METRICS_INITIALIZED);
    metrics::counter!(metric, "outcome" => outcome).increment(count as u64);
}

/// What one record knows about the principals it names, by principal.
#[derive(Debug, Default)]
pub(crate) struct Enrichment {
    emails: HashMap<UserId, String>,
    roles: HashMap<RoleId, ArcRoleIdent>,
}

impl Enrichment {
    pub(crate) fn email(&self, user_id: &UserId) -> Option<String> {
        self.emails.get(user_id).cloned()
    }

    pub(crate) fn role(&self, role_id: &RoleId) -> Option<&ArcRoleIdent> {
        self.roles.get(role_id)
    }

    /// What a record naming `users` and `roles` adds about them, and about the request's
    /// caller: emails from the token or `catalog`, role sources from `catalog`. Emails are
    /// empty when the operator left them off; both are empty without a catalog.
    pub(crate) async fn resolve<'a>(
        catalog: Option<&dyn EventCatalog>,
        request_metadata: &'a RequestMetadata,
        users: impl IntoIterator<Item = &'a UserId>,
        roles: impl IntoIterator<Item = &'a RoleId>,
    ) -> Self {
        let Some(catalog) = catalog else {
            return Self::default();
        };
        let (emails, roles) = tokio::join!(
            emails(catalog, request_metadata, users),
            role_sources(catalog, roles),
        );
        Self { emails, roles }
    }
}

/// The emails of the caller and of `users`, from the token where it covers them.
async fn emails<'a>(
    catalog: &dyn EventCatalog,
    request_metadata: &'a RequestMetadata,
    users: impl IntoIterator<Item = &'a UserId>,
) -> HashMap<UserId, String> {
    if !include_user_email() {
        return HashMap::new();
    }
    let caller = request_metadata.user_id();
    let from_token = caller.zip(claims_email(request_metadata));

    let mut seen = HashSet::new();
    let to_look_up: Vec<UserId> = caller
        .filter(|_| from_token.is_none())
        .into_iter()
        .chain(users)
        .filter(|id| from_token.is_none_or(|(caller, _)| caller != *id))
        .filter(|id| seen.insert(*id))
        .cloned()
        .collect();

    let mut emails = lookup_emails(catalog, &to_look_up).await;
    if let Some((caller, email)) = from_token {
        emails.insert(caller.clone(), email.to_owned());
    }
    emails
}

/// Look up `user_ids`, best-effort: an error or a timeout gives no emails, logged at debug.
async fn lookup_emails(catalog: &dyn EventCatalog, user_ids: &[UserId]) -> HashMap<UserId, String> {
    if user_ids.is_empty() {
        return HashMap::new();
    }
    match tokio::time::timeout(LOOKUP_TIMEOUT, catalog.user_emails(user_ids)).await {
        Ok(Ok(found)) => {
            let emails: HashMap<UserId, String> = found
                .into_iter()
                .filter_map(|(id, email)| email.email().map(|email| (id, email.to_owned())))
                .collect();
            record_lookups(METRIC_EMAIL_LOOKUPS_TOTAL, "found", emails.len());
            record_lookups(
                METRIC_EMAIL_LOOKUPS_TOTAL,
                "missing",
                user_ids.len() - emails.len(),
            );
            emails
        }
        Ok(Err(error)) => {
            tracing::debug!("Audit records carry no email: looking up users failed: {error}");
            record_lookups(METRIC_EMAIL_LOOKUPS_TOTAL, "error", user_ids.len());
            HashMap::new()
        }
        Err(_) => {
            tracing::debug!(
                "Audit records carry no email: looking up users took longer than {LOOKUP_TIMEOUT:?}"
            );
            record_lookups(METRIC_EMAIL_LOOKUPS_TOTAL, "timeout", user_ids.len());
            HashMap::new()
        }
    }
}

/// The provider and source of each of `roles`, best-effort: an error or a timeout gives
/// none, logged at debug.
async fn role_sources<'a>(
    catalog: &dyn EventCatalog,
    roles: impl IntoIterator<Item = &'a RoleId>,
) -> HashMap<RoleId, ArcRoleIdent> {
    let mut seen = HashSet::new();
    let role_ids: Vec<RoleId> = roles
        .into_iter()
        .filter(|id| seen.insert(**id))
        .copied()
        .collect();
    if role_ids.is_empty() {
        return HashMap::new();
    }
    match tokio::time::timeout(LOOKUP_TIMEOUT, catalog.role_sources(&role_ids)).await {
        Ok(Ok(found)) => {
            record_lookups(METRIC_ROLE_LOOKUPS_TOTAL, "found", found.len());
            record_lookups(
                METRIC_ROLE_LOOKUPS_TOTAL,
                "missing",
                role_ids.len() - found.len(),
            );
            found
        }
        Ok(Err(error)) => {
            tracing::debug!("Audit records carry no role source: looking up roles failed: {error}");
            record_lookups(METRIC_ROLE_LOOKUPS_TOTAL, "error", role_ids.len());
            HashMap::new()
        }
        Err(_) => {
            tracing::debug!(
                "Audit records carry no role source: looking up roles took longer than {LOOKUP_TIMEOUT:?}"
            );
            record_lookups(METRIC_ROLE_LOOKUPS_TOTAL, "timeout", role_ids.len());
            HashMap::new()
        }
    }
}

/// The actor of a record about `user_id` raised outside the request's own records: a role
/// resolution, say. It carries the email known without a database read: from `request`'s
/// token when the token is `user_id`'s and has one, otherwise from the user cache. None with
/// emails disabled.
pub async fn principal_with_known_email(
    user_id: &UserId,
    request: Option<&RequestMetadata>,
) -> ActorRecord {
    let actor = ActorRecord::principal(user_id);
    if !include_user_email() {
        return actor;
    }
    let from_token = request
        .filter(|request| request.user_id() == Some(user_id))
        .and_then(claims_email)
        .map(str::to_owned);
    let email = match from_token {
        Some(email) => Some(email),
        None => cached_user_email(user_id)
            .await
            .and_then(|email| email.email().map(str::to_owned)),
    };
    actor.with_email(email)
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

    /// With emails disabled, the default, a record looks up no email, even with a catalog.
    #[tokio::test]
    async fn disabled_emails_look_nothing_up() {
        let bob = UserId::new_unchecked("oidc", "bob");
        let caller = RequestMetadata::test_user(UserId::new_unchecked("oidc", "alice"));
        let enrichment = Enrichment::resolve(Some(&Unreachable), &caller, [&bob], []).await;
        assert!(enrichment.emails.is_empty());
    }
}
