use std::sync::Arc;

use crate::{
    api::{RequestMetadata, management::v1::user::User},
    service::UserWrite,
};

// ===== User Events =====
//
// Fired after the write commits, and only when it changed `name`, `email`,
// `user_type` or whether the user is deleted. Each carries the user as of the moment of
// the write: a consumer that reads the row back may find it changed again, or, after a
// delete, scrubbed.

/// Event emitted when a user is created.
#[derive(Clone, Debug)]
pub struct UserCreatedEvent {
    pub user: Arc<User>,
    /// The request that made the write; `None` for a role-provider sync, which runs
    /// without one.
    pub request_metadata: Option<Arc<RequestMetadata>>,
}

/// Event emitted when a user is updated, including when a deleted user is created again.
#[derive(Clone, Debug)]
pub struct UserUpdatedEvent {
    pub user: Arc<User>,
    /// The row as it was before the write; `None` only when another transaction created
    /// the user while this write ran.
    pub previous: Option<Arc<User>>,
    /// The request that made the write; `None` for a role-provider sync, which runs
    /// without one.
    pub request_metadata: Option<Arc<RequestMetadata>>,
}

/// Event emitted when a user is deleted.
#[derive(Clone, Debug)]
pub struct UserDeletedEvent {
    /// The row as it was before the delete cleared its name and email.
    pub user: Arc<User>,
    pub request_metadata: Arc<RequestMetadata>,
}

/// The event a user write fires.
#[derive(Clone, Debug)]
pub enum UserWrittenEvent {
    Created(UserCreatedEvent),
    Updated(UserUpdatedEvent),
}

impl UserWrittenEvent {
    /// The event for `write`, made by the request `request_metadata`.
    #[must_use]
    pub fn new(write: UserWrite, request_metadata: Option<Arc<RequestMetadata>>) -> Self {
        match write {
            UserWrite::Created(user) => Self::Created(UserCreatedEvent {
                user,
                request_metadata,
            }),
            UserWrite::Updated { user, previous } => Self::Updated(UserUpdatedEvent {
                user,
                previous,
                request_metadata,
            }),
        }
    }
}
