#[cfg(any(feature = "router", feature = "test-utils", test))]
mod rules;

#[cfg(feature = "router")]
pub(super) use rules::token_roles;

use crate::api::management::v1::user::UserType;

/// What Lakekeeper reads from a verified token about its principal: a display name, an email
/// and a user type.
#[derive(Debug, Clone, Default, PartialEq, typed_builder::TypedBuilder)]
pub struct PrincipalDetails {
    #[builder(default, setter(into, strip_option))]
    name: Option<String>,
    #[builder(default, setter(into, strip_option))]
    email: Option<String>,
    #[builder(default, setter(strip_option))]
    user_type: Option<UserType>,
}

impl PrincipalDetails {
    /// A display name. It is not unique and may change.
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    #[must_use]
    pub fn email(&self) -> Option<&str> {
        self.email.as_deref()
    }

    #[must_use]
    pub fn user_type(&self) -> Option<UserType> {
        self.user_type
    }
}
