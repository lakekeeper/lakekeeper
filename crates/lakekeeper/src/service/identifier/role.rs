use std::{str::FromStr, sync::Arc};

use iceberg_ext::catalog::rest::ErrorModel;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::service::RoleId;

pub type RoleIdentRef = Arc<RoleIdent>;

const ROLE_ID_SEPARATOR: char = '~';
/// Provider ID used for all server-managed (Lakekeeper-generated) roles.
pub(crate) const LAKEKEEPER_ROLE_PROVIDER_ID: &str = "lakekeeper";

// ── Error types ───────────────────────────────────────────────────────────────

/// Validation error for [`RoleProviderId`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RoleProviderIdError {
    #[error("provider ID cannot be empty")]
    Empty,
    #[error("provider ID must be lowercase, got: {0:?}")]
    NotLowercase(String),
    #[error("provider ID must not contain the separator '{ROLE_ID_SEPARATOR}', got: {0:?}")]
    ContainsSeparator(String),
    #[error("provider ID must not contain control characters, got: {0:?}")]
    ContainsControlChars(String),
}

/// Validation error for [`RoleSourceId`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RoleSourceIdError {
    #[error("Provided Role ID cannot be empty")]
    Empty,
    #[error("Provided Role ID must not contain control characters, got: {0:?}")]
    ContainsControlChars(String),
}

/// Validation error for [`RoleIdent`] parsing.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RoleIdError {
    #[error("invalid role provider ID: {0}")]
    InvalidProvider(#[from] RoleProviderIdError),
    #[error("invalid role source ID: {0}")]
    InvalidSourceId(#[from] RoleSourceIdError),
    #[error("role ID must be in format 'provider{ROLE_ID_SEPARATOR}id', got: `{0}`")]
    MissingFormatSeparator(String),
    #[error("role ID exceeds maximum length of {max} characters, actual length: {actual}")]
    TooLong { max: usize, actual: usize },
}

impl From<RoleIdError> for ErrorModel {
    fn from(e: RoleIdError) -> Self {
        ErrorModel::bad_request(e.to_string(), "InvalidRoleId", None)
    }
}

// ── RoleProviderId ────────────────────────────────────────────────────────────

/// Identifies the system of record that owns a role.
///
/// Must be non-empty, lowercase ASCII, and must not contain the `~` separator
/// or control characters. Well-known values: `"lakekeeper"` (server-managed),
/// `"oidc"`, `"ldap"`, etc.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct RoleProviderId(String);

impl std::default::Default for RoleProviderId {
    fn default() -> Self {
        Self(LAKEKEEPER_ROLE_PROVIDER_ID.to_string())
    }
}

impl RoleProviderId {
    /// Constructs a validated [`RoleProviderId`].
    ///
    /// # Errors
    /// Returns [`RoleProviderIdError`] if the value is empty, contains `~`,
    /// contains control characters, or is not fully lowercase.
    pub fn try_new(value: impl Into<String>) -> Result<Self, RoleProviderIdError> {
        let value = value.into();
        if value.is_empty() {
            return Err(RoleProviderIdError::Empty);
        }
        if value.contains(ROLE_ID_SEPARATOR) {
            return Err(RoleProviderIdError::ContainsSeparator(value));
        }
        if value.contains(|c: char| c.is_control()) {
            return Err(RoleProviderIdError::ContainsControlChars(value));
        }
        if value != value.to_lowercase() {
            return Err(RoleProviderIdError::NotLowercase(value));
        }
        Ok(Self(value))
    }

    /// Constructs a [`RoleProviderId`] without validation. Only for tests.
    #[cfg(any(test, feature = "test-utils"))]
    #[must_use]
    pub fn new_unchecked(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the provider ID as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn is_lakekeeper(&self) -> bool {
        self.as_str() == LAKEKEEPER_ROLE_PROVIDER_ID
    }

    #[must_use]
    pub fn is_external(&self) -> bool {
        !self.is_lakekeeper()
    }
}

impl std::fmt::Display for RoleProviderId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for RoleProviderId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for RoleProviderId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::try_new(s).map_err(serde::de::Error::custom)
    }
}

impl FromStr for RoleProviderId {
    type Err = RoleProviderIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_new(s.to_string())
    }
}

// ── RoleSourceId ──────────────────────────────────────────────────────────────

/// A role's identifier within its provider's namespace.
///
/// Must be non-empty and must not contain control characters. Case-sensitive.
/// May contain `~` (only the provider portion is forbidden from using it).
/// Examples: `"123e4567-e89b-..."` (Lakekeeper-managed), `"admin-group"` (OIDC).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct RoleSourceId(String);

impl RoleSourceId {
    /// Constructs a validated [`RoleSourceId`].
    ///
    /// # Errors
    /// Returns [`RoleSourceIdError`] if the value is empty or contains control characters.
    pub fn try_new(value: impl Into<String>) -> Result<Self, RoleSourceIdError> {
        let value = value.into();
        if value.is_empty() {
            return Err(RoleSourceIdError::Empty);
        }
        if value.contains(|c: char| c.is_control()) {
            return Err(RoleSourceIdError::ContainsControlChars(value));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn new_from_role_id(role_id: RoleId) -> Self {
        Self(role_id.to_string())
    }

    /// Constructs a [`RoleSourceId`] without validation. Only for tests.
    #[cfg(any(test, feature = "test-utils"))]
    #[must_use]
    pub fn new_unchecked(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the source ID as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for RoleSourceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for RoleSourceId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for RoleSourceId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::try_new(s).map_err(serde::de::Error::custom)
    }
}

impl FromStr for RoleSourceId {
    type Err = RoleSourceIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_new(s.to_string())
    }
}

// ── RoleIdent ─────────────────────────────────────────────────────────────────

/// Project-scoped role identifier combining a [`RoleProviderId`] and a [`RoleSourceId`].
///
/// **Not globally unique.** The same `provider~source_id` string can appear in multiple
/// projects; uniqueness is enforced by the DB only on `(project_id, provider_id, source_id)`.
/// The DB's opaque UUID primary key is the globally unique handle and is never surfaced
/// in API responses.
///
/// Serializes and parses as `"provider~source_id"` (e.g. `"lakekeeper~019756ab-..."`
/// or `"oidc~admin-group"`). This composite string is used in REST path parameters
/// and the `x-assume-role` header. The two parts are also exposed individually in
/// API responses (`provider-id` and `source-id` fields) to support filtering by provider.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RoleIdent {
    provider: RoleProviderId,
    source_id: RoleSourceId,
}

impl RoleIdent {
    #[must_use]
    pub fn new(provider: RoleProviderId, source_id: RoleSourceId) -> Self {
        Self {
            provider,
            source_id,
        }
    }

    /// Convenience constructor that validates both parts from raw strings.
    ///
    /// # Errors
    /// Returns [`RoleIdError`] if either part fails its validation rules.
    pub fn try_new_from_strs(
        provider: impl Into<String>,
        source_id: impl Into<String>,
    ) -> Result<Self, RoleIdError> {
        let provider = RoleProviderId::try_new(provider)?;
        let source_id = RoleSourceId::try_new(source_id)?;
        Ok(Self::new(provider, source_id))
    }

    /// Generates a new Lakekeeper-managed role ID with a UUIDv7 source ID.
    #[must_use]
    pub fn new_random() -> Self {
        Self {
            provider: RoleProviderId(LAKEKEEPER_ROLE_PROVIDER_ID.to_string()),
            source_id: RoleSourceId(Uuid::now_v7().to_string()),
        }
    }

    /// Generates a new Lakekeeper-managed role ID with a UUIDv7 source ID in an `Arc`.
    #[must_use]
    pub fn new_random_arc() -> Arc<Self> {
        Arc::new(Self::new_random())
    }

    /// Constructs a [`RoleIdent`] without validation. Only for tests.
    #[cfg(any(test, feature = "test-utils"))]
    #[must_use]
    pub fn new_unchecked(provider: impl Into<String>, source_id: impl Into<String>) -> Self {
        Self {
            provider: RoleProviderId(provider.into()),
            source_id: RoleSourceId(source_id.into()),
        }
    }

    /// Constructs a [`RoleIdent`] from values read directly out of the database,
    /// bypassing validation.
    ///
    /// The DB is the authoritative source of truth: values were validated on
    /// write and are assumed to be well-formed. Using this avoids redundant
    /// validation on every read and makes it clear at the call site why
    /// validation is intentionally skipped.
    ///
    /// **Do not use this outside of DB deserialization code.**
    pub(crate) fn from_db_unchecked(
        provider: impl Into<String>,
        source_id: impl Into<String>,
    ) -> Self {
        Self {
            provider: RoleProviderId(provider.into()),
            source_id: RoleSourceId(source_id.into()),
        }
    }

    /// Returns the provider portion.
    #[must_use]
    pub fn provider_id(&self) -> &RoleProviderId {
        &self.provider
    }

    /// Returns the source ID portion.
    #[must_use]
    pub fn source_id(&self) -> &RoleSourceId {
        &self.source_id
    }

    /// Parses a `provider~source_id` string, returning an [`ErrorModel`] on failure.
    ///
    /// Bare UUIDs are **not** accepted; use [`RoleIdWithFallback::from_str_or_bad_request`]
    /// for backward-compatible parsing that also accepts bare UUIDs.
    ///
    /// # Errors
    /// Returns `ErrorModel` with `BAD_REQUEST` status if the string is not a valid
    /// `provider~source_id`.
    pub fn from_str_or_bad_request(s: &str) -> Result<Self, ErrorModel> {
        Self::try_from(s).map_err(ErrorModel::from)
    }

    #[must_use]
    pub fn is_lakekeeper(&self) -> bool {
        self.provider.is_lakekeeper()
    }

    #[must_use]
    pub fn is_external(&self) -> bool {
        self.provider.is_external()
    }
}

impl std::fmt::Display for RoleIdent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{ROLE_ID_SEPARATOR}{}", self.provider, self.source_id)
    }
}

impl FromStr for RoleIdent {
    type Err = RoleIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s)
    }
}

impl TryFrom<&str> for RoleIdent {
    type Error = RoleIdError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let (provider, source_id) = s
            .split_once(ROLE_ID_SEPARATOR)
            .ok_or_else(|| RoleIdError::MissingFormatSeparator(s.to_string()))?;
        Self::try_new_from_strs(provider, source_id)
    }
}

impl TryFrom<String> for RoleIdent {
    type Error = RoleIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl Serialize for RoleIdent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for RoleIdent {
    fn deserialize<D>(deserializer: D) -> Result<RoleIdent, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        RoleIdent::try_from(s).map_err(|e| serde::de::Error::custom(e.to_string()))
    }
}

// ── RoleIdWithFallback ────────────────────────────────────────────────────────

/// Wrapper around [`RoleIdent`] that accepts bare UUIDs for backward compatibility.
///
/// Use this type for API endpoints that need to support legacy clients sending
/// role identifiers in the old bare-UUID format. In those contexts, a bare UUID
/// like `"123e4567-e89b-..."` is automatically interpreted as `"lakekeeper~123e4567-e89b-..."`.
///
/// New code and strict validation paths should use [`RoleIdent`] directly, which
/// requires the `provider~source_id` format.
///
/// # Example
/// ```ignore
/// // In headers or path parameters that must support legacy clients:
/// #[derive(Deserialize)]
/// struct AssumeRoleRequest {
///     role: RoleIdWithFallback,
/// }
///
/// let role_id: RoleIdent = request.role.into_inner();
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct RoleIdWithFallback(RoleIdent);

impl RoleIdWithFallback {
    /// Unwraps the inner [`RoleIdent`].
    #[must_use]
    pub fn into_inner(self) -> RoleIdent {
        self.0
    }

    /// Returns a reference to the inner [`RoleIdent`].
    #[must_use]
    pub fn as_role_id(&self) -> &RoleIdent {
        &self.0
    }

    /// Parses a [`RoleIdWithFallback`], returning an [`ErrorModel`] on failure.
    ///
    /// Accepts bare UUIDs as a backward-compatible shorthand for `lakekeeper~<uuid>`.
    ///
    /// # Errors
    /// Returns `ErrorModel` with `BAD_REQUEST` status if the string is not valid.
    pub fn from_str_or_bad_request(s: &str) -> Result<Self, ErrorModel> {
        Self::try_from(s).map_err(ErrorModel::from)
    }
}

impl std::ops::Deref for RoleIdWithFallback {
    type Target = RoleIdent;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<RoleIdent> for RoleIdWithFallback {
    fn from(role_id: RoleIdent) -> Self {
        Self(role_id)
    }
}

impl TryFrom<&str> for RoleIdWithFallback {
    type Error = RoleIdError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        // Backward-compat: bare UUID → lakekeeper~<uuid>
        if let Ok(uuid) = Uuid::parse_str(s) {
            return Ok(Self(RoleIdent::try_new_from_strs(
                LAKEKEEPER_ROLE_PROVIDER_ID,
                uuid.to_string(),
            )?));
        }
        Ok(Self(RoleIdent::try_from(s)?))
    }
}

impl TryFrom<String> for RoleIdWithFallback {
    type Error = RoleIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl FromStr for RoleIdWithFallback {
    type Err = RoleIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s)
    }
}

impl<'de> Deserialize<'de> for RoleIdWithFallback {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::try_from(s.as_str()).map_err(serde::de::Error::custom)
    }
}

impl std::fmt::Display for RoleIdWithFallback {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── RoleProviderId ────────────────────────────────────────────────────────

    #[test]
    fn provider_id_valid() {
        let p = RoleProviderId::try_new("lakekeeper").unwrap();
        assert_eq!(p.as_str(), "lakekeeper");
    }

    #[test]
    fn provider_id_empty() {
        assert_eq!(
            RoleProviderId::try_new("").unwrap_err(),
            RoleProviderIdError::Empty
        );
    }

    #[test]
    fn provider_id_contains_separator() {
        assert!(matches!(
            RoleProviderId::try_new("oidc~ext").unwrap_err(),
            RoleProviderIdError::ContainsSeparator(_)
        ));
    }

    #[test]
    fn provider_id_not_lowercase() {
        assert!(matches!(
            RoleProviderId::try_new("Oidc").unwrap_err(),
            RoleProviderIdError::NotLowercase(_)
        ));
    }

    #[test]
    fn provider_id_control_chars() {
        assert!(matches!(
            RoleProviderId::try_new("oidc\n").unwrap_err(),
            RoleProviderIdError::ContainsControlChars(_)
        ));
    }

    // ── RoleSourceId ──────────────────────────────────────────────────────────

    #[test]
    fn source_id_valid() {
        let id = RoleSourceId::try_new("admin-group").unwrap();
        assert_eq!(id.as_str(), "admin-group");
    }

    #[test]
    fn source_id_may_contain_separator() {
        // The source ID portion is allowed to contain ~
        let id = RoleSourceId::try_new("group~admin").unwrap();
        assert_eq!(id.as_str(), "group~admin");
    }

    #[test]
    fn source_id_empty() {
        assert_eq!(
            RoleSourceId::try_new("").unwrap_err(),
            RoleSourceIdError::Empty
        );
    }

    #[test]
    fn source_id_control_chars() {
        assert!(matches!(
            RoleSourceId::try_new("id\n").unwrap_err(),
            RoleSourceIdError::ContainsControlChars(_)
        ));
    }

    // ── RoleIdent ─────────────────────────────────────────────────────────────

    #[test]
    fn role_id_valid() {
        let r = RoleIdent::try_new_from_strs("lakekeeper", "123e4567-e89b-12d3-a456-426614174000")
            .unwrap();
        assert_eq!(r.provider_id().as_str(), "lakekeeper");
        assert_eq!(
            r.source_id().as_str(),
            "123e4567-e89b-12d3-a456-426614174000"
        );
        assert_eq!(
            r.to_string(),
            "lakekeeper~123e4567-e89b-12d3-a456-426614174000"
        );
    }

    #[test]
    fn role_id_new_random() {
        let r = RoleIdent::new_random();
        assert_eq!(r.provider_id().as_str(), "lakekeeper");
        assert!(!r.source_id().as_str().is_empty());
        assert!(r.to_string().starts_with("lakekeeper~"));
    }

    #[test]
    fn role_id_from_str() {
        let r = RoleIdent::from_str("oidc~admin-group").unwrap();
        assert_eq!(r.provider_id().as_str(), "oidc");
        assert_eq!(r.source_id().as_str(), "admin-group");
    }

    #[test]
    fn role_id_bare_uuid_rejected() {
        // Bare UUIDs are not accepted by RoleIdent; use RoleIdWithFallback for that.
        let uuid = "123e4567-e89b-12d3-a456-426614174000";
        assert!(matches!(
            RoleIdent::try_from(uuid).unwrap_err(),
            RoleIdError::MissingFormatSeparator(_)
        ));
    }

    #[test]
    fn role_id_source_id_may_contain_separator() {
        // Only the first ~ is the split point; the rest belongs to source_id
        let r = RoleIdent::from_str("oidc~group~admin").unwrap();
        assert_eq!(r.provider_id().as_str(), "oidc");
        assert_eq!(r.source_id().as_str(), "group~admin");
        assert_eq!(r.to_string(), "oidc~group~admin");
    }

    #[test]
    fn role_id_provider_with_separator_rejected() {
        // ~ in provider is now invalid
        assert!(matches!(
            RoleIdent::try_new_from_strs("oidc~ext", "group").unwrap_err(),
            RoleIdError::InvalidProvider(RoleProviderIdError::ContainsSeparator(_))
        ));
    }

    #[test]
    fn role_id_missing_separator() {
        assert!(matches!(
            RoleIdent::try_from("no-separator").unwrap_err(),
            RoleIdError::MissingFormatSeparator(_)
        ));
    }

    #[test]
    fn role_id_too_long() {
        let long_id = "a".repeat(300);
        assert!(matches!(
            RoleIdent::try_new_from_strs("provider", &long_id).unwrap_err(),
            RoleIdError::TooLong { .. }
        ));
    }

    #[test]
    fn role_id_serde_roundtrip() {
        let r = RoleIdent::new_unchecked("lakekeeper", "test-id");
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(v, serde_json::json!("lakekeeper~test-id"));
        let r2: RoleIdent = serde_json::from_value(v).unwrap();
        assert_eq!(r, r2);
    }

    // ── RoleIdWithFallback ────────────────────────────────────────────────────

    #[test]
    fn role_id_with_fallback_bare_uuid() {
        let uuid = "123e4567-e89b-12d3-a456-426614174000";
        let r = RoleIdWithFallback::try_from(uuid).unwrap();
        assert_eq!(r.provider_id().as_str(), "lakekeeper");
        assert_eq!(r.source_id().as_str(), uuid);
        assert_eq!(r.to_string(), format!("lakekeeper~{uuid}"));
    }

    #[test]
    fn role_id_with_fallback_composite() {
        let r = RoleIdWithFallback::try_from("oidc~admin-group").unwrap();
        assert_eq!(r.provider_id().as_str(), "oidc");
        assert_eq!(r.source_id().as_str(), "admin-group");
        assert_eq!(r.to_string(), "oidc~admin-group");
    }

    #[test]
    fn role_id_with_fallback_invalid() {
        // Non-UUID without separator should still fail
        assert!(matches!(
            RoleIdWithFallback::try_from("no-separator").unwrap_err(),
            RoleIdError::MissingFormatSeparator(_)
        ));
    }

    #[test]
    fn role_id_with_fallback_into_inner() {
        let uuid = "123e4567-e89b-12d3-a456-426614174000";
        let fallback = RoleIdWithFallback::try_from(uuid).unwrap();
        let role_id = fallback.into_inner();
        assert_eq!(role_id.provider_id().as_str(), "lakekeeper");
        assert_eq!(role_id.source_id().as_str(), uuid);
    }

    #[test]
    fn role_id_with_fallback_deref() {
        let fallback = RoleIdWithFallback::try_from("oidc~test").unwrap();
        // Test Deref works
        assert_eq!(fallback.provider_id().as_str(), "oidc");
        assert_eq!(fallback.source_id().as_str(), "test");
    }

    #[test]
    fn role_id_with_fallback_serde_bare_uuid() {
        let uuid = "123e4567-e89b-12d3-a456-426614174000";
        let v = serde_json::json!(uuid);
        let r: RoleIdWithFallback = serde_json::from_value(v).unwrap();
        assert_eq!(r.provider_id().as_str(), "lakekeeper");
        assert_eq!(r.source_id().as_str(), uuid);

        // Serializes as full composite format
        let out = serde_json::to_value(&r).unwrap();
        assert_eq!(out, serde_json::json!(format!("lakekeeper~{uuid}")));
    }

    #[test]
    fn role_id_with_fallback_serde_composite() {
        let v = serde_json::json!("oidc~admin");
        let r: RoleIdWithFallback = serde_json::from_value(v).unwrap();
        assert_eq!(r.to_string(), "oidc~admin");

        let out = serde_json::to_value(&r).unwrap();
        assert_eq!(out, serde_json::json!("oidc~admin"));
    }
}
