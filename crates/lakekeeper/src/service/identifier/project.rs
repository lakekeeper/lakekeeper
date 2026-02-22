use std::{ops::Deref, str::FromStr, sync::Arc};

use iceberg_ext::catalog::rest::ErrorModel;

/// Reference to [`ProjectId`] that can be cheaply cloned and shared.
pub type ArcProjectId = Arc<ProjectId>;

#[derive(Debug, serde::Serialize, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "sqlx", derive(sqlx::Type))]
#[cfg_attr(feature = "sqlx", sqlx(transparent))]
#[serde(transparent)]
pub struct ProjectId(String);

impl<'de> serde::Deserialize<'de> for ProjectId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<ProjectId, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        ProjectId::try_new(s).map_err(|e| serde::de::Error::custom(e.message))
    }
}

impl From<ProjectId> for String {
    fn from(ident: ProjectId) -> Self {
        ident.0
    }
}

impl ProjectId {
    #[must_use]
    pub fn new(id: uuid::Uuid) -> Self {
        Self(id.to_string())
    }

    #[must_use]
    pub fn new_random() -> Self {
        Self(uuid::Uuid::now_v7().to_string())
    }

    /// Create a new project id from a string.
    ///
    /// # Errors
    /// Returns an error if the provided string is not a valid project id.
    /// Valid project ids may only contain alphanumeric characters, hyphens and underscores.
    pub fn try_new(id: String) -> Result<Self, ErrorModel> {
        if id.is_empty() {
            return Err(ErrorModel::bad_request(
                "Project IDs must not be empty",
                "MalformedProjectID",
                None,
            ));
        }

        if id
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
        {
            Ok(Self(id))
        } else {
            Err(ErrorModel::bad_request(
                format!(
                    "Project IDs may only contain alphanumeric characters, hyphens and underscores. Got: `{id}`",
                ),
                "MalformedProjectID",
                None,
            ))
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    #[doc(hidden)]
    pub fn from_db_unchecked(id: String) -> Self {
        Self(id)
    }
}

impl Deref for ProjectId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for ProjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for ProjectId {
    type Err = ErrorModel;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        ProjectId::try_new(s.to_string())
    }
}

impl From<uuid::Uuid> for ProjectId {
    fn from(uuid: uuid::Uuid) -> Self {
        Self::new(uuid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::ROLE_PROVIDER_SEPARATOR;

    #[test]
    fn test_project_id_cant_contain_role_separator() {
        let id = format!("invalid{}id", ROLE_PROVIDER_SEPARATOR);
        assert!(ProjectId::try_new(id).is_err());
    }
}
