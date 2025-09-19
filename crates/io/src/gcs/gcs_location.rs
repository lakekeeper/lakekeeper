use std::str::FromStr;

use crate::{InvalidLocationError, Location};

#[derive(Debug, thiserror::Error)]
#[error("Invalid GCS Bucket Name `{bucket}`: {reason}")]
pub struct InvalidGCSBucketName {
    pub reason: String,
    pub bucket: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GcsLocation {
    location: Location,
}

impl GcsLocation {
    /// Create a new [`GcsLocation`].
    ///
    /// # Errors
    /// Fails if the bucket name is invalid or the key contains unescaped slashes.
    pub fn new(bucket_name: &str, key: &[&str]) -> Result<Self, InvalidLocationError> {
        validate_bucket_name(bucket_name)
            .map_err(|e| InvalidLocationError::new(format!("gs://{bucket_name}"), e.to_string()))?;

        // Keys may not contain slashes
        if key.iter().any(|k| k.contains('/')) {
            return Err(InvalidLocationError::new(
                format!("{key:?}"),
                "GCS key contains unescaped slashes (/)".to_string(),
            ));
        }

        let location = format!("gs://{bucket_name}");
        let mut location = Location::from_str(&location).map_err(|e| {
            InvalidLocationError::new(
                location.clone(),
                format!("Failed to parse as Location - {}", e.reason),
            )
        })?;
        if !key.is_empty() {
            location.without_trailing_slash().extend(key.iter());
        }

        Ok(GcsLocation { location })
    }

    /// Get the bucket name.
    #[must_use]
    pub fn bucket_name(&self) -> &str {
        (self.location.host_str()).unwrap_or_default()
    }

    #[must_use]
    pub fn key(&self) -> Vec<&str> {
        self.location.path_segments()
    }

    #[must_use]
    pub fn object_name(&self) -> String {
        self.key().join("/")
    }

    #[must_use]
    pub fn location(&self) -> &Location {
        &self.location
    }

    /// Create a new [`GcsLocation`] location from a [`Location`].
    ///
    /// # Errors
    /// - Fails if the location is not a valid GCS location (must use `gs` scheme).
    pub fn try_from_location(location: &Location) -> Result<Self, InvalidLocationError> {
        // Protocol must be gs
        if location.scheme() != "gs" {
            let reason = format!(
                "GCS location must use gs protocol. Found: {}",
                location.scheme()
            );
            return Err(InvalidLocationError::new(location.to_string(), reason));
        }

        let bucket_name = location.host_str().ok_or_else(|| {
            InvalidLocationError::new(
                location.to_string(),
                "GCS location does not have a bucket name.".to_string(),
            )
        })?;

        GcsLocation::new(bucket_name, &location.path_segments())
    }

    /// Create a new GCS location from a string.
    ///
    /// # Errors
    /// - Fails if the location is not a valid GCS location
    pub fn try_from_str(s: &str) -> Result<Self, InvalidLocationError> {
        let location = Location::from_str(s).map_err(|e| {
            InvalidLocationError::new(
                s.to_string(),
                format!("Could not parse GCS location from string: {e}"),
            )
        })?;

        Self::try_from_location(&location)
    }

    #[must_use]
    pub fn into_location(self) -> Location {
        self.location
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.location.as_str()
    }
}

impl TryFrom<&Location> for GcsLocation {
    type Error = InvalidLocationError;

    fn try_from(location: &Location) -> Result<Self, Self::Error> {
        GcsLocation::try_from_location(location)
    }
}

impl FromStr for GcsLocation {
    type Err = InvalidLocationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        GcsLocation::try_from_str(s)
    }
}

/// Validate the GCS bucket name according to GCS naming conventions.
///
/// # Errors
/// * If the bucket name has less than 3 or more than 63 characters.
/// * If the bucket name contains invalid characters (must be lowercase letters, numbers, dots, and hyphens).
/// * If the bucket name does not start and end with a letter or number.
/// * If the bucket name contains two adjacent periods.
/// * If the bucket name is an IP address in dotted-decimal notation.
/// * If the bucket name starts with the "goog" prefix.
pub fn validate_bucket_name(bucket: &str) -> Result<(), InvalidGCSBucketName> {
    // Bucket names must be between 3 (min) and 63 (max) characters long.
    if bucket.len() < 3 || bucket.len() > 63 {
        return Err(InvalidGCSBucketName {
            reason: "must be between 3 and 63 characters long.".to_string(),
            bucket: bucket.to_string(),
        });
    }

    // Bucket names can consist only of lowercase letters, numbers, dots (.), and hyphens (-).
    if !bucket
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '-')
    {
        return Err(InvalidGCSBucketName {
            reason: "can consist only of lowercase letters, numbers, dots (.), and hyphens (-)."
                .to_string(),
            bucket: bucket.to_string(),
        });
    }

    // Bucket names must begin and end with a letter or number.
    if !bucket
        .chars()
        .next()
        .unwrap_or_default()
        .is_ascii_alphanumeric()
        || !bucket
            .chars()
            .last()
            .unwrap_or_default()
            .is_ascii_alphanumeric()
    {
        return Err(InvalidGCSBucketName {
            reason: "must begin and end with a letter or number.".to_string(),
            bucket: bucket.to_string(),
        });
    }

    // Bucket names must not contain two adjacent periods.
    if bucket.contains("..") {
        return Err(InvalidGCSBucketName {
            reason: " must not contain two adjacent periods.".to_string(),
            bucket: bucket.to_string(),
        });
    }

    // Bucket names cannot be represented as an IP address in dotted-decimal notation.
    if bucket.parse::<std::net::Ipv4Addr>().is_ok() {
        return Err(InvalidGCSBucketName {
            reason:
                "Bucket name cannot be represented as an IP address in dotted-decimal notation."
                    .to_string(),
            bucket: bucket.to_string(),
        });
    }

    // Bucket names cannot begin with the "goog" prefix.
    if bucket.starts_with("goog") {
        return Err(InvalidGCSBucketName {
            reason: "cannot begin with the \"goog\" prefix.".to_string(),
            bucket: bucket.to_string(),
        });
    }

    Ok(())
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;

    // Bucket names: Your bucket names must meet the following requirements:
    //
    // Bucket names can only contain lowercase letters, numeric characters, dashes (-), underscores (_), and dots (.). Spaces are not allowed. Names containing dots require verification.
    // Bucket names must start and end with a number or letter.
    // Bucket names must contain 3-63 characters. Names containing dots can contain up to 222 characters, but each dot-separated component can be no longer than 63 characters.
    // Bucket names cannot be represented as an IP address in dotted-decimal notation (for example, 192.168.5.4).
    // Bucket names cannot begin with the "goog" prefix.
    #[test]
    fn test_valid_bucket_names() {
        // Valid bucket names
        assert!(validate_bucket_name("valid-bucket-name").is_ok());
        assert!(validate_bucket_name("valid.bucket.name").is_ok());
        assert!(validate_bucket_name("valid-bucket-name-123").is_ok());
        assert!(validate_bucket_name("123-valid-bucket-name").is_ok());
        assert!(validate_bucket_name("valid-bucket-name-123").is_ok());
        assert!(validate_bucket_name("valid.bucket.name.123").is_ok());

        // Invalid bucket names
        assert!(validate_bucket_name("Invalid-Bucket-Name").is_err()); // Uppercase letters
        assert!(validate_bucket_name("invalid_bucket_name").is_err()); // Underscores
        assert!(validate_bucket_name("invalid bucket name").is_err()); // Spaces
        assert!(validate_bucket_name("invalid..bucket..name").is_err()); // Adjacent periods
        assert!(validate_bucket_name("invalid-bucket-name-").is_err()); // Ends with hyphen
        assert!(validate_bucket_name("-invalid-bucket-name").is_err()); // Starts with hyphen
        assert!(validate_bucket_name("192.168.5.4").is_err()); // IP address format
        assert!(validate_bucket_name("goog-bucket-name").is_err()); // Begins with "goog"
        assert!(validate_bucket_name("a").is_err()); // Less than 3 characters
        assert!(validate_bucket_name("a".repeat(64).as_str()).is_err()); // More than 63 characters
    }
}
