use std::str::FromStr;

use crate::{error::InvalidLocationError, s3::S3_CUSTOM_SCHEMES, Location};

#[derive(Debug, thiserror::Error)]
#[error("Invalid bucket name `{bucket}`: {reason}")]
pub struct InvalidBucketName {
    pub reason: String,
    pub bucket: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct S3Location {
    location: Location,
}

impl S3Location {
    /// Create a new S3 location.
    ///
    /// # Errors
    /// Fails if the bucket name is invalid or the key contains unescaped slashes.
    pub fn new(
        bucket_name: &str,
        key: &[&str],
        scheme: Option<String>,
    ) -> Result<Self, InvalidLocationError> {
        validate_bucket_name(bucket_name)
            .map_err(|e| InvalidLocationError::new(format!("s3://{bucket_name}"), e.to_string()))?;
        // Keys may not contain slashes
        if key.iter().any(|k| k.contains('/')) {
            return Err(InvalidLocationError::new(
                format!("{key:?}"),
                "S3 key contains unescaped slashes (/)".to_string(),
            ));
        }

        let scheme = scheme.unwrap_or("s3".to_string());

        if !S3_CUSTOM_SCHEMES.contains(&scheme.as_str()) && scheme != "s3" {
            return Err(InvalidLocationError::new(
                format!("s3://{bucket_name}"),
                format!("S3 location must use s3, s3a or s3n protocol. Found: {scheme}"),
            ));
        }

        let location = format!("{scheme}://{bucket_name}");
        let mut location = Location::from_str(&location).map_err(|e| {
            InvalidLocationError::new(
                location.clone(),
                format!("Failed to parse as Location - {}", e.reason),
            )
        })?;
        if !key.is_empty() {
            location.without_trailing_slash().extend(key.iter());
        }

        Ok(S3Location {
            // bucket_name,
            // key,
            location,
        })
    }

    #[must_use]
    pub fn bucket_name(&self) -> &str {
        (self.location.host_str()).unwrap_or_default()
    }

    #[must_use]
    pub fn key(&self) -> Vec<&str> {
        self.location.path_segments()
    }

    #[must_use]
    pub fn location(&self) -> &Location {
        &self.location
    }

    /// Create a new S3 location from a `Location`.
    ///
    /// If `allow_variants` is set to true, `s3a://` and `s3n://` schemes are allowed.
    ///
    /// # Errors
    /// - Fails if the location is not a valid S3 location
    pub fn try_from_location(
        location: &Location,
        allow_variants: bool,
    ) -> Result<Self, InvalidLocationError> {
        let is_custom_variant = S3_CUSTOM_SCHEMES.contains(&location.scheme());
        // Protocol must be s3
        if (location.scheme() != "s3") && !(allow_variants && is_custom_variant) {
            let reason = if allow_variants {
                format!(
                    "S3 location must use s3, s3a or s3n protocol. Found: {}",
                    location.scheme()
                )
            } else {
                format!(
                    "S3 location must use s3 protocol. Found: {}",
                    location.scheme()
                )
            };
            return Err(InvalidLocationError::new(location.to_string(), reason));
        }

        let bucket_name = location.host_str().ok_or_else(|| {
            InvalidLocationError::new(
                location.to_string(),
                "S3 location does not have a bucket name.".to_string(),
            )
        })?;

        if is_custom_variant {
            S3Location::new(
                bucket_name,
                &location.path_segments(),
                Some(location.scheme().to_string()),
            )
        } else {
            S3Location::new(bucket_name, &location.path_segments(), None)
        }
    }

    /// Create a new S3 location from a string.
    ///
    /// If `allow_s3a` is set to true, `s3a://` and `s3n://` schemes are allowed.
    ///
    /// # Errors
    /// - Fails if the location is not a valid S3 location
    pub fn try_from_str(s: &str, allow_s3a: bool) -> Result<Self, InvalidLocationError> {
        let location = Location::from_str(s).map_err(|e| {
            InvalidLocationError::new(
                s.to_string(),
                format!("Could not parse S3 location from string: {e}"),
            )
        })?;

        Self::try_from_location(&location, allow_s3a)
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

impl std::fmt::Display for S3Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.location)
    }
}

/// Validate the S3 bucket name according to S3 naming rules.
///
/// Rules:
/// - Must be between 3 and 63 characters long.
/// - Can only contain lowercase letters, numbers, dots (.), and hyphens (-).
/// - Must start and end with a letter or number.
/// - Must not contain two adjacent periods (..).
///
/// # Errors
/// If the bucket name is invalid, an `InvalidBucketName` error is returned
#[allow(clippy::missing_panics_doc)]
pub fn validate_bucket_name(bucket: &str) -> Result<(), InvalidBucketName> {
    // Bucket names must be between 3 (min) and 63 (max) characters long.
    if bucket.len() < 3 || bucket.len() > 63 {
        return Err(InvalidBucketName {
            reason: "`bucket` must be between 3 and 63 characters long.".to_string(),
            bucket: bucket.to_string(),
        });
    }

    // Bucket names can consist only of lowercase letters, numbers, dots (.), and hyphens (-).
    if !bucket
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '-')
    {
        return Err(
            InvalidBucketName {
                reason: "Bucket name can consist only of lowercase letters, numbers, dots (.), and hyphens (-).".to_string(),
                bucket: bucket.to_string(),
            }
        );
    }

    // Bucket names must begin and end with a letter or number.
    // Unwrap will not fail as the length is already checked.
    if !bucket.chars().next().unwrap().is_ascii_alphanumeric()
        || !bucket.chars().last().unwrap().is_ascii_alphanumeric()
    {
        return Err(InvalidBucketName {
            reason: "Bucket name must begin and end with a letter or number.".to_string(),
            bucket: bucket.to_string(),
        });
    }

    // Bucket names must not contain two adjacent periods.
    if bucket.contains("..") {
        return Err(InvalidBucketName {
            reason: "Bucket name must not contain two adjacent periods.".to_string(),
            bucket: bucket.to_string(),
        });
    }

    Ok(())
}

impl std::ops::Deref for S3Location {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_bucket_name() {
        let cases = vec![
            ("foo".to_string(), true),
            ("my-bucket".to_string(), true),
            ("my.bucket".to_string(), true),
            ("my..bucket".to_string(), false),
            // 64 characters
            ("a".repeat(63), true),
            ("a".repeat(64), false),
            // 2 characters
            ("a".repeat(2), false),
            ("a".repeat(3), true),
            // Special-chars
            ("1bucket".to_string(), true),
            ("my_bucket".to_string(), false),
            ("my-ö-bucket".to_string(), false),
            // Invalid start / end chars
            (".my-bucket".to_string(), false),
            ("my-bucket.".to_string(), false),
        ];

        for (bucket, expected) in cases {
            let result = validate_bucket_name(&bucket);
            if expected {
                assert!(result.is_ok());
            } else {
                assert!(result.is_err());
            }
        }
    }

    #[test]
    fn test_parse_s3_location() {
        let cases = vec![
            // (
            //     "s3://test-bucket/test_prefix/namespace/table",
            //     "test-bucket",
            //     vec!["test_prefix", "namespace", "table"],
            // ),
            // (
            //     "s3://test-bucket/test_prefix/namespace/table/",
            //     "test-bucket",
            //     vec!["test_prefix", "namespace", "table", ""],
            // ),
            // (
            //     "s3://test-bucket/test_prefix",
            //     "test-bucket",
            //     vec!["test_prefix"],
            // ),
            // (
            //     "s3://test-bucket/test_prefix/",
            //     "test-bucket",
            //     vec!["test_prefix", ""],
            // ),
            ("s3://test-bucket/", "test-bucket", vec![""]),
            ("s3://test-bucket", "test-bucket", vec![]),
            (
                "s3://bucket.with.point/foo",
                "bucket.with.point",
                vec!["foo"],
            ),
        ];

        for (location, bucket, prefix) in cases {
            let result = S3Location::try_from_str(location, false).unwrap();
            assert_eq!(result.bucket_name(), bucket);
            assert_eq!(result.key(), prefix);
            assert_eq!(result.as_str().to_string(), location);
        }
    }

    #[test]
    fn parse_invalid_s3_location() {
        let cases = vec![
            // wrong prefix
            "abc://test-bucket/foo",
            "test-bucket/foo",
            "/test-bucket/foo",
            // Invalid bucket name
            "s3://test_bucket/foo",
            // S3a is not allowed
            "s3a://test-bucket/foo",
        ];

        for case in cases {
            let result = S3Location::try_from_str(case, false);
            assert!(result.is_err());
        }
    }

    #[test]
    fn test_parse_s3_location_invalid_proto() {
        S3Location::try_from_str("adls://test-bucket/foo/", false).unwrap_err();
    }

    #[test]
    fn test_parse_s3a_location() {
        let location = S3Location::try_from_str("s3a://test-bucket/foo/", true).unwrap();
        assert_eq!(location.as_str().to_string(), "s3a://test-bucket/foo/",);
    }

    #[test]
    fn test_s3_location_display() {
        let cases = vec![
            "s3://bucket/foo",
            "s3://bucket/foo/bar",
            "s3://bucket/foo/bar/",
            "s3a://bucket/foo/bar/",
        ];
        for case in cases {
            let location = S3Location::try_from_str(case, true).unwrap();
            let printed = location.to_string();
            assert_eq!(printed, case);
        }
    }
}
