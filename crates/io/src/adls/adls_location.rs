use std::str::FromStr as _;

use percent_encoding::percent_decode_str;
use url::Host;

use crate::{
    InvalidLocationError, Location,
    adls::{ADLS_CUSTOM_SCHEMES, DEFAULT_HOST},
};

#[derive(Debug, thiserror::Error)]
#[error("Invalid ADLS filesystem / container name `{filesystem}`: {reason}")]
pub struct InvalidADLSFilesystemName {
    pub reason: String,
    pub filesystem: String,
}

#[derive(Debug, thiserror::Error)]
#[error("Invalid ADLS path segment `{segment}`: {reason}")]
pub struct InvalidADLSPathSegment {
    pub reason: String,
    pub segment: String,
}

#[derive(Debug, thiserror::Error)]
#[error("Invalid ADLS host `{host}`: {reason}")]
pub struct InvalidADLSHost {
    pub reason: String,
    pub host: String,
}

#[derive(Debug, thiserror::Error)]
#[error("Invalid ADLS Account Name `{account}`: {reason}")]
pub struct InvalidADLSAccountName {
    pub reason: String,
    pub account: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AdlsLocation {
    account_name: String,
    filesystem: String,
    endpoint_suffix: String,
    // Redundant, but useful for failsafe access
    location: Location,
}

impl AdlsLocation {
    /// Create a new [`AdlsLocation`] from already-canonical parts.
    ///
    /// `key` segments are URL-encoded canonical form (as produced by
    /// [`Location::path_segments`]); they are passed straight through
    /// [`Location::extend`] which re-canonicalises but does not change
    /// the structural meaning.
    ///
    /// This constructor is `pub(crate)` because callers should go
    /// through [`Self::try_from_location`] / [`Self::try_from_str`],
    /// which run the ADLS-specific structural and Azure-aliasing checks
    /// against the parsed `Location`.
    ///
    /// # Errors
    /// Fails if account name, filesystem name, host, scheme, or any
    /// segment violates URL canonicalisation.
    pub(crate) fn new(
        account_name: String,
        filesystem: String,
        host: String,
        key: &[&str],
        // Optional custom prefix for the scheme, e.g., "wasbs"
        scheme: Option<String>,
    ) -> Result<Self, InvalidLocationError> {
        let scheme = scheme.unwrap_or("abfss".to_string());
        let location_dbg = format!(
            "{scheme}://{filesystem}@{account_name}.{host}/{}",
            key.join("/")
        );

        if !ADLS_CUSTOM_SCHEMES.contains(&scheme.as_str()) && scheme != "abfss" {
            return Err(InvalidLocationError::new(
                location_dbg.clone(),
                format!("ADLS location must use abfss, or wasbs protocol. Found: {scheme}"),
            ));
        }

        validate_filesystem_name(&filesystem)
            .map_err(|e| InvalidLocationError::new(location_dbg.clone(), e.to_string()))?;
        validate_account_name(&account_name)
            .map_err(|e| InvalidLocationError::new(location_dbg.clone(), e.to_string()))?;

        let endpoint_suffix = normalize_host(host)
            .map_err(|e| InvalidLocationError::new(location_dbg.clone(), e.to_string()))?
            .unwrap_or(DEFAULT_HOST.to_string());

        let location = format!("{scheme}://{filesystem}@{account_name}.{endpoint_suffix}");
        let mut location = Location::from_str(&location).map_err(|e| {
            InvalidLocationError::new(
                location,
                format!("Failed to parse as Location - {}", e.reason),
            )
        })?;

        if !key.is_empty() {
            location
                .without_trailing_slash()
                .extend(key.iter())
                .map_err(|e| {
                    InvalidLocationError::new(
                        e.value,
                        format!("Failed to extend location with key segments - {}", e.reason),
                    )
                })?;
        }

        Ok(Self {
            account_name,
            filesystem,
            endpoint_suffix,
            location,
        })
    }

    #[must_use]
    pub fn location(&self) -> &Location {
        &self.location
    }

    #[must_use]
    pub fn account_name(&self) -> &str {
        &self.account_name
    }

    #[must_use]
    pub fn filesystem(&self) -> &str {
        &self.filesystem
    }

    #[must_use]
    pub fn endpoint_suffix(&self) -> &str {
        &self.endpoint_suffix
    }

    #[must_use]
    pub fn scheme(&self) -> &str {
        self.location.scheme()
    }

    /// Egress encoder: the path string fed to the Azure SDK when
    /// constructing request URLs.
    ///
    /// Returns the canonical path component, with literal `?` re-encoded
    /// to `%3F`. The Azure SDK's URL construction uses `Url::join` which
    /// truncates at the first `?` (treats it as the query separator);
    /// our canonical Location keeps `%3F` encoded already, but defensive
    /// re-encoding here ensures any future change that allows literal
    /// `?` in the path still produces a safe SDK request URL.
    #[must_use]
    pub fn blob_name(&self) -> String {
        self.location
            .path()
            .unwrap_or_default()
            .to_string()
            .replace('?', "%3F")
    }

    /// Create a new `AdlsLocation` from a Location.
    ///
    /// If `allow_variants` is set to true, `wasbs://` schemes are allowed.
    ///
    /// # Errors
    /// - Fails if the location is not a valid ADLS location
    pub fn try_from_location(
        location: &Location,
        allow_variants: bool,
    ) -> Result<Self, InvalidLocationError> {
        let schema = location.scheme();
        let is_custom_variant = ADLS_CUSTOM_SCHEMES.contains(&schema);

        // Protocol must be abfss or wasbs (if allowed)
        if schema != "abfss" && !(allow_variants && is_custom_variant) {
            let reason = if allow_variants {
                format!(
                    "ADLS location must use abfss or wasbs protocol. Found: {}",
                    location.scheme()
                )
            } else {
                format!(
                    "ADLS location must use abfss protocol. Found: {}",
                    location.scheme()
                )
            };

            return Err(InvalidLocationError::new(location.to_string(), reason));
        }

        let filesystem = location.username().unwrap_or_default().to_string();
        let host = location.host_str().to_string();
        // Host: account_name.endpoint_suffix
        let (account_name, endpoint_suffix) =
            host.split_once('.')
                .ok_or_else(|| InvalidLocationError::new(
                    location.to_string(),
                    "ADLS location host must be in the format <account_name>.<endpoint_suffix>. Specified location has no point (.)".to_string(),
                ))?;

        // Azure-specific: reject path segments that decode to whitespace-only.
        // Azure Blob/DFS endpoints reject `InvalidUri` for these. The other
        // path-traversal classes (decoded `/`, `.`, `..`, segment ending in
        // `.`) are already rejected at `Location::from_str`.
        for segment in location.path_segments() {
            let decoded = percent_decode_str(segment).decode_utf8_lossy();
            if !decoded.is_empty() && decoded.trim().is_empty() {
                return Err(InvalidLocationError::new(
                    location.to_string(),
                    format!("ADLS path segment `{segment}` decodes to whitespace only"),
                ));
            }
        }

        let custom_prefix = if is_custom_variant {
            Some(schema.to_string())
        } else {
            None
        };

        Self::new(
            account_name.to_string(),
            filesystem,
            endpoint_suffix.to_string(),
            &location.path_segments(),
            custom_prefix,
        )
    }

    /// Create a new ADLS location from a string.
    ///
    /// If `allow_wasbs` is set to true, `wasbs://` and `abfss://` schemes are allowed.
    ///
    /// # Errors
    /// - Fails if the location is not a valid ADLS location
    pub fn try_from_str(s: &str, allow_variants: bool) -> Result<Self, InvalidLocationError> {
        let location = Location::from_str(s).map_err(|e| {
            InvalidLocationError::new(
                s.to_string(),
                format!("Could not parse ADLS location from string: {e}"),
            )
        })?;

        Self::try_from_location(&location, allow_variants)
    }
}

// https://learn.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata
/// Validates the filesystem name according to Azure Storage naming rules.
///
/// # Errors
/// - If the filesystem name is empty.
/// - If the filesystem name contains consecutive hyphens.
/// - If the filesystem name is not between 3 and 63 characters long.
/// - If the filesystem name contains characters other than lowercase letters, numbers, and hyphens
/// - If the filesystem name does not begin and end with a letter or number.
///
pub fn validate_filesystem_name(container: &str) -> Result<(), InvalidADLSFilesystemName> {
    if container.is_empty() {
        return Err(InvalidADLSFilesystemName {
            reason: "Filesystem name must not be empty.".to_string(),
            filesystem: container.to_string(),
        });
    }

    // Container names must not contain consecutive hyphens.
    if container.contains("--") {
        return Err(InvalidADLSFilesystemName {
            reason: "Filesystem name must not contain consecutive hyphens.".to_string(),
            filesystem: container.to_string(),
        });
    }

    let container = container.chars().collect::<Vec<char>>();
    // Container names must be between 3 (min) and 63 (max) characters long.
    if container.len() < 3 || container.len() > 63 {
        return Err(InvalidADLSFilesystemName {
            reason: "Filesystem name must be between 3 and 63 characters long.".to_string(),
            filesystem: container.iter().collect::<String>(),
        });
    }

    // Container names can consist only of lowercase letters, numbers, and hyphens (-).
    if !container
        .iter()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == '-')
    {
        return Err(InvalidADLSFilesystemName {
            reason: "Filesystem name can only contain lowercase letters, numbers, and hyphens (-)."
                .to_string(),
            filesystem: container.iter().collect::<String>(),
        });
    }

    // Container names must begin and end with a letter or number.
    // Unwrap will not fail as the length is already checked.
    if !container.first().is_some_and(char::is_ascii_alphanumeric)
        || !container.last().is_some_and(char::is_ascii_alphanumeric)
    {
        return Err(InvalidADLSFilesystemName {
            reason: "Filesystem name must begin and end with a letter or number.".to_string(),
            filesystem: container.iter().collect::<String>(),
        });
    }

    Ok(())
}

/// Normalizes the host string.
///
/// If the host is empty, it returns `None`.
///
/// # Errors
/// - If the host contains slashes or is not a valid hostname, it returns an `InvalidADLSHost` error.
pub fn normalize_host(host: String) -> Result<Option<String>, InvalidADLSHost> {
    // If endpoint suffix is Some(""), set it to None.
    if host.is_empty() {
        Ok(None)
    } else {
        // Endpoint suffix must not contain slashes.
        if host.contains('/') {
            return Err(InvalidADLSHost {
                reason: "Must not contain slashes.".to_string(),
                host: host.clone(),
            });
        }

        // Endpoint suffix must be a valid hostname
        if Host::parse(&host).is_err() {
            return Err(InvalidADLSHost {
                reason: "Must be a valid hostname.".to_string(),
                host: host.clone(),
            });
        }

        Ok(Some(host))
    }
}

/// Validates the ADLS account name.
///
/// # Errors
/// - If the length is not between 3 and 24 characters.
/// - If the account name contains uppercase letters or special characters.
pub fn validate_account_name(account_name: &str) -> Result<(), InvalidADLSAccountName> {
    if account_name.len() < 3 || account_name.len() > 24 {
        return Err(InvalidADLSAccountName {
            reason: "Must be between 3 and 24 characters long.".to_string(),
            account: account_name.to_string(),
        });
    }

    if !account_name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
    {
        return Err(InvalidADLSAccountName {
            reason: "Must contain only lowercase letters and numbers.".to_string(),
            account: account_name.to_string(),
        });
    }

    Ok(())
}

impl std::fmt::Display for AdlsLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.location)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_valid_container_names() {
        for name in &[
            "abc", "a1b2c3", "a-b-c", "1-2-3", "a1-b2-c3", "abc123", "123abc",
        ] {
            assert!(validate_filesystem_name(name).is_ok(), "{}", name);
        }
    }

    #[test]
    fn test_invalid_container_length() {
        assert!(validate_filesystem_name("ab").is_err(), "ab");
        assert!(
            validate_filesystem_name(&"a".repeat(64)).is_err(),
            "64 character long string"
        );
    }

    #[test]
    fn test_invalid_container_characters() {
        for name in &[
            "Abc",     // Uppercase letter
            "abc!",    // Special character
            "abc.def", // Dot character
            "abc_def", // Underscore character
        ] {
            assert!(validate_filesystem_name(name).is_err(), "{}", name);
        }
    }

    #[test]
    fn test_invalid_start_end() {
        for name in &[
            "-abc",   // Starts with hyphen
            "abc-",   // Ends with hyphen
            "-abc-",  // Starts and ends with hyphen
            "1-2-3-", // Ends with hyphen
        ] {
            assert!(validate_filesystem_name(name).is_err(), "{}", name);
        }
    }

    #[test]
    fn test_consecutive_hyphens_container_name() {
        for name in &[
            "a--b", // Consecutive hyphens
            "1--2", // Consecutive hyphens
            "a--1", // Consecutive hyphens
        ] {
            assert!(validate_filesystem_name(name).is_err(), "{}", name);
        }
    }

    #[test]
    fn test_validate_host() {
        assert_eq!(
            normalize_host("dfs.core.windows.net".to_string()).unwrap(),
            Some("dfs.core.windows.net".to_string())
        );
        assert!(normalize_host(String::new()).unwrap().is_none());
    }

    #[test]
    fn test_validate_account_name() {
        for name in &["abc", "a1b2c3", "abc123", "123abc"] {
            assert!(validate_account_name(name).is_ok(), "{}", name);
        }
    }

    #[test]
    fn test_invalid_account_names() {
        for name in &["Abc", "abc!", "abc.def", "abc_def", "abc/def"] {
            assert!(validate_account_name(name).is_err(), "{}", name);
        }
    }

    #[test]
    fn test_parse_adls_location() {
        // (input, expected_canonical_form, account_name, filesystem, endpoint)
        // Non-ASCII chars and literal spaces canonicalise to percent-encoded
        // form (RFC 3986 — the only reserved alphabet for URL paths).
        let cases = vec![
            (
                "abfss://filesystem@account0name.foo.com",
                "abfss://filesystem@account0name.foo.com",
                "account0name",
                "filesystem",
                "foo.com",
            ),
            (
                "abfss://filesystem@account0name.dfs.core.windows.net/one",
                "abfss://filesystem@account0name.dfs.core.windows.net/one",
                "account0name",
                "filesystem",
                "dfs.core.windows.net",
            ),
            (
                "abfss://filesystem@account0name.foo.com/one",
                "abfss://filesystem@account0name.foo.com/one",
                "account0name",
                "filesystem",
                "foo.com",
            ),
            (
                "abfss://filesystem@account0name.foo.com/one/",
                "abfss://filesystem@account0name.foo.com/one/",
                "account0name",
                "filesystem",
                "foo.com",
            ),
            (
                "abfss://filesystem@account0name.foo.com/one/ã.txt",
                "abfss://filesystem@account0name.foo.com/one/%C3%A3.txt",
                "account0name",
                "filesystem",
                "foo.com",
            ),
            (
                "abfss://filesystem@account0name.foo.com/one/other-file with spaces.txt",
                "abfss://filesystem@account0name.foo.com/one/other-file%20with%20spaces.txt",
                "account0name",
                "filesystem",
                "foo.com",
            ),
        ];

        for (input, canonical, account_name, filesystem, endpoint_suffix) in cases {
            let adls_location =
                AdlsLocation::try_from_location(&Location::from_str(input).unwrap(), false)
                    .unwrap();
            assert_eq!(adls_location.account_name(), account_name);
            assert_eq!(adls_location.filesystem(), filesystem);
            assert_eq!(adls_location.endpoint_suffix(), endpoint_suffix);
            // Roundtrip to canonical form (not necessarily byte-identical to input).
            assert_eq!(adls_location.to_string(), canonical);
        }
    }

    #[test]
    fn test_invalid_adls_location() {
        // Some inputs are now rejected up-front by `Location::from_str`'s
        // canonicalisation (e.g. host trailing dot, decoded path-traversal
        // segments). Others still pass parsing and are rejected by
        // ADLS-specific validation. Accept either rejection point.
        let cases = vec![
            "abfss://filesystem@account_name",
            "abfss://filesystem@account_name.example.com./foo",
            "s3://filesystem@account_name.dfs.core.windows/foo",
            "abfss://account_name.dfs.core.windows/foo",
        ];

        for location in cases {
            let parse_then_adls = Location::from_str(location)
                .map_err(|_| ())
                .and_then(|l| AdlsLocation::try_from_location(&l, false).map_err(|_| ()));
            assert!(
                parse_then_adls.is_err(),
                "expected reject (parse or ADLS) for `{location}`"
            );
        }
    }

    #[test]
    fn test_rejects_problematic_decoded_segments() {
        // Most of these are now rejected up-front at `Location::from_str`
        // by the global canonicalisation rule (Phase 1 of the location
        // refactor). A few that decode to non-ASCII whitespace still pass
        // parsing on certain backends; either rejection point is acceptable
        // — what matters is that they don't reach the cloud SDK.
        for bad in [
            "abfss://filesystem@account0name.dfs.core.windows.net/foo/%20/bar",
            "abfss://filesystem@account0name.dfs.core.windows.net/foo/%09/bar",
            "abfss://filesystem@account0name.dfs.core.windows.net/foo/%20%20/bar",
            // Non-ASCII whitespace (NBSP, U+00A0) — `str::trim()` covers it,
            // so the ADLS check correctly rejects despite our segment-level
            // canonicalisation accepting it.
            "abfss://filesystem@account0name.dfs.core.windows.net/foo/%C2%A0/bar",
            "abfss://filesystem@account0name.dfs.core.windows.net/foo/%2E/bar",
            "abfss://filesystem@account0name.dfs.core.windows.net/foo/%2e%2e/bar",
            "abfss://filesystem@account0name.dfs.core.windows.net/foo/%2F/bar",
            "abfss://filesystem@account0name.dfs.core.windows.net/foo/x%2Fy/bar",
        ] {
            let parse_then_adls = Location::from_str(bad)
                .map_err(|_| ())
                .and_then(|l| AdlsLocation::try_from_location(&l, false).map_err(|_| ()));
            assert!(
                parse_then_adls.is_err(),
                "expected reject (parse or ADLS) for `{bad}`"
            );
        }
        // Segments that include — but do not decode-to — one of the
        // forbidden values are fine.
        for ok in [
            "abfss://filesystem@account0name.dfs.core.windows.net/foo/x%20y/bar",
            "abfss://filesystem@account0name.dfs.core.windows.net/foo/x%2Ey/bar",
        ] {
            let loc = Location::from_str(ok).unwrap();
            AdlsLocation::try_from_location(&loc, false)
                .unwrap_or_else(|e| panic!("expected accept for `{ok}`: {e}"));
        }
    }

    #[test]
    fn test_parse_wasbs_location() {
        let location = "wasbs://filesystem@account0name.foo.com/path/to/data";

        // Test with allow_variants = true
        let result = AdlsLocation::try_from_location(&Location::from_str(location).unwrap(), true);

        assert!(result.is_ok(), "Should parse with allow_variants = true");
        let adls_location = result.unwrap();

        // Check that it was normalized to abfss
        assert_eq!(adls_location.location().scheme(), "wasbs",);

        // Check that other properties were preserved
        assert_eq!(adls_location.account_name(), "account0name");
        assert_eq!(adls_location.filesystem(), "filesystem");
        assert_eq!(adls_location.endpoint_suffix(), "foo.com");

        // Test with allow_variants = false
        let result = AdlsLocation::try_from_location(&Location::from_str(location).unwrap(), false);
        assert!(result.is_err(), "Should fail with allow_variants = false");
    }
}
