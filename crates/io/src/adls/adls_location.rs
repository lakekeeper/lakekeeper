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
    /// Create a new [`AdlsLocation`] from the given parameters.
    ///
    /// # Errors
    /// Fails if validation of account name, filesystem name or key fails.
    pub fn new(
        account_name: String,
        filesystem: String,
        host: String,
        key: &[&str], // Not URL Encoded
        // Optional custom prefix for the scheme, e.g., "abfss"
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
        validate_adls_host_account(&account_name)
            .map_err(|e| InvalidLocationError::new(location_dbg.clone(), e.to_string()))?;

        for path_segment in key {
            if path_segment.contains('/') {
                return Err(InvalidLocationError::new(
                    location_dbg.clone(),
                    format!("ADLS path segment `{path_segment}` must not contain slashes."),
                ));
            }
            // Reject segments whose decoded form would create silent
            // path-divergence bugs. Our `Location` stores the raw URL input,
            // but `url::Url::parse` (used by the SDK during request-URL
            // construction) normalises encoded dot-segments and may decode
            // `%2F`. The result is a Lakekeeper-side path that disagrees with
            // what Azure actually receives. Reject up-front rather than let
            // it surface as InvalidUri / silent 403 / wrong-path writes.
            //   - `%20` (and other whitespace) → Azure rejects InvalidUri
            //   - `%2E` / `%2E%2E` → `url::Url` strips, path silently shrinks
            //   - `%2F` (or any encoded slash) → ambiguous nesting
            let decoded = percent_decode_str(path_segment).decode_utf8_lossy();
            let invalid = decoded.contains('/')
                || decoded == "."
                || decoded == ".."
                || (!decoded.is_empty() && decoded.trim().is_empty());
            if invalid {
                return Err(InvalidLocationError::new(
                    location_dbg.clone(),
                    format!(
                        "ADLS path segment `{path_segment}` decodes to a value that is normalised or rejected by URL/Azure handling."
                    ),
                ));
            }
        }

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
            location.without_trailing_slash().extend(key.iter());
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

    #[must_use]
    pub fn blob_name(&self) -> String {
        // The azure_storage_datalake SDK constructs the wire URL via
        // `Url::join`, which interprets `%XX` triplets in the blob name as
        // already percent-encoded — so a stored blob name `%41bc` would go
        // out on the wire as `%41bc` and the server would URL-decode it to
        // `Abc`, aliasing two distinct keys. To keep the byte-literal model,
        // pre-encode `%` (and the other URL-syntactic chars `?`/`#`) here so
        // the SDK's join produces the right wire bytes for one server-side
        // decode pass to land on our intended key.
        use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
        const SDK_ESCAPE: &AsciiSet = &CONTROLS.add(b'%').add(b'?').add(b'#');
        let path = self.location.path().unwrap_or_default();
        utf8_percent_encode(path, SDK_ESCAPE).to_string()
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
        let host = location
            .host_str()
            .ok_or_else(|| {
                InvalidLocationError::new(
                    location.to_string(),
                    "ADLS location has no host specified".to_string(),
                )
            })?
            .to_string();
        // Host: account_name.endpoint_suffix
        let (account_name, endpoint_suffix) =
            host.split_once('.')
                .ok_or_else(|| InvalidLocationError::new(
                    location.to_string(),
                    "ADLS location host must be in the format <account_name>.<endpoint_suffix>. Specified location has no point (.)".to_string(),
                ))?;

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

/// Strict validation for a generic Azure Storage account name, as documented
/// in <https://learn.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata>.
///
/// 3-24 chars, lowercase letters and digits only. Use this for the
/// `account_name` field a user supplies on a `GenericAdlsProfile`.
///
/// # Errors
/// - If the length is not between 3 and 24 characters.
/// - If the name contains anything other than `[a-z0-9]`.
pub fn validate_storage_account_name(account_name: &str) -> Result<(), InvalidADLSAccountName> {
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

/// Permissive validation for the first DNS label of an ADLS-compatible host
/// (the `<account>` segment in `abfss://<filesystem>@<account>.<suffix>/...`).
///
/// Accepts the union of what supported backends actually use as a first label:
/// generic Azure Storage accounts (`[a-z0-9]{3,24}`), Fabric / `OneLake` global
/// (`onelake`), regional (`<region>-onelake`), and workspace-scoped
/// private-link hosts (32 lowercase-hex workspace IDs). The check is therefore
/// a generic DNS label (RFC 1035-ish): 1-63 chars, `[a-z0-9-]`, no leading or
/// trailing hyphen, no consecutive hyphens.
///
/// Strict per-backend rules live with the profile types — this is only meant
/// to be called when parsing a user-supplied URL where the backend variant
/// isn't yet known.
///
/// # Errors
/// - If the label is empty or longer than 63 characters.
/// - If it contains anything other than `[a-z0-9-]`.
/// - If it starts or ends with a hyphen, or contains consecutive hyphens.
pub fn validate_adls_host_account(account_name: &str) -> Result<(), InvalidADLSAccountName> {
    if account_name.is_empty() || account_name.len() > 63 {
        return Err(InvalidADLSAccountName {
            reason: "Must be between 1 and 63 characters long.".to_string(),
            account: account_name.to_string(),
        });
    }
    if !account_name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        return Err(InvalidADLSAccountName {
            reason: "Must contain only lowercase letters, digits, and hyphens.".to_string(),
            account: account_name.to_string(),
        });
    }
    if account_name.starts_with('-') || account_name.ends_with('-') {
        return Err(InvalidADLSAccountName {
            reason: "Must not start or end with a hyphen.".to_string(),
            account: account_name.to_string(),
        });
    }
    if account_name.contains("--") {
        return Err(InvalidADLSAccountName {
            reason: "Must not contain consecutive hyphens.".to_string(),
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

// fn urldecode(s: &str) -> String {
//     url::form_urlencoded::parse(s.as_bytes())
//         .map(|(k, v)| {
//             if v.is_empty() {
//                 k.to_string()
//             } else {
//                 format!("{k}={v}")
//             }
//         })
//         .collect::<Vec<_>>()
//         .join("&")
// }

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
    fn test_validate_storage_account_name_accepts_valid() {
        let max = "a".repeat(24);
        let cases: &[&str] = &["abc", "a1b2c3", "abc123", "123abc", "onelake", &max];
        for name in cases {
            assert!(validate_storage_account_name(name).is_ok(), "{name}");
        }
    }

    #[test]
    fn test_validate_storage_account_name_rejects_invalid() {
        let too_long = "a".repeat(25);
        // Strict path rejects hyphens — the Fabric host shapes
        // (`centralus-onelake`, 32-hex workspace IDs > 24 chars) are
        // intentionally NOT valid generic storage account names.
        let cases: &[&str] = &[
            "",
            "ab",
            &too_long,
            "Abc",
            "abc!",
            "abc-def",
            "centralus-onelake",
            "c5e8a1f37b2d4e8a9f1c3b6d8e5a2f47",
        ];
        for name in cases {
            assert!(validate_storage_account_name(name).is_err(), "{name}");
        }
    }

    #[test]
    fn test_validate_adls_host_account_accepts_valid() {
        let max = "a".repeat(63);
        let cases: &[&str] = &[
            "abc",
            "onelake",
            "centralus-onelake",
            // Fabric private-link workspace ID (32 lowercase hex chars).
            "c5e8a1f37b2d4e8a9f1c3b6d8e5a2f47",
            "westus-onelake",
            "a",
            &max,
        ];
        for name in cases {
            assert!(validate_adls_host_account(name).is_ok(), "{name}");
        }
    }

    #[test]
    fn test_validate_adls_host_account_rejects_invalid() {
        let too_long = "a".repeat(64);
        let cases: &[&str] = &[
            "", &too_long, "-foo", "foo-", "a--b", "Abc", "abc.def", "abc!",
        ];
        for name in cases {
            assert!(validate_adls_host_account(name).is_err(), "{name}");
        }
    }

    #[test]
    fn test_try_from_location_onelake_regional() {
        let loc = Location::from_str(
            "abfss://c5e8a1f3-7b2d-4e8a-9f1c-3b6d8e5a2f47@centralus-onelake.dfs.fabric.microsoft.com/lh/Files/test/",
        )
        .unwrap();
        let adls = AdlsLocation::try_from_location(&loc, false).unwrap();
        assert_eq!(adls.account_name(), "centralus-onelake");
        assert_eq!(adls.endpoint_suffix(), "dfs.fabric.microsoft.com");
    }

    #[test]
    fn test_try_from_location_onelake_private_link() {
        let loc = Location::from_str(
            "abfss://c5e8a1f3-7b2d-4e8a-9f1c-3b6d8e5a2f47@c5e8a1f37b2d4e8a9f1c3b6d8e5a2f47.zc5.dfs.fabric.microsoft.com/lh/Files/test/",
        )
        .unwrap();
        let adls = AdlsLocation::try_from_location(&loc, false).unwrap();
        assert_eq!(adls.account_name(), "c5e8a1f37b2d4e8a9f1c3b6d8e5a2f47");
        assert_eq!(adls.endpoint_suffix(), "zc5.dfs.fabric.microsoft.com");
    }

    #[test]
    fn test_strict_storage_account_name_rejects_assorted_invalid_chars() {
        for name in &["Abc", "abc!", "abc.def", "abc_def", "abc/def"] {
            assert!(validate_storage_account_name(name).is_err(), "{name}");
        }
    }

    #[test]
    fn test_parse_adls_location() {
        let cases = vec![
            (
                "abfss://filesystem@account0name.foo.com",
                "account0name",
                "filesystem",
                "foo.com",
            ),
            (
                "abfss://filesystem@account0name.dfs.core.windows.net/one",
                "account0name",
                "filesystem",
                "dfs.core.windows.net",
            ),
            (
                "abfss://filesystem@account0name.foo.com/one",
                "account0name",
                "filesystem",
                "foo.com",
            ),
            (
                "abfss://filesystem@account0name.foo.com/one/",
                "account0name",
                "filesystem",
                "foo.com",
            ),
            (
                "abfss://filesystem@account0name.foo.com/one/ã.txt",
                "account0name",
                "filesystem",
                "foo.com",
            ),
            (
                "abfss://filesystem@account0name.foo.com/one/other-file with spaces.txt",
                "account0name",
                "filesystem",
                "foo.com",
            ),
        ];

        for (location_str, account_name, filesystem, endpoint_suffix) in cases {
            let adls_location =
                AdlsLocation::try_from_location(&Location::from_str(location_str).unwrap(), false)
                    .unwrap();
            assert_eq!(adls_location.account_name(), account_name);
            assert_eq!(adls_location.filesystem(), filesystem);
            assert_eq!(adls_location.endpoint_suffix(), endpoint_suffix);
            // Roundtrip
            assert_eq!(adls_location.to_string(), location_str);
        }
    }

    #[test]
    fn test_invalid_adls_location() {
        // Each input must be rejected SOMEWHERE in the parse chain — either
        // by `Location::from_str` (e.g. host trailing dot is now globally
        // rejected for backend-aliasing safety) or by ADLS-specific
        // validation. The original test asserted only the latter.
        let cases = vec![
            "abfss://filesystem@account_name",
            "abfss://filesystem@account_name.example.com./foo",
            "s3://filesystem@account_name.dfs.core.windows/foo",
            "abfss://account_name.dfs.core.windows/foo",
        ];

        for input in cases {
            let result = Location::from_str(input).and_then(|loc| {
                AdlsLocation::try_from_location(&loc, false).map_err(|e| {
                    crate::location::LocationParseError {
                        value: input.to_string(),
                        reason: e.to_string(),
                    }
                })
            });
            assert!(
                result.is_err(),
                "{input:?} unexpectedly accepted: {result:?}"
            );
        }
    }

    #[test]
    fn test_rejects_problematic_decoded_segments() {
        for bad in [
            // whitespace-only — Azure rejects InvalidUri
            "abfss://filesystem@account0name.dfs.core.windows.net/foo/%20/bar",
            "abfss://filesystem@account0name.dfs.core.windows.net/foo/%09/bar",
            "abfss://filesystem@account0name.dfs.core.windows.net/foo/%20%20/bar",
            // dot-segments — `url::Url` normalises these, path silently shrinks
            "abfss://filesystem@account0name.dfs.core.windows.net/foo/%2E/bar",
            "abfss://filesystem@account0name.dfs.core.windows.net/foo/%2e%2e/bar",
            // encoded slash — ambiguous nesting once decoded
            "abfss://filesystem@account0name.dfs.core.windows.net/foo/%2F/bar",
            "abfss://filesystem@account0name.dfs.core.windows.net/foo/x%2Fy/bar",
        ] {
            let loc = Location::from_str(bad).unwrap();
            let res = AdlsLocation::try_from_location(&loc, false);
            assert!(res.is_err(), "expected reject for `{bad}`");
        }
        // Non-empty segments that include but do not decode-to one of the
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

    // #[test]
    // fn test_url_decode() {
    //     let cases = vec![
    //         ("key=value", "key=value"),
    //         ("%20with%20spaces", " with spaces"),
    //         ("key%2Fwith%2Fslashes=value", "key/with/slashes=value"),
    //         ("foo%3Dbar", "foo=bar"),
    //         ("/key/%C3%BCbersetzen/comp l3x", "/key/übersetzen/comp l3x"),
    //     ];

    //     for (input, expected) in cases {
    //         let decoded = urldecode(input);
    //         assert_eq!(decoded, expected, "Failed for input: {input}");
    //     }
    // }
}
