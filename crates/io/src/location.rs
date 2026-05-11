use std::str::{FromStr, RMatchIndices};

use unicode_general_category::{GeneralCategory, get_general_category};

/// Hard cap on accepted Location length. S3 keys are spec'd at 1024 chars,
/// ADLS path components total under 1KB; multiplying by ~4 for UTF-8 worst
/// case plus scheme/authority overhead lands at 4 `KiB`. Cap up-front so
/// error paths can't allocate megabytes from a pathological input.
const MAX_LOCATION_LEN: usize = 4096;

#[derive(Debug, Eq, PartialEq, Clone)]
#[allow(clippy::struct_field_names)]
pub struct Location {
    full_location: String,
    scheme: String,
    authority_and_path: String, // Everything after ://
}

impl std::hash::Hash for Location {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.full_location.hash(state);
    }
}

#[derive(thiserror::Error, Debug, PartialEq)]
#[error("Failed to parse '{value}' as Location: {reason}")]
pub struct LocationParseError {
    pub value: String,
    pub reason: String,
}

impl Location {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.full_location
    }

    #[must_use]
    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    #[must_use]
    pub fn host_str(&self) -> Option<&str> {
        // First isolate the authority (everything before the first `/`),
        // then split userinfo off the authority via the LAST `@` (RFC 3986).
        // Splitting on `@` before isolating the authority is wrong — a
        // literal `@` in the path (e.g. `fs@host/foo@bar`) would otherwise
        // be picked up as the userinfo separator.
        let authority = self
            .authority_and_path
            .split_once('/')
            .map_or(self.authority_and_path.as_str(), |(a, _)| a);
        let host_part = authority
            .rsplit_once('@')
            .map_or(authority, |(_userinfo, h)| h);
        Some(host_part)
    }

    #[must_use]
    pub fn authority_and_path(&self) -> &str {
        &self.authority_and_path
    }

    #[must_use]
    pub fn authority_with_host(&self) -> &str {
        // Everything before the first slash of authority_and_path
        if let Some(slash_pos) = self.authority_and_path.find('/') {
            &self.authority_and_path[..slash_pos]
        } else {
            &self.authority_and_path
        }
    }

    #[must_use]
    pub fn path(&self) -> Option<&str> {
        self.authority_and_path.split_once('/').map(|x| x.1)
    }

    #[must_use]
    pub fn path_segments(&self) -> Vec<&str> {
        if let Some(path) = self.path() {
            path.split('/').collect()
        } else {
            Vec::new()
        }
    }

    #[must_use]
    pub fn username(&self) -> Option<&str> {
        self.authority_and_path
            .split_once('@')
            .and_then(|(auth, _)| auth.split_once(':').map(|(user, _)| user).or(Some(auth)))
    }

    pub fn with_trailing_slash(&mut self) -> &mut Self {
        if !self.authority_and_path.ends_with('/') {
            self.authority_and_path.push('/');
        }
        self.full_location = format!("{}://{}", self.scheme, self.authority_and_path);
        self
    }

    pub fn without_trailing_slash(&mut self) -> &mut Self {
        self.authority_and_path = self.authority_and_path.trim_end_matches('/').to_string();
        self.full_location = format!("{}://{}", self.scheme, self.authority_and_path);
        self
    }

    pub fn set_scheme_unchecked_mut(&mut self, scheme: &str) -> &mut Self {
        self.scheme = scheme.to_string();
        self.full_location = format!("{}://{}", self.scheme, self.authority_and_path);
        self
    }

    pub fn extend<I>(&mut self, segments: I) -> &mut Self
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        let extension = segments
            .into_iter()
            .map(|s| {
                if s.as_ref().is_empty() {
                    "/"
                } else {
                    s.as_ref()
                }
                .to_string()
            })
            .collect::<Vec<_>>()
            .join("/");
        // Remove duplicate slashes if any
        let extension = {
            let mut result = String::with_capacity(extension.len());
            let mut prev_slash = false;
            for ch in extension.chars() {
                if ch == '/' {
                    if !prev_slash {
                        result.push(ch);
                    }
                    prev_slash = true;
                } else {
                    result.push(ch);
                    prev_slash = false;
                }
            }
            result
        };

        if !self.authority_and_path.ends_with('/')
            && !extension.starts_with('/')
            && !extension.is_empty()
        {
            self.authority_and_path.push('/');
        }
        self.authority_and_path.push_str(&extension);
        self.full_location = format!("{}://{}", self.scheme, self.authority_and_path);
        self
    }

    pub fn push(&mut self, segment: &str) -> &mut Self {
        self.extend([segment]);
        self
    }

    #[must_use]
    pub fn cloning_push(&self, segment: &str) -> Self {
        let mut cloned = self.clone();
        cloned.push(segment);
        cloned
    }

    // /// Clones the location and pushes a segment to the path.
    // #[must_use]
    // pub fn cloning_push(&self, segment: &str) -> Self {
    //     let mut cloned = self.clone();
    //     cloned.push(segment);
    //     cloned
    // }

    // /// Follows the same logic as `url::MutPathSegments::pop`,
    // /// except that getting `MutPathSegments`is not fallible.
    // /// Non-fallibility by the constructor which checks
    // /// cannot-be-a-base.
    // pub fn pop(&mut self) -> &mut Self {
    //     if let Ok(mut path) = self.0.path_segments_mut() {
    //         path.pop();
    //     }
    //     self
    // }

    // Check if the location is a sublocation of the other location.
    // If the locations are the same, it is considered a sublocation.
    #[must_use]
    pub fn is_sublocation_of(&self, other: &Location) -> bool {
        if self == other {
            return true;
        }

        let mut other_folder = other.clone();
        other_folder.with_trailing_slash();

        self.to_string().starts_with(other_folder.as_str())
    }

    #[must_use]
    pub fn partial_locations<'a>(&'a self) -> impl IntoIterator<Item = &'a str> {
        let scheme_index = self.scheme().len() + 3; // 3 for "://"
        let url_string = self.full_location.trim_end_matches('/');
        let pointer = url_string.rmatch_indices('/');

        let iter: PartialLocationsIter<'a> = PartialLocationsIter {
            pointer,
            loc: url_string,
            full_loc: Some(url_string),
            scheme_index,
        };
        iter
    }

    #[must_use]
    /// Remove the last path segment. Always keeps the authority and host.
    /// Result is a directory (with trailing slash).
    pub fn parent(&self) -> Self {
        let mut authority_and_path = self.authority_and_path.clone();
        if let Some(last_slash) = authority_and_path.trim_end_matches('/').rfind('/') {
            authority_and_path.truncate(last_slash + 1); // Keep the trailing slash
        }

        let full_location = format!("{}://{}", self.scheme, authority_and_path);
        Location {
            full_location,
            scheme: self.scheme.clone(),
            authority_and_path,
        }
    }

    pub fn pop(&mut self) -> &mut Self {
        if let Some(last_slash) = self.authority_and_path.trim_end_matches('/').rfind('/') {
            self.authority_and_path.truncate(last_slash + 1); // Keep the trailing slash
        }
        self.full_location = format!("{}://{}", self.scheme, self.authority_and_path);
        self
    }
}

struct PartialLocationsIter<'a> {
    pointer: RMatchIndices<'a, char>,
    loc: &'a str,
    full_loc: Option<&'a str>,
    scheme_index: usize,
}

impl<'a> Iterator for PartialLocationsIter<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(full_loc) = self.full_loc.take() {
            return Some(full_loc);
        }

        let (idx, _) = self.pointer.next()?;

        if idx < self.scheme_index {
            return None;
        }
        Some(&self.loc[..idx])
    }
}

impl std::fmt::Display for Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.full_location)
    }
}

/// Reject characters that `url::Url::parse` silently strips, percent-encodes,
/// or otherwise normalises in a way that would create a parser-discrepancy
/// gap (validator sees one string, we store another).
///
/// Rejects:
/// - C0/C1 controls and DEL (Unicode `Cc` general category) — `\t\n\r` are
///   the most dangerous since `url::Url` strips them silently
/// - Bidi/format/zero-width chars (Unicode `Cf` general category) —
///   visual-spoofing AND `url::Url` percent-encodes them silently.
///   `is_format_or_invisible` defers to the `unicode-general-category`
///   crate's table, so this covers the entire Cf category (including
///   the Tag block `U+E0000..U+E007F`, the canonical ASCII-smuggling
///   vehicle that hand-rolled subsets historically miss).
fn check_unsafe_chars(s: &str) -> Result<(), String> {
    for (idx, c) in s.char_indices() {
        if c.is_control() {
            return Err(format!(
                "control character (U+{:04X}) at byte {idx} in input",
                c as u32
            ));
        }
        if is_format_or_invisible(c) {
            return Err(format!(
                "bidi/format/invisible character (U+{:04X}) at byte {idx} in input",
                c as u32
            ));
        }
    }
    Ok(())
}

/// True for any char in Unicode `Cf` general category (bidi controls,
/// zero-width formatters, BOM, Tag block `U+E0000..U+E007F`, etc.). Uses
/// the upstream Unicode tables so the rejection set stays current with
/// new Unicode releases without manual upkeep — and so it's complete:
/// hand-coded subsets historically miss the Tag block, which is the
/// canonical "ASCII smuggling" vehicle.
fn is_format_or_invisible(c: char) -> bool {
    get_general_category(c) == GeneralCategory::Format
}

/// Validate the path portion of a Location's `authority_and_path`. Rejects
/// inputs that `url::Url::parse` would silently collapse (`.`, `..`) or
/// that downstream URL parsers would interpret inconsistently
/// (empty middle segments). Trailing slash is allowed and ignored.
///
/// Applied **globally** (not gated on scheme). Of our supported backends,
/// only Azure ADLS uses `Url::join` internally and would actually collapse
/// these segments — S3/GCS treat keys as opaque bytes and would accept
/// them. Rejecting globally trades a tiny slice of valid S3/GCS namespace
/// (object keys with literal `/./`/`/../`/`//`) for one rule that can be
/// audited without a per-scheme matrix; matches the same trade-off made
/// for `check_host` on Azure trailing-dot.
fn check_path_segments(authority_and_path: &str) -> Result<(), String> {
    let Some((_authority, path)) = authority_and_path.split_once('/') else {
        return Ok(()); // No path — just authority.
    };
    if path.is_empty() {
        return Ok(());
    }
    let body = path.trim_end_matches('/');
    if body.is_empty() {
        return Ok(()); // path was just "/".
    }
    for seg in body.split('/') {
        if seg.is_empty() {
            return Err(format!(
                "empty path segment (consecutive `/`) in {authority_and_path:?}"
            ));
        }
        if seg == "." || seg == ".." {
            return Err(format!(
                "path segment `{seg}` is reserved (`.`/`..` would be \
                 collapsed by URL parsers — encode as %2E/%2E%2E if \
                 literal segment intended)"
            ));
        }
    }
    Ok(())
}

/// Reject a host with a trailing dot — Azure Blob Storage aliases
/// `account.` to `account`, so two byte-distinct catalog entries would
/// collide on the same storage account. S3/GCS treat them as distinct,
/// but globally rejecting is simpler than gating per-scheme and the
/// trailing-dot form is never useful in object-storage URIs.
fn check_host(authority_and_path: &str) -> Result<(), String> {
    // Isolate the authority FIRST (everything before the first `/`), then
    // split userinfo off the authority via the LAST `@`. Doing it in the
    // other order would let a literal `@` in the path get picked up as
    // the userinfo separator and produce a phantom "host".
    let authority = authority_and_path
        .split_once('/')
        .map_or(authority_and_path, |(a, _)| a);
    let after_userinfo = authority
        .rsplit_once('@')
        .map_or(authority, |(_userinfo, rest)| rest);
    let host = after_userinfo
        .split_once(':')
        .map_or(after_userinfo, |(h, _)| h);
    if host.ends_with('.') {
        return Err(
            "host has a trailing `.` — Azure Blob aliases `host.` to `host`, \
             reject globally to avoid backend-specific divergence"
                .to_string(),
        );
    }
    if host.is_empty() {
        // Defensive: `url::Url::parse` already rejects empty hosts on
        // special schemes, but our split-based parser here doesn't depend
        // on that and the cost of the explicit check is one comparison.
        return Err("host is empty".to_string());
    }
    Ok(())
}

impl FromStr for Location {
    type Err = LocationParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        // Length cap before any further work — every reject path clones
        // `value` into the error, so an unbounded input would let a caller
        // turn one bad request into a multi-megabyte allocation per error.
        // Don't echo the input on this rejection.
        if value.len() > MAX_LOCATION_LEN {
            return Err(LocationParseError {
                value: format!("<{} bytes, truncated>", value.len()),
                reason: format!(
                    "Location exceeds {MAX_LOCATION_LEN}-byte limit ({} bytes)",
                    value.len()
                ),
            });
        }

        // Pre-validate the raw input BEFORE handing to `url::Url::parse`,
        // because the parser silently mutates several smuggling-relevant
        // chars (`\t\n\r` stripped, `.`/`..` collapsed, controls / Cf
        // percent-encoded). We store `value` verbatim, so the validator
        // must see the same bytes the catalog will see.
        check_unsafe_chars(value).map_err(|reason| LocationParseError {
            value: value.to_string(),
            reason,
        })?;

        let location = url::Url::parse(value).map_err(|e| LocationParseError {
            value: value.to_string(),
            reason: format!("Not a valid URL - `{e}`"),
        })?;

        if location.cannot_be_a_base() {
            return Err(LocationParseError {
                value: value.to_string(),
                reason: "Malformed URL (Cannot be a base). Adding a relative path to this URL results in a malformed URL.".to_string(),
            });
        }

        if location.fragment().is_some() {
            return Err(LocationParseError {
                value: value.to_string(),
                reason: "URL has a fragment (#) — encode `#` in object names as %23".to_string(),
            });
        }

        if location.query().is_some() {
            return Err(LocationParseError {
                value: value.to_string(),
                reason: "URL has a query (?) — encode `?` in object names as %3F".to_string(),
            });
        }

        let (scheme, location) = {
            let s = value.split("://").collect::<Vec<_>>();
            if s.len() != 2 {
                return Err(LocationParseError {
                    value: value.to_string(),
                    reason: "Expected exactly one :// in the Location".to_string(),
                });
            }
            (s[0].to_string(), s[1].to_string())
        };

        check_host(&location).map_err(|reason| LocationParseError {
            value: value.to_string(),
            reason,
        })?;
        check_path_segments(&location).map_err(|reason| LocationParseError {
            value: value.to_string(),
            reason,
        })?;

        Ok(Location {
            full_location: value.to_string(),
            scheme,
            authority_and_path: location,
        })
    }
}

impl AsRef<str> for Location {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_location_with_whitespace() {
        let location = Location::from_str("s3://bucket/foo /bar").unwrap();
        assert_eq!(location.as_str(), "s3://bucket/foo /bar");
        let location = Location::from_str("s3://bucket/foo%20/bar").unwrap();
        assert_eq!(location.as_str(), "s3://bucket/foo%20/bar");
    }

    /// Inputs that must be rejected up-front because either:
    /// - `url::Url::parse` silently strips/normalises them, creating a
    ///   parser-discrepancy gap (we'd validate one string and store another)
    /// - or they trigger backend-specific aliasing (Azure host trailing dot)
    ///
    /// Every entry is a (label, input) where `Location::from_str(input)` MUST
    /// return an error with a reason that mentions the right reject category.
    #[test]
    fn test_rejects_smuggling_and_normalisation_vectors() {
        let cases: &[(&str, &str, &str)] = &[
            // (label, input, expected substring in error reason)
            ("tab in path", "s3://bucket/foo\tbar", "control"),
            ("LF in path", "s3://bucket/foo\nbar", "control"),
            ("CR in path", "s3://bucket/foo\rbar", "control"),
            ("NUL in path", "s3://bucket/foo\x00bar", "control"),
            ("DEL in path", "s3://bucket/foo\x7Fbar", "control"),
            ("BEL in path", "s3://bucket/foo\x07bar", "control"),
            // C1 controls (multibyte UTF-8: 0xC2 0x80..0xC2 0x9F)
            ("NEL in path", "s3://bucket/foo\u{0085}bar", "control"),
            // Bidi/format chars (Cf category) — visual-spoofing AND
            // url::Url silently percent-encodes them, so we'd accept inputs
            // a downstream parser might render differently.
            ("ZWSP in path", "s3://bucket/foo\u{200B}bar", "format"),
            ("RLO in path", "s3://bucket/foo\u{202E}bar", "format"),
            ("BOM in path", "s3://bucket/foo\u{FEFF}bar", "format"),
            // Tag block (U+E0000..U+E007F) — the canonical "ASCII smuggling"
            // vehicle. The hand-coded Cf table missed this; the crate-
            // backed lookup catches it. Pin so the regression couldn't
            // sneak back in if someone simplifies the check.
            ("tag char in path", "s3://bucket/foo\u{E0041}bar", "format"),
            ("language tag char", "s3://bucket/foo\u{E0001}bar", "format"),
            // Path traversal — url::Url silently collapses these, leaving us
            // with a stored path that doesn't match what the validator saw.
            ("dot segment", "s3://bucket/foo/./bar", "path segment"),
            ("dotdot segment", "s3://bucket/foo/../bar", "path segment"),
            ("trailing dot segment", "s3://bucket/foo/.", "path segment"),
            ("empty middle segment", "s3://bucket/foo//bar", "empty"),
            ("leading double slash", "s3://bucket//foo", "empty"),
            // Host trailing dot — Azure aliases `host.` to `host`, which
            // would collapse two byte-distinct catalog entries onto the same
            // storage account.
            (
                "host trailing dot",
                "abfss://fs@account.dfs.core.windows.net./path",
                "trailing",
            ),
        ];
        let mut failures = Vec::new();
        for (label, input, expected) in cases {
            match Location::from_str(input) {
                Ok(parsed) => failures.push(format!(
                    "{label}: input {input:?} unexpectedly accepted as {parsed:?}"
                )),
                Err(e) => {
                    if !e.reason.to_lowercase().contains(&expected.to_lowercase()) {
                        failures.push(format!(
                            "{label}: input {input:?} rejected, but reason {:?} \
                             does not contain expected category {expected:?}",
                            e.reason
                        ));
                    }
                }
            }
        }
        assert!(
            failures.is_empty(),
            "{} failure(s):\n  {}",
            failures.len(),
            failures.join("\n  ")
        );
    }

    /// Inputs whose chars LOOK suspicious but are legitimately representable
    /// (percent-encoded forms of the rejected chars, plus literal sub-delims
    /// and unreserved chars). The byte-literal model says these survive as-is.
    #[test]
    fn test_accepts_percent_encoded_forms_of_rejected_chars() {
        // Each input must round-trip byte-for-byte.
        let cases = [
            "s3://bucket/foo%09bar", // %09 = tab
            "s3://bucket/foo%0Abar", // %0A = LF
            "s3://bucket/foo%7Fbar", // %7F = DEL
            "s3://bucket/foo%2Ebar", // %2E = '.'
            "s3://bucket/foo+bar",
            "s3://bucket/foo~bar",
            "s3://bucket/foo!bar",
            "s3://bucket/foo'bar",
            "s3://bucket/foo*bar",
            "s3://bucket/foo$bar",
            "s3://bucket/%41bc", // alphanumeric encoded
            "s3://bucket/Abc",   // alphanumeric literal — distinct from above
            "s3://bucket/%3F",
            "s3://bucket/%3f", // hex case kept distinct
        ];
        for input in cases {
            let loc = Location::from_str(input)
                .unwrap_or_else(|e| panic!("{input:?} rejected: {}", e.reason));
            assert_eq!(loc.as_str(), input, "{input:?} mutated");
        }
    }

    #[test]
    fn test_rejects_oversized_input() {
        // Bound-check: cap is at MAX_LOCATION_LEN; one byte over must
        // reject without echoing the input back into the error.
        let prefix = "s3://bucket/";
        let pad = "a".repeat(MAX_LOCATION_LEN - prefix.len() + 1);
        let oversized = format!("{prefix}{pad}");
        assert_eq!(oversized.len(), MAX_LOCATION_LEN + 1);
        let err = Location::from_str(&oversized).unwrap_err();
        assert!(err.reason.contains("limit"), "{}", err.reason);
        // Error must NOT echo the megabyte input — keeps logs/db rows bounded.
        assert!(
            !err.value.contains("aaaa"),
            "value should be truncated marker"
        );
        // At-cap is fine.
        let pad = "a".repeat(MAX_LOCATION_LEN - prefix.len());
        let at_cap = format!("{prefix}{pad}");
        assert_eq!(at_cap.len(), MAX_LOCATION_LEN);
        Location::from_str(&at_cap).unwrap();
    }

    #[test]
    fn test_host_str_isolates_authority_before_at_split() {
        // Baseline: simple userinfo@host.
        let loc = Location::from_str("abfss://user@account.dfs.core.windows.net/foo").unwrap();
        assert_eq!(loc.host_str(), Some("account.dfs.core.windows.net"));

        // Regression: a literal `@` in the PATH must not be picked up as
        // the userinfo separator. Earlier versions did `rsplit_once('@')`
        // on the entire authority_and_path — for this input that took the
        // path's `@` and yielded host=`bar`. Fix isolates the authority
        // (first split on `/`) before splitting userinfo.
        let loc = Location::from_str("abfss://fs@account.dfs.core.windows.net/foo@bar").unwrap();
        assert_eq!(loc.host_str(), Some("account.dfs.core.windows.net"));

        // Multiple `@` in the authority itself — RFC says the last one
        // is the userinfo separator. Both accessor and `check_host` use
        // `rsplit_once`, so they agree.
        let loc = Location::from_str("s3://x@y@bucket/path").unwrap();
        assert_eq!(loc.host_str(), Some("bucket"));
    }

    #[test]
    fn test_rejects_query_and_fragment() {
        // Literal `?` and `#` in URL paths get parsed as query/fragment by
        // `url::Url`, which would diverge from our raw `authority_and_path`
        // view. Reject up-front and require percent-encoded `%3F`/`%23`.
        let frag = Location::from_str("s3://bucket/foo#bar").unwrap_err();
        assert!(frag.reason.contains("fragment"), "{}", frag.reason);
        let query = Location::from_str("s3://bucket/foo?bar").unwrap_err();
        assert!(query.reason.contains("query"), "{}", query.reason);
        // Encoded forms must still work.
        Location::from_str("s3://bucket/foo%23bar").unwrap();
        Location::from_str("s3://bucket/foo%3Fbar").unwrap();
    }

    #[test]
    fn test_parent() {
        let location = Location::from_str("s3://bucket/foo/bar").unwrap();
        let parent = location.parent();
        assert_eq!(parent.as_str(), "s3://bucket/foo/");

        let location = Location::from_str("s3://bucket/foo/bar/").unwrap();
        let parent = location.parent();
        assert_eq!(parent.as_str(), "s3://bucket/foo/");

        let location = Location::from_str("s3://bucket/").unwrap();
        let parent = location.parent();
        assert_eq!(parent.as_str(), "s3://bucket/");

        let location = Location::from_str("s3://bucket").unwrap();
        let parent = location.parent();
        assert_eq!(parent.as_str(), "s3://bucket");

        let location = Location::from_str("s3://user:pass@bucket/foo").unwrap();
        let parent = location.parent();
        assert_eq!(parent.as_str(), "s3://user:pass@bucket/");
    }

    #[test]
    fn test_pop() {
        let mut location = Location::from_str("s3://bucket/foo/bar").unwrap();
        location.pop();
        assert_eq!(location.as_str(), "s3://bucket/foo/");

        let mut location = Location::from_str("s3://bucket/foo/bar/").unwrap();
        location.pop();
        assert_eq!(location.as_str(), "s3://bucket/foo/");

        let mut location = Location::from_str("s3://bucket/").unwrap();
        location.pop();
        assert_eq!(location.as_str(), "s3://bucket/");

        let mut location = Location::from_str("s3://bucket").unwrap();
        location.pop();
        assert_eq!(location.as_str(), "s3://bucket");

        let mut location = Location::from_str("s3://user:pass@bucket/foo").unwrap();
        location.pop();
        assert_eq!(location.as_str(), "s3://user:pass@bucket/");
    }

    #[test]
    fn test_is_sublocation_of() {
        let cases = vec![
            ("s3://bucket/foo", "s3://bucket/foo", true),
            ("s3://bucket/foo/", "s3://bucket/foo/bar", true),
            ("s3://bucket/foo", "s3://bucket/foo/bar", true),
            ("s3://bucket/foo", "s3://bucket/baz/bar", false),
            ("s3://bucket/foo", "s3://bucket/foo-bar", false),
        ];

        for (parent, maybe_sublocation, expected) in cases {
            let parent = Location::from_str(parent).unwrap();
            let maybe_sublocation = Location::from_str(maybe_sublocation).unwrap();
            let result = maybe_sublocation.is_sublocation_of(&parent);
            assert_eq!(
                result, expected,
                "Parent: {parent}, Sublocation: {maybe_sublocation}, Expected: {expected}",
            );
        }
    }

    #[test]
    fn test_partial_locations() {
        let cases = vec![
            (
                "s3://bucket/foo/bar/baz",
                vec![
                    "s3://bucket",
                    "s3://bucket/foo",
                    "s3://bucket/foo/bar",
                    "s3://bucket/foo/bar/baz",
                ],
            ),
            (
                "s3://bucket/foo/bar/baz/",
                vec![
                    "s3://bucket",
                    "s3://bucket/foo",
                    "s3://bucket/foo/bar",
                    "s3://bucket/foo/bar/baz",
                ],
            ),
            ("s3://bucket", vec!["s3://bucket"]),
            ("s3://bucket/", vec!["s3://bucket"]),
        ];

        for (location, expected) in cases {
            let location = Location::from_str(location).unwrap();
            let mut result: Vec<_> = location.partial_locations().into_iter().collect();
            result.sort_unstable();
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_extend() {
        let mut location = Location::from_str("s3://bucket").unwrap();
        location.extend(["foo", "bar"]);
        assert_eq!(location.as_str(), "s3://bucket/foo/bar");

        let mut location = Location::from_str("s3://bucket/").unwrap();
        location.extend(["foo", "bar"]);
        assert_eq!(location.as_str(), "s3://bucket/foo/bar");
    }

    #[test]
    fn test_push() {
        let mut location = Location::from_str("s3://bucket").unwrap();
        location.push("foo");
        assert_eq!(location.as_str(), "s3://bucket/foo");

        let mut location = Location::from_str("s3://bucket/").unwrap();
        location.push("foo");
        assert_eq!(location.as_str(), "s3://bucket/foo");
    }

    #[test]
    fn test_path() {
        let location = Location::from_str("s3://bucket/foo/bar").unwrap();
        assert_eq!(location.path(), Some("foo/bar"));

        let location = Location::from_str("s3://bucket/").unwrap();
        assert_eq!(location.path(), Some(""));

        let location = Location::from_str("s3://bucket").unwrap();
        assert_eq!(location.path(), None);
    }

    #[test]
    fn test_path_segments() {
        let location = Location::from_str("s3://bucket/foo/bar").unwrap();
        assert_eq!(location.path_segments(), vec!["foo", "bar"]);

        let location = Location::from_str("s3://bucket/foo/bar/").unwrap();
        assert_eq!(location.path_segments(), vec!["foo", "bar", ""]);

        let location = Location::from_str("s3://bucket/").unwrap();
        assert_eq!(location.path_segments(), vec![""]);

        let location = Location::from_str("s3://bucket").unwrap();
        assert_eq!(location.path_segments(), Vec::<&str>::new());
    }

    #[test]
    fn test_username() {
        let location = Location::from_str("s3://user:pass@bucket/foo/bar").unwrap();
        assert_eq!(location.username(), Some("user"));

        let location = Location::from_str("s3://bucket/foo/bar").unwrap();
        assert_eq!(location.username(), None);

        let location = Location::from_str("s3://user@bucket/foo/bar").unwrap();
        assert_eq!(location.username(), Some("user"));
    }

    #[test]
    fn test_authority_with_host() {
        let location = Location::from_str("s3://bucket/foo/bar").unwrap();
        assert_eq!(location.authority_with_host(), "bucket");

        let location = Location::from_str("s3://user@bucket/foo/bar").unwrap();
        assert_eq!(location.authority_with_host(), "user@bucket");

        let location = Location::from_str("s3://user:pass@bucket/foo/bar").unwrap();
        assert_eq!(location.authority_with_host(), "user:pass@bucket");

        let location = Location::from_str("s3://bucket/").unwrap();
        assert_eq!(location.authority_with_host(), "bucket");
    }

    #[test]
    fn test_with_trailing_slash() {
        let mut location = Location::from_str("s3://bucket/foo/bar").unwrap();
        location.with_trailing_slash();
        assert_eq!(location.as_str(), "s3://bucket/foo/bar/");
        assert_eq!(location.full_location, "s3://bucket/foo/bar/");
        assert_eq!(location.authority_and_path, "bucket/foo/bar/");

        let mut location = Location::from_str("s3://bucket/foo/bar/").unwrap();
        location.with_trailing_slash();
        assert_eq!(location.as_str(), "s3://bucket/foo/bar/");
        assert_eq!(location.full_location, "s3://bucket/foo/bar/");
        assert_eq!(location.authority_and_path, "bucket/foo/bar/");
    }

    #[test]
    fn test_without_trailing_slash() {
        let mut location = Location::from_str("s3://bucket/foo/bar/").unwrap();
        location.without_trailing_slash();
        assert_eq!(location.as_str(), "s3://bucket/foo/bar");
        assert_eq!(location.full_location, "s3://bucket/foo/bar");
        assert_eq!(location.authority_and_path, "bucket/foo/bar");

        let mut location = Location::from_str("s3://bucket/foo/bar").unwrap();
        location.without_trailing_slash();
        assert_eq!(location.as_str(), "s3://bucket/foo/bar");
        assert_eq!(location.authority_and_path, "bucket/foo/bar");
    }
}
