use std::str::{FromStr, RMatchIndices};

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};

/// A canonical URL-string location.
///
/// Two `Location`s are equal iff their canonical strings are byte-equal.
/// `Location::from_str` is the only constructor that produces a canonical
/// form; `extend`/`push` re-canonicalise after appending.
///
/// Canonicalisation rules (RFC 3986-grounded; see
/// `docs/location-design.md` and `docs/location-refactor-plan.md`):
/// - Reject NUL, tab, CR, LF, other C0 controls, DEL, bidi/format chars
///   anywhere in the input.
/// - Lowercase scheme and host. Reject host trailing dot.
/// - Per path segment: decode `%XX` for unreserved and sub-delim bytes;
///   keep `%XX` (uppercase hex) for reserved bytes (`?`, `#`, `[`, `]`,
///   `%`); percent-encode literal non-ASCII / reserved / space; reject
///   decoded `/`, `.`, `..`, empty segments, segments ending with `.`,
///   and decoded controls.
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

    /// The host (and optional `:port`), with userinfo and path stripped.
    /// Always non-empty for a canonical Location.
    #[must_use]
    pub fn host_str(&self) -> &str {
        // Everything after `@` (if present), up to the first `/`.
        let after_userinfo = match self.authority_and_path.find('@') {
            Some(at_pos) => &self.authority_and_path[at_pos + 1..],
            None => &self.authority_and_path,
        };
        match after_userinfo.find('/') {
            Some(slash_pos) => &after_userinfo[..slash_pos],
            None => after_userinfo,
        }
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

    /// Rebuild `full_location` from `scheme` + `authority_and_path`.
    /// Every mutator that touches either field must call this to keep the
    /// canonical invariant intact.
    fn rebuild_full_location(&mut self) {
        self.full_location.clear();
        self.full_location.push_str(&self.scheme);
        self.full_location.push_str("://");
        self.full_location.push_str(&self.authority_and_path);
    }

    pub fn with_trailing_slash(&mut self) -> &mut Self {
        if !self.authority_and_path.ends_with('/') {
            self.authority_and_path.push('/');
        }
        self.rebuild_full_location();
        self
    }

    pub fn without_trailing_slash(&mut self) -> &mut Self {
        // Strip exactly one trailing `/` (canonical form has at most one).
        if let Some(stripped) = self.authority_and_path.strip_suffix('/') {
            self.authority_and_path.truncate(stripped.len());
        }
        self.rebuild_full_location();
        self
    }

    /// Replace the scheme without re-validating. The caller MUST pass a
    /// canonical scheme (lowercase ASCII, starts with letter). Otherwise
    /// the resulting `Location` violates the canonical invariant and
    /// `Location::from_str(self.as_str())` will fail or produce a
    /// different value.
    ///
    /// Intended only for scheme-family normalisation against a known
    /// allow-list (e.g. `s3a` → `s3`, `wasbs` → `abfss`). Do not use with
    /// caller-supplied scheme strings.
    pub fn set_scheme_unchecked_mut(&mut self, scheme: &str) -> &mut Self {
        debug_assert!(
            !scheme.is_empty()
                && scheme.bytes().all(|b| b.is_ascii_lowercase()
                    || b.is_ascii_digit()
                    || matches!(b, b'+' | b'-' | b'.'))
                && scheme.as_bytes()[0].is_ascii_lowercase(),
            "set_scheme_unchecked_mut called with non-canonical scheme `{scheme}` — see doc",
        );
        self.scheme = scheme.to_string();
        self.rebuild_full_location();
        self
    }

    /// Append path segments. Each non-empty segment is canonicalised
    /// (decoded unreserved/sub-delim, percent-encoded reserved/space/
    /// non-ASCII).
    ///
    /// Empty-segment handling:
    /// - **trailing** empty preserves a trailing `/` on the result —
    ///   `extend(["foo", ""])` ends in `/foo/`. `extend([""])` alone is
    ///   equivalent to `with_trailing_slash`.
    /// - **leading** empty(ies) is/are a no-op — `extend(["", "foo"])`
    ///   and `extend(["", "", "foo"])` are both the same as
    ///   `extend(["foo"])`. `extend` always inserts a separator before
    ///   the first canonical segment, so leading `""`s add nothing.
    /// - **middle** empty (between two non-empty segments) is rejected
    ///   — it would create consecutive `/` in the canonical path, which
    ///   `Location::from_str` also rejects.
    ///
    /// # Errors
    /// Fails if any segment violates canonicalisation rules: contains
    /// `/`, decodes to `.`/`..`, ends with `.`, contains a control byte
    /// / smuggling char, or is an empty middle segment.
    pub fn extend<I>(&mut self, segments: I) -> Result<&mut Self, LocationParseError>
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        // Materialise so we can index for the trailing-slash sentinel and
        // for distinguishing leading / middle / trailing empties.
        // `Vec<I::Item>` doesn't copy string contents — it's just
        // `Vec<&str>` for `extend(["foo", "bar"])`.
        let segments: Vec<I::Item> = segments.into_iter().collect();
        let last_idx = segments.len().saturating_sub(1);
        let extension_has_trailing_slash = segments.last().is_some_and(|s| s.as_ref().is_empty());

        let mut canon_segs: Vec<String> = Vec::with_capacity(segments.len());
        for (i, item) in segments.iter().enumerate() {
            let s = item.as_ref();
            if s.is_empty() {
                if i == last_idx {
                    continue; // trailing-slash sentinel
                }
                if canon_segs.is_empty() {
                    continue; // leading empty (no-op)
                }
                return Err(LocationParseError {
                    value: self.full_location.clone(),
                    reason: format!(
                        "empty path segment at position {i} would create consecutive `/`"
                    ),
                });
            }
            check_smuggling_chars(s).map_err(|reason| LocationParseError {
                value: self.full_location.clone(),
                reason: format!("invalid extension segment: {reason}"),
            })?;
            let canon = canonicalize_segment(s).map_err(|reason| LocationParseError {
                value: self.full_location.clone(),
                reason: format!("invalid extension segment: {reason}"),
            })?;
            canon_segs.push(canon);
        }

        if canon_segs.is_empty() {
            if extension_has_trailing_slash && !self.authority_and_path.ends_with('/') {
                self.authority_and_path.push('/');
                self.rebuild_full_location();
            }
            return Ok(self);
        }
        let extension = canon_segs.join("/");
        if !self.authority_and_path.ends_with('/') {
            self.authority_and_path.push('/');
        }
        self.authority_and_path.push_str(&extension);
        if extension_has_trailing_slash {
            self.authority_and_path.push('/');
        }
        self.rebuild_full_location();
        Ok(self)
    }

    /// Append a single path segment. See [`Location::extend`] for rules.
    ///
    /// # Errors
    /// See [`Location::extend`].
    pub fn push(&mut self, segment: &str) -> Result<&mut Self, LocationParseError> {
        self.extend([segment])?;
        Ok(self)
    }

    /// True if `self` is the same as or nested inside `other`.
    ///
    /// Operates on canonical strings: `self.as_str()` must start with
    /// `other.as_str()` plus a `/` boundary (or be byte-equal). No
    /// allocations.
    #[must_use]
    pub fn is_sublocation_of(&self, other: &Location) -> bool {
        let mine = self.as_str();
        let theirs = other.as_str();
        if mine == theirs {
            return true;
        }
        // `theirs` already ends with `/`: pure prefix check.
        if let Some(stripped) = theirs.strip_suffix('/') {
            if mine.len() > stripped.len() && mine.as_bytes()[stripped.len()] == b'/' {
                return mine.starts_with(stripped);
            }
            return false;
        }
        // `theirs` has no trailing slash: `mine` must start with `theirs/`.
        mine.len() > theirs.len()
            && mine.as_bytes()[theirs.len()] == b'/'
            && mine.starts_with(theirs)
    }

    pub fn partial_locations(&self) -> impl Iterator<Item = &str> {
        let scheme_index = self.scheme().len() + 3; // 3 for "://"
        let url_string = self.full_location.trim_end_matches('/');
        PartialLocationsIter {
            pointer: url_string.rmatch_indices('/'),
            loc: url_string,
            full_loc: Some(url_string),
            scheme_index,
        }
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
        self.rebuild_full_location();
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

impl FromStr for Location {
    type Err = LocationParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        // Reject smuggling chars upfront. Verified empirically that
        // `url::Url::parse` silently strips `\t`/`\n`/`\r` from paths,
        // which would let unsigned bytes reach downstream consumers.
        check_smuggling_chars(value).map_err(|reason| LocationParseError {
            value: value.to_string(),
            reason,
        })?;

        let url = url::Url::parse(value).map_err(|e| LocationParseError {
            value: value.to_string(),
            reason: format!("Not a valid URL - `{e}`"),
        })?;

        if url.cannot_be_a_base() {
            return Err(LocationParseError {
                value: value.to_string(),
                reason: "Malformed URL (Cannot be a base). Adding a relative path to this URL results in a malformed URL.".to_string(),
            });
        }

        if url.fragment().is_some() {
            return Err(LocationParseError {
                value: value.to_string(),
                reason: "URL has a fragment (#) — encode `#` in object names as %23".to_string(),
            });
        }

        if url.query().is_some() {
            return Err(LocationParseError {
                value: value.to_string(),
                reason: "URL has a query (?) — encode `?` in object names as %3F".to_string(),
            });
        }

        // `url.scheme()` is already lowercased by the parser.
        let scheme = url.scheme().to_string();
        let authority_and_path =
            canonicalize_url_to_authority_and_path(value, &url).map_err(|reason| {
                LocationParseError {
                    value: value.to_string(),
                    reason,
                }
            })?;

        let full_location = format!("{scheme}://{authority_and_path}");
        Ok(Location {
            full_location,
            scheme,
            authority_and_path,
        })
    }
}

impl AsRef<str> for Location {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

// --- Canonicalisation helpers --------------------------------------------
//
// Built on top of two pre-existing crates:
//   - `url` (WHATWG): used as a syntax validator + extracts authority parts
//     (userinfo, host, port). For non-special schemes (s3/abfss/gs/etc.) it
//     does NOT collapse `..`/`.`, so we can trust its path view too.
//   - `percent-encoding`: per-segment decode-and-re-encode. The custom
//     `CANONICAL_PATH_ENCODE_SET` declares "encode everything except
//     unreserved + sub-delims" — `utf8_percent_encode` then emits uppercase
//     hex for the kept-encoded bytes, collapsing `%2d`/`%2D`/`-` to `-` and
//     `%3f`/`%3F` to `%3F`.
//
// Layered on top: smuggling-char rejection (because `url::Url` silently
// strips `\t`/`\n`/`\r`), path-traversal rejection (because we want to
// REJECT `.`/`..`, not normalize them away), Azure-blob trailing-dot
// rejection, and consecutive-slash rejection.

/// `AsciiSet` for `utf8_percent_encode` to produce canonical path segments.
///
/// Encodes everything except RFC 3986 §3.3 `pchar`:
///
/// ```text
/// pchar      = unreserved / sub-delims / ":" / "@"        §3.3
/// unreserved = ALPHA / DIGIT / "-" / "." / "_" / "~"      §2.3
/// sub-delims = "!" / "$" / "&" / "'" / "(" / ")"
///            / "*" / "+" / "," / ";" / "="                §2.2
/// ```
///
/// `NON_ALPHANUMERIC` already encodes everything except alphanumerics,
/// so we just `remove()` the non-alphanum bytes that pchar keeps literal.
const CANONICAL_PATH_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    // unreserved (non-alphanumeric part)
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~')
    // sub-delims
    .remove(b'!')
    .remove(b'$')
    .remove(b'&')
    .remove(b'\'')
    .remove(b'(')
    .remove(b')')
    .remove(b'*')
    .remove(b'+')
    .remove(b',')
    .remove(b';')
    .remove(b'=')
    // pchar extras (gen-delims allowed in a path segment)
    .remove(b':')
    .remove(b'@');

/// Reject Unicode control characters (general category `Cc` — C0 controls,
/// DEL, C1 controls) in the input string. Closes the WHATWG-vs-RFC parser-
/// discrepancy gap: `url::Url::parse` silently strips `\t`/`\n`/`\r`
/// (verified empirically), so we must reject before the parser sees them.
///
/// Bidi/format chars (`Cf` general category, e.g. `U+200B` ZWSP, `U+202E`
/// RLO) are intentionally NOT rejected here — they're percent-encoded by
/// canonicalisation (`<RLO>` → `%E2%80%AE`), so they don't introduce
/// aliasing. Visual-spoofing defence belongs to the display layer.
fn check_smuggling_chars(s: &str) -> Result<(), String> {
    for (idx, c) in s.char_indices() {
        if c.is_control() {
            return Err(format!(
                "control character (U+{:04X}) at byte {idx} in input",
                c as u32
            ));
        }
    }
    Ok(())
}

/// Canonicalise a single URL path segment.
///
/// Algorithm: percent-decode → security checks on decoded bytes →
/// re-encode using `CANONICAL_PATH_ENCODE_SET`. Decode-then-re-encode
/// collapses equivalence classes (`%2D`/`%2d`/`-` all → `-`) and produces
/// uppercase hex for kept-encoded bytes (`%3f` → `%3F`).
///
/// Errors:
/// - empty segment
/// - decodes to `.`, `..`, or contains `/` (`%2F` ambiguous nesting)
/// - decoded byte is a C0 control or DEL
/// - segment ends in `.` (Azure Blob endpoint aliases `foo.` to `foo`)
/// - decoded bytes are not valid UTF-8
fn canonicalize_segment(seg: &str) -> Result<String, String> {
    if seg.is_empty() {
        return Err("empty path segment".to_string());
    }
    let decoded: Vec<u8> = percent_decode_str(seg).collect();
    if decoded.iter().any(u8::is_ascii_control) {
        return Err(format!("decoded ASCII control byte in segment `{seg}`"));
    }
    if decoded.contains(&b'/') {
        return Err(format!("decoded `/` (literal or `%2F`) in segment `{seg}`"));
    }
    if decoded.as_slice() == b"." || decoded.as_slice() == b".." {
        return Err(format!(
            "path segment decodes to `{}` (path-traversal)",
            std::str::from_utf8(&decoded).unwrap_or("?")
        ));
    }
    if decoded.last() == Some(&b'.') {
        // Azure Blob endpoint silently strips trailing dots, S3/GCS preserve.
        // Reject globally to avoid backend-specific aliasing surprises.
        return Err(format!(
            "path segment `{seg}` decodes to a value ending in `.`"
        ));
    }
    let decoded_str = std::str::from_utf8(&decoded)
        .map_err(|_| format!("path segment `{seg}` decodes to non-UTF-8 bytes"))?;
    Ok(utf8_percent_encode(decoded_str, CANONICAL_PATH_ENCODE_SET).to_string())
}

/// Canonicalise a path string (segments joined by `/`, with optional
/// trailing slash). Rejects empty middle segments (consecutive `/`).
fn canonicalize_path(path: &str) -> Result<String, String> {
    if path.is_empty() {
        return Ok(String::new());
    }
    let trailing_slash = path.ends_with('/');
    let body = path.trim_end_matches('/');
    if body.is_empty() {
        return Err("path consists only of slashes".to_string());
    }
    let segs: Vec<&str> = body.split('/').collect();
    let mut out = Vec::with_capacity(segs.len());
    for seg in segs {
        if seg.is_empty() {
            return Err("empty path segment (consecutive `/`)".to_string());
        }
        out.push(canonicalize_segment(seg)?);
    }
    let mut joined = out.join("/");
    if trailing_slash {
        joined.push('/');
    }
    Ok(joined)
}

/// Build the canonical authority+path string from `url::Url`-parsed parts
/// and the RAW input string.
///
/// Why raw input for the path: `url::Url` on non-special schemes still
/// decodes `%2E`/`%2e` and normalises the resulting `.`/`..` segments
/// away (verified empirically). We need the raw bytes to enforce
/// path-traversal rejection.
///
/// Why raw input for the authority: we reject two equivalence-class
/// hazards that `url::Url` would otherwise let through silently:
/// - **non-ASCII** host bytes — `url::Url` percent-encodes them
///   (`bückét` → `b%C3%BCck%C3%A9t`) but our backends (S3 buckets, ADLS
///   accounts, GCS buckets) are restricted ASCII. Encoded forms create
///   an equivalence class.
/// - **percent-encoded** host bytes — `url::Url` preserves `%62ucket`
///   literally as the host, but the cloud server URL-decodes the request
///   to `bucket`. Two table locations with `%62ucket` vs `bucket` would
///   alias in cloud storage but be distinct rows in our DB.
fn canonicalize_url_to_authority_and_path(
    raw_input: &str,
    url: &url::Url,
) -> Result<String, String> {
    // Split raw at `://` to inspect the original authority bytes.
    let scheme_end = raw_input
        .find("://")
        .ok_or_else(|| "input lacks `://` separator".to_string())?;
    let after_scheme = &raw_input[scheme_end + 3..];
    let raw_authority_end = after_scheme.find('/').unwrap_or(after_scheme.len());
    let raw_authority = &after_scheme[..raw_authority_end];
    if !raw_authority.is_ascii() {
        return Err(format!(
            "non-ASCII bytes in authority `{raw_authority}` (use punycode IDN form for hosts)"
        ));
    }
    if raw_authority.contains('%') {
        return Err(format!(
            "percent-encoded byte in authority `{raw_authority}` — hosts must be literal ASCII (cloud servers URL-decode the host, which would alias to a different name)"
        ));
    }

    // Authority components: trust `url::Url` for the userinfo/host/port
    // split (well-tested for IPv6 brackets, port detection, etc.).
    let host = url
        .host_str()
        .ok_or_else(|| "URL has no host".to_string())?;
    if host.is_empty() {
        return Err("empty host".to_string());
    }
    if host.ends_with('.') {
        return Err(format!("host has trailing dot: `{host}`"));
    }
    let mut authority = host.to_ascii_lowercase();
    if let Some(port) = url.port() {
        authority = format!("{authority}:{port}");
    }
    if !url.username().is_empty() {
        let userinfo = match url.password() {
            Some(p) => format!("{}:{}", url.username(), p),
            None => url.username().to_string(),
        };
        authority = format!("{userinfo}@{authority}");
    }

    // Path: extract from RAW input. Per-segment canonicalisation handles
    // the `%2E`-style equivalences explicitly (decode, then check for
    // `.`/`..`/etc., then re-encode).
    let raw_path = &after_scheme[raw_authority_end..];
    if raw_path.is_empty() {
        return Ok(authority);
    }
    debug_assert!(raw_path.starts_with('/'));
    let canon_path = canonicalize_path(&raw_path[1..])?;
    Ok(format!("{authority}/{canon_path}"))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_location_with_whitespace_canonicalises_to_percent_encoded() {
        // Literal space and `%20` collapse to the same canonical form
        // (`%20`). Space is not unreserved per RFC 3986; canonical form
        // keeps it encoded.
        let literal = Location::from_str("s3://bucket/foo bar").unwrap();
        assert_eq!(literal.as_str(), "s3://bucket/foo%20bar");
        let encoded = Location::from_str("s3://bucket/foo%20bar").unwrap();
        assert_eq!(encoded.as_str(), "s3://bucket/foo%20bar");
        assert_eq!(literal, encoded);
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
            let mut result: Vec<_> = location.partial_locations().collect();
            result.sort_unstable();
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_extend() {
        let mut location = Location::from_str("s3://bucket").unwrap();
        location.extend(["foo", "bar"]).unwrap();
        assert_eq!(location.as_str(), "s3://bucket/foo/bar");

        let mut location = Location::from_str("s3://bucket/").unwrap();
        location.extend(["foo", "bar"]).unwrap();
        assert_eq!(location.as_str(), "s3://bucket/foo/bar");
    }

    #[test]
    fn test_push() {
        let mut location = Location::from_str("s3://bucket").unwrap();
        location.push("foo").unwrap();
        assert_eq!(location.as_str(), "s3://bucket/foo");

        let mut location = Location::from_str("s3://bucket/").unwrap();
        location.push("foo").unwrap();
        assert_eq!(location.as_str(), "s3://bucket/foo");
    }

    #[test]
    fn test_extend_canonicalises_appended_segments() {
        // extend / push must produce a canonical Location, regardless of
        // which encoded/decoded form the caller passes.
        let mut a = Location::from_str("s3://bucket").unwrap();
        a.extend(["foo bar"]).unwrap();
        let mut b = Location::from_str("s3://bucket").unwrap();
        b.extend(["foo%20bar"]).unwrap();
        assert_eq!(a, b);
        assert_eq!(a.as_str(), "s3://bucket/foo%20bar");

        // Mixed-hex `%2d` and unreserved `-` collapse the same way.
        let mut a = Location::from_str("s3://bucket").unwrap();
        a.extend(["foo-bar"]).unwrap();
        let mut b = Location::from_str("s3://bucket").unwrap();
        b.extend(["foo%2dbar"]).unwrap();
        let mut c = Location::from_str("s3://bucket").unwrap();
        c.extend(["foo%2Dbar"]).unwrap();
        assert_eq!(a, b);
        assert_eq!(a, c);
        assert_eq!(a.as_str(), "s3://bucket/foo-bar");
    }

    #[test]
    fn test_extend_empty_segment_handling() {
        let base = || Location::from_str("s3://bucket/foo").unwrap();

        // Trailing empty: preserves trailing slash sentinel.
        let mut trailing = base();
        trailing.extend(["bar", ""]).unwrap();
        assert_eq!(trailing.as_str(), "s3://bucket/foo/bar/");

        // Single trailing empty alone: equivalent to with_trailing_slash.
        let mut only_trailing = base();
        only_trailing.extend([""]).unwrap();
        assert_eq!(only_trailing.as_str(), "s3://bucket/foo/");

        // Leading empty: no-op (extend always inserts a separator).
        let mut leading = base();
        leading.extend(["", "bar"]).unwrap();
        assert_eq!(leading.as_str(), "s3://bucket/foo/bar");

        // Leading + trailing empties around content: leading no-op,
        // trailing preserved.
        let mut both = base();
        both.extend(["", "bar", ""]).unwrap();
        assert_eq!(both.as_str(), "s3://bucket/foo/bar/");

        // Middle empty between non-empty segments: rejected (would
        // produce `//` in the canonical path).
        let mut middle = base();
        let err = middle.extend(["bar", "", "baz"]).unwrap_err();
        assert!(err.reason.contains("consecutive `/`"), "{}", err.reason);
    }

    #[test]
    fn test_extend_rejects_invalid_segments() {
        let mut loc = Location::from_str("s3://bucket").unwrap();
        // Path traversal.
        assert!(loc.clone().extend(["..", "foo"]).is_err());
        assert!(loc.clone().extend([".", "foo"]).is_err());
        // Trailing dot (Azure Blob aliasing).
        assert!(loc.clone().extend(["foo."]).is_err());
        // Decoded `/` would create nesting ambiguity.
        assert!(loc.clone().extend(["foo%2Fbar"]).is_err());
        // Literal `/` in segment — caller must split.
        assert!(loc.clone().extend(["foo/bar"]).is_err());
        // Decoded control byte.
        assert!(loc.clone().extend(["foo%00bar"]).is_err());
        // Smuggling char in input.
        assert!(loc.extend(["foo\tbar"]).is_err());
    }

    #[test]
    fn test_idempotent_canonicalisation() {
        // canonicalize(canonicalize(s)) == canonicalize(s)
        let inputs = [
            "s3://bucket/Foo-Bar",
            "S3://Bucket/Foo%2Dbar",
            "s3://BUCKET/foo%2dBAR",
            "abfss://fs@account.dfs.core.windows.net/Foo%21Bar/",
            "gs://bucket/foo%2Abar",
        ];
        for input in inputs {
            let once = Location::from_str(input).unwrap();
            let twice = Location::from_str(once.as_str()).unwrap();
            assert_eq!(once.as_str(), twice.as_str(), "non-idempotent: {input}");
        }
    }

    #[test]
    fn test_canonical_collapses_unreserved_and_subdelim_encodings() {
        // Equivalence classes from RFC 3986 §6.2.2.2: unreserved chars
        // and sub-delims are URI-equivalent whether literal or %XX.
        let pairs = [
            ("s3://bucket/foo%2Dbar", "s3://bucket/foo-bar"),
            ("s3://bucket/foo%2Ebar", "s3://bucket/foo.bar"),
            ("s3://bucket/foo%5Fbar", "s3://bucket/foo_bar"),
            ("s3://bucket/foo%7Ebar", "s3://bucket/foo~bar"),
            ("s3://bucket/foo%41bar", "s3://bucket/fooAbar"),
            ("s3://bucket/foo%2Bbar", "s3://bucket/foo+bar"),
            ("s3://bucket/foo%2Abar", "s3://bucket/foo*bar"),
            ("s3://bucket/foo%24bar", "s3://bucket/foo$bar"),
            ("s3://bucket/foo%27bar", "s3://bucket/foo'bar"),
        ];
        for (a, b) in pairs {
            let la = Location::from_str(a).unwrap();
            let lb = Location::from_str(b).unwrap();
            assert_eq!(la, lb, "expected `{a}` and `{b}` to canonicalise equal");
        }
    }

    #[test]
    fn test_canonical_uppercases_hex_in_kept_percent_encodings() {
        // `?` is reserved (`%3F`) so canonical keeps it encoded — and
        // uppercases the hex digits.
        let lower = Location::from_str("s3://bucket/foo%3fbar").unwrap();
        let upper = Location::from_str("s3://bucket/foo%3Fbar").unwrap();
        assert_eq!(lower, upper);
        assert_eq!(lower.as_str(), "s3://bucket/foo%3Fbar");
    }

    #[test]
    fn test_canonical_lowercases_scheme_and_host() {
        let l = Location::from_str("S3://Bucket/Foo").unwrap();
        assert_eq!(l.as_str(), "s3://bucket/Foo");
        assert_eq!(l.scheme(), "s3");
    }

    #[test]
    fn test_rejects_smuggling_chars() {
        // Unicode `Cc` (control) — must reject, otherwise `url::Url::parse`
        // silently strips `\t`/`\n`/`\r` and we lose bytes.
        for bad in [
            "s3://bucket/foo\tbar",
            "s3://bucket/foo\nbar",
            "s3://bucket/foo\rbar",
            "s3://bucket/foo\u{0000}bar",
            "s3://bucket/foo\u{007F}bar",
            "s3://bucket/foo\u{0085}bar", // C1 next-line
        ] {
            assert!(
                Location::from_str(bad).is_err(),
                "expected reject for input containing control char: {bad:?}"
            );
        }
    }

    #[test]
    fn test_bidi_format_chars_percent_encoded_not_rejected() {
        // Unicode `Cf` (format/bidi) chars are NOT rejected — they get
        // percent-encoded by canonicalisation, which makes them
        // byte-distinct from their ASCII look-alikes (no aliasing).
        // Visual-spoofing protection is a display-layer concern.
        let cases = [
            ("s3://bucket/foo\u{200B}bar", "s3://bucket/foo%E2%80%8Bbar"), // ZWSP
            ("s3://bucket/foo\u{202E}bar", "s3://bucket/foo%E2%80%AEbar"), // RLO
            ("s3://bucket/foo\u{FEFF}bar", "s3://bucket/foo%EF%BB%BFbar"), // BOM
        ];
        for (input, canonical) in cases {
            let loc = Location::from_str(input)
                .unwrap_or_else(|e| panic!("expected accept for {input:?}: {e}"));
            assert_eq!(loc.as_str(), canonical, "input was {input:?}");
        }
    }

    #[test]
    fn test_rejects_decoded_controls_and_special_segments() {
        for bad in [
            "s3://bucket/%2E/foo",    // decodes to `.`
            "s3://bucket/%2E%2E/foo", // decodes to `..`
            "s3://bucket/%2F/foo",    // decoded `/`
            "s3://bucket/foo/%2Fbar", // decoded `/` inside segment
            "s3://bucket/%00/foo",    // NUL
            "s3://bucket/%09/foo",    // tab
            "s3://bucket/%0A/foo",    // LF
            "s3://bucket/foo./bar",   // segment ends with `.` (Azure aliasing)
            "s3://bucket/foo%2E/bar", // same after decode
            "s3://bucket//foo",       // empty middle segment
            "s3://bucket/foo//bar",   // empty middle segment
        ] {
            assert!(
                Location::from_str(bad).is_err(),
                "expected reject for: {bad}"
            );
        }
    }

    #[test]
    fn test_rejects_host_trailing_dot_and_non_ascii() {
        assert!(Location::from_str("s3://bucket./foo").is_err());
        assert!(Location::from_str("abfss://fs@account.dfs.core.windows.net./x").is_err());
        assert!(Location::from_str("s3://bückét/foo").is_err());
    }

    #[test]
    fn test_rejects_percent_encoded_host() {
        // Cloud servers URL-decode the host on the wire, so `%62ucket` would
        // alias to `bucket`. Reject upfront to keep our DB rows in 1:1
        // correspondence with cloud-side host names.
        for bad in [
            "s3://%62ucket/foo",  // %62 = 'b'
            "s3://%41BC/foo",     // %41 = 'A'
            "s3://buc%2Eket/foo", // %2E = '.'
            "abfss://fs@%61ccount.dfs.core.windows.net/x",
        ] {
            let err = Location::from_str(bad)
                .expect_err(&format!("expected reject for percent-encoded host: {bad}"));
            assert!(
                err.reason.contains("authority") || err.reason.contains("percent"),
                "{}",
                err.reason
            );
        }
    }

    #[test]
    fn test_is_sublocation_of_collapses_encoding_equivalent_forms() {
        // Primary security goal of the canonicalisation refactor: two
        // locations that decode to the same physical cloud path must
        // canonicalise to byte-equal strings, so `is_sublocation_of`
        // returns the right answer regardless of how the caller wrote it.
        let cases = [
            ("s3://bucket/foo-bar/x", "s3://bucket/foo%2Dbar/", true),
            ("s3://bucket/foo-bar/x", "s3://bucket/foo%2dbar/", true),
            ("s3://bucket/foo-bar/x", "s3://bucket/foo%2Dbar", true),
            ("s3://bucket/Foo/x", "s3://bucket/%46oo/", true),
            ("s3://bucket/foo-bar/x", "s3://bucket/foo-baz", false),
        ];
        for (child, parent, expected) in cases {
            let c = Location::from_str(child).unwrap();
            let p = Location::from_str(parent).unwrap();
            assert_eq!(
                c.is_sublocation_of(&p),
                expected,
                "child={child} parent={parent}"
            );
        }
    }

    #[test]
    fn test_extend_output_is_idempotent_through_from_str() {
        let mut loc = Location::from_str("s3://bucket").unwrap();
        loc.extend(["Foo%2Dbar", "data%2A", "file.parquet"])
            .unwrap();
        let canonical = loc.as_str().to_string();
        let reparsed = Location::from_str(&canonical).unwrap();
        assert_eq!(reparsed.as_str(), canonical);
    }

    #[test]
    fn test_extend_rejects_percent_encoded_control() {
        let loc = Location::from_str("s3://bucket").unwrap();
        // %09 is tab, %0A is LF, %1F is unit separator, %7F is DEL.
        for bad in ["foo%09bar", "foo%0Abar", "foo%1Fbar", "foo%7Fbar"] {
            assert!(
                loc.clone().extend([bad]).is_err(),
                "expected reject for `{bad}`"
            );
        }
    }

    #[test]
    fn test_extend_multiple_leading_empties_are_no_op() {
        let mut a = Location::from_str("s3://bucket").unwrap();
        a.extend(["", "", "foo"]).unwrap();
        let mut b = Location::from_str("s3://bucket").unwrap();
        b.extend(["foo"]).unwrap();
        assert_eq!(a, b);
        assert_eq!(a.as_str(), "s3://bucket/foo");
    }

    #[test]
    fn test_canonical_preserves_userinfo_and_port() {
        let l = Location::from_str("s3://user:pass@bucket:9000/foo").unwrap();
        assert_eq!(l.as_str(), "s3://user:pass@bucket:9000/foo");
        let l = Location::from_str("s3://user:pass@Bucket:9000/Foo").unwrap();
        assert_eq!(l.as_str(), "s3://user:pass@bucket:9000/Foo");
    }

    #[test]
    fn test_canonical_preserves_trailing_slash() {
        let with_slash = Location::from_str("s3://bucket/foo/").unwrap();
        assert_eq!(with_slash.as_str(), "s3://bucket/foo/");
        let without = Location::from_str("s3://bucket/foo").unwrap();
        assert_eq!(without.as_str(), "s3://bucket/foo");
        // Equivalence: trailing-slash is meaningful (different cloud objects).
        assert_ne!(with_slash, without);
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
