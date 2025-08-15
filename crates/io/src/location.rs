use std::str::{FromStr, RMatchIndices};

#[derive(Debug, Eq, PartialEq, Clone)]
#[allow(clippy::struct_field_names)]
pub struct Location {
    full_location: String,
    scheme: String,
    authority_and_path: String, // Everything after ://
}

#[derive(thiserror::Error, Debug)]
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
        // Everything between @ and the first slash
        let host_part = if let Some(at_pos) = self.authority_and_path.find('@') {
            // If there's an @ symbol, take everything after it
            &self.authority_and_path[at_pos + 1..]
        } else {
            // If there's no @ symbol, use the whole location
            &self.authority_and_path
        };

        // Now find the first slash and return everything before it
        match host_part.find('/') {
            Some(slash_pos) => Some(&host_part[..slash_pos]),
            None => Some(host_part), // No slash found, return the whole host part
        }
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

impl FromStr for Location {
    type Err = LocationParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
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
                reason: "URL has a fragment (#)".to_string(),
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
