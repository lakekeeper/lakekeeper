use std::str::{FromStr, RMatchIndices};

use crate::configs::{ConfigParseError, ConfigProperty, NotCustomProp, ParseError, ParseFromStr};

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Location(url::Url);

impl NotCustomProp for Location {}

impl Location {
    #[must_use]
    pub fn url(&self) -> &url::Url {
        &self.0
    }

    #[must_use]
    pub fn into_url(self) -> url::Url {
        self.0
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn with_trailing_slash(&mut self) -> &mut Self {
        if let Ok(mut path) = self.0.path_segments_mut() {
            path.pop_if_empty().push("");
        };
        self
    }

    pub fn without_trailing_slash(&mut self) -> &mut Self {
        if let Ok(mut path) = self.0.path_segments_mut() {
            path.pop_if_empty();
        };
        self
    }

    /// Follows the same logic as `url::MutPathSegments::extend`,
    /// except that getting `MutPathSegments`is not fallible.
    /// Non-fallibility by the constructor which checks
    /// cannot-be-a-base.
    pub fn extend<I>(&mut self, segments: I) -> &mut Self
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        if let Ok(mut path) = self.0.path_segments_mut() {
            path.extend(segments);
        };
        self
    }

    /// Follows the same logic as `url::MutPathSegments::push`,
    /// except that getting `MutPathSegments`is not fallible.
    /// Non-fallibility by the constructor which checks
    /// cannot-be-a-base.
    pub fn push(&mut self, segment: &str) -> &mut Self {
        if let Ok(mut path) = self.0.path_segments_mut() {
            path.push(segment);
        };
        self
    }

    /// Clones the location and pushes a segment to the path.
    #[must_use]
    pub fn cloning_push(&self, segment: &str) -> Self {
        let mut cloned = self.clone();
        cloned.push(segment);
        cloned
    }

    /// Follows the same logic as `url::MutPathSegments::pop`,
    /// except that getting `MutPathSegments`is not fallible.
    /// Non-fallibility by the constructor which checks
    /// cannot-be-a-base.
    pub fn pop(&mut self) -> &mut Self {
        if let Ok(mut path) = self.0.path_segments_mut() {
            path.pop();
        };
        self
    }

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
        let scheme_index = self.url().scheme().len() + 3; // 3 for "://"
        let url_string = self.url().as_str().trim_end_matches('/');
        let pointer = url_string.rmatch_indices('/');

        let iter: PartialLocationsIter<'a> = PartialLocationsIter {
            pointer,
            loc: url_string,
            full_loc: Some(url_string),
            scheme_index,
        };
        iter
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

impl ConfigProperty for Location {
    const KEY: &'static str = "location";
    type Type = Self;

    fn key(&self) -> &str {
        Self::KEY
    }

    fn value(&self) -> &Self::Type {
        self
    }

    fn into_value(self) -> Self::Type {
        self
    }
}

impl std::fmt::Display for Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ParseFromStr for Location {
    fn parse_value(value: &str) -> Result<Self, ParseError> {
        let location = url::Url::parse(value).map_err(|_e| ParseError {
            value: value.to_string(),
            typ: "Url".to_string(),
        })?;

        if location.cannot_be_a_base() {
            return Err(ParseError {
                value: value.to_string(),
                typ: "Url with base".to_string(),
            });
        }

        if location.fragment().is_some() {
            return Err(ParseError {
                value: value.to_string(),
                typ: "Url without fragment".to_string(),
            });
        }

        if location.query().is_some() {
            return Err(ParseError {
                value: value.to_string(),
                typ: "Url without query".to_string(),
            });
        }

        Ok(Location(location))
    }
}

impl FromStr for Location {
    type Err = ConfigParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        <Location as ConfigProperty>::parse_value(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
