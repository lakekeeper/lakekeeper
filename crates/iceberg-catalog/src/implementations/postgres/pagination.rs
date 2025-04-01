// TODO: lift this from DB module?

use std::{fmt::Display, str::FromStr};

use base64::Engine;
use chrono::Utc;
use iceberg_ext::catalog::rest::ErrorModel;
use thiserror::Error;

#[derive(Debug, PartialEq)]
pub(crate) enum PaginateToken<T> {
    V1(V1PaginateToken<T>),
}

#[derive(Debug, PartialEq)]
pub(crate) struct V1PaginateToken<T> {
    pub(crate) created_at: chrono::DateTime<Utc>,
    pub(crate) id: T,
}

impl<T> Display for PaginateToken<T>
where
    T: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let token_string = match self {
            PaginateToken::V1(V1PaginateToken { created_at, id }) => {
                format!("1&{}&{}", created_at.timestamp_micros(), id)
            }
        };
        write!(
            f,
            "{}",
            base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(&token_string)
        )
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct RoundTrippableDuration(pub(crate) iso8601::Duration);

#[derive(Debug, Error)]
#[error("Failed to parse duration: {0}")]
pub(super) struct DurationError(String);

impl TryFrom<&str> for RoundTrippableDuration {
    type Error = ErrorModel;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let duration = iso8601::Duration::from_str(value).map_err(|e| {
            tracing::info!("Failed to parse duration: {e}");
            ErrorModel::bad_request(
                "Invalid duration".to_string(),
                "DurationParseError".to_string(),
                Some(Box::new(DurationError(e))),
            )
        })?;
        Ok(Self(duration))
    }
}

impl Display for RoundTrippableDuration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<T, Z> TryFrom<&str> for PaginateToken<T>
where
    T: for<'a> TryFrom<&'a str, Error = Z> + Display,
    Z: std::error::Error + Send + Sync + 'static,
{
    type Error = ErrorModel;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let s = String::from_utf8(base64::prelude::BASE64_URL_SAFE_NO_PAD.decode(s).map_err(
            |e| {
                tracing::info!("Failed to decode b64 encoded page token");
                ErrorModel::bad_request(
                    "Invalid paginate token".to_string(),
                    "PaginateTokenDecodeError".to_string(),
                    Some(Box::new(e)),
                )
            },
        )?)
        .map_err(|e| {
            tracing::info!("Decoded b64 contained an invalid utf8-sequence.");
            ErrorModel::bad_request(
                "Invalid paginate token".to_string(),
                "PaginateTokenDecodeError".to_string(),
                Some(Box::new(e)),
            )
        })?;

        let parts = s.splitn(3, '&').collect::<Vec<_>>();

        match *parts.first().ok_or(parse_error(None))? {
            "1" => match &parts[1..] {
                &[ts, id] => {
                    let created_at = chrono::DateTime::from_timestamp_micros(
                        ts.parse().map_err(|e| parse_error(Some(Box::new(e))))?,
                    )
                    .ok_or(parse_error(None))?;
                    let id = id.try_into().map_err(|e| parse_error(Some(Box::new(e))))?;
                    Ok(PaginateToken::V1(V1PaginateToken { created_at, id }))
                }
                _ => Err(parse_error(None)),
            },
            _ => Err(parse_error(None)),
        }
    }
}

fn parse_error(e: Option<Box<dyn std::error::Error + Send + Sync + 'static>>) -> ErrorModel {
    ErrorModel::bad_request(
        "Invalid paginate token".to_string(),
        "PaginateTokenParseError".to_string(),
        e,
    )
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::service::ProjectId;

    #[test]
    fn test_roundtrip_duration() {
        let duration =
            RoundTrippableDuration(iso8601::Duration::from_str("P1Y2M3DT4H5M6S").unwrap());
        let duration_str = duration.to_string();
        let duration_after: RoundTrippableDuration =
            RoundTrippableDuration::try_from(duration_str.as_str()).unwrap();
        assert_eq!(duration, duration_after);
    }

    #[test]
    fn test_paginate_token() {
        let created_at = Utc::now();
        let token = PaginateToken::V1(V1PaginateToken {
            created_at,
            id: ProjectId::new(uuid::Uuid::nil()),
        });

        let token_str = token.to_string();
        let token: PaginateToken<uuid::Uuid> = PaginateToken::try_from(token_str.as_str()).unwrap();
        // we lose some precision while serializing the timestamp making tests flaky
        let created_at =
            chrono::DateTime::from_timestamp_micros(created_at.timestamp_micros()).unwrap();
        assert_eq!(
            token,
            PaginateToken::V1(V1PaginateToken {
                created_at,
                id: uuid::Uuid::nil(),
            })
        );
    }

    #[test]
    fn test_paginate_token_with_ampersand() {
        let created_at = Utc::now();
        let token = PaginateToken::V1(V1PaginateToken {
            created_at,
            id: "kubernetes/some-name&with&ampersand".to_string(),
        });

        let token_str = token.to_string();
        let token: PaginateToken<String> = PaginateToken::try_from(token_str.as_str()).unwrap();
        // we lose some precision while serializing the timestamp making tests flaky
        let created_at =
            chrono::DateTime::from_timestamp_micros(created_at.timestamp_micros()).unwrap();
        assert_eq!(
            token,
            PaginateToken::V1(V1PaginateToken {
                created_at,
                id: "kubernetes/some-name&with&ampersand".to_string(),
            })
        );
    }

    #[test]
    fn test_paginate_token_with_user_id() {
        let created_at = Utc::now();
        let token = PaginateToken::V1(V1PaginateToken {
            created_at,
            id: "kubernetes/some-name",
        });

        let token_str = token.to_string();
        let token: PaginateToken<String> = PaginateToken::try_from(token_str.as_str()).unwrap();
        // we lose some precision while serializing the timestamp making tests flaky
        let created_at =
            chrono::DateTime::from_timestamp_micros(created_at.timestamp_micros()).unwrap();
        assert_eq!(
            token,
            PaginateToken::V1(V1PaginateToken {
                created_at,
                id: "kubernetes/some-name".to_string(),
            })
        );
    }
}
