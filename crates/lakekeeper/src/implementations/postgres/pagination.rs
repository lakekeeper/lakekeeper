// TODO: lift this from DB module?

use std::{fmt::Display, str::FromStr};

use base64::Engine;
use chrono::Utc;
use iceberg_ext::catalog::rest::ErrorModel;

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

impl<T> TryFrom<&str> for PaginateToken<T>
where
    T: FromStr + Display,
    <T as FromStr>::Err: Display,
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
                    let id = id.parse().map_err(|e| {
                        parse_error(Some(Box::new(ErrorModel::bad_request(
                            format!("Pagination id could not be parsed: {e}"),
                            "PaginationTokenIdParseError".to_string(),
                            None,
                        ))))
                    })?;
                    Ok(PaginateToken::V1(V1PaginateToken { created_at, id }))
                }
                _ => Err(parse_error(None)),
            },
            _ => Err(parse_error(None)),
        }
    }
}

fn parse_error(source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>) -> ErrorModel {
    ErrorModel::bad_request(
        "Invalid paginate token".to_string(),
        "PaginateTokenParseError".to_string(),
        source,
    )
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::service::ProjectId;

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

    #[test]
    fn test_paginate_token_edge_cases_valid() {
        // minimum timestamp
        let min_created_at = chrono::DateTime::from_timestamp_micros(0).unwrap();
        let token = PaginateToken::V1(V1PaginateToken {
            created_at: min_created_at,
            id: "test".to_string(),
        });
        let token_str = token.to_string();
        let parsed: PaginateToken<String> = PaginateToken::try_from(token_str.as_str()).unwrap();
        assert_eq!(parsed, token);

        // very large timestamp (but valid)
        let large_created_at =
            chrono::DateTime::from_timestamp_micros(253_402_300_799_000_000).unwrap(); // Year 9999
        let token = PaginateToken::V1(V1PaginateToken {
            created_at: large_created_at,
            id: "test".to_string(),
        });
        let token_str = token.to_string();
        let parsed: PaginateToken<String> = PaginateToken::try_from(token_str.as_str()).unwrap();
        assert_eq!(parsed, token);

        // empty string ID
        let created_at = Utc::now();
        let token = PaginateToken::V1(V1PaginateToken {
            created_at,
            id: String::new(),
        });
        let token_str = token.to_string();
        let parsed: PaginateToken<String> = PaginateToken::try_from(token_str.as_str()).unwrap();
        let expected_created_at =
            chrono::DateTime::from_timestamp_micros(created_at.timestamp_micros()).unwrap();
        assert_eq!(
            parsed,
            PaginateToken::V1(V1PaginateToken {
                created_at: expected_created_at,
                id: String::new(),
            })
        );

        // very long ID string
        let long_id = "a".repeat(1000);
        let token = PaginateToken::V1(V1PaginateToken {
            created_at,
            id: long_id.clone(),
        });
        let token_str = token.to_string();
        let parsed: PaginateToken<String> = PaginateToken::try_from(token_str.as_str()).unwrap();
        let expected_created_at =
            chrono::DateTime::from_timestamp_micros(created_at.timestamp_micros()).unwrap();
        assert_eq!(
            parsed,
            PaginateToken::V1(V1PaginateToken {
                created_at: expected_created_at,
                id: long_id,
            })
        );
    }

    #[test]
    fn test_paginate_token_special_characters() {
        let created_at = Utc::now();
        let test_cases = vec![
            "special/chars@#$%^*(){}[]|\\:;\"'<>?,./",
            "unicode测试🚀🎉",
            "newlines\nand\ttabs",
            "quotes\"and'apostrophes",
            "equals=signs&ampersands",
        ];

        for test_id in test_cases {
            let token = PaginateToken::V1(V1PaginateToken {
                created_at,
                id: test_id.to_string(),
            });
            let token_str = token.to_string();
            let parsed: PaginateToken<String> =
                PaginateToken::try_from(token_str.as_str()).unwrap();
            let expected_created_at =
                chrono::DateTime::from_timestamp_micros(created_at.timestamp_micros()).unwrap();
            assert_eq!(
                parsed,
                PaginateToken::V1(V1PaginateToken {
                    created_at: expected_created_at,
                    id: test_id.to_string(),
                }),
                "Failed for ID: {test_id}",
            );
        }
    }

    #[test]
    fn test_paginate_token_numeric_ids() {
        let created_at = Utc::now();

        // i32 ID
        let token = PaginateToken::V1(V1PaginateToken {
            created_at,
            id: 42i32,
        });
        let token_str = token.to_string();
        let parsed: PaginateToken<i32> = PaginateToken::try_from(token_str.as_str()).unwrap();
        let expected_created_at =
            chrono::DateTime::from_timestamp_micros(created_at.timestamp_micros()).unwrap();
        assert_eq!(
            parsed,
            PaginateToken::V1(V1PaginateToken {
                created_at: expected_created_at,
                id: 42i32,
            })
        );

        // negative number
        let token = PaginateToken::V1(V1PaginateToken {
            created_at,
            id: -123i64,
        });
        let token_str = token.to_string();
        let parsed: PaginateToken<i64> = PaginateToken::try_from(token_str.as_str()).unwrap();
        let expected_created_at =
            chrono::DateTime::from_timestamp_micros(created_at.timestamp_micros()).unwrap();
        assert_eq!(
            parsed,
            PaginateToken::V1(V1PaginateToken {
                created_at: expected_created_at,
                id: -123i64,
            })
        );
    }

    #[test]
    fn test_paginate_token_invalid_base64() {
        // Invalid base64 characters
        let invalid_tokens = vec![
            "invalid base64!",
            "not_base64@#$",
            "spaces in token",
            "incomplete_padding=",
            "\n\t\r",
        ];

        for invalid_token in invalid_tokens {
            let result: Result<PaginateToken<String>, _> = PaginateToken::try_from(invalid_token);
            assert!(
                result.is_err(),
                "Should fail for invalid base64: {invalid_token}",
            );
            let error = result.unwrap_err();
            assert_eq!(error.r#type, "PaginateTokenDecodeError");
            assert!(error.message.contains("Invalid paginate token"));
        }
    }

    #[test]
    fn test_paginate_token_invalid_utf8() {
        // base64 that decodes to invalid UTF-8
        let invalid_utf8_bytes = vec![0xFF, 0xFE, 0xFD]; // Invalid UTF-8 sequence
        let invalid_base64 = base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(&invalid_utf8_bytes);

        let result: Result<PaginateToken<String>, _> =
            PaginateToken::try_from(invalid_base64.as_str());
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.r#type, "PaginateTokenDecodeError");
        assert!(error.message.contains("Invalid paginate token"));
    }

    #[test]
    fn test_paginate_token_malformed_structure() {
        let encode = |s: &str| base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(s);

        let malformed_cases = vec![
            // Empty string
            encode(""),
            // Missing parts
            encode("1"),
            encode("1&"),
            encode("1&123456789"),
            // Wrong version
            encode("2&123456789&test"),
            encode("0&123456789&test"),
            encode("invalid&123456789&test"),
            // No ampersands
            encode("1-123456789-test"),
            encode("1 123456789 test"),
        ];

        for malformed in malformed_cases {
            let result: Result<PaginateToken<String>, _> =
                PaginateToken::try_from(malformed.as_str());
            assert!(
                result.is_err(),
                "Should fail for malformed token: {malformed}",
            );
            let error = result.unwrap_err();
            assert_eq!(error.r#type, "PaginateTokenParseError");
        }
    }

    #[test]
    fn test_paginate_token_invalid_timestamp() {
        let encode = |s: &str| base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(s);

        let invalid_timestamp_cases = vec![
            // Non-numeric timestamp
            encode("1&not_a_number&test"),
            encode("1&abc123&test"),
            encode("1&12.34&test"), // Decimal not allowed
            encode("1&&test"),      // Empty timestamp
            // Out of range timestamps - only test extremely large numbers
            encode("1&99999999999999999999999999999&test"), // Way too large
        ];

        for invalid in invalid_timestamp_cases {
            let result: Result<PaginateToken<String>, _> =
                PaginateToken::try_from(invalid.as_str());
            assert!(
                result.is_err(),
                "Should fail for invalid timestamp: {invalid}",
            );
            let error = result.unwrap_err();
            assert!(
                error.r#type == "PaginateTokenParseError"
                    || error.r#type == "PaginateTokenDecodeError"
            );
        }
    }

    #[test]
    fn test_paginate_token_invalid_id_parsing() {
        let encode = |s: &str| base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(s);

        // numeric ID type but non-numeric string
        let invalid_numeric = encode("1&123456789&not_a_number");
        let result: Result<PaginateToken<i32>, _> =
            PaginateToken::try_from(invalid_numeric.as_str());
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.r#type, "PaginateTokenParseError");
        assert!(error.message.contains("Invalid paginate token"));

        // UUID type but invalid UUID string
        let invalid_uuid = encode("1&123456789&not-a-valid-uuid");
        let result: Result<PaginateToken<uuid::Uuid>, _> =
            PaginateToken::try_from(invalid_uuid.as_str());
        assert!(result.is_err());
    }

    #[test]
    fn test_paginate_token_display_format() {
        let created_at = chrono::DateTime::from_timestamp_micros(1_609_459_200_000_000).unwrap(); // 2021-01-01 00:00:00 UTC
        let token = PaginateToken::V1(V1PaginateToken {
            created_at,
            id: "test-id".to_string(),
        });

        let token_str = token.to_string();

        // Verify it's valid base64
        let decoded = base64::prelude::BASE64_URL_SAFE_NO_PAD
            .decode(&token_str)
            .unwrap();
        let decoded_str = String::from_utf8(decoded).unwrap();

        // Verify the internal format
        assert!(decoded_str.starts_with("1&"));
        assert!(decoded_str.contains("1609459200000000"));
        assert!(decoded_str.ends_with("&test-id"));
    }

    #[test]
    fn test_paginate_token_empty_string() {
        let result: Result<PaginateToken<String>, _> = PaginateToken::try_from("");
        assert!(result.is_err());
        let error = result.unwrap_err();
        // Empty string fails at parse stage, not decode stage
        assert_eq!(error.r#type, "PaginateTokenParseError");
    }
}
