use google_cloud_auth::credentials::CredentialsFile;
use lakekeeper_io::Location;
use serde::{Deserialize, Serialize};

use super::{HTTP_CLIENT, STS_URL, TokenSource};
use crate::service::storage::{
    ShortTermCredentialsRequest, StoragePermissions, error::TableConfigError, gcs::GcsServiceKey,
};

pub(crate) async fn downscope(
    token_source: TokenSource,
    bucket: &str,
    stc_request: &ShortTermCredentialsRequest,
) -> Result<STSResponse, TableConfigError> {
    let token = token_source.token().await.map_err(|e| {
        tracing::error!("Failed to get token from token source: {:?}", e);
        TableConfigError::FailedDependency("Failed to get gcp token from token source".to_string())
    })?;

    let gcs_sts_request = &STSRequest::from_token_and_options(
        &token,
        &Options::from_location_and_permissions(
            bucket,
            &stc_request.table_location,
            stc_request.storage_permissions,
        )?,
    )?;

    let response = HTTP_CLIENT
        .clone()
        .post(STS_URL.clone())
        .json(&gcs_sts_request)
        .send()
        .await
        .map_err(|e| {
            tracing::error!("Failed to send downscoping request: {:?}", e);
            TableConfigError::FailedDependency("Failed to send downscoping request".to_string())
        })?
        .json::<serde_json::Value>()
        .await
        .map_err(|e| {
            tracing::error!(
                "Downscoping did not return a JSON body: {e:?}. Request: {gcs_sts_request:?}",
            );
            TableConfigError::FailedDependency("Failed to downscope.".to_string())
        })?;

    serde_json::from_value(response.clone()).map_err(|e| {
        tracing::error!(
            "Failed to parse downscoping response: {e:?}. Received Body: {response}. Request: {gcs_sts_request:?}",
        );
        TableConfigError::FailedDependency("Failed to downscope.".to_string())
    })
}

#[derive(Deserialize, Clone, veil::Redact)]
pub(crate) struct STSResponse {
    #[redact(partial)]
    pub(crate) access_token: String,
    pub(crate) expires_in: Option<usize>,
    token_type: String,
}

#[derive(Serialize, veil::Redact)]
struct STSRequest {
    // urn:ietf:params:oauth:grant-type:token-exchange
    pub grant_type: String,
    /// The full resource name of the identity provider; for example:
    /// //iam.googleapis.com/projects/<project-number>/locations/global/workloadIdentityPools/<pool-id>/providers/<provider-id>
    /// for workload identity pool providers, or
    /// //iam.googleapis.com/locations/global/workforcePools/<pool-id>/providers/<provider-id> for
    /// workforce pool providers. Required when exchanging an external credential for a Google
    /// access token.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audience: Option<String>,
    /// The OAuth 2.0 scopes to include on the resulting access token, formatted as a list of space-
    /// delimited, case-sensitive strings. Required when exchanging an external credential for a
    /// Google access token.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    // urn:ietf:params:oauth:token-type:access_token
    pub requested_token_type: String,
    #[redact(partial)]
    pub subject_token: String,
    pub subject_token_type: String,
    // serialized json string
    pub options: String,
}

impl STSRequest {
    fn from_token_and_options(token: &str, options: &Options) -> Result<Self, TableConfigError> {
        let op = serde_json::to_string(options).map_err(|e| {
            TableConfigError::Internal("Failed to serialize options".to_string(), Some(Box::new(e)))
        })?;
        Ok(Self {
            grant_type: "urn:ietf:params:oauth:grant-type:token-exchange".to_string(),
            audience: None,
            scope: None,
            requested_token_type: "urn:ietf:params:oauth:token-type:access_token".to_string(),
            subject_token: token.to_string(),
            subject_token_type: "urn:ietf:params:oauth:token-type:access_token".to_string(),
            // A string with JSON-format Credential Access Boundary, encoded with percent encoding.
            options: percent_encoding::utf8_percent_encode(&op, percent_encoding::NON_ALPHANUMERIC)
                .to_string(),
        })
    }
}

#[derive(Serialize, Deserialize)]
struct Options {
    #[serde(rename = "accessBoundary")]
    access_boundary: AccessBoundary,
}

impl Options {
    fn from_location_and_permissions(
        bucket: &str,
        table_location: &Location,
        storage_permissions: StoragePermissions,
    ) -> Result<Self, TableConfigError> {
        let bucket_cel = escape_for_cel_single_quoted(bucket)?;
        let path_cel = gcs_cel_object_prefix(bucket, table_location)?;

        Ok(Options {
            access_boundary: AccessBoundary {
                access_boundary_rules: vec![AccessBoundaryRule {
                    available_resource: format!(
                        "//storage.googleapis.com/projects/_/buckets/{bucket}",
                    ),
                    available_permissions: match storage_permissions {
                        StoragePermissions::Read => {
                            vec!["inRole:roles/storage.objectViewer".to_string()]
                        }
                        StoragePermissions::ReadWrite => vec![
                            "inRole:roles/storage.objectViewer".to_string(),
                            "inRole:roles/storage.objectCreator".to_string(),
                        ],
                        StoragePermissions::ReadWriteDelete => vec![
                            "inRole:roles/storage.objectUser".to_string(),
                        ],
                    },
                    availability_condition: AvailabilityCondition {
                        title: "obj-prefixes".to_string(),
                        // getAttribute is needed for Listing operations.
                        expression: format!(
                            "resource.name.startsWith('projects/_/buckets/{bucket_cel}/objects/{path_cel}') || resource.name.startsWith('projects/_/buckets/{bucket_cel}/folders/{path_cel}') || api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith('{path_cel}')",
                        ),
                    }.into(),
                }],
            },
        })
    }
}

// --- Egress encoders ---------------------------------------------------
//
// Each function here produces the byte form a GCP-side consumer expects
// from a `Location`. They live next to the CEL access-boundary builder
// so a reviewer can verify the canonical form matches what GCS does
// server-side when evaluating the access boundary against
// `resource.name`.

/// CEL-escaped object-name prefix relative to the bucket root, with a
/// trailing `/`. Used in the `startsWith(...)` checks of the GCS access
/// boundary CEL expression.
///
/// Uses the canonical-encoded path (no decode). When a client writes to
/// GCS, the SDK URL-encodes the key for the wire (so a `%` in the key
/// becomes `%25`); the server URL-decodes once and stores at the
/// canonical-encoded form. CEL `startsWith` against `resource.name` is
/// byte-string compare, so the prefix must be the same bytes as the
/// canonical Location.
fn gcs_cel_object_prefix(
    bucket: &str,
    table_location: &Location,
) -> Result<String, TableConfigError> {
    let mut table_location = table_location.clone();
    table_location.with_trailing_slash();
    let bucket_prefix = format!("gs://{bucket}/");
    let prefixless_location = table_location
        .as_str()
        .strip_prefix(&bucket_prefix)
        .ok_or_else(|| {
            TableConfigError::Internal(
                format!(
                    "Refusing to build GCS access boundary: table location `{}` is not under bucket `{bucket}`",
                    table_location.as_str()
                ),
                None,
            )
        })?;
    escape_for_cel_single_quoted(prefixless_location)
}

/// Escape `value` for interpolation inside a CEL single-quoted literal.
/// GCP's access-boundary CEL doesn't accept `r'...'` raw strings or `+`
/// concat. Control chars without a CEL escape are rejected.
fn escape_for_cel_single_quoted(value: &str) -> Result<String, TableConfigError> {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '\'' => out.push_str("\\'"),
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0C}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => {
                return Err(TableConfigError::Internal(
                    format!(
                        "Refusing to build GCS access boundary: input contains an unsupported control character (U+{:04X})",
                        c as u32
                    ),
                    None,
                ));
            }
            c => out.push(c),
        }
    }
    Ok(out)
}

#[derive(Serialize, Deserialize)]
struct AccessBoundary {
    #[serde(rename = "accessBoundaryRules")]
    access_boundary_rules: Vec<AccessBoundaryRule>,
}

#[derive(Serialize, Deserialize)]
struct AccessBoundaryRule {
    #[serde(rename = "availableResource")]
    available_resource: String,
    #[serde(rename = "availablePermissions")]
    available_permissions: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    availability_condition: Option<AvailabilityCondition>,
}

#[derive(Serialize, Debug, Deserialize)]
struct AvailabilityCondition {
    title: String,
    expression: String,
}

impl From<&GcsServiceKey> for CredentialsFile {
    fn from(
        GcsServiceKey {
            r#type: tp,
            project_id,
            private_key_id,
            private_key,
            client_email,
            client_id,
            auth_uri,
            token_uri,
            auth_provider_x509_cert_url: _,
            client_x509_cert_url: _,
            universe_domain: _,
        }: &GcsServiceKey,
    ) -> Self {
        Self {
            tp: tp.clone(),
            client_email: Some(client_email.clone()),
            private_key_id: Some(private_key_id.clone()),
            private_key: Some(private_key.clone()),
            auth_uri: Some(auth_uri.clone()),
            token_uri: Some(token_uri.clone()),
            project_id: Some(project_id.clone()),
            client_secret: None,
            client_id: Some(client_id.clone()),
            refresh_token: None,
            audience: None,
            subject_token_type: None,
            token_url_external: None,
            token_info_url: None,
            service_account_impersonation_url: None,
            service_account_impersonation: None,
            delegates: None,
            credential_source: None,
            quota_project_id: None,
            workforce_pool_user_project: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn gcs_cel_object_prefix_uses_canonical_encoded_form() {
        // GCS `resource.name` matches the bytes that GCS actually stores.
        // Clients URL-encode `%` → `%25` for the wire and the server
        // decodes once, so a path of `foo%20bar` ends up as the literal
        // 7-char key `foo%20bar` (not `foo bar`). The CEL `startsWith`
        // prefix must use the same canonical-encoded form.
        let loc = Location::from_str("gs://my-bucket/wh/foo%20bar/").unwrap();
        let prefix = gcs_cel_object_prefix("my-bucket", &loc).unwrap();
        assert_eq!(prefix, "wh/foo%20bar/");
    }

    #[test]
    fn gcs_cel_object_prefix_collapses_mixed_hex_to_same_form() {
        // `%2D` and `%2d` and literal `-` all canonicalise to literal `-`
        // (unreserved char), so the CEL prefix is identical for all three
        // input forms.
        let a = Location::from_str("gs://b/foo-bar/").unwrap();
        let b = Location::from_str("gs://b/foo%2Dbar/").unwrap();
        let c = Location::from_str("gs://b/foo%2dbar/").unwrap();
        let pa = gcs_cel_object_prefix("b", &a).unwrap();
        let pb = gcs_cel_object_prefix("b", &b).unwrap();
        let pc = gcs_cel_object_prefix("b", &c).unwrap();
        assert_eq!(pa, pb);
        assert_eq!(pa, pc);
        assert_eq!(pa, "foo-bar/");
    }

    #[test]
    fn gcs_cel_object_prefix_rejects_location_outside_bucket() {
        let loc = Location::from_str("gs://other-bucket/wh/").unwrap();
        let err = gcs_cel_object_prefix("my-bucket", &loc).unwrap_err();
        match err {
            TableConfigError::Internal(msg, _) => {
                assert!(msg.contains("not under bucket"), "{msg}");
            }
            other => panic!("expected Internal, got {other:?}"),
        }
    }

    #[test]
    fn escape_for_cel_single_quoted_passes_plain_value() {
        assert_eq!(escape_for_cel_single_quoted("foo/bar").unwrap(), "foo/bar");
        assert_eq!(
            escape_for_cel_single_quoted("foo/bar/üñîçødé").unwrap(),
            "foo/bar/üñîçødé",
        );
    }

    #[test]
    fn escape_for_cel_single_quoted_escapes_quote_backslash_and_double_quote() {
        // Injection payload: closing the literal early. The `'` must be
        // escaped to `\'` so the CEL parser keeps reading inside the literal.
        assert_eq!(
            escape_for_cel_single_quoted("') || true || x.startsWith('").unwrap(),
            "\\') || true || x.startsWith(\\'",
        );
        assert_eq!(
            escape_for_cel_single_quoted(r"foo\bar").unwrap(),
            r"foo\\bar"
        );
        assert_eq!(
            escape_for_cel_single_quoted(r#"foo"bar"#).unwrap(),
            r#"foo\"bar"#
        );
    }

    #[test]
    fn escape_for_cel_single_quoted_escapes_handled_control_chars() {
        assert_eq!(escape_for_cel_single_quoted("a\nb").unwrap(), "a\\nb");
        assert_eq!(escape_for_cel_single_quoted("a\rb").unwrap(), "a\\rb");
        assert_eq!(escape_for_cel_single_quoted("a\tb").unwrap(), "a\\tb");
        assert_eq!(escape_for_cel_single_quoted("a\u{08}b").unwrap(), "a\\bb");
        assert_eq!(escape_for_cel_single_quoted("a\u{0C}b").unwrap(), "a\\fb");
    }

    #[test]
    fn escape_for_cel_single_quoted_rejects_unsupported_control_chars() {
        // NUL and other unhandled control chars have no CEL escape.
        for cp in [0x00u32, 0x01, 0x07, 0x0B, 0x1F, 0x7F] {
            let input = format!("a{}b", char::from_u32(cp).unwrap());
            let err = escape_for_cel_single_quoted(&input)
                .expect_err(&format!("U+{cp:04X} must be rejected"));
            assert!(matches!(err, TableConfigError::Internal(_, _)));
        }
    }

    #[test]
    fn options_neutralizes_cel_injection_in_path() {
        let bucket = "my-bucket";
        let location: Location = "gs://my-bucket/wh/safe-prefix/".parse().unwrap();
        let opts =
            Options::from_location_and_permissions(bucket, &location, StoragePermissions::Read)
                .unwrap();
        let expr = &opts.access_boundary.access_boundary_rules[0]
            .availability_condition
            .as_ref()
            .unwrap()
            .expression;
        assert!(
            expr.contains("/buckets/my-bucket/objects/wh/safe-prefix/"),
            "expected inline path literal in expression, got: {expr}"
        );
        // Guard against regressing to forms GCP rejects.
        assert!(!expr.contains("r'"), "raw-string literal in: {expr}");
        assert!(!expr.contains(" + "), "string concat in: {expr}");
    }

    #[test]
    fn options_escapes_quote_in_path_instead_of_rejecting() {
        // A path containing `'` must produce a CEL expression where the quote
        // is escaped (`\'`), not closed early. The location is accepted, not
        // rejected.
        let bucket = "my-bucket";
        let location: Location = "gs://my-bucket/x'/data/"
            .parse()
            .expect("URL parse should accept ' (sub-delim)");
        let opts =
            Options::from_location_and_permissions(bucket, &location, StoragePermissions::Read)
                .unwrap();
        let expr = &opts.access_boundary.access_boundary_rules[0]
            .availability_condition
            .as_ref()
            .unwrap()
            .expression;
        assert!(
            expr.contains("/objects/x\\'/data/"),
            "expected escaped `'` in expression, got: {expr}"
        );
    }

    #[test]
    fn options_rejects_cross_bucket_table_location() {
        let location: Location = "gs://other-bucket/data/".parse().unwrap();
        let result = Options::from_location_and_permissions(
            "my-bucket",
            &location,
            StoragePermissions::Read,
        );
        let Err(err) = result else {
            panic!("cross-bucket location must be rejected");
        };
        assert!(matches!(err, TableConfigError::Internal(_, _)));
    }
}
