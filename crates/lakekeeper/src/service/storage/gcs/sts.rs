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
        let mut table_location = table_location.clone();
        table_location.with_trailing_slash();
        let prefixless_location = table_location
            .as_str()
            .replace(&format!("gs://{bucket}/"), "");

        validate_safe_for_cel_single_quoted(bucket)?;
        validate_safe_for_cel_single_quoted(&prefixless_location)?;

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
                            "resource.name.startsWith('projects/_/buckets/{bucket}/objects/{prefixless_location}') || resource.name.startsWith('projects/_/buckets/{bucket}/folders/{prefixless_location}') || api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith('{prefixless_location}')",
                        ),
                    }.into(),
                }],
            },
        })
    }
}

/// Reject characters that can escape a CEL single-quoted string literal.
/// GCP's access-boundary CEL doesn't accept `r'...'` raw strings or `+`
/// concat, so values are interpolated directly into `'...'`.
fn validate_safe_for_cel_single_quoted(value: &str) -> Result<(), TableConfigError> {
    for c in value.chars() {
        let reject = c == '\'' || c == '\\' || c == '\n' || c == '\r' || c.is_control();
        if reject {
            return Err(TableConfigError::Internal(
                format!(
                    "Refusing to build GCS access boundary: input contains a character that cannot appear in a CEL single-quoted string (U+{:04X})",
                    c as u32
                ),
                None,
            ));
        }
    }
    Ok(())
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
    use super::*;

    #[test]
    fn validate_safe_for_cel_single_quoted_accepts_plain_value() {
        validate_safe_for_cel_single_quoted("foo/bar").unwrap();
        // Multibyte UTF-8 and double quotes are fine inside a single-quoted
        // CEL literal — they have no special meaning.
        validate_safe_for_cel_single_quoted("foo/bar/üñîçødé").unwrap();
        validate_safe_for_cel_single_quoted(r#"foo"bar"#).unwrap();
    }

    #[test]
    fn validate_safe_for_cel_single_quoted_rejects_single_quote() {
        // Injection payload: `') || true || resource.name.startsWith('`. Must
        // be rejected, otherwise the closing `'` would terminate the literal
        // and let the rest be parsed as CEL syntax.
        let err = validate_safe_for_cel_single_quoted("') || true || resource.name.startsWith('")
            .expect_err("single quote must be rejected");
        assert!(matches!(err, TableConfigError::Internal(_, _)));
    }

    #[test]
    fn validate_safe_for_cel_single_quoted_rejects_backslash() {
        // Backslash starts an escape sequence in non-raw CEL strings (e.g.
        // `\'` would be a literal `'`, defeating the closing-delimiter
        // check). Must be rejected.
        let err = validate_safe_for_cel_single_quoted(r"foo\bar")
            .expect_err("backslash must be rejected");
        assert!(matches!(err, TableConfigError::Internal(_, _)));
    }

    #[test]
    fn validate_safe_for_cel_single_quoted_rejects_newline_and_carriage_return() {
        validate_safe_for_cel_single_quoted("foo\nbar").expect_err("newline must be rejected");
        validate_safe_for_cel_single_quoted("foo\rbar").expect_err("CR must be rejected");
    }

    #[test]
    fn options_neutralizes_cel_injection_in_path() {
        // End-to-end: the constructed CEL expression must not be twistable
        // into evaluating to `true` by an injection attempt in the path.
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
        // GCP's CEL subset doesn't accept raw-string literals or string
        // concat; safety comes from `validate_safe_for_cel_single_quoted`
        // having rejected anything that could escape the surrounding `'...'`.
        // The expression should embed bucket/path inline as plain literals.
        assert!(
            expr.contains("/buckets/my-bucket/objects/wh/safe-prefix/"),
            "expected inline path literal in expression, got: {expr}"
        );
        // And it must not regress to using the rejected raw-string / concat
        // forms.
        assert!(
            !expr.contains("r'"),
            "expression must not use raw-string literals (rejected by GCP), got: {expr}"
        );
        assert!(
            !expr.contains(" + "),
            "expression must not use string concat (rejected by GCP), got: {expr}"
        );
    }

    #[test]
    fn options_rejects_cel_injection_payload_in_path() {
        // A table location whose path contains `'` must cause credential
        // construction to fail outright rather than producing a smuggled CEL
        // expression.
        let bucket = "my-bucket";
        let location: Location = "gs://my-bucket/x'/data/"
            .parse()
            .expect("URL parse should accept ' (sub-delim)");
        let result =
            Options::from_location_and_permissions(bucket, &location, StoragePermissions::Read);
        let Err(err) = result else {
            panic!("input with ' must be rejected");
        };
        assert!(matches!(err, TableConfigError::Internal(_, _)));
    }
}
