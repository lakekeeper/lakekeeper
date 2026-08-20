//! STACKIT Object Storage.
//!
//! STACKIT serves S3 over `NetApp` `StorageGRID`, so every wire operation — signing,
//! downscoped policies, STS — is the S3 implementation's. This module is a
//! narrow public surface over it: it exposes only the knobs STACKIT customers
//! can actually act on, derives the rest, and translates `StorageGRID`'s errors
//! into advice a STACKIT customer can follow.
//!
//! The profile is persisted in its own shape so it round-trips as STACKIT, and
//! converts to an [`S3Profile`] on demand via [`StackitProfile::to_s3`].

use std::collections::BTreeMap;

use lakekeeper_io::{
    InvalidLocationError, Location,
    s3::{S3Location, S3Storage, validate_bucket_name},
};
use serde::{Deserialize, Serialize};
use url::Url;
use veil::Redact;

use super::{
    S3Credential, S3Flavor, S3Profile, ShortTermCredentialsRequest, TableConfig,
    error::{
        CredentialsError, InvalidProfileError, TableConfigError, UpdateError, ValidationError,
    },
    s3::{S3AccessKeyCredential, S3UrlStyleDetectionMode},
    storage_layout::StorageLayout,
};
use crate::{
    WarehouseId,
    api::{
        CatalogConfig, iceberg::v1::tables::DataAccessMode,
        management::v1::warehouse::TabularDeleteProfile,
    },
    request_metadata::RequestMetadata,
    service::BasicTabularInfo,
};

const DEFAULT_STS_TOKEN_VALIDITY_SECONDS: u64 = 3600;

fn fn_true() -> bool {
    true
}

fn fn_sts_validity() -> u64 {
    DEFAULT_STS_TOKEN_VALIDITY_SECONDS
}

/// Storage profile for STACKIT Object Storage.
#[derive(
    Hash, Debug, Eq, Clone, PartialEq, Serialize, Deserialize, typed_builder::TypedBuilder,
)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct StackitProfile {
    /// Name of the STACKIT bucket.
    ///
    /// Must not contain `.`: STACKIT addresses buckets as a subdomain of the
    /// endpoint, and its wildcard certificate covers only a single label.
    pub bucket: String,
    /// Subpath within the bucket to use.
    #[serde(default)]
    #[builder(default, setter(strip_option))]
    pub key_prefix: Option<String>,
    /// STACKIT region, e.g. `eu01`.
    pub region: String,
    /// Endpoint override. Normally omitted — the endpoint is derived from
    /// `region`.
    ///
    /// Set this only for a STACKIT endpoint outside the public naming scheme,
    /// which STACKIT hands out per customer. Such an endpoint is a distinct
    /// storage tenant, not another route to the same bucket, so it is immutable
    /// once the warehouse exists.
    #[serde(default)]
    #[builder(default, setter(strip_option))]
    pub endpoint: Option<Url>,
    /// Vend temporary downscoped credentials via STS. Defaults to enabled.
    ///
    /// Requires `credentials-group-urn`, and requires the credentials group to
    /// carry a trust policy allowing `sts:AssumeRole`. Disable it to fall back
    /// to remote signing on storage that predates `StorageGRID` 12.0.
    #[serde(default = "fn_true")]
    #[builder(default = true)]
    pub sts_enabled: bool,
    /// URN of the STACKIT credentials group to assume when vending credentials,
    /// e.g. `urn:sgws:identity::87066461224079950546:group/credentials-group-a1b2c3`.
    ///
    /// Copy it verbatim from the credentials group; it is not derivable.
    /// Required when `sts-enabled` is true, optional otherwise — but validated
    /// whenever it is present.
    #[serde(default)]
    #[builder(default, setter(strip_option))]
    pub credentials_group_urn: Option<String>,
    /// Validity of vended credentials in seconds. Defaults to 3600.
    #[serde(default = "fn_sts_validity")]
    #[builder(default = DEFAULT_STS_TOKEN_VALIDITY_SECONDS)]
    pub sts_token_validity_seconds: u64,
    /// Allow clients to have Lakekeeper sign their S3 requests. Defaults to
    /// enabled, and is the only client path when `sts-enabled` is false.
    #[serde(default = "fn_true")]
    #[builder(default = true)]
    pub remote_signing_enabled: bool,
    /// Push `s3.delete-enabled=false` to clients, discouraging Spark from
    /// deleting files directly and bypassing soft-deletion. Defaults to true.
    #[serde(default = "fn_true")]
    #[builder(default = true)]
    pub push_s3_delete_disabled: bool,
    /// Storage layout for namespace and tabular paths.
    #[serde(default)]
    #[builder(default, setter(strip_option))]
    pub storage_layout: Option<StorageLayout>,
}

impl StackitProfile {
    /// Endpoint this profile talks to — the override if set, else derived from
    /// `region`.
    ///
    /// The region is re-validated here rather than trusted from
    /// [`Self::normalize`]: it is interpolated into a hostname, so a value
    /// containing `@`, `#` or `/` could otherwise redirect the derived URL to
    /// another host entirely.
    ///
    /// # Errors
    /// Fails if the derived host is not a valid URL, which requires a `region`
    /// that survived [`Self::normalize`].
    pub fn endpoint(&self) -> Result<Url, ValidationError> {
        if let Some(endpoint) = &self.endpoint {
            return Ok(endpoint.clone());
        }
        validate_region(&self.region)?;
        let host = format!("https://object.storage.{}.onstackit.cloud", self.region);
        Url::parse(&host).map_err(|e| {
            InvalidProfileError {
                source: Some(Box::new(e)),
                reason: format!(
                    "Could not derive a STACKIT endpoint from region `{}`",
                    self.region
                ),
                entity: "region".to_string(),
            }
            .into()
        })
    }

    /// The equivalent [`S3Profile`], which owns all wire behaviour.
    ///
    /// STACKIT is `StorageGRID`, so: S3-compatible flavor, virtual-host
    /// addressing (its wildcard certificate covers exactly one label, which
    /// [`Self::normalize`] enforces by rejecting dotted bucket names), and no
    /// legacy MD5 — `DeleteObjects` accepts the default checksum.
    ///
    /// The credentials-group URN travels in `sts_role_arn`: `StorageGRID`'s
    /// `AssumeRole` takes it in the `RoleArn` field even though it names a
    /// group rather than a role.
    ///
    /// # Errors
    /// Fails if the endpoint cannot be derived.
    pub fn to_s3(&self) -> Result<S3Profile, ValidationError> {
        Ok(S3Profile {
            bucket: self.bucket.clone(),
            key_prefix: self.key_prefix.clone(),
            region: self.region.clone(),
            endpoint: Some(self.endpoint()?),
            sts_enabled: self.sts_enabled,
            // StorageGRID takes the credentials-group URN in `RoleArn`.
            sts_role_arn: self.credentials_group_urn.clone(),
            sts_token_validity_seconds: self.sts_token_validity_seconds,
            remote_signing_enabled: self.remote_signing_enabled,
            push_s3_delete_disabled: self.push_s3_delete_disabled,
            storage_layout: self.storage_layout.clone(),

            // Pinned for STACKIT. Measured against StorageGRID 12.0.0.7 rather
            // than assumed; see the module docs.
            flavor: S3Flavor::S3Compat,
            // Virtual-host addressing: STACKIT publishes wildcard DNS and a
            // `*.<endpoint>` certificate. `normalize` rejects dotted bucket
            // names, which that certificate cannot cover.
            path_style_access: Some(false),
            remote_signing_url_style: S3UrlStyleDetectionMode::VirtualHost,
            // `DeleteObjects` accepts the default checksum.
            legacy_md5_behavior: Some(false),
            // STS shares the S3 host.
            sts_endpoint: None,

            // AWS-only, and deliberately not exposed on the STACKIT profile.
            assume_role_arn: None,
            sts_session_tags: BTreeMap::new(),
            aws_kms_key_arn: None,
            // `s3a://` / `s3n://` are for migrating Hadoop tables; not a
            // STACKIT concern.
            allow_alternative_protocols: None,
        })
    }

    /// Validate the STACKIT profile and canonicalize it.
    ///
    /// # Errors
    /// - `bucket` is not a valid S3 bucket name, or contains a `.`
    /// - `region` is empty
    /// - `sts_enabled` without a `credentials_group_urn`
    /// - `credentials_group_urn` is present but is not a STACKIT
    ///   credentials-group URN, whether or not `sts_enabled` is set
    /// - both `sts_enabled` and `remote_signing_enabled` are false, leaving
    ///   clients no way to reach the data
    pub(crate) fn normalize(
        &mut self,
        credential: Option<&StackitCredential>,
    ) -> Result<(), ValidationError> {
        validate_bucket_name(&self.bucket).map_err(|e| InvalidProfileError {
            source: None,
            reason: e.to_string(),
            entity: "bucket".to_string(),
        })?;
        if self.bucket.contains('.') {
            return Err(InvalidProfileError {
                source: None,
                reason: format!(
                    "STACKIT bucket names must not contain `.`: buckets are addressed as a \
                     subdomain of the endpoint, and STACKIT's wildcard certificate matches \
                     only a single label. Got `{}`.",
                    self.bucket
                ),
                entity: "bucket".to_string(),
            }
            .into());
        }

        self.region = self.region.trim().to_string();
        validate_region(&self.region)?;

        if let Some(key_prefix) = self.key_prefix.as_mut() {
            *key_prefix = key_prefix.trim().trim_matches('/').to_string();
        }
        if self.key_prefix.as_ref().is_some_and(String::is_empty) {
            self.key_prefix = None;
        }

        // Treat blank as absent, so `"credentials-group-urn": ""` is a missing
        // value rather than a stored empty string.
        if let Some(urn) = self.credentials_group_urn.as_mut() {
            *urn = urn.trim().to_string();
        }
        if self
            .credentials_group_urn
            .as_ref()
            .is_some_and(String::is_empty)
        {
            self.credentials_group_urn = None;
        }

        match self.credentials_group_urn.as_ref() {
            // Validated whenever supplied, not only when STS is on: otherwise a
            // typo persists silently and only surfaces when someone enables STS
            // later, far from the change that introduced it.
            Some(urn) => validate_credentials_group_urn(urn)?,
            None if self.sts_enabled => {
                return Err(InvalidProfileError {
                    source: None,
                    reason: "`credentials-group-urn` is required when `sts-enabled` is true. \
                             Copy it from the STACKIT credentials group, or set \
                             `sts-enabled` to false to use remote signing instead."
                        .to_string(),
                    entity: "credentials-group-urn".to_string(),
                }
                .into());
            }
            None => {}
        }

        if !self.sts_enabled && !self.remote_signing_enabled {
            return Err(InvalidProfileError {
                source: None,
                reason: "`sts-enabled` and `remote-signing-enabled` are both false, so clients \
                         would have no way to read or write data. Enable at least one."
                    .to_string(),
                entity: "remote-signing-enabled".to_string(),
            }
            .into());
        }

        // Cross-check against the S3 implementation that actually talks to
        // STACKIT, so a shape it would reject cannot pass validation here.
        let s3_credential = credential.map(|c| S3Credential::from(c.clone()));
        self.to_s3()?.normalize(s3_credential.as_ref())?;
        Ok(())
    }

    /// Check that `other` is a permitted evolution of this profile.
    ///
    /// # Errors
    /// Fails if a field is changed that would move the warehouse's data.
    pub fn update_with(self, mut other: Self) -> Result<Self, UpdateError> {
        if self.bucket != other.bucket {
            return Err(UpdateError::ImmutableField("bucket".to_string()));
        }
        if self.key_prefix != other.key_prefix {
            return Err(UpdateError::ImmutableField("key_prefix".to_string()));
        }
        if self.region != other.region {
            return Err(UpdateError::ImmutableField("region".to_string()));
        }
        // A different STACKIT endpoint is a different storage tenant, so moving
        // it would silently repoint the warehouse at other data. Unlike plain
        // S3, where an endpoint change is usually just another route.
        if self.endpoint != other.endpoint {
            return Err(UpdateError::ImmutableField("endpoint".to_string()));
        }
        // An update that omits the layout keeps the current one; resetting it
        // would change where new tables are written. Matches `S3Profile`.
        if other.storage_layout.is_none() {
            other.storage_layout = self.storage_layout;
        }
        Ok(other)
    }

    /// Whether this profile's data location overlaps `other`'s.
    #[must_use]
    pub fn is_overlapping_location(&self, other: &Self) -> bool {
        match (self.to_s3(), other.to_s3()) {
            (Ok(this), Ok(that)) => this.is_overlapping_location(&that),
            // An un-normalized profile cannot be proven disjoint, so treat it
            // as overlapping rather than waving it through.
            _ => true,
        }
    }

    /// Root location of this warehouse's data.
    ///
    /// # Errors
    /// Fails for un-normalized profiles.
    pub fn base_location(&self) -> Result<Location, InvalidLocationError> {
        self.to_s3()
            .map_err(|e| {
                InvalidLocationError::new(
                    self.bucket.clone(),
                    format!("Invalid STACKIT profile: {e}"),
                )
            })?
            .base_location()
            .map(S3Location::into_location)
    }

    /// Catalog-level config handed to clients.
    ///
    /// # Errors
    /// Fails for un-normalized profiles.
    pub fn generate_catalog_config(
        &self,
        warehouse_id: WarehouseId,
        request_metadata: &RequestMetadata,
        delete_profile: TabularDeleteProfile,
    ) -> Result<CatalogConfig, ValidationError> {
        Ok(self
            .to_s3()?
            .generate_catalog_config(warehouse_id, request_metadata, delete_profile))
    }

    /// Storage client for Lakekeeper's own IO.
    ///
    /// # Errors
    /// Fails if the client cannot be built.
    pub async fn lakekeeper_io(
        &self,
        credential: Option<&StackitCredential>,
    ) -> Result<S3Storage, CredentialsError> {
        let s3_credential = credential.map(|c| S3Credential::from(c.clone()));
        self.to_s3()
            .map_err(|e| CredentialsError::Misconfiguration(e.to_string()))?
            .lakekeeper_io(s3_credential.as_ref())
            .await
    }

    /// Per-table config handed to clients, including vended credentials.
    ///
    /// # Errors
    /// Fails if credentials cannot be vended. `StorageGRID`'s own errors are
    /// translated into STACKIT-specific advice by
    /// [`explain_stackit_sts_failure`].
    pub async fn generate_table_config(
        &self,
        data_access: DataAccessMode,
        credential: Option<&StackitCredential>,
        stc_request: ShortTermCredentialsRequest,
        tabular_info: &impl BasicTabularInfo,
        request_metadata: &RequestMetadata,
    ) -> Result<TableConfig, TableConfigError> {
        let s3_credential = credential.map(|c| S3Credential::from(c.clone()));
        self.to_s3()
            .map_err(|e| TableConfigError::Misconfiguration(e.to_string()))?
            .generate_table_config(
                data_access,
                s3_credential.as_ref(),
                stc_request,
                tabular_info,
                request_metadata,
            )
            .await
            .map_err(|e| self.explain_sts_failure(e))
    }

    /// Rewrite a `StorageGRID` failure into advice a STACKIT customer can act on.
    ///
    /// The raw errors name `StorageGRID` concepts a STACKIT customer never sees,
    /// and the trust-policy prerequisite is invisible in them.
    fn explain_sts_failure(&self, error: TableConfigError) -> TableConfigError {
        let raw = error.to_string();
        let advice = if raw.contains("MethodNotAllowed") {
            Some(format!(
                "STACKIT storage in region `{}` does not offer an STS endpoint. This is \
                 StorageGRID older than 12.0. Ask STACKIT support to migrate the storage, or \
                 set `sts-enabled` to false to use remote signing instead.",
                self.region
            ))
        } else if raw.contains("cannot be found") {
            Some(format!(
                "STACKIT could not find the credentials group `{}`. Check the URN against the \
                 credentials group — note it uses the group's ID, not its display name.",
                self.credentials_group_urn.as_deref().unwrap_or("<unset>")
            ))
        } else if raw.contains("Invalid resource type") || raw.contains("Failed to parse RoleArn") {
            Some(
                "`credentials-group-urn` is not a STACKIT credentials-group URN. It must look \
                 like `urn:sgws:identity::<account>:group/credentials-group-<id>`."
                    .to_string(),
            )
        } else if raw.contains("AccessDenied") || raw.contains("not authorized") {
            self.credentials_group_urn.as_deref().map(|urn| {
                format!(
                    "STACKIT refused to assume credentials group `{urn}`. The group needs a \
                     trust policy allowing `sts:AssumeRole`: {}",
                    trust_policy_hint(urn)
                )
            })
        } else {
            None
        };

        match advice {
            Some(advice) => TableConfigError::Misconfiguration(format!("{advice} ({raw})")),
            None => error,
        }
    }
}

/// The trust policy a STACKIT credentials group needs before it can be assumed.
///
/// The principal is the same URN with `:group/` replaced by `:user/`.
fn trust_policy_hint(group_urn: &str) -> String {
    let principal = group_urn.replace(":group/", ":user/");
    format!(
        r#"{{"Statement":[{{"Action":"sts:AssumeRole","Effect":"Allow","Principal":{{"AWS":"{principal}"}}}}]}}"#
    )
}

/// Validate a STACKIT region.
///
/// The region is interpolated into the derived hostname, so the character set
/// is restricted to what cannot alter the URL's authority. Lowercase
/// alphanumerics and `-` cover every STACKIT region (`eu01`) and leave room for
/// AWS-style names without admitting `@`, `#`, `/`, `:` or `.`.
fn validate_region(region: &str) -> Result<(), ValidationError> {
    let valid = !region.is_empty()
        && region
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
    if valid {
        return Ok(());
    }
    Err(InvalidProfileError {
        source: None,
        reason: format!(
            "STACKIT `region` must be non-empty and contain only lowercase letters, digits \
             and `-`, e.g. `eu01`. Got `{region}`."
        ),
        entity: "region".to_string(),
    }
    .into())
}

/// Validate a STACKIT credentials-group URN.
///
/// Shape: `urn:sgws:identity::<account>:group/<name>`. Checked up-front because
/// the alternative is a `StorageGRID` parse error at first table load.
fn validate_credentials_group_urn(urn: &str) -> Result<(), ValidationError> {
    let invalid = |reason: String| -> ValidationError {
        InvalidProfileError {
            source: None,
            reason,
            entity: "credentials-group-urn".to_string(),
        }
        .into()
    };

    if urn.starts_with("arn:") {
        return Err(invalid(format!(
            "`{urn}` is an AWS ARN. STACKIT expects a credentials-group URN of the form \
             `urn:sgws:identity::<account>:group/credentials-group-<id>`."
        )));
    }

    // urn : sgws : identity : <empty> : <account> : group/<name>
    let parts: Vec<&str> = urn.splitn(6, ':').collect();
    let shape_ok = parts.len() == 6
        && parts[0] == "urn"
        && parts[1] == "sgws"
        && parts[2] == "identity"
        && parts[3].is_empty()
        && !parts[4].is_empty()
        && parts[4].chars().all(|c| c.is_ascii_digit())
        && parts[5].starts_with("group/")
        && parts[5].len() > "group/".len();

    if !shape_ok {
        return Err(invalid(format!(
            "`{urn}` is not a STACKIT credentials-group URN. Expected \
             `urn:sgws:identity::<account>:group/<group-id>`, copied verbatim from the \
             credentials group."
        )));
    }
    Ok(())
}

/// Credentials for STACKIT Object Storage.
///
/// Its own enum rather than a reuse of `S3Credential`, so STACKIT can gain
/// authentication methods without inheriting AWS-only ones.
#[derive(Debug, Hash, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(tag = "credential-type", rename_all = "kebab-case")]
pub enum StackitCredential {
    /// Access key and secret, created inside a STACKIT credentials group.
    AccessKey(StackitAccessKeyCredential),
}

#[derive(Redact, Hash, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "open-api", schema(title = "StackitCredentialAccessKey"))]
#[serde(rename_all = "kebab-case")]
pub struct StackitAccessKeyCredential {
    /// Access key ID of a key created in the STACKIT credentials group.
    #[serde(alias = "aws-access-key-id")]
    pub access_key_id: String,
    /// Secret shown once when the access key was created.
    #[redact(partial)]
    #[serde(alias = "aws-secret-access-key")]
    pub secret_access_key: String,
}

impl From<StackitCredential> for S3Credential {
    fn from(value: StackitCredential) -> Self {
        match value {
            StackitCredential::AccessKey(c) => S3Credential::AccessKey(S3AccessKeyCredential {
                access_key_id: c.access_key_id,
                secret_access_key: c.secret_access_key,
                external_id: None,
            }),
        }
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;

    /// Live tests against STACKIT. Skipped unless `LAKEKEEPER_TEST__STACKIT_*`
    /// is set; `unwrap` on the variables so a half-configured environment fails
    /// loudly instead of passing without testing anything.
    pub(crate) mod stackit_integration_tests {
        use super::{
            super::super::{
                StorageCredential,
                validation::{ValidationCheckName, ValidationCheckStatus},
            },
            *,
        };
        use crate::{
            request_metadata::RequestMetadata,
            service::storage::{StorageProfile, s3::test::test_block_on},
        };

        pub(crate) fn storage_profile(key_prefix: &str) -> (StackitProfile, StackitCredential) {
            let mut profile = StackitProfile::builder()
                .bucket(std::env::var("LAKEKEEPER_TEST__STACKIT_BUCKET").unwrap())
                .region(std::env::var("LAKEKEEPER_TEST__STACKIT_REGION").unwrap())
                .key_prefix(key_prefix.to_string())
                .credentials_group_urn(
                    std::env::var("LAKEKEEPER_TEST__STACKIT_CREDENTIALS_GROUP_URN").unwrap(),
                )
                .build();
            // Optional: the endpoint is normally derived. Set while STACKIT's
            // regular object storage predates the STS endpoint.
            if let Ok(endpoint) = std::env::var("LAKEKEEPER_TEST__STACKIT_ENDPOINT") {
                profile.endpoint = Some(endpoint.parse().unwrap());
            }
            let credential = StackitCredential::AccessKey(StackitAccessKeyCredential {
                access_key_id: std::env::var("LAKEKEEPER_TEST__STACKIT_ACCESS_KEY_ID").unwrap(),
                secret_access_key: std::env::var("LAKEKEEPER_TEST__STACKIT_SECRET_ACCESS_KEY")
                    .unwrap(),
            });
            (profile, credential)
        }

        /// Every storage check must actually *pass*, not merely not-fail.
        ///
        /// Asserted against the report rather than `validate_access`, which
        /// collapses skipped into success — so a configuration that silently
        /// skipped credential vending would otherwise look green here.
        #[test]
        fn test_can_validate() {
            // Shared runtime: the S3 client behind this profile is a static.
            test_block_on(
                async {
                    let (profile, credential) =
                        storage_profile(&format!("validate-{}", uuid::Uuid::now_v7()));
                    let credential: StorageCredential = StorageCredential::Stackit(credential);
                    let mut profile: StorageProfile = StorageProfile::Stackit(profile);

                    profile.normalize(Some(&credential)).unwrap();
                    let report = Box::pin(profile.validate_access_report(
                        Some(&credential),
                        None,
                        &RequestMetadata::new_unauthenticated(),
                    ))
                    .await;
                    assert!(report.valid, "{:?}", report.checks);

                    for name in [
                        ValidationCheckName::StorageClientInitialized,
                        ValidationCheckName::LakekeeperReadWrite,
                        ValidationCheckName::VendedCredentialsIssued,
                        ValidationCheckName::VendedCredentialsReadWrite,
                        ValidationCheckName::VendedCredentialsScopeEnforced,
                        ValidationCheckName::Cleanup,
                    ] {
                        let check = report
                            .checks
                            .iter()
                            .find(|c| c.name == name)
                            .unwrap_or_else(|| panic!("report is missing {name}"));
                        assert_eq!(
                            check.status,
                            ValidationCheckStatus::Passed,
                            "{name} did not pass: {check:?}"
                        );
                    }
                },
                true,
            );
        }

        /// Vending must be refused when no credentials group is configured,
        /// rather than falling back to the un-downscoped parent credentials.
        #[test]
        fn test_vending_without_a_credentials_group_is_refused() {
            test_block_on(
                async {
                    let (mut profile, credential) =
                        storage_profile(&format!("nogroup-{}", uuid::Uuid::now_v7()));
                    profile.credentials_group_urn = None;
                    let credential: StorageCredential = StorageCredential::Stackit(credential);
                    let mut profile: StorageProfile = StorageProfile::Stackit(profile);

                    let err = profile
                        .normalize(Some(&credential))
                        .unwrap_err()
                        .to_string();
                    assert!(err.contains("credentials-group-urn"), "{err}");
                },
                true,
            );
        }
    }

    fn profile() -> StackitProfile {
        StackitProfile::builder()
            .bucket("my-warehouse".to_string())
            .region("eu01".to_string())
            .credentials_group_urn(
                "urn:sgws:identity::87066461224079950546:group/credentials-group-a1b2c3"
                    .to_string(),
            )
            .build()
    }

    #[test]
    fn endpoint_is_derived_from_the_region() {
        assert_eq!(
            profile().endpoint().unwrap().as_str(),
            "https://object.storage.eu01.onstackit.cloud/"
        );
    }

    #[test]
    fn explicit_endpoint_wins_over_derivation() {
        let mut p = profile();
        p.endpoint = Some(Url::parse("https://private.example.com").unwrap());
        assert_eq!(
            p.endpoint().unwrap().as_str(),
            "https://private.example.com/"
        );
    }

    #[test]
    fn sts_is_enabled_by_default() {
        assert!(profile().sts_enabled);
    }

    #[test]
    fn the_urn_travels_as_the_sts_role_arn() {
        let s3 = profile().to_s3().unwrap();
        assert_eq!(
            s3.sts_role_arn.as_deref(),
            Some("urn:sgws:identity::87066461224079950546:group/credentials-group-a1b2c3")
        );
        assert_eq!(s3.flavor, S3Flavor::S3Compat);
        assert_eq!(s3.legacy_md5_behavior, Some(false));
    }

    #[test]
    fn dotted_bucket_names_are_rejected_because_virtual_host_tls_would_fail() {
        let mut p = profile();
        p.bucket = "my.warehouse".to_string();
        let err = p.normalize(None).unwrap_err().to_string();
        assert!(err.contains("must not contain"), "{err}");
    }

    #[test]
    fn sts_without_a_group_urn_is_rejected() {
        let mut p = profile();
        p.credentials_group_urn = None;
        let err = p.normalize(None).unwrap_err().to_string();
        assert!(err.contains("credentials-group-urn"), "{err}");
    }

    #[test]
    fn disabling_sts_makes_the_group_urn_optional() {
        let mut p = profile();
        p.credentials_group_urn = None;
        p.sts_enabled = false;
        p.normalize(None).unwrap();
    }

    #[test]
    fn a_blank_group_urn_counts_as_absent() {
        let mut p = profile();
        p.credentials_group_urn = Some("   ".to_string());
        // With STS on, blank is reported as missing rather than as malformed.
        let err = p.clone().normalize(None).unwrap_err().to_string();
        assert!(err.contains("is required when"), "{err}");

        // With STS off it is simply cleared, not stored as an empty string.
        p.sts_enabled = false;
        p.normalize(None).unwrap();
        assert_eq!(p.credentials_group_urn, None);
    }

    #[test]
    fn a_supplied_urn_is_validated_even_when_sts_is_disabled() {
        let mut p = profile();
        p.sts_enabled = false;
        p.credentials_group_urn = Some("credentials-group-8cd7b4".to_string());
        let err = p.normalize(None).unwrap_err().to_string();
        assert!(err.contains("not a STACKIT credentials-group URN"), "{err}");
    }

    #[test]
    fn a_group_urn_is_trimmed() {
        let mut p = profile();
        p.credentials_group_urn = Some(
            "  urn:sgws:identity::87066461224079950546:group/credentials-group-8cd7b4  "
                .to_string(),
        );
        p.normalize(None).unwrap();
        assert_eq!(
            p.credentials_group_urn.as_deref(),
            Some("urn:sgws:identity::87066461224079950546:group/credentials-group-8cd7b4")
        );
    }

    #[test]
    fn a_region_cannot_inject_a_different_host_into_the_derived_endpoint() {
        // The region is interpolated into the hostname, so `@` (userinfo) and
        // `#` (fragment) would otherwise move the authority off onstackit.cloud.
        for region in [
            "eu01@evil.example",
            "eu01#.onstackit.cloud",
            "eu01/x",
            "eu01.evil",
            "EU01",
            "",
        ] {
            let mut p = profile();
            p.region = region.to_string();
            assert!(
                p.clone().normalize(None).is_err(),
                "expected region `{region}` to be rejected"
            );
            assert!(
                p.endpoint().is_err(),
                "endpoint() accepted region `{region}`"
            );
        }
        // Hyphens stay valid, so an AWS-style region is not rejected.
        let mut p = profile();
        p.region = "eu-central-1".to_string();
        p.normalize(None).unwrap();
    }

    #[test]
    fn an_update_that_omits_the_layout_keeps_the_current_one() {
        let mut before = profile();
        before.storage_layout = Some(StorageLayout::default());
        let mut after = before.clone();
        after.storage_layout = None;

        let merged = before.clone().update_with(after).unwrap();
        assert_eq!(
            merged.storage_layout, before.storage_layout,
            "omitting storage-layout must not reset it"
        );
    }

    #[test]
    fn disabling_both_client_paths_is_rejected() {
        let mut p = profile();
        p.sts_enabled = false;
        p.remote_signing_enabled = false;
        let err = p.normalize(None).unwrap_err().to_string();
        assert!(err.contains("no way to read or write"), "{err}");
    }

    #[test]
    fn an_aws_arn_is_rejected_with_the_expected_shape() {
        let err = validate_credentials_group_urn("arn:aws:iam::123456789012:role/x")
            .unwrap_err()
            .to_string();
        assert!(err.contains("AWS ARN"), "{err}");
        assert!(err.contains("urn:sgws:identity"), "{err}");
    }

    #[test]
    fn malformed_urns_are_rejected() {
        for urn in [
            "urn:sgws:identity::87066461224079950546:role/x",
            "urn:sgws:iam::87066461224079950546:group/x",
            "urn:sgws:identity::notdigits:group/x",
            "urn:sgws:identity::87066461224079950546:group/",
            "credentials-group-a1b2c3",
        ] {
            assert!(
                validate_credentials_group_urn(urn).is_err(),
                "expected `{urn}` to be rejected"
            );
        }
    }

    #[test]
    fn the_trust_policy_hint_names_the_user_form_of_the_group() {
        let hint = trust_policy_hint(
            "urn:sgws:identity::87066461224079950546:group/credentials-group-a1b2c3",
        );
        assert!(
            hint.contains(
                r#""AWS":"urn:sgws:identity::87066461224079950546:user/credentials-group-a1b2c3""#
            ),
            "{hint}"
        );
    }

    #[test]
    fn relocating_fields_are_immutable() {
        let base = profile();
        for mutate in [
            (|mut p: StackitProfile| {
                p.bucket = "other".to_string();
                p
            }) as fn(StackitProfile) -> StackitProfile,
            |mut p: StackitProfile| {
                p.region = "eu02".to_string();
                p
            },
            |mut p: StackitProfile| {
                p.endpoint =
                    Some(Url::parse("https://dataplatform.storage.eu01.onstackit.cloud").unwrap());
                p
            },
            |mut p: StackitProfile| {
                p.key_prefix = Some("elsewhere".to_string());
                p
            },
        ] {
            assert!(base.clone().update_with(mutate(base.clone())).is_err());
        }
        // Rotating a credentials group is not a relocation.
        let mut ok = base.clone();
        ok.credentials_group_urn =
            Some("urn:sgws:identity::87066461224079950546:group/credentials-group-zzz".to_string());
        assert!(base.clone().update_with(ok).is_ok());
    }

    #[test]
    fn a_minimal_profile_deserializes_with_the_documented_defaults() {
        let p: StackitProfile = serde_json::from_str(
            r#"{"bucket":"b","region":"eu01","credentials-group-urn":"urn:sgws:identity::1:group/g"}"#,
        )
        .unwrap();
        assert!(p.sts_enabled);
        assert!(p.remote_signing_enabled);
        assert!(p.push_s3_delete_disabled);
        assert_eq!(p.sts_token_validity_seconds, 3600);
        assert_eq!(p.endpoint, None);
    }

    #[test]
    fn a_custom_endpoint_is_honoured_and_frozen() {
        let dataplatform = Url::parse("https://dataplatform.storage.eu01.onstackit.cloud").unwrap();
        let mut p = profile();
        p.endpoint = Some(dataplatform.clone());
        p.normalize(None).unwrap();
        assert_eq!(p.to_s3().unwrap().endpoint, Some(dataplatform));
    }
}
