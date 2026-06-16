#[cfg(test)]
use std::sync::LazyLock;
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use azure_storage::{
    prelude::{BlobSasPermissions, BlobSignedResource},
    shared_access_signature::{
        SasToken,
        service_sas::{BlobSharedAccessSignature, SasKey},
    },
};
use azure_storage_blobs::prelude::BlobServiceClient;
use iceberg_ext::configs::table::{TableProperties, adls, creds, custom};
use lakekeeper_io::{
    InvalidLocationError,
    adls::{AdlsLocation, AdlsStorage, AzureAuth, AzureSasAuth, AzureSettings},
};
use time::OffsetDateTime;
#[cfg(test)]
use url::Url;

use crate::{
    api::{CatalogConfig, RequestMetadata, Result, iceberg::supported_endpoints},
    request_metadata::UserAgent,
    service::{
        BasicTabularInfo,
        storage::{
            ShortTermCredentialsRequest, StoragePermissions, TableConfig,
            cache::{ADLS_STC_CACHE, CachedStc, STCCacheKey, get_or_load_stc},
            error::{CredentialsError, InvalidProfileError, TableConfigError, ValidationError},
        },
    },
};

mod az_profile;
mod credentials;
mod onelake_profile;

pub use az_profile::GenericAdlsProfile;
pub use credentials::AzCredential;
pub use onelake_profile::{EndpointMode, OneLakeProfile, TopLevelFolder};

const DEFAULT_GENERIC_ADLS_HOST: &str = "dfs.core.windows.net";

#[cfg(test)]
static DEFAULT_AUTHORITY_HOST: LazyLock<Url> = LazyLock::new(|| {
    Url::parse("https://login.microsoftonline.com").expect("Default authority host is a valid URL")
});

// SAS validity / cache constants. All durations are expressed as `i64` because
// `time::Duration::seconds` and the Azure SAS API both take `i64`. Conversions
// at API boundaries thus stay direct, with no fallible cast.
const MAX_GENERIC_ADLS_SAS_TOKEN_VALIDITY_SECONDS: i64 = 7 * 24 * 60 * 60;
const MAX_ONELAKE_SAS_TOKEN_VALIDITY_SECONDS: i64 = 60 * 60;
const SAS_TOKEN_DEFAULT_VALIDITY_SECONDS: i64 = 3600;

/// Backshift applied to `signed_start` to tolerate clock skew across machines.
/// The SAS is technically valid for `SAS_TOKEN_START_BACKSHIFT_SECONDS` in the
/// past. Bump this if observed clock drift grows.
const SAS_TOKEN_START_BACKSHIFT_SECONDS: i64 = 60;

/// Minimum wall-clock validity we want a freshly-minted SAS to have from
/// "now" — i.e. the smallest window between the moment Lakekeeper hands the
/// SAS to the caller and its `signed_expiry`. Independent of clock drift.
const MIN_SAS_TOKEN_WALL_CLOCK_VALIDITY_SECONDS: i64 = 60;

/// Floor for the effective SAS lifetime sent to Azure. Derived: the
/// `signed_start` is backshifted, so to give the caller
/// `MIN_SAS_TOKEN_WALL_CLOCK_VALIDITY_SECONDS` of wall-clock validity from
/// "now" we have to mint at least that plus the backshift window.
const MIN_SAS_TOKEN_EFFECTIVE_TTL_SECONDS: i64 =
    MIN_SAS_TOKEN_WALL_CLOCK_VALIDITY_SECONDS + SAS_TOKEN_START_BACKSHIFT_SECONDS;

/// User-supplied TTL strictly below this value triggers a warning log
/// (not a rejection — the value is silently floored at
/// [`MIN_SAS_TOKEN_EFFECTIVE_TTL_SECONDS`] at mint time).
const SAS_TOKEN_WARN_THRESHOLD_SECONDS: i64 = 60;

/// Floor for the cache `valid_until` window — prevents an unusually short
/// user TTL from collapsing the cache lifetime to zero (which would disable
/// caching). The `StcExpiry` cache policy further halves this and caps at
/// [`MAX_ONELAKE_SAS_TOKEN_VALIDITY_SECONDS`].
const MIN_CACHE_VALID_FOR_SECONDS: i64 = 10;

// Compile-time invariants: catch a future tweak (e.g. raising
// `SAS_TOKEN_START_BACKSHIFT_SECONDS` to 3 minutes to tolerate more drift)
// from silently producing a configuration where the floor exceeds a backend
// cap, the cache outlives the SAS, or a freshly-minted token has zero
// wall-clock validity. If any of these fire, re-tune the surrounding
// constants — don't just disable the assertion.
const _: () = {
    // Floor must be positive — `effective_ttl_seconds().max(floor)` would
    // otherwise be a no-op and our minimum-validity guarantee evaporates.
    assert!(
        MIN_SAS_TOKEN_EFFECTIVE_TTL_SECONDS > 0,
        "MIN_SAS_TOKEN_EFFECTIVE_TTL_SECONDS must be positive",
    );
    // The floor must not exceed `OneLake`'s hard 1-hour cap; otherwise
    // `effective_ttl_seconds()` could return a value that
    // `validate_sas_token_validity_seconds` would have rejected on input.
    assert!(
        MIN_SAS_TOKEN_EFFECTIVE_TTL_SECONDS <= MAX_ONELAKE_SAS_TOKEN_VALIDITY_SECONDS,
        "MIN_SAS_TOKEN_EFFECTIVE_TTL_SECONDS must not exceed the OneLake cap",
    );
    assert!(
        MIN_SAS_TOKEN_EFFECTIVE_TTL_SECONDS <= MAX_GENERIC_ADLS_SAS_TOKEN_VALIDITY_SECONDS,
        "MIN_SAS_TOKEN_EFFECTIVE_TTL_SECONDS must not exceed the generic ADLS cap",
    );
    // The cache must not outlive the SAS — otherwise a cache hit could
    // return a token that's already expired wall-clock-wise.
    assert!(
        MIN_CACHE_VALID_FOR_SECONDS < MIN_SAS_TOKEN_WALL_CLOCK_VALIDITY_SECONDS,
        "MIN_CACHE_VALID_FOR_SECONDS must be smaller than the wall-clock validity floor",
    );
    // The default TTL must clear the floor; otherwise a user with no
    // override gets bumped up implicitly, which we'd rather make explicit.
    assert!(
        SAS_TOKEN_DEFAULT_VALIDITY_SECONDS >= MIN_SAS_TOKEN_EFFECTIVE_TTL_SECONDS,
        "SAS_TOKEN_DEFAULT_VALIDITY_SECONDS must clear the floor",
    );
};

impl From<StoragePermissions> for BlobSasPermissions {
    fn from(value: StoragePermissions) -> Self {
        match value {
            StoragePermissions::Read => BlobSasPermissions {
                read: true,
                list: true,
                ..Default::default()
            },
            StoragePermissions::ReadWrite => BlobSasPermissions {
                read: true,
                write: true,
                add: true,
                list: true,
                ..Default::default()
            },
            StoragePermissions::ReadWriteDelete => BlobSasPermissions {
                read: true,
                write: true,
                add: true,
                delete: true,
                list: true,
                ..Default::default()
            },
        }
    }
}

fn default_true() -> bool {
    true
}

/// Removes the hostname and user from the path.
/// Keeps only the path and optionally the scheme.
/// Reduce an ADLS URL to its rooted blob path (`/<blob_name>`).
pub(crate) fn reduce_scheme_string(path: &str) -> Result<String, InvalidLocationError> {
    let l = AdlsLocation::try_from_str(path, true)?;
    Ok(format!("/{}", l.blob_name().trim_start_matches('/')))
}

fn iceberg_sas_property_key(account_name: &str, endpoint_suffix: &str) -> String {
    format!("adls.sas-token.{account_name}.{endpoint_suffix}")
}

fn iceberg_expiration_property_key(account_name: &str, endpoint_suffix: &str) -> String {
    format!("adls.sas-token-expires-at-ms.{account_name}.{endpoint_suffix}")
}

/// Validate the user-supplied SAS-token TTL against the backend's allowed
/// maximum. Logs a warning (does not reject) for non-zero values below
/// [`SAS_TOKEN_WARN_THRESHOLD_SECONDS`] — such tokens are accepted, but at
/// mint time they are floored at the minimum effective TTL (see
/// [`effective_ttl_seconds`]).
///
/// `max_ttl` is in seconds; both the value and the max are passed as the
/// stored profile's `Option<u64>` (so the caller doesn't need to coerce).
fn validate_sas_token_validity_seconds(
    user_ttl: Option<u64>,
    max_ttl: i64,
) -> Result<(), ValidationError> {
    let Some(n) = user_ttl else {
        return Ok(());
    };
    if n == 0 {
        return Err(InvalidProfileError {
            source: None,
            reason: "SAS token validity must be greater than 0 seconds.".to_string(),
            entity: "sas-token-validity-seconds".to_string(),
        }
        .into());
    }
    // `max_ttl` is a positive constant (≤ 604_800), so the unsigned comparison is safe.
    if n > u64::try_from(max_ttl).expect("max_ttl is a positive constant") {
        return Err(InvalidProfileError {
            source: None,
            reason: format!("SAS token validity must not exceed {max_ttl} seconds."),
            entity: "sas-token-validity-seconds".to_string(),
        }
        .into());
    }
    let warn_threshold =
        u64::try_from(SAS_TOKEN_WARN_THRESHOLD_SECONDS).expect("warn threshold is positive");
    if n < warn_threshold {
        tracing::warn!(
            sas_token_validity_seconds = n,
            "Token lifetime less than {warn_threshold} seconds (provided value: {n}). Generated tokens will use minimum lifetime of at least {min_ttl}s.",
            warn_threshold = SAS_TOKEN_WARN_THRESHOLD_SECONDS,
            min_ttl = MIN_SAS_TOKEN_EFFECTIVE_TTL_SECONDS,
        );
    }
    Ok(())
}

/// Compute the effective TTL (in seconds, `i64`) sent to Azure when minting a
/// SAS.
///
/// Floored at [`MIN_SAS_TOKEN_EFFECTIVE_TTL_SECONDS`] so that, combined with
/// the [`SAS_TOKEN_START_BACKSHIFT_SECONDS`] backshift on `signed_start`, the
/// resulting SAS has at least [`MIN_SAS_TOKEN_WALL_CLOCK_VALIDITY_SECONDS`]
/// of wall-clock validity from "now". Callers are expected to have validated
/// the user-supplied value against the backend's max via
/// [`validate_sas_token_validity_seconds`], so out-of-range values are not
/// re-checked here.
fn effective_ttl_seconds(user_ttl: Option<u64>) -> i64 {
    let user = user_ttl
        .and_then(|n| i64::try_from(n).ok())
        .unwrap_or(SAS_TOKEN_DEFAULT_VALIDITY_SECONDS);
    user.max(MIN_SAS_TOKEN_EFFECTIVE_TTL_SECONDS)
}

/// Compute the SAS validity window `(signed_start, signed_expiry)`.
///
/// `signed_start` is set [`SAS_TOKEN_START_BACKSHIFT_SECONDS`] in the past to
/// tolerate clock skew. `signed_expiry = signed_start + effective_ttl`, so
/// the wall-clock validity from "now" is
/// `effective_ttl - SAS_TOKEN_START_BACKSHIFT_SECONDS`.
fn sas_validity_window(effective_ttl: i64) -> (OffsetDateTime, OffsetDateTime) {
    let start =
        OffsetDateTime::now_utc() - time::Duration::seconds(SAS_TOKEN_START_BACKSHIFT_SECONDS);
    let end = start.saturating_add(time::Duration::seconds(effective_ttl));
    (start, end)
}

/// Compute the cache eviction time for a freshly-minted SAS.
///
/// The cache is set to expire [`SAS_TOKEN_START_BACKSHIFT_SECONDS`] before
/// the SAS itself does, so any token returned from a cache hit still has
/// wall-clock validity left. Floored at [`MIN_CACHE_VALID_FOR_SECONDS`] so
/// an unusually short user TTL doesn't collapse the cache window to zero.
/// The `StcExpiry` policy in the cache layer further halves this and caps at
/// [`MAX_ONELAKE_SAS_TOKEN_VALIDITY_SECONDS`].
fn cache_valid_until(effective_ttl: i64) -> Option<Instant> {
    let cache_secs =
        (effective_ttl - SAS_TOKEN_START_BACKSHIFT_SECONDS).max(MIN_CACHE_VALID_FOR_SECONDS);
    // cache_secs ≥ MIN_CACHE_VALID_FOR_SECONDS > 0; conversion to u64 is sound.
    let cache_secs_u64 = u64::try_from(cache_secs).expect("cache_secs is positive by construction");
    Instant::now().checked_add(Duration::from_secs(cache_secs_u64))
}

/// Compute the canonical-resource string for a SAS signature.
///
/// Azure recomputes the canonical-resource by URL-decoding the request URL
/// path. Hand-rolling with the percent-encoded form (e.g. literal `%3F`)
/// produces a signature mismatch — so we URL-decode the path first.
///
/// Returns `(canonical_resource, depth)`. `depth` is the number of segments in
/// the rootless path; required by `BlobSharedAccessSignature::signed_directory_depth`.
fn canonical_resource(
    account_name: &str,
    filesystem: &str,
    stc_request: &ShortTermCredentialsRequest,
) -> Result<(String, usize), CredentialsError> {
    let path = reduce_scheme_string(stc_request.table_location.as_ref()).map_err(|e| {
        CredentialsError::ShortTermCredential {
            reason: format!("Invalid ADLS location for SAS signing: {e}"),
            source: Some(Box::new(e)),
        }
    })?;
    let rootless_path = path.trim_start_matches('/').trim_end_matches('/');
    let depth = rootless_path.split('/').count();
    let decoded_path = percent_encoding::percent_decode_str(rootless_path).decode_utf8_lossy();
    let resource = format!("/blob/{account_name}/{filesystem}/{decoded_path}");
    Ok((resource, depth))
}

/// Build a SAS token for a directory resource.
fn build_directory_sas(
    canonical_resource: String,
    permissions: BlobSasPermissions,
    signed_expiry: OffsetDateTime,
    depth: usize,
    key: impl Into<SasKey>,
) -> Result<String, CredentialsError> {
    BlobSharedAccessSignature::new(
        key,
        canonical_resource,
        permissions,
        signed_expiry,
        BlobSignedResource::Directory,
    )
    .signed_directory_depth(depth)
    .token()
    .map_err(|e| CredentialsError::ShortTermCredential {
        reason: "Error getting azure sas token.".to_string(),
        source: Some(Box::new(e)),
    })
}

/// Mint a SAS via Azure user-delegation-key flow.
async fn mint_sas_via_delegation_key(
    client: BlobServiceClient,
    sas_token_start: OffsetDateTime,
    sas_token_end: OffsetDateTime,
    canonical_resource: String,
    permissions: BlobSasPermissions,
    depth: usize,
) -> Result<(String, OffsetDateTime), CredentialsError> {
    tracing::debug!(
        "Requesting user delegation key from azure for sas token generation - Valid from {sas_token_start} to {sas_token_end}",
    );
    let delegation_key = client
        .get_user_deligation_key(sas_token_start, sas_token_end)
        .await
        .map_err(|e| CredentialsError::ShortTermCredential {
            reason: "Error getting azure user delegation key.".to_string(),
            source: Some(Box::new(e)),
        })?;
    let signed_expiry = delegation_key.user_deligation_key.signed_expiry;
    tracing::debug!(
        "Successfully obtained user delegation key from azure for sas token generation - Valid from {} until {signed_expiry}",
        delegation_key.user_deligation_key.signed_start
    );
    let key = delegation_key.user_deligation_key;
    let sas = build_directory_sas(canonical_resource, permissions, signed_expiry, depth, key)?;
    Ok((sas, signed_expiry))
}

/// Description of an ADLS-compatible storage profile sufficient for shared SAS
/// minting + cache plumbing. Both `GenericAdlsProfile` and `OneLakeProfile`
/// build one of these and hand it to [`get_or_mint_sas`].
pub(super) struct SasMintContext<'a> {
    pub(super) account_name: &'a str,
    pub(super) filesystem: &'a str,
    pub(super) user_ttl: Option<u64>,
    pub(super) settings: &'a AzureSettings,
}

/// Get a SAS token from the cache, or mint a new one on a miss.
///
/// Concurrent identical requests are coalesced onto one mint per cache key
/// (see [`get_or_load_stc`]); the closure constructs a `BlobServiceClient` only
/// on a miss. The cache stores the `(sas, expiration)` pair directly.
pub(super) async fn get_or_mint_sas(
    cache_key: STCCacheKey,
    ctx: SasMintContext<'_>,
    credential: &AzCredential,
    stc_request: &ShortTermCredentialsRequest,
) -> Result<(String, OffsetDateTime), CredentialsError> {
    let effective = effective_ttl_seconds(ctx.user_ttl);
    let valid_until = cache_valid_until(effective);

    get_or_load_stc(&ADLS_STC_CACHE, cache_key, || async move {
        let (start, end) = sas_validity_window(effective);
        tracing::debug!(
            "Generating SAS token with requested validity - start: {start}, end: {end}",
        );
        let (canonical, depth) = canonical_resource(ctx.account_name, ctx.filesystem, stc_request)?;
        let permissions: BlobSasPermissions = stc_request.storage_permissions.into();

        let (sas, expiration) = match credential {
            AzCredential::ClientCredentials { .. } => {
                let auth = AzureAuth::try_from(credential.clone())?;
                let client = ctx.settings.get_blob_service_client(&auth).await?;
                mint_sas_via_delegation_key(client, start, end, canonical, permissions, depth)
                    .await?
            }
            AzCredential::SharedAccessKey { key } => {
                let sas = build_directory_sas(
                    canonical,
                    permissions,
                    end,
                    depth,
                    azure_core::auth::Secret::new(key.clone()),
                )?;
                (sas, end)
            }
            AzCredential::AzureSystemIdentity {} => {
                let auth = AzureAuth::try_from(credential.clone())?;
                let client = ctx.settings.get_blob_service_client(&auth).await?;
                mint_sas_via_delegation_key(client, start, end, canonical, permissions, depth)
                    .await
                    .map_err(|e| {
                        tracing::debug!("Failed to get azure system identity token: {e}");
                        CredentialsError::ShortTermCredential {
                            reason: "Failed to get azure system identity token".to_string(),
                            source: Some(Box::new(e)),
                        }
                    })?
            }
        };
        Ok::<_, CredentialsError>(CachedStc::new((sas, expiration), valid_until))
    })
    .await
}

/// The static `CatalogConfig` returned by both ADLS profile types. Carries
/// no profile-specific overrides — only the standard endpoint list.
#[allow(clippy::module_name_repetitions)]
pub(super) fn adls_catalog_config() -> CatalogConfig {
    CatalogConfig {
        defaults: HashMap::default(),
        overrides: HashMap::default(),
        endpoints: supported_endpoints().to_vec(),
    }
}

/// Prefix-overlap predicate used by both profiles' `is_overlapping_location`.
///
/// `None` is treated as the filesystem root — any path overlaps root. Two
/// `Some` values overlap iff one is a directory prefix of the other (with `/`
/// boundary handling, so `prefix` does not overlap `prefix-extra`).
pub(super) fn key_prefix_overlaps(a: Option<&str>, b: Option<&str>) -> bool {
    if a == b {
        return true;
    }
    match (a, b) {
        (Some(p1), Some(p2)) => {
            let s1 = format!("{p1}/");
            let s2 = format!("{p2}/");
            s1.starts_with(&s2) || s2.starts_with(&s1)
        }
        (None, _) | (_, None) => true,
    }
}

/// Decide whether to emit the `adls.sas-token-expires-at-ms.*` iceberg property.
///
/// `PyIceberg` ≤ 0.10.0 incorrectly extracts the account name from *any* property
/// starting with `adls.sas-token`, including `…-expires-at-ms.*`, which breaks
/// endpoint detection. Skip the expires-at key for those versions.
pub(super) fn should_emit_sas_expires_at_key(request_metadata: &RequestMetadata) -> bool {
    let pyiceberg_version = match request_metadata.user_agent() {
        Some(UserAgent::PyIceberg { version }) => Some(version),
        _ => None,
    };
    let Some(version) = pyiceberg_version else {
        return true;
    };
    semver::Version::parse(version)
        .ok()
        .is_some_and(|v| v > semver::Version::new(0, 10, 0))
}

/// All inputs both ADLS profile types need to produce a `TableConfig` for a
/// vended-credentials response. The profile types build one of these from
/// their own fields and hand it to [`generate_adls_table_config`].
pub(super) struct AdlsTableConfigContext<'a, T: BasicTabularInfo> {
    pub(super) cache_key: STCCacheKey,
    pub(super) sas_mint: SasMintContext<'a>,
    pub(super) credential: &'a AzCredential,
    pub(super) stc_request: ShortTermCredentialsRequest,
    pub(super) sas_property_key: String,
    pub(super) sas_expires_at_property_key: String,
    pub(super) tabular_info: &'a T,
    pub(super) request_metadata: &'a RequestMetadata,
    /// Extra `(key, value)` pairs to emit into the vended-creds config.
    /// `OneLake` uses this to publish `adls.account-host` so that pyiceberg /
    /// `adlfs.AzureBlobFileSystem` targets `*.fabric.microsoft.com` instead
    /// of defaulting to `<account>.blob.core.windows.net`.
    pub(super) extra_config: Vec<(String, String)>,
}

/// Mint (or look up cached) SAS + build the iceberg `TableConfig` for it.
///
/// Both ADLS profile types delegate the body of their `generate_table_config`
/// to this once the data-access early-out has been handled.
///
/// # Errors
/// Fails if a SAS token cannot be obtained from cache or Azure.
pub(super) async fn generate_adls_table_config<T: BasicTabularInfo>(
    ctx: AdlsTableConfigContext<'_, T>,
) -> Result<TableConfig, TableConfigError> {
    let (sas, expiration) = get_or_mint_sas(
        ctx.cache_key,
        ctx.sas_mint,
        ctx.credential,
        &ctx.stc_request,
    )
    .await?;

    let mut creds = TableProperties::default();
    let expiration_ms = expiration.unix_timestamp().saturating_mul(1000);
    creds.insert(&creds::ExpirationTimeMs(expiration_ms));
    creds.insert(&custom::CustomConfig {
        key: ctx.sas_property_key,
        value: sas,
    });
    creds.insert(&adls::RefreshClientCredentialsEndpoint(
        ctx.request_metadata
            .refresh_client_credentials_endpoint_for_table(
                ctx.tabular_info.warehouse_id(),
                ctx.tabular_info.tabular_ident(),
            ),
    ));

    if should_emit_sas_expires_at_key(ctx.request_metadata) {
        creds.insert(&custom::CustomConfig {
            key: ctx.sas_expires_at_property_key,
            value: expiration_ms.to_string(),
        });
    } else {
        tracing::debug!(
            "Skipping `adls.sas-token-expires-at-ms` property for PyIceberg ≤ 0.10.0 due to known parsing issue."
        );
    }

    for (key, value) in ctx.extra_config {
        creds.insert(&custom::CustomConfig { key, value });
    }

    Ok(TableConfig {
        // Back-compat: clients still expect creds duplicated in config.
        config: creds.clone(),
        creds,
    })
}

/// Build an `AdlsStorage` client from an ADLS-style profile + credential.
///
/// Both ADLS profile types share this body verbatim — they only differ in how
/// they build their `AzureSettings`. The helper takes the settings + credential
/// and produces the storage client.
pub(super) async fn adls_lakekeeper_io(
    settings: AzureSettings,
    credential: &AzCredential,
) -> Result<AdlsStorage, CredentialsError> {
    let azure_auth = AzureAuth::try_from(credential.clone())?;
    settings
        .get_storage_client(&azure_auth)
        .await
        .map_err(Into::into)
}

/// Build an `AdlsStorage` client from vended-credentials properties.
///
/// Reads the SAS token (under the profile-specific account/endpoint key) from
/// the iceberg-format `TableProperties` previously produced by
/// `generate_table_config`, and constructs an `AdlsStorage` against the
/// provided Azure settings. Both ADLS profile types call this via their own
/// thin wrapper method.
pub(super) async fn lakekeeper_io_from_vended_adls_table_config(
    settings: AzureSettings,
    sas_property_key: &str,
    config: &TableProperties,
) -> Result<AdlsStorage, CredentialsError> {
    let sas_token = config.get_custom_prop(sas_property_key).ok_or_else(|| {
        CredentialsError::ShortTermCredential {
            reason: format!(
                "ADLS vended credentials are missing SAS token at key '{sas_property_key}'."
            ),
            source: None,
        }
    })?;
    let auth = AzureAuth::Sas(AzureSasAuth { sas_token });
    settings.get_storage_client(&auth).await.map_err(Into::into)
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::service::storage::{
        GenericAdlsProfile, StorageProfile,
        az::DEFAULT_AUTHORITY_HOST,
        storage_layout::{NamespaceNameContext, NamespacePath, TabularNameContext},
    };

    #[test]
    fn test_reduce_scheme_string() {
        let path = "abfss://filesystem@dfs.windows.net/path/_test";
        assert_eq!(reduce_scheme_string(path).unwrap(), "/path/_test");

        let wasbs_path = "wasbs://filesystem@account.windows.net/path/to/data";
        assert_eq!(reduce_scheme_string(wasbs_path).unwrap(), "/path/to/data");

        // Non-ADLS scheme must error rather than silently pass through.
        let non_matching = "http://example.com/path";
        assert!(reduce_scheme_string(non_matching).is_err());
    }

    #[test]
    fn test_effective_ttl_floors_at_min() {
        assert_eq!(
            effective_ttl_seconds(None),
            SAS_TOKEN_DEFAULT_VALIDITY_SECONDS
        );
        assert_eq!(
            effective_ttl_seconds(Some(0)),
            MIN_SAS_TOKEN_EFFECTIVE_TTL_SECONDS
        );
        assert_eq!(
            effective_ttl_seconds(Some(30)),
            MIN_SAS_TOKEN_EFFECTIVE_TTL_SECONDS
        );
        assert_eq!(effective_ttl_seconds(Some(120)), 120);
        assert_eq!(effective_ttl_seconds(Some(3600)), 3600);
        assert_eq!(effective_ttl_seconds(Some(86_400)), 86_400);
    }

    #[test]
    fn test_sas_validity_window_wall_clock() {
        // Effective TTL = 3600s → wall-clock validity from "now" = 3540s
        // (signed_start is SAS_TOKEN_START_BACKSHIFT_SECONDS in the past,
        // signed_expiry = signed_start + 3600).
        let (start, end) = sas_validity_window(3600);
        let now = OffsetDateTime::now_utc();
        let from_start = (now - start).whole_seconds();
        let backshift = SAS_TOKEN_START_BACKSHIFT_SECONDS;
        assert!(
            (backshift - 1..=backshift + 1).contains(&from_start),
            "start ≈ now - {backshift}s, got {from_start}"
        );
        let remaining = (end - now).whole_seconds();
        let expected = 3600 - backshift;
        assert!(
            (expected - 1..=expected + 1).contains(&remaining),
            "wall-clock validity ≈ {expected}s, got {remaining}"
        );
    }

    #[test]
    fn test_sas_validity_window_at_floor() {
        // Effective TTL at the floor → wall-clock validity =
        // MIN_SAS_TOKEN_WALL_CLOCK_VALIDITY_SECONDS by construction.
        let (_start, end) = sas_validity_window(MIN_SAS_TOKEN_EFFECTIVE_TTL_SECONDS);
        let now = OffsetDateTime::now_utc();
        let remaining = (end - now).whole_seconds();
        let target = MIN_SAS_TOKEN_WALL_CLOCK_VALIDITY_SECONDS;
        assert!(
            (target - 1..=target + 1).contains(&remaining),
            "wall-clock validity at floor ≈ {target}s, got {remaining}"
        );
    }

    /// Read the cache-window remaining-seconds as `i64` for direct comparison
    /// against the surrounding `i64` constants. The values involved are
    /// always small (≤ [`MAX_ONELAKE_SAS_TOKEN_VALIDITY_SECONDS`] = 3600),
    /// so the conversion never wraps in practice.
    fn cache_remaining_secs(until: Instant) -> i64 {
        i64::try_from(until.duration_since(Instant::now()).as_secs())
            .expect("cache remaining seconds fit in i64")
    }

    #[test]
    fn test_cache_valid_until_subtracts_backshift() {
        // Cache window = effective_ttl - SAS_TOKEN_START_BACKSHIFT_SECONDS.
        let until = cache_valid_until(3600).unwrap();
        let remaining = cache_remaining_secs(until);
        let expected = 3600 - SAS_TOKEN_START_BACKSHIFT_SECONDS;
        assert!(
            (expected - 1..=expected + 1).contains(&remaining),
            "got {remaining}"
        );
    }

    #[test]
    fn test_cache_valid_until_floors_at_min() {
        // At the effective-TTL floor, cache ≈
        // MIN_SAS_TOKEN_WALL_CLOCK_VALIDITY_SECONDS — well above
        // MIN_CACHE_VALID_FOR_SECONDS.
        let until = cache_valid_until(MIN_SAS_TOKEN_EFFECTIVE_TTL_SECONDS).unwrap();
        let remaining = cache_remaining_secs(until);
        let target = MIN_SAS_TOKEN_WALL_CLOCK_VALIDITY_SECONDS;
        assert!((target - 1..=target + 1).contains(&remaining));

        // Defensive: if an effective TTL ever fell *below* the backshift, the
        // floor protects us (`effective_ttl_seconds()` won't actually return
        // such a value, but the helper must still be safe).
        let until = cache_valid_until(SAS_TOKEN_START_BACKSHIFT_SECONDS - 30).unwrap();
        let remaining = cache_remaining_secs(until);
        assert!(
            remaining >= MIN_CACHE_VALID_FOR_SECONDS - 1,
            "expected ≥ MIN_CACHE_VALID_FOR_SECONDS ({MIN_CACHE_VALID_FOR_SECONDS}), got {remaining}"
        );
    }

    #[test]
    fn test_validate_sas_token_validity_seconds_zero_rejected() {
        let err =
            validate_sas_token_validity_seconds(Some(0), MAX_ONELAKE_SAS_TOKEN_VALIDITY_SECONDS)
                .unwrap_err();
        assert!(format!("{err:?}").contains("greater than 0"), "{err:?}");
    }

    #[test]
    fn test_validate_sas_token_validity_seconds_above_max_rejected() {
        let err =
            validate_sas_token_validity_seconds(Some(3601), MAX_ONELAKE_SAS_TOKEN_VALIDITY_SECONDS)
                .unwrap_err();
        assert!(format!("{err:?}").contains("3600"), "{err:?}");
    }

    #[test]
    fn test_validate_sas_token_validity_seconds_none_ok() {
        validate_sas_token_validity_seconds(None, MAX_ONELAKE_SAS_TOKEN_VALIDITY_SECONDS).unwrap();
    }

    #[test]
    fn test_validate_sas_token_validity_seconds_at_max_ok() {
        validate_sas_token_validity_seconds(Some(3600), MAX_ONELAKE_SAS_TOKEN_VALIDITY_SECONDS)
            .unwrap();
    }

    #[test]
    #[tracing_test::traced_test]
    fn test_validate_sas_token_validity_seconds_below_threshold_warns() {
        validate_sas_token_validity_seconds(Some(30), MAX_ONELAKE_SAS_TOKEN_VALIDITY_SECONDS)
            .unwrap();
        let expected = format!(
            "Token lifetime less than {SAS_TOKEN_WARN_THRESHOLD_SECONDS} seconds (provided value: 30)",
        );
        assert!(logs_contain(&expected), "expected substring: {expected}");
    }

    /// Regression test for the `OneLake` SAS canonical-resource bug that
    /// produced `401 Authentication Failed with Access token validation failed`
    /// on regional and private-link `OneLake` warehouses. The `OneLake` SAS
    /// canonical resource is always `/blob/onelake/<workspace>/...`, never the
    /// regional/private-link DNS label — see
    /// <https://learn.microsoft.com/en-us/fabric/onelake/how-to-create-a-onelake-shared-access-signature>.
    #[test]
    fn test_canonical_resource_for_onelake_regional_uses_global_onelake_account() {
        use std::str::FromStr;

        use lakekeeper_io::Location;

        use crate::{
            WarehouseId,
            service::{
                TableId, TabularId,
                storage::{ShortTermCredentialsRequest, StoragePermissions},
            },
        };

        let stc = ShortTermCredentialsRequest {
            table_location: Location::from_str(
                "abfss://c5e8a1f3-7b2d-4e8a-9f1c-3b6d8e5a2f47@centralus-onelake.dfs.fabric.microsoft.com/lh/Files/test/",
            )
            .unwrap(),
            storage_permissions: StoragePermissions::ReadWrite,
            warehouse_id: WarehouseId::new_random(),
            tabular_id: TabularId::Table(TableId::new_random()),
        };
        let (canonical, depth) =
            canonical_resource("onelake", "c5e8a1f3-7b2d-4e8a-9f1c-3b6d8e5a2f47", &stc).unwrap();
        assert_eq!(
            canonical,
            "/blob/onelake/c5e8a1f3-7b2d-4e8a-9f1c-3b6d8e5a2f47/lh/Files/test"
        );
        // Depth = number of segments in the rootless path `/lh/Files/test` → 3.
        assert_eq!(depth, 3);
    }

    #[test]
    fn test_canonical_resource_for_onelake_private_link_uses_global_onelake_account() {
        use std::str::FromStr;

        use lakekeeper_io::Location;

        use crate::{
            WarehouseId,
            service::{
                TableId, TabularId,
                storage::{ShortTermCredentialsRequest, StoragePermissions},
            },
        };

        let stc = ShortTermCredentialsRequest {
            table_location: Location::from_str(
                "abfss://c5e8a1f37b2d4e8a9f1c3b6d8e5a2f47@c5e8a1f37b2d4e8a9f1c3b6d8e5a2f47.zc5.dfs.fabric.microsoft.com/lh/Files/test/",
            )
            .unwrap(),
            storage_permissions: StoragePermissions::ReadWrite,
            warehouse_id: WarehouseId::new_random(),
            tabular_id: TabularId::Table(TableId::new_random()),
        };
        let (canonical, _depth) =
            canonical_resource("onelake", "c5e8a1f37b2d4e8a9f1c3b6d8e5a2f47", &stc).unwrap();
        assert!(
            canonical.starts_with("/blob/onelake/c5e8a1f37b2d4e8a9f1c3b6d8e5a2f47/"),
            "canonical = {canonical}",
        );
    }

    pub(crate) mod azure_integration_tests {
        use crate::{
            api::RequestMetadata,
            service::storage::{
                AzCredential, GenericAdlsProfile, StorageCredential, StorageProfile,
            },
        };

        pub(crate) fn azure_profile() -> GenericAdlsProfile {
            let account_name = std::env::var("LAKEKEEPER_TEST__AZURE_STORAGE_ACCOUNT_NAME")
                .expect("LAKEKEEPER_TEST__AZURE_STORAGE_ACCOUNT_NAME to be set");
            let filesystem = std::env::var("LAKEKEEPER_TEST__AZURE_STORAGE_FILESYSTEM")
                .expect("LAKEKEEPER_TEST__AZURE_STORAGE_FILESYSTEM to be set");

            let key_prefix = format!("test-{}", uuid::Uuid::now_v7());
            GenericAdlsProfile {
                filesystem,
                key_prefix: Some(key_prefix.clone()),
                account_name,
                authority_host: None,
                host: None,
                sas_token_validity_seconds: None,
                allow_alternative_protocols: false,
                sas_enabled: true,
                storage_layout: None,
            }
        }

        pub(crate) fn client_creds() -> AzCredential {
            let client_id = std::env::var("LAKEKEEPER_TEST__AZURE_CLIENT_ID")
                .expect("LAKEKEEPER_TEST__AZURE_CLIENT_ID to be set");
            let client_secret = std::env::var("LAKEKEEPER_TEST__AZURE_CLIENT_SECRET")
                .expect("LAKEKEEPER_TEST__AZURE_CLIENT_SECRET to be set");
            let tenant_id = std::env::var("LAKEKEEPER_TEST__AZURE_TENANT_ID")
                .expect("LAKEKEEPER_TEST__AZURE_TENANT_ID to be set");

            AzCredential::ClientCredentials {
                client_id,
                client_secret,
                tenant_id,
            }
        }

        pub(crate) fn shared_key() -> AzCredential {
            let key = std::env::var("LAKEKEEPER_TEST__AZURE_STORAGE_SHARED_KEY")
                .expect("LAKEKEEPER_TEST__AZURE_STORAGE_SHARED_KEY to be set");
            AzCredential::SharedAccessKey { key }
        }

        #[tokio::test]
        async fn test_can_validate_adls() {
            for (cred, typ) in [
                (client_creds(), "client-creds"),
                (shared_key(), "shared-key"),
            ] {
                let prof = azure_profile();
                let mut prof: StorageProfile = prof.into();
                prof.normalize(Some(&cred.clone().into()))
                    .expect("failed to validate profile");
                let cred: StorageCredential = cred.into();
                Box::pin(prof.validate_access(
                    Some(&cred),
                    None,
                    &RequestMetadata::new_unauthenticated(),
                ))
                .await
                .unwrap_or_else(|e| panic!("Failed to validate '{typ}' due to '{e:?}'"));
            }
        }

        mod azure_system_credentials_integration_tests {
            use super::*;

            #[tokio::test]
            async fn test_system_identity_can_validate() {
                let prof = azure_profile();
                let mut prof: StorageProfile = prof.into();
                prof.normalize(None).expect("failed to validate profile");
                let cred = AzCredential::AzureSystemIdentity {};
                let cred: StorageCredential = cred.into();
                Box::pin(prof.validate_access(
                    Some(&cred),
                    None,
                    &RequestMetadata::new_unauthenticated(),
                ))
                .await
                .unwrap_or_else(|e| panic!("Failed to validate system identity due to '{e:?}'"));
            }
        }
    }

    /// Live `OneLake` (Microsoft Fabric) integration tests.
    ///
    /// Default-ignored (`#[ignore]`) — opt in with `cargo test -- --ignored`
    /// or `cargo nextest run --run-ignored=all`. The nextest `default` and
    /// `ci_no_secrets` profiles also filter this module out by name; either
    /// gate alone is sufficient, both are kept for parallelism with the other
    /// secret-requiring integration test modules.
    ///
    /// # Required env vars
    /// - `LAKEKEEPER_TEST__ONELAKE_WORKSPACE_ID` — Fabric workspace UUID.
    /// - `LAKEKEEPER_TEST__ONELAKE_LAKEHOUSE_ID` — lakehouse UUID inside that workspace.
    /// - `LAKEKEEPER_TEST__ONELAKE_CLIENT_ID` — Entra app client ID with rights on the workspace.
    /// - `LAKEKEEPER_TEST__ONELAKE_CLIENT_SECRET` — client secret for that app.
    /// - `LAKEKEEPER_TEST__ONELAKE_TENANT_ID` — Entra tenant ID.
    ///
    /// # Mode-specific
    /// - `LAKEKEEPER_TEST__ONELAKE_REGION` — Azure region slug
    ///   (e.g. `centralus`, `westus`). Required by
    ///   `test_can_validate_onelake_regional`; ignored otherwise.
    pub(crate) mod onelake_integration_tests {
        use uuid::Uuid;

        use crate::{
            api::RequestMetadata,
            service::storage::{
                AzCredential, EndpointMode, OneLakeProfile, StorageCredential, StorageProfile,
                TopLevelFolder,
            },
        };

        pub(crate) fn onelake_profile() -> OneLakeProfile {
            let workspace_id = std::env::var("LAKEKEEPER_TEST__ONELAKE_WORKSPACE_ID")
                .expect("LAKEKEEPER_TEST__ONELAKE_WORKSPACE_ID to be set");
            let lakehouse_id = std::env::var("LAKEKEEPER_TEST__ONELAKE_LAKEHOUSE_ID")
                .expect("LAKEKEEPER_TEST__ONELAKE_LAKEHOUSE_ID to be set");
            let directory_rel_path = Some(format!("test-{}", uuid::Uuid::now_v7()));
            OneLakeProfile {
                workspace_id: Uuid::parse_str(&workspace_id)
                    .expect("LAKEKEEPER_TEST__ONELAKE_WORKSPACE_ID is not a valid UUID"),
                lakehouse_id: Uuid::parse_str(&lakehouse_id)
                    .expect("LAKEKEEPER_TEST__ONELAKE_LAKEHOUSE_ID is not a valid UUID"),
                directory_rel_path,
                top_level_folder: TopLevelFolder::Files,
                endpoint_mode: EndpointMode::Default,
                sas_token_validity_seconds: None,
                sas_enabled: true,
                authority_host: None,
                storage_layout: None,
            }
        }

        pub(crate) fn client_creds() -> AzCredential {
            let client_id = std::env::var("LAKEKEEPER_TEST__ONELAKE_CLIENT_ID")
                .expect("LAKEKEEPER_TEST__ONELAKE_CLIENT_ID to be set");
            let client_secret = std::env::var("LAKEKEEPER_TEST__ONELAKE_CLIENT_SECRET")
                .expect("LAKEKEEPER_TEST__ONELAKE_CLIENT_SECRET to be set");
            let tenant_id = std::env::var("LAKEKEEPER_TEST__ONELAKE_TENANT_ID")
                .expect("LAKEKEEPER_TEST__ONELAKE_TENANT_ID to be set");

            AzCredential::ClientCredentials {
                client_id,
                client_secret,
                tenant_id,
            }
        }

        #[tokio::test]
        #[ignore = "live OneLake test; opt in with --ignored (see module docs)"]
        async fn test_can_validate_onelake() {
            let prof = onelake_profile();
            let cred = client_creds();
            let mut prof: StorageProfile = prof.into();
            prof.normalize(Some(&cred.clone().into()))
                .expect("failed to validate profile");
            let cred: StorageCredential = cred.into();
            Box::pin(prof.validate_access(
                Some(&cred),
                None,
                &RequestMetadata::new_unauthenticated(),
            ))
            .await
            .unwrap_or_else(|e| panic!("Failed to validate OneLake profile due to '{e:?}'"));
        }

        /// End-to-end check that regional `OneLake` warehouses validate. This
        /// is the live counterpart to the unit-level `canonical_resource` test:
        /// it actually mints a user-delegation SAS against
        /// `<region>-onelake.dfs.fabric.microsoft.com` and exercises the
        /// vended-credential read/write/delete path that used to 401 when the
        /// canonical resource was signed against the regional account.
        #[tokio::test]
        #[ignore = "live OneLake test; opt in with --ignored (see module docs). \
                    Also requires LAKEKEEPER_TEST__ONELAKE_REGION."]
        async fn test_can_validate_onelake_regional() {
            let region = std::env::var("LAKEKEEPER_TEST__ONELAKE_REGION")
                .expect("LAKEKEEPER_TEST__ONELAKE_REGION to be set");
            let mut prof = onelake_profile();
            prof.endpoint_mode = EndpointMode::Regional { region };
            let cred = client_creds();
            let mut prof: StorageProfile = prof.into();
            prof.normalize(Some(&cred.clone().into()))
                .expect("failed to validate profile");
            let cred: StorageCredential = cred.into();
            Box::pin(prof.validate_access(
                Some(&cred),
                None,
                &RequestMetadata::new_unauthenticated(),
            ))
            .await
            .unwrap_or_else(|e| {
                panic!("Failed to validate regional OneLake profile due to '{e:?}'")
            });
        }
    }

    #[test]
    fn test_default_authority() {
        assert_eq!(
            DEFAULT_AUTHORITY_HOST.as_str(),
            "https://login.microsoftonline.com/"
        );
    }

    #[test]
    fn test_default_adls_locations() {
        let profile = GenericAdlsProfile {
            filesystem: "filesystem".to_string(),
            key_prefix: Some("test_prefix".to_string()),
            account_name: "account".to_string(),
            authority_host: None,
            host: None,
            sas_token_validity_seconds: None,
            allow_alternative_protocols: false,
            sas_enabled: true,
            storage_layout: None,
        };

        let sp: StorageProfile = profile.clone().into();

        let namespace_uuid = uuid::Uuid::now_v7();
        let tabular_uuid = uuid::Uuid::now_v7();
        let namespace_path = NamespacePath::new(vec![NamespaceNameContext {
            name: "test_ns".to_string(),
            uuid: namespace_uuid,
        }]);
        let tabular_name_context = TabularNameContext {
            name: "test_tabular".to_string(),
            uuid: tabular_uuid,
        };
        let namespace_location = sp.default_namespace_location(&namespace_path).unwrap();

        let location = sp.default_tabular_location(&namespace_location, &tabular_name_context);
        assert_eq!(
            location.to_string(),
            format!(
                "abfss://filesystem@account.dfs.core.windows.net/test_prefix/{namespace_uuid}/{tabular_uuid}"
            )
        );

        let mut profile = profile.clone();
        profile.key_prefix = None;
        profile.host = Some("blob.com".to_string());
        let sp: StorageProfile = profile.into();

        let namespace_location = sp.default_namespace_location(&namespace_path).unwrap();
        let location = sp.default_tabular_location(&namespace_location, &tabular_name_context);
        assert_eq!(
            location.to_string(),
            format!("abfss://filesystem@account.blob.com/{namespace_uuid}/{tabular_uuid}")
        );
    }

    #[test]
    fn test_allow_alternative_protocols() {
        let profile = GenericAdlsProfile {
            filesystem: "filesystem".to_string(),
            key_prefix: Some("test_prefix".to_string()),
            account_name: "account".to_string(),
            authority_host: None,
            host: None,
            sas_token_validity_seconds: None,
            allow_alternative_protocols: true,
            sas_enabled: true,
            storage_layout: None,
        };

        assert!(
            profile.is_allowed_schema("abfss"),
            "abfss should be allowed"
        );
        assert!(
            profile.is_allowed_schema("wasbs"),
            "wasbs should be allowed with flag set"
        );

        let profile = GenericAdlsProfile {
            filesystem: "filesystem".to_string(),
            key_prefix: Some("test_prefix".to_string()),
            account_name: "account".to_string(),
            authority_host: None,
            host: None,
            sas_token_validity_seconds: None,
            allow_alternative_protocols: false,
            sas_enabled: true,
            storage_layout: None,
        };

        assert!(
            profile.is_allowed_schema("abfss"),
            "abfss should always be allowed"
        );
        assert!(
            !profile.is_allowed_schema("wasbs"),
            "wasbs should not be allowed with flag unset"
        );
    }
}

#[cfg(test)]
mod is_overlapping_location_tests {
    use super::*;

    fn create_profile(
        filesystem: &str,
        account_name: &str,
        host: Option<&str>,
        authority_host: Option<&str>,
        key_prefix: Option<&str>,
    ) -> GenericAdlsProfile {
        GenericAdlsProfile {
            filesystem: filesystem.to_string(),
            account_name: account_name.to_string(),
            host: host.map(ToString::to_string),
            authority_host: authority_host.map(|url| url.parse().unwrap()),
            key_prefix: key_prefix.map(ToString::to_string),
            sas_token_validity_seconds: None,
            allow_alternative_protocols: false,
            sas_enabled: true,
            storage_layout: None,
        }
    }

    #[test]
    fn test_non_overlapping_different_filesystem() {
        let profile1 = create_profile("filesystem1", "account", None, None, Some("prefix"));
        let profile2 = create_profile("filesystem2", "account", None, None, Some("prefix"));

        assert!(!profile1.is_overlapping_location(&profile2));
    }

    #[test]
    fn test_non_overlapping_different_account_name() {
        let profile1 = create_profile("filesystem", "account1", None, None, Some("prefix"));
        let profile2 = create_profile("filesystem", "account2", None, None, Some("prefix"));

        assert!(!profile1.is_overlapping_location(&profile2));
    }

    #[test]
    fn test_non_overlapping_different_host() {
        let profile1 = create_profile("filesystem", "account", Some("host1"), None, Some("prefix"));
        let profile2 = create_profile("filesystem", "account", Some("host2"), None, Some("prefix"));

        assert!(!profile1.is_overlapping_location(&profile2));
    }

    #[test]
    fn test_non_overlapping_different_authority_host() {
        let profile1 = create_profile(
            "filesystem",
            "account",
            None,
            Some("https://login1.example.com"),
            Some("prefix"),
        );
        let profile2 = create_profile(
            "filesystem",
            "account",
            None,
            Some("https://login2.example.com"),
            Some("prefix"),
        );

        assert!(!profile1.is_overlapping_location(&profile2));
    }

    #[test]
    fn test_overlapping_identical_key_prefix() {
        let profile1 = create_profile("filesystem", "account", None, None, Some("prefix"));
        let profile2 = create_profile("filesystem", "account", None, None, Some("prefix"));

        assert!(profile1.is_overlapping_location(&profile2));
    }

    #[test]
    fn test_overlapping_one_prefix_of_other() {
        let profile1 = create_profile("filesystem", "account", None, None, Some("prefix"));
        let profile2 = create_profile("filesystem", "account", None, None, Some("prefix/subpath"));

        assert!(profile1.is_overlapping_location(&profile2));
        assert!(profile2.is_overlapping_location(&profile1)); // Test symmetry
    }

    #[test]
    fn test_overlapping_no_key_prefix() {
        let profile1 = create_profile("filesystem", "account", None, None, None);
        let profile2 = create_profile("filesystem", "account", None, None, Some("prefix"));

        assert!(profile1.is_overlapping_location(&profile2));
        assert!(profile2.is_overlapping_location(&profile1)); // Test symmetry
    }

    #[test]
    fn test_non_overlapping_unrelated_key_prefixes() {
        let profile1 = create_profile("filesystem", "account", None, None, Some("prefix1"));
        let profile2 = create_profile("filesystem", "account", None, None, Some("prefix2"));

        // These don't overlap as neither is a prefix of the other
        assert!(!profile1.is_overlapping_location(&profile2));
    }

    #[test]
    fn test_overlapping_both_no_key_prefix() {
        let profile1 = create_profile("filesystem", "account", None, None, None);
        let profile2 = create_profile("filesystem", "account", None, None, None);

        assert!(profile1.is_overlapping_location(&profile2));
    }

    #[test]
    fn test_complex_key_prefix_scenarios() {
        // Prefix with similar characters but not a prefix relationship
        let profile1 = create_profile("filesystem", "account", None, None, Some("prefix"));
        let profile2 = create_profile("filesystem", "account", None, None, Some("prefix-extra"));

        // Not overlapping since "prefix" is not a prefix of "prefix-extra"
        assert!(!profile1.is_overlapping_location(&profile2));

        // Actual prefix case
        let profile3 = create_profile("filesystem", "account", None, None, Some("prefix"));
        let profile4 = create_profile("filesystem", "account", None, None, Some("prefix/sub"));

        assert!(profile3.is_overlapping_location(&profile4));
    }
}
