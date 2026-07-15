use std::str::FromStr;

use azure_storage::CloudLocation;
use iceberg_ext::configs::table::TableProperties;
use lakekeeper_io::{
    InvalidLocationError, Location,
    adls::{AdlsStorage, AzureSettings},
};
use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

use super::{
    AdlsTableConfigContext, AzCredential, MAX_ONELAKE_SAS_TOKEN_VALIDITY_SECONDS, SasMintContext,
    adls_catalog_config, adls_lakekeeper_io, generate_adls_table_config,
    iceberg_expiration_property_key, iceberg_sas_property_key, key_prefix_overlaps,
    lakekeeper_io_from_vended_adls_table_config, validate_sas_token_validity_seconds,
};
use crate::{
    WarehouseId,
    api::{CatalogConfig, RequestMetadata, Result, iceberg::v1::tables::DataAccessMode},
    service::{
        BasicTabularInfo,
        storage::{
            ShortTermCredentialsRequest, TableConfig,
            cache::STCCacheKey,
            error::{
                CredentialsError, InvalidProfileError, TableConfigError, UpdateError,
                ValidationError,
            },
            storage_layout::StorageLayout,
        },
    },
};

/// Top-level managed folder within a Fabric lakehouse.
///
/// Fabric reserves `Files/` and `Tables/` as managed folders directly under
/// each lakehouse item. `Files/` is the default for Lakekeeper-managed Iceberg
/// tables; `Tables/` is supported for completeness but writing Iceberg metadata
/// there conflicts with Fabric's automatic Delta/Iceberg virtualization.
#[derive(Debug, Hash, Eq, Copy, Clone, PartialEq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
pub enum TopLevelFolder {
    /// Iceberg-managed data area. Recommended.
    #[default]
    Files,
    /// Fabric-managed table area. Conflicts with automatic table virtualization
    /// when used to store Iceberg metadata — only choose this if you know what
    /// you're doing.
    Tables,
}

impl TopLevelFolder {
    fn as_path_segment(self) -> &'static str {
        match self {
            TopLevelFolder::Files => "Files",
            TopLevelFolder::Tables => "Tables",
        }
    }
}

/// How Lakekeeper connects to the `OneLake` DFS endpoint.
///
/// Fabric supports two kinds of Azure Private Link configurations, and only
/// one of them maps to a dedicated variant here:
///
/// - **Tenant-level private link**: traffic to the global host
///   `onelake.dfs.fabric.microsoft.com` is routed privately via DNS that
///   points the global FQDN at a tenant-PE NIC. From Lakekeeper's
///   perspective this is indistinguishable from public traffic — use
///   `Default`. (Same shape as a private endpoint in front of a regular
///   ADLS Gen2 storage account.)
/// - **Workspace-level private link**: each workspace gets its own
///   `<wsId>.z<xy>.dfs.fabric.microsoft.com` FQDN routed via a
///   workspace-scoped PE. Lakekeeper needs to build that FQDN — use
///   [`WorkspacePrivateLink`].
#[derive(Debug, Hash, Eq, Clone, PartialEq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum EndpointMode {
    /// Use the global `OneLake` endpoint `onelake.dfs.fabric.microsoft.com`. Default.
    ///
    /// Also the correct choice for tenant-level private link — tenant PE
    /// only changes DNS resolution, not the URL Lakekeeper constructs.
    #[default]
    Default,
    /// Use a region-pinned endpoint `<region>-onelake.dfs.fabric.microsoft.com`.
    /// Use this when data residency requires the request to stay within a
    /// specific Azure region.
    Regional {
        /// Azure region slug, e.g. `westus`, `centralus`, `northeurope`.
        /// Trimmed and lowercased at validation time, then pattern-checked to
        /// match the Azure region-slug shape (lowercase ASCII letter followed
        /// by lowercase letters or digits) so a stray `.` or `-` can't smuggle
        /// an extra host segment into the resolved DFS host. An unknown but
        /// well-shaped slug still surfaces as a DNS-resolution failure at
        /// access time. See `normalize_endpoint_mode` for the exact rule.
        region: String,
    },
    /// Use a workspace-scoped private-link endpoint
    /// `<workspaceId>.z<xy>.dfs.fabric.microsoft.com`. The host is computed
    /// from the workspace ID at runtime; users only opt in via this variant.
    ///
    /// For *tenant*-level private link, stay on [`Default`] — the global
    /// onelake FQDN is what gets routed through a tenant PE.
    #[serde(rename = "workspace-private-link")]
    WorkspacePrivateLink,
}

/// Storage profile for a Microsoft Fabric / `OneLake` lakehouse.
///
/// Convenience wrapper around the ADLS Gen2 surface that derives the
/// account name (`onelake`), container (workspace ID), key prefix
/// (`<lakehouse>/Files/<sub>`), and endpoint host from the supplied workspace
/// and lakehouse UUIDs and endpoint mode.
#[derive(Debug, Hash, Eq, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct OneLakeProfile {
    /// UUID of the Fabric workspace this warehouse lives in.
    pub workspace_id: Uuid,
    /// UUID of the lakehouse within the workspace.
    pub lakehouse_id: Uuid,
    /// Subpath beneath `<top-level-folder>/` inside the lakehouse — the root
    /// directory under which Lakekeeper writes all warehouse data.
    pub directory_rel_path: Option<String>,
    /// Top-level managed folder. Defaults to `Files`.
    #[serde(default)]
    pub top_level_folder: TopLevelFolder,
    /// Endpoint connection mode. Defaults to the global endpoint.
    #[serde(default)]
    pub endpoint_mode: EndpointMode,
    /// SAS-token validity in seconds. Default: 3600. Max: 3600 (`OneLake` cap).
    pub sas_token_validity_seconds: Option<u64>,
    /// Enable SAS-token generation. Defaults to true.
    #[serde(default = "super::default_true")]
    pub sas_enabled: bool,
    /// The authority host to use for authentication.
    /// Default: `https://login.microsoftonline.com`.
    pub authority_host: Option<Url>,
    /// Storage layout for namespace and tabular paths.
    #[serde(default)]
    pub storage_layout: Option<StorageLayout>,
}

impl OneLakeProfile {
    /// Validate the `OneLake` storage profile.
    ///
    /// # Errors
    /// - Fails if the SAS-token TTL is 0 or above the 1-hour `OneLake` cap.
    /// - Fails if `directory_rel_path` is empty or contains `..`.
    /// - Fails if `endpoint_mode = Regional { region }` has an empty `region`.
    /// - Fails if the supplied credential is `SharedAccessKey` (unsupported by
    ///   `OneLake`, which has no storage-account key).
    /// - Fails if `storage_layout` is set to anything other than
    ///   [`StorageLayout::Default`]. `OneLake` silently percent-decodes `%XX`
    ///   sequences in blob paths, so layouts that embed `{name}` segments
    ///   (`tabular-only`, `full-hierarchy`) would alias paths after server-side
    ///   decoding; only the default `{uuid}`-only layout is currently supported.
    pub(crate) fn normalize(
        &mut self,
        credential: Option<&AzCredential>,
    ) -> Result<(), ValidationError> {
        if let Some(cred) = credential
            && matches!(cred, AzCredential::SharedAccessKey { .. })
        {
            return Err(InvalidProfileError {
                source: None,
                reason: "`OneLake` does not support shared-access-key credentials. Use client-credentials or system identity.".to_string(),
                entity: "credential".to_string(),
            }
            .into());
        }

        if let Some(layout) = &self.storage_layout
            && !matches!(layout, StorageLayout::Default)
        {
            return Err(InvalidProfileError {
                source: None,
                reason: "`OneLake` currently only supports the `default` storage layout. \
                         OneLake silently percent-decodes `%XX` sequences in blob paths, \
                         so `{name}` templates in `tabular-only` / `full-hierarchy` layouts \
                         would alias to the same blob after server-side decoding. \
                         Omit `storage-layout` or set it to `{\"type\": \"default\"}`."
                    .to_string(),
                entity: "storage-layout".to_string(),
            }
            .into());
        }

        validate_sas_token_validity_seconds(
            self.sas_token_validity_seconds,
            MAX_ONELAKE_SAS_TOKEN_VALIDITY_SECONDS,
        )?;
        self.normalize_directory_rel_path()?;
        self.normalize_endpoint_mode()?;
        Ok(())
    }

    fn normalize_directory_rel_path(&mut self) -> Result<(), ValidationError> {
        if let Some(mut directory_rel_path) = self.directory_rel_path.clone() {
            directory_rel_path = directory_rel_path.trim_matches('/').to_string();
            if directory_rel_path.is_empty() {
                return Err(InvalidProfileError {
                    source: None,
                    reason: "`directory-rel-path` must not be empty if specified.".to_string(),
                    entity: "directory-rel-path".to_string(),
                }
                .into());
            }
            if directory_rel_path.split('/').any(|seg| seg == "..") {
                return Err(InvalidProfileError {
                    source: None,
                    reason: "`directory-rel-path` must not contain `..` segments.".to_string(),
                    entity: "directory-rel-path".to_string(),
                }
                .into());
            }
            // Match the GenericAdlsProfile key-prefix budget so we leave room for
            // table-level path segments under it.
            if directory_rel_path.len() > 512 {
                return Err(InvalidProfileError {
                    source: None,
                    reason: "`directory-rel-path` must be less than 512 characters.".to_string(),
                    entity: "directory-rel-path".to_string(),
                }
                .into());
            }
            self.directory_rel_path = Some(directory_rel_path);
        }
        Ok(())
    }

    fn normalize_endpoint_mode(&mut self) -> Result<(), ValidationError> {
        if let EndpointMode::Regional { region } = &mut self.endpoint_mode {
            // DNS is case-insensitive, but Azure region slugs are conventionally
            // lowercase. Lowercasing here keeps the stored profile canonical.
            *region = region.trim().to_lowercase();
            if region.is_empty() {
                return Err(InvalidProfileError {
                    source: None,
                    reason: "Regional endpoint requires a non-empty `region`.".to_string(),
                    entity: "endpoint-mode.region".to_string(),
                }
                .into());
            }
            // Tightened to Azure-region-slug shape: start with a lowercase
            // letter, then lowercase letters/digits. Rejects anything that
            // could smuggle an extra host segment via `.` or `-`, which would
            // otherwise let a user point `<region>-onelake.dfs.fabric...` at
            // an arbitrary DNS subtree.
            let mut chars = region.chars();
            let leads_with_letter = chars.next().is_some_and(|c| c.is_ascii_lowercase());
            let body_alnum = chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit());
            if !leads_with_letter || !body_alnum {
                return Err(InvalidProfileError {
                    source: None,
                    reason: "Regional endpoint `region` must be an Azure region slug \
                         (lowercase ASCII letter followed by lowercase letters or digits)."
                        .to_string(),
                    entity: "endpoint-mode.region".to_string(),
                }
                .into());
            }
        }
        Ok(())
    }

    /// Update the storage profile with another profile.
    ///
    /// Mutable on update: `sas_token_validity_seconds`, `sas_enabled`,
    /// `storage_layout` (carried over from `self` if `other` doesn't set it),
    /// `authority_host` (doesn't affect data location, just AAD routing).
    ///
    /// Immutable: `workspace_id`, `lakehouse_id`, `top_level_folder`,
    /// `directory_rel_path`, `endpoint_mode` — changing any of these would
    /// change the abfss URL host or container path and orphan every table
    /// previously written to the warehouse.
    ///
    /// # Errors
    /// Fails if any immutable field differs between `self` and `other`.
    pub fn update_with(self, mut other: Self) -> Result<Self, UpdateError> {
        if self.workspace_id != other.workspace_id {
            return Err(UpdateError::ImmutableField("workspace_id".to_string()));
        }
        if self.lakehouse_id != other.lakehouse_id {
            return Err(UpdateError::ImmutableField("lakehouse_id".to_string()));
        }
        if self.top_level_folder != other.top_level_folder {
            return Err(UpdateError::ImmutableField("top_level_folder".to_string()));
        }
        if self.directory_rel_path != other.directory_rel_path {
            return Err(UpdateError::ImmutableField(
                "directory_rel_path".to_string(),
            ));
        }
        if self.endpoint_mode != other.endpoint_mode {
            return Err(UpdateError::ImmutableField("endpoint_mode".to_string()));
        }

        if other.storage_layout.is_none() {
            other.storage_layout = self.storage_layout;
        }
        Ok(other)
    }

    #[allow(clippy::unused_self)]
    #[must_use]
    pub fn generate_catalog_config(&self, _: WarehouseId) -> CatalogConfig {
        adls_catalog_config()
    }

    /// Base Location for this storage profile — `abfss://<container>@<host>/<key_prefix>/`.
    ///
    /// # Errors
    /// Can fail for un-normalized profiles (e.g. empty `directory_rel_path`).
    pub fn base_location(&self) -> Result<Location, InvalidLocationError> {
        let location = format!(
            "abfss://{filesystem}@{host}/{key_prefix}/",
            filesystem = self.filesystem(),
            host = self.dfs_host(),
            key_prefix = self.key_prefix(),
        );
        Location::from_str(&location).map_err(|e| {
            InvalidLocationError::new(
                location,
                format!("Failed to create base location for storage profile: {e}"),
            )
        })
    }

    /// Only `abfss://` is allowed for `OneLake` locations; `wasbs` is rejected.
    #[must_use]
    #[allow(clippy::unused_self)]
    pub fn is_allowed_schema(&self, schema: &str) -> bool {
        schema == "abfss"
    }

    /// The "account" portion of the abfss URL — the first DNS label of the
    /// host. This is what an Iceberg client extracts from the table location
    /// to find the matching `adls.sas-token.<account>.<host_suffix>` property,
    /// so it must agree with what the client sees in the URL.
    ///
    /// - `Default` → `onelake`
    /// - `Regional{region}` → `<region>-onelake`
    /// - `WorkspacePrivateLink` → un-dashed workspace UUID
    ///
    /// Distinct from [`Self::sas_account`], which is what we must sign SAS
    /// canonical resources against — the latter is always the literal
    /// `onelake`, regardless of which DNS host serves the request.
    fn host_account(&self) -> String {
        match &self.endpoint_mode {
            EndpointMode::Default => "onelake".to_string(),
            EndpointMode::Regional { region } => format!("{region}-onelake"),
            EndpointMode::WorkspacePrivateLink => self.workspace_id.simple().to_string(),
        }
    }

    /// The account name to embed in the SAS canonical resource.
    ///
    /// Per [Microsoft Learn](https://learn.microsoft.com/en-us/fabric/onelake/how-to-create-a-onelake-shared-access-signature),
    /// the SAS canonical resource for `OneLake` is always
    /// `/blob/onelake/<workspace>/...` — the *literal* string `onelake`,
    /// regardless of whether the request hits the global, a regional, or a
    /// workspace-private-link host. Using the regional or workspace-scoped
    /// label here would mismatch the server's verifier and produce
    /// `401 Access token validation failed`.
    #[allow(clippy::unused_self)]
    fn sas_account(&self) -> &'static str {
        "onelake"
    }

    /// The endpoint suffix — everything after the first DNS label of the host.
    ///
    /// - `Default` / `Regional` → `dfs.fabric.microsoft.com`
    /// - `WorkspacePrivateLink` → `z<xy>.dfs.fabric.microsoft.com` where `<xy>` is the
    ///   first two characters of the un-dashed workspace UUID
    fn endpoint_suffix(&self) -> String {
        match &self.endpoint_mode {
            EndpointMode::Default | EndpointMode::Regional { .. } => {
                "dfs.fabric.microsoft.com".to_string()
            }
            EndpointMode::WorkspacePrivateLink => {
                let wsid = self.workspace_id.simple().to_string();
                // `Uuid::simple` always emits 32 lowercase hex chars.
                let xy = &wsid[..2];
                format!("z{xy}.dfs.fabric.microsoft.com")
            }
        }
    }

    /// The full DFS host — `<account>.<endpoint_suffix>`.
    fn dfs_host(&self) -> String {
        format!("{}.{}", self.host_account(), self.endpoint_suffix())
    }

    /// The full Blob host — the DFS host with the `dfs.` label rewritten to
    /// `blob.`. Published to clients as `adls.account-host` so that pyiceberg
    /// / `adlfs.AzureBlobFileSystem` constructs requests against the `OneLake`
    /// blob surface (`*.blob.fabric.microsoft.com`) instead of defaulting to
    /// the regular Azure Storage suffix `<account>.blob.core.windows.net`.
    fn blob_host(&self) -> String {
        self.dfs_host().replacen(".dfs.", ".blob.", 1)
    }

    /// The container ("filesystem") portion of the abfss URL — the dashed
    /// workspace UUID, as expected by the `OneLake` REST API under
    /// `/<workspace>/<item>/...`.
    fn filesystem(&self) -> String {
        self.workspace_id.to_string()
    }

    /// The `key_prefix` portion of the abfss URL —
    /// `<lakehouse_id>/<top_level_folder>/<directory_rel_path>`.
    fn key_prefix(&self) -> String {
        if let Some(ref directory_rel_path) = self.directory_rel_path {
            format!(
                "{lakehouse}/{folder}/{path}",
                lakehouse = self.lakehouse_id,
                folder = self.top_level_folder.as_path_segment(),
                path = directory_rel_path,
            )
        } else {
            format!(
                "{lakehouse}/{folder}",
                lakehouse = self.lakehouse_id,
                folder = self.top_level_folder.as_path_segment()
            )
        }
    }

    fn cloud_location(&self) -> CloudLocation {
        CloudLocation::Custom {
            account: self.host_account(),
            uri: format!("https://{}", self.dfs_host()),
        }
    }

    /// Cloud location used for the Get-User-Delegation-Key call when minting
    /// SAS tokens. The workspace-FQDN private-link host rejects this call with
    /// `DeniedByPolicy`; the global `OneLake` host serves it. The resulting SAS
    /// is signed against the canonical resource `/blob/onelake/<workspace>/...`
    /// and remains valid for clients that subsequently hit the private-link host.
    fn sas_cloud_location(&self) -> CloudLocation {
        match &self.endpoint_mode {
            EndpointMode::WorkspacePrivateLink => CloudLocation::Custom {
                account: "onelake".to_string(),
                uri: "https://onelake.dfs.fabric.microsoft.com".to_string(),
            },
            EndpointMode::Default | EndpointMode::Regional { .. } => self.cloud_location(),
        }
    }

    #[must_use]
    pub(super) fn azure_settings(&self) -> AzureSettings {
        AzureSettings {
            authority_host: self.authority_host.clone(),
            cloud_location: self.cloud_location(),
        }
    }

    fn sas_azure_settings(&self) -> AzureSettings {
        AzureSettings {
            authority_host: self.authority_host.clone(),
            cloud_location: self.sas_cloud_location(),
        }
    }

    /// Get the Lakekeeper IO for this storage profile.
    ///
    /// # Errors
    /// - If system identity is requested but not enabled in the configuration.
    /// - If the client could not be initialized.
    pub async fn lakekeeper_io(
        &self,
        credential: &AzCredential,
    ) -> Result<AdlsStorage, CredentialsError> {
        adls_lakekeeper_io(self.azure_settings(), credential).await
    }

    /// Build an `AdlsStorage` client from previously-vended credentials.
    pub(in crate::service::storage) async fn lakekeeper_io_from_vended_table_config(
        &self,
        config: &TableProperties,
    ) -> Result<AdlsStorage, CredentialsError> {
        lakekeeper_io_from_vended_adls_table_config(
            self.azure_settings(),
            &self.iceberg_sas_property_key(),
            config,
        )
        .await
    }

    /// Generate the table configuration for `OneLake`.
    ///
    /// # Errors
    /// Fails if a SAS token cannot be generated, or if a `SharedAccessKey`
    /// credential reaches this path (defense-in-depth — `normalize` should
    /// have rejected it at warehouse creation time).
    pub async fn generate_table_config(
        &self,
        data_access: DataAccessMode,
        credential: &AzCredential,
        stc_request: ShortTermCredentialsRequest,
        tabular_info: &impl BasicTabularInfo,
        request_metadata: &RequestMetadata,
    ) -> Result<TableConfig, TableConfigError> {
        // Defense-in-depth: `normalize` rejects `SharedAccessKey` for OneLake,
        // but it only sees the credential when it's passed by the caller. A
        // warehouse whose credential has been swapped to `SharedAccessKey`
        // post-creation would otherwise reach this code and try to mint a
        // SAS that `OneLake` cannot honor.
        if matches!(credential, AzCredential::SharedAccessKey { .. }) {
            return Err(CredentialsError::Misconfiguration(
                "Fabric / OneLake does not support shared-access-key credentials.".to_string(),
            )
            .into());
        }

        if !data_access.provide_credentials() || !self.sas_enabled {
            tracing::debug!(
                "Not providing OneLake SAS credentials - provide_credentials: {}, sas_enabled: {}",
                data_access.provide_credentials(),
                self.sas_enabled
            );
            return Ok(TableConfig {
                creds: TableProperties::default(),
                config: TableProperties::default(),
                credentials_expiration_ms: None,
            });
        }

        let cache_key = STCCacheKey::new(stc_request.clone(), self.into(), Some(credential.into()));
        let settings = self.sas_azure_settings();
        let filesystem = self.filesystem();
        generate_adls_table_config(AdlsTableConfigContext {
            cache_key,
            sas_mint: SasMintContext {
                account_name: self.sas_account(),
                filesystem: &filesystem,
                user_ttl: self.sas_token_validity_seconds,
                settings: &settings,
            },
            credential,
            stc_request,
            sas_property_key: self.iceberg_sas_property_key(),
            sas_expires_at_property_key: self.iceberg_sas_expires_at_property_key(),
            tabular_info,
            request_metadata,
            extra_config: vec![("adls.account-host".to_string(), self.blob_host())],
        })
        .await
    }

    /// The iceberg property key under which the vended SAS token is published.
    ///
    /// Format follows the `adls.sas-token.<account>.<endpoint_suffix>`
    /// convention that Iceberg clients (`PyIceberg`, Iceberg-Java) use to look
    /// up the SAS based on the table location's URL — `<account>` is the
    /// first DNS label of the URL host and `<endpoint_suffix>` is the rest.
    #[must_use]
    pub(super) fn iceberg_sas_property_key(&self) -> String {
        iceberg_sas_property_key(&self.host_account(), &self.endpoint_suffix())
    }

    fn iceberg_sas_expires_at_property_key(&self) -> String {
        iceberg_expiration_property_key(&self.host_account(), &self.endpoint_suffix())
    }

    /// Two `OneLake` profiles overlap if they reference the same workspace +
    /// lakehouse + top-level folder, and one `directory_rel_path` is a
    /// (directory-bounded) prefix of the other.
    ///
    /// `endpoint_mode` and `authority_host` are intentionally NOT part of the
    /// overlap check: they pick the DNS hostname / AAD authority used to
    /// reach the same lakehouse, but the resolved storage location (the
    /// abfss path under `workspace/lakehouse/top_level_folder/directory_rel_path`)
    /// is identical regardless. Two profiles addressing the same on-storage
    /// path through different endpoints overlap.
    #[must_use]
    pub fn is_overlapping_location(&self, other: &Self) -> bool {
        if self.workspace_id != other.workspace_id
            || self.lakehouse_id != other.lakehouse_id
            || self.top_level_folder != other.top_level_folder
        {
            return false;
        }
        key_prefix_overlaps(
            self.directory_rel_path.as_deref(),
            other.directory_rel_path.as_deref(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_WORKSPACE: &str = "c5e8a1f3-7b2d-4e8a-9f1c-3b6d8e5a2f47";
    const SAMPLE_LAKEHOUSE: &str = "9d3e7a1b-4c6f-4a8e-b2d5-1f8c7e3a9b04";

    fn sample_profile() -> OneLakeProfile {
        OneLakeProfile {
            workspace_id: Uuid::parse_str(SAMPLE_WORKSPACE).unwrap(),
            lakehouse_id: Uuid::parse_str(SAMPLE_LAKEHOUSE).unwrap(),
            directory_rel_path: Some("my_warehouse".to_string()),
            top_level_folder: TopLevelFolder::Files,
            endpoint_mode: EndpointMode::Default,
            sas_token_validity_seconds: None,
            sas_enabled: true,
            authority_host: None,
            storage_layout: None,
        }
    }

    #[test]
    fn test_base_location_default_endpoint() {
        let p = sample_profile();
        let loc = p.base_location().unwrap();
        assert_eq!(
            loc.to_string(),
            format!(
                "abfss://{SAMPLE_WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SAMPLE_LAKEHOUSE}/Files/my_warehouse/"
            )
        );
    }

    #[test]
    fn test_base_location_regional_endpoint() {
        let mut p = sample_profile();
        p.endpoint_mode = EndpointMode::Regional {
            region: "westus".to_string(),
        };
        let loc = p.base_location().unwrap();
        assert_eq!(
            loc.to_string(),
            format!(
                "abfss://{SAMPLE_WORKSPACE}@westus-onelake.dfs.fabric.microsoft.com/{SAMPLE_LAKEHOUSE}/Files/my_warehouse/"
            )
        );
    }

    #[test]
    fn test_base_location_private_link() {
        let mut p = sample_profile();
        p.endpoint_mode = EndpointMode::WorkspacePrivateLink;
        let loc = p.base_location().unwrap();
        assert_eq!(
            loc.to_string(),
            format!(
                "abfss://{SAMPLE_WORKSPACE}@c5e8a1f37b2d4e8a9f1c3b6d8e5a2f47.zc5.dfs.fabric.microsoft.com/{SAMPLE_LAKEHOUSE}/Files/my_warehouse/"
            )
        );
    }

    #[test]
    fn test_base_location_tables_folder() {
        let mut p = sample_profile();
        p.top_level_folder = TopLevelFolder::Tables;
        let loc = p.base_location().unwrap();
        assert!(loc.to_string().contains("/Tables/my_warehouse/"));
    }

    #[test]
    fn test_dfs_host_private_link_xy_is_first_two_chars() {
        let p = OneLakeProfile {
            workspace_id: Uuid::parse_str("abcdef12-3456-7890-1234-56789abcdef0").unwrap(),
            endpoint_mode: EndpointMode::WorkspacePrivateLink,
            ..sample_profile()
        };
        assert_eq!(
            p.dfs_host(),
            "abcdef1234567890123456789abcdef0.zab.dfs.fabric.microsoft.com"
        );
    }

    #[test]
    fn test_iceberg_sas_property_key_default_endpoint() {
        // Iceberg clients extract `<account>` as the first DNS label and
        // `<host>` as the remainder; the published key must match what the
        // client looks up from the table location URL.
        let p = sample_profile();
        assert_eq!(
            p.iceberg_sas_property_key(),
            "adls.sas-token.onelake.dfs.fabric.microsoft.com"
        );
    }

    #[test]
    fn test_iceberg_sas_property_key_regional_endpoint() {
        let mut p = sample_profile();
        p.endpoint_mode = EndpointMode::Regional {
            region: "westus".to_string(),
        };
        assert_eq!(
            p.iceberg_sas_property_key(),
            "adls.sas-token.westus-onelake.dfs.fabric.microsoft.com"
        );
    }

    #[test]
    fn test_iceberg_sas_property_key_private_link() {
        let mut p = sample_profile();
        p.endpoint_mode = EndpointMode::WorkspacePrivateLink;
        // account = un-dashed workspace UUID, host = "z<xy>.dfs.fabric.microsoft.com".
        assert_eq!(
            p.iceberg_sas_property_key(),
            "adls.sas-token.c5e8a1f37b2d4e8a9f1c3b6d8e5a2f47.zc5.dfs.fabric.microsoft.com"
        );
    }

    #[test]
    fn test_host_account_and_suffix_compose_to_dfs_host() {
        // Internal invariant: `dfs_host()` must equal `<host_account>.<suffix>`
        // so that base_location and the SAS property key stay consistent with
        // what an Iceberg client sees on the wire.
        for mode in [
            EndpointMode::Default,
            EndpointMode::Regional {
                region: "northeurope".to_string(),
            },
            EndpointMode::WorkspacePrivateLink,
        ] {
            let p = OneLakeProfile {
                endpoint_mode: mode,
                ..sample_profile()
            };
            assert_eq!(
                p.dfs_host(),
                format!("{}.{}", p.host_account(), p.endpoint_suffix())
            );
        }
    }

    #[test]
    fn test_host_account_per_endpoint_mode() {
        let mut p = sample_profile();
        assert_eq!(p.host_account(), "onelake");

        p.endpoint_mode = EndpointMode::Regional {
            region: "westus".to_string(),
        };
        assert_eq!(p.host_account(), "westus-onelake");

        p.endpoint_mode = EndpointMode::WorkspacePrivateLink;
        assert_eq!(p.host_account(), "c5e8a1f37b2d4e8a9f1c3b6d8e5a2f47");
    }

    #[test]
    fn test_sas_account_is_always_onelake() {
        // Regression for `401 Access token validation failed`: the OneLake
        // canonical SAS resource is always /blob/onelake/..., even when the
        // request hits a regional or private-link host. `sas_account()` is
        // the single point of truth that has to stay `onelake` across modes.
        for mode in [
            EndpointMode::Default,
            EndpointMode::Regional {
                region: "centralus".to_string(),
            },
            EndpointMode::WorkspacePrivateLink,
        ] {
            let p = OneLakeProfile {
                endpoint_mode: mode,
                ..sample_profile()
            };
            assert_eq!(p.sas_account(), "onelake");
        }
    }

    #[test]
    fn test_blob_host_per_endpoint_mode() {
        let mut p = sample_profile();
        assert_eq!(p.blob_host(), "onelake.blob.fabric.microsoft.com");

        p.endpoint_mode = EndpointMode::Regional {
            region: "westus".to_string(),
        };
        assert_eq!(p.blob_host(), "westus-onelake.blob.fabric.microsoft.com");

        p.endpoint_mode = EndpointMode::WorkspacePrivateLink;
        assert_eq!(
            p.blob_host(),
            "c5e8a1f37b2d4e8a9f1c3b6d8e5a2f47.zc5.blob.fabric.microsoft.com",
        );
    }

    #[test]
    fn test_sas_cloud_location_pins_to_global_for_private_link() {
        let mut p = sample_profile();
        p.endpoint_mode = EndpointMode::WorkspacePrivateLink;
        match p.sas_cloud_location() {
            CloudLocation::Custom { account, uri } => {
                assert_eq!(account, "onelake");
                assert_eq!(uri, "https://onelake.dfs.fabric.microsoft.com");
            }
            other => panic!("expected Custom, got {other:?}"),
        }
    }

    #[test]
    fn test_sas_cloud_location_matches_data_location_for_default_and_regional() {
        for mode in [
            EndpointMode::Default,
            EndpointMode::Regional {
                region: "westus".to_string(),
            },
        ] {
            let p = OneLakeProfile {
                endpoint_mode: mode,
                ..sample_profile()
            };
            assert_eq!(
                format!("{:?}", p.sas_cloud_location()),
                format!("{:?}", p.cloud_location()),
            );
        }
    }

    #[test]
    fn test_normalize_rejects_region_with_dot() {
        let mut p = sample_profile();
        p.endpoint_mode = EndpointMode::Regional {
            region: "east.us".to_string(),
        };
        let err = p.normalize(None).unwrap_err();
        assert!(format!("{err:?}").contains("region"));
    }

    #[test]
    fn test_normalize_rejects_region_with_hyphen() {
        let mut p = sample_profile();
        p.endpoint_mode = EndpointMode::Regional {
            region: "east-us".to_string(),
        };
        let err = p.normalize(None).unwrap_err();
        assert!(format!("{err:?}").contains("region"));
    }

    #[test]
    fn test_normalize_rejects_region_starting_with_digit() {
        let mut p = sample_profile();
        p.endpoint_mode = EndpointMode::Regional {
            region: "1eastus".to_string(),
        };
        let err = p.normalize(None).unwrap_err();
        assert!(format!("{err:?}").contains("region"));
    }

    #[test]
    fn test_normalize_accepts_alphanumeric_regions() {
        for region in [
            "centralus",
            "eastus",
            "westeurope",
            "eastus2",
            "eastus2euap",
        ] {
            let mut p = sample_profile();
            p.endpoint_mode = EndpointMode::Regional {
                region: region.to_string(),
            };
            p.normalize(None)
                .unwrap_or_else(|e| panic!("region '{region}' should be accepted: {e:?}"));
        }
    }

    #[test]
    fn test_normalize_rejects_zero_ttl() {
        let mut p = sample_profile();
        p.sas_token_validity_seconds = Some(0);
        let err = p.normalize(None).unwrap_err();
        assert!(format!("{err:?}").contains("greater than 0"));
    }

    #[test]
    fn test_normalize_rejects_ttl_above_one_hour() {
        let mut p = sample_profile();
        p.sas_token_validity_seconds = Some(3601);
        let err = p.normalize(None).unwrap_err();
        assert!(format!("{err:?}").contains("3600"));
    }

    #[test]
    fn test_normalize_accepts_ttl_at_one_hour() {
        let mut p = sample_profile();
        p.sas_token_validity_seconds = Some(3600);
        p.normalize(None).unwrap();
    }

    #[test]
    fn test_normalize_rejects_shared_access_key_credential() {
        let mut p = sample_profile();
        let cred = AzCredential::SharedAccessKey {
            key: "fake-key".to_string(),
        };
        let err = p.normalize(Some(&cred)).unwrap_err();
        assert!(format!("{err:?}").contains("shared-access-key"));
    }

    #[test]
    fn test_normalize_accepts_client_credentials() {
        let mut p = sample_profile();
        let cred = AzCredential::ClientCredentials {
            client_id: "c".to_string(),
            tenant_id: "t".to_string(),
            client_secret: "s".to_string(),
        };
        p.normalize(Some(&cred)).unwrap();
    }

    #[test]
    fn test_normalize_accepts_none_directory_rel_path() {
        let mut p = sample_profile();
        p.directory_rel_path = None;
        p.normalize(None).unwrap();
        assert_eq!(p.directory_rel_path, None);
    }

    #[test]
    fn test_normalize_strips_directory_rel_path_slashes() {
        let mut p = sample_profile();
        p.directory_rel_path = Some("/foo/bar/".to_string());
        p.normalize(None).unwrap();
        assert_eq!(p.directory_rel_path.as_deref(), Some("foo/bar"));
    }

    #[test]
    fn test_normalize_rejects_empty_directory_rel_path() {
        let mut p = sample_profile();
        p.directory_rel_path = Some("/".to_string());
        let err = p.normalize(None).unwrap_err();
        assert!(format!("{err:?}").contains("directory-rel-path"));
    }

    #[test]
    fn test_normalize_rejects_parent_dir_traversal() {
        let mut p = sample_profile();
        p.directory_rel_path = Some("foo/../bar".to_string());
        let err = p.normalize(None).unwrap_err();
        assert!(format!("{err:?}").contains(".."));
    }

    #[test]
    fn test_normalize_lowercases_region() {
        let mut p = sample_profile();
        p.endpoint_mode = EndpointMode::Regional {
            region: "  WestUS  ".to_string(),
        };
        p.normalize(None).unwrap();
        match &p.endpoint_mode {
            EndpointMode::Regional { region } => assert_eq!(region, "westus"),
            _ => panic!("expected regional"),
        }
    }

    #[test]
    fn test_normalize_rejects_empty_region() {
        let mut p = sample_profile();
        p.endpoint_mode = EndpointMode::Regional {
            region: String::new(),
        };
        let err = p.normalize(None).unwrap_err();
        assert!(format!("{err:?}").contains("region"));
    }

    #[test]
    fn test_normalize_rejects_full_hierarchy_layout() {
        let mut p = sample_profile();
        p.storage_layout =
            Some(StorageLayout::try_new_full("{name}".into(), "{name}-{uuid}".into()).unwrap());
        let err = p.normalize(None).unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("storage-layout"), "{msg}");
        assert!(msg.contains("default"), "{msg}");
    }

    #[test]
    fn test_normalize_rejects_tabular_only_layout() {
        let mut p = sample_profile();
        p.storage_layout = Some(StorageLayout::try_new_flat("{name}-{uuid}".into()).unwrap());
        let err = p.normalize(None).unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("storage-layout"), "{msg}");
        assert!(msg.contains("default"), "{msg}");
    }

    #[test]
    fn test_normalize_accepts_default_layout() {
        let mut p = sample_profile();
        p.storage_layout = Some(StorageLayout::Default);
        p.normalize(None).unwrap();

        let mut p = sample_profile();
        p.storage_layout = None;
        p.normalize(None).unwrap();
    }

    #[test]
    fn test_update_with_rejects_non_default_layout() {
        // `update_with` doesn't call `normalize` itself — but warehouse-update
        // call sites do. Round-trip through both to guarantee the layout
        // constraint is enforced when callers swap in a new layout.
        let p1 = sample_profile();
        let mut p2 = sample_profile();
        p2.storage_layout = Some(StorageLayout::try_new_flat("{name}-{uuid}".into()).unwrap());
        let mut merged = p1.update_with(p2).unwrap();
        let err = merged.normalize(None).unwrap_err();
        assert!(format!("{err:?}").contains("storage-layout"));
    }

    #[test]
    fn test_update_with_immutable_workspace_id() {
        let p1 = sample_profile();
        let mut p2 = sample_profile();
        p2.workspace_id = Uuid::new_v4();
        let err = p1.update_with(p2).unwrap_err();
        assert!(format!("{err:?}").contains("workspace_id"));
    }

    #[test]
    fn test_update_with_immutable_endpoint_mode() {
        let p1 = sample_profile();
        let mut p2 = sample_profile();
        p2.endpoint_mode = EndpointMode::WorkspacePrivateLink;
        let err = p1.update_with(p2).unwrap_err();
        assert!(format!("{err:?}").contains("endpoint_mode"));
    }

    #[test]
    fn test_update_with_mutates_sas_settings() {
        let p1 = sample_profile();
        let mut p2 = sample_profile();
        p2.sas_token_validity_seconds = Some(1800);
        p2.sas_enabled = false;
        let updated = p1.update_with(p2).unwrap();
        assert_eq!(updated.sas_token_validity_seconds, Some(1800));
        assert!(!updated.sas_enabled);
    }

    #[test]
    fn test_update_with_mutates_authority_host() {
        let mut p1 = sample_profile();
        p1.authority_host = Some("https://login.microsoftonline.com".parse().unwrap());
        let mut p2 = sample_profile();
        p2.authority_host = Some("https://login.microsoftonline.us".parse().unwrap());
        let updated = p1.update_with(p2).unwrap();
        assert_eq!(
            updated.authority_host.as_ref().map(url::Url::as_str),
            Some("https://login.microsoftonline.us/")
        );
    }

    #[test]
    fn test_update_with_preserves_storage_layout_when_other_unset() {
        let mut p1 = sample_profile();
        p1.storage_layout = Some(StorageLayout::default());
        let p2 = sample_profile();
        let updated = p1.update_with(p2).unwrap();
        assert!(updated.storage_layout.is_some());
    }

    #[test]
    fn test_serde_default_round_trip() {
        let p = sample_profile();
        let s = serde_json::to_string(&p).unwrap();
        let back: OneLakeProfile = serde_json::from_str(&s).unwrap();
        assert_eq!(p, back);
    }

    #[test]
    fn test_serde_deserializes_pascal_case_top_level_folder() {
        let json = serde_json::json!({
            "workspace-id": SAMPLE_WORKSPACE,
            "lakehouse-id": SAMPLE_LAKEHOUSE,
            "directory-rel-path": "x",
            "top-level-folder": "Files",
            "endpoint-mode": { "type": "default" },
        });
        let p: OneLakeProfile = serde_json::from_value(json).unwrap();
        assert_eq!(p.top_level_folder, TopLevelFolder::Files);

        let json = serde_json::json!({
            "workspace-id": SAMPLE_WORKSPACE,
            "lakehouse-id": SAMPLE_LAKEHOUSE,
            "directory-rel-path": "x",
            "top-level-folder": "Tables",
            "endpoint-mode": { "type": "default" },
        });
        let p: OneLakeProfile = serde_json::from_value(json).unwrap();
        assert_eq!(p.top_level_folder, TopLevelFolder::Tables);
    }

    #[test]
    fn test_serde_endpoint_mode_variants() {
        let default_json = serde_json::json!({ "type": "default" });
        assert_eq!(
            serde_json::from_value::<EndpointMode>(default_json).unwrap(),
            EndpointMode::Default
        );

        let regional_json = serde_json::json!({ "type": "regional", "region": "westus" });
        assert_eq!(
            serde_json::from_value::<EndpointMode>(regional_json).unwrap(),
            EndpointMode::Regional {
                region: "westus".to_string()
            }
        );

        let private_json = serde_json::json!({ "type": "workspace-private-link" });
        assert_eq!(
            serde_json::from_value::<EndpointMode>(private_json).unwrap(),
            EndpointMode::WorkspacePrivateLink
        );
    }

    #[test]
    fn test_is_overlapping_same_directory() {
        let p1 = sample_profile();
        let p2 = sample_profile();
        assert!(p1.is_overlapping_location(&p2));
    }

    #[test]
    fn test_is_overlapping_directory_prefix() {
        let p1 = sample_profile();
        let mut p2 = sample_profile();
        p2.directory_rel_path = Some("my_warehouse/sub".to_string());
        assert!(p1.is_overlapping_location(&p2));
        assert!(p2.is_overlapping_location(&p1));
    }

    #[test]
    fn test_is_overlapping_none_rel_path() {
        let p1 = sample_profile();
        let mut p2 = sample_profile();
        p2.directory_rel_path = None;
        assert!(p1.is_overlapping_location(&p2));
        assert!(p2.is_overlapping_location(&p1));
    }

    #[test]
    fn test_is_overlapping_different_rel_path() {
        let p1 = sample_profile();
        let mut p2 = sample_profile();
        p2.directory_rel_path = Some("my_second_warehouse".to_string());
        assert!(!p1.is_overlapping_location(&p2));
        assert!(!p2.is_overlapping_location(&p1));
    }

    #[test]
    fn test_is_overlapping_different_workspaces() {
        let p1 = sample_profile();
        let mut p2 = sample_profile();
        p2.workspace_id = Uuid::new_v4();
        assert!(!p1.is_overlapping_location(&p2));
    }

    #[test]
    fn test_is_overlapping_different_top_level_folder() {
        let p1 = sample_profile();
        let mut p2 = sample_profile();
        p2.top_level_folder = TopLevelFolder::Tables;
        assert!(!p1.is_overlapping_location(&p2));
    }

    #[test]
    fn test_is_overlapping_different_endpoint_mode_still_overlaps() {
        // endpoint_mode picks the DNS host to reach the lakehouse; the
        // resolved on-storage path is identical. Two profiles addressing
        // the same workspace+lakehouse+folder+dir overlap regardless of
        // which endpoint they use.
        let p1 = sample_profile();
        let mut p2 = sample_profile();
        p2.endpoint_mode = EndpointMode::Regional {
            region: "westus".to_string(),
        };
        assert!(p1.is_overlapping_location(&p2));

        let mut p3 = sample_profile();
        p3.endpoint_mode = EndpointMode::WorkspacePrivateLink;
        assert!(p1.is_overlapping_location(&p3));
        assert!(p2.is_overlapping_location(&p3));
    }

    #[test]
    fn test_is_overlapping_different_authority_host_still_overlaps() {
        // authority_host picks the AAD instance for token issuance; it
        // doesn't affect where data lands on storage.
        let mut p1 = sample_profile();
        p1.authority_host = Some("https://login.microsoftonline.com".parse().unwrap());
        let mut p2 = sample_profile();
        p2.authority_host = Some("https://login.microsoftonline.us".parse().unwrap());
        assert!(p1.is_overlapping_location(&p2));
    }
}
