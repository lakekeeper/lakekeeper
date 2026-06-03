//! Credential plumbing for the ADLS Gen2 backend.
//!
//! Maps Lakekeeper's [`AzureAuth`] variants to `object_store`'s
//! [`MicrosoftAzureBuilder`] configuration. The interesting case is
//! [`AzureAuth::AzureSystemIdentity`]: the legacy Azure SDK had a
//! `DefaultAzureCredential` that chained env vars → workload identity →
//! managed identity → CLI at request time. `object_store` exposes those
//! mechanisms only as individual builder switches with no auto-chain, so we
//! resolve the chain ourselves *once* at construction time and remember the
//! choice for the lifetime of the [`AdlsStorage`].
//!
//! Probing once mirrors the previous behaviour (which cached the
//! `DefaultAzureCredential` for 30 minutes per `(authority, account)`). The
//! per-token TTL refresh stays inside `object_store`'s internal
//! `TokenCredentialProvider`, where it has always lived.
use object_store::azure::MicrosoftAzureBuilder;
use veil::Redact;

use crate::adls::{AzureAuth, AzureClientCredentialsAuth, AzureSasAuth, AzureSharedAccessKeyAuth};

/// The system-identity mechanism resolved once at construction time, so
/// every per-container `MicrosoftAzureBuilder` we build uses the same one.
///
/// Secret fields (`client_secret`) are redacted by the derived `Debug`. The
/// `token_file` field is a filesystem path to a workload-identity token, not
/// the token itself, so it's left visible. See `AzureClientCredentialsAuth`
/// for the matching pattern on the upstream input types.
#[derive(Redact, Clone)]
pub(crate) enum SystemIdentityMode {
    ClientSecret {
        client_id: String,
        tenant_id: String,
        #[redact(partial)]
        client_secret: String,
    },
    FederatedTokenFile {
        client_id: String,
        tenant_id: String,
        token_file: String,
    },
    ManagedIdentity {
        msi_endpoint: Option<String>,
        client_id: Option<String>,
    },
    AzureCli,
}

/// The credential decision frozen at construction time. Used by every
/// per-container `MicrosoftAzureBuilder` we build for this storage instance.
///
/// All secret-bearing variants redact via the derived `Debug` so that any
/// `tracing::*!(?config)` / panic backtrace / future log call printing an
/// `AdlsClientConfig` (which embeds this enum) cannot leak the raw account
/// key, client secret, or SAS signature. Matches the redaction pattern on
/// `AzureAuth`'s sub-types in `adls.rs`.
#[derive(Redact, Clone)]
pub(crate) enum ResolvedCredential {
    AccessKey(#[redact(partial)] String),
    ClientSecret {
        client_id: String,
        tenant_id: String,
        #[redact(partial)]
        client_secret: String,
    },
    Sas(#[redact(partial)] String),
    SystemIdentity(SystemIdentityMode),
}

impl ResolvedCredential {
    /// Probe the environment for a usable system-identity mechanism.
    ///
    /// Resolution order — first match wins — mirrors the chain that the
    /// legacy `DefaultAzureCredential` used:
    ///   1. **`ClientSecret`** — `AZURE_CLIENT_ID` + `AZURE_TENANT_ID` +
    ///      `AZURE_CLIENT_SECRET`.
    ///   2. **`FederatedTokenFile`** — `AZURE_FEDERATED_TOKEN_FILE` (workload
    ///      identity federation; requires `AZURE_CLIENT_ID` + `AZURE_TENANT_ID`
    ///      too).
    ///   3. **`AzureCli`** — opt-in: `USE_AZURE_CLI=1` (or any case-insensitive
    ///      `true`). Checked *before* `ManagedIdentity` falls through so the
    ///      developer fallback wins over the default IMDS path.
    ///   4. **`ManagedIdentity`** — default fallback: `IDENTITY_ENDPOINT` /
    ///      `MSI_ENDPOINT` if set, otherwise the local-link IMDS endpoint
    ///      (`169.254.169.254`). Always returns *something* — whether the
    ///      request succeeds is decided at token-fetch time.
    ///
    /// **Probe lifetime.** Env vars are read once per `AdlsStorage`. If an
    /// operator rotates `AZURE_CLIENT_SECRET` (or any other probed variable)
    /// at runtime, the running process keeps the mechanism it picked at
    /// startup. To re-probe, recreate the `AdlsStorage` (or restart the
    /// process). The per-token TTL refresh stays inside `object_store`'s
    /// internal `TokenCredentialProvider`, where it has always lived.
    pub(crate) fn resolve_system_identity() -> Self {
        Self::resolve_system_identity_with(|k| std::env::var(k).ok())
    }

    /// Dependency-injected variant of [`Self::resolve_system_identity`] that
    /// reads "env" through a caller-supplied lookup. Used by tests to drive
    /// every precedence arm deterministically without mutating process env
    /// (which is `unsafe` since Rust 1.86 and forbidden in this crate). The
    /// production entry point passes `std::env::var(...).ok()`.
    fn resolve_system_identity_with(env: impl Fn(&str) -> Option<String>) -> Self {
        let non_empty = |k: &str| env(k).filter(|s| !s.is_empty());

        // 1. Service-principal env vars.
        if let (Some(client_id), Some(tenant_id), Some(client_secret)) = (
            non_empty("AZURE_CLIENT_ID"),
            non_empty("AZURE_TENANT_ID"),
            non_empty("AZURE_CLIENT_SECRET"),
        ) {
            return Self::SystemIdentity(SystemIdentityMode::ClientSecret {
                client_id,
                tenant_id,
                client_secret,
            });
        }

        // 2. Federated-token workload identity.
        if let (Some(token_file), Some(client_id), Some(tenant_id)) = (
            non_empty("AZURE_FEDERATED_TOKEN_FILE"),
            non_empty("AZURE_CLIENT_ID"),
            non_empty("AZURE_TENANT_ID"),
        ) {
            return Self::SystemIdentity(SystemIdentityMode::FederatedTokenFile {
                client_id,
                tenant_id,
                token_file,
            });
        }

        // 3. Managed identity. IDENTITY_ENDPOINT is set in App Service / Functions;
        // MSI_ENDPOINT is the legacy name. With neither set, `object_store` falls
        // back to the local-link IMDS endpoint, which is correct for AKS / VMs.
        let msi_endpoint = non_empty("IDENTITY_ENDPOINT").or_else(|| non_empty("MSI_ENDPOINT"));
        let client_id = non_empty("AZURE_CLIENT_ID");

        // 4. Azure CLI is reserved as an explicit fallback for developers who
        // set USE_AZURE_CLI=1 (or any case-insensitive `true`).
        if env("USE_AZURE_CLI").is_some_and(|v| v == "1" || v.eq_ignore_ascii_case("true")) {
            return Self::SystemIdentity(SystemIdentityMode::AzureCli);
        }

        Self::SystemIdentity(SystemIdentityMode::ManagedIdentity {
            msi_endpoint,
            client_id,
        })
    }

    pub(crate) fn from_auth(auth: &AzureAuth) -> Self {
        match auth {
            AzureAuth::ClientCredentials(AzureClientCredentialsAuth {
                client_id,
                tenant_id,
                client_secret,
            }) => Self::ClientSecret {
                client_id: client_id.clone(),
                tenant_id: tenant_id.clone(),
                client_secret: client_secret.clone(),
            },
            AzureAuth::SharedAccessKey(AzureSharedAccessKeyAuth { key }) => {
                Self::AccessKey(key.clone())
            }
            AzureAuth::AzureSystemIdentity => Self::resolve_system_identity(),
            AzureAuth::Sas(AzureSasAuth { sas_token }) => Self::Sas(sas_token.clone()),
        }
    }

    /// Apply this resolved credential to a builder. The builder must already
    /// have `account`, `container`, and (optionally) `endpoint` and
    /// `authority_host` set.
    pub(crate) fn apply(&self, mut builder: MicrosoftAzureBuilder) -> MicrosoftAzureBuilder {
        match self {
            Self::AccessKey(key) => builder.with_access_key(key.clone()),
            Self::ClientSecret {
                client_id,
                tenant_id,
                client_secret,
            } => builder.with_client_secret_authorization(
                client_id.clone(),
                client_secret.clone(),
                tenant_id.clone(),
            ),
            Self::Sas(sas) => {
                // `with_config(SasKey, ...)` accepts the raw query-string form
                // ("sv=...&sig=...") and is more forgiving about whether the
                // string is prefixed by `?` than `with_sas_authorization`,
                // which expects an already-parsed `Vec<(key, value)>`.
                builder = builder.with_config(object_store::azure::AzureConfigKey::SasKey, sas);
                builder
            }
            Self::SystemIdentity(mode) => match mode {
                SystemIdentityMode::ClientSecret {
                    client_id,
                    tenant_id,
                    client_secret,
                } => builder.with_client_secret_authorization(
                    client_id.clone(),
                    client_secret.clone(),
                    tenant_id.clone(),
                ),
                SystemIdentityMode::FederatedTokenFile {
                    client_id,
                    tenant_id,
                    token_file,
                } => builder
                    .with_client_id(client_id.clone())
                    .with_tenant_id(tenant_id.clone())
                    .with_federated_token_file(token_file.clone()),
                SystemIdentityMode::ManagedIdentity {
                    msi_endpoint,
                    client_id,
                } => {
                    if let Some(endpoint) = msi_endpoint {
                        builder = builder.with_msi_endpoint(endpoint.clone());
                    }
                    if let Some(client_id) = client_id {
                        builder = builder.with_client_id(client_id.clone());
                    }
                    builder
                }
                SystemIdentityMode::AzureCli => builder.with_use_azure_cli(true),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `from_auth` for non-system-identity variants is a pure mapping; this
    /// test pins that mapping so changes to enum field names are caught.
    #[test]
    fn from_auth_non_system_identity_variants() {
        let access_key =
            ResolvedCredential::from_auth(&AzureAuth::SharedAccessKey(AzureSharedAccessKeyAuth {
                key: "k".to_string(),
            }));
        assert!(matches!(access_key, ResolvedCredential::AccessKey(_)));

        let client_secret = ResolvedCredential::from_auth(&AzureAuth::ClientCredentials(
            AzureClientCredentialsAuth {
                client_id: "c".to_string(),
                tenant_id: "t".to_string(),
                client_secret: "s".to_string(),
            },
        ));
        assert!(matches!(
            client_secret,
            ResolvedCredential::ClientSecret { .. }
        ));

        let sas = ResolvedCredential::from_auth(&AzureAuth::Sas(AzureSasAuth {
            sas_token: "sv=2024-08-04&sig=abc".to_string(),
        }));
        assert!(matches!(sas, ResolvedCredential::Sas(_)));
    }

    /// `Debug` on every secret-bearing variant must not echo the raw secret
    /// verbatim. The previous custom Azure SDK had this property via
    /// `veil::Redact` on `AzureAuth`'s sub-types; the `ResolvedCredential`
    /// shim introduced for `object_store` must preserve it so any future
    /// `tracing::*!(?config)` / panic backtrace / formatted error cannot
    /// leak account keys, client secrets, or SAS signatures.
    #[test]
    fn debug_redacts_secret_material() {
        let cases = [
            (
                "AccessKey",
                ResolvedCredential::AccessKey("UNIQUE_ACCESS_KEY_VALUE".to_string()),
                "UNIQUE_ACCESS_KEY_VALUE",
            ),
            (
                "ClientSecret",
                ResolvedCredential::ClientSecret {
                    client_id: "cid".to_string(),
                    tenant_id: "tid".to_string(),
                    client_secret: "UNIQUE_CLIENT_SECRET_VALUE".to_string(),
                },
                "UNIQUE_CLIENT_SECRET_VALUE",
            ),
            (
                "Sas",
                ResolvedCredential::Sas("sv=2024&sig=UNIQUE_SIGNATURE_VALUE".to_string()),
                "UNIQUE_SIGNATURE_VALUE",
            ),
            (
                "SystemIdentity::ClientSecret",
                ResolvedCredential::SystemIdentity(SystemIdentityMode::ClientSecret {
                    client_id: "cid".to_string(),
                    tenant_id: "tid".to_string(),
                    client_secret: "UNIQUE_SYS_SECRET_VALUE".to_string(),
                }),
                "UNIQUE_SYS_SECRET_VALUE",
            ),
        ];
        for (label, cred, sentinel) in cases {
            let debug = format!("{cred:?}");
            assert!(
                !debug.contains(sentinel),
                "{label}: Debug leaked sentinel `{sentinel}` in `{debug}`",
            );
        }
    }
}

/// System-identity probe tests. Drives [`ResolvedCredential::resolve_system_identity_with`]
/// directly with a `HashMap`-backed env lookup so each test is deterministic
/// and parallel-safe. We deliberately avoid mutating process env: the crate
/// is `#![forbid(unsafe_code)]`, and `std::env::set_var` has been `unsafe`
/// since Rust 1.86. Driving the probe through its injected lookup is also
/// strictly cleaner than `serial_test`-style serialisation.
#[cfg(test)]
mod system_identity_tests {
    use std::collections::HashMap;

    use super::*;

    /// Build a closure-shaped env lookup from `&[(key, value)]`. Use the empty
    /// slice for "no env vars set".
    fn env_from(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        move |k| map.get(k).cloned()
    }

    #[test]
    fn arm1_client_secret_wins_when_all_three_set() {
        let cred = ResolvedCredential::resolve_system_identity_with(env_from(&[
            ("AZURE_CLIENT_ID", "cid"),
            ("AZURE_TENANT_ID", "tid"),
            ("AZURE_CLIENT_SECRET", "secret"),
        ]));
        match cred {
            ResolvedCredential::SystemIdentity(SystemIdentityMode::ClientSecret {
                client_id,
                tenant_id,
                client_secret,
            }) => {
                assert_eq!(client_id, "cid");
                assert_eq!(tenant_id, "tid");
                assert_eq!(client_secret, "secret");
            }
            other => panic!("expected ClientSecret arm, got {other:?}"),
        }
    }

    #[test]
    fn arm1_falls_through_when_secret_is_empty() {
        let cred = ResolvedCredential::resolve_system_identity_with(env_from(&[
            ("AZURE_CLIENT_ID", "cid"),
            ("AZURE_TENANT_ID", "tid"),
            ("AZURE_CLIENT_SECRET", ""), // empty → fall through to arm 2
            ("AZURE_FEDERATED_TOKEN_FILE", "/tok"),
        ]));
        match cred {
            ResolvedCredential::SystemIdentity(SystemIdentityMode::FederatedTokenFile {
                client_id,
                tenant_id,
                token_file,
            }) => {
                assert_eq!(client_id, "cid");
                assert_eq!(tenant_id, "tid");
                assert_eq!(token_file, "/tok");
            }
            other => panic!("expected FederatedTokenFile arm after empty secret, got {other:?}"),
        }
    }

    #[test]
    fn arm2_federated_token_file_when_set_alongside_client_and_tenant() {
        let cred = ResolvedCredential::resolve_system_identity_with(env_from(&[
            ("AZURE_CLIENT_ID", "cid"),
            ("AZURE_TENANT_ID", "tid"),
            (
                "AZURE_FEDERATED_TOKEN_FILE",
                "/var/run/secrets/azure/tokens/azure-identity-token",
            ),
        ]));
        match cred {
            ResolvedCredential::SystemIdentity(SystemIdentityMode::FederatedTokenFile {
                client_id,
                tenant_id,
                token_file,
            }) => {
                assert_eq!(client_id, "cid");
                assert_eq!(tenant_id, "tid");
                assert_eq!(
                    token_file,
                    "/var/run/secrets/azure/tokens/azure-identity-token"
                );
            }
            other => panic!("expected FederatedTokenFile arm, got {other:?}"),
        }
    }

    #[test]
    fn arm2_falls_through_when_token_file_is_empty() {
        let cred = ResolvedCredential::resolve_system_identity_with(env_from(&[
            ("AZURE_CLIENT_ID", "cid"),
            ("AZURE_TENANT_ID", "tid"),
            ("AZURE_FEDERATED_TOKEN_FILE", ""), // empty → fall through
        ]));
        match cred {
            ResolvedCredential::SystemIdentity(SystemIdentityMode::ManagedIdentity {
                msi_endpoint,
                client_id,
            }) => {
                assert_eq!(msi_endpoint, None);
                assert_eq!(client_id.as_deref(), Some("cid"));
            }
            other => panic!("expected ManagedIdentity after empty token file, got {other:?}"),
        }
    }

    #[test]
    fn arm3_managed_identity_with_identity_endpoint() {
        let cred = ResolvedCredential::resolve_system_identity_with(env_from(&[(
            "IDENTITY_ENDPOINT",
            "http://169.254.169.254/example",
        )]));
        match cred {
            ResolvedCredential::SystemIdentity(SystemIdentityMode::ManagedIdentity {
                msi_endpoint,
                client_id,
            }) => {
                assert_eq!(
                    msi_endpoint.as_deref(),
                    Some("http://169.254.169.254/example")
                );
                assert_eq!(client_id, None);
            }
            other => panic!("expected ManagedIdentity with IDENTITY_ENDPOINT, got {other:?}"),
        }
    }

    #[test]
    fn arm3_managed_identity_legacy_msi_endpoint() {
        let cred = ResolvedCredential::resolve_system_identity_with(env_from(&[(
            "MSI_ENDPOINT",
            "http://msi.legacy.example",
        )]));
        match cred {
            ResolvedCredential::SystemIdentity(SystemIdentityMode::ManagedIdentity {
                msi_endpoint,
                ..
            }) => {
                assert_eq!(msi_endpoint.as_deref(), Some("http://msi.legacy.example"));
            }
            other => panic!("expected ManagedIdentity with MSI_ENDPOINT, got {other:?}"),
        }
    }

    #[test]
    fn arm3_identity_endpoint_wins_over_msi_endpoint() {
        let cred = ResolvedCredential::resolve_system_identity_with(env_from(&[
            ("IDENTITY_ENDPOINT", "http://new.endpoint"),
            ("MSI_ENDPOINT", "http://legacy.endpoint"),
        ]));
        match cred {
            ResolvedCredential::SystemIdentity(SystemIdentityMode::ManagedIdentity {
                msi_endpoint,
                ..
            }) => {
                assert_eq!(msi_endpoint.as_deref(), Some("http://new.endpoint"));
            }
            other => panic!("expected ManagedIdentity preferring IDENTITY_ENDPOINT, got {other:?}"),
        }
    }

    #[test]
    fn arm3_managed_identity_with_no_env_vars_set() {
        let cred = ResolvedCredential::resolve_system_identity_with(env_from(&[]));
        match cred {
            ResolvedCredential::SystemIdentity(SystemIdentityMode::ManagedIdentity {
                msi_endpoint,
                client_id,
            }) => {
                assert_eq!(msi_endpoint, None);
                assert_eq!(client_id, None);
            }
            other => panic!("expected default ManagedIdentity (IMDS), got {other:?}"),
        }
    }

    #[test]
    fn arm4_azure_cli_overrides_managed_identity_defaults() {
        let cred = ResolvedCredential::resolve_system_identity_with(env_from(&[
            ("USE_AZURE_CLI", "1"),
            // Even with IDENTITY_ENDPOINT set, USE_AZURE_CLI takes precedence
            // at arm 3/4 (after arms 1+2 have not matched).
            ("IDENTITY_ENDPOINT", "http://should-not-win"),
        ]));
        match cred {
            ResolvedCredential::SystemIdentity(SystemIdentityMode::AzureCli) => {}
            other => panic!("expected AzureCli, got {other:?}"),
        }
    }

    #[test]
    fn azure_cli_truthy_strings() {
        // Contract: "1" or any case-insensitive "true". "True" / "tRuE" /
        // "trUe" are all accepted.
        for v in ["1", "true", "TRUE", "True", "tRuE"] {
            let cred =
                ResolvedCredential::resolve_system_identity_with(env_from(&[("USE_AZURE_CLI", v)]));
            assert!(
                matches!(
                    cred,
                    ResolvedCredential::SystemIdentity(SystemIdentityMode::AzureCli)
                ),
                "USE_AZURE_CLI={v} should select AzureCli, got {cred:?}",
            );
        }
    }

    #[test]
    fn azure_cli_falsy_value_falls_through() {
        let cred = ResolvedCredential::resolve_system_identity_with(env_from(&[(
            "USE_AZURE_CLI",
            "0", // not in the truthy list
        )]));
        match cred {
            ResolvedCredential::SystemIdentity(SystemIdentityMode::ManagedIdentity { .. }) => {}
            other => panic!("USE_AZURE_CLI=0 should not select AzureCli, got {other:?}"),
        }
    }

    #[test]
    fn arm1_takes_precedence_over_use_azure_cli() {
        // USE_AZURE_CLI is only consulted in the managed-identity arm; arms 1
        // and 2 short-circuit before reaching it.
        let cred = ResolvedCredential::resolve_system_identity_with(env_from(&[
            ("USE_AZURE_CLI", "1"),
            ("AZURE_CLIENT_ID", "cid"),
            ("AZURE_TENANT_ID", "tid"),
            ("AZURE_CLIENT_SECRET", "secret"),
        ]));
        match cred {
            ResolvedCredential::SystemIdentity(SystemIdentityMode::ClientSecret { .. }) => {}
            other => {
                panic!("expected ClientSecret to take precedence over USE_AZURE_CLI, got {other:?}")
            }
        }
    }
}
