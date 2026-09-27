#[cfg(feature = "axum")]
use super::impl_into_response;

/// Server-provided configuration for the catalog.
#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CatalogConfig {
    /// Properties that should be used to override client configuration; applied after defaults and client configuration.
    pub overrides: std::collections::HashMap<String, String>,
    /// Properties that should be used as default configuration; applied before client configuration.
    pub defaults: std::collections::HashMap<String, String>,
    pub endpoints: Vec<String>,
    /// Client reuse window for an `Idempotency-Key`, as an ISO-8601 duration
    /// (e.g. `PT30M`).
    ///
    /// Its presence is what tells a client the server supports
    /// `Idempotency-Key` semantics at all; absent means the client must assume
    /// it is unsupported. So this is skipped when serializing rather than sent
    /// as `null`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key_lifetime: Option<String>,
}

#[cfg(feature = "axum")]
impl_into_response! {CatalogConfig}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_catalog_serialization() {
        let j = serde_json::json!({
            "overrides": {"warehouse": "s3://bucket/warehouse/"},
            "defaults": {"clients": "4"},
            "endpoints": vec!["GET /config"]
        });

        let c: CatalogConfig = serde_json::from_value(j.clone()).unwrap();
        assert_eq!(serde_json::to_value(c).unwrap(), j);
    }

    /// A sibling of `overrides`/`defaults`, not an entry inside them — a client
    /// looking for it in the property maps concludes idempotency is
    /// unsupported, and one applying the maps wholesale gets an unrecognized
    /// property injected into its own config.
    #[test]
    fn idempotency_key_lifetime_is_a_top_level_field() {
        let config = CatalogConfig {
            idempotency_key_lifetime: Some("PT30M".to_string()),
            ..CatalogConfig::default()
        };

        let json = serde_json::to_value(config).unwrap();

        assert_eq!(json["idempotency-key-lifetime"], "PT30M");
        assert!(json["overrides"].as_object().unwrap().is_empty());
        assert!(json["defaults"].as_object().unwrap().is_empty());
    }

    /// Absence is meaningful: it is how a client learns idempotency is not
    /// supported, so the key must not appear at all rather than as `null`.
    #[test]
    fn idempotency_key_lifetime_is_omitted_when_unset() {
        let json = serde_json::to_value(CatalogConfig::default()).unwrap();

        assert!(
            !json
                .as_object()
                .unwrap()
                .contains_key("idempotency-key-lifetime"),
            "{json}"
        );
    }

    #[test]
    fn idempotency_key_lifetime_round_trips() {
        let json = serde_json::json!({
            "overrides": {},
            "defaults": {},
            "endpoints": Vec::<String>::new(),
            "idempotency-key-lifetime": "PT24H",
        });

        let config: CatalogConfig = serde_json::from_value(json.clone()).unwrap();

        assert_eq!(
            config.idempotency_key_lifetime.as_deref(),
            Some("PT24H"),
            "field did not deserialize"
        );
        assert_eq!(serde_json::to_value(config).unwrap(), json);
    }
}
