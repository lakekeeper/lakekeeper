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
}
