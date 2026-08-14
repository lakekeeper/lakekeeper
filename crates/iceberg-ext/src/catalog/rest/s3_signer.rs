use std::collections::HashMap;

use typed_builder::TypedBuilder;

#[cfg(feature = "axum")]
use super::impl_into_response;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, TypedBuilder)]
pub struct S3SignRequest {
    pub region: String,
    pub uri: url::Url,
    #[serde(with = "http_method_serde")]
    pub method: http::Method,
    pub headers: HashMap<String, Vec<String>>,
    pub body: Option<String>,
    /// Echoed back verbatim from [`RemoteSigningConfig::properties`]. The spec
    /// makes this the signer's only channel for per-table state, since the
    /// config carries no endpoint of its own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub properties: Option<HashMap<String, String>>,
    /// Storage provider the request is to be signed for, matching the scheme of
    /// a native storage URI. Absent means `s3`, per the spec's backwards
    /// compatibility rule.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub provider: Option<String>,
}

/// Signer settings advertised on `loadTable`, superseding the deprecated
/// `signer.uri` / `signer.endpoint` config keys.
///
/// Deliberately carries no endpoint: a client that reads this field derives the
/// endpoint as the table's standard `…/tables/{table}/sign` path, which is why
/// serving that route is a precondition for emitting this at all.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, TypedBuilder)]
#[serde(rename_all = "kebab-case")]
pub struct RemoteSigningConfig {
    /// Passed through unchanged in the `properties` of every sign request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub properties: Option<HashMap<String, String>>,
    /// Included unchanged in every request to the signing endpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub headers: Option<HashMap<String, Vec<String>>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, TypedBuilder)]
pub struct S3SignResponse {
    pub uri: url::Url,
    pub headers: HashMap<String, Vec<String>>,
}

#[cfg(feature = "axum")]
impl_into_response!(S3SignResponse);

mod http_method_serde {
    use std::str::FromStr;

    use serde::{Deserialize, Deserializer, Serializer};

    pub(crate) fn serialize<S>(method: &http::Method, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(method.as_str())
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<http::Method, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        http::Method::from_str(&s)
            .map_err(|e| serde::de::Error::custom(format!("Invalid HTTP method: {e}")))
    }
}

#[cfg(test)]
mod tests {

    use serde::Deserialize;

    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct TestMethod {
        #[serde(with = "http_method_serde")]
        method: http::Method,
    }

    #[test]
    fn test_http_method_serde() {
        let sign_request = S3SignRequest::builder()
            .region("us-west-2".to_string())
            .uri(url::Url::parse("https://example.com").unwrap())
            .method(http::Method::GET)
            .headers(HashMap::new())
            .body(None)
            .build();

        let serialized = serde_json::to_string(&sign_request).unwrap();
        let deserialized: S3SignRequest = serde_json::from_str(&serialized).unwrap();
        assert_eq!(sign_request, deserialized);
    }

    /// Every released signing client predates `properties` / `provider`. Their
    /// payloads must keep deserializing, and we must not start emitting the new
    /// keys as `null` to servers reading them back.
    #[test]
    fn sign_request_stays_compatible_without_the_new_fields() {
        let legacy = serde_json::json!({
            "region": "us-west-2",
            "uri": "https://example.com",
            "method": "GET",
            "headers": {},
            "body": null,
        });
        let parsed: S3SignRequest = serde_json::from_value(legacy).unwrap();
        assert_eq!(parsed.properties, None);
        assert_eq!(parsed.provider, None);

        let reserialized = serde_json::to_value(&parsed).unwrap();
        assert!(reserialized.get("properties").is_none());
        assert!(reserialized.get("provider").is_none());
    }

    #[test]
    fn sign_request_round_trips_the_new_fields() {
        let request = S3SignRequest::builder()
            .region("us-west-2".to_string())
            .uri(url::Url::parse("https://example.com").unwrap())
            .method(http::Method::GET)
            .headers(HashMap::new())
            .body(None)
            .properties(Some(HashMap::from([(
                "lakekeeper.tabular-id".to_string(),
                "some-id".to_string(),
            )])))
            .provider(Some("s3".to_string()))
            .build();

        let round_tripped: S3SignRequest =
            serde_json::from_str(&serde_json::to_string(&request).unwrap()).unwrap();
        assert_eq!(request, round_tripped);
    }

    /// Absence is the signal that a client should fall back to the deprecated
    /// `signer.*` config keys, so an unset config must not serialize as `null`.
    #[test]
    fn remote_signing_config_omits_unset_fields() {
        let serialized = serde_json::to_value(RemoteSigningConfig::default()).unwrap();
        assert_eq!(serialized, serde_json::json!({}));
    }

    #[test]
    fn test_http_method_de() {
        let method = "POST";
        let deserialized: TestMethod =
            serde_json::from_str(&format!("{{\"method\":\"{method}\"}}")).unwrap();
        assert_eq!(deserialized.method, http::Method::POST);
    }

    #[test]
    fn test_de() {
        let value = serde_json::json!({
            "region": "eu-central-1",
            "uri": "https://demo-catalog-iceberg.s3.eu-central-1.amazonaws.com?delete",
            "method": "POST",
            "headers": {
                "Content-Length": ["295"],
                "Content-MD5": ["+hmWjZ/juo1mqRvDC1F5AQ=="],
                "Content-Type": ["application/xml"],
                "User-Agent": ["aws-sdk-java/2.24.5 Mac_OS_X/14.4.1 OpenJDK_64-Bit_Server_VM/17.0.11+0 Java/17.0.11 scala/2.12.18 vendor/Homebrew io/sync http/Apache cfg/retry-mode/legacy"],
                "amz-sdk-invocation-id": ["c7c476d8-75f0-1193-9f98-aed5586e8878"],
                "amz-sdk-request": ["attempt=1; max=4"]
            },
            "body": "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Object><Key>test_warehouse/c9af26aa-0afb-11ef-9e97-4fc80c092114/018f49ab-7805-70f6-9264-1f5dc7b323c9/metadata/data/00013-27-e7a39917-8839-4b67-9dd7-ce0635f32d13-0-00001.parquet</Key></Object></Delete>",
        });
        let sign_request: S3SignRequest = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(sign_request.method, http::Method::POST);
        assert_eq!(
            sign_request.uri,
            url::Url::parse("https://demo-catalog-iceberg.s3.eu-central-1.amazonaws.com?delete")
                .unwrap()
        );
        assert_eq!(sign_request.headers.len(), 6);
        assert_eq!(
            sign_request.body,
            Some(value["body"].as_str().unwrap().to_string())
        );
    }
}
