//! Requests for tests of the view owner property rules.

use std::collections::HashMap;

use lakekeeper::{
    MatchedEngines, TrinoEngineConfig, TrustedEngine, api::RequestMetadata, service::UserId,
};

/// The view property in which Trino stores the owner of a `SECURITY DEFINER` view.
pub const OWNER_PROPERTY: &str = "trino.run-as-owner";

/// An authenticated request matched as a trusted Trino engine that owns [`OWNER_PROPERTY`].
#[must_use]
pub fn trino_engine_request() -> RequestMetadata {
    let mut metadata = RequestMetadata::test_user(UserId::new_unchecked("oidc", "trino-engine"));
    metadata.set_engines(MatchedEngines::single(TrustedEngine::Trino(
        TrinoEngineConfig {
            owner_property: OWNER_PROPERTY.to_string(),
            identities: HashMap::new(),
        },
    )));
    metadata
}

/// An authenticated request that is not matched as a trusted engine, for example from Spark or
/// PyIceberg.
#[must_use]
pub fn user_request() -> RequestMetadata {
    RequestMetadata::test_user(UserId::new_unchecked("oidc", "spark-user"))
}
