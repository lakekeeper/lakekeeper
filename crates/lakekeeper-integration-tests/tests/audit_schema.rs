//! Lakekeeper's published audit schema is what the registry of every Lakekeeper crate
//! generates.
//!
//! This binary links every crate that declares audit types for the `lakekeeper` emitter, so
//! its registry holds all of them. `just update-audit-schema` runs this test with
//! `LAKEKEEPER_UPDATE_AUDIT_SCHEMA=1` to write `docs/docs/audit/schema.json`, then again to
//! verify.

// Referenced by nothing else in this test, so named here to keep its registrations linked.
use lakekeeper_authz_openfga as _;

#[test]
fn the_published_schema_is_what_the_registry_generates() {
    lakekeeper::audit::schema::assert_published_schema(
        "lakekeeper",
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."),
        "docs/docs/audit/schema.json",
    );
}
