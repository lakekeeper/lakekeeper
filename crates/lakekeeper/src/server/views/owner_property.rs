//! Engine owner properties of views, such as `trino.run-as-owner`.
//!
//! An engine runs a view as the user its owner property names (`SECURITY DEFINER`). Writing the
//! property, or changing the current version of a view that has it, is reserved for a trusted
//! engine whose owner property is that exact key. When that engine changes the query without
//! writing the property, the commit removes it, so the new query runs as the caller.
//!
//! The rules apply on a server that authenticates requests ([`State::authentication`]), to every
//! request, in-process calls included. Only the authentication middleware matches a request to a
//! trusted engine. A server without authentication sees every client as the same anonymous user
//! and can trust none of them as an engine, so the rules are off there.
//!
//! [`State::authentication`]: crate::service::State::authentication

use std::collections::{BTreeMap, BTreeSet, HashSet};

use http::StatusCode;
use iceberg::{
    NamespaceIdent,
    spec::{ViewMetadata, ViewRepresentation, ViewVersion},
};
use iceberg_ext::catalog::rest::ErrorModel;

use crate::{
    config::{CONFIG, MatchedEngines, TRINO_OWNER_PROPERTY},
    service::{ServerAuthentication, events::impl_authorization_failure_source},
};

/// The protected owner properties on a server with this authentication: `None` if the rules are
/// off.
pub(super) fn protected_properties(
    authentication: ServerAuthentication,
) -> Option<&'static HashSet<String>> {
    authentication
        .is_enabled()
        .then_some(&CONFIG.protected_properties)
}

/// A request wrote an engine owner property without being a trusted engine that owns it.
// `Display` by hand: the Trino advice depends on the key, and thiserror's `fmt =` serves enums
// only.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) struct ProtectedPropertyModification {
    /// The key as the request sent it.
    pub(crate) key: String,
    /// The protected key it matches, ignoring upper and lower case.
    pub(crate) protected: String,
}

impl std::fmt::Display for ProtectedPropertyModification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self { key, protected } = self;
        write!(
            f,
            "View property '{key}' names the user that query engines run this view as. Only a \
             trusted engine whose owner property is exactly '{protected}' may set, change or \
             remove it. Send the request without this property."
        )?;
        if protected == TRINO_OWNER_PROPERTY {
            write!(
                f,
                " Trino writes it for SECURITY DEFINER views: to create them, configure Trino as \
                 a trusted engine in Lakekeeper (see \"Trusted Engines\" in the configuration \
                 documentation), or create the view with SECURITY INVOKER."
            )?;
        }
        Ok(())
    }
}

impl From<ProtectedPropertyModification> for ErrorModel {
    fn from(err: ProtectedPropertyModification) -> Self {
        ErrorModel::builder()
            .code(StatusCode::FORBIDDEN.as_u16())
            .r#type("ProtectedPropertyModification")
            .message(err.to_string())
            .build()
    }
}

impl_authorization_failure_source!(ProtectedPropertyModification => ActionForbidden);

/// A commit changed the current version of a view an engine runs as its owner, and does not come
/// from a trusted engine that owns the owner property.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "Query engines run this view as the user in view property '{owner_property}'. Only a trusted \
     engine whose owner property is exactly '{owner_property}' may change which view version is \
     current. Make the change from that engine. If no such engine is configured, an operator \
     must configure it first (see \"Trusted Engines\" in the Lakekeeper configuration \
     documentation)."
)]
pub(crate) struct ProtectedViewVersionChange {
    pub(crate) owner_property: String,
}

impl From<ProtectedViewVersionChange> for ErrorModel {
    fn from(err: ProtectedViewVersionChange) -> Self {
        ErrorModel::builder()
            .code(StatusCode::FORBIDDEN.as_u16())
            .r#type("ProtectedViewVersionChange")
            .message(err.to_string())
            .build()
    }
}

impl_authorization_failure_source!(ProtectedViewVersionChange => ActionForbidden);

/// The properties a commit sets and removes, as authorized.
#[derive(Debug, Clone, Copy)]
pub(super) struct PropertyWrites<'a> {
    pub(super) updated: &'a BTreeMap<String, String>,
    pub(super) removed: &'a [String],
}

impl<'a> PropertyWrites<'a> {
    /// The keys the commit sets or removes.
    pub(super) fn keys(self) -> impl Iterator<Item = &'a str> {
        self.updated.keys().chain(self.removed).map(String::as_str)
    }

    fn contains(self, key: &str) -> bool {
        self.updated.contains_key(key) || self.removed.iter().any(|r| r == key)
    }
}

/// Checks that a request writes an engine owner property only if it comes from a trusted engine
/// that owns that exact key. `property_keys` are the keys the request sets or removes;
/// `protected_properties` is `None` if the rules are off.
pub(super) fn check_property_writes<'a>(
    property_keys: impl Iterator<Item = &'a str>,
    protected_properties: Option<&HashSet<String>>,
    matched_engines: &MatchedEngines,
) -> Result<(), ProtectedPropertyModification> {
    let Some(protected_properties) = protected_properties else {
        return Ok(());
    };
    for key in property_keys {
        // A key that matches a protected property ignoring upper and lower case is refused too,
        // also for the owning engine: engines read the exact key, so a variant would only
        // mislead readers.
        let Some(protected) = matching_protected_property(key, protected_properties) else {
            continue;
        };

        if !matched_engines.owns_property(key) {
            return Err(ProtectedPropertyModification {
                key: key.to_string(),
                protected: protected.to_string(),
            });
        }
    }

    Ok(())
}

/// The protected property `key` matches, ignoring upper and lower case: the exact key if it is
/// protected, otherwise the first match in sorted order, so the error names the same key on
/// every server.
fn matching_protected_property<'p>(
    key: &str,
    protected_properties: &'p HashSet<String>,
) -> Option<&'p str> {
    protected_properties
        .get(key)
        .or_else(|| {
            protected_properties
                .iter()
                .filter(|p| p.eq_ignore_ascii_case(key))
                .min()
        })
        .map(String::as_str)
}

/// Checks a commit that changes the current version of a view an engine runs as its owner.
///
/// Only a trusted engine that owns each owner property of the view may make it. If the query
/// changes, returns the owner properties the commit leaves untouched: the commit must remove
/// them, so the new query runs as the caller. Trino's `CREATE OR REPLACE VIEW ... SECURITY
/// INVOKER` sends no owner property, and Iceberg keeps stored properties a commit leaves out.
/// `protected_properties` is `None` if the rules are off.
pub(super) fn check_version_change(
    previous: &ViewMetadata,
    new: &ViewMetadata,
    written: PropertyWrites<'_>,
    protected_properties: Option<&HashSet<String>>,
    matched_engines: &MatchedEngines,
) -> Result<Vec<String>, ProtectedViewVersionChange> {
    let Some(protected_properties) = protected_properties else {
        return Ok(Vec::new());
    };
    if previous.current_version_id() == new.current_version_id() {
        return Ok(Vec::new());
    }

    // Sorted, so the error names the same key on every server.
    let owner_properties: BTreeSet<&str> = protected_properties
        .iter()
        .map(String::as_str)
        .filter(|p| previous.properties().contains_key(*p))
        .collect();

    if let Some(owner_property) = owner_properties
        .iter()
        .find(|p| !matched_engines.owns_property(p))
    {
        return Err(ProtectedViewVersionChange {
            owner_property: (*owner_property).to_string(),
        });
    }

    if ViewQuery::of(previous.current_version()) == ViewQuery::of(new.current_version()) {
        return Ok(Vec::new());
    }

    Ok(owner_properties
        .into_iter()
        .filter(|p| !written.contains(p))
        .map(String::from)
        .collect())
}

/// What a view version runs: its SQL in each dialect, and the catalog and namespace that
/// unqualified names in the SQL resolve against.
#[derive(Debug, PartialEq, Eq)]
struct ViewQuery<'a> {
    /// `(dialect in lower case, SQL)`. Dialects are compared ignoring upper and lower case.
    representations: BTreeSet<(String, &'a str)>,
    default_catalog: Option<&'a str>,
    default_namespace: &'a NamespaceIdent,
}

impl<'a> ViewQuery<'a> {
    fn of(version: &'a ViewVersion) -> Self {
        Self {
            representations: version
                .representations()
                .iter()
                .map(|representation| match representation {
                    ViewRepresentation::Sql(sql) => {
                        (sql.dialect.to_ascii_lowercase(), sql.sql.as_str())
                    }
                })
                .collect(),
            default_catalog: version.default_catalog().map(String::as_str),
            default_namespace: version.default_namespace(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use crate::config::{MatchedEngines, TrinoEngineConfig, TrustedEngine, protected_properties};

    const OWNER: &str = "trino.run-as-owner";
    const CUSTOM_OWNER: &str = "custom.run-as-owner";

    fn engine(owner_property: &str) -> TrustedEngine {
        TrustedEngine::Trino(TrinoEngineConfig {
            owner_property: owner_property.to_string(),
            identities: HashMap::new(),
        })
    }

    fn trino() -> MatchedEngines {
        MatchedEngines::single(engine(OWNER))
    }

    /// The protected properties of a server without any trusted engine.
    fn protected() -> HashSet<String> {
        protected_properties(&HashMap::new())
    }

    fn protected_with(owner_properties: &[&str]) -> HashSet<String> {
        owner_properties.iter().copied().map(String::from).collect()
    }

    mod property_writes {
        use std::collections::HashMap;

        use super::{CUSTOM_OWNER, OWNER, engine, protected, protected_with, trino};
        use crate::{
            config::{MatchedEngines, protected_properties},
            server::views::owner_property::{ProtectedPropertyModification, check_property_writes},
            service::events::AuthorizationFailureSource as _,
        };

        fn assert_refused(result: Result<(), ProtectedPropertyModification>) {
            let error = result
                .expect_err("the protected property must be refused")
                .into_error_model();
            assert_eq!(error.code, 403);
            assert_eq!(error.r#type, "ProtectedPropertyModification");
        }

        #[test]
        fn refuses_setting_trino_owner_without_trusted_engines() {
            // A view create carries the property as a key; a commit names it in
            // `set-properties`. Both reach the check as the same key.
            assert_refused(check_property_writes(
                [OWNER].into_iter(),
                Some(&protected()),
                &MatchedEngines::default(),
            ));
        }

        #[test]
        fn refuses_removing_trino_owner_among_other_properties() {
            // `remove-properties` keys are checked like `set-properties` keys.
            assert_refused(check_property_writes(
                ["comment", OWNER, "another.property"].into_iter(),
                Some(&protected()),
                &MatchedEngines::default(),
            ));
        }

        #[test]
        fn refuses_case_variant_of_trino_owner() {
            assert_refused(check_property_writes(
                ["TRINO.run-as-owner"].into_iter(),
                Some(&protected()),
                &MatchedEngines::default(),
            ));
        }

        #[test]
        fn refuses_case_variant_of_trino_owner_from_the_owning_engine() {
            assert_refused(check_property_writes(
                ["TRINO.RUN-AS-OWNER"].into_iter(),
                Some(&protected()),
                &trino(),
            ));
        }

        #[test]
        fn allows_unrelated_properties() {
            let result = check_property_writes(
                ["comment", "spark.query-column-names"].into_iter(),
                Some(&protected()),
                &MatchedEngines::default(),
            );
            assert_eq!(result, Ok(()));
        }

        #[test]
        fn allows_everything_when_the_rules_are_off() {
            let result =
                check_property_writes([OWNER].into_iter(), None, &MatchedEngines::default());
            assert_eq!(result, Ok(()));
        }

        #[test]
        fn allows_trusted_trino_engine_to_set_trino_owner() {
            let result = check_property_writes(
                [OWNER].into_iter(),
                Some(&protected_properties(&HashMap::from([(
                    "trino".to_string(),
                    engine(OWNER),
                )]))),
                &trino(),
            );
            assert_eq!(result, Ok(()));
        }

        #[test]
        fn refuses_trino_owner_from_engine_trusted_for_another_owner_property() {
            // An engine configured with its own owner property does not own the built-in Trino
            // key.
            let protected = protected_properties(&HashMap::from([(
                "custom".to_string(),
                engine(CUSTOM_OWNER),
            )]));
            let custom = MatchedEngines::single(engine(CUSTOM_OWNER));
            assert_refused(check_property_writes(
                [OWNER].into_iter(),
                Some(&protected),
                &custom,
            ));
            let result =
                check_property_writes([CUSTOM_OWNER].into_iter(), Some(&protected), &custom);
            assert_eq!(result, Ok(()));
        }

        #[test]
        fn allows_exact_casing_configured_by_admin() {
            let protected = protected_with(&["Trino.Run-As-Owner"]);
            let result = check_property_writes(
                ["Trino.Run-As-Owner"].into_iter(),
                Some(&protected),
                &MatchedEngines::single(engine("Trino.Run-As-Owner")),
            );
            assert_eq!(result, Ok(()));
        }

        #[test]
        fn refuses_lower_case_when_admin_configured_mixed_case() {
            let protected = protected_with(&["Trino.Run-As-Owner"]);
            assert_refused(check_property_writes(
                [OWNER].into_iter(),
                Some(&protected),
                &MatchedEngines::single(engine("Trino.Run-As-Owner")),
            ));
        }

        #[test]
        fn allows_property_when_any_matched_engine_owns_it() {
            let protected = protected_with(&[OWNER, CUSTOM_OWNER]);
            let matched = MatchedEngines::new(vec![engine(OWNER), engine(CUSTOM_OWNER)]);
            let result = check_property_writes([OWNER].into_iter(), Some(&protected), &matched);
            assert_eq!(result, Ok(()));
        }

        #[test]
        fn refusal_names_the_property_and_the_fix() {
            let error = check_property_writes(
                ["Trino.Run-As-Owner"].into_iter(),
                Some(&protected()),
                &MatchedEngines::default(),
            )
            .expect_err("the protected property must be refused")
            .into_error_model();
            assert_eq!(
                error.message,
                "View property 'Trino.Run-As-Owner' names the user that query engines run this \
                 view as. Only a trusted engine whose owner property is exactly \
                 'trino.run-as-owner' may set, change or remove it. Send the request without \
                 this property. Trino writes it for SECURITY DEFINER views: to create them, \
                 configure Trino as a trusted engine in Lakekeeper (see \"Trusted Engines\" in \
                 the configuration documentation), or create the view with SECURITY INVOKER."
            );
        }

        #[test]
        fn refusal_of_another_owner_property_gives_no_trino_advice() {
            let error = ProtectedPropertyModification {
                key: CUSTOM_OWNER.to_string(),
                protected: CUSTOM_OWNER.to_string(),
            }
            .into_error_model();
            assert_eq!(
                error.message,
                "View property 'custom.run-as-owner' names the user that query engines run this \
                 view as. Only a trusted engine whose owner property is exactly \
                 'custom.run-as-owner' may set, change or remove it. Send the request without \
                 this property."
            );
        }

        #[test]
        fn refusal_of_a_case_variant_names_the_same_protected_key_every_time() {
            // Two configured owner properties that differ only in upper and lower case both
            // match the key. The error names the exact match if there is one, otherwise the
            // first in sorted order.
            for _ in 0..16 {
                // A new set per iteration has a new random hasher state, so a choice that
                // depends on iteration order would differ between iterations.
                let protected = protected_with(&[OWNER, "Trino.Run-As-Owner"]);
                let refused = check_property_writes(
                    ["TRINO.RUN-AS-OWNER"].into_iter(),
                    Some(&protected),
                    &MatchedEngines::default(),
                );
                assert_eq!(
                    refused,
                    Err(ProtectedPropertyModification {
                        key: "TRINO.RUN-AS-OWNER".to_string(),
                        protected: "Trino.Run-As-Owner".to_string(),
                    })
                );
                let refused = check_property_writes(
                    [OWNER].into_iter(),
                    Some(&protected),
                    &MatchedEngines::default(),
                );
                assert_eq!(
                    refused,
                    Err(ProtectedPropertyModification {
                        key: OWNER.to_string(),
                        protected: OWNER.to_string(),
                    })
                );
            }
        }
    }

    mod version_change {
        use std::collections::{BTreeMap, HashMap, HashSet};

        use iceberg::{
            NamespaceIdent,
            spec::{
                NestedField, PrimitiveType, Schema, SqlViewRepresentation, Type as IcebergType,
                ViewFormatVersion, ViewMetadata, ViewMetadataBuilder, ViewRepresentation,
                ViewRepresentations, ViewVersion,
            },
        };

        use super::{CUSTOM_OWNER, OWNER, engine, protected, trino};
        use crate::{
            config::{MatchedEngines, protected_properties},
            server::views::owner_property::{
                PropertyWrites, ProtectedViewVersionChange, check_version_change,
            },
        };

        fn schema(id_doc: Option<&str>) -> Schema {
            let mut id = NestedField::required(1, "id", IcebergType::Primitive(PrimitiveType::Int));
            if let Some(doc) = id_doc {
                id = id.with_doc(doc);
            }
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![id.into()])
                .build()
                .unwrap()
        }

        /// A version of `sql` that resolves unqualified names in `catalog`.`namespace`.
        fn version_in(
            sql: &str,
            dialect: &str,
            catalog: Option<&str>,
            namespace: &str,
        ) -> ViewVersion {
            let builder = ViewVersion::builder()
                .with_schema_id(1)
                .with_timestamp_ms(0)
                .with_default_namespace(NamespaceIdent::new(namespace.to_string()))
                .with_representations(
                    ViewRepresentations::builder()
                        .add_representation(ViewRepresentation::Sql(SqlViewRepresentation {
                            sql: sql.to_string(),
                            dialect: dialect.to_string(),
                        }))
                        .build()
                        .unwrap(),
                );
            match catalog {
                Some(catalog) => builder
                    .with_default_catalog(Some(catalog.to_string()))
                    .build(),
                None => builder.build(),
            }
        }

        fn version(sql: &str, dialect: &str, namespace: &str) -> ViewVersion {
            version_in(sql, dialect, None, namespace)
        }

        /// A view on version 1, `select 1` in Trino's dialect, with the given properties.
        fn view(properties: &[(&str, &str)]) -> ViewMetadata {
            ViewMetadataBuilder::new(
                "s3://bucket/view".to_string(),
                schema(None),
                version("select 1", "trino", "ns"),
                ViewFormatVersion::V1,
                properties
                    .iter()
                    .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                    .collect(),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata
        }

        /// `previous` with `new_version` on `new_schema` added and made current.
        fn with_current_version_on(
            previous: &ViewMetadata,
            new_version: ViewVersion,
            new_schema: Schema,
        ) -> ViewMetadata {
            ViewMetadataBuilder::new_from_metadata(previous.clone())
                .set_current_version(new_version, new_schema)
                .unwrap()
                .build()
                .unwrap()
                .metadata
        }

        /// `previous` with `new_version` added and made current, on the schema of `previous`.
        fn with_current_version(previous: &ViewMetadata, new_version: ViewVersion) -> ViewMetadata {
            with_current_version_on(previous, new_version, schema(None))
        }

        /// `metadata` with an older version made current again.
        fn rolled_back(metadata: &ViewMetadata, version_id: i32) -> ViewMetadata {
            ViewMetadataBuilder::new_from_metadata(metadata.clone())
                .set_current_version_id(version_id)
                .unwrap()
                .build()
                .unwrap()
                .metadata
        }

        /// Writes of a commit that sets and removes the given properties.
        struct Writes {
            updated: BTreeMap<String, String>,
            removed: Vec<String>,
        }

        impl Writes {
            fn none() -> Self {
                Self {
                    updated: BTreeMap::new(),
                    removed: Vec::new(),
                }
            }

            fn set(key: &str, value: &str) -> Self {
                Self {
                    updated: BTreeMap::from([(key.to_string(), value.to_string())]),
                    removed: Vec::new(),
                }
            }

            fn remove(key: &str) -> Self {
                Self {
                    updated: BTreeMap::new(),
                    removed: vec![key.to_string()],
                }
            }

            fn as_writes(&self) -> PropertyWrites<'_> {
                PropertyWrites {
                    updated: &self.updated,
                    removed: &self.removed,
                }
            }
        }

        fn check(
            previous: &ViewMetadata,
            new: &ViewMetadata,
            writes: &Writes,
            protected: &HashSet<String>,
            engines: &MatchedEngines,
        ) -> Result<Vec<String>, ProtectedViewVersionChange> {
            check_version_change(previous, new, writes.as_writes(), Some(protected), engines)
        }

        fn not_owning(owner_property: &str) -> Result<Vec<String>, ProtectedViewVersionChange> {
            Err(ProtectedViewVersionChange {
                owner_property: owner_property.to_string(),
            })
        }

        /// The owner properties an allowed commit removes: the Trino owner property.
        fn owner_removed() -> Vec<String> {
            vec![OWNER.to_string()]
        }

        #[test]
        fn refuses_new_sql_on_definer_view_from_non_engine() {
            let previous = view(&[(OWNER, "alice")]);
            let new = with_current_version(&previous, version("select 2", "trino", "ns"));
            let result = check(
                &previous,
                &new,
                &Writes::none(),
                &protected(),
                &MatchedEngines::default(),
            );
            assert_eq!(result, not_owning(OWNER));
        }

        #[test]
        fn refuses_rollback_on_definer_view_from_non_engine() {
            let first = view(&[(OWNER, "alice")]);
            let second = with_current_version(&first, version("select 2", "trino", "ns"));
            let result = check(
                &second,
                &rolled_back(&second, first.current_version_id()),
                &Writes::none(),
                &protected(),
                &MatchedEngines::default(),
            );
            assert_eq!(result, not_owning(OWNER));
        }

        #[test]
        fn refuses_engine_that_owns_only_one_of_two_owner_properties() {
            let protected = protected_properties(&HashMap::from([(
                "custom".to_string(),
                engine(CUSTOM_OWNER),
            )]));
            let previous = view(&[(OWNER, "alice"), (CUSTOM_OWNER, "alice")]);
            let new = with_current_version(&previous, version("select 2", "trino", "ns"));
            let result = check(
                &previous,
                &new,
                &Writes::set(OWNER, "alice"),
                &protected,
                &trino(),
            );
            assert_eq!(result, not_owning(CUSTOM_OWNER));
        }

        #[test]
        fn allows_any_change_when_the_rules_are_off() {
            let previous = view(&[(OWNER, "alice")]);
            let new = with_current_version(&previous, version("select 2", "trino", "ns"));
            let result = check_version_change(
                &previous,
                &new,
                Writes::none().as_writes(),
                None,
                &MatchedEngines::default(),
            );
            assert_eq!(result, Ok(Vec::new()));
        }

        #[test]
        fn removes_owner_for_new_sql_from_engine_that_does_not_write_it() {
            // Trino's `CREATE OR REPLACE VIEW ... SECURITY INVOKER` on a DEFINER view.
            let previous = view(&[(OWNER, "alice")]);
            let new = with_current_version(&previous, version("select 2", "trino", "ns"));
            let result = check(&previous, &new, &Writes::none(), &protected(), &trino());
            assert_eq!(result, Ok(owner_removed()));
        }

        #[test]
        fn removes_owner_for_rollback_to_older_sql_from_engine_that_does_not_write_it() {
            let first = view(&[(OWNER, "alice")]);
            let second = with_current_version(&first, version("select 2", "trino", "ns"));
            let result = check(
                &second,
                &rolled_back(&second, first.current_version_id()),
                &Writes::none(),
                &protected(),
                &trino(),
            );
            assert_eq!(result, Ok(owner_removed()));
        }

        #[test]
        fn removes_owner_for_new_default_namespace_from_engine_that_does_not_write_it() {
            // The same SQL resolves unqualified names elsewhere.
            let previous = view(&[(OWNER, "alice")]);
            let new = with_current_version(&previous, version("select 1", "trino", "other"));
            let result = check(&previous, &new, &Writes::none(), &protected(), &trino());
            assert_eq!(result, Ok(owner_removed()));
        }

        #[test]
        fn removes_owner_for_new_default_catalog_from_engine_that_does_not_write_it() {
            // Only the catalog that unqualified names resolve against changes.
            let previous = view(&[(OWNER, "alice")]);
            let new = with_current_version(
                &previous,
                version_in("select 1", "trino", Some("other_catalog"), "ns"),
            );
            let result = check(&previous, &new, &Writes::none(), &protected(), &trino());
            assert_eq!(result, Ok(owner_removed()));
        }

        #[test]
        fn keeps_owner_for_new_sql_from_engine_that_writes_it() {
            // Trino's `CREATE OR REPLACE VIEW ... SECURITY DEFINER` sends an owner.
            let previous = view(&[(OWNER, "alice")]);
            let new = with_current_version(&previous, version("select 2", "trino", "ns"));
            let result = check(
                &previous,
                &new,
                &Writes::set(OWNER, "bob"),
                &protected(),
                &trino(),
            );
            assert_eq!(result, Ok(Vec::new()));
        }

        #[test]
        fn allows_new_sql_from_engine_that_removes_the_owner() {
            // Removing the owner property in the same commit also decides whom the new query
            // runs as.
            let previous = view(&[(OWNER, "alice")]);
            let new = with_current_version(&previous, version("select 2", "trino", "ns"));
            let result = check(
                &previous,
                &new,
                &Writes::remove(OWNER),
                &protected(),
                &trino(),
            );
            assert_eq!(result, Ok(Vec::new()));
        }

        #[test]
        fn keeps_owner_for_new_schema_with_same_query_from_engine() {
            // Trino's `COMMENT ON COLUMN`: a new version on a schema with the new column
            // comment, with the same SQL.
            let previous = view(&[(OWNER, "alice")]);
            let new = with_current_version_on(
                &previous,
                version("select 1", "trino", "ns"),
                schema(Some("new comment")),
            );
            assert_ne!(previous.current_version_id(), new.current_version_id());
            let result = check(&previous, &new, &Writes::none(), &protected(), &trino());
            assert_eq!(result, Ok(Vec::new()));
        }

        #[test]
        fn keeps_owner_for_dialect_that_differs_only_in_case_from_engine() {
            let previous = view(&[(OWNER, "alice")]);
            let new = with_current_version(&previous, version("select 1", "TRINO", "ns"));
            assert_ne!(previous.current_version_id(), new.current_version_id());
            let result = check(&previous, &new, &Writes::none(), &protected(), &trino());
            assert_eq!(result, Ok(Vec::new()));
        }

        #[test]
        fn allows_any_version_change_on_view_without_owner() {
            let previous = view(&[("comment", "an invoker view")]);
            let new = with_current_version(&previous, version("select 2", "trino", "ns"));
            let result = check(
                &previous,
                &new,
                &Writes::none(),
                &protected(),
                &MatchedEngines::default(),
            );
            assert_eq!(result, Ok(Vec::new()));
        }

        #[test]
        fn allows_commit_that_keeps_the_current_version() {
            let previous = view(&[(OWNER, "alice")]);
            let new = ViewMetadataBuilder::new_from_metadata(previous.clone())
                .set_properties(HashMap::from([(
                    "comment".to_string(),
                    "new comment".to_string(),
                )]))
                .unwrap()
                .build()
                .unwrap()
                .metadata;
            let result = check(
                &previous,
                &new,
                &Writes::set("comment", "new comment"),
                &protected(),
                &MatchedEngines::default(),
            );
            assert_eq!(result, Ok(Vec::new()));
        }
    }
}
