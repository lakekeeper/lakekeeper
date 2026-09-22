//! The registry every audit type joins, and the small types they share.

use std::{borrow::Cow, fmt, marker::PhantomData};

use serde::Serialize;

use super::emitter::AuditEmitter;

/// A type that reaches the audit log. Implemented only through `#[audit_part]`, which binds
/// the type to the emitter of the crate that defines it and registers it.
pub trait AuditPart: Serialize + schemars::JsonSchema {
    /// The emitter this type belongs to.
    type Emitter: AuditEmitter;
}

/// What role a registered type plays in a record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// A top-level record structure with an `emit()`.
    Shape,
    /// A nested object of a record, or a data enum such as a subject reference.
    Part,
    /// A vocabulary enum: its variants are the values of one wire field.
    Enum,
    /// The `context` object of an operation record.
    Context,
}

/// One registered audit type. Written by `#[audit_part]`, read by the schema builder and the
/// registry tests. Every field is a constant or a function pointer so the entry can live in a
/// `static`.
///
/// **Present in debug builds only.** `#[audit_part]` emits the entry behind
/// `#[cfg(debug_assertions)]`, so a release binary carries no registry and none of the
/// schema-generation code the entries keep alive. Every reader of the registry runs in the
/// dev profile: the unit tests, `just update-audit-schema`, and any tooling built on
/// [`Registration::all`]. In a `--release` test build the registry is empty; call
/// [`Registration::require_registry`] first so the failure names the cause.
pub struct Registration {
    /// The type's role.
    pub kind: Kind,
    /// `core::any::type_name` of the type, lifetimes made `'static`.
    pub type_name: fn() -> &'static str,
    /// `AuditEmitter::NAME` of the defining crate's emitter.
    pub emitter_name: &'static str,
    /// `core::any::type_name` of the defining crate's emitter type.
    pub emitter_type: fn() -> &'static str,
    /// `AuditEmitter::FORMAT` of the defining crate's emitter.
    pub emitter_format: &'static str,
    /// `CARGO_PKG_NAME` of the defining crate.
    pub defining_crate: &'static str,
    /// The type's schema name. `None` for a vocabulary enum, whose schema is its value list.
    pub schema_name: Option<fn() -> Cow<'static, str>>,
    /// The type's JSON Schema. `None` for a vocabulary enum.
    pub schema: Option<fn(&mut schemars::SchemaGenerator) -> schemars::Schema>,
    /// For a vocabulary enum, the wire field its values belong to.
    pub wire_field: Option<&'static str>,
    /// For a vocabulary enum, every value it can put on the wire. Empty otherwise.
    pub wire_values: &'static [&'static str],
}

inventory::collect!(Registration);

impl Registration {
    /// Whether this build carries the registry at all: true in the dev profile, false in
    /// release, where `#[audit_part]` emits no entries.
    #[must_use]
    pub const fn available() -> bool {
        cfg!(debug_assertions)
    }

    /// Stops a registry-dependent test in a build that has no registry, with the reason.
    ///
    /// # Panics
    ///
    /// In a build without `debug_assertions`, always. That is the point: the registry is a
    /// debug-build facility, and a test that reads it in `--release` would otherwise fail on
    /// "type X is not registered" and send the reader looking for a bug that is not there.
    pub fn require_registry() {
        assert!(
            Self::available(),
            "the audit registry exists in debug builds only: `#[audit_part]` registers types \
             behind `cfg(debug_assertions)`. Run registry and schema tests in the dev profile, \
             not with `--release`."
        );
    }

    /// Every registration linked into this binary, all emitters. Empty in release builds; see
    /// [`Registration::require_registry`].
    pub fn all() -> impl Iterator<Item = &'static Registration> {
        inventory::iter::<Registration>.into_iter()
    }

    /// The registrations of one emitter.
    pub fn for_emitter<E: AuditEmitter>() -> impl Iterator<Item = &'static Registration> {
        Self::all().filter(|r| r.emitter_name == E::NAME)
    }
}

impl fmt::Debug for Registration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Registration")
            .field("kind", &self.kind)
            .field("type_name", &(self.type_name)())
            .field("emitter_name", &self.emitter_name)
            .field("defining_crate", &self.defining_crate)
            .field("wire_field", &self.wire_field)
            .field("wire_values", &self.wire_values)
            .finish_non_exhaustive()
    }
}

/// A closed-set value as it reaches the wire, tied to the emitter whose vocabulary it belongs
/// to. Obtainable only from a vocabulary enum's generated `as_wire()`.
///
/// Two types, not one: here the emitter is known at compile time and costs nothing to carry,
/// so mixing two emitters' vocabularies in one record does not compile. Where it cannot be
/// known, the value is an [`AnyWireStr`] and the emitter is a field. Folding them into one
/// type with a default parameter would hide that difference behind a field that is dead
/// whenever the emitter is known.
pub struct WireStr<E: AuditEmitter> {
    text: &'static str,
    _emitter: PhantomData<E>,
}

impl<E: AuditEmitter> WireStr<E> {
    /// Constructed by `#[audit_part]`-generated code. Not part of the public API.
    #[doc(hidden)]
    #[must_use]
    pub const fn new(text: &'static str) -> Self {
        Self {
            text,
            _emitter: PhantomData,
        }
    }

    /// The value as written to the wire.
    #[must_use]
    pub const fn text(self) -> &'static str {
        self.text
    }
}

impl<E: AuditEmitter> Clone for WireStr<E> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<E: AuditEmitter> Copy for WireStr<E> {}
impl<E: AuditEmitter> PartialEq for WireStr<E> {
    fn eq(&self, other: &Self) -> bool {
        self.text == other.text
    }
}
impl<E: AuditEmitter> Eq for WireStr<E> {}
impl<E: AuditEmitter> fmt::Debug for WireStr<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WireStr<{}>({:?})", E::NAME, self.text)
    }
}
impl<E: AuditEmitter> fmt::Display for WireStr<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.text)
    }
}
impl<E: AuditEmitter> Serialize for WireStr<E> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.text)
    }
}
impl<E: AuditEmitter> schemars::JsonSchema for WireStr<E> {
    fn inline_schema() -> bool {
        true
    }
    fn schema_name() -> Cow<'static, str> {
        Cow::Borrowed("WireStr")
    }
    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({ "type": "string" })
    }
}

/// A wire value whose emitter is known only by name: what a core field that accepts values
/// from several emitters holds, such as `action_name`. Obtainable only from a [`WireStr`],
/// so it still cannot be a literal.
///
/// The erased half of the pair described on [`WireStr`]. It exists because two places cannot
/// name their emitter in a type: an action's name, which any authorizer crate supplies, and a
/// context key pushed by a crate this one does not know.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct AnyWireStr {
    text: &'static str,
    emitter: &'static str,
}

impl AnyWireStr {
    /// The value as written to the wire.
    #[must_use]
    pub const fn text(self) -> &'static str {
        self.text
    }

    /// `AuditEmitter::NAME` of the emitter whose vocabulary the value belongs to.
    #[must_use]
    pub const fn emitter(self) -> &'static str {
        self.emitter
    }

    /// A value from nowhere, for tests that build descriptors by hand. Attributed to
    /// `lakekeeper`.
    #[cfg(any(test, feature = "test-utils"))]
    #[must_use]
    pub const fn literal_for_tests(text: &'static str) -> Self {
        Self {
            text,
            emitter: "lakekeeper",
        }
    }
}

impl<E: AuditEmitter> From<WireStr<E>> for AnyWireStr {
    fn from(value: WireStr<E>) -> Self {
        Self {
            text: value.text,
            emitter: E::NAME,
        }
    }
}
impl PartialEq<str> for AnyWireStr {
    fn eq(&self, other: &str) -> bool {
        self.text == other
    }
}
impl PartialEq<&str> for AnyWireStr {
    fn eq(&self, other: &&str) -> bool {
        self.text == *other
    }
}
impl fmt::Debug for AnyWireStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AnyWireStr<{}>({:?})", self.emitter, self.text)
    }
}
impl fmt::Display for AnyWireStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.text)
    }
}
impl Serialize for AnyWireStr {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.text)
    }
}
impl schemars::JsonSchema for AnyWireStr {
    fn inline_schema() -> bool {
        true
    }
    fn schema_name() -> Cow<'static, str> {
        Cow::Borrowed("AnyWireStr")
    }
    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({ "type": "string" })
    }
}

/// The `tracing` target of every audit record. Independent of module paths, so operators can
/// filter on it: `RUST_LOG=warn,lakekeeper::audit=info` keeps audit lines and quiets the rest.
pub const AUDIT_TARGET: &str = "lakekeeper::audit";

/// Whether an audit record emitted now would be recorded by the installed subscriber.
///
/// Callers on a hot path check this before building a record, and the audit listener checks
/// it before enrichment and assembly, so a filtered-out audit log costs no lookup and no
/// allocation. `tracing` caches the answer per call site when the filter is static.
#[must_use]
pub fn enabled() -> bool {
    tracing::enabled!(target: AUDIT_TARGET, tracing::Level::INFO)
}

#[cfg(test)]
mod tests {
    use tracing_subscriber::{EnvFilter, layer::SubscriberExt as _, util::SubscriberInitExt as _};

    use super::*;
    use crate::{
        Lakekeeper,
        audit::{audit_part, is_emitter_name},
    };

    /// A vocabulary enum declared the way any crate declares one.
    #[audit_part(field = "probe_outcome")]
    #[audit(rename_all = "snake_case")]
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum ProbeOutcome {
        /// Everything went fine.
        AllGood,
        /// Renamed explicitly.
        #[audit(rename = "x-y")]
        Renamed,
        /// Carries data; only the name reaches the wire.
        #[allow(dead_code)]
        WithData {
            /// Ignored by the wire value.
            n: u8,
        },
    }

    /// A part declared the way any crate declares one.
    #[audit_part]
    struct ProbePart<'a> {
        /// The only field.
        name: &'a str,
        /// Absent when `None`.
        #[serde(skip_serializing_if = "Option::is_none")]
        count: Option<u32>,
    }

    #[test]
    fn a_vocabulary_enum_yields_serde_renamed_wire_values_tied_to_the_crate_emitter() {
        let good: WireStr<Lakekeeper> = ProbeOutcome::AllGood.as_wire();
        assert_eq!(good.text(), "all_good");
        assert_eq!(ProbeOutcome::Renamed.as_wire().text(), "x-y");
        assert_eq!(
            ProbeOutcome::WithData { n: 1 }.as_wire().text(),
            "with_data"
        );
        assert_eq!(ProbeOutcome::WIRE_VARIANTS.len(), 3);
        assert_eq!(ProbeOutcome::WIRE_NAMES, ["all_good", "x-y", "with_data"]);
        assert_eq!(serde_json::to_value(good).expect("serializes"), "all_good");
        assert_eq!(good.to_string(), "all_good");
        let any: AnyWireStr = good.into();
        assert_eq!(any, "all_good");
        assert_eq!(any.emitter(), "lakekeeper");
    }

    #[test]
    fn declared_types_are_registered_with_this_crate_emitter() {
        Registration::require_registry();
        let regs: Vec<&Registration> = Registration::for_emitter::<Lakekeeper>().collect();
        let by_name = |needle: &str| {
            regs.iter()
                .find(|r| (r.type_name)().trim_end_matches("<'_>").ends_with(needle))
                .unwrap_or_else(|| panic!("{needle} is registered; have {regs:#?}"))
        };
        let outcome = by_name("ProbeOutcome");
        assert_eq!(outcome.kind, Kind::Enum);
        assert_eq!(outcome.wire_field, Some("probe_outcome"));
        assert_eq!(outcome.wire_values, ["all_good", "x-y", "with_data"]);
        assert!(outcome.schema.is_none());
        assert_eq!(outcome.emitter_name, "lakekeeper");
        assert_eq!(outcome.defining_crate, "lakekeeper");
        assert!((outcome.emitter_type)().ends_with("Lakekeeper"));

        let part = by_name("ProbePart");
        assert_eq!(part.kind, Kind::Part);
        assert_eq!(part.wire_field, None);
        assert!(part.wire_values.is_empty());

        let no_context = by_name("NoContext");
        assert_eq!(no_context.kind, Kind::Context);
    }

    #[test]
    fn a_registered_schema_carries_the_doc_comments() {
        Registration::require_registry();
        let reg = Registration::for_emitter::<Lakekeeper>()
            .find(|r| {
                (r.type_name)()
                    .trim_end_matches("<'_>")
                    .ends_with("ProbePart")
            })
            .expect("registered");
        let mut generator = schemars::SchemaGenerator::default();
        let schema = (reg.schema.expect("a part has a schema"))(&mut generator);
        let json = schema.to_value();
        assert_eq!(json["properties"]["name"]["description"], "The only field.");
        assert_eq!(json["required"], serde_json::json!(["name"]));
        assert_eq!(
            (reg.schema_name.expect("a part has a schema name"))(),
            "ProbePart"
        );
    }

    #[test]
    fn every_registration_of_this_emitter_comes_from_this_workspace() {
        Registration::require_registry();
        for reg in Registration::for_emitter::<Lakekeeper>() {
            assert!(
                matches!(
                    reg.defining_crate,
                    "lakekeeper" | "lakekeeper-authz-openfga"
                ),
                "{} claims the lakekeeper emitter from crate {}",
                (reg.type_name)(),
                reg.defining_crate
            );
            assert!((reg.emitter_type)().ends_with("::Lakekeeper"), "{reg:?}");
        }
    }

    fn enabled_under(filter: &str) -> bool {
        let subscriber = tracing_subscriber::registry()
            .with(EnvFilter::new(filter))
            .with(tracing_subscriber::fmt::layer().with_writer(std::io::sink));
        let _guard = subscriber.set_default();
        enabled()
    }

    #[test]
    fn the_gate_follows_the_installed_filter() {
        assert!(!enabled_under("warn"));
        assert!(enabled_under("info"));
        assert!(enabled_under("warn,lakekeeper::audit=info"));
        assert!(!enabled_under("info,lakekeeper::audit=warn"));
    }

    #[test]
    fn emitter_name_and_format_rules() {
        assert!(is_emitter_name("lakekeeper"));
        assert!(is_emitter_name("lakekeeper-plus"));
        assert!(!is_emitter_name(""));
        assert!(!is_emitter_name("Lakekeeper"));
        assert!(!is_emitter_name("1st"));
        assert!(!is_emitter_name("lake_keeper"));
        assert!(super::super::is_major_minor("1.0"));
        assert!(!super::super::is_major_minor("1.0.0"));
    }

    #[test]
    fn emitter_constants_are_the_declared_ones() {
        assert_eq!(<Lakekeeper as AuditEmitter>::NAME, "lakekeeper");
        assert_eq!(
            <Lakekeeper as AuditEmitter>::FORMAT,
            super::super::AUDIT_FORMAT
        );
    }
}
