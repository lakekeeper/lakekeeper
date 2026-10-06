//! The registry every audit type joins, and the small types they share.

use std::{borrow::Cow, fmt, marker::PhantomData};

use serde::Serialize;

use super::emitter::{AuditEmitter, EmitterStamp};

/// A type that reaches the audit log. Implemented only through `#[audit_part]`, which binds
/// the type to the emitter of the crate that defines it and registers it.
pub trait AuditPart: Serialize + schemars::JsonSchema {
    /// The emitter this type belongs to.
    type Emitter: AuditEmitter;
}

/// One name a type puts on the wire, with the doc comment that explains it.
///
/// One struct per name, so a name and its description cannot fall out of step.
#[derive(Clone, Copy, Debug)]
pub struct WireName {
    /// The name as it reaches the wire.
    pub text: &'static str,
    /// The doc comment on the variant, or empty when it has none.
    pub doc: &'static str,
    /// The `context` keys a variant of this vocabulary can put beside itself in the same flat
    /// object. Read from the variant's named fields: `drop` carries `force` and `purge`.
    ///
    /// A field whose own type picks the keys declares them with
    /// `#[audit(expands_to = "a, b")]`; a test checks that against what the emitting code
    /// writes.
    pub carries: &'static [&'static str],
    /// The schema of what a key holds, from the type it holds. `None` for a name that is a
    /// value, or a key whose value is always a string, as an entity's are.
    pub value: Option<fn(&mut schemars::SchemaGenerator) -> schemars::Schema>,
}

/// A key of an authorization record's own `context` object, holding its value. Implemented by
/// `#[audit_part(keys_of = "context")]`, so only such a key can be pushed onto an event.
pub trait RecordContextKey {
    /// The emitter whose vocabulary declares the key.
    type Emitter: AuditEmitter;
    /// The key's name on the wire.
    fn wire(&self) -> WireKey<Self::Emitter>;
    /// The value the key carries, as it reaches the wire.
    fn value(&self) -> serde_json::Value;
}

impl WireName {
    /// Names with no descriptions, from a plain list of strings.
    ///
    /// For a vocabulary registered by hand because its enum lives in a crate that cannot
    /// carry the attribute. The list comes from `strum`'s `VariantNames`, so a renamed variant
    /// still moves the registry; the doc comments stay in that crate.
    ///
    /// # Panics
    ///
    /// If `texts` is not exactly `N` long.
    #[must_use]
    pub const fn from_texts<const N: usize>(texts: &'static [&'static str]) -> [Self; N] {
        assert!(
            texts.len() == N,
            "the length must be the list's own, e.g. `const N: usize = LIST.len();`"
        );
        let mut out = [Self {
            text: "",
            doc: "",
            carries: &[],
            value: None,
        }; N];
        let mut i = 0;
        while i < N {
            out[i] = Self {
                text: texts[i],
                doc: "",
                carries: &[],
                value: None,
            };
            i += 1;
        }
        out
    }
}

/// What role a registered type plays in a record, and what it contributes to the wire.
///
/// The data belongs to the variant that has it, so a part cannot be read as if it carried
/// names and a key vocabulary cannot be read as if its names were a field's values.
#[derive(Clone, Copy, Debug)]
pub enum Kind {
    /// A top-level record structure with an `emit()`, carrying the `record_type` value that
    /// names it.
    Shape { record_type: &'static str },
    /// A nested object of a record, or a data enum such as a subject reference.
    Part,
    /// The `context` object of an operation record.
    Context,
    /// A value vocabulary: these names are the values of one wire field.
    Values {
        /// The wire field the names fill.
        field: &'static str,
        /// Every value the enum can put on that field.
        names: &'static [WireName],
        /// Whether the set can never grow without a major version. An open set lists its
        /// values in `x-audit-values`, so a validator accepts a value a later release adds; a
        /// closed one lists them as an `enum`.
        closed: bool,
    },
    /// A key vocabulary: these names are the keys of one object.
    ///
    /// Separate from a value vocabulary because a new name means something else: a new value
    /// changes no format, a new key is a new field and a minor version.
    Keys {
        /// The object the names are keys of.
        object: &'static str,
        /// Every key the enum can put on that object.
        names: &'static [WireName],
    },
}

impl Kind {
    /// The names this type puts on the wire. Empty for a part, a context or a shape, none of
    /// which contributes a vocabulary.
    #[must_use]
    pub const fn names(&self) -> &'static [WireName] {
        match self {
            Self::Values { names, .. } | Self::Keys { names, .. } => names,
            Self::Shape { .. } | Self::Part | Self::Context => &[],
        }
    }

    /// Where those names go: the field whose values they are, or the object whose keys they
    /// are. `None` for a kind that contributes no vocabulary.
    #[must_use]
    pub const fn wire_place(&self) -> Option<&'static str> {
        match self {
            Self::Values { field, .. } => Some(field),
            Self::Keys { object, .. } => Some(object),
            Self::Shape { .. } | Self::Part | Self::Context => None,
        }
    }

    /// The word for this kind in the schema's `x-audit-kind`.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Shape { .. } => "shape",
            Self::Part => "part",
            Self::Context => "context",
            Self::Values { .. } => "enum",
            Self::Keys { .. } => "keys",
        }
    }
}

/// One registered audit type. Written by `#[audit_part]`, read by the schema builder and the
/// registry tests. Every field is a constant or a function pointer so the entry can live in a
/// `static`.
///
/// **Debug builds only.** `#[audit_part]` emits the entry behind `#[cfg(debug_assertions)]`,
/// so a release binary carries no registry and no schema-generation code. Every reader runs in
/// the dev profile; [`Registration::all`] panics in a `--release` build.
pub struct Registration {
    /// The type's role, and whatever that role carries.
    pub kind: Kind,
    /// `core::any::type_name` of the type, lifetimes made `'static`.
    pub type_name: fn() -> &'static str,
    /// The defining crate's emitter.
    pub emitter: EmitterStamp,
    /// `CARGO_PKG_NAME` of the defining crate.
    pub defining_crate: &'static str,
    /// Whether this vocabulary's values are spelled somewhere else. When they are, the case
    /// check leaves them alone; everything this log names itself is `lower_snake_case`.
    pub external_values: bool,
    /// The type's name under `$defs`: its schemars name for a part, its identifier for a
    /// vocabulary.
    pub def_name: fn() -> Cow<'static, str>,
    /// The type's JSON Schema. `None` for a vocabulary.
    pub schema: Option<fn(&mut schemars::SchemaGenerator) -> schemars::Schema>,
}

inventory::collect!(Registration);

impl Registration {
    /// Whether this build carries the registry at all: true in the dev profile, false in
    /// release, where `#[audit_part]` emits no entries.
    #[must_use]
    pub const fn available() -> bool {
        cfg!(debug_assertions)
    }

    /// Every registration linked into this binary, all emitters.
    ///
    /// # Panics
    ///
    /// In a build without `debug_assertions`, always: such a build has no registry, and an
    /// empty one would read as "type X is not registered".
    pub fn all() -> impl Iterator<Item = &'static Registration> {
        assert!(
            Self::available(),
            "the audit registry exists in debug builds only: `#[audit_part]` registers types \
             behind `cfg(debug_assertions)`. Run registry and schema tests in the dev profile, \
             not with `--release`."
        );
        inventory::iter::<Registration>.into_iter()
    }

    /// The registrations of one emitter.
    pub fn for_emitter<E: AuditEmitter>() -> impl Iterator<Item = &'static Registration> {
        Self::all().filter(|r| r.emitter.name == E::NAME)
    }
}

impl fmt::Debug for Registration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Registration")
            .field("kind", &self.kind)
            .field("type_name", &(self.type_name)())
            .field("emitter", &self.emitter.name)
            .field("defining_crate", &self.defining_crate)
            .finish_non_exhaustive()
    }
}

/// A value vocabulary: an enum whose variant names are the values of one wire field.
/// Implemented by `#[audit_part(field = "...")]`, and by hand for a vocabulary whose enum
/// lives in a crate that cannot carry the attribute.
pub trait Vocabulary: Sized + 'static {
    /// The emitter whose vocabulary this is.
    type Emitter: AuditEmitter;
    /// The vocabulary's name under `$defs`, which a field holding one of its values points at.
    const SCHEMA_NAME: &'static str;
    /// This value as it reaches the wire.
    fn wire(&self) -> Wire<Self>;
}

/// The vocabulary of an operation record's `operation`, so that one cannot be passed where an
/// `outcome` belongs. Implemented by `#[audit_part(field = "operation")]`.
pub trait OperationValues: Vocabulary {}

/// The vocabulary of an operation record's `outcome`. Implemented by
/// `#[audit_part(field = "outcome")]`.
pub trait OutcomeValues: Vocabulary {}

/// A value from the vocabulary `T` as it reaches the wire. Obtainable only from the
/// vocabulary's generated `as_wire()`.
///
/// Serializes as the value's name, and its schema is a reference to `T`'s definition, so a
/// field holding one points at the set it is drawn from. The type says which set, so a value
/// from one vocabulary cannot be put in a field that holds another.
pub struct Wire<T> {
    text: &'static str,
    _vocabulary: PhantomData<fn() -> T>,
}

impl<T> Wire<T> {
    /// Constructed by `#[audit_part]`-generated code. Not part of the public API.
    #[doc(hidden)]
    #[must_use]
    pub const fn new(text: &'static str) -> Self {
        Self {
            text,
            _vocabulary: PhantomData,
        }
    }

    /// The value as written to the wire.
    #[must_use]
    pub const fn text(self) -> &'static str {
        self.text
    }
}

impl<T> Clone for Wire<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for Wire<T> {}
impl<T> PartialEq for Wire<T> {
    fn eq(&self, other: &Self) -> bool {
        self.text == other.text
    }
}
impl<T> Eq for Wire<T> {}
impl<T> fmt::Debug for Wire<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Wire({:?})", self.text)
    }
}
impl<T> fmt::Display for Wire<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.text)
    }
}
impl<T> Serialize for Wire<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.text)
    }
}
impl<T: Vocabulary> schemars::JsonSchema for Wire<T> {
    fn inline_schema() -> bool {
        true
    }
    fn schema_name() -> Cow<'static, str> {
        Cow::Owned(format!("Wire<{}>", T::SCHEMA_NAME))
    }
    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({ "$ref": format!("#/$defs/{}", T::SCHEMA_NAME) })
    }
}

/// A key of an audit object as it reaches the wire, tied to the emitter whose vocabulary it
/// belongs to. Obtainable only from a key enum's generated `as_wire()`.
///
/// Convertible into neither [`Wire`] nor [`AnyWireStr`], and not serializable, so a key
/// cannot be passed by accident where a record expects a value. [`Self::text`] and the
/// generated `as_str` still yield the plain string.
pub struct WireKey<E: AuditEmitter> {
    text: &'static str,
    _emitter: PhantomData<E>,
}

/// The impls a wire-name type gets whatever it names. Derives cannot supply them: the
/// derived bounds would demand `Clone`, `Eq` and `Debug` of the emitter, which carries no
/// data and needs none of them.
macro_rules! wire_name_impls {
    ($ty:ident) => {
        impl<E: AuditEmitter> $ty<E> {
            /// Constructed by `#[audit_part]`-generated code. Not part of the public API.
            #[doc(hidden)]
            #[must_use]
            pub const fn new(text: &'static str) -> Self {
                Self {
                    text,
                    _emitter: PhantomData,
                }
            }

            /// The name as written to the wire.
            #[must_use]
            pub const fn text(self) -> &'static str {
                self.text
            }
        }

        impl<E: AuditEmitter> Clone for $ty<E> {
            fn clone(&self) -> Self {
                *self
            }
        }
        impl<E: AuditEmitter> Copy for $ty<E> {}
        impl<E: AuditEmitter> PartialEq for $ty<E> {
            fn eq(&self, other: &Self) -> bool {
                self.text == other.text
            }
        }
        impl<E: AuditEmitter> Eq for $ty<E> {}
        impl<E: AuditEmitter> fmt::Debug for $ty<E> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}<{}>({:?})", stringify!($ty), E::NAME, self.text)
            }
        }
        impl<E: AuditEmitter> fmt::Display for $ty<E> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.text)
            }
        }
    };
}

wire_name_impls!(WireKey);

/// A wire value whose emitter is known only by name: what a core field that accepts values
/// from several emitters holds, such as `action_name`. Obtainable only from a [`Wire`], so it
/// still cannot be a literal.
///
/// The type-erased form of [`Wire`], for the two places that cannot name their emitter in a
/// type: an action's name, which any authorizer crate supplies, and a context key pushed by a
/// crate this one does not know.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct AnyWireStr {
    text: &'static str,
    emitter: EmitterStamp,
}

impl AnyWireStr {
    /// The value as written to the wire.
    #[must_use]
    pub const fn text(self) -> &'static str {
        self.text
    }

    /// The emitter whose vocabulary the value belongs to, with its format version. Carried
    /// with the value because the registry, which could also answer this, exists in debug
    /// builds only.
    #[must_use]
    pub const fn emitter(self) -> EmitterStamp {
        self.emitter
    }
}

impl<T: Vocabulary> From<Wire<T>> for AnyWireStr {
    fn from(value: Wire<T>) -> Self {
        Self {
            text: value.text,
            emitter: EmitterStamp::of::<T::Emitter>(),
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
        write!(f, "AnyWireStr<{}>({:?})", self.emitter.name, self.text)
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
    /// Marked open: the value comes from whichever emitter produced the record, so no one
    /// emitter's schema can list every value. The schema builder leaves such a field a plain
    /// string with no value set.
    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({ "type": "string", "x-audit-open": true })
    }
}

/// The `tracing` target every audit record is written to, and the one [`enabled`] asks about.
///
/// A fixed name, not a module path, so an operator filter keeps working when the code moves:
/// `RUST_LOG=warn,lakekeeper::audit=info` keeps audit lines and quiets the rest.
pub const AUDIT_TARGET: &str = "lakekeeper::audit";

/// Module paths that an operator filter may name expecting to select audit records.
///
/// Records are written to [`AUDIT_TARGET`] only, so a filter naming one of these matches
/// none of them. Read by [`warn_on_retired_audit_filter`].
const RETIRED_TARGETS: &[&str] = &[
    // Authorization, replay and grant records, emitted from the audit module itself.
    "lakekeeper::service::events::backends::audit",
    // Admission rejections, emitted from the gate.
    "lakekeeper::service::admission",
];

/// The directives in `filter` that name a path from [`RETIRED_TARGETS`] and so select no
/// audit record.
///
/// A directive selects a target by prefix. One that is a prefix of a retired path but not of
/// [`AUDIT_TARGET`] matches nothing. A broader directive such as `lakekeeper` matches audit
/// records too, and is not reported.
pub(super) fn retired_audit_directives(filter: &str) -> Vec<&str> {
    filter
        .split(',')
        .map(|directive| directive.split('=').next().unwrap_or(directive).trim())
        .filter(|target| {
            !target.is_empty()
                && RETIRED_TARGETS.iter().any(|old| old.starts_with(*target))
                && !AUDIT_TARGET.starts_with(target)
        })
        .collect()
}

/// Warn when the log filter selects audit records by a module path, which matches none.
///
/// Called once at start-up. Such a filter fails silently: it matches nothing, so the operator
/// sees an empty audit stream and no reason for it.
///
/// Written to standard error, not through `tracing`: the filter this warns about could
/// suppress the warning itself.
pub fn warn_on_retired_audit_filter() {
    let Ok(filter) = std::env::var("RUST_LOG") else {
        return;
    };
    let retired = retired_audit_directives(&filter);
    if retired.is_empty() {
        return;
    }
    eprintln!(
        "warning: RUST_LOG selects audit records by {retired:?}, which matches none of \
         them. Audit records are emitted on the fixed target `{AUDIT_TARGET}`, not on a \
         module path. See docs/docs/logging.md."
    );
}

/// Whether an audit record emitted now reaches the log.
///
/// Two switches must both be open: `LAKEKEEPER__AUDIT__TRACING__ENABLED`, which decides
/// whether the catalog keeps an audit trail at all, and the `tracing` filter on
/// [`AUDIT_TARGET`], which decides whether the subscriber records what is written there.
///
/// Every record passes this on its way out, so the configuration switch also covers records
/// emitted outside the event listener. Code that suppresses an ordinary log line because the
/// audit record covers it must ask this first: with either switch closed the event would be
/// in neither log.
///
/// `tracing` caches its half per call site when the filter is static.
#[must_use]
pub fn enabled() -> bool {
    crate::CONFIG.audit.tracing.enabled
        && tracing::enabled!(target: AUDIT_TARGET, tracing::Level::INFO)
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
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum ProbeOutcome {
        /// Everything went fine.
        AllGood,
        /// Renamed explicitly.
        #[audit(rename = "renamed_value")]
        Renamed,
        /// Carries data; only the name reaches the wire.
        #[allow(dead_code)]
        WithData {
            /// Ignored by the wire value.
            n: u8,
        },
    }

    /// A key vocabulary declared the way any crate declares one.
    #[audit_part(keys_of = "probe")]
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum ProbeKey {
        /// The only key.
        FirstKey(bool),
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
        let good: Wire<ProbeOutcome> = ProbeOutcome::AllGood.as_wire();
        assert_eq!(good.text(), "all_good");
        assert_eq!(ProbeOutcome::Renamed.as_wire().text(), "renamed_value");
        assert_eq!(
            ProbeOutcome::WithData { n: 1 }.as_wire().text(),
            "with_data"
        );
        // `as_str` is generated from the same match as `as_wire`, so the two cannot answer
        // differently and neither can drift from the value the registry declares.
        assert_eq!(ProbeOutcome::AllGood.as_str(), good.text());
        assert_eq!(ProbeOutcome::Renamed.as_str(), "renamed_value");
        assert_eq!(ProbeOutcome::WIRE_VARIANTS.len(), 3);
        assert_eq!(
            ProbeOutcome::WIRE_NAMES,
            ["all_good", "renamed_value", "with_data"]
        );
        assert_eq!(serde_json::to_value(good).expect("serializes"), "all_good");
        assert_eq!(good.to_string(), "all_good");
        let any: AnyWireStr = good.into();
        assert_eq!(any, "all_good");
        assert_eq!(any.emitter().name, "lakekeeper");
    }

    #[test]
    fn a_key_vocabulary_yields_a_key_type_that_no_value_field_accepts() {
        let key: WireKey<Lakekeeper> = ProbeKey::FirstKey(true).as_wire();
        assert_eq!(key.text(), "first_key");
        assert_eq!(ProbeKey::FirstKey(true).as_str(), "first_key");
        assert_eq!(ProbeKey::WIRE_NAMES, ["first_key"]);
        assert_eq!(key.to_string(), "first_key");
        // `WireKey` implements neither `Serialize` nor `Into<AnyWireStr>`, so a key cannot be
        // written into a field that holds a value. The compiler enforces that, not this test.
    }

    #[test]
    fn declared_types_are_registered_with_this_crate_emitter() {
        let regs: Vec<&Registration> = Registration::for_emitter::<Lakekeeper>().collect();
        let by_name = |needle: &str| {
            regs.iter()
                .find(|r| (r.type_name)().trim_end_matches("<'_>").ends_with(needle))
                .unwrap_or_else(|| panic!("{needle} is registered; have {regs:#?}"))
        };
        let texts = |reg: &Registration| -> Vec<&str> {
            reg.kind.names().iter().map(|name| name.text).collect()
        };

        let outcome = by_name("ProbeOutcome");
        assert_eq!(outcome.kind.wire_place(), Some("probe_outcome"));
        assert_eq!(texts(outcome), ["all_good", "renamed_value", "with_data"]);
        assert!(matches!(outcome.kind, Kind::Values { .. }));
        assert!(outcome.schema.is_none());
        assert_eq!(outcome.emitter.name, "lakekeeper");
        assert_eq!(outcome.defining_crate, "lakekeeper");

        let key = by_name("ProbeKey");
        assert!(matches!(key.kind, Kind::Keys { .. }));
        assert_eq!(key.kind.wire_place(), Some("probe"));
        // Each name carries its own description, so the two cannot fall out of step.
        let [name] = key.kind.names() else {
            panic!("one key: {:?}", key.kind.names());
        };
        assert_eq!((name.text, name.doc), ("first_key", "The only key."));
        assert!(name.carries.is_empty());
        let mut generator = schemars::SchemaGenerator::default();
        let held = (name.value.expect("a key of `probe` holds its value"))(&mut generator);
        assert_eq!(held.to_value(), serde_json::json!({ "type": "boolean" }));

        let part = by_name("ProbePart");
        assert!(matches!(part.kind, Kind::Part));
        assert_eq!(part.kind.wire_place(), None);
        assert!(part.kind.names().is_empty());
    }

    #[test]
    fn a_registered_schema_carries_the_doc_comments() {
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
        assert_eq!((reg.def_name)(), "ProbePart");
    }

    #[test]
    fn every_registration_of_this_emitter_comes_from_this_workspace() {
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
        assert!(is_emitter_name("lakekeeper_plus"));
        assert!(is_emitter_name("lakekeeper2"));
        assert!(!is_emitter_name(""));
        assert!(!is_emitter_name("Lakekeeper"));
        assert!(!is_emitter_name("1st"));
        assert!(!is_emitter_name("lakekeeper-plus"));
        assert!(!is_emitter_name("lakekeeper__plus"));
        assert!(!is_emitter_name("lakekeeper_"));
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
