//! `#[audit_part]`: the one attribute every type that reaches the Lakekeeper audit log
//! carries.
//!
//! Five placements:
//!
//! - **A part** (`#[audit_part]` on a struct or a data enum): a nested object of a record.
//!   Expands to `#[derive(Serialize, JsonSchema)]` through `lakekeeper`'s re-exports, so the
//!   defining crate needs no direct dependency on either; to
//!   `impl lakekeeper::audit::AuditPart for T { type Emitter = crate::audit_emitter::Emitter; }`,
//!   binding the type to the emitter of the crate that defines it; and to a registry entry.
//!   Every named field needs a doc comment: it is the field's description in the schema.
//! - **A context** (`#[audit_part(context)]` on a struct): the `context` object of an operation
//!   record. Same as a part, registered as a context.
//! - **A shape** (`#[audit_part(shape = "authorization")]` on a struct, in `lakekeeper` only): a
//!   whole record. Expands to its schema, its registry entry, and its `emit(self, message)`:
//!   the audit gate, then one `tracing::info!` stamping `event_source`, `audit_format` and
//!   `record_type`, with every field under its own name and a field that serializes to `null`
//!   left off. The field list is the struct's, so the record and its schema cannot differ.
//! - **A value vocabulary** (`#[audit_part(field = "outcome")]`): its variant *names* are the
//!   values of one wire field. Variants may carry data; only the name reaches the wire. Expands
//!   to `as_wire(&self) -> Wire<Self>`, `as_str(&self) -> &'static str`, and
//!   `WIRE_VARIANTS` / `WIRE_NAMES`, and to a registry entry whose `Kind::Values` carries the
//!   field and every value with its doc comment. No derives are added and `AuditPart` is not
//!   implemented: such an enum often already serializes differently for an API.
//!   Outside Lakekeeper only `keys_of = "context"` is accepted: the other objects take their
//!   keys from a Lakekeeper enum, so a vocabulary declared elsewhere could not be emitted.
//! - **A key vocabulary** (`#[audit_part(keys_of = "entity")]`): its variant names are the
//!   *keys* of one object, here an `entity`. Same expansion, except that `as_wire` yields a
//!   `WireKey<Emitter>`, which converts into no value type — so a key cannot be passed where a
//!   value is expected, or the other way round — and the registry entry is a `Kind::Keys`.
//!   The distinction is not cosmetic: a new value on
//!   a field changes no format, while a new key is a new field, which is a minor version.
//!
//! Every name a vocabulary puts on the wire is its variant name in `snake_case`, or the name
//! `#[audit(rename = "...")]` gives that variant. `strum` and `serde` attributes are not read:
//! an enum that is also an API type keeps its API spelling to itself. The macro rejects any
//! name that is not `lower_snake_case`, unless the vocabulary is spelled somewhere else —
//! another product publishes the values, or they mirror a vocabulary that does — in which case
//! it adds `external_values` and says how with `#[audit(rename_all = "...")]`. Name that
//! vocabulary in the enum's doc comment.
//!
//! Each placement takes a closed set of `#[audit(...)]` keys, each `key = "string"`: a
//! vocabulary enum takes `rename_all` (with `external_values` only); a variant of a value
//! vocabulary takes `rename` and `carries`; a field of such a variant takes `expands_to`, which
//! every unnamed field of an action needs; a variant of a key vocabulary takes `rename`; a
//! part takes none. An unknown key, a key given twice and a value that is not a string are
//! rejected. The attributes are consumed here and never reach the compiler.
//!
//! Registry entries exist in **debug builds only**: the entry is behind
//! `#[cfg(debug_assertions)]`, so release binaries carry neither the registry nor the
//! schema-generation code it keeps alive. Tests that read the registry run in the dev profile;
//! in a `--release` test build the registry is empty, and
//! `lakekeeper::audit::Registration::require_registry()` says so instead of failing obscurely.
//!
//! Rules enforced at expansion: no type or const generic parameters (lifetimes are fine and
//! become `'static` in the registry); every named field of a part or context has a doc
//! comment.

use std::collections::BTreeMap;

use heck::{
    ToKebabCase, ToLowerCamelCase, ToPascalCase, ToShoutyKebabCase, ToShoutySnakeCase, ToSnakeCase,
};
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{
    Attribute, Data, DeriveInput, Error, Expr, Fields, GenericParam, Lit, Meta, Result, Token,
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
};

/// The parsed attribute arguments.
#[derive(Default)]
struct Args {
    /// `field = "outcome"`: this enum's variant names are the values of that wire field.
    field: Option<String>,
    /// `keys_of = "entity"`: this enum's variant names are the keys of that object.
    keys_of: Option<String>,
    /// `context`: this struct is the `context` object of an operation record.
    context: bool,
    /// `shape = "authorization"`: this struct is a whole record, and that is the
    /// `record_type` value naming it.
    shape: Option<String>,
    /// `external_values`: these values are spelled somewhere else — another product
    /// publishes them, or they mirror a vocabulary that does — so the case check skips them.
    external_values: bool,
}

impl Parse for Args {
    fn parse(input: ParseStream) -> Result<Self> {
        let mut args = Args::default();
        let metas = Punctuated::<Meta, Token![,]>::parse_terminated(input)?;
        for meta in metas {
            match &meta {
                Meta::Path(p) if p.is_ident("context") => args.context = true,
                Meta::Path(p) if p.is_ident("external_values") => args.external_values = true,
                Meta::NameValue(nv) if nv.path.is_ident("shape") => {
                    args.shape = Some(lit_str(&nv.value, "shape")?);
                }
                Meta::NameValue(nv) if nv.path.is_ident("field") => {
                    args.field = Some(lit_str(&nv.value, "field")?);
                }
                Meta::NameValue(nv) if nv.path.is_ident("keys_of") => {
                    args.keys_of = Some(lit_str(&nv.value, "keys_of")?);
                }
                other => {
                    return Err(Error::new_spanned(
                        other,
                        "unknown argument: expected `field = \"<wire field>\"`, \
                         `keys_of = \"<object>\"`, `context`, `shape = \"<record_type>\"` \
                         or `external_values`",
                    ));
                }
            }
        }
        // An action or entity key reaches the wire only through a Lakekeeper enum, so one
        // declared elsewhere could never be emitted.
        if let Some(object) = args.keys_of.as_deref()
            && matches!(object, "action" | "entity")
            && std::env::var("CARGO_PKG_NAME").as_deref() != Ok("lakekeeper")
        {
            return Err(Error::new(
                proc_macro2::Span::call_site(),
                format!(
                    "`keys_of = \"{object}\"` is Lakekeeper's own, and a vocabulary declared \
                     here could never be emitted. Only `keys_of = \"context\"` is open to \
                     another emitter, through `push_extra_context`."
                ),
            ));
        }
        if args.external_values && args.field.is_none() {
            return Err(Error::new(
                proc_macro2::Span::call_site(),
                "`external_values` says the values of a field are spelled elsewhere, so it \
                 belongs on `field = \"<wire field>\"`. Every key and every value this log \
                 names itself is `lower_snake_case`.",
            ));
        }
        if args.field.is_some() && args.keys_of.is_some() {
            return Err(Error::new(
                proc_macro2::Span::call_site(),
                "an enum names either the values of a field or the keys of an object, not both",
            ));
        }
        if args.context && (args.field.is_some() || args.keys_of.is_some()) {
            return Err(input.error("`context` and a vocabulary argument cannot be combined"));
        }
        if args.shape.is_some() && (args.context || args.field.is_some() || args.keys_of.is_some())
        {
            return Err(input.error("`shape` stands alone"));
        }
        Ok(args)
    }
}

fn lit_str(expr: &Expr, what: &str) -> Result<String> {
    if let Expr::Lit(l) = expr
        && let Lit::Str(s) = &l.lit
    {
        return Ok(s.value());
    }
    Err(Error::new_spanned(
        expr,
        format!("`{what}` takes a string literal"),
    ))
}

/// Declares a type as part of the audit log. See the crate documentation.
#[proc_macro_attribute]
pub fn audit_part(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as Args);
    let input = parse_macro_input!(item as DeriveInput);
    match expand(&args, &input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// What a vocabulary enum's variant names become on the wire.
#[derive(Clone, Copy)]
enum Vocabulary<'a> {
    /// The values of the wire field of this name. `external` when they are spelled somewhere
    /// else, which exempts them from the case check.
    Values { field: &'a str, external: bool },
    /// The keys of the object of this name.
    Keys(&'a str),
}

fn expand(args: &Args, input: &DeriveInput) -> Result<TokenStream2> {
    reject_type_generics(input)?;
    let vocabulary = match (&args.field, &args.keys_of) {
        (Some(field), _) => Some(Vocabulary::Values {
            field,
            external: args.external_values,
        }),
        (_, Some(object)) => Some(Vocabulary::Keys(object)),
        (None, None) => None,
    };
    match (&input.data, vocabulary) {
        (Data::Enum(e), Some(vocabulary)) => expand_vocabulary(input, e, vocabulary),
        (Data::Struct(_) | Data::Enum(_), None) => expand_part(input, args),
        (Data::Struct(_), Some(_)) => Err(Error::new_spanned(
            &input.ident,
            "`field` and `keys_of` are for vocabulary enums; a struct is a part or a `context`",
        )),
        (Data::Union(_), _) => Err(Error::new_spanned(
            &input.ident,
            "unions cannot be audit parts",
        )),
    }
}

/// A part, a context or a shape: derives, `AuditPart`, registration with the schema.
///
/// A shape is a whole record. It is described but not serialised: a record reaches the wire as
/// the log event's own fields, written one by one by its `emit()`, so nothing ever serialises
/// the struct. Deriving `Serialize` for it would produce a nested object no consumer ever
/// sees, and would demand `Serialize` of every vocabulary enum it holds.
fn expand_part(input: &DeriveInput, args: &Args) -> Result<TokenStream2> {
    let (context, shape) = (args.context, args.shape.as_deref());
    let is_shape = shape.is_some();
    if matches!(input.data, Data::Enum(_)) && (context || is_shape) {
        return Err(Error::new_spanned(
            &input.ident,
            "`context` and `shape` are for structs",
        ));
    }
    require_field_docs(input)?;
    reject_audit_attrs(input)?;
    let emit = match shape {
        Some(record_type) => shape_emit(input, record_type)?,
        None => quote!(),
    };
    let ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let static_ty = static_type(input);
    let kind = match (shape, context) {
        (Some(record_type), _) => {
            quote!(::lakekeeper::audit::Kind::Shape { record_type: #record_type })
        }
        (None, true) => quote!(::lakekeeper::audit::Kind::Context),
        (None, false) => quote!(::lakekeeper::audit::Kind::Part),
    };
    let derives = if is_shape {
        quote!(#[derive(::lakekeeper::__private::schemars::JsonSchema)])
    } else {
        quote!(
            #[derive(::lakekeeper::__private::serde::Serialize, ::lakekeeper::__private::schemars::JsonSchema)]
            #[serde(crate = "::lakekeeper::__private::serde")]
        )
    };
    let part_impl = if is_shape {
        quote!()
    } else {
        quote! {
            impl #impl_generics ::lakekeeper::audit::AuditPart for #ident #ty_generics #where_clause {
                type Emitter = crate::audit_emitter::Emitter;
            }
        }
    };
    let item = strip_our_attrs(input);
    Ok(quote! {
        #derives
        #[schemars(crate = "::lakekeeper::__private::schemars")]
        #item

        #part_impl

        #emit

        #[cfg(debug_assertions)]
        ::lakekeeper::__private::inventory::submit! {
            ::lakekeeper::audit::Registration {
                kind: #kind,
                type_name: || ::core::any::type_name::<#static_ty>(),
                emitter: ::lakekeeper::audit::EmitterStamp::of::<crate::audit_emitter::Emitter>(),
                emitter_type: || ::core::any::type_name::<crate::audit_emitter::Emitter>(),
                defining_crate: env!("CARGO_PKG_NAME"),
                external_values: false,
                schema_name: ::core::option::Option::Some(|| <#static_ty as ::lakekeeper::__private::schemars::JsonSchema>::schema_name()),
                schema: ::core::option::Option::Some(|generator| <#static_ty as ::lakekeeper::__private::schemars::JsonSchema>::json_schema(generator)),
            }
        }
    })
}

/// A vocabulary enum: `as_wire()`, the name lists, registration with the names. No derives,
/// no `AuditPart`.
fn expand_vocabulary(
    input: &DeriveInput,
    data: &syn::DataEnum,
    vocabulary: Vocabulary<'_>,
) -> Result<TokenStream2> {
    let ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let static_ty = static_type(input);
    let external_values = matches!(vocabulary, Vocabulary::Values { external: true, .. });
    // Every name this log owns is `snake_case`, so that is the rule unless the vocabulary is
    // spelled somewhere else and says how.
    let enum_attrs = audit_attrs(&input.attrs, &["rename_all"], "on a vocabulary enum")?;
    let rule = match enum_attrs.get("rename_all") {
        Some(_) if !external_values => {
            return Err(Error::new_spanned(
                &input.ident,
                "every name this log owns is `snake_case`, which needs no `rename_all`. A \
                 vocabulary spelled somewhere else adds `external_values` and says how with \
                 `rename_all`.",
            ));
        }
        Some(rule) => rule.clone(),
        None => "snake_case".to_string(),
    };
    let variant_keys: &[&str] = match vocabulary {
        Vocabulary::Values { .. } => &["rename", "carries"],
        Vocabulary::Keys(_) => &["rename"],
    };

    // Keys and values reach the wire as different types on purpose. A `WireKey` converts into
    // no value type, so an enum declared as keys cannot be written where a field's value is
    // expected, and a value enum cannot become an object key.
    let (wire_ty, kind_of, conversions) = match vocabulary {
        Vocabulary::Values { field, .. } => (
            quote!(::lakekeeper::audit::Wire<#ident #ty_generics>),
            quote!(::lakekeeper::audit::Kind::Values { field: #field, names: &WIRE }),
            quote! {
                impl #impl_generics ::core::convert::From<#ident #ty_generics>
                    for ::lakekeeper::audit::Wire<#ident #ty_generics> #where_clause {
                    fn from(value: #ident #ty_generics) -> Self {
                        value.as_wire()
                    }
                }

                impl #impl_generics ::core::convert::From<#ident #ty_generics>
                    for ::lakekeeper::audit::AnyWireStr #where_clause {
                    fn from(value: #ident #ty_generics) -> Self {
                        value.as_wire().into()
                    }
                }
            },
        ),
        Vocabulary::Keys(object) => (
            quote!(::lakekeeper::audit::WireKey<crate::audit_emitter::Emitter>),
            quote!(::lakekeeper::audit::Kind::Keys { object: #object, names: &WIRE }),
            quote! {
                impl #impl_generics ::core::convert::From<#ident #ty_generics>
                    for ::lakekeeper::audit::WireKey<crate::audit_emitter::Emitter> #where_clause {
                    fn from(value: #ident #ty_generics) -> Self {
                        value.as_wire()
                    }
                }
            },
        ),
    };
    let (as_wire_doc, as_str_doc, variants_doc, names_doc) = match vocabulary {
        Vocabulary::Values { .. } => (
            "The value as it reaches the wire, tied to this crate's emitter.",
            "The value as it reaches the wire, as a plain string.",
            "Every value this enum can put on the wire.",
            "Every value this enum can put on the wire, as plain strings.",
        ),
        Vocabulary::Keys(_) => (
            "The key as it reaches the wire, tied to this crate's emitter.",
            "The key as it reaches the wire, as a plain string.",
            "Every key this enum can put on the wire.",
            "Every key this enum can put on the wire, as plain strings.",
        ),
    };

    let mut arms = Vec::new();
    let mut value_arms = Vec::new();
    let mut names = Vec::new();
    let mut docs = Vec::new();
    let mut carries = Vec::new();
    let mut values = Vec::new();
    // An `entity` key's value is always a string, so its keys hold nothing. Every other
    // object's keys hold their value, and the type they hold is the declaration of what the
    // key carries: the compiler checks every write against it.
    let keys_hold_values = matches!(vocabulary, Vocabulary::Keys(object) if object != "entity");
    for v in &data.variants {
        let variant_attrs = audit_attrs(&v.attrs, variant_keys, "on a variant")?;
        let wire = match variant_attrs.get("rename") {
            Some(explicit) => explicit.clone(),
            None => apply_rule(&rule, &v.ident.to_string())?,
        };
        if !external_values && !is_lower_snake_case(&wire) {
            return Err(Error::new_spanned(
                v,
                format!(
                    "`{wire}` is not `lower_snake_case`, which is how this log spells every name \
                     it owns: runs of `[a-z0-9]` joined by single underscores, starting with a \
                     letter. A vocabulary spelled somewhere else adds `external_values`."
                ),
            ));
        }
        let vident = &v.ident;
        let pattern = match &v.fields {
            Fields::Unit => quote!(Self::#vident),
            Fields::Named(_) => quote!(Self::#vident { .. }),
            Fields::Unnamed(_) => quote!(Self::#vident(..)),
        };
        arms.push(quote!(#pattern => <#wire_ty>::new(#wire)));
        names.push(wire);
        docs.push(doc_text(&v.attrs).unwrap_or_default());
        if let Vocabulary::Keys(object) = vocabulary {
            match (&v.fields, keys_hold_values) {
                (Fields::Unnamed(held), true) if held.unnamed.len() == 1 => {
                    let ty = &held.unnamed[0].ty;
                    values.push(quote!(::core::option::Option::Some(
                        (|generator: &mut ::lakekeeper::__private::schemars::SchemaGenerator| {
                            generator.subschema_for::<#ty>()
                        }) as fn(&mut ::lakekeeper::__private::schemars::SchemaGenerator)
                            -> ::lakekeeper::__private::schemars::Schema
                    )));
                    value_arms.push(quote!(
                        Self::#vident(value) => ::lakekeeper::__private::serde_json::to_value(value)
                    ));
                }
                (_, true) => {
                    return Err(Error::new_spanned(
                        v,
                        format!(
                            "a key of `{object}` holds its value: write `{vident}(<type>)`, \
                             with the type the key carries on the wire"
                        ),
                    ));
                }
                (Fields::Unit, false) => values.push(quote!(::core::option::Option::None)),
                (_, false) => {
                    return Err(Error::new_spanned(
                        v,
                        format!(
                            "a key of `{object}` holds no value: its value is a string the \
                             record carries beside it"
                        ),
                    ));
                }
            }
        } else {
            values.push(quote!(::core::option::Option::None));
        }
        // A field name is already in the enum's own spelling, so it takes the same rule the
        // variant names take.
        let carried = variant_context(v, &rule, &variant_attrs, vocabulary)?;
        carries.push(quote!(&[#(#carried),*]));
    }
    let count = names.len();
    let wire_consts = names.iter().map(|n| quote!(<#wire_ty>::new(#n)));
    let item = strip_our_attrs(input);
    let schema_name = ident.to_string();

    // What a key hands over: its value, in the JSON type the schema publishes for it.
    let value_fn = if keys_hold_values {
        quote! {
            /// The value this key carries, as it reaches the wire.
            ///
            /// # Panics
            ///
            /// If the held value does not serialize to JSON, which no type with a schema of
            /// its own does: string keys, finite numbers.
            #[must_use]
            pub fn value(&self) -> ::lakekeeper::__private::serde_json::Value {
                let value = match self { #(#value_arms),* };
                value.expect("an audit context value serializes to JSON")
            }
        }
    } else {
        quote!()
    };
    // A key of the record's own `context` object can be pushed onto an event; keys of the
    // other objects cannot, because those objects are written by Lakekeeper alone.
    let record_context_impl = match vocabulary {
        Vocabulary::Keys("context") => quote! {
            impl #impl_generics ::lakekeeper::audit::RecordContextKey for #ident #ty_generics #where_clause {
                type Emitter = crate::audit_emitter::Emitter;
                fn wire(&self) -> ::lakekeeper::audit::WireKey<crate::audit_emitter::Emitter> {
                    self.as_wire()
                }
                fn value(&self) -> ::lakekeeper::__private::serde_json::Value {
                    #ident::value(self)
                }
            }
        },
        _ => quote!(),
    };
    // A value vocabulary is a type the rest of the code can name: a field holding one of its
    // values is a `Wire<Self>`, which points at this definition. An operation's and an
    // outcome's sets are marked, so the two cannot be passed in each other's place.
    let vocabulary_impls = match vocabulary {
        Vocabulary::Values { field, .. } => {
            let marker = match field {
                "operation" => quote! {
                    impl #impl_generics ::lakekeeper::audit::OperationValues for #ident #ty_generics #where_clause {}
                },
                "outcome" => quote! {
                    impl #impl_generics ::lakekeeper::audit::OutcomeValues for #ident #ty_generics #where_clause {}
                },
                _ => quote!(),
            };
            quote! {
                impl #impl_generics ::lakekeeper::audit::Vocabulary for #ident #ty_generics #where_clause {
                    type Emitter = crate::audit_emitter::Emitter;
                    const SCHEMA_NAME: &'static str = #schema_name;
                    fn wire(&self) -> ::lakekeeper::audit::Wire<Self> {
                        self.as_wire()
                    }
                }

                #marker
            }
        }
        Vocabulary::Keys(_) => quote!(),
    };

    Ok(quote! {
        #item

        impl #impl_generics #ident #ty_generics #where_clause {
            #[doc = #as_wire_doc]
            #[must_use]
            pub const fn as_wire(&self) -> #wire_ty {
                match self { #(#arms),* }
            }

            #[doc = #as_str_doc]
            ///
            /// Generated, so it cannot answer differently from `as_wire` and therefore
            /// cannot differ from the name the schema declares. A `strum`, `serde` or
            /// hand-written derivation would be a second source of truth, and the two agree
            /// only until someone renames a variant.
            #[must_use]
            pub const fn as_str(&self) -> &'static str {
                self.as_wire().text()
            }

            #value_fn

            #[doc = #variants_doc]
            pub const WIRE_VARIANTS: [#wire_ty; #count] = [#(#wire_consts),*];

            #[doc = #names_doc]
            pub const WIRE_NAMES: [&'static str; #count] = [#(#names),*];
        }

        #conversions

        #record_context_impl

        #vocabulary_impls

        #[cfg(debug_assertions)]
        ::lakekeeper::__private::inventory::submit! {
            ::lakekeeper::audit::Registration {
                kind: {
                    // The names and their descriptions in one list, built once here, so the
                    // registry cannot hold a description against the wrong name.
                    const WIRE: [::lakekeeper::audit::WireName; #count] = [
                        #(::lakekeeper::audit::WireName {
                            text: #names,
                            doc: #docs,
                            carries: #carries,
                            value: #values,
                        }),*
                    ];
                    #kind_of
                },
                type_name: || ::core::any::type_name::<#static_ty>(),
                emitter: ::lakekeeper::audit::EmitterStamp::of::<crate::audit_emitter::Emitter>(),
                emitter_type: || ::core::any::type_name::<crate::audit_emitter::Emitter>(),
                defining_crate: env!("CARGO_PKG_NAME"),
                external_values: #external_values,
                schema_name: ::core::option::Option::None,
                schema: ::core::option::Option::None,
            }
        }
    })
}

/// The re-emitted item: without `#[audit_part]` and without our `#[audit(...)]` helper
/// attributes, which no derive knows. Everything else (serde, strum, schemars, docs) passes
/// through.
fn strip_our_attrs(input: &DeriveInput) -> DeriveInput {
    fn is_ours(a: &Attribute) -> bool {
        a.path().is_ident("audit_part") || a.path().is_ident("audit")
    }
    let mut out = input.clone();
    out.attrs.retain(|a| !is_ours(a));
    match &mut out.data {
        Data::Enum(e) => {
            for v in &mut e.variants {
                v.attrs.retain(|a| !is_ours(a));
                strip_fields(&mut v.fields);
            }
        }
        Data::Struct(s) => strip_fields(&mut s.fields),
        Data::Union(_) => {}
    }
    fn strip_fields(fields: &mut Fields) {
        for f in fields.iter_mut() {
            f.attrs.retain(|a| !is_ours(a));
        }
    }
    out
}

/// The `emit` of a shape: the one way its record reaches the log.
///
/// Every field goes on the line under its own name, in struct order, so the record and the
/// schema derived from the same struct cannot list different fields. A field whose value
/// serializes to `null` is left off the line. `record_type` is stamped from the attribute, so
/// the struct holds no such field.
fn shape_emit(input: &DeriveInput, record_type: &str) -> Result<TokenStream2> {
    if std::env::var("CARGO_PKG_NAME").as_deref() != Ok("lakekeeper") {
        return Err(Error::new_spanned(
            &input.ident,
            "a record shape is Lakekeeper's own; another product emits an operation record \
             through `OperationRecord`",
        ));
    }
    let Data::Struct(syn::DataStruct {
        fields: Fields::Named(named),
        ..
    }) = &input.data
    else {
        return Err(Error::new_spanned(
            &input.ident,
            "a shape is a struct with named fields: each name is a key of the record",
        ));
    };
    let mut fields = Vec::new();
    for field in &named.named {
        let name = field.ident.as_ref().expect("a named field");
        if name == "record_type" {
            return Err(Error::new_spanned(
                field,
                "`record_type` is stamped from `shape = \"...\"`; the struct holds no such field",
            ));
        }
        for attr in field
            .attrs
            .iter()
            .filter(|a| a.path().is_ident("serde") || a.path().is_ident("schemars"))
        {
            attr.parse_nested_meta(|meta| {
                if ["rename", "skip", "flatten"]
                    .iter()
                    .any(|banned| meta.path.is_ident(banned))
                {
                    return Err(meta.error(
                        "a shape's field name is its key on the record, and every field is \
                         on it: no `rename`, `skip` or `flatten`",
                    ));
                }
                if meta.input.peek(Token![=]) {
                    let _: Expr = meta.value()?.parse()?;
                }
                Ok(())
            })?;
        }
        fields.push(name);
    }
    let ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    Ok(quote! {
        impl #impl_generics #ident #ty_generics #where_clause {
            /// Write this record as one log line, or nothing when the audit trail is off.
            ///
            /// Asked before anything is serialized, so a switched-off audit trail costs nothing,
            /// and every record passes here, so the configuration switch covers every record
            /// whatever built it.
            pub(crate) fn emit(self, message: &'static str) {
                if !crate::audit::enabled() {
                    return;
                }
                #(let #fields = ::lakekeeper::audit::AuditJson::present(&self.#fields);)*
                ::tracing::info!(
                    target: crate::audit::AUDIT_TARGET,
                    event_source = crate::service::events::backends::audit::EVENT_SOURCE,
                    audit_format = crate::service::events::backends::audit::AUDIT_FORMAT,
                    record_type = #record_type,
                    #(#fields = #fields.as_ref().map(::tracing::field::valuable),)*
                    "{}",
                    message
                );
            }
        }
    })
}

/// A part, a context or a shape takes no `#[audit]` key: its schema comes from its type, and
/// its field names are its own.
fn reject_audit_attrs(input: &DeriveInput) -> Result<()> {
    const PLACEMENT: &str = "on an audit part";
    audit_attrs(&input.attrs, &[], PLACEMENT)?;
    let fields: Vec<&Fields> = match &input.data {
        Data::Struct(s) => vec![&s.fields],
        Data::Enum(e) => {
            for v in &e.variants {
                audit_attrs(&v.attrs, &[], PLACEMENT)?;
            }
            e.variants.iter().map(|v| &v.fields).collect()
        }
        Data::Union(_) => Vec::new(),
    };
    for field in fields.into_iter().flatten() {
        audit_attrs(&field.attrs, &[], PLACEMENT)?;
    }
    Ok(())
}

fn reject_type_generics(input: &DeriveInput) -> Result<()> {
    for p in &input.generics.params {
        match p {
            GenericParam::Lifetime(_) => {}
            GenericParam::Type(t) => {
                return Err(Error::new_spanned(
                    t,
                    "an audit part cannot be generic over a type: the schema has to be one document",
                ));
            }
            GenericParam::Const(c) => {
                return Err(Error::new_spanned(
                    c,
                    "an audit part cannot have const generics",
                ));
            }
        }
    }
    Ok(())
}

fn static_type(input: &DeriveInput) -> TokenStream2 {
    let ident = &input.ident;
    let lifetimes = input.generics.lifetimes().count();
    if lifetimes == 0 {
        quote!(#ident)
    } else {
        let statics = std::iter::repeat_n(quote!('static), lifetimes);
        quote!(#ident<#(#statics),*>)
    }
}

fn has_doc(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|a| a.path().is_ident("doc"))
}

/// The doc comment on an item, its lines trimmed and joined, or `None` when it has none.
///
/// A variant's doc comment is the only description its wire name can have: a name is a
/// string in a list, not a schema of its own, so nothing else carries it to a consumer.
fn doc_text(attrs: &[Attribute]) -> Option<String> {
    let mut lines = Vec::new();
    for attr in attrs.iter().filter(|a| a.path().is_ident("doc")) {
        if let Meta::NameValue(nv) = &attr.meta
            && let Expr::Lit(lit) = &nv.value
            && let Lit::Str(s) = &lit.lit
        {
            lines.push(s.value().trim().to_string());
        }
    }
    let text = lines.join("\n").trim().to_string();
    (!text.is_empty()).then_some(text)
}

fn require_field_docs(input: &DeriveInput) -> Result<()> {
    let fields: Vec<&syn::Field> = match &input.data {
        Data::Struct(s) => match &s.fields {
            Fields::Named(n) => n.named.iter().collect(),
            Fields::Unnamed(_) | Fields::Unit => Vec::new(),
        },
        Data::Enum(e) => e
            .variants
            .iter()
            .flat_map(|v| match &v.fields {
                Fields::Named(n) => n.named.iter().collect::<Vec<_>>(),
                Fields::Unnamed(_) | Fields::Unit => Vec::new(),
            })
            .collect(),
        Data::Union(_) => Vec::new(),
    };
    for f in fields {
        if !has_doc(&f.attrs) {
            let name = f
                .ident
                .as_ref()
                .map_or_else(String::new, ToString::to_string);
            return Err(Error::new_spanned(
                f,
                format!(
                    "audit field `{name}` needs a doc comment: it is the field's description in the schema"
                ),
            ));
        }
    }
    Ok(())
}

/// The `#[audit(...)]` keys on one item, checked against the keys its placement takes.
///
/// Every key is `key = "string"`. A key the placement does not take, a key given twice, and a
/// value that is not a string literal are all rejected, so a misspelt key cannot compile and
/// leave the declaration it meant to make unsaid.
fn audit_attrs(
    attrs: &[Attribute],
    allowed: &[&str],
    placement: &str,
) -> Result<BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    for attr in attrs.iter().filter(|a| a.path().is_ident("audit")) {
        attr.parse_nested_meta(|meta| {
            let key = meta
                .path
                .get_ident()
                .map_or_else(String::new, ToString::to_string);
            if !allowed.contains(&key.as_str()) {
                let takes = if allowed.is_empty() {
                    "it takes none".to_string()
                } else {
                    format!("it takes `{}`", allowed.join("`, `"))
                };
                return Err(meta.error(format!(
                    "`{key}` is not an `#[audit]` key {placement}: {takes}"
                )));
            }
            let Lit::Str(value) = meta.value()?.parse::<Lit>()? else {
                return Err(meta.error(format!("`{key}` takes a string literal")));
            };
            if out.insert(key.clone(), value.value()).is_some() {
                return Err(meta.error(format!("`{key}` is given twice")));
            }
            Ok(())
        })?;
    }
    Ok(out)
}

/// Whether `name` is one or more runs of `[a-z0-9]` joined by single underscores, starting
/// with a letter.
fn is_lower_snake_case(name: &str) -> bool {
    name.starts_with(|c: char| c.is_ascii_lowercase())
        && name.split('_').all(|segment| {
            !segment.is_empty()
                && segment
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        })
}

/// A comma-separated key list, as `expands_to` and `carries` both write one. Empty entries
/// are dropped, so a trailing comma or a stray space declares no key.
fn split_keys(declared: &str) -> Vec<String> {
    declared
        .split(',')
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .map(ToString::to_string)
        .collect()
}

/// The `context` keys a variant can put beside itself.
///
/// A variant's named fields are its context keys: `Drop { force, purge }` carries `force` and
/// `purge`, under the enum's own rename rule so the names match the wire.
///
/// A field whose own type decides the keys lends its name to none of them, so it lists them
/// literally: `#[audit(expands_to = "a, b")]` carries `a` and `b` in place of the field. An
/// unnamed field of an action has no name to lend, so it always lists them, `""` for none.
///
/// A variant with no fields at all can still reach a record with context beside it, when the
/// handler that names the action assembles the context itself. It lists those keys with
/// `#[audit(carries = "a, b")]` on the variant.
///
/// # Errors
///
/// If a variant both has fields and declares `carries`, which would say the same thing twice;
/// if a field of a key vocabulary carries `#[audit]`; or if an unnamed field of an action
/// declares no `expands_to`.
fn variant_context(
    variant: &syn::Variant,
    rule: &str,
    attrs: &BTreeMap<String, String>,
    vocabulary: Vocabulary<'_>,
) -> Result<Vec<String>> {
    let field_keys: &[&str] = match vocabulary {
        Vocabulary::Values { .. } => &["expands_to"],
        Vocabulary::Keys(_) => &[],
    };
    let fields: Vec<(&syn::Field, BTreeMap<String, String>)> = variant
        .fields
        .iter()
        .map(|field| Ok((field, audit_attrs(&field.attrs, field_keys, "on a field")?)))
        .collect::<Result<_>>()?;
    if let Some(declared) = attrs.get("carries") {
        if !matches!(variant.fields, Fields::Unit) {
            return Err(Error::new_spanned(
                variant,
                "this variant has fields, which are already its context keys, so `carries` \
                 would say it a second way. `carries` is for a variant whose context is \
                 assembled by the handler that names the action; a field whose type picks \
                 the keys takes `expands_to` instead.",
            ));
        }
        return Ok(split_keys(declared));
    }
    let is_action = matches!(
        vocabulary,
        Vocabulary::Values {
            field: "action_name",
            ..
        }
    );
    let mut carries = Vec::new();
    for (field, field_attrs) in fields {
        if let Some(declared) = field_attrs.get("expands_to") {
            carries.extend(split_keys(declared));
            continue;
        }
        match &field.ident {
            Some(ident) => carries.push(apply_rule(rule, &ident.to_string())?),
            None if is_action => {
                return Err(Error::new_spanned(
                    field,
                    "an unnamed field of an action has no name to lend a context key, so it \
                     says which keys it writes: `#[audit(expands_to = \"a, b\")]`, or \
                     `expands_to = \"\"` for none",
                ));
            }
            None => {}
        }
    }
    Ok(carries)
}

fn apply_rule(rule: &str, name: &str) -> Result<String> {
    Ok(match rule {
        "snake_case" => name.to_snake_case(),
        "kebab-case" => name.to_kebab_case(),
        "camelCase" => name.to_lower_camel_case(),
        "PascalCase" => name.to_pascal_case(),
        "SCREAMING_SNAKE_CASE" => name.to_shouty_snake_case(),
        "SCREAMING-KEBAB-CASE" => name.to_shouty_kebab_case(),
        "lowercase" => name.to_lowercase(),
        "UPPERCASE" => name.to_uppercase(),
        other => {
            return Err(Error::new(
                proc_macro2::Span::call_site(),
                format!("unsupported rename rule `{other}` on a vocabulary enum"),
            ));
        }
    })
}

#[cfg(test)]
mod tests {
    use syn::parse_str;

    use super::*;

    /// The macro's answer for one declaration: `Ok` with the expansion, or the rejection
    /// message.
    fn expand_str(args: &str, item: &str) -> std::result::Result<String, String> {
        let args: Args = parse_str(args).map_err(|e| e.to_string())?;
        let input: DeriveInput = parse_str(item).map_err(|e| e.to_string())?;
        expand(&args, &input)
            .map(|ts| ts.to_string())
            .map_err(|e| e.to_string())
    }

    /// The text between the first `open` and the next `close`, for reading one field out of
    /// an expansion without depending on how the rest of it is spaced.
    #[track_caller]
    fn between<'a>(text: &'a str, open: &str, close: &str) -> &'a str {
        let start = text
            .find(open)
            .unwrap_or_else(|| panic!("no `{open}` in the expansion:\n{text}"))
            + open.len();
        let len = text[start..]
            .find(close)
            .unwrap_or_else(|| panic!("no `{close}` after `{open}`"));
        text[start..start + len].trim()
    }

    /// One space between tokens, so an assertion reads as Rust rather than as whatever
    /// spacing `proc-macro2` happened to print.
    fn squash(text: &str) -> String {
        text.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    /// The rejection message for a declaration that must not expand.
    #[track_caller]
    fn rejection(args: &str, item: &str) -> String {
        expand_str(args, item).expect_err("this declaration must be rejected")
    }

    #[test]
    fn a_rename_rule_spells_the_wire_name_the_way_serde_does() {
        let rule = |rule: &str| apply_rule(rule, "GrantCreated").expect("a known rule");
        assert_eq!(rule("snake_case"), "grant_created");
        assert_eq!(rule("kebab-case"), "grant-created");
        assert_eq!(rule("camelCase"), "grantCreated");
        assert_eq!(rule("PascalCase"), "GrantCreated");
        assert_eq!(rule("SCREAMING_SNAKE_CASE"), "GRANT_CREATED");
        // Kebab, not snake: the two rules differ only in the separator, and answering with an
        // underscore would rename every value of an enum that asked for this rule.
        assert_eq!(rule("SCREAMING-KEBAB-CASE"), "GRANT-CREATED");
        assert_eq!(rule("lowercase"), "grantcreated");
        assert_eq!(rule("UPPERCASE"), "GRANTCREATED");
        assert!(apply_rule("Train-Case", "GrantCreated").is_err());
    }

    #[test]
    fn a_wire_name_is_snake_case_unless_the_audit_attribute_says_otherwise() {
        let wire = |args: &str, item: &str| expand_str(args, item).expect("expands");
        // `snake_case` is the default, and strum and serde are not read: an enum's API
        // spelling cannot change what it puts on the wire.
        let expansion = wire(
            r#"field = "outcome""#,
            r#"
            #[strum(serialize_all = "kebab-case")]
            enum Outcome {
                #[serde(rename = "from-serde")]
                SomethingHappened,
                #[audit(rename = "explicit_name")]
                Renamed,
            }"#,
        );
        assert!(expansion.contains(r#""something_happened""#), "{expansion}");
        assert!(expansion.contains(r#""explicit_name""#), "{expansion}");
        let names = between(&expansion, "WIRE_NAMES : [& 'static str ; 2usize] = [", "]");
        assert_eq!(squash(names), r#""something_happened" , "explicit_name""#);
        // A vocabulary spelled elsewhere says how, and is exempt from the case rule.
        let expansion = wire(
            r#"field = "kind", external_values"#,
            r#"
            #[audit(rename_all = "kebab-case")]
            enum Kind { GenericTable }"#,
        );
        assert!(expansion.contains(r#""generic-table""#), "{expansion}");
        // A variant `strum` disables still names a value: the macro reads no `strum`.
        let expansion = wire(
            r#"field = "outcome""#,
            "enum Outcome { #[strum(disabled)] Hidden, Shown }",
        );
        assert!(expansion.contains(r#""hidden""#), "{expansion}");
    }

    #[test]
    fn an_audit_key_is_checked_against_its_placement() {
        // Unknown, duplicated, non-string and misplaced keys are all rejected, so a typo
        // cannot compile and leave its declaration unsaid.
        assert!(
            rejection(
                r#"field = "outcome""#,
                r#"enum E { #[audit(renam = "a")] A }"#
            )
            .contains("`renam` is not an `#[audit]` key on a variant")
        );
        assert!(
            rejection(
                r#"field = "outcome""#,
                r#"enum E { #[audit(rename = "a", rename = "b")] A }"#
            )
            .contains("given twice")
        );
        assert!(
            rejection(r#"field = "outcome""#, "enum E { #[audit(rename = 1)] A }")
                .contains("takes a string literal")
        );
        assert!(
            rejection(
                r#"keys_of = "context""#,
                r#"enum K { #[audit(carries = "a")] A(bool) }"#
            )
            .contains("`carries` is not an `#[audit]` key on a variant")
        );
        assert!(
            rejection(
                "",
                r#"struct S { #[audit(rename = "a")] /// A field.
                a: u8 }"#
            )
            .contains("on an audit part: it takes none")
        );
        // A name this log owns is `snake_case`; only an external vocabulary may say otherwise.
        assert!(
            rejection(
                r#"field = "outcome""#,
                r#"#[audit(rename_all = "kebab-case")] enum E { A }"#
            )
            .contains("needs no `rename_all`")
        );
        assert!(
            rejection(
                r#"field = "outcome""#,
                r#"enum E { #[audit(rename = "Not-Snake")] A }"#
            )
            .contains("is not `lower_snake_case`")
        );
        // An unnamed field of an action says which keys it writes, `""` for none.
        assert!(
            rejection(r#"field = "action_name""#, "enum E { A(u8) }").contains("no name to lend")
        );
        assert!(
            expand_str(
                r#"field = "action_name""#,
                r#"enum E { A(#[audit(expands_to = "")] u8) }"#
            )
            .is_ok()
        );
    }

    #[test]
    fn a_variant_doc_comment_becomes_the_value_description() {
        let expansion = expand_str(
            r#"field = "outcome""#,
            r#"
            enum Outcome {
                /// The operation completed.
                Success,
                Other,
            }"#,
        )
        .expect("expands");
        // Each name carries its own description, so a variant with no doc comment cannot
        // shift a later variant's description onto the wrong name. Asserting on the whole
        // list is what shows the pairing; asserting that the text appears somewhere would
        // not.
        let wire = between(&expansion, "WireName ; 2usize] = [", "] ;")
            .replace(":: lakekeeper :: audit :: ", "");
        assert_eq!(
            squash(&wire),
            squash(
                r#"WireName { text : "success" , doc : "The operation completed." , carries : & [] , value : :: core :: option :: Option :: None , } ,
                   WireName { text : "other" , doc : "" , carries : & [] , value : :: core :: option :: Option :: None , }"#
            ),
            "{expansion}"
        );
    }

    #[test]
    fn keys_and_values_are_different_kinds() {
        let values = expand_str(r#"field = "outcome""#, "enum Outcome { Success }").expect("ok");
        assert!(values.contains("Kind :: Values"), "{values}");
        assert!(values.contains(":: Wire <"), "{values}");

        // `as_str` comes from the same expansion for both, so no vocabulary can spell a
        // name one way for the registry and another for the code that emits it.
        assert!(values.contains("fn as_str"), "{values}");

        assert!(values.contains(r#"field : "outcome""#), "{values}");
        // A value vocabulary is a type a field can name, through `Wire<Self>`.
        assert!(
            values.contains(":: lakekeeper :: audit :: Vocabulary for"),
            "{values}"
        );
        assert!(values.contains("SCHEMA_NAME"), "{values}");

        let keys = expand_str(r#"keys_of = "context""#, "enum Key { SelfRead(bool) }").expect("ok");
        assert!(keys.contains("Kind :: Keys"), "{keys}");
        assert!(keys.contains(r#"object : "context""#), "{keys}");
        assert!(keys.contains("WireKey"), "{keys}");
        // No conversion into a value type: that is what stops a key reaching a field that
        // holds a value.
        assert!(!keys.contains("AnyWireStr"), "{keys}");
        assert!(keys.contains("fn as_str"), "{keys}");
        // A key of the record's own `context` is what an event accepts as pushed context.
        assert!(keys.contains("RecordContextKey"), "{keys}");
    }

    #[test]
    fn a_declaration_that_cannot_be_described_is_rejected() {
        // Two vocabularies at once: the names would be both values and keys.
        assert!(
            rejection(r#"field = "outcome", keys_of = "context""#, "enum E { A }")
                .contains("not both")
        );
        // A struct is a part, a context or a shape — never a vocabulary.
        assert!(
            rejection(r#"field = "outcome""#, "struct S { a: u8 }").contains("vocabulary enums")
        );
        assert!(
            rejection(r#"keys_of = "context""#, "struct S { a: u8 }").contains("vocabulary enums")
        );
        // `context` and `shape` describe a struct.
        assert!(rejection("context", "enum E { A }").contains("for structs"));
        assert!(rejection(r#"shape = "operation""#, "enum E { A }").contains("for structs"));
        // `shape` stands alone.
        assert!(rejection(r#"shape = "operation", context"#, "struct S;").contains("stands alone"));
        // `keys_of` for an object whose keys come from a Lakekeeper enum: allowed here,
        // because these tests run as the `lakekeeper-audit-macros` crate and the rule only
        // fires elsewhere. What it must not do is reject Lakekeeper's own declarations, and
        // the audit tests in `lakekeeper` cover that by compiling them.
        assert!(rejection(r#"keys_of = "action""#, "enum K { A }").contains("Lakekeeper's own"));
        assert!(
            rejection(r#"keys_of = "entity""#, "enum K { A }").contains("could never be emitted")
        );
        assert!(expand_str(r#"keys_of = "context""#, "enum K { A(bool) }").is_ok());
        // Unknown arguments name the ones that exist.
        assert!(rejection("nonsense", "struct S;").contains("unknown argument"));
        // A union has no describable shape.
        assert!(rejection("", "union U { a: u8 }").contains("unions"));
        // One document per type, so no parameter the schema would have to guess.
        assert!(rejection("", "struct S<T> { a: T }").contains("generic over a type"));
        assert!(
            rejection("", "struct S<const N: usize> { a: [u8; N] }").contains("const generics")
        );
        // Every field of a part is a field of the schema, and a field with no description is
        // a field a consumer cannot act on.
        assert!(rejection("", "struct S { undocumented: u8 }").contains("needs a doc comment"));
        // A variant's fields are already its context keys, so `carries` beside them would say
        // it twice.
        assert!(
            rejection(
                r#"field = "action_name""#,
                r#"enum E { #[audit(carries = "a")] V { /// A field.
                     a: u8 } }"#
            )
            .contains("say it a second way")
        );
    }

    #[test]
    fn a_key_holds_its_value_and_the_type_it_holds_is_published() {
        let expansion = expand_str(
            r#"keys_of = "context""#,
            r#"
            enum K {
                /// A flag.
                Flag(bool),
                /// One of a closed set.
                Level(Wire<RootLevelGrants>),
            }"#,
        )
        .expect("a key holding a value expands");
        let squashed = squash(&expansion);
        assert!(
            squashed.contains("subschema_for :: < bool >"),
            "{expansion}"
        );
        assert!(
            squashed.contains("subschema_for :: < Wire < RootLevelGrants > >"),
            "{expansion}"
        );
        assert!(squashed.contains("fn value"), "{expansion}");

        // A key of `context` or `action` holds exactly one value.
        assert!(rejection(r#"keys_of = "context""#, "enum K { A }").contains("holds its value"));
        assert!(
            rejection(r#"keys_of = "context""#, "enum K { A(bool, bool) }")
                .contains("holds its value")
        );
        // An entity key holds nothing: its value is always a string.
        assert!(
            rejection(r#"keys_of = "entity""#, "enum K { A(bool) }")
                .contains("could never be emitted")
                || expand_str(r#"keys_of = "entity""#, "enum K { A(bool) }").is_err()
        );
    }

    #[test]
    fn a_variant_declares_the_context_a_handler_assembles() {
        let expansion = expand_str(
            r#"field = "action_name""#,
            r#"
            enum E {
                /// An action whose context the handler assembles.
                #[audit(carries = "second_key, first_key")]
                Assembled,
                /// An action that carries nothing.
                Bare,
            }"#,
        )
        .expect("`carries` is allowed on a variant with no fields");
        // Declared verbatim and in the order written: nothing resolves or sorts these.
        assert!(expansion.contains(r#""second_key""#), "{expansion}");
        assert!(expansion.contains(r#""first_key""#), "{expansion}");
        // A variant that declares nothing still registers, carrying nothing.
        assert!(
            expansion.contains(
                r#"text : "bare" , doc : "An action that carries nothing." , carries : & []"#
            ),
            "{expansion}"
        );
    }

    #[test]
    fn a_shape_is_lakekeepers_own() {
        // These tests run as the macro crate, so a shape is refused here as it is in any crate
        // but Lakekeeper's. Lakekeeper's own shapes are exercised by its fixture tests, which
        // run the generated `emit()`.
        assert!(
            rejection(
                r#"shape = "operation""#,
                "struct S { /// A field.
                a: u8 }"
            )
            .contains("Lakekeeper's own")
        );
    }

    #[test]
    fn a_lifetime_is_fine_and_becomes_static_in_the_registry() {
        let expansion = expand_str(
            "",
            r#"
            struct Borrowed<'a> {
                /// The only field.
                name: &'a str,
            }"#,
        )
        .expect("a lifetime is allowed");
        assert!(expansion.contains("Borrowed < 'static >"), "{expansion}");
    }
}
