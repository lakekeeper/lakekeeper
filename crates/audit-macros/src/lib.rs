//! `#[audit_part]`: the one attribute every type that reaches the Lakekeeper audit log
//! carries.
//!
//! Four placements:
//!
//! - **A part** (`#[audit_part]` on a struct or a data enum): a nested object of a record.
//!   Expands to `#[derive(Serialize, JsonSchema)]` through `lakekeeper`'s re-exports, so the
//!   defining crate needs no direct dependency on either; to
//!   `impl lakekeeper::audit::AuditPart for T { type Emitter = crate::audit_emitter::Emitter; }`,
//!   binding the type to the emitter of the crate that defines it; and to a registry entry.
//!   Every named field needs a doc comment: it is the field's description in the schema.
//! - **A context** (`#[audit_part(context)]` on a struct): the `context` object of an operation
//!   record. Same as a part, registered as a context.
//! - **A value vocabulary** (`#[audit_part(field = "outcome")]`): its variant *names* are the
//!   values of one wire field. Variants may carry data; only the name reaches the wire. Expands
//!   to `as_wire(&self) -> WireStr<Emitter>`, `as_str(&self) -> &'static str`, and
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
//! A value vocabulary whose values are spelled somewhere else — another product publishes
//! them, or they mirror a vocabulary that does — adds `external_values`, and is then exempt
//! from the `lower_snake_case` rule every other name is held to. Say which vocabulary in the
//! enum's doc comment. Without the marker the values are checked, so one cannot end up
//! unchecked by being forgotten.
//!
//! Wire names of a vocabulary enum follow, in this order of precedence, `#[audit(rename_all =
//! "...")]`, `#[strum(serialize_all = "...")]`, `#[serde(rename_all = "...")]`, else the
//! variant name verbatim; per variant, `#[audit(rename = "...")]`, `#[strum(to_string =
//! "...")]` or `#[strum(serialize = "...")]`, `#[serde(rename = "...")]`. `#[strum(disabled)]`
//! variants are skipped, as `IntoStaticStr` skips them. The `#[audit(...)]` helper attributes
//! are consumed by this macro and never reach the compiler, so an enum needs no serde or strum
//! derive to name its wire values.
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

        #[cfg(debug_assertions)]
        ::lakekeeper::__private::inventory::submit! {
            ::lakekeeper::audit::Registration {
                kind: #kind,
                type_name: || ::core::any::type_name::<#static_ty>(),
                emitter_name: <crate::audit_emitter::Emitter as ::lakekeeper::audit::AuditEmitter>::NAME,
                emitter_type: || ::core::any::type_name::<crate::audit_emitter::Emitter>(),
                emitter_format: <crate::audit_emitter::Emitter as ::lakekeeper::audit::AuditEmitter>::FORMAT,
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
    let rule = rename_all_rule(&input.attrs)?;
    let external_values = matches!(vocabulary, Vocabulary::Values { external: true, .. });

    // Keys and values reach the wire as different types on purpose. A `WireKey` converts into
    // no value type, so an enum declared as keys cannot be written where a field's value is
    // expected, and a value enum cannot become an object key.
    let (wire_ty, kind_of, conversions) = match vocabulary {
        Vocabulary::Values { field, .. } => (
            quote!(::lakekeeper::audit::WireStr<crate::audit_emitter::Emitter>),
            quote!(::lakekeeper::audit::Kind::Values { field: #field, names: &WIRE }),
            quote! {
                impl #impl_generics ::core::convert::From<#ident #ty_generics>
                    for ::lakekeeper::audit::WireStr<crate::audit_emitter::Emitter> #where_clause {
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
    let mut names = Vec::new();
    let mut docs = Vec::new();
    for v in &data.variants {
        if strum_disabled(&v.attrs)? {
            continue;
        }
        let wire = match variant_rename(&v.attrs)? {
            Some(explicit) => explicit,
            None => apply_rule(rule.as_deref(), &v.ident.to_string())?,
        };
        let vident = &v.ident;
        let pattern = match &v.fields {
            Fields::Unit => quote!(Self::#vident),
            Fields::Named(_) => quote!(Self::#vident { .. }),
            Fields::Unnamed(_) => quote!(Self::#vident(..)),
        };
        arms.push(quote!(#pattern => <#wire_ty>::new(#wire)));
        names.push(wire);
        docs.push(doc_text(&v.attrs).unwrap_or_default());
    }
    let count = names.len();
    let wire_consts = names.iter().map(|n| quote!(<#wire_ty>::new(#n)));
    let item = strip_our_attrs(input);

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

            #[doc = #variants_doc]
            pub const WIRE_VARIANTS: [#wire_ty; #count] = [#(#wire_consts),*];

            #[doc = #names_doc]
            pub const WIRE_NAMES: [&'static str; #count] = [#(#names),*];
        }

        #conversions

        #[cfg(debug_assertions)]
        ::lakekeeper::__private::inventory::submit! {
            ::lakekeeper::audit::Registration {
                kind: {
                    // The names and their descriptions in one list, built once here, so the
                    // registry cannot hold a description against the wrong name.
                    const WIRE: [::lakekeeper::audit::WireName; #count] = [
                        #(::lakekeeper::audit::WireName { text: #names, doc: #docs }),*
                    ];
                    #kind_of
                },
                type_name: || ::core::any::type_name::<#static_ty>(),
                emitter_name: <crate::audit_emitter::Emitter as ::lakekeeper::audit::AuditEmitter>::NAME,
                emitter_type: || ::core::any::type_name::<crate::audit_emitter::Emitter>(),
                emitter_format: <crate::audit_emitter::Emitter as ::lakekeeper::audit::AuditEmitter>::FORMAT,
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

/// The value of `#[<attr>(<key> = "...")]`, if present.
fn nested_str(attrs: &[Attribute], attr: &str, key: &str) -> Result<Option<String>> {
    let mut out = None;
    for a in attrs.iter().filter(|a| a.path().is_ident(attr)) {
        a.parse_nested_meta(|meta| {
            if meta.path.is_ident(key) {
                if out.is_none() {
                    let value: Lit = meta.value()?.parse()?;
                    if let Lit::Str(s) = value {
                        out = Some(s.value());
                    }
                } else {
                    let _: Expr = meta.value()?.parse()?;
                }
            } else if meta.input.peek(Token![=]) {
                let _: Expr = meta.value()?.parse()?;
            } else if meta.input.peek(syn::token::Paren) {
                let _ = meta.parse_nested_meta(|_| Ok(()));
            }
            Ok(())
        })?;
    }
    Ok(out)
}

/// Whether `#[<attr>(<flag>)]` is present.
fn nested_flag(attrs: &[Attribute], attr: &str, flag: &str) -> Result<bool> {
    let mut out = false;
    for a in attrs.iter().filter(|a| a.path().is_ident(attr)) {
        a.parse_nested_meta(|meta| {
            if meta.path.is_ident(flag) && !meta.input.peek(Token![=]) {
                out = true;
            } else if meta.input.peek(Token![=]) {
                let _: Expr = meta.value()?.parse()?;
            } else if meta.input.peek(syn::token::Paren) {
                let _ = meta.parse_nested_meta(|_| Ok(()));
            }
            Ok(())
        })?;
    }
    Ok(out)
}

fn rename_all_rule(attrs: &[Attribute]) -> Result<Option<String>> {
    if let Some(r) = nested_str(attrs, "audit", "rename_all")? {
        return Ok(Some(r));
    }
    if let Some(r) = nested_str(attrs, "strum", "serialize_all")? {
        return Ok(Some(r));
    }
    nested_str(attrs, "serde", "rename_all")
}

fn variant_rename(attrs: &[Attribute]) -> Result<Option<String>> {
    if let Some(r) = nested_str(attrs, "audit", "rename")? {
        return Ok(Some(r));
    }
    if let Some(r) = nested_str(attrs, "strum", "to_string")? {
        return Ok(Some(r));
    }
    if let Some(r) = nested_str(attrs, "strum", "serialize")? {
        return Ok(Some(r));
    }
    nested_str(attrs, "serde", "rename")
}

fn strum_disabled(attrs: &[Attribute]) -> Result<bool> {
    nested_flag(attrs, "strum", "disabled")
}

fn apply_rule(rule: Option<&str>, name: &str) -> Result<String> {
    Ok(match rule {
        None => name.to_string(),
        Some("snake_case") => name.to_snake_case(),
        Some("kebab-case" | "kebab_case") => name.to_kebab_case(),
        Some("camelCase" | "camel_case") => name.to_lower_camel_case(),
        Some("PascalCase") => name.to_pascal_case(),
        Some("SCREAMING_SNAKE_CASE" | "shouty_snake_case") => name.to_shouty_snake_case(),
        Some("SCREAMING-KEBAB-CASE") => name.to_shouty_kebab_case(),
        Some("lowercase") => name.to_lowercase(),
        Some("UPPERCASE") => name.to_uppercase(),
        Some(other) => {
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
        let rule = |rule: &str| apply_rule(Some(rule), "GrantCreated").expect("a known rule");
        assert_eq!(
            apply_rule(None, "GrantCreated").expect("no rule"),
            "GrantCreated"
        );
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
        assert!(apply_rule(Some("Train-Case"), "GrantCreated").is_err());
    }

    #[test]
    fn a_wire_name_follows_the_declared_precedence() {
        let wire = |item: &str| expand_str(r#"field = "outcome""#, item).expect("expands");
        // `#[audit(rename)]` wins over strum and serde, which is what lets an enum name its
        // wire value without changing how it serialises for an API.
        let expansion = wire(
            r#"
            #[audit(rename_all = "snake_case")]
            enum Outcome {
                #[audit(rename = "x-y")]
                #[serde(rename = "from_serde")]
                Renamed,
                Plain,
            }"#,
        );
        assert!(expansion.contains(r#""x-y""#), "{expansion}");
        assert!(expansion.contains(r#""plain""#), "{expansion}");
        // `#[strum(disabled)]` drops the variant, as `IntoStaticStr` drops it.
        let expansion = wire(
            r#"
            enum Outcome {
                #[strum(disabled)]
                Hidden,
                Shown,
            }"#,
        );
        // The variant survives in the re-emitted enum; only its wire name is gone.
        assert!(!expansion.contains(r#""Hidden""#), "{expansion}");
        assert!(expansion.contains(r#""Shown""#), "{expansion}");
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
                r#"WireName { text : "Success" , doc : "The operation completed." } ,
                   WireName { text : "Other" , doc : "" }"#
            ),
            "{expansion}"
        );
    }

    #[test]
    fn keys_and_values_are_different_kinds() {
        let values = expand_str(r#"field = "outcome""#, "enum Outcome { Success }").expect("ok");
        assert!(values.contains("Kind :: Values"), "{values}");
        assert!(values.contains("WireStr"), "{values}");

        // `as_str` comes from the same expansion for both, so no vocabulary can spell a
        // name one way for the registry and another for the code that emits it.
        assert!(values.contains("fn as_str"), "{values}");

        assert!(values.contains(r#"field : "outcome""#), "{values}");

        let keys = expand_str(r#"keys_of = "context""#, "enum Key { SelfRead }").expect("ok");
        assert!(keys.contains("Kind :: Keys"), "{keys}");
        assert!(keys.contains(r#"object : "context""#), "{keys}");
        assert!(keys.contains("WireKey"), "{keys}");
        // No conversion into a value type: that is what stops a key reaching a field that
        // holds a value.
        assert!(!keys.contains("AnyWireStr"), "{keys}");
        assert!(keys.contains("fn as_str"), "{keys}");
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
        assert!(expand_str(r#"keys_of = "context""#, "enum K { A }").is_ok());
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
