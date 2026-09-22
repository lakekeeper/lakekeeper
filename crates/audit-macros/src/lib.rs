//! `#[audit_part]`: the one attribute every type that reaches the Lakekeeper audit log
//! carries.
//!
//! Three placements:
//!
//! - **A part** (`#[audit_part]` on a struct or a data enum): a nested object of a record.
//!   Expands to `#[derive(Serialize, JsonSchema)]` through `lakekeeper`'s re-exports, so the
//!   defining crate needs no direct dependency on either; to
//!   `impl lakekeeper::audit::AuditPart for T { type Emitter = crate::audit_emitter::Emitter; }`,
//!   binding the type to the emitter of the crate that defines it; and to a registry entry.
//!   Every named field needs a doc comment: it is the field's description in the schema and in
//!   the generated reference.
//! - **A context** (`#[audit_part(context)]` on a struct): the `context` object of an operation
//!   record. Same as a part, registered as a context.
//! - **A vocabulary enum** (`#[audit_part(field = "outcome")]`): its variant *names* are the
//!   values of one wire field. Variants may carry data; only the name reaches the wire. Expands
//!   to `as_wire(&self) -> WireStr<Emitter>` and to `WIRE_VARIANTS` / `WIRE_NAMES`, and to a
//!   registry entry carrying the value list. No derives are added and `AuditPart` is not
//!   implemented: such an enum often already serializes differently for an API.
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

use heck::{ToKebabCase, ToLowerCamelCase, ToPascalCase, ToShoutySnakeCase, ToSnakeCase};
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
    /// `context`: this struct is the `context` object of an operation record.
    context: bool,
}

impl Parse for Args {
    fn parse(input: ParseStream) -> Result<Self> {
        let mut args = Args::default();
        let metas = Punctuated::<Meta, Token![,]>::parse_terminated(input)?;
        for meta in metas {
            match &meta {
                Meta::Path(p) if p.is_ident("context") => args.context = true,
                Meta::NameValue(nv) if nv.path.is_ident("field") => {
                    args.field = Some(lit_str(&nv.value, "field")?);
                }
                other => {
                    return Err(Error::new_spanned(
                        other,
                        "unknown argument: expected `field = \"<wire field>\"` or `context`",
                    ));
                }
            }
        }
        if args.context && args.field.is_some() {
            return Err(input.error("`context` and `field` cannot be combined"));
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

fn expand(args: &Args, input: &DeriveInput) -> Result<TokenStream2> {
    reject_type_generics(input)?;
    match (&input.data, &args.field, args.context) {
        (Data::Enum(e), Some(field), _) => expand_vocabulary(input, e, field),
        (Data::Struct(_) | Data::Enum(_), None, _) => expand_part(input, args.context),
        (Data::Struct(_), Some(_), _) => Err(Error::new_spanned(
            &input.ident,
            "`field = ...` is for vocabulary enums; a struct is a part or a `context`",
        )),
        (Data::Union(_), _, _) => Err(Error::new_spanned(
            &input.ident,
            "unions cannot be audit parts",
        )),
    }
}

/// A part or a context: derives, `AuditPart`, registration with the schema.
fn expand_part(input: &DeriveInput, context: bool) -> Result<TokenStream2> {
    if matches!(input.data, Data::Enum(_)) && context {
        return Err(Error::new_spanned(&input.ident, "`context` is for structs"));
    }
    require_field_docs(input)?;
    let ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let static_ty = static_type(input);
    let kind = if context {
        quote!(::lakekeeper::audit::Kind::Context)
    } else {
        quote!(::lakekeeper::audit::Kind::Part)
    };
    let item = strip_our_attrs(input);
    Ok(quote! {
        #[derive(::lakekeeper::__private::serde::Serialize, ::lakekeeper::__private::schemars::JsonSchema)]
        #[serde(crate = "::lakekeeper::__private::serde")]
        #[schemars(crate = "::lakekeeper::__private::schemars")]
        #item

        impl #impl_generics ::lakekeeper::audit::AuditPart for #ident #ty_generics #where_clause {
            type Emitter = crate::audit_emitter::Emitter;
        }

        #[cfg(debug_assertions)]
        ::lakekeeper::__private::inventory::submit! {
            ::lakekeeper::audit::Registration {
                kind: #kind,
                type_name: || ::core::any::type_name::<#static_ty>(),
                emitter_name: <crate::audit_emitter::Emitter as ::lakekeeper::audit::AuditEmitter>::NAME,
                emitter_type: || ::core::any::type_name::<crate::audit_emitter::Emitter>(),
                defining_crate: env!("CARGO_PKG_NAME"),
                schema_name: ::core::option::Option::Some(|| <#static_ty as ::lakekeeper::__private::schemars::JsonSchema>::schema_name()),
                schema: ::core::option::Option::Some(|generator| <#static_ty as ::lakekeeper::__private::schemars::JsonSchema>::json_schema(generator)),
                wire_field: ::core::option::Option::None,
                wire_values: &[],
            }
        }
    })
}

/// A vocabulary enum: `as_wire()`, the value lists, registration with the values. No derives,
/// no `AuditPart`.
fn expand_vocabulary(
    input: &DeriveInput,
    data: &syn::DataEnum,
    field: &str,
) -> Result<TokenStream2> {
    let ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let static_ty = static_type(input);
    let rule = rename_all_rule(&input.attrs)?;

    let mut arms = Vec::new();
    let mut names = Vec::new();
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
        arms.push(quote!(#pattern => ::lakekeeper::audit::WireStr::new(#wire)));
        names.push(wire);
    }
    let count = names.len();
    let wire_strs = names
        .iter()
        .map(|n| quote!(::lakekeeper::audit::WireStr::new(#n)));
    let item = strip_our_attrs(input);

    Ok(quote! {
        #item

        impl #impl_generics #ident #ty_generics #where_clause {
            /// The value as it reaches the wire, tied to this crate's emitter.
            #[must_use]
            pub const fn as_wire(&self) -> ::lakekeeper::audit::WireStr<crate::audit_emitter::Emitter> {
                match self { #(#arms),* }
            }

            /// Every value this enum can put on the wire.
            pub const WIRE_VARIANTS: [::lakekeeper::audit::WireStr<crate::audit_emitter::Emitter>; #count] = [#(#wire_strs),*];

            /// Every value this enum can put on the wire, as plain strings.
            pub const WIRE_NAMES: [&'static str; #count] = [#(#names),*];
        }

        impl #impl_generics ::core::convert::From<#ident #ty_generics> for ::lakekeeper::audit::WireStr<crate::audit_emitter::Emitter> #where_clause {
            fn from(value: #ident #ty_generics) -> Self {
                value.as_wire()
            }
        }

        impl #impl_generics ::core::convert::From<#ident #ty_generics> for ::lakekeeper::audit::AnyWireStr #where_clause {
            fn from(value: #ident #ty_generics) -> Self {
                value.as_wire().into()
            }
        }

        #[cfg(debug_assertions)]
        ::lakekeeper::__private::inventory::submit! {
            ::lakekeeper::audit::Registration {
                kind: ::lakekeeper::audit::Kind::Enum,
                type_name: || ::core::any::type_name::<#static_ty>(),
                emitter_name: <crate::audit_emitter::Emitter as ::lakekeeper::audit::AuditEmitter>::NAME,
                emitter_type: || ::core::any::type_name::<crate::audit_emitter::Emitter>(),
                defining_crate: env!("CARGO_PKG_NAME"),
                schema_name: ::core::option::Option::None,
                schema: ::core::option::Option::None,
                wire_field: ::core::option::Option::Some(#field),
                wire_values: &<#static_ty>::WIRE_NAMES,
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
                    "audit field `{name}` needs a doc comment: it is the field's description in the schema and in the generated reference"
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
        Some("kebab-case") | Some("kebab_case") => name.to_kebab_case(),
        Some("camelCase") | Some("camel_case") => name.to_lower_camel_case(),
        Some("PascalCase") => name.to_pascal_case(),
        Some("SCREAMING_SNAKE_CASE") | Some("SCREAMING-KEBAB-CASE") | Some("shouty_snake_case") => {
            name.to_shouty_snake_case()
        }
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
