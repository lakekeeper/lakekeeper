//! `#[audit_part]`: the one attribute every type that reaches the Lakekeeper audit log
//! carries.
//!
//! On a struct or enum it expands to:
//!
//! - `#[derive(Serialize, JsonSchema)]`, through `lakekeeper`'s re-exports so the defining
//!   crate needs no direct dependency on either;
//! - `impl lakekeeper::audit::AuditPart for T { type Emitter = crate::audit_emitter::Emitter; }`,
//!   binding the type to the emitter of the crate that defines it;
//! - a registry entry (`inventory::submit!`) so the emitter's schema and the registry tests
//!   see the type without any list being maintained. **Debug builds only**: the entry is
//!   behind `#[cfg(debug_assertions)]`, so release binaries carry neither the registry nor
//!   the schema-generation code it keeps alive. Tests that read the registry must run in the
//!   dev profile; in a `--release` test build the registry is empty, and
//!   `lakekeeper::audit::Registration::require_registry()` says so instead of failing
//!   obscurely;
//! - for a vocabulary enum, `#[audit_part(field = "outcome")]`, an `as_wire()` yielding the
//!   serde-renamed variant name as a `WireStr<Emitter>`, and `WIRE_VARIANTS`.
//!
//! Arguments: none for a part; `field = "<wire field>"` for a vocabulary enum, whose variants
//! must be unit variants; `context` for the `context` object of an operation record.
//!
//! Rules enforced at expansion: no type or const generic parameters (lifetimes are fine and
//! become `'static` in the registry); every struct field carries a doc comment, because the
//! doc comment is the customer-facing field description.

use heck::{ToKebabCase, ToLowerCamelCase, ToPascalCase, ToShoutySnakeCase, ToSnakeCase};
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    Attribute, Data, DeriveInput, Error, Expr, Fields, GenericParam, Lit, Meta, Result, Token,
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
};

/// The parsed attribute arguments.
#[derive(Default)]
struct Args {
    /// `field = "outcome"`: this enum's variants are the values of that wire field.
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
    let ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    // The type with every lifetime replaced by `'static`, for the registry's function pointers.
    let static_ty = static_type(input);

    let kind;
    let mut extra = TokenStream2::new();
    let wire_field = match (&input.data, &args.field, args.context) {
        (Data::Enum(e), Some(field), _) => {
            kind = quote!(::lakekeeper::audit::Kind::Enum);
            extra.extend(vocabulary_impl(
                input,
                e,
                &impl_generics,
                &ty_generics,
                where_clause,
            )?);
            quote!(::core::option::Option::Some(#field))
        }
        (Data::Enum(_), None, false) | (Data::Struct(_), None, false) => {
            require_field_docs(input)?;
            kind = quote!(::lakekeeper::audit::Kind::Part);
            quote!(::core::option::Option::None)
        }
        (Data::Struct(_), None, true) => {
            require_field_docs(input)?;
            kind = quote!(::lakekeeper::audit::Kind::Context);
            quote!(::core::option::Option::None)
        }
        (Data::Struct(_), Some(_), _) => {
            return Err(Error::new_spanned(
                ident,
                "`field = ...` is for vocabulary enums; a struct is a part or a `context`",
            ));
        }
        (Data::Enum(_), _, true) => {
            return Err(Error::new_spanned(ident, "`context` is for structs"));
        }
        (Data::Union(_), _, _) => {
            return Err(Error::new_spanned(ident, "unions cannot be audit parts"));
        }
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
                schema_name: || <#static_ty as ::lakekeeper::__private::schemars::JsonSchema>::schema_name(),
                schema: |generator| <#static_ty as ::lakekeeper::__private::schemars::JsonSchema>::json_schema(generator),
                wire_field: #wire_field,
            }
        }

        #extra
    })
}

/// The attribute is applied before derives run, so the item we re-emit must not carry
/// `#[audit_part]` again; everything else (serde, schemars, doc attributes) passes through.
fn strip_our_attrs(input: &DeriveInput) -> DeriveInput {
    let mut out = input.clone();
    out.attrs.retain(|a| !a.path().is_ident("audit_part"));
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

/// `serde(rename_all = "...")` on the enum, if any.
fn rename_all(attrs: &[Attribute]) -> Result<Option<String>> {
    let mut out = None;
    for attr in attrs.iter().filter(|a| a.path().is_ident("serde")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename_all") {
                let value: Lit = meta.value()?.parse()?;
                if let Lit::Str(s) = value {
                    out = Some(s.value());
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

/// `serde(rename = "...")` on a variant, if any.
fn rename(attrs: &[Attribute]) -> Result<Option<String>> {
    let mut out = None;
    for attr in attrs.iter().filter(|a| a.path().is_ident("serde")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                let value: Lit = meta.value()?.parse()?;
                if let Lit::Str(s) = value {
                    out = Some(s.value());
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

fn apply_rename_all(rule: Option<&str>, name: &str) -> Result<String> {
    Ok(match rule {
        None => name.to_string(),
        Some("snake_case") => name.to_snake_case(),
        Some("kebab-case") => name.to_kebab_case(),
        Some("camelCase") => name.to_lower_camel_case(),
        Some("PascalCase") => name.to_pascal_case(),
        Some("SCREAMING_SNAKE_CASE") => name.to_shouty_snake_case(),
        Some("lowercase") => name.to_lowercase(),
        Some("UPPERCASE") => name.to_uppercase(),
        Some(other) => {
            return Err(Error::new(
                proc_macro2::Span::call_site(),
                format!("unsupported serde rename_all rule `{other}` on a vocabulary enum"),
            ));
        }
    })
}

/// `as_wire()` and `WIRE_VARIANTS` for a vocabulary enum.
fn vocabulary_impl(
    input: &DeriveInput,
    data: &syn::DataEnum,
    impl_generics: &syn::ImplGenerics,
    ty_generics: &syn::TypeGenerics,
    where_clause: Option<&syn::WhereClause>,
) -> Result<TokenStream2> {
    let ident = &input.ident;
    let rule = rename_all(&input.attrs)?;
    let mut arms = Vec::new();
    let mut variants = Vec::new();
    for v in &data.variants {
        if !matches!(v.fields, Fields::Unit) {
            return Err(Error::new_spanned(
                v,
                "a vocabulary enum (`field = ...`) has unit variants only: each variant is one wire value",
            ));
        }
        let wire = match rename(&v.attrs)? {
            Some(explicit) => explicit,
            None => apply_rename_all(rule.as_deref(), &v.ident.to_string())?,
        };
        let vident = &v.ident;
        arms.push(quote!(Self::#vident => ::lakekeeper::audit::WireStr::new(#wire)));
        variants.push(quote!(::lakekeeper::audit::WireStr::new(#wire)));
    }
    let count = variants.len();
    let variants_ident = format_ident!("WIRE_VARIANTS");
    Ok(quote! {
        impl #impl_generics #ident #ty_generics #where_clause {
            /// The value as it reaches the wire, tied to this crate's emitter.
            #[must_use]
            pub const fn as_wire(self) -> ::lakekeeper::audit::WireStr<crate::audit_emitter::Emitter> {
                match self { #(#arms),* }
            }

            /// Every value this enum can put on the wire.
            pub const #variants_ident: [::lakekeeper::audit::WireStr<crate::audit_emitter::Emitter>; #count] = [#(#variants),*];
        }
    })
}
