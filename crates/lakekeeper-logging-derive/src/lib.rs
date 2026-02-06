use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, parse_macro_input};

/// Derive macro for implementing the `AuditEvent` trait.
///
/// # Struct-level Attribute
///
/// Use `#[audit("custom_action_name")]` on the struct to override the default action name.
/// By default, the action name is derived from the struct name in snake_case.
///
/// # Field Attributes
///
/// - `#[audit(skip)]` - Skip this field entirely from logging
/// - `#[audit(debug)]` - Use Debug formatting (`?`) instead of the default Valuable formatting
/// - `#[audit(skip_none)]` - For `Option<T>` fields: skip the field when `None`, log the inner value when `Some`
///
/// By default, all fields use `valuable::Valuable` for structured logging. Use `#[audit(debug)]`
/// for types that don't implement `Valuable`.
///
/// Attributes can be combined, e.g., `#[audit(skip_none, debug)]`
///
/// # Examples
///
/// ```ignore
/// use lakekeeper_logging_derive::AuditEvent;
///
/// #[derive(AuditEvent)]
/// #[audit("user_login")]  // Custom action name
/// struct LoginEvent {
///     username: String,           // Uses Valuable (default)
///     #[audit(skip)]
///     password_hash: String,      // Never logged
/// }
///
/// #[derive(AuditEvent)]
/// struct TransactionEvent {
///     transaction_id: String,     // Uses Valuable (default)
///     #[audit(skip_none)]
///     error_message: Option<String>,  // Only logged when Some, uses Valuable
///     #[audit(debug)]
///     custom_id: CustomId,        // Uses Debug (no Valuable impl)
///     tags: Vec<String>,          // Uses Valuable (default)
/// }
/// ```
///
/// # Notes on Valuable (Default)
///
/// All fields use `valuable::Valuable` by default for structured logging. This works for:
/// - Primitives: `String`, `&str`, `i32`, `u64`, `bool`, etc.
/// - Collections: `Vec<T>`, `HashMap<K,V>`, `BTreeMap<K,V>`
/// - Custom types with `#[derive(Valuable)]`
///
/// For types without `Valuable`, use `#[audit(debug)]` to fall back to Debug formatting.
///
/// # Notes on `skip_none`
///
/// When using `skip_none`, the macro uses `tracing::Span` to conditionally record fields.
/// All events use span-based logging for consistent output format.
#[proc_macro_derive(AuditEvent, attributes(audit))]
pub fn derive_audit_event(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let action = input
        .attrs
        .iter()
        .find(|a| a.path().is_ident("audit"))
        .and_then(|a| a.parse_args::<syn::LitStr>().ok())
        .map(|lit| lit.value())
        .unwrap_or_else(|| to_snake_case(&name.to_string()));

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => panic!("AuditEvent only supports named fields"),
        },
        _ => panic!("AuditEvent only supports structs"),
    };

    let field_attrs: Vec<FieldAttrs> = fields.iter().map(parse_field_attrs).collect();

    // Generate the static field logs (fields that are always present)
    let static_field_logs: Vec<_> = fields
        .iter()
        .zip(field_attrs.iter())
        .filter_map(|(field, attrs)| {
            if attrs.skip || attrs.skip_none {
                return None;
            }
            let name = &field.ident;
            Some(generate_field_log(name, attrs))
        })
        .collect();

    // Generate the conditional field logs for skip_none fields
    let skip_none_fields: Vec<_> = fields
        .iter()
        .zip(field_attrs.iter())
        .filter_map(|(field, attrs)| {
            if !attrs.skip_none {
                return None;
            }
            let name = &field.ident;
            let name_str = name.as_ref().map(|n| n.to_string()).unwrap_or_default();
            Some((name, name_str, attrs.clone()))
        })
        .collect();

    let has_skip_none = !skip_none_fields.is_empty();

    // Generate conditional recording for skip_none fields
    // Default is Valuable; use #[audit(skip_none, debug)] for Debug formatting
    let skip_none_records: Vec<_> = skip_none_fields
        .iter()
        .map(|(name, name_str, attrs)| {
            if attrs.debug {
                quote! {
                    if let Some(ref val) = self.#name {
                        span.record(#name_str, tracing::field::debug(val));
                    }
                }
            } else {
                // Default: use Valuable for structured logging
                quote! {
                    if let Some(ref val) = self.#name {
                        span.record(#name_str, tracing::field::valuable(&valuable::Valuable::as_value(val)));
                    }
                }
            }
        })
        .collect();

    // Generate Empty field declarations for span
    let skip_none_empty_fields: Vec<_> = skip_none_fields
        .iter()
        .map(|(name, _, _)| {
            quote! { #name = tracing::field::Empty }
        })
        .collect();

    // Generate different implementations based on whether we have skip_none fields
    let expanded = if has_skip_none {
        // Use span-based logging for skip_none fields
        quote! {
            impl AuditEvent for #name {
                fn action(&self) -> &'static str {
                    #action
                }

                fn log<D: AuditContextData>(&self, ctx: &D) {
                    let request_metadata = ctx.request_metadata();

                    let user = request_metadata
                        .user_id()
                        .map_or("anonymous".to_string(), std::string::ToString::to_string);

                    let span = tracing::info_span!(
                        #action,
                        event_source = AUDIT_LOG_EVENT_SOURCE,
                        request_id = %request_metadata.request_id(),
                        user,
                        #(#static_field_logs,)*
                        #(#skip_none_empty_fields,)*
                    );

                    #(#skip_none_records)*

                    span.in_scope(|| {
                        tracing::info!("{}", self.action());
                    });
                }

                fn log_without_context(&self) {
                    let span = tracing::info_span!(
                        #action,
                        event_source = AUDIT_LOG_EVENT_SOURCE,
                        #(#static_field_logs,)*
                        #(#skip_none_empty_fields,)*
                    );

                    #(#skip_none_records)*

                    span.in_scope(|| {
                        tracing::info!("{}", self.action());
                    });
                }
            }
        }
    } else {
        // Use span-based logging for consistency (same format as skip_none events)
        quote! {
            impl AuditEvent for #name {
                fn action(&self) -> &'static str {
                    #action
                }

                fn log<D: AuditContextData>(&self, ctx: &D) {
                    let request_metadata = ctx.request_metadata();

                    let user = request_metadata
                        .user_id()
                        .map_or("anonymous".to_string(), std::string::ToString::to_string);

                    let span = tracing::info_span!(
                        #action,
                        event_source = AUDIT_LOG_EVENT_SOURCE,
                        request_id = %request_metadata.request_id(),
                        user,
                        #(#static_field_logs,)*
                    );

                    span.in_scope(|| {
                        tracing::info!("{}", self.action());
                    });
                }

                fn log_without_context(&self) {
                    let span = tracing::info_span!(
                        #action,
                        event_source = AUDIT_LOG_EVENT_SOURCE,
                        #(#static_field_logs,)*
                    );

                    span.in_scope(|| {
                        tracing::info!("{}", self.action());
                    });
                }
            }
        }
    };

    TokenStream::from(expanded)
}

/// Parsed field attributes
#[derive(Clone, Default)]
struct FieldAttrs {
    skip: bool,
    debug: bool,
    skip_none: bool,
}

/// Parse attributes for a single field
fn parse_field_attrs(field: &syn::Field) -> FieldAttrs {
    let mut attrs = FieldAttrs::default();

    for attr in &field.attrs {
        if !attr.path().is_ident("audit") {
            continue;
        }

        // Try to parse as a list of idents (e.g., #[audit(debug, skip_none)])
        if let Ok(nested) = attr.parse_args_with(
            syn::punctuated::Punctuated::<syn::Ident, syn::Token![,]>::parse_terminated,
        ) {
            for ident in nested {
                match ident.to_string().as_str() {
                    "skip" => attrs.skip = true,
                    "debug" => attrs.debug = true,
                    "skip_none" => attrs.skip_none = true,
                    other => panic!("Unknown audit attribute: {other}"),
                }
            }
        } else if let Ok(ident) = attr.parse_args::<syn::Ident>() {
            // Fallback to single ident for backwards compatibility
            match ident.to_string().as_str() {
                "skip" => attrs.skip = true,
                "debug" => attrs.debug = true,
                "skip_none" => attrs.skip_none = true,
                other => panic!("Unknown audit attribute: {other}"),
            }
        }
    }

    attrs
}

/// Generate the field log expression based on attributes
/// Default is Valuable formatting; use #[audit(debug)] for Debug formatting
fn generate_field_log(name: &Option<syn::Ident>, attrs: &FieldAttrs) -> proc_macro2::TokenStream {
    if attrs.debug {
        quote! { #name = ?self.#name }
    } else {
        // Default: use Valuable for structured logging
        quote! { #name = tracing::field::valuable(&valuable::Valuable::as_value(&self.#name)) }
    }
}

fn to_snake_case(s: &str) -> String {
    let mut result = String::new();
    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() && i > 0 {
            result.push('_');
        }
        result.push(c.to_lowercase().next().unwrap());
    }
    result
}
