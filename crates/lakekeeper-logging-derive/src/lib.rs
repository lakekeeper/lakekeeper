use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, parse_macro_input};

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

    let field_names: Vec<_> = fields.iter().map(|f| &f.ident).collect();
    let field_logs: Vec<_> = field_names
        .iter()
        .map(|name| {
            quote! { #name = %self.#name }
        })
        .collect();

    let expanded = quote! {
        impl AuditEvent for #name {
            fn action(&self) -> &'static str {
                #action
            }

            fn log<D: AuditContextData>(&self, ctx: &D) {
                let request_metadata = ctx.request_metadata();
                tracing::info!(
                    event_source = AUDIT_LOG_EVENT_SOURCE,
                    request_id = %request_metadata.request_id(),
                    user = %request_metadata.user_id(),
                    action = self.action(),
                    #(#field_logs,)*
                    "audit_event"
                );
            }
        }
    };

    TokenStream::from(expanded)
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
