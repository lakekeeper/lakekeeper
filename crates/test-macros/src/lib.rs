//! `#[lakekeeper_storage_postgres::db_test]`: runs `async fn name(pool: PgPool)` as a test on
//! its own database, copied from a template that has every core migration applied.

use proc_macro::TokenStream;
use quote::quote;
use syn::{Error, ItemFn, ReturnType, parse_macro_input};

/// Runs `async fn name(pool: PgPool)` as a test on its own copy of the migrated template
/// database. See `lakekeeper_storage_postgres::test_utils::run_test`.
///
/// A test that checks migrations needs an empty database: give it `#[sqlx::test]`.
#[proc_macro_attribute]
pub fn db_test(args: TokenStream, input: TokenStream) -> TokenStream {
    if !args.is_empty() {
        return Error::new_spanned(
            proc_macro2::TokenStream::from(args),
            "`#[lakekeeper_storage_postgres::db_test]` takes no arguments",
        )
        .into_compile_error()
        .into();
    }
    let input = parse_macro_input!(input as ItemFn);
    if input.sig.asyncness.is_none() {
        return Error::new_spanned(input.sig.fn_token, "the test function must be `async`")
            .into_compile_error()
            .into();
    }
    if let ReturnType::Type(..) = input.sig.output {
        return Error::new_spanned(&input.sig.output, "the test function returns nothing")
            .into_compile_error()
            .into();
    }

    let ItemFn {
        attrs,
        vis,
        sig,
        block,
    } = &input;
    let name = &sig.ident;
    let inputs = &sig.inputs;
    quote! {
        #(#attrs)*
        #[::core::prelude::v1::test]
        #vis fn #name() {
            async fn #name(#inputs) #block

            ::lakekeeper_storage_postgres::test_utils::run_test(
                concat!(module_path!(), "::", stringify!(#name)),
                #name,
            );
        }
    }
    .into()
}
