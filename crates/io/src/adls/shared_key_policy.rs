// Copyright (c) Lakekeeper Authors. All rights reserved.
// Ported and adapted from azure_storage-0.21.0 authorization_policy.rs.

//! Azure Storage `SharedKey` authorization policy for `azure_storage_blob` 0.10+.
//!
//! The new SDK only supports Bearer token (Entra ID) auth out of the box.
//! This policy adds the `Authorization: SharedKey {account}:{sig}` header
//! that is required for storage account key authentication.
//!
//! **Upstream tracking issue**: If `azure_storage_blob` adds a first-class
//! `StorageSharedKeyCredential` (see <https://github.com/Azure/azure-sdk-for-rust/issues/2975>),
//! this entire module — including the manual `x-ms-date` injection and the
//! eight re-declared standard HTTP header constants — can be deleted.

use std::{borrow::Cow, sync::Arc};

use azure_core::{
    credentials::Secret,
    error::{ErrorKind, ResultExt as _},
    hmac::hmac_sha256,
    http::{
        Context, Method, Request, Url,
        headers::{
            AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, HeaderName, Headers, IF_MATCH,
            LAST_MODIFIED, MS_DATE, VERSION,
        },
        policies::{Policy, PolicyResult},
    },
    time::{OffsetDateTime, to_rfc7231},
};

// Standard HTTP headers not re-exported from the new azure_core/typespec_client_core.
const CONTENT_MD5: HeaderName = HeaderName::from_static("content-md5");
const CONTENT_ENCODING: HeaderName = HeaderName::from_static("content-encoding");
const CONTENT_LANGUAGE: HeaderName = HeaderName::from_static("content-language");
const DATE: HeaderName = HeaderName::from_static("date");
const IF_MODIFIED_SINCE: HeaderName = HeaderName::from_static("if-modified-since");
const IF_NONE_MATCH: HeaderName = HeaderName::from_static("if-none-match");
const IF_UNMODIFIED_SINCE: HeaderName = HeaderName::from_static("if-unmodified-since");
const RANGE: HeaderName = HeaderName::from_static("range");

/// A pipeline policy that signs every outgoing request with the Azure Storage
/// [Shared Key authorization scheme].
///
/// [Shared Key authorization scheme]: https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key
#[derive(Debug, Clone)]
pub(crate) struct SharedKeyAuthorizationPolicy {
    account: String,
    key: Secret,
}

impl SharedKeyAuthorizationPolicy {
    /// Create a new policy.
    ///
    /// * `account` – storage account name
    /// * `key`     – the base64-encoded storage account key
    pub(crate) fn new(account: String, key: Secret) -> Self {
        Self { account, key }
    }
}

#[allow(elided_lifetimes_in_paths)]
#[async_trait::async_trait]
impl Policy for SharedKeyAuthorizationPolicy {
    async fn send(
        &self,
        ctx: &Context,
        request: &mut Request,
        next: &[Arc<dyn Policy>],
    ) -> PolicyResult {
        assert!(
            !next.is_empty(),
            "SharedKeyAuthorizationPolicy cannot be the last policy in the pipeline"
        );

        // Don't sign requests that already carry a SAS `sig` query param.
        if !request.url().query_pairs().any(|(k, _)| &*k == "sig") {
            // Inject x-ms-date if not already present. The new SDK pipeline does NOT
            // add this header automatically, but Azure requires it for SharedKey auth.
            if request.headers().get_optional_str(&MS_DATE).is_none() {
                let now = OffsetDateTime::now_utc();
                request.insert_header(MS_DATE, to_rfc7231(&now));
            }
            let auth = generate_authorization(
                request.headers(),
                request.url(),
                request.method(),
                &self.account,
                &self.key,
            )?;
            request.insert_header(AUTHORIZATION, auth);
        }

        next[0].send(ctx, request, &next[1..]).await
    }
}

// ── Internal helpers ──────────────────────────────────────────────────────────

fn generate_authorization(
    headers: &Headers,
    url: &Url,
    method: Method,
    account: &str,
    key: &Secret,
) -> azure_core::Result<String> {
    let str_to_sign = string_to_sign(headers, url, method, account);
    let auth = hmac_sha256(&str_to_sign, key).with_context(
        ErrorKind::Credential,
        "failed to HMAC-sign SharedKey authorization",
    )?;
    Ok(format!("SharedKey {account}:{auth}"))
}

fn add_if_exists<'a>(h: &'a Headers, key: &HeaderName) -> &'a str {
    h.get_optional_str(key).unwrap_or_default()
}

/// Build the [string-to-sign] for the Blob/DataLake service (non-Table).
///
/// [string-to-sign]: https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key#constructing-the-signature-string
fn string_to_sign(h: &Headers, u: &Url, method: Method, account: &str) -> String {
    // content-length must be empty when it is 0.
    let content_length = h
        .get_optional_str(&CONTENT_LENGTH)
        .filter(|&v| v != "0")
        .unwrap_or_default();

    format!(
        "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}{}",
        method.as_ref(),
        add_if_exists(h, &CONTENT_ENCODING),
        add_if_exists(h, &CONTENT_LANGUAGE),
        content_length,
        add_if_exists(h, &CONTENT_MD5),
        add_if_exists(h, &CONTENT_TYPE),
        add_if_exists(h, &DATE),
        add_if_exists(h, &IF_MODIFIED_SINCE),
        add_if_exists(h, &IF_MATCH),
        add_if_exists(h, &IF_NONE_MATCH),
        add_if_exists(h, &IF_UNMODIFIED_SINCE),
        add_if_exists(h, &RANGE),
        canonicalize_header(h),
        canonicalized_resource(account, u),
    )
}

/// Build the canonicalized `x-ms-*` header string.
fn canonicalize_header(headers: &Headers) -> String {
    let mut names: Vec<&HeaderName> = headers
        .iter()
        .filter_map(|(k, _)| k.as_str().starts_with("x-ms").then_some(k))
        .collect();
    names.sort_unstable_by_key(|n| n.as_str());

    let mut result = String::new();
    for name in names {
        let value = headers.get_optional_str(name).unwrap_or_default();
        result.push_str(name.as_str());
        result.push(':');
        result.push_str(value);
        result.push('\n');
    }
    result
}

/// Build the canonicalized resource string.
fn canonicalized_resource(account: &str, uri: &Url) -> String {
    let mut can_res = String::new();
    can_res.push('/');
    can_res.push_str(account);

    for segment in uri.path_segments().into_iter().flatten() {
        can_res.push('/');
        can_res.push_str(segment);
    }
    can_res.push('\n');

    // Collect unique query-parameter names, sort them, then emit "name:val1,val2…"
    let query_pairs = uri.query_pairs();
    let mut param_names: Vec<String> = Vec::new();
    for (k, _) in query_pairs {
        let owned = k.into_owned();
        if !param_names.iter().any(|x| x == &owned) {
            param_names.push(owned);
        }
    }
    param_names.sort();

    for param in &param_names {
        let mut values: Vec<Cow<'_, str>> = query_pairs
            .filter(|(k, _)| k.as_ref() == param.as_str())
            .map(|(_, v)| v)
            .collect();
        values.sort_unstable();

        can_res.push_str(&param.to_lowercase());
        can_res.push(':');
        can_res.push_str(&values.join(","));
        can_res.push('\n');
    }

    // Remove trailing newline.
    if can_res.ends_with('\n') {
        can_res.pop();
    }
    can_res
}

// Keep these in scope to avoid dead-code warnings for imported but only
// indirectly-used constants from azure_core::http::headers.
const _: HeaderName = LAST_MODIFIED;
const _: HeaderName = MS_DATE;
const _: HeaderName = VERSION;
