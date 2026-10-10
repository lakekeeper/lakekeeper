//! What a request to sign may carry. S3 and the stores compatible with it dispatch on query
//! parameters and on `x-` headers, so one that is not listed here could make the signed
//! request do something no table location authorizes. Both are refused unless listed.
//!
//! Standard headers are not restricted: stores do not require them to be signed, so a client
//! could add them after signing anyway.

use std::collections::{BTreeMap, HashMap};

use crate::api::{ErrorModel, Result};

// ----- Query parameters -----

/// Identifies a `ListObjectsV2` request, together with its only valid value.
pub(super) const LIST_TYPE_QUERY_PARAM: &str = "list-type";
pub(super) const LIST_TYPE_V2: &str = "2";
/// The key prefix a list request is scoped to.
pub(super) const PREFIX_QUERY_PARAM: &str = "prefix";
/// Identifies a `DeleteObjects` request.
pub(super) const DELETE_QUERY_PARAM: &str = "delete";
/// Selects one version of an object.
pub(super) const VERSION_ID_QUERY_PARAM: &str = "versionId";
/// Sub-resource holding the tags of an object.
const TAGGING_QUERY_PARAM: &str = "tagging";
/// Telemetry parameter some clients append to name the operation. S3 ignores it.
const X_ID_QUERY_PARAM: &str = "x-id";

/// What a request addresses, which decides the query parameters it may carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RequestKind {
    /// One object, named by the path.
    Object,
    /// `ListObjectsV2`: the keys under its `prefix` parameter.
    List,
    /// `DeleteObjects`: the keys in its body.
    DeleteObjects,
}

/// The query parameters one [`RequestKind`] may carry, and how its errors name it.
struct QueryPolicy {
    keys: &'static [&'static str],
    key_prefixes: &'static [&'static str],
    request: &'static str,
    unsupported: &'static str,
    repeated: &'static str,
}

/// Reading, writing and deleting an object, multipart uploads, tags and attributes. None of
/// them reaches a key other than the one in the path.
const OBJECT_QUERY: QueryPolicy = QueryPolicy {
    keys: &[
        "attributes",
        "max-parts",
        "part-number-marker",
        "partNumber",
        TAGGING_QUERY_PARAM,
        "uploadId",
        "uploads",
        VERSION_ID_QUERY_PARAM,
        X_ID_QUERY_PARAM,
    ],
    // `GetObject` and `HeadObject` override the headers of their response with these.
    key_prefixes: &["response-"],
    request: "an object request",
    unsupported: "UnsupportedObjectParameter",
    repeated: "RepeatedObjectParameter",
};

/// None of them widens the set of keys `prefix` selects.
const LIST_QUERY: QueryPolicy = QueryPolicy {
    keys: &[
        LIST_TYPE_QUERY_PARAM,
        PREFIX_QUERY_PARAM,
        "continuation-token",
        "delimiter",
        "encoding-type",
        "fetch-owner",
        "max-keys",
        "start-after",
        X_ID_QUERY_PARAM,
    ],
    key_prefixes: &[],
    request: "a list request",
    unsupported: "UnsupportedListParameter",
    repeated: "RepeatedListParameter",
};

/// The keys a `DeleteObjects` request deletes are in its body.
const DELETE_OBJECTS_QUERY: QueryPolicy = QueryPolicy {
    keys: &[DELETE_QUERY_PARAM, X_ID_QUERY_PARAM],
    key_prefixes: &[],
    request: "a delete request",
    unsupported: "UnsupportedDeleteParameter",
    repeated: "RepeatedDeleteParameter",
};

impl RequestKind {
    fn query(self) -> &'static QueryPolicy {
        match self {
            Self::Object => &OBJECT_QUERY,
            Self::List => &LIST_QUERY,
            Self::DeleteObjects => &DELETE_OBJECTS_QUERY,
        }
    }
}

/// Rejects requests that carry a query parameter `kind` does not allow, or one of them twice.
/// Which of two values for the same parameter a store applies is implementation defined, so
/// only one of them could be authorized.
pub(super) fn require_known_parameters(uri: &url::Url, kind: RequestKind) -> Result<()> {
    let QueryPolicy {
        keys,
        key_prefixes,
        request,
        unsupported,
        repeated,
    } = kind.query();
    // Some servers also split parameters on `;`, which would hide one inside a value.
    if uri.query().is_some_and(|query| query.contains(';')) {
        return Err(ErrorModel::bad_request(
            format!("Unsupported query parameter separator `;` in {request}"),
            *unsupported,
            None,
        )
        .into());
    }

    let mut seen: Vec<std::borrow::Cow<'_, str>> = Vec::new();
    for (key, _) in uri.query_pairs() {
        let allowed = keys.contains(&key.as_ref())
            || key_prefixes.iter().any(|prefix| key.starts_with(prefix));
        if !allowed {
            return Err(ErrorModel::bad_request(
                format!("Unsupported query parameter for {request}: `{key}`"),
                *unsupported,
                None,
            )
            .into());
        }
        if seen.contains(&key) {
            return Err(ErrorModel::bad_request(
                format!("Repeated query parameter in {request}: `{key}`"),
                *repeated,
                None,
            )
            .into());
        }
        seen.push(key);
    }

    Ok(())
}

/// Rejects requests that change one version of an object rather than read it. Deleting or
/// overwriting a version removes it for good, past what bucket versioning keeps to recover
/// from, and no table operation needs it.
pub(super) fn require_unversioned_change(uri: &url::Url, method: &http::Method) -> Result<()> {
    let is_read = matches!(*method, http::Method::GET | http::Method::HEAD);
    if is_read
        || !uri
            .query_pairs()
            .any(|(key, _)| key == VERSION_ID_QUERY_PARAM)
    {
        return Ok(());
    }
    Err(version_change_error().into())
}

pub(super) fn version_change_error() -> ErrorModel {
    ErrorModel::forbidden(
        "Requests that delete or overwrite a version of an object cannot be signed",
        "VersionChangeNotSignable",
        None,
    )
}

// ----- Headers -----

/// Header naming the object a copy reads. Its location is checked like the destination's.
pub(super) const COPY_SOURCE_HEADER: &str = "x-amz-copy-source";
const ACL_HEADER: &str = "x-amz-acl";
const STORAGE_CLASS_HEADER: &str = "x-amz-storage-class";
const SSE_HEADER: &str = "x-amz-server-side-encryption";
const SSE_KMS_KEY_ID_HEADER: &str = "x-amz-server-side-encryption-aws-kms-key-id";
const SSE_KMS_CONTEXT_HEADER: &str = "x-amz-server-side-encryption-context";
const SSE_KMS_BUCKET_KEY_HEADER: &str = "x-amz-server-side-encryption-bucket-key-enabled";
const SSE_CUSTOMER_HEADERS: &[&str] = &[
    "x-amz-server-side-encryption-customer-algorithm",
    "x-amz-server-side-encryption-customer-key",
    "x-amz-server-side-encryption-customer-key-md5",
];

/// The `x-` headers a request may carry. Those whose effect depends on their value are
/// checked by [`require_signable_headers`].
const X_HEADERS: &[&str] = &[
    // Replaced by the signer, not signed, or, for a session token, refused by the store
    // unless it belongs to the signing credentials.
    "x-amz-content-sha256",
    "x-amz-date",
    "x-amz-security-token",
    "x-amzn-trace-id",
    // Integrity checks and transfer encoding.
    "x-amz-decoded-content-length",
    "x-amz-mp-object-size",
    "x-amz-sdk-checksum-algorithm",
    "x-amz-te",
    "x-amz-trailer",
    // Conditions, which only narrow the request.
    "x-amz-copy-source-if-match",
    "x-amz-copy-source-if-modified-since",
    "x-amz-copy-source-if-none-match",
    "x-amz-copy-source-if-unmodified-since",
    "x-amz-expected-bucket-owner",
    "x-amz-if-match-initiated-time",
    "x-amz-if-match-last-modified-time",
    "x-amz-if-match-size",
    "x-amz-source-expected-bucket-owner",
    // Properties of the signed object and how it is read or written.
    "x-amz-copy-source-range",
    "x-amz-copy-source-server-side-encryption-customer-algorithm",
    "x-amz-copy-source-server-side-encryption-customer-key",
    "x-amz-copy-source-server-side-encryption-customer-key-md5",
    "x-amz-max-parts",
    "x-amz-metadata-directive",
    "x-amz-object-attributes",
    "x-amz-part-number-marker",
    "x-amz-request-payer",
    "x-amz-tagging",
    "x-amz-tagging-directive",
    "x-amz-write-offset-bytes",
    // Checked by value.
    ACL_HEADER,
    COPY_SOURCE_HEADER,
    SSE_HEADER,
    SSE_KMS_BUCKET_KEY_HEADER,
    SSE_KMS_CONTEXT_HEADER,
    SSE_KMS_KEY_ID_HEADER,
    SSE_CUSTOMER_HEADERS[0],
    SSE_CUSTOMER_HEADERS[1],
    SSE_CUSTOMER_HEADERS[2],
    STORAGE_CLASS_HEADER,
];

/// Checksums of every algorithm, and user metadata.
const X_HEADER_PREFIXES: &[&str] = &["x-amz-checksum-", "x-amz-meta-"];

/// User metadata some stores act on: they unpack an archive and store each entry under its
/// own key, whatever key was signed.
const ARCHIVE_EXTRACT_HEADER_PREFIXES: &[&str] = &["x-amz-meta-minio-", "x-amz-meta-snowball-"];

/// Header namespaces outside `x-` that some stores dispatch on.
const VENDOR_HEADER_PREFIXES: &[&str] = &["cf-", "ibm-"];

/// Canned ACLs that grant no one but the bucket owner access.
const SIGNABLE_CANNED_ACLS: &[&str] =
    &["private", "bucket-owner-read", "bucket-owner-full-control"];

/// Storage classes whose objects can be read without restoring them first, which is not
/// signable. `INTELLIGENT_TIERING` only needs a restore if the bucket opts into archive tiers.
const READABLE_STORAGE_CLASSES: &[&str] = &[
    "EXPRESS_ONEZONE",
    "GLACIER_IR",
    "INTELLIGENT_TIERING",
    "ONEZONE_IA",
    "STANDARD",
    "STANDARD_IA",
];

/// Released error types for headers clients are likely to try. Whether a header is refused
/// is decided by [`X_HEADERS`] alone.
const REFUSAL_TYPES: &[(&str, &str, &str)] = &[
    (
        "x-amz-grant-",
        "Requests that change access control cannot be signed",
        "AccessControlNotSignable",
    ),
    (
        "x-amz-object-lock-",
        "Requests that change an object lock cannot be signed",
        "ObjectLockNotSignable",
    ),
    (
        "x-amz-bypass-governance-retention",
        "Requests that change an object lock cannot be signed",
        "ObjectLockNotSignable",
    ),
];

/// The headers of a request to sign, by lower-case name. aws-sigv4 signs names that differ
/// only in case as one header, so their values are checked together.
#[derive(Debug)]
pub(super) struct RequestHeaders<'a>(BTreeMap<String, Vec<&'a str>>);

impl<'a> RequestHeaders<'a> {
    /// Refuses names that are not ASCII tokens, which aws-sigv4 would lowercase into another
    /// name, and names containing `_`, which some stores read as `-`.
    pub(super) fn new(headers: &'a HashMap<String, Vec<String>>) -> Result<Self> {
        let mut by_name = BTreeMap::<String, Vec<&'a str>>::new();
        for (name, values) in headers {
            if !name.is_ascii()
                || name.contains('_')
                || http::HeaderName::from_bytes(name.as_bytes()).is_err()
            {
                return Err(ErrorModel::bad_request(
                    format!("Invalid header name `{}`", name.escape_debug()),
                    "InvalidHeaderName",
                    None,
                )
                .into());
            }
            by_name
                .entry(name.to_ascii_lowercase())
                .or_default()
                .extend(values.iter().map(String::as_str));
        }
        Ok(Self(by_name))
    }

    fn contains(&self, name: &str) -> bool {
        self.0.contains_key(name)
    }

    /// The value of a header that takes one, without the spaces and tabs HTTP strips.
    /// Refuses a second value, also one joined with `,`, as stores differ on which they use.
    pub(super) fn single(&self, name: &str) -> Result<Option<&'a str>> {
        let Some(values) = self.0.get(name) else {
            return Ok(None);
        };
        match values.as_slice() {
            [value] if !value.contains(',') => Ok(Some(value.trim_matches([' ', '\t']))),
            _ => Err(ErrorModel::bad_request(
                format!("The header `{name}` must have exactly one value"),
                "InvalidHeaderValue",
                None,
            )
            .into()),
        }
    }
}

/// Rejects requests with a header that is not listed, or whose value would reach beyond the
/// keys the request is authorized for or leave objects the warehouse cannot read.
/// `kms_key` is the key the warehouse encrypts with, if any.
pub(super) fn require_signable_headers(
    headers: &RequestHeaders<'_>,
    method: &http::Method,
    kms_key: Option<&str>,
) -> Result<()> {
    for name in headers.0.keys() {
        require_known_header(name)?;
    }
    require_signable_acl(headers)?;
    require_signable_storage_class(headers)?;
    require_signable_encryption(headers, method, kms_key)?;

    // A copy reads its source, which is checked against the table location with the
    // destination. Only an object write copies.
    if headers.contains(COPY_SOURCE_HEADER) && *method != http::Method::PUT {
        return Err(ErrorModel::forbidden(
            "Only requests that write an object may copy",
            "CopyNotSignable",
            None,
        )
        .into());
    }

    Ok(())
}

fn require_known_header(name: &str) -> Result<()> {
    let is_listed = || {
        X_HEADERS.contains(&name)
            || (X_HEADER_PREFIXES
                .iter()
                .any(|prefix| name.starts_with(prefix))
                && !ARCHIVE_EXTRACT_HEADER_PREFIXES
                    .iter()
                    .any(|prefix| name.starts_with(prefix)))
    };
    let is_restricted = name.starts_with("x-")
        || VENDOR_HEADER_PREFIXES
            .iter()
            .any(|prefix| name.starts_with(prefix));
    if !is_restricted || is_listed() {
        return Ok(());
    }

    let error = match REFUSAL_TYPES
        .iter()
        .find(|(prefix, _, _)| name.starts_with(prefix))
    {
        Some((_, message, r#type)) => ErrorModel::forbidden(*message, *r#type, None),
        None => ErrorModel::forbidden(
            format!("Requests with the header `{name}` cannot be signed"),
            "HeaderNotSignable",
            None,
        ),
    };
    Err(error.into())
}

fn require_signable_acl(headers: &RequestHeaders<'_>) -> Result<()> {
    match headers.single(ACL_HEADER)? {
        Some(acl)
            if !SIGNABLE_CANNED_ACLS
                .iter()
                .any(|signable| acl.eq_ignore_ascii_case(signable)) =>
        {
            Err(ErrorModel::forbidden(
                "Requests that change access control cannot be signed",
                "AccessControlNotSignable",
                None,
            )
            .into())
        }
        _ => Ok(()),
    }
}

fn require_signable_storage_class(headers: &RequestHeaders<'_>) -> Result<()> {
    match headers.single(STORAGE_CLASS_HEADER)? {
        Some(class) if !READABLE_STORAGE_CLASSES.contains(&class) => Err(ErrorModel::forbidden(
            "Objects may only be stored in a class that can be read without restoring them",
            "StorageClassNotSignable",
            None,
        )
        .into()),
        _ => Ok(()),
    }
}

/// A request may leave encryption to the bucket default, or name the warehouse's KMS key, or
/// keys S3 manages if the warehouse has none. A key the client holds would leave objects the warehouse cannot read, so a
/// customer-provided key may only read.
fn require_signable_encryption(
    headers: &RequestHeaders<'_>,
    method: &http::Method,
    kms_key: Option<&str>,
) -> Result<()> {
    let refuse =
        |message: &str| Err(ErrorModel::forbidden(message, "EncryptionNotSignable", None).into());

    let is_read = matches!(*method, http::Method::GET | http::Method::HEAD);
    if !is_read
        && SSE_CUSTOMER_HEADERS
            .iter()
            .any(|name| headers.contains(name))
    {
        return refuse("Objects may not be written with a customer-provided key");
    }

    let encryption = headers.single(SSE_HEADER)?;
    let key_id = headers.single(SSE_KMS_KEY_ID_HEADER)?;
    let has_kms_options =
        headers.contains(SSE_KMS_CONTEXT_HEADER) || headers.contains(SSE_KMS_BUCKET_KEY_HEADER);
    match kms_key {
        // `aws:kms` without a key id encrypts with a key S3 manages, not the warehouse's.
        Some(kms_key) => {
            let is_default = encryption.is_none() && key_id.is_none() && !has_kms_options;
            let is_warehouse_key =
                matches!(encryption, Some("aws:kms" | "aws:kms:dsse")) && key_id == Some(kms_key);
            if is_default || is_warehouse_key {
                Ok(())
            } else {
                refuse("Objects may only be encrypted with the KMS key of the warehouse")
            }
        }
        None => {
            if matches!(encryption, None | Some("AES256")) && key_id.is_none() && !has_kms_options {
                Ok(())
            } else {
                refuse(
                    "Objects may only be encrypted with keys S3 manages, as the warehouse has no KMS key",
                )
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    const OBJECT_URI: &str = "http://s3.example.com:8333/bucket/wh/tbl/data/x.parquet";
    const LIST_URI: &str = "http://s3.example.com:8333/bucket?list-type=2&prefix=wh/tbl/";
    const DELETE_URI: &str = "http://s3.example.com:8333/bucket?delete";
    const KMS_KEY: &str = "arn:aws:kms:eu-central-1:123456789012:key/abc";

    fn owned(headers: &[(&str, &str)]) -> HashMap<String, Vec<String>> {
        headers
            .iter()
            .map(|(name, value)| ((*name).to_string(), vec![(*value).to_string()]))
            .collect()
    }

    fn check(
        headers: &HashMap<String, Vec<String>>,
        method: &http::Method,
        kms_key: Option<&str>,
    ) -> std::result::Result<(), (u16, String)> {
        RequestHeaders::new(headers)
            .and_then(|headers| require_signable_headers(&headers, method, kms_key))
            .map_err(|e| (e.error.code, e.error.r#type))
    }

    fn check_err(headers: &[(&str, &str)], method: &http::Method) -> (u16, String) {
        check(&owned(headers), method, None)
            .expect_err(&format!("{method} with {headers:?} must not be signable"))
    }

    fn query_err(uri: &str, kind: RequestKind) -> (u16, String) {
        let err = require_known_parameters(&url::Url::parse(uri).unwrap(), kind)
            .expect_err(&format!("{uri} must not be signable"));
        (err.error.code, err.error.r#type)
    }

    /// Each listed parameter on its own keeps a request signable.
    #[test]
    fn test_every_listed_query_parameter_is_allowed() {
        for (kind, base) in [
            (RequestKind::Object, OBJECT_URI),
            (RequestKind::List, LIST_URI),
            (RequestKind::DeleteObjects, DELETE_URI),
        ] {
            let base = url::Url::parse(base).unwrap();
            let present = base
                .query_pairs()
                .map(|(key, _)| key.into_owned())
                .collect::<Vec<_>>();
            let policy = kind.query();
            let keys = policy
                .keys
                .iter()
                .filter(|key| !present.iter().any(|present| present == *key))
                .map(|key| (*key).to_string())
                .chain(
                    policy
                        .key_prefixes
                        .iter()
                        .map(|prefix| format!("{prefix}x")),
                );
            for key in keys {
                let mut uri = base.clone();
                uri.query_pairs_mut().append_pair(&key, "v");
                require_known_parameters(&uri, kind)
                    .unwrap_or_else(|e| panic!("{kind:?} {uri} must be signable: {e:?}"));
            }
        }
    }

    /// Sub-resources that would turn a request into another operation, for every kind of
    /// request. A key added to a list here must never become signable by accident.
    #[test]
    fn test_dangerous_query_parameters_are_refused() {
        for key in [
            "acl",
            "ACL",
            "policy",
            "retention",
            "legal-hold",
            "object-lock",
            "restore",
            "select",
            "torrent",
            "renameObject",
            "append",
            "versions",
            "undelete",
            "lifecycle",
            "x-amz-acl",
            "x-amz-copy-source",
            "x-amz-rename-source",
            "X-Amz-Grant-Read",
            "x-goog-acl",
            "X-Amz-Security-Token",
        ] {
            for (kind, uri, expected) in [
                (
                    RequestKind::Object,
                    format!("{OBJECT_URI}?{key}=v"),
                    "UnsupportedObjectParameter",
                ),
                (
                    RequestKind::List,
                    format!("{LIST_URI}&{key}=v"),
                    "UnsupportedListParameter",
                ),
                (
                    RequestKind::DeleteObjects,
                    format!("{DELETE_URI}&{key}=v"),
                    "UnsupportedDeleteParameter",
                ),
            ] {
                assert_eq!(
                    query_err(&uri, kind),
                    (400, expected.to_string()),
                    "Test case: {kind:?} {uri}"
                );
            }
        }
        // Parameters of one kind of request are not parameters of another.
        for (uri, kind, expected) in [
            (
                format!("{LIST_URI}&uploads"),
                RequestKind::List,
                "UnsupportedListParameter",
            ),
            (
                format!("{LIST_URI}&response-content-type=a"),
                RequestKind::List,
                "UnsupportedListParameter",
            ),
            (
                format!("{DELETE_URI}&versionId=v1"),
                RequestKind::DeleteObjects,
                "UnsupportedDeleteParameter",
            ),
            (
                format!("{OBJECT_URI}?prefix=wh"),
                RequestKind::Object,
                "UnsupportedObjectParameter",
            ),
        ] {
            assert_eq!(
                query_err(&uri, kind),
                (400, expected.to_string()),
                "Test case: {uri}"
            );
        }
    }

    #[test]
    fn test_query_separator_and_repeats_are_refused() {
        for (uri, kind, expected) in [
            (
                format!("{OBJECT_URI}?x-id=GetObject;retention"),
                RequestKind::Object,
                "UnsupportedObjectParameter",
            ),
            (
                format!("{LIST_URI};acl"),
                RequestKind::List,
                "UnsupportedListParameter",
            ),
            (
                format!("{OBJECT_URI}?versionId=v1&versionId=v2"),
                RequestKind::Object,
                "RepeatedObjectParameter",
            ),
            (
                format!("{LIST_URI}&prefix=wh/other/"),
                RequestKind::List,
                "RepeatedListParameter",
            ),
            (
                format!("{DELETE_URI}&delete"),
                RequestKind::DeleteObjects,
                "RepeatedDeleteParameter",
            ),
        ] {
            assert_eq!(
                query_err(&uri, kind),
                (400, expected.to_string()),
                "Test case: {uri}"
            );
        }
    }

    #[test]
    fn test_versions_may_be_read() {
        for (method, query) in [
            (http::Method::GET, "?versionId=v1"),
            (http::Method::HEAD, "?versionId=v1"),
            (http::Method::GET, "?tagging&versionId=v1"),
            (http::Method::PUT, "?tagging"),
            (http::Method::DELETE, "?tagging"),
            (http::Method::DELETE, ""),
            (http::Method::PUT, "?partNumber=1&uploadId=abc"),
        ] {
            let uri = url::Url::parse(&format!("{OBJECT_URI}{query}")).unwrap();
            require_unversioned_change(&uri, &method)
                .unwrap_or_else(|e| panic!("{method} {uri} must be signable: {e:?}"));
        }
    }

    #[test]
    fn test_versions_may_not_be_deleted_or_overwritten() {
        for method in [http::Method::DELETE, http::Method::PUT, http::Method::POST] {
            // A store without the tagging sub-resource would delete or overwrite the version.
            for query in ["?versionId=v1", "?tagging&versionId=v1"] {
                let uri = url::Url::parse(&format!("{OBJECT_URI}{query}")).unwrap();
                let err = require_unversioned_change(&uri, &method)
                    .expect_err(&format!("{method} {uri} must not be signable"));
                assert_eq!(
                    (err.error.code, err.error.r#type.as_str()),
                    (403, "VersionChangeNotSignable"),
                    "Test case: {method} {query}"
                );
            }
        }
    }

    /// The headers Iceberg clients send, including what tracing agents and the AWS SDKs add.
    #[test]
    fn test_client_headers_are_allowed() {
        let headers = owned(&[
            ("Host", "s3.example.com:8333"),
            ("Content-Type", "application/octet-stream"),
            ("Content-Length", "10"),
            ("Content-MD5", "abc"),
            ("Content-Encoding", "aws-chunked"),
            ("Expect", "100-continue"),
            ("If-None-Match", "*"),
            ("Range", "bytes=0-9"),
            ("User-Agent", "client"),
            ("amz-sdk-invocation-id", "id"),
            ("amz-sdk-request", "attempt=1"),
            ("traceparent", "00-abc-def-01"),
            ("X-Amz-Content-SHA256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER"),
            ("x-amz-date", "20261008T000000Z"),
            ("X-Amzn-Trace-Id", "Root=1-abc;Parent=def;Sampled=1"),
            ("x-amz-te", "append-md5"),
            ("X-Amz-Trailer", "x-amz-checksum-crc32"),
            ("X-Amz-Decoded-Content-Length", "10"),
            ("x-amz-sdk-checksum-algorithm", "CRC32"),
            ("x-amz-checksum-crc32", "abc"),
            ("x-amz-checksum-crc64nvme", "abc"),
            ("x-amz-checksum-mode", "ENABLED"),
            ("x-amz-meta-iceberg", "1"),
            ("x-amz-tagging", "a=b"),
            ("x-amz-storage-class", "STANDARD_IA"),
            ("x-amz-acl", "bucket-owner-full-control"),
            ("x-amz-server-side-encryption", "AES256"),
            ("x-amz-expected-bucket-owner", "123456789012"),
            ("x-amz-request-payer", "requester"),
        ]);
        for method in [http::Method::GET, http::Method::PUT, http::Method::POST] {
            check(&headers, &method, None)
                .unwrap_or_else(|e| panic!("{method} must be signable: {e:?}"));
        }
    }

    #[test]
    fn test_unlisted_headers_are_refused() {
        for (header, expected) in [
            ("x-amz-grant-read", "AccessControlNotSignable"),
            ("X-Amz-Grant-Full-Control", "AccessControlNotSignable"),
            ("x-amz-object-lock-mode", "ObjectLockNotSignable"),
            (
                "X-Amz-Object-Lock-Retain-Until-Date",
                "ObjectLockNotSignable",
            ),
            ("x-amz-object-lock-legal-hold", "ObjectLockNotSignable"),
            ("x-amz-bypass-governance-retention", "ObjectLockNotSignable"),
            ("x-amz-rename-source", "HeaderNotSignable"),
            ("x-amz-rename-source-if-match", "HeaderNotSignable"),
            ("x-amz-website-redirect-location", "HeaderNotSignable"),
            ("x-amz-object-annotation-directive", "HeaderNotSignable"),
            ("x-amz-mfa", "HeaderNotSignable"),
            ("x-amz-not-yet-invented", "HeaderNotSignable"),
            // Unpacking an archive stores each entry under its own key.
            ("x-amz-meta-snowball-auto-extract", "HeaderNotSignable"),
            ("X-Amz-Meta-Minio-Snowball-Prefix", "HeaderNotSignable"),
            // Namespaces other stores dispatch on.
            ("x-goog-copy-source", "HeaderNotSignable"),
            ("x-minio-force-delete", "HeaderNotSignable"),
            ("x-oss-callback", "HeaderNotSignable"),
            ("x-rgw-object-type", "HeaderNotSignable"),
            ("x-scal-s3-version-id", "HeaderNotSignable"),
            ("X-Copy-From", "HeaderNotSignable"),
            ("cf-create-bucket-if-missing", "HeaderNotSignable"),
            ("ibm-sse-kp-encryption-algorithm", "HeaderNotSignable"),
        ] {
            for method in [http::Method::GET, http::Method::PUT] {
                assert_eq!(
                    check_err(&[("content-type", "a"), (header, "v")], &method),
                    (403, expected.to_string()),
                    "Test case: {method} {header}"
                );
            }
        }
    }

    #[test]
    fn test_ambiguous_header_names_are_refused() {
        for header in [
            "x_amz_copy_source",
            "X_Amz_Acl",
            // KELVIN SIGN lowercases to `k`, which would sign `x-amz-object-lock-mode`.
            "x-amz-object-loc\u{212A}-mode",
            "x-amz-acl ",
            "x-amz-acl:",
            "",
        ] {
            assert_eq!(
                check_err(&[(header, "v")], &http::Method::PUT),
                (400, "InvalidHeaderName".to_string()),
                "Test case: {header:?}"
            );
        }
    }

    /// aws-sigv4 joins the values of names that differ only in case, so a store would read
    /// both.
    #[test]
    fn test_single_valued_headers_take_one_value() {
        let spellings = HashMap::from([
            (
                "x-amz-storage-class".to_string(),
                vec!["STANDARD".to_string()],
            ),
            (
                "X-Amz-Storage-Class".to_string(),
                vec!["GLACIER".to_string()],
            ),
        ]);
        let repeated = HashMap::from([(
            "x-amz-acl".to_string(),
            vec!["private".to_string(), "public-read".to_string()],
        )]);
        for headers in [
            spellings,
            repeated,
            owned(&[("x-amz-acl", "private,public-read")]),
            owned(&[("x-amz-server-side-encryption", "AES256,aws:kms")]),
        ] {
            assert_eq!(
                check(&headers, &http::Method::PUT, None),
                Err((400, "InvalidHeaderValue".to_string())),
                "Test case: {headers:?}"
            );
        }
    }

    #[test]
    fn test_canned_acls() {
        for acl in [
            "bucket-owner-full-control",
            "Bucket-Owner-Full-Control",
            "bucket-owner-read",
            "private",
            " private\t",
        ] {
            check(&owned(&[("X-Amz-Acl", acl)]), &http::Method::PUT, None)
                .unwrap_or_else(|e| panic!("{acl} must be signable: {e:?}"));
        }
        for acl in [
            "public-read",
            "public-read-write",
            "authenticated-read",
            "aws-exec-read",
            "log-delivery-write",
            "private\u{85}",
            "",
        ] {
            assert_eq!(
                check_err(&[("x-amz-acl", acl)], &http::Method::PUT),
                (403, "AccessControlNotSignable".to_string()),
                "Test case: {acl:?}"
            );
        }
    }

    #[test]
    fn test_storage_classes() {
        for class in READABLE_STORAGE_CLASSES {
            check(
                &owned(&[("x-amz-storage-class", class)]),
                &http::Method::PUT,
                None,
            )
            .unwrap_or_else(|e| panic!("{class} must be signable: {e:?}"));
        }
        for class in [
            "GLACIER",
            "DEEP_ARCHIVE",
            "REDUCED_REDUNDANCY",
            "OUTPOSTS",
            "standard",
            "",
        ] {
            assert_eq!(
                check_err(&[("x-amz-storage-class", class)], &http::Method::PUT),
                (403, "StorageClassNotSignable".to_string()),
                "Test case: {class:?}"
            );
        }
    }

    #[test]
    fn test_encryption_with_the_warehouse_key() {
        for headers in [
            vec![],
            vec![
                ("x-amz-server-side-encryption", "aws:kms"),
                ("x-amz-server-side-encryption-aws-kms-key-id", KMS_KEY),
            ],
            vec![
                ("x-amz-server-side-encryption", "aws:kms:dsse"),
                ("x-amz-server-side-encryption-aws-kms-key-id", KMS_KEY),
                ("x-amz-server-side-encryption-context", "e30="),
                ("x-amz-server-side-encryption-bucket-key-enabled", "true"),
            ],
        ] {
            check(&owned(&headers), &http::Method::PUT, Some(KMS_KEY))
                .unwrap_or_else(|e| panic!("{headers:?} must be signable: {e:?}"));
        }
        for headers in [
            vec![("x-amz-server-side-encryption", "aws:kms")],
            vec![("x-amz-server-side-encryption", "AES256")],
            vec![
                ("x-amz-server-side-encryption", "aws:kms"),
                (
                    "x-amz-server-side-encryption-aws-kms-key-id",
                    "arn:aws:kms:eu-central-1:999999999999:key/other",
                ),
            ],
            vec![("x-amz-server-side-encryption-aws-kms-key-id", KMS_KEY)],
            vec![("x-amz-server-side-encryption-bucket-key-enabled", "true")],
        ] {
            assert_eq!(
                check(&owned(&headers), &http::Method::PUT, Some(KMS_KEY)),
                Err((403, "EncryptionNotSignable".to_string())),
                "Test case: {headers:?}"
            );
        }
    }

    #[test]
    fn test_encryption_without_a_warehouse_key() {
        for headers in [vec![], vec![("x-amz-server-side-encryption", "AES256")]] {
            check(&owned(&headers), &http::Method::PUT, None)
                .unwrap_or_else(|e| panic!("{headers:?} must be signable: {e:?}"));
        }
        for headers in [
            vec![("x-amz-server-side-encryption", "aws:kms")],
            vec![
                ("x-amz-server-side-encryption", "aws:kms"),
                ("x-amz-server-side-encryption-aws-kms-key-id", KMS_KEY),
            ],
            vec![("x-amz-server-side-encryption-context", "e30=")],
        ] {
            assert_eq!(
                check(&owned(&headers), &http::Method::PUT, None),
                Err((403, "EncryptionNotSignable".to_string())),
                "Test case: {headers:?}"
            );
        }
    }

    /// A key the client holds may read objects, but not write them.
    #[test]
    fn test_customer_keys_only_read() {
        let headers = owned(&[
            ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
            ("x-amz-server-side-encryption-customer-key", "a2V5"),
            ("x-amz-server-side-encryption-customer-key-md5", "bWQ1"),
        ]);
        for method in [http::Method::GET, http::Method::HEAD] {
            check(&headers, &method, None)
                .unwrap_or_else(|e| panic!("{method} must be signable: {e:?}"));
        }
        for method in [http::Method::PUT, http::Method::POST] {
            assert_eq!(
                check(&headers, &method, None),
                Err((403, "EncryptionNotSignable".to_string())),
                "Test case: {method}"
            );
        }
    }

    #[test]
    fn test_only_object_writes_copy() {
        let headers = owned(&[
            ("X-Amz-Copy-Source", "bucket/wh/tbl/data/a.parquet"),
            ("x-amz-copy-source-range", "bytes=0-9"),
            ("x-amz-copy-source-if-match", "etag"),
            ("x-amz-metadata-directive", "COPY"),
        ]);
        check(&headers, &http::Method::PUT, None).unwrap();
        for method in [
            http::Method::GET,
            http::Method::HEAD,
            http::Method::POST,
            http::Method::DELETE,
        ] {
            assert_eq!(
                check(&headers, &method, None),
                Err((403, "CopyNotSignable".to_string())),
                "Test case: {method}"
            );
        }
    }
}
