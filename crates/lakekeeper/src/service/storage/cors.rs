//! Whether storage lets the Lakekeeper UI reach it from the browser.
//!
//! LoQE runs DuckDB-WASM in the browser and talks to object storage directly with
//! vended credentials, so the bucket's CORS policy must allow the Lakekeeper
//! origin. The check sends the preflight a browser would send and grades the
//! answer. A preflight cannot show which response headers are exposed, so the
//! `ETag` / `Content-Range` requirement is stated in the warning instead.

use std::{
    sync::LazyLock,
    time::{Duration, Instant},
};

use iceberg_ext::catalog::rest::ErrorModel;
use url::Url;

use super::{
    S3Profile, StorageProfile,
    validation::{ProbeDeadlines, ValidationCheck, ValidationCheckName, elapsed_ms},
};

pub(crate) const PROBED_METHODS: [&str; 5] = ["GET", "HEAD", "PUT", "POST", "DELETE"];

/// `scheme://host[:port]` of `base_url`, as a browser sends it in `Origin`.
pub(crate) fn origin_of(base_url: &str) -> Option<String> {
    let url = Url::parse(base_url).ok()?;
    let origin = url.origin();
    origin.is_tuple().then(|| origin.ascii_serialization())
}

/// The CORS-relevant parts of a preflight response.
#[derive(Debug, Clone)]
pub(crate) struct PreflightResponse {
    pub(crate) status: u16,
    pub(crate) allow_origin: Option<String>,
    pub(crate) allow_methods: Option<String>,
    pub(crate) allow_headers: Option<String>,
}

fn tokens(list: Option<&str>) -> Vec<String> {
    list.unwrap_or_default()
        .split(',')
        .map(|t| t.trim().to_ascii_lowercase())
        .filter(|t| !t.is_empty())
        .collect()
}

/// What a preflight answered where a browser needs something else.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Mismatch {
    /// The response header, or `status` / `preflight` for the response itself.
    pub(crate) header: &'static str,
    pub(crate) expected: String,
    pub(crate) found: String,
}

fn found(value: Option<&str>) -> String {
    value.map_or_else(|| "no header".to_string(), |v| format!("`{}`", v.trim()))
}

/// Whether a browser would proceed after this preflight.
///
/// # Errors
/// The first response field that would make a browser refuse the request.
pub(crate) fn preflight_allows(
    response: &PreflightResponse,
    origin: &str,
    method: &str,
    request_headers: &[&str],
) -> Result<(), Mismatch> {
    if !(200..300).contains(&response.status) {
        return Err(Mismatch {
            header: "status",
            expected: "2xx".to_string(),
            found: response.status.to_string(),
        });
    }
    let allowed_origin = response.allow_origin.as_deref().map(str::trim);
    if !allowed_origin.is_some_and(|o| o == "*" || o == origin) {
        return Err(Mismatch {
            header: "Access-Control-Allow-Origin",
            expected: format!("`{origin}` or `*`"),
            found: found(allowed_origin),
        });
    }
    let methods = tokens(response.allow_methods.as_deref());
    // Browsers accept CORS-safelisted methods whatever the preflight lists.
    let safelisted = ["GET", "HEAD", "POST"]
        .iter()
        .any(|m| m.eq_ignore_ascii_case(method));
    if !safelisted
        && !methods
            .iter()
            .any(|m| m == "*" || m.eq_ignore_ascii_case(method))
    {
        return Err(Mismatch {
            header: "Access-Control-Allow-Methods",
            expected: format!("`{method}` or `*`"),
            found: found(response.allow_methods.as_deref()),
        });
    }
    let headers = tokens(response.allow_headers.as_deref());
    let wildcard = headers.iter().any(|h| h == "*");
    let listed = |h: &str| headers.iter().any(|a| a.eq_ignore_ascii_case(h));
    // Browsers never let `*` stand in for `Authorization`.
    let covered = |h: &str| listed(h) || (wildcard && !h.eq_ignore_ascii_case("authorization"));
    let missing: Vec<&str> = request_headers
        .iter()
        .copied()
        .filter(|h| !covered(h))
        .collect();
    if !missing.is_empty() {
        let expected = if wildcard {
            format!(
                "`{}` listed explicitly (`*` does not cover it)",
                missing.join("`, `")
            )
        } else {
            format!("`{}` or `*`", missing.join("`, `"))
        };
        return Err(Mismatch {
            header: "Access-Control-Allow-Headers",
            expected,
            found: found(response.allow_headers.as_deref()),
        });
    }
    Ok(())
}

/// One line per distinct mismatch, naming every method it applies to, in
/// first-seen order.
pub(crate) fn finding_lines(findings: &[(&str, Mismatch)]) -> Vec<String> {
    let mut grouped: Vec<(Vec<&str>, &Mismatch)> = Vec::new();
    for (method, mismatch) in findings {
        match grouped.iter_mut().find(|(_, m)| *m == mismatch) {
            Some((methods, _)) => methods.push(method),
            None => grouped.push((vec![method], mismatch)),
        }
    }
    grouped
        .into_iter()
        .map(|(methods, m)| {
            format!(
                "`{}`: {} expected {}, found {}",
                methods.join("`, `"),
                m.header,
                m.expected,
                m.found
            )
        })
        .collect()
}

const PROBE_KEY: &str = ".lakekeeper-cors-probe";

pub(crate) const S3_REQUEST_HEADERS: &[&str] = &[
    "authorization",
    "content-type",
    "range",
    "x-amz-content-sha256",
    "x-amz-date",
    "x-amz-security-token",
];
pub(crate) const GCS_REQUEST_HEADERS: &[&str] = &["authorization", "content-type", "range"];

/// Where to send the preflight, or why there is nothing to probe.
#[derive(Debug)]
pub(crate) enum ProbeTarget {
    Http {
        url: Url,
        request_headers: &'static [&'static str],
    },
    Unsupported(&'static str),
}

/// The URL a browser would use for an object below the warehouse location.
///
/// # Errors
/// The profile's base location or endpoint cannot be turned into a URL.
pub(crate) fn probe_target(profile: &StorageProfile) -> Result<ProbeTarget, String> {
    let base = profile.base_location().map_err(|e| e.to_string())?;
    let key: Vec<&str> = base
        .path_segments()
        .into_iter()
        .filter(|s| !s.is_empty())
        .chain([PROBE_KEY])
        .collect();
    match profile {
        StorageProfile::S3(s3) => s3_target(s3, &key),
        StorageProfile::Stackit(stackit) => {
            s3_target(&stackit.to_s3().map_err(|e| e.to_string())?, &key)
        }
        StorageProfile::Gcs(gcs) => {
            let mut url =
                Url::parse("https://storage.googleapis.com").map_err(|e| e.to_string())?;
            push_path(
                &mut url,
                std::iter::once(gcs.bucket.as_str()).chain(key.iter().copied()),
            );
            Ok(ProbeTarget::Http {
                url,
                request_headers: GCS_REQUEST_HEADERS,
            })
        }
        StorageProfile::Adls(_) | StorageProfile::OneLake(_) => Ok(ProbeTarget::Unsupported(
            "LoQE does not support ADLS storage.",
        )),
        #[cfg(feature = "test-utils")]
        StorageProfile::Memory(_) => Ok(ProbeTarget::Unsupported(
            "In-memory storage has no HTTP endpoint.",
        )),
    }
}

fn s3_target(s3: &S3Profile, key: &[&str]) -> Result<ProbeTarget, String> {
    let endpoint = match &s3.endpoint {
        Some(endpoint) => endpoint.clone(),
        None => Url::parse(&format!(
            "https://s3.{}.{}",
            s3.region,
            aws_dns_suffix(&s3.region)
        ))
        .map_err(|e| e.to_string())?,
    };
    let mut url = endpoint.clone();
    if s3.path_style_access == Some(true) {
        push_path(
            &mut url,
            std::iter::once(s3.bucket.as_str()).chain(key.iter().copied()),
        );
    } else {
        let host = endpoint.host_str().ok_or("S3 endpoint has no host")?;
        url.set_host(Some(&format!("{}.{host}", s3.bucket)))
            .map_err(|e| e.to_string())?;
        push_path(&mut url, key.iter().copied());
    }
    Ok(ProbeTarget::Http {
        url,
        request_headers: S3_REQUEST_HEADERS,
    })
}

/// DNS suffix of the AWS partition that `region` belongs to.
fn aws_dns_suffix(region: &str) -> &'static str {
    if region.starts_with("cn-") {
        "amazonaws.com.cn"
    } else if region.starts_with("us-isob-") {
        "sc2s.sgov.gov"
    } else if region.starts_with("us-iso-") {
        "c2s.ic.gov"
    } else {
        "amazonaws.com"
    }
}

fn push_path<'a>(url: &mut Url, segments: impl IntoIterator<Item = &'a str>) {
    if let Ok(mut path) = url.path_segments_mut() {
        path.pop_if_empty().extend(segments);
    }
}

static HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        // Only fails if the TLS backend or system DNS config can't be
        // initialized — `reqwest::Client::new()` panics on the same condition.
        .expect("Failed to build CORS probe HTTP client")
});

const FIX_HINT: &str = "Browser-based access from the Lakekeeper UI (LoQE) needs a CORS \
    policy allowing GET, HEAD, PUT, POST and DELETE from this origin, all request headers, \
    and exposing ETag and Content-Range. See the storage documentation, section \
    \"CORS Configuration\".";

/// The `cors-origin-allowed` check for `profile`, probed with the origin of `base_url`.
pub(crate) async fn cors_check(
    profile: &StorageProfile,
    base_url: &str,
    deadlines: ProbeDeadlines,
) -> ValidationCheck {
    let name = ValidationCheckName::CorsOriginAllowed;
    match probe_target(profile) {
        Ok(ProbeTarget::Http {
            url,
            request_headers,
        }) => cors_check_at(&url, request_headers, base_url, deadlines).await,
        Ok(ProbeTarget::Unsupported(reason)) => ValidationCheck::skipped(name, reason),
        Err(e) => ValidationCheck::skipped(name, format!("No URL to probe: {e}")),
    }
}

/// The `cors-origin-allowed` check against `url`.
async fn cors_check_at(
    url: &Url,
    request_headers: &[&str],
    base_url: &str,
    deadlines: ProbeDeadlines,
) -> ValidationCheck {
    let name = ValidationCheckName::CorsOriginAllowed;
    let Some(origin) = origin_of(base_url) else {
        return ValidationCheck::skipped(
            name,
            format!("`{base_url}` has no origin to probe with."),
        );
    };
    let started = Instant::now();
    let result = deadlines
        .probe(async {
            probe_url(url, &origin, request_headers)
                .await
                .map_err(|findings| {
                    let methods: Vec<&str> = findings.iter().map(|(m, _)| *m).collect();
                    let mut error = ErrorModel::precondition_failed(
                        format!(
                            "CORS does not allow origin `{origin}` for `{}`. {FIX_HINT}",
                            methods.join("`, `")
                        ),
                        "CorsOriginNotAllowed",
                        None,
                    );
                    error.stack = finding_lines(&findings);
                    error
                })
        })
        .await;
    match result {
        Ok(()) => ValidationCheck::passed(name, elapsed_ms(started)),
        Err(error) => ValidationCheck::warning(name, elapsed_ms(started), error),
    }
}

/// Preflight every probed method at `url`.
///
/// # Errors
/// Every method a browser would refuse, with what its preflight answered.
async fn probe_url(
    url: &Url,
    origin: &str,
    request_headers: &[&str],
) -> Result<(), Vec<(&'static str, Mismatch)>> {
    let requested = request_headers.join(", ");
    let results = futures::future::join_all(PROBED_METHODS.iter().map(|method| {
        let requested = &requested;
        async move {
            preflight(url, origin, method, requested, request_headers)
                .await
                .map_err(|mismatch| (*method, mismatch))
        }
    }))
    .await;
    let findings: Vec<_> = results.into_iter().filter_map(Result::err).collect();
    if findings.is_empty() {
        Ok(())
    } else {
        Err(findings)
    }
}

/// One preflight for `method`, graded as a browser would.
async fn preflight(
    url: &Url,
    origin: &str,
    method: &str,
    requested: &str,
    request_headers: &[&str],
) -> Result<(), Mismatch> {
    let response = HTTP_CLIENT
        .request(reqwest::Method::OPTIONS, url.clone())
        .header("Origin", origin)
        .header("Access-Control-Request-Method", method)
        .header("Access-Control-Request-Headers", requested)
        .send()
        .await
        .map_err(|e| Mismatch {
            header: "preflight",
            expected: format!("a response from `{}`", url.origin().ascii_serialization()),
            found: e.to_string(),
        })?;
    let header = |n: &str| {
        response
            .headers()
            .get(n)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    };
    let answer = PreflightResponse {
        status: response.status().as_u16(),
        allow_origin: header("access-control-allow-origin"),
        allow_methods: header("access-control-allow-methods"),
        allow_headers: header("access-control-allow-headers"),
    };
    preflight_allows(&answer, origin, method, request_headers)
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEADERS: [&str; 2] = ["authorization", "x-amz-date"];

    use axum::{Router, http::HeaderMap, response::IntoResponse, routing::options};

    use crate::service::storage::{
        MemoryProfile,
        validation::{ProbeDeadlines, ValidationCheckStatus},
    };

    async fn serve(router: Router) -> Url {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        Url::parse(&format!("http://{addr}/my-wh/.lakekeeper-cors-probe")).unwrap()
    }

    async fn echo_cors(headers: HeaderMap) -> impl IntoResponse {
        let get = |n: &str| {
            headers
                .get(n)
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_string()
        };
        [
            ("access-control-allow-origin", get("origin")),
            (
                "access-control-allow-methods",
                get("access-control-request-method"),
            ),
            (
                "access-control-allow-headers",
                get("access-control-request-headers"),
            ),
        ]
    }

    #[tokio::test]
    async fn a_permissive_server_passes_every_method() {
        let url = serve(Router::new().route("/{*path}", options(echo_cors))).await;
        probe_url(&url, "http://localhost:8181", S3_REQUEST_HEADERS)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_server_without_cors_names_every_method() {
        let url = serve(Router::new().route("/{*path}", options(|| async { "" }))).await;
        let findings = probe_url(&url, "http://localhost:8181", S3_REQUEST_HEADERS)
            .await
            .unwrap_err();
        let methods: Vec<&str> = findings.iter().map(|(m, _)| *m).collect();
        assert_eq!(methods, PROBED_METHODS);
        assert!(
            findings
                .iter()
                .all(|(_, m)| m.header == "Access-Control-Allow-Origin" && m.found == "no header")
        );
    }

    #[tokio::test]
    async fn a_warning_summarizes_and_details_the_findings() {
        let url = serve(Router::new().route("/{*path}", options(|| async { "" }))).await;
        let check = cors_check_at(
            &url,
            S3_REQUEST_HEADERS,
            "http://localhost:8181",
            ProbeDeadlines::from_request_limit(
                tokio::time::Instant::now(),
                std::time::Duration::from_secs(30),
            ),
        )
        .await;
        assert_eq!(check.status, ValidationCheckStatus::Warning);
        let error = check.error.unwrap();
        assert!(
            error.message.starts_with(
                "CORS does not allow origin `http://localhost:8181` for `GET`, `HEAD`, `PUT`, \
                 `POST`, `DELETE`."
            ),
            "{}",
            error.message
        );
        assert_eq!(error.stack.len(), 1, "{:?}", error.stack);
        assert!(
            error.stack[0].contains("expected `http://localhost:8181` or `*`, found no header"),
            "{:?}",
            error.stack
        );
    }

    #[tokio::test]
    async fn only_the_report_sends_preflights() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };

        let preflights = Arc::new(AtomicUsize::new(0));
        let counter = preflights.clone();
        let url = serve(Router::new().route(
            "/{*path}",
            options(move || {
                counter.fetch_add(1, Ordering::SeqCst);
                async { "" }
            }),
        ))
        .await;
        let endpoint = format!("http://{}", url.authority());
        let mut profile = S3Profile::builder()
            .bucket("my-wh".to_string())
            .region("local".to_string())
            .flavor(S3Flavor::S3Compat)
            .sts_enabled(false)
            .build();
        profile.endpoint = Some(endpoint.parse().unwrap());
        profile.path_style_access = Some(true);
        let profile = StorageProfile::S3(profile);
        let request = crate::request_metadata::RequestMetadata::new_unauthenticated();

        // Create and update report failures only, so they skip a check that can only warn.
        let _ = profile.validate_access(None, None, &request).await;
        assert_eq!(preflights.load(Ordering::SeqCst), 0);

        let report = profile.validate_access_report(None, None, &request).await;
        assert_eq!(preflights.load(Ordering::SeqCst), PROBED_METHODS.len());
        assert!(
            report
                .checks
                .iter()
                .any(|c| c.name == ValidationCheckName::CorsOriginAllowed)
        );
    }

    #[tokio::test]
    async fn an_unsupported_backend_is_skipped() {
        let deadlines = ProbeDeadlines::from_request_limit(
            tokio::time::Instant::now(),
            std::time::Duration::from_secs(30),
        );
        let check = cors_check(
            &StorageProfile::Memory(MemoryProfile::default()),
            "http://localhost:8181",
            deadlines,
        )
        .await;
        assert_eq!(check.status, ValidationCheckStatus::Skipped);
        assert!(check.reason.is_some());
    }

    use crate::service::storage::{
        GcsProfile, S3Flavor, S3Profile, StackitProfile, StorageProfile,
    };

    fn url_of(profile: &StorageProfile) -> String {
        match probe_target(profile).unwrap() {
            ProbeTarget::Http { url, .. } => url.to_string(),
            ProbeTarget::Unsupported(r) => panic!("unsupported: {r}"),
        }
    }

    fn s3(endpoint: Option<&str>, path_style: Option<bool>, region: &str) -> StorageProfile {
        let mut p = S3Profile::builder()
            .bucket("my-wh".to_string())
            .key_prefix("pre/fix".to_string())
            .region(region.to_string())
            .flavor(if endpoint.is_some() {
                S3Flavor::S3Compat
            } else {
                S3Flavor::Aws
            })
            .sts_enabled(false)
            .build();
        p.endpoint = endpoint.map(|e| e.parse().unwrap());
        p.path_style_access = path_style;
        StorageProfile::S3(p)
    }

    #[test]
    fn aws_uses_the_regional_virtual_host() {
        assert_eq!(
            url_of(&s3(None, None, "eu-central-1")),
            "https://my-wh.s3.eu-central-1.amazonaws.com/pre/fix/.lakekeeper-cors-probe"
        );
        assert_eq!(
            url_of(&s3(None, None, "cn-north-1")),
            "https://my-wh.s3.cn-north-1.amazonaws.com.cn/pre/fix/.lakekeeper-cors-probe"
        );
    }

    #[test]
    fn path_style_puts_the_bucket_in_the_path() {
        assert_eq!(
            url_of(&s3(Some("http://localhost:9000"), Some(true), "local")),
            "http://localhost:9000/my-wh/pre/fix/.lakekeeper-cors-probe"
        );
    }

    #[test]
    fn a_custom_endpoint_gets_a_virtual_host() {
        assert_eq!(
            url_of(&s3(Some("https://s3.example.com"), Some(false), "x")),
            "https://my-wh.s3.example.com/pre/fix/.lakekeeper-cors-probe"
        );
    }

    #[test]
    fn stackit_goes_through_its_s3_profile() {
        let p = StackitProfile::builder()
            .bucket("my-wh".to_string())
            .region("eu01".to_string())
            .sts_enabled(false)
            .build();
        assert_eq!(
            url_of(&StorageProfile::Stackit(p)),
            "https://my-wh.object.storage.eu01.onstackit.cloud/.lakekeeper-cors-probe"
        );
    }

    #[test]
    fn gcs_uses_the_storage_api_host() {
        let p = GcsProfile {
            bucket: "my-wh".to_string(),
            key_prefix: Some("pre".to_string()),
            sts_enabled: false,
            storage_layout: None,
        };
        let ProbeTarget::Http {
            url,
            request_headers,
        } = probe_target(&StorageProfile::Gcs(p)).unwrap()
        else {
            panic!("expected an HTTP target")
        };
        assert_eq!(
            url.as_str(),
            "https://storage.googleapis.com/my-wh/pre/.lakekeeper-cors-probe"
        );
        assert_eq!(request_headers, GCS_REQUEST_HEADERS);
    }

    fn ok(origin: &str, methods: &str, headers: &str) -> PreflightResponse {
        PreflightResponse {
            status: 200,
            allow_origin: Some(origin.to_string()),
            allow_methods: Some(methods.to_string()),
            allow_headers: Some(headers.to_string()),
        }
    }

    #[test]
    fn the_origin_drops_path_and_default_port() {
        assert_eq!(
            origin_of("https://lk.example.com/lakekeeper").as_deref(),
            Some("https://lk.example.com")
        );
        assert_eq!(
            origin_of("https://lk.example.com:443").as_deref(),
            Some("https://lk.example.com")
        );
        assert_eq!(
            origin_of("http://localhost:8181").as_deref(),
            Some("http://localhost:8181")
        );
        assert_eq!(origin_of("not a url"), None);
    }

    #[test]
    fn an_exact_origin_with_listed_method_and_headers_is_allowed() {
        let r = ok(
            "https://lk.example.com",
            "GET, PUT",
            "authorization, x-amz-date",
        );
        assert_eq!(
            preflight_allows(&r, "https://lk.example.com", "PUT", &HEADERS),
            Ok(())
        );
    }

    #[test]
    fn a_header_wildcard_does_not_cover_authorization() {
        // Browsers never let `*` stand in for `Authorization`.
        let r = ok("*", "*", "*");
        let m = preflight_allows(&r, "https://lk.example.com", "GET", &HEADERS).unwrap_err();
        assert_eq!(m.header, "Access-Control-Allow-Headers");
        assert_eq!(
            m.expected,
            "`authorization` listed explicitly (`*` does not cover it)"
        );
        assert_eq!(m.found, "`*`");
    }

    #[test]
    fn wildcards_are_allowed() {
        let r = ok("*", "*", "*, Authorization");
        assert_eq!(
            preflight_allows(&r, "https://lk.example.com", "DELETE", &HEADERS),
            Ok(())
        );
    }

    #[test]
    fn header_and_method_matching_ignores_case_and_spaces() {
        let r = ok(
            "https://lk.example.com",
            "get,Put",
            "Authorization,X-Amz-Date",
        );
        assert_eq!(
            preflight_allows(&r, "https://lk.example.com", "PUT", &HEADERS),
            Ok(())
        );
    }

    #[test]
    fn another_origin_reports_expected_and_found() {
        let r = ok("https://other.example.com", "*", "*");
        let m = preflight_allows(&r, "https://lk.example.com", "GET", &HEADERS).unwrap_err();
        assert_eq!(m.header, "Access-Control-Allow-Origin");
        assert_eq!(m.expected, "`https://lk.example.com` or `*`");
        assert_eq!(m.found, "`https://other.example.com`");
    }

    #[test]
    fn an_origin_differing_in_case_is_not_allowed() {
        let r = ok("https://LK.example.com", "*", "*");
        let m = preflight_allows(&r, "https://lk.example.com", "PUT", &HEADERS).unwrap_err();
        assert_eq!(m.header, "Access-Control-Allow-Origin");
        assert_eq!(m.found, "`https://LK.example.com`");
    }

    #[test]
    fn a_missing_method_reports_expected_and_found() {
        let r = ok("https://lk.example.com", "GET", "authorization");
        let m = preflight_allows(&r, "https://lk.example.com", "PUT", &HEADERS).unwrap_err();
        assert_eq!(m.header, "Access-Control-Allow-Methods");
        assert_eq!(m.expected, "`PUT` or `*`");
        assert_eq!(m.found, "`GET`");
    }

    #[test]
    fn safelisted_methods_pass_without_being_listed() {
        let r = ok("https://lk.example.com", "PUT", "authorization, x-amz-date");
        for method in ["GET", "HEAD", "POST"] {
            assert_eq!(
                preflight_allows(&r, "https://lk.example.com", method, &HEADERS),
                Ok(()),
                "{method}"
            );
        }
    }

    #[test]
    fn safelisted_methods_pass_without_an_allow_methods_header() {
        let mut r = ok("https://lk.example.com", "", "authorization, x-amz-date");
        r.allow_methods = None;
        assert_eq!(
            preflight_allows(&r, "https://lk.example.com", "GET", &HEADERS),
            Ok(())
        );
        let m = preflight_allows(&r, "https://lk.example.com", "DELETE", &HEADERS).unwrap_err();
        assert_eq!(m.header, "Access-Control-Allow-Methods");
    }

    #[test]
    fn missing_headers_are_listed_as_expected() {
        let r = ok("https://lk.example.com", "PUT", "authorization");
        let m = preflight_allows(&r, "https://lk.example.com", "PUT", &HEADERS).unwrap_err();
        assert_eq!(m.header, "Access-Control-Allow-Headers");
        assert_eq!(m.expected, "`x-amz-date` or `*`");
        assert_eq!(m.found, "`authorization`");
    }

    #[test]
    fn a_non_success_status_is_not_allowed_even_with_cors_headers() {
        let mut r = ok("*", "*", "*");
        r.status = 403;
        let m = preflight_allows(&r, "https://lk.example.com", "GET", &HEADERS).unwrap_err();
        assert_eq!(m.header, "status");
        assert_eq!(m.found, "403");
    }

    #[test]
    fn no_cors_headers_is_found_as_no_header() {
        let r = PreflightResponse {
            status: 200,
            allow_origin: None,
            allow_methods: None,
            allow_headers: None,
        };
        let m = preflight_allows(&r, "https://lk.example.com", "GET", &HEADERS).unwrap_err();
        assert_eq!(m.found, "no header");
    }

    #[test]
    fn methods_with_the_same_finding_share_one_line() {
        let origin_mismatch = || Mismatch {
            header: "Access-Control-Allow-Origin",
            expected: "`a` or `*`".to_string(),
            found: "no header".to_string(),
        };
        let lines = finding_lines(&[
            ("GET", origin_mismatch()),
            ("PUT", origin_mismatch()),
            (
                "DELETE",
                Mismatch {
                    header: "Access-Control-Allow-Methods",
                    expected: "`DELETE` or `*`".to_string(),
                    found: "`GET, PUT`".to_string(),
                },
            ),
        ]);
        assert_eq!(
            lines,
            vec![
                "`GET`, `PUT`: Access-Control-Allow-Origin expected `a` or `*`, found no header"
                    .to_string(),
                "`DELETE`: Access-Control-Allow-Methods expected `DELETE` or `*`, found `GET, \
                 PUT`"
                    .to_string(),
            ]
        );
    }
}
