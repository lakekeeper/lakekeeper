//! Structured, exhaustive reporting for warehouse configuration validation.
//!
//! Every individual probe a warehouse configuration is subjected to is recorded
//! as a [`ValidationCheck`] — including the ones that passed and the ones that
//! were never run. Callers that only care about pass/fail collapse the report
//! with [`ValidationReport::into_result`]; callers that surface the outcome to a
//! human (the `validate` management endpoints) return the whole report.

use std::{
    future::Future,
    time::{Duration, Instant},
};

use iceberg_ext::catalog::rest::ErrorModel;
use serde::{Deserialize, Serialize};

/// A single check performed against a warehouse configuration.
///
/// Each name is a claim about the configuration that is true exactly when the
/// check's `status` is `passed`. A check that asserts a property of a subject is
/// named `<subject>-<predicate>`, the predicate an adjective or past participle;
/// a check that exercises an operation is named after the operation. A check is
/// never named after the failure it detects — a requirement that is naturally
/// negative is stated as the positive property that must hold.
///
/// These are stable wire identifiers. Adding a value is a breaking change for
/// generated clients, which reject unknown enum values, so new checks ship in a
/// release that clients must upgrade to.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    strum_macros::Display,
    strum_macros::EnumIter,
)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
pub enum ValidationCheckName {
    /// The storage profile is internally consistent: field values are valid for
    /// the backend and cross-field rules hold. Says nothing about reachability.
    ProfileWellFormed,
    /// The new profile is a permitted evolution of the warehouse's current one.
    ProfileCompatible,
    /// The warehouse name is well-formed and not already used in the project.
    /// Compared case-insensitively, matching the database's uniqueness constraint.
    WarehouseNameValid,
    /// The requested warehouse ID is not already used by a warehouse in this
    /// project. Skipped when no ID was requested.
    ///
    /// Advisory: it examines this project only and reserves nothing, so passing
    /// it does not mean the ID is free. IDs are unique instance-wide, so a
    /// collision — with a warehouse in another project, or one created between
    /// this check and the create request — surfaces only on the create itself,
    /// as `WarehouseIdAlreadyExists`.
    WarehouseIdAvailable,
    /// No other warehouse in the project occupies or overlaps this location.
    /// Overlapping locations would let one warehouse's vended credentials reach
    /// another's data.
    LocationExclusive,
    /// The warehouse's spec may be changed by this caller — an externally
    /// managed warehouse is locked to its control plane.
    SpecMutable,
    /// The Iceberg format-version policy is self-consistent: the allowed set is
    /// non-empty and the default is a member of it.
    FormatVersionPolicyConsistent,
    /// The caller is permitted to create a warehouse under the requested
    /// `managed-by` value. Creating an externally managed warehouse requires
    /// instance-admin privilege.
    ManagedByAllowed,
    /// A storage client can be constructed from the profile and credential.
    /// Local work only — it does not prove the backend is reachable.
    StorageClientInitialized,
    /// Lakekeeper itself can write, read back and delete a test file, using the
    /// warehouse's own credential.
    LakekeeperReadWrite,
    /// Temporary downscoped credentials can be issued for a table location,
    /// by whichever mechanism the backend provides.
    VendedCredentialsIssued,
    /// Vended credentials can write, read back and delete below the table location.
    VendedCredentialsReadWrite,
    /// Vended credentials are refused write access outside the table location.
    VendedCredentialsScopeEnforced,
    /// Everything written during validation was removed again.
    Cleanup,
    /// The storage's CORS policy lets the Lakekeeper origin read and write
    /// objects from a browser, as the in-browser query console (LoQE) does.
    /// Reported as `warning` when it does not: the warehouse works without it.
    CorsOriginAllowed,
}

/// The outcome of a single check.
///
/// `passed`, `failed` and `warning` are verdicts about the configuration; only
/// `failed` makes it invalid. `skipped` is not a verdict: the check did not
/// apply, or a prerequisite failed, and `reason` says which. Skipped checks never make a configuration invalid, so a report
/// can be `valid` with nothing actually verified — read the individual checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub enum ValidationCheckStatus {
    Passed,
    Failed,
    /// Not applicable to this configuration, or not attempted because a
    /// prerequisite check failed. Never counts as a failure.
    Skipped,
    /// The check found a problem that does not make the configuration invalid.
    /// `error` says what was found. Never counts as a failure.
    Warning,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ValidationCheck {
    pub name: ValidationCheckName,
    pub status: ValidationCheckStatus,
    /// Wall-clock duration of the check. Absent for skipped checks.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    /// Why the check was skipped. Only set for `skipped`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// What went wrong. Only set for `failed` and `warning`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorModel>,
}

impl ValidationCheck {
    #[must_use]
    pub fn passed(name: ValidationCheckName, duration_ms: u64) -> Self {
        Self {
            name,
            status: ValidationCheckStatus::Passed,
            duration_ms: Some(duration_ms),
            reason: None,
            error: None,
        }
    }

    #[must_use]
    pub fn failed(
        name: ValidationCheckName,
        duration_ms: u64,
        error: impl Into<ErrorModel>,
    ) -> Self {
        Self {
            name,
            status: ValidationCheckStatus::Failed,
            duration_ms: Some(duration_ms),
            reason: None,
            error: Some(error.into()),
        }
    }

    #[must_use]
    pub fn warning(
        name: ValidationCheckName,
        duration_ms: u64,
        error: impl Into<ErrorModel>,
    ) -> Self {
        Self {
            name,
            status: ValidationCheckStatus::Warning,
            duration_ms: Some(duration_ms),
            reason: None,
            error: Some(error.into()),
        }
    }

    #[must_use]
    pub fn skipped(name: ValidationCheckName, reason: impl Into<String>) -> Self {
        Self {
            name,
            status: ValidationCheckStatus::Skipped,
            duration_ms: None,
            reason: Some(reason.into()),
            error: None,
        }
    }

    #[must_use]
    pub fn is_failed(&self) -> bool {
        matches!(self.status, ValidationCheckStatus::Failed)
    }
}

/// Make an error safe to embed in a `200 OK` report body.
///
/// Errors normally reach clients through `IcebergErrorResponse::into_response`,
/// which for 5xx strips the stack, substitutes a correlation id and logs the
/// detail. A check error is serialized directly instead, so it has to do the
/// same here — otherwise internal detail ships to the caller and nothing is
/// logged for support to correlate against.
fn sanitize_embedded_error(name: ValidationCheckName, error: ErrorModel) -> ErrorModel {
    if error.code < 500 {
        return error;
    }

    let error_id = error.error_id;
    if !error.skip_log {
        tracing::error!(
            event_source = "validation_check",
            check = %name,
            error = ?error,
            "Internal error during warehouse validation"
        );
    }
    ErrorModel {
        message: error.message,
        r#type: error.r#type,
        code: error.code,
        source: None,
        stack: vec![format!("Error ID: {error_id}")],
        error_id,
        skip_log: error.skip_log,
    }
}

/// The outcome of validating a warehouse configuration.
///
/// `valid` is true exactly when no check failed — skipped and warning checks do
/// not make a configuration invalid.
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ValidationReport {
    pub valid: bool,
    pub checks: Vec<ValidationCheck>,
}

impl ValidationReport {
    #[must_use]
    pub fn new(checks: Vec<ValidationCheck>) -> Self {
        Self {
            valid: !checks.iter().any(ValidationCheck::is_failed),
            checks,
        }
    }

    /// Redact and log any 5xx check error, making the report safe to serialize
    /// into a `200 OK` body.
    ///
    /// Applied at the response boundary rather than at construction, so the
    /// mutating create/update paths keep the full error for
    /// [`Self::into_result`] and are logged once, by the normal error response
    /// path, rather than twice.
    #[must_use]
    pub fn sanitized_for_response(mut self) -> Self {
        for check in &mut self.checks {
            if let Some(error) = check.error.take() {
                check.error = Some(sanitize_embedded_error(check.name, error));
            }
        }
        self
    }

    /// Collapse the report into the first failure, discarding the detail.
    ///
    /// Used by the mutating create/update paths, which must reject with a single
    /// error. The remaining failures are appended to the error's stack so no
    /// diagnostic is lost.
    ///
    /// # Errors
    /// Fails if any check failed.
    pub fn into_result(self) -> Result<(), ErrorModel> {
        // A failed check always carries an error when built in-process, but the
        // type is public and `Deserialize`, so never let a missing one turn a
        // failed report into success.
        let mut failures = self
            .checks
            .into_iter()
            .filter(ValidationCheck::is_failed)
            .map(|c| {
                let name = c.name;
                (
                    name,
                    c.error.unwrap_or_else(|| {
                        ErrorModel::internal(
                            format!("Check `{name}` failed without a recorded error."),
                            "ValidationCheckFailed",
                            None,
                        )
                    }),
                )
            });

        let Some((name, mut first)) = failures.next() else {
            return Ok(());
        };
        first.message = format!("Storage validation failed [{name}]: {}", first.message);
        for (name, other) in failures {
            first
                .stack
                .push(format!("Also failed [{name}]: {}", other.message));
        }
        Err(first)
    }
}

/// Accumulates checks and times them.
#[derive(Debug, Default)]
pub(crate) struct ReportBuilder {
    checks: Vec<ValidationCheck>,
}

impl ReportBuilder {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Record the outcome of an already-executed check, timed from `started`.
    ///
    /// Only correct when the check ran to completion immediately before the
    /// call. For checks awaited concurrently, time them where they run and use
    /// [`Self::record_timed`] — otherwise every branch of a `join!` reports the
    /// slowest branch's duration.
    pub(crate) fn record<E: Into<ErrorModel>>(
        &mut self,
        name: ValidationCheckName,
        started: Instant,
        result: Result<(), E>,
    ) -> bool {
        self.record_timed(name, elapsed_ms(started), result)
    }

    /// Record a check whose duration was measured where it ran.
    ///
    /// Returns whether the check passed, so callers can gate dependent checks.
    pub(crate) fn record_timed<E: Into<ErrorModel>>(
        &mut self,
        name: ValidationCheckName,
        duration_ms: u64,
        result: Result<(), E>,
    ) -> bool {
        match result {
            Ok(()) => {
                self.checks.push(ValidationCheck::passed(name, duration_ms));
                true
            }
            Err(error) => {
                self.checks
                    .push(ValidationCheck::failed(name, duration_ms, error));
                false
            }
        }
    }

    pub(crate) fn skip(&mut self, name: ValidationCheckName, reason: impl Into<String>) {
        self.checks.push(ValidationCheck::skipped(name, reason));
    }

    pub(crate) fn push(&mut self, check: ValidationCheck) {
        self.checks.push(check);
    }

    pub(crate) fn extend(&mut self, checks: impl IntoIterator<Item = ValidationCheck>) {
        self.checks.extend(checks);
    }

    pub(crate) fn build(self) -> ValidationReport {
        ValidationReport::new(self.checks)
    }
}

pub(crate) fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// Every check [`StorageProfile::validate_access_report`] reports, in report order.
///
/// [`StorageProfile::validate_access_report`]: super::StorageProfile::validate_access_report
pub(crate) const STORAGE_CHECKS: [ValidationCheckName; 7] = [
    ValidationCheckName::StorageClientInitialized,
    ValidationCheckName::LakekeeperReadWrite,
    ValidationCheckName::VendedCredentialsIssued,
    ValidationCheckName::VendedCredentialsReadWrite,
    ValidationCheckName::VendedCredentialsScopeEnforced,
    ValidationCheckName::Cleanup,
    ValidationCheckName::CorsOriginAllowed,
];

/// Reason recorded when the server has storage validation switched off.
pub(crate) const SKIPPED_BY_CONFIG: &str =
    "Storage validation is disabled on this server (LAKEKEEPER__SKIP_STORAGE_VALIDATION).";

/// Reason recorded when a check could not run because an earlier one failed.
pub(crate) const SKIPPED_PREREQUISITE: &str = "Not attempted: a prerequisite check failed.";

/// Time limits for the storage probes of one validation, counted from the
/// request's arrival.
///
/// Probes get two thirds of `LAKEKEEPER__MAX_REQUEST_TIME` and cleanup the
/// following sixth, so storage that never answers yields a report naming the
/// stalled probe while the request-timeout layer still has time to spare.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ProbeDeadlines {
    /// Every probe except cleanup must finish by then.
    pub(crate) probes: tokio::time::Instant,
    /// Cleanup must finish by then. It starts after the probes, so a probe that
    /// ran out does not also leave its files behind.
    pub(crate) cleanup: tokio::time::Instant,
    probe_budget: Duration,
    cleanup_budget: Duration,
}

impl ProbeDeadlines {
    /// Deadlines for a request that arrived at `received_at` and may take at
    /// most `request_limit`.
    pub(crate) fn from_request_limit(
        received_at: tokio::time::Instant,
        request_limit: Duration,
    ) -> Self {
        // Divided first so that an effectively unbounded limit cannot overflow.
        let probe_budget = request_limit / 3 * 2;
        let cleanup_budget = request_limit / 6;
        let probes = received_at
            .checked_add(probe_budget)
            .unwrap_or_else(far_future);
        let cleanup = probes
            .checked_add(cleanup_budget)
            .unwrap_or_else(far_future);
        Self {
            probes,
            cleanup,
            probe_budget,
            cleanup_budget,
        }
    }

    /// `probe`, failed if it has not finished by [`Self::probes`].
    pub(crate) fn probe<T, E: Into<ErrorModel>>(
        &self,
        probe: impl Future<Output = Result<T, E>>,
    ) -> impl Future<Output = Result<T, ErrorModel>> {
        // Boxed before the returned future captures it: probes are large storage
        // futures, and every caller awaits this one.
        let probe = Box::pin(probe);
        let deadline = self.probes;
        let error = exceeded_error("probe", self.probe_budget, "two thirds");
        async move {
            match tokio::time::timeout_at(deadline, probe).await {
                Ok(result) => result.map_err(Into::into),
                Err(_) => Err(error),
            }
        }
    }

    /// The `cleanup` check from `cleanup`, failed if it has not finished by
    /// [`Self::cleanup`].
    pub(crate) fn cleanup(
        &self,
        cleanup: impl Future<Output = ValidationCheck>,
    ) -> impl Future<Output = ValidationCheck> {
        let cleanup = Box::pin(cleanup);
        let deadline = self.cleanup;
        let error = exceeded_error(
            "cleanup",
            self.cleanup_budget,
            "the sixth after the probes' two thirds",
        );
        async move {
            let started = Instant::now();
            tokio::time::timeout_at(deadline, cleanup)
                .await
                .unwrap_or_else(|_| {
                    ValidationCheck::failed(
                        ValidationCheckName::Cleanup,
                        elapsed_ms(started),
                        error,
                    )
                })
        }
    }
}

/// An instant beyond any request, matching what `tokio::time::sleep` saturates to.
fn far_future() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_hours(24 * 365 * 30)
}

fn exceeded_error(stage: &str, budget: Duration, share: &str) -> ErrorModel {
    ErrorModel::precondition_failed(
        format!(
            "Storage did not respond within the validation's {stage} time limit ({}, {share} of \
             LAKEKEEPER__MAX_REQUEST_TIME counted from the request's arrival). Check that \
             Lakekeeper can reach the storage and its credential endpoints: DNS resolution, \
             firewalls and egress rules.",
            format_budget(budget)
        ),
        "StorageProbeTimeout",
        None,
    )
}

fn format_budget(budget: Duration) -> String {
    if budget.subsec_millis() == 0 && budget.as_secs() > 0 {
        format!("{}s", budget.as_secs())
    } else {
        format!("{}ms", budget.as_millis())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn err(msg: &str) -> ErrorModel {
        ErrorModel::bad_request(msg, "TestError", None)
    }

    fn deadlines(limit: Duration) -> ProbeDeadlines {
        ProbeDeadlines::from_request_limit(tokio::time::Instant::now(), limit)
    }

    #[test]
    fn a_warning_does_not_make_the_report_invalid() {
        let report = ValidationReport::new(vec![
            ValidationCheck::passed(ValidationCheckName::ProfileWellFormed, 1),
            ValidationCheck::warning(ValidationCheckName::Cleanup, 3, err("cors")),
        ]);
        assert!(report.valid);
        assert!(report.into_result().is_ok());
    }

    #[test]
    fn a_warning_carries_its_error_and_serializes_as_warning() {
        let check = ValidationCheck::warning(ValidationCheckName::Cleanup, 3, err("cors"));
        assert_eq!(check.status, ValidationCheckStatus::Warning);
        assert_eq!(check.duration_ms, Some(3));
        assert!(check.reason.is_none());
        let json = serde_json::to_value(&check).unwrap();
        assert_eq!(json["status"], "warning");
        assert_eq!(json["error"]["message"], "cors");
    }

    #[tokio::test(start_paused = true)]
    async fn probe_deadlines_split_the_request_limit() {
        let start = tokio::time::Instant::now();
        let deadlines = deadlines(Duration::from_secs(30));
        assert_eq!(deadlines.probes - start, Duration::from_secs(20));
        assert_eq!(deadlines.cleanup - start, Duration::from_secs(25));
    }

    #[tokio::test(start_paused = true)]
    async fn probe_deadlines_count_from_the_request_arrival() {
        let received_at = tokio::time::Instant::now();
        tokio::time::advance(Duration::from_secs(10)).await;
        let deadlines = ProbeDeadlines::from_request_limit(received_at, Duration::from_secs(30));
        assert_eq!(
            deadlines.probes - tokio::time::Instant::now(),
            Duration::from_secs(10)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_unbounded_request_limit_does_not_overflow() {
        let deadlines = deadlines(Duration::MAX);
        assert!(deadlines.probes > tokio::time::Instant::now() + Duration::from_hours(24));
        assert!(deadlines.cleanup >= deadlines.probes);
    }

    #[tokio::test(start_paused = true)]
    async fn a_probe_that_finishes_in_time_keeps_its_result() {
        let deadlines = deadlines(Duration::from_secs(30));
        let ok = deadlines.probe(async { Ok::<_, ErrorModel>(7) }).await;
        assert_eq!(ok.unwrap(), 7);
        let failed = deadlines
            .probe(async { Err::<(), _>(err("no write")) })
            .await;
        assert_eq!(failed.unwrap_err().r#type, "TestError");
    }

    #[tokio::test(start_paused = true)]
    async fn a_stalled_probe_fails_at_the_deadline() {
        let deadlines = deadlines(Duration::from_secs(30));
        let stalled = deadlines
            .probe(std::future::pending::<Result<(), ErrorModel>>())
            .await;
        let error = stalled.unwrap_err();
        assert_eq!(error.r#type, "StorageProbeTimeout");
        assert!(error.message.contains("probe time limit (20s"), "{error:?}");
        assert!(tokio::time::Instant::now() >= deadlines.probes);
    }

    #[tokio::test(start_paused = true)]
    async fn a_stalled_cleanup_fails_with_its_own_limit() {
        let deadlines = deadlines(Duration::from_secs(30));
        let check = deadlines
            .cleanup(std::future::pending::<ValidationCheck>())
            .await;
        assert_eq!(check.name, ValidationCheckName::Cleanup);
        assert_eq!(check.status, ValidationCheckStatus::Failed);
        let error = check.error.unwrap();
        assert!(
            error.message.contains("cleanup time limit (5s"),
            "{error:?}"
        );
        assert!(tokio::time::Instant::now() >= deadlines.cleanup);
    }

    #[tokio::test(start_paused = true)]
    async fn sub_second_limits_are_reported_in_milliseconds() {
        let deadlines = deadlines(Duration::from_millis(900));
        let error = deadlines
            .probe(std::future::pending::<Result<(), ErrorModel>>())
            .await
            .unwrap_err();
        assert!(error.message.contains("(600ms"), "{error:?}");
    }

    #[test]
    fn report_is_valid_when_only_passed_and_skipped() {
        let report = ValidationReport::new(vec![
            ValidationCheck::passed(ValidationCheckName::ProfileWellFormed, 1),
            ValidationCheck::skipped(
                ValidationCheckName::VendedCredentialsReadWrite,
                "sts disabled",
            ),
        ]);
        assert!(report.valid);
        assert!(report.into_result().is_ok());
    }

    #[test]
    fn report_is_invalid_when_any_check_failed() {
        let report = ValidationReport::new(vec![
            ValidationCheck::passed(ValidationCheckName::ProfileWellFormed, 1),
            ValidationCheck::failed(ValidationCheckName::LakekeeperReadWrite, 5, err("no write")),
        ]);
        assert!(!report.valid);
    }

    #[test]
    fn into_result_reports_first_failure_and_stacks_the_rest() {
        let report = ValidationReport::new(vec![
            ValidationCheck::failed(ValidationCheckName::LakekeeperReadWrite, 5, err("no write")),
            ValidationCheck::failed(
                ValidationCheckName::VendedCredentialsReadWrite,
                5,
                err("no sts"),
            ),
        ]);
        let error = report.into_result().expect_err("report has failures");
        assert!(error.message.contains("lakekeeper-read-write"), "{error:?}");
        assert!(error.message.contains("no write"), "{error:?}");
        assert_eq!(error.stack.len(), 1);
        assert!(
            error.stack[0].contains("vended-credentials-read-write"),
            "{error:?}"
        );
    }

    #[test]
    fn internal_errors_are_redacted_and_get_a_correlation_id() {
        let mut internal = ErrorModel::internal("connection to catalog lost", "DbError", None);
        internal.stack = vec!["sensitive internal detail".to_string()];
        let expected_id = internal.error_id;

        let report = ValidationReport::new(vec![ValidationCheck::failed(
            ValidationCheckName::WarehouseNameValid,
            1,
            internal,
        )])
        .sanitized_for_response();

        let error = report.checks[0].error.as_ref().expect("error preserved");
        assert_eq!(
            error.stack,
            vec![format!("Error ID: {expected_id}")],
            "the internal stack must never reach a 200 body"
        );
        assert_eq!(error.code, 500);
    }

    #[test]
    fn client_errors_pass_through_sanitizing_untouched() {
        let mut bad_request = ErrorModel::bad_request("bad bucket", "InvalidBucket", None);
        bad_request.stack = vec!["useful context for the caller".to_string()];

        let report = ValidationReport::new(vec![ValidationCheck::failed(
            ValidationCheckName::ProfileWellFormed,
            1,
            bad_request,
        )])
        .sanitized_for_response();

        let error = report.checks[0].error.as_ref().expect("error preserved");
        assert_eq!(
            error.stack,
            vec!["useful context for the caller".to_string()]
        );
    }

    #[test]
    fn a_failed_check_without_an_error_still_fails_into_result() {
        // Reachable only via deserialization, but must never read as success.
        let report = ValidationReport {
            valid: false,
            checks: vec![ValidationCheck {
                name: ValidationCheckName::Cleanup,
                status: ValidationCheckStatus::Failed,
                duration_ms: None,
                reason: None,
                error: None,
            }],
        };
        let error = report.into_result().expect_err("must not collapse to Ok");
        assert!(error.message.contains("cleanup"), "{error:?}");
    }

    #[test]
    fn skipped_checks_carry_a_reason_and_no_duration() {
        let check = ValidationCheck::skipped(ValidationCheckName::Cleanup, "because");
        assert_eq!(check.reason.as_deref(), Some("because"));
        assert!(check.duration_ms.is_none());
        assert!(!check.is_failed());
    }
}
