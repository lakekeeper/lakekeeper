//! Whether a STACKIT bucket keeps out the project's other credentials groups.
//!
//! Every access key in a STACKIT project reaches every bucket in the project
//! unless the bucket policy denies it. The check reads the policy with the
//! warehouse's own access key and passes when a `Deny` statement names the
//! groups it spares in `NotPrincipal`. Whether Lakekeeper's group is among them
//! stays unchecked: the S3 API does not reveal which group an access key
//! belongs to.

use std::time::Instant;

use iceberg_ext::catalog::rest::ErrorModel;
use lakekeeper_io::ErrorKind;

use super::{
    StorageCredential, StorageProfile,
    stackit::StackitProfile,
    validation::{ProbeDeadlines, ValidationCheck, ValidationCheckName, elapsed_ms},
};
use crate::service::storage::ValidationError;

const FIX_HINT: &str = "Set a bucket policy with a `Deny` statement whose `NotPrincipal` lists \
    Lakekeeper's credentials group and an admin group. See the storage documentation, section \
    \"Restricting bucket access\".";

/// What reading the bucket policy found.
#[derive(Debug)]
enum PolicyRead {
    Found(String),
    Missing,
    /// The warehouse's access key may not read the policy.
    Denied,
}

/// The `bucket-policy-restricts-access` check.
pub(crate) async fn bucket_policy_check(
    profile: &StorageProfile,
    credential: Option<&StorageCredential>,
    deadlines: ProbeDeadlines,
) -> ValidationCheck {
    let name = ValidationCheckName::BucketPolicyRestrictsAccess;
    let StorageProfile::Stackit(stackit) = profile else {
        return ValidationCheck::skipped(
            name,
            "Only checked for STACKIT, where every credentials group of a project reaches every \
             bucket in it unless the bucket policy denies it.",
        );
    };
    let started = Instant::now();
    let policy = deadlines.probe(read_policy(stackit, credential)).await;
    verdict(&stackit.bucket, policy, elapsed_ms(started))
}

async fn read_policy(
    profile: &StackitProfile,
    credential: Option<&StorageCredential>,
) -> Result<PolicyRead, ErrorModel> {
    let credential = credential
        .map(StorageCredential::try_to_stackit)
        .transpose()
        .map_err(|e| ValidationError::from(Box::new(super::CredentialsError::from(e))))?;
    let storage = profile
        .lakekeeper_io(credential)
        .await
        .map_err(|e| ValidationError::from(Box::new(e)))?;
    match storage.bucket_policy(&profile.bucket).await {
        Ok(Some(policy)) => Ok(PolicyRead::Found(policy)),
        Ok(None) => Ok(PolicyRead::Missing),
        Err(e) if e.kind() == ErrorKind::PermissionDenied => Ok(PolicyRead::Denied),
        Err(e) => Err(ValidationError::from(Box::new(e)).into()),
    }
}

/// The check for `bucket` from what reading its policy returned.
fn verdict(
    bucket: &str,
    policy: Result<PolicyRead, ErrorModel>,
    duration_ms: u64,
) -> ValidationCheck {
    let name = ValidationCheckName::BucketPolicyRestrictsAccess;
    let finding = match policy {
        Ok(PolicyRead::Found(policy)) if restricts_access(&policy) => {
            return ValidationCheck::passed(name, duration_ms);
        }
        Ok(PolicyRead::Found(_)) => ErrorModel::precondition_failed(
            format!(
                "The bucket policy of `{bucket}` has no `Deny` statement with `NotPrincipal`, so \
                 other credentials groups of the STACKIT project may reach the bucket. {FIX_HINT}"
            ),
            "BucketPolicyUnrestricted",
            None,
        ),
        Ok(PolicyRead::Missing) => ErrorModel::precondition_failed(
            format!(
                "Bucket `{bucket}` has no bucket policy, so every credentials group of the \
                 STACKIT project reaches it. {FIX_HINT}"
            ),
            "BucketPolicyMissing",
            None,
        ),
        Ok(PolicyRead::Denied) => ErrorModel::precondition_failed(
            format!(
                "The storage credential may not read the bucket policy of `{bucket}` \
                 (`s3:GetBucketPolicy` is denied), so it is unknown whether other credentials \
                 groups of the STACKIT project reach the bucket. Allow `s3:GetBucketPolicy` for \
                 Lakekeeper's credentials group to have the policy checked."
            ),
            "BucketPolicyReadDenied",
            None,
        ),
        Err(e) => ErrorModel::precondition_failed(
            format!(
                "Could not read the bucket policy of `{bucket}`, so it is unknown whether other \
                 credentials groups of the STACKIT project reach the bucket: {}",
                e.message
            ),
            "BucketPolicyUnreadable",
            None,
        ),
    };
    ValidationCheck::warning(name, duration_ms, finding)
}

/// Whether `policy` has a `Deny` statement sparing only the groups in its
/// `NotPrincipal`.
fn restricts_access(policy: &str) -> bool {
    let Ok(policy) = serde_json::from_str::<serde_json::Value>(policy) else {
        return false;
    };
    let statements = match &policy["Statement"] {
        serde_json::Value::Array(statements) => statements.iter().collect(),
        statement @ serde_json::Value::Object(_) => vec![statement],
        _ => vec![],
    };
    statements.into_iter().any(|statement| {
        statement["Effect"]
            .as_str()
            .is_some_and(|effect| effect.eq_ignore_ascii_case("deny"))
            && statement.get("NotPrincipal").is_some()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::storage::{MemoryProfile, validation::ValidationCheckStatus};

    const DENY_OTHERS: &str = r#"{"Statement":[{"Effect":"Deny","NotPrincipal":{"SGWS":["urn:sgws:identity::12345678901234567890:group/credentials-group-a1b2c3"]},"Action":"s3:*","Resource":["urn:sgws:s3:::my-bucket","urn:sgws:s3:::my-bucket/*"]}]}"#;

    #[test]
    fn a_deny_with_not_principal_restricts_access() {
        assert!(restricts_access(DENY_OTHERS));
    }

    #[test]
    fn a_single_statement_object_is_accepted() {
        assert!(restricts_access(
            r#"{"Statement":{"Effect":"Deny","NotPrincipal":{"SGWS":"urn:sgws:identity::12345678901234567890:group/credentials-group-a1b2c3"},"Action":"s3:*","Resource":"urn:sgws:s3:::my-bucket/*"}}"#
        ));
    }

    #[test]
    fn allow_statements_do_not_restrict_access() {
        assert!(!restricts_access(
            r#"{"Statement":[{"Effect":"Allow","Principal":{"SGWS":"urn:sgws:identity::12345678901234567890:group/credentials-group-a1b2c3"},"Action":"s3:*","Resource":"urn:sgws:s3:::my-bucket/*"}]}"#
        ));
    }

    #[test]
    fn a_deny_naming_principals_does_not_restrict_other_groups() {
        assert!(!restricts_access(
            r#"{"Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:DeleteObject","Resource":"urn:sgws:s3:::my-bucket/*"}]}"#
        ));
    }

    #[test]
    fn an_unparsable_policy_does_not_restrict_access() {
        assert!(!restricts_access("not json"));
    }

    #[test]
    fn a_restricting_policy_passes() {
        let check = verdict(
            "my-bucket",
            Ok(PolicyRead::Found(DENY_OTHERS.to_string())),
            5,
        );
        assert_eq!(check.status, ValidationCheckStatus::Passed);
    }

    #[test]
    fn a_missing_policy_is_a_warning() {
        let check = verdict("my-bucket", Ok(PolicyRead::Missing), 5);
        assert_eq!(check.status, ValidationCheckStatus::Warning);
        let error = check.error.unwrap();
        assert_eq!(error.r#type, "BucketPolicyMissing");
        assert!(error.message.contains("`my-bucket`"), "{}", error.message);
    }

    #[test]
    fn an_unrestricted_policy_is_a_warning() {
        let check = verdict(
            "my-bucket",
            Ok(PolicyRead::Found(r#"{"Statement":[]}"#.to_string())),
            5,
        );
        assert_eq!(check.status, ValidationCheckStatus::Warning);
        assert_eq!(check.error.unwrap().r#type, "BucketPolicyUnrestricted");
    }

    #[test]
    fn a_denied_read_is_a_warning_asking_for_read_access() {
        let check = verdict("my-bucket", Ok(PolicyRead::Denied), 5);
        assert_eq!(check.status, ValidationCheckStatus::Warning);
        let error = check.error.unwrap();
        assert_eq!(error.r#type, "BucketPolicyReadDenied");
        assert!(
            error.message.contains("Allow `s3:GetBucketPolicy`"),
            "{}",
            error.message
        );
    }

    #[test]
    fn an_unreadable_policy_is_a_warning_naming_the_cause() {
        let unreachable =
            ErrorModel::precondition_failed("probe time limit", "StorageProbeTimeout", None);
        let check = verdict("my-bucket", Err(unreachable), 5);
        assert_eq!(check.status, ValidationCheckStatus::Warning);
        let error = check.error.unwrap();
        assert_eq!(error.r#type, "BucketPolicyUnreadable");
        assert!(
            error.message.ends_with("probe time limit"),
            "{}",
            error.message
        );
    }

    #[tokio::test]
    async fn other_storage_is_skipped() {
        let check = bucket_policy_check(
            &StorageProfile::Memory(MemoryProfile::default()),
            None,
            ProbeDeadlines::from_request_limit(
                tokio::time::Instant::now(),
                std::time::Duration::from_secs(30),
            ),
        )
        .await;
        assert_eq!(check.status, ValidationCheckStatus::Skipped);
    }
}
