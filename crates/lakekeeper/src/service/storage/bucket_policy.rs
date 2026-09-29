//! Whether a STACKIT bucket keeps out the project's other credentials groups.
//!
//! Every access key in a STACKIT project reaches every bucket in the project
//! unless the bucket policy denies it. The check reads the policy with the
//! warehouse's own access key and passes when `Deny` statements, which spare
//! only named credentials groups in their `NotPrincipal`, deny every action on
//! the bucket and on the warehouse's objects without a `Condition`. Whether
//! Lakekeeper's group is among the spared ones is unchecked: the S3 API hides
//! which group an access key belongs to.
//!
//! Credentials Lakekeeper vends act as the assumed group, so such a policy keeps
//! them working. This holds on STACKIT, verified against its storage; AWS treats
//! assumed-role sessions differently.

use std::time::Instant;

use iceberg_ext::catalog::rest::ErrorModel;
use lakekeeper_io::ErrorKind;

use super::{
    StorageCredential, StorageProfile,
    stackit::StackitProfile,
    validation::{
        ProbeDeadlines, SKIPPED_PREREQUISITE, ValidationCheck, ValidationCheckName, elapsed_ms,
    },
};
use crate::service::storage::ValidationError;

const FIX_HINT: &str = "Set a bucket policy with a `Deny` statement whose `NotPrincipal` lists \
    Lakekeeper's credentials group and an admin group. See the STACKIT storage documentation, \
    section \"Restricting Bucket Access\".";

/// At most this many values of a policy field are quoted in a finding.
const QUOTED_VALUES: usize = 5;
/// Quoted values are cut to this many characters.
const QUOTED_LENGTH: usize = 200;

/// What reading the bucket policy found.
#[derive(Debug)]
enum PolicyRead {
    Found(String),
    Missing,
    /// The warehouse's access key may not read the policy.
    Denied,
    /// No storage client could be built, which `storage-client-initialized`
    /// reports.
    ClientUnavailable,
}

/// The `bucket-access-restricted` check.
pub(crate) async fn bucket_access_check(
    profile: &StorageProfile,
    credential: Option<&StorageCredential>,
    deadlines: ProbeDeadlines,
) -> ValidationCheck {
    let name = ValidationCheckName::BucketAccessRestricted;
    let StorageProfile::Stackit(stackit) = profile else {
        return ValidationCheck::skipped(
            name,
            "Only checked for STACKIT, where every credentials group of a project reaches every \
             bucket in it unless the bucket policy denies it.",
        );
    };
    let started = Instant::now();
    let policy = deadlines.probe(read_policy(stackit, credential)).await;
    verdict(&Scope::of(stackit), policy, elapsed_ms(started))
}

async fn read_policy(
    profile: &StackitProfile,
    credential: Option<&StorageCredential>,
) -> Result<PolicyRead, ErrorModel> {
    let Ok(credential) = credential
        .map(StorageCredential::try_to_stackit)
        .transpose()
    else {
        return Ok(PolicyRead::ClientUnavailable);
    };
    let Ok(storage) = profile.lakekeeper_io(credential).await else {
        return Ok(PolicyRead::ClientUnavailable);
    };
    match storage.bucket_policy(&profile.bucket).await {
        Ok(Some(policy)) => Ok(PolicyRead::Found(policy)),
        Ok(None) => Ok(PolicyRead::Missing),
        Err(e) if e.kind() == ErrorKind::PermissionDenied => Ok(PolicyRead::Denied),
        Err(e) => Err(ValidationError::from(Box::new(e)).into()),
    }
}

/// The storage a warehouse's bucket policy has to protect.
#[derive(Debug)]
struct Scope<'a> {
    bucket: &'a str,
    key_prefix: Option<&'a str>,
}

impl<'a> Scope<'a> {
    fn of(profile: &'a StackitProfile) -> Self {
        Self {
            bucket: &profile.bucket,
            key_prefix: profile.key_prefix.as_deref(),
        }
    }

    /// The path below which the warehouse's objects live, e.g. `bucket/prefix/`.
    fn object_path(&self) -> String {
        match self.key_prefix.map(|p| p.trim_matches('/')) {
            Some(prefix) if !prefix.is_empty() => format!("{}/{prefix}/", self.bucket),
            _ => format!("{}/", self.bucket),
        }
    }

    /// The object resources that protect the warehouse, broadest first.
    fn expected_object_resources(&self) -> String {
        let bucket_wide = format!("`urn:sgws:s3:::{}/*`", self.bucket);
        let objects = self.object_path();
        if objects == format!("{}/", self.bucket) {
            bucket_wide
        } else {
            format!("{bucket_wide} or `urn:sgws:s3:::{objects}*`")
        }
    }
}

/// The check for `scope` from what reading its policy returned.
fn verdict(
    scope: &Scope<'_>,
    policy: Result<PolicyRead, ErrorModel>,
    duration_ms: u64,
) -> ValidationCheck {
    let name = ValidationCheckName::BucketAccessRestricted;
    let bucket = scope.bucket;
    let finding = match policy {
        Ok(PolicyRead::ClientUnavailable) => {
            return ValidationCheck::skipped(name, SKIPPED_PREREQUISITE);
        }
        Ok(PolicyRead::Found(policy)) => match grade(&policy, scope) {
            Ok(()) => return ValidationCheck::passed(name, duration_ms),
            Err(finding) => finding.into_error(bucket),
        },
        Ok(PolicyRead::Missing) => ErrorModel::precondition_failed(
            format!(
                "Bucket `{bucket}` has no bucket policy, so every credentials group of the \
                 STACKIT project, including groups created later, can read and write it. \
                 {FIX_HINT}"
            ),
            "BucketPolicyMissing",
            None,
        ),
        Ok(PolicyRead::Denied) => ErrorModel::precondition_failed(
            format!(
                "Reading the bucket policy of `{bucket}` was refused (AccessDenied), so it is \
                 unknown whether other credentials groups of the STACKIT project can reach the \
                 bucket. Either a bucket policy denies Lakekeeper's credentials group entirely: \
                 check that its `NotPrincipal` lists the group of `credentials-group-urn`. Or \
                 `s3:GetBucketPolicy` is denied to that group: allow it to have the policy \
                 checked."
            ),
            "BucketPolicyReadDenied",
            None,
        ),
        Err(e) => ErrorModel::precondition_failed(
            format!(
                "Could not read the bucket policy of `{bucket}`, so it is unknown whether other \
                 credentials groups of the STACKIT project can reach the bucket: {}",
                e.message
            ),
            "BucketPolicyReadFailed",
            None,
        ),
    };
    ValidationCheck::warning(name, duration_ms, finding)
}

/// A requirement a policy leaves unmet.
#[derive(Debug, PartialEq, Eq)]
struct Gap {
    field: &'static str,
    expected: String,
    found: String,
}

/// Why a bucket policy does not restrict access.
#[derive(Debug, PartialEq, Eq)]
enum Finding {
    InvalidJson,
    NoDenyStatement,
    /// What the `Deny` statements with `NotPrincipal` leave open.
    Open(Vec<Gap>),
}

impl Finding {
    fn into_error(self, bucket: &str) -> ErrorModel {
        let gaps = match self {
            Finding::InvalidJson => {
                return unrestricted(format!(
                    "The bucket policy of `{bucket}` is not valid JSON. {FIX_HINT}"
                ));
            }
            Finding::NoDenyStatement => {
                return unrestricted(format!(
                    "The bucket policy of `{bucket}` has no `Deny` statement with `NotPrincipal`, \
                     so other credentials groups of the STACKIT project can reach the bucket. \
                     {FIX_HINT}"
                ));
            }
            Finding::Open(gaps) => gaps,
        };
        let fields: Vec<&str> = gaps.iter().map(|g| g.field).collect();
        unrestricted(format!(
            "The bucket policy of `{bucket}` lets other credentials groups of the STACKIT \
             project reach the bucket: its `Deny` statement with `NotPrincipal` falls short in {}. \
             {FIX_HINT}",
            fields.join(", ")
        ))
        .append_details(
            gaps.iter()
                .map(|g| format!("{}: expected {}, found {}", g.field, g.expected, g.found)),
        )
    }
}

fn unrestricted(message: String) -> ErrorModel {
    ErrorModel::precondition_failed(message, "BucketPolicyUnrestricted", None)
}

/// `Ok` if `Deny` statements sparing only named groups deny every action on the
/// bucket and on the warehouse's objects.
///
/// Statements that deny every action without a `Condition` count together, so
/// the bucket and its objects may be covered by separate statements. Without
/// such a statement, the gaps of the statement closest to one are reported.
fn grade(policy: &str, scope: &Scope<'_>) -> Result<(), Finding> {
    let Ok(policy) = serde_json::from_str::<serde_json::Value>(policy) else {
        return Err(Finding::InvalidJson);
    };
    let statements = match &policy["Statement"] {
        serde_json::Value::Array(statements) => statements.iter().collect(),
        statement @ serde_json::Value::Object(_) => vec![statement],
        _ => vec![],
    };
    let denies: Vec<&serde_json::Value> = statements
        .into_iter()
        .filter(|statement| {
            statement["Effect"]
                .as_str()
                .is_some_and(|effect| effect.eq_ignore_ascii_case("deny"))
                && statement.get("NotPrincipal").is_some()
        })
        .collect();
    if denies.is_empty() {
        return Err(Finding::NoDenyStatement);
    }

    let qualifying: Vec<&serde_json::Value> = denies
        .iter()
        .copied()
        .filter(|statement| scope_free_gaps(statement).is_empty())
        .collect();
    if !qualifying.is_empty() {
        let resources: Vec<&str> = qualifying
            .iter()
            .flat_map(|statement| one_or_many(&statement["Resource"]))
            .collect();
        return match resource_gap(&resources, scope) {
            None => Ok(()),
            Some(gap) => Err(Finding::Open(vec![gap])),
        };
    }

    let closest = denies
        .into_iter()
        .map(|statement| {
            let mut gaps = scope_free_gaps(statement);
            if statement.get("NotResource").is_none() {
                gaps.extend(resource_gap(&one_or_many(&statement["Resource"]), scope));
            }
            gaps
        })
        .min_by_key(Vec::len)
        .unwrap_or_default();
    Err(Finding::Open(closest))
}

/// What a `Deny` statement leaves open, apart from which resources it covers.
fn scope_free_gaps(statement: &serde_json::Value) -> Vec<Gap> {
    let mut gaps = Vec::new();
    let spared = principals(&statement["NotPrincipal"]);
    let unnamed: Vec<&str> = spared
        .iter()
        .copied()
        .filter(|p| !names_an_identity(p))
        .collect();
    if spared.is_empty() || !unnamed.is_empty() {
        gaps.push(Gap {
            field: "`NotPrincipal`",
            expected: "only credentials-group URNs such as \
                       `urn:sgws:identity::<account>:group/credentials-group-<id>`; `*`, an \
                       account or its root spares every group"
                .to_string(),
            found: quoted(if spared.is_empty() { &spared } else { &unnamed }),
        });
    }
    if let Some(not_action) = statement.get("NotAction") {
        gaps.push(Gap {
            field: "`Action`",
            expected: "`s3:*`".to_string(),
            found: format!("`NotAction` {}", quoted(&one_or_many(not_action))),
        });
    } else {
        let actions = one_or_many(&statement["Action"]);
        if !actions
            .iter()
            .any(|a| *a == "*" || a.eq_ignore_ascii_case("s3:*"))
        {
            gaps.push(Gap {
                field: "`Action`",
                expected: "`s3:*`".to_string(),
                found: quoted(&actions),
            });
        }
    }
    if let Some(not_resource) = statement.get("NotResource") {
        gaps.push(Gap {
            field: "`Resource`",
            expected: "`Resource` naming the bucket and its objects".to_string(),
            found: format!("`NotResource` {}", quoted(&one_or_many(not_resource))),
        });
    }
    if statement.get("Condition").is_some() {
        gaps.push(Gap {
            field: "`Condition`",
            expected: "none".to_string(),
            found: "a `Condition`".to_string(),
        });
    }
    gaps
}

/// What `resources` leave uncovered of the bucket and the warehouse's objects.
fn resource_gap(resources: &[&str], scope: &Scope<'_>) -> Option<Gap> {
    let mut missing = Vec::new();
    if !resources.iter().any(|r| covers(r, scope.bucket)) {
        missing.push(format!("`urn:sgws:s3:::{}`", scope.bucket));
    }
    let objects = format!("{}*", scope.object_path());
    if !resources.iter().any(|r| covers(r, &objects)) {
        missing.push(scope.expected_object_resources());
    }
    (!missing.is_empty()).then(|| Gap {
        field: "`Resource`",
        expected: missing.join(" and "),
        found: quoted(resources),
    })
}

/// Whether the policy resource `resource` matches everything `target` names.
/// A trailing `*` in `target` stands for any continuation.
fn covers(resource: &str, target: &str) -> bool {
    if resource == "*" {
        return true;
    }
    let Some(pattern) = ["urn:sgws:s3:::", "arn:aws:s3:::"]
        .iter()
        .find_map(|scheme| resource.strip_prefix(scheme))
    else {
        return false;
    };
    match pattern.strip_suffix('*') {
        // Other wildcards in the stem also match themselves literally.
        Some(stem) => target.starts_with(stem),
        None => pattern == target,
    }
}

/// Whether `principal` names one credentials group or its access keys, as
/// opposed to `*`, an account, or an account's root.
fn names_an_identity(principal: &str) -> bool {
    let Some(identity) = ["urn:sgws:identity::", "arn:aws:iam::"]
        .iter()
        .find_map(|scheme| principal.strip_prefix(scheme))
    else {
        return false;
    };
    let Some((account, path)) = identity.split_once(':') else {
        return false;
    };
    let name = path
        .strip_prefix("group/")
        .or_else(|| path.strip_prefix("user/"));
    !account.is_empty() && name.is_some_and(|name| !name.is_empty() && !name.contains(['*', '?']))
}

/// The principals a `Principal` or `NotPrincipal` value names, under any key.
fn principals(value: &serde_json::Value) -> Vec<&str> {
    match value {
        serde_json::Value::Object(by_kind) => by_kind.values().flat_map(one_or_many).collect(),
        other => one_or_many(other),
    }
}

/// A policy value that may be one string or a list of them.
fn one_or_many(value: &serde_json::Value) -> Vec<&str> {
    match value {
        serde_json::Value::String(s) => vec![s.as_str()],
        serde_json::Value::Array(values) => values.iter().filter_map(|v| v.as_str()).collect(),
        _ => vec![],
    }
}

/// `values` quoted for a finding, capped in number and length.
fn quoted(values: &[&str]) -> String {
    if values.is_empty() {
        return "none".to_string();
    }
    let shown: Vec<String> = values
        .iter()
        .take(QUOTED_VALUES)
        .map(|v| {
            if v.chars().count() > QUOTED_LENGTH {
                format!("`{}…`", v.chars().take(QUOTED_LENGTH).collect::<String>())
            } else {
                format!("`{v}`")
            }
        })
        .collect();
    let hidden = values.len().saturating_sub(QUOTED_VALUES);
    if hidden == 0 {
        shown.join(", ")
    } else {
        format!("{} and {hidden} more", shown.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::storage::{MemoryProfile, validation::ValidationCheckStatus};

    const LAKEKEEPER: &str =
        "urn:sgws:identity::12345678901234567890:group/credentials-group-a1b2c3";

    const SCOPE: Scope<'static> = Scope {
        bucket: "my-bucket",
        key_prefix: None,
    };

    /// A policy with one `Deny` statement sparing Lakekeeper's group, with `fields`.
    fn deny(fields: &str) -> String {
        format!(
            r#"{{"Statement":{{"Effect":"Deny","NotPrincipal":{{"SGWS":"{LAKEKEEPER}"}},{fields}}}}}"#
        )
    }

    const EVERYTHING: &str =
        r#""Action":"s3:*","Resource":["urn:sgws:s3:::my-bucket","urn:sgws:s3:::my-bucket/*"]"#;

    /// The recommended policy from the storage documentation.
    fn recommended() -> String {
        format!(
            r#"{{"Statement":[
                {{"Sid":"OnlyLakekeeperAndAdmin","Effect":"Deny","NotPrincipal":{{"SGWS":["{LAKEKEEPER}","urn:sgws:identity::12345678901234567890:group/credentials-group-d4e5f6"]}},{EVERYTHING}}},
                {{"Sid":"OnlyAdminChangesPolicy","Effect":"Deny","NotPrincipal":{{"SGWS":"urn:sgws:identity::12345678901234567890:group/credentials-group-d4e5f6"}},"Action":["s3:PutBucketPolicy","s3:DeleteBucketPolicy"],"Resource":"urn:sgws:s3:::my-bucket"}}
            ]}}"#
        )
    }

    fn gaps(policy: &str) -> Vec<Gap> {
        match grade(policy, &SCOPE) {
            Err(Finding::Open(gaps)) => gaps,
            other => panic!("expected gaps, got {other:?}"),
        }
    }

    fn fields(policy: &str) -> Vec<&'static str> {
        gaps(policy).iter().map(|g| g.field).collect()
    }

    #[test]
    fn the_recommended_policy_restricts_access() {
        assert_eq!(grade(&recommended(), &SCOPE), Ok(()));
    }

    #[test]
    fn a_single_statement_object_and_arn_forms_are_accepted() {
        let policy = r#"{"Statement":{"Effect":"deny","NotPrincipal":{"AWS":["arn:aws:iam::12345678901234567890:user/credentials-group-a1b2c3"]},"Action":["S3:*"],"Resource":["arn:aws:s3:::my-bucket","arn:aws:s3:::my-bucket/*"]}}"#;
        assert_eq!(grade(policy, &SCOPE), Ok(()));
    }

    #[test]
    fn wildcard_resources_cover_the_bucket_and_its_objects() {
        for resource in [
            r#""*""#,
            r#""urn:sgws:s3:::*""#,
            r#""urn:sgws:s3:::my-bucket*""#,
        ] {
            let policy = deny(&format!(r#""Action":"*","Resource":{resource}"#));
            assert_eq!(grade(&policy, &SCOPE), Ok(()), "{resource}");
        }
    }

    #[test]
    fn a_resource_without_a_scheme_covers_nothing() {
        let found = gaps(&deny(
            r#""Action":"s3:*","Resource":["my-bucket","my-bucket/*"]"#,
        ));
        assert_eq!(found[0].field, "`Resource`");
        assert_eq!(
            found[0].expected,
            "`urn:sgws:s3:::my-bucket` and `urn:sgws:s3:::my-bucket/*`"
        );
        assert_eq!(found[0].found, "`my-bucket`, `my-bucket/*`");
    }

    #[test]
    fn the_bucket_and_its_objects_may_be_covered_by_separate_statements() {
        let policy = format!(
            r#"{{"Statement":[
                {{"Effect":"Deny","NotPrincipal":{{"SGWS":"{LAKEKEEPER}"}},"Action":"s3:*","Resource":"urn:sgws:s3:::my-bucket"}},
                {{"Effect":"Deny","NotPrincipal":{{"SGWS":"{LAKEKEEPER}"}},"Action":"s3:*","Resource":"urn:sgws:s3:::my-bucket/*"}}
            ]}}"#
        );
        assert_eq!(grade(&policy, &SCOPE), Ok(()));
    }

    #[test]
    fn a_resource_for_the_key_prefix_covers_the_warehouse() {
        let policy = deny(
            r#""Action":"s3:*","Resource":["urn:sgws:s3:::my-bucket","urn:sgws:s3:::my-bucket/lakekeeper/*"]"#,
        );
        let scope = Scope {
            bucket: "my-bucket",
            key_prefix: Some("/lakekeeper/"),
        };
        assert_eq!(grade(&policy, &scope), Ok(()));

        let other = Scope {
            bucket: "my-bucket",
            key_prefix: Some("other"),
        };
        let Err(Finding::Open(found)) = grade(&policy, &other) else {
            panic!("expected gaps");
        };
        assert_eq!(
            found[0].expected,
            "`urn:sgws:s3:::my-bucket/*` or `urn:sgws:s3:::my-bucket/other/*`"
        );
    }

    #[test]
    fn wildcard_characters_in_a_key_prefix_match_themselves() {
        let policy = deny(
            r#""Action":"s3:*","Resource":["urn:sgws:s3:::my-bucket","urn:sgws:s3:::my-bucket/a*b/*"]"#,
        );
        let scope = Scope {
            bucket: "my-bucket",
            key_prefix: Some("a*b"),
        };
        assert_eq!(grade(&policy, &scope), Ok(()));
        assert_eq!(fields(&policy), vec!["`Resource`"]);
    }

    #[test]
    fn an_empty_key_prefix_protects_the_whole_bucket() {
        for key_prefix in [None, Some(""), Some("/")] {
            let scope = Scope {
                bucket: "my-bucket",
                key_prefix,
            };
            assert_eq!(scope.object_path(), "my-bucket/", "{key_prefix:?}");
        }
    }

    #[test]
    fn a_not_principal_that_spares_everyone_restricts_nothing() {
        for spared in [
            r#""*""#,
            r#"{"SGWS":"*"}"#,
            r#"{"AWS":["*"]}"#,
            r#"{"SGWS":"12345678901234567890"}"#,
            r#"{"SGWS":"urn:sgws:identity::12345678901234567890:root"}"#,
            r#"{"AWS":"arn:aws:iam::12345678901234567890:root"}"#,
            r#"{"SGWS":"urn:sgws:identity::12345678901234567890:group/*"}"#,
            r#"{"SGWS":[]}"#,
            "{}",
            "null",
        ] {
            let policy = format!(
                r#"{{"Statement":{{"Effect":"Deny","NotPrincipal":{spared},{EVERYTHING}}}}}"#
            );
            assert_eq!(fields(&policy), vec!["`NotPrincipal`"], "{spared}");
        }
    }

    #[test]
    fn only_the_unnamed_principals_are_quoted() {
        let policy = format!(
            r#"{{"Statement":{{"Effect":"Deny","NotPrincipal":{{"SGWS":["{LAKEKEEPER}","*"]}},{EVERYTHING}}}}}"#
        );
        assert_eq!(gaps(&policy)[0].found, "`*`");
    }

    #[test]
    fn a_narrow_action_is_expected_and_found() {
        let found = gaps(&deny(
            r#""Action":"s3:DeleteObject","Resource":["urn:sgws:s3:::my-bucket","urn:sgws:s3:::my-bucket/*"]"#,
        ));
        assert_eq!(
            found,
            vec![Gap {
                field: "`Action`",
                expected: "`s3:*`".to_string(),
                found: "`s3:DeleteObject`".to_string(),
            }]
        );
    }

    #[test]
    fn missing_bucket_or_object_resources_are_named() {
        let objects_only = gaps(&deny(
            r#""Action":"s3:*","Resource":"urn:sgws:s3:::my-bucket/*""#,
        ));
        assert_eq!(objects_only[0].expected, "`urn:sgws:s3:::my-bucket`");
        let bucket_only = gaps(&deny(
            r#""Action":"s3:*","Resource":"urn:sgws:s3:::my-bucket""#,
        ));
        assert_eq!(bucket_only[0].expected, "`urn:sgws:s3:::my-bucket/*`");
        let other_prefix = gaps(&deny(
            r#""Action":"s3:*","Resource":["urn:sgws:s3:::my-bucket","urn:sgws:s3:::my-bucket/other/*"]"#,
        ));
        assert_eq!(other_prefix[0].expected, "`urn:sgws:s3:::my-bucket/*`");
    }

    #[test]
    fn absent_actions_and_resources_are_found_as_none() {
        let found = gaps(&deny(r#""Sid":"empty""#));
        assert_eq!(found.len(), 2, "{found:?}");
        assert!(found.iter().all(|g| g.found == "none"), "{found:?}");
    }

    #[test]
    fn conditions_and_negated_fields_are_named() {
        let found = fields(&deny(
            r#""NotAction":"s3:GetBucketPolicy","NotResource":"urn:sgws:s3:::my-bucket/public/*","Condition":{"IpAddress":{"aws:SourceIp":"192.0.2.0/24"}}"#,
        ));
        assert_eq!(found, vec!["`Action`", "`Resource`", "`Condition`"]);
    }

    #[test]
    fn the_closest_statement_is_reported() {
        let policy = format!(
            r#"{{"Statement":[
                {{"Effect":"Deny","NotPrincipal":{{"SGWS":"{LAKEKEEPER}"}},"Action":"s3:DeleteObject","Resource":"urn:sgws:s3:::other","Condition":{{}}}},
                {{"Effect":"Deny","NotPrincipal":{{"SGWS":"{LAKEKEEPER}"}},"Action":"s3:*","Resource":"urn:sgws:s3:::my-bucket/*","Condition":{{}}}}
            ]}}"#
        );
        assert_eq!(fields(&policy), vec!["`Condition`", "`Resource`"]);
    }

    #[test]
    fn quoted_values_are_capped() {
        let many: Vec<String> = (0..8).map(|i| format!("urn:sgws:s3:::b{i}")).collect();
        let many: Vec<&str> = many.iter().map(String::as_str).collect();
        assert!(
            quoted(&many).ends_with("`urn:sgws:s3:::b4` and 3 more"),
            "{}",
            quoted(&many)
        );
        let long = "x".repeat(QUOTED_LENGTH + 10);
        assert_eq!(quoted(&[&long]).chars().count(), QUOTED_LENGTH + 3);
    }

    #[test]
    fn policies_without_a_deny_not_principal_statement_restrict_nothing() {
        for policy in [
            r#"{"Statement":[{"Effect":"Allow","Principal":{"SGWS":"urn:sgws:identity::12345678901234567890:group/credentials-group-a1b2c3"},"Action":"s3:*","Resource":"*"}]}"#,
            r#"{"Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:*","Resource":"*"}]}"#,
            r#"{"Statement":[]}"#,
            r#"{"Version":"2012-10-17"}"#,
        ] {
            assert_eq!(
                grade(policy, &SCOPE),
                Err(Finding::NoDenyStatement),
                "{policy}"
            );
        }
    }

    #[test]
    fn an_unparsable_policy_does_not_restrict_access() {
        assert_eq!(grade("not json", &SCOPE), Err(Finding::InvalidJson));
    }

    #[test]
    fn the_scope_comes_from_the_profile() {
        let profile = StackitProfile::builder()
            .bucket("my-bucket".to_string())
            .region("eu01".to_string())
            .key_prefix("lakekeeper".to_string())
            .credentials_group_urn(LAKEKEEPER.to_string())
            .build();
        let scope = Scope::of(&profile);
        assert_eq!(scope.bucket, "my-bucket");
        assert_eq!(scope.object_path(), "my-bucket/lakekeeper/");
    }

    #[test]
    fn a_restricting_policy_passes() {
        let check = verdict(&SCOPE, Ok(PolicyRead::Found(recommended())), 5);
        assert_eq!(check.status, ValidationCheckStatus::Passed);
    }

    #[test]
    fn an_open_policy_is_a_warning_with_expected_and_found_details() {
        let policy = deny(r#""Action":"s3:DeleteObject","Resource":"*""#);
        let check = verdict(&SCOPE, Ok(PolicyRead::Found(policy)), 5);
        assert_eq!(check.status, ValidationCheckStatus::Warning);
        let error = check.error.unwrap();
        assert_eq!(error.r#type, "BucketPolicyUnrestricted");
        assert!(
            error.message.contains("falls short in `Action`."),
            "{}",
            error.message
        );
        assert_eq!(
            error.stack,
            vec!["`Action`: expected `s3:*`, found `s3:DeleteObject`".to_string()]
        );
    }

    #[test]
    fn a_missing_policy_is_a_warning() {
        let check = verdict(&SCOPE, Ok(PolicyRead::Missing), 5);
        assert_eq!(check.status, ValidationCheckStatus::Warning);
        let error = check.error.unwrap();
        assert_eq!(error.r#type, "BucketPolicyMissing");
        assert!(
            error.message.contains("including groups created later"),
            "{}",
            error.message
        );
    }

    #[test]
    fn a_denied_read_is_a_warning_naming_both_causes() {
        let check = verdict(&SCOPE, Ok(PolicyRead::Denied), 5);
        assert_eq!(check.status, ValidationCheckStatus::Warning);
        let error = check.error.unwrap();
        assert_eq!(error.r#type, "BucketPolicyReadDenied");
        assert!(
            error.message.contains("`NotPrincipal` lists"),
            "{}",
            error.message
        );
        assert!(
            error.message.contains("`s3:GetBucketPolicy`"),
            "{}",
            error.message
        );
    }

    #[test]
    fn a_failed_read_is_a_warning_naming_the_cause() {
        let unreachable =
            ErrorModel::precondition_failed("probe time limit", "StorageProbeTimeout", None);
        let check = verdict(&SCOPE, Err(unreachable), 5);
        assert_eq!(check.status, ValidationCheckStatus::Warning);
        let error = check.error.unwrap();
        assert_eq!(error.r#type, "BucketPolicyReadFailed");
        assert!(
            error.message.ends_with("probe time limit"),
            "{}",
            error.message
        );
    }

    #[test]
    fn an_unavailable_client_skips_the_check() {
        let check = verdict(&SCOPE, Ok(PolicyRead::ClientUnavailable), 5);
        assert_eq!(check.status, ValidationCheckStatus::Skipped);
        assert_eq!(check.reason.as_deref(), Some(SKIPPED_PREREQUISITE));
    }

    #[tokio::test]
    async fn other_storage_is_skipped() {
        let check = bucket_access_check(
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
