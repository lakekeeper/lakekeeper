//! The rules that turn what a scan read into [`Finding`]s. No I/O.

use serde_json::json;

use super::{Finding, FindingKind, Severity};

/// Manifests a [`FindingKind::ManifestLengthMismatch`] lists as examples.
const MAX_EXAMPLES: usize = 5;

/// A manifest whose length in the manifest list is missing, not positive, or differs from its
/// bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct LengthMismatch {
    path: String,
    recorded: Option<i64>,
    actual: u64,
}

impl LengthMismatch {
    /// The mismatch of a manifest, if its `recorded` length is wrong.
    pub(super) fn of(path: &str, recorded: Option<i64>, actual: u64) -> Option<Self> {
        let correct = recorded
            .filter(|recorded| *recorded > 0)
            .is_some_and(|recorded| u64::try_from(recorded) == Ok(actual));
        (!correct).then(|| Self {
            path: path.to_string(),
            recorded,
            actual,
        })
    }
}

/// [`FindingKind::ManifestLengthMismatch`] if any manifest has a wrong length. The evidence names
/// the number of such manifests and the first [`MAX_EXAMPLES`] by path.
pub(super) fn manifest_length_mismatch(mut mismatches: Vec<LengthMismatch>) -> Option<Finding> {
    if mismatches.is_empty() {
        return None;
    }
    mismatches.sort_unstable_by(|a, b| a.path.cmp(&b.path));
    let examples: Vec<_> = mismatches
        .iter()
        .take(MAX_EXAMPLES)
        .map(|mismatch| {
            json!({
                "path": mismatch.path,
                "recorded": mismatch.recorded,
                "actual": mismatch.actual,
            })
        })
        .collect();
    Some(Finding {
        kind: FindingKind::ManifestLengthMismatch,
        severity: Severity::Warning,
        evidence: json!({
            "count": mismatches.len(),
            "examples": examples,
        }),
        thresholds: json!({}),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_length_equal_to_the_bytes_is_correct() {
        assert_eq!(LengthMismatch::of("m", Some(100), 100), None);
    }

    #[test]
    fn a_different_missing_or_non_positive_length_is_a_mismatch() {
        for recorded in [Some(99), Some(101), None, Some(0), Some(-100)] {
            assert_eq!(
                LengthMismatch::of("m", recorded, 100),
                Some(LengthMismatch {
                    path: "m".to_string(),
                    recorded,
                    actual: 100,
                }),
                "{recorded:?}"
            );
        }
    }

    #[test]
    fn a_zero_length_of_an_empty_file_is_a_mismatch() {
        assert!(LengthMismatch::of("m", Some(0), 0).is_some());
    }

    #[test]
    fn no_mismatch_is_no_finding() {
        assert_eq!(manifest_length_mismatch(Vec::new()), None);
    }

    #[test]
    fn the_finding_counts_every_mismatch_and_names_the_first_five_by_path() {
        let mismatches = (0..7)
            .rev()
            .map(|i| LengthMismatch::of(&format!("m{i}"), Some(10), 20).unwrap())
            .collect();

        let finding = manifest_length_mismatch(mismatches).unwrap();

        assert_eq!(
            finding,
            Finding {
                kind: FindingKind::ManifestLengthMismatch,
                severity: Severity::Warning,
                evidence: json!({
                    "count": 7,
                    "examples": (0..5)
                        .map(|i| json!({"path": format!("m{i}"), "recorded": 10, "actual": 20}))
                        .collect::<Vec<_>>(),
                }),
                thresholds: json!({}),
            }
        );
    }

    #[test]
    fn a_missing_length_is_null_in_the_evidence() {
        let finding =
            manifest_length_mismatch(vec![LengthMismatch::of("m", None, 20).unwrap()]).unwrap();

        assert_eq!(
            finding.evidence,
            json!({"count": 1, "examples": [{"path": "m", "recorded": null, "actual": 20}]})
        );
    }
}
