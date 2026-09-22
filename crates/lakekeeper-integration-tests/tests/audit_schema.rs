//! The committed audit schema and field reference are the merge of every crate's committed
//! crate schema.
//!
//! Each crate that declares audit types writes `audit-schema.json` at its root from its own
//! registry (see `lakekeeper::audit::schema`); this test merges the crate schemas of the
//! `lakekeeper` emitter into `audit-format/schema.json` and renders the field reference from
//! it. `just update-audit-schema` runs the crate schema tests and then these with
//! `LAKEKEEPER_UPDATE_AUDIT_SCHEMA=1` to write, then again to verify.

use std::path::{Path, PathBuf};

use lakekeeper::audit::{reference::render_reference, schema};

const EMITTER: &str = "lakekeeper";

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Every committed crate schema under `crates/`, sorted by path so the merge is deterministic.
fn committed_crate_schemas() -> Vec<serde_json::Value> {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(repo_root().join("crates"))
        .expect("crates dir")
        .map(|e| e.expect("entry").path().join("audit-schema.json"))
        .filter(|p| p.is_file())
        .collect();
    paths.sort();
    assert!(
        !paths.is_empty(),
        "no crates/*/audit-schema.json files found"
    );
    paths
        .iter()
        .map(|p| {
            serde_json::from_str(&std::fs::read_to_string(p).expect("crate schema"))
                .unwrap_or_else(|e| panic!("{} is not JSON: {e}", p.display()))
        })
        .filter(|f: &serde_json::Value| f["x-audit-emitter"]["name"] == EMITTER)
        .collect()
}

fn write_or_compare(path: &Path, generated: &str, what: &str) {
    if std::env::var_os(schema::UPDATE_ENV).is_some() {
        std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
        std::fs::write(path, generated)
            .unwrap_or_else(|e| panic!("writing {}: {e}", path.display()));
        return;
    }
    let committed = std::fs::read_to_string(path).unwrap_or_else(|e| {
        panic!(
            "cannot read the committed {what} at {}: {e}\n\nGenerate it with `just update-audit-schema`.",
            path.display()
        )
    });
    assert!(
        committed == generated,
        "the committed {what} at {} is not the merge of the committed crate schemas. Run `just update-audit-schema` and review the diff: it is what a consumer of the audit log will see.",
        path.display()
    );
}

#[test]
fn the_committed_schema_is_the_merge_of_the_crate_schemas() {
    let merged = schema::merge_crate_schemas(&committed_crate_schemas());
    write_or_compare(
        &repo_root().join("audit-format/schema.json"),
        &schema::render(&merged),
        "audit schema",
    );
}

#[test]
fn the_committed_reference_matches_the_schema() {
    let merged = schema::merge_crate_schemas(&committed_crate_schemas());
    write_or_compare(
        &repo_root().join("docs/docs/audit/reference.md"),
        &render_reference(&merged),
        "audit field reference",
    );
}

/// Every crate that declares audit types commits a crate schema.
///
/// The merge reads committed files, so a crate that declares types but never wrote its schema
/// would be absent from the result with nothing to notice. The declarations are in the
/// sources, so that is where this looks.
#[test]
fn every_crate_that_declares_audit_types_commits_a_crate_schema() {
    let mut declaring: Vec<String> = Vec::new();
    for entry in std::fs::read_dir(repo_root().join("crates")).expect("crates dir") {
        let dir = entry.expect("entry").path();
        // The macro crate defines the attribute and declares no audit type itself.
        if dir.file_name().and_then(std::ffi::OsStr::to_str) == Some("audit-macros") {
            continue;
        }
        if !declares_audit_types(&dir.join("src")) {
            continue;
        }
        assert!(
            dir.join("audit-schema.json").is_file(),
            "{} declares audit types and commits no audit-schema.json, so its types reach no \
             schema. Add a test calling `lakekeeper::audit::schema::assert_crate_schema_committed` \
             and run `just update-audit-schema`.",
            dir.display()
        );
        declaring.push(package_name(&dir));
    }
    declaring.sort();

    let mut committed: Vec<String> = committed_crate_schemas()
        .iter()
        .map(|f| {
            f["x-audit-crate"]
                .as_str()
                .expect("x-audit-crate")
                .to_string()
        })
        .collect();
    committed.sort();
    assert_eq!(
        committed, declaring,
        "the committed crate schemas and the crates that declare audit types disagree"
    );
    assert!(
        declaring.len() >= 2,
        "found {} declaring crates, so the scan is not reaching the tree",
        declaring.len()
    );
}

/// Whether any non-test source under `dir` carries the attribute that declares an audit type.
fn declares_audit_types(dir: &Path) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if declares_audit_types(&path) {
                return true;
            }
            continue;
        }
        if path.extension().and_then(std::ffi::OsStr::to_str) != Some("rs")
            || path.file_name().and_then(std::ffi::OsStr::to_str) == Some("tests.rs")
        {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        if text
            .lines()
            .map(str::trim_start)
            .any(|l| l.starts_with("#[") && !l.starts_with("//") && l.contains("audit_part("))
        {
            return true;
        }
    }
    false
}

/// The `name` of the package in `dir`.
fn package_name(dir: &Path) -> String {
    let manifest = std::fs::read_to_string(dir.join("Cargo.toml"))
        .unwrap_or_else(|e| panic!("reading {}: {e}", dir.display()));
    manifest
        .lines()
        .find_map(|l| l.strip_prefix("name = "))
        .map(|n| n.trim().trim_matches('"').to_string())
        .unwrap_or_else(|| panic!("no package name in {}", dir.display()))
}
