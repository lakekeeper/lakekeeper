use std::sync::{Arc, Mutex};

use assert_json_diff::{CompareMode, Config, assert_json_matches_no_panic};
use valuable::{Valuable, Value, Visit};

use super::{contract::contract_fields, *};
use crate::{
    request_metadata::{PrivilegeSource, RequestMetadata, RequestMetadataTestBuilder, UserAgent},
    service::{
        authz::{ActionDescriptor, DeterminingFactor, PolicyEffect},
        events::context::{
            ActionContextKey, EntityField, EntityType, EventEntities, FIELD_NAME_NAMESPACE,
            FIELD_NAME_NAMESPACE_ID, FIELD_NAME_PROJECT_ID, FIELD_NAME_TABLE, FIELD_NAME_TABLE_ID,
            FIELD_NAME_WAREHOUSE_ID,
        },
    },
};

/// Collects rendered log lines so a test can assert on the JSON a consumer
/// actually receives, rather than on the `Valuable` shape alone.
#[derive(Clone, Default)]
struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for CapturedLogs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("log buffer poisoned").extend(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl tracing_subscriber::fmt::MakeWriter<'_> for CapturedLogs {
    type Writer = Self;

    fn make_writer(&self) -> Self::Writer {
        self.clone()
    }
}

/// Render audit events through the same JSON formatter the binary configures
/// (`crates/lakekeeper-bin/src/main.rs`), and return the parsed lines.
///
/// Generic over the emitting call so the whole audit surface is reachable:
/// [`EventListener::authorization_succeeded`],
/// [`EventListener::authorization_failed`] and
/// [`EventListener::grants_changed`].
///
/// Returns a `Vec` because `grants_changed` emits one record *per grant
/// triple*, not one per call. Use [`emit_and_capture_one`] where exactly one
/// record is expected.
fn emit_and_capture<F, Fut>(emit: F) -> Vec<serde_json::Value>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    let logs = CapturedLogs::default();
    // Mirrors the binary's subscriber. Every setting is pinned deliberately,
    // including those that match today's defaults, so a `tracing-subscriber`
    // upgrade that changes a default breaks this line rather than silently
    // rewriting what every test sees.
    let subscriber = tracing_subscriber::fmt()
        .json()
        .flatten_event(true)
        // Production sets this; `Json::default()` leaves it `true`. Without it
        // the helper renders a `span` object the binary never emits — harmless
        // while no span is active, wrong the moment a test runs under one (and
        // production always does: the router installs a request span).
        .with_current_span(false)
        .with_span_list(true)
        // Production gates these on `CONFIG_BIN.debug.extended_logs`, i.e. off
        // by default. Pin them off so this file's own line numbers can never
        // leak into a captured record.
        .with_file(false)
        .with_line_number(false)
        .with_writer(logs.clone())
        .finish();

    tracing::subscriber::with_default(subscriber, || {
        futures::executor::block_on(emit()).expect("emitting an audit event must not fail");
    });

    let bytes = logs.0.lock().expect("log buffer poisoned").clone();
    let text = String::from_utf8(bytes).expect("log output must be utf-8");
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("log line must be valid json"))
        .collect()
}

/// [`emit_and_capture`] for the case where exactly one record is expected.
#[track_caller]
fn emit_and_capture_one<F, Fut>(emit: F) -> serde_json::Value
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    let mut records = emit_and_capture(emit);
    assert_eq!(
        records.len(),
        1,
        "expected exactly one audit record, got {}",
        records.len()
    );
    records.pop().expect("length asserted above")
}

fn succeeded_event(request_metadata: RequestMetadata) -> AuthorizationSucceededEvent {
    let entities = Arc::new(EventEntities::one(EntityDescriptor::new(EntityType::Table)));
    let actions = Arc::new(vec![
        ActionDescriptor::builder().action_name("read_data").build(),
    ]);
    AuthorizationSucceededEvent {
        request_metadata: Arc::new(request_metadata),
        entities,
        actions,
        extra_context: Arc::new(std::collections::HashMap::new()),
        authorizations: Arc::new(vec![sample(Vec::new())]),
    }
}

// ── Wire-format fixtures ────────────────────────────────────────────────────
//
// Each fixture is a committed record of exactly what one audit event renders to
// on the wire. Together they are the only thing in the tree that observes the
// emitted JSON, and therefore the only thing that can detect an unintended
// change to the audit format.
//
// Every value below is fixed. Random ids or a clock would make each run differ,
// and at most one `extra_context` key is used per fixture: `extra_context` is a
// `HashMap`, so two or more keys render in an unstable order and the fixtures
// would fail at random.
//
// To regenerate after a deliberate change: `just update-audit-fixtures`.

const FIXTURE_WAREHOUSE_ID: &str = "019684ff-0000-7000-8000-000000000001";
const FIXTURE_TABLE_ID: &str = "019684ff-0000-7000-8000-000000000002";
const FIXTURE_NAMESPACE_ID: &str = "019684ff-0000-7000-8000-000000000003";

fn fixture_path(name: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/service/events/backends/audit/fixtures/v1")
        .join(format!("{name}.json"))
}

/// Assert that `emitted` still matches the committed fixture, and classify any
/// difference as a major or a minor change to [`AUDIT_FORMAT`].
///
/// Read and written at runtime rather than embedded with `include_str!`, so the
/// same code path can also regenerate the file. A brand-new fixture would
/// otherwise fail to compile before it could be generated.
#[track_caller]
fn assert_matches_fixture(name: &str, emitted: &serde_json::Value) {
    let path = fixture_path(name);

    if std::env::var_os("LAKEKEEPER_UPDATE_AUDIT_FIXTURES").is_some() {
        std::fs::create_dir_all(path.parent().expect("fixture path has a parent"))
            .expect("creating the fixture directory");
        let mut json = serde_json::to_string_pretty(emitted).expect("an audit record serialises");
        json.push('\n');
        std::fs::write(&path, json).unwrap_or_else(|e| panic!("writing {}: {e}", path.display()));
        return;
    }

    let committed = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "cannot read the committed audit fixture {}: {e}\n\n\
             If this fixture is new, generate it with `just update-audit-fixtures`. \
             If it was moved or deleted, restore it: it is the record of what \
             audit_format {AUDIT_FORMAT} puts on the wire, and without it nothing \
             detects a change to the audit log format.",
            path.display()
        )
    });
    let committed: serde_json::Value = serde_json::from_str(&committed)
        .unwrap_or_else(|e| panic!("fixture {} is not valid JSON: {e}", path.display()));

    // A fixture of `{}` satisfies the subset check below unconditionally, so an
    // emptied or truncated file would switch the breaking-change check off while
    // leaving a green test. Floor the key count.
    assert!(
        committed
            .as_object()
            .is_some_and(|object| object.len() >= 6),
        "fixture {} has fewer than 6 keys and looks truncated. Compared against a \
         near-empty fixture, the check below asserts almost nothing.",
        path.display()
    );

    // Is every key the fixture records still present, with the same type and
    // value? `CompareMode::Inclusive` walks the right-hand value and requires the
    // left to contain it, so with the fixture on the right this asserts
    // "fixture is a subset of emitted": extra keys in `emitted` pass.
    //
    // Do not re-derive that direction from assert-json-diff's own documentation,
    // which describes `Inclusive` the other way round; the behaviour above is
    // what its `diff.rs` implements and what this test relies on. Reversed, this
    // check would pass while a field was being deleted.
    if let Err(difference) =
        assert_json_matches_no_panic(emitted, &committed, Config::new(CompareMode::Inclusive))
    {
        panic!(
            "a field recorded in {name} is missing, renamed, retyped, or has a \
             different VALUE.\n\n{difference}\n\n\
             A key moving, or a wire-enum value being renamed (`entity_type`, \
             `decision`, `actor_type` and the rest reach the log as string VALUES), \
             BREAKS CONSUMERS: bump the MAJOR half of AUDIT_FORMAT (now \
             {AUDIT_FORMAT}) and regenerate with `just update-audit-fixtures`. A \
             changed test INPUT does not: regenerate and leave AUDIT_FORMAT alone — \
             bumping for that is rejected by `just check-audit-format-bump`.\n\n\
             See the audit log section of docs/docs/developer-guide.md."
        );
    }

    // Reaching here means nothing recorded in the fixture moved, so the only way
    // to differ is a key present in `emitted` and absent from the fixture: a
    // purely additive change, which existing consumers can ignore.
    if let Err(difference) =
        assert_json_matches_no_panic(emitted, &committed, Config::new(CompareMode::Strict))
    {
        panic!(
            "the audit log format gained a field: additive, so existing consumers \
             keep working.\n\n{difference}\n\n\
             Bump the MINOR half of AUDIT_FORMAT (now {AUDIT_FORMAT}), regenerate \
             with `just update-audit-fixtures`, and document the field in \
             docs/docs/logging.md."
        );
    }
}

/// Pin the direction of [`CompareMode::Inclusive`], which the fixture comparison
/// above depends on and which cannot be read off the dependency.
///
/// `assert-json-diff` is a caret dependency, and its own documentation describes
/// `Inclusive` the opposite way round from what it implements. So the direction is
/// neither obvious from the call nor safe to re-derive from the docs, and a minor
/// upgrade that "fixed" the implementation to match the documentation would silently
/// invert the fixture check: a deleted field would start reading as an addition, and
/// a breaking change would be classified as a minor one.
///
/// The two assertions here are deliberately each other's mirror. Swapping them makes
/// this test fail, which is the point — it fails here, loudly, instead of in the
/// classification of somebody else's change.
#[test]
fn inclusive_comparison_requires_the_right_hand_side_to_be_contained_in_the_left() {
    let subset = serde_json::json!({ "kept": 1 });
    let superset = serde_json::json!({ "kept": 1, "extra": 2 });
    let inclusive = || Config::new(CompareMode::Inclusive);

    // Extra keys on the LEFT are allowed. This is the case the fixture check relies
    // on: `assert_json_matches!(&emitted, &fixture, Inclusive)` must tolerate an
    // emitted record that has gained a field.
    assert!(
        assert_json_matches_no_panic(&superset, &subset, inclusive()).is_ok(),
        "Inclusive must accept extra keys in the left-hand value. If this fails, the \
         crate has inverted the comparison and the fixture check now treats an added \
         field as a removed one."
    );

    // Extra keys on the RIGHT are a failure. This is what makes a removed field a
    // breaking change rather than an additive one.
    assert!(
        assert_json_matches_no_panic(&subset, &superset, inclusive()).is_err(),
        "Inclusive must reject keys present in the right-hand value and missing from \
         the left. If this fails, the fixture check would pass while a field is being \
         deleted from the audit log."
    );
}

fn fixture_table_entity() -> EntityDescriptor {
    EntityDescriptor::new(EntityType::Table)
        .field(FIELD_NAME_WAREHOUSE_ID, &FIXTURE_WAREHOUSE_ID)
        .field(FIELD_NAME_TABLE_ID, &FIXTURE_TABLE_ID)
        .field(FIELD_NAME_TABLE, &"sales.orders")
}

fn fixture_namespace_entity() -> EntityDescriptor {
    EntityDescriptor::new(EntityType::Namespace)
        .field(FIELD_NAME_WAREHOUSE_ID, &FIXTURE_WAREHOUSE_ID)
        .field(FIELD_NAME_NAMESPACE_ID, &FIXTURE_NAMESPACE_ID)
        .field(FIELD_NAME_NAMESPACE, &"sales")
}

fn fixture_read_action() -> ActionDescriptor {
    ActionDescriptor::builder().action_name("read_data").build()
}

/// An action carrying context, so the fixtures pin that nesting too.
fn fixture_action_with_context() -> ActionDescriptor {
    ActionDescriptor::builder()
        .action_name("update_table_properties")
        .context_string(ActionContextKey::Name, "orders")
        .context_list(
            ActionContextKey::RemovedProperties,
            vec!["stale.key".to_string()],
        )
        .build()
}

/// A create action, carrying the client-requested name and id.
fn fixture_create_table_action() -> ActionDescriptor {
    ActionDescriptor::builder()
        .action_name("create_table")
        .context_string(ActionContextKey::Name, "orders")
        .context_string(ActionContextKey::TableId, FIXTURE_TABLE_ID)
        .build()
}

/// A drop action. `force` and `purge` are emitted only when the client asked for
/// them, so their presence here pins the "true" form and their absence elsewhere
/// pins the other.
fn fixture_drop_action() -> ActionDescriptor {
    ActionDescriptor::builder()
        .action_name("drop")
        .context_string(ActionContextKey::Force, "true")
        .context_string(ActionContextKey::Purge, "true")
        .build()
}

/// A warehouse entity carrying `project-id`, which real requests emit and the other
/// fixtures do not.
fn fixture_warehouse_entity() -> EntityDescriptor {
    EntityDescriptor::new(EntityType::Warehouse)
        .field(
            FIELD_NAME_PROJECT_ID,
            &"00000000-0000-0000-0000-000000000000",
        )
        .field(FIELD_NAME_WAREHOUSE_ID, &FIXTURE_WAREHOUSE_ID)
}

/// The simplest per-decision entry: no id, no `for-principal`, no
/// `determined_by`. Pins which keys are omitted rather than emitted as null.
fn fixture_plain_authorization() -> Authorization {
    Authorization {
        id: None,
        for_principal: None,
        action: fixture_read_action(),
        entity: fixture_table_entity(),
        allowed: Some(true),
        determined_by: Vec::new(),
    }
}

/// A minimal entry for a denied decision. `CannotSeeResource`, `ResourceNotFound`
/// and `ActionForbidden` are definitive denials, so the per-decision `allowed` must
/// be `false` — a denied record carrying `allowed: true` describes a shape the
/// emitter cannot produce.
fn fixture_denied_authorization() -> Authorization {
    Authorization {
        allowed: Some(false),
        ..fixture_plain_authorization()
    }
}

/// A fully-populated entry, so the fixtures pin the optional keys in their
/// present form as well as their absent one, and both `DeterminingFactor`
/// variants including its own `None` fields.
fn fixture_detailed_authorization() -> Authorization {
    Authorization {
        id: Some("check-0".to_string()),
        for_principal: Some(UserOrRoleId::User(
            crate::service::authn::UserId::try_from("oidc~bob").expect("valid test user id"),
        )),
        action: fixture_read_action(),
        entity: fixture_namespace_entity(),
        allowed: Some(false),
        determined_by: vec![
            DeterminingFactor::Policy {
                policy_id: "policy-42".to_string(),
                name: Some("deny-stale-namespaces".to_string()),
                effect: PolicyEffect::Forbid,
                source: Some("cedar".to_string()),
            },
            DeterminingFactor::SystemAuthority {
                source: None,
                reason: None,
            },
        ],
    }
}

fn fixture_context(entries: &[(&str, &str)]) -> Arc<std::collections::HashMap<String, String>> {
    Arc::new(
        entries
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect(),
    )
}

/// An authenticated caller with a `User-Agent`, so the fixtures pin the populated
/// form of both `actor` and `user_agent`.
fn fixture_metadata() -> RequestMetadata {
    RequestMetadataTestBuilder::builder()
        .actor(Actor::Principal(
            crate::service::authn::UserId::try_from("oidc~alice").expect("valid test user id"),
        ))
        .user_agent(UserAgent::parse("Apache-Spark/3.5.1 (Scala/2.12)"))
        .build()
}

fn fixture_error() -> Arc<crate::service::events::AuthorizationError> {
    Arc::new(crate::service::events::AuthorizationError {
        r#type: "NotAuthorized".to_string(),
        code: 403,
        message: "Principal is not allowed to read this table".to_string(),
        stack: vec!["authorizer: no matching grant".to_string()],
        error_id: "019684ff-0000-7000-8000-0000000000ff".to_string(),
    })
}

/// Every fixture, so that both tests below cover the whole committed set rather
/// than whichever files happen to exist.
const FIXTURE_NAMES: &[&str] = &[
    "authz_succeeded_single",
    "authz_succeeded_plural",
    "authz_succeeded_action_entities",
    "authz_succeeded_actions_entity",
    "authz_failed_single",
    "authz_failed_context",
    "authz_succeeded_rich_action_context",
    "grant_created",
    "grant_revoked",
];

fn read_fixture(name: &str) -> serde_json::Value {
    let path = fixture_path(name);
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
    serde_json::from_str(&text)
        .unwrap_or_else(|e| panic!("fixture {} is not valid JSON: {e}", path.display()))
}

/// Action context keys whose value is a client-supplied map. Their keys are user
/// data — `docs/docs/logging.md` documents them as "arbitrary [...] not part of the
/// audit format" — so the walk below records the container and stops there, rather
/// than demanding that a customer's table property be documented as a format field.
///
/// The authorization and operational `context` objects are deliberately *not* listed.
/// Their keys are string literals at call sites in this repository, so requiring each
/// to be documented is exactly the point.
const FREE_FORM_CONTAINERS: &[&str] = &[
    ActionContextKey::Properties.as_str(),
    ActionContextKey::UpdatedProperties.as_str(),
];

/// Collect every JSON object key in `value`, at any depth, except inside the
/// free-form containers above.
fn collect_keys(value: &serde_json::Value, out: &mut Vec<String>) {
    match value {
        serde_json::Value::Object(fields) => {
            for (key, nested) in fields {
                out.push(key.clone());
                if !FREE_FORM_CONTAINERS.contains(&key.as_str()) {
                    collect_keys(nested, out);
                }
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                collect_keys(item, out);
            }
        }
        _ => {}
    }
}

/// Every field the audit log puts on the wire must be documented, so the reference
/// in `docs/docs/logging.md` cannot quietly fall behind the code.
///
/// Driven off the committed fixtures, so it covers what is actually emitted rather
/// than what some type declares. Add a field and this fails, naming it.
///
/// Coverage is therefore bounded by the fixtures: a key emitted only by a code path
/// no fixture exercises is invisible here. Widening the fixture set widens this
/// check too, which is the main reason to add one.
///
/// Keys are matched as `` `key` `` — a field table entry or inline mention, not a
/// bare appearance inside a JSON example, since an example is not a description.
/// The consumer-facing audit log reference, embedded at COMPILE time: if
/// `logging.md` is deleted or moved, this line fails the build with "couldn't read
/// …: No such file or directory". It can never silently read an empty string. The
/// path is relative to this file, so it climbs from `backends/audit/` to the
/// repository root; `crate::api::endpoints` uses the same technique for the
/// committed `OpenAPI` specs.
const LOGGING_DOC: &str = include_str!("../../../../../../../docs/docs/logging.md");

#[test]
fn every_emitted_audit_field_is_documented() {
    // The compile-time check on LOGGING_DOC only covers the file being gone. This covers the other
    // failure: the file is still there but no longer holds the audit reference —
    // split into another page, replaced by a stub, or gutted — which would
    // otherwise surface as one baffling failure per field.
    assert!(
        LOGGING_DOC.contains("{#audit-logs}"),
        "docs/docs/logging.md no longer contains the `{{#audit-logs}}` anchor. The \
         audit log documentation has moved, been split, or been deleted. This test \
         asserts that every field the audit log emits is documented there, so point \
         it at the new location and update the `#audit-logs` links in the other docs."
    );

    let mut keys = Vec::new();
    for name in FIXTURE_NAMES {
        collect_keys(&read_fixture(name), &mut keys);
    }
    // The subscriber-owned keys are stripped before a fixture is written, so the walk above
    // never sees them. They are still on the wire, and `logging.md` restates the list —
    // this makes the Rust constant the one that decides what that list says.
    keys.extend(
        super::contract::ENVELOPE_KEYS
            .iter()
            .map(|key| (*key).to_string()),
    );
    keys.sort();
    keys.dedup();

    let undocumented: Vec<&String> = keys
        .iter()
        .filter(|key| !LOGGING_DOC.contains(&format!("`{key}`")))
        .collect();

    assert!(
        undocumented.is_empty(),
        "these audit log fields are emitted but not documented in \
         docs/docs/logging.md: {undocumented:?}\n\n\
         Add each one to the relevant field table. A field nobody documented is a \
         field consumers have to reverse-engineer from example output, which is how \
         the reference fell out of step with the code before.\n\n\
         Adding a field is a minor change to the audit format: see the audit log \
         section of docs/docs/developer-guide.md."
    );
}

// ── Variant tags of the derived audit enums ─────────────────────────────────
//
// These three types reach the wire through `#[derive(valuable::Valuable)]`, which
// emits whatever variants the type has. Adding one therefore changes the audit log
// format with no code change anywhere, and no fixture can catch it: a fixture can
// only exercise a variant that already exists. Verified — adding a variant to
// `ContextValue`, which is matched by hand, fails to build with E0004, while adding
// one to `DeterminingFactor` compiles clean.
//
// Each match below has no wildcard arm, so a new variant stops the build *here*, at
// the point where its wire tag has to be chosen and documented.

#[deny(clippy::wildcard_enum_match_arm)]
fn determining_factor_tag(factor: &DeterminingFactor) -> &'static str {
    match factor {
        DeterminingFactor::Policy { .. } => "Policy",
        DeterminingFactor::SystemAuthority { .. } => "SystemAuthority",
    }
}

#[deny(clippy::wildcard_enum_match_arm)]
fn policy_effect_tag(effect: PolicyEffect) -> &'static str {
    match effect {
        PolicyEffect::Permit => "Permit",
        PolicyEffect::Forbid => "Forbid",
    }
}

/// Every variant a derived audit enum can put on the wire must be documented.
///
/// The variant sets come from `strum`, not from lists written out here. That is the
/// point: a hand-written list is exhaustive only until somebody forgets to extend it,
/// and this test's whole job is to notice a variant nobody thought about.
///
/// Two conventions are accepted because the reference uses both: enum tags appear as
/// `` `Permit` `` while `privilege_source` values appear as `` `"authorizer"` ``.
///
/// # The residual gap
///
/// `VARIANTS` gives Rust identifiers, while what reaches the wire comes from the tag
/// functions. Where the two coincide — as they do today for every enum here — this
/// test covers the wire names. Rename a tag while leaving its variant name alone and
/// it will not notice. Closing that would mean deriving the tags themselves, which is
/// a larger change to the `valuable` plumbing than the risk warrants.
#[test]
fn every_variant_a_derived_audit_enum_can_emit_is_documented() {
    use strum::{VariantArray as _, VariantNames as _};

    use crate::service::events::AuthorizationFailureReason as Reason;

    // `DeterminingFactor`'s variants carry fields, so values cannot be enumerated and
    // these two have to be built by hand. The assertion below is what keeps the pair
    // honest against the type.
    let factors = [
        DeterminingFactor::Policy {
            policy_id: String::new(),
            name: None,
            effect: PolicyEffect::Permit,
            source: None,
        },
        DeterminingFactor::SystemAuthority {
            source: None,
            reason: None,
        },
    ];
    let factor_tags: Vec<&'static str> = factors.iter().map(determining_factor_tag).collect();
    assert_eq!(
        factor_tags.len(),
        DeterminingFactor::VARIANTS.len(),
        "`DeterminingFactor` has {} variants but only {} are built here. Its variants \
         carry fields, so `strum` cannot enumerate values and this list is hand-built: \
         add the missing one. Variants: {:?}",
        DeterminingFactor::VARIANTS.len(),
        factor_tags.len(),
        DeterminingFactor::VARIANTS,
    );
    for name in DeterminingFactor::VARIANTS {
        assert!(
            factor_tags.contains(name),
            "`DeterminingFactor::{name}` is not covered by the hand-built list in this \
             test, so its wire tag is never checked against the documentation."
        );
    }

    let tags: Vec<&'static str> = factor_tags
        .into_iter()
        .chain(
            PolicyEffect::VARIANTS
                .iter()
                .copied()
                .map(policy_effect_tag),
        )
        .chain(
            Reason::VARIANTS
                .iter()
                .map(super::contract::failure_reason_tag),
        )
        .chain(
            PrivilegeSource::VARIANTS
                .iter()
                .copied()
                .map(PrivilegeSource::as_str),
        )
        .collect();

    for tag in tags {
        assert!(
            LOGGING_DOC.contains(&format!("`{tag}`"))
                || LOGGING_DOC.contains(&format!("`\"{tag}\"`")),
            "the audit log can emit `{tag}`, but docs/docs/logging.md does not mention \
             it. A variant of one of these enums reaches the wire as a value, so it is \
             part of the format: document what it means, and treat adding one as a \
             minor change to the audit format."
        );
    }
}

/// Every key the audit log can emit must be documented — checked against the type
/// system, not against the fixtures.
///
/// This is the one check here that is not sample-based. The fixture tests and the
/// documentation test above can only see keys some fixture happens to emit, so a key
/// on a path nobody wrote a fixture for is invisible to them. Comparing audit records
/// from a running server against these fixtures found five such keys, two of them
/// undocumented, which is what prompted closing the key spaces into enums.
///
/// Because the sets below come from `VariantArray`, adding a key cannot escape this
/// check: a new variant is either listed here or the build fails in `as_str`.
///
/// Keys are required as a **table row** rather than a bare mention, so that an
/// unrelated use of the same word elsewhere in the page cannot satisfy it — the
/// action context key `source` and the `determined_by` field `source` are different
/// things that happen to share a name.
#[test]
fn every_key_the_audit_log_can_emit_is_documented() {
    use strum::VariantArray as _;

    let mut missing: Vec<String> = Vec::new();

    // A row whose FIRST column is the key. Matching anywhere on the line is not
    // enough: `| `Policy` | `source` |` in the determining-factor table would then
    // satisfy a lookup for the unrelated action context key `source`.
    let has_row = |key: &str| {
        let cell = format!("| `{key}`");
        LOGGING_DOC
            .lines()
            .any(|line| line.trim_start().starts_with(&cell))
    };

    for field in EntityField::VARIANTS {
        let key = field.as_str();
        if !has_row(key) {
            missing.push(format!("entity field `{key}` ({field:?})"));
        }
    }
    for key in ActionContextKey::VARIANTS {
        let name = key.as_str();
        if !has_row(name) {
            missing.push(format!("action context key `{name}` ({key:?})"));
        }
    }
    // `entity_type` values are documented as a prose list rather than a table, so a
    // plain mention is the right bar for these.
    for entity_type in EntityType::VARIANTS {
        let name = entity_type.as_str();
        if !LOGGING_DOC.contains(&format!("`{name}`")) {
            missing.push(format!("entity type `{name}` ({entity_type:?})"));
        }
    }

    assert!(
        missing.is_empty(),
        "the audit log can emit these keys, but docs/docs/logging.md does not \
         document them:\n  {}\n\n\
         Add a row to the relevant field table in docs/docs/logging.md. Every key the \
         emitter can produce is part of the wire format, whether or not a fixture \
         happens to exercise it.",
        missing.join("\n  ")
    );
}

/// The fixture directory and [`FIXTURE_NAMES`] must agree. Without this, deleting a
/// test leaves an orphan fixture that nothing asserts, and a fixture added by hand
/// is never compared against anything.
#[test]
fn the_fixture_directory_matches_the_declared_set() {
    let directory = fixture_path("unused")
        .parent()
        .expect("fixture path has a parent")
        .to_path_buf();

    let mut on_disk: Vec<String> = std::fs::read_dir(&directory)
        .unwrap_or_else(|e| panic!("reading {}: {e}", directory.display()))
        .map(|entry| entry.expect("readable directory entry").file_name())
        .filter_map(|name| {
            name.to_str()
                .and_then(|name| name.strip_suffix(".json"))
                .map(str::to_owned)
        })
        .collect();
    on_disk.sort();

    let mut declared: Vec<String> = FIXTURE_NAMES.iter().map(|n| (*n).to_string()).collect();
    declared.sort();

    assert_eq!(
        on_disk, declared,
        "the fixtures on disk and the ones declared in FIXTURE_NAMES have drifted. A \
         fixture with no test asserting it detects nothing; a declared fixture with \
         no file makes the tests fail on a missing file instead of on a real change. \
         Regenerate with `just update-audit-fixtures`."
    );
}

/// One action, one entity: `audit_log!` emits the singular `action` / `entity`
/// keys. No `extra_context`, and an anonymous caller with no `User-Agent`, so
/// this fixture is the one that pins the absent and null forms.
#[test]
fn fixture_authz_succeeded_single_action_single_entity() {
    let record = emit_and_capture_one(|| {
        AuditEventListener.authorization_succeeded(AuthorizationSucceededEvent {
            request_metadata: Arc::new(RequestMetadataTestBuilder::builder().build()),
            entities: Arc::new(EventEntities::one(fixture_table_entity())),
            actions: Arc::new(vec![fixture_read_action()]),
            extra_context: fixture_context(&[]),
            authorizations: Arc::new(vec![fixture_plain_authorization()]),
        })
    });

    assert_matches_fixture("authz_succeeded_single", &contract_fields(record));
}

/// Several actions and several entities: `audit_log!` switches to the plural
/// `actions` / `entities` keys. Also carries `extra_context`, an action with its
/// own context, and a fully-populated per-decision entry.
#[test]
fn fixture_authz_succeeded_plural_actions_plural_entities() {
    let record = emit_and_capture_one(|| {
        AuditEventListener.authorization_succeeded(AuthorizationSucceededEvent {
            request_metadata: Arc::new(fixture_metadata()),
            entities: Arc::new(EventEntities::many([
                fixture_table_entity(),
                fixture_namespace_entity(),
            ])),
            actions: Arc::new(vec![fixture_read_action(), fixture_action_with_context()]),
            extra_context: fixture_context(&[("invoked-by", "maintenance-task")]),
            authorizations: Arc::new(vec![
                fixture_plain_authorization(),
                fixture_detailed_authorization(),
            ]),
        })
    });

    assert_matches_fixture("authz_succeeded_plural", &contract_fields(record));
}

/// One action, several entities: the singular `action` key with the plural
/// `entities` key. This mixed arity is its own arm of `audit_log!`.
#[test]
fn fixture_authz_succeeded_single_action_plural_entities() {
    let record = emit_and_capture_one(|| {
        AuditEventListener.authorization_succeeded(AuthorizationSucceededEvent {
            request_metadata: Arc::new(fixture_metadata()),
            entities: Arc::new(EventEntities::many([
                fixture_table_entity(),
                fixture_namespace_entity(),
            ])),
            actions: Arc::new(vec![fixture_read_action()]),
            extra_context: fixture_context(&[]),
            authorizations: Arc::new(vec![fixture_plain_authorization()]),
        })
    });

    assert_matches_fixture("authz_succeeded_action_entities", &contract_fields(record));
}

/// Several actions, one entity: the remaining arm, plural `actions` with the
/// singular `entity` key.
#[test]
fn fixture_authz_succeeded_plural_actions_single_entity() {
    let record = emit_and_capture_one(|| {
        AuditEventListener.authorization_succeeded(AuthorizationSucceededEvent {
            request_metadata: Arc::new(fixture_metadata()),
            entities: Arc::new(EventEntities::one(fixture_table_entity())),
            actions: Arc::new(vec![fixture_read_action(), fixture_action_with_context()]),
            extra_context: fixture_context(&[]),
            authorizations: Arc::new(vec![fixture_plain_authorization()]),
        })
    });

    assert_matches_fixture("authz_succeeded_actions_entity", &contract_fields(record));
}

/// Action context and entity fields that real traffic emits but the other fixtures
/// do not: `name`, `table_id`, `force`, `purge`, and `project-id`.
///
/// Added after comparing these fixtures against audit records from a running server,
/// which emitted all five. Without a fixture that carries them, nothing checks that
/// they stay documented — the documentation test walks the fixtures, so its reach is
/// exactly the fixtures' reach.
#[test]
fn fixture_authz_succeeded_rich_action_context() {
    let record = emit_and_capture_one(|| {
        AuditEventListener.authorization_succeeded(AuthorizationSucceededEvent {
            request_metadata: Arc::new(fixture_metadata()),
            entities: Arc::new(EventEntities::one(fixture_warehouse_entity())),
            actions: Arc::new(vec![fixture_create_table_action(), fixture_drop_action()]),
            extra_context: fixture_context(&[]),
            authorizations: Arc::new(vec![fixture_plain_authorization()]),
        })
    });

    assert_matches_fixture(
        "authz_succeeded_rich_action_context",
        &contract_fields(record),
    );
}

/// A denied authorization. Carries `failure_reason` and `error`, which succeeded
/// events do not, and records `decision: "denied"`.
#[test]
fn fixture_authz_failed_single_action_single_entity() {
    let record = emit_and_capture_one(|| {
        AuditEventListener.authorization_failed(AuthorizationFailedEvent {
            request_metadata: Arc::new(fixture_metadata()),
            entities: Arc::new(EventEntities::one(fixture_table_entity())),
            actions: Arc::new(vec![fixture_read_action()]),
            failure_reason: crate::service::events::AuthorizationFailureReason::ActionForbidden,
            error: fixture_error(),
            extra_context: fixture_context(&[]),
            authorizations: Arc::new(vec![fixture_detailed_authorization()]),
        })
    });

    assert_matches_fixture("authz_failed_single", &contract_fields(record));
}

/// A denied authorization that also carries `extra_context`, which is emitted by
/// a different arm of the listener from the one above.
#[test]
fn fixture_authz_failed_with_context() {
    let record = emit_and_capture_one(|| {
        AuditEventListener.authorization_failed(AuthorizationFailedEvent {
            request_metadata: Arc::new(fixture_metadata()),
            entities: Arc::new(EventEntities::one(fixture_namespace_entity())),
            actions: Arc::new(vec![fixture_read_action()]),
            failure_reason: crate::service::events::AuthorizationFailureReason::CannotSeeResource,
            error: fixture_error(),
            extra_context: fixture_context(&[("self-read", "false")]),
            authorizations: Arc::new(vec![fixture_denied_authorization()]),
        })
    });

    assert_matches_fixture("authz_failed_context", &contract_fields(record));
}

/// The operational family, emitted through `audit_operation!` rather than
/// `audit_log!` — a different shape entirely, with `operation` / `outcome` /
/// `context` and no `entity` or `decision`.
///
/// One `grants_changed` event emits one record per grant triple, revocations
/// first, so this covers both operations in the order a consumer sees them.
#[test]
fn fixture_grants_changed_emits_one_record_per_triple() {
    let principal = UserOrRoleId::User(
        crate::service::authn::UserId::try_from("oidc~alice").expect("valid test user id"),
    );
    let spec = |privilege: &str, resource: GrantResource| crate::service::authz::GrantSpec {
        principal: principal.clone(),
        resource,
        privilege: privilege.to_string(),
    };
    let uuid = |s: &str| s.parse::<uuid::Uuid>().expect("fixed test uuid");
    let table = || GrantResource::Table {
        warehouse_id: crate::service::WarehouseId::new(uuid(FIXTURE_WAREHOUSE_ID)),
        table_id: crate::service::TableId::new(uuid(FIXTURE_TABLE_ID)),
    };

    let records = emit_and_capture(|| {
        AuditEventListener.grants_changed(GrantsChangedEvent::new(
            vec![spec("modify", table())],
            vec![spec("select", table())],
            Arc::new(fixture_metadata()),
        ))
    });

    assert_eq!(
        records.len(),
        2,
        "one record per grant triple, revocation first: {records:?}"
    );
    let mut records = records.into_iter();
    let revoked = records.next().expect("the revoked record");
    let created = records.next().expect("the created record");

    assert_matches_fixture("grant_revoked", &contract_fields(revoked));
    assert_matches_fixture("grant_created", &contract_fields(created));
}

/// The envelope keys are deliberately outside the format contract, so no fixture
/// records them — which means nothing would notice if the subscriber stopped
/// emitting them entirely. Assert the ones a consumer genuinely relies on.
#[test]
fn audit_records_carry_the_envelope_keys_consumers_rely_on() {
    let record = emit_and_capture_one(|| {
        AuditEventListener.authorization_succeeded(succeeded_event(fixture_metadata()))
    });

    for key in ["timestamp", "level", "message", "target"] {
        assert!(
            record.get(key).is_some(),
            "the log subscriber stopped emitting `{key}`. It is outside the \
             audit_format contract, so no fixture covers it, but consumers do rely \
             on it: {record}"
        );
    }
}

/// The audit log has to say which client made the call, verbatim — a SIEM
/// classifies the string, so Lakekeeper must not normalise it away.
#[test]
fn an_audit_event_records_the_user_agent_verbatim() {
    let metadata = RequestMetadataTestBuilder::builder()
        .user_agent(UserAgent::parse("Apache-Spark/3.5.1 (Scala/2.12)"))
        .build();

    let event = emit_and_capture_one(|| {
        AuditEventListener.authorization_succeeded(succeeded_event(metadata))
    });

    assert_eq!(
        event.get("user_agent").and_then(serde_json::Value::as_str),
        Some("Apache-Spark/3.5.1 (Scala/2.12)"),
    );
}

/// The capture helper must render what the binary renders. Nothing else pins
/// that, and if it drifts every fixture captured through it silently describes
/// a shape production never emits.
///
/// `with_current_span(false)` is the setting that is easy to lose, and it is
/// only observable while a span is active — which production always is, since
/// the router installs a request span around every call.
#[test]
fn the_capture_helper_omits_envelope_keys_production_omits() {
    use tracing::Instrument as _;

    let metadata = RequestMetadataTestBuilder::builder().build();
    let record = emit_and_capture_one(|| {
        // Built inside the closure, so the span is registered with the capture
        // subscriber rather than whatever is globally installed, and
        // `Instrument` makes it current while the future is polled.
        let span = tracing::info_span!("request");
        AuditEventListener
            .authorization_succeeded(succeeded_event(metadata))
            .instrument(span)
    });

    assert!(
        record.get("span").is_none(),
        "captured record carries a `span` key. Production sets \
         `.with_current_span(false)` (crates/lakekeeper-bin/src/main.rs), so this \
         helper must too — otherwise captured fixtures describe a shape the binary \
         never emits. Got: {record}"
    );
}

/// The context-free form of [`audit_operation`].
///
/// Nothing in this repository emits an operational audit event without context, and
/// the macro's own example is marked `ignore` so it is never compiled — so without
/// this test the optional-context arm has no coverage at all, and a change to it
/// would compile and ship unnoticed. Also pins that omitting the context omits the
/// key rather than emitting it as null.
#[test]
fn an_operational_audit_record_without_context_omits_the_context_key() {
    let user_id =
        crate::service::authn::UserId::try_from("oidc~alice").expect("valid test user id");

    let record = emit_and_capture_one(|| async {
        audit_operation!(
            operation = "probe_operation",
            actor = AuditPrincipal(&user_id),
            outcome = "success",
            "probe"
        );
        Ok(())
    });

    assert_eq!(
        record.get("operation").and_then(serde_json::Value::as_str),
        Some("probe_operation"),
    );
    assert!(
        record.get("context").is_none(),
        "an operation emitted without context must omit the key entirely, not emit \
         null: {record}"
    );
}

/// Every committed fixture satisfies the format contract.
///
/// The fixture tests either side of this one compare emitted bytes against a committed
/// file, which detects drift but says nothing about whether the file describes a record
/// the emitter could actually produce — the fixture is generated by the test that
/// asserts against it, so a wrongly built event yields a fixture that agrees with it.
/// One did: a `CannotSeeResource` denial whose per-decision entry said `allowed: true`,
/// which passed every test until a human read the JSON.
///
/// These are the same rules the corpus test in `lakekeeper-integration-tests` applies to
/// records from real requests, shared rather than copied. Running them here costs
/// nothing and needs no database, so the cheap half of the check is always on.
#[test]
fn every_committed_fixture_satisfies_the_format_contract() {
    for name in FIXTURE_NAMES {
        super::contract::assert_satisfies(&read_fixture(name), &format!("fixture {name}"));
    }
}

/// A request that sent no `User-Agent` must be distinguishable from one
/// that sent a client named "unknown", so the field is null rather than a
/// sentinel.
#[test]
fn an_audit_event_without_a_user_agent_records_null() {
    let metadata = RequestMetadataTestBuilder::builder().build();

    let event = emit_and_capture_one(|| {
        AuditEventListener.authorization_succeeded(succeeded_event(metadata))
    });

    assert_eq!(
        event.get("user_agent"),
        Some(&serde_json::Value::Null),
        "the key must be present so consumers can tell 'not sent' from 'not recorded'"
    );
}

/// Records key/value pairs, flattening a nested map into `key=value` pairs joined
/// by `,` so a whole context can be asserted with one exact comparison.
#[derive(Default)]
struct EntryCollector {
    entries: Vec<(String, String)>,
}

impl Visit for EntryCollector {
    fn visit_value(&mut self, _value: Value<'_>) {}
    fn visit_entry(&mut self, key: Value<'_>, value: Value<'_>) {
        let Value::String(key) = key else { return };
        let rendered = match value {
            Value::String(s) => s.to_string(),
            Value::Mappable(m) => {
                let mut inner = EntryCollector::default();
                m.visit(&mut inner);
                inner
                    .entries
                    .iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect::<Vec<_>>()
                    .join(",")
            }
            other => format!("{other:?}"),
        };
        self.entries.push((key.to_string(), rendered));
    }
}

fn grant_context(
    principal: &UserOrRoleId,
    privilege: &str,
    resource: &GrantResource,
) -> Vec<(String, String)> {
    let mut collector = EntryCollector::default();
    GrantContextValue {
        principal,
        privilege,
        resource,
    }
    .visit(&mut collector);
    collector.entries
}

/// A revoked grant is hard-deleted, so this context is the only surviving record of
/// it — every part of the triple has to be present and correctly labelled.
#[test]
fn a_grant_context_carries_the_full_triple() {
    let warehouse_id = crate::service::WarehouseId::new_random();
    let table_id = crate::service::TableId::new_random();
    let principal = UserOrRoleId::User(
        crate::service::authn::UserId::try_from("oidc~alice").expect("valid test user id"),
    );

    let entries = grant_context(
        &principal,
        "select",
        &GrantResource::Table {
            warehouse_id,
            table_id,
        },
    );

    assert_eq!(
        entries,
        vec![
            ("principal".to_string(), "user=oidc~alice".to_string()),
            ("privilege".to_string(), "select".to_string()),
            ("resource_type".to_string(), "table".to_string()),
            ("resource_id".to_string(), table_id.to_string()),
            ("warehouse_id".to_string(), warehouse_id.to_string()),
        ]
    );
}

/// A server grant has no id and no warehouse: the resource type is its whole
/// identity. Those keys are omitted rather than emitted empty, so a consumer can
/// tell "server-wide" from "an id we failed to record".
#[test]
fn a_server_grant_context_omits_the_id_and_warehouse() {
    let principal = UserOrRoleId::Role(crate::service::RoleId::new_random());
    let entries = grant_context(&principal, "admin", &GrantResource::Server);

    let keys: Vec<&str> = entries.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(keys, vec!["principal", "privilege", "resource_type"]);
    assert_eq!(entries[2].1, "server");
    // A role principal is labelled as one, so it cannot be read as a user id.
    assert!(
        entries[0].1.starts_with("role="),
        "expected a role-labelled principal, got {}",
        entries[0].1
    );
}

/// Records the top-level map keys an `Authorization` emits when visited.
#[derive(Default)]
struct KeyCollector {
    keys: Vec<String>,
}

impl Visit for KeyCollector {
    fn visit_value(&mut self, _value: Value<'_>) {}
    fn visit_entry(&mut self, key: Value<'_>, _value: Value<'_>) {
        if let Value::String(k) = key {
            self.keys.push(k.to_string());
        }
    }
}

fn sample(determined_by: Vec<DeterminingFactor>) -> Authorization {
    Authorization {
        id: None,
        for_principal: None,
        action: ActionDescriptor {
            action_name: "read",
            context: Vec::new(),
        },
        entity: EntityDescriptor::new(EntityType::Table),
        allowed: Some(true),
        determined_by,
    }
}

#[test]
fn determined_by_emitted_when_present() {
    let auth = sample(vec![DeterminingFactor::Policy {
        policy_id: "policy0".to_string(),
        name: Some("allow-read".to_string()),
        effect: PolicyEffect::Permit,
        source: None,
    }]);
    let mut collector = KeyCollector::default();
    auth.visit(&mut collector);
    assert_eq!(
        collector.keys,
        vec!["action", "entity", "allowed", "determined_by"],
    );
    assert_eq!(auth.size_hint().0, collector.keys.len());
}

#[test]
fn determined_by_absent_when_empty() {
    let auth = sample(Vec::new());
    let mut collector = KeyCollector::default();
    auth.visit(&mut collector);
    assert_eq!(collector.keys, vec!["action", "entity", "allowed"]);
    assert_eq!(auth.size_hint().0, collector.keys.len());
}

/// Every rule in [`contract`] is only ever run against records that satisfy it: all nine
/// fixtures pass, and so does every record the corpus test captures. That verifies nothing
/// about the rules themselves — one could be deleted, or silently stop matching, and the
/// whole suite would stay green. The historical bug this module guards against is exactly
/// that shape: a rule that looked right and never fired.
///
/// Each case starts from a committed fixture and breaks one thing.
fn violations_after(
    fixture: &str,
    mutate: impl FnOnce(&mut serde_json::Map<String, serde_json::Value>),
) -> Vec<String> {
    let mut record = read_fixture(fixture);
    mutate(record.as_object_mut().expect("a fixture is a JSON object"));
    super::contract::violations(&record)
}

#[test]
fn contract_rejects_a_record_that_is_not_audit() {
    let found = violations_after("authz_succeeded_single", |r| {
        r.insert("event_source".into(), "app".into());
    });
    assert_eq!(found, vec!["`event_source` is not \"audit\""]);
}

#[test]
fn contract_rejects_a_record_with_no_version() {
    let found = violations_after("authz_succeeded_single", |r| {
        r.remove("audit_format");
    });
    assert_eq!(
        found,
        vec!["no `audit_format`: every audit record must declare its wire format version"]
    );
}

#[test]
fn contract_rejects_an_entity_key_outside_the_enum() {
    let found = violations_after("authz_succeeded_single", |r| {
        r["entity"]["not-a-field"] = "x".into();
    });
    assert_eq!(
        found,
        vec![
            "entity keys not in `EntityField`: [\"not-a-field\"]. Every key an entity can \
             carry must be a variant of that enum, so the key space stays enumerable and \
             documentable"
        ]
    );
}

/// Per-decision entries carry their own entity. The key check reads those too, so a bogus
/// key cannot hide one level down.
#[test]
fn contract_rejects_an_entity_key_inside_a_per_decision_entry() {
    let found = violations_after("authz_succeeded_single", |r| {
        r["authorizations"][0]["entity"]["not-a-field"] = "x".into();
    });
    assert_eq!(
        found,
        vec![
            "entity keys not in `EntityField`: [\"not-a-field\"]. Every key an entity can \
             carry must be a variant of that enum, so the key space stays enumerable and \
             documentable"
        ]
    );
}

#[test]
fn contract_rejects_an_action_context_key_outside_the_enum() {
    let found = violations_after("authz_succeeded_single", |r| {
        r["action"]["not-a-context-key"] = "x".into();
    });
    assert_eq!(
        found,
        vec![
            "action context keys not in `ActionContextKey`: [\"not-a-context-key\"]. Add a \
             variant rather than a bare literal, so the key is enumerable and the \
             documentation test sees it"
        ]
    );
}

#[test]
fn contract_rejects_an_unknown_entity_type() {
    let found = violations_after("authz_succeeded_single", |r| {
        r["entity"]["entity_type"] = "banana".into();
    });
    assert_eq!(
        found,
        vec!["`entity_type` is `banana`, not in `EntityType`"]
    );
}

/// `properties` is client input. A caller who names a table property `entity_type` is not
/// making a claim about the audit format, and must not fail the contract.
#[test]
fn contract_ignores_client_property_keys_that_collide_with_its_own() {
    let found = violations_after("authz_succeeded_single", |r| {
        r["action"]["properties"] = serde_json::json!({
            "entity_type": "banana",
            "not-a-field": "x",
        });
    });
    assert_eq!(found, Vec::<String>::new());
}

#[test]
fn contract_rejects_a_failure_reason_on_a_record_that_was_not_denied() {
    let found = violations_after("authz_failed_single", |r| {
        r.insert("decision".into(), "allowed".into());
    });
    assert_eq!(
        found,
        vec!["`failure_reason` is present but `decision` is not `denied`"]
    );
}

/// The definitive-denial rule reads the variant from the object's key, so a re-encoding
/// would retire it silently. It must trip instead.
#[test]
fn contract_rejects_a_re_encoded_failure_reason() {
    let found = violations_after("authz_failed_single", |r| {
        r.insert("failure_reason".into(), "ActionForbidden".into());
    });
    assert_eq!(
        found,
        vec![
            "`failure_reason` is `\"ActionForbidden\"`, not an object. The definitive-denial \
             rule reads the variant from this object's key, so a re-encoding disables it: \
             teach that rule the new encoding, then update this one"
        ]
    );
}

/// The rule that caught a real committed fixture: a denial the request was evaluated for
/// cannot carry a per-decision entry claiming it was allowed.
#[test]
fn contract_rejects_a_definitive_denial_that_claims_allowed() {
    let found = violations_after("authz_failed_single", |r| {
        r["authorizations"][0]["allowed"] = true.into();
    });
    assert_eq!(
        found,
        vec![
            "a definitive denial carries an `authorizations` entry with `allowed: true`. The \
             emitter cannot produce that, so either the record is wrong or this rule is"
        ]
    );
}
