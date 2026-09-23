use std::sync::{Arc, Mutex};

use assert_json_diff::{CompareMode, Config, assert_json_matches_no_panic};
use iceberg::{NamespaceIdent, TableIdent};

use super::{contract::contract_fields, *};
use crate::{
    WarehouseId,
    audit::AnyWireStr,
    request_metadata::{RequestMetadata, RequestMetadataTestBuilder, UserAgent},
    service::{
        admission::{
            AdmissionContext, AdmissionGate, AdmissionGates, AdmissionRejection, GateDecision,
        },
        authn::{Actor, UserId},
        authz::{
            ActionDescriptor, CatalogAction as _, CatalogNamespaceAction, CatalogTableAction,
            DeterminingFactor, GrantResource, PolicyEffect, UserOrRoleId,
        },
        events::{
            Authorization,
            context::{
                ActionContextKey, EntityDescriptor, EntityType, EventEntities,
                FIELD_NAME_NAMESPACE, FIELD_NAME_NAMESPACE_ID, FIELD_NAME_PROJECT_ID,
                FIELD_NAME_TABLE, FIELD_NAME_TABLE_ID, FIELD_NAME_WAREHOUSE_ID,
                UserProvidedEntity as _, UserProvidedTable,
            },
        },
        idempotency::IdempotencyKey,
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
        ActionDescriptor::builder()
            .action_name(AnyWireStr::literal_for_tests("read_data"))
            .build(),
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
// and at most one `extra_context` field is used per fixture: `extra_context` is a
// `HashMap`, so two or more entries render in an unstable order and the fixtures
// would fail at random.
//
// To regenerate after a deliberate change: `just update-audit-fixtures`.

const FIXTURE_WAREHOUSE_ID: &str = "019684ff-0000-7000-8000-000000000001";
const FIXTURE_TABLE_ID: &str = "019684ff-0000-7000-8000-000000000002";
const FIXTURE_NAMESPACE_ID: &str = "019684ff-0000-7000-8000-000000000003";
const FIXTURE_REQUEST_ID: &str = "019684ff-0000-7000-8000-000000000005";
const FIXTURE_ERROR_ID: &str = "019684ff-0000-7000-8000-000000000006";
const FIXTURE_ROLE_ID: &str = "019684ff-0000-7000-8000-000000000007";
const FIXTURE_IDEMPOTENCY_KEY: &str = "019684ff-0000-7000-8000-000000000004";

/// The fixture directory for the format the code emits right now, `fixtures/v{MAJOR}`,
/// derived from [`AUDIT_FORMAT`].
fn fixture_dir() -> std::path::PathBuf {
    let major = AUDIT_FORMAT
        .split('.')
        .next()
        .expect("AUDIT_FORMAT is MAJOR.MINOR, asserted at compile time");
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
        "src/service/events/backends/audit/fixtures/v{major}"
    ))
}

fn fixture_path(name: &str) -> std::path::PathBuf {
    fixture_dir().join(format!("{name}.json"))
}

/// Assert that `emitted` still matches the committed fixture, and classify any
/// difference as a major or a minor change to [`AUDIT_FORMAT`].
///
/// Read and written at runtime rather than embedded with `include_str!`, so the
/// same code path can also regenerate the file. A brand-new fixture would
/// otherwise fail to compile before it could be generated.
#[track_caller]
fn assert_matches_fixture(name: &str, emitted: &serde_json::Value) {
    // The whole record validates against the shape its `record_type` names, whatever the
    // fixture says: the fixture pins the sample, the schema pins the declaration.
    let schema = crate::audit::schema::audit_schema_for("lakekeeper");
    crate::audit::validate::assert_valid_record(&schema, emitted, name);
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
             If a `major` fragment just raised AUDIT_FORMAT to {AUDIT_FORMAT}, this is \
             expected and the fix is one command: the fixture directory is named for \
             the major version, and `just update-audit-fixtures` renames it to \
             `fixtures/v{}` and regenerates the contents. It moves the directory rather \
             than copying it — the old format is unreproducible once the code emits the \
             new one, so a directory left behind can never be regenerated or kept \
             passing, and `check-audit-format` rejects two directories anyway.\n\n\
             Otherwise: if this fixture is new, generate it with \
             `just update-audit-fixtures`. If it was moved or deleted, restore it — it \
             is the record of what audit_format {AUDIT_FORMAT} puts on the wire, and \
             without it nothing detects a change to the audit log format.",
            path.display(),
            AUDIT_FORMAT.split('.').next().unwrap_or("?"),
        )
    });
    let committed: serde_json::Value = serde_json::from_str(&committed)
        .unwrap_or_else(|e| panic!("fixture {} is not valid JSON: {e}", path.display()));

    // A fixture of `{}` satisfies the subset check below unconditionally, so an
    // emptied or truncated file would switch the breaking-change check off while
    // leaving a green test. Floor the field count.
    assert!(
        committed
            .as_object()
            .is_some_and(|object| object.len() >= 6),
        "fixture {} has fewer than 6 keys and looks truncated. Compared against a \
         near-empty fixture, the check below asserts almost nothing.",
        path.display()
    );

    // Is every field the fixture records still present, with the same type and
    // value? `CompareMode::Inclusive` walks the right-hand value and requires the
    // left to contain it, so with the fixture on the right this asserts
    // "fixture is a subset of emitted": extra fields in `emitted` pass.
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
             BREAKS CONSUMERS: record it with a `major` fragment under \
             audit-format/unreleased/ and run `just update-audit-fixtures`, which \
             computes AUDIT_FORMAT (now {AUDIT_FORMAT}) from the fragments. A changed \
             test INPUT does not: regenerate and write no fragment.\n\n\
             Decide which of the two this is. `just check-audit-format` cannot: it \
             compares shapes, so it reports the changed value and defers, and it \
             passes either way.\n\n\
             You are not asked to pick a version number, and a `major` fragment also \
             RENAMES the fixture directory, because it is named for the major version \
             it describes. `just update-audit-fixtures` does both. Do not keep the old \
             directory alongside the new one: a fixture is what the CURRENT code \
             emits, so once the code emits the new format the old one can never be \
             regenerated or kept passing. `check-audit-format` requires exactly one \
             directory and compares across the rename.\n\n\
             See the audit log section of docs/docs/developer-guide.md."
        );
    }

    // Reaching here means nothing recorded in the fixture moved, so the only way
    // to differ is a field present in `emitted` and absent from the fixture: a
    // purely additive change, which existing consumers can ignore.
    if let Err(difference) =
        assert_json_matches_no_panic(emitted, &committed, Config::new(CompareMode::Strict))
    {
        panic!(
            "the audit log format gained a field: additive, so existing consumers \
             keep working.\n\n{difference}\n\n\
             Record it with a `minor` fragment under audit-format/unreleased/, run \
             `just update-audit-fixtures` — which computes AUDIT_FORMAT (now \
             {AUDIT_FORMAT}) from the fragments — and document the field in \
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

    // Extra fields on the LEFT are allowed. This is the case the fixture check relies
    // on: `assert_json_matches!(&emitted, &fixture, Inclusive)` must tolerate an
    // emitted record that has gained a field.
    assert!(
        assert_json_matches_no_panic(&superset, &subset, inclusive()).is_ok(),
        "Inclusive must accept extra keys in the left-hand value. If this fails, the \
         crate has inverted the comparison and the fixture check now treats an added \
         field as a removed one."
    );

    // Extra fields on the RIGHT are a failure. This is what makes a removed field a
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
    ActionDescriptor::builder()
        .action_name(AnyWireStr::literal_for_tests("read_data"))
        .build()
}

/// An action carrying context, so the fixtures pin that nesting too.
fn fixture_action_with_context() -> ActionDescriptor {
    ActionDescriptor::builder()
        .action_name(
            CatalogNamespaceAction::UpdateProperties {
                removed_properties: Arc::new(Vec::new()),
                updated_properties: Arc::new(std::collections::BTreeMap::new()),
            }
            .as_wire(),
        )
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
        .action_name(AnyWireStr::literal_for_tests("create_table"))
        .context_string(ActionContextKey::Name, "orders")
        .context_string(ActionContextKey::TableId, FIXTURE_TABLE_ID)
        .build()
}

/// A drop action. `force` and `purge` are emitted only when the client asked for
/// them, so their presence here pins the "true" form and their absence elsewhere
/// pins the other.
fn fixture_drop_action() -> ActionDescriptor {
    ActionDescriptor::builder()
        .action_name(AnyWireStr::literal_for_tests("drop"))
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
/// `determined_by`. Pins which fields are omitted rather than emitted as null.
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

/// A fully-populated entry, so the fixtures pin the optional fields in their
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
        determined_by: vec![DeterminingFactor::Policy {
            policy_id: "policy-42".to_string(),
            name: Some("deny-stale-namespaces".to_string()),
            effect: PolicyEffect::Forbid,
            source: Some("cedar".to_string()),
        }],
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
    "authz_succeeded_idempotency_key",
    "idempotent_replay",
    "admission_forbidden",
    "admission_unavailable",
];

fn read_fixture(name: &str) -> serde_json::Value {
    let path = fixture_path(name);
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
    serde_json::from_str(&text)
        .unwrap_or_else(|e| panic!("fixture {} is not valid JSON: {e}", path.display()))
}

/// Every field the audit log puts on the wire must be documented, so the reference
/// in `docs/docs/logging.md` cannot quietly fall behind the code.
///
/// Driven off the committed fixtures, so it covers what is actually emitted rather
/// than what some type declares. Add a field and this fails, naming it.
///
/// Coverage is therefore bounded by the fixtures: a field emitted only by a code path
/// no fixture exercises is invisible here. Widening the fixture set widens this
/// check too, which is the main reason to add one.
///
/// Fields are matched as `` `name` `` — a field table entry or inline mention, not a
/// bare appearance inside a JSON example, since an example is not a description.
/// The consumer-facing audit log reference, embedded at COMPILE time: if
/// `logging.md` is deleted or moved, this line fails the build with "couldn't read
/// …: No such file or directory". It can never silently read an empty string. The
/// path is relative to this file, so it climbs from `backends/audit/` to the
/// repository root; `crate::api::endpoints` uses the same technique for the
/// committed `OpenAPI` specs.
const LOGGING_DOC: &str = include_str!("../../../../../../../docs/docs/logging.md");

/// Every complete audit record shown in `docs/docs/logging.md` declares the CURRENT
/// `AUDIT_FORMAT`.
#[test]
fn every_audit_record_example_in_the_docs_declares_the_current_format() {
    let expected = format!("\"audit_format\": \"{AUDIT_FORMAT}\"");
    let mut checked = 0;

    for block in LOGGING_DOC.split("```json").skip(1) {
        let Some(block) = block.split("```").next() else {
            continue;
        };
        // Complete records only. The page also shows field-level fragments — an `actor`
        // object, an `action` object — which are not records and must not grow a version.
        if !block.contains("\"event_source\": \"audit\"") {
            continue;
        }
        checked += 1;
        assert!(
            block.contains(&expected),
            "an audit record example in docs/docs/logging.md does not declare \
             {expected}. Every audit record carries the field, and the same page says so, \
             so an example without it teaches a consumer the wrong shape. If AUDIT_FORMAT \
             just changed, update the example records — nothing regenerates them.\n\n{block}"
        );
    }

    // A floor, for the same reason the fixture comparison has one: if the block detection
    // stops matching — the page switches to `json5` fences, say — every assertion above is
    // skipped and this test passes while checking nothing.
    assert!(
        checked >= 10,
        "expected at least 10 complete audit record examples in docs/docs/logging.md, \
         found {checked}. Either the examples were removed, or the ```json fence \
         detection above no longer matches them and this test is now asserting nothing."
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
/// fields. No `extra_context`, and an anonymous caller with no `User-Agent`, so
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
/// `actions` / `entities` fields. Also carries `extra_context`, an action with its
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

/// One action, several entities: the singular `action` field with the plural
/// `entities` field. This mixed arity is its own arm of `audit_log!`.
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
/// singular `entity` field.
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

/// An authorization carrying an `Idempotency-Key`.
///
/// The first call of an idempotent operation is authorized like any other and records the
/// key; only a repeat is served from the store and emits a replay record. Every other
/// authorization fixture is built from a request without one, so the field is seen as `null`
/// and nothing pins what a real key looks like next to a real `user_agent`.
#[test]
fn fixture_authz_succeeded_with_idempotency_key() {
    let mut request_metadata = fixture_metadata();
    request_metadata.with_idempotency_key(
        IdempotencyKey::parse(FIXTURE_IDEMPOTENCY_KEY).expect("valid test idempotency key"),
    );

    let record = emit_and_capture_one(|| {
        AuditEventListener.authorization_succeeded(AuthorizationSucceededEvent {
            request_metadata: Arc::new(request_metadata),
            entities: Arc::new(EventEntities::one(fixture_table_entity())),
            actions: Arc::new(vec![fixture_read_action()]),
            extra_context: fixture_context(&[]),
            authorizations: Arc::new(vec![fixture_plain_authorization()]),
        })
    });

    assert_matches_fixture("authz_succeeded_idempotency_key", &contract_fields(record));
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

/// The operational family, emitted through `OperationRecord` — a different shape
/// entirely, with `operation` / `outcome` / `context` and no `entity` or `decision`.
///
/// The replay family, which is neither authorization nor operational: it carries the
/// authorization family's `action` / `entity` / `privilege_source` and the operational
/// family's `operation` / `outcome`, and deliberately no `decision` — no authorization ran.
///
/// Pinned because a consumer that switched on the presence of `entity` to mean "this record
/// has a decision" is wrong about this family, and nothing else in the committed set shows
/// the combination.
#[test]
fn fixture_idempotent_replay() {
    let uuid = |s: &str| s.parse::<uuid::Uuid>().expect("fixed test uuid");
    let entities = UserProvidedTable {
        warehouse_id: crate::service::WarehouseId::new(uuid(FIXTURE_WAREHOUSE_ID)),
        table: TableIdent {
            namespace: NamespaceIdent::new("sales".to_string()),
            name: "orders".to_string(),
        }
        .into(),
    }
    .event_entities();

    let record = emit_and_capture_one(|| {
        AuditEventListener.idempotent_replay_served(IdempotentReplayEvent {
            request_metadata: Arc::new(fixture_metadata()),
            entities: Arc::new(entities),
            actions: Arc::new(vec![fixture_drop_action()]),
            idempotency_key: IdempotencyKey::parse(FIXTURE_IDEMPOTENCY_KEY)
                .expect("fixed test key"),
        })
    });

    assert_matches_fixture("idempotent_replay", &contract_fields(record));
}

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

/// Recursively collect every `.rs` file under `dir`.
fn rust_sources(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap_or_else(|e| panic!("reading {}: {e}", dir.display()))
    {
        let path = entry.expect("a readable directory entry").path();
        if path.is_dir() {
            rust_sources(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

/// Blank out whole-line comments, keeping every byte position so line numbers stay true.
///
/// Only a line that *starts* with `//` is blanked, never the tail of a line after one.
/// Blanking from the first `//` anywhere would also blank everything after a `//` inside a
/// string literal — a URL, a path, a regex — and hide a real assignment sitting after it on
/// the same line. Erring the other way costs at most a false positive on a trailing comment
/// that happens to spell a wire-value assignment, which fails loudly and is reworded in the
/// comment; a false negative in a backstop is silent, which is the failure this guard exists
/// to prevent.
///
/// Whole-line comments have to be skipped because the macro's doc comments show callers the
/// literal an external crate would pass, and emit nothing themselves.
fn code_only(text: &str) -> String {
    text.lines()
        .map(|line| {
            if line.trim_start().starts_with("//") {
                format!("{}\n", " ".repeat(line.len()))
            } else {
                format!("{line}\n")
            }
        })
        .collect()
}

/// The comment handling in [`code_only`] is the guard's only blind spot, so pin both
/// directions: a comment stays hidden, and code after a string containing `//` does not.
#[test]
fn code_only_hides_comments_without_hiding_code_after_a_slashed_string() {
    let hidden = |line: &str| !code_only(line).contains("operation = \"");
    assert!(
        hidden("// operation = \"x\","),
        "a whole-line comment must stay hidden"
    );
    assert!(
        hidden("    /// operation = \"x\","),
        "an indented doc comment too"
    );
    assert!(
        !hidden("let u = \"https://x.test\"; operation = \"x\","),
        "a `//` inside a string must not hide the assignment after it"
    );
    assert!(
        !hidden("error_type: \"a//b\", operation = \"x\","),
        "nor a `//` inside any other string"
    );
    assert_eq!(
        code_only("// hi\ncode\n").lines().count(),
        2,
        "line count and therefore line numbers must survive"
    );
}

/// A bare string becomes a wire value only inside the attribute's expansion.
///
/// Every value a record carries is a `WireStr`, and the only constructor is
/// `WireStr::new`. That constructor has to be public, because the attribute expands in the
/// crate that uses it and names the full path, and Rust has no way to offer a function to
/// one caller alone. So the type system closes every door but this one, and this test
/// watches it: a call anywhere else would put a string on the wire that no vocabulary enum
/// declares, so it would reach no schema and no rename check.
#[test]
fn only_the_attribute_turns_a_bare_string_into_a_wire_value() {
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    rust_sources(&src, &mut files);
    assert!(
        files.len() > 50,
        "only {} source files found under {}, so this test would pass by scanning nothing",
        files.len(),
        src.display()
    );

    let mut offenders = Vec::new();
    for file in files {
        let relative = file.strip_prefix(&src).unwrap_or(&file).to_path_buf();
        // `part.rs` declares the constructor; `tests.rs` may name any vocabulary, including
        // one this crate does not own.
        if matches!(
            relative.file_name().and_then(std::ffi::OsStr::to_str),
            Some("part.rs" | "tests.rs")
        ) {
            continue;
        }
        let text = std::fs::read_to_string(&file)
            .unwrap_or_else(|e| panic!("reading {}: {e}", file.display()));
        for (n, line) in code_only(&text).lines().enumerate() {
            if line.contains("WireStr::new") {
                offenders.push(format!("{}:{}: {}", relative.display(), n + 1, line.trim()));
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "a wire value is built from a bare string outside the attribute:\n  {}\n\n\
         Put `#[audit_part(field = \"...\")]` on a vocabulary enum and emit \
         `Variant::as_wire()`. A value that reaches the wire any other way is in no schema, \
         so renaming it later breaks every consumer while the format check reports nothing.\n\n\
         See the audit log section of docs/docs/developer-guide.md.",
        offenders.join("\n  ")
    );
}

/// A gate that rejects, so the admission path emits its record. Both kinds are
/// covered because they reach the wire differently: a denial names the rule that
/// decided it, a fail-closed one carries none and is the shape a consumer sees
/// during an upstream outage.
#[derive(Debug)]
struct FixtureGate {
    rejection: fn() -> AdmissionRejection,
}

#[async_trait::async_trait]
impl AdmissionGate for FixtureGate {
    fn name(&self) -> &'static str {
        "fixture_gate"
    }

    async fn admit(&self, _: AdmissionContext<'_>) -> Result<GateDecision, AdmissionRejection> {
        Err((self.rejection)())
    }
}

/// Request metadata with a pinned `request_id`, which the admission record
/// carries in its context and which a random id per run would make
/// uncomparable.
fn fixture_admission_metadata(actor: Actor) -> RequestMetadata {
    RequestMetadataTestBuilder::builder()
        .actor(actor)
        .request_id(FIXTURE_REQUEST_ID.parse().expect("fixed test uuid"))
        .build()
}

fn emit_admission_rejection(
    actor: Actor,
    rejection: fn() -> AdmissionRejection,
) -> serde_json::Value {
    let metadata = fixture_admission_metadata(actor);
    let records = emit_and_capture(|| async {
        AdmissionGates::new(vec![Arc::new(FixtureGate { rejection })])
            .admit(AdmissionContext::new(&metadata, None))
            .await
            .expect_err("the fixture gate rejects");
        Ok(())
    });
    // A fail-closed rejection also warns on the general stream, which is not an
    // audit record. Take the one record that is.
    let mut audit: Vec<serde_json::Value> = records
        .into_iter()
        .filter(|record| record["event_source"] == "audit")
        .collect();
    assert_eq!(
        audit.len(),
        1,
        "expected exactly one audit record from the admission path: {audit:#?}"
    );
    contract_fields(audit.pop().expect("length asserted above"))
}

/// An authoritative denial, for a caller acting through an assumed role.
///
/// The assumed-role actor is the point: admission is the only operational record
/// that renders the request's resolved actor (through
/// `RequestMetadata::audit_actor`) rather than the bare principal, so this is
/// the one fixture pinning the three-field actor shape on an operational
/// record. Every other operational fixture uses `AuditPrincipal` and cannot.
#[test]
fn fixture_admission_forbidden() {
    let user_id = UserId::try_from("oidc~alice").expect("valid test user id");
    // Deterministic from the id: the ident, and with it the `provider_id` and
    // `source_id` the record renders, are derived from it rather than generated.
    let assumed_role = Arc::new(crate::service::Role::new_random_with_id(
        crate::service::RoleId::new(FIXTURE_ROLE_ID.parse().expect("fixed test uuid")),
    ));
    let record = emit_admission_rejection(
        Actor::Role {
            principal: user_id,
            assumed_role,
        },
        || {
            AdmissionRejection::forbidden(
                "Principal is not admitted to this instance",
                "ExternalEnforceForbidden",
            )
            .denied_by("instance_access")
            .with_error_id(FIXTURE_ERROR_ID.parse().expect("fixed test uuid"))
        },
    );
    assert_matches_fixture("admission_forbidden", &record);
}

/// A gate failing closed. `denied_by` is absent — there was no rule, the gate
/// could not reach the upstream that has them — so this fixture is what pins
/// that the field is optional rather than always present.
#[test]
fn fixture_admission_unavailable() {
    let user_id = UserId::try_from("oidc~alice").expect("valid test user id");
    let record = emit_admission_rejection(Actor::Principal(user_id), || {
        AdmissionRejection::unavailable(
            "Permission service is unreachable",
            "ExternalEnforceUnavailable",
            std::time::Duration::from_secs(30),
            None,
        )
        .with_error_id(FIXTURE_ERROR_ID.parse().expect("fixed test uuid"))
    });
    assert_matches_fixture("admission_unavailable", &record);
}

/// The envelope fields are deliberately outside the format contract, so no fixture
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

/// The context-free form of an operation record.
///
/// Nothing in this repository emits an operation record without context, so without this
/// test the `None` path of `OperationRecord::emit` would have no coverage. Also pins that
/// omitting the context omits the field rather than emitting it as null.
#[test]
fn an_operational_audit_record_without_context_omits_the_context_key() {
    /// A test-only operation vocabulary; registered under `lakekeeper`, and kept out of the
    /// generated schema by its `tests` module path.
    #[crate::audit::audit_part(field = "operation")]
    #[audit(rename_all = "snake_case")]
    #[derive(Clone, Copy)]
    enum OperationProbe {
        /// The probe.
        ProbeOperation,
    }
    #[crate::audit::audit_part(field = "outcome")]
    #[audit(rename_all = "snake_case")]
    #[derive(Clone, Copy)]
    enum OutcomeProbe {
        /// Fine.
        Success,
    }

    let user_id =
        crate::service::authn::UserId::try_from("oidc~alice").expect("valid test user id");

    let record = emit_and_capture_one(|| async {
        crate::audit::OperationRecord::new(
            OperationProbe::ProbeOperation.as_wire(),
            crate::audit::ActorRecord::principal(&user_id),
            OutcomeProbe::Success.as_wire(),
        )
        .message("probe")
        .emit();
        Ok(())
    });

    assert_eq!(
        record.get("operation").and_then(serde_json::Value::as_str),
        Some("probe_operation"),
    );
    assert!(
        !record
            .as_object()
            .expect("a record is an object")
            .contains_key("context"),
        "an absent context must be absent, not null: {record}"
    );
    assert_eq!(record["actor"]["principal"], "oidc~alice");
    assert_eq!(record["outcome"], "success");
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
fn an_audit_event_without_a_user_agent_omits_the_key() {
    let metadata = RequestMetadataTestBuilder::builder().build();

    let event = emit_and_capture_one(|| {
        AuditEventListener.authorization_succeeded(succeeded_event(metadata))
    });

    assert_eq!(
        event.get("user_agent"),
        None,
        "a caller that sent no `User-Agent` leaves the key out; a `null` would claim the \
         header was seen and held nothing"
    );
}

/// The grant context as `(key, rendered value)` pairs in wire order, a nested object
/// flattened to `k=v` pairs joined by `,`, so a whole context can be asserted at once.
fn grant_context(
    principal: &UserOrRoleId,
    privilege: &str,
    resource: &GrantResource,
) -> Vec<(String, String)> {
    fn render(value: &serde_json::Value) -> String {
        match value {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Object(map) => map
                .iter()
                .map(|(k, v)| format!("{k}={}", render(v)))
                .collect::<Vec<_>>()
                .join(","),
            other => other.to_string(),
        }
    }
    let json = AuditJson::of(&GrantContextRecord::new(principal, privilege, resource));
    json.value()
        .as_object()
        .expect("a grant context is an object")
        .iter()
        .map(|(k, v)| (k.clone(), render(v)))
        .collect()
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
/// identity. Those fields are omitted rather than emitted empty, so a consumer can
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

/// The top-level keys a decision entry emits, in wire order.
fn decision_keys(authorization: &Authorization) -> Vec<String> {
    AuditJson::of(&assemble::decision(authorization))
        .value()
        .as_object()
        .expect("a decision entry is an object")
        .keys()
        .cloned()
        .collect()
}

fn sample(determined_by: Vec<DeterminingFactor>) -> Authorization {
    Authorization {
        id: None,
        for_principal: None,
        action: ActionDescriptor {
            action_name: AnyWireStr::literal_for_tests("read"),
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
    assert_eq!(
        decision_keys(&auth),
        vec!["action", "entity", "allowed", "determined_by"],
    );
}

#[test]
fn determined_by_absent_when_empty() {
    let auth = sample(Vec::new());
    assert_eq!(decision_keys(&auth), vec!["action", "entity", "allowed"]);
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
        r["entities"][0]["not-a-field"] = "x".into();
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

/// Per-decision entries carry their own entity. The field check reads those too, so a bogus
/// field cannot hide one level down.
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
        r["actions"][0]["not-a-context-key"] = "x".into();
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
        r["entities"][0]["entity_type"] = "banana".into();
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
        r["actions"][0]["properties"] = serde_json::json!({
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
        r.insert(
            "failure_reason".into(),
            serde_json::json!({ "ActionForbidden": [] }),
        );
    });
    assert_eq!(
        found,
        vec![
            "`failure_reason` is `{\"ActionForbidden\":[]}`, not a string. The \
             definitive-denial rule reads the variant from that string, so a re-encoding \
             disables it: teach that rule the new encoding, then update this one"
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

/// `strum` derives one name from a variant and the attribute derives another, while a
/// consumer reads only what `as_wire()` puts on the wire. Two derivations, one string: pin
/// them to each other, for a variant that carries data and for a unit variant.
#[test]
fn a_derived_action_name_is_the_name_that_reaches_the_wire() {
    use crate::service::authz::CatalogTableAction;

    let variants = <CatalogTableAction as strum::VariantNames>::VARIANTS;

    let carries_data = CatalogTableAction::Drop {
        force: true,
        purge: true,
    };
    let on_the_wire = carries_data.as_wire().text();
    assert_eq!(on_the_wire, "drop");
    assert!(
        variants.contains(&on_the_wire),
        "`CatalogTableAction::Drop` reaches the wire as `{on_the_wire}`, which is not among \
         the names {variants:?} `strum` derives. The two spellings have drifted \
         apart."
    );

    let unit = CatalogTableAction::ReadData;
    let on_the_wire = unit.as_wire().text();
    assert_eq!(on_the_wire, "read_data");
    assert!(
        variants.contains(&on_the_wire),
        "`CatalogTableAction::ReadData` reaches the wire as `{on_the_wire}`, which is not \
         among the names {variants:?} `strum` derives."
    );
}

/// Built from the production types rather than by hand: the record's claim is
/// that it matches what a real drop reports, which a hand-rolled descriptor
/// cannot demonstrate.
fn replay_event(warehouse_id: WarehouseId, actor: Actor) -> IdempotentReplayEvent {
    let request_metadata = RequestMetadataTestBuilder::builder()
        .actor(actor)
        .user_agent(UserAgent::parse("Apache-Spark/3.5.1"))
        .build();
    let entities = UserProvidedTable {
        warehouse_id,
        table: TableIdent {
            namespace: NamespaceIdent::new("sales".to_string()),
            name: "orders".to_string(),
        }
        .into(),
    }
    .event_entities();

    IdempotentReplayEvent {
        request_metadata: Arc::new(request_metadata),
        entities: Arc::new(entities),
        actions: Arc::new(vec![
            CatalogTableAction::Drop {
                force: true,
                purge: true,
            }
            .action_descriptor(),
        ]),
        idempotency_key: IdempotencyKey::parse("0198f2c0-0000-7000-8000-000000000001")
            .expect("a valid uuid"),
    }
}

/// A replay has to be attributable — who, which action with which flags, and
/// against which target — and it must not claim an authorization decision,
/// because none was made.
#[test]
fn a_replay_records_the_actor_action_and_target_but_no_decision() {
    let warehouse_id = WarehouseId::new_random();
    let event = replay_event(
        warehouse_id,
        Actor::Principal(UserId::try_from("oidc~alice").expect("a valid user id")),
    );

    let event = emit_and_capture_one(|| AuditEventListener.idempotent_replay_served(event));

    assert_eq!(
        event.get("record_type").and_then(serde_json::Value::as_str),
        Some("replay"),
        "a replay names its own shape; it does not borrow `operation` and `outcome` from the \
         operational family to be recognised"
    );
    assert_eq!(event.get("operation"), None);
    assert_eq!(event.get("outcome"), None);
    assert_eq!(
        event
            .get("idempotency_key")
            .and_then(serde_json::Value::as_str),
        Some("0198f2c0-0000-7000-8000-000000000001"),
        "the record that served the request has to be identifiable"
    );

    // Who. Without this the record says a drop was replayed but not by whom,
    // which is the question the event exists to answer.
    assert_eq!(
        event
            .pointer("/actor/principal")
            .and_then(serde_json::Value::as_str),
        Some("oidc~alice"),
    );
    assert_eq!(
        event
            .get("privilege_source")
            .and_then(serde_json::Value::as_str),
        Some("authorizer"),
    );
    assert_eq!(
        event.get("user_agent").and_then(serde_json::Value::as_str),
        Some("Apache-Spark/3.5.1"),
    );

    // What, including the flags: a purging force drop must not be recorded as
    // a plain one.
    assert_eq!(
        event
            .pointer("/actions/0/action_name")
            .and_then(serde_json::Value::as_str),
        Some("drop"),
    );
    assert_eq!(
        event
            .pointer("/actions/0/force")
            .and_then(serde_json::Value::as_str),
        Some("true"),
    );
    assert_eq!(
        event
            .pointer("/actions/0/purge")
            .and_then(serde_json::Value::as_str),
        Some("true"),
    );

    // Against what, as the caller named it.
    assert_eq!(
        event
            .pointer("/entities/0/entity_type")
            .and_then(serde_json::Value::as_str),
        Some("table"),
    );
    assert_eq!(
        event
            .pointer("/entities/0/warehouse-id")
            .and_then(serde_json::Value::as_str),
        Some(warehouse_id.to_string().as_str()),
    );
    assert_eq!(
        event
            .pointer("/entities/0/namespace")
            .and_then(serde_json::Value::as_str),
        Some("sales"),
    );
    assert_eq!(
        event
            .pointer("/entities/0/table")
            .and_then(serde_json::Value::as_str),
        Some("orders"),
        "the target is the name the caller sent, since a replay resolves nothing"
    );

    assert_eq!(
        event.get("decision"),
        None,
        "no authorization ran, so the record must not imply one"
    );
}

/// The key is on every audit record, not only the replay one: it is what ties
/// a retry to the request that did the work. Where an endpoint authorizes
/// before detecting the replay, the original carries the key too, so the pair
/// is the only sign of a retry — neither record marks itself as one.
#[test]
fn an_authorization_record_carries_the_idempotency_key() {
    let key = IdempotencyKey::parse("0198f2c0-0000-7000-8000-000000000002").expect("valid");
    let mut metadata = RequestMetadataTestBuilder::builder().build();
    metadata.with_idempotency_key(key);

    let event = emit_and_capture_one(|| {
        AuditEventListener.authorization_succeeded(succeeded_event(metadata))
    });

    assert_eq!(
        event
            .get("idempotency_key")
            .and_then(serde_json::Value::as_str),
        Some("0198f2c0-0000-7000-8000-000000000002"),
    );

    // A request without one leaves the key out, as for `user_agent`.
    let without = emit_and_capture_one(|| {
        AuditEventListener.authorization_succeeded(succeeded_event(
            RequestMetadataTestBuilder::builder().build(),
        ))
    });
    assert_eq!(without.get("idempotency_key"), None);
}

/// A caller claiming an emergency override has to be visible in the audit
/// log even when no authorizer acts on the claim — the built-in authorizers
/// ignore the header, so this event is the only record that it was sent.
#[test]
fn an_audit_event_records_the_break_glass_reason() {
    let mut metadata = RequestMetadataTestBuilder::builder().build();
    metadata.with_break_glass(Some("INC-1234 undoing lockout forbid".to_string()));

    let event = emit_and_capture_one(|| {
        AuditEventListener.authorization_succeeded(succeeded_event(metadata))
    });

    assert_eq!(
        event.get("break_glass").and_then(serde_json::Value::as_str),
        Some("INC-1234 undoing lockout forbid"),
    );
}

/// Nearly every request claims nothing, and an absent field says exactly what
/// a null would, so the field is omitted rather than padding every
/// authorization event in the catalog with `"break_glass": null`.
#[test]
fn an_audit_event_without_a_break_glass_claim_omits_the_field() {
    let metadata = RequestMetadataTestBuilder::builder().build();

    let event = emit_and_capture_one(|| {
        AuditEventListener.authorization_succeeded(succeeded_event(metadata))
    });

    assert_eq!(event.get("break_glass"), None);
}

// ── the registry ─────────────────────────────────────────────────────────────

/// Every Rust source file of this crate, for the source-scan rules below.
fn crate_sources() -> Vec<(std::path::PathBuf, String)> {
    fn walk(dir: &std::path::Path, out: &mut Vec<(std::path::PathBuf, String)>) {
        for entry in std::fs::read_dir(dir).expect("source dir") {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.push((
                    path.clone(),
                    std::fs::read_to_string(&path).expect("source"),
                ));
            }
        }
    }
    let mut out = Vec::new();
    walk(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut out,
    );
    out
}

/// `AuditPart` is implemented by `#[audit_part]` and nothing else: a hand-written impl would
/// put a type on the wire without registering it, so the schema would not know it.
#[test]
fn audit_part_is_only_implemented_through_the_attribute() {
    let offenders: Vec<String> = crate_sources()
        .into_iter()
        .filter(|(path, _)| !path.ends_with("part.rs") && !path.ends_with("tests.rs"))
        .filter(|(_, text)| code_only(text).contains("AuditPart for"))
        .map(|(path, _)| path.display().to_string())
        .collect();
    assert!(
        offenders.is_empty(),
        "hand-written `impl AuditPart for` found; use `#[audit_part]`: {offenders:?}"
    );
}

/// Only the shapes write an audit record. A `tracing::info!(event_source = "audit", …)`
/// anywhere else would bypass the envelope, the schema and every check.
#[test]
fn only_the_shapes_emit_audit_records() {
    let offenders: Vec<String> = crate_sources()
        .into_iter()
        .filter(|(path, _)| !path.ends_with("shapes.rs") && !path.ends_with("tests.rs"))
        .filter(|(_, text)| code_only(text).contains("event_source = \"audit\""))
        .map(|(path, _)| path.display().to_string())
        .collect();
    assert!(
        offenders.is_empty(),
        "`event_source = \"audit\"` outside the shapes: {offenders:?}"
    );
}

/// No key of a `context` map may spell a top-level field, whatever its separator: a consumer
/// flattening nested keys would see two fields of one name with unrelated meanings.
#[test]
fn no_context_key_spells_a_top_level_field() {
    use crate::audit::{Kind, Registration};
    Registration::require_registry();
    let normalise = |s: &str| s.replace('-', "_");
    let top_level: Vec<String> = super::shapes::TOP_LEVEL_FIELDS
        .iter()
        .map(|f| normalise(f))
        .collect();
    for reg in
        Registration::all().filter(|r| r.kind == Kind::Enum && r.wire_field == Some("context-key"))
    {
        for key in reg.wire_values {
            assert!(
                !top_level.contains(&normalise(key)),
                "context key `{key}` of {} spells the top-level field `{key}`",
                (reg.type_name)()
            );
        }
    }
}

/// The schema this crate's registry generates is a valid, self-contained document: every
/// `$ref` resolves, every part has a description on every property, and every registered
/// type appears under `$defs`.
#[test]
fn the_generated_schema_is_self_contained_and_documented() {
    use crate::audit::{
        Kind, Registration,
        schema::{audit_schema_for, short_type_name},
    };
    fn refs(v: &serde_json::Value, out: &mut Vec<String>) {
        match v {
            serde_json::Value::Object(m) => {
                if let Some(r) = m.get("$ref").and_then(serde_json::Value::as_str) {
                    out.push(r.to_string());
                }
                m.values().for_each(|v| refs(v, out));
            }
            serde_json::Value::Array(a) => a.iter().for_each(|v| refs(v, out)),
            _ => {}
        }
    }
    let schema = audit_schema_for("lakekeeper");
    let defs = schema["$defs"].as_object().expect("$defs");
    // every registration appears
    for reg in Registration::for_emitter::<crate::Lakekeeper>()
        .filter(|r| !(r.type_name)().contains("::tests::"))
    {
        let name = match reg.kind {
            Kind::Enum => short_type_name((reg.type_name)()),
            _ => (reg.schema_name.expect("part schema name"))().to_string(),
        };
        assert!(
            defs.contains_key(&name),
            "{name} is registered but not in $defs"
        );
    }
    // every $ref resolves
    let mut found = Vec::new();
    refs(&schema, &mut found);
    for r in found {
        let name = r
            .strip_prefix("#/$defs/")
            .unwrap_or_else(|| panic!("unexpected $ref {r}"));
        assert!(defs.contains_key(name), "$ref {r} does not resolve");
    }
    // every property of every part is described
    for (name, def) in defs {
        if let Some(props) = def["properties"].as_object() {
            for (prop, spec) in props {
                assert!(
                    spec.get("description").is_some() || spec.get("$ref").is_some(),
                    "{name}.{prop} has no description"
                );
            }
        }
    }
}

/// This crate's crate schema is what its registry generates. `just update-audit-schema` writes
/// it with `LAKEKEEPER_UPDATE_AUDIT_SCHEMA=1`; the integration tests merge every crate's crate
/// schema into the emitter's schema.
#[test]
fn the_committed_crate_schema_matches_the_registry() {
    crate::audit::schema::assert_crate_schema_committed(
        env!("CARGO_PKG_NAME"),
        env!("CARGO_MANIFEST_DIR"),
    );
}

/// The values this crate names are house style.
#[test]
fn the_wire_values_this_crate_names_are_house_style() {
    crate::audit::schema::assert_wire_values_are_house_style(env!("CARGO_PKG_NAME"));
}

/// The schema's description of a record's shape is what the emitter actually writes.
///
/// A shape is described by deriving from its struct, but a record reaches the wire as the log
/// event's own fields, written one by one by `emit()`. Those two could drift: a field added to
/// the struct and not to `emit()` would be promised and never sent, and one added to `emit()`
/// and not to the struct would be sent and never described. Every committed record is checked
/// against the shape its `record_type` names, which is what makes the derived description
/// worth trusting.
#[test]
fn the_shapes_and_the_record_type_vocabulary_declare_the_same_names() {
    let schema = crate::audit::schema::audit_schema_for("lakekeeper");
    let mut declared: Vec<String> = schema["$defs"]
        .as_object()
        .expect("$defs")
        .values()
        .filter_map(|d| d["x-audit-record-type"].as_str().map(str::to_owned))
        .collect();
    declared.sort();
    let mut vocabulary: Vec<String> = RecordType::WIRE_NAMES
        .iter()
        .map(|n| (*n).to_owned())
        .collect();
    vocabulary.sort();
    assert_eq!(
        declared, vocabulary,
        "each shape names the `record_type` it carries and the vocabulary lists them all; a \
         value in one and not the other means a consumer routes on something no shape \
         describes, or a shape nothing can reach"
    );
}

#[test]
fn every_record_matches_the_shape_its_type_names() {
    let schema = crate::audit::schema::audit_schema_for("lakekeeper");
    let defs = schema["$defs"].as_object().expect("$defs");
    let shapes: std::collections::BTreeMap<&str, &serde_json::Value> = defs
        .iter()
        .filter(|(_, d)| d["x-audit-kind"] == "shape")
        .map(|(name, d)| (name.as_str(), d))
        .collect();
    assert_eq!(
        shapes.len(),
        3,
        "one definition per shape: {:?}",
        shapes.keys()
    );

    for name in FIXTURE_NAMES {
        let record = read_fixture(name);
        let record_type = record["record_type"]
            .as_str()
            .unwrap_or_else(|| panic!("{name} carries no `record_type`"));
        let (_, shape) = shapes
            .iter()
            .find(|(shape_name, _)| {
                shape_name.to_lowercase() == format!("{}record", record_type.replace('_', ""))
            })
            .unwrap_or_else(|| panic!("{name} says `{record_type}`, which names no shape"));

        let properties = shape["properties"].as_object().expect("properties");
        let envelope = ["event_source", "audit_format"];
        for key in record.as_object().expect("an object").keys() {
            assert!(
                envelope.contains(&key.as_str()) || properties.contains_key(key),
                "{name} carries `{key}`, which `{record_type}` does not describe. Add it to \
                 the shape struct, or stop emitting it."
            );
        }
        for required in shape["required"].as_array().into_iter().flatten() {
            let required = required.as_str().expect("a property name");
            assert!(
                record.get(required).is_some(),
                "`{record_type}` promises `{required}` on every record, and {name} has none. \
                 Make the field optional, or emit it."
            );
        }
    }
}

/// Every complete audit record shown in `docs/docs/logging.md` validates against the schema.
///
/// The page teaches consumers what to expect, so an example that no longer matches what the
/// code emits teaches the wrong thing. Nothing regenerates these examples, which is exactly
/// why they need checking: a shape change leaves them behind in silence.
#[test]
fn every_audit_record_example_in_the_docs_validates() {
    let schema = crate::audit::schema::audit_schema_for("lakekeeper");
    let mut checked = 0;

    for block in LOGGING_DOC.split("```json").skip(1) {
        let Some(block) = block.split("```").next() else {
            continue;
        };
        if !block.contains("\"event_source\": \"audit\"") {
            continue;
        }
        checked += 1;
        let record: serde_json::Value = serde_json::from_str(block).unwrap_or_else(|e| {
            panic!(
                "an audit record example in docs/docs/logging.md is not valid JSON: {e}\n\n{block}"
            )
        });
        crate::audit::validate::assert_valid_record(
            &schema,
            &record,
            &format!("docs/docs/logging.md example {checked}"),
        );
    }

    assert!(
        checked >= 8,
        "only {checked} audit record examples found in docs/docs/logging.md; the page is \
         supposed to show one per family and several per authorization case, so this is \
         reading the wrong file or the wrong fences"
    );
}

/// The gate and the emission name one target.
///
/// `enabled()` asks the subscriber whether a record would be recorded, and the shapes write
/// the record. If those named different targets the answer would be about a target nothing
/// writes to: a filter that enables one would build records the other drops, and a filter
/// that disables it would skip records that would have been emitted.
#[test]
fn the_gate_and_the_emission_name_one_target() {
    let record = emit_and_capture_one(|| {
        AuditEventListener.authorization_succeeded(succeeded_event(fixture_metadata()))
    });
    assert_eq!(
        record.get("target").and_then(serde_json::Value::as_str),
        Some(crate::audit::AUDIT_TARGET),
        "a record must be written to the target `enabled()` asks about"
    );
    assert!(
        !crate::audit::AUDIT_TARGET.contains("backends"),
        "the target is a fixed name, not this module's path: `{}` moves when the code is \
         reorganised, and operator filters would move with it",
        crate::audit::AUDIT_TARGET
    );
}

/// A filter that used to select audit records is reported; one that still works is not.
#[test]
fn a_filter_naming_the_retired_target_is_reported() {
    use crate::service::events::backends::audit::part::retired_audit_directives as retired;

    // Directives that selected audit records by this crate's module path, and no longer do.
    for filter in [
        "lakekeeper::service::events::backends::audit=info",
        "info,lakekeeper::service::events=trace",
        "lakekeeper::service=debug,sqlx=warn",
        // The admission gate emitted from its own module, so its path is retired too.
        "lakekeeper::service::admission=info",
    ] {
        assert!(
            !retired(filter).is_empty(),
            "`{filter}` used to select audit records and no longer does, so an operator \
             needs telling"
        );
    }

    // Directives that still select them, or never claimed to.
    for filter in [
        "info",
        "lakekeeper=info",
        "lakekeeper::audit=info",
        "warn,lakekeeper::audit=info",
        "sqlx=warn,tower_http=debug",
        "",
    ] {
        assert_eq!(
            retired(filter),
            Vec::<&str>::new(),
            "`{filter}` still selects audit records, so warning about it would be noise"
        );
    }
}

/// An operation record's `context` is checked against this schema only when the record says
/// this emitter produced it.
///
/// A context belongs to whoever declared it. Checking another product's context against
/// Lakekeeper's declarations would report a violation of a contract the record never claimed,
/// and would in effect demand that every product declare its contexts here.
#[test]
fn a_context_is_checked_only_against_the_emitter_that_declared_it() {
    use crate::audit::validate::assert_record_parts_valid;

    let schema = crate::audit::schema::audit_schema_for("lakekeeper");
    let foreign = |name: serde_json::Value| {
        serde_json::json!({
            "record_type": "operation",
            "emitter": { "name": name, "format": "1.0" },
            "operation": "ldap_resolve_roles",
            "actor": { "actor_type": "principal", "principal": "oidc~alice" },
            "outcome": "success",
            "context": { "provider_id": "ldap", "role_count": 3 },
        })
    };

    // Another emitter's context: not ours to judge.
    assert_record_parts_valid(
        &schema,
        &foreign("lakekeeper-plus".into()),
        "another emitter",
    );

    // Ours, and the context matches none we declare: caught.
    let ours = foreign("lakekeeper".into());
    let checked = std::panic::catch_unwind(|| {
        assert_record_parts_valid(&schema, &ours, "this emitter");
    });
    assert!(
        checked.is_err(),
        "a context claiming to be ours and matching no declared context must be reported"
    );
}
