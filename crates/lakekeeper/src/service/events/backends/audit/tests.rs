use std::sync::{Arc, Mutex};

use assert_json_diff::{CompareMode, Config, assert_json_matches_no_panic};
use iceberg::{NamespaceIdent, TableIdent};

use super::*;
use crate::{
    api::management::v1::{
        check::UserOrRole as AuthzUserOrRole,
        grant::{ApplyGrants, ApplyGrantsRequest, RevokeSubtreeGrants, RevokeSubtreeGrantsRequest},
    },
    audit::validate::contract_fields,
    request_metadata::{RequestMetadata, RequestMetadataTestBuilder, UserAgent},
    service::{
        admission::{
            AdmissionContext, AdmissionGate, AdmissionGates, AdmissionRejection, AdmissionTrigger,
            GateDecision,
        },
        authn::{Actor, UserId},
        authz::{
            ActionDescriptor, CatalogNamespaceAction, CatalogProjectAction, CatalogTableAction,
            DeterminingFactor, EventAction as _, GrantResource, PolicyEffect, ResourceType,
            RoleSourceSystem, RootLevelGrants, SubtreeGrantPrincipal, SubtreeGrantPrivileges,
            SubtreeGrantScope, SubtreeResourceTypes, UserOrRoleId,
        },
        events::{
            Authorization,
            context::{
                APIEventActions as _, EntityDescriptor, EntityType, EventEntities,
                FIELD_NAME_NAMESPACE, FIELD_NAME_NAMESPACE_ID, FIELD_NAME_PROJECT_ID,
                FIELD_NAME_TABLE, FIELD_NAME_TABLE_ID, FIELD_NAME_WAREHOUSE_ID, HandlerContextKey,
                UserProvidedEntity as _, UserProvidedTable, synthesise_authorizations,
            },
        },
        idempotency::IdempotencyKey,
    },
};

/// Collects rendered log lines so a test can assert on the JSON a consumer
/// actually receives.
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
    // The binary's log format, without file and line numbers, which would leak into a
    // captured record.
    let subscriber = crate::audit::log_format(false)
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
    let actions = Arc::new(vec![CatalogTableAction::ReadData.action_descriptor()]);
    AuthorizationSucceededEvent {
        request_metadata: Arc::new(request_metadata),
        occurred_at: chrono::Utc::now(),
        entities,
        actions,
        extra_context: Arc::new(std::collections::BTreeMap::new()),
        authorizations: Arc::new(vec![sample(Vec::new())]),
    }
}

// ── Wire-format fixtures ────────────────────────────────────────────────────
//
// Each fixture is a committed record of exactly what one audit event renders to
// on the wire. They are the only check that observes the emitted JSON, so the
// only one that detects an unintended change to the audit format.
//
// Every value is fixed, so each run produces the same record.
//
// To regenerate after a deliberate change: `just update-audit-fixtures`.

const FIXTURE_WAREHOUSE_ID: &str = "019684ff-0000-7000-8000-000000000001";
const FIXTURE_TABLE_ID: &str = "019684ff-0000-7000-8000-000000000002";
const FIXTURE_NAMESPACE_ID: &str = "019684ff-0000-7000-8000-000000000003";
const FIXTURE_REQUEST_ID: &str = "019684ff-0000-7000-8000-000000000005";
const FIXTURE_ERROR_ID: &str = "019684ff-0000-7000-8000-000000000006";
const FIXTURE_ROLE_ID: &str = "019684ff-0000-7000-8000-000000000007";
const FIXTURE_IDEMPOTENCY_KEY: &str = "019684ff-0000-7000-8000-000000000004";
const FIXTURE_CREATED_BEFORE: &str = "2026-01-01T00:00:00Z";

/// The fixture directory: one, holding what the code emits now.
fn fixture_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/service/events/backends/audit/fixtures")
}

fn fixture_path(name: &str) -> std::path::PathBuf {
    fixture_dir().join(format!("{name}.json"))
}

/// Assert that `emitted` validates against the schema and matches the committed fixture,
/// or write the fixture when `LAKEKEEPER_UPDATE_AUDIT_FIXTURES` is set.
///
/// Read and written at runtime, not embedded with `include_str!`, so the same
/// code path can regenerate the file and a new fixture compiles before it exists.
#[track_caller]
fn assert_matches_fixture(name: &str, emitted: &serde_json::Value) {
    // The whole record validates against the shape its `record_type` names, whatever the
    // fixture says: the fixture pins the sample, the schema pins the declaration.
    let schema = crate::audit::schema::audit_schema_for("lakekeeper");
    crate::audit::validate::assert_valid_record(&schema, emitted, name);
    let mut emitted = emitted.clone();
    crate::audit::validate::pin_time(&mut emitted, name);
    let emitted = &emitted;
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
             If this fixture is new, generate it with `just update-audit-fixtures`. If it \
             was moved or deleted, restore it — it is the record of what audit_format \
             {AUDIT_FORMAT} puts on the wire, and without it nothing detects a change to \
             the audit log format.",
            path.display(),
        )
    });
    let committed: serde_json::Value = serde_json::from_str(&committed)
        .unwrap_or_else(|e| panic!("fixture {} is not valid JSON: {e}", path.display()));

    // A fixture of `{}` satisfies the subset check unconditionally, so an emptied
    // or truncated file would switch the breaking-change check off. Floor the
    // field count.
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
    // assert-json-diff's own documentation describes `Inclusive` the other way
    // round; its `diff.rs` implements the direction stated here, and
    // `inclusive_comparison_requires_the_right_hand_side_to_be_contained_in_the_left`
    // pins it.
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
             Decide which of the two this is. A renamed value of a registered set also \
             shows in the schema diff, which `just check-audit-format` reads. You are \
             not asked to pick a version number.\n\n\
             See the audit log section of docs/docs/developer-guide.md."
        );
    }

    // Nothing recorded in the fixture moved, so any remaining difference is a
    // field only `emitted` has: an additive change.
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

/// Pin the direction of [`CompareMode::Inclusive`], which [`assert_matches_fixture`]
/// depends on.
///
/// `assert-json-diff` is a caret dependency whose documentation describes
/// `Inclusive` the opposite way round from what it implements. A minor upgrade that
/// "fixed" the implementation would silently invert the fixture check: a deleted
/// field would be classified as an addition. The two assertions mirror each other.
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

    // Extra fields on the RIGHT are a failure, so a removed field is a breaking
    // change.
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

/// An action a table and a namespace can both carry, for the fixtures that pair one action
/// with entities of more than one kind.
fn fixture_metadata_action() -> ActionDescriptor {
    CatalogTableAction::GetMetadata.action_descriptor()
}

/// A namespace delete that asked for every override, built from the action's own descriptor.
fn fixture_namespace_delete_action() -> ActionDescriptor {
    CatalogNamespaceAction::Delete {
        force: true,
        purge: true,
        recursive: true,
    }
    .action_descriptor()
}

fn fixture_read_action() -> ActionDescriptor {
    CatalogTableAction::ReadData.action_descriptor()
}

/// An action carrying context, so the fixtures pin that nesting too. Both context
/// shapes at once: a list and a map beside the action name.
///
/// Built by the action's own `action_descriptor()`, so it matches the running code.
fn fixture_action_with_context() -> ActionDescriptor {
    CatalogNamespaceAction::UpdateProperties {
        removed_properties: Arc::new(vec!["stale.key".to_string()]),
        updated_properties: Arc::new(std::collections::BTreeMap::from([(
            "owner".to_string(),
            "analytics".to_string(),
        )])),
    }
    .action_descriptor()
}

/// A create action, carrying the client-requested name and id.
fn fixture_create_table_action() -> ActionDescriptor {
    CatalogNamespaceAction::CreateTable {
        name: Some("orders".to_string()),
        table_id: Some(crate::service::TableId::new(
            FIXTURE_TABLE_ID.parse().expect("fixed test uuid"),
        )),
        properties: Arc::new(std::collections::BTreeMap::new()),
    }
    .action_descriptor()
}

/// A drop that asked for both overrides, built from the action's own descriptor.
///
/// `force` and `purge` are always on the wire; this pins the `true` form and
/// `authz_succeeded_empty_collections` the `false` one.
fn fixture_drop_action() -> ActionDescriptor {
    CatalogTableAction::Drop {
        force: true,
        purge: true,
    }
    .action_descriptor()
}

/// A commit whose every collection is empty, built from the action's own descriptor.
///
/// A commit that changed no properties, targeted no refs and carried no update kinds still
/// names all four keys: "nothing" is an empty value, not a missing key.
fn fixture_empty_collections_action() -> ActionDescriptor {
    CatalogTableAction::Commit {
        updated_properties: Arc::new(std::collections::BTreeMap::new()),
        removed_properties: Arc::new(Vec::new()),
        target_refs: Arc::new(std::collections::BTreeSet::new()),
        update_kinds: Arc::new(std::collections::BTreeSet::new()),
    }
    .action_descriptor()
}

/// A grant apply, built by the handler's own `event_actions()`, so it matches the running
/// code.
///
/// Two principals of different kinds and two privileges across both lists, so the record
/// shows the `user:`/`role:` prefixes, and `writes` and `deletes` as counts.
fn fixture_apply_grants_action() -> ActionDescriptor {
    let request: ApplyGrantsRequest = serde_json::from_value(serde_json::json!({
        "writes": [
            {"privilege": "select", "principal": {"user": "oidc~alice"}},
            {"privilege": "describe", "principal": {"role": FIXTURE_ROLE_ID}},
        ],
        "deletes": [{"privilege": "select", "principal": {"role": FIXTURE_ROLE_ID}}],
    }))
    .expect("a valid apply request body");

    let mut actions = ApplyGrants::of(&request).event_actions();
    assert_eq!(actions.len(), 1, "an apply emits exactly one action");
    actions.remove(0)
}

/// A subtree revoke, built by the handler's own `event_actions()`, so it matches the
/// running code.
///
/// Narrowed on every axis a request can narrow: a named principal, two resource kinds, two
/// privileges, the root's own grants left out, partial removal allowed, and a cutoff. The
/// unnarrowed request carries the same keys with their widest values.
fn fixture_revoke_subtree_grants_action() -> ActionDescriptor {
    let scope: SubtreeGrantScope = serde_json::from_value(serde_json::json!({
        "resource_types": ["table", "view"],
        "root_level": "excluded",
        "privileges": {"only": {"names": ["describe", "select"]}},
        "principal": {"one": {"user": "oidc~alice"}},
        "dry_run": false,
    }))
    .expect("a valid subtree grant scope");
    let request: RevokeSubtreeGrantsRequest = serde_json::from_value(serde_json::json!({
        "principal": {"user": "oidc~alice"},
        "privilege": ["select", "describe"],
        "resource-type": ["table", "view"],
        "include-root-level": false,
        "allow-partial": true,
        "created-before": FIXTURE_CREATED_BEFORE,
    }))
    .expect("a valid revoke request body");

    let mut actions = RevokeSubtreeGrants::of(&request, &scope).event_actions();
    assert_eq!(actions.len(), 1, "a revoke emits exactly one action");
    actions.remove(0)
}

/// A warehouse entity carrying `project_id`, which real requests emit and the other
/// fixtures do not.
fn fixture_warehouse_entity() -> EntityDescriptor {
    EntityDescriptor::new(EntityType::Warehouse)
        .field(
            FIELD_NAME_PROJECT_ID,
            &"00000000-0000-0000-0000-000000000000",
        )
        .field(FIELD_NAME_WAREHOUSE_ID, &FIXTURE_WAREHOUSE_ID)
}

/// A succeeded event whose per-decision entries are the ones the handler synthesises for a
/// call site that supplies none: one per (entity, action) pair.
///
/// Synthesised, not written by hand, so a fixture's `authorizations[]` only names actions
/// and entities its own lists carry, as the emitter's records do.
fn fixture_succeeded_event(
    request_metadata: RequestMetadata,
    entities: EventEntities,
    actions: Vec<ActionDescriptor>,
    extra_context: Arc<
        std::collections::BTreeMap<&'static str, crate::service::events::context::ContextEntry>,
    >,
) -> AuthorizationSucceededEvent {
    let entities = Arc::new(entities);
    let actions = Arc::new(actions);
    let authorizations = Arc::new(synthesise_authorizations(
        &entities,
        &actions,
        None,
        Some(true),
    ));
    AuthorizationSucceededEvent {
        request_metadata: Arc::new(request_metadata),
        occurred_at: chrono::Utc::now(),
        entities,
        actions,
        extra_context,
        authorizations,
    }
}

/// The simplest per-decision entry: no id, no `for_principal`, no `determined_by`. Pins
/// which fields are omitted, not emitted as null.
///
/// Takes the pair it describes, so a fixture supplying its own list still draws them from
/// its own `actions` and `entities`. A definitive denial must carry `allowed: false`; the
/// emitter never produces a denied record with `true`.
fn fixture_decision(
    action: ActionDescriptor,
    entity: EntityDescriptor,
    allowed: bool,
) -> Authorization {
    Authorization {
        id: None,
        for_principal: None,
        action,
        entity,
        allowed: Some(allowed),
        determined_by: Vec::new(),
    }
}

/// A fully-populated entry, so the fixtures pin the optional fields in their present form as
/// well as their absent one, and `DeterminingFactor` including its own `None` fields.
///
/// This is the batch-check shape: a client that names its own checks gets an `id` back per
/// entry, and a policy authorizer reports what decided each one.
fn fixture_detailed_decision(action: ActionDescriptor, entity: EntityDescriptor) -> Authorization {
    Authorization {
        id: Some("check-0".to_string()),
        for_principal: Some(UserOrRoleId::User(
            crate::service::authn::UserId::try_from("oidc~bob").expect("valid test user id"),
        )),
        action,
        entity,
        allowed: Some(false),
        determined_by: vec![DeterminingFactor::Policy {
            policy_id: "policy-42".to_string(),
            name: Some("deny-stale-namespaces".to_string()),
            effect: PolicyEffect::Forbid,
            source: Some("cedar".to_string()),
        }],
    }
}

/// Context entries as a handler pushes them: each key holding its value, attributed to the
/// emitter that declares the key.
fn fixture_context(
    entries: &[HandlerContextKey],
) -> Arc<std::collections::BTreeMap<&'static str, crate::service::events::context::ContextEntry>> {
    Arc::new(
        entries
            .iter()
            .map(|key| {
                (
                    key.as_str(),
                    crate::service::events::context::ContextEntry::of(key),
                )
            })
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
        .request_id(
            FIXTURE_REQUEST_ID
                .parse::<uuid::Uuid>()
                .expect("fixed test uuid"),
        )
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

/// Every fixture, so the documentation and directory tests cover the whole
/// committed set, not whichever files happen to exist.
const FIXTURE_NAMES: &[&str] = &[
    "authz_succeeded_single",
    "authz_succeeded_break_glass",
    "authz_succeeded_plural",
    "authz_succeeded_action_entities",
    "authz_succeeded_actions_entity",
    "authz_failed_single",
    "authz_failed_context",
    "authz_failed_admission_gate",
    "authz_succeeded_rich_action_context",
    "authz_succeeded_create_role_source_system",
    "authz_succeeded_revoke_subtree_grants",
    "authz_succeeded_project_subtree_grants",
    "authz_succeeded_apply_grants",
    "authz_succeeded_empty_collections",
    "authz_succeeded_empty_batch_check",
    "grant_created",
    "grant_created_server",
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

/// The consumer-facing audit log reference, embedded at compile time: if
/// `logging.md` is deleted or moved, the build fails. The path climbs from
/// `backends/audit/` to the repository root.
const LOGGING_DOC: &str = include_str!("../../../../../../../docs/docs/logging.md");

/// Every key in a JSON tree, at any depth, as a flat list.
///
/// `opaque` names the keys whose value is a map the client supplied, such as table
/// properties. Those are recorded but not descended into: their keys are request data.
fn collect_keys(
    value: &serde_json::Value,
    opaque: &std::collections::BTreeSet<&str>,
    out: &mut Vec<String>,
) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, child) in map {
                out.push(key.clone());
                if !opaque.contains(key.as_str()) {
                    collect_keys(child, opaque, out);
                }
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                collect_keys(item, opaque, out);
            }
        }
        _ => {}
    }
}

/// The context keys that hold a client-supplied map, read from the registry, so a key
/// that becomes object-valued is covered at once.
fn client_supplied_map_keys() -> std::collections::BTreeSet<&'static str> {
    use crate::audit::{Kind, Registration};

    let mut keys = std::collections::BTreeSet::new();
    for reg in Registration::for_emitter::<crate::Lakekeeper>() {
        if let Kind::Keys { names, .. } = reg.kind {
            for name in names {
                let mut generator = schemars::SchemaGenerator::default();
                let holds = name.value.map(|schema| schema(&mut generator).to_value());
                if holds.is_some_and(|schema| schema["type"] == "object") {
                    keys.insert(name.text);
                }
            }
        }
    }
    keys
}

/// Every field the audit log puts on the wire is documented in
/// `docs/docs/logging.md`.
///
/// Driven off the committed fixtures, so it covers what is actually emitted. A field
/// emitted only by a code path no fixture exercises is invisible here; adding a
/// fixture widens this check.
///
/// Fields are matched as `` `name` ``: a field table entry or inline mention, not a
/// bare appearance inside a JSON example.
#[test]
fn every_emitted_audit_field_is_documented() {
    // The compile-time check on LOGGING_DOC covers a missing file. This covers a file
    // that exists but lacks the audit reference, which would otherwise fail once per
    // field.
    assert!(
        LOGGING_DOC.contains("{#audit-logs}"),
        "docs/docs/logging.md no longer contains the `{{#audit-logs}}` anchor. The \
         audit log documentation has moved, been split, or been deleted. This test \
         asserts that every field the audit log emits is documented there, so point \
         it at the new location and update the `#audit-logs` links in the other docs."
    );

    // `emitters` is keyed by product name; the docs list the products in their own table.
    let mut opaque = client_supplied_map_keys();
    opaque.insert("emitters");
    let mut keys = Vec::new();
    for name in FIXTURE_NAMES {
        collect_keys(&read_fixture(name), &opaque, &mut keys);
    }
    // Fixtures omit the subscriber-owned fields, but they are on the wire and
    // `logging.md` lists them; `ENVELOPE_KEYS` decides that list.
    keys.extend(
        crate::audit::validate::ENVELOPE_KEYS
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
         Add each one to the relevant field table.\n\n\
         Adding a field is a minor change to the audit format: see the audit log \
         section of docs/docs/developer-guide.md."
    );
}

/// The fixture directory and [`FIXTURE_NAMES`] agree: no orphan fixture, and no
/// fixture added by hand that nothing compares.
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

/// One action and one entity, each still in the `actions` / `entities` list. No
/// `extra_context`, and an anonymous caller with no `User-Agent`, so this fixture is the one
/// that pins which fields an unadorned request leaves out.
#[test]
fn fixture_authz_succeeded_single_action_single_entity() {
    let record = emit_and_capture_one(|| {
        AuditEventListener.authorization_succeeded(fixture_succeeded_event(
            RequestMetadataTestBuilder::builder()
                .request_id(
                    FIXTURE_REQUEST_ID
                        .parse::<uuid::Uuid>()
                        .expect("fixed test uuid"),
                )
                .build(),
            EventEntities::one(fixture_table_entity()),
            vec![fixture_read_action()],
            fixture_context(&[]),
        ))
    });

    assert_matches_fixture("authz_succeeded_single", &contract_fields(record));
}

/// A caller who claims break-glass: the record carries the reason they stated.
#[test]
fn fixture_authz_succeeded_break_glass() {
    let mut metadata = RequestMetadataTestBuilder::builder()
        .request_id(
            FIXTURE_REQUEST_ID
                .parse::<uuid::Uuid>()
                .expect("fixed test uuid"),
        )
        .build();
    metadata.with_break_glass(Some("INC-1234 undoing lockout forbid".to_string()));
    let record = emit_and_capture_one(|| {
        AuditEventListener.authorization_succeeded(fixture_succeeded_event(
            metadata,
            EventEntities::one(fixture_table_entity()),
            vec![fixture_read_action()],
            fixture_context(&[]),
        ))
    });

    assert_matches_fixture("authz_succeeded_break_glass", &contract_fields(record));
}

/// Several actions and several entities in the same lists the single-item case uses. Also
/// carries `extra_context`, an action with its own context, and a fully-populated
/// per-decision entry.
#[test]
fn fixture_authz_succeeded_plural_actions_plural_entities() {
    let record = emit_and_capture_one(|| {
        AuditEventListener.authorization_succeeded(AuthorizationSucceededEvent {
            request_metadata: Arc::new(fixture_metadata()),
            occurred_at: chrono::Utc::now(),
            entities: Arc::new(EventEntities::many([
                fixture_table_entity(),
                fixture_namespace_entity(),
            ])),
            actions: Arc::new(vec![fixture_read_action(), fixture_action_with_context()]),
            extra_context: fixture_context(&[HandlerContextKey::InvokedBy(
                crate::service::events::context::InvokingOperation::RegisterTableOverwrite
                    .as_wire(),
            )]),
            authorizations: Arc::new(vec![
                fixture_decision(fixture_read_action(), fixture_table_entity(), true),
                fixture_detailed_decision(
                    fixture_action_with_context(),
                    fixture_namespace_entity(),
                ),
            ]),
        })
    });

    assert_matches_fixture("authz_succeeded_plural", &contract_fields(record));
}

/// One action, several entities: the lengths of the two lists are independent.
#[test]
fn fixture_authz_succeeded_single_action_plural_entities() {
    let record = emit_and_capture_one(|| {
        AuditEventListener.authorization_succeeded(fixture_succeeded_event(
            fixture_metadata(),
            EventEntities::many([fixture_table_entity(), fixture_namespace_entity()]),
            vec![fixture_metadata_action()],
            fixture_context(&[]),
        ))
    });

    assert_matches_fixture("authz_succeeded_action_entities", &contract_fields(record));
}

/// Several actions, one entity: the mixed arity the other way round.
#[test]
fn fixture_authz_succeeded_plural_actions_single_entity() {
    let record = emit_and_capture_one(|| {
        AuditEventListener.authorization_succeeded(fixture_succeeded_event(
            fixture_metadata(),
            EventEntities::one(fixture_namespace_entity()),
            vec![fixture_metadata_action(), fixture_action_with_context()],
            fixture_context(&[]),
        ))
    });

    assert_matches_fixture("authz_succeeded_actions_entity", &contract_fields(record));
}

/// Action context that real traffic emits but the other fixtures do not: `name`,
/// `table_id`, `force`, `purge` and `recursive`, the last carried by no other fixture.
///
/// Both actions are a namespace's, so the synthesised decision per (action, entity) pair
/// describes a request the server can actually serve.
#[test]
fn fixture_authz_succeeded_rich_action_context() {
    let record = emit_and_capture_one(|| {
        AuditEventListener.authorization_succeeded(fixture_succeeded_event(
            fixture_metadata(),
            EventEntities::one(fixture_namespace_entity()),
            vec![
                fixture_create_table_action(),
                fixture_namespace_delete_action(),
            ],
            fixture_context(&[]),
        ))
    });

    assert_matches_fixture(
        "authz_succeeded_rich_action_context",
        &contract_fields(record),
    );
}

/// The action with the widest context Lakekeeper emits: nine keys, six of them the scope
/// the authorizer is asked with.
///
/// The only fixture with `root_level` and `privilege_scope`, and with all six scope keys
/// together, which a consumer needs to reconstruct the filter a revoke ran with.
#[test]
fn fixture_authz_succeeded_revoke_subtree_grants() {
    let record = emit_and_capture_one(|| {
        AuditEventListener.authorization_succeeded(fixture_succeeded_event(
            fixture_metadata(),
            EventEntities::one(fixture_warehouse_entity()),
            vec![fixture_revoke_subtree_grants_action()],
            fixture_context(&[]),
        ))
    });

    assert_matches_fixture(
        "authz_succeeded_revoke_subtree_grants",
        &contract_fields(record),
    );
}

/// A grant apply, the other management action whose context the handler assembles from the
/// request.
///
/// The only fixture with `writes`, `deletes` and `principals`, and the only one showing this
/// action to the guard that checks an action carries only what it declares.
#[test]
fn fixture_authz_succeeded_apply_grants() {
    let record = emit_and_capture_one(|| {
        AuditEventListener.authorization_succeeded(fixture_succeeded_event(
            fixture_metadata(),
            EventEntities::one(fixture_warehouse_entity()),
            vec![fixture_apply_grants_action()],
            fixture_context(&[]),
        ))
    });

    assert_matches_fixture("authz_succeeded_apply_grants", &contract_fields(record));
}

/// A request that asked for nothing: a commit that changed nothing, and a drop that forced
/// nothing.
///
/// Pins every form of "none": an empty `{}`, an empty `[]`, and a `false` flag. Each tells
/// "the request asked for none of this" apart from "this action has no such field".
#[test]
fn fixture_authz_succeeded_empty_collections() {
    let record = emit_and_capture_one(|| {
        AuditEventListener.authorization_succeeded(fixture_succeeded_event(
            fixture_metadata(),
            EventEntities::one(fixture_table_entity()),
            vec![
                fixture_empty_collections_action(),
                CatalogTableAction::Drop {
                    force: false,
                    purge: false,
                }
                .action_descriptor(),
            ],
            fixture_context(&[]),
        ))
    });

    assert_matches_fixture(
        "authz_succeeded_empty_collections",
        &contract_fields(record),
    );
}

/// A batch check with no checks: the call succeeded and checked nothing, so the record names
/// no entity and carries no per-decision entry. Built from the batch endpoint's own entity
/// and action types, which is what the handler hands to the event.
#[test]
fn fixture_authz_succeeded_empty_batch_check() {
    let checks: Vec<crate::api::management::v1::check::CatalogActionCheckItem> = Vec::new();
    let entities = (crate::service::ServerId::new_random(), checks).event_entities();
    let actions = crate::service::events::context::IntrospectPermissions {}.event_actions();
    let record = emit_and_capture_one(|| {
        AuditEventListener.authorization_succeeded(fixture_succeeded_event(
            fixture_metadata(),
            entities,
            actions,
            fixture_context(&[]),
        ))
    });

    assert_eq!(record["entities"], serde_json::json!([]));
    assert_eq!(record["authorizations"], serde_json::json!([]));
    assert_matches_fixture(
        "authz_succeeded_empty_batch_check",
        &contract_fields(record),
    );
}

/// An authorization carrying an `Idempotency-Key`.
///
/// The first call of an idempotent operation is authorized like any other and records the
/// key; only a repeat emits a replay record. The only authorization fixture with the key.
#[test]
fn fixture_authz_succeeded_with_idempotency_key() {
    let mut request_metadata = fixture_metadata();
    request_metadata.with_idempotency_key(
        IdempotencyKey::parse(FIXTURE_IDEMPOTENCY_KEY).expect("valid test idempotency key"),
    );

    let record = emit_and_capture_one(|| {
        AuditEventListener.authorization_succeeded(fixture_succeeded_event(
            request_metadata,
            EventEntities::one(fixture_table_entity()),
            vec![fixture_read_action()],
            fixture_context(&[]),
        ))
    });

    assert_matches_fixture("authz_succeeded_idempotency_key", &contract_fields(record));
}

/// A role create that names an external identity: `create_role` carries
/// `requested_provider_id` and `requested_source_id` next to `name`. Built from the
/// real action, so the fixture pins what `CatalogProjectAction::CreateRole` emits.
#[test]
fn fixture_authz_succeeded_create_role_source_system() {
    let action = CatalogProjectAction::CreateRole {
        name: Some("analysts".to_string()),
        source_system: Some(RoleSourceSystem {
            provider_id: "ldap".parse().expect("valid provider id"),
            source_id: "analysts".parse().expect("valid source id"),
        }),
    };
    let record = emit_and_capture_one(|| {
        AuditEventListener.authorization_succeeded(fixture_succeeded_event(
            fixture_metadata(),
            EventEntities::one(EntityDescriptor::new(EntityType::Project).field(
                FIELD_NAME_PROJECT_ID,
                &"00000000-0000-0000-0000-000000000000",
            )),
            vec![action.action_descriptor()],
            fixture_context(&[]),
        ))
    });

    assert_matches_fixture(
        "authz_succeeded_create_role_source_system",
        &contract_fields(record),
    );
}

/// `GET /management/v1/grants` about another principal: `read_subtree_grants` on the
/// project entity, with the six scope fields and `self-read: false`. Built from the real
/// action, with the scope that listing asks.
#[test]
fn fixture_authz_succeeded_project_subtree_grants() {
    let action = CatalogProjectAction::ReadSubtreeGrants {
        scope: Some(SubtreeGrantScope {
            resource_types: SubtreeResourceTypes::new(
                <ResourceType as strum::VariantArray>::VARIANTS
                    .iter()
                    .copied()
                    .filter(|kind| *kind != ResourceType::Server)
                    .collect(),
            )
            .expect("a project holds at least one resource kind"),
            root_level: RootLevelGrants::Included,
            privileges: SubtreeGrantPrivileges::Every {},
            principal: SubtreeGrantPrincipal::One(AuthzUserOrRole::User(UserId::new_unchecked(
                "oidc", "bob",
            ))),
            dry_run: false,
        }),
    };
    let record = emit_and_capture_one(|| {
        AuditEventListener.authorization_succeeded(fixture_succeeded_event(
            fixture_metadata(),
            EventEntities::one(EntityDescriptor::new(EntityType::Project).field(
                FIELD_NAME_PROJECT_ID,
                &"00000000-0000-0000-0000-000000000000",
            )),
            vec![action.action_descriptor()],
            fixture_context(&[HandlerContextKey::SelfRead(false)]),
        ))
    });

    assert_matches_fixture(
        "authz_succeeded_project_subtree_grants",
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
            occurred_at: chrono::Utc::now(),
            entities: Arc::new(EventEntities::one(fixture_table_entity())),
            actions: Arc::new(vec![fixture_read_action()]),
            failure_reason: crate::service::events::AuthorizationFailureReason::ActionForbidden,
            error: fixture_error(),
            extra_context: fixture_context(&[]),
            authorizations: Arc::new(vec![fixture_detailed_decision(
                fixture_read_action(),
                fixture_table_entity(),
            )]),
        })
    });

    assert_matches_fixture("authz_failed_single", &contract_fields(record));
}

/// A denied authorization that also carries `extra_context`.
#[test]
fn fixture_authz_failed_with_context() {
    let record = emit_and_capture_one(|| {
        AuditEventListener.authorization_failed(AuthorizationFailedEvent {
            request_metadata: Arc::new(fixture_metadata()),
            occurred_at: chrono::Utc::now(),
            entities: Arc::new(EventEntities::one(fixture_table_entity())),
            actions: Arc::new(vec![fixture_read_action()]),
            failure_reason: crate::service::events::AuthorizationFailureReason::CannotSeeResource,
            error: fixture_error(),
            extra_context: fixture_context(&[HandlerContextKey::SelfRead(false)]),
            authorizations: Arc::new(vec![fixture_decision(
                fixture_read_action(),
                fixture_table_entity(),
                false,
            )]),
        })
    });

    assert_matches_fixture("authz_failed_context", &contract_fields(record));
}

/// A check for another user whom an admission gate would refuse: the `AdmissionGate`
/// factor, once naming the refusing check and once without a `check`.
#[test]
fn fixture_authz_failed_admission_gate() {
    let for_bob = || {
        Some(UserOrRoleId::User(
            crate::service::authn::UserId::try_from("oidc~bob").expect("valid test user id"),
        ))
    };
    let record = emit_and_capture_one(|| {
        AuditEventListener.authorization_failed(AuthorizationFailedEvent {
            request_metadata: Arc::new(fixture_metadata()),
            occurred_at: chrono::Utc::now(),
            entities: Arc::new(EventEntities::many([
                fixture_table_entity(),
                fixture_namespace_entity(),
            ])),
            actions: Arc::new(vec![fixture_metadata_action()]),
            failure_reason: crate::service::events::AuthorizationFailureReason::ActionForbidden,
            error: fixture_error(),
            extra_context: fixture_context(&[]),
            authorizations: Arc::new(vec![
                Authorization {
                    id: Some("check-0".to_string()),
                    for_principal: for_bob(),
                    action: fixture_metadata_action(),
                    entity: fixture_table_entity(),
                    allowed: Some(false),
                    determined_by: vec![DeterminingFactor::AdmissionGate {
                        gate: "gate-a".to_string(),
                        check: Some("check-a".to_string()),
                    }],
                },
                Authorization {
                    id: Some("check-1".to_string()),
                    for_principal: for_bob(),
                    action: fixture_metadata_action(),
                    entity: fixture_namespace_entity(),
                    allowed: Some(false),
                    determined_by: vec![DeterminingFactor::AdmissionGate {
                        gate: "gate-a".to_string(),
                        check: None,
                    }],
                },
            ]),
        })
    });

    assert_matches_fixture("authz_failed_admission_gate", &contract_fields(record));
}

/// A replay record: the `actions`, `entities` and `privilege_source` of an authorization
/// record, and no `decision`, because no authorization ran. A consumer must not read
/// `entities` as "this record has a decision".
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
            occurred_at: chrono::Utc::now(),
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

/// A grant on the server, to a role: the grant context carries neither a resource id nor a
/// warehouse, and names the principal as a role.
#[test]
fn fixture_grant_created_server() {
    let principal = UserOrRoleId::Role(crate::service::RoleId::new(
        FIXTURE_ROLE_ID.parse().expect("fixed test uuid"),
    ));
    let records = emit_and_capture(|| {
        AuditEventListener.grants_changed(GrantsChangedEvent::new(
            vec![],
            vec![crate::service::authz::GrantSpec {
                principal,
                resource: GrantResource::Server,
                privilege: "admin".to_string(),
            }],
            Arc::new(fixture_metadata()),
        ))
    });
    let [created] = <[_; 1]>::try_from(records).expect("one record for one grant");

    assert_matches_fixture("grant_created_server", &contract_fields(created));
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
/// Only a line that *starts* with `//` is blanked. Blanking from any `//` would also hide
/// code after a `//` inside a string literal, such as a URL; a trailing comment that spells a
/// wire-value assignment fails loudly instead, and a reworded comment fixes it.
///
/// Whole-line comments are skipped because the macro's doc comments show the literal an
/// external crate would pass.
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
/// Every value a record carries is a `Wire`, built only by `Wire::new`. The constructor is
/// public because the attribute expands in the crate that uses it. A call anywhere else
/// would put a string on the wire that no vocabulary declares, outside the schema and the
/// rename check.
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
            // Both constructors: a key needs the schema as much as a value does. A vocabulary
            // registered by hand takes its names from `VariantNames`, not a bare string.
            let built = line.contains("Wire::new") || line.contains("WireKey::new");
            if built && !line.contains("VariantNames>::VARIANTS") {
                offenders.push(format!("{}:{}: {}", relative.display(), n + 1, line.trim()));
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "a wire name is built from a bare string outside the attribute:\n  {}\n\n\
         Put `#[audit_part(field = \"...\")]` on a value vocabulary, or \
         `#[audit_part(keys_of = \"...\")]` on a key vocabulary, and emit \
         `Variant::as_wire()`. A name that reaches the wire any other way is in no schema, \
         so renaming it later breaks every consumer while the format check reports nothing.\n\n\
         See the audit log section of docs/docs/developer-guide.md.",
        offenders.join("\n  ")
    );
}

/// A gate that rejects, so the admission path emits its record. A denial names the
/// rule that decided it; a fail-closed rejection, seen during an upstream outage,
/// names none.
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

/// Request metadata with a pinned `request_id`, so the admission record is
/// comparable across runs.
fn fixture_admission_metadata(actor: Actor) -> RequestMetadata {
    RequestMetadataTestBuilder::builder()
        .actor(actor)
        .request_id(
            FIXTURE_REQUEST_ID
                .parse::<uuid::Uuid>()
                .expect("fixed test uuid"),
        )
        .build()
}

fn emit_admission_rejection(
    actor: Actor,
    rejection: fn() -> AdmissionRejection,
) -> serde_json::Value {
    let metadata = fixture_admission_metadata(actor);
    let user_id = metadata.user_id().expect("the fixture actor is a user");
    let records = emit_and_capture(|| async {
        AdmissionGates::new(vec![Arc::new(FixtureGate { rejection })])
            .admit(AdmissionContext::new(
                user_id,
                AdmissionTrigger::from_request(&metadata),
            ))
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
/// Admission is the only operational record built from the request's resolved actor
/// (`ActorRecord::from_request`), so this is the one fixture pinning the three-field actor
/// shape on an operational record.
#[test]
fn fixture_admission_forbidden() {
    let user_id = UserId::try_from("oidc~alice").expect("valid test user id");
    // The ident, and with it `provider_id` and `source_id`, are derived from the id,
    // so the record is deterministic.
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

/// A gate failing closed. `denied_by` is absent: the gate could not reach the
/// upstream that holds the rules. Pins that the field is optional.
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

/// The envelope fields are outside the format contract, so no fixture records
/// them. Assert the ones a consumer relies on.
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

/// The capture helper renders what the binary renders; otherwise every fixture
/// would describe a shape production never emits.
///
/// `with_current_span(false)` is only observable while a span is active, as the
/// router's request span always is in production.
#[test]
fn the_capture_helper_omits_envelope_keys_production_omits() {
    use tracing::Instrument as _;

    let metadata = RequestMetadataTestBuilder::builder().build();
    let record = emit_and_capture_one(|| {
        // Built inside the closure, so the span is registered with the capture
        // subscriber; `Instrument` makes it current while the future is polled.
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
/// Nothing in this repository emits an operation record without context, so this covers
/// the `None` path of `OperationRecord::emit`, and pins that the field is omitted, not
/// written as null.
#[test]
fn an_operational_audit_record_without_context_omits_the_context_key() {
    /// A test-only operation vocabulary; registered under `lakekeeper`, and kept out of the
    /// generated schema by its `tests` module path.
    #[crate::audit::audit_part(field = "operation")]
    #[derive(Clone, Copy)]
    enum OperationProbe {
        /// The probe.
        ProbeOperation,
    }
    #[crate::audit::audit_part(field = "outcome")]
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
            crate::audit::RecordOrigin::without_request(crate::audit::ActorRecord::principal(
                &user_id,
            )),
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

/// The document validates a whole record from its root, so a consumer can point a stock
/// validator at the file without routing first.
#[test]
fn the_schema_root_validates_a_whole_record() {
    let schema = crate::audit::schema::audit_schema_for("lakekeeper");
    let validator = jsonschema::validator_for(&schema).expect("the schema is a valid schema");
    let record = read_fixture("authz_failed_single");
    assert!(
        validator.is_valid(&record),
        "a committed record validates from the root"
    );

    // The stamps are the root's own: a record without one is no audit record.
    let mut unstamped = record.clone();
    unstamped
        .as_object_mut()
        .expect("object")
        .remove("audit_format");
    assert!(!validator.is_valid(&unstamped));

    // The root routes to the shape, so a shape's own rules apply.
    let mut wrong = record.clone();
    wrong["decision"] = serde_json::json!(true);
    assert!(!validator.is_valid(&wrong));

    // An unknown record type still validates against the fields every record shares.
    let mut newer = record;
    newer["record_type"] = serde_json::json!("from_a_newer_release");
    assert!(validator.is_valid(&newer));
}

/// Every per-decision entry in a fixture names an action and an entity that the same record
/// lists at the top level.
///
/// The handler synthesises `authorizations[]` from those two lists, so a record naming
/// anything else is one it cannot produce.
#[test]
fn every_fixture_decides_only_on_what_it_lists() {
    let mut wrong = Vec::new();
    for name in FIXTURE_NAMES {
        let record = read_fixture(name);
        let Some(entries) = record
            .get("authorizations")
            .and_then(serde_json::Value::as_array)
        else {
            continue;
        };
        let listed = |field: &str| -> Vec<serde_json::Value> {
            record
                .get(field)
                .and_then(serde_json::Value::as_array)
                .cloned()
                .unwrap_or_default()
        };
        let (actions, entities) = (listed("actions"), listed("entities"));
        for (index, entry) in entries.iter().enumerate() {
            for (field, list, listed) in [
                ("action", "actions", &actions),
                ("entity", "entities", &entities),
            ] {
                let Some(value) = entry.get(field) else {
                    continue;
                };
                if !listed.contains(value) {
                    wrong.push(format!(
                        "fixture {name}: authorizations[{index}].{field} is {value}, which is \
                         not in the record's `{list}`"
                    ));
                }
            }
        }
    }
    assert!(
        wrong.is_empty(),
        "these fixtures decide on something they do not list:\n  {}\n\n\
         Build the entries with `fixture_succeeded_event`, which pairs them the way the \
         handler does, or pass the fixture's own action and entity to `fixture_decision` / \
         `fixture_detailed_decision`.",
        wrong.join("\n  ")
    );
}

fn sample(determined_by: Vec<DeterminingFactor>) -> Authorization {
    Authorization {
        id: None,
        for_principal: None,
        action: CatalogTableAction::ReadData.action_descriptor(),
        entity: EntityDescriptor::new(EntityType::Table),
        allowed: Some(true),
        determined_by,
    }
}

/// `strum` and the attribute each derive a name from a variant; a consumer sees only
/// `as_wire()`. Pin the two to each other, for a data-carrying and a unit variant.
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

// ── the registry ─────────────────────────────────────────────────────────────

/// Every Rust source file of this crate, for the source-scan rules.
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
        // `tests.rs` names the literal in its own assertions. The shapes themselves hold no
        // such line: the attribute writes their `emit()`.
        .filter(|(path, _)| !path.ends_with("tests.rs"))
        .filter(|(_, text)| code_only(text).contains("event_source = \"audit\""))
        .map(|(path, _)| path.display().to_string())
        .collect();
    assert!(
        offenders.is_empty(),
        "`event_source = \"audit\"` outside the shapes: {offenders:?}"
    );
}

/// One key of an object means one thing, whoever declared it.
///
/// The same name at two paths, such as `actor.principal` and `context.principal`, is no
/// clash; two vocabularies declaring the same key of one object are.
#[test]
fn no_object_declares_a_key_twice() {
    crate::audit::schema::assert_no_object_declares_a_key_twice();
}

/// No action in a fixture carries a key its variant did not declare.
///
/// The published schema says which keys an action carries. Keys read from field names
/// cannot drift; keys declared with `expands_to` can, when the field's type gains a key. This
/// checks records the emitting code actually wrote, so it covers only actions some fixture
/// exercises.
#[test]
fn no_fixture_action_carries_an_undeclared_key() {
    use crate::audit::{Kind, Registration};

    let mut declared: std::collections::BTreeMap<&str, std::collections::BTreeSet<&str>> =
        std::collections::BTreeMap::new();
    for reg in Registration::for_emitter::<crate::Lakekeeper>() {
        if let Kind::Values {
            field: "action_name",
            names,
            ..
        } = reg.kind
        {
            for name in names {
                declared
                    .entry(name.text)
                    .or_default()
                    .extend(name.carries.iter().copied());
            }
        }
    }
    assert!(
        declared.len() > 20,
        "only {} action names in the registry, so this is checking almost nothing",
        declared.len()
    );

    let mut checked = 0usize;
    let mut undeclared = Vec::new();
    for fixture in FIXTURE_NAMES {
        let record = read_fixture(fixture);
        let mut actions: Vec<&serde_json::Value> = record["actions"]
            .as_array()
            .map(|a| a.iter().collect())
            .unwrap_or_default();
        if let Some(entries) = record["authorizations"].as_array() {
            actions.extend(entries.iter().map(|entry| &entry["action"]));
        }
        for action in actions {
            let Some(object) = action.as_object() else {
                continue;
            };
            let Some(name) = object
                .get("action_name")
                .and_then(serde_json::Value::as_str)
            else {
                continue;
            };
            let Some(keys) = declared.get(name) else {
                continue; // an action of another emitter
            };
            checked += 1;
            for key in object.keys().filter(|k| k.as_str() != "action_name") {
                if !keys.contains(key.as_str()) {
                    undeclared.push(format!("{fixture}: `{name}` carries undeclared `{key}`"));
                }
            }
        }
    }
    assert!(
        checked > 0,
        "no fixture action matched a registered action name"
    );
    assert!(
        undeclared.is_empty(),
        "these fixtures carry a key the action does not declare:\n  {}\n\n\
         The schema tells a consumer which keys an action carries, so one that reaches a \
         record without being declared is invisible to them. Add it to the variant's fields, \
         or to its `expands_to` where its type chooses the keys.",
        undeclared.join("\n  ")
    );
}

/// Every key an action says it carries is a key the action object declares.
#[test]
fn every_carried_key_is_a_declared_key() {
    crate::audit::schema::assert_carried_keys_are_declared_keys::<crate::Lakekeeper>();
}

/// Every `context` key this crate declares reaches a `push_extra_context` call.
///
/// The schema check catches a renamed or removed key, not a deleted push site: the key
/// leaves the wire while its declaration stays.
#[test]
fn every_declared_context_key_is_pushed() {
    crate::audit::schema::assert_every_context_key_is_pushed::<crate::Lakekeeper>(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(".."),
    );
}

/// The schema this crate's registry generates is a valid, self-contained document: every
/// `$ref` resolves, every part has a description on every property, and every registered
/// type appears under `$defs`.
#[test]
fn the_generated_schema_is_self_contained_and_documented() {
    use crate::audit::{Kind, Registration, schema::audit_schema_for};
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
    fn properties_described(name: &str, def: &serde_json::Value) {
        if let Some(props) = def["properties"].as_object() {
            for (prop, spec) in props {
                // A branch's `if` names the value it matches; it describes nothing.
                if spec.get("const").is_some() && spec.as_object().is_some_and(|o| o.len() == 1) {
                    continue;
                }
                assert!(
                    spec.get("description").is_some(),
                    "{name}.{prop} has no description"
                );
            }
        }
        for branch in def["allOf"].as_array().into_iter().flatten() {
            properties_described(name, &branch["then"]);
        }
    }
    let schema = audit_schema_for("lakekeeper");
    let defs = schema["$defs"].as_object().expect("$defs");
    // every registration appears, except a key set: its keys are properties of their object
    for reg in Registration::for_emitter::<crate::Lakekeeper>()
        .filter(|r| !(r.type_name)().contains("::tests::"))
    {
        let name = match reg.kind {
            Kind::Keys { .. } => continue,
            _ => (reg.def_name)().to_string(),
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
    // every property is described, including those an `if`/`then` branch adds
    for (name, def) in defs {
        properties_described(name, def);
    }
    // a value's description belongs to a value the set actually has
    for (name, def) in defs {
        let Some(descriptions) = def["x-audit-descriptions"].as_object() else {
            continue;
        };
        let names: Vec<&str> = def
            .get("enum")
            .or_else(|| def.get("x-audit-values"))
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(serde_json::Value::as_str)
            .collect();
        for described in descriptions.keys() {
            assert!(
                names.contains(&described.as_str()),
                "{name} describes `{described}`, which is not one of its names: {names:?}"
            );
        }
    }
    crate::audit::schema::assert_descriptions_are_prose(&schema);
}

/// The schema's description of a record's shape is what the emitter actually writes.
///
/// A shape's schema is derived from its struct, but `emit()` writes the record field by
/// field, and the two could drift. Every committed record is checked against the shape its
/// `record_type` names.
#[test]
fn the_shapes_and_the_record_type_vocabulary_declare_the_same_names() {
    let schema = crate::audit::schema::audit_schema_for("lakekeeper");
    let mut declared: Vec<String> = schema["$defs"]
        .as_object()
        .expect("$defs")
        .values()
        .filter_map(|d| {
            d["properties"]["record_type"]["const"]
                .as_str()
                .map(str::to_owned)
        })
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

/// A field whose values are a closed set points at the set; one that is open does not.
///
/// Without the link the schema would list the two decisions and still accept
/// `"decision": "banana"`.
///
/// `action_name`, `operation` and `outcome` stay open: their values come from whichever
/// emitter produced the record, so Lakekeeper's vocabulary would reject valid records.
#[test]
fn a_closed_value_set_is_linked_and_an_open_one_is_not() {
    use crate::audit::{schema::audit_schema_for, validate::is_valid_part};
    let schema = audit_schema_for("lakekeeper");
    let property = |def: &str, name: &str| schema["$defs"][def]["properties"][name].clone();

    for (def, name, owner) in [
        ("AuthorizationRecord", "decision", "Decision"),
        ("AuthorizationRecord", "privilege_source", "PrivilegeSource"),
        (
            "AuthorizationRecord",
            "failure_reason",
            "AuthorizationFailureReason",
        ),
        ("ActorRecord", "actor_type", "ActorType"),
        ("EntityRecord", "entity_type", "EntityType"),
        ("GrantContextRecord", "resource_type", "ResourceType"),
    ] {
        assert_eq!(
            property(def, name)["$ref"],
            format!("#/$defs/{owner}"),
            "{def}.{name} must point at the set of values it can hold"
        );
    }

    for (def, name) in [
        ("ActionRecord", "action_name"),
        ("OperationRecord", "operation"),
        ("OperationRecord", "outcome"),
    ] {
        assert_eq!(
            property(def, name)["x-audit-open"],
            serde_json::json!(true),
            "{def}.{name} is filled by any emitter, so it must stay a plain string"
        );
    }

    // Each shape pins its one `record_type`, so a record checked against the wrong shape
    // is rejected.
    for (def, record_type) in [
        ("AuthorizationRecord", "authorization"),
        ("ReplayRecord", "replay"),
        ("OperationRecord", "operation"),
    ] {
        assert_eq!(property(def, "record_type")["const"], record_type);
    }

    // The same record passes unmodified and fails on a value the vocabulary does not list.
    let mut body = contract_fields(read_fixture("authz_succeeded_single"));
    if let Some(object) = body.as_object_mut() {
        object.remove("event_source");
        object.remove("audit_format");
    }
    assert!(
        is_valid_part(&schema, "AuthorizationRecord", &body),
        "the fixture must satisfy its own shape: {body}"
    );
    body["decision"] = "banana".into();
    assert!(
        !is_valid_part(&schema, "AuthorizationRecord", &body),
        "`decision` accepted a value outside its vocabulary, so the schema checks nothing"
    );
}

/// An operation record from another emitter still satisfies the shape.
///
/// Lakekeeper owns the three shapes; other products fill them with their own vocabulary, so
/// `operation` and `outcome` must not point at Lakekeeper's value sets.
#[test]
fn an_operation_record_from_another_emitter_satisfies_the_shape() {
    use crate::audit::{schema::audit_schema_for, validate::is_valid_part};
    let schema = audit_schema_for("lakekeeper");

    let mut body = contract_fields(read_fixture("grant_created"));
    if let Some(object) = body.as_object_mut() {
        object.remove("event_source");
        object.remove("audit_format");
    }
    assert!(
        is_valid_part(&schema, "OperationRecord", &body),
        "lakekeeper's own operation record must satisfy the shape: {body}"
    );

    body["emitters"] = serde_json::json!({ "lakekeeper_plus": "1.0" });
    body["operation"] = "license_checked".into();
    body["outcome"] = "expired".into();
    assert!(
        is_valid_part(&schema, "OperationRecord", &body),
        "another emitter's operation and outcome must not be measured against lakekeeper's \
         vocabulary: {body}"
    );

    // The shape a record names is still checked, whoever emitted it.
    body["record_type"] = "authorization".into();
    assert!(
        !is_valid_part(&schema, "OperationRecord", &body),
        "`record_type` must pin the shape, so a record naming another one is rejected"
    );
}

/// Every complete audit record shown in `docs/docs/logging.md` validates against the schema.
///
/// Nothing regenerates these examples, so a shape change would leave them silently wrong.
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
        // The schema takes any `MAJOR.MINOR`, so the version is checked here: nothing
        // regenerates the examples when `AUDIT_FORMAT` moves.
        assert_eq!(
            record["audit_format"], AUDIT_FORMAT,
            "an audit record example in docs/docs/logging.md declares another format:\n\n{block}"
        );
        // An example that shows the log line's envelope shows the target a `RUST_LOG` filter
        // selects audit records by.
        if record.get("message").is_some() {
            assert_eq!(
                record["target"],
                crate::audit::AUDIT_TARGET,
                "an audit record example in docs/docs/logging.md does not show the audit \
                 target:\n\n{block}"
            );
        }
        crate::audit::validate::assert_valid_record(
            &schema,
            &record,
            &format!("docs/docs/logging.md example {checked}"),
        );
    }

    // A floor: if block detection stops matching (say the page switches to `json5`
    // fences), the test would otherwise pass while checking nothing.
    assert!(
        checked >= 10,
        "only {checked} audit record examples found in docs/docs/logging.md; the page is \
         supposed to show one per family and several per authorization case, so this is \
         reading the wrong file or the wrong fences"
    );
}

/// The gate and the emission name one target.
///
/// `enabled()` asks the subscriber about a target, and the shapes write to one. If the two
/// differed, the gate would answer for a target nothing writes to.
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

/// A filter that selects no audit record is reported; one that selects them is not.
#[test]
fn a_filter_naming_the_retired_target_is_reported() {
    use crate::service::events::backends::audit::part::retired_audit_directives as retired;

    // Directives naming a module path, which matches no audit record.
    for filter in [
        "lakekeeper::service::events::backends::audit=info",
        "info,lakekeeper::service::events=trace",
        "lakekeeper::service=debug,sqlx=warn",
        // The admission gate's own module path, which an operator may equally have named.
        "lakekeeper::service::admission=info",
    ] {
        assert!(
            !retired(filter).is_empty(),
            "`{filter}` selects no audit record, so an operator who wrote it needs telling"
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
/// A context belongs to the emitter that declared it; another product's contexts are not in
/// Lakekeeper's schema.
#[test]
fn a_context_is_checked_only_against_the_emitter_that_declared_it() {
    use crate::audit::validate::assert_context_valid;

    let schema = crate::audit::schema::audit_schema_for("lakekeeper");
    let foreign = |name: &str| {
        serde_json::json!({
            "record_type": "operation",
            "emitters": { name: "1.0" },
            "operation": "ldap_resolve_roles",
            "actor": { "actor_type": "principal", "principal": "oidc~alice" },
            "outcome": "success",
            "context": { "provider_id": "ldap", "role_count": 3 },
        })
    };

    // Another emitter's context: not ours to judge.
    assert_context_valid(&schema, &foreign("lakekeeper_plus"), "another emitter");

    // Ours, and the context matches none we declare: caught.
    let ours = foreign("lakekeeper");
    let checked = std::panic::catch_unwind(|| {
        assert_context_valid(&schema, &ours, "this emitter");
    });
    assert!(
        checked.is_err(),
        "a context claiming to be ours and matching no declared context must be reported"
    );
}
