//! Audit records with role source ids switched off: every role is named by its id and
//! provider, without its source id, on decision subjects, grant recipients, grant records
//! and the assumed-role actor.
//!
//! Source ids are switched off for the whole process with `omit_role_source_id_in_tests`,
//! so these tests live in a binary of their own. Capture is thread-local, so every test
//! runs on the current-thread runtime `sqlx::test` provides.

use std::sync::Arc;

use lakekeeper::{
    api::{
        RequestMetadata,
        management::v1::{
            ApiServer,
            check::{
                CatalogActionCheckItem, CatalogActionCheckOperation,
                CatalogActionsBatchCheckRequest, RoleAssignee, UserOrRole, check_internal,
            },
            grant::{ApplyGrantsRequest, GrantEntry, Service as _},
            role::{CreateRoleRequest, Service as _},
        },
    },
    audit::omit_role_source_id_in_tests,
    service::{
        UserId,
        authz::{AllowAllAuthorizer, CatalogServerAction},
        events::{CatalogStoreReader, backends::audit::AuditEventListener},
    },
};
use lakekeeper_integration_tests::{SetupTestCatalog, memory_io_profile};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use serde_json::Value;
use sqlx::PgPool;

#[derive(Clone, Default)]
struct CapturedLogs(Arc<std::sync::Mutex<Vec<u8>>>);

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

impl CapturedLogs {
    fn records(&self) -> Vec<Value> {
        let bytes = self.0.lock().expect("log buffer poisoned").clone();
        String::from_utf8(bytes)
            .expect("log output is utf-8")
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter(|record| record["event_source"] == "audit")
            .map(lakekeeper::audit::validate::contract_fields)
            .collect()
    }
}

/// The published schema, which every record must satisfy without source ids too.
fn committed_schema() -> Value {
    serde_json::from_str(
        &std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../docs/docs/audit/schema.json"),
        )
        .expect("the committed audit schema; generate it with `just update-audit-schema`"),
    )
    .expect("the committed schema is JSON")
}

/// A role named by its id and provider, without its source id.
fn assert_without_source_id(
    value: &Value,
    id_key: &str,
    role_id: &str,
    provider_id: &str,
    place: &str,
) {
    assert_eq!(value[id_key], role_id, "{place}: {value:#}");
    assert_eq!(value["provider_id"], provider_id, "{place}: {value:#}");
    assert!(value.get("source_id").is_none(), "{place}: {value:#}");
}

#[sqlx::test]
async fn roles_are_named_without_their_source_id(pool: PgPool) {
    omit_role_source_id_in_tests();
    let logs = CapturedLogs::default();
    let _guard = tracing::subscriber::set_default(
        lakekeeper::audit::log_format(false)
            .with_writer(logs.clone())
            .finish(),
    );
    let (ctx, warehouse) = SetupTestCatalog::builder()
        .pool(pool)
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;
    ctx.v1_state
        .events
        .append(Arc::new(AuditEventListener::with_catalog(Arc::new(
            CatalogStoreReader::<PostgresBackend>::new(ctx.v1_state.catalog.clone()),
        ))))
        .await;
    let project_id = (*warehouse.project_id).clone();
    let caller_id = UserId::new_unchecked("oidc", "role-off-caller");
    let mut caller = RequestMetadata::test_user(caller_id.clone());
    caller.with_project_id(project_id.clone());

    let role = ApiServer::<PostgresBackend, AllowAllAuthorizer, SecretsState>::create_role(
        CreateRoleRequest {
            name: "analysts".to_string(),
            description: None,
            project_id: None,
            provider_id: None,
            source_id: None,
        },
        ctx.clone(),
        caller.clone(),
    )
    .await
    .unwrap();
    let role_id = role.id.to_string();
    let provider_id = role.provider_id.to_string();

    // A check about the role, by a caller acting as it.
    let mut acting_as_role = RequestMetadata::test_user_assumed_role(caller_id, role.id);
    acting_as_role.with_project_id(project_id);
    check_internal(
        ctx.clone(),
        acting_as_role,
        CatalogActionsBatchCheckRequest {
            checks: vec![CatalogActionCheckItem {
                id: None,
                identity: Some(UserOrRole::Role(RoleAssignee::from_role(role.id))),
                operation: CatalogActionCheckOperation::Server {
                    action: CatalogServerAction::ProvisionUsers,
                },
            }],
            error_on_not_found: false,
        },
    )
    .await
    .unwrap();
    ApiServer::<PostgresBackend, AllowAllAuthorizer, SecretsState>::apply_warehouse_grants(
        warehouse.warehouse_id,
        ctx.clone(),
        caller,
        ApplyGrantsRequest {
            writes: vec![GrantEntry {
                privilege: "get_metadata".to_string(),
                principal: UserOrRole::Role(RoleAssignee::from_role(role.id)),
            }],
            deletes: Vec::new(),
        },
    )
    .await
    .unwrap();

    // create_role, the check, apply_grants, and the grant_created record.
    let records = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let records = logs.records();
            if records.len() >= 4 {
                return records;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("fewer than 4 audit records: {:#?}", logs.records()));
    let schema = committed_schema();
    for (index, record) in records.iter().enumerate() {
        lakekeeper::audit::validate::assert_valid_record(
            &schema,
            record,
            &format!("record {index}"),
        );
    }

    let check = records
        .iter()
        .find(|r| r["actions"][0]["action_name"] == "introspect_permissions")
        .expect("the check record");
    assert_eq!(check["actor"]["actor_type"], "assumed_role");
    assert_without_source_id(
        &check["actor"]["assumed_role"],
        "role_id",
        &role_id,
        &provider_id,
        "actor.assumed_role",
    );
    assert_without_source_id(
        &check["authorizations"][0]["for_principal"],
        "role",
        &role_id,
        &provider_id,
        "for_principal",
    );
    let apply = records
        .iter()
        .find(|r| r["actions"][0]["action_name"] == "apply_grants")
        .expect("the apply_grants record");
    assert_without_source_id(
        &apply["actions"][0]["principals"][0],
        "role",
        &role_id,
        &provider_id,
        "apply_grants principals",
    );
    let grant = records
        .iter()
        .find(|r| r["operation"] == "grant_created")
        .expect("the grant record");
    assert_without_source_id(
        &grant["context"]["principal"],
        "role",
        &role_id,
        &provider_id,
        "grant_created principal",
    );
}
