//! Audit records with emails enabled: the actor's email from the token or the catalog, the
//! emails of decision subjects and grant recipients, and nothing when nothing is known.
//!
//! Emails are switched on for the whole process with `include_user_email_in_tests`, so
//! these tests live in a binary of their own. Capture is thread-local, so every test runs
//! on the current-thread runtime `sqlx::test` provides.

use std::{collections::HashMap, sync::Arc, time::Duration};

use lakekeeper::{
    api::{
        RequestMetadata,
        management::v1::{
            ApiServer,
            check::{
                CatalogActionCheckItem, CatalogActionCheckOperation,
                CatalogActionsBatchCheckRequest, UserOrRole, check_internal,
            },
            grant::{ApplyGrantsRequest, GrantEntry, Service as _},
            user::{UserLastUpdatedWith, UserType},
        },
    },
    audit::include_user_email_in_tests,
    service::{
        CatalogBackendError, CatalogStore, Transaction, UserId, UserUpsertMode,
        authz::{AllowAllAuthorizer, CatalogServerAction, GrantResource, GrantSpec, UserOrRoleId},
        events::{
            CatalogStoreReader, EventCatalog, EventListener, GrantsChangedEvent,
            backends::audit::AuditEventListener,
        },
        user_cache::UserEmail,
    },
};
use lakekeeper_integration_tests::{SetupTestCatalog, memory_io_profile};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use serde_json::Value;
use sqlx::PgPool;

type Ctx = lakekeeper::api::ApiContext<
    lakekeeper::service::State<AllowAllAuthorizer, PostgresBackend, SecretsState>,
>;

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

    /// The first `count` audit records; the listener writes on detached tasks.
    async fn wait_for(&self, count: usize) -> Vec<Value> {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let records = self.records();
                if records.len() >= count {
                    let schema = committed_schema();
                    for (index, record) in records.iter().enumerate() {
                        lakekeeper::audit::validate::assert_valid_record(
                            &schema,
                            record,
                            &format!("record {index}"),
                        );
                    }
                    return records;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("fewer than {count} audit records: {:#?}", self.records()))
    }
}

/// The published schema, which every record must satisfy, emails included.
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

struct Fixture {
    ctx: Ctx,
    warehouse_id: lakekeeper::WarehouseId,
    project_id: lakekeeper::ProjectId,
    logs: CapturedLogs,
    _guard: tracing::subscriber::DefaultGuard,
}

impl Fixture {
    /// A catalog whose audit listener reads from `catalog`, or from the catalog store.
    async fn new(pool: PgPool, catalog: Option<Arc<dyn EventCatalog>>) -> Self {
        include_user_email_in_tests();
        let logs = CapturedLogs::default();
        let guard = tracing::subscriber::set_default(
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
        let catalog = catalog.unwrap_or_else(|| {
            Arc::new(CatalogStoreReader::<PostgresBackend>::new(
                ctx.v1_state.catalog.clone(),
            ))
        });
        ctx.v1_state
            .events
            .append(Arc::new(AuditEventListener::with_catalog(catalog)))
            .await;
        Self {
            ctx,
            warehouse_id: warehouse.warehouse_id,
            project_id: (*warehouse.project_id).clone(),
            logs,
            _guard: guard,
        }
    }

    async fn user(&self, id: &UserId, email: Option<&str>) {
        let mut t = <PostgresBackend as CatalogStore>::Transaction::begin_write(
            self.ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap();
        PostgresBackend::create_or_update_user(
            id,
            "Someone",
            email,
            UserLastUpdatedWith::CreateEndpoint,
            UserType::Human,
            UserUpsertMode::Overwrite,
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();
    }

    /// A server check by `caller`, about each of `subjects`, or about the caller.
    async fn check(&self, caller: RequestMetadata, subjects: &[&UserId]) {
        let item = |identity: Option<UserOrRole>| CatalogActionCheckItem {
            id: None,
            identity,
            operation: CatalogActionCheckOperation::Server {
                action: CatalogServerAction::ProvisionUsers,
            },
        };
        let checks = if subjects.is_empty() {
            vec![item(None)]
        } else {
            subjects
                .iter()
                .map(|subject| item(Some(UserOrRole::User((*subject).clone()))))
                .collect()
        };
        check_internal(
            self.ctx.clone(),
            caller,
            CatalogActionsBatchCheckRequest {
                checks,
                error_on_not_found: false,
            },
        )
        .await
        .unwrap();
    }
}

fn user(name: &str) -> UserId {
    UserId::new_unchecked("oidc", name)
}

/// The `for_principal` subjects of a record's decisions, by user id.
fn subject_emails(record: &Value) -> HashMap<String, Option<String>> {
    record["authorizations"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|decision| decision.get("for_principal"))
        .map(|subject| {
            (
                subject["user"].as_str().unwrap().to_string(),
                subject
                    .get("email")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            )
        })
        .collect()
}

/// The token's email wins over the catalog's, so a caller with one costs no lookup.
#[sqlx::test]
async fn the_actor_email_comes_from_the_token_first(pool: PgPool) {
    let f = Fixture::new(pool, None).await;
    let alice = user("email-alice");
    f.user(&alice, Some("alice@catalog.example.com")).await;

    f.check(
        RequestMetadata::test_user_with_email(alice.clone(), "alice@token.example.com"),
        &[],
    )
    .await;
    let records = f.logs.wait_for(1).await;
    assert_eq!(records[0]["actor"]["email"], "alice@token.example.com");
}

/// A token without an email falls back to the catalog.
#[sqlx::test]
async fn the_actor_email_falls_back_to_the_catalog(pool: PgPool) {
    let f = Fixture::new(pool, None).await;
    let carol = user("email-carol");
    f.user(&carol, Some("carol@catalog.example.com")).await;

    f.check(RequestMetadata::test_user(carol.clone()), &[])
        .await;
    let records = f.logs.wait_for(1).await;
    assert_eq!(records[0]["actor"]["email"], "carol@catalog.example.com");
}

/// Subjects get their emails; one without a user row or without an email gets no field.
#[sqlx::test]
async fn decision_subjects_carry_their_emails(pool: PgPool) {
    let f = Fixture::new(pool, None).await;
    let bob = user("email-bob");
    let nameless = user("email-no-email");
    let unknown = user("email-unknown");
    f.user(&bob, Some("bob@example.com")).await;
    f.user(&nameless, None).await;

    f.check(
        RequestMetadata::test_user(user("email-admin")),
        &[&bob, &nameless, &unknown],
    )
    .await;
    let records = f.logs.wait_for(1).await;
    let subjects = subject_emails(&records[0]);
    assert_eq!(
        subjects[&bob.to_string()].as_deref(),
        Some("bob@example.com")
    );
    assert_eq!(subjects[&nameless.to_string()], None);
    assert_eq!(subjects[&unknown.to_string()], None);
    // An actor without a row has no email either; the key is absent, not null.
    assert!(records[0]["actor"].get("email").is_none());
}

/// Grant records carry the recipient's email; a role recipient never has one.
#[sqlx::test]
async fn grant_records_carry_the_recipients_email(pool: PgPool) {
    let f = Fixture::new(pool, None).await;
    let bob = user("email-grantee");
    f.user(&bob, Some("grantee@example.com")).await;

    let listener = AuditEventListener::with_catalog(Arc::new(
        CatalogStoreReader::<PostgresBackend>::new(f.ctx.v1_state.catalog.clone()),
    ));
    listener
        .grants_changed(GrantsChangedEvent::new(
            Vec::new(),
            vec![GrantSpec {
                principal: UserOrRoleId::User(bob.clone()),
                privilege: "provision_users".to_string(),
                resource: GrantResource::Server,
            }],
            Arc::new(RequestMetadata::test_user_with_email(
                user("email-granter"),
                "granter@example.com",
            )),
        ))
        .await
        .unwrap();
    let records = f.logs.wait_for(1).await;
    assert_eq!(records[0]["operation"], "grant_created");
    assert_eq!(records[0]["actor"]["email"], "granter@example.com");
    assert_eq!(
        records[0]["context"]["principal"]["email"],
        "grantee@example.com"
    );
}

/// The principals an `apply_grants` record lists carry their emails, like every other
/// place a user is named.
#[sqlx::test]
async fn apply_grants_principals_carry_their_emails(pool: PgPool) {
    let f = Fixture::new(pool, None).await;
    let bob = user("email-apply-bob");
    let carol = user("email-apply-carol");
    f.user(&bob, Some("bob@example.com")).await;
    f.user(&carol, None).await;

    let mut caller =
        RequestMetadata::test_user_with_email(user("email-applier"), "applier@example.com");
    caller.with_project_id(f.project_id.clone());
    let entry = |principal: &UserId| GrantEntry {
        privilege: "get_metadata".to_string(),
        principal: UserOrRole::User(principal.clone()),
    };
    ApiServer::<PostgresBackend, AllowAllAuthorizer, SecretsState>::apply_warehouse_grants(
        f.warehouse_id,
        f.ctx.clone(),
        caller,
        ApplyGrantsRequest {
            writes: vec![entry(&bob), entry(&carol)],
            deletes: Vec::new(),
        },
    )
    .await
    .unwrap();

    let records = f.logs.wait_for(3).await;
    let apply = records
        .iter()
        .find(|record| record["record_type"] == "authorization")
        .expect("the apply_grants record");
    let principals = &apply["actions"][0]["principals"];
    assert_eq!(principals[0]["user"], bob.to_string());
    assert_eq!(principals[0]["email"], "bob@example.com");
    assert_eq!(principals[1]["user"], carol.to_string());
    assert!(principals[1].get("email").is_none());
    // The decision's copy of the action carries the same.
    assert_eq!(
        apply["authorizations"][0]["action"]["principals"],
        *principals
    );
}

#[derive(Debug)]
struct FailingCatalog;

#[async_trait::async_trait]
impl EventCatalog for FailingCatalog {
    async fn user_emails(
        &self,
        _user_ids: &[UserId],
    ) -> Result<HashMap<UserId, UserEmail>, CatalogBackendError> {
        Err(CatalogBackendError::new_unexpected(std::io::Error::other(
            "the database is down",
        )))
    }
}

/// A failed lookup still writes the record, without the emails it could not find.
#[sqlx::test]
async fn a_failed_lookup_writes_the_record_without_emails(pool: PgPool) {
    let f = Fixture::new(pool, Some(Arc::new(FailingCatalog))).await;
    let bob = user("email-down");

    f.check(RequestMetadata::test_user(user("email-caller")), &[&bob])
        .await;
    let records = f.logs.wait_for(1).await;
    assert!(records[0]["actor"].get("email").is_none());
    assert_eq!(subject_emails(&records[0])[&bob.to_string()], None);
}
