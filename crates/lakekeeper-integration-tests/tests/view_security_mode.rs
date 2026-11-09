//! A view commit that switches between running as the owner and running as the caller needs
//! `ChangeSecurityMode`: adding an engine owner property for the principal and the owner it
//! names, removing one for the principal. Each verdict has its own audit record. Other commits
//! do not ask for it, and a commit whose switch changed after the decision is not committed.

use std::sync::{Arc, Mutex};

use chrono::Utc;
use futures::future::BoxFuture;
use http::StatusCode;
use iceberg::{NamespaceIdent, TableIdent};
use iceberg_ext::catalog::rest::{CommitViewRequest, IcebergErrorResponse, LoadViewResult};
use lakekeeper::{
    WarehouseId,
    api::{
        ApiContext, RequestMetadata,
        iceberg::{
            types::Prefix,
            v1::{DataAccess, NamespaceParameters, ViewParameters, views::ViewService as _},
        },
        management::v1::warehouse::TabularDeleteProfile,
    },
    server::CatalogServer,
    service::{
        State, UserId,
        authz::{
            self, UserOrRoleId,
            tests::{HidingAuthorizer, RecordedTabularCheck},
        },
        events::EventListener,
    },
};
use lakekeeper_integration_tests::{
    CapturingAuthzListener, OWNER_PROPERTY, RecordedDecision, create_ns, create_view_request,
    memory_io_profile, setup_simple, trino_engine_request, user_request,
};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use serde_json::json;
use sqlx::PgPool;

type Context = ApiContext<State<HidingAuthorizer, PostgresBackend, SecretsState>>;

struct Setup {
    ctx: Context,
    authz: HidingAuthorizer,
    warehouse_id: WarehouseId,
    namespace: NamespaceIdent,
    concurrent: ConcurrentCommit,
}

/// A commit of another client that lands while Lakekeeper decides a commit: it runs once,
/// inside the first authorizer check for which its condition holds.
type Armed = (Box<dyn Fn() -> bool + Send>, BoxFuture<'static, ()>);

#[derive(Clone, Default)]
struct ConcurrentCommit(Arc<Mutex<Option<Armed>>>);

impl ConcurrentCommit {
    fn arm(&self, when: impl Fn() -> bool + Send + 'static, commit: BoxFuture<'static, ()>) {
        *self.0.lock().unwrap() = Some((Box::new(when), commit));
    }

    fn take_if_due(&self) -> Option<BoxFuture<'static, ()>> {
        let mut armed = self.0.lock().unwrap();
        let due = armed.as_ref().is_some_and(|(when, _)| when());
        due.then(|| armed.take().map(|(_, commit)| commit))
            .flatten()
    }
}

impl Setup {
    async fn new(pool: PgPool) -> Self {
        let concurrent = ConcurrentCommit::default();
        let hook = concurrent.clone();
        let authz = HidingAuthorizer::new().with_check_hook(move || {
            let commit = hook.take_if_due();
            Box::pin(async move {
                if let Some(commit) = commit {
                    commit.await;
                }
            })
        });
        let (ctx, warehouse) = setup_simple(
            pool,
            memory_io_profile(),
            None,
            authz.clone(),
            TabularDeleteProfile::Hard {},
            None,
        )
        .await;
        let namespace = create_ns(ctx.clone(), warehouse.warehouse_id.to_string(), "ns".into())
            .await
            .namespace;
        Self {
            ctx,
            authz,
            warehouse_id: warehouse.warehouse_id,
            namespace,
            concurrent,
        }
    }

    async fn capture_records(&self) -> Arc<CapturingAuthzListener> {
        let listener = Arc::new(CapturingAuthzListener::default());
        self.ctx
            .v1_state
            .events
            .append(listener.clone() as Arc<dyn EventListener>)
            .await;
        listener
    }

    /// A commit by Trino that will land while Lakekeeper decides the next commit.
    fn concurrent_trino_commit(&self, updates: serde_json::Value) -> BoxFuture<'static, ()> {
        let ctx = self.ctx.clone();
        let params = self.params();
        Box::pin(async move {
            let request: CommitViewRequest =
                serde_json::from_value(json!({ "updates": updates })).unwrap();
            CatalogServer::commit_view(
                params,
                request,
                ctx,
                DataAccess::not_specified(),
                trino_engine_request(),
            )
            .await
            .unwrap();
        })
    }

    fn params(&self) -> ViewParameters {
        ViewParameters {
            prefix: Some(Prefix(self.warehouse_id.to_string())),
            view: TableIdent::new(self.namespace.clone(), "v".to_string()),
        }
    }

    /// Creates the view `v` as the trusted Trino engine does, running as `owner` if given.
    async fn create(&self, owner: Option<&str>) -> uuid::Uuid {
        let mut request = create_view_request(Some("v"), None);
        if let Some(owner) = owner {
            request
                .properties
                .insert(OWNER_PROPERTY.to_string(), owner.to_string());
        }
        CatalogServer::create_view(
            NamespaceParameters {
                prefix: Some(Prefix(self.warehouse_id.to_string())),
                namespace: self.namespace.clone(),
            },
            request,
            self.ctx.clone(),
            DataAccess::not_specified(),
            trino_engine_request(),
        )
        .await
        .unwrap()
        .metadata
        .uuid()
    }

    async fn commit(
        &self,
        updates: serde_json::Value,
        request_metadata: RequestMetadata,
    ) -> Result<LoadViewResult, IcebergErrorResponse> {
        let request: CommitViewRequest =
            serde_json::from_value(json!({ "updates": updates })).unwrap();
        CatalogServer::commit_view(
            self.params(),
            request,
            self.ctx.clone(),
            DataAccess::not_specified(),
            request_metadata,
        )
        .await
    }

    async fn commit_as_trino(
        &self,
        updates: serde_json::Value,
    ) -> Result<LoadViewResult, IcebergErrorResponse> {
        self.commit(updates, trino_engine_request()).await
    }

    async fn owner(&self) -> Option<String> {
        load_view(&self.ctx, self.params())
            .await
            .metadata
            .properties()
            .get(OWNER_PROPERTY)
            .cloned()
    }

    fn block_security_mode_changes(&self) {
        self.authz.block_action("view:ChangeSecurityMode");
    }

    /// The `ChangeSecurityMode` checks the authorizer received since `since`.
    fn security_mode_checks(&self, since: usize) -> Vec<RecordedTabularCheck> {
        self.authz.tabular_checks()[since..]
            .iter()
            .flatten()
            .filter(|check| check.action == "ChangeSecurityMode")
            .cloned()
            .collect()
    }
}

async fn load_view(ctx: &Context, params: ViewParameters) -> LoadViewResult {
    CatalogServer::load_view(
        params,
        lakekeeper::api::iceberg::v1::views::LoadViewRequest::default(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap()
}

fn set_owner(owner: &str) -> serde_json::Value {
    json!({"action": "set-properties", "updates": {OWNER_PROPERTY: owner}})
}

/// Updates that add a version running `sql` and make it current, as Trino's
/// `CREATE OR REPLACE VIEW` sends them, followed by `extra`. The dialect is the one of the
/// created view: Iceberg refuses a replace that drops a dialect.
fn replace(sql: &str, extra: &[serde_json::Value]) -> serde_json::Value {
    let mut updates = vec![
        json!({
            "action": "add-view-version",
            "view-version": {
                "version-id": 2,
                "schema-id": 0,
                "timestamp-ms": Utc::now().timestamp_millis(),
                "summary": {"engine-name": "trino"},
                "representations": [{"type": "sql", "sql": sql, "dialect": "spark"}],
                "default-namespace": []
            }
        }),
        json!({"action": "set-current-view-version", "view-version-id": -1}),
    ];
    updates.extend_from_slice(extra);
    serde_json::Value::Array(updates)
}

fn assert_switch_refused(err: &IcebergErrorResponse) {
    assert_eq!(err.error.code, StatusCode::FORBIDDEN);
    assert_eq!(err.error.r#type, "SecurityModeChangeForbidden");
}

fn alice() -> authz::UserOrRole {
    authz::UserOrRole::User(UserId::new_unchecked("oidc", "alice"))
}

fn decision(action: &str, for_principal: Option<&str>, allowed: bool) -> RecordedDecision {
    RecordedDecision {
        action_name: action.to_string(),
        for_principal: for_principal
            .map(|user| UserOrRoleId::User(UserId::new_unchecked("oidc", user))),
        allowed: Some(allowed),
    }
}

/// The decisions of the captured succeeded and failed records, each list in a fixed order:
/// records are dispatched from spawned tasks.
fn decisions(
    records: &CapturingAuthzListener,
) -> (Vec<Vec<RecordedDecision>>, Vec<Vec<RecordedDecision>>) {
    let (mut succeeded, mut failed) = records.recorded_decisions();
    succeeded.sort_by_key(|record| format!("{record:?}"));
    failed.sort_by_key(|record| format!("{record:?}"));
    (succeeded, failed)
}

fn assert_view_changed_during_commit(err: &IcebergErrorResponse) {
    assert_eq!(err.error.code, StatusCode::CONFLICT);
    assert_eq!(err.error.r#type, "CommitFailedException");
}

#[sqlx::test]
async fn setting_the_owner_needs_change_security_mode(pool: PgPool) {
    let setup = Setup::new(pool).await;
    setup.create(None).await;
    setup.block_security_mode_changes();

    // Trino's `CREATE OR REPLACE VIEW ... SECURITY DEFINER` over an INVOKER view.
    let err = setup
        .commit_as_trino(replace("select 2", &[set_owner("alice")]))
        .await
        .expect_err("making a view run as its owner needs change_security_mode");
    assert_switch_refused(&err);
    let err = setup
        .commit_as_trino(json!([set_owner("alice")]))
        .await
        .expect_err("setting the owner property alone switches the mode too");
    assert_switch_refused(&err);
    assert_eq!(setup.owner().await, None);
}

#[sqlx::test]
async fn setting_the_owner_asks_the_principal_and_the_owner(pool: PgPool) {
    let setup = Setup::new(pool).await;
    let view_id = setup.create(None).await;
    let object = format!("view:{}/{view_id}", setup.warehouse_id);

    let records = setup.capture_records().await;
    let since = setup.authz.tabular_checks().len();
    setup
        .commit_as_trino(replace("select 2", &[set_owner("alice")]))
        .await
        .unwrap();
    // The commit's own record, then one `change_security_mode` record per checked principal.
    assert_eq!(records.settled_counts(3, 0).await, (3, 0));
    assert_eq!(
        decisions(&records),
        (
            vec![
                vec![decision("change_security_mode", None, true)],
                vec![decision("change_security_mode", Some("alice"), true)],
                vec![decision("commit", None, true)],
            ],
            vec![]
        )
    );
    assert_eq!(setup.owner().await, Some("alice".to_string()));
    // The owner is the user of the engine's identity provider, checked as the view will run.
    assert_eq!(
        setup.security_mode_checks(since),
        vec![
            RecordedTabularCheck {
                object: object.clone(),
                action: "ChangeSecurityMode".to_string(),
                user: None,
                is_delegated_execution: false,
            },
            RecordedTabularCheck {
                object: object.clone(),
                action: "ChangeSecurityMode".to_string(),
                user: Some(alice()),
                is_delegated_execution: true,
            },
        ]
    );
}

#[sqlx::test]
async fn setting_an_owner_without_the_permission_is_refused(pool: PgPool) {
    let setup = Setup::new(pool).await;
    let view_id = setup.create(None).await;
    // Alice may do nothing on the view; the principal, Trino's identity, may switch the mode.
    setup
        .authz
        .hide_for_user(&alice(), &format!("view:{}/{view_id}", setup.warehouse_id));
    let records = setup.capture_records().await;

    let err = setup
        .commit_as_trino(replace("select 2", &[set_owner("alice")]))
        .await
        .expect_err("the new owner must be allowed to switch the mode");
    assert_switch_refused(&err);
    // The message names the id to give the permission to.
    assert!(
        err.error.message.contains(
            "Lakekeeper checks the new owner as user 'oidc~alice', who does not have it. Keep \
             the security mode of the view, or give 'oidc~alice' this permission on the view."
        ),
        "{}",
        err.error.message
    );
    assert_eq!(records.settled_counts(2, 1).await, (2, 1));
    assert_eq!(
        decisions(&records),
        (
            vec![
                vec![decision("change_security_mode", None, true)],
                vec![decision("commit", None, true)],
            ],
            vec![vec![decision("change_security_mode", Some("alice"), false)]]
        )
    );
    assert_eq!(setup.owner().await, None);

    setup
        .commit_as_trino(replace("select 2", &[set_owner("bob")]))
        .await
        .unwrap();
    assert_eq!(setup.owner().await, Some("bob".to_string()));
}

#[sqlx::test]
async fn removing_the_owner_needs_change_security_mode(pool: PgPool) {
    let setup = Setup::new(pool).await;
    setup.create(Some("alice")).await;
    setup.block_security_mode_changes();

    // Trino's `CREATE OR REPLACE VIEW ... SECURITY INVOKER` with new SQL: Lakekeeper would
    // remove the owner property.
    let err = setup
        .commit_as_trino(replace("select 2", &[]))
        .await
        .expect_err("making a view run as the caller needs change_security_mode");
    assert_switch_refused(&err);
    let err = setup
        .commit_as_trino(json!([{"action": "remove-properties", "removals": [OWNER_PROPERTY]}]))
        .await
        .expect_err("removing the owner property switches the mode too");
    assert_switch_refused(&err);
    assert_eq!(setup.owner().await, Some("alice".to_string()));
}

#[sqlx::test]
async fn removing_the_owner_asks_only_the_principal(pool: PgPool) {
    let setup = Setup::new(pool).await;
    let view_id = setup.create(Some("alice")).await;
    setup
        .authz
        .hide_for_user(&alice(), &format!("view:{}/{view_id}", setup.warehouse_id));

    let since = setup.authz.tabular_checks().len();
    let replaced = setup
        .commit_as_trino(replace("select 2", &[]))
        .await
        .unwrap();
    assert_eq!(replaced.metadata.properties().get(OWNER_PROPERTY), None);
    assert_eq!(
        setup
            .security_mode_checks(since)
            .into_iter()
            .map(|check| check.user)
            .collect::<Vec<_>>(),
        vec![None]
    );
}

#[sqlx::test]
async fn commits_that_keep_the_mode_need_no_switch_permission(pool: PgPool) {
    let setup = Setup::new(pool).await;
    setup.create(Some("alice")).await;
    setup.block_security_mode_changes();
    let since = setup.authz.tabular_checks().len();

    // Another owner: `CREATE OR REPLACE VIEW ... SECURITY DEFINER` by another Trino user.
    setup
        .commit_as_trino(replace("select 2", &[set_owner("bob")]))
        .await
        .unwrap();
    assert_eq!(setup.owner().await, Some("bob".to_string()));
    // The same owner again, as `ALTER VIEW ... REFRESH` sends it.
    setup
        .commit_as_trino(json!([set_owner("bob")]))
        .await
        .unwrap();
    // `COMMENT ON VIEW`.
    setup
        .commit_as_trino(json!([{"action": "set-properties", "updates": {"comment": "c"}}]))
        .await
        .unwrap();
    // `COMMENT ON COLUMN`: a new version with the same query keeps the owner.
    setup
        .commit_as_trino(replace("select 2", &[]))
        .await
        .unwrap();
    assert_eq!(setup.owner().await, Some("bob".to_string()));
    // Nothing switched, so nothing was asked.
    assert_eq!(setup.security_mode_checks(since), Vec::new());
}

/// The stored view becomes a DEFINER view after Lakekeeper read it for the authorization of an
/// INVOKER replacement with new SQL, which would remove the new owner without asking.
#[sqlx::test]
async fn a_view_that_becomes_definer_during_the_decision_is_not_switched_back(pool: PgPool) {
    let setup = Setup::new(pool).await;
    setup.create(None).await;
    setup.concurrent.arm(
        || true,
        setup.concurrent_trino_commit(json!([set_owner("carol")])),
    );

    let err = setup
        .commit_as_trino(replace("select 2", &[]))
        .await
        .expect_err("the stored view switched after the decision");
    assert_view_changed_during_commit(&err);
    assert_eq!(setup.owner().await, Some("carol".to_string()));

    // Sent again, the commit is decided on the stored view and asks the switch.
    let since = setup.authz.tabular_checks().len();
    setup
        .commit_as_trino(replace("select 2", &[]))
        .await
        .unwrap();
    assert_eq!(setup.owner().await, None);
    assert_eq!(
        setup
            .security_mode_checks(since)
            .into_iter()
            .map(|check| check.user)
            .collect::<Vec<_>>(),
        vec![None]
    );
}

/// The stored view gets another owner after Lakekeeper decided that the commit makes it run as
/// alice: the commit would then only change the owner, which nobody decided.
#[sqlx::test]
async fn a_switch_decided_on_an_older_view_is_not_committed(pool: PgPool) {
    let setup = Setup::new(pool).await;
    setup.create(None).await;
    // During the first check after the commit's own: the switch check of the identity that
    // sends the commit, which comes after the decision.
    let authz = setup.authz.clone();
    let since = authz.tabular_checks().len();
    setup.concurrent.arm(
        move || {
            authz.tabular_checks()[since..]
                .iter()
                .flatten()
                .any(|check| check.action.starts_with("Commit"))
        },
        setup.concurrent_trino_commit(json!([set_owner("carol")])),
    );

    let err = setup
        .commit_as_trino(replace("select 2", &[set_owner("alice")]))
        .await
        .expect_err("the stored view changed after the decision");
    assert_view_changed_during_commit(&err);
    assert_eq!(setup.owner().await, Some("carol".to_string()));
}

#[sqlx::test]
async fn untrusted_clients_replace_invoker_views_without_switch_permission(pool: PgPool) {
    let setup = Setup::new(pool).await;
    setup.create(None).await;
    setup.block_security_mode_changes();

    let since = setup.authz.tabular_checks().len();
    setup
        .commit(replace("select 2", &[]), user_request())
        .await
        .unwrap();
    // A client that cannot write an owner property cannot switch the mode, so it is not asked.
    assert_eq!(setup.security_mode_checks(since), Vec::new());
    assert_eq!(setup.owner().await, None);
}
