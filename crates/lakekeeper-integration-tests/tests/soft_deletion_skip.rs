//! A call that skips the soft-deletion window of a warehouse needs `ModifySoftDeletion` on
//! the warehouse, in addition to what the call needs anyway. Force that only skips
//! protection needs nothing more.
//!
//! The refused direction blocks `ModifySoftDeletion` and leaves every other action allowed,
//! as for a holder of `modify`/`manage` on the namespace or the object. The allowed
//! direction lifts that block, as for a holder of the warehouse right.

use std::sync::Arc;

use http::StatusCode;
use iceberg::{NamespaceIdent, TableIdent};
use iceberg_ext::catalog::rest::RegisterTableRequest;
use lakekeeper::{
    WarehouseId,
    api::{
        ApiContext, ErrorModel,
        data::v1::generic_tables::{GenericTableParameters, GenericTableService as _},
        iceberg::{
            types::{DropParams, Prefix},
            v1::{
                DataAccess, NamespaceParameters, TableParameters, ViewParameters,
                namespace::{NamespaceDropFlags, NamespaceService as _},
                tables::TablesService as _,
                views::ViewService as _,
            },
        },
        management::v1::{
            ApiServer,
            namespace::NamespaceManagementService as _,
            table::TableManagementService as _,
            tasks::{ControlTaskAction, ControlTasksRequest, ListTasksRequest, Service as _},
            warehouse::{Service as _, TabularDeleteProfile, UpdateWarehouseDeleteProfileRequest},
        },
    },
    server::CatalogServer,
    service::{
        CatalogNamespaceOps as _, CatalogTabularOps as _, State, TableId, TabularIdentBorrowed,
        TabularListFlags,
        authz::tests::HidingAuthorizer,
        events::{AuthorizationFailureReason, EventListener, context::ActionContextKey},
        tasks::{TaskId, tabular_expiration_queue::QUEUE_NAME as EXPIRATION_QUEUE_NAME},
    },
};
use lakekeeper_integration_tests::{
    CapturingAuthzListener, RecordedAction, create_generic_table, create_ns, create_table,
    create_view, memory_io_profile, random_request_metadata, setup,
};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;

type Ctx = ApiContext<State<HidingAuthorizer, PostgresBackend, SecretsState>>;

const MODIFY_SOFT_DELETION: &str = "warehouse:ModifySoftDeletion";
const NS: &str = "ns";

fn soft() -> TabularDeleteProfile {
    TabularDeleteProfile::Soft {
        expiration_seconds: chrono::Duration::seconds(3600),
    }
}

struct Setup {
    ctx: Ctx,
    authz: HidingAuthorizer,
    warehouse_id: WarehouseId,
}

impl Setup {
    async fn new(pool: PgPool, profile: TabularDeleteProfile) -> Self {
        let authz = HidingAuthorizer::new();
        let (ctx, warehouse) = setup(
            pool,
            memory_io_profile(),
            None,
            authz.clone(),
            profile,
            None,
            1,
            None,
        )
        .await;
        let warehouse_id = warehouse.warehouse_id;
        create_ns(ctx.clone(), warehouse_id.to_string(), NS.to_string()).await;
        Self {
            ctx,
            authz,
            warehouse_id,
        }
    }

    fn prefix(&self) -> Option<Prefix> {
        Some(Prefix(self.warehouse_id.to_string()))
    }

    async fn table(&self, name: &str) -> TableId {
        create_table(
            self.ctx.clone(),
            self.warehouse_id.to_string(),
            NS,
            name,
            false,
        )
        .await
        .unwrap()
        .metadata
        .uuid()
        .into()
    }

    async fn view(&self, name: &str) {
        create_view(
            self.ctx.clone(),
            &self.warehouse_id.to_string(),
            NS,
            name,
            None,
        )
        .await
        .unwrap();
    }

    async fn generic_table(&self, name: &str) {
        create_generic_table(self.ctx.clone(), self.warehouse_id.to_string(), NS, name)
            .await
            .unwrap();
    }

    async fn protect_table(&self, table_id: TableId) {
        ApiServer::set_table_protection(
            table_id,
            self.warehouse_id,
            true,
            self.ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();
    }

    async fn drop(&self, kind: Kind, name: &str, force: bool) -> Result<(), ErrorModel> {
        let params = DropParams {
            purge_requested: false,
            force,
        };
        let ident = TableIdent::new(NamespaceIdent::new(NS.to_string()), name.to_string());
        let md = random_request_metadata();
        let result = match kind {
            Kind::Table => {
                CatalogServer::drop_table(
                    TableParameters {
                        prefix: self.prefix(),
                        table: ident,
                    },
                    params,
                    self.ctx.clone(),
                    md,
                )
                .await
            }
            Kind::View => {
                CatalogServer::drop_view(
                    ViewParameters {
                        prefix: self.prefix(),
                        view: ident,
                    },
                    params,
                    self.ctx.clone(),
                    md,
                )
                .await
            }
            Kind::GenericTable => {
                CatalogServer::drop_generic_table(
                    GenericTableParameters {
                        prefix: self.prefix(),
                        namespace: ident.namespace,
                        table_name: ident.name,
                    },
                    params,
                    self.ctx.clone(),
                    md,
                )
                .await
            }
        };
        result.map_err(|e| e.error)
    }

    async fn delete_namespace(&self, recursive: bool, force: bool) -> Result<(), ErrorModel> {
        CatalogServer::drop_namespace(
            NamespaceParameters {
                prefix: self.prefix(),
                namespace: NamespaceIdent::new(NS.to_string()),
            },
            NamespaceDropFlags {
                force,
                purge: false,
                recursive,
            },
            self.ctx.clone(),
            random_request_metadata(),
        )
        .await
        .map_err(|e| e.error)
    }

    async fn namespace_exists(&self) -> bool {
        PostgresBackend::get_namespace(
            self.warehouse_id,
            NamespaceIdent::new(NS.to_string()),
            self.ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap()
        .is_some()
    }

    async fn state(&self, kind: Kind, name: &str) -> Found {
        let ident = TableIdent::new(NamespaceIdent::new(NS.to_string()), name.to_string());
        let borrowed = || match kind {
            Kind::Table => TabularIdentBorrowed::Table(&ident),
            Kind::View => TabularIdentBorrowed::View(&ident),
            Kind::GenericTable => TabularIdentBorrowed::GenericTable(&ident),
        };
        for (flags, found) in [
            (TabularListFlags::active_and_staged(), Found::Active),
            (TabularListFlags::only_deleted(), Found::SoftDeleted),
        ] {
            let infos = PostgresBackend::get_tabular_infos_by_ident(
                self.warehouse_id,
                &[borrowed()],
                flags,
                self.ctx.v1_state.catalog.clone(),
            )
            .await
            .unwrap();
            if !infos.is_empty() {
                return found;
            }
        }
        Found::Gone
    }

    async fn register_overwrite(&self, name: &str) -> Result<(), ErrorModel> {
        let table = TableIdent::new(NamespaceIdent::new(NS.to_string()), name.to_string());
        let metadata_location = PostgresBackend::get_table_info(
            self.warehouse_id,
            table,
            TabularListFlags::active(),
            self.ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap()
        .unwrap()
        .metadata_location
        .unwrap();
        self.register_overwrite_from(name, metadata_location.to_string())
            .await
    }

    async fn register_overwrite_from(
        &self,
        name: &str,
        metadata_location: String,
    ) -> Result<(), ErrorModel> {
        CatalogServer::register_table(
            NamespaceParameters {
                prefix: self.prefix(),
                namespace: NamespaceIdent::new(NS.to_string()),
            },
            RegisterTableRequest::builder()
                .name(name.to_string())
                .metadata_location(metadata_location)
                .overwrite(true)
                .build(),
            DataAccess::not_specified(),
            self.ctx.clone(),
            random_request_metadata(),
        )
        .await
        .map(|_| ())
        .map_err(|e| e.error)
    }

    async fn soft_deletion_tasks(&self) -> Vec<TaskId> {
        ApiServer::list_tasks(
            self.warehouse_id,
            ListTasksRequest {
                queue_name: Some(vec![EXPIRATION_QUEUE_NAME.clone()]),
                ..Default::default()
            },
            self.ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap()
        .tasks
        .into_iter()
        .map(|t| t.task_id)
        .collect()
    }

    async fn control(
        &self,
        action: ControlTaskAction,
        task_ids: Vec<TaskId>,
    ) -> Result<(), ErrorModel> {
        ApiServer::control_tasks(
            self.warehouse_id,
            ControlTasksRequest { action, task_ids },
            self.ctx.clone(),
            random_request_metadata(),
        )
        .await
        .map(|_| ())
        .map_err(|e| e.error)
    }

    async fn listen(&self) -> Arc<CapturingAuthzListener> {
        let listener = Arc::new(CapturingAuthzListener::default());
        self.ctx
            .v1_state
            .events
            .append(listener.clone() as Arc<dyn EventListener>)
            .await;
        listener
    }
}

#[derive(Debug, Clone, Copy)]
enum Kind {
    Table,
    View,
    GenericTable,
}

#[derive(Debug, PartialEq, Eq)]
enum Found {
    Active,
    SoftDeleted,
    Gone,
}

#[track_caller]
fn assert_needs_modify_soft_deletion(result: Result<(), ErrorModel>) {
    let err = result.expect_err("skipping soft-deletion without ModifySoftDeletion");
    assert_eq!(err.code, StatusCode::FORBIDDEN.as_u16(), "{err:?}");
    assert_eq!(err.r#type, "WarehouseActionForbidden", "{err:?}");
}

fn drop_action(force: bool) -> RecordedAction {
    RecordedAction {
        action_name: "drop".to_string(),
        context: vec![
            ActionContextKey::Force(force),
            ActionContextKey::Purge(false),
        ],
    }
}

#[sqlx::test]
async fn forced_drop_in_soft_deletion_warehouse_needs_modify_soft_deletion(pool: PgPool) {
    let s = Setup::new(pool, soft()).await;
    let all = [Kind::Table, Kind::View, Kind::GenericTable];
    let name = |kind: Kind, what: &str| format!("{kind:?}_{what}").to_lowercase();
    for kind in all {
        for what in ["kept", "forced"] {
            match kind {
                Kind::Table => {
                    s.table(&name(kind, what)).await;
                }
                Kind::View => s.view(&name(kind, what)).await,
                Kind::GenericTable => s.generic_table(&name(kind, what)).await,
            }
        }
    }
    s.authz.block_action(MODIFY_SOFT_DELETION);

    for kind in all {
        let (kept, forced) = (name(kind, "kept"), name(kind, "forced"));
        assert_needs_modify_soft_deletion(s.drop(kind, &forced, true).await);
        assert_eq!(s.state(kind, &forced).await, Found::Active, "{kind:?}");
        // A drop without force keeps the window and needs nothing more.
        s.drop(kind, &kept, false).await.unwrap();
        assert_eq!(s.state(kind, &kept).await, Found::SoftDeleted, "{kind:?}");
    }

    s.authz.unblock_action(MODIFY_SOFT_DELETION);
    for kind in all {
        let forced = name(kind, "forced");
        s.drop(kind, &forced, true).await.unwrap();
        assert_eq!(s.state(kind, &forced).await, Found::Gone, "{kind:?}");
    }
}

#[sqlx::test]
async fn refused_forced_drop_is_audited_as_the_drop(pool: PgPool) {
    let s = Setup::new(pool, soft()).await;
    s.table("t").await;
    s.authz.block_action(MODIFY_SOFT_DELETION);
    let listener = s.listen().await;

    assert_needs_modify_soft_deletion(s.drop(Kind::Table, "t", true).await);

    assert_eq!(listener.settled_counts(0, 1).await, (0, 1));
    assert_eq!(
        listener.failure_reasons(),
        vec![AuthorizationFailureReason::ActionForbidden]
    );
    assert_eq!(
        listener.recorded_actions(),
        (vec![], vec![vec![drop_action(true)]])
    );
}

#[sqlx::test]
async fn forced_drop_in_hard_deletion_warehouse_needs_no_extra_right(pool: PgPool) {
    let s = Setup::new(pool, TabularDeleteProfile::Hard {}).await;
    let table_id = s.table("t").await;
    s.protect_table(table_id).await;
    s.view("v").await;
    s.generic_table("g").await;
    s.authz.block_action(MODIFY_SOFT_DELETION);

    // Force that only skips protection stays with the object.
    s.drop(Kind::Table, "t", true).await.unwrap();
    s.drop(Kind::View, "v", true).await.unwrap();
    s.drop(Kind::GenericTable, "g", true).await.unwrap();
    assert_eq!(s.state(Kind::Table, "t").await, Found::Gone);
    assert_eq!(s.state(Kind::View, "v").await, Found::Gone);
    assert_eq!(s.state(Kind::GenericTable, "g").await, Found::Gone);
}

#[sqlx::test]
async fn recursive_namespace_delete_in_soft_deletion_warehouse_needs_modify_soft_deletion(
    pool: PgPool,
) {
    let s = Setup::new(pool, soft()).await;
    s.table("active").await;
    s.table("dropped").await;
    s.drop(Kind::Table, "dropped", false).await.unwrap();
    s.authz.block_action(MODIFY_SOFT_DELETION);

    assert_needs_modify_soft_deletion(s.delete_namespace(true, true).await);
    // Without force the delete is refused anyway; the missing right is reported first.
    assert_needs_modify_soft_deletion(s.delete_namespace(true, false).await);
    assert!(s.namespace_exists().await);
    assert_eq!(s.state(Kind::Table, "active").await, Found::Active);
    assert_eq!(s.state(Kind::Table, "dropped").await, Found::SoftDeleted);

    s.authz.unblock_action(MODIFY_SOFT_DELETION);
    s.delete_namespace(true, true).await.unwrap();
    assert!(!s.namespace_exists().await);
    assert_eq!(s.state(Kind::Table, "active").await, Found::Gone);
    assert_eq!(s.state(Kind::Table, "dropped").await, Found::Gone);
}

#[sqlx::test]
async fn forced_delete_of_an_empty_namespace_needs_no_extra_right(pool: PgPool) {
    let s = Setup::new(pool, soft()).await;
    let namespace_id = PostgresBackend::get_namespace(
        s.warehouse_id,
        NamespaceIdent::new(NS.to_string()),
        s.ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap()
    .namespace_id();
    ApiServer::set_namespace_protection(
        namespace_id,
        s.warehouse_id,
        true,
        s.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    s.authz.block_action(MODIFY_SOFT_DELETION);

    s.delete_namespace(false, true).await.unwrap();
    assert!(!s.namespace_exists().await);
}

#[sqlx::test]
async fn recursive_namespace_delete_in_hard_deletion_warehouse_needs_no_extra_right(pool: PgPool) {
    let s = Setup::new(pool, TabularDeleteProfile::Hard {}).await;
    let table_id = s.table("t").await;
    s.protect_table(table_id).await;
    s.authz.block_action(MODIFY_SOFT_DELETION);

    s.delete_namespace(true, true).await.unwrap();
    assert!(!s.namespace_exists().await);
    assert_eq!(s.state(Kind::Table, "t").await, Found::Gone);
}

#[sqlx::test]
async fn running_a_soft_deletion_task_early_needs_modify_soft_deletion(pool: PgPool) {
    let s = Setup::new(pool, soft()).await;
    s.table("t").await;
    s.drop(Kind::Table, "t", false).await.unwrap();
    let tasks = s.soft_deletion_tasks().await;
    assert_eq!(tasks.len(), 1);
    s.authz.block_action(MODIFY_SOFT_DELETION);

    // `ControlAllTasks` on the warehouse is allowed, and does not suffice.
    assert_needs_modify_soft_deletion(s.control(ControlTaskAction::RunNow, tasks.clone()).await);
    assert_needs_modify_soft_deletion(
        s.control(
            ControlTaskAction::RunAt {
                scheduled_for: chrono::Utc::now() + chrono::Duration::minutes(1),
            },
            tasks.clone(),
        )
        .await,
    );

    // The check follows the task's queue, not the warehouse's profile today.
    s.authz.unblock_action(MODIFY_SOFT_DELETION);
    ApiServer::update_warehouse_delete_profile(
        s.warehouse_id,
        UpdateWarehouseDeleteProfileRequest {
            delete_profile: TabularDeleteProfile::Hard {},
        },
        s.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    s.authz.block_action(MODIFY_SOFT_DELETION);
    assert_needs_modify_soft_deletion(s.control(ControlTaskAction::RunNow, tasks.clone()).await);
    assert_eq!(s.state(Kind::Table, "t").await, Found::SoftDeleted);

    s.authz.unblock_action(MODIFY_SOFT_DELETION);
    s.control(ControlTaskAction::RunNow, tasks).await.unwrap();
}

#[sqlx::test]
async fn cancelling_a_soft_deletion_task_needs_no_extra_right(pool: PgPool) {
    let s = Setup::new(pool, soft()).await;
    s.table("t").await;
    s.drop(Kind::Table, "t", false).await.unwrap();
    let tasks = s.soft_deletion_tasks().await;
    s.authz.block_action(MODIFY_SOFT_DELETION);

    s.control(ControlTaskAction::Cancel, tasks).await.unwrap();
    assert_eq!(s.state(Kind::Table, "t").await, Found::Active);
}

#[sqlx::test]
async fn register_overwrite_of_an_active_table_in_soft_deletion_warehouse_needs_modify_soft_deletion(
    pool: PgPool,
) {
    let s = Setup::new(pool, soft()).await;
    let table_id = s.table("t").await;
    s.authz.block_action(MODIFY_SOFT_DELETION);
    let listener = s.listen().await;

    assert_needs_modify_soft_deletion(s.register_overwrite("t").await);
    assert_eq!(s.state(Kind::Table, "t").await, Found::Active);
    // The replaced table skips the window, so its drop is recorded as forced.
    let (_, failed) = listener.recorded_actions();
    assert_eq!(failed, vec![vec![drop_action(true)]]);

    s.authz.unblock_action(MODIFY_SOFT_DELETION);
    s.register_overwrite("t").await.unwrap();
    let info = PostgresBackend::get_table_info(
        s.warehouse_id,
        TableIdent::new(NamespaceIdent::new(NS.to_string()), "t".to_string()),
        TabularListFlags::active(),
        s.ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(info.tabular_id, table_id);
}

#[sqlx::test]
async fn register_overwrite_of_a_staged_table_needs_no_extra_right(pool: PgPool) {
    let s = Setup::new(pool, soft()).await;
    let source = create_table(
        s.ctx.clone(),
        s.warehouse_id.to_string(),
        NS,
        "source",
        false,
    )
    .await
    .unwrap();
    // The registered table takes the id of the source, so the source must be gone.
    s.drop(Kind::Table, "source", true).await.unwrap();
    create_table(
        s.ctx.clone(),
        s.warehouse_id.to_string(),
        NS,
        "staged",
        true,
    )
    .await
    .unwrap();
    s.authz.block_action(MODIFY_SOFT_DELETION);
    let listener = s.listen().await;

    s.register_overwrite_from("staged", source.metadata_location.unwrap())
        .await
        .unwrap();
    assert_eq!(s.state(Kind::Table, "staged").await, Found::Active);
    let (succeeded, failed) = listener.recorded_actions();
    assert!(failed.is_empty(), "{failed:?}");
    assert_eq!(
        succeeded
            .into_iter()
            .filter(|actions| actions.iter().any(|a| a.action_name == "drop"))
            .collect::<Vec<_>>(),
        vec![vec![drop_action(false)]]
    );
}

#[sqlx::test]
async fn register_overwrite_in_hard_deletion_warehouse_needs_no_extra_right(pool: PgPool) {
    let s = Setup::new(pool, TabularDeleteProfile::Hard {}).await;
    s.table("t").await;
    s.authz.block_action(MODIFY_SOFT_DELETION);

    s.register_overwrite("t").await.unwrap();
    assert_eq!(s.state(Kind::Table, "t").await, Found::Active);
}

/// The holder of the warehouse right overwrites a protected table: refused, as the replaced
/// table's protection holds although its drop is recorded as forced.
async fn assert_register_overwrite_keeps_protection(pool: PgPool, profile: TabularDeleteProfile) {
    let s = Setup::new(pool, profile).await;
    let table_id = s.table("t").await;
    s.protect_table(table_id).await;

    let err = s
        .register_overwrite("t")
        .await
        .expect_err("a protected table cannot be overwritten");
    assert_eq!(err.code, StatusCode::CONFLICT.as_u16(), "{err:?}");
    assert_eq!(
        err.r#type, "ProtectedTabularDeletionWithoutForce",
        "{err:?}"
    );
    assert_eq!(s.state(Kind::Table, "t").await, Found::Active);
}

#[sqlx::test]
async fn register_overwrite_keeps_protection_in_soft_deletion_warehouse(pool: PgPool) {
    assert_register_overwrite_keeps_protection(pool, soft()).await;
}

#[sqlx::test]
async fn register_overwrite_keeps_protection_in_hard_deletion_warehouse(pool: PgPool) {
    assert_register_overwrite_keeps_protection(pool, TabularDeleteProfile::Hard {}).await;
}
