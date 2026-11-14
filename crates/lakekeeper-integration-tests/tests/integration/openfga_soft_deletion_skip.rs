//! Skipping soft deletion against a real OpenFGA store plus a Postgres catalog: `modify` on a
//! namespace drops, deletes and runs tasks below it, but a call that skips the soft-deletion
//! window also needs `modify` on the warehouse. In a warehouse with hard deletion, force only
//! skips protection and `modify` on the namespace suffices.
//!
//! Gated behind the `openfga_integration_tests` module so the default nextest filter
//! excludes it; runs under `--profile ci` with a live OpenFGA at
//! `LAKEKEEPER__OPENFGA__ENDPOINT`.

// Nested one level deep so the test path contains `::openfga_integration_tests::`,
// which the default nextest filter excludes (a root module would not match).
mod soft_deletion_skip {
    mod openfga_integration_tests {
        use std::sync::Arc;

        use iceberg::{NamespaceIdent, TableIdent};
        use iceberg_ext::catalog::rest::RegisterTableRequest;
        use lakekeeper::{
            ProjectId, WarehouseId,
            api::{
                ApiContext, ErrorModel, RequestMetadata,
                iceberg::{
                    types::{DropParams, Prefix},
                    v1::{
                        CreateNamespaceRequest, DataAccess, NamespaceParameters, TableParameters,
                        ViewParameters,
                        namespace::{NamespaceDropFlags, NamespaceService as _},
                        tables::TablesService as _,
                        views::ViewService as _,
                    },
                },
                management::v1::{
                    ApiServer,
                    check::UserOrRole,
                    grant::{ApplyGrantsRequest, GrantEntry, Service as _},
                    table::TableManagementService as _,
                    tasks::{
                        ControlTaskAction, ControlTasksRequest, ListTasksRequest, Service as _,
                    },
                    warehouse::TabularDeleteProfile,
                },
            },
            server::CatalogServer,
            service::{
                CatalogNamespaceOps as _, CatalogTabularOps as _, NamespaceId, State, TableId,
                TabularListFlags, UserId,
                tasks::{TaskId, tabular_expiration_queue::QUEUE_NAME as EXPIRATION_QUEUE_NAME},
            },
        };
        use lakekeeper_authz_openfga::{
            OpenFGAAuthorizer, new_authorizer_in_empty_store_from_default_config,
        };
        use lakekeeper_integration_tests::{
            SetupTestCatalog, create_table_request, create_view_request, grant_on_namespace,
            memory_io_profile, principal_metadata,
        };
        use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
        use sqlx::PgPool;

        type Ctx = ApiContext<State<OpenFGAAuthorizer, PostgresBackend, SecretsState>>;

        struct Setup {
            ctx: Ctx,
            admin: RequestMetadata,
            project_id: Arc<ProjectId>,
            warehouse_id: WarehouseId,
        }

        fn namespace() -> NamespaceIdent {
            NamespaceIdent::new("ns".to_string())
        }

        fn ident(name: &str) -> TableIdent {
            TableIdent::new(namespace(), name.to_string())
        }

        impl Setup {
            /// Namespace `ns` with tables `t1`, `t2` and view `v`, created by the admin.
            async fn new(pool: PgPool, profile: TabularDeleteProfile) -> Self {
                let authorizer = new_authorizer_in_empty_store_from_default_config()
                    .await
                    .expect("OpenFGA must be reachable at LAKEKEEPER__OPENFGA__ENDPOINT");
                let admin = UserId::new_unchecked("oidc", "admin");
                let (ctx, warehouse) = SetupTestCatalog::builder()
                    .pool(pool)
                    .storage_profile(memory_io_profile())
                    .authorizer(authorizer)
                    .delete_profile(profile)
                    .user_id(Some(admin.clone()))
                    .number_of_warehouses(1)
                    .build()
                    .setup()
                    .await;
                let s = Self {
                    admin: principal_metadata(&admin, &warehouse.project_id),
                    ctx,
                    project_id: warehouse.project_id,
                    warehouse_id: warehouse.warehouse_id,
                };
                CatalogServer::create_namespace(
                    s.prefix(),
                    CreateNamespaceRequest {
                        namespace: namespace(),
                        properties: None,
                    },
                    s.ctx.clone(),
                    s.admin.clone(),
                )
                .await
                .unwrap();
                for table in ["t1", "t2"] {
                    CatalogServer::create_table(
                        s.ns_params(),
                        create_table_request(Some(table.to_string()), Some(false)),
                        DataAccess::not_specified(),
                        s.ctx.clone(),
                        s.admin.clone(),
                    )
                    .await
                    .unwrap();
                }
                CatalogServer::create_view(
                    s.ns_params(),
                    create_view_request(Some("v"), None),
                    s.ctx.clone(),
                    DataAccess::not_specified(),
                    s.admin.clone(),
                )
                .await
                .unwrap();
                s
            }

            fn prefix(&self) -> Option<Prefix> {
                Some(Prefix(self.warehouse_id.to_string()))
            }

            fn ns_params(&self) -> NamespaceParameters {
                NamespaceParameters {
                    prefix: self.prefix(),
                    namespace: namespace(),
                }
            }

            async fn namespace_id(&self) -> NamespaceId {
                PostgresBackend::get_namespace(
                    self.warehouse_id,
                    namespace(),
                    self.ctx.v1_state.catalog.clone(),
                )
                .await
                .unwrap()
                .unwrap()
                .namespace_id()
            }

            /// A principal with `modify` and `create` on the namespace.
            async fn namespace_modifier(&self) -> RequestMetadata {
                let user = UserId::new_unchecked("oidc", "namespace-modifier");
                let namespace_id = self.namespace_id().await;
                for privilege in ["modify", "create"] {
                    grant_on_namespace(
                        &self.ctx,
                        &self.admin,
                        self.warehouse_id,
                        namespace_id,
                        privilege,
                        &user,
                    )
                    .await;
                }
                principal_metadata(&user, &self.project_id)
            }

            /// A principal with `modify` and `create` on the warehouse.
            async fn warehouse_modifier(&self) -> RequestMetadata {
                let user = UserId::new_unchecked("oidc", "warehouse-modifier");
                ApiServer::<PostgresBackend, OpenFGAAuthorizer, SecretsState>::apply_warehouse_grants(
                    self.warehouse_id,
                    self.ctx.clone(),
                    self.admin.clone(),
                    ApplyGrantsRequest {
                        writes: ["modify", "create"]
                            .into_iter()
                            .map(|privilege| GrantEntry {
                                privilege: privilege.to_string(),
                                principal: UserOrRole::User(user.clone()),
                            })
                            .collect(),
                        deletes: vec![],
                    },
                )
                .await
                .unwrap();
                principal_metadata(&user, &self.project_id)
            }

            async fn drop_table(
                &self,
                md: &RequestMetadata,
                name: &str,
                force: bool,
            ) -> Result<(), ErrorModel> {
                CatalogServer::drop_table(
                    TableParameters {
                        prefix: self.prefix(),
                        table: ident(name),
                    },
                    DropParams {
                        purge_requested: false,
                        force,
                    },
                    self.ctx.clone(),
                    md.clone(),
                )
                .await
                .map_err(|e| e.error)
            }

            async fn drop_view(
                &self,
                md: &RequestMetadata,
                name: &str,
                force: bool,
            ) -> Result<(), ErrorModel> {
                CatalogServer::drop_view(
                    ViewParameters {
                        prefix: self.prefix(),
                        view: ident(name),
                    },
                    DropParams {
                        purge_requested: false,
                        force,
                    },
                    self.ctx.clone(),
                    md.clone(),
                )
                .await
                .map_err(|e| e.error)
            }

            async fn delete_namespace_recursively(
                &self,
                md: &RequestMetadata,
            ) -> Result<(), ErrorModel> {
                CatalogServer::drop_namespace(
                    self.ns_params(),
                    NamespaceDropFlags {
                        force: true,
                        purge: false,
                        recursive: true,
                    },
                    self.ctx.clone(),
                    md.clone(),
                )
                .await
                .map_err(|e| e.error)
            }

            /// A metadata file of a different table: the admin creates `name`, drops it with
            /// `force` and gets its id and metadata location. The registered table takes the
            /// source's id, so the source must be gone.
            async fn source(&self, name: &str) -> (TableId, String) {
                let source = CatalogServer::create_table(
                    self.ns_params(),
                    create_table_request(Some(name.to_string()), Some(false)),
                    DataAccess::not_specified(),
                    self.ctx.clone(),
                    self.admin.clone(),
                )
                .await
                .unwrap();
                self.drop_table(&self.admin, name, true).await.unwrap();
                (
                    source.metadata.uuid().into(),
                    source.metadata_location.unwrap(),
                )
            }

            /// Registers the current metadata file of `name` over itself: the same table UUID.
            async fn register_overwrite(
                &self,
                md: &RequestMetadata,
                name: &str,
            ) -> Result<(), ErrorModel> {
                let metadata_location = PostgresBackend::get_table_info(
                    self.warehouse_id,
                    ident(name),
                    TabularListFlags::active(),
                    self.ctx.v1_state.catalog.clone(),
                )
                .await
                .unwrap()
                .unwrap()
                .metadata_location
                .unwrap();
                self.register_overwrite_from(md, name, metadata_location.to_string())
                    .await
            }

            async fn register_overwrite_from(
                &self,
                md: &RequestMetadata,
                name: &str,
                metadata_location: String,
            ) -> Result<(), ErrorModel> {
                CatalogServer::register_table(
                    self.ns_params(),
                    RegisterTableRequest::builder()
                        .name(name.to_string())
                        .metadata_location(metadata_location)
                        .overwrite(true)
                        .build(),
                    DataAccess::not_specified(),
                    self.ctx.clone(),
                    md.clone(),
                )
                .await
                .map(|_| ())
                .map_err(|e| e.error)
            }

            async fn run_now(
                &self,
                md: &RequestMetadata,
                task_ids: Vec<TaskId>,
            ) -> Result<(), ErrorModel> {
                ApiServer::control_tasks(
                    self.warehouse_id,
                    ControlTasksRequest {
                        action: ControlTaskAction::RunNow,
                        task_ids,
                    },
                    self.ctx.clone(),
                    md.clone(),
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
                    self.admin.clone(),
                )
                .await
                .unwrap()
                .tasks
                .into_iter()
                .map(|t| t.task_id)
                .collect()
            }

            async fn table_is_active(&self, name: &str) -> bool {
                PostgresBackend::get_table_info(
                    self.warehouse_id,
                    ident(name),
                    TabularListFlags::active(),
                    self.ctx.v1_state.catalog.clone(),
                )
                .await
                .unwrap()
                .is_some()
            }

            async fn table_id(&self, name: &str) -> TableId {
                PostgresBackend::get_table_info(
                    self.warehouse_id,
                    ident(name),
                    TabularListFlags::active(),
                    self.ctx.v1_state.catalog.clone(),
                )
                .await
                .unwrap()
                .unwrap()
                .tabular_id
            }
        }

        #[track_caller]
        fn assert_needs_modify_soft_deletion(what: &str, result: Result<(), ErrorModel>) {
            let err = result.expect_err(what);
            assert_eq!(err.code, 403, "{what}: {err:?}");
            assert_eq!(err.r#type, "WarehouseActionForbidden", "{what}: {err:?}");
        }

        /// Registering needs `modify` on the warehouse, which also allows skipping soft
        /// deletion, so a principal without it is refused before any soft-deletion check.
        #[track_caller]
        fn assert_needs_register_right(s: &Setup, what: &str, result: Result<(), ErrorModel>) {
            let err = result.expect_err(what);
            assert_eq!(err.code, 403, "{what}: {err:?}");
            assert_eq!(
                err.message,
                format!(
                    "Warehouse action `can_register_table` forbidden on warehouse `{}`",
                    s.warehouse_id
                ),
                "{what}: {err:?}"
            );
        }

        #[sqlx::test]
        async fn namespace_modify_cannot_skip_soft_deletion(pool: PgPool) {
            let s = Setup::new(pool, Setup::soft()).await;
            let (_, source_location) = s.source("source").await;
            let md = s.namespace_modifier().await;
            let t1 = s.table_id("t1").await;

            assert_needs_modify_soft_deletion(
                "forced table drop",
                s.drop_table(&md, "t1", true).await,
            );
            assert_needs_modify_soft_deletion(
                "forced view drop",
                s.drop_view(&md, "v", true).await,
            );
            assert_needs_register_right(
                &s,
                "register overwrite with a different table",
                s.register_overwrite_from(&md, "t1", source_location).await,
            );
            assert_needs_register_right(
                &s,
                "register overwrite with the same table",
                s.register_overwrite(&md, "t1").await,
            );
            assert_eq!(s.table_id("t1").await, t1);

            // A drop without force keeps the window.
            s.drop_table(&md, "t2", false).await.unwrap();
            let tasks = s.soft_deletion_tasks().await;
            assert_eq!(tasks.len(), 1);
            assert_needs_modify_soft_deletion("run now", s.run_now(&md, tasks).await);

            assert_needs_modify_soft_deletion(
                "recursive namespace delete",
                s.delete_namespace_recursively(&md).await,
            );
            assert!(s.table_is_active("t1").await);
        }

        #[sqlx::test]
        async fn warehouse_modify_can_skip_soft_deletion(pool: PgPool) {
            let s = Setup::new(pool, Setup::soft()).await;
            let (source_id, source_location) = s.source("source").await;
            let md = s.warehouse_modifier().await;

            s.register_overwrite_from(&md, "t1", source_location)
                .await
                .unwrap();
            assert_eq!(s.table_id("t1").await, source_id);
            s.drop_view(&md, "v", true).await.unwrap();
            s.drop_table(&md, "t2", false).await.unwrap();
            s.run_now(&md, s.soft_deletion_tasks().await).await.unwrap();
            s.drop_table(&md, "t1", true).await.unwrap();
            assert!(!s.table_is_active("t1").await);
        }

        #[sqlx::test]
        async fn warehouse_modify_deletes_namespace_recursively(pool: PgPool) {
            let s = Setup::new(pool, Setup::soft()).await;
            let md = s.warehouse_modifier().await;

            s.delete_namespace_recursively(&md).await.unwrap();
            assert!(!s.table_is_active("t1").await);
        }

        #[sqlx::test]
        async fn namespace_modify_force_drops_in_hard_deletion_warehouse(pool: PgPool) {
            let s = Setup::new(pool, TabularDeleteProfile::Hard {}).await;
            ApiServer::set_table_protection(
                s.table_id("t1").await,
                s.warehouse_id,
                true,
                s.ctx.clone(),
                s.admin.clone(),
            )
            .await
            .unwrap();
            let md = s.namespace_modifier().await;

            s.drop_table(&md, "t1", true).await.unwrap();
            s.drop_view(&md, "v", true).await.unwrap();
            assert_needs_register_right(
                &s,
                "register overwrite",
                s.register_overwrite(&md, "t2").await,
            );
            s.delete_namespace_recursively(&md).await.unwrap();
            assert!(!s.table_is_active("t2").await);
        }

        impl Setup {
            fn soft() -> TabularDeleteProfile {
                TabularDeleteProfile::Soft {
                    expiration_seconds: chrono::Duration::seconds(3600),
                }
            }
        }
    }
}
