//! Renaming a tabular **across namespaces** end-to-end against a real OpenFGA store plus a
//! Postgres catalog.
//!
//! A rename that changes the namespace changes which grants the tabular inherits. Nothing
//! else in the suite can show that:
//!
//! * `server_tables_postgres.rs` / `server_views_rename.rs` drive the same endpoints with
//!   `AllowAllAuthorizer` / `HidingAuthorizer`, which model no hierarchy at all — a table
//!   left pointing at the namespace it came from is invisible there.
//! * `authz/openfga/v4.10/store.fga.yaml` pins the model's inheritance but runs no Rust, so
//!   it cannot see that the endpoint never re-points the edge.
//! * `authz-openfga`'s own hook tests exercise the tuple writes directly, without the
//!   endpoint that has to call them.
//!
//! What is left, and what this file covers, is the round trip that the reported bug lived
//! in: a principal granted only on the source namespace must lose the tabular when it is
//! moved out, and a principal granted only on the destination must gain it.
//!
//! Gated behind the `openfga_integration_tests` module so the default nextest filter
//! excludes it; runs under `--profile ci` with a live OpenFGA at
//! `LAKEKEEPER__OPENFGA__ENDPOINT`.

// Nested one level deep so the test path contains `::openfga_integration_tests::`,
// which the default nextest filter excludes (a root module would not match).
mod rename_tabular {
    mod openfga_integration_tests {
        use std::sync::Arc;

        use iceberg::{NamespaceIdent, TableIdent};
        use lakekeeper::{
            ProjectId, WarehouseId,
            api::{
                ApiContext, RequestMetadata, RequestMetadataTestBuilder,
                data::v1::generic_tables::{
                    CreateGenericTableRequest, GenericTableParameters, GenericTableService as _,
                    RenameGenericTableRequest, RenameGenericTableTarget,
                },
                iceberg::{
                    types::Prefix,
                    v1::{
                        CreateNamespaceRequest, DataAccess, NamespaceParameters,
                        RenameTableRequest, TableParameters, ViewParameters,
                        namespace::NamespaceService as _,
                        tables::{LoadTableRequest, TablesService as _},
                        views::{LoadViewRequest, ViewService as _},
                    },
                },
                management::v1::{
                    ApiServer,
                    check::UserOrRole,
                    grant::{ApplyGrantsRequest, GrantEntry, Service as _},
                },
            },
            server::CatalogServer,
            service::{
                AuthZTableInfo as _, CatalogNamespaceOps as _, CatalogStore,
                CatalogTabularOps as _, GenericTableFormat, NamespaceId, State, TableId,
                TabularListFlags, Transaction as _, UserId, authn::Actor, authz::Authorizer as _,
            },
        };
        use lakekeeper_authz_openfga::{
            OpenFGAAuthorizer, RECONCILE_LOCK_KEY, ReconcileMode,
            new_authorizer_in_empty_store_from_default_config,
            reconcile_hierarchy_tuples_from_catalog,
        };
        use lakekeeper_integration_tests::{
            SetupTestCatalog, create_table_request, create_view_request, memory_io_profile,
        };
        use lakekeeper_storage_postgres::{PostgresAdvisoryLock, PostgresBackend, SecretsState};
        use sqlx::PgPool;

        type Ctx = ApiContext<State<OpenFGAAuthorizer, PostgresBackend, SecretsState>>;
        type Server = ApiServer<PostgresBackend, OpenFGAAuthorizer, SecretsState>;

        /// OpenFGA-backed context with a freshly-migrated, isolated store, bootstrapping
        /// `admin` as operator.
        async fn setup(pool: PgPool) -> (Ctx, UserId, Arc<ProjectId>, WarehouseId) {
            let authorizer = new_authorizer_in_empty_store_from_default_config()
                .await
                .expect("OpenFGA must be reachable at LAKEKEEPER__OPENFGA__ENDPOINT");
            let admin = UserId::new_unchecked("oidc", "admin");
            let (ctx, warehouse) = SetupTestCatalog::builder()
                .pool(pool)
                .storage_profile(memory_io_profile())
                .authorizer(authorizer)
                .user_id(Some(admin.clone()))
                .number_of_warehouses(1)
                .build()
                .setup()
                .await;
            (ctx, admin, warehouse.project_id, warehouse.warehouse_id)
        }

        fn metadata(user_id: &UserId, project_id: &ProjectId) -> RequestMetadata {
            RequestMetadataTestBuilder::builder()
                .actor(Actor::Principal(user_id.clone()))
                .project_id(Some(project_id.clone().into()))
                .build()
        }

        fn ns(name: &str) -> NamespaceIdent {
            NamespaceIdent::new(name.to_string())
        }

        async fn create_namespace(
            ctx: &Ctx,
            md: &RequestMetadata,
            warehouse_id: WarehouseId,
            name: &str,
        ) -> NamespaceId {
            CatalogServer::create_namespace(
                Some(Prefix(warehouse_id.to_string())),
                CreateNamespaceRequest {
                    namespace: ns(name),
                    properties: None,
                },
                ctx.clone(),
                md.clone(),
            )
            .await
            .unwrap();
            PostgresBackend::get_namespace(warehouse_id, ns(name), ctx.v1_state.catalog.clone())
                .await
                .unwrap()
                .unwrap()
                .namespace_id()
        }

        async fn create_table(
            ctx: &Ctx,
            md: &RequestMetadata,
            warehouse_id: WarehouseId,
            namespace: &str,
            name: &str,
        ) {
            CatalogServer::create_table(
                NamespaceParameters {
                    prefix: Some(Prefix(warehouse_id.to_string())),
                    namespace: ns(namespace),
                },
                create_table_request(Some(name.to_string()), Some(false)),
                DataAccess::not_specified(),
                ctx.clone(),
                md.clone(),
            )
            .await
            .unwrap();
        }

        /// Grant one privilege on a namespace, through the same endpoint an operator uses.
        async fn grant_on_namespace(
            ctx: &Ctx,
            md: &RequestMetadata,
            warehouse_id: WarehouseId,
            namespace_id: NamespaceId,
            privilege: &str,
            user: &UserId,
        ) {
            Server::apply_namespace_grants(
                warehouse_id,
                namespace_id,
                ctx.clone(),
                md.clone(),
                ApplyGrantsRequest {
                    writes: vec![GrantEntry {
                        privilege: privilege.to_string(),
                        principal: UserOrRole::User(user.clone()),
                    }],
                    deletes: vec![],
                },
            )
            .await
            .unwrap();
        }

        /// Collapse a load into allowed / denied, failing the test on any other error.
        ///
        /// Without this, an assertion that access is *denied* would be satisfied just as
        /// well by a 500 or a contract-verification failure. Lakekeeper hides tabulars a
        /// principal may not see, so a denial arrives as 404 (403 is accepted here for the
        /// paths that surface it directly).
        ///
        /// It does not rule out the *other* vacuity: a tabular that is simply absent is
        /// also 404. Every denial assertion below is therefore paired with a load that
        /// must still succeed, which is what proves the tabular is there to be denied.
        fn denied_or_panic(err: &lakekeeper::api::ErrorModel, what: &str) -> bool {
            assert!(
                err.code == 403 || err.code == 404,
                "{what}: expected a 403/404 authorization outcome, got {err:?}"
            );
            false
        }

        async fn can_load_table(
            ctx: &Ctx,
            md: &RequestMetadata,
            warehouse_id: WarehouseId,
            namespace: &str,
            name: &str,
        ) -> bool {
            match CatalogServer::load_table(
                TableParameters {
                    prefix: Some(Prefix(warehouse_id.to_string())),
                    table: TableIdent::new(ns(namespace), name.to_string()),
                },
                LoadTableRequest::default(),
                ctx.clone(),
                md.clone(),
            )
            .await
            {
                Ok(_) => true,
                Err(e) => denied_or_panic(&e.error, &format!("load table {namespace}.{name}")),
            }
        }

        async fn can_load_view(
            ctx: &Ctx,
            md: &RequestMetadata,
            warehouse_id: WarehouseId,
            namespace: &str,
            name: &str,
        ) -> bool {
            match CatalogServer::load_view(
                ViewParameters {
                    prefix: Some(Prefix(warehouse_id.to_string())),
                    view: TableIdent::new(ns(namespace), name.to_string()),
                },
                LoadViewRequest::default(),
                ctx.clone(),
                md.clone(),
            )
            .await
            {
                Ok(_) => true,
                Err(e) => denied_or_panic(&e.error, &format!("load view {namespace}.{name}")),
            }
        }

        async fn can_load_generic_table(
            ctx: &Ctx,
            md: &RequestMetadata,
            warehouse_id: WarehouseId,
            namespace: &str,
            name: &str,
        ) -> bool {
            match CatalogServer::load_generic_table(
                GenericTableParameters {
                    prefix: Some(Prefix(warehouse_id.to_string())),
                    namespace: ns(namespace),
                    table_name: name.to_string(),
                },
                ctx.clone(),
                DataAccess::not_specified(),
                md.clone(),
            )
            .await
            {
                Ok(_) => true,
                Err(e) => {
                    denied_or_panic(&e.error, &format!("load generic table {namespace}.{name}"))
                }
            }
        }

        fn rename(from: (&str, &str), to: (&str, &str)) -> RenameTableRequest {
            RenameTableRequest {
                source: TableIdent::new(ns(from.0), from.1.to_string()),
                destination: TableIdent::new(ns(to.0), to.1.to_string()),
            }
        }

        /// Reconcile in dry-run: reports what it *would* change without touching anything.
        ///
        /// Only `tuples_deleted` is a drift signal — the additive pass pushes every tuple
        /// the catalog implies and lets OpenFGA dedupe, so `tuples_submitted` is non-zero
        /// even for a consistent store. `tuples_deleted` counts only tuples OpenFGA holds
        /// and the catalog contradicts, which is exactly a stale parent edge.
        async fn drift_report(ctx: &Ctx) -> lakekeeper_authz_openfga::ReconcileReport {
            reconcile(ctx, ReconcileMode::AddMissingAndDeleteDrift, true).await
        }

        /// The `lakekeeper openfga reconcile` CLI, in-process.
        async fn reconcile(
            ctx: &Ctx,
            mode: ReconcileMode,
            dry_run: bool,
        ) -> lakekeeper_authz_openfga::ReconcileReport {
            let state = ctx.v1_state.catalog.clone();
            // The previous guard releases its session-level lock when sqlx closes the
            // connection, which it does on a spawned task — so a lock taken moments ago can
            // still be held. Retry rather than flake.
            let mut lock = None;
            for _ in 0..50 {
                lock = PostgresAdvisoryLock::try_acquire(&state, RECONCILE_LOCK_KEY)
                    .await
                    .expect("acquire reconcile lock");
                if lock.is_some() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            let lock = lock.expect("reconcile lock free within 5s");
            reconcile_hierarchy_tuples_from_catalog::<PostgresBackend>(
                state,
                lock,
                ctx.v1_state.authz.client(),
                ctx.v1_state.authz.server_id(),
                mode,
                dry_run,
            )
            .await
            .expect("reconcile")
        }

        /// Rename in the catalog *only*, leaving the authorizer untouched — what every
        /// release before this fix did. Used to manufacture the legacy drift that operators
        /// have to repair after upgrading.
        async fn rename_in_catalog_only(
            ctx: &Ctx,
            warehouse_id: WarehouseId,
            table_id: TableId,
            source_namespace_id: NamespaceId,
            from: (&str, &str),
            to: (&str, &str),
        ) {
            let mut t = <PostgresBackend as CatalogStore>::Transaction::begin_write(
                ctx.v1_state.catalog.clone(),
            )
            .await
            .unwrap();
            PostgresBackend::rename_tabular(
                warehouse_id,
                table_id,
                source_namespace_id,
                &TableIdent::new(ns(from.0), from.1.to_string()),
                &TableIdent::new(ns(to.0), to.1.to_string()),
                t.transaction(),
            )
            .await
            .unwrap();
            t.commit().await.unwrap();
        }

        /// The reported bug, as a test: a developer granted `select` on `before` only must
        /// not keep reading the table after an operator moves it to `after`.
        #[sqlx::test]
        async fn renaming_a_table_across_namespaces_moves_its_inherited_grants(pool: PgPool) {
            let (ctx, admin, project_id, warehouse_id) = setup(pool).await;
            let admin_md = metadata(&admin, &project_id);

            let before = create_namespace(&ctx, &admin_md, warehouse_id, "before").await;
            create_namespace(&ctx, &admin_md, warehouse_id, "after").await;
            create_table(&ctx, &admin_md, warehouse_id, "before", "tbl").await;
            // The control table from the bug report: proves the developer has no blanket
            // access to `after`, so a pass below is about the moved table, not the grant.
            create_table(&ctx, &admin_md, warehouse_id, "after", "no_access").await;

            let developer = UserId::new_unchecked("oidc", "developer");
            let developer_md = metadata(&developer, &project_id);
            for privilege in ["describe", "select"] {
                grant_on_namespace(&ctx, &admin_md, warehouse_id, before, privilege, &developer)
                    .await;
            }

            assert!(
                can_load_table(&ctx, &developer_md, warehouse_id, "before", "tbl").await,
                "precondition: the grant on `before` reaches the table"
            );
            assert!(
                !can_load_table(&ctx, &developer_md, warehouse_id, "after", "no_access").await,
                "precondition: the developer has no access to `after`"
            );

            CatalogServer::rename_table(
                Some(Prefix(warehouse_id.to_string())),
                rename(("before", "tbl"), ("after", "tbl")),
                ctx.clone(),
                admin_md.clone(),
            )
            .await
            .expect("the operator may rename the table");

            assert!(
                !can_load_table(&ctx, &developer_md, warehouse_id, "after", "tbl").await,
                "a table moved out of `before` must not keep the grants it inherited there"
            );
            // The developer is unchanged otherwise: still no access to `after` at large.
            assert!(!can_load_table(&ctx, &developer_md, warehouse_id, "after", "no_access").await);
            // The table is reachable at its new address — so the denial above is a denial,
            // not a 404 for a table that went missing. `admin` owns it, so this says nothing
            // about the attach; `renaming_a_table_into_a_namespace_grants_that_namespaces_privileges`
            // covers that half.
            assert!(can_load_table(&ctx, &admin_md, warehouse_id, "after", "tbl").await);

            let after_drift = drift_report(&ctx).await;
            assert_eq!(
                after_drift.tuples_deleted, 0,
                "the rename must leave no edge to the old namespace behind; {after_drift:?}"
            );
        }

        /// The other direction: moving *into* a namespace must hand over that namespace's
        /// grants. A hook that only deleted the old edge would pass the test above while
        /// leaving the table unreachable by anyone but its owner.
        #[sqlx::test]
        async fn renaming_a_table_into_a_namespace_grants_that_namespaces_privileges(pool: PgPool) {
            let (ctx, admin, project_id, warehouse_id) = setup(pool).await;
            let admin_md = metadata(&admin, &project_id);

            create_namespace(&ctx, &admin_md, warehouse_id, "before").await;
            let after = create_namespace(&ctx, &admin_md, warehouse_id, "after").await;
            create_table(&ctx, &admin_md, warehouse_id, "before", "tbl").await;

            let analyst = UserId::new_unchecked("oidc", "analyst");
            let analyst_md = metadata(&analyst, &project_id);
            for privilege in ["describe", "select"] {
                grant_on_namespace(&ctx, &admin_md, warehouse_id, after, privilege, &analyst).await;
            }

            assert!(
                !can_load_table(&ctx, &analyst_md, warehouse_id, "before", "tbl").await,
                "precondition: a grant on `after` does not reach a table in `before`"
            );

            CatalogServer::rename_table(
                Some(Prefix(warehouse_id.to_string())),
                rename(("before", "tbl"), ("after", "tbl")),
                ctx.clone(),
                admin_md.clone(),
            )
            .await
            .unwrap();

            assert!(
                can_load_table(&ctx, &analyst_md, warehouse_id, "after", "tbl").await,
                "a table moved into `after` must inherit that namespace's grants"
            );

            let after_drift = drift_report(&ctx).await;
            assert_eq!(after_drift.tuples_deleted, 0, "{after_drift:?}");
        }

        /// A rename *within* one namespace changes no hierarchy, so the re-parent is
        /// skipped.
        ///
        /// Like the case-only test below, this cannot discriminate the skip from running
        /// the hook pair — detach then attach of the same namespace converges. What it
        /// pins is that an in-place rename does not cost the table its grants, under any
        /// of those implementations.
        #[sqlx::test]
        async fn renaming_a_table_within_its_namespace_keeps_its_grants(pool: PgPool) {
            let (ctx, admin, project_id, warehouse_id) = setup(pool).await;
            let admin_md = metadata(&admin, &project_id);

            let before = create_namespace(&ctx, &admin_md, warehouse_id, "before").await;
            create_table(&ctx, &admin_md, warehouse_id, "before", "tbl").await;

            let developer = UserId::new_unchecked("oidc", "developer");
            let developer_md = metadata(&developer, &project_id);
            for privilege in ["describe", "select"] {
                grant_on_namespace(&ctx, &admin_md, warehouse_id, before, privilege, &developer)
                    .await;
            }

            CatalogServer::rename_table(
                Some(Prefix(warehouse_id.to_string())),
                rename(("before", "tbl"), ("before", "renamed")),
                ctx.clone(),
                admin_md.clone(),
            )
            .await
            .unwrap();

            assert!(
                can_load_table(&ctx, &developer_md, warehouse_id, "before", "renamed").await,
                "an in-place rename must not disturb the grants the table inherits"
            );

            let after_drift = drift_report(&ctx).await;
            assert_eq!(after_drift.tuples_deleted, 0, "{after_drift:?}");
        }

        /// Views ride the same hook through a different endpoint and a different OpenFGA
        /// object type, so they get their own round trip.
        #[sqlx::test]
        async fn renaming_a_view_across_namespaces_moves_its_inherited_grants(pool: PgPool) {
            let (ctx, admin, project_id, warehouse_id) = setup(pool).await;
            let admin_md = metadata(&admin, &project_id);

            let before = create_namespace(&ctx, &admin_md, warehouse_id, "before").await;
            let after = create_namespace(&ctx, &admin_md, warehouse_id, "after").await;
            CatalogServer::create_view(
                NamespaceParameters {
                    prefix: Some(Prefix(warehouse_id.to_string())),
                    namespace: ns("before"),
                },
                create_view_request(Some("v"), None),
                ctx.clone(),
                DataAccess::not_specified(),
                admin_md.clone(),
            )
            .await
            .unwrap();

            let developer = UserId::new_unchecked("oidc", "developer");
            let developer_md = metadata(&developer, &project_id);
            let analyst = UserId::new_unchecked("oidc", "analyst");
            let analyst_md = metadata(&analyst, &project_id);
            for privilege in ["describe", "select"] {
                grant_on_namespace(&ctx, &admin_md, warehouse_id, before, privilege, &developer)
                    .await;
                grant_on_namespace(&ctx, &admin_md, warehouse_id, after, privilege, &analyst).await;
            }

            assert!(
                can_load_view(&ctx, &developer_md, warehouse_id, "before", "v").await,
                "precondition: the grant on `before` reaches the view"
            );
            assert!(
                !can_load_view(&ctx, &analyst_md, warehouse_id, "before", "v").await,
                "precondition: the grant on `after` does not yet reach it"
            );

            CatalogServer::rename_view(
                Some(Prefix(warehouse_id.to_string())),
                rename(("before", "v"), ("after", "v")),
                ctx.clone(),
                admin_md.clone(),
            )
            .await
            .expect("the operator may rename the view");

            assert!(
                !can_load_view(&ctx, &developer_md, warehouse_id, "after", "v").await,
                "a view moved out of `before` must not keep the grants it inherited there"
            );
            assert!(
                can_load_view(&ctx, &analyst_md, warehouse_id, "after", "v").await,
                "and must inherit the grants of the namespace it moved into"
            );

            let after_drift = drift_report(&ctx).await;
            assert_eq!(after_drift.tuples_deleted, 0, "{after_drift:?}");
        }

        /// Remediation for deployments that already renamed tables across namespaces
        /// before this fix: which `lakekeeper openfga reconcile` mode actually repairs it.
        ///
        /// The stale edge is a *surplus* tuple, so the default additive mode cannot remove
        /// it — it adds the destination edge and leaves the source edge live, which leaves
        /// the table inheriting from *both* namespaces. Only `--mode add-and-delete-drift`
        /// closes it. This is asserted rather than documented because the guidance we give
        /// operators is only as good as the mode actually being able to do it.
        #[sqlx::test]
        async fn reconcile_repairs_legacy_rename_drift_only_in_delete_mode(pool: PgPool) {
            let (ctx, admin, project_id, warehouse_id) = setup(pool).await;
            let admin_md = metadata(&admin, &project_id);

            let before = create_namespace(&ctx, &admin_md, warehouse_id, "before").await;
            let after = create_namespace(&ctx, &admin_md, warehouse_id, "after").await;
            create_table(&ctx, &admin_md, warehouse_id, "before", "tbl").await;

            let developer = UserId::new_unchecked("oidc", "developer");
            let developer_md = metadata(&developer, &project_id);
            let analyst = UserId::new_unchecked("oidc", "analyst");
            let analyst_md = metadata(&analyst, &project_id);
            for privilege in ["describe", "select"] {
                grant_on_namespace(&ctx, &admin_md, warehouse_id, before, privilege, &developer)
                    .await;
                grant_on_namespace(&ctx, &admin_md, warehouse_id, after, privilege, &analyst).await;
            }

            let table_id = PostgresBackend::get_table_info(
                warehouse_id,
                TableIdent::new(ns("before"), "tbl".to_string()),
                TabularListFlags::active(),
                ctx.v1_state.catalog.clone(),
            )
            .await
            .unwrap()
            .unwrap()
            .table_id();

            // The pre-fix world: catalog moved, authorizer did not.
            rename_in_catalog_only(
                &ctx,
                warehouse_id,
                table_id,
                before,
                ("before", "tbl"),
                ("after", "tbl"),
            )
            .await;

            assert!(
                can_load_table(&ctx, &developer_md, warehouse_id, "after", "tbl").await,
                "precondition: this is the bug — the stale edge keeps the old grants live"
            );

            // The default mode is purely additive, so it cannot retract the stale edge.
            let additive = reconcile(&ctx, ReconcileMode::AddMissingOnly, false).await;
            assert_eq!(
                additive.tuples_deleted, 0,
                "add-missing must delete nothing, by definition; {additive:?}"
            );
            assert!(
                can_load_table(&ctx, &developer_md, warehouse_id, "after", "tbl").await,
                "`--mode add-missing` cannot repair this drift: it adds the destination \
                 edge and leaves the source edge, so the table inherits from both"
            );

            // The delete mode retracts it.
            let with_deletes =
                reconcile(&ctx, ReconcileMode::AddMissingAndDeleteDrift, false).await;
            assert!(
                with_deletes.tuples_deleted > 0,
                "add-and-delete-drift must retract the stale edge; {with_deletes:?}"
            );
            assert!(
                !can_load_table(&ctx, &developer_md, warehouse_id, "after", "tbl").await,
                "`--mode add-and-delete-drift` must close the inherited access"
            );
            // Not `admin`: it owns the table outright, so it could reach it with no parent
            // edge at all. The analyst holds only a grant on `after`, so this also pins
            // that reconcile's additive pass wrote the destination edge.
            assert!(
                can_load_table(&ctx, &analyst_md, warehouse_id, "after", "tbl").await,
                "and must leave the table inheriting from the namespace it now lives in"
            );

            let residual = drift_report(&ctx).await;
            assert_eq!(residual.tuples_deleted, 0, "{residual:?}");
        }

        /// Generic tables ride the same hook through a third endpoint and a third OpenFGA
        /// object type. They get their own round trip because the wiring is per-endpoint:
        /// this is the copy that reads the source namespace from a struct field rather than
        /// a trait method, so a wrong field here is invisible to the other two tests.
        #[sqlx::test]
        async fn renaming_a_generic_table_across_namespaces_moves_its_inherited_grants(
            pool: PgPool,
        ) {
            let (ctx, admin, project_id, warehouse_id) = setup(pool).await;
            let admin_md = metadata(&admin, &project_id);

            let before = create_namespace(&ctx, &admin_md, warehouse_id, "before").await;
            let after = create_namespace(&ctx, &admin_md, warehouse_id, "after").await;
            CatalogServer::create_generic_table(
                NamespaceParameters {
                    prefix: Some(Prefix(warehouse_id.to_string())),
                    namespace: ns("before"),
                },
                CreateGenericTableRequest {
                    name: "gt".to_string(),
                    format: GenericTableFormat::Unknown("lance".to_string()),
                    base_location: None,
                    doc: None,
                    properties: std::collections::HashMap::default(),
                    schema: None,
                    statistics: None,
                },
                ctx.clone(),
                admin_md.clone(),
            )
            .await
            .unwrap();

            let developer = UserId::new_unchecked("oidc", "developer");
            let developer_md = metadata(&developer, &project_id);
            let analyst = UserId::new_unchecked("oidc", "analyst");
            let analyst_md = metadata(&analyst, &project_id);
            for privilege in ["describe", "select"] {
                grant_on_namespace(&ctx, &admin_md, warehouse_id, before, privilege, &developer)
                    .await;
                grant_on_namespace(&ctx, &admin_md, warehouse_id, after, privilege, &analyst).await;
            }

            assert!(
                can_load_generic_table(&ctx, &developer_md, warehouse_id, "before", "gt").await,
                "precondition: the grant on `before` reaches the generic table"
            );
            assert!(
                !can_load_generic_table(&ctx, &analyst_md, warehouse_id, "before", "gt").await,
                "precondition: the grant on `after` does not yet reach it"
            );

            CatalogServer::rename_generic_table(
                Some(Prefix(warehouse_id.to_string())),
                RenameGenericTableRequest {
                    source: RenameGenericTableTarget {
                        namespace: vec!["before".to_string()],
                        name: "gt".to_string(),
                    },
                    destination: RenameGenericTableTarget {
                        namespace: vec!["after".to_string()],
                        name: "gt".to_string(),
                    },
                },
                ctx.clone(),
                admin_md.clone(),
            )
            .await
            .expect("the operator may rename the generic table");

            assert!(
                !can_load_generic_table(&ctx, &developer_md, warehouse_id, "after", "gt").await,
                "a generic table moved out of `before` must not keep the grants it inherited \
                 there"
            );
            assert!(
                can_load_generic_table(&ctx, &analyst_md, warehouse_id, "after", "gt").await,
                "and must inherit the grants of the namespace it moved into"
            );

            let after_drift = drift_report(&ctx).await;
            assert_eq!(after_drift.tuples_deleted, 0, "{after_drift:?}");
        }

        /// Namespace idents are case-insensitive, so `before` and `BEFORE` are one namespace
        /// — but they are not equal as idents, so the endpoint's `source == destination`
        /// short-circuit does not fire and the request reaches the re-parent logic.
        ///
        /// Detaching and re-attaching the *same* namespace happens to converge under the
        /// detach-then-attach order, so this does not discriminate the id comparison from an
        /// ident one. What it does pin is the order itself: an implementation that attached
        /// the destination before detaching the source would, on this input, attach the
        /// namespace and then immediately delete the edge it just wrote, silently revoking
        /// everyone's inherited access.
        #[sqlx::test]
        async fn renaming_a_table_under_a_differently_cased_namespace_keeps_its_grants(
            pool: PgPool,
        ) {
            let (ctx, admin, project_id, warehouse_id) = setup(pool).await;
            let admin_md = metadata(&admin, &project_id);

            let before = create_namespace(&ctx, &admin_md, warehouse_id, "before").await;
            create_table(&ctx, &admin_md, warehouse_id, "before", "tbl").await;

            let developer = UserId::new_unchecked("oidc", "developer");
            let developer_md = metadata(&developer, &project_id);
            for privilege in ["describe", "select"] {
                grant_on_namespace(&ctx, &admin_md, warehouse_id, before, privilege, &developer)
                    .await;
            }

            CatalogServer::rename_table(
                Some(Prefix(warehouse_id.to_string())),
                rename(("before", "tbl"), ("BEFORE", "renamed")),
                ctx.clone(),
                admin_md.clone(),
            )
            .await
            .expect("a case-only namespace difference names the same namespace");

            assert!(
                can_load_table(&ctx, &developer_md, warehouse_id, "before", "renamed").await,
                "a case-only namespace difference is not a move; the grants must survive"
            );

            let after_drift = drift_report(&ctx).await;
            assert_eq!(after_drift.tuples_deleted, 0, "{after_drift:?}");
        }
    }
}
