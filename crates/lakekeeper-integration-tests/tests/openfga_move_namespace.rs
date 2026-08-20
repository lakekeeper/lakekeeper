//! Moving a namespace end-to-end against a **real** OpenFGA store plus a Postgres catalog.
//!
//! The rest of the move-namespace coverage stops short of this seam:
//!
//! * `namespace_ops.rs` drives the same endpoint but with `AllowAllAuthorizer` /
//!   `HidingAuthorizer`, which never consult a model — a `CatalogNamespaceAction` that maps
//!   to a relation the deployed model does not define would still pass there.
//! * `authz/openfga/v4.9/store.fga.yaml` pins the model's semantics but runs no Rust, so it
//!   cannot catch a wrong action → relation mapping.
//! * `authz-openfga`'s own `openfga_integration_tests` exercise the hook's tuple writes
//!   directly, without the catalog.
//!
//! What is left, and what this file covers, is the round trip: the authorization *check*
//! resolving `can_move` against the real model, and no stale hierarchy edge surviving a real
//! move — asserted with the repo's own drift detector rather than by restating the expected
//! tuples. (That the new edges *land* is pinned by `authz-openfga`'s own hook test, which can
//! read the store directly.)
//!
//! Gated behind the `openfga_integration_tests` module so the default nextest filter
//! excludes it; runs under `--profile ci` with a live OpenFGA at
//! `LAKEKEEPER__OPENFGA__ENDPOINT`.

// Nested one level deep so the test path contains `::openfga_integration_tests::`,
// which the default nextest filter excludes (a root module would not match).
mod move_namespace {
    mod openfga_integration_tests {
        use std::sync::Arc;

        use iceberg::NamespaceIdent;
        use lakekeeper::{
            ProjectId, WarehouseId,
            api::{
                ApiContext, RequestMetadata, RequestMetadataTestBuilder,
                management::v1::{
                    ApiServer,
                    namespace::{MoveNamespaceRequest, NamespaceManagementService as _},
                },
            },
            service::{
                CachePolicy, CatalogNamespaceOps, CatalogStore, CreateNamespaceRequest,
                NamespaceId, State, Transaction, UserId, authn::Actor, authz::Authorizer as _,
            },
        };
        use lakekeeper_authz_openfga::{
            OpenFGAAuthorizer, RECONCILE_LOCK_KEY, ReconcileMode,
            new_authorizer_in_empty_store_from_default_config,
            reconcile_hierarchy_tuples_from_catalog,
        };
        use lakekeeper_integration_tests::{SetupTestCatalog, memory_io_profile};
        use lakekeeper_storage_postgres::{PostgresAdvisoryLock, PostgresBackend, SecretsState};
        use sqlx::PgPool;

        type Ctx = ApiContext<State<OpenFGAAuthorizer, PostgresBackend, SecretsState>>;

        /// OpenFGA-backed context with a freshly-migrated, isolated store, bootstrapping
        /// `admin` as operator.
        async fn setup(pool: PgPool) -> (Ctx, UserId, Arc<ProjectId>, WarehouseId) {
            let authorizer = new_authorizer_in_empty_store_from_default_config()
                .await
                .expect("OpenFGA must be reachable at LAKEKEEPER__OPENFGA__ENDPOINT");
            let user_id = UserId::new_unchecked("oidc", "admin");
            let (ctx, warehouse) = SetupTestCatalog::builder()
                .pool(pool)
                .storage_profile(memory_io_profile())
                .authorizer(authorizer)
                .user_id(Some(user_id.clone()))
                .number_of_warehouses(1)
                .build()
                .setup()
                .await;
            let warehouse_id = warehouse.warehouse_id;
            (ctx, user_id, warehouse.project_id, warehouse_id)
        }

        fn metadata(user_id: &UserId, project_id: &ProjectId) -> RequestMetadata {
            RequestMetadataTestBuilder::builder()
                .actor(Actor::Principal(user_id.clone()))
                .project_id(Some(project_id.clone().into()))
                .build()
        }

        fn ns_ident(parts: &[&str]) -> NamespaceIdent {
            NamespaceIdent::from_vec(parts.iter().map(ToString::to_string).collect()).unwrap()
        }

        /// Create a namespace through the catalog *and* the authorizer hook, so its hierarchy
        /// and ownership tuples exist in OpenFGA — matching what the create endpoint does.
        async fn create_ns(
            ctx: &Ctx,
            md: &RequestMetadata,
            warehouse_id: WarehouseId,
            parts: &[&str],
        ) -> NamespaceId {
            use lakekeeper::service::authz::NamespaceParent;

            let ident = ns_ident(parts);
            let mut t = <PostgresBackend as CatalogStore>::Transaction::begin_write(
                ctx.v1_state.catalog.clone(),
            )
            .await
            .unwrap();
            let created = PostgresBackend::create_namespace(
                warehouse_id,
                NamespaceId::new_random(),
                CreateNamespaceRequest {
                    namespace: ident.clone(),
                    properties: None,
                },
                t.transaction(),
            )
            .await
            .unwrap();
            t.commit().await.unwrap();

            let parent = created.parent_namespaces_id().map_or(
                NamespaceParent::Warehouse(warehouse_id),
                NamespaceParent::Namespace,
            );
            ctx.v1_state
                .authz
                .create_namespace(md, created.namespace_id(), parent)
                .await
                .unwrap();

            created.namespace_id()
        }

        /// Reconcile in dry-run: reports what it *would* change without touching anything.
        ///
        /// Only `tuples_deleted` is a drift signal. The additive pass
        /// (`write_missing_from_index`) pushes every tuple the catalog implies and relies on
        /// OpenFGA's idempotent write to dedupe, so `tuples_submitted` is non-zero even for a
        /// perfectly consistent store. `tuples_deleted` comes from the diff walk, which
        /// deletes only the tuples OpenFGA holds and the catalog contradicts — precisely the
        /// stale-edge failure mode a move can produce.
        async fn drift_report(
            ctx: &Ctx,
            pool: &PgPool,
        ) -> lakekeeper_authz_openfga::ReconcileReport {
            let state = ctx.v1_state.catalog.clone();
            let lock = PostgresAdvisoryLock::try_acquire(&state, RECONCILE_LOCK_KEY)
                .await
                .expect("acquire reconcile lock")
                .expect("reconcile lock free");
            let _ = pool;
            reconcile_hierarchy_tuples_from_catalog::<PostgresBackend>(
                state,
                lock,
                ctx.v1_state.authz.client(),
                ctx.v1_state.authz.server_id(),
                ReconcileMode::AddMissingAndDeleteDrift,
                true,
            )
            .await
            .expect("reconcile dry-run")
        }

        /// Re-parenting through the endpoint leaves the catalog and OpenFGA in agreement.
        ///
        /// Uses reconcile's own drift detection as the oracle: if the hook wrote the wrong
        /// edges, failed to delete the old ones, or was skipped, reconcile would report
        /// tuples it needs to add or delete.
        #[sqlx::test]
        async fn move_namespace_leaves_no_authz_drift(pool: PgPool) {
            let (ctx, admin, project_id, warehouse_id) = setup(pool.clone()).await;
            let md = metadata(&admin, &project_id);

            create_ns(&ctx, &md, warehouse_id, &["src"]).await;
            create_ns(&ctx, &md, warehouse_id, &["dst"]).await;
            let movable = create_ns(&ctx, &md, warehouse_id, &["src", "movable"]).await;

            let before = drift_report(&ctx, &pool).await;
            assert_eq!(
                before.tuples_deleted, 0,
                "precondition: a freshly built store holds no contradicted tuples; {before:?}"
            );

            ApiServer::move_namespace(
                movable,
                warehouse_id,
                MoveNamespaceRequest {
                    destination: ns_ident(&["dst", "movable"]),
                    force: false,
                },
                ctx.clone(),
                md.clone(),
            )
            .await
            .expect("the operator may move a namespace");

            let after = drift_report(&ctx, &pool).await;
            assert_eq!(
                after.tuples_deleted, 0,
                "the hook must leave no edge to the old parent behind; {after:?}"
            );

            // And the catalog really did move it.
            let reloaded = PostgresBackend::get_namespace_cache_aware(
                warehouse_id,
                movable,
                CachePolicy::Skip,
                ctx.v1_state.catalog.clone(),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(reloaded.namespace_ident(), &ns_ident(&["dst", "movable"]));
        }

        /// A move to the warehouse root crosses parent *kinds*, so the inverse relation
        /// changes from `child` to `namespace`. Re-checked here against the real model
        /// because getting this wrong is invisible to a fake authorizer.
        #[sqlx::test]
        async fn move_namespace_to_warehouse_root_leaves_no_authz_drift(pool: PgPool) {
            let (ctx, admin, project_id, warehouse_id) = setup(pool.clone()).await;
            let md = metadata(&admin, &project_id);

            create_ns(&ctx, &md, warehouse_id, &["parent"]).await;
            let movable = create_ns(&ctx, &md, warehouse_id, &["parent", "movable"]).await;

            ApiServer::move_namespace(
                movable,
                warehouse_id,
                MoveNamespaceRequest {
                    destination: ns_ident(&["movable"]),
                    force: false,
                },
                ctx.clone(),
                md.clone(),
            )
            .await
            .unwrap();

            let after = drift_report(&ctx, &pool).await;
            assert_eq!(
                after.tuples_deleted, 0,
                "moving to the root must leave no namespace-parent edge behind; {after:?}"
            );
        }

        /// `can_move` is resolved against the deployed model, so a principal with no grants
        /// is denied. If `can_move` were missing from the model the check would error rather
        /// than cleanly deny, and if it were defined too permissively this would pass the
        /// move through.
        #[sqlx::test]
        async fn move_namespace_denied_for_unprivileged_principal(pool: PgPool) {
            let (ctx, admin, project_id, warehouse_id) = setup(pool.clone()).await;
            let admin_md = metadata(&admin, &project_id);

            create_ns(&ctx, &admin_md, warehouse_id, &["src"]).await;
            create_ns(&ctx, &admin_md, warehouse_id, &["dst"]).await;
            let movable = create_ns(&ctx, &admin_md, warehouse_id, &["src", "movable"]).await;

            let stranger = UserId::new_unchecked("oidc", "stranger");
            let stranger_md = metadata(&stranger, &project_id);

            let err = ApiServer::move_namespace(
                movable,
                warehouse_id,
                MoveNamespaceRequest {
                    destination: ns_ident(&["dst", "movable"]),
                    force: false,
                },
                ctx.clone(),
                stranger_md,
            )
            .await
            .expect_err("a principal without grants must not move a namespace");
            assert!(
                err.error.code == 403 || err.error.code == 404,
                "expected forbidden or not-found, got {err:?}"
            );

            // Nothing moved, and no authorization state was touched.
            let reloaded = PostgresBackend::get_namespace_cache_aware(
                warehouse_id,
                movable,
                CachePolicy::Skip,
                ctx.v1_state.catalog.clone(),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(reloaded.namespace_ident(), &ns_ident(&["src", "movable"]));

            let after = drift_report(&ctx, &pool).await;
            assert_eq!(after.tuples_deleted, 0);
        }
    }
}
