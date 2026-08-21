//! Caller-supplied warehouse ids against a **real** OpenFGA store plus a Postgres catalog.
//!
//! `warehouse_ops.rs` covers the same endpoint with `AllowAllAuthorizer`, which writes no
//! tuples and consults no model. That leaves the part of a chosen id that actually carries
//! safety weight untested:
//!
//! * whether deleting a warehouse really frees its id — the whole "an id becomes available
//!   again" behaviour rests on `delete_all_relations` clearing the tuples, and under
//!   `AllowAllAuthorizer` there are none to clear, so the test passes vacuously;
//! * whether a recreated warehouse is owned by whoever recreated it, rather than inheriting
//!   its predecessor's ownership;
//! * what a caller sees when they aim a chosen id at one that still has tuples — the
//!   orphaned-tuple case a create whose commit failed leaves behind.
//!
//! Gated behind the `openfga_integration_tests` module so the default nextest filter
//! excludes it; runs with a live OpenFGA at `LAKEKEEPER__OPENFGA__ENDPOINT`.

// Nested one level deep so the test path contains `::openfga_integration_tests::`,
// which the default nextest filter excludes (a root module would not match).
mod warehouse_id {
    mod openfga_integration_tests {
        use std::sync::Arc;

        use lakekeeper::{
            ProjectId, WarehouseId,
            api::{
                ApiContext, RequestMetadata, RequestMetadataTestBuilder,
                management::v1::{
                    ApiServer, DeleteWarehouseQuery,
                    warehouse::{CreateWarehouseRequest, Service as _, TabularDeleteProfile},
                },
            },
            service::{
                CatalogStore, CatalogWarehouseOps, State, Transaction, UserId, authn::Actor,
                authz::CatalogWarehouseAction,
            },
        };
        use lakekeeper_authz_openfga::{
            OpenFGAAuthorizer, new_authorizer_in_empty_store_from_default_config,
        };
        use lakekeeper_integration_tests::{SetupTestCatalog, memory_io_profile};
        use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
        use sqlx::PgPool;
        use uuid::Uuid;

        type Ctx = ApiContext<State<OpenFGAAuthorizer, PostgresBackend, SecretsState>>;

        /// OpenFGA-backed context with a freshly-migrated, isolated store, bootstrapping
        /// `admin` as operator.
        async fn setup(pool: PgPool) -> (Ctx, UserId, Arc<ProjectId>) {
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
            (ctx, user_id, warehouse.project_id)
        }

        fn metadata(user_id: &UserId, project_id: &ProjectId) -> RequestMetadata {
            RequestMetadataTestBuilder::builder()
                .actor(Actor::Principal(user_id.clone()))
                .project_id(Some(project_id.clone().into()))
                .build()
        }

        fn request(
            name: String,
            warehouse_id: WarehouseId,
            project_id: &ProjectId,
        ) -> CreateWarehouseRequest {
            CreateWarehouseRequest::builder()
                .warehouse_name(name)
                .warehouse_id(warehouse_id)
                .project_id(project_id.clone())
                .storage_profile(memory_io_profile())
                .delete_profile(TabularDeleteProfile::Hard {})
                .build()
        }

        /// Deleting a warehouse frees its id for real: the authorizer's tuple cleanup runs,
        /// so the guard that would otherwise refuse the id is satisfied, and the recreated
        /// warehouse belongs to whoever recreated it.
        #[sqlx::test]
        async fn delete_frees_the_id_and_recreate_takes_fresh_ownership(pool: PgPool) {
            let (ctx, admin, project_id) = setup(pool).await;
            let md = metadata(&admin, &project_id);
            let warehouse_id = WarehouseId::new_random();

            ApiServer::create_warehouse(
                request(
                    format!("recycled-{}", Uuid::now_v7()),
                    warehouse_id,
                    &project_id,
                ),
                ctx.clone(),
                md.clone(),
            )
            .await
            .unwrap();

            ApiServer::delete_warehouse(
                warehouse_id,
                DeleteWarehouseQuery { force: false },
                ctx.clone(),
                md.clone(),
            )
            .await
            .unwrap();

            // The load-bearing assertion: with the predecessor's tuples gone, the create
            // guard passes and the id is usable again. If `delete_all_relations` ever stops
            // clearing them, this fails with `ObjectHasRelations` instead.
            let recreated = ApiServer::create_warehouse(
                request(
                    format!("recycled-{}", Uuid::now_v7()),
                    warehouse_id,
                    &project_id,
                ),
                ctx.clone(),
                md.clone(),
            )
            .await
            .unwrap();
            assert_eq!(recreated.warehouse_id(), warehouse_id);

            // Ownership was written fresh rather than inherited: the recreating principal can
            // act on the warehouse it just made.
            ApiServer::set_warehouse_protection(warehouse_id, true, ctx.clone(), md)
                .await
                .unwrap();
        }

        /// A chosen id whose tuples outlived its warehouse is refused rather than silently
        /// adopting them.
        ///
        /// Reproduces the orphaned-tuple state a create leaves behind when the authorizer
        /// write lands and the transaction then fails to commit, by deleting the warehouse
        /// through the catalog only — skipping the endpoint's authz cleanup.
        #[sqlx::test]
        async fn a_chosen_id_with_leftover_tuples_is_refused(pool: PgPool) {
            let (ctx, admin, project_id) = setup(pool).await;
            let md = metadata(&admin, &project_id);
            let warehouse_id = WarehouseId::new_random();

            ApiServer::create_warehouse(
                request(
                    format!("orphaned-{}", Uuid::now_v7()),
                    warehouse_id,
                    &project_id,
                ),
                ctx.clone(),
                md.clone(),
            )
            .await
            .unwrap();

            // Catalog-only delete: the row goes, the tuples stay.
            let mut t = <PostgresBackend as CatalogStore>::Transaction::begin_write(
                ctx.v1_state.catalog.clone(),
            )
            .await
            .unwrap();
            PostgresBackend::delete_warehouse(
                warehouse_id,
                DeleteWarehouseQuery { force: false },
                t.transaction(),
            )
            .await
            .unwrap();
            t.commit().await.unwrap();

            let err = ApiServer::create_warehouse(
                request(
                    format!("orphaned-{}", Uuid::now_v7()),
                    warehouse_id,
                    &project_id,
                ),
                ctx.clone(),
                md,
            )
            .await
            .unwrap_err();

            assert_eq!(err.error.code, 409, "{:?}", err.error);
            assert_eq!(err.error.r#type, "ObjectHasRelations");

            // Ownership tuples make the warehouse an *object*, so this is the guard's first
            // phase, which never described what it found. The second phase — reached only
            // when the warehouse survives as a *user* of another object, i.e. after a partial
            // cleanup — did, and no longer does; that one is pinned in `authz-openfga`'s
            // `test_require_no_relations_used_in_other_relations`. Asserted here too because
            // the invariant is the same either way: for a 4xx the stack is serialized to the
            // caller verbatim, and a caller aiming at someone else's id must not learn whose
            // project it is.
            let rendered = format!("{:?}", err.error);
            assert!(
                !rendered.contains(&project_id.to_string()),
                "conflict leaked the owning project: {rendered}"
            );
        }

        /// The action used above is a spec mutation, so the ownership assertion in the first
        /// test is meaningful rather than a no-op for any caller.
        #[test]
        fn protection_is_a_spec_mutation() {
            assert!(CatalogWarehouseAction::SetProtection.is_spec_mutation());
        }
    }
}
