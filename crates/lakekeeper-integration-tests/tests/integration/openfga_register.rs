//! Registering a table against a real OpenFGA store plus a Postgres catalog. A table dropped
//! without purging leaves its files behind, and a register adopts them. `create` on a
//! namespace alone must not be enough for that: registering needs `modify` on the warehouse.
//!
//! Gated behind the `openfga_integration_tests` module so the default nextest filter
//! excludes it; runs under `--profile ci` with a live OpenFGA at
//! `LAKEKEEPER__OPENFGA__ENDPOINT`.

// Nested one level deep so the test path contains `::openfga_integration_tests::`,
// which the default nextest filter excludes (a root module would not match).
mod register {
    mod openfga_integration_tests {
        use iceberg::{NamespaceIdent, TableIdent};
        use iceberg_ext::catalog::rest::RegisterTableRequest;
        use lakekeeper::{
            api::{
                iceberg::{
                    types::{DropParams, Prefix},
                    v1::{
                        CreateNamespaceRequest, DataAccess, NamespaceParameters, TableParameters,
                        namespace::NamespaceService as _,
                        tables::{
                            LoadTableRequest, LoadTableResultOrNotModified, TablesService as _,
                        },
                    },
                },
                management::v1::{
                    ApiServer,
                    check::UserOrRole,
                    grant::{ApplyGrantsRequest, GrantEntry, Service as _},
                },
            },
            server::CatalogServer,
            service::{CatalogNamespaceOps as _, UserId},
        };
        use lakekeeper_authz_openfga::new_authorizer_in_empty_store_from_default_config;
        use lakekeeper_integration_tests::{
            SetupTestCatalog, create_table_request, grant_on_namespace, memory_io_profile,
            principal_metadata,
        };
        use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
        use sqlx::PgPool;

        #[sqlx::test]
        async fn test_create_alone_cannot_adopt_a_dropped_table(pool: PgPool) {
            let authorizer = new_authorizer_in_empty_store_from_default_config()
                .await
                .expect("OpenFGA must be reachable at LAKEKEEPER__OPENFGA__ENDPOINT");
            let admin = UserId::new_unchecked("oidc", "admin");
            let (ctx, warehouse) = SetupTestCatalog::builder()
                .pool(pool)
                .storage_profile(memory_io_profile())
                .authorizer(authorizer)
                .user_id(Some(admin.clone()))
                .build()
                .setup()
                .await;
            let warehouse_id = warehouse.warehouse_id;
            let admin_md = principal_metadata(&admin, &warehouse.project_id);
            let namespace = NamespaceIdent::new("ns".to_string());
            let ns_params = NamespaceParameters {
                prefix: Some(Prefix(warehouse_id.to_string())),
                namespace: namespace.clone(),
            };

            CatalogServer::create_namespace(
                ns_params.prefix.clone(),
                CreateNamespaceRequest {
                    namespace: namespace.clone(),
                    properties: None,
                },
                ctx.clone(),
                admin_md.clone(),
            )
            .await
            .unwrap();
            let secret = CatalogServer::create_table(
                ns_params.clone(),
                create_table_request(Some("secret".to_string()), Some(false)),
                DataAccess::not_specified(),
                ctx.clone(),
                admin_md.clone(),
            )
            .await
            .unwrap();
            CatalogServer::drop_table(
                TableParameters {
                    prefix: ns_params.prefix.clone(),
                    table: TableIdent::new(namespace.clone(), "secret".to_string()),
                },
                DropParams {
                    purge_requested: false,
                    force: false,
                },
                ctx.clone(),
                admin_md.clone(),
            )
            .await
            .unwrap();

            let namespace_id = PostgresBackend::get_namespace(
                warehouse_id,
                namespace.clone(),
                ctx.v1_state.catalog.clone(),
            )
            .await
            .unwrap()
            .unwrap()
            .namespace_id();
            let adopter = UserId::new_unchecked("oidc", "adopter");
            grant_on_namespace(
                &ctx,
                &admin_md,
                warehouse_id,
                namespace_id,
                "create",
                &adopter,
            )
            .await;
            let adopter_md = principal_metadata(&adopter, &warehouse.project_id);

            let register = || {
                CatalogServer::register_table(
                    ns_params.clone(),
                    RegisterTableRequest::builder()
                        .name("loot".to_string())
                        .metadata_location(secret.metadata_location.clone().unwrap())
                        .build(),
                    DataAccess::not_specified(),
                    ctx.clone(),
                    adopter_md.clone(),
                )
            };
            let err = register()
                .await
                .expect_err("create on the namespace alone must not register")
                .error;
            assert_eq!(err.code, 403, "{err:?}");
            assert_eq!(err.r#type, "WarehouseActionForbidden", "{err:?}");

            ApiServer::<PostgresBackend, _, SecretsState>::apply_warehouse_grants(
                warehouse_id,
                ctx.clone(),
                admin_md.clone(),
                ApplyGrantsRequest {
                    writes: vec![GrantEntry {
                        privilege: "modify".to_string(),
                        principal: UserOrRole::User(adopter.clone()),
                    }],
                    deletes: vec![],
                },
            )
            .await
            .unwrap();
            let registered = register()
                .await
                .expect("modify on the warehouse allows registering");

            // The registering principal owns the table, so it gets what a load gives it.
            let LoadTableResultOrNotModified::LoadTableResult(loaded) = CatalogServer::load_table(
                TableParameters {
                    prefix: ns_params.prefix.clone(),
                    table: TableIdent::new(namespace, "loot".to_string()),
                },
                LoadTableRequest::default(),
                ctx.clone(),
                adopter_md,
            )
            .await
            .unwrap() else {
                panic!("an unconditional load must not return 304");
            };
            assert_eq!(registered.etag, loaded.etag);
        }
    }
}
