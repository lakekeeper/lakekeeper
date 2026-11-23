//! A recursive namespace drop against a real OpenFGA store plus a Postgres catalog.
//!
//! Gated behind the `openfga_integration_tests` module so the default nextest filter
//! excludes it; runs under `--profile ci` with a live OpenFGA at
//! `LAKEKEEPER__OPENFGA__ENDPOINT`.

// Nested one level deep so the test path contains `::openfga_integration_tests::`,
// which the default nextest filter excludes (a root module would not match).
mod drop_namespace {
    mod openfga_integration_tests {
        use std::collections::HashMap;

        use iceberg::{NamespaceIdent, TableIdent};
        use lakekeeper::{
            api::{
                ApiContext, RequestMetadata,
                data::v1::generic_tables::{
                    CreateGenericTableRequest, GenericTableParameters, GenericTableService as _,
                },
                iceberg::{
                    types::{DropParams, Prefix},
                    v1::{
                        CreateNamespaceRequest, DataAccess, NamespaceParameters,
                        RegisterTableRequest, TableParameters, ViewParameters,
                        namespace::{NamespaceDropFlags, NamespaceService as _},
                        tables::TablesService as _,
                        views::ViewService as _,
                    },
                },
                management::v1::warehouse::TabularDeleteProfile,
            },
            server::CatalogServer,
            service::{GenericTableFormat, State, UserId},
        };
        use lakekeeper_authz_openfga::{
            OpenFGAAuthorizer, new_authorizer_in_empty_store_from_default_config,
        };
        use lakekeeper_integration_tests::{
            SetupTestCatalog, create_table_request, create_view_request, memory_io_profile,
            principal_metadata,
        };
        use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
        use sqlx::PgPool;
        use uuid::Uuid;

        type Ctx = ApiContext<State<OpenFGAAuthorizer, PostgresBackend, SecretsState>>;

        /// OpenFGA-backed context with a soft-deletion warehouse, bootstrapping `admin` as
        /// operator.
        async fn setup(pool: PgPool) -> (Ctx, RequestMetadata, String) {
            let authorizer = new_authorizer_in_empty_store_from_default_config()
                .await
                .expect("OpenFGA must be reachable at LAKEKEEPER__OPENFGA__ENDPOINT");
            let admin = UserId::new_unchecked("oidc", "admin");
            let (ctx, warehouse) = SetupTestCatalog::builder()
                .pool(pool)
                .storage_profile(memory_io_profile())
                .authorizer(authorizer)
                .delete_profile(TabularDeleteProfile::Soft {
                    expiration_seconds: chrono::Duration::seconds(3600),
                })
                .user_id(Some(admin.clone()))
                .number_of_warehouses(1)
                .build()
                .setup()
                .await;
            let md = principal_metadata(&admin, &warehouse.project_id);
            (ctx, md, warehouse.warehouse_id.to_string())
        }

        /// Tuples, as `object#relation@user`, whose object or user is the entity with `id`.
        async fn tuples_of(ctx: &Ctx, id: Uuid) -> Vec<String> {
            let id = id.to_string();
            let mut tuples = Vec::new();
            let mut continuation_token: Option<String> = None;
            loop {
                let response = ctx
                    .v1_state
                    .authz
                    .client()
                    .read(100, None, continuation_token)
                    .await
                    .unwrap()
                    .into_inner();
                tuples.extend(
                    response
                        .tuples
                        .into_iter()
                        .filter_map(|tuple| tuple.key)
                        .filter(|key| key.object.contains(&id) || key.user.contains(&id))
                        .map(|key| format!("{}#{}@{}", key.object, key.relation, key.user)),
                );
                if response.continuation_token.is_empty() {
                    break;
                }
                continuation_token = Some(response.continuation_token);
            }
            tuples.sort();
            tuples
        }

        /// Tuples a create by `admin` writes for the child `object` of namespace `ns_id`.
        fn created_tuples(object: &str, ns_id: Uuid) -> Vec<String> {
            vec![
                format!("{object}#ownership@user:oidc~admin"),
                format!("{object}#parent@namespace:{ns_id}"),
                format!("namespace:{ns_id}#child@{object}"),
            ]
        }

        async fn tabular_id(pool: &PgPool, name: &str) -> Uuid {
            sqlx::query_scalar("SELECT tabular_id FROM tabular WHERE name = $1")
                .bind(name)
                .fetch_one(pool)
                .await
                .unwrap()
        }

        /// A recursive drop removes the tuples of the tables, views and generic tables that
        /// were soft-deleted before, so a dropped table can be registered again.
        #[lakekeeper_storage_postgres::db_test]
        async fn recursive_drop_removes_tuples_of_soft_deleted_children(pool: PgPool) {
            let (ctx, md, prefix) = setup(pool.clone()).await;
            let ns_ident = NamespaceIdent::new("ns".to_string());
            let ns_params = NamespaceParameters {
                prefix: Some(Prefix(prefix.clone())),
                namespace: ns_ident.clone(),
            };
            CatalogServer::create_namespace(
                Some(Prefix(prefix.clone())),
                CreateNamespaceRequest {
                    namespace: ns_ident.clone(),
                    properties: None,
                },
                ctx.clone(),
                md.clone(),
            )
            .await
            .unwrap();
            let metadata_location = CatalogServer::create_table(
                ns_params.clone(),
                create_table_request(Some("tab".to_string()), Some(false)),
                DataAccess::not_specified(),
                ctx.clone(),
                md.clone(),
            )
            .await
            .unwrap()
            .metadata_location
            .unwrap();
            CatalogServer::create_view(
                ns_params.clone(),
                create_view_request(Some("view"), None),
                ctx.clone(),
                DataAccess::not_specified(),
                md.clone(),
            )
            .await
            .unwrap();
            CatalogServer::create_generic_table(
                ns_params.clone(),
                CreateGenericTableRequest {
                    name: "gt".to_string(),
                    format: GenericTableFormat::Unknown("lance".to_string()),
                    base_location: None,
                    doc: None,
                    properties: HashMap::default(),
                    schema: None,
                    statistics: None,
                },
                ctx.clone(),
                md.clone(),
            )
            .await
            .unwrap();

            let soft_drop = DropParams {
                purge_requested: false,
                force: false,
            };
            CatalogServer::drop_table(
                TableParameters {
                    prefix: Some(Prefix(prefix.clone())),
                    table: TableIdent::new(ns_ident.clone(), "tab".to_string()),
                },
                soft_drop.clone(),
                ctx.clone(),
                md.clone(),
            )
            .await
            .unwrap();
            CatalogServer::drop_view(
                ViewParameters {
                    prefix: Some(Prefix(prefix.clone())),
                    view: TableIdent::new(ns_ident.clone(), "view".to_string()),
                },
                soft_drop.clone(),
                ctx.clone(),
                md.clone(),
            )
            .await
            .unwrap();
            CatalogServer::drop_generic_table(
                GenericTableParameters {
                    prefix: Some(Prefix(prefix.clone())),
                    namespace: ns_ident.clone(),
                    table_name: "gt".to_string(),
                },
                soft_drop,
                ctx.clone(),
                md.clone(),
            )
            .await
            .unwrap();

            let ns_id: Uuid =
                sqlx::query_scalar("SELECT namespace_id FROM namespace WHERE namespace_name = $1")
                    .bind(vec!["ns"])
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            let mut ids = Vec::new();
            for (name, fga_type) in [
                ("tab", "lakekeeper_table"),
                ("view", "lakekeeper_view"),
                ("gt", "lakekeeper_generic_table"),
            ] {
                let id = tabular_id(&pool, name).await;
                assert_eq!(
                    tuples_of(&ctx, id).await,
                    created_tuples(&format!("{fga_type}:{prefix}/{id}"), ns_id),
                    "{name}"
                );
                ids.push((name, id));
            }
            let table_id = ids[0].1;

            CatalogServer::drop_namespace(
                ns_params.clone(),
                NamespaceDropFlags {
                    force: true,
                    purge: false,
                    recursive: true,
                },
                ctx.clone(),
                md.clone(),
            )
            .await
            .unwrap();

            let mut left_over = Vec::new();
            for (name, id) in ids {
                left_over.push((name, tuples_of(&ctx, id).await));
            }
            assert_eq!(
                left_over,
                [("tab", vec![]), ("view", vec![]), ("gt", vec![])]
            );

            // The table keeps its id when its metadata file is registered again.
            CatalogServer::create_namespace(
                Some(Prefix(prefix.clone())),
                CreateNamespaceRequest {
                    namespace: ns_ident,
                    properties: None,
                },
                ctx.clone(),
                md.clone(),
            )
            .await
            .unwrap();
            CatalogServer::register_table(
                ns_params,
                RegisterTableRequest {
                    name: "tab".to_string(),
                    metadata_location,
                    overwrite: false,
                },
                DataAccess::not_specified(),
                ctx.clone(),
                md,
            )
            .await
            .unwrap();
            assert_eq!(tabular_id(&pool, "tab").await, table_id);
        }
    }
}
