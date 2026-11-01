//! The `write` privilege end-to-end against a real OpenFGA store plus a Postgres catalog: a
//! principal granted `write` on a namespace commits data to a table below it through the
//! REST endpoints, but cannot change the table's structure or properties, nor drop it.
//!
//! The model tests pin the relations and `authz-openfga`'s own tests pin the mapping of a
//! commit to them. This file covers what neither can: that the commit endpoint hands the
//! authorizer the update kinds a real request carries.
//!
//! Gated behind the `openfga_integration_tests` module so the default nextest filter
//! excludes it; runs under `--profile ci` with a live OpenFGA at
//! `LAKEKEEPER__OPENFGA__ENDPOINT`.

// Nested one level deep so the test path contains `::openfga_integration_tests::`,
// which the default nextest filter excludes (a root module would not match).
mod write_tier {
    mod openfga_integration_tests {
        use std::{collections::HashMap, sync::Arc};

        use iceberg::{
            NamespaceIdent, TableIdent, TableUpdate,
            spec::{
                MAIN_BRANCH, NestedField, Operation, PrimitiveType, Schema, Snapshot,
                SnapshotReference, SnapshotRetention, Summary, Type,
            },
        };
        use iceberg_ext::catalog::rest::{CommitTableRequest, CommitTransactionRequest};
        use lakekeeper::{
            ProjectId, WarehouseId,
            api::{
                ApiContext, RequestMetadata,
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
            },
            server::CatalogServer,
            service::{CatalogNamespaceOps as _, NamespaceId, State, UserId},
        };
        use lakekeeper_authz_openfga::{
            OpenFGAAuthorizer, new_authorizer_in_empty_store_from_default_config,
        };
        use lakekeeper_integration_tests::{
            SetupTestCatalog, create_table_request, grant_on_namespace, memory_io_profile,
            principal_metadata,
        };
        use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
        use sqlx::PgPool;

        type Ctx = ApiContext<State<OpenFGAAuthorizer, PostgresBackend, SecretsState>>;

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

        fn namespace() -> NamespaceIdent {
            NamespaceIdent::new("ns".to_string())
        }

        fn table_params(warehouse_id: WarehouseId) -> TableParameters {
            TableParameters {
                prefix: Some(Prefix(warehouse_id.to_string())),
                table: TableIdent::new(namespace(), "t".to_string()),
            }
        }

        async fn commit(
            ctx: &Ctx,
            md: &RequestMetadata,
            warehouse_id: WarehouseId,
            updates: Vec<TableUpdate>,
        ) -> Result<(), lakekeeper::api::ErrorModel> {
            let params = table_params(warehouse_id);
            CatalogServer::commit_table(
                params.clone(),
                CommitTableRequest {
                    identifier: Some(params.table),
                    requirements: vec![],
                    updates,
                },
                ctx.clone(),
                md.clone(),
            )
            .await
            .map(|_| ())
            .map_err(|e| e.error)
        }

        fn append(snapshot_id: i64) -> Vec<TableUpdate> {
            let snapshot = Snapshot::builder()
                .with_snapshot_id(snapshot_id)
                .with_timestamp_ms(chrono::Utc::now().timestamp_millis())
                .with_sequence_number(snapshot_id)
                .with_manifest_list(format!("/path/to/manifest-{snapshot_id}.avro"))
                .with_summary(Summary {
                    operation: Operation::Append,
                    additional_properties: HashMap::new(),
                })
                .with_schema_id(0)
                .build();
            vec![
                TableUpdate::AddSnapshot { snapshot },
                TableUpdate::SetSnapshotRef {
                    ref_name: MAIN_BRANCH.to_string(),
                    reference: SnapshotReference {
                        snapshot_id,
                        retention: SnapshotRetention::Branch {
                            min_snapshots_to_keep: None,
                            max_snapshot_age_ms: None,
                            max_ref_age_ms: None,
                        },
                    },
                },
            ]
        }

        /// Creates namespace `ns` with the given tables as `admin` and grants `write` on the
        /// namespace to a new principal, whose request metadata it returns.
        async fn namespace_with_writer(
            ctx: &Ctx,
            admin_md: &RequestMetadata,
            project_id: &ProjectId,
            warehouse_id: WarehouseId,
            tables: &[&str],
        ) -> RequestMetadata {
            CatalogServer::create_namespace(
                Some(Prefix(warehouse_id.to_string())),
                CreateNamespaceRequest {
                    namespace: namespace(),
                    properties: None,
                },
                ctx.clone(),
                admin_md.clone(),
            )
            .await
            .unwrap();
            for table in tables {
                CatalogServer::create_table(
                    NamespaceParameters {
                        prefix: Some(Prefix(warehouse_id.to_string())),
                        namespace: namespace(),
                    },
                    create_table_request(Some((*table).to_string()), Some(false)),
                    DataAccess::not_specified(),
                    ctx.clone(),
                    admin_md.clone(),
                )
                .await
                .unwrap();
            }
            let namespace_id: NamespaceId = PostgresBackend::get_namespace(
                warehouse_id,
                namespace(),
                ctx.v1_state.catalog.clone(),
            )
            .await
            .unwrap()
            .unwrap()
            .namespace_id();

            let writer = UserId::new_unchecked("oidc", "writer");
            grant_on_namespace(ctx, admin_md, warehouse_id, namespace_id, "write", &writer).await;
            principal_metadata(&writer, project_id)
        }

        #[sqlx::test]
        async fn write_commits_data_but_not_structure(pool: PgPool) {
            let (ctx, admin, project_id, warehouse_id) = setup(pool).await;
            let admin_md = principal_metadata(&admin, &project_id);
            let writer_md =
                namespace_with_writer(&ctx, &admin_md, &project_id, warehouse_id, &["t"]).await;

            // Reading and appending are what `write` is for.
            CatalogServer::load_table(
                table_params(warehouse_id),
                LoadTableRequest::default(),
                ctx.clone(),
                writer_md.clone(),
            )
            .await
            .expect("write implies select");
            commit(&ctx, &writer_md, warehouse_id, append(1))
                .await
                .expect("an append is a data commit");

            // A new schema, a property and a commit that names no update need `modify`.
            let schema = Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(3, "extra", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap();
            for (what, updates) in [
                (
                    "a schema change",
                    vec![
                        TableUpdate::AddSchema { schema },
                        TableUpdate::SetCurrentSchema { schema_id: 1 },
                    ],
                ),
                (
                    "a property change",
                    vec![TableUpdate::SetProperties {
                        updates: HashMap::from([(
                            "write.format.default".to_string(),
                            "orc".to_string(),
                        )]),
                    }],
                ),
                ("an empty commit", vec![]),
            ] {
                let err = commit(&ctx, &writer_md, warehouse_id, updates)
                    .await
                    .expect_err(&format!("{what} needs modify"));
                assert_eq!(err.code, 403, "{what}: {err:?}");
            }

            let err = CatalogServer::drop_table(
                table_params(warehouse_id),
                DropParams {
                    purge_requested: false,
                    force: false,
                },
                ctx.clone(),
                writer_md.clone(),
            )
            .await
            .expect_err("dropping needs modify")
            .error;
            assert_eq!(err.code, 403, "{err:?}");

            // The same table still takes an append, and `modify` still takes the rest.
            commit(&ctx, &writer_md, warehouse_id, append(2))
                .await
                .expect("a second append is a data commit");
            commit(
                &ctx,
                &admin_md,
                warehouse_id,
                vec![TableUpdate::SetProperties {
                    updates: HashMap::from([("owner".to_string(), "team".to_string())]),
                }],
            )
            .await
            .expect("modify changes properties");
        }

        /// Each table of a transaction is checked on its own updates, and one refusal
        /// refuses the whole transaction.
        #[sqlx::test]
        async fn a_transaction_with_a_structural_change_is_refused_as_a_whole(pool: PgPool) {
            let (ctx, admin, project_id, warehouse_id) = setup(pool).await;
            let admin_md = principal_metadata(&admin, &project_id);
            let writer_md =
                namespace_with_writer(&ctx, &admin_md, &project_id, warehouse_id, &["t", "t2"])
                    .await;
            let change = |table: &str, updates: Vec<TableUpdate>| CommitTableRequest {
                identifier: Some(TableIdent::new(namespace(), table.to_string())),
                requirements: vec![],
                updates,
            };

            let err = CatalogServer::commit_transaction(
                Some(Prefix(warehouse_id.to_string())),
                CommitTransactionRequest {
                    table_changes: vec![
                        change("t", append(1)),
                        change(
                            "t2",
                            vec![TableUpdate::SetProperties {
                                updates: HashMap::from([(
                                    "write.format.default".to_string(),
                                    "orc".to_string(),
                                )]),
                            }],
                        ),
                    ],
                },
                ctx.clone(),
                writer_md.clone(),
            )
            .await
            .expect_err("the property change on t2 needs modify")
            .error;
            assert_eq!(err.code, 403, "{err:?}");

            let LoadTableResultOrNotModified::LoadTableResult(loaded) = CatalogServer::load_table(
                table_params(warehouse_id),
                LoadTableRequest::default(),
                ctx.clone(),
                writer_md.clone(),
            )
            .await
            .unwrap() else {
                panic!("a load without an ETag returns the table");
            };
            assert_eq!(
                loaded.metadata.current_snapshot_id(),
                None,
                "nothing was applied to t"
            );

            CatalogServer::commit_transaction(
                Some(Prefix(warehouse_id.to_string())),
                CommitTransactionRequest {
                    table_changes: vec![change("t", append(1)), change("t2", append(2))],
                },
                ctx.clone(),
                writer_md.clone(),
            )
            .await
            .expect("a transaction of data commits needs only write");
        }
    }
}
