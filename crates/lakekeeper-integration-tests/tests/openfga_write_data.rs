//! The `write_data` privilege end-to-end against a real OpenFGA store plus a Postgres catalog: a
//! principal granted `write_data` on a namespace commits data to a table below it through the
//! REST endpoints, but cannot change the table's structure or properties, nor drop it, nor
//! finish another principal's staged create.
//!
//! The model tests pin the relations and `authz-openfga`'s own tests pin the mapping of a
//! commit to them. This file covers what neither can: that the commit endpoint hands the
//! authorizer the update kinds a real request carries, that a commit to a staged table needs
//! `modify`, and how batch-check answers commit checks.
//!
//! Gated behind the `openfga_integration_tests` module so the default nextest filter
//! excludes it; runs under `--profile ci` with a live OpenFGA at
//! `LAKEKEEPER__OPENFGA__ENDPOINT`.

// Nested one level deep so the test path contains `::openfga_integration_tests::`,
// which the default nextest filter excludes (a root module would not match).
mod write_data {
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
                management::v1::check::{CatalogActionsBatchCheckRequest, check_internal},
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
            named_table_params(warehouse_id, "t")
        }

        fn named_table_params(warehouse_id: WarehouseId, table: &str) -> TableParameters {
            TableParameters {
                prefix: Some(Prefix(warehouse_id.to_string())),
                table: TableIdent::new(namespace(), table.to_string()),
            }
        }

        async fn commit(
            ctx: &Ctx,
            md: &RequestMetadata,
            warehouse_id: WarehouseId,
            updates: Vec<TableUpdate>,
        ) -> Result<(), lakekeeper::api::ErrorModel> {
            commit_to(ctx, md, table_params(warehouse_id), updates).await
        }

        async fn commit_to(
            ctx: &Ctx,
            md: &RequestMetadata,
            params: TableParameters,
            updates: Vec<TableUpdate>,
        ) -> Result<(), lakekeeper::api::ErrorModel> {
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

        /// Creates namespace `ns` with the given tables as `admin` and grants `write_data` on the
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
            principal_with(
                ctx,
                admin_md,
                project_id,
                warehouse_id,
                "writer",
                "write_data",
            )
            .await
        }

        /// Grants `privilege` on namespace `ns` to a new principal `name`, whose request
        /// metadata it returns.
        async fn principal_with(
            ctx: &Ctx,
            admin_md: &RequestMetadata,
            project_id: &ProjectId,
            warehouse_id: WarehouseId,
            name: &str,
            privilege: &str,
        ) -> RequestMetadata {
            let namespace_id: NamespaceId = PostgresBackend::get_namespace(
                warehouse_id,
                namespace(),
                ctx.v1_state.catalog.clone(),
            )
            .await
            .unwrap()
            .unwrap()
            .namespace_id();

            let user = UserId::new_unchecked("oidc", name);
            grant_on_namespace(ctx, admin_md, warehouse_id, namespace_id, privilege, &user).await;
            principal_metadata(&user, project_id)
        }

        /// Stages table `name` in `ns` as the principal of `md`, which then owns it, and
        /// returns its id.
        async fn stage(
            ctx: &Ctx,
            md: &RequestMetadata,
            warehouse_id: WarehouseId,
            name: &str,
        ) -> uuid::Uuid {
            CatalogServer::create_table(
                NamespaceParameters {
                    prefix: Some(Prefix(warehouse_id.to_string())),
                    namespace: namespace(),
                },
                create_table_request(Some(name.to_string()), Some(true)),
                DataAccess::not_specified(),
                ctx.clone(),
                md.clone(),
            )
            .await
            .unwrap()
            .metadata
            .uuid()
        }

        async fn load(
            ctx: &Ctx,
            md: &RequestMetadata,
            params: TableParameters,
        ) -> lakekeeper::api::iceberg::v1::LoadTableResult {
            let LoadTableResultOrNotModified::LoadTableResult(loaded) = CatalogServer::load_table(
                params,
                LoadTableRequest::default(),
                ctx.clone(),
                md.clone(),
            )
            .await
            .unwrap() else {
                panic!("a load without an ETag returns the table");
            };
            loaded
        }

        #[sqlx::test]
        async fn write_commits_data_but_not_structure(pool: PgPool) {
            let (ctx, admin, project_id, warehouse_id) = setup(pool).await;
            let admin_md = principal_metadata(&admin, &project_id);
            let writer_md =
                namespace_with_writer(&ctx, &admin_md, &project_id, warehouse_id, &["t"]).await;

            // Reading and appending are what `write_data` is for.
            CatalogServer::load_table(
                table_params(warehouse_id),
                LoadTableRequest::default(),
                ctx.clone(),
                writer_md.clone(),
            )
            .await
            .expect("write_data implies select");
            commit(&ctx, &writer_md, warehouse_id, append(1))
                .await
                .expect("an append is a data commit");

            // Spark sends a commit request without updates before a write with
            // `mergeSchema` that adds no column.
            commit(&ctx, &writer_md, warehouse_id, vec![])
                .await
                .expect("a commit request without updates is a data commit");

            // A new schema and a property need `modify`.
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
            .expect("a transaction of data commits needs only write_data");
        }

        /// A commit to a staged table finishes its creation, so it needs `modify`, whatever it
        /// contains. A principal with only `create` stages a table, owns it, and finishes the
        /// create; one with `write_data` cannot.
        #[sqlx::test]
        async fn a_staged_create_is_finished_only_by_its_owner(pool: PgPool) {
            let (ctx, admin, project_id, warehouse_id) = setup(pool).await;
            let admin_md = principal_metadata(&admin, &project_id);
            let writer_md =
                namespace_with_writer(&ctx, &admin_md, &project_id, warehouse_id, &[]).await;
            let creator_md = principal_with(
                &ctx,
                &admin_md,
                &project_id,
                warehouse_id,
                "creator",
                "create",
            )
            .await;
            stage(&ctx, &creator_md, warehouse_id, "staged").await;
            let staged = || named_table_params(warehouse_id, "staged");

            for (what, updates) in [
                ("an append", append(1)),
                (
                    "a commit that changes neither data nor structure",
                    vec![TableUpdate::RemoveStatistics { snapshot_id: 1 }],
                ),
                ("a commit request without updates", vec![]),
            ] {
                let err = commit_to(&ctx, &writer_md, staged(), updates)
                    .await
                    .expect_err(&format!("{what} to a staged table needs modify"));
                assert_eq!(err.code, 403, "{what}: {err:?}");
            }
            let err = CatalogServer::load_table(
                staged(),
                LoadTableRequest::default(),
                ctx.clone(),
                writer_md.clone(),
            )
            .await
            .expect_err("the table is still staged")
            .error;
            assert_eq!(err.code, 404, "{err:?}");

            commit_to(&ctx, &creator_md, staged(), append(1))
                .await
                .expect("the owner finishes the staged create");
            assert_eq!(
                load(&ctx, &writer_md, staged())
                    .await
                    .metadata
                    .current_snapshot_id(),
                Some(1)
            );
            commit_to(&ctx, &writer_md, staged(), append(2))
                .await
                .expect("once created, the table takes data commits with write_data");
        }

        /// A transaction that commits to a staged table needs `modify` on it, and one refusal
        /// refuses the whole transaction.
        #[sqlx::test]
        async fn a_transaction_with_a_staged_table_is_refused_as_a_whole(pool: PgPool) {
            let (ctx, admin, project_id, warehouse_id) = setup(pool).await;
            let admin_md = principal_metadata(&admin, &project_id);
            let writer_md =
                namespace_with_writer(&ctx, &admin_md, &project_id, warehouse_id, &["t"]).await;
            stage(&ctx, &admin_md, warehouse_id, "staged").await;
            let change = |table: &str, updates: Vec<TableUpdate>| CommitTableRequest {
                identifier: Some(TableIdent::new(namespace(), table.to_string())),
                requirements: vec![],
                updates,
            };

            let err = CatalogServer::commit_transaction(
                Some(Prefix(warehouse_id.to_string())),
                CommitTransactionRequest {
                    table_changes: vec![change("t", append(1)), change("staged", append(1))],
                },
                ctx.clone(),
                writer_md.clone(),
            )
            .await
            .expect_err("the append to the staged table needs modify")
            .error;
            assert_eq!(err.code, 403, "{err:?}");
            assert_eq!(
                load(&ctx, &writer_md, table_params(warehouse_id))
                    .await
                    .metadata
                    .current_snapshot_id(),
                None,
                "nothing was applied to t"
            );
        }

        /// A check that names no update kinds asks about a commit of any kind, so it needs
        /// `modify`, and JSON cannot mark a check as a commit request. A commit check on a
        /// staged table asks about a commit of any kind as well.
        #[sqlx::test]
        async fn a_commit_check_without_update_kinds_needs_modify(pool: PgPool) {
            let (ctx, admin, project_id, warehouse_id) = setup(pool).await;
            let admin_md = principal_metadata(&admin, &project_id);
            let writer_md =
                namespace_with_writer(&ctx, &admin_md, &project_id, warehouse_id, &["t"]).await;
            let creator_md = principal_with(
                &ctx,
                &admin_md,
                &project_id,
                warehouse_id,
                "creator",
                "create",
            )
            .await;
            let staged = stage(&ctx, &creator_md, warehouse_id, "staged").await;

            let on_t = |action: serde_json::Value| {
                serde_json::json!({"operation": {"table": {
                    "action": action,
                    "namespace": ["ns"],
                    "table": "t",
                    "warehouse-id": warehouse_id,
                }}})
            };
            let on_staged = |action: serde_json::Value| {
                serde_json::json!({"operation": {"table": {
                    "action": action,
                    "warehouse-id": warehouse_id,
                    "table-id": staged,
                }}})
            };
            let append = serde_json::json!({
                "action": "commit",
                "update_kinds": ["add-snapshot", "set-snapshot-ref"]
            });
            let allowed = async |md: &RequestMetadata, checks: Vec<serde_json::Value>| {
                let request: CatalogActionsBatchCheckRequest = serde_json::from_value(
                    serde_json::json!({"checks": checks, "error-on-not-found": true}),
                )
                .unwrap();
                check_internal(ctx.clone(), md.clone(), request)
                    .await
                    .unwrap()
                    .results
                    .into_iter()
                    .map(|r| r.allowed)
                    .collect::<Vec<_>>()
            };

            assert_eq!(
                allowed(
                    &writer_md,
                    vec![
                        on_t(serde_json::json!({"action": "commit"})),
                        on_t(serde_json::json!({"action": "commit", "update_kinds": []})),
                        on_t(serde_json::json!({
                            "action": "commit",
                            "update_kinds_listing": {"every_update": true}
                        })),
                        on_t(append.clone()),
                        on_staged(serde_json::json!({"action": "get_metadata"})),
                        on_staged(append.clone()),
                    ]
                )
                .await,
                vec![false, false, false, true, true, false]
            );
            assert_eq!(
                allowed(&creator_md, vec![on_staged(append)]).await,
                vec![true],
                "the owner may finish the staged create"
            );
        }
    }
}
