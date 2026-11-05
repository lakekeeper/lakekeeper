//! The `write` privilege end-to-end against a real OpenFGA store plus a Postgres catalog: a
//! principal granted `write` on a namespace makes every commit to a table below it through
//! the REST endpoints, including schema, property and history changes and the commit that
//! finishes a staged create, but cannot drop or rename the table.
//!
//! The model tests pin the relations and `authz-openfga`'s own tests pin the mapping of a
//! commit to them. This file covers what neither can: that real requests reach the relations
//! the model tests check, and how batch-check answers commit checks.
//!
//! Gated behind the `openfga_integration_tests` module so the default nextest filter
//! excludes it; runs under `--profile ci` with a live OpenFGA at
//! `LAKEKEEPER__OPENFGA__ENDPOINT`.

// Nested one level deep so the test path contains `::openfga_integration_tests::`,
// which the default nextest filter excludes (a root module would not match).
mod write {
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
            principal_with(ctx, admin_md, project_id, warehouse_id, "writer", "write").await
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

        fn schema_change() -> Vec<TableUpdate> {
            let schema = Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(3, "extra", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap();
            vec![
                TableUpdate::AddSchema { schema },
                TableUpdate::SetCurrentSchema { schema_id: 1 },
            ]
        }

        fn property_change() -> Vec<TableUpdate> {
            vec![TableUpdate::SetProperties {
                updates: HashMap::from([("write.format.default".to_string(), "orc".to_string())]),
            }]
        }

        #[sqlx::test]
        async fn write_makes_every_commit_but_never_drops_or_renames(pool: PgPool) {
            let (ctx, admin, project_id, warehouse_id) = setup(pool).await;
            let admin_md = principal_metadata(&admin, &project_id);
            let writer_md =
                namespace_with_writer(&ctx, &admin_md, &project_id, warehouse_id, &["t"]).await;

            CatalogServer::load_table(
                table_params(warehouse_id),
                LoadTableRequest::default(),
                ctx.clone(),
                writer_md.clone(),
            )
            .await
            .expect("write implies select");
            for (what, updates) in [
                ("an append", append(1)),
                ("a commit request without updates", vec![]),
                ("a schema change", schema_change()),
                ("a property change", property_change()),
                ("a second append", append(2)),
                (
                    "snapshot expiry",
                    vec![TableUpdate::RemoveSnapshots {
                        snapshot_ids: vec![1],
                    }],
                ),
                (
                    "a new branch",
                    vec![TableUpdate::SetSnapshotRef {
                        ref_name: "audit".to_string(),
                        reference: SnapshotReference {
                            snapshot_id: 2,
                            retention: SnapshotRetention::Branch {
                                min_snapshots_to_keep: None,
                                max_snapshot_age_ms: None,
                                max_ref_age_ms: None,
                            },
                        },
                    }],
                ),
                (
                    "a branch removal",
                    vec![TableUpdate::RemoveSnapshotRef {
                        ref_name: "audit".to_string(),
                    }],
                ),
            ] {
                commit(&ctx, &writer_md, warehouse_id, updates)
                    .await
                    .unwrap_or_else(|e| panic!("{what} is a commit write allows: {e:?}"));
            }
            let loaded = load(&ctx, &writer_md, table_params(warehouse_id)).await;
            assert_eq!(loaded.metadata.current_schema_id(), 1);
            assert_eq!(loaded.metadata.current_snapshot_id(), Some(2));
            assert_eq!(
                loaded
                    .metadata
                    .properties()
                    .get("write.format.default")
                    .map(String::as_str),
                Some("orc")
            );

            let err = CatalogServer::rename_table(
                Some(Prefix(warehouse_id.to_string())),
                iceberg_ext::catalog::rest::RenameTableRequest {
                    source: TableIdent::new(namespace(), "t".to_string()),
                    destination: TableIdent::new(namespace(), "t_renamed".to_string()),
                },
                ctx.clone(),
                writer_md.clone(),
            )
            .await
            .expect_err("renaming needs modify")
            .error;
            assert_eq!(err.code, 403, "{err:?}");
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

            CatalogServer::drop_table(
                table_params(warehouse_id),
                DropParams {
                    purge_requested: false,
                    force: false,
                },
                ctx.clone(),
                admin_md.clone(),
            )
            .await
            .expect("modify drops");
        }

        /// A transaction of structural and data commits to several tables needs only `write`.
        #[sqlx::test]
        async fn a_transaction_with_structural_changes_needs_only_write(pool: PgPool) {
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

            CatalogServer::commit_transaction(
                Some(Prefix(warehouse_id.to_string())),
                CommitTransactionRequest {
                    table_changes: vec![change("t", append(1)), change("t2", property_change())],
                },
                ctx.clone(),
                writer_md.clone(),
            )
            .await
            .expect("write commits to both tables");
            assert_eq!(
                load(&ctx, &writer_md, table_params(warehouse_id))
                    .await
                    .metadata
                    .current_snapshot_id(),
                Some(1)
            );
        }

        /// A commit to a staged table finishes its creation, and `write` makes it like any
        /// other commit: a principal with `create` stages a table, and one with `write` on the
        /// namespace finishes the create.
        #[sqlx::test]
        async fn write_finishes_a_staged_create(pool: PgPool) {
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

            let mut updates = schema_change();
            updates.extend(append(1));
            commit_to(&ctx, &writer_md, staged(), updates)
                .await
                .expect("write finishes the staged create");
            let loaded = load(&ctx, &writer_md, staged()).await;
            assert_eq!(loaded.metadata.current_snapshot_id(), Some(1));
            assert_eq!(loaded.metadata.current_schema_id(), 1);
        }

        /// A commit check asks the write tier, whatever it names and whether the table is
        /// staged.
        #[sqlx::test]
        async fn every_commit_check_asks_write(pool: PgPool) {
            let (ctx, admin, project_id, warehouse_id) = setup(pool).await;
            let admin_md = principal_metadata(&admin, &project_id);
            let writer_md =
                namespace_with_writer(&ctx, &admin_md, &project_id, warehouse_id, &["t"]).await;
            let reader_md = principal_with(
                &ctx,
                &admin_md,
                &project_id,
                warehouse_id,
                "reader",
                "select",
            )
            .await;
            let staged = stage(&ctx, &admin_md, warehouse_id, "staged").await;

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
            let checks = vec![
                on_t(serde_json::json!({"action": "commit"})),
                on_t(serde_json::json!({"action": "commit", "update_kinds": ["add-schema"]})),
                on_t(serde_json::json!({
                    "action": "commit",
                    "updated_properties": {"owner": "team"}
                })),
                on_t(append.clone()),
                on_staged(append),
                on_t(serde_json::json!({"action": "drop"})),
            ];
            let allowed = async |md: &RequestMetadata| {
                let request: CatalogActionsBatchCheckRequest = serde_json::from_value(
                    serde_json::json!({"checks": checks.clone(), "error-on-not-found": true}),
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
                allowed(&writer_md).await,
                vec![true, true, true, true, true, false]
            );
            assert_eq!(
                allowed(&reader_md).await,
                vec![false, false, false, false, false, false]
            );
        }
    }
}
