//! The grants API on the **authorizer arm** (OpenFGA), end-to-end against a real
//! OpenFGA store + Postgres catalog. The catalog-arm twin (AllowAll + Postgres) is
//! `grant_ops.rs`; this file pins what only the OpenFGA arm can show:
//!
//! * grants and `/permissions/…/assignments` are two views of one set of tuples;
//! * grant authority comes from the model's `can_grant_*` relations, so a caller who
//!   cannot grant a privilege is refused;
//! * a project-scoped listing resolves each object back to its project through the
//!   model's own hierarchy edges, including the namespace parent walk.
//!
//! Gated behind the `openfga_integration_tests` module so the default nextest filter
//! excludes it; it runs under `--profile ci` with a live OpenFGA at
//! `LAKEKEEPER__OPENFGA__ENDPOINT`.

// Nested one level deep so the test path contains `::openfga_integration_tests::`,
// which the default nextest filter excludes (a root module would not match).
mod grant {
    mod openfga_integration_tests {
        use std::sync::Arc;

        use iceberg::NamespaceIdent;
        use lakekeeper::{
            ProjectId, WarehouseId,
            api::{
                ApiContext, RequestMetadata, RequestMetadataTestBuilder,
                iceberg::v1::{
                    CreateNamespaceRequest, PageToken, PaginationQuery, namespace::NamespaceService,
                },
                management::v1::{
                    ApiServer,
                    check::UserOrRole,
                    grant::{
                        ApplyGrantsRequest, GrantEntry, GrantResourceResponse, ListGrantsQuery,
                        Service as _,
                    },
                },
            },
            server::CatalogServer,
            service::{
                CatalogNamespaceOps as _, NamespaceId, State, UserId,
                authn::Actor,
                authz::{AuthZGrantOps as _, Authorizer as _, GrantResource, ResourceType},
            },
        };
        use lakekeeper_authz_openfga::{
            OpenFGAAuthorizer, new_authorizer_in_empty_store_from_default_config,
        };
        use lakekeeper_integration_tests::{SetupTestCatalog, memory_io_profile};
        use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
        use sqlx::PgPool;

        type Ctx = ApiContext<State<OpenFGAAuthorizer, PostgresBackend, SecretsState>>;
        type Server = ApiServer<PostgresBackend, OpenFGAAuthorizer, SecretsState>;

        /// An OpenFGA-backed context with a freshly-migrated, isolated store,
        /// bootstrapping `admin` as operator — who therefore inherits the
        /// `can_grant_*` relations the grant surface checks.
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

        fn entry(privilege: &str, user: &UserId) -> GrantEntry {
            GrantEntry {
                privilege: privilege.to_string(),
                principal: UserOrRole::User(user.clone()),
            }
        }

        fn writes(entries: Vec<GrantEntry>) -> ApplyGrantsRequest {
            ApplyGrantsRequest {
                writes: entries,
                deletes: vec![],
            }
        }

        fn deletes(entries: Vec<GrantEntry>) -> ApplyGrantsRequest {
            ApplyGrantsRequest {
                writes: vec![],
                deletes: entries,
            }
        }

        fn no_pagination() -> PaginationQuery {
            PaginationQuery::new(PageToken::Empty, None)
        }

        async fn create_namespace(
            ctx: &Ctx,
            md: &RequestMetadata,
            warehouse_id: WarehouseId,
            name: &str,
        ) -> NamespaceId {
            CatalogServer::create_namespace(
                Some(warehouse_id.to_string().into()),
                CreateNamespaceRequest {
                    namespace: NamespaceIdent::new(name.to_string()),
                    properties: None,
                },
                ctx.clone(),
                md.clone(),
            )
            .await
            .unwrap();
            PostgresBackend::get_namespace(
                warehouse_id,
                NamespaceIdent::new(name.to_string()),
                ctx.v1_state.catalog.clone(),
            )
            .await
            .unwrap()
            .unwrap()
            .namespace_id()
        }

        /// A warehouse grant round-trips through OpenFGA: the write lands as an
        /// assignment tuple, the listing reads it back with the tuple's timestamp, and
        /// the revoke removes it.
        #[sqlx::test]
        async fn apply_list_and_revoke_a_warehouse_grant(pool: PgPool) {
            let (ctx, admin, project_id, warehouse_id) = setup(pool).await;
            let md = metadata(&admin, &project_id);
            let bob = UserId::new_unchecked("oidc", "bob");

            Server::apply_warehouse_grants(
                warehouse_id,
                ctx.clone(),
                md.clone(),
                writes(vec![entry("select", &bob)]),
            )
            .await
            .unwrap();

            let page = Server::list_warehouse_grants(
                warehouse_id,
                ctx.clone(),
                md.clone(),
                ListGrantsQuery::default(),
                no_pagination(),
            )
            .await
            .unwrap();
            let listed: Vec<&_> = page
                .grants
                .iter()
                .filter(|g| g.principal == UserOrRole::User(bob.clone()))
                .collect();
            assert_eq!(listed.len(), 1);
            assert_eq!(listed[0].privilege, "select");
            assert_eq!(
                listed[0].resource,
                GrantResourceResponse::Warehouse { warehouse_id }
            );
            assert!(listed[0].recognized);
            assert!(listed[0].created_at.is_some());

            Server::apply_warehouse_grants(
                warehouse_id,
                ctx.clone(),
                md.clone(),
                deletes(vec![entry("select", &bob)]),
            )
            .await
            .unwrap();

            let page = Server::list_warehouse_grants(
                warehouse_id,
                ctx.clone(),
                md,
                ListGrantsQuery::default(),
                no_pagination(),
            )
            .await
            .unwrap();
            assert!(
                !page
                    .grants
                    .iter()
                    .any(|g| g.principal == UserOrRole::User(bob.clone()))
            );
        }

        /// The vocabulary is the model's assignable relations, so a warehouse *action*
        /// name is rejected on write while a privilege outside the vocabulary stays
        /// revocable — a grant written before the model changed must not get stuck.
        #[sqlx::test]
        async fn the_vocabulary_is_the_models_assignable_relations(pool: PgPool) {
            let (ctx, admin, project_id, warehouse_id) = setup(pool).await;
            let md = metadata(&admin, &project_id);
            let bob = UserId::new_unchecked("oidc", "bob");

            let names: Vec<String> = ctx
                .v1_state
                .authz
                .grantable_privileges(ResourceType::Warehouse)
                .iter()
                .map(|p| p.name.clone())
                .collect();
            assert_eq!(
                names,
                vec![
                    "ownership",
                    "pass_grants",
                    "manage_grants",
                    "describe",
                    "select",
                    "create",
                    "modify"
                ]
            );

            // `get_metadata` is a warehouse action, never an assignable privilege.
            let err = Server::apply_warehouse_grants(
                warehouse_id,
                ctx.clone(),
                md.clone(),
                writes(vec![entry("get_metadata", &bob)]),
            )
            .await
            .unwrap_err();
            assert_eq!(err.error.code, 400);
            assert_eq!(err.error.r#type, "InvalidGrantPrivilege");

            // Revokes skip vocabulary *validation*, but the authority gate still asks
            // whether the caller may revoke that privilege — and a name outside the
            // vocabulary has no `can_grant_*` relation to check, so the answer is no.
            // Safe here, and an arm divergence: no tuple can exist for a relation the
            // model does not define, so nothing revocable is being withheld. The
            // catalog arm stores privileges as opaque text, where such a row *can*
            // exist and must stay revocable, so there the gate resolves normally.
            let err = Server::apply_warehouse_grants(
                warehouse_id,
                ctx.clone(),
                md,
                deletes(vec![entry("get_metadata", &bob)]),
            )
            .await
            .unwrap_err();
            assert_eq!(err.error.code, 403);
            assert_eq!(err.error.r#type, "GrantActionForbidden");
        }

        /// Grant authority is the model's `can_grant_*` relation, not a catalog
        /// action: the operator holds it, a user with no relations does not.
        #[sqlx::test]
        async fn grant_authority_comes_from_the_can_grant_relations(pool: PgPool) {
            let (ctx, admin, project_id, warehouse_id) = setup(pool).await;
            let authorizer = &ctx.v1_state.authz;
            let resource = GrantResource::Warehouse(warehouse_id);

            let as_admin = authorizer
                .are_allowed_grants(
                    &metadata(&admin, &project_id),
                    None,
                    &resource,
                    &["select", "modify"],
                )
                .await
                .unwrap();
            assert_eq!(as_admin, vec![true, true]);

            let nobody = UserId::new_unchecked("oidc", "nobody");
            let as_nobody = authorizer
                .are_allowed_grants(
                    &metadata(&nobody, &project_id),
                    None,
                    &resource,
                    &["select", "modify"],
                )
                .await
                .unwrap();
            assert_eq!(as_nobody, vec![false, false]);

            // A name outside the vocabulary is a deny, not an error: it may come from
            // another authorizer's vocabulary.
            let unknown = authorizer
                .are_allowed_grants(
                    &metadata(&admin, &project_id),
                    None,
                    &resource,
                    &["get_metadata"],
                )
                .await
                .unwrap();
            assert_eq!(unknown, vec![false]);
        }

        /// Answering for another principal requires read-assignments authority on the
        /// resource, enforced inside the authorizer rather than by its callers.
        #[sqlx::test]
        async fn answering_for_another_principal_needs_the_read_gate(pool: PgPool) {
            let (ctx, admin, project_id, warehouse_id) = setup(pool).await;
            let authorizer = &ctx.v1_state.authz;
            let resource = GrantResource::Warehouse(warehouse_id);
            let bob = UserId::new_unchecked("oidc", "bob");
            let for_bob = lakekeeper::service::authz::UserOrRole::User(bob.clone());

            // The operator may inspect.
            let decisions = authorizer
                .are_allowed_grants(
                    &metadata(&admin, &project_id),
                    Some(&for_bob),
                    &resource,
                    &["select"],
                )
                .await
                .unwrap();
            assert_eq!(decisions, vec![false]);

            // A caller with no relations may not, and is told so rather than getting
            // an answer about someone else's access.
            let nobody = UserId::new_unchecked("oidc", "nobody");
            let err = authorizer
                .are_allowed_grants(
                    &metadata(&nobody, &project_id),
                    Some(&for_bob),
                    &resource,
                    &["select"],
                )
                .await
                .unwrap_err();
            let err =
                lakekeeper::service::events::AuthorizationFailureSource::into_error_model(err);
            assert_eq!(err.code, 403);
        }

        /// The project-scoped listing resolves every object back to its project
        /// through the model's hierarchy edges — including a namespace, whose object
        /// carries no warehouse and is recovered by walking `parent`.
        #[sqlx::test]
        async fn a_project_listing_resolves_objects_through_the_hierarchy(pool: PgPool) {
            let (ctx, admin, project_id, warehouse_id) = setup(pool).await;
            let md = metadata(&admin, &project_id);
            let bob = UserId::new_unchecked("oidc", "bob");
            let namespace_id = create_namespace(&ctx, &md, warehouse_id, "grant_ofga").await;

            Server::apply_warehouse_grants(
                warehouse_id,
                ctx.clone(),
                md.clone(),
                writes(vec![entry("select", &bob)]),
            )
            .await
            .unwrap();
            Server::apply_namespace_grants(
                warehouse_id,
                namespace_id,
                ctx.clone(),
                md.clone(),
                writes(vec![entry("select", &bob)]),
            )
            .await
            .unwrap();

            let page = Server::list_grants(
                ctx.clone(),
                md,
                ListGrantsQuery {
                    principal_user: Some(bob.clone()),
                    principal_role: None,
                },
                no_pagination(),
            )
            .await
            .unwrap();

            let mut resources: Vec<GrantResourceResponse> =
                page.grants.into_iter().map(|g| g.resource).collect();
            resources.sort_by_key(|r| format!("{r:?}"));
            assert_eq!(
                resources,
                vec![
                    GrantResourceResponse::Namespace {
                        warehouse_id,
                        namespace_id,
                    },
                    GrantResourceResponse::Warehouse { warehouse_id },
                ]
            );
        }

        /// The audit and export view: every grant in the project, no principal named.
        /// Reads the store differently from the narrowed form — with no principal there
        /// is no user to filter tuples by — so it needs its own coverage.
        #[sqlx::test]
        async fn a_project_listing_without_a_principal_returns_every_grant(pool: PgPool) {
            let (ctx, admin, project_id, warehouse_id) = setup(pool).await;
            let admin_principal = UserOrRole::User(admin.clone());
            let md = metadata(&admin, &project_id);
            let bob = UserId::new_unchecked("oidc", "bob");
            let carol = UserId::new_unchecked("oidc", "carol");

            Server::apply_warehouse_grants(
                warehouse_id,
                ctx.clone(),
                md.clone(),
                writes(vec![entry("select", &bob), entry("describe", &carol)]),
            )
            .await
            .unwrap();

            let page =
                Server::list_grants(ctx.clone(), md, ListGrantsQuery::default(), no_pagination())
                    .await
                    .unwrap();

            let mut listed: Vec<(UserOrRole, String)> = page
                .grants
                .into_iter()
                .filter(|g| g.resource == GrantResourceResponse::Warehouse { warehouse_id })
                .map(|g| (g.principal, g.privilege))
                .collect();
            listed.sort_by_key(|(principal, privilege)| format!("{principal:?}{privilege}"));
            // Creating the warehouse made admin its owner, so that tuple is a grant on
            // the warehouse too and belongs in an unnarrowed listing.
            assert_eq!(
                listed,
                vec![
                    (admin_principal, "ownership".to_string()),
                    (UserOrRole::User(bob), "select".to_string()),
                    (UserOrRole::User(carol), "describe".to_string()),
                ]
            );
        }

        /// A grant written through `/grants` is visible through the older
        /// `/permissions/…/assignments` API, because both are views of one tuple.
        /// This is what lets `/grants` supersede that API without a migration.
        #[sqlx::test]
        async fn a_grant_and_an_assignment_are_the_same_tuple(pool: PgPool) {
            let (ctx, admin, project_id, warehouse_id) = setup(pool).await;
            let md = metadata(&admin, &project_id);
            let bob = UserId::new_unchecked("oidc", "bob");

            Server::apply_warehouse_grants(
                warehouse_id,
                ctx.clone(),
                md.clone(),
                writes(vec![entry("select", &bob)]),
            )
            .await
            .unwrap();

            // Read back through the authorizer's own facet rather than the
            // authorizer-private HTTP surface: same tuples, no router needed.
            let page = ctx
                .v1_state
                .authz
                .grants()
                .expect("the OpenFGA authorizer owns grants")
                .list_grants(
                    &md,
                    lakekeeper::service::authz::GrantFilter::on(
                        GrantResource::Warehouse(warehouse_id),
                        None,
                    ),
                    no_pagination(),
                )
                .await
                .unwrap();
            let privileges: Vec<String> = page
                .grants
                .into_iter()
                .filter(|g| {
                    g.principal == lakekeeper::service::authz::UserOrRoleId::User(bob.clone())
                })
                .map(|g| g.privilege)
                .collect();
            assert_eq!(privileges, vec!["select".to_string()]);
        }
        /// The per-resource vocabulary filters the model's assignable relations by the
        /// caller's own `can_grant_*` relations — the operator may grant everything on
        /// its warehouse, a principal with no relations may grant nothing.
        #[sqlx::test]
        async fn grantable_privileges_follow_the_can_grant_relations(pool: PgPool) {
            let (ctx, admin, project_id, warehouse_id) = setup(pool).await;

            let as_admin = Server::get_warehouse_grantable_privileges(
                warehouse_id,
                ctx.clone(),
                metadata(&admin, &project_id),
                no_principal(),
            )
            .await
            .unwrap();
            assert!(as_admin.privileges.iter().all(|p| p.allowed));
            let mut names: Vec<&str> = as_admin
                .privileges
                .iter()
                .map(|p| p.privilege.name.as_str())
                .collect();
            names.sort_unstable();
            assert_eq!(
                names,
                vec![
                    "create",
                    "describe",
                    "manage_grants",
                    "modify",
                    "ownership",
                    "pass_grants",
                    "select"
                ]
            );

            let nobody = UserId::new_unchecked("oidc", "nobody");
            let as_nobody = Server::get_warehouse_grantable_privileges(
                warehouse_id,
                ctx.clone(),
                metadata(&nobody, &project_id),
                no_principal(),
            )
            .await
            .unwrap();
            // The vocabulary is the same; only the markers differ. A picker rendering
            // this shows every privilege, all of them unavailable.
            let unavailable: Vec<&str> = as_nobody
                .privileges
                .iter()
                .filter(|p| !p.allowed)
                .map(|p| p.privilege.name.as_str())
                .collect();
            assert_eq!(unavailable.len(), 7);
            assert_eq!(as_nobody.privileges.len(), 7);
        }

        /// A per-resource listing narrowed to one principal is served by narrowing the
        /// same `Read` on its user field. OpenFGA validates the shape of a `Read` tuple
        /// key, so a user filter alongside a full object id and an empty relation has to
        /// be proven against a real server rather than reasoned about.
        #[sqlx::test]
        async fn a_resource_listing_narrows_to_one_principal(pool: PgPool) {
            let (ctx, admin, project_id, warehouse_id) = setup(pool).await;
            let md = metadata(&admin, &project_id);
            let bob = UserId::new_unchecked("oidc", "bob");
            let carol = UserId::new_unchecked("oidc", "carol");

            Server::apply_warehouse_grants(
                warehouse_id,
                ctx.clone(),
                md.clone(),
                writes(vec![
                    entry("select", &bob),
                    entry("describe", &bob),
                    entry("select", &carol),
                ]),
            )
            .await
            .unwrap();

            let listed = async |query: ListGrantsQuery| {
                let page = Server::list_warehouse_grants(
                    warehouse_id,
                    ctx.clone(),
                    md.clone(),
                    query,
                    no_pagination(),
                )
                .await
                .unwrap();
                let mut out: Vec<(UserOrRole, String)> = page
                    .grants
                    .into_iter()
                    .map(|g| (g.principal, g.privilege))
                    .collect();
                out.sort_by_key(|(principal, privilege)| format!("{principal:?}{privilege}"));
                out
            };

            assert_eq!(
                listed(ListGrantsQuery {
                    principal_user: Some(bob.clone()),
                    principal_role: None,
                })
                .await,
                vec![
                    (UserOrRole::User(bob.clone()), "describe".to_string()),
                    (UserOrRole::User(bob.clone()), "select".to_string()),
                ]
            );
            assert_eq!(
                listed(ListGrantsQuery {
                    principal_user: Some(carol.clone()),
                    principal_role: None,
                })
                .await,
                vec![(UserOrRole::User(carol), "select".to_string())]
            );

            // The bootstrap makes admin the warehouse's owner, so the unnarrowed listing
            // carries that ownership tuple on top of the three grants written here.
            assert_eq!(listed(ListGrantsQuery::default()).await.len(), 4);
        }

        /// Reading your own grants on a resource needs no grant-read authority, only
        /// permission to see the resource. Under a real authorizer that means a user with
        /// one grant on a warehouse can read it back, while the same request for someone
        /// else is refused.
        #[sqlx::test]
        async fn your_own_grants_on_a_resource_need_no_grant_read_authority(pool: PgPool) {
            let (ctx, admin, project_id, warehouse_id) = setup(pool).await;
            let admin_md = metadata(&admin, &project_id);
            let bob = UserId::new_unchecked("oidc", "bob");
            let carol = UserId::new_unchecked("oidc", "carol");

            Server::apply_warehouse_grants(
                warehouse_id,
                ctx.clone(),
                admin_md,
                writes(vec![entry("select", &bob), entry("select", &carol)]),
            )
            .await
            .unwrap();

            let bob_md = metadata(&bob, &project_id);
            let page = Server::list_warehouse_grants(
                warehouse_id,
                ctx.clone(),
                bob_md.clone(),
                ListGrantsQuery {
                    principal_user: Some(bob.clone()),
                    principal_role: None,
                },
                no_pagination(),
            )
            .await
            .unwrap();
            assert_eq!(
                page.grants
                    .iter()
                    .map(|g| g.privilege.as_str())
                    .collect::<Vec<_>>(),
                vec!["select"]
            );
            assert_eq!(page.grants[0].principal, UserOrRole::User(bob.clone()));

            // Carol's grants are someone else's access, so the gate applies.
            let err = Server::list_warehouse_grants(
                warehouse_id,
                ctx.clone(),
                bob_md.clone(),
                ListGrantsQuery {
                    principal_user: Some(carol),
                    principal_role: None,
                },
                no_pagination(),
            )
            .await
            .unwrap_err();
            assert_eq!(err.error.code, 403);

            // So does the unnarrowed listing.
            let err = Server::list_warehouse_grants(
                warehouse_id,
                ctx,
                bob_md,
                ListGrantsQuery::default(),
                no_pagination(),
            )
            .await
            .unwrap_err();
            assert_eq!(err.error.code, 403);
        }

        fn no_principal() -> lakekeeper::api::management::v1::grant::GetGrantAccessQuery {
            lakekeeper::api::management::v1::grant::GetGrantAccessQuery::default()
        }
    }
}
