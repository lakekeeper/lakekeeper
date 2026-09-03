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
                CatalogNamespaceOps as _, CatalogWarehouseOps as _, NamespaceId, ResolvedWarehouse,
                State, UserId,
                authn::Actor,
                authz::{
                    AuthZGrantOps as _, Authorizer as _, GrantAuthorityCheck, GrantOp,
                    GrantResource, GrantTarget, ResourceType, UserOrRole as AuthzUserOrRole,
                },
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

        /// The warehouse as the handlers resolve it before the gate: a grant target carries
        /// the resource's ancestry, not its id, so an authorizer that resolves inheritance
        /// itself can place it. This one reads inheritance from its own tuples and uses only
        /// the id, which is why every test here can share one target.
        async fn warehouse_target(ctx: &Ctx, warehouse_id: WarehouseId) -> Arc<ResolvedWarehouse> {
            PostgresBackend::get_active_warehouse_by_id(warehouse_id, ctx.v1_state.catalog.clone())
                .await
                .expect("the setup's warehouse resolves")
                .expect("the setup's warehouse is active")
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

        /// A grant-authority question naming no grantee, as the grantable-privileges
        /// endpoint asks it. The tests that name one build the check directly.
        fn check(privilege: &str) -> GrantAuthorityCheck<'_> {
            GrantAuthorityCheck::grantable(privilege)
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
                    "modify",
                    "manage_tags"
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
            let warehouse = warehouse_target(&ctx, warehouse_id).await;
            let target = GrantTarget::Warehouse(&warehouse);

            let as_admin = authorizer
                .are_allowed_grants(
                    &metadata(&admin, &project_id),
                    None,
                    &target,
                    &[check("select"), check("modify")],
                )
                .await
                .unwrap();
            assert_eq!(as_admin, vec![true, true]);

            let nobody = UserId::new_unchecked("oidc", "nobody");
            let as_nobody = authorizer
                .are_allowed_grants(
                    &metadata(&nobody, &project_id),
                    None,
                    &target,
                    &[check("select"), check("modify")],
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
                    &target,
                    &[check("get_metadata")],
                )
                .await
                .unwrap();
            assert_eq!(unknown, vec![false]);
        }

        /// The grantee reaches the authorizer but does not change its answer: no
        /// `can_grant_*` relation reads who receives the privilege, so this arm checks
        /// each distinct privilege once and repeats the answer per check.
        ///
        /// Pins the fan-out, which is the part that can go wrong silently: one decision
        /// per check, in the order asked, with repeated and unknown privileges mixed in.
        #[sqlx::test]
        async fn grant_authority_repeats_one_answer_per_grantee(pool: PgPool) {
            let (ctx, admin, project_id, warehouse_id) = setup(pool).await;
            let authorizer = &ctx.v1_state.authz;
            let warehouse = warehouse_target(&ctx, warehouse_id).await;
            let target = GrantTarget::Warehouse(&warehouse);
            let alice = AuthzUserOrRole::User(UserId::new_unchecked("oidc", "alice"));
            let bob = AuthzUserOrRole::User(UserId::new_unchecked("oidc", "bob"));

            let decisions = authorizer
                .are_allowed_grants(
                    &metadata(&admin, &project_id),
                    None,
                    &target,
                    &[
                        GrantAuthorityCheck::entry("select", Some(&alice), GrantOp::Grant),
                        GrantAuthorityCheck::entry("select", Some(&bob), GrantOp::Grant),
                        GrantAuthorityCheck::entry("modify", Some(&alice), GrantOp::Grant),
                    ],
                )
                .await
                .unwrap();
            assert_eq!(decisions, vec![true, true, true]);

            // An unknown name is a deny wherever it sits, and does not shift the
            // decisions of the checks around it.
            let mixed = authorizer
                .are_allowed_grants(
                    &metadata(&admin, &project_id),
                    None,
                    &target,
                    &[
                        GrantAuthorityCheck::entry("get_metadata", Some(&alice), GrantOp::Grant),
                        GrantAuthorityCheck::entry("select", Some(&alice), GrantOp::Grant),
                        GrantAuthorityCheck::entry("get_metadata", Some(&bob), GrantOp::Grant),
                        GrantAuthorityCheck::entry("select", Some(&bob), GrantOp::Grant),
                    ],
                )
                .await
                .unwrap();
            assert_eq!(mixed, vec![false, true, false, true]);

            let nobody = UserId::new_unchecked("oidc", "nobody");
            let as_nobody = authorizer
                .are_allowed_grants(
                    &metadata(&nobody, &project_id),
                    None,
                    &target,
                    &[
                        GrantAuthorityCheck::entry("select", Some(&alice), GrantOp::Grant),
                        GrantAuthorityCheck::entry("select", Some(&bob), GrantOp::Grant),
                    ],
                )
                .await
                .unwrap();
            assert_eq!(as_nobody, vec![false, false]);
        }

        /// A principal holding only direct `manage_grants` on a warehouse can read
        /// back what it applies. In the model `manage_grants` reaches
        /// `can_read_assignments` but not `describe`, so before the grant-read action
        /// doubled as visibility (see `require_warehouse_action`) the listing and the
        /// delegated grantable-privileges view answered the masked not-found — grants
        /// could be written but never read back. A principal without grant-read stays
        /// masked.
        #[sqlx::test]
        async fn a_grant_admin_without_describe_reads_what_it_applies(pool: PgPool) {
            let (ctx, admin, project_id, warehouse_id) = setup(pool).await;
            let grant_admin = UserId::new_unchecked("oidc", "grant-admin");
            let bob = UserId::new_unchecked("oidc", "bob");

            Server::apply_warehouse_grants(
                warehouse_id,
                ctx.clone(),
                metadata(&admin, &project_id),
                writes(vec![entry("manage_grants", &grant_admin)]),
            )
            .await
            .unwrap();

            let as_grant_admin = metadata(&grant_admin, &project_id);
            Server::apply_warehouse_grants(
                warehouse_id,
                ctx.clone(),
                as_grant_admin.clone(),
                writes(vec![entry("select", &bob)]),
            )
            .await
            .unwrap();

            let page = Server::list_warehouse_grants(
                warehouse_id,
                ctx.clone(),
                as_grant_admin.clone(),
                ListGrantsQuery::default(),
                no_pagination(),
            )
            .await
            .unwrap();
            let mut held: Vec<(String, UserOrRole)> = page
                .grants
                .iter()
                .map(|g| (g.privilege.clone(), g.principal.clone()))
                .collect();
            held.sort_by(|a, b| a.0.cmp(&b.0));
            assert_eq!(
                held,
                vec![
                    (
                        "manage_grants".to_string(),
                        UserOrRole::User(grant_admin.clone())
                    ),
                    // The creator's ownership arrives as a tuple like any grant.
                    ("ownership".to_string(), UserOrRole::User(admin.clone())),
                    ("select".to_string(), UserOrRole::User(bob.clone())),
                ]
            );

            // The delegated grantable-privileges view gates on the same grant-read
            // action and is unmasked the same way. Bob holds nothing, so every
            // privilege of the pinned vocabulary comes back disallowed.
            let asked = Server::get_warehouse_grantable_privileges(
                warehouse_id,
                ctx.clone(),
                as_grant_admin,
                lakekeeper::api::management::v1::grant::GetGrantAccessQuery {
                    principal_user: Some(bob.clone()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            let names: Vec<&str> = asked
                .privileges
                .iter()
                .map(|p| p.privilege.name.as_str())
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
                    "modify",
                    "manage_tags"
                ]
            );
            assert!(asked.privileges.iter().all(|p| !p.allowed));

            // No grant-read, no visibility: the masking is unchanged for everyone else.
            let nobody = UserId::new_unchecked("oidc", "nobody");
            let err = Server::list_warehouse_grants(
                warehouse_id,
                ctx.clone(),
                metadata(&nobody, &project_id),
                ListGrantsQuery::default(),
                no_pagination(),
            )
            .await
            .unwrap_err();
            assert_eq!(err.error.code, 404);
            assert_eq!(err.error.r#type, "NoSuchWarehouseException");
        }

        /// The instance-admin bypass splits at the grant surface: reads pass, writes do
        /// not. It is a static config credential, so if it could write grants a leaked one
        /// would escalate any principal to admin — here, by writing the very tuples the
        /// `/permissions` API refuses it. Reading is left bypassed like every other
        /// control-plane read, so an operator can still audit who holds what. Both halves
        /// are pinned: the write exemption is the only one of its kind and would be
        /// undone by anyone "restoring consistency", and the read half is what stops that
        /// exemption from being over-applied.
        #[sqlx::test]
        async fn an_instance_admin_may_read_grants_but_not_write_them(pool: PgPool) {
            let (ctx, admin, project_id, warehouse_id) = setup(pool).await;
            let operator = UserId::new_unchecked("oidc", "leaked-operator");
            let carol = UserId::new_unchecked("oidc", "carol");
            let as_instance_admin = RequestMetadataTestBuilder::builder()
                .actor(Actor::Principal(operator.clone()))
                .project_id(Some(project_id.clone()))
                .is_instance_admin(true)
                .build();

            // The escalation the split exists to prevent.
            let err = Server::apply_project_grants(
                ctx.clone(),
                as_instance_admin.clone(),
                writes(vec![entry("project_admin", &carol)]),
            )
            .await
            .unwrap_err();
            assert_eq!(err.error.code, 403);
            assert_eq!(err.error.r#type, "GrantActionForbidden");

            // Nothing was written: carol holds no project grant, and the only
            // `project_admin` is the bootstrapped operator.
            let page = Server::list_project_grants(
                ctx.clone(),
                metadata(&admin, &project_id),
                ListGrantsQuery::default(),
                no_pagination(),
            )
            .await
            .unwrap();
            assert_eq!(
                page.grants
                    .iter()
                    .map(|g| (g.principal.clone(), g.privilege.clone()))
                    .collect::<Vec<_>>(),
                vec![(UserOrRole::User(admin.clone()), "project_admin".to_string())]
            );

            // The authority check itself denies rather than allowing wholesale, so the
            // vocabulary endpoint reports what an instance admin may really grant: nothing.
            let warehouse = warehouse_target(&ctx, warehouse_id).await;
            let decisions = ctx
                .v1_state
                .authz
                .are_allowed_grants(
                    &as_instance_admin,
                    None,
                    &GrantTarget::Warehouse(&warehouse),
                    &[check("select"), check("modify")],
                )
                .await
                .unwrap();
            assert_eq!(decisions, vec![false, false]);

            // Reading is a different question from granting. An instance admin holds no
            // relations in the model, so without the bypass this listing would be
            // refused; it is allowed because auditing access is a control-plane read.
            let page = Server::list_warehouse_grants(
                warehouse_id,
                ctx.clone(),
                as_instance_admin,
                ListGrantsQuery::default(),
                no_pagination(),
            )
            .await
            .unwrap();
            assert_eq!(
                page.grants
                    .iter()
                    .map(|g| g.privilege.as_str())
                    .collect::<Vec<_>>(),
                vec!["ownership"]
            );
        }

        /// Answering for another principal requires read-assignments authority on the
        /// resource, enforced inside the authorizer rather than by its callers.
        #[sqlx::test]
        async fn answering_for_another_principal_needs_the_read_gate(pool: PgPool) {
            let (ctx, admin, project_id, warehouse_id) = setup(pool).await;
            let authorizer = &ctx.v1_state.authz;
            let warehouse = warehouse_target(&ctx, warehouse_id).await;
            let target = GrantTarget::Warehouse(&warehouse);
            let bob = UserId::new_unchecked("oidc", "bob");
            let for_bob = lakekeeper::service::authz::UserOrRole::User(bob.clone());

            // The operator may inspect.
            let decisions = authorizer
                .are_allowed_grants(
                    &metadata(&admin, &project_id),
                    Some(&for_bob),
                    &target,
                    &[check("select")],
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
                    &target,
                    &[check("select")],
                )
                .await
                .unwrap_err();
            let err =
                lakekeeper::service::events::AuthorizationFailureSource::into_error_model(err);
            assert_eq!(err.code, 403);
        }

        /// This authorizer indexes permissions by resource, so it cannot answer
        /// "everything one principal holds in this project" without reading the store a
        /// level at a time. It refuses instead of returning one unpageable response
        /// sized by the deployment. The catalog-backed arm answers the same request
        /// normally — see the `grant_ops` twin.
        #[sqlx::test]
        async fn the_project_scoped_listing_is_not_implemented(pool: PgPool) {
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

            let err = Server::list_grants(
                ctx.clone(),
                md.clone(),
                ListGrantsQuery {
                    principal_user: Some(bob.clone()),
                    principal_role: None,
                },
                no_pagination(),
            )
            .await
            .unwrap_err();
            assert_eq!(err.error.code, 501);
            assert_eq!(err.error.r#type, "GrantListingNotImplemented");

            // The remedy the refusal names has to actually work.
            let page = Server::list_warehouse_grants(
                warehouse_id,
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
            assert_eq!(
                page.grants
                    .iter()
                    .map(|g| g.privilege.as_str())
                    .collect::<Vec<_>>(),
                vec!["select"]
            );
        }

        /// A namespace is the one level whose `OpenFGA` object carries no warehouse, so
        /// its own listing is a distinct read path from the tabular levels. Kept because
        /// the refusal above sends clients here, and a nested resource is the case where
        /// that advice is least obviously sufficient.
        #[sqlx::test]
        async fn a_namespace_grant_is_applied_and_read_back_on_its_own_listing(pool: PgPool) {
            let (ctx, admin, project_id, warehouse_id) = setup(pool).await;
            let md = metadata(&admin, &project_id);
            let bob = UserId::new_unchecked("oidc", "bob");
            let namespace_id = create_namespace(&ctx, &md, warehouse_id, "grant_ofga").await;

            Server::apply_namespace_grants(
                warehouse_id,
                namespace_id,
                ctx.clone(),
                md.clone(),
                writes(vec![entry("select", &bob)]),
            )
            .await
            .unwrap();

            let page = Server::list_namespace_grants(
                warehouse_id,
                namespace_id,
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
            assert_eq!(
                page.grants
                    .iter()
                    .map(|g| (g.resource.clone(), g.privilege.clone()))
                    .collect::<Vec<_>>(),
                vec![(
                    GrantResourceResponse::Namespace {
                        warehouse_id,
                        namespace_id,
                    },
                    "select".to_string()
                )]
            );
        }

        /// Unsupported must not become a capability oracle: a caller with no relations
        /// is refused for lacking authority, and only an authorized one learns that the
        /// listing is unavailable. The gate runs before the store is consulted.
        #[sqlx::test]
        async fn the_authorization_gate_precedes_the_refusal(pool: PgPool) {
            let (ctx, _admin, project_id, _warehouse_id) = setup(pool).await;
            let nobody = UserId::new_unchecked("oidc", "nobody");

            let err = Server::list_grants(
                ctx.clone(),
                metadata(&nobody, &project_id),
                ListGrantsQuery {
                    principal_user: Some(UserId::new_unchecked("oidc", "bob")),
                    principal_role: None,
                },
                no_pagination(),
            )
            .await
            .unwrap_err();
            assert_eq!(err.error.code, 403);
            assert_eq!(err.error.r#type, "ProjectActionForbidden");
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
                    "manage_tags",
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
            assert_eq!(unavailable.len(), 8);
            assert_eq!(as_nobody.privileges.len(), 8);
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

        /// Subtree operations need grants indexed by container. `OpenFGA` indexes tuples
        /// by object, pages per object with independent tokens, and filters non-privilege
        /// tuples after fetching, so walking a subtree would mean one paged read per
        /// resource — the same reason the project-scoped listing refuses.
        ///
        /// All four routes refuse, and refuse *before* the authorization gate: a `501` is
        /// about the deployment rather than about this caller, so there is no decision to
        /// record. The refusal reaches an operator who holds every relevant permission,
        /// which is what pins that the check runs first.
        #[sqlx::test]
        async fn the_subtree_routes_are_not_implemented(pool: PgPool) {
            use lakekeeper::api::management::v1::grant::{
                ListSubtreeGrantsQuery, RevokeSubtreeGrantsRequest,
            };

            let (ctx, admin, project_id, warehouse_id) = setup(pool).await;
            let md = metadata(&admin, &project_id);
            let namespace_id = create_namespace(&ctx, &md, warehouse_id, "grant_subtree").await;
            let revoke = || RevokeSubtreeGrantsRequest {
                principal: None,
                privilege: vec![],
                resource_type: vec![],
                created_before: None,
                limit: None,
                allow_partial: false,
                include_root_level: true,
                dry_run: false,
            };

            let errors = [
                Server::list_namespace_subtree_grants(
                    warehouse_id,
                    namespace_id,
                    ctx.clone(),
                    md.clone(),
                    ListSubtreeGrantsQuery::default(),
                    no_pagination(),
                )
                .await
                .unwrap_err(),
                Server::list_warehouse_subtree_grants(
                    warehouse_id,
                    ctx.clone(),
                    md.clone(),
                    ListSubtreeGrantsQuery::default(),
                    no_pagination(),
                )
                .await
                .unwrap_err(),
                Server::revoke_namespace_subtree_grants(
                    warehouse_id,
                    namespace_id,
                    ctx.clone(),
                    md.clone(),
                    revoke(),
                )
                .await
                .unwrap_err(),
                Server::revoke_warehouse_subtree_grants(warehouse_id, ctx, md, revoke())
                    .await
                    .unwrap_err(),
            ];
            for err in errors {
                assert_eq!(err.error.code, 501);
                assert_eq!(err.error.r#type, "SubtreeGrantsNotImplemented");
            }
        }

        fn no_principal() -> lakekeeper::api::management::v1::grant::GetGrantAccessQuery {
            lakekeeper::api::management::v1::grant::GetGrantAccessQuery::default()
        }
    }
}
