//! Role-membership management reads on the **authorizer arm** (OpenFGA),
//! end-to-end against a real OpenFGA store + Postgres catalog. The catalog-arm
//! twin (AllowAll + Postgres) lives in `role_membership_ops.rs`; this file pins
//! the OpenFGA behaviour: assignment edges live in OpenFGA, role identity is
//! hydrated from the catalog (across projects), and dangling tuples (a role id
//! with no catalog row anywhere) are dropped.
//!
//! Roles are created through the management API so their `role#project` tuple
//! exists in OpenFGA (a role created directly in the DB would have no project
//! edge, so `can_read_assignments` — `can_list_roles from project` — could not
//! resolve). Assignments are written through `add_role_members`, which routes to
//! the authorizer. The bootstrapped operator (`setup`) inherits
//! `project_admin → security_admin → … → can_list_roles` and so may read.
//!
//! Gated behind the `openfga_integration_tests` module so the default nextest
//! filter excludes it; it runs under `--profile ci` with a live OpenFGA at
//! `LAKEKEEPER__OPENFGA__ENDPOINT`.

// Nested one level deep so the test path contains `::openfga_integration_tests::`,
// which the default nextest filter excludes (a root module would not match).
mod role_membership {
    mod openfga_integration_tests {
        use std::{collections::HashSet, sync::Arc};

        use lakekeeper::{
            ProjectId,
            api::{
                ApiContext, RequestMetadata, RequestMetadataTestBuilder,
                management::v1::{
                    ApiServer,
                    role::{CreateRoleRequest, Service as _},
                    role_membership::{
                        AddRoleMembersRequest, ListMembersQuery, ListRolesPageQuery, RoleMember,
                        RoleMemberRef, RoleMemberType, Service as _,
                    },
                },
            },
            service::{
                RoleId, State, UserId,
                authn::Actor,
                authz::{Authorizer as _, UserOrRoleId},
            },
        };
        use lakekeeper_authz_openfga::{
            OpenFGAAuthorizer, new_authorizer_in_empty_store_from_default_config,
        };
        use lakekeeper_integration_tests::{SetupTestCatalog, memory_io_profile};
        use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
        use sqlx::PgPool;

        type Ctx = ApiContext<State<OpenFGAAuthorizer, PostgresBackend, SecretsState>>;

        /// Build an OpenFGA-backed context with a freshly-migrated, isolated store,
        /// bootstrapping `user_id` as operator. Returns the context, the operator's
        /// id, and the project the warehouse was created in.
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

        /// Create a role via the management API (writes the `role#project` +
        /// ownership tuples in OpenFGA and the catalog row).
        async fn create_role(ctx: &Ctx, md: &RequestMetadata, name: &str) -> RoleId {
            ApiServer::create_role(
                CreateRoleRequest::builder().name(name.to_string()).build(),
                ctx.clone(),
                md.clone(),
            )
            .await
            .unwrap()
            .id
        }

        fn user_member(user_id: &UserId) -> RoleMemberRef {
            RoleMemberRef::User {
                id: user_id.clone(),
            }
        }

        fn role_member(role_id: RoleId) -> RoleMemberRef {
            RoleMemberRef::Role { id: role_id }
        }

        /// The kind of a response member, for `?type=`-style assertions.
        fn kind(m: &RoleMember) -> RoleMemberType {
            match m {
                RoleMember::User(_) => RoleMemberType::User,
                RoleMember::Role(_) => RoleMemberType::Role,
            }
        }

        /// The member's principal id rendered as a string.
        fn member_id_string(m: &RoleMember) -> String {
            match m {
                RoleMember::User(u) => u.id.to_string(),
                RoleMember::Role(rm) => rm.id.to_string(),
            }
        }

        fn list_query(r#type: Option<RoleMemberType>) -> ListMembersQuery {
            ListMembersQuery {
                r#type,
                page_token: None,
                page_size: None,
            }
        }

        fn page_query() -> ListRolesPageQuery {
            ListRolesPageQuery {
                page_token: None,
                page_size: None,
            }
        }

        /// A role's direct members (a member role B and a user U) are read back from
        /// OpenFGA, role identity hydrated from the catalog, both carrying the
        /// tuple's write timestamp. The `?type=` filter composes.
        #[sqlx::test]
        async fn list_role_members_openfga(pool: PgPool) {
            let (ctx, admin, project_id) = setup(pool).await;
            let md = metadata(&admin, &project_id);
            let a = create_role(&ctx, &md, "A").await;
            let b = create_role(&ctx, &md, "B").await;
            let u = UserId::new_unchecked("oidc", "u");

            ApiServer::add_role_members(
                ctx.clone(),
                md.clone(),
                a,
                AddRoleMembersRequest {
                    members: vec![role_member(b), user_member(&u)],
                },
            )
            .await
            .unwrap();

            let page = ApiServer::list_role_members(ctx.clone(), md.clone(), a, list_query(None))
                .await
                .unwrap();
            let users: HashSet<String> = page
                .members
                .iter()
                .filter(|m| kind(m) == RoleMemberType::User)
                .map(member_id_string)
                .collect();
            let roles: HashSet<String> = page
                .members
                .iter()
                .filter(|m| kind(m) == RoleMemberType::Role)
                .map(member_id_string)
                .collect();
            assert_eq!(users, [u.to_string()].into_iter().collect());
            assert_eq!(roles, [b.to_string()].into_iter().collect());

            // The `?type=` filter composes on the authorizer arm.
            let only_roles = ApiServer::list_role_members(
                ctx.clone(),
                md.clone(),
                a,
                list_query(Some(RoleMemberType::Role)),
            )
            .await
            .unwrap();
            assert_eq!(only_roles.members.len(), 1);
            assert_eq!(kind(&only_roles.members[0]), RoleMemberType::Role);
            assert_eq!(member_id_string(&only_roles.members[0]), b.to_string());

            // The role member B is hydrated from the catalog (id-only in OpenFGA).
            let b_member = page
                .members
                .iter()
                .find_map(|m| match m {
                    RoleMember::Role(rm) if rm.id == b => Some(rm),
                    _ => None,
                })
                .expect("role member B present");
            assert_eq!(b_member.name, "B");
            // User `u` was assigned via OpenFGA but never provisioned in the catalog,
            // so it has no catalog row: every identity field beyond the id is null.
            let u_member = page
                .members
                .iter()
                .find_map(|m| match m {
                    RoleMember::User(uu) if uu.id == u => Some(uu),
                    _ => None,
                })
                .expect("user member u present");
            assert_eq!(u_member.name, None);
            assert_eq!(u_member.email, None);
            assert_eq!(u_member.user_type, None);
        }

        /// A child role's direct `member-of` listing names its parent, with identity
        /// hydrated from the catalog and the tuple's write timestamp.
        #[sqlx::test]
        async fn list_role_member_of_openfga(pool: PgPool) {
            let (ctx, admin, project_id) = setup(pool).await;
            let md = metadata(&admin, &project_id);
            let parent = create_role(&ctx, &md, "P").await;
            let child = create_role(&ctx, &md, "C").await;

            ApiServer::add_role_members(
                ctx.clone(),
                md.clone(),
                parent,
                AddRoleMembersRequest {
                    members: vec![role_member(child)],
                },
            )
            .await
            .unwrap();

            let page = ApiServer::list_role_member_of(ctx.clone(), md.clone(), child, page_query())
                .await
                .unwrap();
            assert_eq!(page.roles.len(), 1);
            assert_eq!(page.roles[0].id, parent);
            // Identity is hydrated from the catalog (the authorizer only stores ids).
            assert_eq!(page.roles[0].name, "P");
        }

        /// A user's directly-assigned roles are hydrated from the catalog. An unknown
        /// user is tolerated (empty page, 200) — OpenFGA cannot prove non-existence,
        /// so the authorizer arm never 404s (unlike the catalog arm).
        #[sqlx::test]
        async fn list_user_roles_openfga(pool: PgPool) {
            let (ctx, admin, project_id) = setup(pool).await;
            let md = metadata(&admin, &project_id);
            let r = create_role(&ctx, &md, "R").await;
            let u = UserId::new_unchecked("oidc", "u");

            ApiServer::add_role_members(
                ctx.clone(),
                md.clone(),
                r,
                AddRoleMembersRequest {
                    members: vec![user_member(&u)],
                },
            )
            .await
            .unwrap();

            let page = ApiServer::list_user_roles(ctx.clone(), md.clone(), u.clone(), page_query())
                .await
                .unwrap();
            assert_eq!(page.roles.len(), 1);
            assert_eq!(page.roles[0].id, r);
            assert_eq!(page.roles[0].name, "R");

            // Unknown user → empty page, NOT a 404 (authorizer-arm divergence).
            let ghost = UserId::new_unchecked("oidc", "ghost");
            let empty = ApiServer::list_user_roles(ctx.clone(), md.clone(), ghost, page_query())
                .await
                .unwrap();
            assert!(empty.roles.is_empty());
            assert!(empty.next_page_token.is_none());
        }

        /// The three transitive listings are NOT supported under an
        /// assignment-managing authorizer (OpenFGA): they return 501 after the
        /// authz check passes. The direct listings (above) ARE supported; full
        /// transitive support lives on catalog backends (see role_membership_ops.rs).
        #[sqlx::test]
        async fn transitive_listings_not_supported_openfga(pool: PgPool) {
            let (ctx, admin, project_id) = setup(pool).await;
            let md = metadata(&admin, &project_id);
            let role = create_role(&ctx, &md, "A").await;
            let user = UserId::new_unchecked("oidc", "u");

            let members_err = ApiServer::list_role_transitive_members(
                ctx.clone(),
                md.clone(),
                role,
                list_query(None),
            )
            .await
            .unwrap_err();
            let member_of_err = ApiServer::list_role_transitive_member_of(
                ctx.clone(),
                md.clone(),
                role,
                page_query(),
            )
            .await
            .unwrap_err();
            let roles_err =
                ApiServer::list_user_transitive_roles(ctx.clone(), md.clone(), user, page_query())
                    .await
                    .unwrap_err();

            for err in [members_err, member_of_err, roles_err] {
                assert_eq!(err.error.code, http::StatusCode::NOT_IMPLEMENTED.as_u16());
                assert_eq!(
                    err.error.r#type,
                    "TransitiveRoleMembershipListingNotImplemented"
                );
            }
        }

        /// An actor with no grants in the project is DENIED these reads — the authz
        /// check fires before any listing. Guards against silently dropping the
        /// check (every other test exercises only the allow path).
        #[sqlx::test]
        async fn reads_denied_for_unprivileged_openfga(pool: PgPool) {
            let (ctx, admin, project_id) = setup(pool).await;
            let a = create_role(&ctx, &metadata(&admin, &project_id), "A").await;

            let nobody = UserId::new_unchecked("oidc", "nobody");
            let nobody_md = metadata(&nobody, &project_id);

            // Role-target read (require_role_action).
            let err =
                ApiServer::list_role_members(ctx.clone(), nobody_md.clone(), a, list_query(None))
                    .await
                    .unwrap_err();
            assert_eq!(err.error.code, http::StatusCode::FORBIDDEN.as_u16());

            // Transitive role read is gated by the PROJECT-scoped `ListRoles`
            // capability, and authz fires BEFORE the catalog-only 501, so an
            // unprivileged actor is denied (403) rather than handed a 501.
            let err = ApiServer::list_role_transitive_members(
                ctx.clone(),
                nobody_md.clone(),
                a,
                list_query(None),
            )
            .await
            .unwrap_err();
            assert_eq!(err.error.code, http::StatusCode::FORBIDDEN.as_u16());

            // User-target read of ANOTHER user (require_user_action; same-user would
            // short-circuit allow, so target the admin to exercise the real check).
            let err =
                ApiServer::list_user_roles(ctx.clone(), nobody_md, admin.clone(), page_query())
                    .await
                    .unwrap_err();
            assert_eq!(err.error.code, http::StatusCode::FORBIDDEN.as_u16());
        }

        /// A role id present in OpenFGA but with no catalog row anywhere (a dangling
        /// tuple, e.g. a since-deleted role) is dropped from the listing with a
        /// warn; real members remain. This is the behavior unique to the authorizer
        /// arm — the catalog arm can never have a dangling role id.
        #[sqlx::test]
        async fn orphan_role_member_dropped_openfga(pool: PgPool) {
            let (ctx, admin, project_id) = setup(pool).await;
            let md = metadata(&admin, &project_id);
            let a = create_role(&ctx, &md, "A").await;
            let u = UserId::new_unchecked("oidc", "u");
            ApiServer::add_role_members(
                ctx.clone(),
                md.clone(),
                a,
                AddRoleMembersRequest {
                    members: vec![user_member(&u)],
                },
            )
            .await
            .unwrap();

            // Write a member-role edge for a role that was never created in the
            // catalog, directly via the authorizer facet.
            let ghost = RoleId::new_random();
            ctx.v1_state
                .authz
                .role_assignments()
                .expect("openfga manages assignments")
                .add_role_assignments(&md, project_id.clone(), &[(UserOrRoleId::Role(ghost), a)])
                .await
                .unwrap();

            let page = ApiServer::list_role_members(ctx.clone(), md.clone(), a, list_query(None))
                .await
                .unwrap();
            // The ghost role is dropped; only the real user member remains.
            assert_eq!(page.members.len(), 1);
            assert_eq!(kind(&page.members[0]), RoleMemberType::User);
            assert_eq!(member_id_string(&page.members[0]), u.to_string());
        }

        /// Direct members paginate via OpenFGA's continuation token: a page_size=1
        /// drain yields every member exactly once, no gaps or duplicates.
        #[sqlx::test]
        async fn list_role_members_paginates_openfga(pool: PgPool) {
            let (ctx, admin, project_id) = setup(pool).await;
            let md = metadata(&admin, &project_id);
            let a = create_role(&ctx, &md, "A").await;
            let u1 = UserId::new_unchecked("oidc", "u1");
            let u2 = UserId::new_unchecked("oidc", "u2");
            let u3 = UserId::new_unchecked("oidc", "u3");
            ApiServer::add_role_members(
                ctx.clone(),
                md.clone(),
                a,
                AddRoleMembersRequest {
                    members: vec![user_member(&u1), user_member(&u2), user_member(&u3)],
                },
            )
            .await
            .unwrap();

            let mut seen: Vec<String> = Vec::new();
            let mut token: Option<String> = None;
            let mut guard = 0;
            loop {
                guard += 1;
                assert!(guard < 20, "pagination did not terminate");
                let query = ListMembersQuery {
                    r#type: None,
                    page_token: token.clone(),
                    page_size: Some(1),
                };
                let page = ApiServer::list_role_members(ctx.clone(), md.clone(), a, query)
                    .await
                    .unwrap();
                assert!(page.members.len() <= 1);
                seen.extend(page.members.iter().map(member_id_string));
                match page.next_page_token {
                    Some(t) => token = Some(t),
                    None => break,
                }
            }
            assert_eq!(seen.len(), 3, "each member returned exactly once");
            let unique: HashSet<String> = seen.into_iter().collect();
            assert_eq!(
                unique,
                [u1.to_string(), u2.to_string(), u3.to_string()]
                    .into_iter()
                    .collect()
            );
        }
    }
}
