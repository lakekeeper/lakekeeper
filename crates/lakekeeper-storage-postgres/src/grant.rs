//! Grant storage: `principal -> privilege -> resource` rows.
//!
//! Authoritative for direct grants when the authorizer does not manage them itself
//! (`Authorizer::grants() == None`). The privilege is stored by name as text; its
//! vocabulary belongs to the authorizer and is never interpreted here.
//!
//! A grant's identity is its whole row, so apply is
//! `on conflict do nothing returning` and revoke is `delete returning`: both report
//! the exact delta, which is what lets callers emit events only for real changes.
//! The revoke matches principals with `is not distinct from`, mirroring the
//! `nulls not distinct` unique key: exactly one of `user_id`/`role_id` is set and which
//! one varies per row, so no constraint pins them and plain equality would never match
//! the null side.
//!
//! Locating rows by resource only ever compares the columns that resource kind
//! populates: `grant_resource_target` already forces the rest to null for a given
//! `resource_type`, so comparing them would filter nothing. Each comparison is written
//! `($n is null or column = $n)` rather than `column is not distinct from $n`, which is
//! what keeps one statement serving every resource kind while staying indexable — the
//! planner folds the null test away for a non-null parameter and uses the equality as
//! an index condition, whereas `is not distinct from` is not indexable at all and turns
//! these statements into full scans.
//!
//! The kind of tabular a grant is on is likewise checked once per statement, against
//! the parameters only, never joined per row.
//!
//! Applying a diff takes a per-resource advisory lock first. Deletes and inserts in one
//! transaction make two crossing diffs — each revoking a grant the other adds — wait on
//! each other's uncommitted rows, which the deadlock detector resolves by killing one.
//! Serializing per resource also keeps the result equal to some order of the two
//! requests: run concurrently, both revokes can fail to stick and leave a state neither
//! caller asked for. Folding the delete and insert into one statement does not help —
//! a row lock is the tuple's `xmax`, held to transaction end and taken as rows are
//! processed, so the same wait-for cycle forms across the statement boundary.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use lakekeeper::{
    CONFIG,
    api::iceberg::v1::PaginationQuery,
    service::{
        ApplyGrantsStoreError, DatabaseIntegrityError, GenericTableId, GrantLockTimeout,
        GrantTargetNotFound, GrantUserNotFound, ListGrantsStoreError, NamespaceId, ProjectId,
        TableId, TagDefinitionId, ViewId, WarehouseId,
        authn::UserId,
        authz::{
            AppliedGrants, GrantFilter, GrantResource, GrantRow, GrantSpec, ListGrantsResultPage,
            PrincipalType, UserOrRoleId,
        },
    },
};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::{
    dbutils::DBErrorHandler,
    pagination::{PaginateToken, V1PaginateToken},
    tabular::TabularType,
};

/// The resource kinds as stored. Coarser than the API's [`ResourceType`]: tables,
/// views and generic tables share one value, because `tabular` already records which
/// of the three an id refers to and a second copy here could disagree with it. Readers
/// recover the distinction by joining `tabular`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "grant_resource_type", rename_all = "kebab-case")]
enum StoredResourceType {
    Server,
    Project,
    Warehouse,
    Namespace,
    Tabular,
    Tag,
}

/// Which kind of tabular a resource names, or `None` if it is not a tabular.
fn tabular_kind_of(resource: &GrantResource) -> Option<TabularType> {
    match resource {
        GrantResource::Table { .. } => Some(TabularType::Table),
        GrantResource::View { .. } => Some(TabularType::View),
        GrantResource::GenericTable { .. } => Some(TabularType::GenericTable),
        GrantResource::Server
        | GrantResource::Project(_)
        | GrantResource::Warehouse(_)
        | GrantResource::Namespace { .. }
        | GrantResource::Tag(_) => None,
    }
}

impl StoredResourceType {
    fn of(resource: &GrantResource) -> Self {
        match resource {
            GrantResource::Server => Self::Server,
            GrantResource::Project(_) => Self::Project,
            GrantResource::Warehouse(_) => Self::Warehouse,
            GrantResource::Namespace { .. } => Self::Namespace,
            GrantResource::Table { .. }
            | GrantResource::View { .. }
            | GrantResource::GenericTable { .. } => Self::Tabular,
            GrantResource::Tag(_) => Self::Tag,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Server => "server",
            Self::Project => "project",
            Self::Warehouse => "warehouse",
            Self::Namespace => "namespace",
            Self::Tabular => "tabular",
            Self::Tag => "tag",
        }
    }
}

/// Distinct seed for the grant-apply advisory lock so a grant lock can never collide
/// with another feature's lock on the same hash space. (`grantapl` in ASCII.)
const GRANT_APPLY_LOCK_SEED: i64 = 0x6772_616E_7461_706C;

/// Foreign keys guard the principal and every resource column, so a violation
/// means the grant named something that does not exist.
///
/// A contended-lock outcome is retriable, not a backend failure: `55P03` is the
/// `lock_timeout` elapsing on the advisory lock, and `40P01` is the deadlock detector
/// picking this transaction as its victim.
///
/// The `40P01` arm is load-bearing, not defensive. The advisory lock is keyed per
/// resource, so it only orders writers that go through `apply_grants`; the paths that
/// remove grants without one — `delete_grants_for_user`, and the `on delete cascade`
/// from dropping a warehouse, namespace, tabular or tag definition — structurally
/// cannot take it. A deadlock there is expected under load, and must reach the caller
/// as a retriable conflict rather than a 503 that reads as "backend down" and stops
/// them retrying.
fn map_write_error(err: sqlx::Error) -> ApplyGrantsStoreError {
    match &err {
        sqlx::Error::Database(db) if db.is_foreign_key_violation() => {
            GrantTargetNotFound::new().into()
        }
        sqlx::Error::Database(db) if matches!(db.code().as_deref(), Some("55P03" | "40P01")) => {
            GrantLockTimeout::new().into()
        }
        _ => err.into_catalog_backend_error().into(),
    }
}

/// Total order over a grant's identity: resource, then principal, then privilege.
type GrantOrderKey = (String, String, String);

/// Sort key for one grant, built from the same canonical resource key the lock uses so
/// the two orderings cannot drift apart.
fn order_key(spec: &GrantSpec) -> GrantOrderKey {
    let principal = match &spec.principal {
        UserOrRoleId::User(user_id) => format!("user:{user_id}"),
        UserOrRoleId::Role(role_id) => format!("role:{role_id}"),
    };
    (
        resource_lock_key(&spec.resource),
        principal,
        spec.privilege.clone(),
    )
}

/// Canonical advisory-lock key for a resource: the stored discriminator plus the
/// columns that discriminator populates.
///
/// Keyed on the stored shape rather than the requested kind, so a diff naming a table
/// and one naming a view with the same id take the same lock — they address the same
/// rows. Built from literals and ids only, never from a derived `Debug` rendering, so
/// two replicas on different builds still agree during a rolling deploy.
fn resource_lock_key(resource: &GrantResource) -> String {
    let columns = ResourceColumns::of(resource);
    let uuid = |id: Option<Uuid>| id.map(|i| i.to_string()).unwrap_or_default();
    format!(
        "{}|{}|{}|{}|{}|{}",
        StoredResourceType::of(resource).as_str(),
        columns.project_id.as_deref().unwrap_or_default(),
        uuid(columns.warehouse_id),
        uuid(columns.namespace_id),
        uuid(columns.tabular_id),
        uuid(columns.tag_definition_id),
    )
}

/// Serialize this diff against concurrent diffs on the same resources.
///
/// Keys are sorted before acquisition: a diff spanning two resources takes two locks,
/// and taking them in a canonical order is what stops two such diffs from deadlocking
/// on the locks themselves.
///
/// `lock_timeout` bounds the wait so a stuck holder fails fast with a typed retriable
/// error instead of hanging, and is deliberately **not** reset afterwards: it must cover
/// the row locks the apply takes next, not just the advisory locks. Those row locks
/// conflict with writers that cannot take the advisory lock — a cascade from dropping the
/// container, a user deletion, or any handler holding `FOR UPDATE` on a parent row across
/// a network call — and such a wait is neither a deadlock nor a foreign-key failure, so
/// nothing else would end it. Unbounded, the apply would wait behind them while holding
/// the advisory lock, turning every concurrent apply on the resource into a retriable
/// conflict and keeping a write-pool connection past the request's own timeout.
///
/// `SET LOCAL` scopes the bound to this transaction, and the apply owns its transaction
/// (see `CatalogGrantOps::apply_grants`), so it cannot reach a caller's other statements.
async fn lock_resources(
    writes: &[GrantSpec],
    deletes: &[GrantSpec],
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), ApplyGrantsStoreError> {
    let mut keys: Vec<String> = writes
        .iter()
        .chain(deletes)
        .map(|spec| resource_lock_key(&spec.resource))
        .collect();
    keys.sort_unstable();
    keys.dedup();
    if keys.is_empty() {
        return Ok(());
    }

    sqlx::query("SET LOCAL lock_timeout = '3s'")
        .execute(&mut **transaction)
        .await
        .map_err(DBErrorHandler::into_catalog_backend_error)?;
    for key in keys {
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, $2))")
            .bind(key)
            .bind(GRANT_APPLY_LOCK_SEED)
            .execute(&mut **transaction)
            .await
            .map_err(map_write_error)?;
    }
    Ok(())
}

/// The columns that make up a grant's identity, plus provenance.
#[derive(Debug)]
struct GrantAssignmentRow {
    grant_id: Uuid,
    principal_type: PrincipalType,
    user_id: Option<String>,
    role_id: Option<Uuid>,
    privilege: String,
    resource_type: StoredResourceType,
    project_id: Option<String>,
    warehouse_id: Option<Uuid>,
    namespace_id: Option<Uuid>,
    tabular_id: Option<Uuid>,
    tag_definition_id: Option<Uuid>,
    /// Joined from `tabular`: which of table, view or generic table `tabular_id` names.
    /// Null for every non-tabular grant.
    tabular_typ: Option<TabularType>,
    created_at: DateTime<Utc>,
}

impl GrantAssignmentRow {
    fn principal(&self) -> Result<UserOrRoleId, DatabaseIntegrityError> {
        match self.principal_type {
            PrincipalType::User => {
                let id = self.user_id.as_deref().ok_or_else(|| {
                    DatabaseIntegrityError::new("Grant with principal_type `user` has no user_id")
                })?;
                Ok(UserOrRoleId::User(user_id_from_db(id)?))
            }
            PrincipalType::Role => {
                let id = self.role_id.ok_or_else(|| {
                    DatabaseIntegrityError::new("Grant with principal_type `role` has no role_id")
                })?;
                Ok(UserOrRoleId::Role(id.into()))
            }
        }
    }

    fn resource(&self) -> Result<GrantResource, DatabaseIntegrityError> {
        let resource_type = self.resource_type;
        let missing = |column: &str| {
            DatabaseIntegrityError::new(format!(
                "Grant on {} has no {column}",
                resource_type.as_str()
            ))
        };
        let warehouse_id = || -> Result<WarehouseId, DatabaseIntegrityError> {
            Ok(self
                .warehouse_id
                .ok_or_else(|| missing("warehouse_id"))?
                .into())
        };
        let tabular_id = || -> Result<Uuid, DatabaseIntegrityError> {
            self.tabular_id.ok_or_else(|| missing("tabular_id"))
        };

        Ok(match resource_type {
            StoredResourceType::Server => GrantResource::Server,
            StoredResourceType::Project => GrantResource::Project(ProjectId::from_db_unchecked(
                self.project_id
                    .clone()
                    .ok_or_else(|| missing("project_id"))?,
            )),
            StoredResourceType::Warehouse => GrantResource::Warehouse(warehouse_id()?),
            StoredResourceType::Namespace => GrantResource::Namespace {
                warehouse_id: warehouse_id()?,
                namespace_id: NamespaceId::from(
                    self.namespace_id.ok_or_else(|| missing("namespace_id"))?,
                ),
            },
            // The stored kind is just `tabular`; which of the three it is comes from
            // the joined tabular row. A tabular grant whose tabular is gone cannot
            // exist — grant_tabular_fkey cascades — so a missing typ is corruption.
            StoredResourceType::Tabular => {
                let warehouse_id = warehouse_id()?;
                let tabular_id = tabular_id()?;
                match self.tabular_typ.ok_or_else(|| missing("tabular typ"))? {
                    TabularType::Table => GrantResource::Table {
                        warehouse_id,
                        table_id: TableId::from(tabular_id),
                    },
                    TabularType::View => GrantResource::View {
                        warehouse_id,
                        view_id: ViewId::from(tabular_id),
                    },
                    TabularType::GenericTable => GrantResource::GenericTable {
                        warehouse_id,
                        generic_table_id: GenericTableId::from(tabular_id),
                    },
                }
            }
            StoredResourceType::Tag => GrantResource::Tag(TagDefinitionId::from(
                self.tag_definition_id
                    .ok_or_else(|| missing("tag_definition_id"))?,
            )),
        })
    }

    fn into_spec(self) -> Result<GrantSpec, DatabaseIntegrityError> {
        Ok(GrantSpec {
            principal: self.principal()?,
            resource: self.resource()?,
            privilege: self.privilege,
        })
    }

    /// The grant as a spec, using a resource the caller already knows.
    fn into_spec_on(self, resource: GrantResource) -> Result<GrantSpec, DatabaseIntegrityError> {
        Ok(GrantSpec {
            principal: self.principal()?,
            resource,
            privilege: self.privilege,
        })
    }

    /// The grant as a listing row, using a resource the caller already knows. Every
    /// grant in a resource-scoped listing is on that one resource, so the tabular kind
    /// need not be recovered per row.
    fn into_row_on(self, resource: GrantResource) -> Result<GrantRow, DatabaseIntegrityError> {
        let principal = self.principal()?;
        Ok(GrantRow {
            principal,
            resource,
            privilege: self.privilege,
            created_at: Some(self.created_at),
        })
    }

    fn into_row(self) -> Result<GrantRow, DatabaseIntegrityError> {
        let principal = self.principal()?;
        let resource = self.resource()?;
        Ok(GrantRow {
            principal,
            resource,
            privilege: self.privilege,
            created_at: Some(self.created_at),
        })
    }
}

fn user_id_from_db(s: &str) -> Result<UserId, DatabaseIntegrityError> {
    UserId::try_from(s).map_err(|e| DatabaseIntegrityError::new(e.message))
}

/// A resource split into the columns that locate it. Exactly one shape is populated,
/// keyed by the resource type.
struct ResourceColumns {
    project_id: Option<String>,
    warehouse_id: Option<Uuid>,
    namespace_id: Option<Uuid>,
    tabular_id: Option<Uuid>,
    tag_definition_id: Option<Uuid>,
}

impl ResourceColumns {
    fn of(resource: &GrantResource) -> Self {
        let mut columns = Self {
            project_id: None,
            warehouse_id: None,
            namespace_id: None,
            tabular_id: None,
            tag_definition_id: None,
        };
        match resource {
            GrantResource::Server => {}
            GrantResource::Project(project_id) => {
                columns.project_id = Some(project_id.to_string());
            }
            GrantResource::Warehouse(warehouse_id) => {
                columns.warehouse_id = Some(**warehouse_id);
            }
            GrantResource::Namespace {
                warehouse_id,
                namespace_id,
            } => {
                columns.warehouse_id = Some(**warehouse_id);
                columns.namespace_id = Some(**namespace_id);
            }
            GrantResource::Table {
                warehouse_id,
                table_id,
            } => {
                columns.warehouse_id = Some(**warehouse_id);
                columns.tabular_id = Some(**table_id);
            }
            GrantResource::View {
                warehouse_id,
                view_id,
            } => {
                columns.warehouse_id = Some(**warehouse_id);
                columns.tabular_id = Some(**view_id);
            }
            GrantResource::GenericTable {
                warehouse_id,
                generic_table_id,
            } => {
                columns.warehouse_id = Some(**warehouse_id);
                columns.tabular_id = Some(**generic_table_id);
            }
            GrantResource::Tag(tag_definition_id) => {
                columns.tag_definition_id = Some(**tag_definition_id);
            }
        }
        columns
    }
}

/// The principal-and-privilege half of a grant's identity, as parallel arrays. One
/// entry per input spec, index-aligned. Sufficient on its own for the revoke, which
/// binds the resource as scalars.
#[derive(Default)]
struct PrincipalColumns {
    principal_type: Vec<PrincipalType>,
    user_id: Vec<Option<String>>,
    role_id: Vec<Option<Uuid>>,
    privilege: Vec<String>,
}

impl PrincipalColumns {
    fn from_specs<'a>(specs: impl IntoIterator<Item = &'a GrantSpec>) -> Self {
        let mut columns = Self::default();
        for spec in specs {
            match &spec.principal {
                UserOrRoleId::User(user_id) => {
                    columns.principal_type.push(PrincipalType::User);
                    columns.user_id.push(Some(user_id.to_string()));
                    columns.role_id.push(None);
                }
                UserOrRoleId::Role(role_id) => {
                    columns.principal_type.push(PrincipalType::Role);
                    columns.user_id.push(None);
                    columns.role_id.push(Some(**role_id));
                }
            }
            columns.privilege.push(spec.privilege.clone());
        }
        columns
    }
}

/// The full identity columns of a grant, split into the parallel arrays the insert
/// binds. One entry per input spec, index-aligned.
#[derive(Default)]
struct GrantColumns {
    principal: PrincipalColumns,
    resource_type: Vec<StoredResourceType>,
    project_id: Vec<Option<String>>,
    warehouse_id: Vec<Option<Uuid>>,
    namespace_id: Vec<Option<Uuid>>,
    tabular_id: Vec<Option<Uuid>>,
    tag_definition_id: Vec<Option<Uuid>>,
}

impl GrantColumns {
    /// Takes references so the caller can order them without cloning the specs.
    fn from_specs(specs: &[&GrantSpec]) -> Self {
        let mut columns = Self {
            principal: PrincipalColumns::from_specs(specs.iter().copied()),
            ..Self::default()
        };
        for spec in specs {
            columns
                .resource_type
                .push(StoredResourceType::of(&spec.resource));

            let resource = ResourceColumns::of(&spec.resource);
            columns.project_id.push(resource.project_id);
            columns.warehouse_id.push(resource.warehouse_id);
            columns.namespace_id.push(resource.namespace_id);
            columns.tabular_id.push(resource.tabular_id);
            columns.tag_definition_id.push(resource.tag_definition_id);
        }
        columns
    }
}

/// A grant on a tabular names which kind of tabular it is; the tabular must really be
/// of that kind, or the grant is on something other than what it says.
///
/// The listing and the revoke get this for free as a predicate. A write cannot: with
/// `on conflict do nothing` an unmatched guard is indistinguishable from a grant that
/// already existed, so a wrong kind would insert nothing and be reported as "already
/// granted" rather than refused. Hence an explicit check, once per resource written.
/// A write naming an unknown user would otherwise surface as the foreign key's
/// "principal or resource of a grant does not exist", which names nothing. Roles are
/// validated in the management layer, where their project scope lives; users have no
/// scope, so their existence check lives with the store that enforces it. Mirrors the
/// foreign key exactly — soft-deleted users keep their row and stay grantable.
async fn require_users_exist(
    specs: &[GrantSpec],
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), ApplyGrantsStoreError> {
    let mut user_ids: Vec<String> = specs
        .iter()
        .filter_map(|spec| match &spec.principal {
            UserOrRoleId::User(user_id) => Some(user_id.to_string()),
            UserOrRoleId::Role(_) => None,
        })
        .collect();
    if user_ids.is_empty() {
        return Ok(());
    }
    user_ids.sort_unstable();
    user_ids.dedup();
    let found = sqlx::query_scalar!(r#"SELECT id FROM users WHERE id = ANY($1)"#, &user_ids)
        .fetch_all(&mut **transaction)
        .await
        .map_err(DBErrorHandler::into_catalog_backend_error)?;
    if let Some(missing) = user_ids.iter().find(|id| !found.contains(id)) {
        return Err(GrantUserNotFound::new(missing.as_str()).into());
    }
    Ok(())
}

async fn require_tabular_kind(
    resource: &GrantResource,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), ApplyGrantsStoreError> {
    let Some(kind) = tabular_kind_of(resource) else {
        return Ok(());
    };
    let columns = ResourceColumns::of(resource);
    let found = sqlx::query_scalar!(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM tabular
            WHERE warehouse_id = $1 AND tabular_id = $2 AND typ = $3
        ) AS "found!"
        "#,
        columns.warehouse_id,
        columns.tabular_id,
        kind as TabularType,
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| ApplyGrantsStoreError::from(e.into_catalog_backend_error()))?;

    if found {
        Ok(())
    } else {
        Err(GrantTargetNotFound::new().into())
    }
}

/// Group specs by the resource they name, preserving first-seen order so the locks are
/// taken in a stable sequence. Not an ordering guarantee for the reported delta: inserts
/// are re-sorted into canonical order below, and the delete delta comes back in scan
/// order. Every current caller works on one resource at a time, so this yields a single
/// group.
fn group_by_resource(specs: &[GrantSpec]) -> Vec<(&GrantResource, Vec<&GrantSpec>)> {
    let mut groups: Vec<(&GrantResource, Vec<&GrantSpec>)> = Vec::new();
    for spec in specs {
        if let Some((_, group)) = groups
            .iter_mut()
            .find(|(resource, _)| *resource == &spec.resource)
        {
            group.push(spec);
        } else {
            groups.push((&spec.resource, vec![spec]));
        }
    }
    groups
}

fn rows_into_specs(
    rows: Vec<GrantAssignmentRow>,
) -> Result<Vec<GrantSpec>, DatabaseIntegrityError> {
    rows.into_iter()
        .map(GrantAssignmentRow::into_spec)
        .collect()
}

/// Insert grants into a transaction the caller opened for other work.
///
/// Takes no advisory lock: that serializes diffs which cross — each revoking what the
/// other adds — and an insert with no delete side has nothing to cross. It does bound its
/// wait, because the foreign keys take `FOR KEY SHARE` on the grant's resource and
/// principal, and another handler may hold `FOR UPDATE` there across a network call;
/// unbounded, the caller would queue behind it holding its own connection. The bound is
/// reset so it governs nothing but this insert.
pub(crate) async fn insert_grants_bounded(
    specs: &[GrantSpec],
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<Vec<GrantSpec>, ApplyGrantsStoreError> {
    if specs.is_empty() {
        return Ok(Vec::new());
    }
    sqlx::query("SET LOCAL lock_timeout = '3s'")
        .execute(&mut **transaction)
        .await
        .map_err(DBErrorHandler::into_catalog_backend_error)?;
    let created = insert_grants(specs, transaction).await?;
    sqlx::query("SET LOCAL lock_timeout = DEFAULT")
        .execute(&mut **transaction)
        .await
        .map_err(DBErrorHandler::into_catalog_backend_error)?;
    Ok(created)
}

/// Insert `specs`, ignoring grants that already exist. Returns the grants actually
/// created.
pub(crate) async fn insert_grants(
    specs: &[GrantSpec],
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<Vec<GrantSpec>, ApplyGrantsStoreError> {
    if specs.is_empty() {
        return Ok(Vec::new());
    }
    // A tabular grant must name the kind the tabular actually is. Checked before the
    // write, once per distinct resource, for the reason given on require_tabular_kind.
    for (resource, _) in group_by_resource(specs) {
        require_tabular_kind(resource, transaction).await?;
    }
    require_users_exist(specs, transaction).await?;

    // Insert in a canonical order. Rows are processed in array order, so two concurrent
    // inserts of overlapping keys take their unique-index locks in the same sequence and
    // cannot form a wait-for cycle. The per-resource lock in `lock_resources` already
    // serializes applies; this also covers callers that structurally cannot take it.
    // (The delete side has no equivalent: its row locks are acquired in whatever order
    // the planner scans, which the input order does not control.)
    let mut ordered: Vec<(GrantOrderKey, &GrantSpec)> =
        specs.iter().map(|spec| (order_key(spec), spec)).collect();
    ordered.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
    let ordered: Vec<&GrantSpec> = ordered.into_iter().map(|(_, spec)| spec).collect();

    let columns = GrantColumns::from_specs(&ordered);

    // The insert is wrapped in a CTE because RETURNING cannot join, and the tabular
    // kind the rows do not store has to come back with them.
    let rows = sqlx::query_as!(
        GrantAssignmentRow,
        r#"
        WITH inserted AS (
            INSERT INTO grant_assignment (
                principal_type, user_id, role_id, privilege, resource_type,
                project_id, warehouse_id, namespace_id, tabular_id, tag_definition_id
            )
            SELECT
                t.principal_type, t.user_id, t.role_id, t.privilege, t.resource_type,
                t.project_id, t.warehouse_id, t.namespace_id, t.tabular_id,
                t.tag_definition_id
            FROM UNNEST(
                $1::grant_principal_type[], $2::text[], $3::uuid[], $4::text[],
                $5::grant_resource_type[],
                $6::text[], $7::uuid[], $8::uuid[], $9::uuid[], $10::uuid[]
            ) AS t(
                principal_type, user_id, role_id, privilege, resource_type,
                project_id, warehouse_id, namespace_id, tabular_id, tag_definition_id
            )
            ON CONFLICT ON CONSTRAINT grant_unique DO NOTHING
            RETURNING
                grant_id, principal_type, user_id, role_id, privilege, resource_type,
                project_id, warehouse_id, namespace_id, tabular_id, tag_definition_id,
                created_at
        )
        SELECT
            i.grant_id AS "grant_id!",
            i.principal_type AS "principal_type!: PrincipalType", i.user_id,
            i.role_id, i.privilege AS "privilege!",
            i.resource_type AS "resource_type!: StoredResourceType",
            i.project_id, i.warehouse_id, i.namespace_id, i.tabular_id, i.tag_definition_id,
            tab.typ AS "tabular_typ?: TabularType",
            i.created_at AS "created_at!"
        FROM inserted i
        LEFT JOIN tabular tab
               ON i.warehouse_id = tab.warehouse_id AND i.tabular_id = tab.tabular_id
        "#,
        &columns.principal.principal_type as &[PrincipalType],
        &columns.principal.user_id as &[Option<String>],
        &columns.principal.role_id as &[Option<Uuid>],
        &columns.principal.privilege,
        &columns.resource_type as &[StoredResourceType],
        &columns.project_id as &[Option<String>],
        &columns.warehouse_id as &[Option<Uuid>],
        &columns.namespace_id as &[Option<Uuid>],
        &columns.tabular_id as &[Option<Uuid>],
        &columns.tag_definition_id as &[Option<Uuid>],
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(map_write_error)?;

    Ok(rows_into_specs(rows)?)
}

/// Delete `specs`, ignoring grants that do not exist. Returns the grants actually
/// removed.
///
/// One statement per distinct resource: binding the resource as scalars is what lets
/// its columns be compared with `=` and drive an index, which a join against the
/// unnested resource columns cannot do.
pub(crate) async fn delete_grants(
    specs: &[GrantSpec],
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<Vec<GrantSpec>, ApplyGrantsStoreError> {
    let mut removed = Vec::new();
    for (resource, group) in group_by_resource(specs) {
        removed.extend(delete_grants_on_resource(resource, group, transaction).await?);
    }
    Ok(removed)
}

/// Delete the grants in `specs`, all of which name `resource`.
///
/// Split into a user arm and a role arm rather than one statement matching both with
/// `IS NOT DISTINCT FROM`. That form cannot be a hash key, so the join degrades to a
/// filter over every grant on the resource: it compared half a million rows to delete a
/// hundred on a warehouse holding five thousand grants, and the cost grows with the
/// resource's grant count times the diff's size. Splitting lets each arm compare its one
/// populated principal column with `=` and hold the other `NULL` as a constant. The
/// planner then chooses by estimate: a hash join over the resource's grants, or — via
/// the redundant `= ANY` predicate each arm carries — index probes on
/// `grant_user_idx`/`grant_role_idx` per principal. Whichever side is smaller bounds
/// the cost; without the redundant predicate the principal probe is not available and
/// a large resource is scanned however few principals the diff names.
async fn delete_grants_on_resource(
    resource: &GrantResource,
    specs: Vec<&GrantSpec>,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<Vec<GrantSpec>, ApplyGrantsStoreError> {
    let mut user_ids: Vec<String> = Vec::new();
    let mut user_privileges: Vec<String> = Vec::new();
    let mut role_ids: Vec<Uuid> = Vec::new();
    let mut role_privileges: Vec<String> = Vec::new();
    for spec in specs {
        match &spec.principal {
            UserOrRoleId::User(user_id) => {
                user_ids.push(user_id.to_string());
                user_privileges.push(spec.privilege.clone());
            }
            UserOrRoleId::Role(role_id) => {
                role_ids.push(**role_id);
                role_privileges.push(spec.privilege.clone());
            }
        }
    }

    let mut removed = Vec::new();
    if !user_ids.is_empty() {
        removed.extend(
            delete_user_grants_on_resource(resource, &user_ids, &user_privileges, transaction)
                .await?,
        );
    }
    if !role_ids.is_empty() {
        removed.extend(
            delete_role_grants_on_resource(resource, &role_ids, &role_privileges, transaction)
                .await?,
        );
    }
    Ok(removed)
}

/// The user arm of [`delete_grants_on_resource`].
async fn delete_user_grants_on_resource(
    resource: &GrantResource,
    user_ids: &[String],
    privileges: &[String],
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<Vec<GrantSpec>, ApplyGrantsStoreError> {
    let resource_columns = ResourceColumns::of(resource);
    let rows = sqlx::query_as!(
        GrantAssignmentRow,
        r#"
        DELETE FROM grant_assignment ga
        USING UNNEST($1::text[], $2::text[]) AS t(user_id, privilege)
        WHERE ga.principal_type = 'user'::grant_principal_type
          -- Constant, so it joins `grant_unique`'s third column to the entry's own two.
          AND ga.role_id IS NULL
          AND ga.user_id = t.user_id
          -- Redundant with the join, deliberately: an array predicate on the base
          -- table is an index condition on grant_user_idx, where the join condition
          -- alone is not, so the planner can probe by principal instead of scanning
          -- the resource when the resource side is the large one.
          AND ga.user_id = ANY($1)
          AND ga.privilege = t.privilege
          AND ga.resource_type = $3::grant_resource_type
          -- Only the columns this resource kind populates; see the module docs.
          AND ($4::text IS NULL OR ga.project_id = $4)
          AND ($5::uuid IS NULL OR ga.warehouse_id = $5)
          AND ($6::uuid IS NULL OR ga.namespace_id = $6)
          AND ($7::uuid IS NULL OR ga.tabular_id = $7)
          AND ($8::uuid IS NULL OR ga.tag_definition_id = $8)
          -- Revoking on one kind of tabular must not remove another kind's grants, so
          -- the tabular has to really be of the kind named. Correlated on the
          -- parameters only, hence evaluated once rather than per row.
          AND ($9::tabular_type IS NULL
               OR EXISTS (SELECT 1 FROM tabular tab
                          WHERE tab.warehouse_id = $5 AND tab.tabular_id = $7
                            AND tab.typ = $9))
        RETURNING
            ga.grant_id, ga.principal_type AS "principal_type: PrincipalType", ga.user_id,
            ga.role_id, ga.privilege, ga.resource_type AS "resource_type: StoredResourceType",
            ga.project_id, ga.warehouse_id, ga.namespace_id, ga.tabular_id, ga.tag_definition_id,
            -- Not joined: the statement above already proved the caller's kind, which
            -- is echoed onto each returned grant.
            NULL::tabular_type AS "tabular_typ?: TabularType",
            ga.created_at
        "#,
        user_ids,
        privileges,
        StoredResourceType::of(resource) as StoredResourceType,
        resource_columns.project_id.as_deref(),
        resource_columns.warehouse_id,
        resource_columns.namespace_id,
        resource_columns.tabular_id,
        resource_columns.tag_definition_id,
        tabular_kind_of(resource) as Option<TabularType>,
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(map_write_error)?;

    rows.into_iter()
        .map(|row| row.into_spec_on(resource.clone()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(Into::into)
}

/// The role arm of [`delete_grants_on_resource`].
async fn delete_role_grants_on_resource(
    resource: &GrantResource,
    role_ids: &[Uuid],
    privileges: &[String],
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<Vec<GrantSpec>, ApplyGrantsStoreError> {
    let resource_columns = ResourceColumns::of(resource);
    let rows = sqlx::query_as!(
        GrantAssignmentRow,
        r#"
        DELETE FROM grant_assignment ga
        USING UNNEST($1::uuid[], $2::text[]) AS t(role_id, privilege)
        WHERE ga.principal_type = 'role'::grant_principal_type
          AND ga.user_id IS NULL
          AND ga.role_id = t.role_id
          -- Redundant with the join; see the user arm.
          AND ga.role_id = ANY($1::uuid[])
          AND ga.privilege = t.privilege
          AND ga.resource_type = $3::grant_resource_type
          AND ($4::text IS NULL OR ga.project_id = $4)
          AND ($5::uuid IS NULL OR ga.warehouse_id = $5)
          AND ($6::uuid IS NULL OR ga.namespace_id = $6)
          AND ($7::uuid IS NULL OR ga.tabular_id = $7)
          AND ($8::uuid IS NULL OR ga.tag_definition_id = $8)
          AND ($9::tabular_type IS NULL
               OR EXISTS (SELECT 1 FROM tabular tab
                          WHERE tab.warehouse_id = $5 AND tab.tabular_id = $7
                            AND tab.typ = $9))
        RETURNING
            ga.grant_id, ga.principal_type AS "principal_type: PrincipalType", ga.user_id,
            ga.role_id, ga.privilege, ga.resource_type AS "resource_type: StoredResourceType",
            ga.project_id, ga.warehouse_id, ga.namespace_id, ga.tabular_id, ga.tag_definition_id,
            NULL::tabular_type AS "tabular_typ?: TabularType",
            ga.created_at
        "#,
        role_ids,
        privileges,
        StoredResourceType::of(resource) as StoredResourceType,
        resource_columns.project_id.as_deref(),
        resource_columns.warehouse_id,
        resource_columns.namespace_id,
        resource_columns.tabular_id,
        resource_columns.tag_definition_id,
        tabular_kind_of(resource) as Option<TabularType>,
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(map_write_error)?;

    rows.into_iter()
        .map(|row| row.into_spec_on(resource.clone()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(Into::into)
}

/// Delete every grant held by `user_id`, returning what was removed.
///
/// Users are soft-deleted, so the `grant_user_fkey` cascade never fires for them.
/// A deleted user keeps their id and can return on re-login, so their grants must be
/// removed explicitly or the returning account would silently regain access.
pub(crate) async fn delete_grants_for_user(
    user_id: &UserId,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<Vec<GrantSpec>, ApplyGrantsStoreError> {
    let rows = sqlx::query_as!(
        GrantAssignmentRow,
        r#"
        WITH deleted AS (
            DELETE FROM grant_assignment
            WHERE principal_type = 'user'::grant_principal_type AND user_id = $1
            RETURNING
                grant_id, principal_type, user_id, role_id, privilege, resource_type,
                project_id, warehouse_id, namespace_id, tabular_id, tag_definition_id,
                created_at
        )
        SELECT
            d.grant_id AS "grant_id!",
            d.principal_type AS "principal_type!: PrincipalType", d.user_id,
            d.role_id, d.privilege AS "privilege!",
            d.resource_type AS "resource_type!: StoredResourceType",
            d.project_id, d.warehouse_id, d.namespace_id, d.tabular_id, d.tag_definition_id,
            tab.typ AS "tabular_typ?: TabularType",
            d.created_at AS "created_at!"
        FROM deleted d
        LEFT JOIN tabular tab
               ON d.warehouse_id = tab.warehouse_id AND d.tabular_id = tab.tabular_id
        "#,
        user_id.to_string(),
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(map_write_error)?;

    Ok(rows_into_specs(rows)?)
}

/// List direct grants matching `filter`, keyset-paginated on
/// `(created_at, grant_id)`.
///
/// Principal- and project-scoped listings join the resource tables to resolve each
/// grant's project, and skip grants on soft-deleted tabulars so a trashed table's
/// grants do not surface in a principal's access list. Server grants have no
/// project and are excluded from both.
pub(crate) async fn list_grants<'e, 'c: 'e, E>(
    filter: &GrantFilter,
    pagination: PaginationQuery,
    connection: E,
) -> Result<ListGrantsResultPage, ListGrantsStoreError>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let page = PageBounds::of(pagination)?;

    // One statement per shape rather than one gated by a parameter: a top-level `or`
    // between the two makes the planner produce a single plan that can index neither.
    // A resource-scoped listing also carries its resource through, so those rows need
    // not recover the tabular kind themselves.
    let (rows, on_resource) = match filter {
        GrantFilter::ByResource {
            resource,
            principal,
        } => {
            let (user, role) = split_principal(principal.as_ref());
            (
                select_grants_on_resource(resource, user.as_deref(), role, page, connection)
                    .await?,
                Some(resource),
            )
        }
        GrantFilter::ByPrincipal {
            principal,
            project_id,
        } => {
            let (user, role) = split_principal(Some(principal));
            (
                select_grants_in_project(user.as_deref(), role, project_id, page, connection)
                    .await?,
                None,
            )
        }
        GrantFilter::ByProject(project_id) => (
            select_grants_in_project(None, None, project_id, page, connection).await?,
            None,
        ),
    };

    let next_page_token = rows.last().map(|r| {
        PaginateToken::V1(V1PaginateToken::<Uuid> {
            created_at: r.created_at,
            id: r.grant_id,
        })
        .to_string()
    });

    let grants = rows
        .into_iter()
        .map(|row| match on_resource {
            Some(resource) => row.into_row_on(resource.clone()),
            None => row.into_row(),
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(ListGrantsResultPage {
        grants,
        next_page_token,
    })
}

/// A principal as the two nullable columns the listing statements bind: exactly one is
/// set, which is what makes `principal_type` redundant to test alongside them.
fn split_principal(principal: Option<&UserOrRoleId>) -> (Option<String>, Option<Uuid>) {
    match principal {
        Some(UserOrRoleId::User(user_id)) => (Some(user_id.to_string()), None),
        Some(UserOrRoleId::Role(role_id)) => (None, Some(**role_id)),
        None => (None, None),
    }
}

/// The decoded keyset position and page size every listing statement binds.
#[derive(Clone, Copy)]
struct PageBounds {
    created_at: Option<DateTime<Utc>>,
    id: Option<Uuid>,
    page_size: i64,
}

impl PageBounds {
    fn of(
        PaginationQuery {
            page_size,
            page_token,
        }: PaginationQuery,
    ) -> Result<Self, ListGrantsStoreError> {
        let page_size = CONFIG.page_size_or_pagination_default(page_size);
        let token: Option<PaginateToken<Uuid>> = page_token
            .as_option()
            .map(PaginateToken::try_from)
            .transpose()?;
        let (created_at, id) = match token {
            Some(PaginateToken::V1(V1PaginateToken { created_at, id })) => {
                (Some(created_at), Some(id))
            }
            None => (None, None),
        };
        Ok(Self {
            created_at,
            id,
            page_size,
        })
    }
}

/// Exactly the grants on one resource. The resource columns locate the rows outright,
/// so no per-row join is needed; the caller's resource is echoed onto each row, which
/// the statement first proves correct for tabulars.
///
/// A soft-deleted tabular is not filtered out here: naming one resource asks about that
/// resource, and its grants are intact for undrop to restore. The roll-ups over many
/// resources filter instead, so a trashed table stays out of an access list.
///
/// A principal narrows the rows further. Unlike the resource/project split above this is
/// a conjunct, so it needs no statement of its own: the resource columns still locate the
/// rows and the principal only shrinks the set they index.
async fn select_grants_on_resource<'e, 'c: 'e, E>(
    resource: &GrantResource,
    user_id: Option<&str>,
    role_id: Option<Uuid>,
    page: PageBounds,
    connection: E,
) -> Result<Vec<GrantAssignmentRow>, ListGrantsStoreError>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let columns = ResourceColumns::of(resource);
    sqlx::query_as!(
        GrantAssignmentRow,
        r#"
        SELECT
            ga.grant_id,
            ga.principal_type AS "principal_type: PrincipalType", ga.user_id, ga.role_id,
            ga.privilege, ga.resource_type AS "resource_type: StoredResourceType",
            ga.project_id, ga.warehouse_id, ga.namespace_id, ga.tabular_id, ga.tag_definition_id,
            -- Deliberately not joined per row: every returned grant is on the one
            -- resource asked for, so joining would probe the same tabular repeatedly.
            -- The kind is instead checked once, below.
            NULL::tabular_type AS "tabular_typ?: TabularType",
            ga.created_at
        FROM grant_assignment ga
        WHERE ga.resource_type = $1::grant_resource_type
          -- Only the columns this resource kind populates; see the module docs.
          AND ($2::text IS NULL OR ga.project_id = $2)
          AND ($3::uuid IS NULL OR ga.warehouse_id = $3)
          AND ($4::uuid IS NULL OR ga.namespace_id = $4)
          AND ($5::uuid IS NULL OR ga.tabular_id = $5)
          AND ($6::uuid IS NULL OR ga.tag_definition_id = $6)
          -- A tabular resource names a kind; the tabular must really be of that kind,
          -- or this is a listing of some other resource's grants. Correlated on the
          -- parameters only, so it is evaluated once per statement, not per row.
          AND ($7::tabular_type IS NULL
               OR EXISTS (SELECT 1 FROM tabular tab
                          WHERE tab.warehouse_id = $3 AND tab.tabular_id = $5
                            AND tab.typ = $7))
          -- Optional narrowing to one principal; both null lists every principal. The
          -- unused principal column is held null — as in the delete arms — so a
          -- grant_unique probe can descend past it instead of stopping at the gap;
          -- PG 18's skip scan recovers this on its own, PG 17's does not.
          AND ($8::text IS NULL
               OR (ga.principal_type = 'user'::grant_principal_type AND ga.user_id = $8
                   AND ga.role_id IS NULL))
          AND ($9::uuid IS NULL
               OR (ga.principal_type = 'role'::grant_principal_type AND ga.role_id = $9
                   AND ga.user_id IS NULL))
          -- Keyset. Written as an unconditional row comparison against a floor rather
          -- than as `$10 is null or …`: any null test on the parameters leaves the whole
          -- term unindexable, and it then plans as a filter that re-reads every row
          -- before the requested page.
          AND (ga.created_at, ga.grant_id)
              > (COALESCE($10, '-infinity'::timestamptz),
                 COALESCE($11, '00000000-0000-0000-0000-000000000000'::uuid))
        ORDER BY ga.created_at, ga.grant_id ASC
        LIMIT $12
        "#,
        StoredResourceType::of(resource) as StoredResourceType,
        columns.project_id.as_deref(),
        columns.warehouse_id,
        columns.namespace_id,
        columns.tabular_id,
        columns.tag_definition_id,
        tabular_kind_of(resource) as Option<TabularType>,
        user_id,
        role_id,
        page.created_at,
        page.id,
        page.page_size,
    )
    .fetch_all(connection)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)
    .map_err(Into::into)
}

/// Every grant in one project, optionally narrowed to a single principal.
///
/// Joins the resource tables to resolve each grant's project, and skips grants on
/// soft-deleted tabulars so a trashed table's grants do not surface in a principal's
/// access list; `select_grants_on_resource` deliberately does not. Server grants have
/// no project and are excluded.
///
/// Without a principal the project predicate lives on joined columns, so no index
/// narrows it and each page reads in proportion to every grant in the project. That
/// arm has no endpoint and exists for tests and a possible future export — do not put
/// it on a request path.
async fn select_grants_in_project<'e, 'c: 'e, E>(
    user_id: Option<&str>,
    role_id: Option<Uuid>,
    project_id: &ProjectId,
    page: PageBounds,
    connection: E,
) -> Result<Vec<GrantAssignmentRow>, ListGrantsStoreError>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    sqlx::query_as!(
        GrantAssignmentRow,
        r#"
        SELECT
            ga.grant_id,
            ga.principal_type AS "principal_type: PrincipalType", ga.user_id, ga.role_id,
            ga.privilege, ga.resource_type AS "resource_type: StoredResourceType",
            ga.project_id, ga.warehouse_id, ga.namespace_id, ga.tabular_id, ga.tag_definition_id,
            t.typ AS "tabular_typ?: TabularType",
            ga.created_at
        FROM grant_assignment ga
        LEFT JOIN warehouse w ON ga.warehouse_id = w.warehouse_id
        LEFT JOIN tag_definition td ON ga.tag_definition_id = td.tag_definition_id
        LEFT JOIN tabular t
               ON ga.warehouse_id = t.warehouse_id AND ga.tabular_id = t.tabular_id
        -- The unused principal column is held null; see select_grants_on_resource.
        WHERE ($1::text IS NULL
               OR (ga.principal_type = 'user'::grant_principal_type AND ga.user_id = $1
                   AND ga.role_id IS NULL))
          AND ($2::uuid IS NULL
               OR (ga.principal_type = 'role'::grant_principal_type AND ga.role_id = $2
                   AND ga.user_id IS NULL))
          AND (ga.tabular_id IS NULL OR t.deleted_at IS NULL)
          AND (
              w.project_id = $3
              OR (ga.resource_type = 'project'::grant_resource_type AND ga.project_id = $3)
              OR (ga.resource_type = 'tag'::grant_resource_type AND td.project_id = $3)
          )
          -- Keyset; see select_grants_on_resource for why it is written this way.
          AND (ga.created_at, ga.grant_id)
              > (COALESCE($4, '-infinity'::timestamptz),
                 COALESCE($5, '00000000-0000-0000-0000-000000000000'::uuid))
        ORDER BY ga.created_at, ga.grant_id ASC
        LIMIT $6
        "#,
        user_id,
        role_id,
        project_id.to_string(),
        page.created_at,
        page.id,
        page.page_size,
    )
    .fetch_all(connection)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)
    .map_err(Into::into)
}

/// Every grant held by any of `principals` on any of `resources`.
///
/// The evaluation-path fetch, narrowed on both axes: the principal arms and the
/// resource arms each probe their own indexes, and the planner combines the two
/// sides with a bitmap AND. Neither a coarse resource
/// holding one grant per principal in the deployment nor a principal holding one
/// grant per table can make the answer large — the result is bounded by chain size
/// times privileges per level, so it needs no cap and no warning.
///
/// `principals` must be the **effective** set — the acting principal plus every role
/// they hold, transitively — and `resources` the full resolved chain, including
/// [`GrantResource::Server`] if server grants should count. Resolving either is the
/// caller's job; this returns exactly what it is asked for, so an omitted role or
/// ancestor silently costs access.
///
/// Unpaginated and unordered — a set, so no `ORDER BY`: sorting a result the caller
/// will bucket anyway is pure cost.
///
/// No joins. Every row's resource is echoed from the matching entry of `resources`,
/// so tables, views and generic tables keep the kind the caller asked with instead of
/// re-reading `tabular` per call, and grants on soft-deleted tabulars are included,
/// matching the resource-scoped listing.
pub(crate) async fn list_grants_on_resources<'e, 'c: 'e, E>(
    principals: &[UserOrRoleId],
    resources: &[GrantResource],
    connection: E,
) -> Result<Vec<GrantSpec>, ListGrantsStoreError>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    if principals.is_empty() || resources.is_empty() {
        return Ok(Vec::new());
    }

    let mut user_ids: Vec<String> = Vec::new();
    let mut role_ids: Vec<Uuid> = Vec::new();
    for principal in principals {
        match principal {
            UserOrRoleId::User(user_id) => user_ids.push(user_id.to_string()),
            UserOrRoleId::Role(role_id) => role_ids.push(**role_id),
        }
    }

    // One arm per level, columns split per arm so each probes its own index. The SQL
    // matches the warehouse-scoped arrays as an over-approximating cross product —
    // the cheaper probe — and the lookup maps tighten it: they key on the full
    // (warehouse, id) pair, because `tabular`'s primary key is composite, so the same
    // tabular id can legitimately exist in two warehouses and a bare id would echo a
    // grant from one onto the other. A row whose pair was not requested is dropped.
    // The kind a tabular echoes is taken from the caller's entry on trust; naming one
    // tabular twice with different kinds is a caller bug this path cannot detect
    // without the join it exists to avoid (the write path checks it instead).
    let mut include_server = false;
    let mut project_ids: Vec<String> = Vec::new();
    let mut warehouse_ids: Vec<Uuid> = Vec::new();
    let mut namespace_warehouses: Vec<Uuid> = Vec::new();
    let mut namespace_ids: Vec<Uuid> = Vec::new();
    let mut tabular_warehouses: Vec<Uuid> = Vec::new();
    let mut tabular_ids: Vec<Uuid> = Vec::new();
    let mut tag_ids: Vec<Uuid> = Vec::new();
    let mut projects: HashMap<String, &GrantResource> = HashMap::new();
    let mut warehouses: HashMap<Uuid, &GrantResource> = HashMap::new();
    let mut namespaces: HashMap<(Uuid, Uuid), &GrantResource> = HashMap::new();
    let mut tabulars: HashMap<(Uuid, Uuid), &GrantResource> = HashMap::new();
    let mut tags: HashMap<Uuid, &GrantResource> = HashMap::new();
    for resource in resources {
        match resource {
            GrantResource::Server => include_server = true,
            GrantResource::Project(project_id) => {
                project_ids.push(project_id.to_string());
                projects.insert(project_id.to_string(), resource);
            }
            GrantResource::Warehouse(warehouse_id) => {
                warehouse_ids.push(**warehouse_id);
                warehouses.insert(**warehouse_id, resource);
            }
            GrantResource::Namespace {
                warehouse_id,
                namespace_id,
            } => {
                namespace_warehouses.push(**warehouse_id);
                namespace_ids.push(**namespace_id);
                namespaces.insert((**warehouse_id, **namespace_id), resource);
            }
            GrantResource::Table {
                warehouse_id,
                table_id,
            } => {
                tabular_warehouses.push(**warehouse_id);
                tabular_ids.push(**table_id);
                tabulars.insert((**warehouse_id, **table_id), resource);
            }
            GrantResource::View {
                warehouse_id,
                view_id,
            } => {
                tabular_warehouses.push(**warehouse_id);
                tabular_ids.push(**view_id);
                tabulars.insert((**warehouse_id, **view_id), resource);
            }
            GrantResource::GenericTable {
                warehouse_id,
                generic_table_id,
            } => {
                tabular_warehouses.push(**warehouse_id);
                tabular_ids.push(**generic_table_id);
                tabulars.insert((**warehouse_id, **generic_table_id), resource);
            }
            GrantResource::Tag(tag_definition_id) => {
                tag_ids.push(**tag_definition_id);
                tags.insert(**tag_definition_id, resource);
            }
        }
    }

    let rows = sqlx::query_as!(
        GrantAssignmentRow,
        r#"
        -- `!` on the columns declared NOT NULL: this statement has no ORDER BY, and
        -- without one sqlx's nullability inference reports every output as nullable.
        SELECT
            ga.grant_id AS "grant_id!",
            ga.principal_type AS "principal_type!: PrincipalType", ga.user_id, ga.role_id,
            ga.privilege AS "privilege!",
            ga.resource_type AS "resource_type!: StoredResourceType",
            ga.project_id, ga.warehouse_id, ga.namespace_id, ga.tabular_id, ga.tag_definition_id,
            -- Not joined: the caller's resource list already carries every kind.
            NULL::tabular_type AS "tabular_typ?: TabularType",
            ga.created_at AS "created_at!"
        FROM grant_assignment ga
        -- The unused principal column is held null; see select_grants_on_resource.
        WHERE ((ga.principal_type = 'user'::grant_principal_type AND ga.user_id = ANY($1)
                AND ga.role_id IS NULL)
               OR (ga.principal_type = 'role'::grant_principal_type AND ga.role_id = ANY($2)
                   AND ga.user_id IS NULL))
          AND (($3 AND ga.resource_type = 'server'::grant_resource_type)
               OR (ga.resource_type = 'project'::grant_resource_type
                   AND ga.project_id = ANY($4))
               OR (ga.resource_type = 'warehouse'::grant_resource_type
                   AND ga.warehouse_id = ANY($5))
               OR (ga.resource_type = 'namespace'::grant_resource_type
                   AND ga.warehouse_id = ANY($6) AND ga.namespace_id = ANY($7))
               OR (ga.resource_type = 'tabular'::grant_resource_type
                   AND ga.warehouse_id = ANY($8) AND ga.tabular_id = ANY($9))
               OR (ga.resource_type = 'tag'::grant_resource_type
                   AND ga.tag_definition_id = ANY($10)))
        "#,
        &user_ids,
        &role_ids,
        include_server,
        &project_ids,
        &warehouse_ids,
        &namespace_warehouses,
        &namespace_ids,
        &tabular_warehouses,
        &tabular_ids,
        &tag_ids,
    )
    .fetch_all(connection)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)
    .map_err(ListGrantsStoreError::from)?;

    rows.into_iter()
        .filter_map(|row| {
            let resource: Option<GrantResource> = match row.resource_type {
                StoredResourceType::Server => Some(GrantResource::Server),
                StoredResourceType::Project => row
                    .project_id
                    .as_deref()
                    .and_then(|id| projects.get(id))
                    .map(|r| (*r).clone()),
                StoredResourceType::Warehouse => row
                    .warehouse_id
                    .and_then(|id| warehouses.get(&id))
                    .map(|r| (*r).clone()),
                StoredResourceType::Namespace => row
                    .warehouse_id
                    .zip(row.namespace_id)
                    .and_then(|pair| namespaces.get(&pair))
                    .map(|r| (*r).clone()),
                StoredResourceType::Tabular => row
                    .warehouse_id
                    .zip(row.tabular_id)
                    .and_then(|pair| tabulars.get(&pair))
                    .map(|r| (*r).clone()),
                StoredResourceType::Tag => row
                    .tag_definition_id
                    .and_then(|id| tags.get(&id))
                    .map(|r| (*r).clone()),
            };
            // A miss is the SQL's cross-product over-approximation being tightened
            // to the requested (warehouse, id) pairs — drop the row.
            let resource = resource?;
            Some(
                row.into_spec_on(resource)
                    .map_err(ListGrantsStoreError::from),
            )
        })
        .collect()
}

/// Apply a grant diff in one transaction: deletes first, then inserts, so a diff
/// that both revokes and re-grants the same privilege ends in the granted state.
pub(crate) async fn apply_grants(
    writes: &[GrantSpec],
    deletes: &[GrantSpec],
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<AppliedGrants, ApplyGrantsStoreError> {
    lock_resources(writes, deletes, transaction).await?;
    let removed = delete_grants(deletes, transaction).await?;
    let created = insert_grants(writes, transaction).await?;
    Ok(AppliedGrants { created, removed })
}

#[cfg(test)]
mod tests {
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use lakekeeper::{
        api::iceberg::v1::PageToken,
        service::{CatalogCreateTagDefinitionRequest, RoleId, TagScope, TagValueSpec},
    };
    use sqlx::PgPool;

    use super::*;
    use crate::{
        CatalogState, tabular::table::tests::create_table_with_schema, tag::create_tag_definition,
        warehouse::test::initialize_warehouse,
    };

    /// `users` requires a NOT-NULL `last_updated_with`; `name` is nullable.
    async fn seed_user(pool: &PgPool, id: &str) -> String {
        sqlx::query(
            "INSERT INTO users (id, name, user_type, last_updated_with) \
             VALUES ($1, $1, 'human', 'create-endpoint') ON CONFLICT DO NOTHING",
        )
        .bind(id)
        .execute(pool)
        .await
        .expect("seed user");
        id.to_string()
    }

    async fn seed_project(pool: &PgPool) -> String {
        let project_id = format!("proj-{}", Uuid::now_v7());
        sqlx::query("INSERT INTO project (project_id, project_name) VALUES ($1, $1)")
            .bind(&project_id)
            .execute(pool)
            .await
            .expect("seed project");
        project_id
    }

    /// Seeds a project and a role in it, returning the role id.
    async fn seed_role(pool: &PgPool) -> Uuid {
        let project_id = seed_project(pool).await;
        let role_id = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO role (id, name, project_id, source_id) VALUES ($1, 'grant-test', $2, $1::text)",
        )
        .bind(role_id)
        .bind(&project_id)
        .execute(pool)
        .await
        .expect("seed role");
        role_id
    }

    /// Seeds a role inside an existing project, unlike [`seed_role`] which makes its own.
    async fn seed_role_in(pool: &PgPool, project_id: &ProjectId, name: &str) -> RoleId {
        let role_id = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO role (id, name, project_id, source_id) VALUES ($1, $2, $3, $1::text)",
        )
        .bind(role_id)
        .bind(name)
        .bind(project_id.to_string())
        .execute(pool)
        .await
        .expect("seed role in project");
        role_id.into()
    }

    fn role_spec(role_id: RoleId, resource: GrantResource, privilege: &str) -> GrantSpec {
        GrantSpec {
            principal: UserOrRoleId::Role(role_id),
            resource,
            privilege: privilege.to_string(),
        }
    }

    fn user_spec(user: &str, resource: GrantResource, privilege: &str) -> GrantSpec {
        GrantSpec {
            principal: UserOrRoleId::User(UserId::try_from(user).expect("valid user id")),
            resource,
            privilege: privilege.to_string(),
        }
    }

    fn simple_schema() -> Schema {
        Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .expect("valid schema")
    }

    fn no_pagination() -> PaginationQuery {
        PaginationQuery::new(lakekeeper::api::iceberg::v1::PageToken::Empty, None)
    }

    // ─── schema constraints ───────────────────────────────────────────────────

    #[sqlx::test]
    async fn accepts_server_grant(pool: PgPool) {
        let user = seed_user(&pool, "oidc~alice").await;
        sqlx::query(
            "INSERT INTO grant_assignment (principal_type, user_id, resource_type, privilege) \
             VALUES ('user', $1, 'server', 'admin')",
        )
        .bind(&user)
        .execute(&pool)
        .await
        .expect("server grant inserts");
    }

    #[sqlx::test]
    async fn rejects_namespace_grant_without_warehouse(pool: PgPool) {
        let user = seed_user(&pool, "oidc~alice").await;
        let err = sqlx::query(
            "INSERT INTO grant_assignment \
             (principal_type, user_id, resource_type, privilege, namespace_id) \
             VALUES ('user', $1, 'namespace', 'describe', gen_random_uuid())",
        )
        .bind(&user)
        .execute(&pool)
        .await
        .unwrap_err();
        assert_eq!(
            err.as_database_error().unwrap().constraint(),
            Some("grant_resource_target")
        );
    }

    #[sqlx::test]
    async fn rejects_two_principals_on_one_grant(pool: PgPool) {
        // Sets both principal columns.
        let user = seed_user(&pool, "oidc~alice").await;
        let role_id = seed_role(&pool).await;
        let err = sqlx::query(
            "INSERT INTO grant_assignment \
             (principal_type, user_id, role_id, resource_type, privilege) \
             VALUES ('user', $1, $2, 'server', 'admin')",
        )
        .bind(&user)
        .bind(role_id)
        .execute(&pool)
        .await
        .unwrap_err();
        assert_eq!(
            err.as_database_error().unwrap().constraint(),
            Some("grant_principal_shape")
        );
    }

    #[sqlx::test]
    async fn rejects_principal_type_mismatch(pool: PgPool) {
        let user = seed_user(&pool, "oidc~alice").await;
        let err = sqlx::query(
            "INSERT INTO grant_assignment (principal_type, user_id, resource_type, privilege) \
             VALUES ('role', $1, 'server', 'admin')",
        )
        .bind(&user)
        .execute(&pool)
        .await
        .unwrap_err();
        assert_eq!(
            err.as_database_error().unwrap().constraint(),
            Some("grant_principal_shape")
        );
    }

    #[sqlx::test]
    async fn unique_rejects_duplicate(pool: PgPool) {
        let user = seed_user(&pool, "oidc~alice").await;
        let insert = || {
            sqlx::query(
                "INSERT INTO grant_assignment (principal_type, user_id, resource_type, privilege) \
                 VALUES ('user', $1, 'server', 'admin')",
            )
            .bind(&user)
            .execute(&pool)
        };
        insert().await.expect("first insert");
        assert_eq!(
            insert()
                .await
                .unwrap_err()
                .as_database_error()
                .unwrap()
                .constraint(),
            Some("grant_unique")
        );
    }

    #[sqlx::test]
    async fn rejects_overlong_privilege(pool: PgPool) {
        let user = seed_user(&pool, "oidc~alice").await;
        let err = sqlx::query(
            "INSERT INTO grant_assignment (principal_type, user_id, resource_type, privilege) \
             VALUES ('user', $1, 'server', repeat('x', 257))",
        )
        .bind(&user)
        .execute(&pool)
        .await
        .unwrap_err();
        assert_eq!(
            err.as_database_error().unwrap().constraint(),
            Some("grant_privilege_length")
        );
    }

    #[sqlx::test]
    async fn rejects_unknown_resource_type(pool: PgPool) {
        // The enum type rejects the value before any constraint runs, so an unknown
        // resource kind can never reach a row. 22P02 is invalid_text_representation.
        let user = seed_user(&pool, "oidc~alice").await;
        let err = sqlx::query(
            "INSERT INTO grant_assignment (principal_type, user_id, resource_type, privilege) \
             VALUES ('user', $1, 'nope', 'admin')",
        )
        .bind(&user)
        .execute(&pool)
        .await
        .unwrap_err();
        assert_eq!(
            err.as_database_error().unwrap().code().as_deref(),
            Some("22P02"),
            "an unknown resource_type must be rejected by the enum type"
        );
    }

    #[sqlx::test]
    async fn rejects_unknown_principal_type(pool: PgPool) {
        let user = seed_user(&pool, "oidc~alice").await;
        let err = sqlx::query(
            "INSERT INTO grant_assignment (principal_type, user_id, resource_type, privilege) \
             VALUES ('group', $1, 'server', 'admin')",
        )
        .bind(&user)
        .execute(&pool)
        .await
        .unwrap_err();
        assert_eq!(
            err.as_database_error().unwrap().code().as_deref(),
            Some("22P02"),
            "an unknown principal_type must be rejected by the enum type"
        );
    }

    // ─── apply / list ─────────────────────────────────────────────────────────

    #[sqlx::test]
    async fn apply_reports_the_exact_delta_and_is_idempotent(pool: PgPool) {
        let user = seed_user(&pool, "oidc~alice").await;
        let spec = user_spec(&user, GrantResource::Server, "admin");

        let mut txn = pool.begin().await.unwrap();
        let applied = apply_grants(std::slice::from_ref(&spec), &[], &mut txn)
            .await
            .unwrap();
        txn.commit().await.unwrap();
        assert_eq!(applied.created, vec![spec.clone()]);
        assert_eq!(applied.removed, Vec::new());

        // Re-applying creates nothing.
        let mut txn = pool.begin().await.unwrap();
        let applied = apply_grants(std::slice::from_ref(&spec), &[], &mut txn)
            .await
            .unwrap();
        txn.commit().await.unwrap();
        assert_eq!(applied.created, Vec::new());

        // Revoking reports exactly what went away; revoking again reports nothing.
        let mut txn = pool.begin().await.unwrap();
        let applied = apply_grants(&[], std::slice::from_ref(&spec), &mut txn)
            .await
            .unwrap();
        assert_eq!(applied.removed, vec![spec.clone()]);
        let applied = apply_grants(&[], std::slice::from_ref(&spec), &mut txn)
            .await
            .unwrap();
        assert_eq!(applied.removed, Vec::new());
        txn.commit().await.unwrap();

        let grants = list_grants(
            &GrantFilter::on(GrantResource::Server, None),
            no_pagination(),
            &pool,
        )
        .await
        .unwrap()
        .grants;
        assert_eq!(grants, Vec::new());
    }

    /// A server grant is the one level that populates no resource column, so its
    /// round trip exercises a distinct read path from every other level.
    #[sqlx::test]
    async fn a_server_grant_round_trips_through_apply_and_list(pool: PgPool) {
        let user = seed_user(&pool, "oidc~alice").await;
        let spec = user_spec(&user, GrantResource::Server, "admin");

        let mut txn = pool.begin().await.unwrap();
        apply_grants(std::slice::from_ref(&spec), &[], &mut txn)
            .await
            .unwrap();
        txn.commit().await.unwrap();

        let grants = list_grants(
            &GrantFilter::on(GrantResource::Server, None),
            no_pagination(),
            &pool,
        )
        .await
        .unwrap()
        .grants;
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0].privilege, "admin");
        assert_eq!(grants[0].principal, spec.principal);
        assert_eq!(grants[0].resource, GrantResource::Server);
        assert!(grants[0].created_at.is_some());
    }

    #[sqlx::test]
    async fn a_missing_user_is_named(pool: PgPool) {
        let spec = user_spec("oidc~ghost", GrantResource::Server, "admin");
        let mut txn = pool.begin().await.unwrap();
        let err = insert_grants(std::slice::from_ref(&spec), &mut txn)
            .await
            .unwrap_err();
        let ApplyGrantsStoreError::GrantUserNotFound(err) = err else {
            panic!("expected a missing-user error, got {err:?}");
        };
        assert_eq!(err.to_string(), "User `oidc~ghost` does not exist");
    }

    /// A grant row records only that its resource is *a* tabular; which kind it is
    /// comes from the tabular itself. So a grant cannot disagree with the catalog about
    /// a tabular's kind — the disagreement is inexpressible rather than rejected. The
    /// same privilege on the same tabular is one row however it was addressed, and it
    /// always reads back as the kind the tabular actually is.
    #[sqlx::test]
    async fn a_grant_on_a_tabular_takes_the_tabulars_own_kind(pool: PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let user = seed_user(&pool, "oidc~alice").await;
        let (table_id, _) =
            create_table_with_schema(state.clone(), warehouse_id, simple_schema()).await;

        let as_table = GrantResource::Table {
            warehouse_id,
            table_id,
        };
        // The same tabular, addressed as though it were a view.
        let as_view = GrantResource::View {
            warehouse_id,
            view_id: ViewId::from(*table_id),
        };

        // Granting on it as though it were a view is refused, not quietly retargeted.
        let mut txn = pool.begin().await.unwrap();
        let err = insert_grants(
            std::slice::from_ref(&user_spec(&user, as_view.clone(), "select")),
            &mut txn,
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, ApplyGrantsStoreError::GrantTargetNotFound(_)),
            "expected a missing-target error, got {err:?}"
        );
        txn.rollback().await.unwrap();

        let mut txn = pool.begin().await.unwrap();
        let created = insert_grants(
            std::slice::from_ref(&user_spec(&user, as_table.clone(), "select")),
            &mut txn,
        )
        .await
        .expect("granting on it as a table is accepted");
        assert_eq!(created.len(), 1);
        assert_eq!(created[0].resource, as_table);
        txn.commit().await.unwrap();

        // Listed under the kind the tabular actually is.
        let page = list_grants(
            &GrantFilter::on(as_table.clone(), None),
            no_pagination(),
            &pool,
        )
        .await
        .unwrap();
        assert_eq!(page.grants.len(), 1);
        assert_eq!(page.grants[0].resource, as_table);

        // And not under any other kind: asking for a view's grants must not return a
        // table's, even when the id matches.
        let page = list_grants(
            &GrantFilter::on(as_view.clone(), None),
            no_pagination(),
            &pool,
        )
        .await
        .unwrap();
        assert_eq!(page.grants, Vec::new());

        // Nor may revoking a view's grants remove a table's.
        let mut txn = pool.begin().await.unwrap();
        let removed = delete_grants(&[user_spec(&user, as_view, "select")], &mut txn)
            .await
            .unwrap();
        assert_eq!(removed, Vec::new());
        txn.commit().await.unwrap();

        let page = list_grants(
            &GrantFilter::on(as_table.clone(), None),
            no_pagination(),
            &pool,
        )
        .await
        .unwrap();
        assert_eq!(page.grants.len(), 1);
    }

    /// Revokes are issued one statement per distinct resource. A diff spanning two
    /// resources must still remove exactly the grants it names.
    #[sqlx::test]
    async fn a_revoke_spanning_two_resources_removes_both(pool: PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let user = seed_user(&pool, "oidc~alice").await;
        let (table_id, _) =
            create_table_with_schema(state.clone(), warehouse_id, simple_schema()).await;

        let on_warehouse = user_spec(&user, GrantResource::Warehouse(warehouse_id), "modify");
        let on_table = user_spec(
            &user,
            GrantResource::Table {
                warehouse_id,
                table_id,
            },
            "select",
        );
        let on_server = user_spec(&user, GrantResource::Server, "admin");
        let all = vec![on_warehouse.clone(), on_table.clone(), on_server.clone()];

        let mut txn = pool.begin().await.unwrap();
        let created = insert_grants(&all, &mut txn).await.unwrap();
        assert_eq!(created.len(), 3);

        // Revoke two of the three in one call, across two different resources.
        let removed = delete_grants(&[on_warehouse.clone(), on_table.clone()], &mut txn)
            .await
            .unwrap();
        assert_eq!(removed.len(), 2);
        txn.commit().await.unwrap();

        let page = list_grants(
            &GrantFilter::on(GrantResource::Server, None),
            no_pagination(),
            &pool,
        )
        .await
        .unwrap();
        assert_eq!(page.grants.len(), 1);
        assert_eq!(page.grants[0].resource, GrantResource::Server);

        for resource in [
            GrantResource::Warehouse(warehouse_id),
            GrantResource::Table {
                warehouse_id,
                table_id,
            },
        ] {
            let page = list_grants(&GrantFilter::on(resource, None), no_pagination(), &pool)
                .await
                .unwrap();
            assert_eq!(page.grants, Vec::new());
        }
    }

    /// The bootstrap write shares its transaction with a resource create, so the bound it
    /// needs for its own foreign-key waits must not outlive it. The diff path sets the
    /// same transaction-local `lock_timeout` and deliberately keeps it — asserted here as
    /// the contrast.
    #[sqlx::test]
    async fn bootstrapping_grants_leaves_the_transaction_settings_alone(pool: PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let user = seed_user(&pool, "oidc~alice").await;
        let spec = user_spec(&user, GrantResource::Warehouse(warehouse_id), "ownership");

        let mut txn = pool.begin().await.unwrap();
        let created = insert_grants_bounded(std::slice::from_ref(&spec), &mut txn)
            .await
            .unwrap();
        assert_eq!(created, vec![spec.clone()]);
        let timeout: String = sqlx::query_scalar("SHOW lock_timeout")
            .fetch_one(&mut *txn)
            .await
            .unwrap();
        assert_eq!(timeout, "0");

        // Re-running the same bootstrap creates nothing: a replayed create, or a
        // re-registered table that kept its id, must not double-grant.
        let again = insert_grants_bounded(std::slice::from_ref(&spec), &mut txn)
            .await
            .unwrap();
        assert_eq!(again, Vec::new());

        apply_grants(std::slice::from_ref(&spec), &[], &mut txn)
            .await
            .unwrap();
        let timeout: String = sqlx::query_scalar("SHOW lock_timeout")
            .fetch_one(&mut *txn)
            .await
            .unwrap();
        assert_eq!(timeout, "3s");
        txn.commit().await.unwrap();

        let page = list_grants(
            &GrantFilter::on(GrantResource::Warehouse(warehouse_id), None),
            no_pagination(),
            &pool,
        )
        .await
        .unwrap();
        assert_eq!(
            page.grants
                .iter()
                .map(|row| row.privilege.as_str())
                .collect::<Vec<_>>(),
            vec!["ownership"]
        );
    }

    #[sqlx::test]
    async fn a_diff_that_revokes_and_regrants_ends_granted(pool: PgPool) {
        // Deletes run before inserts, so the same privilege appearing on both sides
        // resolves to granted rather than revoked.
        let user = seed_user(&pool, "oidc~alice").await;
        let spec = user_spec(&user, GrantResource::Server, "admin");

        let mut txn = pool.begin().await.unwrap();
        insert_grants(std::slice::from_ref(&spec), &mut txn)
            .await
            .unwrap();
        let applied = apply_grants(
            std::slice::from_ref(&spec),
            std::slice::from_ref(&spec),
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();
        assert_eq!(applied.removed, vec![spec.clone()]);
        assert_eq!(applied.created, vec![spec.clone()]);

        let grants = list_grants(
            &GrantFilter::on(GrantResource::Server, None),
            no_pagination(),
            &pool,
        )
        .await
        .unwrap()
        .grants;
        assert_eq!(grants.len(), 1);
    }

    /// A table and a view carrying the same id address the same rows, so they must
    /// take the same lock. Keys are also built from stable literals and ids only, so
    /// two replicas on different builds agree during a rolling deploy.
    #[test]
    fn the_lock_key_follows_the_stored_shape() {
        let warehouse_id = WarehouseId::from(Uuid::nil());
        let id = Uuid::from_u128(7);
        let as_table = resource_lock_key(&GrantResource::Table {
            warehouse_id,
            table_id: id.into(),
        });
        let as_view = resource_lock_key(&GrantResource::View {
            warehouse_id,
            view_id: id.into(),
        });
        assert_eq!(as_table, as_view);
        assert_eq!(
            as_table,
            "tabular||00000000-0000-0000-0000-000000000000||00000000-0000-0000-0000-000000000007|"
        );

        // A different resource kind on the same ids is a different lock.
        assert_eq!(
            resource_lock_key(&GrantResource::Namespace {
                warehouse_id,
                namespace_id: id.into(),
            }),
            "namespace||00000000-0000-0000-0000-000000000000|00000000-0000-0000-0000-000000000007||"
        );
        assert_eq!(resource_lock_key(&GrantResource::Server), "server|||||");
    }

    /// Applying a diff must hold the resource's advisory lock for the rest of the
    /// transaction: that is what serializes two crossing diffs, which would otherwise
    /// wait on each other's uncommitted rows until the deadlock detector killed one.
    /// Probed with `pg_try_advisory_xact_lock`, which reports contention instead of
    /// waiting for it.
    #[sqlx::test]
    async fn applying_a_diff_holds_the_resource_lock_until_commit(pool: PgPool) {
        let alice = seed_user(&pool, "oidc~alice").await;
        let spec = user_spec(&alice, GrantResource::Server, "admin");

        let taken = |lock_held_elsewhere: bool| {
            let pool = pool.clone();
            async move {
                let mut probe = pool.begin().await.unwrap();
                let free: bool = sqlx::query_scalar(
                    "SELECT pg_try_advisory_xact_lock(hashtextextended($1, $2))",
                )
                .bind(resource_lock_key(&GrantResource::Server))
                .bind(GRANT_APPLY_LOCK_SEED)
                .fetch_one(&mut *probe)
                .await
                .unwrap();
                probe.rollback().await.unwrap();
                assert_eq!(
                    free, !lock_held_elsewhere,
                    "lock availability did not match expectation"
                );
            }
        };

        taken(false).await;

        let mut txn = pool.begin().await.unwrap();
        apply_grants(std::slice::from_ref(&spec), &[], &mut txn)
            .await
            .unwrap();
        // Still open: the lock is transaction-scoped, so it must be held right now.
        taken(true).await;

        txn.commit().await.unwrap();
        // Released by commit — no manual unlock, no leak.
        taken(false).await;
    }

    /// A contended lock is the caller's cue to retry, not a backend failure.
    #[test]
    fn a_lock_timeout_is_a_retriable_conflict() {
        let model = iceberg_ext::catalog::rest::ErrorModel::from(GrantLockTimeout::new());
        assert_eq!(model.code, 409);
        assert_eq!(model.r#type, "GrantLockTimeout");
    }

    /// The SQLSTATEs must actually reach that mapping. Asserting the rendering alone
    /// would let a typo'd code ship as a 503 that reads as "backend unavailable", which
    /// tells a client to give up rather than retry.
    #[sqlx::test]
    async fn lock_contention_sqlstates_map_to_the_retriable_error(pool: PgPool) {
        // `lock_timeout` elapsing on the advisory lock. Provoked for real: hold the
        // lock in one transaction, then try to take it in another with a short bound.
        let key = resource_lock_key(&GrantResource::Server);
        let mut holder = pool.begin().await.unwrap();
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, $2))")
            .bind(&key)
            .bind(GRANT_APPLY_LOCK_SEED)
            .execute(&mut *holder)
            .await
            .unwrap();

        let mut blocked = pool.begin().await.unwrap();
        sqlx::query("SET LOCAL lock_timeout = '100ms'")
            .execute(&mut *blocked)
            .await
            .unwrap();
        let err = sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, $2))")
            .bind(&key)
            .bind(GRANT_APPLY_LOCK_SEED)
            .execute(&mut *blocked)
            .await
            .expect_err("must not acquire a held lock");
        assert_eq!(
            err.as_database_error().and_then(|db| db.code()).as_deref(),
            Some("55P03"),
            "expected lock_not_available"
        );
        assert!(matches!(
            map_write_error(err),
            ApplyGrantsStoreError::GrantLockTimeout(_)
        ));
        blocked.rollback().await.unwrap();
        holder.rollback().await.unwrap();
    }

    /// The evaluation-path fetch returns every grant the named principals hold on the
    /// requested chain — one resource per level, server included explicitly — and
    /// nothing else. Each level matches through its own OR-arm, so a decoy per level
    /// is what proves none of them leaks: a mis-bound parameter surfaces as a
    /// neighbour's grant rather than as an error. A second user and an ungranted role
    /// pin the principal narrowing; the decoy resources pin the chain narrowing.
    #[sqlx::test]
    async fn the_evaluation_fetch_returns_exactly_the_requested_chain(pool: PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (project_id, warehouse_id) =
            initialize_warehouse(state.clone(), None, None, None, true).await;
        let decoy_project_id = ProjectId::new_random();
        let (decoy_project, decoy_warehouse) =
            initialize_warehouse(state.clone(), None, Some(&decoy_project_id), None, true).await;
        let user = seed_user(&pool, "oidc~alice").await;
        let other_user = seed_user(&pool, "oidc~bob").await;

        let (table_id, _) =
            create_table_with_schema(state.clone(), warehouse_id, simple_schema()).await;
        let (decoy_table, _) =
            create_table_with_schema(state.clone(), decoy_warehouse, simple_schema()).await;
        let namespace_id: Uuid =
            sqlx::query_scalar("SELECT namespace_id FROM tabular WHERE tabular_id = $1")
                .bind(*table_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        let decoy_namespace: Uuid =
            sqlx::query_scalar("SELECT namespace_id FROM tabular WHERE tabular_id = $1")
                .bind(*decoy_table)
                .fetch_one(&pool)
                .await
                .unwrap();
        let tag = TagDefinitionId::new_random();
        let mut tag_txn = pool.begin().await.unwrap();
        create_tag_definition(
            &project_id,
            CatalogCreateTagDefinitionRequest::builder()
                .tag_definition_id(tag)
                .name("pii")
                .description(None)
                .scope(&[TagScope::Table])
                .value_spec(TagValueSpec::Marker)
                .build(),
            &mut tag_txn,
        )
        .await
        .expect("create tag definition");
        tag_txn.commit().await.unwrap();

        // Every level that can carry a grant inside this project, plus the server, which
        // belongs to no project but applies inside this one.
        let in_scope = vec![
            GrantResource::Server,
            GrantResource::Project((*project_id).clone()),
            GrantResource::Warehouse(warehouse_id),
            GrantResource::Namespace {
                warehouse_id,
                namespace_id: namespace_id.into(),
            },
            GrantResource::Table {
                warehouse_id,
                table_id,
            },
            GrantResource::Tag(tag),
        ];
        // The same shapes elsewhere: reachable only by an arm that ignores the
        // requested resource list.
        let out_of_scope = [
            GrantResource::Project((*decoy_project).clone()),
            GrantResource::Warehouse(decoy_warehouse),
            GrantResource::Namespace {
                warehouse_id: decoy_warehouse,
                namespace_id: decoy_namespace.into(),
            },
            GrantResource::Table {
                warehouse_id: decoy_warehouse,
                table_id: decoy_table,
            },
        ];

        let mut txn = pool.begin().await.unwrap();
        let specs: Vec<GrantSpec> = in_scope
            .iter()
            .chain(out_of_scope.iter())
            .map(|resource| user_spec(&user, resource.clone(), "select"))
            // Another principal holds the same grants; none may come back.
            .chain(
                in_scope
                    .iter()
                    .map(|resource| user_spec(&other_user, resource.clone(), "select")),
            )
            .collect();
        insert_grants(&specs, &mut txn).await.unwrap();
        txn.commit().await.unwrap();

        let principals = [UserOrRoleId::User(UserId::try_from(user.as_str()).unwrap())];
        let mut fetched = list_grants_on_resources(&principals, &in_scope, &pool)
            .await
            .unwrap()
            .into_iter()
            .map(|spec| spec.resource)
            .collect::<Vec<_>>();
        fetched.sort_by_key(|resource| format!("{resource:?}"));
        let mut expected = in_scope.clone();
        expected.sort_by_key(|resource| format!("{resource:?}"));
        assert_eq!(fetched, expected);

        // A role the user does not hold contributes nothing; no principals or no
        // resources ask about nothing.
        let unheld = [UserOrRoleId::Role(RoleId::new_random())];
        assert_eq!(
            list_grants_on_resources(&unheld, &in_scope, &pool)
                .await
                .unwrap(),
            Vec::new()
        );
        assert_eq!(
            list_grants_on_resources(&[], &in_scope, &pool)
                .await
                .unwrap(),
            Vec::new()
        );
        assert_eq!(
            list_grants_on_resources(&principals, &[], &pool)
                .await
                .unwrap(),
            Vec::new()
        );
    }

    /// The statement matches the warehouse-scoped arrays as a cross product, so a grant
    /// on `(warehouse B, table X)` also matches a request naming `(A, X)` and `(B, Y)`.
    /// `tabular`'s primary key is composite — the same id can exist in two warehouses —
    /// so the echo must key on the full pair and drop the cross-match; a bare-id echo
    /// would report the grant on a resource nobody holds it on.
    #[sqlx::test]
    async fn a_cross_warehouse_id_match_is_dropped_not_echoed(pool: PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_a) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let other_project = ProjectId::new_random();
        let (_, warehouse_b) =
            initialize_warehouse(state.clone(), None, Some(&other_project), None, true).await;
        let user = seed_user(&pool, "oidc~alice").await;
        let (table_x, _) =
            create_table_with_schema(state.clone(), warehouse_b, simple_schema()).await;

        let mut txn = pool.begin().await.unwrap();
        insert_grants(
            &[user_spec(
                &user,
                GrantResource::Table {
                    warehouse_id: warehouse_b,
                    table_id: table_x,
                },
                "select",
            )],
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        // Neither requested resource carries the grant: `(A, X)` shares only the id,
        // `(B, Y)` only the warehouse — yet together they cross-match `(B, X)`.
        let principals = [UserOrRoleId::User(UserId::try_from(user.as_str()).unwrap())];
        let resources = [
            GrantResource::Table {
                warehouse_id: warehouse_a,
                table_id: table_x,
            },
            GrantResource::Table {
                warehouse_id: warehouse_b,
                table_id: TableId::from(Uuid::now_v7()),
            },
        ];
        assert_eq!(
            list_grants_on_resources(&principals, &resources, &pool)
                .await
                .unwrap(),
            Vec::new()
        );
    }

    /// The namespace arm cross-matches the same way, and its echo map is separately
    /// keyed code. `namespace_id` is globally unique today, so the cross-request can
    /// only name a pair that does not exist — but the contract accepts arbitrary
    /// resource lists, and the map keying is the only thing enforcing the tightening.
    #[sqlx::test]
    async fn a_cross_warehouse_namespace_match_is_dropped_not_echoed(pool: PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_a) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let other_project = ProjectId::new_random();
        let (_, warehouse_b) =
            initialize_warehouse(state.clone(), None, Some(&other_project), None, true).await;
        let user = seed_user(&pool, "oidc~alice").await;
        let (table_in_b, _) =
            create_table_with_schema(state.clone(), warehouse_b, simple_schema()).await;
        let namespace_n: Uuid =
            sqlx::query_scalar("SELECT namespace_id FROM tabular WHERE tabular_id = $1")
                .bind(*table_in_b)
                .fetch_one(&pool)
                .await
                .unwrap();

        let mut txn = pool.begin().await.unwrap();
        insert_grants(
            &[user_spec(
                &user,
                GrantResource::Namespace {
                    warehouse_id: warehouse_b,
                    namespace_id: namespace_n.into(),
                },
                "select",
            )],
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        // `(A, N)` shares only the id with the granted `(B, N)`, `(B, M)` only the
        // warehouse — yet together they cross-match it.
        let principals = [UserOrRoleId::User(UserId::try_from(user.as_str()).unwrap())];
        let resources = [
            GrantResource::Namespace {
                warehouse_id: warehouse_a,
                namespace_id: namespace_n.into(),
            },
            GrantResource::Namespace {
                warehouse_id: warehouse_b,
                namespace_id: Uuid::now_v7().into(),
            },
        ];
        assert_eq!(
            list_grants_on_resources(&principals, &resources, &pool)
                .await
                .unwrap(),
            Vec::new()
        );
    }

    /// The evaluation fetch includes grants on soft-deleted tabulars — undrop needs its
    /// authorization intact. A hygiene `deleted_at` filter added here would break that
    /// silently, so the documented contract is pinned.
    #[sqlx::test]
    async fn the_evaluation_fetch_includes_grants_on_soft_deleted_tabulars(pool: PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let user = seed_user(&pool, "oidc~alice").await;
        let (table_id, _) =
            create_table_with_schema(state.clone(), warehouse_id, simple_schema()).await;

        let resource = GrantResource::Table {
            warehouse_id,
            table_id,
        };
        let spec = user_spec(&user, resource.clone(), "select");
        let mut txn = pool.begin().await.unwrap();
        insert_grants(std::slice::from_ref(&spec), &mut txn)
            .await
            .unwrap();
        txn.commit().await.unwrap();

        sqlx::query("UPDATE tabular SET deleted_at = now() WHERE tabular_id = $1")
            .bind(*table_id)
            .execute(&pool)
            .await
            .unwrap();

        let principals = [UserOrRoleId::User(UserId::try_from(user.as_str()).unwrap())];
        assert_eq!(
            list_grants_on_resources(&principals, &[resource], &pool)
                .await
                .unwrap(),
            vec![spec]
        );
    }

    /// A principal's own grants and their roles' grants arrive in one fetch: the caller
    /// passes the effective set and the two arms of the principal predicate — which read
    /// different columns — must both contribute.
    #[sqlx::test]
    async fn the_evaluation_fetch_unions_a_users_grants_with_their_roles(pool: PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (project_id, warehouse_id) =
            initialize_warehouse(state.clone(), None, None, None, true).await;
        let user = seed_user(&pool, "oidc~alice").await;
        let role_id = seed_role_in(&pool, &project_id, "analysts").await;
        let unheld_role = seed_role_in(&pool, &project_id, "auditors").await;

        let user_grant = GrantResource::Warehouse(warehouse_id);
        let role_grant = GrantResource::Project((*project_id).clone());
        let mut txn = pool.begin().await.unwrap();
        insert_grants(
            &[
                user_spec(&user, user_grant.clone(), "select"),
                role_spec(role_id, role_grant.clone(), "select"),
                // Held by a role outside the effective set.
                role_spec(unheld_role, GrantResource::Server, "select"),
            ],
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        let principals = [
            UserOrRoleId::User(UserId::try_from(user.as_str()).unwrap()),
            UserOrRoleId::Role(role_id),
        ];
        // Server is requested, so the exclusion of the unheld role's server grant is
        // attributable to the principal narrowing alone.
        let resources = [
            GrantResource::Server,
            user_grant.clone(),
            role_grant.clone(),
        ];
        let mut fetched = list_grants_on_resources(&principals, &resources, &pool)
            .await
            .unwrap()
            .into_iter()
            .map(|spec| spec.resource)
            .collect::<Vec<_>>();
        fetched.sort_by_key(|resource| format!("{resource:?}"));
        let mut expected = vec![user_grant, role_grant];
        expected.sort_by_key(|resource| format!("{resource:?}"));
        assert_eq!(fetched, expected);
    }

    /// A resource listing narrowed to one principal must return that principal's rows
    /// and nothing else — including for a role, whose id lives in a different column.
    /// The two narrowing parameters sit between the resource columns and the keyset in
    /// the statement, so a mis-numbered bind would show up here as rows from the wrong
    /// principal rather than as an error.
    #[sqlx::test]
    async fn resource_listing_narrows_to_one_principal(pool: PgPool) {
        let alice = seed_user(&pool, "oidc~alice").await;
        let bob = seed_user(&pool, "oidc~bob").await;
        let role_id = seed_role(&pool).await;

        let mut txn = pool.begin().await.unwrap();
        let specs = vec![
            user_spec(&alice, GrantResource::Server, "admin"),
            user_spec(&bob, GrantResource::Server, "operator"),
            GrantSpec {
                principal: UserOrRoleId::Role(role_id.into()),
                resource: GrantResource::Server,
                privilege: "operator".to_string(),
            },
        ];
        insert_grants(&specs, &mut txn).await.unwrap();
        txn.commit().await.unwrap();

        let listed = |principal: Option<UserOrRoleId>| {
            let pool = pool.clone();
            async move {
                let page = list_grants(
                    &GrantFilter::on(GrantResource::Server, principal),
                    no_pagination(),
                    &pool,
                )
                .await
                .unwrap();
                page.grants
                    .into_iter()
                    .map(|g| (g.principal, g.privilege))
                    .collect::<Vec<_>>()
            }
        };

        let alice_principal = UserOrRoleId::User(UserId::try_from(alice.as_str()).unwrap());
        assert_eq!(
            listed(Some(alice_principal.clone())).await,
            vec![(alice_principal, "admin".to_string())]
        );
        let role_principal = UserOrRoleId::Role(role_id.into());
        assert_eq!(
            listed(Some(role_principal.clone())).await,
            vec![(role_principal, "operator".to_string())]
        );
        assert_eq!(listed(None).await.len(), 3);
    }

    /// A principal-scoped listing must resolve each grant's project through the
    /// resource it names. Grants on warehouses, projects and tag definitions each
    /// take a different join, so a listing that only joined warehouses would
    /// silently drop the other two.
    #[sqlx::test]
    async fn principal_listing_covers_every_project_scoped_resource(pool: PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (project_id, warehouse_id) =
            initialize_warehouse(state.clone(), None, None, None, true).await;
        let user = seed_user(&pool, "oidc~alice").await;

        let tag_definition_id = TagDefinitionId::new_random();
        let mut txn = pool.begin().await.unwrap();
        create_tag_definition(
            &project_id,
            CatalogCreateTagDefinitionRequest::builder()
                .tag_definition_id(tag_definition_id)
                .name("pii")
                .description(None)
                .scope(&[TagScope::Table])
                .value_spec(TagValueSpec::Marker)
                .build(),
            &mut txn,
        )
        .await
        .expect("create tag definition");

        // A second principal, so the project-wide filter cannot pass by returning the
        // same set as the principal-scoped one — the only variant that distinguishes
        // them has no endpoint, so this test is its whole coverage.
        let other = seed_user(&pool, "oidc~bob").await;

        let specs = vec![
            user_spec(&user, GrantResource::Warehouse(warehouse_id), "select"),
            user_spec(
                &user,
                GrantResource::Project((*project_id).clone()),
                "create_warehouse",
            ),
            user_spec(&user, GrantResource::Tag(tag_definition_id), "apply"),
            // Server grants have no project, so they are out of scope by design.
            user_spec(&user, GrantResource::Server, "admin"),
            user_spec(&other, GrantResource::Warehouse(warehouse_id), "modify"),
        ];
        insert_grants(&specs, &mut txn).await.unwrap();
        txn.commit().await.unwrap();

        let principal = UserOrRoleId::User(UserId::try_from(user.as_str()).unwrap());
        let page = list_grants(
            &GrantFilter::ByPrincipal {
                principal: principal.clone(),
                project_id: (*project_id).clone(),
            },
            no_pagination(),
            &pool,
        )
        .await
        .unwrap();

        let mut listed: Vec<_> = page.grants.iter().map(|g| g.privilege.clone()).collect();
        listed.sort();
        assert_eq!(listed, vec!["apply", "create_warehouse", "select"]);

        // Every grant in the project, regardless of principal: alice's three plus
        // bob's one, and still no server grant.
        let page = list_grants(
            &GrantFilter::ByProject((*project_id).clone()),
            no_pagination(),
            &pool,
        )
        .await
        .unwrap();
        let mut listed: Vec<_> = page.grants.iter().map(|g| g.privilege.clone()).collect();
        listed.sort();
        assert_eq!(
            listed,
            vec!["apply", "create_warehouse", "modify", "select"]
        );
    }

    /// A soft-deleted table still has its row, so its grants survive for undrop.
    /// They must not surface in a principal's access list while the table is in the
    /// trash.
    #[sqlx::test]
    async fn principal_listing_hides_grants_on_soft_deleted_tables(pool: PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (project_id, warehouse_id) =
            initialize_warehouse(state.clone(), None, None, None, true).await;
        let user = seed_user(&pool, "oidc~alice").await;
        let (table_id, _) =
            create_table_with_schema(state.clone(), warehouse_id, simple_schema()).await;

        let spec = user_spec(
            &user,
            GrantResource::Table {
                warehouse_id,
                table_id,
            },
            "select",
        );
        let mut txn = pool.begin().await.unwrap();
        insert_grants(std::slice::from_ref(&spec), &mut txn)
            .await
            .unwrap();
        txn.commit().await.unwrap();

        let principal = UserOrRoleId::User(UserId::try_from(user.as_str()).unwrap());
        let filter = GrantFilter::ByPrincipal {
            principal,
            project_id: (*project_id).clone(),
        };
        let page = list_grants(&filter, no_pagination(), &pool).await.unwrap();
        assert_eq!(page.grants.len(), 1);

        // Soft-delete the table; the grant row stays but must drop out of the listing.
        sqlx::query("UPDATE tabular SET deleted_at = now() WHERE tabular_id = $1")
            .bind(*table_id)
            .execute(&pool)
            .await
            .expect("soft-delete table");

        let page = list_grants(&filter, no_pagination(), &pool).await.unwrap();
        assert_eq!(page.grants, Vec::new());

        // The resource-scoped listing still shows it: the grant is intact and undrop
        // must be able to restore a table with its grants.
        let page = list_grants(
            &GrantFilter::on(
                GrantResource::Table {
                    warehouse_id,
                    table_id,
                },
                None,
            ),
            no_pagination(),
            &pool,
        )
        .await
        .unwrap();
        assert_eq!(page.grants.len(), 1);
    }

    #[sqlx::test]
    async fn listing_paginates_on_a_stable_keyset(pool: PgPool) {
        let user = seed_user(&pool, "oidc~alice").await;
        let specs: Vec<_> = ["a", "b", "c"]
            .into_iter()
            .map(|privilege| user_spec(&user, GrantResource::Server, privilege))
            .collect();
        let mut txn = pool.begin().await.unwrap();
        insert_grants(&specs, &mut txn).await.unwrap();
        txn.commit().await.unwrap();

        let mut seen = Vec::new();
        let mut token = PageToken::Empty;
        for _ in 0..5 {
            let page = list_grants(
                &GrantFilter::on(GrantResource::Server, None),
                PaginationQuery::new(token, Some(2)),
                &pool,
            )
            .await
            .unwrap();
            seen.extend(page.grants.iter().map(|g| g.privilege.clone()));
            match page.next_page_token {
                Some(t) if page.grants.len() == 2 => token = PageToken::Present(t),
                _ => break,
            }
        }
        // Exactly the three rows, each once: a keyset that fails to advance would
        // repeat a page instead.
        seen.sort();
        assert_eq!(seen, vec!["a", "b", "c"]);
    }

    #[sqlx::test]
    async fn deleting_a_user_removes_their_grants(pool: PgPool) {
        let user = seed_user(&pool, "oidc~alice").await;
        let other = seed_user(&pool, "oidc~bob").await;
        let user_id = UserId::try_from(user.as_str()).unwrap();
        let specs = vec![
            user_spec(&user, GrantResource::Server, "admin"),
            user_spec(&other, GrantResource::Server, "admin"),
        ];

        let mut txn = pool.begin().await.unwrap();
        insert_grants(&specs, &mut txn).await.unwrap();
        let removed = delete_grants_for_user(&user_id, &mut txn).await.unwrap();
        txn.commit().await.unwrap();
        assert_eq!(removed, vec![specs[0].clone()]);

        let grants = list_grants(
            &GrantFilter::on(GrantResource::Server, None),
            no_pagination(),
            &pool,
        )
        .await
        .unwrap()
        .grants;
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0].principal, specs[1].principal);
    }
}
