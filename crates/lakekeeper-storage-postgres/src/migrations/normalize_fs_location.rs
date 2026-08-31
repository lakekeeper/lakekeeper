use futures::{FutureExt, future::BoxFuture};
use iceberg_ext::catalog::rest::ErrorModel;
use sqlx::Postgres;
use uuid::Uuid;

use crate::migrations::MigrationHook;

pub(super) struct NormalizeFsLocationHook;

impl MigrationHook for NormalizeFsLocationHook {
    fn apply<'c>(
        &self,
        trx: &'c mut sqlx::Transaction<'_, Postgres>,
    ) -> BoxFuture<'c, anyhow::Result<()>> {
        normalize_fs_locations(trx).boxed()
    }

    fn name(&self) -> &'static str {
        "normalize_fs_location"
    }

    fn version() -> i64
    where
        Self: Sized,
    {
        20_260_830_000_000
    }
}

/// How many collisions to name before saying that more exist. One more than this
/// is fetched, so a report can tell the difference without counting the rest --
/// the queries below are unbounded joins over `tabular` otherwise, and a
/// migration is the wrong place to materialize a million rows to print five.
const MAX_REPORTED: usize = 5;

/// A location more than one tabular occupies, with those tabulars' ids.
type Collision = (Uuid, String, Vec<Uuid>);

/// Brings every stored `fs_location` to the one spelling the collision check
/// compares against, and validates the constraint that keeps it there.
///
/// The trimming lives here rather than in the accompanying `.sql` file because a
/// shipped migration's body should not change, and a later migration may need to
/// run this again. Being here also lets it look at the rows *before* it rewrites
/// them, which is what separates a collision this migration would create from one
/// that was already there.
///
/// Runs inside the transaction that spans the whole migration run, so returning an
/// error rolls every migration back and leaves the catalog on its previous version.
async fn normalize_fs_locations(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
) -> anyhow::Result<()> {
    refuse_collisions_the_trim_would_create(transaction).await?;
    report_existing_collisions(transaction).await?;

    let trimmed = trim_trailing_slashes(transaction).await?;
    tracing::info!("Normalized {trimmed} table or view location(s) that ended in a slash");

    report_nested_locations(transaction).await?;
    validate_no_trailing_slash(transaction).await?;
    Ok(())
}

/// Fails the migration if trimming would land two tabulars on one location.
///
/// Here the migration is the cause: `b/a` and `b/a/` are distinct locations today,
/// and collapsing them puts two tabulars in one place, which no later step can
/// undo and nothing in Lakekeeper can arbitrate. Failing rolls the run back and
/// leaves the catalog on its previous version, where both tabulars still resolve.
async fn refuse_collisions_the_trim_would_create(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
) -> anyhow::Result<()> {
    // `count(DISTINCT fs_location) > 1` is what makes this the trim's doing: the
    // group shares a trimmed location but not a stored one, so the rows only
    // collide once this hook has run. Groups that already collide are reported by
    // `report_existing_collisions` instead.
    let created: Vec<Collision> = fetch_collisions(
        transaction,
        r#"
        SELECT warehouse_id, rtrim(fs_location, '/'), array_agg(tabular_id)
        FROM tabular
        GROUP BY warehouse_id, rtrim(fs_location, '/')
        HAVING count(*) > 1 AND count(DISTINCT fs_location) > 1
        LIMIT $1
        "#,
    )
    .await?;
    if created.is_empty() {
        return Ok(());
    }

    let listed = summarize(&created, |(warehouse_id, location, ids)| {
        format!("{location} in warehouse {warehouse_id}, held by {ids:?}")
    });
    tracing::error!("Cannot normalize table locations: {listed}");
    Err(ErrorModel::failed_dependency(
        format!(
            "Removing the trailing slash from these table or view locations would put more than              one tabular on the same location, which Lakekeeper cannot resolve on its own. Leave              only one tabular on each, then migrate again. Note that both tabulars' files sit              under the same prefix, so purging either one deletes the other's data -- move or              deregister rather than purge. Affected: {listed}."
        ),
        "TrimWouldShareTabularLocations",
        None,
    )
    .into())
}

/// Reports locations that more than one tabular already occupies.
///
/// Not this migration's doing and not blocked by it: these tabulars resolve today
/// and go on resolving afterwards. A table's commit never consults the collision
/// check at all -- `ensure_location_available` runs on create and on a view
/// commit -- so for two tables nothing refuses anything, and failing the upgrade
/// over it would strand a catalog on a state the upgrade does not touch.
async fn report_existing_collisions(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
) -> anyhow::Result<()> {
    let shared: Vec<Collision> = fetch_collisions(
        transaction,
        r#"
        SELECT warehouse_id, fs_location, array_agg(tabular_id)
        FROM tabular
        GROUP BY warehouse_id, fs_location
        HAVING count(*) > 1
        LIMIT $1
        "#,
    )
    .await?;
    if shared.is_empty() {
        return Ok(());
    }

    let listed = summarize(&shared, |(warehouse_id, location, ids)| {
        format!("{location} in warehouse {warehouse_id}, held by {ids:?}")
    });
    tracing::warn!(
        "More than one tabular occupies the same location. Their files share a prefix, so purging          one deletes the other's data. A view commit against either is refused until only one          remains. {listed}"
    );
    Ok(())
}

/// Strips trailing slashes from `fs_location`, returning how many rows changed.
///
/// A trailing slash on a stored location hides the collision that finds it as an
/// ancestor: the equality half of the check compares against candidates that are
/// already trimmed, so a stored `X/` never equals one, and a tabular can be created
/// at `X/child`. (An exact duplicate at `X` is still caught -- `X/` falls inside the
/// byte range `["X/", "X0")`.) No writer produces such a row: the iceberg metadata
/// types trim a location on their way in, and the create paths normalize
/// explicitly. These are rows that predate that, including whatever the
/// 20250216105917 backfill copied out of the old `location` column verbatim.
async fn trim_trailing_slashes(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
) -> anyhow::Result<u64> {
    let result = sqlx::query(
        "UPDATE tabular SET fs_location = rtrim(fs_location, '/') WHERE fs_location LIKE '%/'",
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| {
        tracing::error!("Failed to normalize table locations: {e:?}");
        ErrorModel::internal(
            "Failed to normalize table locations",
            "FailedToNormalizeLocations",
            Some(Box::new(e)),
        )
    })?;
    Ok(result.rows_affected())
}

/// Warns about locations that contain another tabular's location.
///
/// Recoverable by moving one tabular, and both stay readable, so this reports
/// rather than blocks. Commits against either are refused while it stands -- the
/// contained tabular carries the containing one among its `$2` candidates, so the
/// equality half catches it from that side too.
async fn report_nested_locations(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
) -> anyhow::Result<()> {
    let nested = nested_locations(transaction).await?;
    if nested.is_empty() {
        return Ok(());
    }

    let listed = summarize(&nested, |(warehouse_id, outer, inner)| {
        format!("{outer} contains {inner} in warehouse {warehouse_id}")
    });
    tracing::warn!(
        "A table or view location contains another tabular's location. Commits against either are          refused until one is moved; both remain readable. {listed}"
    );
    Ok(())
}

/// Validates the constraint the accompanying migration attached `NOT VALID`.
///
/// Until this runs, the constraint governs new writes but says nothing about rows
/// that were already stored. Validating is what checks the trimming above: it fails
/// if any stored location still ends in a slash. Validating an already-valid
/// constraint does nothing, so this is re-runnable.
async fn validate_no_trailing_slash(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
) -> anyhow::Result<()> {
    sqlx::query("ALTER TABLE tabular VALIDATE CONSTRAINT tabular_fs_location_no_trailing_slash")
        .execute(&mut **transaction)
        .await
        .map_err(|e| {
            tracing::error!("Failed to validate normalized table locations: {e:?}");
            ErrorModel::internal(
                "Failed to validate normalized table locations",
                "FailedToValidateLocations",
                Some(Box::new(e)),
            )
        })?;
    Ok(())
}

/// Names up to [`MAX_REPORTED`] entries, saying only that more exist beyond them.
///
/// The queries fetch one row past the limit for exactly this, so a report never
/// needs the full result set to know it is incomplete.
fn summarize<T>(rows: &[T], describe: impl Fn(&T) -> String) -> String {
    let listed = rows
        .iter()
        .take(MAX_REPORTED)
        .map(&describe)
        .collect::<Vec<_>>()
        .join("; ");
    if rows.len() > MAX_REPORTED {
        format!("{listed}; and more beyond these {MAX_REPORTED}")
    } else {
        listed
    }
}

/// Runs one of the grouping queries above, which each take the row limit as `$1`.
async fn fetch_collisions(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    query: &'static str,
) -> anyhow::Result<Vec<Collision>> {
    sqlx::query_as(query)
        .bind(i64::try_from(MAX_REPORTED + 1).unwrap_or(i64::MAX))
        .fetch_all(&mut **transaction)
        .await
        .map_err(|e| {
            tracing::error!("Failed to check for shared table locations: {e:?}");
            ErrorModel::internal(
                "Failed to check for shared table locations",
                "FailedToCheckSharedLocations",
                Some(Box::new(e)),
            )
            .into()
        })
}

/// Pairs where the first tabular's location contains the second's, as
/// `(warehouse_id, containing, contained)`.
///
/// `~>=~` and `~<~` compare bytes, so a location is read literally -- `\`, `%`
/// and `_` in a path carry no meaning here -- and the index this migration creates
/// serves it. Unordered and limited on purpose: one tabular sitting at an ancestor
/// of its whole warehouse pairs with every row in it, and the report names five.
async fn nested_locations(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
) -> anyhow::Result<Vec<(Uuid, String, String)>> {
    sqlx::query_as(
        r#"
        SELECT o.warehouse_id, o.fs_location, i.fs_location
        FROM tabular o
        JOIN tabular i
          ON i.warehouse_id = o.warehouse_id
         AND i.tabular_id <> o.tabular_id
         AND i.fs_location ~>=~ (o.fs_location || '/')
         AND i.fs_location ~<~  (o.fs_location || '0')
        LIMIT $1
        "#,
    )
    .bind(i64::try_from(MAX_REPORTED + 1).unwrap_or(i64::MAX))
    .fetch_all(&mut **transaction)
    .await
    .map_err(|e| {
        tracing::error!("Failed to check for nested table locations: {e:?}");
        ErrorModel::internal(
            "Failed to check for nested table locations",
            "FailedToCheckNestedLocations",
            Some(Box::new(e)),
        )
        .into()
    })
}

#[cfg(test)]
mod tests {
    use std::str::FromStr as _;

    use lakekeeper_io::Location;
    use sqlx::PgPool;
    use uuid::Uuid;

    use super::{MAX_REPORTED, nested_locations, normalize_fs_locations, summarize};
    use crate::{
        CatalogState,
        namespace::tests::initialize_namespace,
        tabular::{CreateTabular, TabularType, create_tabular},
        warehouse::test::initialize_warehouse,
    };

    /// Creates `n` tabulars at unrelated locations `bkt/t0..bkt/t{n-1}`, returning
    /// the warehouse and their ids.
    ///
    /// All go through `create_tabular`, so none can start out colliding or carrying
    /// a trailing slash. The tests below move one afterwards with raw SQL, which is
    /// the only way to reach the states this hook exists for -- every writer trims,
    /// and the constraint refuses a trailing slash outright.
    async fn tabulars(pool: &PgPool, n: usize) -> (Uuid, Vec<Uuid>) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let ident = iceberg_ext::NamespaceIdent::from_vec(vec!["ns".to_string()]).unwrap();
        let namespace_id = *initialize_namespace(state.clone(), warehouse_id, &ident, None)
            .await
            .namespace_id();

        let mut ids = Vec::new();
        for i in 0..n {
            let id = Uuid::now_v7();
            let location = Location::from_str(&format!("s3://bkt/t{i}")).unwrap();
            let metadata_location =
                Location::from_str(&format!("s3://bkt/t{i}/metadata/v1.json")).unwrap();
            let name = format!("t{i}");
            let mut transaction = pool.begin().await.unwrap();
            create_tabular(
                CreateTabular {
                    id,
                    name: &name,
                    namespace_id,
                    warehouse_id: *warehouse_id,
                    typ: TabularType::Table,
                    metadata_location: Some(&metadata_location),
                    location: &location,
                },
                &mut transaction,
            )
            .await
            .unwrap();
            transaction.commit().await.unwrap();
            ids.push(id);
        }
        (*warehouse_id, ids)
    }

    /// Puts `location` in the column directly, around the constraint.
    ///
    /// Dropping and re-attaching `NOT VALID` is what the accompanying migration
    /// does, so what the hook meets afterwards is the state it meets on a real
    /// upgrade: rows that predate the trimming writers, under a constraint that
    /// governs new writes but has not been validated.
    async fn store_location_unchecked(pool: &PgPool, tabular_id: Uuid, location: &str) {
        sqlx::query("ALTER TABLE tabular DROP CONSTRAINT tabular_fs_location_no_trailing_slash")
            .execute(pool)
            .await
            .expect("the migration did not attach the constraint");
        sqlx::query("UPDATE tabular SET fs_location = $2 WHERE tabular_id = $1")
            .bind(tabular_id)
            .bind(location)
            .execute(pool)
            .await
            .unwrap();
        sqlx::query(
            "ALTER TABLE tabular ADD CONSTRAINT tabular_fs_location_no_trailing_slash \
             CHECK (fs_location NOT LIKE '%/') NOT VALID",
        )
        .execute(pool)
        .await
        .unwrap();
    }

    /// The hook trims a stored trailing slash and validates the constraint.
    #[sqlx::test]
    async fn the_hook_trims_a_stored_trailing_slash(pool: PgPool) {
        let (_, ids) = tabulars(&pool, 1).await;
        store_location_unchecked(&pool, ids[0], "bkt/t0/").await;

        let mut transaction = pool.begin().await.unwrap();
        normalize_fs_locations(&mut transaction).await.unwrap();

        let stored: String =
            sqlx::query_scalar("SELECT fs_location FROM tabular WHERE tabular_id = $1")
                .bind(ids[0])
                .fetch_one(&mut *transaction)
                .await
                .unwrap();
        assert_eq!(stored, "bkt/t0", "the trailing slash was not trimmed");

        let validated: bool =
            sqlx::query_scalar("SELECT convalidated FROM pg_constraint WHERE conname = $1")
                .bind("tabular_fs_location_no_trailing_slash")
                .fetch_one(&mut *transaction)
                .await
                .unwrap();
        assert!(validated, "the constraint was left unvalidated");
    }

    /// The constraint governs writes from the moment the migration attaches it,
    /// before the hook has validated it.
    #[sqlx::test]
    async fn the_attached_constraint_rejects_a_trailing_slash(pool: PgPool) {
        let (_, ids) = tabulars(&pool, 1).await;

        let err = sqlx::query("UPDATE tabular SET fs_location = 'bkt/t0/' WHERE tabular_id = $1")
            .bind(ids[0])
            .execute(&pool)
            .await
            .expect_err("a trailing slash was accepted into the column");
        assert!(
            err.to_string()
                .contains("tabular_fs_location_no_trailing_slash"),
            "{err}"
        );
    }

    /// A trim that would collapse two distinct locations onto one fails the
    /// migration, and nothing is written.
    ///
    /// This is the case the migration itself causes: `bkt/t0` and `bkt/t0/` resolve
    /// to two tabulars today.
    #[sqlx::test]
    async fn a_trim_that_would_share_a_location_fails_the_migration(pool: PgPool) {
        let (_, ids) = tabulars(&pool, 2).await;
        store_location_unchecked(&pool, ids[1], "bkt/t0/").await;

        let mut transaction = pool.begin().await.unwrap();
        let err = normalize_fs_locations(&mut transaction)
            .await
            .expect_err("trimming bkt/t0/ collides with bkt/t0");
        let message = format!("{err:?}");
        assert!(
            message.contains("bkt/t0") && message.contains(&ids[1].to_string()),
            "the error names neither the location nor the tabulars: {message}"
        );

        let untouched: String =
            sqlx::query_scalar("SELECT fs_location FROM tabular WHERE tabular_id = $1")
                .bind(ids[1])
                .fetch_one(&mut *transaction)
                .await
                .unwrap();
        assert_eq!(
            untouched, "bkt/t0/",
            "the trim ran before the check that was supposed to prevent it"
        );
    }

    /// Two tabulars that already share a location do not fail the migration.
    ///
    /// Not this migration's doing: they resolve before it and after it. A table's
    /// commit never consults the collision check, so failing the upgrade would
    /// strand a catalog over a state the upgrade does not touch.
    #[sqlx::test]
    async fn an_existing_shared_location_does_not_fail_the_migration(pool: PgPool) {
        let (_, ids) = tabulars(&pool, 2).await;
        sqlx::query("UPDATE tabular SET fs_location = 'bkt/t0' WHERE tabular_id = $1")
            .bind(ids[1])
            .execute(&pool)
            .await
            .unwrap();

        let mut transaction = pool.begin().await.unwrap();
        normalize_fs_locations(&mut transaction)
            .await
            .expect("an existing shared location must not block the upgrade");
    }

    /// One location containing another is found and reported, but does not fail the
    /// migration -- moving either tabular resolves it, and both stay readable.
    #[sqlx::test]
    async fn a_nested_location_is_reported_without_failing_the_migration(pool: PgPool) {
        let (warehouse_id, ids) = tabulars(&pool, 2).await;
        sqlx::query("UPDATE tabular SET fs_location = 'bkt/t0/inner' WHERE tabular_id = $1")
            .bind(ids[1])
            .execute(&pool)
            .await
            .unwrap();

        let mut transaction = pool.begin().await.unwrap();
        let nested = nested_locations(&mut transaction).await.unwrap();
        assert_eq!(
            nested,
            vec![(
                warehouse_id,
                "bkt/t0".to_string(),
                "bkt/t0/inner".to_string()
            )],
            "the containing/contained pair was not found"
        );

        normalize_fs_locations(&mut transaction)
            .await
            .expect("a nested location must not block the upgrade");
    }

    /// A `\` in a location is read literally, so an unrelated tabular at the
    /// escaped spelling is not reported as nested -- while a genuinely nested one
    /// still is.
    ///
    /// Both directions in one test: asserting only the empty case would be
    /// satisfied by a detector that finds nothing at all.
    #[sqlx::test]
    async fn a_backslash_is_not_read_as_an_escape(pool: PgPool) {
        let (warehouse_id, ids) = tabulars(&pool, 3).await;
        for (id, location) in [
            (ids[0], r"bkt/w\dir"),
            (ids[1], "bkt/wdir/unrelated"),
            (ids[2], r"bkt/w\dir/genuinely-nested"),
        ] {
            sqlx::query("UPDATE tabular SET fs_location = $2 WHERE tabular_id = $1")
                .bind(id)
                .bind(location)
                .execute(&pool)
                .await
                .unwrap();
        }

        let mut transaction = pool.begin().await.unwrap();
        let nested = nested_locations(&mut transaction).await.unwrap();
        assert_eq!(
            nested,
            vec![(
                warehouse_id,
                r"bkt/w\dir".to_string(),
                r"bkt/w\dir/genuinely-nested".to_string()
            )],
            r"expected only the genuine pair: `bkt/wdir/unrelated` sits under the spelling a \
              pattern reaches once `\` escapes the `d`, not under `bkt/w\dir`"
        );
    }

    /// Beyond `MAX_REPORTED` entries the report says so instead of listing them.
    ///
    /// The queries fetch one row past the limit for this, and it is the branch an
    /// operator reads when their upgrade fails, so it is worth pinning.
    #[test]
    fn a_report_longer_than_the_limit_says_more_exist() {
        let rows: Vec<usize> = (0..=MAX_REPORTED).collect();
        let listed = summarize(&rows, |n| format!("row{n}"));
        assert!(
            listed.contains("and more beyond these"),
            "an over-long report did not say more exist: {listed}"
        );
        assert!(
            !listed.contains(&format!("row{MAX_REPORTED}")),
            "an over-long report listed past the limit: {listed}"
        );

        let exact: Vec<usize> = (0..MAX_REPORTED).collect();
        let listed = summarize(&exact, |n| format!("row{n}"));
        assert!(
            !listed.contains("and more"),
            "a report at exactly the limit claimed more exist: {listed}"
        );
    }
}
