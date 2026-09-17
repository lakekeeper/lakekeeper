//! Cross-type location-collision tests for generic tables and datasets.
//!
//! Iceberg tables, views, generic tables and datasets share one location space
//! per warehouse: no tabular may sit at another's location, above it, or below
//! it, whatever the two kinds are. Purging a tabular removes everything under its
//! location, and an imported dataset registers what it finds there, so an overlap
//! across kinds destroys or claims the other tabular's data just as it would
//! between two Iceberg tables.
use std::collections::HashMap;

use http::StatusCode;
use lakekeeper::{
    api::{
        Result,
        data::v1::{
            datasets::{CreateDatasetRequest, DatasetService as _},
            generic_tables::{CreateGenericTableRequest, GenericTableService as _},
        },
        iceberg::v1::{DataAccess, tables::TablesService as _, views::ViewService as _},
    },
    server::CatalogServer,
    service::GenericTableFormat,
};
use lakekeeper_integration_tests::{
    TestNamespace, create_table_request, create_view_request, random_request_metadata,
};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, Clone, Copy)]
enum Kind {
    Table,
    View,
    GenericTable,
    Dataset,
}

#[derive(Debug, Clone, Copy)]
enum Relation {
    Same,
    /// The second tabular sits at the parent directory of the first.
    Parent,
    /// The second tabular sits inside the first.
    Child,
}

async fn create_at(ns: &TestNamespace, kind: Kind, name: &str, location: &str) -> Result<()> {
    let ctx = ns.ctx.clone();
    let params = ns.params();
    match kind {
        Kind::Table => {
            let mut request = create_table_request(Some(name.to_string()), Some(false));
            request.location = Some(location.to_string());
            CatalogServer::create_table(
                params,
                request,
                DataAccess::not_specified(),
                ctx,
                random_request_metadata(),
            )
            .await
            .map(|_| ())
        }
        Kind::View => CatalogServer::create_view(
            params,
            create_view_request(Some(name), Some(location)),
            ctx,
            DataAccess::not_specified(),
            random_request_metadata(),
        )
        .await
        .map(|_| ()),
        Kind::GenericTable => CatalogServer::create_generic_table(
            params,
            CreateGenericTableRequest {
                name: name.to_string(),
                format: GenericTableFormat::Unknown("lance".to_string()),
                base_location: Some(location.to_string()),
                doc: None,
                properties: HashMap::default(),
                schema: None,
                statistics: None,
            },
            ctx,
            random_request_metadata(),
        )
        .await
        .map(|_| ()),
        // A location makes it an imported dataset: the one kind that names its own.
        Kind::Dataset => CatalogServer::create_dataset(
            params,
            CreateDatasetRequest {
                name: name.to_string(),
                location: Some(location.to_string()),
                constraints: None,
            },
            ctx,
            random_request_metadata(),
        )
        .await
        .map(|_| ()),
    }
}

/// Every pairing that involves a generic table, in both orders.
const GENERIC_TABLE_PAIRS: [(Kind, Kind); 5] = [
    (Kind::Table, Kind::GenericTable),
    (Kind::GenericTable, Kind::Table),
    (Kind::View, Kind::GenericTable),
    (Kind::GenericTable, Kind::View),
    (Kind::GenericTable, Kind::GenericTable),
];

/// Every pairing that involves a dataset, in both orders.
const DATASET_PAIRS: [(Kind, Kind); 7] = [
    (Kind::Table, Kind::Dataset),
    (Kind::Dataset, Kind::Table),
    (Kind::View, Kind::Dataset),
    (Kind::Dataset, Kind::View),
    (Kind::GenericTable, Kind::Dataset),
    (Kind::Dataset, Kind::GenericTable),
    (Kind::Dataset, Kind::Dataset),
];

/// Every case among `pairs` whose second tabular is not refused with
/// `LocationAlreadyTaken`. Collected, so a break shows every case it affects, not
/// only the first.
async fn uncaught_collisions(ns: &TestNamespace, pairs: &[(Kind, Kind)]) -> Vec<String> {
    let base_location = ns.base_location();
    let mut wrong = Vec::new();
    for &(first, second) in pairs {
        for relation in [Relation::Same, Relation::Parent, Relation::Child] {
            let dir = format!("{base_location}/{}", Uuid::now_v7());
            let first_location = format!("{dir}/outer");
            let second_location = match relation {
                Relation::Same => first_location.clone(),
                Relation::Parent => dir.clone(),
                Relation::Child => format!("{first_location}/inner"),
            };

            create_at(
                ns,
                first,
                &format!("first_{}", Uuid::now_v7().simple()),
                &first_location,
            )
            .await
            .unwrap_or_else(|e| panic!("creating the first {first:?} failed: {e:?}"));

            let case = format!("{second:?} at {relation:?} of {first:?}");
            match create_at(
                ns,
                second,
                &format!("second_{}", Uuid::now_v7().simple()),
                &second_location,
            )
            .await
            {
                Ok(()) => wrong.push(format!("{case}: created")),
                Err(e)
                    if e.error.code == StatusCode::CONFLICT
                        && e.error.r#type == "LocationAlreadyTaken" => {}
                Err(e) => wrong.push(format!(
                    "{case}: refused with {} {}, expected 409 LocationAlreadyTaken",
                    e.error.code, e.error.r#type
                )),
            }
        }
    }
    wrong
}

/// `<dir>/<first_leaf>` and `<dir>/<second_leaf>` share a string prefix and no
/// path segment.
async fn assert_shared_prefix_is_free(
    ns: &TestNamespace,
    pairs: &[(Kind, Kind)],
    (first_leaf, second_leaf): (&str, &str),
) {
    for &(first, second) in pairs {
        let dir = format!("{}/{}", ns.base_location(), Uuid::now_v7());
        create_at(
            ns,
            first,
            &format!("first_{}", Uuid::now_v7().simple()),
            &format!("{dir}/{first_leaf}"),
        )
        .await
        .unwrap_or_else(|e| panic!("creating the first {first:?} failed: {e:?}"));

        create_at(
            ns,
            second,
            &format!("second_{}", Uuid::now_v7().simple()),
            &format!("{dir}/{second_leaf}"),
        )
        .await
        .unwrap_or_else(|e| {
            panic!("{second:?} next to {first:?} at a shared string prefix was refused: {e:?}")
        });
    }
}

/// A second tabular at the first one's location, at its parent directory, or
/// inside it is refused with `LocationAlreadyTaken`, for every pairing with a
/// generic table.
#[sqlx::test]
async fn test_generic_table_location_collides_across_kinds(pool: PgPool) {
    let ns = TestNamespace::new(pool).await;
    let wrong = uncaught_collisions(&ns, &GENERIC_TABLE_PAIRS).await;
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// A location that only shares a string prefix with another tabular's -- no path
/// segment -- is free, across kinds.
#[sqlx::test]
async fn test_generic_table_sibling_with_shared_prefix_does_not_collide(pool: PgPool) {
    let ns = TestNamespace::new(pool).await;
    assert_shared_prefix_is_free(&ns, &GENERIC_TABLE_PAIRS, ("tbl", "tbl-sibling")).await;
}

/// A second tabular at the first one's location, at its parent directory, or
/// inside it is refused with `LocationAlreadyTaken`, for every pairing with a
/// dataset.
#[sqlx::test]
async fn test_dataset_location_collides_across_kinds(pool: PgPool) {
    let ns = TestNamespace::new(pool).await;
    let wrong = uncaught_collisions(&ns, &DATASET_PAIRS).await;
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// A location that only shares a string prefix with another tabular's -- no path
/// segment -- is free, across kinds.
#[sqlx::test]
async fn test_dataset_sibling_with_shared_prefix_does_not_collide(pool: PgPool) {
    let ns = TestNamespace::new(pool).await;
    assert_shared_prefix_is_free(&ns, &DATASET_PAIRS, ("data", "data2")).await;
}
