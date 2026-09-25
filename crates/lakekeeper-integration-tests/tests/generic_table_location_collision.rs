//! Cross-type location-collision tests for generic tables.
//!
//! Iceberg tables, views and generic tables share one location space per
//! warehouse: no tabular may sit at another's location, above it, or below it,
//! whatever the two kinds are. Purging a tabular removes everything under its
//! location, so an overlap across kinds destroys the other tabular's data just as
//! it would between two Iceberg tables.
use http::StatusCode;
use iceberg::NamespaceIdent;
use lakekeeper::{
    api::{
        ApiContext,
        data::v1::generic_tables::{CreateGenericTableRequest, GenericTableService as _},
        iceberg::{
            types::Prefix,
            v1::{
                DataAccess, namespace::NamespaceParameters, tables::TablesService as _,
                views::ViewService as _,
            },
        },
        management::v1::warehouse::TabularDeleteProfile,
    },
    server::CatalogServer,
    service::{GenericTableFormat, State, authz::AllowAllAuthorizer},
};
use lakekeeper_integration_tests::{
    create_ns, create_table_request, create_view_request, memory_io_profile,
    random_request_metadata, setup,
};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;
use uuid::Uuid;

type TestApiContext = ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>;

#[derive(Debug, Clone, Copy)]
enum Kind {
    Table,
    View,
    GenericTable,
}

#[derive(Debug, Clone, Copy)]
enum Relation {
    Same,
    /// The second tabular sits at the parent directory of the first.
    Parent,
    /// The second tabular sits inside the first.
    Child,
}

struct Warehouse {
    ctx: TestApiContext,
    params: NamespaceParameters,
    base_location: String,
}

async fn make_warehouse(pool: PgPool) -> Warehouse {
    let storage_profile = memory_io_profile();
    let base_location = storage_profile.base_location().unwrap().to_string();
    let (ctx, warehouse) = setup(
        pool,
        storage_profile,
        None,
        AllowAllAuthorizer::default(),
        TabularDeleteProfile::Hard {},
        None,
        1,
        None,
    )
    .await;
    let prefix = warehouse.warehouse_id.to_string();
    let ns_name = format!("ns_{}", Uuid::now_v7());
    create_ns(ctx.clone(), prefix.clone(), ns_name.clone()).await;
    Warehouse {
        ctx,
        params: NamespaceParameters {
            prefix: Some(Prefix(prefix)),
            namespace: NamespaceIdent::new(ns_name),
        },
        base_location: base_location.trim_end_matches('/').to_string(),
    }
}

async fn create_at(
    warehouse: &Warehouse,
    kind: Kind,
    name: &str,
    location: &str,
) -> lakekeeper::api::Result<()> {
    let ctx = warehouse.ctx.clone();
    let params = warehouse.params.clone();
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
                properties: std::collections::HashMap::default(),
                schema: None,
                statistics: None,
            },
            ctx,
            random_request_metadata(),
        )
        .await
        .map(|_| ()),
    }
}

/// Every pairing that involves a generic table, in both orders.
const PAIRS: [(Kind, Kind); 5] = [
    (Kind::Table, Kind::GenericTable),
    (Kind::GenericTable, Kind::Table),
    (Kind::View, Kind::GenericTable),
    (Kind::GenericTable, Kind::View),
    (Kind::GenericTable, Kind::GenericTable),
];

/// A second tabular at the first one's location, at its parent directory, or
/// inside it is refused with `LocationAlreadyTaken`, for every pairing with a
/// generic table.
#[sqlx::test]
async fn test_generic_table_location_collides_across_kinds(pool: PgPool) {
    let warehouse = make_warehouse(pool).await;

    // Collected rather than asserted in the loop, so a break shows every case it
    // affects instead of only the first.
    let mut wrong = Vec::new();
    for (first, second) in PAIRS {
        for relation in [Relation::Same, Relation::Parent, Relation::Child] {
            let dir = format!("{}/{}", warehouse.base_location, Uuid::now_v7());
            let first_location = format!("{dir}/outer");
            let second_location = match relation {
                Relation::Same => first_location.clone(),
                Relation::Parent => dir.clone(),
                Relation::Child => format!("{first_location}/inner"),
            };

            create_at(
                &warehouse,
                first,
                &format!("first_{}", Uuid::now_v7().simple()),
                &first_location,
            )
            .await
            .unwrap_or_else(|e| panic!("creating the first {first:?} failed: {e:?}"));

            let case = format!("{second:?} at {relation:?} of {first:?}");
            match create_at(
                &warehouse,
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
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// A location that only shares a string prefix with another tabular's -- no path
/// segment -- is free, across kinds.
#[sqlx::test]
async fn test_generic_table_sibling_with_shared_prefix_does_not_collide(pool: PgPool) {
    let warehouse = make_warehouse(pool).await;

    for (first, second) in PAIRS {
        let dir = format!("{}/{}", warehouse.base_location, Uuid::now_v7());
        create_at(
            &warehouse,
            first,
            &format!("first_{}", Uuid::now_v7().simple()),
            &format!("{dir}/tbl"),
        )
        .await
        .unwrap_or_else(|e| panic!("creating the first {first:?} failed: {e:?}"));

        create_at(
            &warehouse,
            second,
            &format!("second_{}", Uuid::now_v7().simple()),
            &format!("{dir}/tbl-sibling"),
        )
        .await
        .unwrap_or_else(|e| {
            panic!("{second:?} next to {first:?} at a shared string prefix was refused: {e:?}")
        });
    }
}
