//! Regression coverage for S3 remote signing of generic tables.
//!
//! Generic tables advertise remote signing in their load `config`
//! (`s3.remote-signing-enabled`, `signer.uri`, `signer.endpoint`), but the sign
//! path used to resolve the tabular at the requested location and then discard
//! everything that wasn't an Iceberg table, so requests under a generic table's
//! own `base-location` were rejected with `NoSuchTableLocationException`. These
//! tests exercise both the warehouse-scoped and table-scoped signer entrypoints
//! on an STS-disabled S3-compatible warehouse (as with Scaleway / MinIO without
//! STS in issue #1908).

use std::collections::HashMap;

use http::{Method, StatusCode};
use iceberg::{NamespaceIdent, TableIdent};
use iceberg_ext::catalog::rest::S3SignRequest;
use lakekeeper::{
    WarehouseId,
    api::{
        ApiContext,
        data::v1::generic_tables::{CreateGenericTableRequest, GenericTableService as _},
        iceberg::{
            types::Prefix,
            v1::{namespace::NamespaceParameters, s3_signer::Service as _},
        },
    },
    server::CatalogServer,
    service::{
        CatalogTabularOps as _, GenericTableFormat, SecretStore as _, State, TabularListFlags,
        authz::AllowAllAuthorizer,
        storage::{
            S3Credential, S3Flavor, S3Profile, StorageCredential, StorageProfile,
            s3::S3AccessKeyCredential,
        },
    },
};
use lakekeeper_integration_tests::{get_api_context, random_request_metadata};
use lakekeeper_storage_postgres::{
    PostgresBackend, SecretsState, namespace::tests::initialize_namespace,
    warehouse::test::initialize_warehouse,
};
use sqlx::PgPool;

type Ctx = ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>;

const ENDPOINT: &str = "http://localhost:9000";
const REGION: &str = "local";
const BUCKET: &str = "tests";

/// An S3-compatible profile with STS disabled but remote signing enabled — the
/// exact combination the issue reports (no credential vending available, so
/// signing is the only mechanism). Signing is offline sigv4 crypto, so no live
/// S3 is required.
fn s3_signing_profile_and_cred() -> (StorageProfile, StorageCredential) {
    let cred: StorageCredential = S3Credential::AccessKey(S3AccessKeyCredential {
        access_key_id: "minio-root-user".to_string(),
        secret_access_key: "minio-root-password".to_string(),
        external_id: None,
    })
    .into();

    let mut profile: StorageProfile = S3Profile::builder()
        .bucket(BUCKET.to_string())
        .region(REGION.to_string())
        .endpoint(ENDPOINT.parse().unwrap())
        .path_style_access(true)
        .sts_enabled(false)
        .flavor(S3Flavor::S3Compat)
        .build()
        .into();
    // `remote_signing_enabled` defaults to true in the builder.
    profile.normalize(Some(&cred)).unwrap();
    (profile, cred)
}

async fn setup(pool: PgPool) -> (Ctx, NamespaceIdent, WarehouseId) {
    lakekeeper_storage_postgres::migrations::migrate_core_only(&pool)
        .await
        .unwrap();
    let ctx = get_api_context(&pool, AllowAllAuthorizer::default()).await;
    let state = ctx.v1_state.catalog.clone();

    let (profile, cred) = s3_signing_profile_and_cred();
    let secret_id = ctx
        .v1_state
        .secrets
        .create_storage_secret(cred)
        .await
        .unwrap();

    let (_project_id, warehouse_id) =
        initialize_warehouse(state.clone(), Some(profile), None, Some(secret_id), true).await;

    let namespace = initialize_namespace(
        state,
        warehouse_id,
        &NamespaceIdent::new(uuid::Uuid::now_v7().to_string()),
        None,
    )
    .await
    .namespace_ident()
    .clone();

    (ctx, namespace, warehouse_id)
}

fn create_request(name: &str) -> CreateGenericTableRequest {
    CreateGenericTableRequest {
        name: name.to_string(),
        format: GenericTableFormat::Unknown("lance".to_string()),
        base_location: None,
        doc: None,
        properties: HashMap::default(),
        schema: None,
        statistics: None,
    }
}

/// Creates a generic table and returns its `base-location` (an `s3://…` URI).
async fn create_generic_table(
    ctx: &Ctx,
    namespace: &NamespaceIdent,
    warehouse_id: WarehouseId,
    name: &str,
) -> String {
    CatalogServer::create_generic_table(
        NamespaceParameters {
            prefix: Some(warehouse_id.to_string().into()),
            namespace: namespace.clone(),
        },
        create_request(name),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap()
    .table
    .base_location
}

/// Turns an `s3://bucket/key` base-location into the corresponding path-style
/// endpoint URL and appends `suffix`.
fn sign_url(base_location: &str, suffix: &str) -> url::Url {
    let http = base_location.replacen("s3://", &format!("{ENDPOINT}/"), 1);
    format!("{http}/{suffix}").parse().unwrap()
}

fn sign_request(method: Method, uri: url::Url) -> S3SignRequest {
    S3SignRequest::builder()
        .region(REGION.to_string())
        .uri(uri)
        .method(method)
        .headers(HashMap::from([(
            "x-amz-content-sha256".to_string(),
            vec!["UNSIGNED-PAYLOAD".to_string()],
        )]))
        .body(None)
        .build()
}

fn assert_signed(response: &iceberg_ext::catalog::rest::S3SignResponse) {
    assert!(
        response
            .headers
            .keys()
            .any(|k| k.eq_ignore_ascii_case("authorization")),
        "expected a signed request with an Authorization header, got headers: {:?}",
        response.headers,
    );
}

/// Warehouse-scoped signer (`/v1/aws/s3/sign`, no table id) — resolves purely by
/// location. This is the entrypoint the issue's `warehouse-scoped signer` hits.
#[sqlx::test]
async fn test_sign_generic_table_warehouse_scoped(pool: PgPool) {
    let (ctx, namespace, warehouse_id) = setup(pool).await;
    let base_location = create_generic_table(&ctx, &namespace, warehouse_id, "blobs").await;

    let response = CatalogServer::sign(
        Some(Prefix(warehouse_id.to_string())),
        None,
        sign_request(Method::PUT, sign_url(&base_location, "data/file.bin")),
        ctx,
        random_request_metadata(),
    )
    .await
    .expect("a PUT under the generic table's base-location must be signed");

    assert_signed(&response);
}

/// Table-scoped signer (`/signer/{prefix}/tabular-id/{id}/…`) with the generic
/// table's id. `get_table_info` can't resolve a generic-table id as a `TableId`,
/// so this exercises the location-based fallback — the `table-scoped signer`
/// path from the issue.
#[sqlx::test]
async fn test_sign_generic_table_table_scoped(pool: PgPool) {
    let (ctx, namespace, warehouse_id) = setup(pool).await;
    let base_location = create_generic_table(&ctx, &namespace, warehouse_id, "blobs").await;

    let gt_info = PostgresBackend::get_generic_table_info(
        warehouse_id,
        TableIdent::new(namespace.clone(), "blobs".to_string()),
        TabularListFlags::active(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .expect("generic table exists");

    let response = CatalogServer::sign(
        Some(Prefix(warehouse_id.to_string())),
        Some(gt_info.tabular_id.into_uuid()),
        sign_request(Method::GET, sign_url(&base_location, "data/file.bin")),
        ctx,
        random_request_metadata(),
    )
    .await
    .expect("table-scoped signer must fall back to location and sign");

    assert_signed(&response);
}

/// A URI that is not under any tabular's location must still be rejected.
#[sqlx::test]
async fn test_sign_generic_table_rejects_foreign_location(pool: PgPool) {
    let (ctx, namespace, warehouse_id) = setup(pool).await;
    let _base_location = create_generic_table(&ctx, &namespace, warehouse_id, "blobs").await;

    let foreign: url::Url = format!("{ENDPOINT}/{BUCKET}/not-a-table/file.bin")
        .parse()
        .unwrap();

    let err = CatalogServer::sign(
        Some(Prefix(warehouse_id.to_string())),
        None,
        sign_request(Method::PUT, foreign),
        ctx,
        random_request_metadata(),
    )
    .await
    .expect_err("signing a URI outside any table location must fail");

    assert_eq!(err.error.code, StatusCode::BAD_REQUEST, "{err:?}");
    assert_eq!(err.error.r#type, "NoSuchTableLocationException", "{err:?}");
}
