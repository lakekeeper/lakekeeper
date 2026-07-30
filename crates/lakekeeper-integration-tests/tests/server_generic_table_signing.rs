//! Coverage for S3 remote signing of generic tables.
//!
//! Generic tables advertise remote signing in their load `config`, but the signer
//! used to drop everything that wasn't an Iceberg table, rejecting requests under
//! a generic table's own `base-location` with `NoSuchTableLocationException`
//! (issue #1908). The warehouse here is STS-disabled and S3-compatible, as with
//! Scaleway / MinIO without STS, where signing is the only credential mechanism.

use std::collections::HashMap;

use http::{Method, StatusCode};
use iceberg::{NamespaceIdent, TableIdent, spec::ViewMetadata};
use iceberg_ext::catalog::rest::{S3SignRequest, S3SignResponse};
use lakekeeper::{
    WarehouseId,
    api::{
        ApiContext, IcebergErrorResponse,
        data::v1::generic_tables::{
            CreateGenericTableRequest, GenericTableParameters, GenericTableService as _,
        },
        iceberg::{
            types::Prefix,
            v1::{DataAccess, namespace::NamespaceParameters, s3_signer::Service as _},
        },
    },
    server::{CatalogServer, tables::create_table::create_table_request_into_table_metadata},
    service::{
        AllowedFormatVersions, CatalogNamespaceOps as _, CatalogStore, CatalogTableOps as _,
        CatalogTabularOps as _, CatalogViewOps as _, GenericTableFormat, NamespaceId,
        SecretStore as _, State, TableCreation, TableId, TabularListFlags, Transaction as _,
        authz::{
            AllowAllAuthorizer, Authorizer, CatalogGenericTableAction, CatalogTableAction,
            tests::HidingAuthorizer,
        },
        storage::{
            S3Credential, S3Flavor, S3Profile, StorageCredential, StorageProfile,
            s3::S3AccessKeyCredential,
        },
    },
};
use lakekeeper_integration_tests::{
    create_table_request, get_api_context, random_request_metadata,
};
use lakekeeper_io::Location;
use lakekeeper_storage_postgres::{
    PostgresBackend, SecretsState, namespace::tests::initialize_namespace,
    tabular::view::tests::view_request, warehouse::test::initialize_warehouse,
};
use sqlx::PgPool;

type Ctx<A> = ApiContext<State<A, PostgresBackend, SecretsState>>;

const ENDPOINT: &str = "http://localhost:9000";
const REGION: &str = "local";
const BUCKET: &str = "tests";
const GENERIC_TABLE: &str = "blobs";

/// An S3-compatible profile with STS disabled but remote signing enabled. Signing
/// is offline sigv4 crypto, so no live S3 is required.
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

/// Returns the context, the (single-level) namespace name and the warehouse id.
async fn setup<A: Authorizer>(pool: PgPool, authorizer: A) -> (Ctx<A>, String, WarehouseId) {
    lakekeeper_storage_postgres::migrations::migrate_core_only(&pool)
        .await
        .unwrap();
    let ctx = get_api_context(&pool, authorizer).await;
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

    let namespace_name = uuid::Uuid::now_v7().to_string();
    initialize_namespace(state, warehouse_id, &namespace(&namespace_name), None).await;

    (ctx, namespace_name, warehouse_id)
}

fn namespace(name: &str) -> NamespaceIdent {
    NamespaceIdent::new(name.to_string())
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
async fn create_generic_table<A: Authorizer>(
    ctx: &Ctx<A>,
    namespace_name: &str,
    warehouse_id: WarehouseId,
    name: &str,
) -> String {
    CatalogServer::create_generic_table(
        NamespaceParameters {
            prefix: Some(warehouse_id.to_string().into()),
            namespace: namespace(namespace_name),
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

async fn generic_table_id<A: Authorizer>(
    ctx: &Ctx<A>,
    namespace_name: &str,
    warehouse_id: WarehouseId,
    name: &str,
) -> uuid::Uuid {
    *PostgresBackend::get_generic_table_info(
        warehouse_id,
        TableIdent::new(namespace(namespace_name), name.to_string()),
        TabularListFlags::active(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .expect("generic table exists")
    .tabular_id
}

async fn resolve_namespace_id<A: Authorizer>(
    ctx: &Ctx<A>,
    warehouse_id: WarehouseId,
    namespace_name: &str,
) -> NamespaceId {
    PostgresBackend::get_namespace(
        warehouse_id,
        namespace(namespace_name),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .expect("namespace exists")
    .namespace_id()
}

/// Creates an Iceberg table, bypassing the REST handler so that no metadata has to
/// be written to S3. The location goes in the warehouse's bucket so that it can be
/// addressed by a signable URI.
async fn create_catalog_table<A: Authorizer>(
    ctx: &Ctx<A>,
    namespace_name: &str,
    warehouse_id: WarehouseId,
    name: &str,
) -> (TableId, Location) {
    let state = ctx.v1_state.catalog.clone();
    let namespace_id = resolve_namespace_id(ctx, warehouse_id, namespace_name).await;

    let location = format!("s3://{BUCKET}/{namespace_name}/{name}");
    let mut request = create_table_request(Some(name.to_string()), None);
    request.location = Some(location.clone());

    let table_id = TableId::from(uuid::Uuid::now_v7());
    let metadata = create_table_request_into_table_metadata(
        table_id,
        request,
        &AllowedFormatVersions::default(),
        None,
    )
    .unwrap();
    let metadata_location: Location = format!("{location}/metadata/00000.metadata.json")
        .parse()
        .unwrap();
    let table_ident = TableIdent::new(namespace(namespace_name), name.to_string());

    let mut transaction = <PostgresBackend as CatalogStore>::Transaction::begin_write(state)
        .await
        .unwrap();
    let (info, _staged) = PostgresBackend::create_table(
        TableCreation {
            warehouse_id,
            namespace_id,
            table_ident: &table_ident,
            metadata_location: Some(&metadata_location),
            table_metadata: &metadata,
        },
        transaction.transaction(),
    )
    .await
    .unwrap();
    transaction.commit().await.unwrap();

    (info.tabular_id, info.location)
}

/// Creates a view, bypassing the REST handler for the same reason as
/// [`create_catalog_table`].
async fn create_catalog_view<A: Authorizer>(
    ctx: &Ctx<A>,
    namespace_name: &str,
    warehouse_id: WarehouseId,
    name: &str,
) -> ViewMetadata {
    let state = ctx.v1_state.catalog.clone();
    let namespace_id = resolve_namespace_id(ctx, warehouse_id, namespace_name).await;

    let location: Location = format!("s3://{BUCKET}/{namespace_name}/{name}")
        .parse()
        .unwrap();
    let metadata_location: Location = format!("{location}/metadata/00000.metadata.json")
        .parse()
        .unwrap();
    let metadata = view_request(None, &location);

    let mut transaction = <PostgresBackend as CatalogStore>::Transaction::begin_write(state)
        .await
        .unwrap();
    PostgresBackend::create_view(
        warehouse_id,
        namespace_id,
        &TableIdent::new(namespace(namespace_name), name.to_string()),
        &metadata,
        &metadata_location,
        transaction.transaction(),
    )
    .await
    .unwrap();
    transaction.commit().await.unwrap();

    metadata
}

/// Turns an `s3://bucket/key` base-location into the corresponding path-style
/// endpoint URL and appends `suffix`.
fn sign_url(base_location: &str, suffix: &str) -> url::Url {
    let http = base_location.replacen("s3://", &format!("{ENDPOINT}/"), 1);
    format!("{http}/{suffix}").parse().unwrap()
}

fn sign_request(method: Method, uri: url::Url) -> S3SignRequest {
    sign_request_with_body(method, uri, None)
}

fn sign_request_with_body(method: Method, uri: url::Url, body: Option<String>) -> S3SignRequest {
    S3SignRequest::builder()
        .region(REGION.to_string())
        .uri(uri)
        .method(method)
        .headers(HashMap::from([(
            "x-amz-content-sha256".to_string(),
            vec!["UNSIGNED-PAYLOAD".to_string()],
        )]))
        .body(body)
        .build()
}

/// The S3 `DeleteObjects` request engines use for bulk deletes: `POST /{bucket}?delete`
/// with the keys in an XML body. The signer rebuilds one location per key, so this is
/// the only path where a single request carries more than one location.
fn batch_delete_request(keys: &[String]) -> S3SignRequest {
    let objects = keys
        .iter()
        .map(|key| format!("<Object><Key>{key}</Key></Object>"))
        .collect::<String>();

    sign_request_with_body(
        Method::POST,
        format!("{ENDPOINT}/{BUCKET}?delete").parse().unwrap(),
        Some(format!("<Delete>{objects}</Delete>")),
    )
}

/// The bucket-relative S3 key of `suffix` under `base_location`.
fn key_under(base_location: &str, suffix: &str) -> String {
    let prefix = format!("s3://{BUCKET}/");
    format!(
        "{}/{suffix}",
        base_location
            .strip_prefix(&prefix)
            .expect("base location is in the warehouse bucket")
    )
}

/// Signs `method` against `data/file.bin` under `base_location` through the
/// warehouse-scoped signer (`/v1/aws/s3/sign`, no tabular id).
async fn sign_warehouse_scoped<A: Authorizer>(
    ctx: &Ctx<A>,
    warehouse_id: WarehouseId,
    base_location: &str,
    method: Method,
) -> lakekeeper::api::Result<S3SignResponse> {
    sign_warehouse_scoped_request(
        ctx,
        warehouse_id,
        sign_request(method, sign_url(base_location, "data/file.bin")),
    )
    .await
}

async fn sign_warehouse_scoped_request<A: Authorizer>(
    ctx: &Ctx<A>,
    warehouse_id: WarehouseId,
    request: S3SignRequest,
) -> lakekeeper::api::Result<S3SignResponse> {
    CatalogServer::sign(
        Some(Prefix(warehouse_id.to_string())),
        None,
        request,
        ctx.clone(),
        random_request_metadata(),
    )
    .await
}

/// Signs `method` against `data/file.bin` under `base_location` through the
/// table-scoped signer (`/signer/{prefix}/tabular-id/{id}/…`).
async fn sign_table_scoped<A: Authorizer>(
    ctx: &Ctx<A>,
    warehouse_id: WarehouseId,
    tabular_id: uuid::Uuid,
    base_location: &str,
    method: Method,
) -> lakekeeper::api::Result<S3SignResponse> {
    CatalogServer::sign(
        Some(Prefix(warehouse_id.to_string())),
        Some(tabular_id),
        sign_request(method, sign_url(base_location, "data/file.bin")),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
}

#[track_caller]
fn assert_signed(response: &S3SignResponse) {
    assert!(
        response
            .headers
            .keys()
            .any(|k| k.eq_ignore_ascii_case("authorization")),
        "expected a signed request with an Authorization header, got headers: {:?}",
        response.headers,
    );
}

#[track_caller]
fn assert_forbidden(err: &IcebergErrorResponse) {
    assert_eq!(err.error.code, StatusCode::FORBIDDEN, "{err:?}");
    assert_eq!(err.error.r#type, "GenericTableActionForbidden", "{err:?}");
}

/// Warehouse-scoped signer for every method the signer maps to an operation: `GET` and
/// `HEAD` (read), `PUT` (write) and `DELETE`.
#[sqlx::test]
async fn test_sign_generic_table_warehouse_scoped(pool: PgPool) {
    let (ctx, namespace_name, warehouse_id) = setup(pool, AllowAllAuthorizer::default()).await;
    let base_location =
        create_generic_table(&ctx, &namespace_name, warehouse_id, GENERIC_TABLE).await;

    for method in [Method::GET, Method::HEAD, Method::PUT, Method::DELETE] {
        let Ok(response) =
            sign_warehouse_scoped(&ctx, warehouse_id, &base_location, method.clone()).await
        else {
            panic!("a {method} under the generic table's base-location must be signed");
        };

        assert_signed(&response);
    }
}

/// Table-scoped signer with the generic table's id, which resolves only because the
/// signer probes all tabular types rather than assuming an Iceberg table. The
/// per-method behaviour is shared with the warehouse-scoped route and covered above.
#[sqlx::test]
async fn test_sign_generic_table_table_scoped(pool: PgPool) {
    let (ctx, namespace_name, warehouse_id) = setup(pool, AllowAllAuthorizer::default()).await;
    let base_location =
        create_generic_table(&ctx, &namespace_name, warehouse_id, GENERIC_TABLE).await;
    let tabular_id = generic_table_id(&ctx, &namespace_name, warehouse_id, GENERIC_TABLE).await;

    let response = sign_table_scoped(&ctx, warehouse_id, tabular_id, &base_location, Method::GET)
        .await
        .expect("table-scoped signer must resolve the generic table by id");

    assert_signed(&response);
}

/// PyIceberg <= 0.9.1 reuses the signer URI of the first sign call for every
/// subsequent call, so the id in the path may belong to a different tabular than the
/// requested location. The location fallback must still resolve the URI's owner.
#[sqlx::test]
async fn test_sign_generic_table_falls_back_to_location_on_foreign_id(pool: PgPool) {
    let (ctx, namespace_name, warehouse_id) = setup(pool, AllowAllAuthorizer::default()).await;
    let base_location =
        create_generic_table(&ctx, &namespace_name, warehouse_id, GENERIC_TABLE).await;
    create_generic_table(&ctx, &namespace_name, warehouse_id, "other-blobs").await;
    let other_id = generic_table_id(&ctx, &namespace_name, warehouse_id, "other-blobs").await;

    let response = sign_table_scoped(&ctx, warehouse_id, other_id, &base_location, Method::GET)
        .await
        .expect("signer must fall back to location lookup when the path id does not match the URI");

    assert_signed(&response);
}

/// Without `write_data` on the generic table, mutating operations are rejected
/// while reads keep working.
#[sqlx::test]
async fn test_sign_generic_table_requires_write_data(pool: PgPool) {
    let authz = HidingAuthorizer::new();
    let (ctx, namespace_name, warehouse_id) = setup(pool, authz.clone()).await;
    let base_location =
        create_generic_table(&ctx, &namespace_name, warehouse_id, GENERIC_TABLE).await;

    authz
        .block_action(format!("generic_table:{:?}", CatalogGenericTableAction::WriteData).as_str());

    for method in [Method::PUT, Method::DELETE] {
        let Err(err) =
            sign_warehouse_scoped(&ctx, warehouse_id, &base_location, method.clone()).await
        else {
            panic!("{method} must be rejected without write_data");
        };
        assert_forbidden(&err);
    }

    // Reads are unaffected.
    let response = sign_warehouse_scoped(&ctx, warehouse_id, &base_location, Method::GET)
        .await
        .expect("read_data is still granted");
    assert_signed(&response);
}

#[sqlx::test]
async fn test_sign_generic_table_table_scoped_requires_write_data(pool: PgPool) {
    let authz = HidingAuthorizer::new();
    let (ctx, namespace_name, warehouse_id) = setup(pool, authz.clone()).await;
    let base_location =
        create_generic_table(&ctx, &namespace_name, warehouse_id, GENERIC_TABLE).await;
    let tabular_id = generic_table_id(&ctx, &namespace_name, warehouse_id, GENERIC_TABLE).await;

    authz
        .block_action(format!("generic_table:{:?}", CatalogGenericTableAction::WriteData).as_str());

    let err = sign_table_scoped(&ctx, warehouse_id, tabular_id, &base_location, Method::PUT)
        .await
        .expect_err("table-scoped PUT must be rejected without write_data");
    assert_forbidden(&err);

    // A read through the same by-id route still works.
    let response = sign_table_scoped(&ctx, warehouse_id, tabular_id, &base_location, Method::GET)
        .await
        .expect("read_data is still granted");
    assert_signed(&response);
}

/// Without `read_data` on the generic table, reads are rejected while writes keep
/// working.
#[sqlx::test]
async fn test_sign_generic_table_requires_read_data(pool: PgPool) {
    let authz = HidingAuthorizer::new();
    let (ctx, namespace_name, warehouse_id) = setup(pool, authz.clone()).await;
    let base_location =
        create_generic_table(&ctx, &namespace_name, warehouse_id, GENERIC_TABLE).await;

    authz.block_action(format!("generic_table:{:?}", CatalogGenericTableAction::ReadData).as_str());

    let err = sign_warehouse_scoped(&ctx, warehouse_id, &base_location, Method::GET)
        .await
        .expect_err("GET must be rejected without read_data");
    assert_forbidden(&err);

    // Writes are unaffected.
    let response = sign_warehouse_scoped(&ctx, warehouse_id, &base_location, Method::PUT)
        .await
        .expect("write_data is still granted");
    assert_signed(&response);
}

/// Bulk deletes reach the signer as `POST /{bucket}?delete` rather than as `DELETE`,
/// and are authorized as writes like any other delete.
#[sqlx::test]
async fn test_sign_generic_table_batch_delete(pool: PgPool) {
    let authz = HidingAuthorizer::new();
    let (ctx, namespace_name, warehouse_id) = setup(pool, authz.clone()).await;
    let base_location =
        create_generic_table(&ctx, &namespace_name, warehouse_id, GENERIC_TABLE).await;

    let request = batch_delete_request(&[
        key_under(&base_location, "data/a.bin"),
        key_under(&base_location, "data/b.bin"),
    ]);

    let response = sign_warehouse_scoped_request(&ctx, warehouse_id, request.clone())
        .await
        .expect("a bulk delete of the generic table's own files must be signed");
    assert_signed(&response);

    authz
        .block_action(format!("generic_table:{:?}", CatalogGenericTableAction::WriteData).as_str());
    let err = sign_warehouse_scoped_request(&ctx, warehouse_id, request)
        .await
        .expect_err("a bulk delete must be rejected without write_data");
    assert_forbidden(&err);
}

/// Every key of a bulk delete is validated, not just the one that resolved the table.
#[sqlx::test]
async fn test_sign_generic_table_batch_delete_rejects_foreign_key(pool: PgPool) {
    let (ctx, namespace_name, warehouse_id) = setup(pool, AllowAllAuthorizer::default()).await;
    let base_location =
        create_generic_table(&ctx, &namespace_name, warehouse_id, GENERIC_TABLE).await;

    let request = batch_delete_request(&[
        key_under(&base_location, "data/a.bin"),
        "not-a-table/file.bin".to_string(),
    ]);

    let err = sign_warehouse_scoped_request(&ctx, warehouse_id, request)
        .await
        .expect_err("a bulk delete touching another location must fail");

    assert_eq!(err.error.code, StatusCode::BAD_REQUEST, "{err:?}");
    assert_eq!(err.error.r#type, "RequestUriMismatch", "{err:?}");
}

/// A caller that cannot even see the generic table must not learn that it exists.
#[sqlx::test]
async fn test_sign_generic_table_invisible_is_not_found(pool: PgPool) {
    let authz = HidingAuthorizer::new();
    let (ctx, namespace_name, warehouse_id) = setup(pool, authz.clone()).await;
    let base_location =
        create_generic_table(&ctx, &namespace_name, warehouse_id, GENERIC_TABLE).await;
    let tabular_id = generic_table_id(&ctx, &namespace_name, warehouse_id, GENERIC_TABLE).await;

    authz.hide(&format!("generic_table:{warehouse_id}/{tabular_id}"));

    let err = sign_warehouse_scoped(&ctx, warehouse_id, &base_location, Method::GET)
        .await
        .expect_err("an invisible generic table must not be signable");

    assert_eq!(err.error.code, StatusCode::NOT_FOUND, "{err:?}");
    assert_eq!(err.error.r#type, "NoSuchGenericTableException", "{err:?}");
}

/// Iceberg tables share the resolution path with generic tables, but must keep
/// authorizing against the *table* relations.
#[sqlx::test]
async fn test_sign_iceberg_table_is_unaffected(pool: PgPool) {
    let authz = HidingAuthorizer::new();
    let (ctx, namespace_name, warehouse_id) = setup(pool, authz.clone()).await;
    let (table_id, location) =
        create_catalog_table(&ctx, &namespace_name, warehouse_id, "my_table").await;
    let location = location.to_string();

    let response = sign_warehouse_scoped(&ctx, warehouse_id, &location, Method::GET)
        .await
        .expect("warehouse-scoped signer must sign iceberg tables");
    assert_signed(&response);

    let response = sign_table_scoped(&ctx, warehouse_id, *table_id, &location, Method::PUT)
        .await
        .expect("table-scoped signer must sign iceberg tables");
    assert_signed(&response);

    authz.block_action(format!("table:{:?}", CatalogTableAction::WriteData).as_str());
    let err = sign_warehouse_scoped(&ctx, warehouse_id, &location, Method::PUT)
        .await
        .expect_err("PUT must be rejected without table write_data");
    assert_eq!(err.error.code, StatusCode::FORBIDDEN, "{err:?}");
    assert_eq!(err.error.r#type, "TableActionForbidden", "{err:?}");
}

/// A URI that is not under any tabular's location must still be rejected.
#[sqlx::test]
async fn test_sign_generic_table_rejects_foreign_location(pool: PgPool) {
    let (ctx, namespace_name, warehouse_id) = setup(pool, AllowAllAuthorizer::default()).await;
    create_generic_table(&ctx, &namespace_name, warehouse_id, GENERIC_TABLE).await;

    let foreign: url::Url = format!("{ENDPOINT}/{BUCKET}/not-a-table/file.bin")
        .parse()
        .unwrap();

    let err = sign_warehouse_scoped_request(&ctx, warehouse_id, sign_request(Method::PUT, foreign))
        .await
        .expect_err("signing a URI outside any table location must fail");

    assert_eq!(err.error.code, StatusCode::BAD_REQUEST, "{err:?}");
    assert_eq!(err.error.r#type, "NoSuchTableLocationException", "{err:?}");
}

/// Views are not signable — including through the table-scoped route, which now
/// resolves ids of any tabular type.
#[sqlx::test]
async fn test_sign_view_is_not_signable(pool: PgPool) {
    let (ctx, namespace_name, warehouse_id) = setup(pool, AllowAllAuthorizer::default()).await;
    let view = create_catalog_view(&ctx, &namespace_name, warehouse_id, "myview").await;

    let err = sign_table_scoped(
        &ctx,
        warehouse_id,
        view.uuid(),
        view.location(),
        Method::GET,
    )
    .await
    .expect_err("views must not be signable");

    assert_eq!(err.error.code, StatusCode::BAD_REQUEST, "{err:?}");
    assert_eq!(err.error.r#type, "NoSuchTableLocationException", "{err:?}");
}

/// An id that belongs to no tabular resolves to `None` rather than erroring — that is
/// what makes the signer fall back to the location lookup for dropped tabulars.
///
/// Resolution of each individual tabular type is covered end-to-end by the signer
/// tests above.
#[sqlx::test]
async fn test_get_tabular_info_by_uuid_unknown_id(pool: PgPool) {
    let (ctx, _namespace_name, warehouse_id) = setup(pool, AllowAllAuthorizer::default()).await;

    let resolved = PostgresBackend::get_tabular_info_by_uuid(
        warehouse_id,
        uuid::Uuid::now_v7(),
        TabularListFlags::active(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();

    assert!(resolved.is_none(), "unknown ids must not resolve");
}

#[sqlx::test]
async fn test_generic_table_load_advertises_remote_signing(pool: PgPool) {
    let (ctx, namespace_name, warehouse_id) = setup(pool, AllowAllAuthorizer::default()).await;
    create_generic_table(&ctx, &namespace_name, warehouse_id, GENERIC_TABLE).await;
    let tabular_id = generic_table_id(&ctx, &namespace_name, warehouse_id, GENERIC_TABLE).await;

    let response = CatalogServer::load_generic_table(
        GenericTableParameters {
            prefix: Some(warehouse_id.to_string().into()),
            namespace: namespace(&namespace_name),
            table_name: GENERIC_TABLE.to_string(),
        },
        ctx.clone(),
        // STS is disabled on this warehouse, so remote signing is the only option.
        DataAccess {
            vended_credentials: false,
            remote_signing: true,
        },
        random_request_metadata(),
    )
    .await
    .expect("loading a generic table must succeed");

    let config = response
        .config
        .expect("load response must carry a config when access is delegated");

    assert_eq!(
        config.get("s3.remote-signing-enabled").map(String::as_str),
        Some("true"),
        "config must advertise remote signing: {config:?}"
    );

    // The endpoint carries this table's own id, and is emitted under both the current
    // (`signer.*`) and legacy (`s3.signer.*`) keys.
    let expected_endpoint =
        format!("v1/signer/{warehouse_id}/tabular-id/{tabular_id}/v1/aws/s3/sign");
    for key in ["signer.endpoint", "s3.signer.endpoint"] {
        assert_eq!(
            config.get(key).map(String::as_str),
            Some(expected_endpoint.as_str()),
            "config[{key}] must point at this table's signer endpoint: {config:?}"
        );
    }
    for key in ["signer.uri", "s3.signer.uri"] {
        assert!(
            config.contains_key(key),
            "config must advertise {key}: {config:?}"
        );
    }
}
