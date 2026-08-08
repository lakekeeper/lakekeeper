//! Dry-run validation of warehouse configurations.
//!
//! The contract under test: validation reports the outcome of *every* check it
//! considered, never stops at the first failure, and never mutates anything.

use lakekeeper::{
    ProjectId,
    api::management::v1::{
        ApiServer,
        warehouse::{
            CreateWarehouseRequest, Service, TabularDeleteProfile,
            UpdateWarehouseCredentialRequest, UpdateWarehouseStorageRequest,
            ValidateWarehouseResponse, ValidationCheckName, ValidationCheckStatus,
        },
    },
    service::{
        CatalogCreateWarehouseRequest, CatalogStore, CatalogWarehouseOps, Transaction,
        WarehouseStatus,
        authz::{AllowAllAuthorizer, CatalogWarehouseAction, tests::HidingAuthorizer},
        storage::{MemoryProfile, S3Flavor, S3Profile, StorageProfile},
    },
};
use lakekeeper_integration_tests::{SetupTestCatalog, memory_io_profile, random_request_metadata};
use lakekeeper_storage_postgres::PostgresBackend;
use sqlx::PgPool;
use strum::IntoEnumIterator as _;
use uuid::Uuid;

/// Every check the report can contain.
///
/// Derived from the enum rather than hand-listed, so a newly added check that no
/// endpoint emits fails the exhaustiveness assertion instead of going unnoticed.
fn all_checks() -> Vec<ValidationCheckName> {
    ValidationCheckName::iter().collect()
}

fn status_of(
    response: &ValidateWarehouseResponse,
    name: ValidationCheckName,
) -> ValidationCheckStatus {
    response
        .checks
        .iter()
        .find(|c| c.name == name)
        .unwrap_or_else(|| panic!("report is missing check {name}: {:?}", response.checks))
        .status
}

/// Every check must appear exactly once, whatever its outcome — that is what
/// makes the report an account of what was and was not covered.
fn assert_report_is_exhaustive(response: &ValidateWarehouseResponse) {
    let all_checks = all_checks();
    for name in all_checks.iter().copied() {
        let matches = response.checks.iter().filter(|c| c.name == name).count();
        assert_eq!(
            matches, 1,
            "expected exactly one `{name}` check, got {matches}: {:?}",
            response.checks
        );
    }
    assert_eq!(response.checks.len(), all_checks.len());

    // A failed check always explains itself; a skipped one always gives a reason.
    for check in &response.checks {
        match check.status {
            ValidationCheckStatus::Failed => assert!(
                check.error.is_some(),
                "failed check {} carries no error",
                check.name
            ),
            ValidationCheckStatus::Skipped => assert!(
                check.reason.is_some(),
                "skipped check {} carries no reason",
                check.name
            ),
            ValidationCheckStatus::Passed => assert!(check.error.is_none()),
        }
    }
    assert_eq!(
        response.valid,
        !response
            .checks
            .iter()
            .any(|c| c.status == ValidationCheckStatus::Failed)
    );
}

#[sqlx::test]
async fn test_validate_warehouse_passes_and_persists_nothing(pool: PgPool) {
    let (ctx, _) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let project_id = ProjectId::from(Uuid::nil());
    let before = PostgresBackend::list_warehouses(
        &project_id,
        Some(WarehouseStatus::active_and_inactive().to_vec()),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();

    let response = ApiServer::validate_warehouse(
        CreateWarehouseRequest::builder()
            .warehouse_name(format!("validated-{}", Uuid::now_v7()))
            .storage_profile(memory_io_profile())
            .build(),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    assert!(response.valid, "{:?}", response.checks);
    assert_report_is_exhaustive(&response);

    assert_eq!(
        status_of(&response, ValidationCheckName::ProfileWellFormed),
        ValidationCheckStatus::Passed
    );
    assert_eq!(
        status_of(&response, ValidationCheckName::WarehouseNameValid),
        ValidationCheckStatus::Passed
    );
    assert_eq!(
        status_of(&response, ValidationCheckName::LocationExclusive),
        ValidationCheckStatus::Passed
    );
    assert_eq!(
        status_of(&response, ValidationCheckName::LakekeeperReadWrite),
        ValidationCheckStatus::Passed
    );
    assert_eq!(
        status_of(&response, ValidationCheckName::Cleanup),
        ValidationCheckStatus::Passed
    );
    // The in-memory backend vends no credentials, so those probes do not apply.
    assert_eq!(
        status_of(&response, ValidationCheckName::VendedCredentialsReadWrite),
        ValidationCheckStatus::Skipped
    );
    assert_eq!(
        status_of(&response, ValidationCheckName::ProfileCompatible),
        ValidationCheckStatus::Skipped
    );

    let after = PostgresBackend::list_warehouses(
        &project_id,
        Some(WarehouseStatus::active_and_inactive().to_vec()),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        before.len(),
        after.len(),
        "validation must not create a warehouse"
    );
}

#[sqlx::test]
async fn test_validate_warehouse_reports_name_conflict_without_stopping(pool: PgPool) {
    let (ctx, warehouse) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let response = ApiServer::validate_warehouse(
        CreateWarehouseRequest::builder()
            .warehouse_name(warehouse.warehouse_name.clone())
            .storage_profile(memory_io_profile())
            .build(),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    assert!(!response.valid);
    assert_report_is_exhaustive(&response);
    assert_eq!(
        status_of(&response, ValidationCheckName::WarehouseNameValid),
        ValidationCheckStatus::Failed
    );
    // The storage probes must still have run: a name clash is not a reason to
    // leave the caller guessing about their credentials.
    assert_eq!(
        status_of(&response, ValidationCheckName::LakekeeperReadWrite),
        ValidationCheckStatus::Passed
    );
}

#[sqlx::test]
async fn test_validate_warehouse_detects_a_duplicate_name_differing_in_case(pool: PgPool) {
    // `warehouse_name` is stored under a case-insensitive collation, so the
    // create would be rejected by the database. A case-sensitive comparison here
    // would report a green tick for a name that cannot be used.
    let (ctx, warehouse) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let shouting = warehouse.warehouse_name.to_uppercase();
    assert_ne!(shouting, warehouse.warehouse_name, "name must have letters");

    let response = ApiServer::validate_warehouse(
        CreateWarehouseRequest::builder()
            .warehouse_name(shouting)
            .storage_profile(memory_io_profile())
            .build(),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    assert!(!response.valid, "{:?}", response.checks);
    assert_report_is_exhaustive(&response);
    assert_eq!(
        status_of(&response, ValidationCheckName::WarehouseNameValid),
        ValidationCheckStatus::Failed
    );
}

#[sqlx::test]
async fn test_validate_storage_reports_the_compatibility_check(pool: PgPool) {
    let (ctx, warehouse) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let response = ApiServer::validate_storage_profile(
        warehouse.warehouse_id,
        UpdateWarehouseStorageRequest {
            storage_profile: MemoryProfile::default().into(),
            storage_credential: None,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    assert_report_is_exhaustive(&response);
    assert_eq!(
        status_of(&response, ValidationCheckName::LakekeeperReadWrite),
        ValidationCheckStatus::Passed,
        "the new location is reachable"
    );
    assert_eq!(
        status_of(&response, ValidationCheckName::LocationExclusive),
        ValidationCheckStatus::Skipped
    );
    // The memory profile permits relocation; the rejection of a *moved* location
    // is covered by the unit tests on `profile_compatibility_check`, which can
    // use a profile type that actually forbids it.
    assert_eq!(
        status_of(&response, ValidationCheckName::ProfileCompatible),
        ValidationCheckStatus::Passed
    );
    // Nothing about an update touches these.
    for name in [
        ValidationCheckName::WarehouseNameValid,
        ValidationCheckName::FormatVersionPolicyConsistent,
        ValidationCheckName::ManagedByAllowed,
    ] {
        assert_eq!(status_of(&response, name), ValidationCheckStatus::Skipped);
    }
    // A self-managed warehouse's spec is mutable, so this must be evaluated,
    // not skipped — it is the check that stops a green dry-run on a locked
    // warehouse.
    assert_eq!(
        status_of(&response, ValidationCheckName::SpecMutable),
        ValidationCheckStatus::Passed
    );
}

#[sqlx::test]
async fn test_validate_warehouse_reports_an_unusable_profile_without_probing(pool: PgPool) {
    let (ctx, _) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    // Bucket names must be at least 3 characters: `normalize` rejects this.
    let bad_profile: StorageProfile = S3Profile::builder()
        .bucket("a".to_string())
        .region("us-east-1".to_string())
        .sts_enabled(false)
        .flavor(S3Flavor::Aws)
        .build()
        .into();

    let response = ApiServer::validate_warehouse(
        CreateWarehouseRequest::builder()
            .warehouse_name(format!("syntax-{}", Uuid::now_v7()))
            .storage_profile(bad_profile)
            .build(),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    assert!(!response.valid);
    assert_report_is_exhaustive(&response);
    assert_eq!(
        status_of(&response, ValidationCheckName::ProfileWellFormed),
        ValidationCheckStatus::Failed
    );
    // A profile that cannot be normalized cannot be probed, so every storage
    // check is skipped rather than reported as a storage failure.
    for name in [
        ValidationCheckName::LocationExclusive,
        ValidationCheckName::StorageClientInitialized,
        ValidationCheckName::LakekeeperReadWrite,
        ValidationCheckName::Cleanup,
    ] {
        assert_eq!(
            status_of(&response, name),
            ValidationCheckStatus::Skipped,
            "{name}"
        );
    }
    // ...but the name check does not depend on the profile, so it still runs.
    // This is the "never stops at the first failure" contract.
    assert_eq!(
        status_of(&response, ValidationCheckName::WarehouseNameValid),
        ValidationCheckStatus::Passed
    );
}

#[sqlx::test]
async fn test_validate_warehouse_reports_a_location_overlap(pool: PgPool) {
    // Memory profiles never overlap, so this needs a real storage type. The
    // neighbour is inserted straight into the catalog because creating it
    // through the API would validate (and fail to reach) its storage.
    let (ctx, _) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let profile: StorageProfile = S3Profile::builder()
        .bucket("overlap-bucket".to_string())
        .key_prefix("shared".to_string())
        .region("us-east-1".to_string())
        .endpoint("http://127.0.0.1:1".parse().unwrap())
        .path_style_access(true)
        .sts_enabled(false)
        .flavor(S3Flavor::S3Compat)
        .build()
        .into();

    let project_id = ProjectId::from(Uuid::nil());
    let mut transaction =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    PostgresBackend::create_warehouse(
        &project_id,
        CatalogCreateWarehouseRequest::builder()
            .warehouse_name("neighbour".to_string())
            .storage_profile(profile.clone())
            .delete_profile(TabularDeleteProfile::Hard {})
            .build(),
        transaction.transaction(),
    )
    .await
    .unwrap();
    transaction.commit().await.unwrap();

    let response = ApiServer::validate_warehouse(
        CreateWarehouseRequest::builder()
            .warehouse_name(format!("overlapping-{}", Uuid::now_v7()))
            .storage_profile(profile)
            .build(),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    assert!(!response.valid);
    assert_report_is_exhaustive(&response);
    assert_eq!(
        status_of(&response, ValidationCheckName::LocationExclusive),
        ValidationCheckStatus::Failed
    );
    // The storage probe also fails (nothing is listening) — both are reported,
    // which is the point of an exhaustive report.
    assert_eq!(
        status_of(&response, ValidationCheckName::LakekeeperReadWrite),
        ValidationCheckStatus::Failed
    );
}

#[sqlx::test]
async fn test_validate_storage_access(pool: PgPool) {
    let (ctx, warehouse) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let response = ApiServer::validate_storage_access(
        warehouse.warehouse_id,
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    assert!(response.valid, "{:?}", response.checks);
    assert_report_is_exhaustive(&response);
    assert_eq!(
        status_of(&response, ValidationCheckName::LakekeeperReadWrite),
        ValidationCheckStatus::Passed
    );
    assert_eq!(
        status_of(&response, ValidationCheckName::Cleanup),
        ValidationCheckStatus::Passed
    );
    // Nothing about an existing warehouse's stored profile is up for validation.
    for name in [
        ValidationCheckName::ProfileWellFormed,
        ValidationCheckName::ProfileCompatible,
        ValidationCheckName::WarehouseNameValid,
        ValidationCheckName::LocationExclusive,
    ] {
        assert_eq!(status_of(&response, name), ValidationCheckStatus::Skipped);
    }
}

#[sqlx::test]
async fn test_validate_storage_credential_leaves_the_stored_credential_alone(pool: PgPool) {
    let (ctx, warehouse) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    let before = PostgresBackend::get_warehouse_by_id(
        warehouse.warehouse_id,
        WarehouseStatus::active(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    let response = ApiServer::validate_storage_credential(
        warehouse.warehouse_id,
        UpdateWarehouseCredentialRequest {
            new_storage_credential: None,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    assert!(response.valid, "{:?}", response.checks);
    assert_report_is_exhaustive(&response);

    let after = PostgresBackend::get_warehouse_by_id(
        warehouse.warehouse_id,
        WarehouseStatus::active(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(before.storage_secret_id, after.storage_secret_id);
    assert_eq!(before.version, after.version);
}

#[sqlx::test]
async fn test_validate_endpoints_deny_an_unauthorized_caller(pool: PgPool) {
    // The entire security argument for these endpoints is "same permission as
    // the mutation they stand in for" — pin that down rather than assuming it.
    let authz = HidingAuthorizer::new();
    let (ctx, warehouse) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(authz.clone())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;

    // Prefix match: the action debug-formats with its `name` field, so block the
    // bare variant name rather than a particular name.
    authz.block_action("project:CreateWarehouse");
    authz.block_action(&format!(
        "warehouse:{:?}",
        CatalogWarehouseAction::UpdateStorage
    ));

    let create_denied = ApiServer::validate_warehouse(
        CreateWarehouseRequest::builder()
            .warehouse_name(format!("denied-{}", Uuid::now_v7()))
            .storage_profile(memory_io_profile())
            .build(),
        ctx.clone(),
        random_request_metadata(),
    )
    .await;
    assert!(
        create_denied.is_err(),
        "validate must not probe storage without create permission"
    );

    let storage_denied = ApiServer::validate_storage_profile(
        warehouse.warehouse_id,
        UpdateWarehouseStorageRequest {
            storage_profile: memory_io_profile(),
            storage_credential: None,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await;
    assert!(storage_denied.is_err());

    let stored_denied = ApiServer::validate_storage_access(
        warehouse.warehouse_id,
        ctx.clone(),
        random_request_metadata(),
    )
    .await;
    assert!(stored_denied.is_err());
}
