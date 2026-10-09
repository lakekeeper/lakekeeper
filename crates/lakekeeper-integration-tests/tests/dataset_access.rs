//! Reading a version's bytes: access grants and signed file URLs.
//!
//! The in-memory storage profile's URLs are not fetchable; they name the file
//! and the expiry, which is what these tests check. A real signer serves a file
//! in `s3_compat_integration_tests` below.
use std::{fmt::Debug, sync::Arc};

use http::StatusCode;
use iceberg::NamespaceIdent;
use lakekeeper::{
    api::{
        RequestMetadata, RequestMetadataTestBuilder, Result,
        data::v1::datasets::{
            CreateDatasetAccessGrantRequest, DatasetAccessGrantParameters,
            DatasetAccessGrantResponse, DatasetAccessMode, DatasetService as _,
            ImportDatasetRequest, ImportDatasetResponse, ListDatasetFilesQuery,
            SignDatasetFilesRequest, SignedDatasetFile,
        },
        iceberg::types::Prefix,
    },
    server::CatalogServer,
    service::{
        DatasetAccessGrantId, DatasetOwnership, DatasetSnapshotId, Role, UserId,
        authn::Actor,
        authz::{AllowAllAuthorizer, Authorizer, tests::HidingAuthorizer},
    },
};
use lakekeeper_integration_tests::{
    CapturingAuthzListener, CommitFileExt as _, DATASET, TestDataset, TestNamespace, eventually,
    file, random_request_metadata,
};
use sqlx::PgPool;
use uuid::Uuid;

fn as_user(name: &str) -> RequestMetadata {
    RequestMetadataTestBuilder::builder()
        .actor(Actor::Principal(UserId::new_unchecked("oidc", name)))
        .build()
}

async fn access_grant<A: Authorizer>(
    ds: &TestDataset<A>,
    content_type: Option<&str>,
    metadata: RequestMetadata,
) -> DatasetAccessGrantResponse {
    CatalogServer::create_dataset_access_grant(
        ds.ref_params("main"),
        CreateDatasetAccessGrantRequest {
            content_type: content_type.map(ToString::to_string),
        },
        ds.ctx.clone(),
        metadata,
    )
    .await
    .unwrap()
}

async fn sign<A: Authorizer>(
    ds: &TestDataset<A>,
    grant: &DatasetAccessGrantResponse,
    snapshot_id: DatasetSnapshotId,
    keys: &[&str],
    metadata: RequestMetadata,
) -> Result<Vec<(String, String)>> {
    sign_files(ds, grant, snapshot_id, keys, metadata)
        .await
        .map(|files| files.into_iter().map(|f| (f.logical_key, f.url)).collect())
}

async fn sign_files<A: Authorizer>(
    ds: &TestDataset<A>,
    grant: &DatasetAccessGrantResponse,
    snapshot_id: DatasetSnapshotId,
    keys: &[&str],
    metadata: RequestMetadata,
) -> Result<Vec<SignedDatasetFile>> {
    CatalogServer::sign_dataset_files(
        ds.snapshot_params(snapshot_id),
        SignDatasetFilesRequest {
            grant_id: grant.grant_id,
            keys: keys.iter().map(ToString::to_string).collect(),
        },
        ds.ctx.clone(),
        metadata,
    )
    .await
    .map(|signed| signed.files)
}

async fn revoke<A: Authorizer>(
    ds: &TestDataset<A>,
    grant_id: DatasetAccessGrantId,
    metadata: RequestMetadata,
) -> Result<()> {
    CatalogServer::revoke_dataset_access_grant(
        DatasetAccessGrantParameters {
            prefix: Some(Prefix(ds.prefix.clone())),
            namespace: NamespaceIdent::new(ds.namespace.clone()),
            dataset_name: ds.name.clone(),
            grant_id,
        },
        ds.ctx.clone(),
        metadata,
    )
    .await
}

fn refused<T: Debug>(result: Result<T>, code: StatusCode, error_type: &str) {
    let err = result.expect_err(error_type);
    assert_eq!(
        (err.error.code, err.error.r#type.as_str()),
        (code.as_u16(), error_type),
        "{err:?}"
    );
}

/// A grant pins the snapshot its ref resolved to: it signs that snapshot's files,
/// wherever their bytes are, and not a file committed after it was issued.
#[sqlx::test]
async fn test_a_grant_signs_its_snapshots_files_only(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;
    let head = ds
        .commit(
            None,
            vec![
                file("a.jpg").content_type("image/jpeg"),
                file("notes.txt")
                    .content_type("text/plain")
                    .at("docs/notes.txt"),
                file("x.jpg")
                    .content_type("image/jpeg")
                    .at(format!("{}/elsewhere/x.jpg", ds.location)),
            ],
            &[],
        )
        .await;
    let grant = access_grant(&ds, None, random_request_metadata()).await;
    assert_eq!(grant.snapshot_id, head);
    assert_eq!(grant.access_mode, DatasetAccessMode::Presigned);
    assert_eq!(grant.max_keys_per_request, 1_000);

    let signed = sign(
        &ds,
        &grant,
        head,
        &["a.jpg", "notes.txt", "x.jpg"],
        random_request_metadata(),
    )
    .await
    .unwrap();
    let base = ds.location.trim_end_matches('/');
    let paths: Vec<(&str, &str)> = signed
        .iter()
        .map(|(key, url)| (key.as_str(), url.split('?').next().unwrap()))
        .collect();
    assert_eq!(
        paths,
        [
            ("a.jpg", format!("{base}/a.jpg").as_str()),
            ("notes.txt", format!("{base}/docs/notes.txt").as_str()),
            ("x.jpg", format!("{base}/elsewhere/x.jpg").as_str()),
        ]
    );

    let moved = ds
        .commit(
            Some(head),
            vec![file("b.jpg").content_type("image/jpeg")],
            &[],
        )
        .await;
    refused(
        sign(&ds, &grant, head, &["b.jpg"], random_request_metadata()).await,
        StatusCode::FORBIDDEN,
        "DatasetFilesOutsideGrant",
    );
    let fresh = access_grant(&ds, None, random_request_metadata()).await;
    assert_eq!(fresh.snapshot_id, moved);
    sign(&ds, &fresh, moved, &["b.jpg"], random_request_metadata())
        .await
        .expect("a grant on the moved ref signs its new file");
}

/// Signing reads nothing outside the dataset's location, whatever the manifest
/// says: the URL is signed with the warehouse's own credential.
#[sqlx::test]
async fn test_signing_refuses_a_path_outside_the_dataset(pool: PgPool) {
    let ds = TestDataset::managed(pool.clone()).await;
    let head = ds
        .commit(
            None,
            vec![
                file("a.jpg").content_type("image/jpeg"),
                file("b.jpg").content_type("image/jpeg"),
                file("c.jpg").content_type("image/jpeg"),
            ],
            &[],
        )
        .await;
    // Manifest rows past the commit's checks: a URI elsewhere, and paths a signer
    // building a URL resolves above the location.
    for (key, path) in [
        ("a.jpg", "memory://elsewhere/secret.parquet"),
        ("b.jpg", "%2e%2e/%2e%2e/elsewhere/secret.parquet"),
        ("c.jpg", "x\\..\\..\\elsewhere\\secret.parquet"),
    ] {
        sqlx::query("UPDATE dataset_manifest_entry SET physical_path = $1 WHERE logical_key = $2")
            .bind(path)
            .bind(key)
            .execute(&pool)
            .await
            .unwrap();
    }
    let grant = access_grant(&ds, None, random_request_metadata()).await;

    for key in ["a.jpg", "b.jpg", "c.jpg"] {
        let err = sign(&ds, &grant, head, &[key], random_request_metadata())
            .await
            .unwrap_err();
        assert_eq!(err.error.r#type, "InvalidPhysicalPath", "{key}: {err:?}");
    }
}

/// A grant narrowed to a content type signs files of that type only.
#[sqlx::test]
async fn test_a_grant_signs_its_content_type_only(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;
    let head = ds
        .commit(
            None,
            vec![
                file("a.jpg").content_type("image/jpeg"),
                file("notes.txt").content_type("text/plain"),
            ],
            &[],
        )
        .await;
    let grant = access_grant(&ds, Some("image/jpeg"), random_request_metadata()).await;
    assert_eq!(grant.content_type.as_deref(), Some("image/jpeg"));
    sign(&ds, &grant, head, &["a.jpg"], random_request_metadata())
        .await
        .unwrap();
    refused(
        sign(
            &ds,
            &grant,
            head,
            &["a.jpg", "notes.txt"],
            random_request_metadata(),
        )
        .await,
        StatusCode::FORBIDDEN,
        "DatasetFilesOutsideGrant",
    );
}

/// Revoking a grant stops the next signing call, as does its expiry.
#[sqlx::test]
async fn test_a_revoked_or_expired_grant_signs_nothing(pool: PgPool) {
    let ds = TestDataset::managed(pool.clone()).await;
    let head = ds
        .commit(None, vec![file("a.jpg").content_type("image/jpeg")], &[])
        .await;

    let revoked = access_grant(&ds, None, random_request_metadata()).await;
    sign(&ds, &revoked, head, &["a.jpg"], random_request_metadata())
        .await
        .unwrap();
    revoke(&ds, revoked.grant_id, random_request_metadata())
        .await
        .unwrap();
    refused(
        sign(&ds, &revoked, head, &["a.jpg"], random_request_metadata()).await,
        StatusCode::FORBIDDEN,
        "DatasetAccessGrantRevoked",
    );
    revoke(&ds, revoked.grant_id, random_request_metadata())
        .await
        .expect("revoking twice is not an error");

    let expired = access_grant(&ds, None, random_request_metadata()).await;
    sqlx::query(
        "UPDATE dataset_access_grant SET expires_at = now() - interval '1 second'
         WHERE grant_id = $1",
    )
    .bind(Uuid::from(expired.grant_id))
    .execute(&pool)
    .await
    .unwrap();
    refused(
        sign(&ds, &expired, head, &["a.jpg"], random_request_metadata()).await,
        StatusCode::FORBIDDEN,
        "DatasetAccessGrantExpired",
    );
}

/// A grant serves only the caller that obtained it, and only through the
/// snapshot it covers. Someone else's grant reads as absent.
#[sqlx::test]
async fn test_a_grant_serves_its_holder_and_snapshot_only(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;
    let head = ds
        .commit(None, vec![file("a.jpg").content_type("image/jpeg")], &[])
        .await;
    let grant = access_grant(&ds, None, as_user("alice")).await;

    sign(&ds, &grant, head, &["a.jpg"], as_user("alice"))
        .await
        .unwrap();
    for other in [as_user("bob"), random_request_metadata()] {
        refused(
            sign(&ds, &grant, head, &["a.jpg"], other).await,
            StatusCode::NOT_FOUND,
            "DatasetAccessGrantNotFound",
        );
    }
    refused(
        sign(
            &ds,
            &grant,
            DatasetSnapshotId::from(Uuid::now_v7()),
            &["a.jpg"],
            as_user("alice"),
        )
        .await,
        StatusCode::FORBIDDEN,
        "DatasetAccessGrantSnapshotMismatch",
    );
}

/// A grant issued under an assumed role belongs to that principal acting as that
/// role, by id: renaming the role keeps it, and the principal alone cannot use it.
#[sqlx::test]
async fn test_a_grant_follows_its_assumed_role_by_id(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;
    let head = ds
        .commit(None, vec![file("a.jpg").content_type("image/jpeg")], &[])
        .await;
    let role = Role::new_random();
    let acting_as = |role: Role| {
        RequestMetadataTestBuilder::builder()
            .actor(Actor::Role {
                principal: UserId::new_unchecked("oidc", "alice"),
                assumed_role: Arc::new(role),
            })
            .build()
    };
    let grant = access_grant(&ds, None, acting_as(role.clone())).await;

    let renamed = Role {
        name: "renamed".to_string(),
        ..role
    };
    sign(&ds, &grant, head, &["a.jpg"], acting_as(renamed))
        .await
        .expect("a renamed role keeps its grants");
    for other in [as_user("alice"), acting_as(Role::new_random())] {
        refused(
            sign(&ds, &grant, head, &["a.jpg"], other).await,
            StatusCode::NOT_FOUND,
            "DatasetAccessGrantNotFound",
        );
    }
}

/// Each key asked for gets a URL, in the order asked, a repeated key included.
#[sqlx::test]
async fn test_signing_answers_each_key_in_order(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;
    let head = ds
        .commit(
            None,
            vec![
                file("a.jpg").content_type("image/jpeg"),
                file("b.jpg").content_type("image/jpeg"),
            ],
            &[],
        )
        .await;
    let grant = access_grant(&ds, None, random_request_metadata()).await;

    let signed = sign(
        &ds,
        &grant,
        head,
        &["b.jpg", "a.jpg", "b.jpg"],
        random_request_metadata(),
    )
    .await
    .expect("every key is in the snapshot");
    let keys: Vec<&str> = signed.iter().map(|(key, _)| key.as_str()).collect();
    assert_eq!(keys, ["b.jpg", "a.jpg", "b.jpg"]);
}

/// A file that records an object version is signed at that version.
#[sqlx::test]
async fn test_a_pinned_version_is_what_is_signed(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;
    let pinned = file("pinned.jpg")
        .content_type("image/jpeg")
        .version_id("v7");
    let head = ds
        .commit(
            None,
            vec![pinned, file("current.jpg").content_type("image/jpeg")],
            &[],
        )
        .await;
    let grant = access_grant(&ds, None, random_request_metadata()).await;

    let signed = sign(
        &ds,
        &grant,
        head,
        &["pinned.jpg", "current.jpg"],
        random_request_metadata(),
    )
    .await
    .unwrap();

    assert!(signed[0].1.ends_with("&version=v7"), "{}", signed[0].1);
    assert!(!signed[1].1.contains("version="), "{}", signed[1].1);
}

/// A batch holds at least one key and at most the configured maximum.
#[sqlx::test]
async fn test_a_signing_batch_is_bounded(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;
    let head = ds
        .commit(None, vec![file("a.jpg").content_type("image/jpeg")], &[])
        .await;
    let grant = access_grant(&ds, None, random_request_metadata()).await;
    let too_many: Vec<String> = (0..=grant.max_keys_per_request)
        .map(|i| format!("{i}.jpg"))
        .collect();
    let too_many: Vec<&str> = too_many.iter().map(String::as_str).collect();
    for keys in [&[][..], &too_many[..]] {
        refused(
            sign(&ds, &grant, head, keys, random_request_metadata()).await,
            StatusCode::BAD_REQUEST,
            "InvalidSignBatch",
        );
    }
    // The maximum itself is a valid batch: it reaches the scope check, which
    // refuses the keys the snapshot does not hold.
    refused(
        sign(&ds, &grant, head, &too_many[1..], random_request_metadata()).await,
        StatusCode::FORBIDDEN,
        "DatasetFilesOutsideGrant",
    );
}

/// A holder revoking its own grant only needs to see the dataset, and its record
/// says so; revoking another caller's records the action it took.
#[sqlx::test]
async fn test_a_revoke_records_the_action_it_took(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;
    ds.commit(None, vec![file("a.jpg").content_type("image/jpeg")], &[])
        .await;
    let own = access_grant(&ds, None, as_user("alice")).await;
    let anothers = access_grant(&ds, None, as_user("alice")).await;
    let listener = Arc::new(CapturingAuthzListener::default());
    ds.ctx.v1_state.events.append(listener.clone()).await;
    // Events go out on their own tasks, so a grant's `read_data` record can still
    // arrive after the listener is in place: only the revokes' records count.
    let revoke_actions = || {
        listener
            .recorded_actions()
            .0
            .into_iter()
            .flatten()
            .map(|action| action.action_name)
            .filter(|action| action != "read_data")
            .collect::<Vec<_>>()
    };

    revoke(&ds, own.grant_id, as_user("alice")).await.unwrap();
    eventually("the revoke is recorded", || async {
        !revoke_actions().is_empty()
    })
    .await;
    assert_eq!(revoke_actions()[0], "get_metadata");
    revoke(&ds, anothers.grant_id, as_user("bob"))
        .await
        .unwrap();
    eventually("the revoke is recorded", || async {
        revoke_actions().len() > 1
    })
    .await;
    assert_eq!(revoke_actions()[1], "revoke_access_grants");
}

/// A ref with no commits has no files to grant.
#[sqlx::test]
async fn test_an_empty_ref_grants_nothing(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;
    refused(
        CatalogServer::create_dataset_access_grant(
            ds.ref_params("main"),
            CreateDatasetAccessGrantRequest::default(),
            ds.ctx.clone(),
            random_request_metadata(),
        )
        .await,
        StatusCode::CONFLICT,
        "DatasetRefHasNoSnapshot",
    );
}

/// A grant's holder may always revoke it; revoking another caller's takes
/// `RevokeAccessGrants`.
#[sqlx::test]
async fn test_revoking_another_callers_grant_takes_its_own_action(pool: PgPool) {
    let authorizer = HidingAuthorizer::new();
    let ds = TestNamespace::with_authorizer(pool, authorizer.clone())
        .await
        .create_dataset(DATASET, DatasetOwnership::Managed, None)
        .await;
    let head = ds
        .commit(None, vec![file("a.jpg").content_type("image/jpeg")], &[])
        .await;

    let revoked_by_bob = access_grant(&ds, None, as_user("alice")).await;
    revoke(&ds, revoked_by_bob.grant_id, as_user("bob"))
        .await
        .expect("the action lets another caller revoke it");
    refused(
        sign(&ds, &revoked_by_bob, head, &["a.jpg"], as_user("alice")).await,
        StatusCode::FORBIDDEN,
        "DatasetAccessGrantRevoked",
    );

    authorizer.block_action("dataset:RevokeAccessGrants");
    let grant = access_grant(&ds, None, as_user("alice")).await;
    refused(
        revoke(&ds, grant.grant_id, as_user("bob")).await,
        StatusCode::FORBIDDEN,
        "DatasetActionForbidden",
    );
    revoke(&ds, grant.grant_id, as_user("alice"))
        .await
        .expect("a grant's holder revokes it without the action");
}

/// The file listing says how to read: the in-memory profile vends nothing, so
/// signed URLs.
#[sqlx::test]
async fn test_the_listing_says_how_to_read(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;
    ds.commit(None, vec![file("a.jpg").content_type("image/jpeg")], &[])
        .await;
    let listed = CatalogServer::list_dataset_files(
        ds.ref_params("main"),
        ListDatasetFilesQuery::default(),
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert_eq!(listed.access_mode, DatasetAccessMode::Presigned);
}

/// Each signed file carries the etag its snapshot recorded, for the reader to hold
/// the download's `ETag` against; a file that recorded none carries none.
#[sqlx::test]
async fn test_signed_files_carry_the_recorded_etag(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;
    let head = ds
        .commit(
            None,
            vec![
                file("a.jpg")
                    .content_type("image/jpeg")
                    .etag("\"9b2cf535f27731c974343645a3985328\""),
                file("b.jpg").content_type("image/jpeg"),
            ],
            &[],
        )
        .await;
    let grant = access_grant(&ds, None, random_request_metadata()).await;

    let signed = sign_files(
        &ds,
        &grant,
        head,
        &["a.jpg", "b.jpg"],
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert_eq!(
        signed[0].etag.as_deref(),
        Some("\"9b2cf535f27731c974343645a3985328\"")
    );
    assert_eq!(signed[1].etag, None);
}

/// Recording versions needs a listing that reports them; the in-memory store's,
/// like Azure's, reports none. Refused before a task is queued.
#[sqlx::test]
async fn test_record_versions_is_refused_where_listings_carry_none(pool: PgPool) {
    let ds = TestDataset::imported(pool).await;
    for queued in [false, true] {
        refused(
            CatalogServer::import_dataset(
                ds.params(),
                ImportDatasetRequest {
                    record_versions: Some(true),
                    queued: Some(queued),
                    ..Default::default()
                },
                ds.ctx.clone(),
                random_request_metadata(),
            )
            .await,
            StatusCode::BAD_REQUEST,
            "InvalidRecordVersions",
        );
    }
}

// Nested one level deep so each test path contains its module's name between
// `::`s, which the nextest filters match (a root module would not).
mod access {
    /// Run against the S3-compatible store configured via `LAKEKEEPER_TEST__S3_*`:
    /// the default nextest profile skips them, the `s3_compat` and CI profiles run
    /// them.
    mod s3_compat_integration_tests {
        use lakekeeper_integration_tests::s3_compatible_profile;
        use reqwest::{Client, Url, header::RANGE};

        use super::super::*;

        /// A signed URL serves the committed file's bytes over plain HTTP, ranges
        /// included, with a key that needs encoding.
        #[sqlx::test]
        async fn test_a_signed_url_serves_the_committed_file(pool: PgPool) {
            let (profile, credential) = s3_compatible_profile();
            let ds = TestNamespace::on_storage(
                pool,
                AllowAllAuthorizer::default(),
                profile,
                Some(credential),
            )
            .await
            .create_dataset(DATASET, DatasetOwnership::Managed, None)
            .await;

            let data: Vec<u8> = (0..=255u8).cycle().take(4096).collect();
            ds.write("a photo+1.jpg", &data).await;
            let head = ds
                .commit(
                    None,
                    vec![file("a photo+1.jpg").content_type("image/jpeg")],
                    &[],
                )
                .await;
            let grant = access_grant(&ds, None, random_request_metadata()).await;
            let signed = sign(
                &ds,
                &grant,
                head,
                &["a photo+1.jpg"],
                random_request_metadata(),
            )
            .await
            .unwrap();
            let url = &signed[0].1;

            let client = Client::new();
            let whole = client
                .get(url)
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap()
                .bytes()
                .await
                .unwrap();
            assert_eq!(whole.as_ref(), data.as_slice());
            let range = client
                .get(url)
                .header(RANGE, "bytes=100-199")
                .send()
                .await
                .unwrap();
            assert_eq!(range.status(), StatusCode::PARTIAL_CONTENT);
            assert_eq!(range.bytes().await.unwrap().as_ref(), &data[100..200]);
        }
        /// An import records the etag S3 lists, and a file that records a version is
        /// signed at it: `versionId` goes into the URL, and into its signature.
        #[sqlx::test]
        async fn test_s3_records_etags_and_signs_versions(pool: PgPool) {
            let (profile, credential) = s3_compatible_profile();
            let ds = TestNamespace::on_storage(
                pool,
                AllowAllAuthorizer::default(),
                profile,
                Some(credential),
            )
            .await
            .create_dataset(DATASET, DatasetOwnership::Imported, None)
            .await;
            ds.write("listed.bin", b"listed").await;

            let imported = CatalogServer::import_dataset(
                ds.params(),
                ImportDatasetRequest::default(),
                ds.ctx.clone(),
                random_request_metadata(),
            )
            .await
            .unwrap();
            let files = CatalogServer::list_dataset_files(
                ds.ref_params("main"),
                ListDatasetFilesQuery::default(),
                ds.ctx.clone(),
                random_request_metadata(),
            )
            .await
            .unwrap()
            .files;
            assert!(files[0].etag.is_some(), "{:?}", files[0]);

            let head = ds
                .commit(
                    imported.snapshot_id,
                    vec![
                        file("pinned.bin")
                            .content_type("application/octet-stream")
                            .version_id("3HL4kqtJlcpXroDTDmJ"),
                    ],
                    &[],
                )
                .await;
            let grant = access_grant(&ds, None, random_request_metadata()).await;
            let signed = sign(
                &ds,
                &grant,
                head,
                &["pinned.bin", "listed.bin"],
                random_request_metadata(),
            )
            .await
            .unwrap();
            let pinned = Url::parse(&signed[0].1).unwrap();
            assert!(
                pinned
                    .query_pairs()
                    .any(|(k, v)| k == "versionId" && v == "3HL4kqtJlcpXroDTDmJ"),
                "{pinned}"
            );
            let current = Url::parse(&signed[1].1).unwrap();
            assert!(
                !current.query_pairs().any(|(k, _)| k == "versionId"),
                "{current}"
            );
        }
    }

    /// These need a bucket with versioning on, and create it: they run where the
    /// store's identity may create buckets, as CI's MinIO allows, and not under the
    /// `s3_compat` profile, whose identity cannot.
    mod s3_versioning_integration_tests {
        use std::collections::BTreeMap;

        use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};
        use futures::StreamExt as _;
        use lakekeeper::{
            api::data::v1::datasets::{CreateDatasetRefRequest, DatasetRefSource, ImportMode},
            service::{
                DatasetRefType,
                storage::{StorageCredential, StorageProfile},
            },
        };
        use lakekeeper_integration_tests::s3_compatible_profile;
        use lakekeeper_io::{LakekeeperStorage as _, StorageBackend};

        use super::super::*;

        const VERSIONED_BUCKET: &str = "tests-versioned";

        /// A profile on a bucket with versioning on, beside the tests' bucket. Created
        /// by the first test that needs it; each test writes under its own prefix.
        async fn versioned_s3_profile() -> (StorageProfile, StorageCredential) {
            let (mut profile, credential) = s3_compatible_profile();
            let StorageProfile::S3(s3) = &mut profile else {
                panic!("an S3 profile")
            };
            s3.bucket = VERSIONED_BUCKET.to_string();
            let StorageBackend::S3(storage) = profile.file_io(Some(&credential)).await.unwrap()
            else {
                panic!("S3 storage")
            };
            // Already there on every run but the first.
            let _ = storage
                .client()
                .create_bucket()
                .bucket(VERSIONED_BUCKET)
                .send()
                .await;
            storage
                .client()
                .put_bucket_versioning()
                .bucket(VERSIONED_BUCKET)
                .versioning_configuration(
                    VersioningConfiguration::builder()
                        .status(BucketVersioningStatus::Enabled)
                        .build(),
                )
                .send()
                .await
                .unwrap();
            (profile, credential)
        }

        /// An imported dataset on the versioned bucket.
        async fn versioned_dataset(pool: PgPool) -> TestDataset {
            let (profile, credential) = versioned_s3_profile().await;
            TestNamespace::on_storage(
                pool,
                AllowAllAuthorizer::default(),
                profile,
                Some(credential),
            )
            .await
            .create_dataset(DATASET, DatasetOwnership::Imported, None)
            .await
        }

        /// An import that records the versions it lists.
        async fn import(ds: &TestDataset, mode: ImportMode, check: bool) -> ImportDatasetResponse {
            CatalogServer::import_dataset(
                ds.params(),
                ImportDatasetRequest {
                    mode: Some(mode),
                    record_versions: Some(true),
                    check_materialization: Some(check),
                    ..Default::default()
                },
                ds.ctx.clone(),
                random_request_metadata(),
            )
            .await
            .unwrap()
        }

        async fn tag(ds: &TestDataset, snapshot_id: DatasetSnapshotId) {
            CatalogServer::create_dataset_ref(
                ds.params(),
                CreateDatasetRefRequest {
                    name: "v1".to_string(),
                    typ: DatasetRefType::Tag,
                    source: DatasetRefSource::Snapshot { snapshot_id },
                },
                ds.ctx.clone(),
                random_request_metadata(),
            )
            .await
            .unwrap();
        }

        /// A pinned file is judged by its version: as recorded while S3 still holds it,
        /// though the key was deleted, and missing once the version itself is gone.
        #[sqlx::test]
        async fn test_a_pinned_file_is_judged_by_its_version(pool: PgPool) {
            let ds = versioned_dataset(pool).await;
            let StorageBackend::S3(storage) = ds.storage().await else {
                panic!("S3 storage")
            };
            ds.write("a.bin", b"pinned").await;
            let first = import(&ds, ImportMode::AddOnly, false)
                .await
                .snapshot_id
                .unwrap();
            tag(&ds, first).await;
            let version = CatalogServer::list_dataset_files(
                ds.ref_params("main"),
                ListDatasetFilesQuery::default(),
                ds.ctx.clone(),
                random_request_metadata(),
            )
            .await
            .unwrap()
            .files[0]
                .version_id
                .clone()
                .unwrap();

            // Deleting the key leaves the version behind a delete marker.
            ds.delete("a.bin").await;
            let held = import(&ds, ImportMode::Sync, true).await;
            assert_eq!(held.removed, 1, "main drops the key");
            assert_eq!(
                held.materialization.unwrap().degraded_snapshots,
                0,
                "the tag's version is still stored"
            );

            let path = ds.path("a.bin");
            let key = path
                .strip_prefix(&format!("s3://{VERSIONED_BUCKET}/"))
                .unwrap();
            storage
                .client()
                .delete_object()
                .bucket(VERSIONED_BUCKET)
                .key(key)
                .version_id(&version)
                .send()
                .await
                .unwrap();
            let gone = import(&ds, ImportMode::AddOnly, true).await;
            assert_eq!(gone.materialization.unwrap().degraded_snapshots, 1);
        }

        /// Listing current versions a page at a time: every key that still exists
        /// lists once, at its newest version, however one key's versions fall
        /// across pages, and a deleted key not at all.
        #[tokio::test]
        async fn test_current_versions_list_once_across_pages() {
            let (profile, credential) = versioned_s3_profile().await;
            let StorageBackend::S3(storage) = profile.file_io(Some(&credential)).await.unwrap()
            else {
                panic!("S3 storage")
            };
            let base = profile.base_location().unwrap().to_string();
            let base = base.trim_end_matches('/');
            let key_of = |name: &str| {
                format!("{base}/{name}")
                    .strip_prefix(&format!("s3://{VERSIONED_BUCKET}/"))
                    .unwrap()
                    .to_string()
            };
            let mut newest = BTreeMap::new();
            for name in ["k0", "k1", "k2", "k3", "k4"] {
                for generation in 0..3 {
                    let put = storage
                        .client()
                        .put_object()
                        .bucket(VERSIONED_BUCKET)
                        .key(key_of(name))
                        .body(format!("{name}-{generation}").into_bytes().into())
                        .send()
                        .await
                        .unwrap();
                    newest.insert(name.to_string(), put.version_id().unwrap().to_string());
                }
            }
            storage.delete(&format!("{base}/k2")).await.unwrap();
            newest.remove("k2");

            let mut listed = BTreeMap::new();
            let mut pages = storage.list_current_versions(base, Some(2)).await.unwrap();
            while let Some(page) = pages.next().await {
                for file in page.unwrap() {
                    let name = file
                        .location()
                        .to_string()
                        .rsplit('/')
                        .next()
                        .unwrap()
                        .to_string();
                    let version = file.version().map(ToString::to_string).unwrap();
                    assert!(
                        listed.insert(name.clone(), version).is_none(),
                        "{name} listed twice"
                    );
                }
            }
            assert_eq!(listed, newest);
        }

        /// With `record-versions`, an import pins each file to the version it listed.
        /// After the key is written again, a URL signed for the older snapshot still
        /// serves its bytes, with the etag the sign response named, and the check
        /// judges the pinned file as recorded.
        #[sqlx::test]
        async fn test_record_versions_pins_imported_files(pool: PgPool) {
            let ds = versioned_dataset(pool).await;
            ds.write("a.bin", b"first").await;

            let first = import(&ds, ImportMode::AddOnly, false)
                .await
                .snapshot_id
                .unwrap();
            let files = CatalogServer::list_dataset_files(
                ds.ref_params("main"),
                ListDatasetFilesQuery::default(),
                ds.ctx.clone(),
                random_request_metadata(),
            )
            .await
            .unwrap()
            .files;
            assert!(files[0].version_id.is_some(), "{:?}", files[0]);
            let grant = access_grant(&ds, None, random_request_metadata()).await;
            tag(&ds, first).await;

            ds.write("a.bin", b"second, and longer").await;
            let synced = import(&ds, ImportMode::Sync, true).await;
            assert_eq!(synced.modified, 1);
            assert_eq!(
                synced.materialization.unwrap().degraded_snapshots,
                0,
                "the tag's file is pinned to its version"
            );

            let signed = sign_files(&ds, &grant, first, &["a.bin"], random_request_metadata())
                .await
                .unwrap();
            let response = reqwest::get(&signed[0].url).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let etag = response
                .headers()
                .get("etag")
                .and_then(|etag| etag.to_str().ok())
                .map(ToString::to_string);
            assert_eq!(response.bytes().await.unwrap().as_ref(), b"first");
            assert_eq!(etag, signed[0].etag, "the reader's check holds");
        }
    }
}
