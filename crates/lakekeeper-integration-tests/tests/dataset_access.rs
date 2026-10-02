//! Reading a version's bytes: access grants and signed file URLs.
//!
//! The in-memory storage profile's URLs are not fetchable; they name the file
//! and the expiry, which is what these tests check. A real signer serves a file
//! in `s3_compat_integration_tests` below.
use std::sync::Arc;

use http::StatusCode;
use iceberg::NamespaceIdent;
use lakekeeper::{
    api::{
        ApiContext, RequestMetadata, RequestMetadataTestBuilder,
        data::v1::datasets::{
            CommitDatasetRequest, CommitFile, CreateDatasetAccessGrantRequest,
            DatasetAccessGrantParameters, DatasetAccessGrantResponse, DatasetAccessMode,
            DatasetParameters, DatasetRefParameters, DatasetService as _,
            DatasetSnapshotParameters, ImportDatasetRequest, ListDatasetFilesQuery,
            SignDatasetFilesRequest, SignedDatasetFile,
        },
        iceberg::types::Prefix,
        management::v1::warehouse::TabularDeleteProfile,
    },
    server::CatalogServer,
    service::{
        DatasetAccessGrantId, DatasetSnapshotId, Role, State, UserId,
        authn::Actor,
        authz::{AllowAllAuthorizer, Authorizer, tests::HidingAuthorizer},
        events::{EventListener, types::authorization::AuthorizationSucceededEvent},
    },
};
use lakekeeper_integration_tests::{
    create_dataset, create_ns, memory_io_profile, random_request_metadata, setup,
};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;
use uuid::Uuid;

type Ctx<A> = ApiContext<State<A, PostgresBackend, SecretsState>>;

const DS: &str = "images";

struct Dataset<A: Authorizer> {
    ctx: Ctx<A>,
    prefix: String,
    ns: String,
    location: String,
}

async fn make_dataset<A: Authorizer>(pool: PgPool, authorizer: A) -> Dataset<A> {
    let (ctx, warehouse) = setup(
        pool.clone(),
        memory_io_profile(),
        None,
        authorizer,
        TabularDeleteProfile::Hard {},
        None,
        1,
        None,
    )
    .await;
    let prefix = warehouse.warehouse_id.to_string();
    let ns = format!("ns_{}", Uuid::now_v7());
    create_ns(ctx.clone(), prefix.clone(), ns.clone()).await;
    let created = create_dataset(ctx.clone(), prefix.clone(), ns.clone(), DS)
        .await
        .unwrap();
    Dataset {
        ctx,
        prefix,
        ns,
        location: created.dataset.location,
    }
}

fn as_user(name: &str) -> RequestMetadata {
    RequestMetadataTestBuilder::builder()
        .actor(Actor::Principal(UserId::new_unchecked("oidc", name)))
        .build()
}

fn file(key: &str, content_type: &str, physical_path: Option<String>) -> CommitFile {
    CommitFile {
        logical_key: key.to_string(),
        physical_path,
        etag: None,
        size: Some(1),
        content_type: Some(content_type.to_string()),
        checksum: None,
        version_id: None,
        last_modified: None,
    }
}

impl<A: Authorizer + Clone> Dataset<A> {
    fn ref_params(&self, name: &str) -> DatasetRefParameters {
        DatasetRefParameters {
            prefix: Some(Prefix(self.prefix.clone())),
            namespace: NamespaceIdent::new(self.ns.clone()),
            dataset_name: DS.to_string(),
            ref_name: name.to_string(),
        }
    }

    async fn commit(
        &self,
        parent: Option<DatasetSnapshotId>,
        added: Vec<CommitFile>,
    ) -> DatasetSnapshotId {
        CatalogServer::commit_dataset(
            self.ref_params("main"),
            CommitDatasetRequest {
                parent_snapshot_id: parent,
                added,
                removed: vec![],
                summary: None,
                on_constraint_violation: None,
            },
            self.ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap()
        .snapshot_id
    }

    async fn grant(
        &self,
        content_type: Option<&str>,
        metadata: RequestMetadata,
    ) -> DatasetAccessGrantResponse {
        CatalogServer::create_dataset_access_grant(
            self.ref_params("main"),
            CreateDatasetAccessGrantRequest {
                content_type: content_type.map(ToString::to_string),
            },
            self.ctx.clone(),
            metadata,
        )
        .await
        .unwrap()
    }

    async fn sign(
        &self,
        grant: &DatasetAccessGrantResponse,
        snapshot_id: DatasetSnapshotId,
        keys: &[&str],
        metadata: RequestMetadata,
    ) -> lakekeeper::api::Result<Vec<(String, String)>> {
        self.sign_files(grant, snapshot_id, keys, metadata)
            .await
            .map(|files| files.into_iter().map(|f| (f.logical_key, f.url)).collect())
    }

    async fn sign_files(
        &self,
        grant: &DatasetAccessGrantResponse,
        snapshot_id: DatasetSnapshotId,
        keys: &[&str],
        metadata: RequestMetadata,
    ) -> lakekeeper::api::Result<Vec<SignedDatasetFile>> {
        CatalogServer::sign_dataset_files(
            DatasetSnapshotParameters {
                prefix: Some(Prefix(self.prefix.clone())),
                namespace: NamespaceIdent::new(self.ns.clone()),
                dataset_name: DS.to_string(),
                snapshot_id,
            },
            SignDatasetFilesRequest {
                grant_id: grant.grant_id,
                keys: keys.iter().map(ToString::to_string).collect(),
            },
            self.ctx.clone(),
            metadata,
        )
        .await
        .map(|signed| signed.files)
    }

    async fn revoke(
        &self,
        grant_id: DatasetAccessGrantId,
        metadata: RequestMetadata,
    ) -> lakekeeper::api::Result<()> {
        CatalogServer::revoke_dataset_access_grant(
            DatasetAccessGrantParameters {
                prefix: Some(Prefix(self.prefix.clone())),
                namespace: NamespaceIdent::new(self.ns.clone()),
                dataset_name: DS.to_string(),
                grant_id,
            },
            self.ctx.clone(),
            metadata,
        )
        .await
    }
}

fn refused<T: std::fmt::Debug>(
    result: lakekeeper::api::Result<T>,
    code: StatusCode,
    error_type: &str,
) {
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
    let ds = make_dataset(pool, AllowAllAuthorizer::default()).await;
    let head = ds
        .commit(
            None,
            vec![
                file("a.jpg", "image/jpeg", None),
                file(
                    "notes.txt",
                    "text/plain",
                    Some("docs/notes.txt".to_string()),
                ),
                file(
                    "x.jpg",
                    "image/jpeg",
                    Some(format!("{}/elsewhere/x.jpg", ds.location)),
                ),
            ],
        )
        .await;
    let grant = ds.grant(None, random_request_metadata()).await;
    assert_eq!(grant.snapshot_id, head);
    assert_eq!(grant.access_mode, DatasetAccessMode::Presigned);
    assert_eq!(grant.max_keys_per_request, 1_000);

    let signed = ds
        .sign(
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
        .commit(Some(head), vec![file("b.jpg", "image/jpeg", None)])
        .await;
    refused(
        ds.sign(&grant, head, &["b.jpg"], random_request_metadata())
            .await,
        StatusCode::FORBIDDEN,
        "DatasetFilesOutsideGrant",
    );
    let fresh = ds.grant(None, random_request_metadata()).await;
    assert_eq!(fresh.snapshot_id, moved);
    ds.sign(&fresh, moved, &["b.jpg"], random_request_metadata())
        .await
        .expect("a grant on the moved ref signs its new file");
}

/// Signing reads nothing outside the dataset's location, whatever the manifest
/// says: the URL is signed with the warehouse's own credential.
#[sqlx::test]
async fn test_signing_refuses_a_path_outside_the_dataset(pool: PgPool) {
    let ds = make_dataset(pool.clone(), AllowAllAuthorizer::default()).await;
    let head = ds
        .commit(
            None,
            vec![
                file("a.jpg", "image/jpeg", None),
                file("b.jpg", "image/jpeg", None),
                file("c.jpg", "image/jpeg", None),
            ],
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
    let grant = ds.grant(None, random_request_metadata()).await;

    for key in ["a.jpg", "b.jpg", "c.jpg"] {
        let err = ds
            .sign(&grant, head, &[key], random_request_metadata())
            .await
            .unwrap_err();
        assert_eq!(err.error.r#type, "InvalidPhysicalPath", "{key}: {err:?}");
    }
}

/// A grant narrowed to a content type signs files of that type only.
#[sqlx::test]
async fn test_a_grant_signs_its_content_type_only(pool: PgPool) {
    let ds = make_dataset(pool, AllowAllAuthorizer::default()).await;
    let head = ds
        .commit(
            None,
            vec![
                file("a.jpg", "image/jpeg", None),
                file("notes.txt", "text/plain", None),
            ],
        )
        .await;
    let grant = ds
        .grant(Some("image/jpeg"), random_request_metadata())
        .await;
    assert_eq!(grant.content_type.as_deref(), Some("image/jpeg"));
    ds.sign(&grant, head, &["a.jpg"], random_request_metadata())
        .await
        .unwrap();
    refused(
        ds.sign(
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
    let ds = make_dataset(pool.clone(), AllowAllAuthorizer::default()).await;
    let head = ds
        .commit(None, vec![file("a.jpg", "image/jpeg", None)])
        .await;

    let revoked = ds.grant(None, random_request_metadata()).await;
    ds.sign(&revoked, head, &["a.jpg"], random_request_metadata())
        .await
        .unwrap();
    ds.revoke(revoked.grant_id, random_request_metadata())
        .await
        .unwrap();
    refused(
        ds.sign(&revoked, head, &["a.jpg"], random_request_metadata())
            .await,
        StatusCode::FORBIDDEN,
        "DatasetAccessGrantRevoked",
    );
    ds.revoke(revoked.grant_id, random_request_metadata())
        .await
        .expect("revoking twice is not an error");

    let expired = ds.grant(None, random_request_metadata()).await;
    sqlx::query(
        "UPDATE dataset_access_grant SET expires_at = now() - interval '1 second'
         WHERE grant_id = $1",
    )
    .bind(Uuid::from(expired.grant_id))
    .execute(&pool)
    .await
    .unwrap();
    refused(
        ds.sign(&expired, head, &["a.jpg"], random_request_metadata())
            .await,
        StatusCode::FORBIDDEN,
        "DatasetAccessGrantExpired",
    );
}

/// A grant serves only the caller that obtained it, and only through the
/// snapshot it covers. Someone else's grant reads as absent.
#[sqlx::test]
async fn test_a_grant_serves_its_holder_and_snapshot_only(pool: PgPool) {
    let ds = make_dataset(pool, AllowAllAuthorizer::default()).await;
    let head = ds
        .commit(None, vec![file("a.jpg", "image/jpeg", None)])
        .await;
    let grant = ds.grant(None, as_user("alice")).await;

    ds.sign(&grant, head, &["a.jpg"], as_user("alice"))
        .await
        .unwrap();
    for other in [as_user("bob"), random_request_metadata()] {
        refused(
            ds.sign(&grant, head, &["a.jpg"], other).await,
            StatusCode::NOT_FOUND,
            "DatasetAccessGrantNotFound",
        );
    }
    refused(
        ds.sign(
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
    let ds = make_dataset(pool, AllowAllAuthorizer::default()).await;
    let head = ds
        .commit(None, vec![file("a.jpg", "image/jpeg", None)])
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
    let grant = ds.grant(None, acting_as(role.clone())).await;

    let renamed = Role {
        name: "renamed".to_string(),
        ..role
    };
    ds.sign(&grant, head, &["a.jpg"], acting_as(renamed))
        .await
        .expect("a renamed role keeps its grants");
    for other in [as_user("alice"), acting_as(Role::new_random())] {
        refused(
            ds.sign(&grant, head, &["a.jpg"], other).await,
            StatusCode::NOT_FOUND,
            "DatasetAccessGrantNotFound",
        );
    }
}

/// Each key asked for gets a URL, in the order asked, a repeated key included.
#[sqlx::test]
async fn test_signing_answers_each_key_in_order(pool: PgPool) {
    let ds = make_dataset(pool, AllowAllAuthorizer::default()).await;
    let head = ds
        .commit(
            None,
            vec![
                file("a.jpg", "image/jpeg", None),
                file("b.jpg", "image/jpeg", None),
            ],
        )
        .await;
    let grant = ds.grant(None, random_request_metadata()).await;

    let signed = ds
        .sign(
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
    let ds = make_dataset(pool, AllowAllAuthorizer::default()).await;
    let pinned = CommitFile {
        version_id: Some("v7".to_string()),
        ..file("pinned.jpg", "image/jpeg", None)
    };
    let head = ds
        .commit(None, vec![pinned, file("current.jpg", "image/jpeg", None)])
        .await;
    let grant = ds.grant(None, random_request_metadata()).await;

    let signed = ds
        .sign(
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
    let ds = make_dataset(pool, AllowAllAuthorizer::default()).await;
    let head = ds
        .commit(None, vec![file("a.jpg", "image/jpeg", None)])
        .await;
    let grant = ds.grant(None, random_request_metadata()).await;
    let too_many: Vec<String> = (0..=grant.max_keys_per_request)
        .map(|i| format!("{i}.jpg"))
        .collect();
    let too_many: Vec<&str> = too_many.iter().map(String::as_str).collect();
    for keys in [&[][..], &too_many[..]] {
        refused(
            ds.sign(&grant, head, keys, random_request_metadata()).await,
            StatusCode::BAD_REQUEST,
            "InvalidSignBatch",
        );
    }
    // The maximum itself is a valid batch: it reaches the scope check, which
    // refuses the keys the snapshot does not hold.
    refused(
        ds.sign(&grant, head, &too_many[1..], random_request_metadata())
            .await,
        StatusCode::FORBIDDEN,
        "DatasetFilesOutsideGrant",
    );
}

/// The action names of the authorization records a dataset's requests leave.
#[derive(Debug)]
struct AuthzCapture(tokio::sync::mpsc::UnboundedSender<String>);

impl std::fmt::Display for AuthzCapture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AuthzCapture")
    }
}

#[async_trait::async_trait]
impl EventListener for AuthzCapture {
    async fn authorization_succeeded(
        &self,
        event: AuthorizationSucceededEvent,
    ) -> anyhow::Result<()> {
        for action in event.actions.iter() {
            let _ = self.0.send(action.action_name.to_string());
        }
        Ok(())
    }
}

/// A holder revoking its own grant only needs to see the dataset, and its record
/// says so; revoking another caller's records the action it took.
#[sqlx::test]
async fn test_a_revoke_records_the_action_it_took(pool: PgPool) {
    let ds = make_dataset(pool, AllowAllAuthorizer::default()).await;
    ds.commit(None, vec![file("a.jpg", "image/jpeg", None)])
        .await;
    let own = ds.grant(None, as_user("alice")).await;
    let anothers = ds.grant(None, as_user("alice")).await;
    let (sender, mut actions) = tokio::sync::mpsc::unbounded_channel();
    ds.ctx
        .v1_state
        .events
        .append(Arc::new(AuthzCapture(sender)))
        .await;
    // Events go out on their own tasks, so a grant's `read_data` record can still
    // arrive after the listener is in place: only the revokes' records count.
    let mut next_action = async || loop {
        let action = tokio::time::timeout(std::time::Duration::from_secs(5), actions.recv())
            .await
            .expect("the revoke is recorded")
            .unwrap();
        if action != "read_data" {
            break action;
        }
    };

    ds.revoke(own.grant_id, as_user("alice")).await.unwrap();
    assert_eq!(next_action().await, "get_metadata");
    ds.revoke(anothers.grant_id, as_user("bob")).await.unwrap();
    assert_eq!(next_action().await, "revoke_access_grants");
}

/// A ref with no commits has no files to grant.
#[sqlx::test]
async fn test_an_empty_ref_grants_nothing(pool: PgPool) {
    let ds = make_dataset(pool, AllowAllAuthorizer::default()).await;
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
    let ds = make_dataset(pool, authorizer.clone()).await;
    let head = ds
        .commit(None, vec![file("a.jpg", "image/jpeg", None)])
        .await;

    let revoked_by_bob = ds.grant(None, as_user("alice")).await;
    ds.revoke(revoked_by_bob.grant_id, as_user("bob"))
        .await
        .expect("the action lets another caller revoke it");
    refused(
        ds.sign(&revoked_by_bob, head, &["a.jpg"], as_user("alice"))
            .await,
        StatusCode::FORBIDDEN,
        "DatasetAccessGrantRevoked",
    );

    authorizer.block_action("dataset:RevokeAccessGrants");
    let grant = ds.grant(None, as_user("alice")).await;
    refused(
        ds.revoke(grant.grant_id, as_user("bob")).await,
        StatusCode::FORBIDDEN,
        "DatasetActionForbidden",
    );
    ds.revoke(grant.grant_id, as_user("alice"))
        .await
        .expect("a grant's holder revokes it without the action");
}

/// The file listing says how to read: the in-memory profile vends nothing, so
/// signed URLs.
#[sqlx::test]
async fn test_the_listing_says_how_to_read(pool: PgPool) {
    let ds = make_dataset(pool, AllowAllAuthorizer::default()).await;
    ds.commit(None, vec![file("a.jpg", "image/jpeg", None)])
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
    let ds = make_dataset(pool, AllowAllAuthorizer::default()).await;
    let head = ds
        .commit(
            None,
            vec![
                CommitFile {
                    etag: Some("\"9b2cf535f27731c974343645a3985328\"".to_string()),
                    ..file("a.jpg", "image/jpeg", None)
                },
                file("b.jpg", "image/jpeg", None),
            ],
        )
        .await;
    let grant = ds.grant(None, random_request_metadata()).await;

    let signed = ds
        .sign_files(&grant, head, &["a.jpg", "b.jpg"], random_request_metadata())
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
    let ds = make_dataset(pool, AllowAllAuthorizer::default()).await;
    for queued in [false, true] {
        refused(
            CatalogServer::import_dataset(
                DatasetParameters {
                    prefix: Some(Prefix(ds.prefix.clone())),
                    namespace: NamespaceIdent::new(ds.ns.clone()),
                    dataset_name: DS.to_string(),
                },
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
        use lakekeeper_io::LakekeeperStorage as _;

        use super::super::*;

        /// A signed URL serves the committed file's bytes over plain HTTP, ranges
        /// included, with a key that needs encoding.
        #[sqlx::test]
        async fn test_a_signed_url_serves_the_committed_file(pool: PgPool) {
            let (profile, credential) = s3_compatible_profile();
            let (ctx, warehouse) = setup(
                pool,
                profile.clone(),
                Some(credential.clone()),
                AllowAllAuthorizer::default(),
                TabularDeleteProfile::Hard {},
                None,
                1,
                None,
            )
            .await;
            let prefix = warehouse.warehouse_id.to_string();
            let ns = format!("ns_{}", Uuid::now_v7());
            create_ns(ctx.clone(), prefix.clone(), ns.clone()).await;
            let created = create_dataset(ctx.clone(), prefix.clone(), ns.clone(), DS)
                .await
                .unwrap();
            let ds = Dataset {
                ctx,
                prefix,
                ns,
                location: created.dataset.location,
            };

            let data: Vec<u8> = (0..=255u8).cycle().take(4096).collect();
            let storage = profile.file_io(Some(&credential)).await.unwrap();
            storage
                .write(
                    &format!("{}/a photo+1.jpg", ds.location.trim_end_matches('/')),
                    bytes::Bytes::from(data.clone()),
                )
                .await
                .unwrap();
            let head = ds
                .commit(None, vec![file("a photo+1.jpg", "image/jpeg", None)])
                .await;
            let grant = ds.grant(None, random_request_metadata()).await;
            let signed = ds
                .sign(&grant, head, &["a photo+1.jpg"], random_request_metadata())
                .await
                .unwrap();
            let url = &signed[0].1;

            let client = reqwest::Client::new();
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
                .header(reqwest::header::RANGE, "bytes=100-199")
                .send()
                .await
                .unwrap();
            assert_eq!(range.status(), reqwest::StatusCode::PARTIAL_CONTENT);
            assert_eq!(range.bytes().await.unwrap().as_ref(), &data[100..200]);
        }
        /// An import records the etag S3 lists, and a file that records a version is
        /// signed at it: `versionId` goes into the URL, and into its signature.
        #[sqlx::test]
        async fn test_s3_records_etags_and_signs_versions(pool: PgPool) {
            let (profile, credential) = s3_compatible_profile();
            let (ctx, warehouse) = setup(
                pool,
                profile.clone(),
                Some(credential.clone()),
                AllowAllAuthorizer::default(),
                TabularDeleteProfile::Hard {},
                None,
                1,
                None,
            )
            .await;
            let prefix = warehouse.warehouse_id.to_string();
            let ns = format!("ns_{}", Uuid::now_v7());
            create_ns(ctx.clone(), prefix.clone(), ns.clone()).await;
            let created = create_dataset(ctx.clone(), prefix.clone(), ns.clone(), DS)
                .await
                .unwrap();
            let ds = Dataset {
                ctx,
                prefix,
                ns,
                location: created.dataset.location,
            };
            let storage = profile.file_io(Some(&credential)).await.unwrap();
            storage
                .write(
                    &format!("{}/listed.bin", ds.location.trim_end_matches('/')),
                    bytes::Bytes::from_static(b"listed"),
                )
                .await
                .unwrap();

            let imported = CatalogServer::import_dataset(
                DatasetParameters {
                    prefix: Some(Prefix(ds.prefix.clone())),
                    namespace: NamespaceIdent::new(ds.ns.clone()),
                    dataset_name: DS.to_string(),
                },
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
                    vec![CommitFile {
                        version_id: Some("3HL4kqtJlcpXroDTDmJ".to_string()),
                        ..file("pinned.bin", "application/octet-stream", None)
                    }],
                )
                .await;
            let grant = ds.grant(None, random_request_metadata()).await;
            let signed = ds
                .sign(
                    &grant,
                    head,
                    &["pinned.bin", "listed.bin"],
                    random_request_metadata(),
                )
                .await
                .unwrap();
            let pinned = reqwest::Url::parse(&signed[0].1).unwrap();
            assert!(
                pinned
                    .query_pairs()
                    .any(|(k, v)| k == "versionId" && v == "3HL4kqtJlcpXroDTDmJ"),
                "{pinned}"
            );
            let current = reqwest::Url::parse(&signed[1].1).unwrap();
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

        /// A pinned file is judged by its version: as recorded while S3 still holds it,
        /// though the key was deleted, and missing once the version itself is gone.
        #[sqlx::test]
        async fn test_a_pinned_file_is_judged_by_its_version(pool: PgPool) {
            let (profile, credential) = versioned_s3_profile().await;
            let (ctx, warehouse) = setup(
                pool,
                profile.clone(),
                Some(credential.clone()),
                AllowAllAuthorizer::default(),
                TabularDeleteProfile::Hard {},
                None,
                1,
                None,
            )
            .await;
            let prefix = warehouse.warehouse_id.to_string();
            let ns = format!("ns_{}", Uuid::now_v7());
            create_ns(ctx.clone(), prefix.clone(), ns.clone()).await;
            let created = create_dataset(ctx.clone(), prefix.clone(), ns.clone(), DS)
                .await
                .unwrap();
            let ds = Dataset {
                ctx,
                prefix,
                ns,
                location: created.dataset.location,
            };
            let ds_params = || DatasetParameters {
                prefix: Some(Prefix(ds.prefix.clone())),
                namespace: NamespaceIdent::new(ds.ns.clone()),
                dataset_name: DS.to_string(),
            };
            let import = |mode: ImportMode, check: bool| {
                CatalogServer::import_dataset(
                    ds_params(),
                    ImportDatasetRequest {
                        mode: Some(mode),
                        record_versions: Some(true),
                        check_materialization: Some(check),
                        ..Default::default()
                    },
                    ds.ctx.clone(),
                    random_request_metadata(),
                )
            };
            let StorageBackend::S3(storage) = profile.file_io(Some(&credential)).await.unwrap()
            else {
                panic!("S3 storage")
            };
            let path = format!("{}/a.bin", ds.location.trim_end_matches('/'));
            storage
                .write(&path, bytes::Bytes::from_static(b"pinned"))
                .await
                .unwrap();
            let first = import(ImportMode::AddOnly, false)
                .await
                .unwrap()
                .snapshot_id
                .unwrap();
            CatalogServer::create_dataset_ref(
                ds_params(),
                CreateDatasetRefRequest {
                    name: "v1".to_string(),
                    typ: DatasetRefType::Tag,
                    source: DatasetRefSource::Snapshot { snapshot_id: first },
                },
                ds.ctx.clone(),
                random_request_metadata(),
            )
            .await
            .unwrap();
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
            storage.delete(&path).await.unwrap();
            let held = import(ImportMode::Sync, true).await.unwrap();
            assert_eq!(held.removed, 1, "main drops the key");
            assert_eq!(
                held.materialization.unwrap().degraded_snapshots,
                0,
                "the tag's version is still stored"
            );

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
            let gone = import(ImportMode::AddOnly, true).await.unwrap();
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
            let mut pages = storage.list_current_versions(base, Some(2)).unwrap();
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
            let (profile, credential) = versioned_s3_profile().await;
            let (ctx, warehouse) = setup(
                pool,
                profile.clone(),
                Some(credential.clone()),
                AllowAllAuthorizer::default(),
                TabularDeleteProfile::Hard {},
                None,
                1,
                None,
            )
            .await;
            let prefix = warehouse.warehouse_id.to_string();
            let ns = format!("ns_{}", Uuid::now_v7());
            create_ns(ctx.clone(), prefix.clone(), ns.clone()).await;
            let created = create_dataset(ctx.clone(), prefix.clone(), ns.clone(), DS)
                .await
                .unwrap();
            let ds = Dataset {
                ctx,
                prefix,
                ns,
                location: created.dataset.location,
            };
            let ds_params = || DatasetParameters {
                prefix: Some(Prefix(ds.prefix.clone())),
                namespace: NamespaceIdent::new(ds.ns.clone()),
                dataset_name: DS.to_string(),
            };
            let import = |mode: ImportMode, check: bool| {
                CatalogServer::import_dataset(
                    ds_params(),
                    ImportDatasetRequest {
                        mode: Some(mode),
                        record_versions: Some(true),
                        check_materialization: Some(check),
                        ..Default::default()
                    },
                    ds.ctx.clone(),
                    random_request_metadata(),
                )
            };
            let storage = profile.file_io(Some(&credential)).await.unwrap();
            let key = format!("{}/a.bin", ds.location.trim_end_matches('/'));
            storage
                .write(&key, bytes::Bytes::from_static(b"first"))
                .await
                .unwrap();

            let first = import(ImportMode::AddOnly, false)
                .await
                .unwrap()
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
            let grant = ds.grant(None, random_request_metadata()).await;
            CatalogServer::create_dataset_ref(
                ds_params(),
                CreateDatasetRefRequest {
                    name: "v1".to_string(),
                    typ: DatasetRefType::Tag,
                    source: DatasetRefSource::Snapshot { snapshot_id: first },
                },
                ds.ctx.clone(),
                random_request_metadata(),
            )
            .await
            .unwrap();

            storage
                .write(&key, bytes::Bytes::from_static(b"second, and longer"))
                .await
                .unwrap();
            let synced = import(ImportMode::Sync, true).await.unwrap();
            assert_eq!(synced.modified, 1);
            assert_eq!(
                synced.materialization.unwrap().degraded_snapshots,
                0,
                "the tag's file is pinned to its version"
            );

            let signed = ds
                .sign_files(&grant, first, &["a.bin"], random_request_metadata())
                .await
                .unwrap();
            let response = reqwest::get(&signed[0].url).await.unwrap();
            assert_eq!(response.status(), reqwest::StatusCode::OK);
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
