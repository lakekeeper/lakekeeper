//! A dataset to test against, in a namespace of a warehouse of its own.

use bytes::Bytes;
use iceberg::NamespaceIdent;
use lakekeeper::{
    WarehouseId,
    api::{
        ApiContext, Result,
        data::v1::datasets::{
            CommitDatasetRequest, CommitFile, CreateDatasetRequest, DatasetParameters,
            DatasetRefParameters, DatasetService as _, DatasetSnapshotParameters,
            ListDatasetFilesQuery,
        },
        iceberg::{types::Prefix, v1::namespace::NamespaceParameters},
        management::v1::warehouse::TabularDeleteProfile,
    },
    server::CatalogServer,
    service::{
        ArcProjectId, CatalogDatasetOps, CatalogStore, DatasetConstraints, DatasetId,
        DatasetOwnership, DatasetSnapshotId, State, Transaction,
        authz::{AllowAllAuthorizer, Authorizer},
        storage::{StorageCredential, StorageProfile},
    },
};
use lakekeeper_io::{LakekeeperStorage as _, StorageBackend};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;
use uuid::Uuid;

use crate::{create_ns, memory_io_profile, random_request_metadata, setup};

pub type TestApiContext<A = AllowAllAuthorizer> =
    ApiContext<State<A, PostgresBackend, SecretsState>>;

/// The dataset [`TestDataset::managed`] and [`TestDataset::imported`] create.
pub const DATASET: &str = "images";

/// A warehouse with one namespace in it.
#[derive(Clone)]
pub struct TestNamespace<A: Authorizer = AllowAllAuthorizer> {
    pub ctx: TestApiContext<A>,
    pub pool: PgPool,
    pub warehouse_id: WarehouseId,
    pub project_id: ArcProjectId,
    pub prefix: String,
    pub name: String,
    pub storage_profile: StorageProfile,
    pub storage_credential: Option<StorageCredential>,
}

impl TestNamespace {
    /// On the in-memory profile, with every request allowed.
    pub async fn new(pool: PgPool) -> Self {
        Self::with_authorizer(pool, AllowAllAuthorizer::default()).await
    }
}

impl<A: Authorizer> TestNamespace<A> {
    /// On the in-memory profile.
    pub async fn with_authorizer(pool: PgPool, authorizer: A) -> Self {
        Self::on_storage(pool, authorizer, memory_io_profile(), None).await
    }

    pub async fn on_storage(
        pool: PgPool,
        authorizer: A,
        storage_profile: StorageProfile,
        storage_credential: Option<StorageCredential>,
    ) -> Self {
        let (ctx, warehouse) = setup(
            pool.clone(),
            storage_profile.clone(),
            storage_credential.clone(),
            authorizer,
            TabularDeleteProfile::Hard {},
            None,
            1,
            None,
        )
        .await;
        let prefix = warehouse.warehouse_id.to_string();
        let name = format!("ns_{}", Uuid::now_v7());
        create_ns(ctx.clone(), prefix.clone(), name.clone()).await;
        Self {
            ctx,
            pool,
            warehouse_id: warehouse.warehouse_id,
            project_id: warehouse.project_id,
            prefix,
            name,
            storage_profile,
            storage_credential,
        }
    }

    /// The warehouse's base location, without a trailing `/`. A dataset location
    /// must lie under it.
    #[must_use]
    pub fn base_location(&self) -> String {
        self.storage_profile
            .base_location()
            .unwrap()
            .to_string()
            .trim_end_matches('/')
            .to_string()
    }

    #[must_use]
    pub fn params(&self) -> NamespaceParameters {
        NamespaceParameters {
            prefix: Some(Prefix(self.prefix.clone())),
            namespace: NamespaceIdent::new(self.name.clone()),
        }
    }

    #[must_use]
    pub fn dataset_params(&self, name: &str) -> DatasetParameters {
        DatasetParameters {
            prefix: Some(Prefix(self.prefix.clone())),
            namespace: NamespaceIdent::new(self.name.clone()),
            dataset_name: name.to_string(),
        }
    }

    /// Creates the dataset `name`. A managed dataset gets the location Lakekeeper
    /// allocates; an imported one borrows `<base location>/<namespace>/<name>`.
    pub async fn create_dataset(
        &self,
        name: &str,
        ownership: DatasetOwnership,
        constraints: Option<DatasetConstraints>,
    ) -> TestDataset<A> {
        let location = match ownership {
            DatasetOwnership::Managed => None,
            DatasetOwnership::Imported => {
                Some(format!("{}/{}/{name}", self.base_location(), self.name))
            }
        };
        let created = CatalogServer::create_dataset(
            self.params(),
            CreateDatasetRequest {
                name: name.to_string(),
                location,
                constraints,
            },
            self.ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap()
        .dataset;
        TestDataset {
            ctx: self.ctx.clone(),
            pool: self.pool.clone(),
            warehouse_id: self.warehouse_id,
            project_id: self.project_id.clone(),
            prefix: self.prefix.clone(),
            namespace: self.name.clone(),
            name: name.to_string(),
            id: created.id,
            location: created.location,
            storage_profile: self.storage_profile.clone(),
            storage_credential: self.storage_credential.clone(),
        }
    }
}

/// A dataset, alone in its namespace unless a test adds another.
#[derive(Clone)]
pub struct TestDataset<A: Authorizer = AllowAllAuthorizer> {
    pub ctx: TestApiContext<A>,
    pub pool: PgPool,
    pub warehouse_id: WarehouseId,
    pub project_id: ArcProjectId,
    pub prefix: String,
    pub namespace: String,
    pub name: String,
    pub id: DatasetId,
    pub location: String,
    pub storage_profile: StorageProfile,
    pub storage_credential: Option<StorageCredential>,
}

impl TestDataset {
    /// [`DATASET`] as a managed dataset: its files arrive by commit.
    pub async fn managed(pool: PgPool) -> Self {
        TestNamespace::new(pool)
            .await
            .create_dataset(DATASET, DatasetOwnership::Managed, None)
            .await
    }

    /// [`DATASET`] as an imported dataset: an import registers the objects stored
    /// under its location.
    pub async fn imported(pool: PgPool) -> Self {
        TestNamespace::new(pool)
            .await
            .create_dataset(DATASET, DatasetOwnership::Imported, None)
            .await
    }
}

impl<A: Authorizer> TestDataset<A> {
    #[must_use]
    pub fn namespace_params(&self) -> NamespaceParameters {
        NamespaceParameters {
            prefix: Some(Prefix(self.prefix.clone())),
            namespace: NamespaceIdent::new(self.namespace.clone()),
        }
    }

    #[must_use]
    pub fn params(&self) -> DatasetParameters {
        DatasetParameters {
            prefix: Some(Prefix(self.prefix.clone())),
            namespace: NamespaceIdent::new(self.namespace.clone()),
            dataset_name: self.name.clone(),
        }
    }

    #[must_use]
    pub fn ref_params(&self, ref_name: &str) -> DatasetRefParameters {
        DatasetRefParameters {
            prefix: Some(Prefix(self.prefix.clone())),
            namespace: NamespaceIdent::new(self.namespace.clone()),
            dataset_name: self.name.clone(),
            ref_name: ref_name.to_string(),
        }
    }

    #[must_use]
    pub fn snapshot_params(&self, snapshot_id: DatasetSnapshotId) -> DatasetSnapshotParameters {
        DatasetSnapshotParameters {
            prefix: Some(Prefix(self.prefix.clone())),
            namespace: NamespaceIdent::new(self.namespace.clone()),
            dataset_name: self.name.clone(),
            snapshot_id,
        }
    }

    /// Commits to `main` on top of `parent`, and returns the new head.
    ///
    /// # Panics
    /// If the commit is refused.
    pub async fn commit(
        &self,
        parent: Option<DatasetSnapshotId>,
        added: Vec<CommitFile>,
        removed: &[&str],
    ) -> DatasetSnapshotId {
        self.commit_to("main", parent, added, removed).await
    }

    /// [`Self::commit`], to `branch`.
    ///
    /// # Panics
    /// If the commit is refused.
    pub async fn commit_to(
        &self,
        branch: &str,
        parent: Option<DatasetSnapshotId>,
        added: Vec<CommitFile>,
        removed: &[&str],
    ) -> DatasetSnapshotId {
        self.try_commit(branch, parent, added, removed)
            .await
            .unwrap_or_else(|e| panic!("the commit to {branch} is refused: {e:?}"))
    }

    /// [`Self::commit_to`], returning a refusal.
    pub async fn try_commit(
        &self,
        branch: &str,
        parent: Option<DatasetSnapshotId>,
        added: Vec<CommitFile>,
        removed: &[&str],
    ) -> Result<DatasetSnapshotId> {
        CatalogServer::commit_dataset(
            self.ref_params(branch),
            CommitDatasetRequest {
                parent_snapshot_id: parent,
                added,
                removed: removed.iter().map(ToString::to_string).collect(),
                summary: None,
                on_constraint_violation: None,
            },
            self.ctx.clone(),
            random_request_metadata(),
        )
        .await
        .map(|committed| committed.snapshot_id)
    }

    /// The snapshot `ref_name` points at, `None` before its first commit.
    pub async fn head(&self, ref_name: &str) -> Option<DatasetSnapshotId> {
        CatalogServer::list_dataset_refs(self.params(), self.ctx.clone(), random_request_metadata())
            .await
            .unwrap()
            .refs
            .into_iter()
            .find(|r| r.name == ref_name)
            .and_then(|r| r.snapshot_id)
    }

    /// The keys of the first page of `ref_name`'s files, in the order listed.
    pub async fn file_keys(&self, ref_name: &str) -> Vec<String> {
        CatalogServer::list_dataset_files(
            self.ref_params(ref_name),
            ListDatasetFilesQuery::default(),
            self.ctx.clone(),
            random_request_metadata(),
        )
        .await
        .expect("files list")
        .files
        .into_iter()
        .map(|f| f.logical_key)
        .collect()
    }

    /// Folds `branch` into a checkpoint, as the checkpoint worker does. Returns the
    /// snapshot folded, `None` when none was due.
    pub async fn checkpoint(&self, branch: &str) -> Option<DatasetSnapshotId> {
        let mut t = self.begin_write().await;
        let folded = PostgresBackend::checkpoint_dataset_branch(
            self.warehouse_id,
            self.id,
            branch,
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();
        folded
    }

    pub async fn begin_write(&self) -> <PostgresBackend as CatalogStore>::Transaction {
        <PostgresBackend as CatalogStore>::Transaction::begin_write(
            self.ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap()
    }

    #[must_use]
    pub fn path(&self, key: &str) -> String {
        format!("{}/{key}", self.location.trim_end_matches('/'))
    }

    /// The warehouse's storage. In-memory objects live in a thread-local store,
    /// which every in-memory profile on the thread reads.
    pub async fn storage(&self) -> StorageBackend {
        self.storage_profile
            .file_io(self.storage_credential.as_ref())
            .await
            .unwrap()
    }

    pub async fn write(&self, key: &str, bytes: impl AsRef<[u8]>) {
        self.storage()
            .await
            .write(&self.path(key), Bytes::copy_from_slice(bytes.as_ref()))
            .await
            .unwrap();
    }

    pub async fn delete(&self, key: &str) {
        self.storage().await.delete(&self.path(key)).await.unwrap();
    }
}

/// A file to commit under `key`: one byte, stored at `key`, nothing else recorded.
#[must_use]
pub fn file(key: &str) -> CommitFile {
    CommitFile {
        logical_key: key.to_string(),
        physical_path: None,
        etag: None,
        size: Some(1),
        content_type: None,
        checksum: None,
        version_id: None,
        last_modified: None,
    }
}

/// Records more of a [`file`].
pub trait CommitFileExt {
    #[must_use]
    fn etag(self, etag: &str) -> Self;
    #[must_use]
    fn content_type(self, content_type: &str) -> Self;
    #[must_use]
    fn size(self, size: i64) -> Self;
    #[must_use]
    fn version_id(self, version_id: &str) -> Self;
    /// Where the file's bytes are: relative to the dataset's location, or a URI.
    #[must_use]
    fn at(self, physical_path: impl Into<String>) -> Self;
}

impl CommitFileExt for CommitFile {
    fn etag(self, etag: &str) -> Self {
        Self {
            etag: Some(etag.to_string()),
            ..self
        }
    }

    fn content_type(self, content_type: &str) -> Self {
        Self {
            content_type: Some(content_type.to_string()),
            ..self
        }
    }

    fn size(self, size: i64) -> Self {
        Self {
            size: Some(size),
            ..self
        }
    }

    fn version_id(self, version_id: &str) -> Self {
        Self {
            version_id: Some(version_id.to_string()),
            ..self
        }
    }

    fn at(self, physical_path: impl Into<String>) -> Self {
        Self {
            physical_path: Some(physical_path.into()),
            ..self
        }
    }
}
