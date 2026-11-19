use std::collections::HashMap;

use futures::{StreamExt, TryStreamExt};
use iceberg::spec::{DataContentType, TableMetadata, TableProperties};
use lakekeeper_io::{ErrorKind, LakekeeperStorage, Location};

use super::{
    MeasureOutcome, Measurement, NotMeasurableReason, Sizes,
    findings::{LengthMismatch, manifest_length_mismatch},
};
use crate::operations::{
    error::{CancelledError, InternalError, ReadError, RetryableErrorKind},
    manifest_decode::{ManifestEntryStats, visit_manifest_stats},
    manifest_reader::{
        LakekeeperIoProvider, ManifestReader, ManifestReaderError, manifest_refs_of_list,
        with_storage_retries,
    },
    progress_tracker::ProgressTracker,
};

/// Error that ends a measurement without an outcome.
#[derive(Debug, thiserror::Error)]
pub enum MeasureError {
    /// The progress tracker was cancelled.
    #[error("The measurement was cancelled")]
    Cancelled,
    /// Reading a file failed after the reader's retries. A later attempt may succeed.
    #[error("Failed to read table metadata: {0}")]
    Read(ReadError),
}

/// Measure the current snapshot of a table from its metadata.
///
/// Reads the size of `metadata_location`, the current snapshot's manifest list and its manifests
/// through `reader`. Never lists storage. An encrypted table, an alive file without a size, a
/// file that can't be parsed and a file that doesn't exist end as
/// [`MeasureOutcome::NotMeasurable`].
///
/// # Errors
/// [`MeasureError::Cancelled`] once `tracker` is cancelled; [`MeasureError::Read`] for storage
/// errors that remain after the reader's retries.
pub async fn measure<P: LakekeeperIoProvider + Clone + 'static>(
    metadata: &TableMetadata,
    metadata_location: &Location,
    reader: &ManifestReader<P>,
    tracker: &ProgressTracker,
) -> Result<MeasureOutcome, MeasureError> {
    if is_encrypted(metadata) {
        return Ok(MeasureOutcome::NotMeasurable(
            NotMeasurableReason::Encrypted,
        ));
    }
    match scan(metadata, metadata_location, reader, tracker).await {
        Ok(outcome) => Ok(outcome),
        Err(ManifestReaderError::Cancelled(_)) => Err(MeasureError::Cancelled),
        Err(ManifestReaderError::ReadFailed(ReadError::ParseError(e))) => Ok(
            MeasureOutcome::NotMeasurable(NotMeasurableReason::MetadataUnreadable { path: e.path }),
        ),
        // The reader names the file it read as the error's location.
        Err(ManifestReaderError::ReadFailed(ReadError::IOError(e)))
            if e.kind() == ErrorKind::NotFound =>
        {
            match e.location() {
                Some(path) => Ok(MeasureOutcome::NotMeasurable(
                    NotMeasurableReason::MetadataMissing {
                        path: path.to_string(),
                    },
                )),
                None => Err(MeasureError::Read(e.into())),
            }
        }
        Err(ManifestReaderError::ReadFailed(e)) => Err(MeasureError::Read(e)),
    }
}

/// Whether the table's files are encrypted: `encryption.key-id` is set, or the metadata holds
/// encryption keys.
fn is_encrypted(metadata: &TableMetadata) -> bool {
    metadata
        .properties()
        .get(TableProperties::PROPERTY_ENCRYPTION_KEY_ID)
        .is_some_and(|key_id| !key_id.trim().is_empty())
        || metadata.encryption_keys_iter().len() > 0
}

async fn scan<P: LakekeeperIoProvider + Clone + 'static>(
    metadata: &TableMetadata,
    metadata_location: &Location,
    reader: &ManifestReader<P>,
    tracker: &ProgressTracker,
) -> Result<MeasureOutcome, ManifestReaderError> {
    let Some(metadata_file_bytes) =
        file_size(reader.table_operations(), metadata_location, tracker).await?
    else {
        return Ok(MeasureOutcome::NotMeasurable(
            NotMeasurableReason::MissingFileSize {
                path: metadata_location.to_string(),
            },
        ));
    };
    let metadata_file_bytes = to_i64(metadata_file_bytes);

    let Some(snapshot) = metadata.current_snapshot() else {
        return Ok(MeasureOutcome::Measured(Measurement {
            sizes: Sizes {
                live_metadata_bytes: metadata_file_bytes,
                ..Sizes::default()
            },
            findings: Vec::new(),
            snapshot_id: None,
            manifests_read: 0,
        }));
    };
    let snapshot_id = snapshot.snapshot_id();

    let lists: Vec<_> = reader
        .visit_manifest_lists(
            vec![snapshot.clone()],
            tracker.clone(),
            manifest_refs_of_list,
        )
        .try_collect()
        .await?;
    let Some((_, list)) = lists.into_iter().next() else {
        return Err(ReadError::from(InternalError::new(
            format!("The manifest list of snapshot {snapshot_id} was not read"),
            RetryableErrorKind::Permanent,
        ))
        .into());
    };
    let manifests = list.value;
    let manifests_read = u64::try_from(manifests.len()).unwrap_or(u64::MAX);

    let mut totals = ManifestTotals::default();
    let mut manifest_bytes = 0_u64;
    let mut length_mismatches = Vec::new();
    let mut reads =
        std::pin::pin!(
            reader.visit_manifests(manifests, tracker.clone(), |bytes, manifest| {
                let mut totals = ManifestTotals::default();
                visit_manifest_stats(bytes, &manifest.path, |entry| {
                    totals.add(&entry);
                    Ok(())
                })?;
                Ok(totals)
            },)
        );
    while let Some(read) = reads.next().await {
        let (manifest, read) = read?;
        manifest_bytes = manifest_bytes.saturating_add(read.actual_length);
        length_mismatches.extend(LengthMismatch::of(
            &manifest.path,
            manifest.length,
            read.actual_length,
        ));
        totals.merge(read.value);
    }

    if let Some(path) = totals.missing_file_size {
        return Ok(MeasureOutcome::NotMeasurable(
            NotMeasurableReason::MissingFileSize { path },
        ));
    }

    let statistics_bytes = metadata
        .statistics_for_snapshot(snapshot_id)
        .map_or(0, |file| file.file_size_in_bytes)
        .saturating_add(
            metadata
                .partition_statistics_for_snapshot(snapshot_id)
                .map_or(0, |file| file.file_size_in_bytes),
        );
    let live_metadata_bytes = metadata_file_bytes
        .saturating_add(to_i64(list.actual_length))
        .saturating_add(to_i64(manifest_bytes))
        .saturating_add(statistics_bytes);

    Ok(MeasureOutcome::Measured(Measurement {
        sizes: Sizes {
            live_data_bytes: totals.data_bytes,
            live_delete_bytes: totals.delete_bytes(),
            live_metadata_bytes,
        },
        findings: manifest_length_mismatch(length_mismatches)
            .into_iter()
            .collect(),
        snapshot_id: Some(snapshot_id),
        manifests_read,
    }))
}

/// Size of the file at `location` from its storage metadata, without reading it. `None` if the
/// storage reports no size.
async fn file_size<P: LakekeeperIoProvider>(
    provider: &P,
    location: &Location,
    tracker: &ProgressTracker,
) -> Result<Option<u64>, ManifestReaderError> {
    let path = location.as_str();
    let head = with_storage_retries(
        provider,
        |storage| async move { storage.metadata(path).await },
    );
    tokio::select! {
        biased;
        () = tracker.cancelled() => Err(CancelledError {}.into()),
        info = head => match info {
            Ok(info) => Ok(info.size()),
            Err(lakekeeper_io::ReadError::IOError(e)) => {
                Err(ReadError::from(e.set_location(path)).into())
            }
            Err(e) => Err(ReadError::from(e).into()),
        },
    }
}

fn to_i64(bytes: u64) -> i64 {
    i64::try_from(bytes).unwrap_or(i64::MAX)
}

/// File sizes of the alive entries of one or more manifests.
#[derive(Debug, Default, PartialEq, Eq)]
struct ManifestTotals {
    data_bytes: i64,
    /// Position-delete and equality-delete files.
    delete_file_bytes: i64,
    /// Puffin files that hold deletion vectors, with their sizes. Several vectors share one file,
    /// which counts once.
    deletion_vector_files: HashMap<String, i64>,
    /// The first alive entry without a valid file size.
    missing_file_size: Option<String>,
}

impl ManifestTotals {
    fn add(&mut self, entry: &ManifestEntryStats<'_>) {
        if !entry.alive {
            return;
        }
        let Some(size) = entry.file_size_in_bytes.filter(|size| *size >= 0) else {
            self.missing_file_size
                .get_or_insert_with(|| entry.file_path.to_string());
            return;
        };
        match entry.content {
            DataContentType::Data => self.data_bytes = self.data_bytes.saturating_add(size),
            DataContentType::PositionDeletes if entry.content_offset.is_some() => {
                self.deletion_vector_files
                    .insert(entry.file_path.to_string(), size);
            }
            DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                self.delete_file_bytes = self.delete_file_bytes.saturating_add(size);
            }
        }
    }

    fn merge(&mut self, other: Self) {
        self.data_bytes = self.data_bytes.saturating_add(other.data_bytes);
        self.delete_file_bytes = self
            .delete_file_bytes
            .saturating_add(other.delete_file_bytes);
        self.deletion_vector_files
            .extend(other.deletion_vector_files);
        if self.missing_file_size.is_none() {
            self.missing_file_size = other.missing_file_size;
        }
    }

    fn delete_bytes(&self) -> i64 {
        self.deletion_vector_files
            .values()
            .fold(self.delete_file_bytes, |sum, size| {
                sum.saturating_add(*size)
            })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use iceberg::{
        io::FileIO,
        spec::{
            DataFile, DataFileBuilder, DataFileFormat, FormatVersion, ManifestFile,
            ManifestListWriter, ManifestStatus, ManifestWriterBuilder, NestedField, Operation,
            PartitionSpec, PartitionStatisticsFile, PrimitiveType, Schema, Snapshot, SortOrder,
            StatisticsFile, Struct, Summary, TableMetadataBuilder, Type,
        },
    };
    use lakekeeper_io::memory::MemoryStorage;

    use super::*;
    use crate::service::table_health::{Finding, FindingKind, Severity};

    const METADATA_PATH: &str = "memory://t/metadata/v1.metadata.json";
    const LIST_PATH: &str = "memory://t/metadata/snap-1.avro";
    const DATA_MANIFEST_PATH: &str = "memory://t/metadata/data-m0.avro";
    const DELETE_MANIFEST_PATH: &str = "memory://t/metadata/delete-m0.avro";
    const SNAPSHOT_ID: i64 = 1;
    const METADATA_FILE_BYTES: usize = 1234;

    #[derive(Debug, Clone)]
    struct MemoryIo(MemoryStorage);

    impl LakekeeperIoProvider for MemoryIo {
        type LakekeeperIO = MemoryStorage;

        fn lakekeeper_io(&self) -> impl Future<Output = Self::LakekeeperIO> + Send {
            std::future::ready(self.0.clone())
        }
    }

    fn schema() -> Schema {
        Schema::builder()
            .with_fields([Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap()
    }

    fn spec() -> PartitionSpec {
        PartitionSpec::builder(schema())
            .with_spec_id(0)
            .build()
            .unwrap()
    }

    fn file(content: DataContentType, path: &str, size: u64) -> DataFileBuilder {
        let mut builder = DataFileBuilder::default();
        builder
            .content(content)
            .file_format(DataFileFormat::Parquet)
            .file_path(path.to_string())
            .partition(Struct::empty())
            .record_count(10)
            .file_size_in_bytes(size)
            .partition_spec_id(0);
        if content == DataContentType::EqualityDeletes {
            builder.equality_ids(Some(vec![1]));
        }
        builder
    }

    fn deletion_vector(puffin_path: &str, size: u64, offset: i64) -> DataFile {
        file(DataContentType::PositionDeletes, puffin_path, size)
            .file_format(DataFileFormat::Puffin)
            .referenced_data_file(Some(format!("memory://t/data/{offset}.parquet")))
            .content_offset(Some(offset))
            .content_size_in_bytes(Some(40))
            .build()
            .unwrap()
    }

    /// Copies a file iceberg-rust wrote to `file_io` into `storage`. Returns its bytes.
    async fn copy(file_io: &FileIO, storage: &MemoryStorage, path: &str) -> Vec<u8> {
        let bytes = file_io.new_input(path).unwrap().read().await.unwrap();
        storage.write(path, bytes.clone()).await.unwrap();
        bytes.to_vec()
    }

    /// A data manifest with live data of 1000 + 2000 bytes and a deleted entry of 4000 bytes.
    async fn data_manifest(file_io: &FileIO) -> ManifestFile {
        let mut writer = ManifestWriterBuilder::new(
            file_io.new_output(DATA_MANIFEST_PATH).unwrap(),
            Some(SNAPSHOT_ID),
            Arc::new(schema()),
            spec(),
        )
        .build_v2_data();
        let data = |path, size| file(DataContentType::Data, path, size).build().unwrap();
        writer
            .add_file(data("memory://t/data/a.parquet", 1000), 1)
            .unwrap();
        writer
            .add_existing_file(data("memory://t/data/b.parquet", 2000), 1, 1, Some(1))
            .unwrap();
        writer
            .add_delete_file(data("memory://t/data/c.parquet", 4000), 1, Some(1))
            .unwrap();
        writer.write_manifest_file().await.unwrap()
    }

    /// A delete manifest with live deletes of 100 (position) + 200 (equality) + 50 (one Puffin
    /// file with two deletion vectors) + 70 (a Puffin file with one) bytes, and a deleted
    /// position-delete entry of 400 bytes.
    async fn delete_manifest(file_io: &FileIO) -> ManifestFile {
        let mut writer = ManifestWriterBuilder::new(
            file_io.new_output(DELETE_MANIFEST_PATH).unwrap(),
            Some(SNAPSHOT_ID),
            Arc::new(schema()),
            spec(),
        )
        .build_v3_deletes();
        let deletes = |content, path, size| file(content, path, size).build().unwrap();
        writer
            .add_file(
                deletes(
                    DataContentType::PositionDeletes,
                    "memory://t/data/p.parquet",
                    100,
                ),
                1,
            )
            .unwrap();
        writer
            .add_existing_file(
                deletes(
                    DataContentType::EqualityDeletes,
                    "memory://t/data/e.parquet",
                    200,
                ),
                1,
                1,
                Some(1),
            )
            .unwrap();
        writer
            .add_delete_file(
                deletes(
                    DataContentType::PositionDeletes,
                    "memory://t/data/q.parquet",
                    400,
                ),
                1,
                Some(1),
            )
            .unwrap();
        writer
            .add_file(deletion_vector("memory://t/data/a.puffin", 50, 4), 1)
            .unwrap();
        writer
            .add_file(deletion_vector("memory://t/data/a.puffin", 50, 44), 1)
            .unwrap();
        writer
            .add_file(deletion_vector("memory://t/data/b.puffin", 70, 4), 1)
            .unwrap();
        writer.write_manifest_file().await.unwrap()
    }

    async fn manifest_list(file_io: &FileIO, manifests: Vec<ManifestFile>) {
        let output = file_io
            .new_output(LIST_PATH)
            .unwrap()
            .writer()
            .await
            .unwrap();
        let mut list = ManifestListWriter::v2(output, SNAPSHOT_ID, None, 1);
        list.add_manifests(manifests.into_iter()).unwrap();
        list.close().await.unwrap();
    }

    fn metadata(properties: HashMap<String, String>, with_snapshot: bool) -> TableMetadata {
        let builder = TableMetadataBuilder::new(
            schema(),
            spec(),
            SortOrder::unsorted_order(),
            "memory://t".to_string(),
            FormatVersion::V2,
            properties,
        )
        .unwrap();
        if !with_snapshot {
            return builder.build().unwrap().metadata;
        }
        let snapshot = Snapshot::builder()
            .with_snapshot_id(SNAPSHOT_ID)
            .with_sequence_number(1)
            .with_timestamp_ms(chrono::Utc::now().timestamp_millis())
            .with_schema_id(0)
            .with_manifest_list(LIST_PATH.to_string())
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build();
        builder
            .set_branch_snapshot(snapshot, "main")
            .unwrap()
            .set_statistics(StatisticsFile {
                snapshot_id: SNAPSHOT_ID,
                statistics_path: "memory://t/metadata/stats.puffin".to_string(),
                file_size_in_bytes: 77,
                file_footer_size_in_bytes: 10,
                key_metadata: None,
                blob_metadata: Vec::new(),
            })
            .set_partition_statistics(PartitionStatisticsFile {
                snapshot_id: SNAPSHOT_ID,
                statistics_path: "memory://t/metadata/partition-stats.parquet".to_string(),
                file_size_in_bytes: 33,
            })
            .build()
            .unwrap()
            .metadata
    }

    /// Storage with a metadata.json of [`METADATA_FILE_BYTES`] bytes.
    async fn storage() -> MemoryStorage {
        let storage = MemoryStorage::new_isolated();
        storage
            .write(METADATA_PATH, vec![b'{'; METADATA_FILE_BYTES].into())
            .await
            .unwrap();
        storage
    }

    async fn measured(
        metadata: &TableMetadata,
        storage: MemoryStorage,
        tracker: &ProgressTracker,
    ) -> Result<MeasureOutcome, MeasureError> {
        measure(
            metadata,
            &METADATA_PATH.parse().unwrap(),
            &ManifestReader::new(MemoryIo(storage)),
            tracker,
        )
        .await
    }

    #[tokio::test]
    async fn sums_the_alive_files_and_the_metadata_of_the_current_snapshot() {
        let file_io = FileIO::new_with_memory();
        let storage = storage().await;
        let manifests = vec![
            data_manifest(&file_io).await,
            delete_manifest(&file_io).await,
        ];
        manifest_list(&file_io, manifests).await;
        let manifest_bytes = copy(&file_io, &storage, DATA_MANIFEST_PATH).await.len()
            + copy(&file_io, &storage, DELETE_MANIFEST_PATH).await.len();
        let list_bytes = copy(&file_io, &storage, LIST_PATH).await.len();

        let outcome = measured(
            &metadata(HashMap::new(), true),
            storage,
            &ProgressTracker::new(),
        )
        .await
        .unwrap();

        let expected_metadata_bytes =
            i64::try_from(METADATA_FILE_BYTES + list_bytes + manifest_bytes).unwrap() + 77 + 33;
        assert_eq!(
            outcome,
            MeasureOutcome::Measured(Measurement {
                sizes: Sizes {
                    live_data_bytes: 3000,
                    live_delete_bytes: 420,
                    live_metadata_bytes: expected_metadata_bytes,
                },
                findings: Vec::new(),
                snapshot_id: Some(SNAPSHOT_ID),
                manifests_read: 2,
            })
        );
    }

    #[tokio::test]
    async fn a_table_without_a_snapshot_has_only_its_metadata_file() {
        let outcome = measured(
            &metadata(HashMap::new(), false),
            storage().await,
            &ProgressTracker::new(),
        )
        .await
        .unwrap();

        assert_eq!(
            outcome,
            MeasureOutcome::Measured(Measurement {
                sizes: Sizes {
                    live_data_bytes: 0,
                    live_delete_bytes: 0,
                    live_metadata_bytes: i64::try_from(METADATA_FILE_BYTES).unwrap(),
                },
                findings: Vec::new(),
                snapshot_id: None,
                manifests_read: 0,
            })
        );
    }

    #[tokio::test]
    async fn an_encrypted_table_is_not_measured() {
        let properties = HashMap::from([(
            TableProperties::PROPERTY_ENCRYPTION_KEY_ID.to_string(),
            "key-1".to_string(),
        )]);

        // No files are written: an encrypted table is not read.
        let outcome = measured(
            &metadata(properties, true),
            MemoryStorage::new_isolated(),
            &ProgressTracker::new(),
        )
        .await
        .unwrap();

        assert_eq!(
            outcome,
            MeasureOutcome::NotMeasurable(NotMeasurableReason::Encrypted)
        );
    }

    #[tokio::test]
    async fn a_manifest_that_is_no_avro_file_makes_the_table_unreadable() {
        let file_io = FileIO::new_with_memory();
        let storage = storage().await;
        manifest_list(&file_io, vec![data_manifest(&file_io).await]).await;
        copy(&file_io, &storage, LIST_PATH).await;
        storage
            .write(DATA_MANIFEST_PATH, b"not avro".to_vec().into())
            .await
            .unwrap();

        let outcome = measured(
            &metadata(HashMap::new(), true),
            storage,
            &ProgressTracker::new(),
        )
        .await
        .unwrap();

        assert_eq!(
            outcome,
            MeasureOutcome::NotMeasurable(NotMeasurableReason::MetadataUnreadable {
                path: DATA_MANIFEST_PATH.to_string(),
            })
        );
    }

    /// metadata.json, the manifest list or a manifest that doesn't exist makes the table not
    /// measurable, with the missing file's path.
    #[tokio::test]
    async fn a_missing_metadata_file_makes_the_table_not_measurable() {
        let file_io = FileIO::new_with_memory();
        manifest_list(&file_io, vec![data_manifest(&file_io).await]).await;
        let no_metadata_file = MemoryStorage::new_isolated();
        let no_list = storage().await;
        let no_manifest = storage().await;
        copy(&file_io, &no_manifest, LIST_PATH).await;

        for (storage, path) in [
            (no_metadata_file, METADATA_PATH),
            (no_list, LIST_PATH),
            (no_manifest, DATA_MANIFEST_PATH),
        ] {
            let outcome = measured(
                &metadata(HashMap::new(), true),
                storage,
                &ProgressTracker::new(),
            )
            .await
            .unwrap();

            assert_eq!(
                outcome,
                MeasureOutcome::NotMeasurable(NotMeasurableReason::MetadataMissing {
                    path: path.to_string(),
                }),
            );
        }
    }

    #[tokio::test]
    async fn a_wrong_manifest_length_is_a_finding_and_the_table_is_still_measured() {
        let file_io = FileIO::new_with_memory();
        let storage = storage().await;
        let mut manifest = data_manifest(&file_io).await;
        manifest.manifest_length = 10;
        manifest_list(&file_io, vec![manifest]).await;
        let manifest_bytes = copy(&file_io, &storage, DATA_MANIFEST_PATH).await.len();
        let list_bytes = copy(&file_io, &storage, LIST_PATH).await.len();

        let outcome = measured(
            &metadata(HashMap::new(), true),
            storage,
            &ProgressTracker::new(),
        )
        .await
        .unwrap();

        let expected_metadata_bytes =
            i64::try_from(METADATA_FILE_BYTES + list_bytes + manifest_bytes).unwrap() + 77 + 33;
        assert_eq!(
            outcome,
            MeasureOutcome::Measured(Measurement {
                sizes: Sizes {
                    live_data_bytes: 3000,
                    live_delete_bytes: 0,
                    live_metadata_bytes: expected_metadata_bytes,
                },
                findings: vec![Finding {
                    kind: FindingKind::ManifestLengthMismatch,
                    severity: Severity::Warning,
                    evidence: serde_json::json!({
                        "count": 1,
                        "examples": [{
                            "path": DATA_MANIFEST_PATH,
                            "recorded": 10,
                            "actual": manifest_bytes,
                        }],
                    }),
                    thresholds: serde_json::json!({}),
                }],
                snapshot_id: Some(SNAPSHOT_ID),
                manifests_read: 1,
            })
        );
    }

    #[tokio::test]
    async fn a_cancelled_tracker_ends_the_measurement() {
        let tracker = ProgressTracker::new();
        tracker.cancel();

        let result = measured(&metadata(HashMap::new(), true), storage().await, &tracker).await;

        assert!(matches!(result, Err(MeasureError::Cancelled)), "{result:?}");
    }

    fn entry(
        status: ManifestStatus,
        content: DataContentType,
        path: &str,
        size: Option<i64>,
    ) -> ManifestEntryStats<'_> {
        ManifestEntryStats {
            alive: status != ManifestStatus::Deleted,
            status: Some(status),
            content,
            file_path: path,
            file_size_in_bytes: size,
            record_count: Some(1),
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        }
    }

    #[test]
    fn an_alive_entry_without_a_valid_size_is_reported_and_a_deleted_one_is_not() {
        let mut totals = ManifestTotals::default();
        totals.add(&entry(
            ManifestStatus::Deleted,
            DataContentType::Data,
            "deleted",
            None,
        ));
        assert_eq!(totals.missing_file_size, None);

        totals.add(&entry(
            ManifestStatus::Added,
            DataContentType::Data,
            "negative",
            Some(-1),
        ));
        totals.add(&entry(
            ManifestStatus::Existing,
            DataContentType::Data,
            "missing",
            None,
        ));

        assert_eq!(totals.missing_file_size.as_deref(), Some("negative"));
        assert_eq!(totals.data_bytes, 0);
    }

    #[test]
    fn a_puffin_file_shared_across_manifests_counts_once() {
        let deletion_vector = |offset| ManifestEntryStats {
            content_offset: Some(offset),
            ..entry(
                ManifestStatus::Added,
                DataContentType::PositionDeletes,
                "a.puffin",
                Some(50),
            )
        };
        let mut first = ManifestTotals::default();
        first.add(&deletion_vector(4));
        let mut second = ManifestTotals::default();
        second.add(&deletion_vector(44));
        second.add(&entry(
            ManifestStatus::Added,
            DataContentType::EqualityDeletes,
            "e.parquet",
            Some(200),
        ));

        first.merge(second);

        assert_eq!(first.delete_bytes(), 250);
    }
}
