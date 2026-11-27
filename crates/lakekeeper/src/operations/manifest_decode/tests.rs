use std::{
    collections::{BTreeMap, HashMap},
    io::Cursor,
    sync::Arc,
};

use apache_avro::{
    Codec, DeflateSettings, Reader, Schema as AvroSchema, Writer, ZstandardSettings,
    reader::datum::GenericDatumReader,
    schema::MapSchema,
    types::{Record, Value},
    writer::datum::GenericDatumWriter,
};
use iceberg::{
    io::FileIO,
    spec::{
        DataFile, DataFileBuilder, DataFileFormat, Datum, FormatVersion, Literal,
        ManifestContentType, ManifestFile, ManifestListWriter, ManifestWriterBuilder, NestedField,
        PartitionSpec, PrimitiveType, Schema, Struct, Transform, Type,
    },
};

use super::*;

const LIST_PATH: &str = "memory://t/metadata/snap-1.avro";
const MANIFEST_PATH: &str = "memory://t/metadata/m0.avro";

/// What a visit of a manifest entry yields, owned.
type Row = (bool, DataContentType, String, Option<String>);

fn list_entries(bytes: &[u8]) -> Result<Vec<ManifestListEntry>, ReadError> {
    let mut entries = Vec::new();
    visit_manifest_list(bytes, LIST_PATH, |entry| {
        entries.push(entry);
        Ok(())
    })?;
    Ok(entries)
}

fn visited(bytes: &[u8]) -> Result<Vec<Row>, ReadError> {
    let mut rows = Vec::new();
    visit_manifest(bytes, MANIFEST_PATH, |entry| {
        rows.push((
            entry.alive,
            entry.content,
            entry.file_path.to_string(),
            entry.referenced_data_file.map(str::to_string),
        ));
        Ok(())
    })?;
    Ok(rows)
}

/// The rows of a manifest as iceberg-rust parses it whole.
fn parsed(bytes: &[u8]) -> Vec<Row> {
    let (_metadata, entries) = Manifest::try_from_avro_bytes(bytes, None).unwrap();
    entries
        .iter()
        .map(|entry| {
            (
                entry.status() != ManifestStatus::Deleted,
                entry.content_type(),
                entry.file_path().to_string(),
                entry.data_file().referenced_data_file(),
            )
        })
        .collect()
}

fn parse_reason(result: Result<impl std::fmt::Debug, ReadError>) -> String {
    match result {
        Err(ReadError::ParseError(e)) => e.reason,
        other => panic!("expected a parse error, got {other:?}"),
    }
}

fn record(fields: &[(&str, Value)]) -> Value {
    Value::Record(
        fields
            .iter()
            .map(|(name, value)| ((*name).to_string(), value.clone()))
            .collect(),
    )
}

/// An Avro file of `records` written with `schema`.
fn avro_file(schema: &str, records: &[&[(&str, Value)]]) -> Vec<u8> {
    let schema = AvroSchema::parse_str(schema).unwrap();
    let mut writer = Writer::new(&schema, Vec::new()).unwrap();
    for fields in records {
        let mut record = Record::new(&schema).unwrap();
        for (name, value) in *fields {
            record.put(name, value.clone());
        }
        writer.append_value(record).unwrap();
    }
    writer.into_inner().unwrap()
}

/// `bytes` written again with `codec`, in blocks of about 4096 bytes.
fn reencoded(bytes: &[u8], codec: Codec) -> Vec<u8> {
    let reader = Reader::new(bytes).unwrap();
    let schema = reader.writer_schema().clone();
    let metadata = reader.user_metadata().clone();
    let values: Vec<_> = reader.map(Result::unwrap).collect();
    let mut writer = Writer::builder()
        .schema(&schema)
        .writer(Vec::new())
        .codec(codec)
        .block_size(4096)
        .build()
        .unwrap();
    for (key, value) in metadata {
        writer.add_user_metadata(key, value).unwrap();
    }
    for value in values {
        writer.append_value(value).unwrap();
    }
    writer.into_inner().unwrap()
}

fn codecs() -> [Codec; 4] {
    [
        Codec::Null,
        Codec::Deflate(DeflateSettings::default()),
        Codec::Snappy,
        Codec::Zstandard(ZstandardSettings::default()),
    ]
}

/// `bytes` with the schema in its header changed by `rewrite`. The data blocks stay as they are,
/// so the change must not change how they decode.
fn with_schema(bytes: &[u8], rewrite: impl FnOnce(&mut serde_json::Value)) -> Vec<u8> {
    const MAGIC: &[u8] = b"Obj\x01";
    let body = bytes.strip_prefix(MAGIC).unwrap();
    let metadata_schema = AvroSchema::Map(MapSchema {
        types: Box::new(AvroSchema::Bytes),
        attributes: BTreeMap::default(),
    });
    let mut cursor = Cursor::new(body);
    let Value::Map(mut metadata) = GenericDatumReader::builder(&metadata_schema)
        .build()
        .unwrap()
        .read_value(&mut cursor)
        .unwrap()
    else {
        panic!("the header metadata is a map");
    };
    let rest = &body[usize::try_from(cursor.position()).unwrap()..];
    let Some(Value::Bytes(schema)) = metadata.get("avro.schema") else {
        panic!("the header has a schema");
    };
    let mut schema: serde_json::Value = serde_json::from_slice(schema).unwrap();
    rewrite(&mut schema);
    metadata.insert(
        "avro.schema".to_string(),
        Value::Bytes(serde_json::to_vec(&schema).unwrap()),
    );
    let metadata = GenericDatumWriter::builder(&metadata_schema)
        .build()
        .unwrap()
        .write_value_to_vec(Value::Map(metadata))
        .unwrap();
    [MAGIC, &metadata, rest].concat()
}

/// Calls `visit` on every JSON object in `schema`.
fn for_each_object(
    schema: &mut serde_json::Value,
    visit: &mut impl FnMut(&mut serde_json::Map<String, serde_json::Value>),
) {
    match schema {
        serde_json::Value::Array(values) => {
            for value in values {
                for_each_object(value, visit);
            }
        }
        serde_json::Value::Object(object) => {
            visit(object);
            for value in object.values_mut() {
                for_each_object(value, visit);
            }
        }
        _ => {}
    }
}

/// Renames the fields called `from` to `to`, keeping their field ids.
fn rename_fields(schema: &mut serde_json::Value, renames: &[(&str, &str)]) {
    for_each_object(schema, &mut |object| {
        if object.contains_key("field-id")
            && let Some(serde_json::Value::String(name)) = object.get_mut("name")
            && let Some((_, to)) = renames.iter().find(|(from, _)| from == name)
        {
            *name = (*to).to_string();
        }
    });
}

/// Repeats the definition of each named type where it is referenced, as older iceberg-rust
/// releases wrote manifests.
fn define_named_types_repeatedly(schema: &mut serde_json::Value) {
    fn inline(schema: &mut serde_json::Value, defined: &mut HashMap<String, serde_json::Value>) {
        match schema {
            serde_json::Value::String(name) => {
                if let Some(definition) = defined.get(name.as_str()) {
                    *schema = definition.clone();
                }
            }
            serde_json::Value::Array(schemas) => {
                for schema in schemas {
                    inline(schema, defined);
                }
            }
            serde_json::Value::Object(object) => {
                if let (Some(serde_json::Value::String(name)), Some("record" | "enum" | "fixed")) = (
                    object.get("name"),
                    object.get("type").and_then(serde_json::Value::as_str),
                ) {
                    defined.insert(name.clone(), serde_json::Value::Object(object.clone()));
                }
                for key in ["type", "items", "values", "fields"] {
                    if let Some(child) = object.get_mut(key) {
                        inline(child, defined);
                    }
                }
            }
            _ => {}
        }
    }
    inline(schema, &mut HashMap::new());
}

// ---------- generated manifests ----------

/// How the generated manifests are partitioned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Partitioning {
    Unpartitioned,
    /// Day of a timestamp, two fields of one decimal type, a uuid bucket and a string.
    Mixed,
}

fn column_type(i: usize) -> PrimitiveType {
    match i {
        0 => PrimitiveType::Long,
        1 => PrimitiveType::Timestamp,
        _ => match i % 5 {
            0 => PrimitiveType::String,
            1 => PrimitiveType::Long,
            2 => PrimitiveType::Double,
            3 => PrimitiveType::Int,
            _ => PrimitiveType::Date,
        },
    }
}

const COLUMNS: usize = 12;

fn table_schema() -> Arc<Schema> {
    let mut fields: Vec<_> = (0..COLUMNS)
        .map(|i| {
            Arc::new(NestedField::optional(
                i32::try_from(i + 1).unwrap(),
                format!("c{i}"),
                Type::Primitive(column_type(i)),
            ))
        })
        .collect();
    fields.push(Arc::new(NestedField::optional(
        1000,
        "amount",
        Type::Primitive(PrimitiveType::Decimal {
            precision: 10,
            scale: 2,
        }),
    )));
    fields.push(Arc::new(NestedField::optional(
        1001,
        "id",
        Type::Primitive(PrimitiveType::Uuid),
    )));
    Arc::new(Schema::builder().with_fields(fields).build().unwrap())
}

fn partition_spec(schema: &Arc<Schema>, partitioning: Partitioning) -> PartitionSpec {
    let builder = PartitionSpec::builder(schema.clone()).with_spec_id(0);
    match partitioning {
        Partitioning::Unpartitioned => builder.build().unwrap(),
        Partitioning::Mixed => builder
            .add_partition_field("c1", "c1_day", Transform::Day)
            .unwrap()
            .add_partition_field("amount", "amount", Transform::Identity)
            .unwrap()
            .add_partition_field("amount", "amount_trunc", Transform::Truncate(10))
            .unwrap()
            .add_partition_field("id", "id_bucket", Transform::Bucket(16))
            .unwrap()
            .add_partition_field("c5", "c5", Transform::Identity)
            .unwrap()
            .build()
            .unwrap(),
    }
}

fn partition(partitioning: Partitioning, j: i32) -> Struct {
    match partitioning {
        Partitioning::Unpartitioned => Struct::empty(),
        Partitioning::Mixed => Struct::from_iter([
            Some(Literal::date(20_000 + j % 28)),
            Some(Literal::decimal(i128::from(j) * 100 + 7)),
            Some(Literal::decimal(i128::from(j) * 10)),
            Some(Literal::int(j % 16)),
            Some(Literal::string(format!("s{j}"))),
        ]),
    }
}

fn bound(column: usize, j: i32, upper: bool) -> Datum {
    let value = i64::from(j) + if upper { 1_000_000 } else { 0 };
    match column_type(column) {
        PrimitiveType::Long => Datum::long(value * 7919),
        PrimitiveType::Timestamp => Datum::timestamp_micros(1_790_000_000_000_000 + value),
        PrimitiveType::String => Datum::string(format!("{value:016}")),
        PrimitiveType::Double => Datum::double(f64::from(j) * 1.5),
        PrimitiveType::Int => Datum::int(j),
        _ => Datum::date(20_000 + j % 300),
    }
}

/// Entry `j` of a generated manifest. Position deletes name their data file every other entry;
/// in a v3 manifest every fourth one is a deletion vector.
fn data_file(
    version: FormatVersion,
    content: DataContentType,
    partitioning: Partitioning,
    j: i32,
) -> DataFile {
    let data_path = format!("s3://b/wh/t/data/{j:05}-data.parquet");
    let mut builder = DataFileBuilder::default();
    builder
        .content(content)
        .file_format(DataFileFormat::Parquet)
        .file_path(match content {
            DataContentType::Data => data_path.clone(),
            _ => format!("s3://b/wh/t/data/{j:05}-deletes.parquet"),
        })
        .partition(partition(partitioning, j))
        .record_count(100 + u64::try_from(j).unwrap())
        .file_size_in_bytes(1000 + u64::try_from(j).unwrap())
        .partition_spec_id(0);
    if content == DataContentType::Data {
        let column_id = |i: usize| i32::try_from(i + 1).unwrap();
        let columns = 0..COLUMNS;
        builder
            .column_sizes(columns.clone().map(|i| (column_id(i), 1000)).collect())
            .value_counts(columns.clone().map(|i| (column_id(i), 10)).collect())
            .null_value_counts(columns.clone().map(|i| (column_id(i), 0)).collect())
            .lower_bounds(
                columns
                    .clone()
                    .map(|i| (column_id(i), bound(i, j, false)))
                    .collect(),
            )
            .upper_bounds(columns.map(|i| (column_id(i), bound(i, j, true))).collect());
    }
    if content == DataContentType::PositionDeletes && j % 2 == 0 {
        builder.referenced_data_file(Some(data_path));
        if version == FormatVersion::V3 && j % 4 == 0 {
            builder
                .file_format(DataFileFormat::Puffin)
                .file_path(format!("s3://b/wh/t/data/{j:05}-dv.puffin"))
                .content_offset(Some(4))
                .content_size_in_bytes(Some(40));
        }
    }
    if content == DataContentType::EqualityDeletes {
        builder.equality_ids(Some(vec![1]));
    }
    builder.build().unwrap()
}

/// A manifest with `entries` entries, a third each added, existing and deleted.
async fn manifest(
    file_io: &FileIO,
    version: FormatVersion,
    content: DataContentType,
    partitioning: Partitioning,
    entries: i32,
) -> Vec<u8> {
    let schema = table_schema();
    let path = format!("memory://t/metadata/{version}-{content:?}-{partitioning:?}-m0.avro");
    let builder = ManifestWriterBuilder::new(
        file_io.new_output(&path).unwrap(),
        Some(7),
        schema.clone(),
        partition_spec(&schema, partitioning),
    );
    let deletes = content != DataContentType::Data;
    let mut writer = match (version, deletes) {
        (FormatVersion::V1, _) => builder.build_v1(),
        (FormatVersion::V2, false) => builder.build_v2_data(),
        (FormatVersion::V2, true) => builder.build_v2_deletes(),
        (FormatVersion::V3, false) => builder.build_v3_data(),
        (FormatVersion::V3, true) => builder.build_v3_deletes(),
    };
    for j in 0..entries {
        let data_file = data_file(version, content, partitioning, j);
        match j % 3 {
            0 => writer.add_file(data_file, 5).unwrap(),
            1 => writer.add_existing_file(data_file, 3, 4, Some(4)).unwrap(),
            _ => writer.add_delete_file(data_file, 3, Some(3)).unwrap(),
        }
    }
    writer.write_manifest_file().await.unwrap();
    file_io
        .new_input(&path)
        .unwrap()
        .read()
        .await
        .unwrap()
        .to_vec()
}

/// Every format version and content type a manifest can have.
fn manifest_kinds() -> Vec<(FormatVersion, DataContentType)> {
    let mut kinds = vec![(FormatVersion::V1, DataContentType::Data)];
    for version in [FormatVersion::V2, FormatVersion::V3] {
        for content in [
            DataContentType::Data,
            DataContentType::PositionDeletes,
            DataContentType::EqualityDeletes,
        ] {
            kinds.push((version, content));
        }
    }
    kinds
}

// ---------- manifest lists ----------

fn manifest_file(path: &str, length: i64, partition_spec_id: i32) -> ManifestFile {
    ManifestFile {
        manifest_path: path.to_string(),
        manifest_length: length,
        partition_spec_id,
        content: ManifestContentType::Data,
        sequence_number: 1,
        min_sequence_number: 1,
        added_snapshot_id: 1,
        added_files_count: Some(1),
        existing_files_count: Some(0),
        deleted_files_count: Some(0),
        added_rows_count: Some(3),
        existing_rows_count: Some(0),
        deleted_rows_count: Some(0),
        partitions: None,
        key_metadata: None,
        first_row_id: None,
    }
}

async fn manifest_list(file_io: &FileIO, version: FormatVersion) -> Vec<u8> {
    let path = format!("memory://t/metadata/snap-{version}.avro");
    let output = file_io.new_output(&path).unwrap().writer().await.unwrap();
    let mut list = match version {
        FormatVersion::V1 => ManifestListWriter::v1(output, 1, None),
        FormatVersion::V2 => ManifestListWriter::v2(output, 1, None, 1),
        FormatVersion::V3 => ManifestListWriter::v3(output, 1, None, 1, Some(0)),
    };
    list.add_manifests(
        [
            manifest_file("memory://t/metadata/m0.avro", 4096, 0),
            manifest_file("memory://t/metadata/m1.avro", 512, 3),
        ]
        .into_iter(),
    )
    .unwrap();
    list.close().await.unwrap();
    file_io
        .new_input(&path)
        .unwrap()
        .read()
        .await
        .unwrap()
        .to_vec()
}

fn listed(path: &str, length: Option<i64>, partition_spec_id: Option<i32>) -> ManifestListEntry {
    ManifestListEntry {
        manifest_path: path.to_string(),
        manifest_length: length,
        partition_spec_id,
    }
}

#[tokio::test]
async fn reads_the_manifest_lists_of_every_format_version() {
    let file_io = FileIO::new_with_memory();
    for version in [FormatVersion::V1, FormatVersion::V2, FormatVersion::V3] {
        let bytes = manifest_list(&file_io, version).await;
        for codec in codecs() {
            assert_eq!(
                list_entries(&reencoded(&bytes, codec)).unwrap(),
                vec![
                    listed("memory://t/metadata/m0.avro", Some(4096), Some(0)),
                    listed("memory://t/metadata/m1.avro", Some(512), Some(3)),
                ],
                "{version} {codec:?}"
            );
        }
    }
}

#[tokio::test]
async fn finds_list_fields_its_writer_renamed_by_their_field_ids() {
    let bytes = manifest_list(&FileIO::new_with_memory(), FormatVersion::V2).await;
    let renamed = with_schema(&bytes, |schema| {
        rename_fields(
            schema,
            &[
                ("manifest_path", "path"),
                ("manifest_length", "length"),
                ("partition_spec_id", "spec"),
            ],
        );
    });

    assert_eq!(
        list_entries(&renamed).unwrap(),
        list_entries(&bytes).unwrap()
    );
}

#[test]
fn matches_list_fields_by_name_and_skips_unknown_ones() {
    let bytes = avro_file(
        r#"{"type": "record", "name": "manifest_file", "fields": [
            {"name": "added_by", "type": "string"},
            {"name": "partition_spec_id", "type": "long"},
            {"name": "manifest_length", "type": "int"},
            {"name": "manifest_path", "type": ["null", "string"]}
        ]}"#,
        &[&[
            ("added_by", "a writer".into()),
            ("partition_spec_id", Value::Long(2)),
            ("manifest_length", Value::Int(100)),
            (
                "manifest_path",
                Value::Union(1, Box::new("s3://b/m0.avro".into())),
            ),
        ]],
    );

    assert_eq!(
        list_entries(&bytes).unwrap(),
        vec![listed("s3://b/m0.avro", Some(100), Some(2))]
    );
}

#[test]
fn a_list_entry_without_length_or_spec_id_still_names_its_manifest() {
    let missing = avro_file(
        r#"{"type": "record", "name": "manifest_file", "fields": [
            {"name": "manifest_path", "type": "string"}
        ]}"#,
        &[&[("manifest_path", "s3://b/m0.avro".into())]],
    );
    let unreadable = avro_file(
        r#"{"type": "record", "name": "manifest_file", "fields": [
            {"name": "manifest_path", "type": "string"},
            {"name": "manifest_length", "type": "string"},
            {"name": "partition_spec_id", "type": ["null", "long"]}
        ]}"#,
        &[
            &[
                ("manifest_path", "s3://b/m0.avro".into()),
                ("manifest_length", "100".into()),
                ("partition_spec_id", Value::Union(0, Box::new(Value::Null))),
            ],
            &[
                ("manifest_path", "s3://b/m1.avro".into()),
                ("manifest_length", "100".into()),
                (
                    "partition_spec_id",
                    Value::Union(1, Box::new(Value::Long(i64::from(i32::MAX) + 1))),
                ),
            ],
        ],
    );

    assert_eq!(
        list_entries(&missing).unwrap(),
        vec![listed("s3://b/m0.avro", None, None)]
    );
    assert_eq!(
        list_entries(&unreadable).unwrap(),
        vec![
            listed("s3://b/m0.avro", None, None),
            listed("s3://b/m1.avro", None, None),
        ]
    );
}

#[test]
fn a_list_entry_without_a_manifest_path_is_a_parse_error() {
    let missing = avro_file(
        r#"{"type": "record", "name": "manifest_file", "fields": [
            {"name": "manifest_length", "type": "long"}
        ]}"#,
        &[&[("manifest_length", Value::Long(100))]],
    );
    let null = avro_file(
        r#"{"type": "record", "name": "manifest_file", "fields": [
            {"name": "manifest_path", "type": ["null", "string"]}
        ]}"#,
        &[&[("manifest_path", Value::Union(0, Box::new(Value::Null)))]],
    );

    assert_eq!(
        parse_reason(list_entries(&missing)),
        "missing field `manifest_path`"
    );
    assert_eq!(
        parse_reason(list_entries(&null)),
        "field `manifest_path` is not a string: Some(Null)"
    );
}

#[test]
fn a_list_without_entries_names_no_manifest() {
    let bytes = avro_file(
        r#"{"type": "record", "name": "manifest_file", "fields": [
            {"name": "manifest_path", "type": "string"}
        ]}"#,
        &[],
    );

    assert_eq!(list_entries(&bytes).unwrap(), vec![]);
}

#[test]
fn bytes_that_are_no_avro_file_are_a_parse_error() {
    for bytes in [&b""[..], b"not an avro file", b"Obj\x01"] {
        assert!(
            matches!(list_entries(bytes), Err(ReadError::ParseError(_))),
            "{bytes:?}"
        );
        assert!(
            matches!(visited(bytes), Err(ReadError::ParseError(_))),
            "{bytes:?}"
        );
    }
}

// ---------- manifests ----------

#[tokio::test]
async fn reads_every_manifest_as_iceberg_rust_parses_it() {
    let file_io = FileIO::new_with_memory();
    let mut checked = 0;
    for (version, content) in manifest_kinds() {
        for partitioning in [Partitioning::Unpartitioned, Partitioning::Mixed] {
            let bytes = manifest(&file_io, version, content, partitioning, 30).await;
            let expected = parsed(&bytes);
            assert_eq!(expected.len(), 30);
            assert_eq!(expected.iter().filter(|row| row.0).count(), 20);
            if content == DataContentType::PositionDeletes {
                assert_eq!(expected.iter().filter(|row| row.3.is_some()).count(), 15);
            }
            for codec in codecs() {
                let label = format!("{version} {content:?} {partitioning:?} {codec:?}");
                assert_eq!(
                    visited(&reencoded(&bytes, codec)).unwrap(),
                    expected,
                    "{label}"
                );
                checked += 1;
            }
        }
    }
    assert_eq!(checked, 7 * 2 * 4);
}

#[tokio::test]
async fn reads_a_manifest_that_defines_a_named_type_twice() {
    let bytes = manifest(
        &FileIO::new_with_memory(),
        FormatVersion::V2,
        DataContentType::Data,
        Partitioning::Mixed,
        9,
    )
    .await;
    let repeated = with_schema(&bytes, define_named_types_repeatedly);
    let Err(e) = Reader::new(repeated.as_slice()) else {
        panic!("apache-avro reads a schema that defines a named type twice");
    };
    assert!(
        matches!(e.details(), Details::AmbiguousSchemaDefinition(_)),
        "{e}"
    );

    assert_eq!(visited(&repeated).unwrap(), parsed(&bytes));
}

#[tokio::test]
async fn reads_a_manifest_with_partition_field_names_avro_does_not_allow() {
    let bytes = manifest(
        &FileIO::new_with_memory(),
        FormatVersion::V2,
        DataContentType::Data,
        Partitioning::Mixed,
        9,
    )
    .await;
    let renamed = with_schema(&bytes, |schema| {
        rename_fields(schema, &[("c1_day", "c1.day"), ("c5", "größe")]);
    });
    let Err(e) = Reader::new(renamed.as_slice()) else {
        panic!("apache-avro reads field names it does not allow");
    };
    assert!(matches!(e.details(), Details::FieldName(_)), "{e}");

    assert_eq!(visited(&renamed).unwrap(), parsed(&bytes));
}

#[tokio::test]
async fn finds_manifest_fields_its_writer_renamed_by_their_field_ids() {
    let file_io = FileIO::new_with_memory();
    for (version, content) in manifest_kinds() {
        let bytes = manifest(&file_io, version, content, Partitioning::Mixed, 12).await;
        let renamed = with_schema(&bytes, |schema| {
            rename_fields(
                schema,
                &[
                    ("status", "entry_status"),
                    ("data_file", "file"),
                    ("content", "kind"),
                    ("file_path", "path"),
                    ("referenced_data_file", "deletes_of"),
                ],
            );
        });

        assert_eq!(
            visited(&renamed).unwrap(),
            parsed(&bytes),
            "{version} {content:?}"
        );
    }
}

/// A manifest entry schema with the given status field, data file fields and further fields.
fn entry_schema(status: &str, data_file_fields: &str, data_file_type: &str) -> String {
    let data_file =
        format!(r#"{{"type": "record", "name": "r2", "fields": [{data_file_fields}]}}"#);
    let data_file = data_file_type.replace("RECORD", &data_file);
    format!(
        r#"{{"type": "record", "name": "manifest_entry", "fields": [
            {{"name": "snapshot_id", "type": ["null", "long"], "field-id": 1}},
            {{"name": "data_file", "type": {data_file}, "field-id": 2}}
            {status}
        ]}}"#
    )
}

const PATH_FIELD: &str = r#"{"name": "file_path", "type": "string", "field-id": 100}"#;

fn entry(status: Option<Value>, data_file: Value) -> Vec<(&'static str, Value)> {
    let mut fields = vec![
        ("snapshot_id", Value::Union(1, Box::new(Value::Long(7)))),
        ("data_file", data_file),
    ];
    if let Some(status) = status {
        fields.push(("status", status));
    }
    fields
}

fn path(path: &str) -> Value {
    record(&[("file_path", path.into())])
}

#[test]
fn a_field_id_wins_over_a_name() {
    let schema = entry_schema(
        "",
        r#"{"name": "file_path", "type": "string", "field-id": 999},
           {"name": "location", "type": "string", "field-id": 100}"#,
        "RECORD",
    );
    let bytes = avro_file(
        &schema,
        &[&entry(
            None,
            record(&[
                ("file_path", "not a path".into()),
                ("location", "s3://b/a.parquet".into()),
            ]),
        )],
    );

    assert_eq!(visited(&bytes).unwrap()[0].2, "s3://b/a.parquet");
}

#[test]
fn a_field_without_its_field_id_is_found_by_name() {
    let schema = entry_schema(
        "",
        r#"{"name": "file_path", "type": "string", "field-id": 999}"#,
        "RECORD",
    );
    let bytes = avro_file(&schema, &[&entry(None, path("s3://b/a.parquet"))]);

    assert_eq!(visited(&bytes).unwrap()[0].2, "s3://b/a.parquet");
}

#[test]
fn a_missing_or_unknown_status_keeps_the_file_alive() {
    let without_status = avro_file(
        &entry_schema("", PATH_FIELD, "RECORD"),
        &[&entry(None, path("s3://b/a.parquet"))],
    );
    let schema = entry_schema(
        r#", {"name": "status", "type": "long", "field-id": 0}"#,
        PATH_FIELD,
        "RECORD",
    );
    let statuses: Vec<_> = [0, 1, 2, 7]
        .into_iter()
        .map(|status| entry(Some(Value::Long(status)), path(&format!("s3://b/{status}"))))
        .collect();
    let with_status = avro_file(
        &schema,
        &statuses.iter().map(Vec::as_slice).collect::<Vec<_>>(),
    );

    let alive = |bytes| -> Vec<bool> { visited(bytes).unwrap().iter().map(|row| row.0).collect() };
    assert_eq!(alive(&without_status), [true]);
    assert_eq!(alive(&with_status), [true, true, false, true]);
}

#[test]
fn a_missing_or_unknown_content_is_data() {
    let without_content = avro_file(
        &entry_schema("", PATH_FIELD, "RECORD"),
        &[&entry(None, path("s3://b/a.parquet"))],
    );
    let schema = entry_schema(
        "",
        &format!(r#"{PATH_FIELD}, {{"name": "content", "type": "long", "field-id": 134}}"#),
        "RECORD",
    );
    let contents: Vec<_> = [0, 1, 2, 9]
        .into_iter()
        .map(|content| {
            entry(
                None,
                record(&[
                    ("file_path", format!("s3://b/{content}").into()),
                    ("content", Value::Long(content)),
                ]),
            )
        })
        .collect();
    let with_content = avro_file(
        &schema,
        &contents.iter().map(Vec::as_slice).collect::<Vec<_>>(),
    );

    let contents = |bytes| -> Vec<DataContentType> {
        visited(bytes).unwrap().iter().map(|row| row.1).collect()
    };
    assert_eq!(contents(&without_content), [DataContentType::Data]);
    assert_eq!(
        contents(&with_content),
        [
            DataContentType::Data,
            DataContentType::PositionDeletes,
            DataContentType::EqualityDeletes,
            DataContentType::Data,
        ]
    );
}

#[test]
fn skips_unknown_fields_and_reads_a_data_file_inside_a_union() {
    let schema = entry_schema(
        r#", {"name": "written_by", "type": "string"}, {"name": "status", "type": "int"}"#,
        &format!(
            r#"{{"name": "file_size", "type": "long"}},
               {{"name": "referenced_data_file", "type": ["null", "string"], "field-id": 143}},
               {PATH_FIELD}"#
        ),
        r#"["null", RECORD]"#,
    );
    let data_file = record(&[
        ("file_size", Value::Long(10)),
        (
            "referenced_data_file",
            Value::Union(1, Box::new("s3://b/data.parquet".into())),
        ),
        ("file_path", "s3://b/deletes.parquet".into()),
    ]);
    let bytes = avro_file(
        &schema,
        &[&[
            ("snapshot_id", Value::Union(0, Box::new(Value::Null))),
            ("data_file", Value::Union(1, Box::new(data_file))),
            ("written_by", "a writer".into()),
            ("status", Value::Int(1)),
        ]],
    );

    assert_eq!(
        visited(&bytes).unwrap(),
        vec![(
            true,
            DataContentType::Data,
            "s3://b/deletes.parquet".to_string(),
            Some("s3://b/data.parquet".to_string()),
        )]
    );
}

#[test]
fn a_referenced_data_file_of_another_type_is_ignored() {
    let schema = entry_schema(
        "",
        &format!(
            r#"{PATH_FIELD}, {{"name": "referenced_data_file", "type": "long", "field-id": 143}}"#
        ),
        "RECORD",
    );
    let bytes = avro_file(
        &schema,
        &[&entry(
            None,
            record(&[
                ("file_path", "s3://b/deletes.parquet".into()),
                ("referenced_data_file", Value::Long(3)),
            ]),
        )],
    );

    assert_eq!(visited(&bytes).unwrap()[0].3, None);
}

#[test]
fn an_entry_without_a_file_path_is_a_parse_error() {
    let missing = avro_file(
        &entry_schema(
            "",
            r#"{"name": "file_size", "type": "long", "field-id": 104}"#,
            "RECORD",
        ),
        &[&entry(None, record(&[("file_size", Value::Long(10))]))],
    );
    let null = avro_file(
        &entry_schema(
            "",
            r#"{"name": "file_path", "type": ["null", "string"], "field-id": 100}"#,
            "RECORD",
        ),
        &[&entry(
            None,
            record(&[("file_path", Value::Union(0, Box::new(Value::Null)))]),
        )],
    );

    assert_eq!(
        parse_reason(visited(&missing)),
        "missing field `data_file.file_path`"
    );
    assert_eq!(
        parse_reason(visited(&null)),
        "field `data_file.file_path` is not a string: Some(Null)"
    );
}

#[tokio::test]
async fn a_manifest_list_is_no_manifest() {
    let bytes = manifest_list(&FileIO::new_with_memory(), FormatVersion::V2).await;

    assert_eq!(parse_reason(visited(&bytes)), "missing field `data_file`");
}

#[test]
fn a_manifest_without_entries_visits_nothing() {
    let bytes = avro_file(&entry_schema("", PATH_FIELD, "RECORD"), &[]);

    assert_eq!(visited(&bytes).unwrap(), vec![]);
}

/// A file cut short is read up to the cut or fails; it never panics, and never yields an entry
/// the whole file does not have at that position.
#[tokio::test]
async fn a_manifest_cut_short_yields_a_prefix_of_its_entries_or_fails() {
    let bytes = reencoded(
        &manifest(
            &FileIO::new_with_memory(),
            FormatVersion::V2,
            DataContentType::PositionDeletes,
            Partitioning::Mixed,
            60,
        )
        .await,
        Codec::Deflate(DeflateSettings::default()),
    );
    let all = visited(&bytes).unwrap();
    let mut shorter = 0;
    for cut in 0..bytes.len() {
        match visited(&bytes[..cut]) {
            Ok(rows) => {
                assert_eq!(rows, all[..rows.len()], "cut at {cut}");
                shorter += 1;
            }
            Err(ReadError::ParseError(_)) => {}
            Err(e) => panic!("cut at {cut}: {e}"),
        }
    }
    // Cuts at block boundaries read as shorter manifests.
    assert!(shorter > 0);
}

#[tokio::test]
async fn an_error_of_the_visit_stops_it() {
    let bytes = manifest(
        &FileIO::new_with_memory(),
        FormatVersion::V2,
        DataContentType::Data,
        Partitioning::Unpartitioned,
        5,
    )
    .await;
    let mut visits = 0;

    let result = visit_manifest(&bytes, MANIFEST_PATH, |_| {
        visits += 1;
        if visits == 2 {
            return Err(ParseError {
                target_type: "test".to_string(),
                reason: "stop".to_string(),
                path: MANIFEST_PATH.to_string(),
            }
            .into());
        }
        Ok(())
    });

    assert_eq!(parse_reason(result), "stop");
    assert_eq!(visits, 2);
}

// ---------- sizes and counts ----------

fn full_list_entries(bytes: &[u8]) -> Result<Vec<ManifestListEntryFull>, ReadError> {
    let mut entries = Vec::new();
    visit_manifest_list_full(bytes, LIST_PATH, |entry| {
        entries.push(entry);
        Ok(())
    })?;
    Ok(entries)
}

/// What a visit of a manifest entry's sizes yields, owned.
type StatsRow = (
    Option<ManifestStatus>,
    DataContentType,
    String,
    Option<i64>,
    Option<i64>,
    Option<String>,
    Option<i64>,
    Option<i64>,
);

fn stats(bytes: &[u8]) -> Result<Vec<(bool, StatsRow)>, ReadError> {
    let mut rows = Vec::new();
    visit_manifest_stats(bytes, MANIFEST_PATH, |entry| {
        rows.push((
            entry.alive,
            (
                entry.status,
                entry.content,
                entry.file_path.to_string(),
                entry.file_size_in_bytes,
                entry.record_count,
                entry.referenced_data_file.map(str::to_string),
                entry.content_offset,
                entry.content_size_in_bytes,
            ),
        ));
        Ok(())
    })?;
    Ok(rows)
}

/// The sizes of a manifest's entries as iceberg-rust parses it whole.
fn parsed_stats(bytes: &[u8]) -> Vec<(bool, StatsRow)> {
    let (_metadata, entries) = Manifest::try_from_avro_bytes(bytes, None).unwrap();
    entries
        .iter()
        .map(|entry| {
            let data_file = entry.data_file();
            (
                entry.status() != ManifestStatus::Deleted,
                (
                    Some(entry.status()),
                    data_file.content_type(),
                    data_file.file_path().to_string(),
                    Some(i64::try_from(data_file.file_size_in_bytes()).unwrap()),
                    Some(i64::try_from(data_file.record_count()).unwrap()),
                    data_file.referenced_data_file(),
                    data_file.content_offset(),
                    data_file.content_size_in_bytes(),
                ),
            )
        })
        .collect()
}

#[tokio::test]
async fn reads_the_counts_of_the_manifest_lists_of_every_format_version() {
    let file_io = FileIO::new_with_memory();
    for version in [FormatVersion::V1, FormatVersion::V2, FormatVersion::V3] {
        let bytes = manifest_list(&file_io, version).await;
        // Format version 1 has no sequence numbers.
        let sequence_number = (version != FormatVersion::V1).then_some(1);
        let full = |path: &str, length, partition_spec_id| ManifestListEntryFull {
            manifest_path: path.to_string(),
            manifest_length: Some(length),
            partition_spec_id: Some(partition_spec_id),
            content: ManifestContentType::Data,
            sequence_number,
            added_snapshot_id: Some(1),
            added_files_count: Some(1),
            existing_files_count: Some(0),
            deleted_files_count: Some(0),
            added_rows_count: Some(3),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
        };
        for codec in codecs() {
            assert_eq!(
                full_list_entries(&reencoded(&bytes, codec)).unwrap(),
                vec![
                    full("memory://t/metadata/m0.avro", 4096, 0),
                    full("memory://t/metadata/m1.avro", 512, 3),
                ],
                "{version} {codec:?}"
            );
        }
    }
}

#[tokio::test]
async fn reads_the_content_of_a_delete_manifest_in_a_list() {
    let file_io = FileIO::new_with_memory();
    let path = "memory://t/metadata/snap-deletes.avro";
    let output = file_io.new_output(path).unwrap().writer().await.unwrap();
    let mut list = ManifestListWriter::v2(output, 1, None, 1);
    let mut deletes = manifest_file("memory://t/metadata/d0.avro", 300, 0);
    deletes.content = ManifestContentType::Deletes;
    deletes.added_files_count = Some(2);
    deletes.added_rows_count = Some(9);
    list.add_manifests([deletes].into_iter()).unwrap();
    list.close().await.unwrap();
    let bytes = file_io.new_input(path).unwrap().read().await.unwrap();

    let entries = full_list_entries(&bytes).unwrap();

    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].content, ManifestContentType::Deletes);
    assert_eq!(entries[0].added_files_count, Some(2));
    assert_eq!(entries[0].added_rows_count, Some(9));
}

#[test]
fn finds_list_counts_by_their_format_version_1_names() {
    let bytes = avro_file(
        r#"{"type": "record", "name": "manifest_file", "fields": [
            {"name": "manifest_path", "type": "string"},
            {"name": "added_data_files_count", "type": "int"},
            {"name": "existing_data_files_count", "type": "int"},
            {"name": "deleted_data_files_count", "type": "int"}
        ]}"#,
        &[&[
            ("manifest_path", "s3://b/m0.avro".into()),
            ("added_data_files_count", Value::Int(4)),
            ("existing_data_files_count", Value::Int(5)),
            ("deleted_data_files_count", Value::Int(6)),
        ]],
    );

    let entries = full_list_entries(&bytes).unwrap();

    assert_eq!(
        entries,
        vec![ManifestListEntryFull {
            manifest_path: "s3://b/m0.avro".to_string(),
            manifest_length: None,
            partition_spec_id: None,
            content: ManifestContentType::Data,
            sequence_number: None,
            added_snapshot_id: None,
            added_files_count: Some(4),
            existing_files_count: Some(5),
            deleted_files_count: Some(6),
            added_rows_count: None,
            existing_rows_count: None,
            deleted_rows_count: None,
        }]
    );
}

#[tokio::test]
async fn reads_the_sizes_of_every_manifest_as_iceberg_rust_parses_them() {
    let file_io = FileIO::new_with_memory();
    let mut checked = 0;
    for (version, content) in manifest_kinds() {
        for partitioning in [Partitioning::Unpartitioned, Partitioning::Mixed] {
            let bytes = manifest(&file_io, version, content, partitioning, 30).await;
            let expected = parsed_stats(&bytes);
            if version == FormatVersion::V3 && content == DataContentType::PositionDeletes {
                assert_eq!(
                    expected.iter().filter(|(_, row)| row.6 == Some(4)).count(),
                    8
                );
            }
            for codec in codecs() {
                let label = format!("{version} {content:?} {partitioning:?} {codec:?}");
                assert_eq!(
                    stats(&reencoded(&bytes, codec)).unwrap(),
                    expected,
                    "{label}"
                );
                checked += 1;
            }
        }
    }
    assert_eq!(checked, 7 * 2 * 4);
}

#[tokio::test]
async fn reads_the_sizes_of_a_manifest_that_defines_a_named_type_twice() {
    let bytes = manifest(
        &FileIO::new_with_memory(),
        FormatVersion::V2,
        DataContentType::Data,
        Partitioning::Mixed,
        9,
    )
    .await;
    let repeated = with_schema(&bytes, define_named_types_repeatedly);

    assert_eq!(stats(&repeated).unwrap(), parsed_stats(&bytes));
}

#[test]
fn a_missing_size_or_record_count_is_no_value() {
    let bytes = avro_file(
        &entry_schema(
            r#", {"name": "status", "type": "long", "field-id": 0}"#,
            PATH_FIELD,
            "RECORD",
        ),
        &[&entry(Some(Value::Long(1)), path("s3://b/a.parquet"))],
    );

    assert_eq!(
        stats(&bytes).unwrap(),
        vec![(
            true,
            (
                Some(ManifestStatus::Added),
                DataContentType::Data,
                "s3://b/a.parquet".to_string(),
                None,
                None,
                None,
                None,
                None,
            )
        )]
    );
}

#[test]
fn reads_sizes_written_as_int_and_found_by_field_id() {
    let schema = entry_schema(
        "",
        r#"{"name": "file_path", "type": "string", "field-id": 100},
           {"name": "rows", "type": "int", "field-id": 103},
           {"name": "bytes", "type": ["null", "long"], "field-id": 104}"#,
        "RECORD",
    );
    let bytes = avro_file(
        &schema,
        &[&entry(
            None,
            record(&[
                ("file_path", "s3://b/a.parquet".into()),
                ("rows", Value::Int(12)),
                ("bytes", Value::Union(1, Box::new(Value::Long(3456)))),
            ]),
        )],
    );

    let rows = stats(&bytes).unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1.3, Some(3456));
    assert_eq!(rows[0].1.4, Some(12));
}

#[test]
fn an_unknown_status_is_no_status_and_keeps_the_file_alive() {
    let schema = entry_schema(
        r#", {"name": "status", "type": "long", "field-id": 0}"#,
        PATH_FIELD,
        "RECORD",
    );
    let statuses: Vec<_> = [0, 1, 2, 7]
        .into_iter()
        .map(|status| entry(Some(Value::Long(status)), path(&format!("s3://b/{status}"))))
        .collect();
    let bytes = avro_file(
        &schema,
        &statuses.iter().map(Vec::as_slice).collect::<Vec<_>>(),
    );

    let read: Vec<_> = stats(&bytes)
        .unwrap()
        .into_iter()
        .map(|(alive, row)| (alive, row.0))
        .collect();

    assert_eq!(
        read,
        [
            (true, Some(ManifestStatus::Existing)),
            (true, Some(ManifestStatus::Added)),
            (false, Some(ManifestStatus::Deleted)),
            (true, None),
        ]
    );
}
