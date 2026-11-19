//! Readers of manifest lists and manifests.
//!
//! They decode one record at a time and keep only what their callers need: file paths, or sizes
//! and counts. A visit holds one Avro block and one record. They read what writers get wrong
//! elsewhere: a field is found by its Iceberg field id, else by its name in the spec, in any
//! order; unknown fields are skipped; fields the callers can do without may be missing or of
//! another integer type. Only a missing path fails, as the file it names would look unreferenced.

use apache_avro::{
    Reader, Schema,
    error::Details,
    schema::{RecordField, UnionSchema},
    types::Value,
};
use iceberg::spec::{DataContentType, Manifest, ManifestContentType, ManifestStatus};

use crate::operations::error::{ParseError, ReadError};

#[cfg(test)]
mod tests;

/// The fields of a manifest list entry needed to list a table's files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestListEntry {
    pub manifest_path: String,
    /// The size its writer recorded, which some writers get wrong; `None` if missing.
    pub manifest_length: Option<i64>,
    /// `None` if missing.
    pub partition_spec_id: Option<i32>,
}

/// A manifest list entry with its counts. Every field but the path is `None` if missing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestListEntryFull {
    pub manifest_path: String,
    /// The size its writer recorded, which some writers get wrong.
    pub manifest_length: Option<i64>,
    pub partition_spec_id: Option<i32>,
    /// Data when the entry does not say, as in format version 1.
    pub content: ManifestContentType,
    pub sequence_number: Option<i64>,
    pub added_snapshot_id: Option<i64>,
    pub added_files_count: Option<i64>,
    pub existing_files_count: Option<i64>,
    pub deleted_files_count: Option<i64>,
    pub added_rows_count: Option<i64>,
    pub existing_rows_count: Option<i64>,
    pub deleted_rows_count: Option<i64>,
}

impl From<ManifestListEntryFull> for ManifestListEntry {
    fn from(entry: ManifestListEntryFull) -> Self {
        Self {
            manifest_path: entry.manifest_path,
            manifest_length: entry.manifest_length,
            partition_spec_id: entry.partition_spec_id,
        }
    }
}

/// The fields of a manifest entry needed to list a table's files.
#[derive(Debug, Clone, Copy)]
pub struct ManifestEntryPaths<'a> {
    /// `false` only for an entry whose status says the file was deleted.
    pub alive: bool,
    /// Data when the entry does not say.
    pub content: DataContentType,
    pub file_path: &'a str,
    /// Set for position deletes and deletion vectors that name their data file.
    pub referenced_data_file: Option<&'a str>,
}

/// A manifest entry with the sizes and counts of its file. Every optional field is `None` if
/// missing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestEntryStats<'a> {
    /// `false` only for an entry whose status says the file was deleted.
    pub alive: bool,
    /// `None` if missing or not a status the spec defines.
    pub status: Option<ManifestStatus>,
    /// Data when the entry does not say.
    pub content: DataContentType,
    pub file_path: &'a str,
    pub file_size_in_bytes: Option<i64>,
    pub record_count: Option<i64>,
    /// Set for position deletes and deletion vectors that name their data file.
    pub referenced_data_file: Option<&'a str>,
    /// Where a deletion vector starts in its Puffin file.
    pub content_offset: Option<i64>,
    /// The size of a deletion vector in its Puffin file.
    pub content_size_in_bytes: Option<i64>,
}

impl<'a> From<ManifestEntryStats<'a>> for ManifestEntryPaths<'a> {
    fn from(entry: ManifestEntryStats<'a>) -> Self {
        Self {
            alive: entry.alive,
            content: entry.content,
            file_path: entry.file_path,
            referenced_data_file: entry.referenced_data_file,
        }
    }
}

/// A field a reader looks for: its name and field id in the Iceberg spec.
#[derive(Debug, Clone, Copy)]
struct SpecField {
    name: &'static str,
    id: i64,
}

const MANIFEST_PATH: SpecField = SpecField {
    name: "manifest_path",
    id: 500,
};
const MANIFEST_LENGTH: SpecField = SpecField {
    name: "manifest_length",
    id: 501,
};
const PARTITION_SPEC_ID: SpecField = SpecField {
    name: "partition_spec_id",
    id: 502,
};
const CONTENT_OF_MANIFEST: SpecField = SpecField {
    name: "content",
    id: 517,
};
const SEQUENCE_NUMBER: SpecField = SpecField {
    name: "sequence_number",
    id: 515,
};
const ADDED_SNAPSHOT_ID: SpecField = SpecField {
    name: "added_snapshot_id",
    id: 503,
};
// Format version 1 names the file counts `added_data_files_count` and so on.
const ADDED_FILES_COUNT: SpecField = SpecField {
    name: "added_files_count",
    id: 504,
};
const ADDED_FILES_COUNT_V1: &str = "added_data_files_count";
const EXISTING_FILES_COUNT: SpecField = SpecField {
    name: "existing_files_count",
    id: 505,
};
const EXISTING_FILES_COUNT_V1: &str = "existing_data_files_count";
const DELETED_FILES_COUNT: SpecField = SpecField {
    name: "deleted_files_count",
    id: 506,
};
const DELETED_FILES_COUNT_V1: &str = "deleted_data_files_count";
const ADDED_ROWS_COUNT: SpecField = SpecField {
    name: "added_rows_count",
    id: 512,
};
const EXISTING_ROWS_COUNT: SpecField = SpecField {
    name: "existing_rows_count",
    id: 513,
};
const DELETED_ROWS_COUNT: SpecField = SpecField {
    name: "deleted_rows_count",
    id: 514,
};
const STATUS: SpecField = SpecField {
    name: "status",
    id: 0,
};
const DATA_FILE: SpecField = SpecField {
    name: "data_file",
    id: 2,
};
const CONTENT: SpecField = SpecField {
    name: "content",
    id: 134,
};
const FILE_PATH: SpecField = SpecField {
    name: "file_path",
    id: 100,
};
const REFERENCED_DATA_FILE: SpecField = SpecField {
    name: "referenced_data_file",
    id: 143,
};
const RECORD_COUNT: SpecField = SpecField {
    name: "record_count",
    id: 103,
};
const FILE_SIZE_IN_BYTES: SpecField = SpecField {
    name: "file_size_in_bytes",
    id: 104,
};
const CONTENT_OFFSET: SpecField = SpecField {
    name: "content_offset",
    id: 144,
};
const CONTENT_SIZE_IN_BYTES: SpecField = SpecField {
    name: "content_size_in_bytes",
    id: 145,
};

/// Decodes the manifest list in `bytes` and passes each entry to `visit`.
///
/// # Errors
/// Returns a [`ParseError`] if the file is no Avro file or an entry has no manifest path, or the
/// first error of `visit`.
pub fn visit_manifest_list(
    bytes: &[u8],
    path: &str,
    mut visit: impl FnMut(ManifestListEntry) -> Result<(), ReadError>,
) -> Result<(), ReadError> {
    visit_manifest_list_full(bytes, path, |entry| visit(entry.into()))
}

/// [`visit_manifest_list`] with the content, sequence number, snapshot and counts of each entry.
///
/// # Errors
/// Returns a [`ParseError`] if the file is no Avro file or an entry has no manifest path, or the
/// first error of `visit`.
pub fn visit_manifest_list_full(
    bytes: &[u8],
    path: &str,
    mut visit: impl FnMut(ManifestListEntryFull) -> Result<(), ReadError>,
) -> Result<(), ReadError> {
    let parse_error = parse_error("Manifest List", path);
    let reader = Reader::new(bytes).map_err(|e| parse_error(e.to_string()))?;
    let layout = ListLayout::of(reader.writer_schema()).map_err(&parse_error)?;
    for record in reader {
        let record = record.map_err(|e| parse_error(e.to_string()))?;
        let entry = layout.entry(record).map_err(&parse_error)?;
        visit(entry)?;
    }
    Ok(())
}

/// Decodes the manifest in `bytes` and passes each entry to `visit`.
///
/// A file whose schema defines an Avro named type more than once, as older iceberg-rust releases
/// wrote, or names a field in a way Avro does not allow, as Java writes partition fields with
/// non-ASCII names, is read with iceberg-rust's parser, which holds the whole manifest.
///
/// # Errors
/// Returns a [`ParseError`] if the file is no Avro file or an entry has no data file path, or the
/// first error of `visit`.
pub fn visit_manifest(
    bytes: &[u8],
    path: &str,
    mut visit: impl FnMut(ManifestEntryPaths<'_>) -> Result<(), ReadError>,
) -> Result<(), ReadError> {
    visit_manifest_stats(bytes, path, |entry| visit(entry.into()))
}

/// [`visit_manifest`] with the status, size, record count and deletion-vector range of each
/// entry's file. A missing size or count is `None`, not an error.
///
/// # Errors
/// Returns a [`ParseError`] if the file is no Avro file or an entry has no data file path, or the
/// first error of `visit`.
pub fn visit_manifest_stats(
    bytes: &[u8],
    path: &str,
    mut visit: impl FnMut(ManifestEntryStats<'_>) -> Result<(), ReadError>,
) -> Result<(), ReadError> {
    let parse_error = parse_error("Manifest (Avro)", path);
    let reader = match Reader::new(bytes) {
        Ok(reader) => reader,
        Err(e)
            if matches!(
                e.details(),
                Details::AmbiguousSchemaDefinition(_) | Details::FieldName(_)
            ) =>
        {
            return visit_parsed_manifest(bytes, path, visit);
        }
        Err(e) => return Err(parse_error(e.to_string()).into()),
    };
    let layout = EntryLayout::of(reader.writer_schema()).map_err(&parse_error)?;
    for record in reader {
        let record = record.map_err(|e| parse_error(e.to_string()))?;
        let entry = layout.entry(record).map_err(&parse_error)?;
        visit(ManifestEntryStats {
            alive: entry.status != Some(ManifestStatus::Deleted),
            status: entry.status,
            content: entry.content,
            file_path: &entry.file_path,
            file_size_in_bytes: entry.file_size_in_bytes,
            record_count: entry.record_count,
            referenced_data_file: entry.referenced_data_file.as_deref(),
            content_offset: entry.content_offset,
            content_size_in_bytes: entry.content_size_in_bytes,
        })?;
    }
    Ok(())
}

/// [`visit_manifest_stats`] through iceberg-rust's parser.
fn visit_parsed_manifest(
    bytes: &[u8],
    path: &str,
    mut visit: impl FnMut(ManifestEntryStats<'_>) -> Result<(), ReadError>,
) -> Result<(), ReadError> {
    let (_metadata, entries) = Manifest::try_from_avro_bytes(bytes, Some(path))
        .map_err(|e| parse_error("Manifest (Avro)", path)(e.to_string()))?;
    for entry in &entries {
        let data_file = entry.data_file();
        let referenced_data_file = data_file.referenced_data_file();
        visit(ManifestEntryStats {
            alive: entry.status() != ManifestStatus::Deleted,
            status: Some(entry.status()),
            content: data_file.content_type(),
            file_path: data_file.file_path(),
            file_size_in_bytes: i64::try_from(data_file.file_size_in_bytes()).ok(),
            record_count: i64::try_from(data_file.record_count()).ok(),
            referenced_data_file: referenced_data_file.as_deref(),
            content_offset: data_file.content_offset(),
            content_size_in_bytes: data_file.content_size_in_bytes(),
        })?;
    }
    Ok(())
}

fn parse_error(target_type: &str, path: &str) -> impl Fn(String) -> ParseError {
    move |reason| ParseError {
        target_type: target_type.to_string(),
        reason,
        path: path.to_string(),
    }
}

/// Where the fields of a manifest list entry sit in the writer's records.
#[derive(Debug)]
struct ListLayout {
    manifest_path: usize,
    manifest_length: Option<usize>,
    partition_spec_id: Option<usize>,
    content: Option<usize>,
    sequence_number: Option<usize>,
    added_snapshot_id: Option<usize>,
    added_files_count: Option<usize>,
    existing_files_count: Option<usize>,
    deleted_files_count: Option<usize>,
    added_rows_count: Option<usize>,
    existing_rows_count: Option<usize>,
    deleted_rows_count: Option<usize>,
}

impl ListLayout {
    fn of(schema: &Schema) -> Result<Self, String> {
        let fields = record_fields(schema).ok_or("a manifest list entry is not a record")?;
        Ok(Self {
            manifest_path: position(fields, MANIFEST_PATH)
                .ok_or("missing field `manifest_path`")?,
            manifest_length: position(fields, MANIFEST_LENGTH),
            partition_spec_id: position(fields, PARTITION_SPEC_ID),
            content: position(fields, CONTENT_OF_MANIFEST),
            sequence_number: position(fields, SEQUENCE_NUMBER),
            added_snapshot_id: position(fields, ADDED_SNAPSHOT_ID),
            added_files_count: position(fields, ADDED_FILES_COUNT)
                .or_else(|| position_of_name(fields, ADDED_FILES_COUNT_V1)),
            existing_files_count: position(fields, EXISTING_FILES_COUNT)
                .or_else(|| position_of_name(fields, EXISTING_FILES_COUNT_V1)),
            deleted_files_count: position(fields, DELETED_FILES_COUNT)
                .or_else(|| position_of_name(fields, DELETED_FILES_COUNT_V1)),
            added_rows_count: position(fields, ADDED_ROWS_COUNT),
            existing_rows_count: position(fields, EXISTING_ROWS_COUNT),
            deleted_rows_count: position(fields, DELETED_ROWS_COUNT),
        })
    }

    fn entry(&self, record: Value) -> Result<ManifestListEntryFull, String> {
        let Value::Record(mut fields) = record else {
            return Err("a manifest list entry is not a record".to_string());
        };
        let manifest_path = match take(&mut fields, Some(self.manifest_path)) {
            Some(Value::String(path)) => path,
            other => return Err(format!("field `manifest_path` is not a string: {other:?}")),
        };
        let mut integer_at = |position| take(&mut fields, position).as_ref().and_then(integer);
        Ok(ManifestListEntryFull {
            manifest_path,
            manifest_length: integer_at(self.manifest_length),
            partition_spec_id: integer_at(self.partition_spec_id)
                .and_then(|id| i32::try_from(id).ok()),
            content: match integer_at(self.content) {
                Some(1) => ManifestContentType::Deletes,
                _ => ManifestContentType::Data,
            },
            sequence_number: integer_at(self.sequence_number),
            added_snapshot_id: integer_at(self.added_snapshot_id),
            added_files_count: integer_at(self.added_files_count),
            existing_files_count: integer_at(self.existing_files_count),
            deleted_files_count: integer_at(self.deleted_files_count),
            added_rows_count: integer_at(self.added_rows_count),
            existing_rows_count: integer_at(self.existing_rows_count),
            deleted_rows_count: integer_at(self.deleted_rows_count),
        })
    }
}

/// Where the fields of a manifest entry and of its data file sit in the writer's records.
#[derive(Debug)]
struct EntryLayout {
    status: Option<usize>,
    data_file: usize,
    content: Option<usize>,
    file_path: usize,
    referenced_data_file: Option<usize>,
    record_count: Option<usize>,
    file_size_in_bytes: Option<usize>,
    content_offset: Option<usize>,
    content_size_in_bytes: Option<usize>,
}

/// A manifest entry as [`EntryLayout::entry`] reads it.
#[derive(Debug)]
struct Entry {
    status: Option<ManifestStatus>,
    content: DataContentType,
    file_path: String,
    referenced_data_file: Option<String>,
    record_count: Option<i64>,
    file_size_in_bytes: Option<i64>,
    content_offset: Option<i64>,
    content_size_in_bytes: Option<i64>,
}

impl EntryLayout {
    fn of(schema: &Schema) -> Result<Self, String> {
        let fields = record_fields(schema).ok_or("a manifest entry is not a record")?;
        let data_file = position(fields, DATA_FILE).ok_or("missing field `data_file`")?;
        let data_file_fields =
            record_fields(&fields[data_file].schema).ok_or("field `data_file` is not a record")?;
        Ok(Self {
            status: position(fields, STATUS),
            data_file,
            content: position(data_file_fields, CONTENT),
            file_path: position(data_file_fields, FILE_PATH)
                .ok_or("missing field `data_file.file_path`")?,
            referenced_data_file: position(data_file_fields, REFERENCED_DATA_FILE),
            record_count: position(data_file_fields, RECORD_COUNT),
            file_size_in_bytes: position(data_file_fields, FILE_SIZE_IN_BYTES),
            content_offset: position(data_file_fields, CONTENT_OFFSET),
            content_size_in_bytes: position(data_file_fields, CONTENT_SIZE_IN_BYTES),
        })
    }

    fn entry(&self, record: Value) -> Result<Entry, String> {
        let Value::Record(mut fields) = record else {
            return Err("a manifest entry is not a record".to_string());
        };
        // A missing or unknown status keeps the file alive.
        let status = match take(&mut fields, self.status).as_ref().and_then(integer) {
            Some(0) => Some(ManifestStatus::Existing),
            Some(1) => Some(ManifestStatus::Added),
            Some(2) => Some(ManifestStatus::Deleted),
            _ => None,
        };
        let Some(Value::Record(mut data_file)) = take(&mut fields, Some(self.data_file)) else {
            return Err("field `data_file` is not a record".to_string());
        };
        let content = match take(&mut data_file, self.content)
            .as_ref()
            .and_then(integer)
        {
            Some(1) => DataContentType::PositionDeletes,
            Some(2) => DataContentType::EqualityDeletes,
            _ => DataContentType::Data,
        };
        let file_path = match take(&mut data_file, Some(self.file_path)) {
            Some(Value::String(path)) => path,
            other => {
                return Err(format!(
                    "field `data_file.file_path` is not a string: {other:?}"
                ));
            }
        };
        let referenced_data_file = match take(&mut data_file, self.referenced_data_file) {
            Some(Value::String(path)) => Some(path),
            _ => None,
        };
        let mut integer_at = |position| take(&mut data_file, position).as_ref().and_then(integer);
        Ok(Entry {
            status,
            content,
            file_path,
            referenced_data_file,
            record_count: integer_at(self.record_count),
            file_size_in_bytes: integer_at(self.file_size_in_bytes),
            content_offset: integer_at(self.content_offset),
            content_size_in_bytes: integer_at(self.content_size_in_bytes),
        })
    }
}

/// The fields of a record schema, also behind a union with null.
fn record_fields(schema: &Schema) -> Option<&[RecordField]> {
    match schema {
        Schema::Record(record) => Some(&record.fields),
        Schema::Union(union) => union_record_fields(union),
        _ => None,
    }
}

fn union_record_fields(union: &UnionSchema) -> Option<&[RecordField]> {
    union.variants().iter().find_map(|variant| match variant {
        Schema::Record(record) => Some(record.fields.as_slice()),
        _ => None,
    })
}

/// The position of `field` among the writer's `fields`: the field of its field id, which the
/// spec makes authoritative, else the field of its name.
fn position(fields: &[RecordField], field: SpecField) -> Option<usize> {
    fields
        .iter()
        .position(|f| field_id(f) == Some(field.id))
        .or_else(|| fields.iter().position(|f| f.name == field.name))
}

/// The position of the field named `name`, for names an older format version used.
fn position_of_name(fields: &[RecordField], name: &str) -> Option<usize> {
    fields.iter().position(|f| f.name == name)
}

fn field_id(field: &RecordField) -> Option<i64> {
    let id = field.custom_attributes.get("field-id")?;
    id.as_i64().or_else(|| id.as_str()?.parse().ok())
}

/// Takes the value at `position` out of a record's fields, unwrapped from its union.
fn take(fields: &mut [(String, Value)], position: Option<usize>) -> Option<Value> {
    let (_, value) = fields.get_mut(position?)?;
    match std::mem::replace(value, Value::Null) {
        Value::Union(_, inner) => Some(*inner),
        value => Some(value),
    }
}

fn integer(value: &Value) -> Option<i64> {
    match value {
        Value::Int(v) => Some((*v).into()),
        Value::Long(v) => Some(*v),
        _ => None,
    }
}
