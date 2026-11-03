//! Manifest list parsing for Iceberg tables.
//!
//! A manifest list is an Avro file that lists all manifest files for a snapshot,
//! along with partition field summaries for manifest-level pruning.
//!
//! # Iceberg Format v2
//!
//! Phase 2 targets Iceberg format v2, which includes:
//! - `content` field to distinguish data vs delete manifests
//! - `sequence_number` for snapshot ordering
//!
//! Format v1 tables (no content field) are NOT supported initially.

use apache_avro::types::Value as AvroValue;
use bytes::Bytes;

use crate::error::{IcebergError, Result};

/// Content type for manifest files.
///
/// Critical for skipping delete manifests (not supported in Phase 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ManifestContent {
    /// Data files (default)
    #[default]
    Data = 0,
    /// Delete files (equality or position deletes) - Phase 2 will SKIP these
    Deletes = 1,
}

impl ManifestContent {
    /// Classify a manifest-list `content` value (field 517): `0` is data, `1`
    /// is deletes. Any other value is one this reader cannot classify, and it
    /// is read as deletes: the merge-on-read guards then refuse the snapshot
    /// with their typed error, rather than plan an unknown manifest as data.
    pub fn from_avro(value: i32) -> Self {
        match value {
            0 => Self::Data,
            _ => Self::Deletes,
        }
    }
}

/// Partition field summary from manifest list.
///
/// Used for manifest-level partition pruning.
#[derive(Debug, Clone, Default)]
pub struct PartitionFieldSummary {
    /// Whether this field contains any null values
    pub contains_null: bool,
    /// Whether this field contains any NaN values (for float/double)
    pub contains_nan: Option<bool>,
    /// Lower bound for this field (Iceberg binary encoding)
    pub lower_bound: Option<Vec<u8>>,
    /// Upper bound for this field (Iceberg binary encoding)
    pub upper_bound: Option<Vec<u8>>,
}

/// Entry in a manifest list (points to a manifest file).
#[derive(Debug, Clone)]
pub struct ManifestListEntry {
    /// Path to the manifest file
    pub manifest_path: String,
    /// Length of the manifest file in bytes
    pub manifest_length: i64,
    /// Partition spec ID used by this manifest
    pub partition_spec_id: i32,
    /// Content type: Data or Deletes (MUST parse to skip deletes)
    pub content: ManifestContent,
    /// Sequence number when this manifest was added
    pub sequence_number: i64,
    /// Minimum sequence number of data files in this manifest
    pub min_sequence_number: i64,
    /// Snapshot ID that added this manifest
    pub added_snapshot_id: i64,
    /// `added_files_count` (field 504): entries with status ADDED. The six
    /// counters are optional in v1 and required in v2, so each is `None` when
    /// the writer omitted it. An unknown count must never read as zero.
    pub added_files_count: Option<i32>,
    /// `existing_files_count` (field 505): entries with status EXISTING.
    pub existing_files_count: Option<i32>,
    /// `deleted_files_count` (field 506): entries with status DELETED.
    pub deleted_files_count: Option<i32>,
    /// `added_rows_count` (field 512): rows in the files of ADDED entries.
    pub added_rows_count: Option<i64>,
    /// `existing_rows_count` (field 513): rows in the files of EXISTING entries.
    pub existing_rows_count: Option<i64>,
    /// `deleted_rows_count` (field 514): rows in the files of DELETED entries.
    pub deleted_rows_count: Option<i64>,
    /// Partition field summaries for manifest-level pruning
    pub partitions: Vec<PartitionFieldSummary>,
}

impl ManifestListEntry {
    /// Check if this is a data manifest (not deletes).
    pub fn is_data(&self) -> bool {
        self.content == ManifestContent::Data
    }

    /// Check if this is a delete manifest.
    pub fn is_deletes(&self) -> bool {
        self.content == ManifestContent::Deletes
    }

    /// Files this manifest lists as live: `added_files_count +
    /// existing_files_count`. DELETED entries are tombstones of files the
    /// snapshot removed, so they are not counted. `None` when either counter is
    /// absent or negative, or the sum overflows.
    pub fn live_files_count(&self) -> Option<i64> {
        live_total(
            self.added_files_count.map(i64::from),
            self.existing_files_count.map(i64::from),
        )
    }

    /// Rows in the files this manifest lists as live: `added_rows_count +
    /// existing_rows_count`, under the same rules as [`Self::live_files_count`].
    /// For a delete manifest these are delete records, not table rows.
    pub fn live_rows_count(&self) -> Option<i64> {
        live_total(self.added_rows_count, self.existing_rows_count)
    }
}

/// `added + existing`, or `None` when either is unknown or negative or the sum
/// overflows. A corrupt counter makes the total unknown rather than wrong.
fn live_total(added: Option<i64>, existing: Option<i64>) -> Option<i64> {
    let (added, existing) = (added?, existing?);
    if added < 0 || existing < 0 {
        return None;
    }
    added.checked_add(existing)
}

/// The Avro schema for manifest list entries (v2).
///
/// This is embedded in the manifest list file, but we define it here for reference.
#[allow(dead_code)]
const MANIFEST_LIST_SCHEMA_V2: &str = r#"{
  "type": "record",
  "name": "manifest_file",
  "fields": [
    {"name": "manifest_path", "type": "string"},
    {"name": "manifest_length", "type": "long"},
    {"name": "partition_spec_id", "type": "int"},
    {"name": "content", "type": "int", "default": 0},
    {"name": "sequence_number", "type": "long", "default": 0},
    {"name": "min_sequence_number", "type": "long", "default": 0},
    {"name": "added_snapshot_id", "type": "long"},
    {"name": "added_files_count", "type": "int", "default": 0},
    {"name": "existing_files_count", "type": "int", "default": 0},
    {"name": "deleted_files_count", "type": "int", "default": 0},
    {"name": "added_rows_count", "type": "long", "default": 0},
    {"name": "existing_rows_count", "type": "long", "default": 0},
    {"name": "deleted_rows_count", "type": "long", "default": 0},
    {"name": "partitions", "type": ["null", {
      "type": "array",
      "items": {
        "type": "record",
        "name": "field_summary",
        "fields": [
          {"name": "contains_null", "type": "boolean"},
          {"name": "contains_nan", "type": ["null", "boolean"], "default": null},
          {"name": "lower_bound", "type": ["null", "bytes"], "default": null},
          {"name": "upper_bound", "type": ["null", "bytes"], "default": null}
        ]
      }
    }], "default": null}
  ]
}"#;

/// Parse a manifest list from Avro bytes.
///
/// # Arguments
///
/// * `data` - The raw Avro file contents
///
/// # Returns
///
/// A vector of manifest list entries, with delete manifests filtered OUT
/// by default (Phase 2 does not support delete files).
pub fn parse_manifest_list(data: &Bytes) -> Result<Vec<ManifestListEntry>> {
    parse_manifest_list_with_deletes(data, false)
}

/// Parse a manifest list from Avro bytes, optionally including delete manifests.
///
/// # Arguments
///
/// * `data` - The raw Avro file contents
/// * `include_deletes` - Whether to include delete manifests (default: false)
pub fn parse_manifest_list_with_deletes(
    data: &Bytes,
    include_deletes: bool,
) -> Result<Vec<ManifestListEntry>> {
    let reader = apache_avro::Reader::new(&data[..])
        .map_err(|e| IcebergError::Manifest(format!("Failed to create Avro reader: {e}")))?;

    let mut entries = Vec::new();

    for value_result in reader {
        let value = value_result
            .map_err(|e| IcebergError::Manifest(format!("Failed to read Avro record: {e}")))?;

        let entry = parse_manifest_list_entry(&value)?;

        // Skip delete manifests unless explicitly requested
        if entry.is_deletes() && !include_deletes {
            tracing::debug!(
                manifest_path = %entry.manifest_path,
                "Skipping delete manifest (Phase 2 does not support delete files)"
            );
            continue;
        }

        entries.push(entry);
    }

    Ok(entries)
}

/// Parse a single manifest list entry from an Avro value.
fn parse_manifest_list_entry(value: &AvroValue) -> Result<ManifestListEntry> {
    let record = match value {
        AvroValue::Record(fields) => fields,
        _ => {
            return Err(IcebergError::Manifest(
                "Expected Avro record for manifest list entry".to_string(),
            ))
        }
    };

    // Helper to get field value
    let get_field = |name: &str| -> Option<&AvroValue> {
        record.iter().find(|(n, _)| n == name).map(|(_, v)| v)
    };

    // Required fields
    let manifest_path = match get_field("manifest_path") {
        Some(AvroValue::String(s)) => s.clone(),
        _ => {
            return Err(IcebergError::Manifest(
                "Missing or invalid manifest_path".to_string(),
            ))
        }
    };

    let manifest_length = match get_field("manifest_length") {
        Some(AvroValue::Long(l)) => *l,
        _ => {
            return Err(IcebergError::Manifest(
                "Missing or invalid manifest_length".to_string(),
            ))
        }
    };

    let partition_spec_id = match get_field("partition_spec_id") {
        Some(AvroValue::Int(i)) => *i,
        _ => {
            return Err(IcebergError::Manifest(
                "Missing or invalid partition_spec_id".to_string(),
            ))
        }
    };

    let added_snapshot_id = match get_field("added_snapshot_id") {
        Some(AvroValue::Long(l)) => *l,
        // Sometimes it might be stored as int
        Some(AvroValue::Int(i)) => *i as i64,
        _ => {
            return Err(IcebergError::Manifest(
                "Missing or invalid added_snapshot_id".to_string(),
            ))
        }
    };

    // `content` decides whether the merge-on-read guards see this manifest,
    // so it fails closed. Absent or null is the spec's default, 0 (a v1 list
    // has no such field, and v1 has no delete files). A present value is read
    // through a `["null", "int"]` union, and anything that is not an `int` is
    // as unclassifiable as an unknown `int`: both read as deletes.
    let content = match field_value(record, "content") {
        None | Some(AvroValue::Null) => ManifestContent::Data,
        Some(AvroValue::Int(i)) => ManifestContent::from_avro(*i),
        Some(_) => ManifestContent::Deletes,
    };

    // Optional fields with defaults

    let sequence_number = match get_field("sequence_number") {
        Some(AvroValue::Long(l)) => *l,
        _ => 0,
    };

    let min_sequence_number = match get_field("min_sequence_number") {
        Some(AvroValue::Long(l)) => *l,
        _ => 0,
    };

    // The file counters are named `added_files_count` etc. in the spec, and
    // pyiceberg and BigQuery write those names; Java writers before the rename
    // wrote `added_data_files_count` etc. Readers resolve by field id, so both
    // spellings are the same field.
    let added_files_count = int_counter(record, &["added_files_count", "added_data_files_count"]);
    let existing_files_count = int_counter(
        record,
        &["existing_files_count", "existing_data_files_count"],
    );
    let deleted_files_count =
        int_counter(record, &["deleted_files_count", "deleted_data_files_count"]);
    let added_rows_count = long_counter(record, "added_rows_count");
    let existing_rows_count = long_counter(record, "existing_rows_count");
    let deleted_rows_count = long_counter(record, "deleted_rows_count");

    // Parse partition summaries
    let partitions = match get_field("partitions") {
        Some(AvroValue::Union(_, boxed)) => match boxed.as_ref() {
            AvroValue::Array(items) => items
                .iter()
                .map(parse_partition_summary)
                .collect::<Result<Vec<_>>>()?,
            AvroValue::Null => Vec::new(),
            _ => Vec::new(),
        },
        Some(AvroValue::Array(items)) => items
            .iter()
            .map(parse_partition_summary)
            .collect::<Result<Vec<_>>>()?,
        _ => Vec::new(),
    };

    Ok(ManifestListEntry {
        manifest_path,
        manifest_length,
        partition_spec_id,
        content,
        sequence_number,
        min_sequence_number,
        added_snapshot_id,
        added_files_count,
        existing_files_count,
        deleted_files_count,
        added_rows_count,
        existing_rows_count,
        deleted_rows_count,
        partitions,
    })
}

/// A field's value with the `["null", T]` union an optional v1 field is written
/// as unwrapped, so a present value reads the same in v1 and v2.
fn field_value<'a>(record: &'a [(String, AvroValue)], name: &str) -> Option<&'a AvroValue> {
    let value = record.iter().find(|(n, _)| n == name).map(|(_, v)| v)?;
    Some(match value {
        AvroValue::Union(_, inner) => inner.as_ref(),
        other => other,
    })
}

/// An `int` counter under the first of `names` the record carries. `None` when
/// it is absent, null, or not an integer that fits.
fn int_counter(record: &[(String, AvroValue)], names: &[&str]) -> Option<i32> {
    let value = names.iter().find_map(|name| field_value(record, name))?;
    match value {
        AvroValue::Int(i) => Some(*i),
        AvroValue::Long(l) => i32::try_from(*l).ok(),
        _ => None,
    }
}

/// A `long` counter. `None` when it is absent, null, or not an integer.
fn long_counter(record: &[(String, AvroValue)], name: &str) -> Option<i64> {
    match field_value(record, name)? {
        AvroValue::Long(l) => Some(*l),
        AvroValue::Int(i) => Some(i64::from(*i)),
        _ => None,
    }
}

/// Parse a partition field summary from Avro value.
fn parse_partition_summary(value: &AvroValue) -> Result<PartitionFieldSummary> {
    let record = match value {
        AvroValue::Record(fields) => fields,
        _ => {
            return Err(IcebergError::Manifest(
                "Expected Avro record for partition summary".to_string(),
            ))
        }
    };

    let get_field = |name: &str| -> Option<&AvroValue> {
        record.iter().find(|(n, _)| n == name).map(|(_, v)| v)
    };

    let contains_null = match get_field("contains_null") {
        Some(AvroValue::Boolean(b)) => *b,
        _ => false,
    };

    let contains_nan = match get_field("contains_nan") {
        Some(AvroValue::Union(_, boxed)) => match boxed.as_ref() {
            AvroValue::Boolean(b) => Some(*b),
            _ => None,
        },
        Some(AvroValue::Boolean(b)) => Some(*b),
        _ => None,
    };

    let lower_bound = match get_field("lower_bound") {
        Some(AvroValue::Union(_, boxed)) => match boxed.as_ref() {
            AvroValue::Bytes(b) => Some(b.clone()),
            _ => None,
        },
        Some(AvroValue::Bytes(b)) => Some(b.clone()),
        _ => None,
    };

    let upper_bound = match get_field("upper_bound") {
        Some(AvroValue::Union(_, boxed)) => match boxed.as_ref() {
            AvroValue::Bytes(b) => Some(b.clone()),
            _ => None,
        },
        Some(AvroValue::Bytes(b)) => Some(b.clone()),
        _ => None,
    };

    Ok(PartitionFieldSummary {
        contains_null,
        contains_nan,
        lower_bound,
        upper_bound,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::{types::Record, Schema, Writer};

    fn create_test_manifest_list() -> Bytes {
        let schema = Schema::parse_str(MANIFEST_LIST_SCHEMA_V2).unwrap();
        let mut writer = Writer::new(&schema, Vec::new());

        // Create a data manifest entry
        let mut record = Record::new(writer.schema()).unwrap();
        record.put("manifest_path", "s3://bucket/table/metadata/manifest1.avro");
        record.put("manifest_length", 1234i64);
        record.put("partition_spec_id", 0i32);
        record.put("content", 0i32); // Data
        record.put("sequence_number", 1i64);
        record.put("min_sequence_number", 1i64);
        record.put("added_snapshot_id", 100i64);
        record.put("added_files_count", 5i32);
        record.put("existing_files_count", 0i32);
        record.put("deleted_files_count", 0i32);
        record.put("added_rows_count", 1000i64);
        record.put("existing_rows_count", 0i64);
        record.put("deleted_rows_count", 0i64);
        record.put("partitions", AvroValue::Union(0, Box::new(AvroValue::Null)));

        writer.append(record).unwrap();

        // Create a delete manifest entry (should be skipped by default)
        let mut record2 = Record::new(writer.schema()).unwrap();
        record2.put(
            "manifest_path",
            "s3://bucket/table/metadata/manifest2-deletes.avro",
        );
        record2.put("manifest_length", 500i64);
        record2.put("partition_spec_id", 0i32);
        record2.put("content", 1i32); // Deletes
        record2.put("sequence_number", 2i64);
        record2.put("min_sequence_number", 2i64);
        record2.put("added_snapshot_id", 101i64);
        record2.put("added_files_count", 0i32);
        record2.put("existing_files_count", 0i32);
        record2.put("deleted_files_count", 0i32);
        record2.put("added_rows_count", 0i64);
        record2.put("existing_rows_count", 0i64);
        record2.put("deleted_rows_count", 50i64);
        record2.put("partitions", AvroValue::Union(0, Box::new(AvroValue::Null)));

        writer.append(record2).unwrap();

        Bytes::from(writer.into_inner().unwrap())
    }

    #[test]
    fn test_parse_manifest_list() {
        let data = create_test_manifest_list();
        let entries = parse_manifest_list(&data).unwrap();

        // Should only have 1 entry (delete manifest filtered out)
        assert_eq!(entries.len(), 1);

        let entry = &entries[0];
        assert_eq!(
            entry.manifest_path,
            "s3://bucket/table/metadata/manifest1.avro"
        );
        assert_eq!(entry.manifest_length, 1234);
        assert_eq!(entry.partition_spec_id, 0);
        assert_eq!(entry.content, ManifestContent::Data);
        assert!(entry.is_data());
        assert!(!entry.is_deletes());
        assert_eq!(entry.sequence_number, 1);
        assert_eq!(entry.added_snapshot_id, 100);
        assert_eq!(entry.added_files_count, Some(5));
        assert_eq!(entry.added_rows_count, Some(1000));
        assert_eq!(entry.live_files_count(), Some(5));
        assert_eq!(entry.live_rows_count(), Some(1000));
    }

    /// One manifest-list record with the given counter fields, as a writer whose
    /// schema carries exactly those fields would encode it.
    fn manifest_list_with_counters(counter_fields: &str, counters: &[(&str, AvroValue)]) -> Bytes {
        let schema_json = format!(
            r#"{{"type": "record", "name": "manifest_file", "fields": [
                {{"name": "manifest_path", "type": "string"}},
                {{"name": "manifest_length", "type": "long"}},
                {{"name": "partition_spec_id", "type": "int"}},
                {{"name": "content", "type": "int"}},
                {{"name": "added_snapshot_id", "type": "long"}}
                {counter_fields}
            ]}}"#
        );
        let schema = Schema::parse_str(&schema_json).unwrap();
        let mut writer = Writer::new(&schema, Vec::new());
        let mut record = Record::new(writer.schema()).unwrap();
        record.put("manifest_path", "s3://bucket/t/metadata/m.avro");
        record.put("manifest_length", 10i64);
        record.put("partition_spec_id", 0i32);
        record.put("content", 0i32);
        record.put("added_snapshot_id", 1i64);
        for (name, value) in counters {
            record.put(name, value.clone());
        }
        writer.append(record).unwrap();
        Bytes::from(writer.into_inner().unwrap())
    }

    #[test]
    fn legacy_java_file_counter_names_still_parse() {
        let data = manifest_list_with_counters(
            r#", {"name": "added_data_files_count", "type": "int"},
                 {"name": "existing_data_files_count", "type": "int"},
                 {"name": "deleted_data_files_count", "type": "int"}"#,
            &[
                ("added_data_files_count", AvroValue::Int(2)),
                ("existing_data_files_count", AvroValue::Int(3)),
                ("deleted_data_files_count", AvroValue::Int(1)),
            ],
        );
        let entry = &parse_manifest_list(&data).unwrap()[0];
        assert_eq!(entry.added_files_count, Some(2));
        assert_eq!(entry.existing_files_count, Some(3));
        assert_eq!(entry.deleted_files_count, Some(1));
        assert_eq!(entry.live_files_count(), Some(5));
    }

    #[test]
    fn absent_or_null_counters_are_unknown_not_zero() {
        // A v1 writer may omit the counters or write them as `["null", T]`.
        let data = manifest_list_with_counters(
            r#", {"name": "added_rows_count", "type": ["null", "long"]},
                 {"name": "existing_rows_count", "type": ["null", "long"]}"#,
            &[
                (
                    "added_rows_count",
                    AvroValue::Union(1, Box::new(AvroValue::Long(7))),
                ),
                (
                    "existing_rows_count",
                    AvroValue::Union(0, Box::new(AvroValue::Null)),
                ),
            ],
        );
        let entry = &parse_manifest_list(&data).unwrap()[0];
        assert_eq!(
            entry.added_rows_count,
            Some(7),
            "a union-wrapped value reads"
        );
        assert_eq!(entry.existing_rows_count, None, "a null counter is unknown");
        assert_eq!(entry.live_rows_count(), None);
        assert_eq!(
            entry.added_files_count, None,
            "an absent counter is unknown"
        );
        assert_eq!(entry.live_files_count(), None);
    }

    #[test]
    fn a_negative_or_overflowing_counter_makes_the_live_total_unknown() {
        let entry = |added: i64, existing: i64| {
            let data = manifest_list_with_counters(
                r#", {"name": "added_rows_count", "type": "long"},
                     {"name": "existing_rows_count", "type": "long"}"#,
                &[
                    ("added_rows_count", AvroValue::Long(added)),
                    ("existing_rows_count", AvroValue::Long(existing)),
                ],
            );
            parse_manifest_list(&data).unwrap().remove(0)
        };
        assert_eq!(entry(4, 6).live_rows_count(), Some(10));
        assert_eq!(entry(-1, 6).live_rows_count(), None);
        assert_eq!(entry(i64::MAX, 1).live_rows_count(), None);
    }

    #[test]
    fn test_parse_manifest_list_with_deletes() {
        let data = create_test_manifest_list();
        let entries = parse_manifest_list_with_deletes(&data, true).unwrap();

        // Should have both entries
        assert_eq!(entries.len(), 2);

        assert!(entries[0].is_data());
        assert!(entries[1].is_deletes());
        assert_eq!(entries[1].content, ManifestContent::Deletes);
    }

    #[test]
    fn test_manifest_content_from_avro() {
        assert_eq!(ManifestContent::from_avro(0), ManifestContent::Data);
        assert_eq!(ManifestContent::from_avro(1), ManifestContent::Deletes);
        // A value this reader cannot classify fails closed, as deletes.
        assert_eq!(ManifestContent::from_avro(2), ManifestContent::Deletes);
        assert_eq!(ManifestContent::from_avro(99), ManifestContent::Deletes);
        assert_eq!(ManifestContent::from_avro(-1), ManifestContent::Deletes);
    }

    /// One manifest-list record whose `content` field has the schema
    /// `content_type` (`None`: the field is absent) and the value `content`.
    fn manifest_list_with_content(content_type: Option<&str>, content: Option<AvroValue>) -> Bytes {
        let content_field = content_type
            .map(|t| format!(r#", {{"name": "content", "type": {t}}}"#))
            .unwrap_or_default();
        let schema_json = format!(
            r#"{{"type": "record", "name": "manifest_file", "fields": [
                {{"name": "manifest_path", "type": "string"}},
                {{"name": "manifest_length", "type": "long"}},
                {{"name": "partition_spec_id", "type": "int"}},
                {{"name": "added_snapshot_id", "type": "long"}}
                {content_field}
            ]}}"#
        );
        let schema = Schema::parse_str(&schema_json).unwrap();
        let mut writer = Writer::new(&schema, Vec::new());
        let mut record = Record::new(writer.schema()).unwrap();
        record.put("manifest_path", "s3://bucket/t/metadata/m.avro");
        record.put("manifest_length", 10i64);
        record.put("partition_spec_id", 0i32);
        record.put("added_snapshot_id", 1i64);
        if let Some(content) = content {
            record.put("content", content);
        }
        writer.append(record).unwrap();
        Bytes::from(writer.into_inner().unwrap())
    }

    fn parsed_content(data: &Bytes) -> ManifestContent {
        parse_manifest_list_with_deletes(data, true).unwrap()[0].content
    }

    #[test]
    fn a_union_wrapped_content_is_read_and_an_unknown_one_fails_closed() {
        let union = Some(r#"["null", "int"]"#);
        // A delete manifest written through a `["null", "int"]` union is one.
        let wrapped_delete = manifest_list_with_content(
            union,
            Some(AvroValue::Union(1, Box::new(AvroValue::Int(1)))),
        );
        assert_eq!(parsed_content(&wrapped_delete), ManifestContent::Deletes);
        assert!(
            parse_manifest_list(&wrapped_delete).unwrap().is_empty(),
            "the default parse filters it out as a delete manifest"
        );
        let wrapped_data = manifest_list_with_content(
            union,
            Some(AvroValue::Union(1, Box::new(AvroValue::Int(0)))),
        );
        assert_eq!(parsed_content(&wrapped_data), ManifestContent::Data);

        // Null or absent is the spec's default, data (v1 has no delete files).
        let null =
            manifest_list_with_content(union, Some(AvroValue::Union(0, Box::new(AvroValue::Null))));
        assert_eq!(parsed_content(&null), ManifestContent::Data);
        let absent = manifest_list_with_content(None, None);
        assert_eq!(parsed_content(&absent), ManifestContent::Data);

        // A value this reader cannot classify is deletes, plain or wrapped.
        let unknown = manifest_list_with_content(Some(r#""int""#), Some(AvroValue::Int(2)));
        assert_eq!(parsed_content(&unknown), ManifestContent::Deletes);
        let unknown_wrapped = manifest_list_with_content(
            union,
            Some(AvroValue::Union(1, Box::new(AvroValue::Int(7)))),
        );
        assert_eq!(parsed_content(&unknown_wrapped), ManifestContent::Deletes);
        let not_an_int = manifest_list_with_content(Some(r#""long""#), Some(AvroValue::Long(0)));
        assert_eq!(parsed_content(&not_an_int), ManifestContent::Deletes);
    }
}
