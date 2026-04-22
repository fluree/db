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
    /// Parse from Avro integer value.
    pub fn from_avro(value: i32) -> Self {
        match value {
            1 => Self::Deletes,
            _ => Self::Data,
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
    /// Number of data files added by this manifest
    pub added_data_files_count: i32,
    /// Number of data files with existing status in this manifest
    pub existing_data_files_count: i32,
    /// Number of data files deleted by this manifest
    pub deleted_data_files_count: i32,
    /// Number of rows added by this manifest
    pub added_rows_count: i64,
    /// Number of rows in existing data files
    pub existing_rows_count: i64,
    /// Number of rows deleted by this manifest
    pub deleted_rows_count: i64,
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

    /// Total number of data files in this manifest.
    pub fn total_data_files(&self) -> i32 {
        self.added_data_files_count + self.existing_data_files_count
    }
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
    {"name": "added_data_files_count", "type": "int", "default": 0},
    {"name": "existing_data_files_count", "type": "int", "default": 0},
    {"name": "deleted_data_files_count", "type": "int", "default": 0},
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

    // Optional fields with defaults
    let content = match get_field("content") {
        Some(AvroValue::Int(i)) => ManifestContent::from_avro(*i),
        _ => ManifestContent::Data,
    };

    let sequence_number = match get_field("sequence_number") {
        Some(AvroValue::Long(l)) => *l,
        _ => 0,
    };

    let min_sequence_number = match get_field("min_sequence_number") {
        Some(AvroValue::Long(l)) => *l,
        _ => 0,
    };

    let added_data_files_count = match get_field("added_data_files_count") {
        Some(AvroValue::Int(i)) => *i,
        _ => 0,
    };

    let existing_data_files_count = match get_field("existing_data_files_count") {
        Some(AvroValue::Int(i)) => *i,
        _ => 0,
    };

    let deleted_data_files_count = match get_field("deleted_data_files_count") {
        Some(AvroValue::Int(i)) => *i,
        _ => 0,
    };

    let added_rows_count = match get_field("added_rows_count") {
        Some(AvroValue::Long(l)) => *l,
        _ => 0,
    };

    let existing_rows_count = match get_field("existing_rows_count") {
        Some(AvroValue::Long(l)) => *l,
        _ => 0,
    };

    let deleted_rows_count = match get_field("deleted_rows_count") {
        Some(AvroValue::Long(l)) => *l,
        _ => 0,
    };

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
        added_data_files_count,
        existing_data_files_count,
        deleted_data_files_count,
        added_rows_count,
        existing_rows_count,
        deleted_rows_count,
        partitions,
    })
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
        record.put("added_data_files_count", 5i32);
        record.put("existing_data_files_count", 0i32);
        record.put("deleted_data_files_count", 0i32);
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
        record2.put("added_data_files_count", 0i32);
        record2.put("existing_data_files_count", 0i32);
        record2.put("deleted_data_files_count", 0i32);
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
        assert_eq!(entry.added_data_files_count, 5);
        assert_eq!(entry.added_rows_count, 1000);
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
        assert_eq!(ManifestContent::from_avro(99), ManifestContent::Data); // Unknown defaults to Data
    }
}
