//! Manifest file parsing for Iceberg tables.
//!
//! A manifest file is an Avro file that lists data files with their:
//! - File location and format
//! - Column statistics (min/max bounds, null counts)
//! - Partition values
//!
//! These statistics enable file-level pruning during scan planning.

use std::collections::HashMap;

use apache_avro::types::Value as AvroValue;
use bytes::Bytes;

use crate::error::{IcebergError, Result};

/// File format for data files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FileFormat {
    /// Parquet format (default, supported)
    #[default]
    Parquet,
    /// ORC format (not supported in Phase 2)
    Orc,
    /// Avro format (not supported in Phase 2)
    Avro,
}

impl FileFormat {
    /// Parse from string.
    pub fn parse(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "PARQUET" => Self::Parquet,
            "ORC" => Self::Orc,
            "AVRO" => Self::Avro,
            _ => Self::Parquet, // Default to Parquet
        }
    }

    /// Check if this format is supported in Phase 2.
    pub fn is_supported(&self) -> bool {
        matches!(self, Self::Parquet)
    }
}

/// Status of a manifest entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ManifestEntryStatus {
    /// Existing file (status = 0)
    #[default]
    Existing = 0,
    /// Added file (status = 1)
    Added = 1,
    /// Deleted file (status = 2)
    Deleted = 2,
}

impl ManifestEntryStatus {
    /// Parse from Avro integer value.
    pub fn from_avro(value: i32) -> Self {
        match value {
            1 => Self::Added,
            2 => Self::Deleted,
            _ => Self::Existing,
        }
    }

    /// Check if this is an active (non-deleted) entry.
    pub fn is_active(&self) -> bool {
        !matches!(self, Self::Deleted)
    }
}

/// Partition data for a data file.
#[derive(Debug, Clone, Default)]
pub struct PartitionData {
    /// Partition field values (field_id -> value bytes)
    pub values: HashMap<i32, Option<Vec<u8>>>,
}

/// A data file entry in a manifest.
#[derive(Debug, Clone)]
pub struct DataFile {
    /// Path to the data file
    pub file_path: String,
    /// File format (Parquet, ORC, Avro)
    pub file_format: FileFormat,
    /// Number of records in the file
    pub record_count: i64,
    /// Size of the file in bytes
    pub file_size_in_bytes: i64,
    /// Partition values
    pub partition: PartitionData,
    /// Column sizes (field_id -> size in bytes)
    pub column_sizes: Option<HashMap<i32, i64>>,
    /// Value counts per column (field_id -> count)
    pub value_counts: Option<HashMap<i32, i64>>,
    /// Null value counts per column (field_id -> count)
    pub null_value_counts: Option<HashMap<i32, i64>>,
    /// NaN counts per column (field_id -> count)
    pub nan_value_counts: Option<HashMap<i32, i64>>,
    /// Lower bounds per column (field_id -> encoded bytes)
    pub lower_bounds: Option<HashMap<i32, Vec<u8>>>,
    /// Upper bounds per column (field_id -> encoded bytes)
    pub upper_bounds: Option<HashMap<i32, Vec<u8>>>,
    /// Split offsets for the file
    pub split_offsets: Option<Vec<i64>>,
    /// Sort order ID used by this file
    pub sort_order_id: Option<i32>,
}

impl DataFile {
    /// Check if this file format is supported.
    pub fn is_supported(&self) -> bool {
        self.file_format.is_supported()
    }

    /// Get lower bound for a column by field ID.
    pub fn lower_bound(&self, field_id: i32) -> Option<&[u8]> {
        self.lower_bounds
            .as_ref()
            .and_then(|m| m.get(&field_id))
            .map(std::vec::Vec::as_slice)
    }

    /// Get upper bound for a column by field ID.
    pub fn upper_bound(&self, field_id: i32) -> Option<&[u8]> {
        self.upper_bounds
            .as_ref()
            .and_then(|m| m.get(&field_id))
            .map(std::vec::Vec::as_slice)
    }

    /// Get null count for a column by field ID.
    pub fn null_count(&self, field_id: i32) -> Option<i64> {
        self.null_value_counts
            .as_ref()
            .and_then(|m| m.get(&field_id))
            .copied()
    }

    /// Check if a column might contain non-null values.
    pub fn might_contain_values(&self, field_id: i32) -> bool {
        // If we don't have null counts, assume it might have values
        let null_count = self.null_count(field_id).unwrap_or(0);

        // If we don't have value counts, assume it might have values
        let value_count = self
            .value_counts
            .as_ref()
            .and_then(|m| m.get(&field_id))
            .copied()
            .unwrap_or(self.record_count);

        value_count > null_count
    }
}

/// A manifest entry (wraps DataFile with status and sequence info).
#[derive(Debug, Clone)]
pub struct ManifestEntry {
    /// Entry status (existing, added, deleted)
    pub status: ManifestEntryStatus,
    /// Snapshot ID that added this file (null for existing v1 files)
    pub snapshot_id: Option<i64>,
    /// Sequence number when this file was added
    pub sequence_number: Option<i64>,
    /// File sequence number
    pub file_sequence_number: Option<i64>,
    /// The data file
    pub data_file: DataFile,
}

impl ManifestEntry {
    /// Check if this entry is active (not deleted).
    pub fn is_active(&self) -> bool {
        self.status.is_active()
    }
}

/// Parse a manifest file from Avro bytes.
///
/// # Arguments
///
/// * `data` - The raw Avro file contents
///
/// # Returns
///
/// A vector of manifest entries (only active/non-deleted entries by default).
pub fn parse_manifest(data: &Bytes) -> Result<Vec<ManifestEntry>> {
    parse_manifest_with_deleted(data, false)
}

/// Parse a manifest file, optionally including deleted entries.
pub fn parse_manifest_with_deleted(
    data: &Bytes,
    include_deleted: bool,
) -> Result<Vec<ManifestEntry>> {
    let reader = apache_avro::Reader::new(&data[..])
        .map_err(|e| IcebergError::Manifest(format!("Failed to create Avro reader: {e}")))?;

    let mut entries = Vec::new();

    for value_result in reader {
        let value = value_result
            .map_err(|e| IcebergError::Manifest(format!("Failed to read Avro record: {e}")))?;

        let entry = parse_manifest_entry(&value)?;

        // Skip deleted entries unless explicitly requested
        if !entry.is_active() && !include_deleted {
            continue;
        }

        // Skip unsupported file formats
        if !entry.data_file.is_supported() {
            tracing::warn!(
                file_path = %entry.data_file.file_path,
                format = ?entry.data_file.file_format,
                "Skipping unsupported file format (Phase 2 only supports Parquet)"
            );
            continue;
        }

        entries.push(entry);
    }

    Ok(entries)
}

/// Parse a single manifest entry from an Avro value.
fn parse_manifest_entry(value: &AvroValue) -> Result<ManifestEntry> {
    let record = match value {
        AvroValue::Record(fields) => fields,
        _ => {
            return Err(IcebergError::Manifest(
                "Expected Avro record for manifest entry".to_string(),
            ))
        }
    };

    let get_field = |name: &str| -> Option<&AvroValue> {
        record.iter().find(|(n, _)| n == name).map(|(_, v)| v)
    };

    // Entry-level fields
    let status = match get_field("status") {
        Some(AvroValue::Int(i)) => ManifestEntryStatus::from_avro(*i),
        _ => ManifestEntryStatus::Existing,
    };

    let snapshot_id = match get_field("snapshot_id") {
        Some(AvroValue::Union(_, boxed)) => match boxed.as_ref() {
            AvroValue::Long(l) => Some(*l),
            _ => None,
        },
        Some(AvroValue::Long(l)) => Some(*l),
        _ => None,
    };

    let sequence_number = match get_field("sequence_number") {
        Some(AvroValue::Union(_, boxed)) => match boxed.as_ref() {
            AvroValue::Long(l) => Some(*l),
            _ => None,
        },
        Some(AvroValue::Long(l)) => Some(*l),
        _ => None,
    };

    let file_sequence_number = match get_field("file_sequence_number") {
        Some(AvroValue::Union(_, boxed)) => match boxed.as_ref() {
            AvroValue::Long(l) => Some(*l),
            _ => None,
        },
        Some(AvroValue::Long(l)) => Some(*l),
        _ => None,
    };

    // Parse the nested data_file record
    let data_file_value = get_field("data_file").ok_or_else(|| {
        IcebergError::Manifest("Missing data_file field in manifest entry".to_string())
    })?;

    let data_file = parse_data_file(data_file_value)?;

    Ok(ManifestEntry {
        status,
        snapshot_id,
        sequence_number,
        file_sequence_number,
        data_file,
    })
}

/// Parse a DataFile from an Avro value.
fn parse_data_file(value: &AvroValue) -> Result<DataFile> {
    let record = match value {
        AvroValue::Record(fields) => fields,
        _ => {
            return Err(IcebergError::Manifest(
                "Expected Avro record for data_file".to_string(),
            ))
        }
    };

    let get_field = |name: &str| -> Option<&AvroValue> {
        record.iter().find(|(n, _)| n == name).map(|(_, v)| v)
    };

    // Required fields
    let file_path = match get_field("file_path") {
        Some(AvroValue::String(s)) => s.clone(),
        _ => {
            return Err(IcebergError::Manifest(
                "Missing or invalid file_path".to_string(),
            ))
        }
    };

    let file_format = match get_field("file_format") {
        Some(AvroValue::String(s)) => FileFormat::parse(s),
        _ => FileFormat::Parquet,
    };

    let record_count = match get_field("record_count") {
        Some(AvroValue::Long(l)) => *l,
        _ => 0,
    };

    let file_size_in_bytes = match get_field("file_size_in_bytes") {
        Some(AvroValue::Long(l)) => *l,
        _ => 0,
    };

    // Partition data
    let partition = match get_field("partition") {
        Some(value) => parse_partition_data(value)?,
        _ => PartitionData::default(),
    };

    // Column statistics (maps from field_id)
    let column_sizes = parse_field_id_map_i64(get_field("column_sizes"));
    let value_counts = parse_field_id_map_i64(get_field("value_counts"));
    let null_value_counts = parse_field_id_map_i64(get_field("null_value_counts"));
    let nan_value_counts = parse_field_id_map_i64(get_field("nan_value_counts"));
    let lower_bounds = parse_field_id_map_bytes(get_field("lower_bounds"));
    let upper_bounds = parse_field_id_map_bytes(get_field("upper_bounds"));

    // Split offsets
    let split_offsets = match get_field("split_offsets") {
        Some(AvroValue::Union(_, boxed)) => match boxed.as_ref() {
            AvroValue::Array(arr) => Some(
                arr.iter()
                    .filter_map(|v| match v {
                        AvroValue::Long(l) => Some(*l),
                        _ => None,
                    })
                    .collect(),
            ),
            _ => None,
        },
        Some(AvroValue::Array(arr)) => Some(
            arr.iter()
                .filter_map(|v| match v {
                    AvroValue::Long(l) => Some(*l),
                    _ => None,
                })
                .collect(),
        ),
        _ => None,
    };

    let sort_order_id = match get_field("sort_order_id") {
        Some(AvroValue::Union(_, boxed)) => match boxed.as_ref() {
            AvroValue::Int(i) => Some(*i),
            _ => None,
        },
        Some(AvroValue::Int(i)) => Some(*i),
        _ => None,
    };

    Ok(DataFile {
        file_path,
        file_format,
        record_count,
        file_size_in_bytes,
        partition,
        column_sizes,
        value_counts,
        null_value_counts,
        nan_value_counts,
        lower_bounds,
        upper_bounds,
        split_offsets,
        sort_order_id,
    })
}

/// Parse partition data from Avro value.
fn parse_partition_data(value: &AvroValue) -> Result<PartitionData> {
    let record = match value {
        AvroValue::Record(fields) => fields,
        _ => return Ok(PartitionData::default()),
    };

    let mut values = HashMap::new();

    for (name, value) in record {
        // In Iceberg, partition field names include the source field ID
        // For simplicity, we'll just use sequential IDs here
        // TODO: Parse field IDs from the manifest schema
        let field_id = name.parse::<i32>().unwrap_or(0);

        let bytes = match value {
            AvroValue::Union(_, boxed) => match boxed.as_ref() {
                AvroValue::Bytes(b) => Some(b.clone()),
                AvroValue::Null => None,
                _ => None,
            },
            AvroValue::Bytes(b) => Some(b.clone()),
            AvroValue::Null => None,
            _ => None,
        };

        values.insert(field_id, bytes);
    }

    Ok(PartitionData { values })
}

/// Parse a map from field_id to i64 value.
fn parse_field_id_map_i64(value: Option<&AvroValue>) -> Option<HashMap<i32, i64>> {
    let value = value?;

    let map = match value {
        AvroValue::Union(_, boxed) => match boxed.as_ref() {
            AvroValue::Array(arr) => arr,
            AvroValue::Map(m) => {
                // Convert map to our format
                let mut result = HashMap::new();
                for (k, v) in m {
                    if let Ok(field_id) = k.parse::<i32>() {
                        if let AvroValue::Long(l) = v {
                            result.insert(field_id, *l);
                        }
                    }
                }
                return Some(result);
            }
            _ => return None,
        },
        AvroValue::Array(arr) => arr,
        AvroValue::Map(m) => {
            let mut result = HashMap::new();
            for (k, v) in m {
                if let Ok(field_id) = k.parse::<i32>() {
                    if let AvroValue::Long(l) = v {
                        result.insert(field_id, *l);
                    }
                }
            }
            return Some(result);
        }
        _ => return None,
    };

    // Parse array of key-value records
    let mut result = HashMap::new();
    for item in map {
        if let AvroValue::Record(fields) = item {
            let key = fields.iter().find(|(n, _)| n == "key");
            let value = fields.iter().find(|(n, _)| n == "value");

            if let (Some((_, AvroValue::Int(k))), Some((_, AvroValue::Long(v)))) = (key, value) {
                result.insert(*k, *v);
            }
        }
    }

    if result.is_empty() {
        None
    } else {
        Some(result)
    }
}

/// Parse a map from field_id to bytes value.
fn parse_field_id_map_bytes(value: Option<&AvroValue>) -> Option<HashMap<i32, Vec<u8>>> {
    let value = value?;

    let map = match value {
        AvroValue::Union(_, boxed) => match boxed.as_ref() {
            AvroValue::Array(arr) => arr,
            AvroValue::Map(m) => {
                let mut result = HashMap::new();
                for (k, v) in m {
                    if let Ok(field_id) = k.parse::<i32>() {
                        if let AvroValue::Bytes(b) = v {
                            result.insert(field_id, b.clone());
                        }
                    }
                }
                return Some(result);
            }
            _ => return None,
        },
        AvroValue::Array(arr) => arr,
        AvroValue::Map(m) => {
            let mut result = HashMap::new();
            for (k, v) in m {
                if let Ok(field_id) = k.parse::<i32>() {
                    if let AvroValue::Bytes(b) = v {
                        result.insert(field_id, b.clone());
                    }
                }
            }
            return Some(result);
        }
        _ => return None,
    };

    // Parse array of key-value records
    let mut result = HashMap::new();
    for item in map {
        if let AvroValue::Record(fields) = item {
            let key = fields.iter().find(|(n, _)| n == "key");
            let value = fields.iter().find(|(n, _)| n == "value");

            if let (Some((_, AvroValue::Int(k))), Some((_, AvroValue::Bytes(v)))) = (key, value) {
                result.insert(*k, v.clone());
            }
        }
    }

    if result.is_empty() {
        None
    } else {
        Some(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_format_parsing() {
        assert_eq!(FileFormat::parse("PARQUET"), FileFormat::Parquet);
        assert_eq!(FileFormat::parse("parquet"), FileFormat::Parquet);
        assert_eq!(FileFormat::parse("ORC"), FileFormat::Orc);
        assert_eq!(FileFormat::parse("AVRO"), FileFormat::Avro);
        assert_eq!(FileFormat::parse("unknown"), FileFormat::Parquet);
    }

    #[test]
    fn test_file_format_supported() {
        assert!(FileFormat::Parquet.is_supported());
        assert!(!FileFormat::Orc.is_supported());
        assert!(!FileFormat::Avro.is_supported());
    }

    #[test]
    fn test_manifest_entry_status() {
        assert_eq!(
            ManifestEntryStatus::from_avro(0),
            ManifestEntryStatus::Existing
        );
        assert_eq!(
            ManifestEntryStatus::from_avro(1),
            ManifestEntryStatus::Added
        );
        assert_eq!(
            ManifestEntryStatus::from_avro(2),
            ManifestEntryStatus::Deleted
        );

        assert!(ManifestEntryStatus::Existing.is_active());
        assert!(ManifestEntryStatus::Added.is_active());
        assert!(!ManifestEntryStatus::Deleted.is_active());
    }

    #[test]
    fn test_data_file_bounds_access() {
        let mut lower_bounds = HashMap::new();
        lower_bounds.insert(1, vec![0x01, 0x02, 0x03]);

        let mut upper_bounds = HashMap::new();
        upper_bounds.insert(1, vec![0x04, 0x05, 0x06]);

        let data_file = DataFile {
            file_path: "test.parquet".to_string(),
            file_format: FileFormat::Parquet,
            record_count: 100,
            file_size_in_bytes: 1024,
            partition: PartitionData::default(),
            column_sizes: None,
            value_counts: None,
            null_value_counts: None,
            nan_value_counts: None,
            lower_bounds: Some(lower_bounds),
            upper_bounds: Some(upper_bounds),
            split_offsets: None,
            sort_order_id: None,
        };

        assert_eq!(data_file.lower_bound(1), Some(&[0x01, 0x02, 0x03][..]));
        assert_eq!(data_file.upper_bound(1), Some(&[0x04, 0x05, 0x06][..]));
        assert_eq!(data_file.lower_bound(2), None);
        assert_eq!(data_file.upper_bound(2), None);
    }
}
