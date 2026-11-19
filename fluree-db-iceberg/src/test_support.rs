//! Hand-built manifest lists and manifests for planner tests.
//!
//! Writes the Avro shapes the parsers read (spec field names), with exactly the
//! per-file statistics a test sets, so a test can lay out a table whose files
//! prune differently under different filters without a Parquet writer.

// Most users are the `SendScanPlanner` tests, which need the `aws` feature.
#![cfg_attr(not(feature = "aws"), allow(dead_code))]

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Mutex;

use apache_avro::types::{Record, Value as AvroValue};
use apache_avro::{Schema as AvroSchema, Writer};
use async_trait::async_trait;
use bytes::Bytes;

use crate::error::{IcebergError, Result};
use crate::manifest::{encode_value, ManifestContent, ManifestListEntry, TypedValue};

const MANIFEST_LIST_SCHEMA: &str = r#"{
  "type": "record", "name": "manifest_file",
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
    {"name": "deleted_rows_count", "type": "long", "default": 0}
  ]
}"#;

const MANIFEST_SCHEMA: &str = r#"{
  "type": "record", "name": "manifest_entry",
  "fields": [
    {"name": "status", "type": "int"},
    {"name": "snapshot_id", "type": ["null", "long"], "default": null},
    {"name": "sequence_number", "type": ["null", "long"], "default": null},
    {"name": "data_file", "type": {
      "type": "record", "name": "r2",
      "fields": [
        {"name": "file_path", "type": "string"},
        {"name": "file_format", "type": "string"},
        {"name": "record_count", "type": "long"},
        {"name": "file_size_in_bytes", "type": "long"},
        {"name": "value_counts", "type": ["null", {"type": "map", "values": "long"}], "default": null},
        {"name": "null_value_counts", "type": ["null", {"type": "map", "values": "long"}], "default": null},
        {"name": "lower_bounds", "type": ["null", {"type": "map", "values": "bytes"}], "default": null},
        {"name": "upper_bounds", "type": ["null", {"type": "map", "values": "bytes"}], "default": null}
      ]
    }}
  ]
}"#;

/// One data file's statistics, as a manifest records them.
#[derive(Clone, Debug, Default)]
pub(crate) struct TestFile {
    pub path: String,
    pub record_count: i64,
    /// `(field_id, lower, upper)`.
    pub bounds: Vec<(i32, TypedValue, TypedValue)>,
    /// `(field_id, null count)`; value count is `record_count` for each.
    pub nulls: Vec<(i32, i64)>,
    /// The entry's own data sequence number (`None` inherits the manifest's).
    pub sequence_number: Option<i64>,
}

/// A manifest listing `files`.
pub(crate) fn manifest(files: &[TestFile]) -> Bytes {
    let schema = AvroSchema::parse_str(MANIFEST_SCHEMA).unwrap();
    let mut writer = Writer::new(&schema, Vec::new());
    let long_map = |kv: Vec<(i32, i64)>| {
        AvroValue::Union(
            1,
            Box::new(AvroValue::Map(
                kv.into_iter()
                    .map(|(k, v)| (k.to_string(), AvroValue::Long(v)))
                    .collect(),
            )),
        )
    };
    let bytes_map = |kv: Vec<(i32, Vec<u8>)>| {
        AvroValue::Union(
            1,
            Box::new(AvroValue::Map(
                kv.into_iter()
                    .map(|(k, v)| (k.to_string(), AvroValue::Bytes(v)))
                    .collect(),
            )),
        )
    };
    for f in files {
        let ids: Vec<i32> = f
            .bounds
            .iter()
            .map(|b| b.0)
            .chain(f.nulls.iter().map(|n| n.0))
            .collect();
        let data_file = AvroValue::Record(vec![
            ("file_path".into(), AvroValue::String(f.path.clone())),
            ("file_format".into(), AvroValue::String("PARQUET".into())),
            ("record_count".into(), AvroValue::Long(f.record_count)),
            ("file_size_in_bytes".into(), AvroValue::Long(1024)),
            (
                "value_counts".into(),
                long_map(ids.iter().map(|id| (*id, f.record_count)).collect()),
            ),
            ("null_value_counts".into(), long_map(f.nulls.clone())),
            (
                "lower_bounds".into(),
                bytes_map(
                    f.bounds
                        .iter()
                        .map(|(id, lo, _)| (*id, encode_value(lo)))
                        .collect(),
                ),
            ),
            (
                "upper_bounds".into(),
                bytes_map(
                    f.bounds
                        .iter()
                        .map(|(id, _, hi)| (*id, encode_value(hi)))
                        .collect(),
                ),
            ),
        ]);
        let entry = AvroValue::Record(vec![
            ("status".into(), AvroValue::Int(1)),
            (
                "snapshot_id".into(),
                AvroValue::Union(1, Box::new(AvroValue::Long(1))),
            ),
            (
                "sequence_number".into(),
                match f.sequence_number {
                    Some(s) => AvroValue::Union(1, Box::new(AvroValue::Long(s))),
                    None => AvroValue::Union(0, Box::new(AvroValue::Null)),
                },
            ),
            ("data_file".into(), data_file),
        ]);
        writer.append_value_ref(&entry).unwrap();
    }
    Bytes::from(writer.into_inner().unwrap())
}

/// A manifest list naming `(manifest_path, sequence_number)` data manifests.
pub(crate) fn manifest_list(manifests: &[(&str, i64)]) -> Bytes {
    let schema = AvroSchema::parse_str(MANIFEST_LIST_SCHEMA).unwrap();
    let mut writer = Writer::new(&schema, Vec::new());
    for (path, seq) in manifests {
        let mut record = Record::new(writer.schema()).unwrap();
        record.put("manifest_path", *path);
        record.put("manifest_length", 100i64);
        record.put("partition_spec_id", 0i32);
        record.put("content", 0i32);
        record.put("sequence_number", *seq);
        record.put("min_sequence_number", *seq);
        record.put("added_snapshot_id", 1i64);
        record.put("added_files_count", 1i32);
        record.put("existing_files_count", 0i32);
        record.put("deleted_files_count", 0i32);
        record.put("added_rows_count", 1i64);
        record.put("existing_rows_count", 0i64);
        record.put("deleted_rows_count", 0i64);
        writer.append(record).unwrap();
    }
    Bytes::from(writer.into_inner().unwrap())
}

/// A parsed data-manifest list entry for `path` at sequence number `seq`.
pub(crate) fn list_entry(path: &str, seq: i64) -> ManifestListEntry {
    ManifestListEntry {
        manifest_path: path.to_string(),
        manifest_length: 0,
        partition_spec_id: 0,
        content: ManifestContent::Data,
        sequence_number: seq,
        min_sequence_number: seq,
        added_snapshot_id: 1,
        added_files_count: None,
        existing_files_count: None,
        deleted_files_count: None,
        added_rows_count: None,
        existing_rows_count: None,
        deleted_rows_count: None,
        partitions: Vec::new(),
    }
}

/// A manifest with one file at `path` and no statistics.
pub(crate) fn single_file_manifest(path: &str) -> Bytes {
    manifest(&[TestFile {
        path: path.to_string(),
        record_count: 1,
        ..Default::default()
    }])
}

/// In-memory storage over fixed bytes that records every path read.
#[derive(Debug, Default)]
pub(crate) struct FixedFiles {
    pub files: HashMap<String, Bytes>,
    pub reads: Mutex<Vec<String>>,
}

impl FixedFiles {
    pub(crate) fn reads(&self) -> Vec<String> {
        self.reads.lock().unwrap().clone()
    }

    pub(crate) fn clear_reads(&self) {
        self.reads.lock().unwrap().clear();
    }

    fn get(&self, path: &str) -> Result<Bytes> {
        self.reads.lock().unwrap().push(path.to_string());
        self.files
            .get(path)
            .cloned()
            .ok_or_else(|| IcebergError::storage(format!("no {path}")))
    }
}

#[async_trait]
impl crate::io::SendIcebergStorage for FixedFiles {
    async fn read(&self, path: &str) -> Result<Bytes> {
        self.get(path)
    }
    async fn read_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        Ok(self
            .get(path)?
            .slice(range.start as usize..range.end as usize))
    }
    async fn file_size(&self, path: &str) -> Result<u64> {
        Ok(self.get(path)?.len() as u64)
    }
}

#[async_trait(?Send)]
impl crate::io::IcebergStorage for FixedFiles {
    async fn read(&self, path: &str) -> Result<Bytes> {
        self.get(path)
    }
    async fn read_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        Ok(self
            .get(path)?
            .slice(range.start as usize..range.end as usize))
    }
    async fn file_size(&self, path: &str) -> Result<u64> {
        Ok(self.get(path)?.len() as u64)
    }
}
