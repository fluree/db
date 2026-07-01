//! Metadata-only per-column statistics aggregation (Tier-B).
//!
//! Given the data files listed by a snapshot's manifests, this module aggregates
//! per-column statistics (null/value counts, typed min/max, NaN counts, on-disk
//! bytes) plus authoritative row/file/byte totals. It reads **only** the
//! manifest-list + manifest Avro files — never a Parquet/data file — so it is
//! safe on the metadata-preview path.
//!
//! `distinct_count` is intentionally always `None`: NDV is not derivable from
//! Iceberg metadata alone (Puffin/theta-sketch reading is deferred to PR-5).

use std::collections::HashMap;

use serde_json::Value as JsonValue;

use crate::error::Result;
use crate::io::IcebergStorage;
use crate::manifest::value_codec::decode_bound;
use crate::manifest::{parse_manifest, parse_manifest_list, DataFile, TypedValue};
use crate::metadata::{Schema, SchemaField, Snapshot};

/// Aggregated statistics for a single column across a snapshot's data files.
#[derive(Debug, Clone, Default)]
pub struct AggregatedColumnStats {
    /// Iceberg field ID.
    pub field_id: i32,
    /// Summed null value count (best-effort; `None` if no file reported it).
    pub null_count: Option<i64>,
    /// Summed value count including nulls (best-effort).
    pub value_count: Option<i64>,
    /// `null_count / value_count`, when both known and `value_count > 0`.
    pub null_fraction: Option<f64>,
    /// Summed NaN count (float/double columns; best-effort).
    pub nan_count: Option<i64>,
    /// Column-wide minimum (value_codec-decoded, JSON-rendered).
    pub min: Option<JsonValue>,
    /// Column-wide maximum (value_codec-decoded, JSON-rendered).
    pub max: Option<JsonValue>,
    /// Summed on-disk column size in bytes (best-effort).
    pub on_disk_bytes: Option<i64>,
    /// Distinct value count — ALWAYS `None` (NDV deferred to PR-5).
    pub distinct_count: Option<i64>,
}

/// Aggregated table-level statistics computed from a snapshot's data files.
#[derive(Debug, Clone, Default)]
pub struct TableStatsAggregation {
    /// Per-column stats, keyed by field ID (one entry per scalar column).
    pub columns: HashMap<i32, AggregatedColumnStats>,
    /// Sum of `record_count` across the data files (authoritative row count).
    pub row_count: i64,
    /// Number of data files aggregated.
    pub data_file_count: i64,
    /// Sum of `file_size_in_bytes` across the data files.
    pub total_bytes: i64,
    /// Whether any column carried decodable lower/upper bounds.
    pub had_column_bounds: bool,
}

/// Mutable per-column accumulator used during aggregation.
#[derive(Default)]
struct Acc {
    null_count: Option<i64>,
    value_count: Option<i64>,
    nan_count: Option<i64>,
    on_disk_bytes: Option<i64>,
    min: Option<TypedValue>,
    max: Option<TypedValue>,
}

fn add_opt(slot: &mut Option<i64>, add: i64) {
    *slot = Some(slot.unwrap_or(0) + add);
}

fn lookup(map: Option<&HashMap<i32, i64>>, field_id: i32) -> Option<i64> {
    map.and_then(|m| m.get(&field_id)).copied()
}

/// Keep the smaller of `cur` and `candidate` (by `TypedValue` ordering); on an
/// incomparable pair (type mismatch) the existing value wins.
fn keep_min(cur: Option<TypedValue>, candidate: TypedValue) -> TypedValue {
    match cur {
        Some(cur) => match candidate.partial_cmp(&cur) {
            Some(std::cmp::Ordering::Less) => candidate,
            _ => cur,
        },
        None => candidate,
    }
}

/// Keep the larger of `cur` and `candidate`; on an incomparable pair the
/// existing value wins.
fn keep_max(cur: Option<TypedValue>, candidate: TypedValue) -> TypedValue {
    match cur {
        Some(cur) => match candidate.partial_cmp(&cur) {
            Some(std::cmp::Ordering::Greater) => candidate,
            _ => cur,
        },
        None => candidate,
    }
}

/// Aggregate per-column statistics from a set of data files against a schema.
///
/// Pure and side-effect-free: it reads nothing. Only scalar (non-nested) schema
/// fields are aggregated (R2RML addresses flat columns; nested bounds are not
/// decodable). Every scalar column gets an entry, even when the manifests carry
/// no statistics for it (all-`None`).
pub fn aggregate_column_stats(data_files: &[DataFile], schema: &Schema) -> TableStatsAggregation {
    let scalar_fields: Vec<&SchemaField> =
        schema.fields.iter().filter(|f| !f.is_nested()).collect();
    let mut accs: HashMap<i32, Acc> = scalar_fields
        .iter()
        .map(|f| (f.id, Acc::default()))
        .collect();

    let mut row_count = 0i64;
    let mut total_bytes = 0i64;
    let mut had_column_bounds = false;

    for df in data_files {
        row_count += df.record_count;
        total_bytes += df.file_size_in_bytes;

        for field in &scalar_fields {
            let fid = field.id;
            let Some(acc) = accs.get_mut(&fid) else {
                continue;
            };

            if let Some(n) = lookup(df.null_value_counts.as_ref(), fid) {
                add_opt(&mut acc.null_count, n);
            }
            if let Some(n) = lookup(df.value_counts.as_ref(), fid) {
                add_opt(&mut acc.value_count, n);
            }
            if let Some(n) = lookup(df.nan_value_counts.as_ref(), fid) {
                add_opt(&mut acc.nan_count, n);
            }
            if let Some(n) = lookup(df.column_sizes.as_ref(), fid) {
                add_opt(&mut acc.on_disk_bytes, n);
            }

            if let Some(bytes) = df.lower_bound(fid) {
                if let Ok(v) = decode_bound(bytes, field) {
                    had_column_bounds = true;
                    acc.min = Some(keep_min(acc.min.take(), v));
                }
            }
            if let Some(bytes) = df.upper_bound(fid) {
                if let Ok(v) = decode_bound(bytes, field) {
                    had_column_bounds = true;
                    acc.max = Some(keep_max(acc.max.take(), v));
                }
            }
        }
    }

    let columns = accs
        .into_iter()
        .map(|(fid, acc)| {
            let null_fraction = match (acc.null_count, acc.value_count) {
                (Some(n), Some(v)) if v > 0 => Some(n as f64 / v as f64),
                _ => None,
            };
            let stats = AggregatedColumnStats {
                field_id: fid,
                null_count: acc.null_count,
                value_count: acc.value_count,
                null_fraction,
                nan_count: acc.nan_count,
                min: acc.min.as_ref().map(typed_value_to_json),
                max: acc.max.as_ref().map(typed_value_to_json),
                on_disk_bytes: acc.on_disk_bytes,
                distinct_count: None,
            };
            (fid, stats)
        })
        .collect();

    TableStatsAggregation {
        columns,
        row_count,
        data_file_count: data_files.len() as i64,
        total_bytes,
        had_column_bounds,
    }
}

/// Render a decoded [`TypedValue`] as a JSON value. Numeric temporals are
/// rendered as ISO-8601 strings for readability; the raw integer is used as a
/// fallback if the timestamp is out of range.
pub fn typed_value_to_json(value: &TypedValue) -> JsonValue {
    use chrono::{DateTime, SecondsFormat};

    match value {
        TypedValue::Boolean(b) => JsonValue::Bool(*b),
        TypedValue::Int32(v) => JsonValue::from(*v),
        TypedValue::Int64(v) => JsonValue::from(*v),
        TypedValue::Float32(v) => JsonValue::from(f64::from(*v)),
        TypedValue::Float64(v) => JsonValue::from(*v),
        TypedValue::Date(days) => DateTime::from_timestamp(i64::from(*days) * 86_400, 0)
            .map_or_else(
                || JsonValue::from(*days),
                |dt| JsonValue::from(dt.format("%Y-%m-%d").to_string()),
            ),
        TypedValue::Timestamp(micros) | TypedValue::TimestampTz(micros) => {
            DateTime::from_timestamp_micros(*micros).map_or_else(
                || JsonValue::from(*micros),
                |dt| JsonValue::from(dt.to_rfc3339_opts(SecondsFormat::Micros, true)),
            )
        }
        TypedValue::String(s) => JsonValue::from(s.clone()),
        TypedValue::Bytes(b) => JsonValue::from(hex_encode(b)),
        TypedValue::Uuid(bytes) => JsonValue::from(format_uuid(bytes)),
        TypedValue::Decimal {
            unscaled, scale, ..
        } => JsonValue::from(format_decimal(*unscaled, *scale)),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

fn format_uuid(bytes: &[u8; 16]) -> String {
    let h = hex_encode(bytes);
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

/// Render an unscaled decimal as its scaled decimal string (e.g. `12345`,
/// scale 2 → `"123.45"`). Handles negative values and `scale <= 0`.
fn format_decimal(unscaled: i128, scale: i8) -> String {
    if scale <= 0 {
        // No fractional digits; a negative scale multiplies by 10^-scale.
        let mut s = unscaled.to_string();
        for _ in 0..(-scale) {
            s.push('0');
        }
        return s;
    }
    let scale = scale as usize;
    let negative = unscaled < 0;
    let digits = unscaled.unsigned_abs().to_string();
    let padded = if digits.len() <= scale {
        format!("{:0>width$}", digits, width = scale + 1)
    } else {
        digits
    };
    let split = padded.len() - scale;
    let (int_part, frac_part) = padded.split_at(split);
    let sign = if negative { "-" } else { "" };
    format!("{sign}{int_part}.{frac_part}")
}

/// Read the data files listed by a snapshot's manifests — **manifest-list +
/// manifest Avro only**, never a data file. Returns the collected data files and
/// the number of manifest files read.
///
/// Runtime-agnostic variant (`?Send`); server code should use
/// [`send_read_snapshot_data_files`].
pub async fn read_snapshot_data_files<S: IcebergStorage + ?Sized>(
    storage: &S,
    snapshot: &Snapshot,
) -> Result<(Vec<DataFile>, usize)> {
    let manifest_list_path = snapshot.manifest_list.as_ref().ok_or_else(|| {
        crate::error::IcebergError::Manifest(
            "Snapshot has no manifest list (v1 format not supported)".to_string(),
        )
    })?;

    let manifest_list_data = storage.read(manifest_list_path).await?;
    let manifest_entries = parse_manifest_list(&manifest_list_data)?;

    let mut data_files = Vec::new();
    let mut manifests_read = 0usize;
    for me in &manifest_entries {
        if me.is_deletes() {
            continue;
        }
        let manifest_data = storage.read(&me.manifest_path).await?;
        manifests_read += 1;
        for entry in parse_manifest(&manifest_data)? {
            data_files.push(entry.data_file);
        }
    }

    Ok((data_files, manifests_read))
}

/// Send-safe variant of [`read_snapshot_data_files`] for server-side use.
#[cfg(feature = "aws")]
pub async fn send_read_snapshot_data_files<S: crate::io::SendIcebergStorage + ?Sized>(
    storage: &S,
    snapshot: &Snapshot,
) -> Result<(Vec<DataFile>, usize)> {
    let manifest_list_path = snapshot.manifest_list.as_ref().ok_or_else(|| {
        crate::error::IcebergError::Manifest(
            "Snapshot has no manifest list (v1 format not supported)".to_string(),
        )
    })?;

    let manifest_list_data = storage.read(manifest_list_path).await?;
    let manifest_entries = parse_manifest_list(&manifest_list_data)?;

    let mut data_files = Vec::new();
    let mut manifests_read = 0usize;
    for me in &manifest_entries {
        if me.is_deletes() {
            continue;
        }
        let manifest_data = storage.read(&me.manifest_path).await?;
        manifests_read += 1;
        for entry in parse_manifest(&manifest_data)? {
            data_files.push(entry.data_file);
        }
    }

    Ok((data_files, manifests_read))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::value_codec::encode_value;
    use crate::manifest::{FileFormat, PartitionData};

    fn schema_id_amount() -> Schema {
        Schema {
            schema_id: 0,
            identifier_field_ids: vec![1],
            fields: vec![
                SchemaField {
                    id: 1,
                    name: "ID".to_string(),
                    required: true,
                    field_type: serde_json::json!("long"),
                    doc: None,
                },
                SchemaField {
                    id: 2,
                    name: "AMOUNT".to_string(),
                    required: false,
                    field_type: serde_json::json!("double"),
                    doc: None,
                },
            ],
        }
    }

    fn data_file(
        path: &str,
        record_count: i64,
        size: i64,
        id_range: (i64, i64),
        id_nulls: i64,
        amount_nan: i64,
    ) -> DataFile {
        let mut value_counts = HashMap::new();
        value_counts.insert(1, record_count);
        value_counts.insert(2, record_count);
        let mut null_counts = HashMap::new();
        null_counts.insert(1, id_nulls);
        null_counts.insert(2, 0);
        let mut nan_counts = HashMap::new();
        nan_counts.insert(2, amount_nan);
        let mut column_sizes = HashMap::new();
        column_sizes.insert(1, size / 2);
        column_sizes.insert(2, size / 2);
        let mut lower = HashMap::new();
        lower.insert(1, encode_value(&TypedValue::Int64(id_range.0)));
        let mut upper = HashMap::new();
        upper.insert(1, encode_value(&TypedValue::Int64(id_range.1)));

        DataFile {
            file_path: path.to_string(),
            file_format: FileFormat::Parquet,
            record_count,
            file_size_in_bytes: size,
            partition: PartitionData::default(),
            column_sizes: Some(column_sizes),
            value_counts: Some(value_counts),
            null_value_counts: Some(null_counts),
            nan_value_counts: Some(nan_counts),
            lower_bounds: Some(lower),
            upper_bounds: Some(upper),
            split_offsets: None,
            sort_order_id: None,
        }
    }

    #[test]
    fn aggregates_counts_bounds_and_totals() {
        let schema = schema_id_amount();
        let files = vec![
            data_file("a.parquet", 100, 2000, (10, 50), 3, 1),
            data_file("b.parquet", 200, 4000, (5, 80), 7, 0),
        ];

        let agg = aggregate_column_stats(&files, &schema);

        // Authoritative totals summed across files.
        assert_eq!(agg.row_count, 300);
        assert_eq!(agg.data_file_count, 2);
        assert_eq!(agg.total_bytes, 6000);
        assert!(agg.had_column_bounds);

        let id = &agg.columns[&1];
        assert_eq!(id.null_count, Some(10)); // 3 + 7
        assert_eq!(id.value_count, Some(300)); // 100 + 200
                                               // null_fraction = 10 / 300
        assert!((id.null_fraction.unwrap() - (10.0 / 300.0)).abs() < 1e-9);
        assert_eq!(id.on_disk_bytes, Some(3000)); // 1000 + 2000
                                                  // Typed min/max across files: min(10,5)=5, max(50,80)=80.
        assert_eq!(id.min, Some(serde_json::json!(5)));
        assert_eq!(id.max, Some(serde_json::json!(80)));
        // distinct_count is always None.
        assert_eq!(id.distinct_count, None);

        let amount = &agg.columns[&2];
        assert_eq!(amount.nan_count, Some(1)); // 1 + 0
        assert_eq!(amount.value_count, Some(300));
        // AMOUNT carried no bounds → min/max None.
        assert_eq!(amount.min, None);
        assert_eq!(amount.max, None);
    }

    #[test]
    fn had_column_bounds_false_when_absent() {
        let schema = schema_id_amount();
        let mut df = data_file("a.parquet", 100, 2000, (0, 0), 0, 0);
        df.lower_bounds = None;
        df.upper_bounds = None;
        let agg = aggregate_column_stats(&[df], &schema);
        assert!(!agg.had_column_bounds);
        assert_eq!(agg.columns[&1].min, None);
        assert_eq!(agg.columns[&1].max, None);
        // Row count is still authoritative from record_count.
        assert_eq!(agg.row_count, 100);
    }

    #[test]
    fn typed_value_json_rendering() {
        assert_eq!(
            typed_value_to_json(&TypedValue::Int64(42)),
            serde_json::json!(42)
        );
        assert_eq!(
            typed_value_to_json(&TypedValue::Boolean(true)),
            serde_json::json!(true)
        );
        assert_eq!(
            typed_value_to_json(&TypedValue::String("hi".to_string())),
            serde_json::json!("hi")
        );
        // Date 19723 days since epoch = 2024-01-01.
        assert_eq!(
            typed_value_to_json(&TypedValue::Date(19723)),
            serde_json::json!("2024-01-01")
        );
        // Timestamp micros render ISO-8601 with a trailing Z.
        let ts = typed_value_to_json(&TypedValue::Timestamp(1_700_000_000_000_000));
        assert!(ts.as_str().unwrap().ends_with('Z'), "got {ts}");
        // Bytes → lowercase hex.
        assert_eq!(
            typed_value_to_json(&TypedValue::Bytes(vec![0x0a, 0xff])),
            serde_json::json!("0aff")
        );
        // Decimal scaled string.
        assert_eq!(
            typed_value_to_json(&TypedValue::Decimal {
                unscaled: 12345,
                precision: 10,
                scale: 2
            }),
            serde_json::json!("123.45")
        );
        assert_eq!(
            typed_value_to_json(&TypedValue::Decimal {
                unscaled: -5,
                precision: 10,
                scale: 2
            }),
            serde_json::json!("-0.05")
        );
    }

    // ------------------------------------------------------------------
    // Metadata-only guarantee: the Tier-B reader touches ONLY the
    // manifest-list + manifest Avro files, never a Parquet/data file.
    // ------------------------------------------------------------------

    use apache_avro::{types::Record, types::Value as AvroValue, Schema as AvroSchema, Writer};
    use async_trait::async_trait;
    use bytes::Bytes;
    use std::ops::Range;
    use std::sync::Mutex;

    const MANIFEST_LIST_SCHEMA: &str = r#"{
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
        {"name": "partitions", "type": ["null", {"type": "array", "items": {
          "type": "record", "name": "field_summary",
          "fields": [{"name": "contains_null", "type": "boolean"}]
        }}], "default": null}
      ]
    }"#;

    const MANIFEST_SCHEMA: &str = r#"{
      "type": "record",
      "name": "manifest_entry",
      "fields": [
        {"name": "status", "type": "int"},
        {"name": "snapshot_id", "type": ["null", "long"], "default": null},
        {"name": "data_file", "type": {
          "type": "record",
          "name": "df_record",
          "fields": [
            {"name": "file_path", "type": "string"},
            {"name": "file_format", "type": "string"},
            {"name": "record_count", "type": "long"},
            {"name": "file_size_in_bytes", "type": "long"},
            {"name": "column_sizes", "type": ["null", {"type": "map", "values": "long"}], "default": null},
            {"name": "value_counts", "type": ["null", {"type": "map", "values": "long"}], "default": null},
            {"name": "null_value_counts", "type": ["null", {"type": "map", "values": "long"}], "default": null},
            {"name": "nan_value_counts", "type": ["null", {"type": "map", "values": "long"}], "default": null},
            {"name": "lower_bounds", "type": ["null", {"type": "map", "values": "bytes"}], "default": null},
            {"name": "upper_bounds", "type": ["null", {"type": "map", "values": "bytes"}], "default": null}
          ]
        }}
      ]
    }"#;

    fn build_manifest_list(manifest_path: &str) -> Bytes {
        let schema = AvroSchema::parse_str(MANIFEST_LIST_SCHEMA).unwrap();
        let mut writer = Writer::new(&schema, Vec::new());
        let mut record = Record::new(writer.schema()).unwrap();
        record.put("manifest_path", manifest_path);
        record.put("manifest_length", 100i64);
        record.put("partition_spec_id", 0i32);
        record.put("content", 0i32);
        record.put("sequence_number", 1i64);
        record.put("min_sequence_number", 1i64);
        record.put("added_snapshot_id", 100i64);
        record.put("added_data_files_count", 1i32);
        record.put("existing_data_files_count", 0i32);
        record.put("deleted_data_files_count", 0i32);
        record.put("added_rows_count", 1000i64);
        record.put("existing_rows_count", 0i64);
        record.put("deleted_rows_count", 0i64);
        record.put("partitions", AvroValue::Union(0, Box::new(AvroValue::Null)));
        writer.append(record).unwrap();
        Bytes::from(writer.into_inner().unwrap())
    }

    fn long_map(entries: &[(i32, i64)]) -> AvroValue {
        AvroValue::Map(
            entries
                .iter()
                .map(|(k, v)| (k.to_string(), AvroValue::Long(*v)))
                .collect(),
        )
    }

    fn bytes_map(entries: &[(i32, Vec<u8>)]) -> AvroValue {
        AvroValue::Map(
            entries
                .iter()
                .map(|(k, v)| (k.to_string(), AvroValue::Bytes(v.clone())))
                .collect(),
        )
    }

    fn build_manifest(data_file_path: &str) -> Bytes {
        let schema = AvroSchema::parse_str(MANIFEST_SCHEMA).unwrap();
        let mut writer = Writer::new(&schema, Vec::new());
        let data_file = AvroValue::Record(vec![
            (
                "file_path".to_string(),
                AvroValue::String(data_file_path.to_string()),
            ),
            (
                "file_format".to_string(),
                AvroValue::String("PARQUET".to_string()),
            ),
            ("record_count".to_string(), AvroValue::Long(1000)),
            ("file_size_in_bytes".to_string(), AvroValue::Long(204_800)),
            (
                "column_sizes".to_string(),
                AvroValue::Union(1, Box::new(long_map(&[(1, 4000), (2, 8000)]))),
            ),
            (
                "value_counts".to_string(),
                AvroValue::Union(1, Box::new(long_map(&[(1, 1000), (2, 1000)]))),
            ),
            (
                "null_value_counts".to_string(),
                AvroValue::Union(1, Box::new(long_map(&[(1, 0), (2, 5)]))),
            ),
            (
                "nan_value_counts".to_string(),
                AvroValue::Union(0, Box::new(AvroValue::Null)),
            ),
            (
                "lower_bounds".to_string(),
                AvroValue::Union(
                    1,
                    Box::new(bytes_map(&[(1, encode_value(&TypedValue::Int64(1)))])),
                ),
            ),
            (
                "upper_bounds".to_string(),
                AvroValue::Union(
                    1,
                    Box::new(bytes_map(&[(1, encode_value(&TypedValue::Int64(999)))])),
                ),
            ),
        ]);
        let entry = AvroValue::Record(vec![
            ("status".to_string(), AvroValue::Int(1)),
            (
                "snapshot_id".to_string(),
                AvroValue::Union(1, Box::new(AvroValue::Long(100))),
            ),
            ("data_file".to_string(), data_file),
        ]);
        writer.append_value_ref(&entry).unwrap();
        Bytes::from(writer.into_inner().unwrap())
    }

    /// Storage wrapper that records every path read so a test can assert no
    /// Parquet/data file is ever fetched.
    #[derive(Debug)]
    struct RecordingStorage {
        inner: crate::io::MemoryStorage,
        reads: Mutex<Vec<String>>,
    }

    impl RecordingStorage {
        fn new(inner: crate::io::MemoryStorage) -> Self {
            Self {
                inner,
                reads: Mutex::new(Vec::new()),
            }
        }
        fn reads(&self) -> Vec<String> {
            self.reads.lock().unwrap().clone()
        }
    }

    #[async_trait(?Send)]
    impl IcebergStorage for RecordingStorage {
        async fn read(&self, path: &str) -> Result<Bytes> {
            self.reads.lock().unwrap().push(path.to_string());
            self.inner.read(path).await
        }
        async fn read_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
            self.reads.lock().unwrap().push(path.to_string());
            self.inner.read_range(path, range).await
        }
        async fn file_size(&self, path: &str) -> Result<u64> {
            self.inner.file_size(path).await
        }
    }

    fn snapshot_with_list(list_path: &str) -> Snapshot {
        Snapshot {
            snapshot_id: 100,
            parent_snapshot_id: None,
            sequence_number: 1,
            timestamp_ms: 1000,
            manifest_list: Some(list_path.to_string()),
            manifests: None,
            summary: HashMap::new(),
            schema_id: Some(0),
        }
    }

    #[tokio::test]
    async fn tier_b_reads_only_avro_never_parquet() {
        let list_path = "s3://b/t/metadata/snap.avro";
        let manifest_path = "s3://b/t/metadata/m1.avro";
        let data_path = "s3://b/t/data/f1.parquet";

        let mut mem = crate::io::MemoryStorage::new();
        mem.add_file(list_path, build_manifest_list(manifest_path));
        mem.add_file(manifest_path, build_manifest(data_path));
        // The Parquet data file is deliberately absent: if the reader touched it,
        // MemoryStorage would return a not-found error and the read would fail.
        let storage = RecordingStorage::new(mem);

        let snapshot = snapshot_with_list(list_path);
        let (data_files, manifests_read) =
            read_snapshot_data_files(&storage, &snapshot).await.unwrap();

        assert_eq!(manifests_read, 1);
        assert_eq!(data_files.len(), 1);
        assert_eq!(data_files[0].file_path, data_path);

        // The load-bearing assertion: every fetched path is an Avro file; the
        // Parquet data file is never read.
        let reads = storage.reads();
        assert!(!reads.is_empty());
        assert!(
            reads.iter().all(|p| p.ends_with(".avro")),
            "a non-avro file was read: {reads:?}"
        );
        assert!(reads.iter().any(|p| p == list_path));
        assert!(reads.iter().any(|p| p == manifest_path));
        assert!(
            !reads.iter().any(|p| p.ends_with(".parquet")),
            "a parquet file was read: {reads:?}"
        );

        // And the aggregation over the manifest-derived data files is correct.
        let schema = schema_id_amount();
        let agg = aggregate_column_stats(&data_files, &schema);
        assert_eq!(agg.row_count, 1000);
        assert!(agg.had_column_bounds);
        assert_eq!(agg.columns[&1].min, Some(serde_json::json!(1)));
        assert_eq!(agg.columns[&1].max, Some(serde_json::json!(999)));
        assert_eq!(agg.columns[&2].null_count, Some(5));
    }
}
