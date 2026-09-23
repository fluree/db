//! Convert already-transformed Kernel output into Fluree's Arrow-free batches.
//! Field IDs are supplied by the caller; this layer never invents durable IDs
//! from projection positions or silently substitutes nulls for unsupported data.
use std::{collections::BTreeSet, sync::Arc};

use delta_kernel::arrow::{array::*, datatypes::*, record_batch::RecordBatch};
use fluree_db_tabular::{BatchSchema, Column, ColumnBatch, FieldInfo, FieldType};

use crate::error::{DeltaError, Result};

pub struct BatchBridge {
    arrow_schema: SchemaRef,
    schema: Arc<BatchSchema>,
}

impl BatchBridge {
    /// Validate before executing a scan, including for an empty result. IDs
    /// correspond to logical fields in schema order and must already be resolved
    /// against the selected snapshot by the mapping/provider layer.
    pub fn new(arrow_schema: SchemaRef, field_ids: &[i32]) -> Result<Self> {
        if arrow_schema.fields().len() != field_ids.len()
            || field_ids.iter().collect::<BTreeSet<_>>().len() != field_ids.len()
        {
            return Err(DeltaError::Internal(
                "field IDs must be unique and cover the logical projection".to_string(),
            ));
        }
        let mut names = BTreeSet::new();
        let fields = arrow_schema
            .fields()
            .iter()
            .zip(field_ids)
            .map(|(f, id)| {
                if !names.insert(f.name()) {
                    return Err(DeltaError::SchemaMismatch(format!(
                        "duplicate logical field name: {}",
                        f.name()
                    )));
                }
                Ok(FieldInfo {
                    name: f.name().clone(),
                    field_type: field_type(f.data_type())?,
                    nullable: f.is_nullable(),
                    field_id: *id,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            arrow_schema,
            schema: Arc::new(BatchSchema::new(fields)),
        })
    }

    /// The Arrow schema batches are expected in.
    pub fn arrow_schema(&self) -> &Schema {
        &self.arrow_schema
    }

    pub fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }

    pub fn convert(&self, batch: &RecordBatch) -> Result<ColumnBatch> {
        // Arrow metadata may differ between logical planning and execution;
        // names, types, order, nullability and actual nulls must agree.
        if batch.num_columns() != self.schema.num_fields() {
            return Err(DeltaError::SchemaMismatch(
                "column count changed".to_string(),
            ));
        }
        for ((expected, actual), array) in self
            .arrow_schema
            .fields()
            .iter()
            .zip(batch.schema().fields())
            .zip(batch.columns())
        {
            if expected.name() != actual.name()
                || expected.data_type() != actual.data_type()
                || expected.is_nullable() != actual.is_nullable()
                || (!expected.is_nullable() && array.null_count() > 0)
            {
                return Err(DeltaError::SchemaMismatch(expected.name().clone()));
            }
        }
        let columns = batch
            .columns()
            .iter()
            .map(|a| convert_column(a.as_ref()))
            .collect::<Result<Vec<_>>>()?;
        let mut converted = ColumnBatch::new(self.schema.clone(), columns)
            .map_err(|e| DeltaError::SchemaMismatch(e.to_string()))?;
        // ColumnBatch::new infers zero rows for an empty projection. Arrow can
        // represent a nonzero row count without columns (e.g. COUNT scans).
        converted.num_rows = batch.num_rows();
        Ok(converted)
    }
}

fn field_type(data_type: &DataType) -> Result<FieldType> {
    Ok(match data_type {
        DataType::Boolean => FieldType::Boolean,
        DataType::Int8 | DataType::Int16 | DataType::Int32 => FieldType::Int32,
        DataType::Int64 => FieldType::Int64,
        DataType::Float32 => FieldType::Float32,
        DataType::Float64 => FieldType::Float64,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => FieldType::String,
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => FieldType::Bytes,
        DataType::Date32 => FieldType::Date,
        // Delta `timestamp_ntz` is wall-clock, the same frame the Iceberg reader
        // puts in `Timestamp`; Delta `timestamp` is UTC-adjusted.
        DataType::Timestamp(TimeUnit::Microsecond, None) => FieldType::Timestamp,
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => FieldType::TimestampTz,
        DataType::Decimal128(precision, scale) => FieldType::Decimal {
            precision: *precision,
            scale: *scale,
        },
        other => return Err(DeltaError::UnsupportedType(format!("{other:?}"))),
    })
}

fn array_mismatch(array: &dyn Array) -> DeltaError {
    DeltaError::SchemaMismatch(format!(
        "array does not match its declared type {:?}",
        array.data_type()
    ))
}

fn convert_column(array: &dyn Array) -> Result<Column> {
    macro_rules! typed {
        ($array:ty, $variant:path, $value:expr) => {{
            let a = array
                .as_any()
                .downcast_ref::<$array>()
                .ok_or_else(|| array_mismatch(array))?;
            $variant(a.iter().map(|v| v.map($value)).collect())
        }};
    }
    Ok(match array.data_type() {
        DataType::Boolean => typed!(BooleanArray, Column::Boolean, |v| v),
        DataType::Int8 => typed!(Int8Array, Column::Int32, i32::from),
        DataType::Int16 => typed!(Int16Array, Column::Int32, i32::from),
        DataType::Int32 => typed!(Int32Array, Column::Int32, |v| v),
        DataType::Int64 => typed!(Int64Array, Column::Int64, |v| v),
        DataType::Float32 => typed!(Float32Array, Column::Float32, |v| v),
        DataType::Float64 => typed!(Float64Array, Column::Float64, |v| v),
        DataType::Utf8 => typed!(StringArray, Column::String, str::to_owned),
        DataType::LargeUtf8 => typed!(LargeStringArray, Column::String, str::to_owned),
        DataType::Utf8View => typed!(StringViewArray, Column::String, str::to_owned),
        DataType::Binary => typed!(BinaryArray, Column::Bytes, <[u8]>::to_vec),
        DataType::LargeBinary => typed!(LargeBinaryArray, Column::Bytes, <[u8]>::to_vec),
        DataType::BinaryView => typed!(BinaryViewArray, Column::Bytes, <[u8]>::to_vec),
        DataType::FixedSizeBinary(_) => typed!(FixedSizeBinaryArray, Column::Bytes, <[u8]>::to_vec),
        DataType::Date32 => typed!(Date32Array, Column::Date, |v| v),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let a = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| array_mismatch(array))?;
            if tz.is_some() {
                Column::TimestampTz(a.iter().collect())
            } else {
                Column::Timestamp(a.iter().collect())
            }
        }
        DataType::Decimal128(precision, scale) => {
            let a = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| array_mismatch(array))?;
            Column::Decimal {
                values: a.iter().collect(),
                precision: *precision,
                scale: *scale,
            }
        }
        other => return Err(DeltaError::UnsupportedType(format!("{other:?}"))),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use delta_kernel::arrow::record_batch::RecordBatchOptions;

    fn convert(array: ArrayRef) -> Column {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            array.data_type().clone(),
            true,
        )]));
        let bridge = BatchBridge::new(schema.clone(), &[42]).unwrap();
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        bridge.convert(&batch).unwrap().columns.remove(0)
    }

    #[test]
    fn scalar_values_and_nulls_survive_slices() {
        // Slice offsets, negative values, Unicode, binary zero bytes, and nulls.
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(BooleanArray::from(vec![Some(false), None, Some(true)])),
            Arc::new(Int8Array::from(vec![Some(0), None, Some(i8::MIN)])),
            Arc::new(Int16Array::from(vec![Some(0), None, Some(i16::MIN)])),
            Arc::new(Int32Array::from(vec![Some(0), None, Some(i32::MIN)])),
            Arc::new(Int64Array::from(vec![Some(0), None, Some(i64::MIN)])),
            Arc::new(Float32Array::from(vec![Some(0.), None, Some(-1.25)])),
            Arc::new(Float64Array::from(vec![Some(0.), None, Some(-1.25)])),
            Arc::new(StringArray::from(vec![Some("skip"), None, Some("東京")])),
            Arc::new(BinaryArray::from(vec![
                Some(b"xx".as_slice()),
                None,
                Some(b"\0\xff".as_slice()),
            ])),
            Arc::new(Date32Array::from(vec![Some(0), None, Some(-1)])),
        ];
        let result: Vec<_> = arrays.iter().map(|a| convert(a.slice(1, 2))).collect();
        for column in &result {
            assert_eq!(column.len(), 2);
            assert!(column.is_null(0));
            assert!(!column.is_null(1));
        }
        assert_eq!(result[0].get_bool(1), Some(true));
        assert_eq!(result[1].get_i32(1), Some(i32::from(i8::MIN)));
        assert_eq!(result[2].get_i32(1), Some(i32::from(i16::MIN)));
        assert_eq!(result[3].get_i32(1), Some(i32::MIN));
        assert_eq!(result[4].get_i64(1), Some(i64::MIN));
        assert_eq!(result[5].get_f32(1), Some(-1.25));
        assert_eq!(result[6].get_f64(1), Some(-1.25));
        assert_eq!(result[7].get_string(1), Some("東京"));
        assert_eq!(result[8].get_bytes(1), Some(b"\0\xff".as_slice()));
        assert_eq!(result[9].get_date(1), Some(-1));
    }

    #[test]
    fn decimal_precision_and_timestamp_frames_are_preserved() {
        let decimal = Decimal128Array::from(vec![
            Some(-123_456_789_012_345_678_901_234_567_890_i128),
            None,
        ])
        .with_precision_and_scale(38, 10)
        .unwrap();
        let Column::Decimal {
            values,
            precision,
            scale,
        } = convert(Arc::new(decimal))
        else {
            panic!("wrong decimal type")
        };
        assert_eq!((precision, scale), (38, 10));
        assert_eq!(
            values,
            vec![Some(-123_456_789_012_345_678_901_234_567_890_i128), None]
        );
        let times = TimestampMicrosecondArray::from(vec![Some(-1_234_567), None]);
        let naive = convert(Arc::new(times.clone()));
        let utc = convert(Arc::new(times.with_timezone("UTC")));
        assert_eq!(naive.field_type(), FieldType::Timestamp);
        assert_eq!(utc.field_type(), FieldType::TimestampTz);
        assert_eq!(naive.get_timestamp(0), Some(-1_234_567));
        assert_eq!(utc.get_timestamp(0), Some(-1_234_567));
        assert!(naive.is_null(1) && utc.is_null(1));
    }

    #[test]
    fn view_and_large_arrays_preserve_empty_values_and_nulls() {
        let strings: Vec<ArrayRef> = vec![
            Arc::new(StringViewArray::from(vec![Some(""), None, Some("λ")])),
            Arc::new(LargeStringArray::from(vec![Some(""), None, Some("λ")])),
        ];
        for array in strings {
            let column = convert(array);
            assert_eq!(column.get_string(0), Some(""));
            assert_eq!(column.get_string(1), None);
            assert_eq!(column.get_string(2), Some("λ"));
        }
        let bytes = vec![Some(b"".as_slice()), None, Some(b"\xff".as_slice())];
        let binaries: Vec<ArrayRef> = vec![
            Arc::new(BinaryViewArray::from(bytes.clone())),
            Arc::new(LargeBinaryArray::from(bytes)),
        ];
        for array in binaries {
            let column = convert(array);
            assert_eq!(column.get_bytes(0), Some(b"".as_slice()));
            assert_eq!(column.get_bytes(1), None);
            assert_eq!(column.get_bytes(2), Some(b"\xff".as_slice()));
        }
    }

    #[test]
    fn caller_ids_survive_reordered_projection_and_schema_changes_fail() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("renamed", DataType::Int64, true),
            Field::new("id", DataType::Int64, true),
        ]));
        let bridge = BatchBridge::new(schema.clone(), &[87, 42]).unwrap();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![100])),
                Arc::new(Int64Array::from(vec![9])),
            ],
        )
        .unwrap();
        let converted = bridge.convert(&batch).unwrap().project(&[42, 87]).unwrap();
        assert_eq!(converted.column_by_id(42).unwrap().get_i64(0), Some(9));
        assert_eq!(converted.column_by_id(87).unwrap().get_i64(0), Some(100));
        assert_eq!(
            converted.schema.field_ids().collect::<Vec<_>>(),
            vec![42, 87]
        );
        assert!(BatchBridge::new(schema.clone(), &[42, 42]).is_err());
        assert!(BatchBridge::new(schema, &[42]).is_err());
        let changed = RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int64Array::from(vec![9])) as ArrayRef),
            ("renamed", Arc::new(Int64Array::from(vec![100]))),
        ])
        .unwrap();
        assert!(bridge.convert(&changed).is_err());
    }

    #[test]
    fn unsupported_schemas_fail_even_without_rows() {
        for data_type in [
            DataType::UInt64,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::List(Arc::new(Field::new("element", DataType::Int64, true))),
            DataType::Decimal256(50, 2),
        ] {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "unsupported",
                data_type,
                true,
            )]));
            assert!(BatchBridge::new(schema, &[1]).is_err());
        }
    }

    #[test]
    fn zero_column_batch_preserves_row_count() {
        let schema = Arc::new(Schema::empty());
        let bridge = BatchBridge::new(schema.clone(), &[]).unwrap();
        let batch = RecordBatch::try_new_with_options(
            schema,
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(7)),
        )
        .unwrap();
        let converted = bridge.convert(&batch).unwrap();
        assert_eq!(converted.num_rows, 7);
        assert!(converted.columns.is_empty());
    }
}
