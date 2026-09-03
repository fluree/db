//! Trino type names → `FieldType`, and JSON page values → `Column`.
//!
//! Trino's protocol renders every value as JSON: numbers for integers and
//! doubles (with `"NaN"`/`"Infinity"` strings for non-finite doubles), strings
//! for everything else — dates as `2024-01-01`, timestamps as
//! `2024-01-01 12:34:56.123`, zoned timestamps with a trailing zone, decimals as
//! their exact lexical form, varbinary as base64.

use std::sync::Arc;

use base64::Engine;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use fluree_db_tabular::{BatchSchema, Column, ColumnBatch, FieldInfo, FieldType};
use serde_json::Value;

use crate::error::{Result, SqlError};

/// Map a Trino type signature to a column type. Unknown or structural types
/// (`row`, `array`, `map`, …) land as `String`, carrying Trino's own rendering.
pub fn field_type_from_trino(type_name: &str) -> FieldType {
    let lower = type_name.trim().to_ascii_lowercase();
    // Precision sits between the base name and the zone suffix
    // (`timestamp(6) with time zone`), so settle temporals before splitting.
    if lower.starts_with("timestamp") {
        return if lower.ends_with("with time zone") {
            FieldType::TimestampTz
        } else {
            FieldType::Timestamp
        };
    }
    let (base, args) = match lower.find('(') {
        Some(i) => (
            lower[..i].trim_end(),
            Some(&lower[i + 1..lower.len().saturating_sub(1)]),
        ),
        None => (lower.as_str(), None),
    };
    match base {
        "boolean" => FieldType::Boolean,
        "tinyint" | "smallint" | "integer" | "int" => FieldType::Int32,
        "bigint" => FieldType::Int64,
        "real" | "float" => FieldType::Float32,
        "double" | "double precision" => FieldType::Float64,
        "varbinary" | "binary" | "bytea" => FieldType::Bytes,
        "date" => FieldType::Date,
        "decimal" | "numeric" => {
            let (p, s) = args
                .and_then(|a| {
                    let mut it = a.split(',').map(|x| x.trim().parse::<i64>().ok());
                    let p = it.next().flatten()?;
                    let s = it.next().flatten().unwrap_or(0);
                    Some((p, s))
                })
                .unwrap_or((38, 0));
            FieldType::Decimal {
                precision: p.clamp(1, 76) as u8,
                scale: s.clamp(-128, 127) as i8,
            }
        }
        _ => FieldType::String,
    }
}

/// Build a batch schema from the protocol's column list. Field ids are
/// positional (1-based); the R2RML layer looks columns up by name.
pub fn schema_from_columns(columns: &[(String, String)]) -> Arc<BatchSchema> {
    let fields = columns
        .iter()
        .enumerate()
        .map(|(i, (name, ty))| FieldInfo {
            name: name.clone(),
            field_type: field_type_from_trino(ty),
            nullable: true,
            field_id: i as i32 + 1,
        })
        .collect();
    Arc::new(BatchSchema::new(fields))
}

/// Decode one page of rows into a batch.
pub fn decode_rows(schema: &Arc<BatchSchema>, rows: Vec<Vec<Value>>) -> Result<ColumnBatch> {
    let n = rows.len();
    let mut columns: Vec<Column> = schema
        .fields
        .iter()
        .map(|f| Column::with_capacity(f.field_type, n))
        .collect();

    for (row_idx, row) in rows.into_iter().enumerate() {
        if row.len() != columns.len() {
            return Err(SqlError::Decode(format!(
                "row {row_idx} has {} values but the schema has {} columns",
                row.len(),
                columns.len()
            )));
        }
        for (col_idx, value) in row.into_iter().enumerate() {
            let field = &schema.fields[col_idx];
            push_value(&mut columns[col_idx], &field.name, field.field_type, value)?;
        }
    }

    ColumnBatch::new(Arc::clone(schema), columns).map_err(|e| SqlError::Decode(e.to_string()))
}

fn push_value(column: &mut Column, name: &str, ty: FieldType, value: Value) -> Result<()> {
    let bad = |what: &str, v: &Value| {
        SqlError::Decode(format!(
            "column '{name}' ({ty:?}): expected {what}, got {v}"
        ))
    };
    match column {
        Column::Boolean(v) => v.push(match value {
            Value::Null => None,
            Value::Bool(b) => Some(b),
            other => return Err(bad("boolean", &other)),
        }),
        Column::Int32(v) => v.push(match value {
            Value::Null => None,
            Value::Number(ref n) => Some(
                n.as_i64()
                    .and_then(|i| i32::try_from(i).ok())
                    .ok_or_else(|| bad("32-bit integer", &value))?,
            ),
            other => return Err(bad("integer", &other)),
        }),
        Column::Int64(v) => v.push(match value {
            Value::Null => None,
            Value::Number(ref n) => Some(n.as_i64().ok_or_else(|| bad("64-bit integer", &value))?),
            other => return Err(bad("integer", &other)),
        }),
        Column::Float32(v) => v.push(
            parse_double(&value)
                .map_err(|()| bad("real", &value))?
                .map(|d| d as f32),
        ),
        Column::Float64(v) => v.push(parse_double(&value).map_err(|()| bad("double", &value))?),
        Column::String(v) => v.push(match value {
            Value::Null => None,
            Value::String(s) => Some(s),
            // Structural / unknown types arrive as JSON; keep their rendering.
            other => Some(other.to_string()),
        }),
        Column::Bytes(v) => v.push(match value {
            Value::Null => None,
            Value::String(ref s) => Some(
                base64::engine::general_purpose::STANDARD
                    .decode(s)
                    .map_err(|_| bad("base64 varbinary", &value))?,
            ),
            other => return Err(bad("base64 varbinary", &other)),
        }),
        Column::Date(v) => v.push(match value {
            Value::Null => None,
            Value::String(ref s) => Some(parse_date_days(s).ok_or_else(|| bad("date", &value))?),
            other => return Err(bad("date", &other)),
        }),
        Column::Timestamp(v) => v.push(match value {
            Value::Null => None,
            Value::String(ref s) => {
                Some(parse_timestamp_micros(s).ok_or_else(|| bad("timestamp", &value))?)
            }
            other => return Err(bad("timestamp", &other)),
        }),
        Column::TimestampTz(v) => v.push(match value {
            Value::Null => None,
            Value::String(ref s) => Some(parse_timestamp_micros(s).ok_or_else(|| {
                SqlError::Decode(format!(
                    "column '{name}' (timestamp with time zone): cannot decode '{s}' — \
                         only numeric offsets and UTC/GMT/Z zones are supported; select the \
                         column `AT TIME ZONE 'UTC'` or use the Trino dialect, which does so"
                ))
            })?),
            other => return Err(bad("timestamp with time zone", &other)),
        }),
        Column::Decimal { values, scale, .. } => values.push(match value {
            Value::Null => None,
            Value::String(ref s) => {
                Some(parse_decimal_unscaled(s, *scale).ok_or_else(|| bad("decimal", &value))?)
            }
            Value::Number(ref n) => Some(
                parse_decimal_unscaled(&n.to_string(), *scale)
                    .ok_or_else(|| bad("decimal", &value))?,
            ),
            other => return Err(bad("decimal", &other)),
        }),
    }
    Ok(())
}

fn parse_double(value: &Value) -> std::result::Result<Option<f64>, ()> {
    match value {
        Value::Null => Ok(None),
        Value::Number(n) => n.as_f64().map(Some).ok_or(()),
        Value::String(s) => match s.as_str() {
            "NaN" => Ok(Some(f64::NAN)),
            "Infinity" | "+Infinity" => Ok(Some(f64::INFINITY)),
            "-Infinity" => Ok(Some(f64::NEG_INFINITY)),
            other => other.parse::<f64>().map(Some).map_err(|_| ()),
        },
        _ => Err(()),
    }
}

/// `YYYY-MM-DD` → days since the epoch.
pub fn parse_date_days(s: &str) -> Option<i32> {
    let d = NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d").ok()?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
    i32::try_from((d - epoch).num_days()).ok()
}

/// `YYYY-MM-DD HH:MM:SS[.f{1,12}][ ZONE]` → micros since the epoch (UTC frame
/// once the zone is applied). Sub-microsecond digits are truncated. Zones:
/// `UTC`, `GMT`, `Z`, or `±HH:MM`. Named regions return `None`.
pub fn parse_timestamp_micros(s: &str) -> Option<i64> {
    let s = s.trim();
    let (date_part, rest) = s.split_once(' ')?;
    let (time_part, zone) = match rest.split_once(' ') {
        Some((t, z)) => (t, Some(z.trim())),
        None => (rest, None),
    };

    let date = NaiveDate::parse_from_str(date_part, "%Y-%m-%d").ok()?;
    let (hms, frac) = match time_part.split_once('.') {
        Some((h, f)) => (h, Some(f)),
        None => (time_part, None),
    };
    let time = NaiveTime::parse_from_str(hms, "%H:%M:%S").ok()?;
    let micros_frac: i64 = match frac {
        Some(f) => {
            if f.is_empty() || !f.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            let mut padded: String = f.chars().take(6).collect();
            while padded.len() < 6 {
                padded.push('0');
            }
            padded.parse().ok()?
        }
        None => 0,
    };

    let naive = NaiveDateTime::new(date, time);
    let base = naive
        .and_utc()
        .timestamp_micros()
        .checked_add(micros_frac)?;
    let offset_secs = match zone {
        None => 0,
        Some(z) => parse_zone_offset_secs(z)?,
    };
    base.checked_sub(i64::from(offset_secs) * 1_000_000)
}

fn parse_zone_offset_secs(z: &str) -> Option<i32> {
    match z {
        "UTC" | "GMT" | "Z" | "+00:00" | "-00:00" => Some(0),
        _ => {
            let sign = match z.as_bytes().first()? {
                b'+' => 1,
                b'-' => -1,
                _ => return None,
            };
            let (h, m) = z[1..].split_once(':')?;
            let h: i32 = h.parse().ok()?;
            let m: i32 = m.parse().ok()?;
            if h > 23 || m > 59 {
                return None;
            }
            Some(sign * (h * 3600 + m * 60))
        }
    }
}

/// Exact lexical decimal → unscaled integer at the column's scale. Extra
/// fractional digits beyond `scale` are rejected (the engine would silently
/// misreport the value otherwise).
pub fn parse_decimal_unscaled(s: &str, scale: i8) -> Option<i128> {
    let s = s.trim();
    let (negative, body) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (int_part, frac_part) = body.split_once('.').unwrap_or((body, ""));
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let scale = usize::try_from(scale).ok()?;
    if frac_part.len() > scale && frac_part[scale..].bytes().any(|b| b != b'0') {
        return None;
    }
    let mut digits = String::with_capacity(int_part.len() + scale);
    digits.push_str(int_part);
    digits.push_str(&frac_part[..frac_part.len().min(scale)]);
    for _ in frac_part.len().min(scale)..scale {
        digits.push('0');
    }
    let mut v: i128 = if digits.is_empty() {
        0
    } else {
        digits.parse().ok()?
    };
    if negative {
        v = -v;
    }
    Some(v)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn trino_type_names_map() {
        assert_eq!(field_type_from_trino("bigint"), FieldType::Int64);
        assert_eq!(field_type_from_trino("integer"), FieldType::Int32);
        assert_eq!(field_type_from_trino("varchar(20)"), FieldType::String);
        assert_eq!(field_type_from_trino("varchar"), FieldType::String);
        assert_eq!(field_type_from_trino("double"), FieldType::Float64);
        assert_eq!(field_type_from_trino("real"), FieldType::Float32);
        assert_eq!(field_type_from_trino("date"), FieldType::Date);
        assert_eq!(field_type_from_trino("timestamp(3)"), FieldType::Timestamp);
        assert_eq!(
            field_type_from_trino("timestamp(6) with time zone"),
            FieldType::TimestampTz
        );
        assert_eq!(
            field_type_from_trino("timestamp with time zone"),
            FieldType::TimestampTz
        );
        assert_eq!(
            field_type_from_trino("decimal(10,2)"),
            FieldType::Decimal {
                precision: 10,
                scale: 2
            }
        );
        assert_eq!(
            field_type_from_trino("decimal(38, 0)"),
            FieldType::Decimal {
                precision: 38,
                scale: 0
            }
        );
        assert_eq!(field_type_from_trino("varbinary"), FieldType::Bytes);
        assert_eq!(field_type_from_trino("array(varchar)"), FieldType::String);
        assert_eq!(field_type_from_trino("row(a bigint)"), FieldType::String);
    }

    #[test]
    fn timestamps_parse_with_offsets_and_truncate_nanos() {
        assert_eq!(parse_timestamp_micros("1970-01-01 00:00:00"), Some(0));
        assert_eq!(
            parse_timestamp_micros("1970-01-01 00:00:00.123"),
            Some(123_000)
        );
        assert_eq!(
            parse_timestamp_micros("1970-01-01 00:00:00.123456789"),
            Some(123_456)
        );
        assert_eq!(
            parse_timestamp_micros("1970-01-01 01:00:00 +01:00"),
            Some(0)
        );
        assert_eq!(
            parse_timestamp_micros("1970-01-01 00:00:00.5 UTC"),
            Some(500_000)
        );
        assert_eq!(
            parse_timestamp_micros("1969-12-31 19:00:00 -05:00"),
            Some(0)
        );
        assert_eq!(
            parse_timestamp_micros("1970-01-01 00:00:00 America/New_York"),
            None
        );
        assert_eq!(parse_timestamp_micros("garbage"), None);
        assert_eq!(parse_date_days("1970-01-02"), Some(1));
        assert_eq!(parse_date_days("2024-01-01"), Some(19_723));
    }

    #[test]
    fn decimals_parse_exactly() {
        assert_eq!(parse_decimal_unscaled("12.34", 2), Some(1234));
        assert_eq!(parse_decimal_unscaled("-0.05", 2), Some(-5));
        assert_eq!(parse_decimal_unscaled("5", 2), Some(500));
        assert_eq!(parse_decimal_unscaled("5.1", 2), Some(510));
        assert_eq!(parse_decimal_unscaled("5.100", 2), Some(510));
        assert_eq!(parse_decimal_unscaled("5.101", 2), None);
        assert_eq!(parse_decimal_unscaled("abc", 2), None);
        assert_eq!(parse_decimal_unscaled(".5", 1), Some(5));
    }

    #[test]
    fn decode_a_page() {
        let schema = schema_from_columns(&[
            ("id".into(), "bigint".into()),
            ("n".into(), "integer".into()),
            ("name".into(), "varchar".into()),
            ("d".into(), "double".into()),
            ("born".into(), "date".into()),
            ("at".into(), "timestamp(3) with time zone".into()),
            ("price".into(), "decimal(10,2)".into()),
            ("raw".into(), "varbinary".into()),
            ("ok".into(), "boolean".into()),
            ("tags".into(), "array(varchar)".into()),
        ]);
        let rows = vec![
            vec![
                json!(1),
                json!(2),
                json!("a"),
                json!(1.5),
                json!("2024-01-01"),
                json!("2023-11-14 22:13:20.000 UTC"),
                json!("12.34"),
                json!("aGk="),
                json!(true),
                json!(["x", "y"]),
            ],
            vec![
                json!(2),
                Value::Null,
                Value::Null,
                json!("NaN"),
                Value::Null,
                json!("2023-11-14 23:13:20.000 +01:00"),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
            ],
        ];
        let batch = decode_rows(&schema, rows).unwrap();
        assert_eq!(batch.num_rows, 2);
        let id = batch.column_by_name("id").unwrap();
        assert_eq!(id.get_i64(0), Some(1));
        assert_eq!(batch.column_by_name("n").unwrap().get_i32(0), Some(2));
        assert_eq!(batch.column_by_name("name").unwrap().get_string(1), None);
        assert!(batch
            .column_by_name("d")
            .unwrap()
            .get_f64(1)
            .unwrap()
            .is_nan());
        assert_eq!(
            batch.column_by_name("born").unwrap().get_date(0),
            Some(19_723)
        );
        let at = batch.column_by_name("at").unwrap();
        assert_eq!(at.get_timestamp(0), Some(1_700_000_000_000_000));
        assert_eq!(at.get_timestamp(1), Some(1_700_000_000_000_000));
        match batch.column_by_name("price").unwrap() {
            Column::Decimal { values, scale, .. } => {
                assert_eq!(*scale, 2);
                assert_eq!(values[0], Some(1234));
                assert_eq!(values[1], None);
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(
            batch.column_by_name("raw").unwrap().get_bytes(0),
            Some(&b"hi"[..])
        );
        assert_eq!(batch.column_by_name("ok").unwrap().get_bool(0), Some(true));
        assert_eq!(
            batch.column_by_name("tags").unwrap().get_string(0),
            Some(r#"["x","y"]"#)
        );
    }

    #[test]
    fn decode_rejects_shape_and_type_errors() {
        let schema = schema_from_columns(&[("id".into(), "bigint".into())]);
        assert!(decode_rows(&schema, vec![vec![json!(1), json!(2)]]).is_err());
        assert!(decode_rows(&schema, vec![vec![json!("x")]]).is_err());
        let schema = schema_from_columns(&[("at".into(), "timestamp with time zone".into())]);
        let err = decode_rows(
            &schema,
            vec![vec![json!("2024-01-01 00:00:00 Europe/Oslo")]],
        )
        .unwrap_err();
        assert!(err.to_string().contains("AT TIME ZONE"));
    }
}
