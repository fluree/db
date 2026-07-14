//! Iceberg binary value encoding/decoding.
//!
//! This module provides functions to decode Iceberg-encoded binary values from
//! manifest file bounds (lower_bounds, upper_bounds) and partition data.
//!
//! Iceberg uses little-endian encoding for numeric types and UTF-8 for strings.
//! See: https://iceberg.apache.org/spec/#appendix-d-single-value-serialization

use crate::error::{IcebergError, Result};
use crate::metadata::SchemaField;

/// Typed value for comparison during pruning.
///
/// This enum represents values decoded from Iceberg's binary encoding,
/// suitable for comparison operations in partition/file pruning.
#[derive(Debug, Clone, PartialEq)]
pub enum TypedValue {
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    /// Date: days since 1970-01-01
    Date(i32),
    /// Timestamp: microseconds since epoch (UTC)
    Timestamp(i64),
    /// TimestampTz: microseconds since epoch with timezone
    TimestampTz(i64),
    String(String),
    Bytes(Vec<u8>),
    /// UUID as 16 bytes
    Uuid([u8; 16]),
    /// Decimal: (unscaled_value, precision, scale)
    Decimal {
        unscaled: i128,
        precision: u8,
        scale: i8,
    },
}

impl TypedValue {
    /// Check if this value is less than another of the same type.
    ///
    /// Returns None if the types are incompatible.
    pub fn lt(&self, other: &Self) -> Option<bool> {
        match (self, other) {
            (TypedValue::Boolean(a), TypedValue::Boolean(b)) => Some(!a && *b),
            (TypedValue::Int32(a), TypedValue::Int32(b)) => Some(a < b),
            (TypedValue::Int64(a), TypedValue::Int64(b)) => Some(a < b),
            // NaN is unordered: a NaN operand yields None (incomparable), so
            // `bounds_can_contain`'s `unwrap_or(true)` KEEPS the row group. Raw
            // `<` would return `Some(false)` and could prune a group holding NaN
            // rows — a strict-superset violation. (Reachable only once a float
            // predicate is pushed to the reader; see F15.)
            (TypedValue::Float32(a), TypedValue::Float32(b)) => {
                (!a.is_nan() && !b.is_nan()).then(|| a < b)
            }
            (TypedValue::Float64(a), TypedValue::Float64(b)) => {
                (!a.is_nan() && !b.is_nan()).then(|| a < b)
            }
            (TypedValue::Date(a), TypedValue::Date(b)) => Some(a < b),
            (TypedValue::Timestamp(a), TypedValue::Timestamp(b)) => Some(a < b),
            (TypedValue::TimestampTz(a), TypedValue::TimestampTz(b)) => Some(a < b),
            (TypedValue::String(a), TypedValue::String(b)) => Some(a < b),
            (TypedValue::Bytes(a), TypedValue::Bytes(b)) => Some(a < b),
            // Decimals compare by real value across differing scales (see
            // `decimal_cmp`). Without this arm a pushed decimal predicate compares
            // None → the row group is always kept (never pruned).
            (
                TypedValue::Decimal {
                    unscaled: a,
                    scale: sa,
                    ..
                },
                TypedValue::Decimal {
                    unscaled: b,
                    scale: sb,
                    ..
                },
            ) => decimal_cmp(*a, *sa, *b, *sb).map(std::cmp::Ordering::is_lt),
            _ => None,
        }
    }

    /// Check if this value is less than or equal to another of the same type.
    pub fn le(&self, other: &Self) -> Option<bool> {
        match (self, other) {
            (TypedValue::Boolean(a), TypedValue::Boolean(b)) => Some(a <= b),
            (TypedValue::Int32(a), TypedValue::Int32(b)) => Some(a <= b),
            (TypedValue::Int64(a), TypedValue::Int64(b)) => Some(a <= b),
            // NaN → None → keep the row group (see `lt` and F15).
            (TypedValue::Float32(a), TypedValue::Float32(b)) => {
                (!a.is_nan() && !b.is_nan()).then(|| a <= b)
            }
            (TypedValue::Float64(a), TypedValue::Float64(b)) => {
                (!a.is_nan() && !b.is_nan()).then(|| a <= b)
            }
            (TypedValue::Date(a), TypedValue::Date(b)) => Some(a <= b),
            (TypedValue::Timestamp(a), TypedValue::Timestamp(b)) => Some(a <= b),
            (TypedValue::TimestampTz(a), TypedValue::TimestampTz(b)) => Some(a <= b),
            (TypedValue::String(a), TypedValue::String(b)) => Some(a <= b),
            (TypedValue::Bytes(a), TypedValue::Bytes(b)) => Some(a <= b),
            (
                TypedValue::Decimal {
                    unscaled: a,
                    scale: sa,
                    ..
                },
                TypedValue::Decimal {
                    unscaled: b,
                    scale: sb,
                    ..
                },
            ) => decimal_cmp(*a, *sa, *b, *sb).map(std::cmp::Ordering::is_le),
            _ => None,
        }
    }

    /// Check if this value is greater than another of the same type.
    pub fn gt(&self, other: &Self) -> Option<bool> {
        other.lt(self)
    }

    /// Check if this value is greater than or equal to another of the same type.
    pub fn ge(&self, other: &Self) -> Option<bool> {
        other.le(self)
    }
}

impl PartialOrd for TypedValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (TypedValue::Boolean(a), TypedValue::Boolean(b)) => a.partial_cmp(b),
            (TypedValue::Int32(a), TypedValue::Int32(b)) => a.partial_cmp(b),
            (TypedValue::Int64(a), TypedValue::Int64(b)) => a.partial_cmp(b),
            (TypedValue::Float32(a), TypedValue::Float32(b)) => a.partial_cmp(b),
            (TypedValue::Float64(a), TypedValue::Float64(b)) => a.partial_cmp(b),
            (TypedValue::Date(a), TypedValue::Date(b)) => a.partial_cmp(b),
            (TypedValue::Timestamp(a), TypedValue::Timestamp(b)) => a.partial_cmp(b),
            (TypedValue::TimestampTz(a), TypedValue::TimestampTz(b)) => a.partial_cmp(b),
            (TypedValue::String(a), TypedValue::String(b)) => a.partial_cmp(b),
            (TypedValue::Bytes(a), TypedValue::Bytes(b)) => a.partial_cmp(b),
            // UUIDs compare lexicographically by their 16 big-endian bytes
            // (matching Iceberg's UUID bound ordering).
            (TypedValue::Uuid(a), TypedValue::Uuid(b)) => Some(a.cmp(b)),
            // Decimals compare by real value. Without these arms a multi-file
            // decimal column's min/max aggregation kept the FIRST file's bound
            // (partial_cmp → None → "keep current") instead of the true extremum.
            (
                TypedValue::Decimal {
                    unscaled: a,
                    scale: sa,
                    ..
                },
                TypedValue::Decimal {
                    unscaled: b,
                    scale: sb,
                    ..
                },
            ) => decimal_cmp(*a, *sa, *b, *sb),
            _ => None,
        }
    }
}

/// Compare two decimals by real value, normalizing to a common scale.
///
/// Same-scale decimals — the common case, since a single Iceberg column has one
/// fixed scale — compare by unscaled value directly. Differing scales are
/// brought to a common scale with checked arithmetic; an overflow yields `None`
/// (incomparable) rather than a wrong answer.
fn decimal_cmp(a: i128, sa: i8, b: i128, sb: i8) -> Option<std::cmp::Ordering> {
    if sa == sb {
        return Some(a.cmp(&b));
    }
    let common = sa.max(sb);
    let a_adj = rescale(a, i32::from(common) - i32::from(sa))?;
    let b_adj = rescale(b, i32::from(common) - i32::from(sb))?;
    Some(a_adj.cmp(&b_adj))
}

/// Multiply `value` by `10^exp` (`exp >= 0`), returning `None` on i128 overflow.
fn rescale(value: i128, exp: i32) -> Option<i128> {
    let mut acc = value;
    for _ in 0..exp {
        acc = acc.checked_mul(10)?;
    }
    Some(acc)
}

/// Decode Iceberg-encoded bytes into a typed value.
///
/// # Arguments
///
/// * `bytes` - The Iceberg-encoded binary value
/// * `field` - The schema field containing type information
///
/// # Returns
///
/// The decoded typed value, or an error if the type is unsupported or
/// the bytes cannot be decoded.
pub fn decode_bound(bytes: &[u8], field: &SchemaField) -> Result<TypedValue> {
    decode_by_type_string(bytes, field.type_string())
}

/// Decode bytes by type string.
pub fn decode_by_type_string(bytes: &[u8], type_str: Option<&str>) -> Result<TypedValue> {
    let type_str = type_str
        .ok_or_else(|| IcebergError::Manifest("Cannot decode bound for nested type".to_string()))?;

    match type_str {
        "boolean" => {
            if bytes.is_empty() {
                return Err(IcebergError::Manifest(
                    "Empty bytes for boolean".to_string(),
                ));
            }
            Ok(TypedValue::Boolean(bytes[0] != 0))
        }
        "int" => {
            let arr: [u8; 4] = bytes.try_into().map_err(|_| {
                IcebergError::Manifest(format!(
                    "Invalid int bytes length: expected 4, got {}",
                    bytes.len()
                ))
            })?;
            Ok(TypedValue::Int32(i32::from_le_bytes(arr)))
        }
        "long" => {
            let arr: [u8; 8] = bytes.try_into().map_err(|_| {
                IcebergError::Manifest(format!(
                    "Invalid long bytes length: expected 8, got {}",
                    bytes.len()
                ))
            })?;
            Ok(TypedValue::Int64(i64::from_le_bytes(arr)))
        }
        "float" => {
            let arr: [u8; 4] = bytes.try_into().map_err(|_| {
                IcebergError::Manifest(format!(
                    "Invalid float bytes length: expected 4, got {}",
                    bytes.len()
                ))
            })?;
            Ok(TypedValue::Float32(f32::from_le_bytes(arr)))
        }
        "double" => {
            let arr: [u8; 8] = bytes.try_into().map_err(|_| {
                IcebergError::Manifest(format!(
                    "Invalid double bytes length: expected 8, got {}",
                    bytes.len()
                ))
            })?;
            Ok(TypedValue::Float64(f64::from_le_bytes(arr)))
        }
        "date" => {
            let arr: [u8; 4] = bytes.try_into().map_err(|_| {
                IcebergError::Manifest(format!(
                    "Invalid date bytes length: expected 4, got {}",
                    bytes.len()
                ))
            })?;
            Ok(TypedValue::Date(i32::from_le_bytes(arr)))
        }
        "timestamp" => {
            let arr: [u8; 8] = bytes.try_into().map_err(|_| {
                IcebergError::Manifest(format!(
                    "Invalid timestamp bytes length: expected 8, got {}",
                    bytes.len()
                ))
            })?;
            Ok(TypedValue::Timestamp(i64::from_le_bytes(arr)))
        }
        "timestamptz" => {
            let arr: [u8; 8] = bytes.try_into().map_err(|_| {
                IcebergError::Manifest(format!(
                    "Invalid timestamptz bytes length: expected 8, got {}",
                    bytes.len()
                ))
            })?;
            Ok(TypedValue::TimestampTz(i64::from_le_bytes(arr)))
        }
        "string" => {
            let s = std::str::from_utf8(bytes)
                .map_err(|e| IcebergError::Manifest(format!("Invalid UTF-8 in string: {e}")))?;
            Ok(TypedValue::String(s.to_string()))
        }
        "binary" | "fixed" => Ok(TypedValue::Bytes(bytes.to_vec())),
        "uuid" => {
            let arr: [u8; 16] = bytes.try_into().map_err(|_| {
                IcebergError::Manifest(format!(
                    "Invalid UUID bytes length: expected 16, got {}",
                    bytes.len()
                ))
            })?;
            Ok(TypedValue::Uuid(arr))
        }
        s if s.starts_with("decimal") => {
            // Parse decimal(precision, scale)
            let inner = s.trim_start_matches("decimal(").trim_end_matches(')');
            let parts: Vec<&str> = inner.split(',').collect();
            if parts.len() != 2 {
                return Err(IcebergError::Manifest(format!("Invalid decimal type: {s}")));
            }
            let precision: u8 = parts[0].trim().parse().map_err(|_| {
                IcebergError::Manifest(format!("Invalid decimal precision: {}", parts[0]))
            })?;
            let scale: i8 = parts[1].trim().parse().map_err(|_| {
                IcebergError::Manifest(format!("Invalid decimal scale: {}", parts[1]))
            })?;

            // Decimal is stored as unscaled big-endian two's complement
            let unscaled = decode_decimal_bytes(bytes)?;

            Ok(TypedValue::Decimal {
                unscaled,
                precision,
                scale,
            })
        }
        _ => Err(IcebergError::Manifest(format!(
            "Unsupported type for bound decoding: {type_str}"
        ))),
    }
}

/// Decode decimal bytes (big-endian two's complement) to i128.
fn decode_decimal_bytes(bytes: &[u8]) -> Result<i128> {
    if bytes.is_empty() {
        return Ok(0);
    }
    if bytes.len() > 16 {
        return Err(IcebergError::Manifest(format!(
            "Decimal too large: {} bytes (max 16)",
            bytes.len()
        )));
    }

    // Sign-extend to 16 bytes
    let is_negative = (bytes[0] & 0x80) != 0;
    let mut padded = if is_negative { [0xFF; 16] } else { [0x00; 16] };

    // Copy bytes to the end (big-endian)
    let start = 16 - bytes.len();
    padded[start..].copy_from_slice(bytes);

    Ok(i128::from_be_bytes(padded))
}

/// Encode a typed value back to Iceberg binary format.
///
/// This is useful for building test fixtures and for writing manifest files.
pub fn encode_value(value: &TypedValue) -> Vec<u8> {
    match value {
        TypedValue::Boolean(v) => vec![u8::from(*v)],
        TypedValue::Int32(v) => v.to_le_bytes().to_vec(),
        TypedValue::Int64(v) => v.to_le_bytes().to_vec(),
        TypedValue::Float32(v) => v.to_le_bytes().to_vec(),
        TypedValue::Float64(v) => v.to_le_bytes().to_vec(),
        TypedValue::Date(v) => v.to_le_bytes().to_vec(),
        TypedValue::Timestamp(v) | TypedValue::TimestampTz(v) => v.to_le_bytes().to_vec(),
        TypedValue::String(v) => v.as_bytes().to_vec(),
        TypedValue::Bytes(v) => v.clone(),
        TypedValue::Uuid(v) => v.to_vec(),
        TypedValue::Decimal { unscaled, .. } => {
            // Encode as big-endian, trimming leading sign-extension bytes
            let bytes = unscaled.to_be_bytes();
            let is_negative = *unscaled < 0;
            let skip_byte = if is_negative { 0xFF } else { 0x00 };

            // Find first byte that differs from sign extension
            let start = bytes
                .iter()
                .position(|&b| {
                    if b == skip_byte {
                        false
                    } else if is_negative {
                        // For negative, keep if MSB would change sign
                        (b & 0x80) == 0
                    } else {
                        // For positive, keep if MSB would indicate negative
                        (b & 0x80) != 0
                    }
                })
                .unwrap_or(bytes.len().saturating_sub(1));

            bytes[start..].to_vec()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_boolean() {
        assert_eq!(
            decode_by_type_string(&[0], Some("boolean")).unwrap(),
            TypedValue::Boolean(false)
        );
        assert_eq!(
            decode_by_type_string(&[1], Some("boolean")).unwrap(),
            TypedValue::Boolean(true)
        );
        assert_eq!(
            decode_by_type_string(&[42], Some("boolean")).unwrap(),
            TypedValue::Boolean(true)
        );
    }

    #[test]
    fn test_decode_int() {
        // 42 in little-endian
        let bytes = 42i32.to_le_bytes();
        assert_eq!(
            decode_by_type_string(&bytes, Some("int")).unwrap(),
            TypedValue::Int32(42)
        );

        // Negative number
        let bytes = (-100i32).to_le_bytes();
        assert_eq!(
            decode_by_type_string(&bytes, Some("int")).unwrap(),
            TypedValue::Int32(-100)
        );
    }

    #[test]
    fn test_decode_long() {
        let bytes = 1_234_567_890_123_i64.to_le_bytes();
        assert_eq!(
            decode_by_type_string(&bytes, Some("long")).unwrap(),
            TypedValue::Int64(1_234_567_890_123)
        );
    }

    #[test]
    fn test_decode_float() {
        let bytes = 3.13f32.to_le_bytes();
        assert_eq!(
            decode_by_type_string(&bytes, Some("float")).unwrap(),
            TypedValue::Float32(3.13)
        );
    }

    #[test]
    fn test_decode_double() {
        let bytes = 3.131_592_653_59_f64.to_le_bytes();
        assert_eq!(
            decode_by_type_string(&bytes, Some("double")).unwrap(),
            TypedValue::Float64(3.131_592_653_59)
        );
    }

    #[test]
    fn test_decode_date() {
        // Days since epoch (e.g., 2024-01-01 = 19723 days since 1970-01-01)
        let bytes = 19723i32.to_le_bytes();
        assert_eq!(
            decode_by_type_string(&bytes, Some("date")).unwrap(),
            TypedValue::Date(19723)
        );
    }

    #[test]
    fn test_decode_timestamp() {
        let micros = 1_700_000_000_000_000_i64; // microseconds since epoch
        let bytes = micros.to_le_bytes();
        assert_eq!(
            decode_by_type_string(&bytes, Some("timestamp")).unwrap(),
            TypedValue::Timestamp(micros)
        );
    }

    #[test]
    fn test_decode_string() {
        let bytes = b"hello world";
        assert_eq!(
            decode_by_type_string(bytes, Some("string")).unwrap(),
            TypedValue::String("hello world".to_string())
        );
    }

    #[test]
    fn test_decode_binary() {
        let bytes = vec![0x01, 0x02, 0x03, 0x04];
        assert_eq!(
            decode_by_type_string(&bytes, Some("binary")).unwrap(),
            TypedValue::Bytes(bytes.clone())
        );
    }

    #[test]
    fn test_decode_uuid() {
        let bytes: [u8; 16] = [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ];
        assert_eq!(
            decode_by_type_string(&bytes, Some("uuid")).unwrap(),
            TypedValue::Uuid(bytes)
        );
    }

    #[test]
    fn test_decode_decimal() {
        // 12345 with precision=10, scale=2 means 123.45
        // 12345 in big-endian two's complement
        let value = 12345i128;
        let bytes = value.to_be_bytes();
        // Trim leading zeros
        let trimmed: Vec<u8> = bytes.into_iter().skip_while(|&b| b == 0).collect();

        let result = decode_by_type_string(&trimmed, Some("decimal(10, 2)")).unwrap();
        assert_eq!(
            result,
            TypedValue::Decimal {
                unscaled: 12345,
                precision: 10,
                scale: 2
            }
        );
    }

    #[test]
    fn test_decode_negative_decimal() {
        // -12345 in big-endian two's complement
        let value = -12345i128;
        let bytes = value.to_be_bytes();
        // Keep sign-extension bytes (0xFF prefix for negative)
        let trimmed: Vec<u8> = bytes.into_iter().skip_while(|&b| b == 0xFF).collect();
        // Need at least the MSB to preserve sign
        let trimmed = if trimmed.is_empty() || (trimmed[0] & 0x80) == 0 {
            let mut v = vec![0xFF];
            v.extend(&trimmed);
            v
        } else {
            trimmed
        };

        let result = decode_by_type_string(&trimmed, Some("decimal(10, 2)")).unwrap();
        assert_eq!(
            result,
            TypedValue::Decimal {
                unscaled: -12345,
                precision: 10,
                scale: 2
            }
        );
    }

    #[test]
    fn test_roundtrip_encode_decode() {
        let values = vec![
            TypedValue::Boolean(true),
            TypedValue::Boolean(false),
            TypedValue::Int32(42),
            TypedValue::Int32(-100),
            TypedValue::Int64(1_234_567_890_123),
            TypedValue::Float32(3.13),
            TypedValue::Float64(3.131_592_653_59),
            TypedValue::Date(19723),
            TypedValue::Timestamp(1_700_000_000_000_000),
            TypedValue::String("hello".to_string()),
            TypedValue::Bytes(vec![1, 2, 3, 4]),
        ];

        let types = vec![
            "boolean",
            "boolean",
            "int",
            "int",
            "long",
            "float",
            "double",
            "date",
            "timestamp",
            "string",
            "binary",
        ];

        for (value, type_str) in values.iter().zip(types.iter()) {
            let encoded = encode_value(value);
            let decoded = decode_by_type_string(&encoded, Some(type_str)).unwrap();
            assert_eq!(&decoded, value, "Roundtrip failed for type {type_str}");
        }
    }

    #[test]
    fn test_typed_value_comparison() {
        let a = TypedValue::Int32(10);
        let b = TypedValue::Int32(20);

        assert_eq!(a.lt(&b), Some(true));
        assert_eq!(a.le(&b), Some(true));
        assert_eq!(a.gt(&b), Some(false));
        assert_eq!(a.ge(&b), Some(false));

        assert_eq!(b.lt(&a), Some(false));
        assert_eq!(b.gt(&a), Some(true));

        // Same value
        let c = TypedValue::Int32(10);
        assert_eq!(a.lt(&c), Some(false));
        assert_eq!(a.le(&c), Some(true));
        assert_eq!(a.ge(&c), Some(true));
        assert_eq!(a.gt(&c), Some(false));

        // Different types - comparison returns None
        let s = TypedValue::String("hello".to_string());
        assert_eq!(a.lt(&s), None);
    }

    #[test]
    fn test_string_comparison() {
        let a = TypedValue::String("apple".to_string());
        let b = TypedValue::String("banana".to_string());

        assert_eq!(a.lt(&b), Some(true));
        assert_eq!(b.gt(&a), Some(true));
    }

    #[test]
    fn test_decimal_partial_cmp() {
        use std::cmp::Ordering;
        let d = |unscaled, scale| TypedValue::Decimal {
            unscaled,
            precision: 18,
            scale,
        };
        // Same scale → compare unscaled directly.
        assert_eq!(d(12345, 2).partial_cmp(&d(999, 2)), Some(Ordering::Greater));
        assert_eq!(d(999, 2).partial_cmp(&d(12345, 2)), Some(Ordering::Less));
        assert_eq!(d(500, 2).partial_cmp(&d(500, 2)), Some(Ordering::Equal));
        // Negative values order correctly.
        assert_eq!(d(-5, 2).partial_cmp(&d(5, 2)), Some(Ordering::Less));
        // Differing scales normalize to the same real value: 1.0 (scale 1) ==
        // 1.00 (scale 2) → Equal; 1.5 > 1.00.
        assert_eq!(d(10, 1).partial_cmp(&d(100, 2)), Some(Ordering::Equal));
        assert_eq!(d(15, 1).partial_cmp(&d(100, 2)), Some(Ordering::Greater));
    }

    #[test]
    fn test_uuid_partial_cmp() {
        use std::cmp::Ordering;
        let mut lo = [0u8; 16];
        lo[15] = 1;
        let mut hi = [0u8; 16];
        hi[0] = 1; // most-significant byte set → larger
        assert_eq!(
            TypedValue::Uuid(lo).partial_cmp(&TypedValue::Uuid(hi)),
            Some(Ordering::Less)
        );
        assert_eq!(
            TypedValue::Uuid(hi).partial_cmp(&TypedValue::Uuid(lo)),
            Some(Ordering::Greater)
        );
        assert_eq!(
            TypedValue::Uuid(lo).partial_cmp(&TypedValue::Uuid(lo)),
            Some(Ordering::Equal)
        );
    }

    #[test]
    fn test_error_on_invalid_length() {
        // int requires exactly 4 bytes
        assert!(decode_by_type_string(&[1, 2], Some("int")).is_err());

        // long requires exactly 8 bytes
        assert!(decode_by_type_string(&[1, 2, 3, 4], Some("long")).is_err());
    }

    #[test]
    fn test_error_on_unsupported_type() {
        assert!(decode_by_type_string(&[1, 2, 3, 4], Some("unknown_type")).is_err());
    }

    #[test]
    fn nan_float_compare_is_incomparable() {
        // A NaN operand must yield None (incomparable) so pruning keeps the group,
        // never `Some(false)` (which would let `bounds_can_contain` over-prune a
        // row group holding NaN rows — the F15 hazard).
        let nan = TypedValue::Float64(f64::NAN);
        let five = TypedValue::Float64(5.0);
        assert_eq!(nan.lt(&five), None);
        assert_eq!(nan.le(&five), None);
        assert_eq!(nan.gt(&five), None);
        assert_eq!(nan.ge(&five), None);
        assert_eq!(five.lt(&nan), None);
        assert_eq!(five.le(&nan), None);
        // Finite floats still compare normally.
        assert_eq!(five.lt(&TypedValue::Float64(6.0)), Some(true));
        // Same for f32.
        let nan32 = TypedValue::Float32(f32::NAN);
        assert_eq!(nan32.lt(&TypedValue::Float32(1.0)), None);
        assert_eq!(TypedValue::Float32(1.0).le(&nan32), None);
    }

    #[test]
    fn decimal_lt_le_cross_scale() {
        let d = |unscaled, scale| TypedValue::Decimal {
            unscaled,
            precision: 38,
            scale,
        };
        // 9.99 (scale 2) vs 9.990 (scale 3) are the SAME value.
        assert_eq!(d(999, 2).lt(&d(9990, 3)), Some(false));
        assert_eq!(d(999, 2).le(&d(9990, 3)), Some(true));
        assert_eq!(d(9990, 3).le(&d(999, 2)), Some(true));
        // 9.99 < 20.000.
        assert_eq!(d(999, 2).lt(&d(20000, 3)), Some(true));
        assert_eq!(d(20000, 3).lt(&d(999, 2)), Some(false));
        // gt/ge delegate through le/lt.
        assert_eq!(d(20000, 3).gt(&d(999, 2)), Some(true));
        assert_eq!(d(999, 2).ge(&d(9990, 3)), Some(true));
    }
}
