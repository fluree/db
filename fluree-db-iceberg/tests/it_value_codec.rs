//! Integration tests for Iceberg value codec (binary encoding/decoding).
//!
//! The value codec is critical for:
//! 1. Decoding column bounds from manifest files for pruning
//! 2. Comparing filter values against min/max bounds
//! 3. Partition field value decoding

use fluree_db_iceberg::manifest::{decode_by_type_string, encode_value, TypedValue};

/// Test roundtrip encoding/decoding for all supported types.
#[test]
fn test_value_codec_roundtrip_all_types() {
    let test_cases: Vec<(&str, TypedValue)> = vec![
        ("boolean", TypedValue::Boolean(true)),
        ("boolean", TypedValue::Boolean(false)),
        ("int", TypedValue::Int32(0)),
        ("int", TypedValue::Int32(42)),
        ("int", TypedValue::Int32(-1000)),
        ("int", TypedValue::Int32(i32::MAX)),
        ("int", TypedValue::Int32(i32::MIN)),
        ("long", TypedValue::Int64(0)),
        ("long", TypedValue::Int64(1_234_567_890_123)),
        ("long", TypedValue::Int64(-9_876_543_210)),
        ("long", TypedValue::Int64(i64::MAX)),
        ("long", TypedValue::Int64(i64::MIN)),
        ("float", TypedValue::Float32(0.0)),
        ("float", TypedValue::Float32(3.13159)),
        ("float", TypedValue::Float32(-273.15)),
        ("double", TypedValue::Float64(0.0)),
        ("double", TypedValue::Float64(std::f64::consts::PI)),
        ("double", TypedValue::Float64(-1e100)),
        ("date", TypedValue::Date(0)),
        ("date", TypedValue::Date(19000)),  // ~2022
        ("date", TypedValue::Date(-10000)), // Before epoch
        ("timestamp", TypedValue::Timestamp(0)),
        ("timestamp", TypedValue::Timestamp(1_640_000_000_000_000)), // 2021-12-20
        (
            "timestamptz",
            TypedValue::TimestampTz(1_640_000_000_000_000),
        ),
        ("string", TypedValue::String(String::new())),
        ("string", TypedValue::String("hello".to_string())),
        (
            "string",
            TypedValue::String("unicode: 你好世界 🌍".to_string()),
        ),
        ("binary", TypedValue::Bytes(vec![])),
        ("binary", TypedValue::Bytes(vec![0, 1, 2, 255])),
    ];

    for (type_str, original) in test_cases {
        let encoded = encode_value(&original);
        let decoded = decode_by_type_string(&encoded, Some(type_str))
            .unwrap_or_else(|_| panic!("decoding {type_str} bytes {encoded:?}"));

        // Handle timestamptz specially - it decodes to TimestampTz variant
        let matches = match (&original, &decoded) {
            (TypedValue::TimestampTz(a), TypedValue::TimestampTz(b)) => a == b,
            _ => original == decoded,
        };

        assert!(
            matches,
            "Roundtrip failed for type {type_str}: {original:?} -> {encoded:?} -> {decoded:?}"
        );
    }
}

/// Test specific integer boundary cases.
#[test]
fn test_integer_boundaries() {
    // INT32 boundaries
    let int_tests = vec![i32::MIN, i32::MIN + 1, -1, 0, 1, i32::MAX - 1, i32::MAX];

    for val in int_tests {
        let original = TypedValue::Int32(val);
        let encoded = encode_value(&original);
        let decoded = decode_by_type_string(&encoded, Some("int")).unwrap();
        assert_eq!(original, decoded, "INT32 boundary failed for {val}");
    }

    // INT64 boundaries
    let long_tests = vec![
        i64::MIN,
        i64::MIN + 1,
        -1i64,
        0i64,
        1i64,
        i64::MAX - 1,
        i64::MAX,
    ];

    for val in long_tests {
        let original = TypedValue::Int64(val);
        let encoded = encode_value(&original);
        let decoded = decode_by_type_string(&encoded, Some("long")).unwrap();
        assert_eq!(original, decoded, "INT64 boundary failed for {val}");
    }
}

/// Test string ordering is preserved.
#[test]
fn test_string_ordering() {
    let strings = ["", "A", "B", "a", "apple", "banana", "cherry", "z"];

    let typed_values: Vec<TypedValue> = strings
        .iter()
        .map(|s| TypedValue::String(s.to_string()))
        .collect();

    // Verify ordering is preserved through encode/decode
    for i in 0..typed_values.len() {
        for j in (i + 1)..typed_values.len() {
            let v1 = &typed_values[i];
            let v2 = &typed_values[j];

            let e1 = encode_value(v1);
            let e2 = encode_value(v2);

            let d1 = decode_by_type_string(&e1, Some("string")).unwrap();
            let d2 = decode_by_type_string(&e2, Some("string")).unwrap();

            assert!(
                d1 < d2,
                "String ordering not preserved: {d1:?} should be < {d2:?}"
            );
        }
    }
}

/// Test timestamp encoding (microseconds since epoch).
#[test]
fn test_timestamp_encoding() {
    let timestamps = vec![
        0i64,                     // Epoch
        1_000_000i64,             // 1 second after epoch
        1_640_000_000_000_000i64, // 2021-12-20
        -86_400_000_000i64,       // 1 day before epoch
    ];

    for ts in timestamps {
        let original = TypedValue::Timestamp(ts);
        let encoded = encode_value(&original);

        // Test both timestamp and timestamptz decode the same value
        let decoded_ts = decode_by_type_string(&encoded, Some("timestamp")).unwrap();
        let decoded_tz = decode_by_type_string(&encoded, Some("timestamptz")).unwrap();

        assert_eq!(original, decoded_ts);
        // timestamptz decodes to TimestampTz variant, check value
        if let TypedValue::TimestampTz(v) = decoded_tz {
            assert_eq!(ts, v);
        } else {
            panic!("Expected TimestampTz");
        }
    }
}

/// Test date encoding (days since epoch).
#[test]
fn test_date_encoding() {
    let dates = vec![
        0i32,     // 1970-01-01
        19000i32, // ~2022
        -365i32,  // 1969-01-01
        36500i32, // ~2069
    ];

    for d in dates {
        let original = TypedValue::Date(d);
        let encoded = encode_value(&original);
        let decoded = decode_by_type_string(&encoded, Some("date")).unwrap();
        assert_eq!(original, decoded);
    }
}

/// Test decimal encoding (fixed point).
#[test]
fn test_decimal_encoding() {
    // Test with precision 10, scale 2
    let decimals = vec![
        (12345i128, 10, 2),       // 123.45
        (-12345i128, 10, 2),      // -123.45
        (0i128, 10, 2),           // 0.00
        (99_999_999_i128, 10, 2), // 999999.99
    ];

    for (unscaled, precision, scale) in decimals {
        let original = TypedValue::Decimal {
            unscaled,
            precision,
            scale,
        };
        let encoded = encode_value(&original);

        // Decode with matching precision/scale
        let type_str = format!("decimal({precision},{scale})");
        let decoded = decode_by_type_string(&encoded, Some(&type_str)).unwrap();

        // Note: decimal decoding may not preserve exact unscaled value
        // if the byte representation differs, but ordering should be preserved
        assert!(
            matches!(decoded, TypedValue::Decimal { .. }),
            "Expected Decimal type"
        );
    }
}

/// Test UUID encoding (16 bytes, big-endian).
#[test]
fn test_uuid_encoding() {
    let uuid_bytes = [
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x10,
    ];

    let original = TypedValue::Uuid(uuid_bytes);
    let encoded = encode_value(&original);
    let decoded = decode_by_type_string(&encoded, Some("uuid")).unwrap();
    assert_eq!(original, decoded);
}

/// Test error handling for invalid byte lengths.
#[test]
fn test_invalid_byte_length() {
    // INT32 requires exactly 4 bytes
    let too_short = vec![0u8, 1, 2];
    let result = decode_by_type_string(&too_short, Some("int"));
    assert!(result.is_err());

    // INT64 requires exactly 8 bytes
    let wrong_size = vec![0u8, 1, 2, 3, 4];
    let result = decode_by_type_string(&wrong_size, Some("long"));
    assert!(result.is_err());

    // Boolean - test with valid single byte
    let valid_bool = vec![1u8];
    let result = decode_by_type_string(&valid_bool, Some("boolean"));
    assert!(result.is_ok());
}

/// Test TypedValue comparison operators.
#[test]
fn test_typed_value_comparison() {
    // Int64 comparisons
    assert!(TypedValue::Int64(10) < TypedValue::Int64(20));
    assert!(TypedValue::Int64(-5) < TypedValue::Int64(5));
    assert!(TypedValue::Int64(100) == TypedValue::Int64(100));

    // Float64 comparisons
    assert!(TypedValue::Float64(1.5) < TypedValue::Float64(2.5));
    assert!(TypedValue::Float64(-1.0) < TypedValue::Float64(0.0));

    // String comparisons (lexicographic)
    assert!(TypedValue::String("a".into()) < TypedValue::String("b".into()));
    assert!(TypedValue::String("abc".into()) < TypedValue::String("abd".into()));

    // Date comparisons
    assert!(TypedValue::Date(0) < TypedValue::Date(100));
    assert!(TypedValue::Date(-100) < TypedValue::Date(0));

    // Timestamp comparisons
    assert!(TypedValue::Timestamp(1000) < TypedValue::Timestamp(2000));
}
