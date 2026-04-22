//! Datatype coercion tests
//!
//! Coverage for numeric/type coercion behavior.
//! Tests the `coerce_value_by_datatype` function that converts values to specific XSD datatypes.
//!
//! Note: This tests low-level datatype coercion functionality.
//! All datatype IRIs must be fully expanded (not prefixed).

use fluree_db_core::value::FlakeValue;
use fluree_db_query::parse::lower::coerce_value_by_datatype;
use fluree_vocab::{rdf, xsd};

// Helper function to convert serde_json::Value to FlakeValue for testing
fn json_to_flake_value(json: &serde_json::Value) -> Option<FlakeValue> {
    match json {
        serde_json::Value::String(s) => Some(FlakeValue::String(s.clone())),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(FlakeValue::Long(i))
            } else {
                n.as_f64().map(FlakeValue::Double)
            }
        }
        serde_json::Value::Bool(b) => Some(FlakeValue::Boolean(*b)),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            serde_json::to_string(json).ok().map(FlakeValue::Json)
        }
        _ => None,
    }
}

// Helper function to convert FlakeValue back to serde_json::Value for assertions
fn flake_to_json(fv: &FlakeValue) -> serde_json::Value {
    match fv {
        FlakeValue::String(s) => serde_json::json!(s),
        FlakeValue::Long(i) => serde_json::json!(i),
        FlakeValue::Double(f) => serde_json::json!(f),
        FlakeValue::Boolean(b) => serde_json::json!(b),
        FlakeValue::BigInt(n) => serde_json::json!(n.to_string()),
        FlakeValue::Decimal(d) => serde_json::json!(d.to_string()),
        FlakeValue::DateTime(dt) => serde_json::json!(dt.to_string()),
        FlakeValue::Date(d) => serde_json::json!(d.to_string()),
        FlakeValue::Time(t) => serde_json::json!(t.to_string()),
        FlakeValue::Json(j) => serde_json::json!(j),
        FlakeValue::GYear(v) => serde_json::json!(v.to_string()),
        FlakeValue::GYearMonth(v) => serde_json::json!(v.to_string()),
        FlakeValue::GMonth(v) => serde_json::json!(v.to_string()),
        FlakeValue::GDay(v) => serde_json::json!(v.to_string()),
        FlakeValue::GMonthDay(v) => serde_json::json!(v.to_string()),
        FlakeValue::YearMonthDuration(v) => serde_json::json!(v.to_string()),
        FlakeValue::DayTimeDuration(v) => serde_json::json!(v.to_string()),
        FlakeValue::Duration(v) => serde_json::json!(v.to_string()),
        _ => serde_json::json!(null),
    }
}

// Test wrapper for coerce function
fn coerce(value: serde_json::Value, datatype: &str) -> Option<serde_json::Value> {
    let flake_val = json_to_flake_value(&value)?;
    match coerce_value_by_datatype(flake_val, datatype) {
        Ok(coerced) => Some(flake_to_json(&coerced)),
        Err(_) => None,
    }
}

#[test]
fn coerce_strings() {
    assert_eq!(
        coerce(serde_json::json!("foo"), xsd::STRING),
        Some(serde_json::json!("foo"))
    );
    assert_eq!(coerce(serde_json::json!(42), xsd::STRING), None);
}

#[test]
fn coerce_id() {
    assert_eq!(
        coerce(serde_json::json!("foo"), "@id"),
        Some(serde_json::json!("foo"))
    );
    assert_eq!(
        coerce(serde_json::json!(42), "@id"),
        Some(serde_json::json!(42))
    );
}

#[test]
fn coerce_boolean() {
    assert_eq!(
        coerce(serde_json::json!("true"), xsd::BOOLEAN),
        Some(serde_json::json!(true))
    );
    assert_eq!(
        coerce(serde_json::json!("false"), xsd::BOOLEAN),
        Some(serde_json::json!(false))
    );
    assert_eq!(
        coerce(serde_json::json!(true), xsd::BOOLEAN),
        Some(serde_json::json!(true))
    );
    assert_eq!(
        coerce(serde_json::json!(false), xsd::BOOLEAN),
        Some(serde_json::json!(false))
    );
    assert_eq!(coerce(serde_json::json!("foo"), xsd::BOOLEAN), None);
}

#[test]
fn coerce_date() {
    assert_eq!(coerce(serde_json::json!("1980-10-5Z"), xsd::DATE), None);
    assert_eq!(coerce(serde_json::json!("1980-10-5"), xsd::DATE), None);
    assert_eq!(
        coerce(serde_json::json!("2022-01-05Z"), xsd::DATE),
        Some(serde_json::json!("2022-01-05Z"))
    );
    assert_eq!(coerce(serde_json::json!("foo"), xsd::DATE), None);
}

#[test]
fn coerce_time() {
    assert_eq!(
        coerce(serde_json::json!("12:42:00"), xsd::TIME),
        Some(serde_json::json!("12:42:00"))
    );
    assert_eq!(coerce(serde_json::json!("12:42:5"), xsd::TIME), None);
    assert_eq!(coerce(serde_json::json!("foo"), xsd::TIME), None);
}

#[test]
fn coerce_datetime() {
    assert_eq!(
        coerce(serde_json::json!("1980-10-5T11:23:00Z"), xsd::DATE_TIME),
        None
    );
    assert_eq!(
        coerce(
            serde_json::json!("1980-10-05T11:23:00-06:00"),
            xsd::DATE_TIME
        ),
        Some(serde_json::json!("1980-10-05T11:23:00-06:00"))
    );
    assert_eq!(coerce(serde_json::json!("foo"), xsd::DATE_TIME), None);
}

#[test]
fn coerce_decimal() {
    assert_eq!(
        coerce(serde_json::json!(3.13), xsd::DECIMAL),
        Some(serde_json::json!(3.13))
    );
    assert_eq!(
        coerce(serde_json::json!("3.14"), xsd::DECIMAL),
        Some(serde_json::json!("3.14"))
    );
    assert_eq!(coerce(serde_json::json!("foo"), xsd::DECIMAL), None);
}

#[test]
fn coerce_double() {
    assert_eq!(
        coerce(serde_json::json!("INF"), xsd::DOUBLE),
        Some(serde_json::json!(f64::INFINITY))
    );
    assert_eq!(
        coerce(serde_json::json!("-INF"), xsd::DOUBLE),
        Some(serde_json::json!(f64::NEG_INFINITY))
    );
    assert_eq!(coerce(serde_json::json!("foo"), xsd::DOUBLE), None);
}

#[test]
fn coerce_float() {
    assert_eq!(
        coerce(serde_json::json!("INF"), xsd::FLOAT),
        Some(serde_json::json!(f64::INFINITY))
    );
    assert_eq!(
        coerce(serde_json::json!("-INF"), xsd::FLOAT),
        Some(serde_json::json!(f64::NEG_INFINITY))
    );
    assert_eq!(coerce(serde_json::json!("foo"), xsd::FLOAT), None);
}

#[test]
fn coerce_integer() {
    assert_eq!(
        coerce(serde_json::json!(42), xsd::INTEGER),
        Some(serde_json::json!(42))
    );
    assert_eq!(
        coerce(serde_json::json!("42"), xsd::INTEGER),
        Some(serde_json::json!(42))
    );
    assert_eq!(coerce(serde_json::json!(3.13), xsd::INTEGER), None);
}

#[test]
fn coerce_int() {
    assert_eq!(
        coerce(serde_json::json!(42), xsd::INT),
        Some(serde_json::json!(42))
    );
    assert_eq!(coerce(serde_json::json!(3.13), xsd::INT), None);
    assert_eq!(coerce(serde_json::json!("3.14"), xsd::INT), None);
}

#[test]
fn coerce_unsigned_int() {
    assert_eq!(
        coerce(serde_json::json!(42), xsd::UNSIGNED_INT),
        Some(serde_json::json!(42))
    );
    assert_eq!(coerce(serde_json::json!(-42), xsd::UNSIGNED_INT), None);
}

#[test]
fn coerce_other_integer_types() {
    assert_eq!(
        coerce(serde_json::json!(42), xsd::LONG),
        Some(serde_json::json!(42))
    );
    assert_eq!(
        coerce(serde_json::json!(42), xsd::SHORT),
        Some(serde_json::json!(42))
    );
    assert_eq!(
        coerce(serde_json::json!(42), xsd::BYTE),
        Some(serde_json::json!(42))
    );
    assert_eq!(
        coerce(serde_json::json!(42), xsd::UNSIGNED_LONG),
        Some(serde_json::json!(42))
    );
    assert_eq!(
        coerce(serde_json::json!(42), xsd::UNSIGNED_SHORT),
        Some(serde_json::json!(42))
    );
    assert_eq!(
        coerce(serde_json::json!(42), xsd::UNSIGNED_BYTE),
        Some(serde_json::json!(42))
    );
    assert_eq!(
        coerce(serde_json::json!(42), xsd::POSITIVE_INTEGER),
        Some(serde_json::json!(42))
    );
    assert_eq!(
        coerce(serde_json::json!(-1), xsd::NEGATIVE_INTEGER),
        Some(serde_json::json!(-1))
    );
    assert_eq!(
        coerce(serde_json::json!(0), xsd::NON_POSITIVE_INTEGER),
        Some(serde_json::json!(0))
    );
    assert_eq!(
        coerce(serde_json::json!(0), xsd::NON_NEGATIVE_INTEGER),
        Some(serde_json::json!(0))
    );
    assert_eq!(coerce(serde_json::json!(-1), xsd::POSITIVE_INTEGER), None);
    assert_eq!(
        coerce(serde_json::json!(1), xsd::NON_POSITIVE_INTEGER),
        None
    );
}

#[test]
fn coerce_string_types() {
    assert_eq!(
        coerce(serde_json::json!("foo"), xsd::NORMALIZED_STRING),
        Some(serde_json::json!("foo"))
    );
    assert_eq!(
        coerce(serde_json::json!("foo"), xsd::TOKEN),
        Some(serde_json::json!("foo"))
    );
    assert_eq!(
        coerce(serde_json::json!("en"), xsd::LANGUAGE),
        Some(serde_json::json!("en"))
    );
    assert_eq!(coerce(serde_json::json!(42), xsd::TOKEN), None);
}

#[test]
fn coerce_json() {
    let value = serde_json::json!({"json":"data","is":["cool","right?",1,false,1.0]});
    let coerced = coerce(value, rdf::JSON).expect("json should coerce");
    let parsed: serde_json::Value = serde_json::from_str(coerced.as_str().unwrap()).unwrap();
    assert_eq!(
        parsed,
        serde_json::json!({"json":"data","is":["cool","right?",1,false,1.0]})
    );
}

#[test]
fn coerce_non_coerced_datatypes() {
    assert_eq!(
        coerce(serde_json::json!("0F0A"), xsd::HEX_BINARY),
        Some(serde_json::json!("0F0A"))
    );
    assert_eq!(
        coerce(serde_json::json!("P1DT2H"), xsd::DURATION),
        Some(serde_json::json!("P1DT2H"))
    );
}
