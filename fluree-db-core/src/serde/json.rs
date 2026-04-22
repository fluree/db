//! JSON serialization and deserialization for Fluree index data

use crate::error::{Error, Result};
use crate::flake::{Flake, FlakeMeta};
use crate::sid::Sid;
use crate::temporal::{Date, DateTime, Time};
use crate::value::FlakeValue;
use bigdecimal::BigDecimal;
use fluree_vocab::namespaces::{JSON_LD, RDF, XSD};
use fluree_vocab::xsd_names;
use num_bigint::BigInt;
use serde::Deserialize;
use std::str::FromStr;

fn is_id_dt(dt: &Sid) -> bool {
    dt.namespace_code == JSON_LD && dt.name.as_ref() == "id"
}

fn is_datetime_dt(dt: &Sid) -> bool {
    dt.namespace_code == XSD && dt.name.as_ref() == xsd_names::DATE_TIME
}

fn is_date_dt(dt: &Sid) -> bool {
    dt.namespace_code == XSD && dt.name.as_ref() == xsd_names::DATE
}

fn is_time_dt(dt: &Sid) -> bool {
    dt.namespace_code == XSD && dt.name.as_ref() == xsd_names::TIME
}

fn is_integer_family_dt(dt: &Sid) -> bool {
    dt.namespace_code == XSD && xsd_names::is_integer_family_name(dt.name.as_ref())
}

fn is_decimal_dt(dt: &Sid) -> bool {
    dt.namespace_code == XSD && dt.name.as_ref() == xsd_names::DECIMAL
}

/// Check if datatype is rdf:JSON
fn is_json_dt(dt: &Sid) -> bool {
    dt.namespace_code == RDF && dt.name.as_ref() == "JSON"
}

// === Raw JSON structures for deserialization ===

/// Raw flake as it appears in JSON (7-element array)
#[derive(Debug, Deserialize)]
#[serde(transparent)]
pub struct RawFlake(Vec<serde_json::Value>);

impl RawFlake {
    /// Convert to Flake
    pub fn to_flake(&self) -> Result<Flake> {
        if self.0.len() != 7 {
            return Err(Error::other(format!(
                "Flake array must have 7 elements, got {}",
                self.0.len()
            )));
        }

        let s = deserialize_sid(&self.0[0])?;
        let p = deserialize_sid(&self.0[1])?;
        let dt = deserialize_sid(&self.0[3])?;
        let o = deserialize_object(&self.0[2], &dt)?;
        let t = self.0[4]
            .as_i64()
            .ok_or_else(|| Error::other("t must be integer"))?;
        let op = self.0[5]
            .as_bool()
            .ok_or_else(|| Error::other("op must be boolean"))?;
        let m = deserialize_meta(&self.0[6])?;

        Ok(Flake::new(s, p, o, dt, t, op, m))
    }
}

/// Deserialize a SID from JSON
///
/// SIDs are serialized as `[namespace_code, name]` tuples.
pub fn deserialize_sid(value: &serde_json::Value) -> Result<Sid> {
    match value {
        serde_json::Value::Array(arr) if arr.len() == 2 => {
            let raw_code = arr[0]
                .as_u64()
                .ok_or_else(|| Error::other("SID namespace_code must be integer"))?;
            let ns_code = u16::try_from(raw_code).map_err(|_| {
                Error::other(format!("SID namespace_code {raw_code} exceeds u16::MAX"))
            })?;
            let name = arr[1]
                .as_str()
                .ok_or_else(|| Error::other("SID name must be string"))?
                .to_string();
            Ok(Sid::new(ns_code, name))
        }
        _ => Err(Error::other(format!(
            "SID must be [namespace_code, name] array, got {value:?}"
        ))),
    }
}

/// Deserialize an object value from JSON
///
/// The object type depends on the datatype:
/// - If `dt` is $id (namespace_code=1, name="id"), object is a SID (reference)
/// - If `dt` is xsd:dateTime/date/time, parse string as temporal type
/// - If `dt` is xsd:integer, use BigInt for arbitrary precision
/// - If `dt` is xsd:decimal, use BigDecimal for arbitrary precision
/// - Otherwise, object is a literal value based on JSON type
pub fn deserialize_object(value: &serde_json::Value, dt: &Sid) -> Result<FlakeValue> {
    // Check if this is a reference
    if is_id_dt(dt) {
        let sid = deserialize_sid(value)?;
        return Ok(FlakeValue::Ref(sid));
    }

    // Handle temporal types - parse string values
    if is_datetime_dt(dt) {
        if let serde_json::Value::String(s) = value {
            return DateTime::parse(s)
                .map(|dt| FlakeValue::DateTime(Box::new(dt)))
                .map_err(Error::other);
        }
    }
    if is_date_dt(dt) {
        if let serde_json::Value::String(s) = value {
            return Date::parse(s)
                .map(|d| FlakeValue::Date(Box::new(d)))
                .map_err(Error::other);
        }
    }
    if is_time_dt(dt) {
        if let serde_json::Value::String(s) = value {
            return Time::parse(s)
                .map(|t| FlakeValue::Time(Box::new(t)))
                .map_err(Error::other);
        }
    }

    // Handle arbitrary precision numeric types
    if is_integer_family_dt(dt) {
        match value {
            serde_json::Value::Number(n) => {
                // Try i64 first, fall back to BigInt
                if let Some(i) = n.as_i64() {
                    return Ok(FlakeValue::Long(i));
                }
                // Parse as BigInt from string representation
                let s = n.to_string();
                return BigInt::from_str(&s)
                    .map(|bi| FlakeValue::BigInt(Box::new(bi)))
                    .map_err(|e| Error::other(format!("Invalid integer: {e}")));
            }
            serde_json::Value::String(s) => {
                // Try i64 first, fall back to BigInt
                if let Ok(i) = s.parse::<i64>() {
                    return Ok(FlakeValue::Long(i));
                }
                return BigInt::from_str(s)
                    .map(|bi| FlakeValue::BigInt(Box::new(bi)))
                    .map_err(|e| Error::other(format!("Invalid integer: {e}")));
            }
            _ => {}
        }
    }
    if is_decimal_dt(dt) {
        match value {
            // JSON numbers → Double (policy: JSON already lost precision, use Double)
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    return Ok(FlakeValue::Long(i));
                } else if let Some(f) = n.as_f64() {
                    return Ok(FlakeValue::Double(f));
                }
                return Err(Error::other("Invalid decimal number"));
            }
            // String literals → BigDecimal (preserves precision from source)
            serde_json::Value::String(s) => {
                return BigDecimal::from_str(s)
                    .map(|bd| FlakeValue::Decimal(Box::new(bd)))
                    .map_err(|e| Error::other(format!("Invalid decimal: {e}")));
            }
            _ => {}
        }
    }

    // rdf:JSON: the serialized JSON string must be restored as FlakeValue::Json
    if is_json_dt(dt) {
        if let serde_json::Value::String(s) = value {
            return Ok(FlakeValue::Json(s.clone()));
        }
    }

    // Default deserialization based on JSON type
    match value {
        serde_json::Value::Null => Ok(FlakeValue::Null),
        serde_json::Value::Bool(b) => Ok(FlakeValue::Boolean(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(FlakeValue::Long(i))
            } else if let Some(f) = n.as_f64() {
                Ok(FlakeValue::Double(f))
            } else {
                Err(Error::other("Invalid number"))
            }
        }
        serde_json::Value::String(s) => Ok(FlakeValue::String(s.clone())),
        // Arrays could be SIDs (references) or other complex types
        serde_json::Value::Array(arr) if arr.len() == 2 => {
            // Could be a SID even if dt isn't $id (for backwards compatibility)
            if arr[0].is_i64() && arr[1].is_string() {
                let sid = deserialize_sid(value)?;
                Ok(FlakeValue::Ref(sid))
            } else {
                Ok(FlakeValue::String(value.to_string()))
            }
        }
        _ => Ok(FlakeValue::String(value.to_string())),
    }
}

/// Deserialize flake metadata from JSON
pub fn deserialize_meta(value: &serde_json::Value) -> Result<Option<FlakeMeta>> {
    match value {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::Object(map) => {
            let lang = map.get("lang").and_then(|v| v.as_str()).map(String::from);
            let i = map
                .get("i")
                .and_then(serde_json::Value::as_i64)
                .map(|v| v as i32);
            if lang.is_none() && i.is_none() {
                Ok(None)
            } else {
                Ok(Some(FlakeMeta { lang, i }))
            }
        }
        // Integer metadata (hash for comparison)
        serde_json::Value::Number(n) => {
            let i = n.as_i64().map(|v| v as i32);
            Ok(Some(FlakeMeta { lang: None, i }))
        }
        _ => Ok(None),
    }
}

// NOTE: Legacy JSON leaf-node parsing/serialization was removed.
//
// The current index format stores leaf files as binary `FLI3` (leaflets inside),
// and proxy transport uses `fluree-db-core::serde::flakes_transport` (FLKB).

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_sid() {
        let json = serde_json::json!([42, "example"]);
        let sid = deserialize_sid(&json).unwrap();
        assert_eq!(sid.namespace_code, 42);
        assert_eq!(sid.name.as_ref(), "example");
    }

    #[test]
    fn test_deserialize_flake() {
        let json = serde_json::json!([
            [1, "alice"],  // s
            [2, "name"],   // p
            "Alice",       // o
            [3, "string"], // dt
            100,           // t
            true,          // op
            null           // m
        ]);

        let raw: RawFlake = serde_json::from_value(json).unwrap();
        let flake = raw.to_flake().unwrap();

        assert_eq!(flake.s.namespace_code, 1);
        assert_eq!(flake.s.name.as_ref(), "alice");
        assert_eq!(flake.p.name.as_ref(), "name");
        assert!(matches!(flake.o, FlakeValue::String(ref s) if s == "Alice"));
        assert_eq!(flake.t, 100);
        assert!(flake.op);
        assert!(flake.m.is_none());
    }

    #[test]
    fn test_deserialize_json_datatype_preserves_json_variant() {
        // When a flake has rdf:JSON datatype, deserialize_object should return
        // FlakeValue::Json, not FlakeValue::String. This matters because commit
        // serialization stores @json values as plain JSON strings, and without
        // the datatype check, serde's default path produces FlakeValue::String.
        let rdf_json_dt = Sid::new(RDF, "JSON");
        let json_str = r#"[{"name":"Alice"}]"#;
        let value = serde_json::json!(json_str);

        let result = deserialize_object(&value, &rdf_json_dt).unwrap();
        assert!(
            matches!(&result, FlakeValue::Json(s) if s == json_str),
            "rdf:JSON datatype should produce FlakeValue::Json, got: {result:?}"
        );
    }

    #[test]
    fn test_deserialize_json_datatype_full_flake_roundtrip() {
        // Full flake round-trip: a @json flake serialized as commit JSON should
        // deserialize back with FlakeValue::Json (not FlakeValue::String).
        let json_str = r#"{"name":"John","age":30}"#;
        let flake_json = serde_json::json!([
            [1, "doc1"], // s
            [2, "data"], // p
            json_str,    // o — serialized JSON string
            [3, "JSON"], // dt = rdf:JSON (namespace 3)
            100,         // t
            true,        // op
            null         // m
        ]);

        let raw: RawFlake = serde_json::from_value(flake_json).unwrap();
        let flake = raw.to_flake().unwrap();

        assert!(
            matches!(&flake.o, FlakeValue::Json(s) if s == json_str),
            "Flake with rdf:JSON dt should have FlakeValue::Json, got: {:?}",
            flake.o
        );
    }

    #[test]
    fn test_deserialize_ref_flake() {
        let json = serde_json::json!([
            [1, "alice"], // s
            [2, "knows"], // p
            [1, "bob"],   // o (reference)
            [1, "id"],    // dt = $id
            100,          // t
            true,         // op
            null          // m
        ]);

        let raw: RawFlake = serde_json::from_value(json).unwrap();
        let flake = raw.to_flake().unwrap();

        assert!(flake.is_ref());
        match &flake.o {
            FlakeValue::Ref(sid) => {
                assert_eq!(sid.namespace_code, 1);
                assert_eq!(sid.name.as_ref(), "bob");
            }
            _ => panic!("Expected Ref"),
        }
    }
}
