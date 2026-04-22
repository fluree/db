//! Internal transport format for policy-filtered flakes
//!
//! This format is for tx↔peer internal transport only.
//! Not a public API - may change between versions.
//!
//! # Binary Format
//!
//! ```text
//! +------+------+----------+------------------+
//! | FLKB | ver  | reserved | CBOR payload     |
//! | 4B   | 1B   | 3B       | variable         |
//! +------+------+----------+------------------+
//! ```
//!
//! - Magic bytes: "FLKB" (FLaKes Binary)
//! - Version: 1 byte (currently 1)
//! - Reserved: 3 bytes for alignment
//! - CBOR payload: Array of TransportFlake

use crate::flake::{Flake, FlakeMeta};
use crate::sid::{Sid, SidInterner};
use crate::temporal::{Date, DateTime, Time};
use crate::value::FlakeValue;
use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use serde::{Deserialize, Serialize};

/// Magic bytes: "FLKB" (FLaKes Binary)
pub const MAGIC: &[u8; 4] = b"FLKB";
/// Format version
pub const VERSION: u8 = 1;
/// Header size: magic (4) + version (1) + reserved (3)
pub const HEADER_SIZE: usize = 8;

/// Serializable flake for transport (avoids Sid interning issues)
///
/// The peer will need to re-intern these when reconstructing Flakes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TransportFlake {
    /// Subject as [namespace_code, name] tuple
    pub s: TransportSid,
    /// Predicate as [namespace_code, name] tuple
    pub p: TransportSid,
    /// Object value (canonically encoded)
    pub o: TransportValue,
    /// Datatype as [namespace_code, name] tuple
    pub dt: TransportSid,
    /// Transaction time
    pub t: i64,
    /// Operation: true=assert, false=retract
    pub op: bool,
    /// Language tag (from FlakeMeta, if present)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lang: Option<String>,
    /// List index (from FlakeMeta, if present)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub i: Option<i32>,
}

/// Serializable SID for transport
///
/// Stored as [namespace_code, name] tuple for reconstruction.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TransportSid {
    pub ns: u16,
    pub name: String,
}

impl From<&Sid> for TransportSid {
    fn from(sid: &Sid) -> Self {
        TransportSid {
            ns: sid.namespace_code,
            name: sid.name.to_string(),
        }
    }
}

impl TransportSid {
    /// Reconstruct a Sid using the given interner
    pub fn to_sid(&self, interner: &SidInterner) -> Sid {
        interner.intern(self.ns, &self.name)
    }
}

/// Canonical encoding of FlakeValue for transport
///
/// Each variant maps to a tagged representation for deterministic decoding.
/// The tagged format ensures the decoder knows the exact type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", content = "value")]
pub enum TransportValue {
    /// String value
    #[serde(rename = "string")]
    String(String),
    /// 64-bit signed integer
    #[serde(rename = "long")]
    Long(i64),
    /// 64-bit float
    #[serde(rename = "double")]
    Double(f64),
    /// Boolean
    #[serde(rename = "bool")]
    Bool(bool),
    /// Reference to another subject (SID tuple)
    #[serde(rename = "ref")]
    Ref(TransportSid),
    /// BigInteger (string representation)
    #[serde(rename = "bigint")]
    BigInt(String),
    /// BigDecimal (string representation)
    #[serde(rename = "bigdec")]
    BigDec(String),
    /// DateTime as original string
    #[serde(rename = "datetime")]
    DateTime(String),
    /// Date as original string
    #[serde(rename = "date")]
    Date(String),
    /// Time as original string
    #[serde(rename = "time")]
    Time(String),
    /// Dense vector (embedding)
    #[serde(rename = "vector")]
    Vector(Vec<f64>),
    /// JSON value as string
    #[serde(rename = "json")]
    Json(String),
    /// Null value
    #[serde(rename = "null")]
    Null,
}

impl From<&FlakeValue> for TransportValue {
    fn from(v: &FlakeValue) -> Self {
        match v {
            FlakeValue::String(s) => TransportValue::String(s.clone()),
            FlakeValue::Long(n) => TransportValue::Long(*n),
            FlakeValue::Double(d) => TransportValue::Double(*d),
            FlakeValue::Boolean(b) => TransportValue::Bool(*b),
            FlakeValue::Ref(sid) => TransportValue::Ref(TransportSid::from(sid)),
            FlakeValue::BigInt(bi) => TransportValue::BigInt(bi.to_string()),
            FlakeValue::Decimal(bd) => TransportValue::BigDec(bd.to_string()),
            FlakeValue::DateTime(dt) => TransportValue::DateTime(dt.original().to_string()),
            FlakeValue::Date(d) => TransportValue::Date(d.original().to_string()),
            FlakeValue::Time(t) => TransportValue::Time(t.original().to_string()),
            FlakeValue::GYear(v) => TransportValue::String(v.to_string()),
            FlakeValue::GYearMonth(v) => TransportValue::String(v.to_string()),
            FlakeValue::GMonth(v) => TransportValue::String(v.to_string()),
            FlakeValue::GDay(v) => TransportValue::String(v.to_string()),
            FlakeValue::GMonthDay(v) => TransportValue::String(v.to_string()),
            FlakeValue::YearMonthDuration(v) => TransportValue::String(v.to_string()),
            FlakeValue::DayTimeDuration(v) => TransportValue::String(v.to_string()),
            FlakeValue::Duration(v) => TransportValue::String(v.to_string()),
            FlakeValue::Vector(v) => TransportValue::Vector(v.clone()),
            FlakeValue::Json(s) => TransportValue::Json(s.clone()),
            FlakeValue::GeoPoint(bits) => TransportValue::String(bits.to_string()),
            FlakeValue::Null => TransportValue::Null,
        }
    }
}

impl TransportValue {
    /// Reconstruct a FlakeValue, re-interning any Ref SIDs
    ///
    /// Returns an error if BigInt, BigDecimal, or temporal parsing fails.
    pub fn to_flake_value(
        &self,
        interner: &SidInterner,
    ) -> Result<FlakeValue, FlakesTransportError> {
        match self {
            TransportValue::String(s) => Ok(FlakeValue::String(s.clone())),
            TransportValue::Long(n) => Ok(FlakeValue::Long(*n)),
            TransportValue::Double(d) => Ok(FlakeValue::Double(*d)),
            TransportValue::Bool(b) => Ok(FlakeValue::Boolean(*b)),
            TransportValue::Ref(sid) => Ok(FlakeValue::Ref(sid.to_sid(interner))),
            TransportValue::BigInt(s) => {
                let bi: BigInt = s
                    .parse()
                    .map_err(|e| FlakesTransportError::ParseError(format!("BigInt: {e}")))?;
                Ok(FlakeValue::BigInt(Box::new(bi)))
            }
            TransportValue::BigDec(s) => {
                let bd: BigDecimal = s
                    .parse()
                    .map_err(|e| FlakesTransportError::ParseError(format!("BigDecimal: {e}")))?;
                Ok(FlakeValue::Decimal(Box::new(bd)))
            }
            TransportValue::DateTime(s) => {
                let dt = DateTime::parse(s)
                    .map_err(|e| FlakesTransportError::ParseError(format!("DateTime: {e}")))?;
                Ok(FlakeValue::DateTime(Box::new(dt)))
            }
            TransportValue::Date(s) => {
                let d = Date::parse(s)
                    .map_err(|e| FlakesTransportError::ParseError(format!("Date: {e}")))?;
                Ok(FlakeValue::Date(Box::new(d)))
            }
            TransportValue::Time(s) => {
                let t = Time::parse(s)
                    .map_err(|e| FlakesTransportError::ParseError(format!("Time: {e}")))?;
                Ok(FlakeValue::Time(Box::new(t)))
            }
            TransportValue::Vector(v) => Ok(FlakeValue::Vector(v.clone())),
            TransportValue::Json(s) => Ok(FlakeValue::Json(s.clone())),
            TransportValue::Null => Ok(FlakeValue::Null),
        }
    }
}

impl From<&Flake> for TransportFlake {
    fn from(f: &Flake) -> Self {
        TransportFlake {
            s: TransportSid::from(&f.s),
            p: TransportSid::from(&f.p),
            o: TransportValue::from(&f.o),
            dt: TransportSid::from(&f.dt),
            t: f.t,
            op: f.op,
            lang: f.m.as_ref().and_then(|m| m.lang.clone()),
            i: f.m.as_ref().and_then(|m| m.i),
        }
    }
}

impl TransportFlake {
    /// Reconstruct a Flake using the given interner
    ///
    /// Returns an error if value reconstruction fails.
    pub fn to_flake(&self, interner: &SidInterner) -> Result<Flake, FlakesTransportError> {
        let m = match (&self.lang, &self.i) {
            (Some(lang), i) => Some(FlakeMeta {
                lang: Some(lang.clone()),
                i: *i,
            }),
            (None, Some(i)) => Some(FlakeMeta {
                lang: None,
                i: Some(*i),
            }),
            (None, None) => None,
        };

        Ok(Flake {
            g: None, // TODO: transport format doesn't support graph yet
            s: self.s.to_sid(interner),
            p: self.p.to_sid(interner),
            o: self.o.to_flake_value(interner)?,
            dt: self.dt.to_sid(interner),
            t: self.t,
            op: self.op,
            m,
        })
    }
}

/// Encode flakes for transport
///
/// Returns a binary payload with magic header + CBOR-encoded flakes array.
pub fn encode_flakes(flakes: &[Flake]) -> Result<Vec<u8>, FlakesTransportError> {
    let transport: Vec<TransportFlake> = flakes.iter().map(TransportFlake::from).collect();

    let mut bytes = Vec::with_capacity(HEADER_SIZE + flakes.len() * 100); // rough estimate
    bytes.extend_from_slice(MAGIC);
    bytes.push(VERSION);
    bytes.extend_from_slice(&[0, 0, 0]); // reserved/padding for alignment

    // CBOR encode the flakes array
    ciborium::into_writer(&transport, &mut bytes).map_err(FlakesTransportError::CborEncode)?;

    Ok(bytes)
}

/// Decode flakes from transport format
///
/// Returns TransportFlakes which the caller must convert back to Flakes
/// (requires an interner for SID reconstruction).
pub fn decode_flakes(bytes: &[u8]) -> Result<Vec<TransportFlake>, FlakesTransportError> {
    if bytes.len() < HEADER_SIZE {
        return Err(FlakesTransportError::TooShort);
    }

    if &bytes[0..4] != MAGIC {
        return Err(FlakesTransportError::InvalidMagic);
    }

    let version = bytes[4];
    if version != VERSION {
        return Err(FlakesTransportError::UnsupportedVersion(version));
    }

    // Skip header (8 bytes) and decode CBOR
    let cbor_bytes = &bytes[HEADER_SIZE..];
    ciborium::from_reader(cbor_bytes).map_err(FlakesTransportError::CborDecode)
}

/// Decode transport flakes and reconstruct as core Flakes
///
/// This is a convenience function that combines decoding and reconstruction.
pub fn decode_flakes_interned(
    bytes: &[u8],
    interner: &SidInterner,
) -> Result<Vec<Flake>, FlakesTransportError> {
    let transport = decode_flakes(bytes)?;
    transport
        .into_iter()
        .map(|tf| tf.to_flake(interner))
        .collect()
}

/// Check if bytes start with FLKB magic header
///
/// Use this to detect whether a leaf node contains FLKB-encoded flakes
/// vs traditional JSON-encoded flakes.
#[inline]
pub fn is_flkb_format(bytes: &[u8]) -> bool {
    bytes.len() >= HEADER_SIZE && &bytes[0..4] == MAGIC
}

/// Errors during flakes transport encoding/decoding
#[derive(Debug, thiserror::Error)]
pub enum FlakesTransportError {
    #[error("payload too short")]
    TooShort,
    #[error("invalid magic bytes")]
    InvalidMagic,
    #[error("unsupported version: {0}")]
    UnsupportedVersion(u8),
    #[error("CBOR encode error: {0}")]
    CborEncode(ciborium::ser::Error<std::io::Error>),
    #[error("CBOR decode error: {0}")]
    CborDecode(ciborium::de::Error<std::io::Error>),
    #[error("parse error: {0}")]
    ParseError(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_sid(ns: u16, name: &str) -> Sid {
        Sid::new(ns, name)
    }

    fn make_flake(s: Sid, p: Sid, o: FlakeValue, dt: Sid, t: i64, op: bool) -> Flake {
        Flake {
            g: None,
            s,
            p,
            o,
            dt,
            t,
            op,
            m: None,
        }
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let flakes = vec![
            make_flake(
                make_sid(1, "subject1"),
                make_sid(2, "predicate1"),
                FlakeValue::String("hello".to_string()),
                make_sid(3, "xsd:string"),
                100,
                true,
            ),
            make_flake(
                make_sid(1, "subject2"),
                make_sid(2, "predicate2"),
                FlakeValue::Long(42),
                make_sid(3, "xsd:long"),
                101,
                true,
            ),
        ];

        let encoded = encode_flakes(&flakes).unwrap();

        // Verify header
        assert_eq!(&encoded[0..4], b"FLKB");
        assert_eq!(encoded[4], 1); // version

        // Decode and verify
        let decoded = decode_flakes(&encoded).unwrap();
        assert_eq!(decoded.len(), 2);

        // Check first flake
        assert_eq!(decoded[0].s.ns, 1);
        assert_eq!(decoded[0].s.name, "subject1");
        assert_eq!(decoded[0].t, 100);
        assert!(decoded[0].op);
        assert!(matches!(
            &decoded[0].o,
            TransportValue::String(s) if s == "hello"
        ));

        // Check second flake
        assert_eq!(decoded[1].s.ns, 1);
        assert_eq!(decoded[1].s.name, "subject2");
        assert!(matches!(&decoded[1].o, TransportValue::Long(42)));
    }

    #[test]
    fn test_transport_value_variants() {
        // Test value type conversions (excluding BigInt/Decimal which require external crates)
        let cases: Vec<(FlakeValue, &str)> = vec![
            (FlakeValue::String("test".to_string()), "string"),
            (FlakeValue::Long(123), "long"),
            (FlakeValue::Double(3.13), "double"),
            (FlakeValue::Boolean(true), "bool"),
            (FlakeValue::Ref(make_sid(1, "ref")), "ref"),
            (FlakeValue::Vector(vec![1.0, 2.0, 3.0]), "vector"),
            (FlakeValue::Json(r#"{"key":"value"}"#.to_string()), "json"),
            (FlakeValue::Null, "null"),
        ];

        for (value, expected_type) in cases {
            let transport = TransportValue::from(&value);

            // Serialize to JSON to check the tag
            let json = serde_json::to_string(&transport).unwrap();
            assert!(
                json.contains(&format!(r#""type":"{expected_type}""#)),
                "Expected type '{expected_type}' in JSON: {json}"
            );
        }
    }

    #[test]
    fn test_flake_with_metadata() {
        let mut flake = make_flake(
            make_sid(1, "s"),
            make_sid(2, "p"),
            FlakeValue::String("value".to_string()),
            make_sid(3, "dt"),
            100,
            true,
        );
        flake.m = Some(FlakeMeta {
            lang: Some("en".to_string()),
            i: Some(5),
        });

        let transport = TransportFlake::from(&flake);
        assert_eq!(transport.lang, Some("en".to_string()));
        assert_eq!(transport.i, Some(5));
    }

    #[test]
    fn test_invalid_magic() {
        let bytes = b"XXXX\x01\x00\x00\x00";
        let result = decode_flakes(bytes);
        assert!(matches!(result, Err(FlakesTransportError::InvalidMagic)));
    }

    #[test]
    fn test_unsupported_version() {
        let bytes = b"FLKB\x99\x00\x00\x00";
        let result = decode_flakes(bytes);
        assert!(matches!(
            result,
            Err(FlakesTransportError::UnsupportedVersion(0x99))
        ));
    }

    #[test]
    fn test_too_short() {
        let bytes = b"FLKB";
        let result = decode_flakes(bytes);
        assert!(matches!(result, Err(FlakesTransportError::TooShort)));
    }

    #[test]
    fn test_empty_flakes_roundtrip() {
        let flakes: Vec<Flake> = vec![];
        let encoded = encode_flakes(&flakes).unwrap();
        let decoded = decode_flakes(&encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_is_flkb_format() {
        assert!(is_flkb_format(b"FLKB\x01\x00\x00\x00"));
        assert!(!is_flkb_format(b"JSON"));
        assert!(!is_flkb_format(b"FLK")); // too short
    }

    #[test]
    fn test_decode_flakes_interned() {
        let interner = SidInterner::new();

        let flakes = vec![make_flake(
            make_sid(1, "subject"),
            make_sid(2, "predicate"),
            FlakeValue::String("value".to_string()),
            make_sid(3, "xsd:string"),
            100,
            true,
        )];

        let encoded = encode_flakes(&flakes).unwrap();
        let decoded = decode_flakes_interned(&encoded, &interner).unwrap();

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].s.namespace_code, 1);
        assert_eq!(decoded[0].s.name.as_ref(), "subject");
        assert_eq!(decoded[0].t, 100);
    }

    #[test]
    fn test_to_flake_value_reconstruction() {
        let interner = SidInterner::new();

        // Test string
        let tv = TransportValue::String("hello".to_string());
        let fv = tv.to_flake_value(&interner).unwrap();
        assert!(matches!(fv, FlakeValue::String(s) if s == "hello"));

        // Test long
        let tv = TransportValue::Long(42);
        let fv = tv.to_flake_value(&interner).unwrap();
        assert!(matches!(fv, FlakeValue::Long(42)));

        // Test ref (should use interner)
        let tv = TransportValue::Ref(TransportSid {
            ns: 100,
            name: "test".to_string(),
        });
        let fv = tv.to_flake_value(&interner).unwrap();
        if let FlakeValue::Ref(sid) = fv {
            assert_eq!(sid.namespace_code, 100);
            assert_eq!(sid.name.as_ref(), "test");
        } else {
            panic!("Expected Ref");
        }
    }

    #[test]
    fn test_to_flake_value_bigint() {
        let interner = SidInterner::new();

        // Valid bigint
        let tv = TransportValue::BigInt("12345678901234567890".to_string());
        let fv = tv.to_flake_value(&interner).unwrap();
        assert!(matches!(fv, FlakeValue::BigInt(_)));

        // Invalid bigint returns error (not panic)
        let tv = TransportValue::BigInt("not_a_number".to_string());
        let result = tv.to_flake_value(&interner);
        assert!(matches!(result, Err(FlakesTransportError::ParseError(_))));
    }

    #[test]
    fn test_to_flake_reconstruction_error() {
        let interner = SidInterner::new();

        // Create transport flake with invalid BigInt value
        let tf = TransportFlake {
            s: TransportSid {
                ns: 1,
                name: "s".to_string(),
            },
            p: TransportSid {
                ns: 2,
                name: "p".to_string(),
            },
            o: TransportValue::BigInt("invalid".to_string()),
            dt: TransportSid {
                ns: 3,
                name: "dt".to_string(),
            },
            t: 100,
            op: true,
            lang: None,
            i: None,
        };

        // Should return error, not panic
        let result = tf.to_flake(&interner);
        assert!(result.is_err());
    }

    #[test]
    fn test_to_flake_with_metadata() {
        let interner = SidInterner::new();

        let tf = TransportFlake {
            s: TransportSid {
                ns: 1,
                name: "s".to_string(),
            },
            p: TransportSid {
                ns: 2,
                name: "p".to_string(),
            },
            o: TransportValue::String("value".to_string()),
            dt: TransportSid {
                ns: 3,
                name: "dt".to_string(),
            },
            t: 100,
            op: true,
            lang: Some("en".to_string()),
            i: Some(5),
        };

        let flake = tf.to_flake(&interner).unwrap();
        assert!(flake.m.is_some());
        let m = flake.m.unwrap();
        assert_eq!(m.lang, Some("en".to_string()));
        assert_eq!(m.i, Some(5));
    }
}
