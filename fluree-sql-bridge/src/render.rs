//! Values → the JSON the Trino client protocol uses, and driver type names →
//! Trino type names. Kept driver-agnostic so the three backends share it.

use base64::Engine;
use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use serde_json::{json, Value};

pub fn bool(b: bool) -> Value {
    Value::Bool(b)
}

pub fn int(i: i64) -> Value {
    json!(i)
}

pub fn uint(u: u64) -> Value {
    json!(u)
}

pub fn double(d: f64) -> Value {
    if d.is_nan() {
        json!("NaN")
    } else if d.is_infinite() {
        json!(if d > 0.0 { "Infinity" } else { "-Infinity" })
    } else {
        json!(d)
    }
}

/// Exact decimal at the bridge's fixed scale (the column type is reported as
/// `decimal(38, scale)`); rounds half-even when the value carries more digits.
pub fn decimal(d: &BigDecimal, scale: i64) -> Value {
    let fixed = d.with_scale_round(scale, bigdecimal::RoundingMode::HalfEven);
    json!(fixed.to_plain_string())
}

pub fn string(s: impl Into<String>) -> Value {
    Value::String(s.into())
}

pub fn bytes(b: &[u8]) -> Value {
    json!(base64::engine::general_purpose::STANDARD.encode(b))
}

pub fn date(d: NaiveDate) -> Value {
    json!(d.format("%Y-%m-%d").to_string())
}

pub fn timestamp(t: NaiveDateTime) -> Value {
    json!(t.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
}

pub fn timestamp_tz(t: DateTime<Utc>) -> Value {
    json!(t.format("%Y-%m-%d %H:%M:%S%.6f UTC").to_string())
}

pub fn time(t: NaiveTime) -> Value {
    json!(t.format("%H:%M:%S%.3f").to_string())
}

pub fn jsonish(v: &Value) -> Value {
    Value::String(v.to_string())
}

/// Trino's spelling of the types this bridge produces.
pub mod trino {
    pub const BOOLEAN: &str = "boolean";
    pub const INTEGER: &str = "integer";
    pub const BIGINT: &str = "bigint";
    pub const REAL: &str = "real";
    pub const DOUBLE: &str = "double";
    pub const VARCHAR: &str = "varchar";
    pub const VARBINARY: &str = "varbinary";
    pub const DATE: &str = "date";
    pub const TIMESTAMP: &str = "timestamp(6)";
    pub const TIMESTAMP_TZ: &str = "timestamp(6) with time zone";
    pub const TIME: &str = "time(3)";

    pub fn decimal(scale: i64) -> String {
        format!("decimal(38,{scale})")
    }
}
