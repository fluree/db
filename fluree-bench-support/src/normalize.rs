//! Result normalization for benchmark (LDBC SNB) diffing.
//!
//! `cypher-json` already emits native scalars (dates as ISO strings, longs as
//! numbers), so it is the primary fix. But LDBC validation files are
//! idiosyncratic: some date columns are **epoch-millis integers** (datagen's
//! `LongDateFormatter`), others ISO strings. This adapter normalizes each result
//! cell to the representation a golden file expects — keyed off the **expected**
//! column type, never one blanket rule — plus a belt-and-suspenders flatten for
//! results that arrive as RDF JSON-LD value-objects.
//!
//! NOTE: the exact epoch convention (millis vs seconds, timezone) varies by
//! datagen version — confirm [`iso_to_epoch_millis`] against the actual
//! validation files before trusting cross-query comparisons.

use chrono::{DateTime, NaiveDate};
use serde_json::Value;

/// The representation a result column should take for comparison, derived from
/// the golden / validation file's schema for that column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Expected {
    /// Native value; only RDF value-object flattening is applied.
    #[default]
    Auto,
    /// Epoch milliseconds (UTC) — ISO date/dateTime strings are converted.
    EpochMillis,
    /// ISO-8601 date/dateTime string (flattened, otherwise unchanged).
    Iso,
    /// Integer — numeric strings are coerced to a JSON number.
    Long,
    /// Float — numeric strings are coerced to a JSON number.
    Double,
    /// String — the value is stringified.
    Str,
}

/// Flatten an RDF-faithful JSON-LD value to a native scalar: `{"@value": v, …}`
/// → `v`, `{"@id": iri}` → `iri`, lists recurse, bare scalars pass through.
pub fn flatten(value: Value) -> Value {
    match value {
        Value::Object(mut m) => {
            if let Some(v) = m.remove("@value") {
                v
            } else if let Some(id) = m.remove("@id") {
                id
            } else {
                Value::Object(m)
            }
        }
        Value::Array(items) => Value::Array(items.into_iter().map(flatten).collect()),
        other => other,
    }
}

/// Normalize one result cell to its [`Expected`] representation for diffing.
pub fn normalize(value: Value, expected: Expected) -> Value {
    let value = flatten(value);
    match expected {
        Expected::Auto | Expected::Iso => value,
        Expected::EpochMillis => match value.as_str().and_then(iso_to_epoch_millis) {
            Some(ms) => Value::Number(ms.into()),
            None => value,
        },
        Expected::Long => match value.as_str().and_then(|s| s.trim().parse::<i64>().ok()) {
            Some(n) => Value::Number(n.into()),
            None => value,
        },
        Expected::Double => match value.as_str().and_then(|s| s.trim().parse::<f64>().ok()) {
            Some(f) => serde_json::Number::from_f64(f).map_or(value, Value::Number),
            None => value,
        },
        Expected::Str => match value {
            Value::String(_) => value,
            Value::Null => value,
            other => Value::String(other.to_string()),
        },
    }
}

/// Normalize a whole row against a per-column expected-type spec. Extra cells
/// (beyond `expected`) default to [`Expected::Auto`].
pub fn normalize_row(row: Vec<Value>, expected: &[Expected]) -> Vec<Value> {
    row.into_iter()
        .enumerate()
        .map(|(i, v)| normalize(v, expected.get(i).copied().unwrap_or_default()))
        .collect()
}

/// Parse an LDBC ISO date or dateTime to epoch **milliseconds** (UTC). A
/// date-only `YYYY-MM-DD` is taken at UTC midnight (datagen's
/// `LongDateFormatter` convention). Returns `None` if unparseable.
pub fn iso_to_epoch_millis(s: &str) -> Option<i64> {
    let s = s.trim();
    // Offset-bearing dateTime, RFC 3339 (`…Z` / `…+01:00`).
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.timestamp_millis());
    }
    // LDBC datagen dateTime, e.g. `2010-01-02T03:04:05.000+0000`.
    if let Ok(dt) = DateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.3f%z") {
        return Some(dt.timestamp_millis());
    }
    // Date-only → UTC midnight.
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .ok()
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|ndt| ndt.and_utc().timestamp_millis())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn flatten_strips_rdf_wrappers() {
        assert_eq!(
            flatten(json!({"@value": "1990-11-23", "@type": "…#date"})),
            json!("1990-11-23")
        );
        assert_eq!(flatten(json!({"@id": "ex:alice"})), json!("ex:alice"));
        assert_eq!(flatten(json!("Alice")), json!("Alice"));
        assert_eq!(flatten(json!(42)), json!(42));
        assert_eq!(
            flatten(json!([{"@value": 1}, {"@id": "x"}])),
            json!([1, "x"])
        );
    }

    #[test]
    fn date_to_epoch_millis_utc_midnight() {
        // 1990-11-23T00:00:00Z = 659318400000 ms.
        assert_eq!(iso_to_epoch_millis("1990-11-23"), Some(659_318_400_000));
        // Epoch.
        assert_eq!(iso_to_epoch_millis("1970-01-01"), Some(0));
    }

    #[test]
    fn datetime_to_epoch_millis() {
        assert_eq!(
            iso_to_epoch_millis("1970-01-01T00:00:01.000+0000"),
            Some(1000)
        );
        assert_eq!(iso_to_epoch_millis("1970-01-01T00:00:00Z"), Some(0));
    }

    #[test]
    fn normalize_keys_off_expected_type() {
        // A cypher-json ISO date string, golden file wants epoch millis.
        assert_eq!(
            normalize(json!("1990-11-23"), Expected::EpochMillis),
            json!(659_318_400_000i64)
        );
        // Same value, golden file wants the ISO string — unchanged.
        assert_eq!(
            normalize(json!("1990-11-23"), Expected::Iso),
            json!("1990-11-23")
        );
        // An RDF value-object date, golden wants epoch — flatten then convert.
        assert_eq!(
            normalize(
                json!({"@value": "1990-11-23", "@type": "…#date"}),
                Expected::EpochMillis
            ),
            json!(659_318_400_000i64)
        );
        // Numeric string → number under Long.
        assert_eq!(normalize(json!("42"), Expected::Long), json!(42));
        // Auto leaves natives alone.
        assert_eq!(normalize(json!(42), Expected::Auto), json!(42));
    }

    #[test]
    fn normalize_row_aligns_to_spec() {
        let row = vec![json!("Alice"), json!("1990-11-23"), json!("99")];
        let spec = [Expected::Str, Expected::EpochMillis, Expected::Long];
        assert_eq!(
            normalize_row(row, &spec),
            vec![json!("Alice"), json!(659_318_400_000i64), json!(99)]
        );
    }
}
