//! A JSON number as the RDF literal JSON-LD makes of it.
//!
//! JSON-LD 1.1 API § 8.2.2 (Object to RDF Conversion) types a number by its
//! value, not its spelling: one with a fractional part, or of 10^21 and
//! more, is an `xsd:double`, and any other an `xsd:integer`. So `1.0`, `1e3`
//! and `-0e0` are the integers `1`, `1000` and `0`. Every reader of JSON-LD
//! data (the RDF reader, a transaction, a bulk import, a query's constants)
//! goes through here, so they agree.

use serde_json::Number;

/// What [`JsonNumber::read`] makes of a JSON number.
#[derive(Debug, Clone, PartialEq)]
pub enum JsonNumber {
    Integer(i64),
    /// An integer outside i64 and below 10^21, by its digits.
    BigInteger(String),
    Double(f64),
}

impl JsonNumber {
    /// `n` as JSON-LD types it. `None` only for a number `serde_json` cannot
    /// give as an `f64`.
    pub fn read(n: &Number) -> Option<Self> {
        if let Some(i) = n.as_i64() {
            return Some(JsonNumber::Integer(i));
        }
        if n.is_u64() {
            return Some(JsonNumber::BigInteger(n.to_string()));
        }
        let f = n.as_f64()?;
        Some(if f.fract() != 0.0 || f.abs() >= 1e21 {
            JsonNumber::Double(f)
        } else if f.abs() < 9_223_372_036_854_775_808.0 {
            // Exact: an integral double below 2^63. `-0.0` is `0`.
            JsonNumber::Integer(f as i64)
        } else {
            JsonNumber::BigInteger(format!("{f:.0}"))
        })
    }
}

/// `n` when JSON-LD makes it an integer that fits an i64: `1`, `1.0`, `1e3`.
/// A reader that holds only i64 and f64 takes this, and `n.as_f64()` for
/// every other number.
pub fn json_integer(n: &Number) -> Option<i64> {
    match JsonNumber::read(n)? {
        JsonNumber::Integer(i) => Some(i),
        JsonNumber::BigInteger(_) | JsonNumber::Double(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(text: &str) -> JsonNumber {
        JsonNumber::read(&serde_json::from_str(text).unwrap()).unwrap()
    }

    #[test]
    fn a_number_is_typed_by_its_value() {
        use JsonNumber::{BigInteger, Double, Integer};
        assert_eq!(read("1"), Integer(1));
        assert_eq!(read("1.0"), Integer(1));
        assert_eq!(read("1e3"), Integer(1000));
        assert_eq!(read("-0e0"), Integer(0));
        assert_eq!(read("-9223372036854775808"), Integer(i64::MIN));
        assert_eq!(read("2.5"), Double(2.5));
        assert_eq!(read("1e21"), Double(1e21));
        assert_eq!(
            read("18446744073709551615"),
            BigInteger("18446744073709551615".into())
        );
        assert_eq!(read("1e20"), BigInteger("100000000000000000000".into()));
        assert_eq!(read("-1e20"), BigInteger("-100000000000000000000".into()));
        assert_eq!(json_integer(&serde_json::from_str("7.0").unwrap()), Some(7));
        assert_eq!(json_integer(&serde_json::from_str("7.5").unwrap()), None);
        assert_eq!(json_integer(&serde_json::from_str("1e20").unwrap()), None);
    }
}
