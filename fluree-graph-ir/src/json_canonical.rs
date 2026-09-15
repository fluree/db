//! JSON Canonicalization Scheme (RFC 8785).
//!
//! Every `rdf:JSON` literal is canonicalized at ingest. A literal's identity
//! is its text. One value must therefore yield the same byte string whatever
//! serializer produced it.
//!
//! This implementation departs from the RFC in one place. The RFC renders
//! every number as an IEEE 754 double, which rounds integers past 2^53. An
//! integer that fits `i64` or `u64` is kept exact instead. Output remains
//! deterministic.

use serde_json::{Number, Value};
use std::cmp::Ordering;
use std::fmt::Write as _;

/// Canonicalize a JSON document.
///
/// `Err` when `input` is not valid JSON.
pub fn canonicalize_json(input: &str) -> Result<String, serde_json::Error> {
    let value: Value = serde_json::from_str(input)?;
    Ok(canonicalize_json_value(&value))
}

/// Canonicalize a parsed document. Saves a caller holding a [`Value`] the
/// round trip through text.
pub fn canonicalize_json_value(value: &Value) -> String {
    let mut out = String::new();
    write_value(value, &mut out);
    out
}

fn write_value(value: &Value, out: &mut String) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(true) => out.push_str("true"),
        Value::Bool(false) => out.push_str("false"),
        Value::Number(n) => write_number(n, out),
        Value::String(s) => write_string(s, out),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_value(item, out);
            }
            out.push(']');
        }
        Value::Object(members) => {
            // Keys sort by UTF-16 code unit, not by code point. A non-BMP
            // character encodes to a surrogate pair at 0xD800. It therefore
            // sorts before any BMP character above 0xD7FF.
            let mut keys: Vec<&String> = members.keys().collect();
            keys.sort_by(|a, b| utf16_cmp(a, b));
            out.push('{');
            for (i, key) in keys.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_string(key, out);
                out.push(':');
                write_value(&members[key], out);
            }
            out.push('}');
        }
    }
}

/// Compare two strings by their UTF-16 code units.
fn utf16_cmp(a: &str, b: &str) -> Ordering {
    a.encode_utf16().cmp(b.encode_utf16())
}

/// Write a JSON string literal. `serde_json`'s escaping already matches the
/// scheme. Short forms where they exist, `\u00XX` for other control
/// characters, nothing else escaped.
fn write_string(s: &str, out: &mut String) {
    let escaped = Value::String(s.to_string()).to_string();
    out.push_str(&escaped);
}

fn write_number(n: &Number, out: &mut String) {
    if let Some(i) = n.as_i64() {
        let _ = write!(out, "{i}");
    } else if let Some(u) = n.as_u64() {
        let _ = write!(out, "{u}");
    } else if let Some(f) = n.as_f64() {
        out.push_str(&ecmascript_number(f));
    } else {
        // Parsed JSON yields only those three.
        out.push('0');
    }
}

/// Format a double as ECMAScript's `Number::toString` does.
///
/// Rust's `Display` prints the shortest form that round trips and agrees with
/// ECMAScript wherever neither uses an exponent. ECMAScript switches to
/// exponents at different bounds, and signs a positive one.
fn ecmascript_number(f: f64) -> String {
    if f == 0.0 {
        // Covers -0.0, which ECMAScript prints as "0".
        return "0".to_string();
    }
    let abs = f.abs();
    if (1e-6..1e21).contains(&abs) {
        return format!("{f}");
    }
    // `{:e}` prints `1e21`, `1.5e300`, `1e-7`. ECMAScript signs a positive
    // exponent.
    let exponential = format!("{f:e}");
    match exponential.split_once('e') {
        Some((mantissa, exponent)) if !exponent.starts_with('-') => {
            format!("{mantissa}e+{exponent}")
        }
        _ => exponential,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn canon(s: &str) -> String {
        canonicalize_json(s).expect("valid JSON")
    }

    #[test]
    fn member_order_does_not_change_the_result() {
        // The shape from #1781. One value, two writers.
        let a = canon(r#"[{"name": "alpha", "qty": 1}]"#);
        let b = canon(r#"[{"qty": 1, "name": "alpha"}]"#);
        assert_eq!(a, b);
        assert_eq!(a, r#"[{"name":"alpha","qty":1}]"#);
    }

    #[test]
    fn whitespace_and_nesting_collapse() {
        assert_eq!(
            canon("{\n  \"b\" : [1, 2,\t3],\n  \"a\" : {\"y\": true, \"x\": null}\n}"),
            r#"{"a":{"x":null,"y":true},"b":[1,2,3]}"#
        );
    }

    #[test]
    fn keys_sort_by_utf16_code_units() {
        // U+10000 encodes to the surrogate pair D800 DC00. It sorts before
        // U+FFFD despite the larger code point. Code-point order would
        // reverse the two.
        let out = canon("{\"\u{fffd}\":1,\"\u{10000}\":2}");
        assert_eq!(out, "{\"\u{10000}\":2,\"\u{fffd}\":1}");
    }

    #[test]
    fn keys_sort_by_code_unit_not_by_length() {
        assert_eq!(
            canon(r#"{"aa":1,"a":2,"b":3,"A":4}"#),
            r#"{"A":4,"a":2,"aa":1,"b":3}"#
        );
    }

    #[test]
    fn strings_keep_minimal_escaping_and_raw_utf8() {
        // Short escapes where they exist. `\u00XX` for other control
        // characters. Non-ASCII and `/` unescaped.
        assert_eq!(
            canon("{\"k\":\"a\\u0007b\\tc\\\"d/e\u{20ac}\"}"),
            "{\"k\":\"a\\u0007b\\tc\\\"d/e\u{20ac}\"}"
        );
    }

    #[test]
    fn integers_are_printed_exactly() {
        assert_eq!(canon("[0,-0,1,-1,42]"), "[0,0,1,-1,42]");
        // Past 2^53. Kept exact, not rounded through a double.
        assert_eq!(canon("[9007199254740993]"), "[9007199254740993]");
        assert_eq!(
            canon("[-9223372036854775808,18446744073709551615]"),
            "[-9223372036854775808,18446744073709551615]"
        );
    }

    #[test]
    fn doubles_follow_ecmascript() {
        assert_eq!(canon("[1.0]"), "[1]");
        assert_eq!(canon("[-0.0]"), "[0]");
        assert_eq!(canon("[1.5]"), "[1.5]");
        assert_eq!(canon("[0.000001]"), "[0.000001]");
        assert_eq!(canon("[1e-7]"), "[1e-7]");
        assert_eq!(canon("[1e20]"), "[100000000000000000000]");
        assert_eq!(canon("[1e21]"), "[1e+21]");
        assert_eq!(canon("[1.5e300]"), "[1.5e+300]");
        assert_eq!(canon("[-1e21]"), "[-1e+21]");
        // The shortest form that round trips, as ECMAScript prints it.
        assert_eq!(
            canon("[333333333333333314832.0]"),
            "[333333333333333300000]"
        );
    }

    #[test]
    fn canonicalizing_twice_changes_nothing() {
        for input in [
            r#"{"b":[1,2,{"d":1e21,"c":"x"}],"a":null}"#,
            r#"[{"qty": 1, "name": "alpha"}]"#,
            r#""just a string""#,
            "123",
        ] {
            let once = canon(input);
            assert_eq!(canon(&once), once, "input: {input}");
        }
    }

    #[test]
    fn scalars_are_documents_too() {
        assert_eq!(canon("  true "), "true");
        assert_eq!(canon(" null"), "null");
        assert_eq!(canon(r#"  "hi"  "#), r#""hi""#);
    }

    #[test]
    fn invalid_json_is_an_error() {
        assert!(canonicalize_json("not json").is_err());
        assert!(canonicalize_json("{\"a\":}").is_err());
        assert!(canonicalize_json("").is_err());
    }
}
