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
    match serde_json::to_string(s) {
        Ok(escaped) => out.push_str(&escaped),
        // Serializing a `&str` cannot fail.
        Err(_) => out.push_str("\"\""),
    }
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
/// Rust's `{:e}` supplies the shortest digit string that round trips, which
/// is the same digit string ECMAScript starts from. Two things still differ.
/// When two shortest strings round trip, ECMAScript takes the even one, and
/// Rust does not. ECMAScript also switches to exponential notation at its own
/// bounds, which are expressed here in terms of `n`, the position of the
/// decimal point.
fn ecmascript_number(f: f64) -> String {
    if f == 0.0 {
        // Covers -0.0, which ECMAScript prints as "0".
        return "0".to_string();
    }
    let (digits, n) = shortest_digits(f.abs());
    let rendered = render(&digits, n);
    if f < 0.0 {
        format!("-{rendered}")
    } else {
        rendered
    }
}

/// The shortest digit string that round trips, and `n` such that the value is
/// `0.digits * 10^n`.
fn shortest_digits(abs: f64) -> (String, i32) {
    let exponential = format!("{abs:e}");
    let (mantissa, exponent) = exponential
        .split_once('e')
        .expect("`{:e}` always writes an exponent");
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let exponent: i32 = exponent.parse().expect("`{:e}` writes a decimal exponent");
    let n = exponent + 1;
    (even_tie(digits, n, abs), n)
}

/// Break a tie the way ECMAScript does.
///
/// Two digit strings of the same length can both round trip to `abs`. Rust
/// returns whichever its algorithm reaches; ECMAScript requires the even one.
fn even_tie(digits: String, n: i32, abs: f64) -> String {
    let Ok(value) = digits.parse::<u128>() else {
        return digits;
    };
    if value % 2 == 0 {
        return digits;
    }
    let width = digits.len();
    let scale = n - width as i32;
    for candidate in [value - 1, value + 1] {
        let candidate = candidate.to_string();
        if candidate.len() != width {
            continue;
        }
        if format!("{candidate}e{scale}").parse::<f64>() == Ok(abs) {
            return candidate;
        }
    }
    digits
}

/// Place the decimal point per ECMAScript, given `k` digits and the point
/// position `n`.
fn render(digits: &str, n: i32) -> String {
    let k = digits.len() as i32;
    if (k..=21).contains(&n) {
        // Integral: pad with zeros out to the point.
        return format!("{digits}{}", "0".repeat((n - k) as usize));
    }
    if (1..=21).contains(&n) {
        let (int, frac) = digits.split_at(n as usize);
        return format!("{int}.{frac}");
    }
    if (-5..=0).contains(&n) {
        return format!("0.{}{digits}", "0".repeat((-n) as usize));
    }
    let (first, rest) = digits.split_at(1);
    let mantissa = if rest.is_empty() {
        first.to_string()
    } else {
        format!("{first}.{rest}")
    };
    let exponent = n - 1;
    if exponent >= 0 {
        format!("{mantissa}e+{exponent}")
    } else {
        format!("{mantissa}e{exponent}")
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
    fn ties_pick_the_even_digit_string() {
        // Both "810553441865041.2" and "810553441865041.3" round trip to this
        // double. ECMAScript takes the even one; Rust's own formatting takes
        // the other.
        let f = f64::from_bits(4829839628448721546);
        assert_eq!(format!("{f}"), "810553441865041.3");
        assert_eq!(canon(&format!("[{f}]")), "[810553441865041.2]");
    }

    #[test]
    fn every_double_round_trips_through_its_canonical_form() {
        // A deterministic sweep: whatever spelling is chosen, it must parse
        // back to the same double.
        let mut bits: u64 = 0x1234_5678_9abc_def0;
        for _ in 0..20_000 {
            bits = bits
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let f = f64::from_bits(bits);
            if !f.is_finite() {
                continue;
            }
            let text = ecmascript_number(f);
            let parsed: f64 = text.parse().expect("canonical form parses");
            assert_eq!(parsed.to_bits(), f.to_bits(), "{f} rendered as {text}");
        }
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
