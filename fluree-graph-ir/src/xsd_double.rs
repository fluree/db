//! Canonical XSD lexical form for `xsd:double` values.
//!
//! The W3C canonical `xsd:double` representation is scientific notation with a
//! mantissa in `[1, 10)` (or `0.0` for zero) that always contains a decimal
//! point, an uppercase `E`, and an exponent with no `+` sign or leading zeros:
//! `1000000.0 → "1.0E6"`, `0.001 → "1.0E-3"`, `0.0 → "0.0E0"`.
//!
//! The special values keep their XSD lexical spellings: `NaN`, `INF`, `-INF`.
//!
//! Every RDF-lexical serialization site (SPARQL Results JSON/XML, CSV/TSV,
//! RDF/XML, N-Triples/N-Quads export, `LiteralValue::lexical()`) must route
//! through these helpers so Fluree emits one consistent, spec-aligned form.
//! JSON-LD (and other JSON-native typed output) is deliberately excluded:
//! there a double is a native JSON number, never a lexical string.

use std::fmt::{self, Write as _};

/// Upper bound for the canonical form of any finite `f64`:
/// sign (1) + 17-digit shortest-round-trip mantissa with dot (18) +
/// inserted ".0" (2) + 'E' (1) + exponent up to "-308" (4) = 26. Rounded up.
const BUF_LEN: usize = 32;

/// Fixed-size stack writer used to capture `{:e}` output without allocating.
struct StackBuf {
    bytes: [u8; BUF_LEN],
    len: usize,
}

impl StackBuf {
    fn new() -> Self {
        StackBuf {
            bytes: [0; BUF_LEN],
            len: 0,
        }
    }

    fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len]
    }

    fn push_slice(&mut self, s: &[u8]) {
        self.bytes[self.len..self.len + s.len()].copy_from_slice(s);
        self.len += s.len();
    }
}

impl fmt::Write for StackBuf {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        if self.len + s.len() > BUF_LEN {
            return Err(fmt::Error);
        }
        self.push_slice(s.as_bytes());
        Ok(())
    }
}

/// Render the canonical form of a *finite* `f64` into a stack buffer.
///
/// Builds on Rust's shortest-round-trip scientific formatter (`{:e}`), which
/// already produces exactly one (nonzero, unless the value is zero) digit
/// before the optional decimal point and an exponent with no `+`/leading
/// zeros. Canonicalization is then purely syntactic: ensure the mantissa
/// contains a `.` (insert `.0`) and uppercase the exponent marker.
fn finite_canonical(d: f64) -> StackBuf {
    debug_assert!(d.is_finite());

    let mut sci = StackBuf::new();
    write!(sci, "{d:e}").expect("`{:e}` of an f64 fits in 32 bytes");

    let s = sci.as_bytes();
    let e_pos = s
        .iter()
        .position(|&b| b == b'e')
        .expect("`{:e}` output always contains an exponent");
    let (mantissa, exponent) = (&s[..e_pos], &s[e_pos + 1..]);

    let mut out = StackBuf::new();
    out.push_slice(mantissa);
    if !mantissa.contains(&b'.') {
        out.push_slice(b".0");
    }
    out.push_slice(b"E");
    out.push_slice(exponent);
    out
}

/// The canonical XSD lexical form of an `xsd:double` value, as a `String`.
///
/// Examples: `1000000.0 → "1.0E6"`, `1e30 → "1.0E30"`, `0.001 → "1.0E-3"`,
/// `0.0 → "0.0E0"`, `-0.0 → "-0.0E0"`, `NaN → "NaN"`, `f64::INFINITY → "INF"`.
#[must_use]
pub fn canonical_xsd_double(d: f64) -> String {
    if d.is_nan() {
        return "NaN".to_string();
    }
    if d.is_infinite() {
        return if d.is_sign_positive() { "INF" } else { "-INF" }.to_string();
    }
    let buf = finite_canonical(d);
    // The canonical form is pure ASCII.
    std::str::from_utf8(buf.as_bytes())
        .expect("canonical xsd:double form is ASCII")
        .to_string()
}

/// Append the canonical XSD lexical form of `d` to a `String`.
///
/// Allocation-free apart from growing `out`.
pub fn push_canonical_xsd_double(out: &mut String, d: f64) {
    if d.is_nan() {
        out.push_str("NaN");
        return;
    }
    if d.is_infinite() {
        out.push_str(if d.is_sign_positive() { "INF" } else { "-INF" });
        return;
    }
    let buf = finite_canonical(d);
    out.push_str(std::str::from_utf8(buf.as_bytes()).expect("canonical xsd:double form is ASCII"));
}

/// Append the canonical XSD lexical form of `d` to a byte buffer.
///
/// Zero-allocation variant for the delimited (CSV/TSV) writer's cell buffers.
pub fn write_canonical_xsd_double(out: &mut Vec<u8>, d: f64) {
    if d.is_nan() {
        out.extend_from_slice(b"NaN");
        return;
    }
    if d.is_infinite() {
        out.extend_from_slice(if d.is_sign_positive() {
            b"INF" as &[u8]
        } else {
            b"-INF"
        });
        return;
    }
    let buf = finite_canonical(d);
    out.extend_from_slice(buf.as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The W3C forms table from the burn-down cluster audit
    /// (`docs/audit/burn-down/lexer-formatter.md` §3), plus sign/edge cases.
    const CASES: &[(f64, &str)] = &[
        (1_000_000.0, "1.0E6"),
        (1.0, "1.0E0"),
        (2.2, "2.2E0"),
        (0.001, "1.0E-3"),
        (0.0, "0.0E0"),
        (-0.0, "-0.0E0"),
        (1e30, "1.0E30"),
        (1e-10, "1.0E-10"),
        (123.456, "1.23456E2"),
        (6.02e23, "6.02E23"),
        (-3.0, "-3.0E0"),
        (-12.5, "-1.25E1"),
        // Subnormals and range extremes.
        (5e-324, "5.0E-324"),
        (f64::MAX, "1.7976931348623157E308"),
        (f64::MIN, "-1.7976931348623157E308"),
        (f64::MIN_POSITIVE, "2.2250738585072014E-308"),
    ];

    #[test]
    fn canonical_forms_match_w3c_table() {
        for &(input, expected) in CASES {
            assert_eq!(
                canonical_xsd_double(input),
                expected,
                "canonical_xsd_double({input:?})"
            );
        }
    }

    #[test]
    fn special_values_keep_xsd_spellings() {
        assert_eq!(canonical_xsd_double(f64::NAN), "NaN");
        assert_eq!(canonical_xsd_double(f64::INFINITY), "INF");
        assert_eq!(canonical_xsd_double(f64::NEG_INFINITY), "-INF");
    }

    #[test]
    fn push_variant_matches_string_variant() {
        for &(input, expected) in CASES {
            let mut s = String::from("prefix:");
            push_canonical_xsd_double(&mut s, input);
            assert_eq!(s, format!("prefix:{expected}"));
        }
        let mut s = String::new();
        push_canonical_xsd_double(&mut s, f64::NEG_INFINITY);
        assert_eq!(s, "-INF");
    }

    #[test]
    fn write_variant_matches_string_variant() {
        for &(input, expected) in CASES {
            let mut cell = b"cell:".to_vec();
            write_canonical_xsd_double(&mut cell, input);
            assert_eq!(cell, format!("cell:{expected}").into_bytes());
        }
        let mut cell = Vec::new();
        write_canonical_xsd_double(&mut cell, f64::NAN);
        assert_eq!(cell, b"NaN");
    }

    #[test]
    fn canonical_form_round_trips() {
        for &(input, _) in CASES {
            let parsed: f64 = canonical_xsd_double(input).parse().expect("parse back");
            assert_eq!(parsed.to_bits(), input.to_bits(), "round trip of {input:?}");
        }
    }
}
