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
//!
//! The reverse direction, lexical form to value, is [`parse_xsd_double`] and
//! [`parse_xsd_float`]: the one definition of which spellings are `xsd:double`
//! and `xsd:float` lexical forms.

use std::fmt::{self, Write as _};

mod sealed {
    pub trait Sealed {}
    impl Sealed for f32 {}
    impl Sealed for f64 {}
}

/// Float scalar admitted to the XSD lexical formatters (`f32`/`f64`); sealed.
///
/// Carries the two facts every formatter needs and `LowerExp` alone cannot
/// provide: whether the value is finite (so [`finite_canonical`]'s contract
/// stays machine-checked now that the writer is generic) and the XSD spelling
/// of the non-finite values, written once here for every call site — the
/// canonical formatters below and the XPath cast renderers in
/// `fluree-db-query` alike.
pub trait XsdFloat: sealed::Sealed + fmt::LowerExp + Copy {
    #[doc(hidden)]
    fn is_nan_v(self) -> bool;
    #[doc(hidden)]
    fn is_infinite_v(self) -> bool;
    #[doc(hidden)]
    fn is_sign_positive_v(self) -> bool;

    /// The XSD lexical spelling of a non-finite value — `NaN`, `INF`,
    /// `-INF` — or `None` when the value is finite. Static strings only;
    /// allocates nothing.
    #[inline]
    fn nonfinite_xsd(self) -> Option<&'static str> {
        if self.is_nan_v() {
            Some("NaN")
        } else if self.is_infinite_v() {
            Some(if self.is_sign_positive_v() {
                "INF"
            } else {
                "-INF"
            })
        } else {
            None
        }
    }
}

impl XsdFloat for f64 {
    fn is_nan_v(self) -> bool {
        self.is_nan()
    }
    fn is_infinite_v(self) -> bool {
        self.is_infinite()
    }
    fn is_sign_positive_v(self) -> bool {
        self.is_sign_positive()
    }
}

impl XsdFloat for f32 {
    fn is_nan_v(self) -> bool {
        self.is_nan()
    }
    fn is_infinite_v(self) -> bool {
        self.is_infinite()
    }
    fn is_sign_positive_v(self) -> bool {
        self.is_sign_positive()
    }
}

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

/// Render the canonical form of a *finite* float (`f64` or `f32`) into a
/// stack buffer.
///
/// Builds on Rust's shortest-round-trip scientific formatter (`{:e}`), which
/// already produces exactly one (nonzero, unless the value is zero) digit
/// before the optional decimal point and an exponent with no `+`/leading
/// zeros. Canonicalization is then purely syntactic: ensure the mantissa
/// contains a `.` (insert `.0`) and uppercase the exponent marker.
///
/// Callers must exclude NaN/±INF first (via [`XsdFloat::nonfinite_xsd`]) —
/// their `{:e}` forms carry no exponent, and their XSD spellings (`NaN`,
/// `INF`, `-INF`) differ from Rust's.
fn finite_canonical<F: XsdFloat>(d: F) -> StackBuf {
    debug_assert!(
        d.nonfinite_xsd().is_none(),
        "finite_canonical requires a finite input"
    );
    let mut sci = StackBuf::new();
    write!(sci, "{d:e}").expect("`{:e}` of a float fits in 32 bytes");

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
    if let Some(s) = d.nonfinite_xsd() {
        return s.to_string();
    }
    let buf = finite_canonical(d);
    // The canonical form is pure ASCII.
    std::str::from_utf8(buf.as_bytes())
        .expect("canonical xsd:double form is ASCII")
        .to_string()
}

/// The canonical XSD lexical form of an `xsd:float` value, as a `String`.
///
/// Same grammar as [`canonical_xsd_double`] — mantissa with a decimal point,
/// uppercase `E`, no `+`/leading zeros in the exponent, and the XSD special
/// spellings `NaN`/`INF`/`-INF` — but the mantissa is the f32
/// shortest-round-trip form, so a single-precision value never picks up
/// double-precision widening artifacts (`33.33f32` stays `3.333E1`, not
/// `3.333000183105469E1`).
#[must_use]
pub fn canonical_xsd_float(f: f32) -> String {
    if let Some(s) = f.nonfinite_xsd() {
        return s.to_string();
    }
    let buf = finite_canonical(f);
    // The canonical form is pure ASCII.
    std::str::from_utf8(buf.as_bytes())
        .expect("canonical xsd:float form is ASCII")
        .to_string()
}

/// Append the canonical XSD lexical form of `d` to a `String`.
///
/// Allocation-free apart from growing `out`.
pub fn push_canonical_xsd_double(out: &mut String, d: f64) {
    if let Some(s) = d.nonfinite_xsd() {
        out.push_str(s);
        return;
    }
    let buf = finite_canonical(d);
    out.push_str(std::str::from_utf8(buf.as_bytes()).expect("canonical xsd:double form is ASCII"));
}

/// Append the canonical XSD lexical form of `d` to a byte buffer.
///
/// Zero-allocation variant for the delimited (CSV/TSV) writer's cell buffers.
pub fn write_canonical_xsd_double(out: &mut Vec<u8>, d: f64) {
    if let Some(s) = d.nonfinite_xsd() {
        out.extend_from_slice(s.as_bytes());
        return;
    }
    let buf = finite_canonical(d);
    out.extend_from_slice(buf.as_bytes());
}

/// The value of an `xsd:double` lexical form, or `None` when the string is not
/// in the lexical space (XSD 1.1 Part 2 §3.3.5).
///
/// The lexical space is the numerals (`1`, `-1.5`, `.5`, `1.`, `+1.5E-3`) and
/// the four special spellings `INF`, `+INF`, `-INF` and `NaN`. A numeral too
/// large in magnitude for a double maps to `INF` / `-INF`, as XSD 1.1's rounding
/// rule prescribes. Anything else is not an `xsd:double` lexical form:
/// `inf`, `Infinity`, `nan`, a signed `NaN`, or surrounding whitespace.
///
/// Every place that turns an `xsd:double` or `xsd:float` lexical form into a
/// value should go through this function or [`parse_xsd_float`], so that every
/// write and query surface accepts the same set of spellings.
#[must_use]
pub fn parse_xsd_double(lexical: &str) -> Option<f64> {
    match special_value(lexical) {
        Some(special) => Some(special),
        None if is_numeral_alphabet(lexical) => lexical.parse::<f64>().ok(),
        None => None,
    }
}

/// [`parse_xsd_double`] for `xsd:float` (XSD 1.1 Part 2 §3.3.4): the same
/// lexical space, mapped to the nearest single-precision value. A numeral
/// beyond the single-precision range maps to `INF` / `-INF`.
#[must_use]
pub fn parse_xsd_float(lexical: &str) -> Option<f32> {
    match special_value(lexical) {
        Some(special) => Some(special as f32),
        None if is_numeral_alphabet(lexical) => lexical.parse::<f32>().ok(),
        None => None,
    }
}

/// The four special spellings of the `xsd:double` / `xsd:float` lexical space.
fn special_value(lexical: &str) -> Option<f64> {
    match lexical {
        "INF" | "+INF" => Some(f64::INFINITY),
        "-INF" => Some(f64::NEG_INFINITY),
        "NaN" => Some(f64::NAN),
        _ => None,
    }
}

/// Whether every byte can occur in an XSD numeral: a digit, a sign, the
/// decimal point or an exponent marker.
///
/// Rust's float grammar is XSD's numeral grammar plus the words `inf`,
/// `infinity` and `nan` in any case. Those words are the only accepted inputs
/// that contain any other byte, so a string that passes this check is accepted
/// by Rust's parser exactly when it is an XSD numeral.
fn is_numeral_alphabet(lexical: &str) -> bool {
    lexical
        .bytes()
        .all(|b| b.is_ascii_digit() || matches!(b, b'+' | b'-' | b'.' | b'e' | b'E'))
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

    /// Same forms table shape for the f32 (`xsd:float`) variant, including
    /// the single-precision-mantissa guarantee and range extremes.
    const F32_CASES: &[(f32, &str)] = &[
        (1.0, "1.0E0"),
        (5.0, "5.0E0"),
        (33.33, "3.333E1"),
        (0.001, "1.0E-3"),
        (0.0, "0.0E0"),
        (-0.0, "-0.0E0"),
        (-12.5, "-1.25E1"),
        (1e30, "1.0E30"),
        (f32::MAX, "3.4028235E38"),
        (f32::MIN_POSITIVE, "1.1754944E-38"),
        (1e-45, "1.0E-45"), // subnormal minimum
    ];

    #[test]
    fn float_canonical_forms() {
        for &(input, expected) in F32_CASES {
            assert_eq!(
                canonical_xsd_float(input),
                expected,
                "canonical_xsd_float({input:?})"
            );
        }
    }

    #[test]
    fn float_special_values_keep_xsd_spellings() {
        assert_eq!(canonical_xsd_float(f32::NAN), "NaN");
        assert_eq!(canonical_xsd_float(f32::INFINITY), "INF");
        assert_eq!(canonical_xsd_float(f32::NEG_INFINITY), "-INF");
    }

    #[test]
    fn float_canonical_form_round_trips() {
        for &(input, _) in F32_CASES {
            let parsed: f32 = canonical_xsd_float(input).parse().expect("parse back");
            assert_eq!(parsed.to_bits(), input.to_bits(), "round trip of {input:?}");
        }
    }

    #[test]
    fn parse_accepts_the_xsd_lexical_space() {
        let cases: &[(&str, f64)] = &[
            ("1", 1.0),
            ("-1", -1.0),
            ("+1", 1.0),
            ("1.5", 1.5),
            ("-1.5", -1.5),
            (".5", 0.5),
            ("1.", 1.0),
            ("1.5E3", 1500.0),
            ("1.5e3", 1500.0),
            ("1.5E+3", 1500.0),
            ("-1.5E-3", -0.0015),
            ("1E0", 1.0),
            ("0", 0.0),
            ("-0", -0.0),
            ("INF", f64::INFINITY),
            ("+INF", f64::INFINITY),
            ("-INF", f64::NEG_INFINITY),
        ];
        for &(lexical, expected) in cases {
            let parsed = parse_xsd_double(lexical).unwrap_or_else(|| panic!("{lexical} parses"));
            assert_eq!(parsed.to_bits(), expected.to_bits(), "{lexical}");
            let parsed = parse_xsd_float(lexical).unwrap_or_else(|| panic!("{lexical} parses"));
            assert_eq!(parsed.to_bits(), (expected as f32).to_bits(), "{lexical}");
        }
        assert!(parse_xsd_double("NaN").is_some_and(f64::is_nan));
        assert!(parse_xsd_float("NaN").is_some_and(f32::is_nan));
    }

    #[test]
    fn parse_maps_out_of_range_numerals_to_infinity() {
        assert_eq!(parse_xsd_double("1e400"), Some(f64::INFINITY));
        assert_eq!(parse_xsd_double("-1e400"), Some(f64::NEG_INFINITY));
        assert_eq!(parse_xsd_float("3.5e38"), Some(f32::INFINITY));
        assert_eq!(parse_xsd_float("-3.5e38"), Some(f32::NEG_INFINITY));
        // Below the smallest subnormal: rounds to zero, keeping the sign.
        assert_eq!(parse_xsd_double("1e-400").map(f64::to_bits), Some(0));
    }

    #[test]
    fn parse_refuses_spellings_outside_the_lexical_space() {
        for lexical in [
            "inf",
            "Inf",
            "-inf",
            "+inf",
            "infinity",
            "Infinity",
            "-Infinity",
            "INFINITY",
            "nan",
            "NAN",
            "-NaN",
            "+NaN",
            "",
            " 1",
            "1 ",
            " INF",
            "1.5f",
            "0x10",
            "1_000",
            ".",
            "e5",
            "1e",
            "1e+",
            "--1",
            "1.2.3",
            "abc",
        ] {
            assert_eq!(parse_xsd_double(lexical), None, "{lexical:?}");
            assert_eq!(parse_xsd_float(lexical), None, "{lexical:?}");
        }
    }

    #[test]
    fn parse_reads_back_every_canonical_form() {
        for &(input, canonical) in CASES {
            let parsed = parse_xsd_double(canonical).expect("canonical form parses");
            assert_eq!(parsed.to_bits(), input.to_bits(), "{canonical}");
        }
        for special in [f64::INFINITY, f64::NEG_INFINITY] {
            assert_eq!(
                parse_xsd_double(&canonical_xsd_double(special)),
                Some(special)
            );
        }
        assert!(parse_xsd_double(&canonical_xsd_double(f64::NAN)).is_some_and(f64::is_nan));
    }
}
