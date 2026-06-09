//! Streaming JSON serialization primitives shared by the JSON result formatters.
//!
//! These write JSON tokens directly into a reusable `String` buffer, producing
//! output that is **byte-identical** to `serde_json::to_string` (compact mode).
//! The point is to avoid building a `serde_json::Value` DOM per result cell —
//! no per-cell `Map`/`Vec`/`json!` allocation and no second serialization pass.
//!
//! ## Byte-identity contract
//!
//! The streaming formatters keep their DOM (`-> JsonValue`) counterparts as the
//! reference implementation (used for the `JsonValue` API, `pretty` output, and
//! `@json`/vector leaves). Every streaming formatter has a parity test asserting
//! `stream(result) == serde_json::to_string(&dom(result))`. The primitives here
//! carry their own parity tests against `serde_json` so a serde version bump that
//! changes escaping or float formatting is caught immediately.
//!
//! Two facts make the streaming path tractable:
//! - `serde_json` is built with `preserve_order`, so map keys serialize in
//!   *insertion order*. Streaming writes keys in the same order the `json!`
//!   macros list them — no key sorting required.
//! - `serde_json`'s compact integer path is `itoa`, matched by [`push_i64`]; its
//!   float path post-processes `ryu` output (e.g. it renders `1e30` as `1e+30`),
//!   so [`push_f64`] reuses serde's own `CompactFormatter` rather than calling
//!   `ryu` directly.

use serde_json::Value as JsonValue;

use super::Result;

// JSON string escape codes (0 == no escape). Mirrors serde_json's ESCAPE table.
const QU: u8 = 1; // `"`  -> \"
const BS: u8 = 2; // `\`  -> \\
const BB: u8 = 3; // 0x08 -> \b
const TT: u8 = 4; // 0x09 -> \t
const NN: u8 = 5; // 0x0A -> \n
const FF: u8 = 6; // 0x0C -> \f
const RR: u8 = 7; // 0x0D -> \r
const UU: u8 = 8; // other control (< 0x20) -> \u00XX

const HEX: &[u8; 16] = b"0123456789abcdef";

/// Per-byte escape table. Only `"`, `\`, and the C0 control bytes (< 0x20) need
/// escaping; `serde_json` leaves `/`, DEL (0x7F), and all multi-byte UTF-8 bytes
/// (>= 0x80) verbatim.
const ESCAPE: [u8; 256] = build_escape_table();

const fn build_escape_table() -> [u8; 256] {
    let mut t = [0u8; 256];
    // All C0 controls default to the \u00XX long form...
    let mut i = 0usize;
    while i < 0x20 {
        t[i] = UU;
        i += 1;
    }
    // ...except the five with short escapes.
    t[0x08] = BB;
    t[0x09] = TT;
    t[0x0A] = NN;
    t[0x0C] = FF;
    t[0x0D] = RR;
    t[b'"' as usize] = QU;
    t[b'\\' as usize] = BS;
    t
}

/// Append `s` as a quoted, escaped JSON string (including the surrounding
/// quotes), byte-identical to `serde_json::to_string(&Value::String(s))`.
pub(crate) fn push_json_string(out: &mut String, s: &str) {
    out.push('"');
    escape_json_into(out, s);
    out.push('"');
}

/// Append the escaped *contents* of `s` (no surrounding quotes).
///
/// Bulk-copies runs of clean bytes with a single `push_str` and only handles the
/// escape bytes individually — clean strings (the common case: IRIs, ASCII text)
/// hit a single copy. Every escape byte is ASCII, so the `start..i` / `start..`
/// slices always land on `char` boundaries.
pub(crate) fn escape_json_into(out: &mut String, s: &str) {
    let bytes = s.as_bytes();
    let mut start = 0usize;
    for (i, &b) in bytes.iter().enumerate() {
        let esc = ESCAPE[b as usize];
        if esc == 0 {
            continue;
        }
        if start < i {
            out.push_str(&s[start..i]);
        }
        match esc {
            QU => out.push_str("\\\""),
            BS => out.push_str("\\\\"),
            BB => out.push_str("\\b"),
            TT => out.push_str("\\t"),
            NN => out.push_str("\\n"),
            FF => out.push_str("\\f"),
            RR => out.push_str("\\r"),
            // Remaining C0 controls: \u00XX with lowercase hex (serde_json style).
            _ => {
                out.push_str("\\u00");
                out.push(HEX[(b >> 4) as usize] as char);
                out.push(HEX[(b & 0x0F) as usize] as char);
            }
        }
        start = i + 1;
    }
    if start < bytes.len() {
        out.push_str(&s[start..]);
    }
}

/// Append an `i64` as a JSON number (via `itoa`, matching serde_json).
pub(crate) fn push_i64(out: &mut String, n: i64) {
    let mut buf = itoa::Buffer::new();
    out.push_str(buf.format(n));
}

/// Append a **finite** `f64` as a JSON number, byte-identical to serde_json's
/// compact number formatting.
///
/// Delegates to serde_json's own [`CompactFormatter`] rather than calling `ryu`
/// directly: serde post-processes ryu's output (e.g. it renders `1e30` as
/// `1e+30`), so reusing serde's formatter is the only way to stay byte-identical
/// across serde versions. It writes straight into the buffer — no intermediate
/// `Value` or `String`.
///
/// Callers must guarantee `d.is_finite()`. The result formatters render NaN/±INF
/// as the string sentinels `"NaN"`/`"INF"`/`"-INF"` before reaching here, so a
/// non-finite value is a caller bug — `serde_json` would have emitted `null`.
pub(crate) fn push_f64(out: &mut String, d: f64) {
    use serde_json::ser::{CompactFormatter, Formatter};
    debug_assert!(d.is_finite(), "push_f64 requires a finite value");
    // SAFETY: `write_f64` emits only ASCII bytes (digits, '.', 'e', '+', '-'),
    // which are always valid UTF-8, so the String's invariant is preserved.
    let buf = unsafe { out.as_mut_vec() };
    CompactFormatter
        .write_f64(buf, d)
        .expect("writing to an in-memory buffer is infallible");
}

/// Append a boolean literal.
pub(crate) fn push_bool(out: &mut String, b: bool) {
    out.push_str(if b { "true" } else { "false" });
}

/// Append an already-built `serde_json::Value`, byte-identical to
/// `serde_json::to_string(v)`.
///
/// Reserved for the rare "complex leaf" cells — embedding vectors and parsed
/// `@json` values — where reconstructing serde's exact output by hand is not
/// worth the risk. The hot scalar cells (string / uri / int / double / bool)
/// stream directly and never call this.
pub(crate) fn push_value(out: &mut String, v: &JsonValue) -> Result<()> {
    out.push_str(&serde_json::to_string(v)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    /// Escape every string through both paths and require byte-equality with
    /// serde_json's own string serialization.
    fn assert_string_parity(s: &str) {
        let mut got = String::new();
        push_json_string(&mut got, s);
        let want = serde_json::to_string(&Value::String(s.to_string())).unwrap();
        assert_eq!(got, want, "string parity failed for {s:?}");
    }

    #[test]
    fn string_escaping_matches_serde_json() {
        for s in [
            "",
            "plain ascii",
            "quote\"inside",
            "back\\slash",
            "tab\tand\nnewline\r",
            "form\u{000C}feed\u{0008}back",
            "controls\u{0000}\u{0001}\u{001F}",
            "del\u{007F}kept",             // 0x7F is NOT escaped by serde_json
            "slash/not/escaped",           // '/' is NOT escaped
            "unicode é ü 端 😀 \u{1F600}", // multibyte passes through verbatim
            "c1\u{0085}kept",              // C1 control (>= 0x80) passes through
            "mixed \"a\"\t/b\\ é",
        ] {
            assert_string_parity(s);
        }
    }

    #[test]
    fn every_control_byte_matches_serde_json() {
        // Exhaustively cover U+0000..=U+00FF so no control-char branch drifts.
        for cp in 0u32..=0xFF {
            let ch = char::from_u32(cp).unwrap();
            let s: String = ch.to_string();
            assert_string_parity(&s);
        }
    }

    #[test]
    fn i64_matches_serde_json() {
        for n in [0i64, 1, -1, 42, -42, i64::MIN, i64::MAX, 1_000_000_000_000] {
            let mut got = String::new();
            push_i64(&mut got, n);
            assert_eq!(got, serde_json::to_string(&json!(n)).unwrap(), "i64 {n}");
        }
    }

    #[test]
    fn f64_matches_serde_json() {
        for d in [
            0.0_f64,
            -0.0,
            1.0,
            3.0,
            3.13,
            0.1,
            -2.5,
            1e30,
            1e-7,
            123_456_789.0,
            2.5e-300,
            9_007_199_254_740_993.0,
            f64::MIN,
            f64::MAX,
            std::f64::consts::PI,
        ] {
            let mut got = String::new();
            push_f64(&mut got, d);
            // serde_json renders finite f64 via its compact formatter (ryu + post-processing).
            assert_eq!(got, serde_json::to_string(&json!(d)).unwrap(), "f64 {d}");
        }
    }

    #[test]
    fn bool_matches_serde_json() {
        for b in [true, false] {
            let mut got = String::new();
            push_bool(&mut got, b);
            assert_eq!(got, serde_json::to_string(&json!(b)).unwrap());
        }
    }

    #[test]
    fn value_leaf_matches_serde_json() {
        let v = json!({"name": "Alice", "nums": [1.0, 2.5, -3.0], "ok": true, "nested": {"a": 1}});
        let mut got = String::new();
        push_value(&mut got, &v).unwrap();
        assert_eq!(got, serde_json::to_string(&v).unwrap());
    }
}
