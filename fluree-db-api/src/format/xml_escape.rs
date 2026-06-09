//! Shared XML escape helpers for SPARQL Results XML and RDF/XML formatters.
//!
//! XML 1.0 only allows U+0009, U+000A, U+000D, and U+0020+ in character data.
//! All other control characters (U+0000–U+001F, U+007F, U+0080–U+009F minus
//! the three allowed) are silently stripped to produce valid XML output.

/// Escape text content for XML elements (`&`, `<`, `>`).
///
/// Forbidden XML 1.0 control characters are silently stripped.
pub fn escape_text_into(input: &str, out: &mut String) {
    escape_into(input, out, false);
}

/// Escape attribute values for XML (`&`, `<`, `>`, `"`, `'`).
///
/// Forbidden XML 1.0 control characters are silently stripped.
pub fn escape_attr_into(input: &str, out: &mut String) {
    escape_into(input, out, true);
}

/// Byte-scan escaper shared by text and attribute escaping.
///
/// Runs of bytes that need neither escaping nor stripping are bulk-copied with a
/// single `push_str` rather than decoded and re-pushed char-by-char. Every UTF-8
/// continuation/lead byte (`>= 0x80`) falls into that common path, so multibyte
/// characters are copied verbatim without per-char decoding — only the handful of
/// ASCII metacharacters and the forbidden control points are handled specially.
///
/// The set of stripped code points exactly matches the previous `is_xml_char`
/// gate (XML 1.0 §2.2 `#x9 | #xA | #xD | [#x20-#xD7FF] | [#xE000-#xFFFD] |
/// [#x10000-#x10FFFF]`):
/// - C0 controls other than tab/LF/CR (`0x00-0x08`, `0x0B`, `0x0C`, `0x0E-0x1F`).
/// - The non-characters U+FFFE / U+FFFF (`EF BF BE` / `EF BF BF`) — the only
///   non-ASCII scalar values the production rejects. (C1 controls U+0080-U+009F
///   and DEL U+007F are *valid* in XML 1.0 and are kept verbatim.)
#[inline]
fn escape_into(input: &str, out: &mut String, escape_quotes: bool) {
    let bytes = input.as_bytes();
    let len = bytes.len();
    out.reserve(len);
    let mut clean_start = 0usize;
    let mut i = 0usize;
    while i < len {
        // `(advance, replacement)`: how many bytes the special run consumes and
        // what to emit in its place (empty string = strip).
        let (advance, replacement): (usize, &str) = match bytes[i] {
            b'&' => (1, "&amp;"),
            b'<' => (1, "&lt;"),
            b'>' => (1, "&gt;"),
            b'"' if escape_quotes => (1, "&quot;"),
            b'\'' if escape_quotes => (1, "&apos;"),
            // C0 controls except tab/LF/CR are forbidden in XML 1.0 → strip.
            0x00..=0x08 | 0x0B | 0x0C | 0x0E..=0x1F => (1, ""),
            // U+FFFE / U+FFFF → strip (valid UTF-8 always has the two
            // continuation bytes, so the bounds check only guards truncation).
            0xEF if i + 2 < len && bytes[i + 1] == 0xBF && matches!(bytes[i + 2], 0xBE | 0xBF) => {
                (3, "")
            }
            // Everything else (incl. all multibyte UTF-8) is copied verbatim.
            _ => {
                i += 1;
                continue;
            }
        };
        out.push_str(&input[clean_start..i]);
        out.push_str(replacement);
        i += advance;
        clean_start = i;
    }
    out.push_str(&input[clean_start..]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_null_byte() {
        let mut out = String::new();
        escape_text_into("hello\0world", &mut out);
        assert_eq!(out, "helloworld");
    }

    #[test]
    fn strips_control_chars_keeps_tab_cr_lf() {
        let mut out = String::new();
        // \x01 stripped, \t \n \r kept
        escape_text_into("a\x01b\tc\nd\re", &mut out);
        assert_eq!(out, "ab\tc\nd\re");
    }

    #[test]
    fn escapes_special_chars() {
        let mut out = String::new();
        escape_text_into("a&b<c>d", &mut out);
        assert_eq!(out, "a&amp;b&lt;c&gt;d");
    }

    #[test]
    fn attr_escapes_quotes() {
        let mut out = String::new();
        escape_attr_into("a\"b'c", &mut out);
        assert_eq!(out, "a&quot;b&apos;c");
    }

    #[test]
    fn keeps_multibyte_and_c1_and_del() {
        // Multibyte (é, €, 𝄞), C1 control (U+0085 NEL), and DEL (U+007F) are all
        // valid XML 1.0 and must pass through unescaped and unstripped.
        let input = "é€𝄞\u{0085}\u{007F}x";
        let mut out = String::new();
        escape_text_into(input, &mut out);
        assert_eq!(out, input);
    }

    #[test]
    fn strips_only_fffe_ffff_noncharacters() {
        // U+FFFE and U+FFFF are the only non-ASCII scalar values XML 1.0 rejects.
        let mut out = String::new();
        escape_text_into("a\u{FFFE}b\u{FFFF}c", &mut out);
        assert_eq!(out, "abc");
        // ...but the adjacent valid non-character U+FFFD (replacement char) stays.
        let mut out2 = String::new();
        escape_text_into("a\u{FFFD}b", &mut out2);
        assert_eq!(out2, "a\u{FFFD}b");
    }

    #[test]
    fn escape_into_matches_legacy_char_scan() {
        // Property check: the byte-scan must agree with the original per-char
        // implementation across a tricky mix of specials, controls, and multibyte.
        fn legacy(input: &str, escape_quotes: bool) -> String {
            fn is_xml_char(ch: char) -> bool {
                matches!(ch,
                    '\u{09}' | '\u{0A}' | '\u{0D}'
                    | '\u{20}'..='\u{D7FF}'
                    | '\u{E000}'..='\u{FFFD}'
                    | '\u{10000}'..='\u{10FFFF}')
            }
            let mut out = String::new();
            for ch in input.chars() {
                match ch {
                    '&' => out.push_str("&amp;"),
                    '<' => out.push_str("&lt;"),
                    '>' => out.push_str("&gt;"),
                    '"' if escape_quotes => out.push_str("&quot;"),
                    '\'' if escape_quotes => out.push_str("&apos;"),
                    _ if is_xml_char(ch) => out.push(ch),
                    _ => {}
                }
            }
            out
        }

        let samples = [
            "plain http://example.org/Path#frag",
            "a&b<c>d\"e'f",
            "tabs\tand\nnewlines\r\u{0B}vtab\u{0C}ff\u{1F}us",
            "null\u{0}byte\u{8}bs",
            "unicode é € 𝄞 \u{0085} \u{007F} \u{00A0}",
            "noncharacter \u{FFFE}\u{FFFF} and \u{FFFD}",
            "",
        ];
        for s in samples {
            for eq in [false, true] {
                let mut got = String::new();
                escape_into(s, &mut got, eq);
                assert_eq!(
                    got,
                    legacy(s, eq),
                    "mismatch for {s:?} (escape_quotes={eq})"
                );
            }
        }
    }
}
