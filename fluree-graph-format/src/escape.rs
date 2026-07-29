//! Escaping primitives shared by every text RDF writer
//!
//! These are the pure half of `fluree-db-api`'s `export.rs`: they take strings
//! and a `Write`, and know nothing about where the strings came from. The
//! store-coupled half (resolving a datatype code or a subject id through
//! `BinaryIndexStore`) stays in db-api, which now calls into here.
//!
//! Turtle, TriG, N-Triples and N-Quads share one escaping grammar for string
//! literals and one for IRIs, so one implementation serves all four.

use std::io::{self, Write};

/// Write an N-Triples-escaped string to `w` (without the surrounding quotes).
///
/// Escapes `\`, `"`, `\n`, `\r`, `\t` as `ECHAR`, and every other control
/// character (U+0000..U+001F, U+007F..U+009F) as `\uXXXX`.
///
/// Turtle and TriG share this grammar for quoted literals, so the same
/// function serves all four text syntaxes.
///
/// Only the four-hex `UCHAR` form appears, because `char::is_control` is the
/// Unicode `Cc` category and every one of those code points is at most
/// U+009F. An eight-hex branch here would be unreachable.
pub fn write_escaped_ntriples_string<W: Write>(w: &mut W, s: &str) -> io::Result<()> {
    for ch in s.chars() {
        match ch {
            '\\' => w.write_all(b"\\\\")?,
            '"' => w.write_all(b"\\\"")?,
            '\n' => w.write_all(b"\\n")?,
            '\r' => w.write_all(b"\\r")?,
            '\t' => w.write_all(b"\\t")?,
            c if c.is_control() => w.write_all(&uchar(c as u32))?,
            c => {
                let mut buf = [0u8; 4];
                let encoded = c.encode_utf8(&mut buf);
                w.write_all(encoded.as_bytes())?;
            }
        }
    }
    Ok(())
}

/// Write an IRI with escaping per the N-Triples/Turtle `IRIREF` grammar.
///
/// ```text
/// IRIREF ::= '<' ([^#x00-#x20<>"{}|^`\] | UCHAR)* '>'
/// ```
///
/// so exactly two things may not appear literally: code points at or below
/// U+0020, and the nine characters `<`, `>`, `"`, `{`, `}`, `|`, `^`,
/// `` ` ``, `\`. Everything else — including U+007F..U+009F, which an earlier
/// version of this function escaped — is legal and is written through
/// untouched.
///
/// # Why `UCHAR` and not percent-encoding
///
/// The grammar offers `UCHAR` precisely so a forbidden character can be
/// *spelled* without changing the IRI it denotes: `<http://ex/a b>` reads
/// back as `http://ex/a b`, the IRI we were given.
///
/// Percent-encoding, which this used to do, is a different operation. It emits
/// `http://ex/a%20b` — a **different IRI**, and one that collides with the
/// distinct IRI a caller may separately hold as the literal text
/// `http://ex/a%20b`. Two resources become one, silently, with no error
/// anywhere. That is a data-loss bug rather than an escaping choice, and it
/// was live: `fluree export` routes every IRI through here.
pub fn write_escaped_iri<W: Write>(w: &mut W, iri: &str) -> io::Result<()> {
    for ch in iri.chars() {
        if is_forbidden_in_iriref(ch) {
            w.write_all(&uchar(ch as u32))?;
        } else {
            let mut buf = [0u8; 4];
            let encoded = ch.encode_utf8(&mut buf);
            w.write_all(encoded.as_bytes())?;
        }
    }
    Ok(())
}

/// [`write_escaped_iri`] into a `String` instead of a writer, for callers
/// that cache the escaped form rather than emitting it once.
pub fn escape_iri_into(out: &mut String, iri: &str) {
    for ch in iri.chars() {
        if is_forbidden_in_iriref(ch) {
            // `uchar` is ASCII by construction, so this is a valid `str`.
            out.push_str(std::str::from_utf8(&uchar(ch as u32)).expect("ASCII"));
        } else {
            out.push(ch);
        }
    }
}

/// The `\uXXXX` spelling of `cp`, as six ASCII bytes.
///
/// Four hex digits always suffice for the callers here: the `IRIREF`
/// forbidden set is entirely ASCII, and `char::is_control` reaches only
/// U+009F. An eight-digit `\UXXXXXXXX` branch would be dead code, so there
/// is not one.
fn uchar(cp: u32) -> [u8; 6] {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    debug_assert!(
        cp <= 0xFFFF,
        "no caller can reach a code point above U+FFFF"
    );
    [
        b'\\',
        b'u',
        HEX[((cp >> 12) & 0xF) as usize],
        HEX[((cp >> 8) & 0xF) as usize],
        HEX[((cp >> 4) & 0xF) as usize],
        HEX[(cp & 0xF) as usize],
    ]
}

/// Whether `ch` must be written as a `UCHAR` to appear inside `<…>`.
///
/// The whole forbidden set is ASCII, which is what lets [`uchar`] be
/// four-digit only.
fn is_forbidden_in_iriref(ch: char) -> bool {
    ch as u32 <= 0x20 || matches!(ch, '<' | '>' | '"' | '{' | '}' | '|' | '^' | '`' | '\\')
}

/// Write `"lexical"^^<datatype_iri>`.
pub fn write_typed_literal<W: Write>(
    w: &mut W,
    lexical: &str,
    datatype_iri: &str,
) -> io::Result<()> {
    w.write_all(b"\"")?;
    write_escaped_ntriples_string(w, lexical)?;
    w.write_all(b"\"^^<")?;
    write_escaped_iri(w, datatype_iri)?;
    w.write_all(b">")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn escaped_string(s: &str) -> String {
        let mut buf = Vec::new();
        write_escaped_ntriples_string(&mut buf, s).unwrap();
        String::from_utf8(buf).unwrap()
    }

    fn escaped_iri(iri: &str) -> String {
        let mut buf = Vec::new();
        write_escaped_iri(&mut buf, iri).unwrap();
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn escapes_the_quoted_literal_grammar() {
        assert_eq!(
            escaped_string("hello \"world\"\nline2\\end"),
            "hello \\\"world\\\"\\nline2\\\\end"
        );
    }

    #[test]
    fn escapes_control_characters_as_short_unicode_escapes() {
        assert_eq!(escaped_string("a\x00b\x1Fc"), "a\\u0000b\\u001Fc");
    }

    #[test]
    fn uchar_escapes_what_iriref_forbids() {
        assert_eq!(
            escaped_iri("http://example.org/foo>bar"),
            "http://example.org/foo\\u003Ebar"
        );
        assert_eq!(
            escaped_iri("http://example.org/a\\b<c\"d"),
            "http://example.org/a\\u005Cb\\u003Cc\\u0022d"
        );
        assert_eq!(
            escaped_iri("http://example.org/a b\tc"),
            "http://example.org/a\\u0020b\\u0009c"
        );
        // Every character the grammar names, and nothing else.
        for ch in ['<', '>', '"', '{', '}', '|', '^', '`', '\\'] {
            let escaped = escaped_iri(&format!("x{ch}y"));
            assert_eq!(escaped, format!("x\\u{:04X}y", ch as u32), "{ch:?}");
        }
    }

    /// U+007F..U+009F are legal in `IRIREF` — the grammar forbids only
    /// `#x00-#x20` plus nine punctuation characters. An earlier version
    /// escaped this range anyway, which was over-eager rather than wrong;
    /// leaving it alone keeps the output closer to the IRI we were handed.
    #[test]
    fn c1_controls_and_high_code_points_are_left_alone() {
        for iri in [
            "http://ex/\u{7F}x",
            "http://ex/\u{85}y",
            "http://ex/\u{9F}z",
            "http://ex/\u{a0}nbsp",
            "http://ex/\u{2028}sep",
            "http://ex/\u{FFFD}",
            "http://ex/\u{10FFFF}",
        ] {
            assert_eq!(escaped_iri(iri), iri, "{:?}", iri.escape_debug());
        }
    }

    /// The property percent-encoding did not have. Escaping must be
    /// **injective**: two distinct IRIs must never come out as one string.
    ///
    /// `http://ex/a b` and `http://ex/a%20b` are different resources. Under
    /// percent-encoding both emitted `http://ex/a%20b` and merged, silently.
    /// Under `UCHAR` the first is spelled `a b` and the second is
    /// untouched, so they stay two.
    #[test]
    fn escaping_is_injective() {
        let a = escaped_iri("http://ex/a b");
        let b = escaped_iri("http://ex/a%20b");
        assert_ne!(a, b, "two distinct IRIs collapsed onto one spelling");
        assert_eq!(a, "http://ex/a\\u0020b");
        assert_eq!(b, "http://ex/a%20b", "a percent sign is not special here");
    }

    /// The string form must agree with the writer form character for
    /// character — they are two spellings of one grammar, and a caller
    /// choosing between them must not be choosing between behaviors.
    #[test]
    fn the_string_and_writer_forms_agree() {
        for iri in [
            "http://example.org/plain",
            "http://example.org/a b|c^d`e{f}g",
            "http://example.org/\u{7F}\u{9F}\u{a0}",
            "http://example.org/unicode/ünïcødé",
        ] {
            let mut into = String::new();
            escape_iri_into(&mut into, iri);
            assert_eq!(into, escaped_iri(iri), "disagreement on {iri}");
        }
    }

    #[test]
    fn typed_literals_escape_both_halves() {
        let mut buf = Vec::new();
        write_typed_literal(&mut buf, "a\"b", "http://example.org/dt>x").unwrap();
        assert_eq!(
            String::from_utf8(buf).unwrap(),
            "\"a\\\"b\"^^<http://example.org/dt\\u003Ex>"
        );
    }

    /// The literal escaper reaches only the four-hex form, because
    /// `char::is_control` stops at U+009F. This is the guard on that claim —
    /// if the predicate ever widens, the missing eight-hex branch becomes a
    /// real gap rather than dead code correctly removed.
    #[test]
    fn every_control_character_fits_the_four_hex_form() {
        for cp in (0..=0x10FFFFu32).filter_map(char::from_u32) {
            if cp.is_control() {
                assert!(
                    cp as u32 <= 0xFFFF,
                    "{cp:?} is a control character above U+FFFF"
                );
            }
        }
    }

    /// Non-ASCII passes through unescaped: `IRIREF` allows it, and
    /// percent-encoding it would change the IRI's identity for no reason.
    #[test]
    fn non_ascii_survives_both_grammars() {
        assert_eq!(escaped_iri("http://例え.jp/α"), "http://例え.jp/α");
        assert_eq!(escaped_string("naïve ☃"), "naïve ☃");
    }
}
