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
/// Escapes `\`, `"`, `\n`, `\r`, `\t`, and control characters
/// (U+0000..U+001F, U+007F..U+009F) via `\uXXXX`.
///
/// Turtle and TriG share this grammar for quoted literals, so the same
/// function serves all four text syntaxes.
pub fn write_escaped_ntriples_string<W: Write>(w: &mut W, s: &str) -> io::Result<()> {
    for ch in s.chars() {
        match ch {
            '\\' => w.write_all(b"\\\\")?,
            '"' => w.write_all(b"\\\"")?,
            '\n' => w.write_all(b"\\n")?,
            '\r' => w.write_all(b"\\r")?,
            '\t' => w.write_all(b"\\t")?,
            c if c.is_control() => {
                let cp = c as u32;
                if cp <= 0xFFFF {
                    write!(w, "\\u{cp:04X}")?;
                } else {
                    write!(w, "\\U{cp:08X}")?;
                }
            }
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
/// Disallowed characters in `IRIREF`:
/// - ASCII control and space: U+0000..=U+0020
/// - DEL + C1 controls: U+007F..=U+009F
/// - Punctuation: `<`, `>`, `"`, `{`, `}`, `|`, `^`, `` ` ``, `\`
///
/// Those are percent-encoded from their UTF-8 bytes, so the output stays
/// syntactically valid RDF even when the IRI handed in is not.
pub fn write_escaped_iri<W: Write>(w: &mut W, iri: &str) -> io::Result<()> {
    for ch in iri.chars() {
        let mut buf = [0u8; 4];
        let encoded = ch.encode_utf8(&mut buf);
        if is_forbidden_in_iriref(ch) {
            for &b in encoded.as_bytes() {
                write!(w, "%{b:02X}")?;
            }
        } else {
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
            let mut buf = [0u8; 4];
            let encoded = ch.encode_utf8(&mut buf);
            for &b in encoded.as_bytes() {
                out.push('%');
                out.push_str(&format!("{b:02X}"));
            }
        } else {
            out.push(ch);
        }
    }
}

/// Whether `ch` must be percent-encoded to appear inside `<…>`.
fn is_forbidden_in_iriref(ch: char) -> bool {
    let cp = ch as u32;
    cp <= 0x20
        || (0x7F..=0x9F).contains(&cp)
        || matches!(ch, '<' | '>' | '"' | '{' | '}' | '|' | '^' | '`' | '\\')
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
    fn percent_encodes_what_iriref_forbids() {
        assert_eq!(
            escaped_iri("http://example.org/foo>bar"),
            "http://example.org/foo%3Ebar"
        );
        assert_eq!(
            escaped_iri("http://example.org/a\\b<c\"d"),
            "http://example.org/a%5Cb%3Cc%22d"
        );
        assert_eq!(
            escaped_iri("http://example.org/a b\tc"),
            "http://example.org/a%20b%09c"
        );
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
            "\"a\\\"b\"^^<http://example.org/dt%3Ex>"
        );
    }

    /// Non-ASCII passes through unescaped: `IRIREF` allows it, and
    /// percent-encoding it would change the IRI's identity for no reason.
    #[test]
    fn non_ascii_survives_both_grammars() {
        assert_eq!(escaped_iri("http://例え.jp/α"), "http://例え.jp/α");
        assert_eq!(escaped_string("naïve ☃"), "naïve ☃");
    }
}
