//! Term-level lexical rules shared by the N-Triples, N-Quads, Turtle and TriG
//! writers: string-literal and IRI escaping, and the grammar checks that decide
//! whether an IRI can be written as a prefixed name or a label as a blank node.
//!
//! Escaping is exposed three ways over one scanner: `escape_*` feeds escaped
//! segments to a callback, `push_*` appends to a `String`, and `write_*` writes
//! to an [`io::Write`]. Unescaped runs are passed through as whole slices, so a
//! clean string costs one copy and no allocation.

use std::convert::Infallible;
use std::io;

const HEX: &[u8; 16] = b"0123456789ABCDEF";

/// Escape `s` as the body of a `"…"` string literal, in canonical N-Triples
/// form (also valid Turtle, N-Quads and TriG): `"` `\` and the control
/// characters with a short escape use it (`\t \b \n \r \f`), every other C0
/// control and DEL is `\uXXXX`, and everything else is written as is.
pub fn escape_string<E>(s: &str, mut put: impl FnMut(&str) -> Result<(), E>) -> Result<(), E> {
    let bytes = s.as_bytes();
    let mut start = 0;
    for (i, &b) in bytes.iter().enumerate() {
        let short: &str = match b {
            b'"' => "\\\"",
            b'\\' => "\\\\",
            b'\t' => "\\t",
            0x08 => "\\b",
            b'\n' => "\\n",
            b'\r' => "\\r",
            0x0C => "\\f",
            0x00..=0x1F | 0x7F => "",
            _ => continue,
        };
        // Every escaped byte is ASCII, so `i` is a char boundary.
        if start < i {
            put(&s[start..i])?;
        }
        if short.is_empty() {
            put(ascii(&uchar(b)))?;
        } else {
            put(short)?;
        }
        start = i + 1;
    }
    if start < bytes.len() {
        put(&s[start..])?;
    }
    Ok(())
}

/// Escape `iri` as the body of an `<…>` IRI reference. Characters the `IRIREF`
/// production forbids (U+0000–U+0020, `<>"{}|^` `` ` `` `\`) and the DEL/C1
/// controls, which no IRI may contain, are written as `\uXXXX`. A stored IRI
/// holding one is not a valid IRI to begin with; the escape keeps the document
/// parseable and reads back as the same IRI (percent-encoding would not).
pub fn escape_iri<E>(iri: &str, mut put: impl FnMut(&str) -> Result<(), E>) -> Result<(), E> {
    let bytes = iri.as_bytes();
    let mut start = 0;
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        // C1 controls are U+0080–U+009F: UTF-8 `C2 80` through `C2 9F`.
        let (width, cp) = match b {
            0x00..=0x20 | b'<' | b'>' | b'"' | b'{' | b'}' | b'|' | b'^' | b'`' | b'\\' | 0x7F => {
                (1, b)
            }
            0xC2 if matches!(bytes.get(i + 1), Some(0x80..=0x9F)) => (2, bytes[i + 1]),
            _ => {
                i += 1;
                continue;
            }
        };
        if start < i {
            put(&iri[start..i])?;
        }
        put(ascii(&uchar(cp)))?;
        i += width;
        start = i;
    }
    if start < bytes.len() {
        put(&iri[start..])?;
    }
    Ok(())
}

/// `\u00XX` for a code point below U+0100.
fn uchar(cp: u8) -> [u8; 6] {
    [
        b'\\',
        b'u',
        b'0',
        b'0',
        HEX[(cp >> 4) as usize],
        HEX[(cp & 0xF) as usize],
    ]
}

fn ascii(bytes: &[u8]) -> &str {
    std::str::from_utf8(bytes).expect("escape sequences are ASCII")
}

fn infallible(r: Result<(), Infallible>) {
    r.unwrap_or_else(|never| match never {});
}

/// Append the escaped body of a string literal (no quotes).
pub fn push_string(out: &mut String, s: &str) {
    infallible(escape_string(s, |seg| {
        out.push_str(seg);
        Ok(())
    }));
}

/// Write the escaped body of a string literal (no quotes).
pub fn write_string<W: io::Write + ?Sized>(w: &mut W, s: &str) -> io::Result<()> {
    escape_string(s, |seg| w.write_all(seg.as_bytes()))
}

/// Append the escaped body of an IRI reference (no angle brackets).
pub fn push_iri(out: &mut String, iri: &str) {
    infallible(escape_iri(iri, |seg| {
        out.push_str(seg);
        Ok(())
    }));
}

/// Write the escaped body of an IRI reference (no angle brackets).
pub fn write_iri<W: io::Write + ?Sized>(w: &mut W, iri: &str) -> io::Result<()> {
    escape_iri(iri, |seg| w.write_all(seg.as_bytes()))
}

/// Append `<iri>`.
pub fn push_iri_ref(out: &mut String, iri: &str) {
    out.push('<');
    push_iri(out, iri);
    out.push('>');
}

/// Write `<iri>`.
pub fn write_iri_ref<W: io::Write + ?Sized>(w: &mut W, iri: &str) -> io::Result<()> {
    w.write_all(b"<")?;
    write_iri(w, iri)?;
    w.write_all(b">")
}

/// `PN_CHARS_BASE` from the Turtle grammar.
fn is_pn_chars_base(c: char) -> bool {
    matches!(c,
        'A'..='Z'
        | 'a'..='z'
        | '\u{00C0}'..='\u{00D6}'
        | '\u{00D8}'..='\u{00F6}'
        | '\u{00F8}'..='\u{02FF}'
        | '\u{0370}'..='\u{037D}'
        | '\u{037F}'..='\u{1FFF}'
        | '\u{200C}'..='\u{200D}'
        | '\u{2070}'..='\u{218F}'
        | '\u{2C00}'..='\u{2FEF}'
        | '\u{3001}'..='\u{D7FF}'
        | '\u{F900}'..='\u{FDCF}'
        | '\u{FDF0}'..='\u{FFFD}'
        | '\u{10000}'..='\u{EFFFF}')
}

/// `PN_CHARS_U`: `PN_CHARS_BASE` or `_`.
fn is_pn_chars_u(c: char) -> bool {
    c == '_' || is_pn_chars_base(c)
}

/// `PN_CHARS`: `PN_CHARS_U`, `-`, digits, and three combining ranges.
fn is_pn_chars(c: char) -> bool {
    is_pn_chars_u(c)
        || matches!(c,
            '-' | '0'..='9' | '\u{00B7}' | '\u{0300}'..='\u{036F}' | '\u{203F}'..='\u{2040}')
}

/// Shared shape of `PN_PREFIX`, `PN_LOCAL` and `BLANK_NODE_LABEL`: a first
/// character, then middle characters that may include `.`, and a last
/// character that may not be `.`.
fn matches_name(s: &str, first: impl Fn(char) -> bool, rest: impl Fn(char) -> bool) -> bool {
    let mut chars = s.chars();
    let Some(c) = chars.next() else {
        return false;
    };
    first(c) && !s.ends_with('.') && chars.all(|c| c == '.' || rest(c))
}

/// Whether `prefix` can name a Turtle prefix (`PN_PREFIX`); the empty prefix
/// (`:local`) is allowed.
pub fn is_pn_prefix(prefix: &str) -> bool {
    prefix.is_empty() || matches_name(prefix, is_pn_chars_base, is_pn_chars)
}

/// Whether `local` can be written, unescaped, as the local part of a prefixed
/// name (`PN_LOCAL`); the empty local name (`ex:`) is allowed.
///
/// `%XX` sequences are allowed, since Turtle keeps them verbatim. Local names
/// that need a `\` escape (`/`, `#`, `?`, `(`, …) are rejected: the caller
/// writes the full `<iri>` instead, which every parser reads the same way.
pub fn is_pn_local(local: &str) -> bool {
    if local.is_empty() {
        return true;
    }
    let bytes = local.as_bytes();
    // `%` must start a `%XX` escape; its hex digits are then ordinary name
    // characters, so validate the percent sequences and treat `%` as a name
    // character below.
    let percents_ok = bytes.iter().enumerate().all(|(i, &b)| {
        b != b'%'
            || (bytes.get(i + 1).is_some_and(u8::is_ascii_hexdigit)
                && bytes.get(i + 2).is_some_and(u8::is_ascii_hexdigit))
    });
    percents_ok
        && matches_name(
            local,
            |c| is_pn_chars_u(c) || c == ':' || c == '%' || c.is_ascii_digit(),
            |c| is_pn_chars(c) || c == ':' || c == '%',
        )
}

/// Whether `label` (without `_:`) is a valid `BLANK_NODE_LABEL`.
pub fn is_blank_node_label(label: &str) -> bool {
    matches_name(
        label,
        |c| is_pn_chars_u(c) || c.is_ascii_digit(),
        is_pn_chars,
    )
}

/// Whether `tag` is a language tag proper: `[a-zA-Z]+ ('-' [a-zA-Z0-9]+)*`,
/// the first subtag letters only and no subtag empty.
pub fn is_lang_tag_body(tag: &str) -> bool {
    tag.split('-').enumerate().all(|(i, part)| {
        !part.is_empty()
            && part.chars().all(|c| c.is_ascii_alphanumeric())
            && (i > 0 || part.chars().all(|c| c.is_ascii_alphabetic()))
    })
}

/// Whether `tag` can follow `@` in Turtle or N-Triples: a [language tag
/// body](is_lang_tag_body), optionally with an RDF 1.2 base direction
/// (`--ltr` / `--rtl`). A `LANGTAG` has no escape form, so a writer given
/// anything else cannot write it without changing what the document says.
pub fn is_lang_tag(tag: &str) -> bool {
    let (body, direction) = match tag.split_once("--") {
        Some((body, direction)) => (body, Some(direction)),
        None => (tag, None),
    };
    is_lang_tag_body(body) && matches!(direction, None | Some("ltr" | "rtl"))
}

/// Whether `lexical` is a Turtle `INTEGER` (so `xsd:integer` can be written bare).
pub fn is_turtle_integer(lexical: &str) -> bool {
    let digits = lexical.strip_prefix(['+', '-']).unwrap_or(lexical);
    !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())
}

/// Whether `lexical` is a Turtle `DECIMAL` (`[+-]?[0-9]*\.[0-9]+`).
pub fn is_turtle_decimal(lexical: &str) -> bool {
    let unsigned = lexical.strip_prefix(['+', '-']).unwrap_or(lexical);
    match unsigned.split_once('.') {
        Some((int, frac)) => {
            int.bytes().all(|b| b.is_ascii_digit())
                && !frac.is_empty()
                && frac.bytes().all(|b| b.is_ascii_digit())
        }
        None => false,
    }
}

/// Whether `lexical` is a Turtle `DOUBLE`: a mantissa with an exponent.
pub fn is_turtle_double(lexical: &str) -> bool {
    let unsigned = lexical.strip_prefix(['+', '-']).unwrap_or(lexical);
    let Some((mantissa, exponent)) = unsigned.split_once(['e', 'E']) else {
        return false;
    };
    let exponent = exponent.strip_prefix(['+', '-']).unwrap_or(exponent);
    let mantissa_ok = match mantissa.split_once('.') {
        Some((int, frac)) => {
            (!int.is_empty() || !frac.is_empty())
                && int.bytes().all(|b| b.is_ascii_digit())
                && frac.bytes().all(|b| b.is_ascii_digit())
        }
        None => !mantissa.is_empty() && mantissa.bytes().all(|b| b.is_ascii_digit()),
    };
    mantissa_ok && !exponent.is_empty() && exponent.bytes().all(|b| b.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn string(s: &str) -> String {
        let mut out = String::new();
        push_string(&mut out, s);
        out
    }

    fn iri(s: &str) -> String {
        let mut out = String::new();
        push_iri(&mut out, s);
        out
    }

    #[test]
    fn strings_use_canonical_escapes() {
        assert_eq!(string("plain café"), "plain café");
        assert_eq!(string(r#"a "b" \c"#), r#"a \"b\" \\c"#);
        assert_eq!(string("\t\u{8}\n\r\u{c}"), r"\t\b\n\r\f");
        assert_eq!(string("\u{0}\u{1f}\u{7f}"), r"\u0000\u001F\u007F");
        // C1 controls and non-ASCII are legal in a string literal.
        assert_eq!(string("\u{85}é😀"), "\u{85}é😀");
    }

    #[test]
    fn io_and_string_forms_agree() {
        let s = "x\"y\u{1}z";
        let mut buf = Vec::new();
        write_string(&mut buf, s).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), string(s));
    }

    #[test]
    fn iris_escape_what_iriref_forbids() {
        assert_eq!(iri("http://ex.org/a#b"), "http://ex.org/a#b");
        assert_eq!(iri("http://ex.org/a b"), r"http://ex.org/a\u0020b");
        assert_eq!(
            iri("http://ex.org/<{|}>^`\\\""),
            r"http://ex.org/\u003C\u007B\u007C\u007D\u003E\u005E\u0060\u005C\u0022"
        );
        assert_eq!(
            iri("http://ex.org/\u{7f}\u{85}"),
            r"http://ex.org/\u007F\u0085"
        );
        // Non-control non-ASCII is left alone, including other C2 sequences.
        assert_eq!(iri("http://ex.org/é\u{a0}"), "http://ex.org/é\u{a0}");
    }

    #[test]
    fn prefixed_name_parts() {
        assert!(is_pn_prefix(""));
        assert!(is_pn_prefix("ex"));
        assert!(is_pn_prefix("ex.v2"));
        assert!(!is_pn_prefix("_ex"));
        assert!(!is_pn_prefix("1ex"));
        assert!(!is_pn_prefix("ex."));

        for ok in ["", "alice", "_a", "1", "a.b", "a-b", "a:b", "a%20b", "é"] {
            assert!(is_pn_local(ok), "{ok:?}");
        }
        for bad in ["-a", ".a", "a.", "a/b", "a#b", "a%2", "a%zz", "a b", "(a)"] {
            assert!(!is_pn_local(bad), "{bad:?}");
        }
    }

    #[test]
    fn lang_tags() {
        for ok in ["en", "en-GB", "zh-Hant-TW", "x-1", "en--ltr", "ar--rtl"] {
            assert!(is_lang_tag(ok), "{ok:?}");
        }
        for bad in [
            "",
            "1en",
            "en-",
            "en_GB",
            "en--up",
            "en--ltr--rtl",
            "en . <urn:x> <urn:p> \"o\" . #",
        ] {
            assert!(!is_lang_tag(bad), "{bad:?}");
        }
    }

    #[test]
    fn blank_node_labels() {
        for ok in ["b0", "0", "fdb-01h", "_x", "a.b"] {
            assert!(is_blank_node_label(ok), "{ok:?}");
        }
        for bad in ["", "-a", ".a", "a.", "a/b", "a:b"] {
            assert!(!is_blank_node_label(bad), "{bad:?}");
        }
    }

    #[test]
    fn numeric_shorthands() {
        assert!(is_turtle_integer("42") && is_turtle_integer("-7") && is_turtle_integer("+0"));
        assert!(!is_turtle_integer("") && !is_turtle_integer("-") && !is_turtle_integer("1.0"));
        assert!(is_turtle_decimal("1.5") && is_turtle_decimal("-.5"));
        assert!(!is_turtle_decimal("1") && !is_turtle_decimal("1.") && !is_turtle_decimal("1.5e3"));
        assert!(is_turtle_double("1.0E6") && is_turtle_double("-2e-3") && is_turtle_double(".5E1"));
        assert!(!is_turtle_double("NaN") && !is_turtle_double("INF") && !is_turtle_double("1.5"));
        assert!(!is_turtle_double(".E1") && !is_turtle_double("1E"));
    }
}
