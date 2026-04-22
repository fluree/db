//! Shared XML escape helpers for SPARQL Results XML and RDF/XML formatters.
//!
//! XML 1.0 only allows U+0009, U+000A, U+000D, and U+0020+ in character data.
//! All other control characters (U+0000–U+001F, U+007F, U+0080–U+009F minus
//! the three allowed) are silently stripped to produce valid XML output.

/// Returns `true` if `ch` is allowed in XML 1.0 character data.
///
/// Per XML 1.0 §2.2: `#x9 | #xA | #xD | [#x20-#xD7FF] | [#xE000-#xFFFD] | [#x10000-#x10FFFF]`
#[inline]
fn is_xml_char(ch: char) -> bool {
    matches!(ch,
        '\u{09}' | '\u{0A}' | '\u{0D}' |
        '\u{20}'..='\u{D7FF}' |
        '\u{E000}'..='\u{FFFD}' |
        '\u{10000}'..='\u{10FFFF}'
    )
}

/// Escape text content for XML elements (`&`, `<`, `>`).
///
/// Forbidden XML 1.0 control characters are silently stripped.
pub fn escape_text_into(input: &str, out: &mut String) {
    for ch in input.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            _ if is_xml_char(ch) => out.push(ch),
            _ => {} // strip forbidden control characters
        }
    }
}

/// Escape attribute values for XML (`&`, `<`, `>`, `"`, `'`).
///
/// Forbidden XML 1.0 control characters are silently stripped.
pub fn escape_attr_into(input: &str, out: &mut String) {
    for ch in input.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ if is_xml_char(ch) => out.push(ch),
            _ => {} // strip forbidden control characters
        }
    }
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
}
