//! SPARQL character class predicates.
//!
//! Based on SPARQL 1.1 grammar character productions:
//! - PN_CHARS_BASE, PN_CHARS_U, PN_CHARS
//! - Used for prefixed names and local names

/// Check if a character is in PN_CHARS_BASE.
///
/// ```text
/// PN_CHARS_BASE ::= [A-Z] | [a-z] | [#x00C0-#x00D6] | [#x00D8-#x00F6]
///                 | [#x00F8-#x02FF] | [#x0370-#x037D] | [#x037F-#x1FFF]
///                 | [#x200C-#x200D] | [#x2070-#x218F] | [#x2C00-#x2FEF]
///                 | [#x3001-#xD7FF] | [#xF900-#xFDCF] | [#xFDF0-#xFFFD]
///                 | [#x10000-#xEFFFF]
/// ```
pub fn is_pn_chars_base(c: char) -> bool {
    matches!(c,
        'A'..='Z' |
        'a'..='z' |
        '\u{00C0}'..='\u{00D6}' |
        '\u{00D8}'..='\u{00F6}' |
        '\u{00F8}'..='\u{02FF}' |
        '\u{0370}'..='\u{037D}' |
        '\u{037F}'..='\u{1FFF}' |
        '\u{200C}'..='\u{200D}' |
        '\u{2070}'..='\u{218F}' |
        '\u{2C00}'..='\u{2FEF}' |
        '\u{3001}'..='\u{D7FF}' |
        '\u{F900}'..='\u{FDCF}' |
        '\u{FDF0}'..='\u{FFFD}' |
        '\u{10000}'..='\u{EFFFF}'
    )
}

/// Check if a character is in PN_CHARS_U.
///
/// ```text
/// PN_CHARS_U ::= PN_CHARS_BASE | '_'
/// ```
pub fn is_pn_chars_u(c: char) -> bool {
    is_pn_chars_base(c) || c == '_'
}

/// Check if a character is in PN_CHARS.
///
/// ```text
/// PN_CHARS ::= PN_CHARS_U | '-' | [0-9] | #x00B7 | [#x0300-#x036F] | [#x203F-#x2040]
/// ```
pub fn is_pn_chars(c: char) -> bool {
    is_pn_chars_u(c)
        || c == '-'
        || c.is_ascii_digit()
        || c == '\u{00B7}'
        || matches!(c, '\u{0300}'..='\u{036F}' | '\u{203F}'..='\u{2040}')
}

/// Check if a character can start a prefix name (PN_PREFIX first char).
///
/// ```text
/// PN_PREFIX ::= PN_CHARS_BASE ((PN_CHARS | '.')* PN_CHARS)?
/// ```
pub fn is_pn_prefix_start(c: char) -> bool {
    is_pn_chars_base(c)
}

/// Check if a character can start a local name (PN_LOCAL first char).
///
/// ```text
/// PN_LOCAL ::= (PN_CHARS_U | ':' | [0-9] | PLX) ((PN_CHARS | '.' | ':' | PLX)* (PN_CHARS | ':' | PLX))?
/// ```
pub fn is_pn_local_start(c: char) -> bool {
    is_pn_chars_u(c) || c == ':' || c.is_ascii_digit()
    // PLX (percent-encoded or escaped) is handled separately
}

/// Check if a character is SPARQL whitespace.
///
/// ```text
/// WS ::= #x20 | #x9 | #xD | #xA
/// ```
pub fn is_ws(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\r' | '\n')
}

/// Check if a character can appear in an IRI (unescaped).
///
/// IRIREF allows most characters except control chars and a few special ones.
pub fn is_iri_char(c: char) -> bool {
    !matches!(
        c,
        '<' | '>' | '"' | '{' | '}' | '|' | '^' | '`' | '\\' | '\x00'..='\x20'
    )
}

/// Check if a character starts a variable name.
pub fn is_varname_start(c: char) -> bool {
    is_pn_chars_u(c) || c.is_ascii_digit()
}

/// Check if a character continues a variable name.
pub fn is_varname_char(c: char) -> bool {
    is_pn_chars_u(c)
        || c.is_ascii_digit()
        || c == '\u{00B7}'
        || matches!(c, '\u{0300}'..='\u{036F}' | '\u{203F}'..='\u{2040}')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pn_chars_base() {
        assert!(is_pn_chars_base('a'));
        assert!(is_pn_chars_base('Z'));
        assert!(is_pn_chars_base('é')); // \u00E9
        assert!(!is_pn_chars_base('_'));
        assert!(!is_pn_chars_base('0'));
        assert!(!is_pn_chars_base('-'));
    }

    #[test]
    fn test_pn_chars_u() {
        assert!(is_pn_chars_u('a'));
        assert!(is_pn_chars_u('_'));
        assert!(!is_pn_chars_u('-'));
        assert!(!is_pn_chars_u('0'));
    }

    #[test]
    fn test_pn_chars() {
        assert!(is_pn_chars('a'));
        assert!(is_pn_chars('_'));
        assert!(is_pn_chars('-'));
        assert!(is_pn_chars('0'));
        assert!(!is_pn_chars('.'));
        assert!(!is_pn_chars(':'));
    }

    #[test]
    fn test_is_ws() {
        assert!(is_ws(' '));
        assert!(is_ws('\t'));
        assert!(is_ws('\n'));
        assert!(is_ws('\r'));
        assert!(!is_ws('a'));
    }

    #[test]
    fn test_varname() {
        assert!(is_varname_start('x'));
        assert!(is_varname_start('_'));
        assert!(is_varname_start('0'));
        assert!(!is_varname_start('-'));

        assert!(is_varname_char('x'));
        assert!(is_varname_char('_'));
        assert!(is_varname_char('0'));
        assert!(!is_varname_char('-'));
    }
}
