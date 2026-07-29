//! Turtle character class predicates, and the whole-token productions built
//! from them.
//!
//! Based on Turtle grammar character productions (same as SPARQL):
//! - PN_CHARS_BASE, PN_CHARS_U, PN_CHARS
//! - Used for prefixed names and local names
//!
//! # Why these live here
//!
//! Lifted out of `fluree-graph-turtle`'s lexer, which still uses them through
//! a re-export at `lex::chars`. A *reader* decides what it will accept and a
//! *writer* decides what it may emit, and those are the same question asked
//! twice: a writer that emits a label its own reader rejects has produced a
//! file nobody can read. Two hand-rolled approximations of one grammar drift,
//! and the drift is invisible until something downstream cannot parse what we
//! wrote — so there is one transcription, and both sides use it.
//!
//! The whole-token predicates ([`is_blank_node_label`], [`is_pn_local`],
//! [`is_pn_prefix`]) deliberately omit `PLX` — the percent- and
//! backslash-escape forms. They are used to answer "may this be emitted
//! *verbatim*?", and a name needing an escape is one that may not.

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
pub fn is_pn_prefix_start(c: char) -> bool {
    is_pn_chars_base(c)
}

/// Check if a character can start a local name (PN_LOCAL first char).
pub fn is_pn_local_start(c: char) -> bool {
    is_pn_chars_u(c) || c == ':' || c.is_ascii_digit()
}

/// Check if a character is Turtle whitespace.
pub fn is_ws(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\r' | '\n')
}

/// Check if a character can appear in an IRI (unescaped).
///
/// ```text
/// IRIREF ::= '<' ([^#x00-#x20<>"{}|^`\] | UCHAR)* '>'
/// ```
pub fn is_iri_char(c: char) -> bool {
    !matches!(
        c,
        '<' | '>' | '"' | '{' | '}' | '|' | '^' | '`' | '\\' | '\x00'..='\x20'
    )
}

/// Whether `label` may be written verbatim after `_:`.
///
/// ```text
/// BLANK_NODE_LABEL ::= '_:' (PN_CHARS_U | [0-9]) ((PN_CHARS | '.')* PN_CHARS)?
/// ```
///
/// One production shared by Turtle, TriG, N-Triples and N-Quads, so one answer
/// serves every writer.
///
/// The trailing `.` is the case worth naming: `_:ab.` is not this production,
/// and a reader handed it does not fail — it lexes `_:ab` and takes the `.` as
/// the statement terminator, so the node is silently *renamed*. An empty label
/// is likewise not a label.
pub fn is_blank_node_label(label: &str) -> bool {
    let mut chars = label.chars();
    let Some(first) = chars.next() else {
        return false; // '_:' alone is not a label
    };
    if !(is_pn_chars_u(first) || first.is_ascii_digit()) {
        return false;
    }
    // Every later character may be PN_CHARS or '.', and the last may not be
    // '.'. A one-character label lands here with `last == first`, which is
    // already known to be PN_CHARS.
    let mut last = first;
    for c in chars {
        if !(is_pn_chars(c) || c == '.') {
            return false;
        }
        last = c;
    }
    is_pn_chars(last)
}

/// Whether `local` may be written verbatim after `prefix:`.
///
/// ```text
/// PN_LOCAL ::= (PN_CHARS_U | ':' | [0-9] | PLX) ((PN_CHARS | '.' | ':' | PLX)* (PN_CHARS | ':' | PLX))?
/// ```
///
/// The empty local name is accepted: `ex:` is a legal `PNAME_NS` and denotes
/// the namespace IRI itself.
pub fn is_pn_local(local: &str) -> bool {
    let mut chars = local.chars();
    let Some(first) = chars.next() else {
        return true; // a bare `ex:`
    };
    if !is_pn_local_start(first) {
        return false;
    }
    let mut last = first;
    for c in chars {
        if !(is_pn_chars(c) || c == '.' || c == ':') {
            return false;
        }
        last = c;
    }
    is_pn_chars(last) || last == ':'
}

/// Whether `prefix` may be written verbatim before the `:` of a prefixed name.
///
/// ```text
/// PNAME_NS  ::= PN_PREFIX? ':'
/// PN_PREFIX ::= PN_CHARS_BASE ((PN_CHARS | '.')* PN_CHARS)?
/// ```
///
/// The empty prefix is accepted: `@prefix : <…>` is legal.
pub fn is_pn_prefix(prefix: &str) -> bool {
    let mut chars = prefix.chars();
    let Some(first) = chars.next() else {
        return true; // `@prefix : <…> .`
    };
    if !is_pn_chars_base(first) {
        return false;
    }
    let mut last = first;
    for c in chars {
        if !(is_pn_chars(c) || c == '.') {
            return false;
        }
        last = c;
    }
    is_pn_chars(last)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blank_node_labels_follow_the_shared_production() {
        for legal in ["a", "b1", "0", "_x", "a.b", "a-b", "x\u{B7}y", "\u{C0}x"] {
            assert!(is_blank_node_label(legal), "{legal:?} is legal");
        }
        for illegal in [
            "",         // '_:' alone
            "ab.",      // may not end in '.' — lexes as `_:ab` plus a terminator
            ".a",       // may not begin with '.'
            "-a",       // '-' is PN_CHARS but not PN_CHARS_U
            "\u{B7}a",  // MIDDLE DOT: PN_CHARS, not PN_CHARS_BASE, so not first
            "\u{300}a", // COMBINING GRAVE: same
            "a b", "a\"b", "a\\b", "a\nb", "a#b", "a,b", "a;b", "a(b",
        ] {
            assert!(
                !is_blank_node_label(illegal),
                "{illegal:?} must be rejected"
            );
        }
    }

    /// The gaps between PN_CHARS_BASE's ranges. Every one of these is
    /// non-ASCII, which a first-character-only ASCII check waved through.
    #[test]
    fn the_gaps_between_the_pn_chars_base_ranges_are_not_legal_first_chars() {
        for (cp, what) in [
            ('\u{D7}', "MULTIPLICATION SIGN, between C0-D6 and D8-F6"),
            ('\u{F7}', "DIVISION SIGN, between D8-F6 and F8-2FF"),
            ('\u{37E}', "GREEK QUESTION MARK, between 370-37D and 37F"),
            ('\u{2000}', "EN QUAD, outside every range"),
            ('\u{2041}', "just past the 203F-2040 PN_CHARS range"),
            ('\u{FFFE}', "just past FDF0-FFFD"),
            ('\u{F0000}', "just past 10000-EFFFF"),
        ] {
            assert!(!is_pn_chars_base(cp), "{cp:?} ({what})");
            assert!(
                !is_blank_node_label(&format!("{cp}x")),
                "{cp:?} ({what}) must not start a label"
            );
        }
    }

    #[test]
    fn local_names_and_prefixes_follow_their_productions() {
        assert!(is_pn_local(""), "a bare `ex:` is legal");
        for legal in ["a", "0", "_x", ":x", "a:b", "a.b", "a-b", "x-", "a:"] {
            assert!(is_pn_local(legal), "{legal:?} is a legal PN_LOCAL");
        }
        for illegal in ["-a", "a.", ".a", "a b", "a%b", "a#b"] {
            assert!(!is_pn_local(illegal), "{illegal:?} must be rejected");
        }

        assert!(is_pn_prefix(""), "the empty prefix is legal");
        for legal in ["ex", "a1", "a-b", "a.b", "\u{C0}x"] {
            assert!(is_pn_prefix(legal), "{legal:?} is a legal PN_PREFIX");
        }
        // Unlike PN_LOCAL, a prefix may not start with '_', a digit or ':'.
        for illegal in ["_x", "1st", ":x", "-x", "a.", "has space"] {
            assert!(!is_pn_prefix(illegal), "{illegal:?} must be rejected");
        }
    }
}
