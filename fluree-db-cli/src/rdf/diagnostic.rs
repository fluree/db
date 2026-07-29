//! Turning a parse failure into something a person can act on.
//!
//! [`fluree_graph_turtle::TurtleError`] reports a byte offset. A byte offset
//! is the right thing to carry through a parser and the wrong thing to show a
//! user, so this module resolves it against the source: line, column, the
//! offending line, and a caret under the spot.
//!
//! # Seam
//!
//! This is a local, minimal shape on purpose. A richer `Diagnostic` — with
//! severities, notes, and a `LineIndex` that resolves offsets in O(log n)
//! instead of by scanning — is being built alongside the quad IR. When it
//! lands, [`Diagnostic`] becomes a re-export and [`from_turtle_error`] becomes
//! a `From` impl; the renderers below and both `check` output formats keep
//! working against the same field names, which is why those names were chosen
//! to match. Nothing outside this module reads the offset directly.

use fluree_graph_turtle::TurtleError;
use serde::Serialize;

/// One problem found in a document.
#[derive(Clone, Debug, Serialize)]
pub struct Diagnostic {
    /// Always `"error"` today — the parser stops at the first problem, so
    /// there is nothing yet that is merely a warning. The field exists so the
    /// JSON shape does not change when `--continue-on-error` starts producing
    /// recovered statements.
    pub severity: &'static str,
    /// Byte offset into the decoded document, when the error carries one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<usize>,
    /// 1-based line.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line: Option<usize>,
    /// 1-based column, counted in characters rather than bytes so it lines up
    /// with what a terminal shows.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column: Option<usize>,
    /// The parser's message, without the position prefix it formats in.
    pub message: String,
    /// The source line the offset falls on.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snippet: Option<String>,
}

impl Diagnostic {
    /// A caret line aligned under [`Self::column`], for terminal output.
    ///
    /// Whitespace from the snippet is copied verbatim so that a line indented
    /// with tabs still lines up: replacing a tab with a space would shift the
    /// caret by however wide the terminal renders tabs.
    pub fn caret(&self) -> Option<String> {
        let (snippet, column) = (self.snippet.as_ref()?, self.column?);
        let mut out = String::new();
        for ch in snippet.chars().take(column.saturating_sub(1)) {
            out.push(if ch == '\t' { '\t' } else { ' ' });
        }
        out.push('^');
        Some(out)
    }
}

/// Resolve a parse failure against the source it came from.
///
/// Errors with no position (an undefined prefix, an unresolvable relative
/// IRI) still produce a diagnostic — just without the location fields. A
/// message with nowhere to point is still worth printing.
pub fn from_turtle_error(err: &TurtleError, source: &str) -> Diagnostic {
    let (offset, message) = match err {
        TurtleError::Lexer { position, message } | TurtleError::Parse { position, message } => {
            (Some(*position), message.clone())
        }
        other => (None, other.to_string()),
    };

    let Some(offset) = offset else {
        return Diagnostic {
            severity: "error",
            offset: None,
            line: None,
            column: None,
            message,
            snippet: None,
        };
    };

    let (line, column, snippet) = locate(source, offset);
    Diagnostic {
        severity: "error",
        offset: Some(offset),
        line: Some(line),
        column: Some(column),
        message,
        snippet: Some(snippet),
    }
}

/// Resolve a byte offset to (line, column, source line).
///
/// A linear scan. Fine for one diagnostic on a document that has already been
/// read into memory; the `LineIndex` noted in the module docs is what makes it
/// cheap once `--continue-on-error` reports thousands.
fn locate(source: &str, offset: usize) -> (usize, usize, String) {
    // An offset at or past the end (unexpected EOF) points at the last line.
    let offset = offset.min(source.len());
    let before = &source[..crate::rdf::floor_char_boundary(source, offset)];

    let line_start = before.rfind('\n').map_or(0, |i| i + 1);
    let line_no = before.bytes().filter(|b| *b == b'\n').count() + 1;
    let column = source[line_start..before.len()].chars().count() + 1;

    let line_end = source[line_start..]
        .find('\n')
        .map_or(source.len(), |i| line_start + i);
    let snippet = source[line_start..line_end]
        .trim_end_matches('\r')
        .to_string();

    (line_no, column, snippet)
}

#[cfg(test)]
mod tests {
    use super::*;

    const DOC: &str = "@prefix ex: <http://example.org/> .\nex:a ex:b \"c\" ;\n  ex:d ?? .\n";

    fn diag_at(offset: usize) -> Diagnostic {
        from_turtle_error(&TurtleError::parse(offset, "unexpected token"), DOC)
    }

    #[test]
    fn an_offset_resolves_to_a_line_a_column_and_the_line_itself() {
        // Offset of the '?' on line 3.
        let offset = DOC.find("??").unwrap();
        let d = diag_at(offset);
        assert_eq!(d.line, Some(3));
        assert_eq!(d.column, Some(8));
        assert_eq!(d.snippet.as_deref(), Some("  ex:d ?? ."));
        assert_eq!(d.offset, Some(offset));
        assert_eq!(d.message, "unexpected token");
        assert_eq!(d.severity, "error");
    }

    #[test]
    fn the_caret_sits_under_the_reported_column() {
        let d = diag_at(DOC.find("??").unwrap());
        let caret = d.caret().unwrap();
        assert_eq!(caret, "       ^");
        // The caret index and the snippet index agree — the property that
        // makes the rendering readable at all.
        assert_eq!(
            d.snippet.unwrap().chars().nth(caret.len() - 1),
            Some('?'),
            "the caret must point at the character the column names"
        );
    }

    #[test]
    fn a_tab_indented_line_keeps_its_tabs_in_the_caret_line() {
        // Replacing tabs with spaces shifts the caret by whatever width the
        // terminal renders a tab at, which is exactly when alignment matters.
        let src = "\t\tex:a ex:b ??  .\n";
        let d = from_turtle_error(&TurtleError::parse(src.find("??").unwrap(), "bad"), src);
        let caret = d.caret().unwrap();
        assert!(caret.starts_with("\t\t"), "{caret:?}");
        assert!(caret.ends_with('^'));
    }

    #[test]
    fn the_first_byte_is_line_one_column_one() {
        let d = diag_at(0);
        assert_eq!((d.line, d.column), (Some(1), Some(1)));
        assert_eq!(d.caret().unwrap(), "^");
    }

    #[test]
    fn an_offset_at_eof_points_at_the_last_line_instead_of_panicking() {
        // Unexpected-EOF errors report an offset at the very end of input.
        let d = diag_at(DOC.len());
        assert!(d.line.is_some());
        assert!(d.caret().is_some());

        let d = diag_at(DOC.len() + 500);
        assert!(
            d.line.is_some(),
            "an out-of-range offset must still resolve"
        );
    }

    #[test]
    fn columns_are_counted_in_characters_not_bytes() {
        // "é" is two bytes. A byte column would put the caret past the token.
        let src = "ex:a ex:b \"ééé\" ??\n";
        let offset = src.find("??").unwrap();
        let d = from_turtle_error(&TurtleError::parse(offset, "bad"), src);
        assert_eq!(
            d.column,
            Some(17),
            "16 characters precede the '?'; a byte column would say 20"
        );
        assert_eq!(
            d.snippet.as_ref().unwrap().chars().nth(16),
            Some('?'),
            "column must index characters"
        );
    }

    #[test]
    fn an_error_with_no_position_still_produces_a_message() {
        let d = from_turtle_error(&TurtleError::UndefinedPrefix("ex".into()), DOC);
        assert_eq!(d.offset, None);
        assert_eq!(d.line, None);
        assert_eq!(d.caret(), None);
        assert!(d.message.contains("ex"), "{}", d.message);
    }

    #[test]
    fn a_carriage_return_is_not_shown_as_part_of_the_line() {
        let src = "ex:a ex:b ??\r\n";
        let d = from_turtle_error(&TurtleError::parse(src.find("??").unwrap(), "bad"), src);
        assert_eq!(d.snippet.as_deref(), Some("ex:a ex:b ??"));
    }

    #[test]
    fn lexer_and_parse_errors_are_treated_identically() {
        let offset = DOC.find("??").unwrap();
        let lexed = from_turtle_error(&TurtleError::lexer(offset, "bad char"), DOC);
        let parsed = from_turtle_error(&TurtleError::parse(offset, "bad char"), DOC);
        assert_eq!(lexed.line, parsed.line);
        assert_eq!(lexed.column, parsed.column);
        assert_eq!(lexed.message, parsed.message);
    }

    #[test]
    fn the_json_shape_omits_absent_location_fields() {
        let d = from_turtle_error(&TurtleError::UndefinedPrefix("ex".into()), DOC);
        let v = serde_json::to_value(&d).unwrap();
        assert!(v.get("line").is_none());
        assert!(v.get("offset").is_none());
        assert!(v.get("snippet").is_none());
        assert_eq!(v["severity"], "error");
    }

    #[test]
    fn an_offset_landing_mid_character_resolves_instead_of_panicking() {
        // Slicing a &str off a UTF-8 boundary panics. A clamped or
        // future-producer offset can land inside the two-byte 'é'.
        let src = "héllo <x> .\n";
        let d = from_turtle_error(&TurtleError::parse(2, "bad"), src);
        assert_eq!(d.line, Some(1));
        assert!(d.caret().is_some());
    }
}
