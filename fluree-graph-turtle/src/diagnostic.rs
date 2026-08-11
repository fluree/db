//! Turning a [`TurtleError`] into a reportable [`Diagnostic`]
//!
//! [`TurtleError`] is the parser's *control flow* type: it ends a parse and
//! carries a byte offset, which is all the parser can produce cheaply. A
//! [`Diagnostic`] is the *reporting* type: it carries line, column, severity
//! and a stable code, which is what a CLI prints and what
//! `--continue-on-error` collects.
//!
//! This module is the additive bridge between them. `TurtleError`'s shape is
//! deliberately untouched — every caller of the parser depends on it — so the
//! conversion lives here as an inherent method rather than as a change to the
//! error.

use fluree_graph_ir::{Diagnostic, LineIndex, Severity};

use crate::error::TurtleError;

/// Stable diagnostic codes this crate reports.
///
/// Namespaced by producer so a driver mixing several parsers can tell whose
/// diagnostic it is holding, and so a user can filter on `turtle/` as a
/// group. Every code is a constant: the set is closed and greppable, which
/// is the reason [`Diagnostic::code`] is a `&'static str`.
pub mod code {
    /// The input could not be tokenized.
    pub const LEX: &str = "turtle/lex";
    /// The tokens did not form a valid Turtle statement.
    pub const PARSE: &str = "turtle/parse";
    /// A relative IRI could not be resolved (usually: no `@base`).
    pub const IRI_RESOLUTION: &str = "turtle/iri-resolution";
    /// A prefixed name used a prefix that was never declared.
    pub const UNDEFINED_PREFIX: &str = "turtle/undefined-prefix";
    /// A string escape sequence was malformed.
    pub const INVALID_ESCAPE: &str = "turtle/invalid-escape";
    /// The downstream sink refused an event or its writer failed. Not a
    /// defect in the input — the document may be perfectly valid.
    pub const SINK: &str = "turtle/sink";
}

impl TurtleError {
    /// Convert this error into a [`Diagnostic`] positioned in `index`.
    ///
    /// `index` MUST be built over the very string that was parsed: the byte
    /// offsets in [`TurtleError::Lexer`] and [`TurtleError::Parse`] are
    /// offsets into it. Handing in a different source — a different chunk, a
    /// re-read file — produces a confidently wrong line and column, and
    /// nothing can detect that.
    ///
    /// Errors that carry no offset (an undefined prefix, a sink failure)
    /// become positionless diagnostics: `line` and `col` are 0 and rendering
    /// omits the caret block. Inventing offset 0 for them would point the
    /// caret at the first character of the document, which is a lie.
    ///
    /// # Example
    ///
    /// ```
    /// use fluree_graph_ir::{LineIndex, Severity};
    /// use fluree_graph_turtle::{diagnostic::code, tokenize};
    ///
    /// let source = "ex:name \"ok\" .\nex:other $ .";
    /// let err = tokenize(source).unwrap_err();
    ///
    /// let index = LineIndex::new(source);
    /// let d = err.to_diagnostic(&index);
    ///
    /// assert_eq!(d.severity, Severity::Error);
    /// assert_eq!(d.code, code::LEX);
    /// assert_eq!((d.line, d.col), (2, 10));
    /// assert_eq!(
    ///     d.render(&index),
    ///     "error[turtle/lex]: unexpected character '$' at line 2, column 10\n  \
    ///      |\n2 | ex:other $ .\n  |          ^"
    /// );
    /// ```
    pub fn to_diagnostic(&self, index: &LineIndex<'_>) -> Diagnostic {
        match self {
            // The lexer's message is already a headline plus a caret block
            // (see `make_lex_error`). Take the headline: the block is the
            // renderer's job, and keeping it here would render it twice.
            TurtleError::Lexer { position, message } => Diagnostic::at(
                Severity::Error,
                code::LEX,
                headline(message),
                at_offset(*position),
                index,
            ),
            TurtleError::Parse { position, message } => Diagnostic::at(
                Severity::Error,
                code::PARSE,
                message.clone(),
                at_offset(*position),
                index,
            ),
            TurtleError::IriResolution(_) => {
                Diagnostic::new(Severity::Error, code::IRI_RESOLUTION, self.to_string())
            }
            TurtleError::UndefinedPrefix(_) => {
                Diagnostic::new(Severity::Error, code::UNDEFINED_PREFIX, self.to_string())
            }
            TurtleError::InvalidEscape(_) => {
                Diagnostic::new(Severity::Error, code::INVALID_ESCAPE, self.to_string())
            }
            TurtleError::Sink(_) => Diagnostic::new(Severity::Error, code::SINK, self.to_string()),
        }
    }

    /// [`TurtleError::to_diagnostic`] against a source string, building a
    /// throwaway index. Prefer the index-taking form when reporting more than
    /// one error over the same input.
    pub fn to_diagnostic_for(&self, source: &str) -> Diagnostic {
        self.to_diagnostic(&LineIndex::new(source))
    }
}

/// A zero-width span at `position` — all a producer that reports one offset
/// can honestly claim about the extent of the problem.
fn at_offset(position: usize) -> (u32, u32) {
    let start = u32::try_from(position).unwrap_or(u32::MAX);
    (start, start)
}

/// The first line of a message, which for the lexer's caret messages is the
/// headline and for everything else is the whole thing.
fn headline(message: &str) -> &str {
    message.split_once('\n').map_or(message, |(first, _)| first)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lex::tokenize;
    use crate::parser::parse;
    use fluree_graph_ir::GraphCollectorSink;

    fn parse_err(source: &str) -> TurtleError {
        let mut sink = GraphCollectorSink::new();
        parse(source, &mut sink).expect_err("input must not parse")
    }

    #[test]
    fn a_lex_error_keeps_its_headline_and_gains_a_position() {
        let source = "ex:name \"ok\" .\nex:other $ .";
        let err = tokenize(source).expect_err("input must not lex");
        let index = LineIndex::new(source);
        let d = err.to_diagnostic(&index);

        assert_eq!(d.severity, Severity::Error);
        assert_eq!(d.code, code::LEX);
        assert_eq!((d.line, d.col), (2, 10));
        assert_eq!(
            d.message, "unexpected character '$' at line 2, column 10",
            "the caret block belongs to the renderer, not the message"
        );
        assert!(!d.message.contains('\n'));
        assert_eq!(d.byte_span, (24, 24));
    }

    #[test]
    fn rendering_a_lex_diagnostic_reproduces_the_lexer_s_own_caret_block() {
        // The compatibility claim, checked rather than asserted: the block
        // under a rendered diagnostic is byte-for-byte the block the raw
        // lexer error carries.
        let source = "ex:a \"héllö\" § .";
        let err = tokenize(source).expect_err("input must not lex");
        let TurtleError::Lexer { message, .. } = &err else {
            panic!("expected a lexer error, got {err:?}");
        };
        let (raw_headline, raw_block) = message.split_once('\n').expect("headline then block");

        let index = LineIndex::new(source);
        let rendered = err.to_diagnostic(&index).render(&index);
        let (rendered_headline, rendered_block) =
            rendered.split_once('\n').expect("headline then block");

        assert_eq!(rendered_block, raw_block);
        assert_eq!(
            rendered_headline,
            format!("error[turtle/lex]: {raw_headline}")
        );
    }

    #[test]
    fn a_parse_error_is_positioned_and_keeps_its_message_verbatim() {
        let source = "@prefix ex: <http://example.org/> .\nex:s ex:p ";
        let err = parse_err(source);
        let TurtleError::Parse { position, message } = &err else {
            panic!("expected a parse error, got {err:?}");
        };
        let (position, message) = (*position, message.clone());

        let index = LineIndex::new(source);
        let d = err.to_diagnostic(&index);

        assert_eq!(d.code, code::PARSE);
        assert_eq!(d.message, message, "parse messages carry no caret block");
        assert_eq!(d.byte_span, (position as u32, position as u32));
        assert_eq!(d.position(), Some(index.line_col(position)));
    }

    #[test]
    fn an_undefined_prefix_reports_without_pretending_to_have_a_position() {
        let source = "nope:s nope:p \"o\" .";
        let err = parse_err(source);
        assert!(
            matches!(err, TurtleError::UndefinedPrefix(_)),
            "got {err:?}"
        );

        let d = err.to_diagnostic_for(source);
        assert_eq!(d.code, code::UNDEFINED_PREFIX);
        assert!(!d.has_position(), "offset 0 would point at the wrong place");
        assert_eq!(d.message, err.to_string());
        assert_eq!(
            d.render_with_source(source),
            "error[turtle/undefined-prefix]: Undefined prefix: nope"
        );
    }

    #[test]
    fn a_sink_failure_is_reported_as_the_sink_s_fault_not_the_document_s() {
        let err = TurtleError::Sink(fluree_graph_ir::SinkError::rejected("pipe closed"));
        let d = err.to_diagnostic_for("ex:s ex:p \"o\" .");

        assert_eq!(d.code, code::SINK);
        assert!(!d.has_position());
        assert!(d.message.contains("pipe closed"), "{}", d.message);
    }

    #[test]
    fn every_offset_carrying_variant_lands_inside_the_source() {
        // A diagnostic whose position falls outside the input it claims to
        // describe is worse than none; the index clamps, and these are the
        // inputs that would expose a mismatch.
        for source in [
            "ex:s ex:p $ .",
            "@prefix ex: <http://example.org/> .\nex:s ex:p ",
            "ex:s ex:p \"unterminated",
            "",
        ] {
            let mut sink = GraphCollectorSink::new();
            let Err(err) = parse(source, &mut sink) else {
                continue;
            };
            let index = LineIndex::new(source);
            let d = err.to_diagnostic(&index);
            if !d.has_position() {
                continue;
            }
            assert!(
                d.line >= 1 && d.line <= index.line_count(),
                "{source:?}: {d}"
            );
            assert!(d.col >= 1, "{source:?}: {d}");
            assert!(
                d.byte_span.0 as usize <= source.len(),
                "{source:?}: span {:?} past end",
                d.byte_span
            );
        }
    }
}
