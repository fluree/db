//! Structured parse diagnostics
//!
//! A [`Diagnostic`] is what a producer reports *about* an input rather than
//! what it returns from it. That distinction is what `--continue-on-error`
//! needs: a run collects many diagnostics and still finishes, so the report
//! has to be a value that can be counted, filtered by severity, grouped by
//! code, and rendered — not a `Result` that ends the parse.

use crate::LineIndex;
use serde::Serialize;

/// How serious a [`Diagnostic`] is.
///
/// Ordered least to most severe, so callers can filter with a comparison
/// (`d.severity >= Severity::Warning`). The three levels mirror what riot
/// reports, which is the vocabulary users of this class of tool already have.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    /// Worth mentioning; the input is fine.
    Info,
    /// Accepted, but suspect — a lossy construct, a deprecated form.
    Warning,
    /// Rejected. The statement (or the document) does not parse.
    Error,
}

impl std::fmt::Display for Severity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Severity::Info => "info",
            Severity::Warning => "warning",
            Severity::Error => "error",
        };
        f.write_str(name)
    }
}

/// One thing a producer has to say about its input, with the position that
/// provoked it.
///
/// # Why `code` is `&'static str`
///
/// Codes name a *closed* set of conditions a producer can report, declared as
/// constants next to the producer that raises them (`"turtle/lex"`,
/// `"turtle/parse"`, …). Making the field `&'static str` states that: a code
/// is chosen from that set, never composed at runtime from user input, so it
/// stays greppable, documentable, and safe to match on in tests and in a
/// caller's severity policy. It also keeps construction on the error path
/// allocation-free apart from the message itself.
///
/// The same choice is why [`Diagnostic`] derives [`Serialize`] but not
/// `Deserialize`: diagnostics are produced and reported (`--profile json`,
/// error listings), never read back in, and a `&'static str` cannot be
/// deserialized into.
///
/// # Positions
///
/// `line` and `col` are 1-based and derived from `byte_span.0` by
/// [`Diagnostic::at`], so they cannot disagree with the span. Some conditions
/// have no position at all (an undefined prefix is a fact about the document,
/// not about one offset); those use [`Diagnostic::new`], which leaves `line`,
/// `col`, and the span at 0 — line 0 is the "no position" marker, since real
/// lines start at 1 — and render without a source-context block.
///
/// # Example
///
/// ```
/// use fluree_graph_ir::{Diagnostic, LineIndex, Severity};
///
/// let source = "ex:s ex:p\nex:o ;\n";
/// let index = LineIndex::new(source);
/// let d = Diagnostic::at(Severity::Error, "turtle/parse", "expected '.'", (15, 16), &index);
///
/// assert_eq!((d.line, d.col), (2, 6));
/// assert_eq!(d.to_string(), "error[turtle/parse]: expected '.'");
/// assert_eq!(
///     d.render(&index),
///     "error[turtle/parse]: expected '.'\n  |\n2 | ex:o ;\n  |      ^"
/// );
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Diagnostic {
    /// How serious this is
    pub severity: Severity,
    /// Stable identifier for the condition, e.g. `"turtle/parse"`
    pub code: &'static str,
    /// 1-based line, or 0 when the diagnostic has no source position
    pub line: u32,
    /// 1-based character column, or 0 when there is no source position
    pub col: u32,
    /// Half-open byte range `[start, end)` in the source. A zero-width span
    /// means "at this offset" — which is all a producer that reports a single
    /// offset can honestly claim.
    pub byte_span: (u32, u32),
    /// What went wrong, as one line. Source context belongs in
    /// [`Diagnostic::render`], not in here.
    pub message: String,
}

impl Diagnostic {
    /// A diagnostic with no source position.
    ///
    /// For conditions that are about the document rather than an offset in
    /// it. Renders without a caret block.
    pub fn new(severity: Severity, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            severity,
            code,
            line: 0,
            col: 0,
            byte_span: (0, 0),
            message: message.into(),
        }
    }

    /// A diagnostic at a byte span, with line and column resolved from the
    /// span's start.
    ///
    /// `index` must be built over the same source the span refers to;
    /// nothing can check that, and a mismatched index yields a confidently
    /// wrong position.
    pub fn at(
        severity: Severity,
        code: &'static str,
        message: impl Into<String>,
        byte_span: (u32, u32),
        index: &LineIndex<'_>,
    ) -> Self {
        let (line, col) = index.line_col(byte_span.0 as usize);
        Self {
            severity,
            code,
            line,
            col,
            byte_span,
            message: message.into(),
        }
    }

    /// Whether this diagnostic points at a place in the source
    pub fn has_position(&self) -> bool {
        self.line != 0
    }

    /// The 1-based (line, column), if there is one
    pub fn position(&self) -> Option<(u32, u32)> {
        self.has_position().then_some((self.line, self.col))
    }

    /// Render with source context: the [`Display`](std::fmt::Display)
    /// headline, then the caret block pointing at the offending column.
    ///
    /// The block comes from [`LineIndex::caret_block`], the same call the
    /// Turtle lexer's own error messages use, so a diagnostic and a raw
    /// lexer error describe a position identically.
    ///
    /// A diagnostic with no position renders as the headline alone.
    pub fn render(&self, index: &LineIndex<'_>) -> String {
        if !self.has_position() {
            return self.to_string();
        }
        format!("{self}\n{}", index.caret_block(self.line, self.col))
    }

    /// [`Diagnostic::render`] against a source string, building a throwaway
    /// index. Prefer `render` when reporting more than one diagnostic over
    /// the same input.
    pub fn render_with_source(&self, source: &str) -> String {
        self.render(&LineIndex::new(source))
    }
}

impl std::fmt::Display for Diagnostic {
    /// The one-line headline: `error[turtle/parse]: expected '.'`.
    ///
    /// Deliberately source-free, so a diagnostic can be printed by a caller
    /// that no longer holds the input. The position lives in the caret block
    /// [`Diagnostic::render`] adds.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}[{}]: {}", self.severity, self.code, self.message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CODE: &str = "test/code";

    #[test]
    fn severity_orders_least_to_most_severe() {
        assert!(Severity::Info < Severity::Warning);
        assert!(Severity::Warning < Severity::Error);

        let mut all = [Severity::Error, Severity::Info, Severity::Warning];
        all.sort();
        assert_eq!(
            all,
            [Severity::Info, Severity::Warning, Severity::Error],
            "callers filter with `>=`, so the order is part of the contract"
        );
        assert_eq!(Severity::Error.to_string(), "error");
        assert_eq!(Severity::Warning.to_string(), "warning");
        assert_eq!(Severity::Info.to_string(), "info");
    }

    #[test]
    fn line_and_column_come_from_the_span_start() {
        let source = "one\ntwo\nthree";
        let index = LineIndex::new(source);
        let d = Diagnostic::at(Severity::Error, CODE, "bad", (5, 8), &index);

        assert_eq!((d.line, d.col), (2, 2));
        assert_eq!(d.byte_span, (5, 8));
        assert_eq!(d.position(), Some((2, 2)));
        assert!(d.has_position());
    }

    #[test]
    fn a_positionless_diagnostic_renders_without_a_caret_block() {
        let d = Diagnostic::new(Severity::Warning, CODE, "no offset for this");

        assert_eq!((d.line, d.col), (0, 0), "line 0 is the no-position marker");
        assert!(!d.has_position());
        assert_eq!(d.position(), None);

        let rendered = d.render_with_source("anything");
        assert_eq!(rendered, "warning[test/code]: no offset for this");
        assert!(!rendered.contains('^'));
    }

    #[test]
    fn rendering_is_the_headline_plus_the_shared_caret_block() {
        let source = "ex:s ex:p\nex:o ;\n";
        let index = LineIndex::new(source);
        let d = Diagnostic::at(Severity::Error, CODE, "expected '.'", (15, 16), &index);

        assert_eq!(d.to_string(), "error[test/code]: expected '.'");
        assert_eq!(
            d.render(&index),
            format!("{d}\n{}", index.caret_block(d.line, d.col)),
            "render must not re-implement the block"
        );
        assert_eq!(
            d.render(&index),
            "error[test/code]: expected '.'\n  |\n2 | ex:o ;\n  |      ^"
        );
        assert_eq!(d.render_with_source(source), d.render(&index));
    }

    #[test]
    fn serializes_for_machine_readable_reports() {
        let index = LineIndex::new("ex:s\n");
        let d = Diagnostic::at(Severity::Error, CODE, "bad", (0, 4), &index);
        let json: serde_json::Value = serde_json::to_value(&d).unwrap();

        assert_eq!(json["severity"], "error");
        assert_eq!(json["code"], CODE);
        assert_eq!(json["line"], 1);
        assert_eq!(json["col"], 1);
        assert_eq!(json["byte_span"], serde_json::json!([0, 4]));
        assert_eq!(json["message"], "bad");
    }
}
