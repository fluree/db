//! Diagnostic rendering for human-readable output.
//!
//! Renders diagnostics in a format similar to Rust compiler errors:
//!
//! ```text
//! error[F001]: Property path depth modifiers are not supported
//!   --> query.sparql:3:10
//!    |
//!  3 |   ?s :p+{2,5} ?o
//!    |          ^^^^^ depth modifier not supported
//!    |
//!    = help: Use `+` without depth bounds
//!    = note: Fluree supports +, *, ? but not depth bounds
//! ```

use crate::diag::{Diagnostic, Severity};
use crate::span::LineIndex;

/// Render a diagnostic to a string.
///
/// # Arguments
///
/// * `diag` - The diagnostic to render
/// * `source` - The source text
/// * `filename` - Optional filename for the source location
pub fn render_diagnostic(diag: &Diagnostic, source: &str, filename: Option<&str>) -> String {
    let index = LineIndex::new(source);
    let mut output = String::new();

    // Header line: severity[code]: message
    let severity_str = match diag.severity {
        Severity::Error => "error",
        Severity::Warning => "warning",
        Severity::Note => "note",
    };
    output.push_str(&format!(
        "{}[{}]: {}\n",
        severity_str,
        diag.code.code(),
        diag.message
    ));

    // Location line: --> filename:line:col
    let start_loc = index.line_col(diag.span.start);
    let file = filename.unwrap_or("<input>");
    output.push_str(&format!(
        "  --> {}:{}:{}\n",
        file, start_loc.line, start_loc.col
    ));

    // Source snippet with underline
    render_source_snippet(&mut output, source, &index, diag);

    // Help text
    if let Some(help) = &diag.help {
        for line in help.lines() {
            output.push_str(&format!("   = help: {line}\n"));
        }
    }

    // Note text
    if let Some(note) = &diag.note {
        for line in note.lines() {
            output.push_str(&format!("   = note: {line}\n"));
        }
    }

    output
}

fn render_source_snippet(output: &mut String, source: &str, index: &LineIndex, diag: &Diagnostic) {
    let start_loc = index.line_col(diag.span.start);
    let end_loc = index.line_col(diag.span.end);

    // Calculate gutter width (for line numbers)
    let max_line = end_loc.line;
    let gutter_width = max_line.to_string().len();

    // Render each line with the span
    for line_num in start_loc.line..=end_loc.line {
        let line_start = index.line_start(line_num).unwrap_or(0);
        let line_end = index.line_end(line_num, source);
        let line_text = &source[line_start..line_end.min(source.len())];
        let line_text = line_text.trim_end_matches('\n');

        // Empty gutter line
        output.push_str(&format!("{:>width$} |\n", "", width = gutter_width));

        // Line with source
        output.push_str(&format!("{line_num:>gutter_width$} | {line_text}\n"));

        // Underline
        let underline_start = if line_num == start_loc.line {
            start_loc.col as usize
        } else {
            1
        };
        let underline_end = if line_num == end_loc.line {
            end_loc.col as usize
        } else {
            line_text.len() + 1
        };

        let padding = " ".repeat(underline_start.saturating_sub(1));
        let underline_len = underline_end.saturating_sub(underline_start).max(1);
        let underline = "^".repeat(underline_len);

        // Find label for this span (if any)
        let label_text = diag
            .labels
            .iter()
            .find(|l| {
                let l_start = index.line_col(l.span.start);
                l_start.line == line_num
            })
            .map(|l| format!(" {}", l.message))
            .unwrap_or_default();

        output.push_str(&format!(
            "{:>width$} | {}{}{}\n",
            "",
            padding,
            underline,
            label_text,
            width = gutter_width
        ));
    }

    // Final empty gutter line
    output.push_str(&format!("{:>width$} |\n", "", width = gutter_width));
}

/// Render multiple diagnostics.
pub fn render_diagnostics(
    diagnostics: &[Diagnostic],
    source: &str,
    filename: Option<&str>,
) -> String {
    diagnostics
        .iter()
        .map(|d| render_diagnostic(d, source, filename))
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diag::{DiagCode, Label};
    use crate::span::SourceSpan;

    #[test]
    fn test_render_simple() {
        let source = "SELECT ?x WHERE { }";
        let diag = Diagnostic::error(
            DiagCode::ExpectedToken,
            "Expected triple pattern",
            SourceSpan::new(18, 19),
        );

        let rendered = render_diagnostic(&diag, source, Some("query.sparql"));
        println!("{rendered}");

        assert!(rendered.contains("error[S001]"));
        assert!(rendered.contains("query.sparql:1:19"));
    }

    #[test]
    fn test_render_with_label() {
        let source = "SELECT * WHERE { ?s :p+{2,5} ?o }";
        let diag = Diagnostic::error(
            DiagCode::UnsupportedPropertyPathDepth,
            "Property path depth modifiers are not supported",
            SourceSpan::new(23, 28),
        )
        .with_label(Label::new(SourceSpan::new(23, 28), "depth modifier here"))
        .with_help("Use `+` without depth bounds")
        .with_note("Fluree supports +, *, ? but not depth bounds");

        let rendered = render_diagnostic(&diag, source, Some("query.sparql"));
        println!("{rendered}");

        assert!(rendered.contains("error[F001]"));
        assert!(rendered.contains("= help:"));
        assert!(rendered.contains("= note:"));
    }

    #[test]
    fn test_render_multiline() {
        let source = "SELECT ?x\nWHERE {\n  ?s ?p ?o\n}";
        let diag = Diagnostic::warning(
            DiagCode::UnusedVariable,
            "Variable ?x is never used in pattern",
            SourceSpan::new(7, 9),
        );

        let rendered = render_diagnostic(&diag, source, None);
        println!("{rendered}");

        assert!(rendered.contains("warning[W002]"));
        assert!(rendered.contains("<input>:1:8"));
    }
}
