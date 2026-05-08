//! Diagnostic types for SPARQL parsing errors and warnings.
//!
//! This module provides structured, LLM-friendly diagnostics with:
//! - Stable error codes for programmatic handling
//! - Precise source spans for error locations
//! - Actionable help text with suggested rewrites
//! - JSON serialization for API responses

mod render;

pub use render::{render_diagnostic, render_diagnostics};

use crate::span::SourceSpan;
use serde::{Deserialize, Serialize};

/// Diagnostic severity level.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    /// Unrecoverable error - query cannot be executed
    Error,
    /// Warning - query can execute but may have issues
    Warning,
    /// Informational note
    Note,
}

impl Severity {
    /// Check if this severity is an error.
    pub fn is_error(self) -> bool {
        matches!(self, Severity::Error)
    }
}

impl std::fmt::Display for Severity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Severity::Error => write!(f, "error"),
            Severity::Warning => write!(f, "warning"),
            Severity::Note => write!(f, "note"),
        }
    }
}

/// Stable error codes for diagnostics.
///
/// Organized by category:
/// - `S0xx`: Syntax errors (parser level)
/// - `F0xx`: Fluree restrictions (validator level) - "Fluree doesn't support this"
/// - `R0xx`: Rust port status (lowering level) - "Rust engine not finished"
/// - `W0xx`: Warnings
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum DiagCode {
    // =========================================================================
    // Syntax errors (S001-S099)
    // =========================================================================
    /// Expected a specific token
    #[serde(rename = "S001")]
    ExpectedToken,

    /// String literal not terminated
    #[serde(rename = "S002")]
    UnterminatedString,

    /// Invalid numeric literal
    #[serde(rename = "S003")]
    InvalidNumericLiteral,

    /// Invalid IRI syntax
    #[serde(rename = "S004")]
    InvalidIri,

    /// Unexpected end of input
    #[serde(rename = "S005")]
    UnexpectedEof,

    /// Invalid variable name
    #[serde(rename = "S006")]
    InvalidVariable,

    /// Invalid blank node syntax
    #[serde(rename = "S007")]
    InvalidBlankNode,

    /// Invalid prefixed name
    #[serde(rename = "S008")]
    InvalidPrefixedName,

    /// Undefined prefix
    #[serde(rename = "S009")]
    UndefinedPrefix,

    // =========================================================================
    // Fluree restrictions (F001-F099) - "Fluree doesn't support this"
    // =========================================================================
    /// Property path depth modifiers not supported
    #[serde(rename = "F001")]
    UnsupportedPropertyPathDepth,

    /// Non-predicate property path primary not supported
    #[serde(rename = "F002")]
    UnsupportedPropertyPathPrimary,

    /// USING NAMED not supported in SPARQL Update
    #[serde(rename = "F003")]
    UnsupportedUsingNamed,

    /// Multiple USING clauses not supported
    #[serde(rename = "F004")]
    UnsupportedMultipleUsing,

    /// GRAPH restrictions in SPARQL Update templates
    #[serde(rename = "F005")]
    UnsupportedGraphInUpdate,

    /// Negated property sets not supported
    #[serde(rename = "F006")]
    UnsupportedNegatedPropertySet,

    /// SELECT REDUCED not supported
    #[serde(rename = "F007")]
    UnsupportedSelectReduced,

    /// DISTINCT only supported with COUNT
    #[serde(rename = "F008")]
    UnsupportedDistinctAggregate,

    /// Variables not allowed in INSERT DATA / DELETE DATA
    #[serde(rename = "F009")]
    VariableInGroundData,

    /// Aggregate scope violation: a variable is referenced in a SELECT
    /// projection (or ORDER BY / HAVING) outside of an aggregate function
    /// without appearing in GROUP BY. W3C SPARQL §18.5 forbids this — see
    /// negative-syntax tests `agg08`–`agg12`.
    #[serde(rename = "F010")]
    UngroupedVariableInProjection,

    // =========================================================================
    // Rust port status (R001-R099) - "Rust engine not finished"
    // =========================================================================
    /// Feature lowering not yet implemented in Rust
    #[serde(rename = "R001")]
    LoweringNotImplemented,

    // =========================================================================
    // Warnings (W001-W099)
    // =========================================================================
    /// MINUS may have different semantics than SPARQL spec
    #[serde(rename = "W001")]
    MinusSemanticsPartial,

    /// Variable defined but never used
    #[serde(rename = "W002")]
    UnusedVariable,

    /// Variable used before definition
    #[serde(rename = "W003")]
    VariableUsedBeforeDefinition,
}

impl DiagCode {
    /// Get the string code (e.g., "S001", "F002").
    pub fn code(&self) -> &'static str {
        match self {
            // Syntax
            Self::ExpectedToken => "S001",
            Self::UnterminatedString => "S002",
            Self::InvalidNumericLiteral => "S003",
            Self::InvalidIri => "S004",
            Self::UnexpectedEof => "S005",
            Self::InvalidVariable => "S006",
            Self::InvalidBlankNode => "S007",
            Self::InvalidPrefixedName => "S008",
            Self::UndefinedPrefix => "S009",
            // Fluree
            Self::UnsupportedPropertyPathDepth => "F001",
            Self::UnsupportedPropertyPathPrimary => "F002",
            Self::UnsupportedUsingNamed => "F003",
            Self::UnsupportedMultipleUsing => "F004",
            Self::UnsupportedGraphInUpdate => "F005",
            Self::UnsupportedNegatedPropertySet => "F006",
            Self::UnsupportedSelectReduced => "F007",
            Self::UnsupportedDistinctAggregate => "F008",
            Self::VariableInGroundData => "F009",
            Self::UngroupedVariableInProjection => "F010",
            // Rust port
            Self::LoweringNotImplemented => "R001",
            // Warnings
            Self::MinusSemanticsPartial => "W001",
            Self::UnusedVariable => "W002",
            Self::VariableUsedBeforeDefinition => "W003",
        }
    }

    /// Get the default severity for this code.
    pub fn default_severity(&self) -> Severity {
        match self {
            Self::MinusSemanticsPartial
            | Self::UnusedVariable
            | Self::VariableUsedBeforeDefinition => Severity::Warning,
            _ => Severity::Error,
        }
    }
}

impl std::fmt::Display for DiagCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.code())
    }
}

/// A labeled span within a diagnostic.
///
/// Labels provide additional context about specific locations
/// within the diagnostic's primary span.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Label {
    /// The span this label covers
    pub span: SourceSpan,
    /// The label message
    pub message: String,
}

impl Label {
    /// Create a new label.
    pub fn new(span: impl Into<SourceSpan>, message: impl Into<String>) -> Self {
        Self {
            span: span.into(),
            message: message.into(),
        }
    }
}

/// A diagnostic message from the SPARQL parser.
///
/// Diagnostics are structured to be both human-readable and LLM-friendly,
/// with precise source locations and actionable suggestions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Diagnostic {
    /// Stable error code
    pub code: DiagCode,

    /// Severity level
    pub severity: Severity,

    /// Primary message (one sentence)
    pub message: String,

    /// Primary source span
    pub span: SourceSpan,

    /// Additional labeled spans
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<Label>,

    /// Suggested fix or rewrite
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub help: Option<String>,

    /// Additional context or explanation
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

impl Diagnostic {
    /// Create a new diagnostic with the given code and message.
    pub fn new(code: DiagCode, message: impl Into<String>, span: impl Into<SourceSpan>) -> Self {
        Self {
            severity: code.default_severity(),
            code,
            message: message.into(),
            span: span.into(),
            labels: Vec::new(),
            help: None,
            note: None,
        }
    }

    /// Create an error diagnostic.
    pub fn error(code: DiagCode, message: impl Into<String>, span: impl Into<SourceSpan>) -> Self {
        Self {
            severity: Severity::Error,
            code,
            message: message.into(),
            span: span.into(),
            labels: Vec::new(),
            help: None,
            note: None,
        }
    }

    /// Create a warning diagnostic.
    pub fn warning(
        code: DiagCode,
        message: impl Into<String>,
        span: impl Into<SourceSpan>,
    ) -> Self {
        Self {
            severity: Severity::Warning,
            code,
            message: message.into(),
            span: span.into(),
            labels: Vec::new(),
            help: None,
            note: None,
        }
    }

    /// Add a labeled span.
    pub fn with_label(mut self, label: Label) -> Self {
        self.labels.push(label);
        self
    }

    /// Add help text.
    pub fn with_help(mut self, help: impl Into<String>) -> Self {
        self.help = Some(help.into());
        self
    }

    /// Add a note.
    pub fn with_note(mut self, note: impl Into<String>) -> Self {
        self.note = Some(note.into());
        self
    }

    /// Check if this diagnostic is an error.
    pub fn is_error(&self) -> bool {
        self.severity.is_error()
    }

    /// Check if this diagnostic is a warning.
    pub fn is_warning(&self) -> bool {
        matches!(self.severity, Severity::Warning)
    }
}

/// Result of parsing, including AST and diagnostics.
#[derive(Debug)]
pub struct ParseOutput<T> {
    /// The parsed AST (if parsing succeeded far enough)
    pub ast: Option<T>,
    /// All diagnostics emitted during parsing
    pub diagnostics: Vec<Diagnostic>,
}

impl<T> ParseOutput<T> {
    /// Create a successful parse output.
    pub fn success(ast: T) -> Self {
        Self {
            ast: Some(ast),
            diagnostics: Vec::new(),
        }
    }

    /// Create a parse output with an AST and diagnostics.
    pub fn with_diagnostics(ast: Option<T>, diagnostics: Vec<Diagnostic>) -> Self {
        Self { ast, diagnostics }
    }

    /// Check if there are any errors.
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(Diagnostic::is_error)
    }

    /// Get just the errors.
    pub fn errors(&self) -> impl Iterator<Item = &Diagnostic> {
        self.diagnostics.iter().filter(|d| d.is_error())
    }

    /// Get just the warnings.
    pub fn warnings(&self) -> impl Iterator<Item = &Diagnostic> {
        self.diagnostics.iter().filter(|d| d.is_warning())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_diag_code_string() {
        assert_eq!(DiagCode::ExpectedToken.code(), "S001");
        assert_eq!(DiagCode::UnsupportedPropertyPathDepth.code(), "F001");
        assert_eq!(DiagCode::LoweringNotImplemented.code(), "R001");
        assert_eq!(DiagCode::MinusSemanticsPartial.code(), "W001");
    }

    #[test]
    fn test_diagnostic_builder() {
        let diag = Diagnostic::error(
            DiagCode::UnsupportedPropertyPathDepth,
            "Property path depth modifiers are not supported",
            SourceSpan::new(10, 20),
        )
        .with_label(Label::new(SourceSpan::new(15, 19), "depth modifier here"))
        .with_help("Use `+` without depth bounds")
        .with_note("Fluree supports +, *, ? but not depth bounds");

        assert!(diag.is_error());
        assert_eq!(diag.labels.len(), 1);
        assert!(diag.help.is_some());
        assert!(diag.note.is_some());
    }

    #[test]
    fn test_diagnostic_json() {
        let diag = Diagnostic::error(
            DiagCode::ExpectedToken,
            "Expected 'WHERE'",
            SourceSpan::new(10, 15),
        );

        let json = serde_json::to_string(&diag).unwrap();
        assert!(json.contains("\"code\":\"S001\""));
        assert!(json.contains("\"severity\":\"error\""));
    }

    #[test]
    fn test_parse_output() {
        let output: ParseOutput<String> = ParseOutput::with_diagnostics(
            Some("parsed".to_string()),
            vec![
                Diagnostic::error(DiagCode::ExpectedToken, "error", SourceSpan::new(0, 1)),
                Diagnostic::warning(DiagCode::UnusedVariable, "warning", SourceSpan::new(5, 6)),
            ],
        );

        assert!(output.has_errors());
        assert_eq!(output.errors().count(), 1);
        assert_eq!(output.warnings().count(), 1);
    }
}
