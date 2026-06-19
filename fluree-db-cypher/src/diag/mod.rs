//! Diagnostic types for Cypher parsing errors and warnings.

use crate::span::SourceSpan;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Error,
    Warning,
    Note,
}

impl Severity {
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

/// Stable error codes for Cypher diagnostics.
///
/// - `C0xx`: Syntax errors (parser/lex level)
/// - `C1xx`: Deferred-feature rejections (in spec, not in v1)
/// - `C2xx`: Lowering errors
/// - `C3xx`: Semantic errors (e.g. bare `MATCH (n)` rejected)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DiagCode {
    // Lex / parse
    UnexpectedToken,
    UnterminatedString,
    InvalidEscape,
    InvalidNumber,
    InvalidIdentifier,
    UnexpectedEof,

    // Deferred features
    UnsupportedFeature,
    DeferredVariableLengthPath,
    DeferredCollect,
    DeferredPathValue,
    DeferredUndirectedRelationship,
    DeferredRelationshipMerge,
    DeferredMultiStatement,
    DeferredProcedure,
    DeferredShortestPath,
    DeferredLoadCsv,
    DeferredForeach,
    DeferredSchemaDdl,
    DeferredFunction,

    // Semantic
    BareNodePatternRejected,
    BareCreatePatternRejected,
    InvalidIriMapping,
    ReservedPredicate,
    PropertyPathInAnnotation,
    MissingRelType,

    // Lowering
    LowerError,
}

impl DiagCode {
    pub fn as_str(&self) -> &'static str {
        match self {
            DiagCode::UnexpectedToken => "C001",
            DiagCode::UnterminatedString => "C002",
            DiagCode::InvalidEscape => "C003",
            DiagCode::InvalidNumber => "C004",
            DiagCode::InvalidIdentifier => "C005",
            DiagCode::UnexpectedEof => "C006",

            DiagCode::UnsupportedFeature => "C100",
            DiagCode::DeferredVariableLengthPath => "C101",
            DiagCode::DeferredCollect => "C102",
            DiagCode::DeferredPathValue => "C103",
            DiagCode::DeferredUndirectedRelationship => "C104",
            DiagCode::DeferredRelationshipMerge => "C105",
            DiagCode::DeferredMultiStatement => "C106",
            DiagCode::DeferredProcedure => "C107",
            DiagCode::DeferredShortestPath => "C108",
            DiagCode::DeferredLoadCsv => "C109",
            DiagCode::DeferredForeach => "C110",
            DiagCode::DeferredSchemaDdl => "C111",
            DiagCode::DeferredFunction => "C112",

            DiagCode::BareNodePatternRejected => "C300",
            DiagCode::BareCreatePatternRejected => "C301",
            DiagCode::InvalidIriMapping => "C302",
            DiagCode::ReservedPredicate => "C303",
            DiagCode::PropertyPathInAnnotation => "C304",
            DiagCode::MissingRelType => "C305",

            DiagCode::LowerError => "C200",
        }
    }
}

impl std::fmt::Display for DiagCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A diagnostic message produced by the Cypher front-end.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Diagnostic {
    pub code: DiagCode,
    pub severity: Severity,
    pub message: String,
    pub span: SourceSpan,
    pub help: Option<String>,
}

impl Diagnostic {
    pub fn error(code: DiagCode, message: impl Into<String>, span: SourceSpan) -> Self {
        Self {
            code,
            severity: Severity::Error,
            message: message.into(),
            span,
            help: None,
        }
    }

    pub fn with_help(mut self, help: impl Into<String>) -> Self {
        self.help = Some(help.into());
        self
    }

    pub fn is_error(&self) -> bool {
        self.severity.is_error()
    }
}

/// Output of parsing — an optional AST plus a vector of diagnostics.
#[derive(Clone, Debug, Default)]
pub struct ParseOutput {
    pub ast: Option<crate::ast::CypherAst>,
    pub diagnostics: Vec<Diagnostic>,
}

impl ParseOutput {
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(Diagnostic::is_error)
    }
}
