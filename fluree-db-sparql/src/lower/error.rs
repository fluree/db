//! Lowering error types.

use crate::span::SourceSpan;
use std::sync::Arc;
use thiserror::Error;

/// Error that can occur during lowering.
#[derive(Debug, Error)]
pub enum LowerError {
    /// Prefix used but not declared
    #[error("Undefined prefix '{prefix}' at position {}", span.start)]
    UndefinedPrefix { prefix: Arc<str>, span: SourceSpan },

    /// IRI namespace not registered in the database
    #[error("Unknown namespace for IRI '{iri}'")]
    UnknownNamespace { iri: String, span: SourceSpan },

    /// Full IRI looks like a prefixed name (e.g., <prefix:local> instead of prefix:local)
    #[error(
        "IRI '<{iri}>' looks like a prefixed name wrapped in angle brackets. \
             Prefixed names should not be wrapped in <...>. \
             Either use the full IRI <{expanded}> or remove the angle brackets: {iri}"
    )]
    MisusedPrefixSyntax {
        iri: String,
        expanded: String,
        span: SourceSpan,
    },

    /// Feature not yet implemented in lowering
    #[error("{feature} lowering is not yet implemented")]
    NotImplemented { feature: String, span: SourceSpan },

    /// Query form not supported (e.g., CONSTRUCT, ASK, DESCRIBE, UPDATE)
    #[error("{form} queries are not yet supported for lowering")]
    UnsupportedQueryForm { form: String, span: SourceSpan },

    /// Invalid decimal literal
    #[error("Invalid decimal literal '{value}'")]
    InvalidDecimal { value: String, span: SourceSpan },

    /// Invalid integer literal
    #[error("Invalid integer literal '{value}'")]
    InvalidInteger { value: String, span: SourceSpan },

    /// Unsupported ORDER BY expression (MVP only supports variables)
    #[error("Expression-based ORDER BY is not yet supported; use a variable")]
    UnsupportedOrderByExpression { span: SourceSpan },

    /// Aggregate without alias (SELECT COUNT(?x) without AS ?var)
    #[error("Aggregate expressions must have an alias (AS ?var)")]
    AggregateWithoutAlias { span: SourceSpan },

    /// Unsupported COUNT(*) - engine requires input variable
    #[error("COUNT(*) is not yet supported; use COUNT(?var) instead")]
    UnsupportedCountStar { span: SourceSpan },

    /// Invalid property path pattern
    #[error("Invalid property path: {message}")]
    InvalidPropertyPath { message: String, span: SourceSpan },

    /// Invalid typed literal (e.g., temporal parse failure)
    #[error("Invalid {datatype} literal '{value}': {reason}")]
    InvalidLiteral {
        value: String,
        datatype: String,
        reason: String,
        span: SourceSpan,
    },
}

impl LowerError {
    /// Create an undefined prefix error.
    pub fn undefined_prefix(prefix: impl Into<Arc<str>>, span: SourceSpan) -> Self {
        Self::UndefinedPrefix {
            prefix: prefix.into(),
            span,
        }
    }

    /// Create an unknown namespace error.
    pub fn unknown_namespace(iri: impl Into<String>, span: SourceSpan) -> Self {
        Self::UnknownNamespace {
            iri: iri.into(),
            span,
        }
    }

    /// Create a misused prefix syntax error (e.g., <prefix:local> instead of prefix:local).
    pub fn misused_prefix_syntax(
        iri: impl Into<String>,
        expanded: impl Into<String>,
        span: SourceSpan,
    ) -> Self {
        Self::MisusedPrefixSyntax {
            iri: iri.into(),
            expanded: expanded.into(),
            span,
        }
    }

    /// Create a not implemented error.
    pub fn not_implemented(feature: impl Into<String>, span: SourceSpan) -> Self {
        Self::NotImplemented {
            feature: feature.into(),
            span,
        }
    }

    /// Create an unsupported query form error.
    pub fn unsupported_form(form: impl Into<String>, span: SourceSpan) -> Self {
        Self::UnsupportedQueryForm {
            form: form.into(),
            span,
        }
    }

    /// Create an invalid decimal error.
    pub fn invalid_decimal(value: impl Into<String>, span: SourceSpan) -> Self {
        Self::InvalidDecimal {
            value: value.into(),
            span,
        }
    }

    /// Create an invalid integer error.
    pub fn invalid_integer(value: impl Into<String>, span: SourceSpan) -> Self {
        Self::InvalidInteger {
            value: value.into(),
            span,
        }
    }

    /// Create an unsupported ORDER BY expression error.
    pub fn unsupported_order_by_expr(span: SourceSpan) -> Self {
        Self::UnsupportedOrderByExpression { span }
    }

    /// Create an aggregate without alias error.
    pub fn aggregate_without_alias(span: SourceSpan) -> Self {
        Self::AggregateWithoutAlias { span }
    }

    /// Create an unsupported COUNT(*) error.
    pub fn unsupported_count_star(span: SourceSpan) -> Self {
        Self::UnsupportedCountStar { span }
    }

    /// Create an invalid property path error.
    pub fn invalid_property_path(message: impl Into<String>, span: SourceSpan) -> Self {
        Self::InvalidPropertyPath {
            message: message.into(),
            span,
        }
    }

    /// Create an invalid typed literal error.
    pub fn invalid_literal(
        value: impl Into<String>,
        datatype: impl Into<String>,
        reason: impl Into<String>,
        span: SourceSpan,
    ) -> Self {
        Self::InvalidLiteral {
            value: value.into(),
            datatype: datatype.into(),
            reason: reason.into(),
            span,
        }
    }

    /// Get the span associated with this error.
    pub fn span(&self) -> SourceSpan {
        match self {
            Self::UndefinedPrefix { span, .. } => *span,
            Self::UnknownNamespace { span, .. } => *span,
            Self::MisusedPrefixSyntax { span, .. } => *span,
            Self::NotImplemented { span, .. } => *span,
            Self::UnsupportedQueryForm { span, .. } => *span,
            Self::InvalidDecimal { span, .. } => *span,
            Self::InvalidInteger { span, .. } => *span,
            Self::UnsupportedOrderByExpression { span } => *span,
            Self::AggregateWithoutAlias { span } => *span,
            Self::UnsupportedCountStar { span } => *span,
            Self::InvalidPropertyPath { span, .. } => *span,
            Self::InvalidLiteral { span, .. } => *span,
        }
    }
}

/// Result type for lowering operations.
pub type Result<T> = std::result::Result<T, LowerError>;
