//! Structured diagnostics surfaced by the deterministic emitter.
//!
//! Diagnostics are the emitter's honest account of every decision it could NOT
//! make from metadata alone: skipped nested columns, unverifiable subject keys,
//! FK candidates it refused to fabricate, and perf advisories. They are part of
//! the wire contract (PR-2 persists them, PR-3 surfaces them, PR-4 resolves the
//! `UnresolvedFkCandidate`s).

use serde::{Deserialize, Serialize};

/// Severity of a [`Diagnostic`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Severity {
    /// A blocking problem (e.g. no safe subject key → the table emits no subject).
    Error,
    /// A non-blocking concern the reviewer should see.
    Warning,
    /// A performance advisory (the mapping is correct but may be costly).
    Advisory,
}

/// Machine-readable diagnostic code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DiagCode {
    /// A nested (struct/list/map) column was skipped — R2RML addresses flat columns only.
    NestedColumnSkipped,
    /// No `required` / null-free key could be found — the table emits no subject.
    NoSafeSubjectKey,
    /// A subject key was chosen but its uniqueness is unverifiable metadata-only.
    SubjectKeyUnverified,
    /// A `*_KEY`/`*_ID` column looks like an FK but resolves to no known PK — kept literal, no join fabricated.
    UnresolvedFkCandidate,
    /// A key column matched more than one candidate parent — kept literal, no join fabricated.
    AmbiguousFk,
    /// A candidate FK could not be proven referentially safe (reserved).
    DanglingFkNotProven,
    /// A referenced column does not exist (reserved for the validate cross-check).
    ColumnNotFound,
    /// A referenced table does not exist (reserved for the validate cross-check).
    TableNotFound,
    /// A join's child/parent column types disagree (reserved for the validate cross-check).
    JoinTypeMismatch,
    /// A column reference's casing disagrees with the live schema (reserved).
    CasingMismatch,
    /// An emitted child-fact→hub join on the hub's PK — bounded, but a perf advisory.
    FactHubJoinAdvisory,
}

/// A single structured diagnostic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Diagnostic {
    /// Severity classification.
    pub severity: Severity,
    /// Machine-readable code.
    pub code: DiagCode,
    /// The table the diagnostic pertains to, if any (`"DW.DIM_STORE"`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    /// The column the diagnostic pertains to, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
    /// Human-readable explanation.
    pub message: String,
}

impl Diagnostic {
    /// Build a diagnostic with a given severity/code, table + column context.
    pub fn new(
        severity: Severity,
        code: DiagCode,
        table: impl Into<String>,
        column: Option<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            severity,
            code,
            table: Some(table.into()),
            column,
            message: message.into(),
        }
    }
}
