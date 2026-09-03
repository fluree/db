//! Human-readable rendering of validation results.
//!
//! Enforcement rejects a write with a plain-text report, and the layout of
//! that report is the same wherever the write came from. What differs is how
//! each caller turns a [`Sid`] into something a reader recognises: the api
//! layer compacts against the transaction's JSON-LD context, while callers
//! below it, which cannot see a context, decode to full IRIs. Those callers
//! supply the rendering; the layout lives here.
//!
//! Resolution has to be supplied rather than derived. A `Sid` is a namespace
//! code plus a local name, and the code means nothing without the ledger's
//! namespace map — which this crate has no handle on. Rendering the parts
//! bare produces `13address` for `…/ns/address`, which reads as corrupt data
//! (#1615).

use std::fmt::Write;

use fluree_db_core::Sid;

use crate::compile::Severity;
use crate::validate::{FocusNode, ValidationResult};

/// Render a `Sid` no namespace map could resolve.
///
/// Reached only when a namespace code is absent from both the snapshot and
/// whatever the operation has yet to commit, which means corruption or a bug
/// rather than a missing prefix. Says so, rather than emitting something a
/// reader would mistake for an identifier.
pub fn unresolved_sid(sid: &Sid) -> String {
    format!("[unresolved namespace {}]{}", sid.namespace_code, sid.name)
}

/// Every result in `results` carrying [`Severity::Violation`].
///
/// Callers holding a whole report use this to select what
/// [`format_violations`] renders.
pub fn violations_of(results: &[ValidationResult]) -> Vec<&ValidationResult> {
    results
        .iter()
        .filter(|r| r.severity == Severity::Violation)
        .collect()
}

/// Format violations as the report a rejected write carries.
///
/// `render_node` resolves focus nodes and property paths; `render_component`
/// renders the constraint component's IRI. Both are the caller's, because
/// only the caller knows which namespaces and prefixes are in reach.
pub fn format_violations<N, C>(
    violations: &[&ValidationResult],
    render_node: N,
    render_component: C,
) -> String
where
    N: Fn(&Sid) -> String,
    C: Fn(&str) -> String,
{
    let mut out = String::new();
    let _ = writeln!(
        out,
        "SHACL validation failed with {} violation(s):",
        violations.len()
    );

    for (i, violation) in violations.iter().enumerate() {
        let _ = writeln!(out, "  {}. {}", i + 1, violation.message);

        let focus = match &violation.focus_node {
            FocusNode::Node(sid) => render_node(sid),
            FocusNode::Literal(literal) => literal.value.to_string(),
        };
        let _ = writeln!(out, "     Focus node: {focus}");

        if let Some(path) = &violation.result_path {
            let _ = writeln!(out, "     Path: {}", render_node(path));
        }

        // Which constraint failed. One `sh:message` often covers several
        // constraints on the same property, so the message alone cannot say
        // whether the value was absent, repeated, or the wrong datatype.
        let _ = writeln!(
            out,
            "     Constraint: {}",
            render_component(violation.constraint_component)
        );
    }

    out
}
