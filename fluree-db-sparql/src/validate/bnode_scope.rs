//! V3 — blank-node label scope validation.
//!
//! SPARQL 1.1 §19.6 (grammar note): *"the same blank node label cannot be
//! used in two different basic graph patterns in the same query."*
//!
//! A basic graph pattern ends at a `GraphPatternNotTriples` boundary —
//! `GRAPH`, `OPTIONAL`, `UNION`, `MINUS`, `SERVICE`, a nested `{ }` group,
//! or a sub-`SELECT`. `FILTER` does **not** end a BGP (§18.2.2.5 collects
//! adjacent triple patterns across filters), and this pass conservatively
//! treats `BIND`/`VALUES` and property-path patterns the same way — a
//! label reused across only those constructs is not flagged (under-reject,
//! never over-reject; see `docs/audit/burn-down/parser-syntax-validation.md`
//! §5 open question 1 for the boundary-set decision).
//!
//! ## AST shape note
//!
//! The parser merges plain adjacent triples into a single `Bgp` node and
//! simplifies a single-pattern braced group `{ ... }` to its bare inner
//! pattern (invariant in `parse/query/pattern.rs`). Consequently two
//! *consecutive* `Bgp` siblings inside a `Group` can only arise from an
//! explicitly braced block — i.e. a genuine new basic graph pattern — so
//! consecutive `Bgp` siblings get distinct scopes.

use std::collections::HashMap;
use std::sync::Arc;

use crate::ast::annotation::ReifierId;
use crate::ast::expr::Expression;
use crate::ast::pattern::{GraphPattern, TriplePattern};
use crate::ast::term::{BlankNode, BlankNodeValue, SubjectTerm, Term};
use crate::diag::{DiagCode, Diagnostic, Label};
use crate::span::SourceSpan;

/// Check blank-node label scoping over a query's WHERE pattern.
///
/// Appends one [`DiagCode::BlankNodeLabelCrossScope`] error per label that
/// appears in more than one basic graph pattern.
pub(super) fn check_blank_node_scopes(pattern: &GraphPattern, diagnostics: &mut Vec<Diagnostic>) {
    let mut checker = BnodeScopeChecker {
        labels: HashMap::new(),
        next_scope: 0,
        diagnostics,
    };
    checker.scan_group(pattern);
}

/// First sighting of a blank-node label.
struct Seen {
    scope: usize,
    first_span: SourceSpan,
    reported: bool,
}

struct BnodeScopeChecker<'d> {
    labels: HashMap<Arc<str>, Seen>,
    next_scope: usize,
    diagnostics: &'d mut Vec<Diagnostic>,
}

impl BnodeScopeChecker<'_> {
    fn alloc_scope(&mut self) -> usize {
        let scope = self.next_scope;
        self.next_scope += 1;
        scope
    }

    /// Scan a group graph pattern. A bare non-`Group` node (the parser's
    /// single-pattern simplification) is treated as a one-child group.
    fn scan_group(&mut self, pattern: &GraphPattern) {
        match pattern {
            GraphPattern::Group { patterns, .. } => self.scan_children(patterns),
            other => self.scan_children(std::slice::from_ref(other)),
        }
    }

    fn scan_children(&mut self, children: &[GraphPattern]) {
        let mut scope = self.alloc_scope();
        // See the module docs: consecutive `Bgp` siblings can only come
        // from an explicitly braced `{ }` block, which starts a new BGP.
        let mut prev_was_bgp = false;
        for child in children {
            match child {
                GraphPattern::Bgp { patterns, .. } => {
                    if prev_was_bgp {
                        scope = self.alloc_scope();
                    }
                    for triple in patterns {
                        self.record_triple(triple, scope);
                    }
                    prev_was_bgp = true;
                }
                GraphPattern::Path {
                    subject, object, ..
                } => {
                    // Property-path patterns share the surrounding scope
                    // (conservative; see module docs).
                    self.record_subject(subject, scope);
                    self.record_object(object, scope);
                    prev_was_bgp = false;
                }
                GraphPattern::AnnotationTarget {
                    reifier,
                    triple_term,
                    ..
                } => {
                    self.record_subject(reifier, scope);
                    self.record_subject(&triple_term.subject, scope);
                    self.record_object(&triple_term.object, scope);
                    prev_was_bgp = false;
                }
                GraphPattern::Filter { expr, .. } => {
                    // FILTER does not end a BGP, but an EXISTS pattern
                    // inside it is its own group of basic graph patterns.
                    self.scan_expression(expr);
                    prev_was_bgp = false;
                }
                GraphPattern::Bind { expr, .. } => {
                    self.scan_expression(expr);
                    prev_was_bgp = false;
                }
                GraphPattern::Values { .. } => {
                    // VALUES terms cannot be blank nodes.
                    prev_was_bgp = false;
                }
                GraphPattern::Optional { pattern, .. } => {
                    self.scan_group(pattern);
                    scope = self.alloc_scope();
                    prev_was_bgp = false;
                }
                GraphPattern::Union { left, right, .. } => {
                    self.scan_group(left);
                    self.scan_group(right);
                    scope = self.alloc_scope();
                    prev_was_bgp = false;
                }
                GraphPattern::Minus { left, right, .. } => {
                    self.scan_group(left);
                    self.scan_group(right);
                    scope = self.alloc_scope();
                    prev_was_bgp = false;
                }
                GraphPattern::Graph { pattern, .. } => {
                    self.scan_group(pattern);
                    scope = self.alloc_scope();
                    prev_was_bgp = false;
                }
                GraphPattern::Service { pattern, .. } => {
                    self.scan_group(pattern);
                    scope = self.alloc_scope();
                    prev_was_bgp = false;
                }
                GraphPattern::SubSelect { query, .. } => {
                    self.scan_group(&query.pattern);
                    scope = self.alloc_scope();
                    prev_was_bgp = false;
                }
                GraphPattern::Group { patterns, .. } => {
                    self.scan_children(patterns);
                    scope = self.alloc_scope();
                    prev_was_bgp = false;
                }
            }
        }
    }

    /// Scan EXISTS / NOT EXISTS patterns nested in an expression. Each is a
    /// fresh group graph pattern (its BGPs are distinct from the enclosing
    /// ones).
    fn scan_expression(&mut self, expr: &Expression) {
        let mut nested = Vec::new();
        expr.walk(&mut |e| match e {
            Expression::Exists { pattern, .. } | Expression::NotExists { pattern, .. } => {
                nested.push(pattern.as_ref());
            }
            _ => {}
        });
        for pattern in nested {
            self.scan_group(pattern);
        }
    }

    fn record_triple(&mut self, triple: &TriplePattern, scope: usize) {
        self.record_subject(&triple.subject, scope);
        self.record_object(&triple.object, scope);
        if let Some(annotation) = &triple.annotation {
            for unit in &annotation.units {
                if let Some(ReifierId::BlankNode(b)) = &unit.reifier {
                    self.record_bnode(b, scope);
                }
                if let Some(block) = &unit.block {
                    for entry in &block.entries {
                        self.record_object(&entry.object, scope);
                    }
                }
            }
        }
    }

    fn record_subject(&mut self, subject: &SubjectTerm, scope: usize) {
        if let SubjectTerm::BlankNode(b) = subject {
            self.record_bnode(b, scope);
        }
    }

    fn record_object(&mut self, object: &Term, scope: usize) {
        if let Term::BlankNode(b) = object {
            self.record_bnode(b, scope);
        }
    }

    fn record_bnode(&mut self, bnode: &BlankNode, scope: usize) {
        let BlankNodeValue::Labeled(label) = &bnode.value else {
            // `[]` anonymous blank nodes have no label to reuse.
            return;
        };
        match self.labels.get_mut(label) {
            None => {
                self.labels.insert(
                    label.clone(),
                    Seen {
                        scope,
                        first_span: bnode.span,
                        reported: false,
                    },
                );
            }
            Some(seen) if seen.scope == scope => {}
            Some(seen) => {
                if !seen.reported {
                    seen.reported = true;
                    self.diagnostics.push(
                        Diagnostic::error(
                            DiagCode::BlankNodeLabelCrossScope,
                            format!(
                                "blank node label _:{label} is used in more than one \
                                 basic graph pattern"
                            ),
                            bnode.span,
                        )
                        .with_label(Label::new(seen.first_span, "first used here"))
                        .with_label(Label::new(bnode.span, "reused in a different scope here"))
                        .with_help(
                            "A blank node label cannot cross a GRAPH, OPTIONAL, UNION, \
                             MINUS, SERVICE, `{ }` group, or sub-SELECT boundary \
                             (SPARQL 1.1 §19.6). Use a variable, or repeat the pattern \
                             with a distinct label.",
                        ),
                    );
                }
            }
        }
    }
}
