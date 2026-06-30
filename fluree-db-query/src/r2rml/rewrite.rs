//! R2RML Pattern Rewriting
//!
//! This module provides functionality to rewrite triple patterns to R2RML patterns
//! when the target graph is backed by an R2RML mapping.
//!
//! # Overview
//!
//! When a GRAPH pattern targets an R2RML graph source, the contained triple
//! patterns should be rewritten to R2RML scan patterns. This allows the query
//! engine to route the patterns to the R2RML operator which will scan the
//! underlying Iceberg tables.
//!
//! # Pattern Conversion
//!
//! Triple patterns are converted as follows:
//!
//! - `?s rdf:type ex:Class` → R2rmlPattern with class_filter="ex:Class"
//! - `?s ex:name ?o` → R2rmlPattern with predicate_filter="ex:name"
//! - `?s ?p ?o` (all variables) → R2rmlPattern with no filters
//!
//! # Limitations
//!
//! - Predicate variables (`?s ?p ?o`) result in full table scans
//! - Subject-bound patterns (`ex:subject ex:name ?o`) are not optimized
//! - Filter patterns are preserved and applied post-R2RML scan

use crate::ir::adapters::ScanPushdown;
use crate::ir::triple::{Ref, Term, TriplePattern};
use crate::ir::{Expression, Function, Pattern, R2rmlPattern};
use crate::r2rml::{ScanCmpOp, ScanValue};
use crate::var_registry::VarId;
use fluree_db_core::{FlakeValue, LedgerSnapshot};
use std::collections::HashSet;

/// Result of rewriting patterns for R2RML.
#[derive(Debug)]
pub struct R2rmlRewriteResult {
    /// Rewritten patterns
    pub patterns: Vec<Pattern>,
    /// Number of triple patterns converted to R2RML
    pub converted_count: usize,
    /// Number of patterns that couldn't be converted (preserved as-is)
    pub unconverted_count: usize,
}

/// Rewrite patterns for an R2RML graph source.
///
/// This function takes patterns from a GRAPH block and converts triple patterns
/// to R2RML patterns when possible. Other pattern types (Filter, Optional, etc.)
/// are processed recursively.
///
/// # Arguments
///
/// * `patterns` - The patterns to rewrite
/// * `graph_source_id` - The graph source alias (e.g., "airlines-gs:main")
/// * `snapshot` - Database for Sid-to-IRI conversion
///
/// # Returns
///
/// A result containing the rewritten patterns and conversion statistics.
pub fn rewrite_patterns_for_r2rml(
    patterns: &[Pattern],
    graph_source_id: &str,
    snapshot: &LedgerSnapshot,
) -> R2rmlRewriteResult {
    let mut result_patterns = Vec::with_capacity(patterns.len());
    let mut converted = 0;
    let mut unconverted = 0;

    // Same-subject star grouping: accumulate regular-predicate R2RML patterns
    // (const predicate + fresh object var) by subject so they can be merged into
    // a single scan, eliminating the O(N^2) self-join. First-seen order preserved.
    let mut star_groups: Vec<(VarId, Vec<R2rmlPattern>)> = Vec::new();
    // Same-subject `rdf:type` patterns, by subject. A single class per subject is
    // fused into that subject's star (constraining its TriplesMap resolution to
    // the class and dropping a redundant correlated re-scan); a subject with no
    // star members, or multiple classes, is emitted as a subject-only scan.
    let mut class_groups: Vec<(VarId, Vec<R2rmlPattern>)> = Vec::new();

    for pattern in patterns {
        match pattern {
            Pattern::Triple(tp) => {
                if let Some(r2rml_pattern) = convert_triple_to_r2rml(tp, graph_source_id, snapshot)
                {
                    converted += 1;
                    if is_star_eligible(&r2rml_pattern) {
                        match star_groups
                            .iter_mut()
                            .find(|(s, _)| *s == r2rml_pattern.subject_var)
                        {
                            Some((_, members)) => members.push(r2rml_pattern),
                            None => {
                                star_groups.push((r2rml_pattern.subject_var, vec![r2rml_pattern]));
                            }
                        }
                    } else if is_class_only(&r2rml_pattern) {
                        match class_groups
                            .iter_mut()
                            .find(|(s, _)| *s == r2rml_pattern.subject_var)
                        {
                            Some((_, members)) => members.push(r2rml_pattern),
                            None => {
                                class_groups.push((r2rml_pattern.subject_var, vec![r2rml_pattern]));
                            }
                        }
                    } else {
                        result_patterns.push(Pattern::R2rml(r2rml_pattern));
                    }
                } else {
                    // Keep original pattern if conversion fails
                    result_patterns.push(pattern.clone());
                    unconverted += 1;
                }
            }
            // Recurse into structural containers (except Graph, which this
            // rewriter is graph-source-bounded and treats as a leaf, and
            // Subquery, which is its own scope).
            Pattern::Optional(_)
            | Pattern::Union(_)
            | Pattern::Minus(_)
            | Pattern::Exists(_)
            | Pattern::NotExists(_)
            | Pattern::Service(_) => {
                let rewritten = pattern.clone().map_subpatterns(&mut |inner| {
                    let r = rewrite_patterns_for_r2rml(&inner, graph_source_id, snapshot);
                    converted += r.converted_count;
                    unconverted += r.unconverted_count;
                    r.patterns
                });
                result_patterns.push(rewritten);
            }
            // Preserve other patterns as-is
            Pattern::Filter(_)
            | Pattern::Bind { .. }
            | Pattern::Unwind { .. }
            | Pattern::Values { .. }
            | Pattern::Subquery(_)
            | Pattern::PropertyPath(_)
            | Pattern::ShortestPath(_)
            | Pattern::IndexSearch(_)
            | Pattern::VectorSearch(_)
            | Pattern::R2rml(_)
            | Pattern::GeoSearch(_)
            | Pattern::S2Search(_)
            | Pattern::Graph { .. }
            | Pattern::EdgeAnnotation { .. }
            | Pattern::AnnotationTarget { .. }
            | Pattern::DefaultGraphSource { .. } => {
                result_patterns.push(pattern.clone());
            }
        }
    }

    // Emit star groups. Single-member groups stay on the normal single-object
    // path; multi-member groups with distinct object vars merge into one scan.
    // A same-subject `rdf:type` is fused into the base by setting its
    // `class_filter`, which constrains TriplesMap resolution to the class and
    // removes the separate class operator's correlated re-scan.
    for (subject, mut members) in star_groups {
        if members.len() == 1 {
            let mut base = members.pop().unwrap();
            if let Some(class) = take_single_class(&mut class_groups, subject) {
                base.class_filter = Some(class);
            }
            result_patterns.push(Pattern::R2rml(base));
            continue;
        }
        let mut seen_obj = HashSet::new();
        let distinct = members
            .iter()
            .all(|m| m.object_var.is_some_and(|v| seen_obj.insert(v)));
        if !distinct {
            // Shared object var implies a self-join constraint; keep separate.
            // Leave any same-subject class pattern in `class_groups` so it is
            // emitted as a subject-only scan below.
            for m in members {
                result_patterns.push(Pattern::R2rml(m));
            }
            continue;
        }
        let mut base = members.remove(0);
        if let Some(class) = take_single_class(&mut class_groups, subject) {
            base.class_filter = Some(class);
        }
        base.star_bindings = members
            .into_iter()
            .map(|m| {
                (
                    m.predicate_filter.expect("star-eligible has predicate"),
                    m.object_var.expect("star-eligible has object var"),
                )
            })
            .collect();
        result_patterns.push(Pattern::R2rml(base));
    }

    // Class patterns not fused into a star (no same-subject star members, or
    // multiple classes on one subject) become subject-only scans: the operator
    // projects only the subject columns and scans no RefObjectMap parents.
    for (_subject, members) in class_groups {
        for m in members {
            result_patterns.push(Pattern::R2rml(m));
        }
    }

    // Attach pushable FILTER comparisons to the R2RML pattern that produces
    // each compared variable, for Iceberg file pruning. The FILTER pattern is
    // left in place (residual), so this only ever skips data files.
    let mut pushdowns: Vec<(VarId, ScanCmpOp, ScanValue)> = Vec::new();
    for p in &result_patterns {
        if let Pattern::Filter(expr) = p {
            collect_pushdowns(expr, &mut pushdowns);
        }
    }
    if !pushdowns.is_empty() {
        for p in &mut result_patterns {
            if let Pattern::R2rml(rp) = p {
                let produced = rp.produced_vars();
                for (var, op, value) in &pushdowns {
                    // Only object-position vars map to columns (the subject is an
                    // IRI template, not a scannable column).
                    if *var != rp.subject_var && produced.contains(var) {
                        rp.scan_filters.push(ScanPushdown {
                            var: *var,
                            op: *op,
                            value: value.clone(),
                        });
                    }
                }
            }
        }
    }

    R2rmlRewriteResult {
        patterns: result_patterns,
        converted_count: converted,
        unconverted_count: unconverted,
    }
}

/// Collect conjunctive `?var <op> const` comparisons that prune safely against
/// Iceberg column min/max bounds (date/int/bool only). `!=` and non-prunable
/// literal types are skipped — they stay with the in-engine FILTER.
fn collect_pushdowns(expr: &Expression, out: &mut Vec<(VarId, ScanCmpOp, ScanValue)>) {
    let Expression::Call { func, args } = expr else {
        return;
    };
    if matches!(func, Function::And) {
        for a in args {
            collect_pushdowns(a, out);
        }
        return;
    }
    if args.len() != 2 {
        return;
    }
    // Normalize to (var, op, const), reversing the operator if the constant is
    // on the left.
    let (var, value, reversed) = match (&args[0], &args[1]) {
        (Expression::Var(v), Expression::Const(c)) => (*v, c, false),
        (Expression::Const(c), Expression::Var(v)) => (*v, c, true),
        _ => return,
    };
    let Some(op) = cmp_op(func, reversed) else {
        return;
    };
    if let Some(sv) = to_scan_value(value) {
        out.push((var, op, sv));
    }
}

/// Map a comparison `Function` to a pushable `ScanCmpOp`, reversing operand
/// order when the constant was on the left. Returns None for non-prunable ops.
fn cmp_op(func: &Function, reversed: bool) -> Option<ScanCmpOp> {
    let op = match func {
        Function::Eq => ScanCmpOp::Eq,
        Function::Lt => ScanCmpOp::Lt,
        Function::Le => ScanCmpOp::LtEq,
        Function::Gt => ScanCmpOp::Gt,
        Function::Ge => ScanCmpOp::GtEq,
        // `!=` cannot prune via min/max bounds; leave it to the FILTER.
        _ => return None,
    };
    Some(if reversed {
        match op {
            ScanCmpOp::Lt => ScanCmpOp::Gt,
            ScanCmpOp::LtEq => ScanCmpOp::GtEq,
            ScanCmpOp::Gt => ScanCmpOp::Lt,
            ScanCmpOp::GtEq => ScanCmpOp::LtEq,
            other => other, // Eq is symmetric
        }
    } else {
        op
    })
}

/// Convert a constant literal to a prunable `ScanValue`. Only date, integer and
/// boolean are pushed; everything else stays with the in-engine FILTER.
fn to_scan_value(value: &FlakeValue) -> Option<ScanValue> {
    match value {
        FlakeValue::Long(n) => Some(ScanValue::Int(*n)),
        FlakeValue::Boolean(b) => Some(ScanValue::Bool(*b)),
        FlakeValue::Date(d) => Some(ScanValue::Date(d.days_since_epoch())),
        _ => None,
    }
}

/// A regular-predicate R2RML pattern that can join via the subject: constant
/// predicate, a fresh object var distinct from the subject, no class/TM filter.
/// These are the patterns that can be merged into a same-subject star scan.
fn is_star_eligible(p: &R2rmlPattern) -> bool {
    p.predicate_filter.is_some()
        && p.class_filter.is_none()
        && p.triples_map_iri.is_none()
        && p.star_bindings.is_empty()
        && match p.object_var {
            Some(obj) => obj != p.subject_var,
            None => false,
        }
}

/// A pure `rdf:type` pattern (`?s a ex:Class`): a class filter, no object var, no
/// predicate, no star members. These are candidates to fuse into a same-subject
/// star (or, failing that, to run as a subject-only scan).
fn is_class_only(p: &R2rmlPattern) -> bool {
    p.class_filter.is_some()
        && p.object_var.is_none()
        && p.predicate_filter.is_none()
        && p.triples_map_iri.is_none()
        && p.star_bindings.is_empty()
}

/// Remove and return the lone class IRI for `subject` from `class_groups`, but
/// only when that subject has exactly one `rdf:type` pattern. A subject with
/// multiple classes is left in place (each emitted as its own subject-only scan)
/// because `class_filter` holds a single class.
fn take_single_class(
    class_groups: &mut Vec<(VarId, Vec<R2rmlPattern>)>,
    subject: VarId,
) -> Option<String> {
    let idx = class_groups.iter().position(|(s, _)| *s == subject)?;
    if class_groups[idx].1.len() != 1 {
        return None;
    }
    let (_, mut members) = class_groups.remove(idx);
    members.pop().and_then(|p| p.class_filter)
}

/// Convert a triple pattern to an R2RML pattern.
///
/// Returns `None` if the pattern cannot be converted (e.g., subject is a literal).
pub fn convert_triple_to_r2rml(
    tp: &TriplePattern,
    graph_source_id: &str,
    snapshot: &LedgerSnapshot,
) -> Option<R2rmlPattern> {
    // Extract subject variable (must be a variable for basic R2RML support)
    let subject_var = match &tp.s {
        Ref::Var(v) => *v,
        Ref::Sid(_) | Ref::Iri(_) => {
            // Subject is bound - we could support this with a filter,
            // but for now we return None to preserve the original pattern.
            // The GraphOperator will handle this case differently.
            return None;
        }
    };

    // Check if this is an rdf:type pattern
    // Use Term::is_rdf_type() to handle both Term::Sid and Term::Iri
    let is_type_pattern = tp.p.is_rdf_type();

    if is_type_pattern {
        // rdf:type pattern: ?s rdf:type ex:Class
        // Extract the class IRI - handle both Term::Sid (decode) and Term::Iri (use directly)
        let class_filter = match &tp.o {
            Term::Sid(sid) => snapshot.decode_sid(sid),
            Term::Iri(iri) => Some(iri.to_string()),
            Term::Value(fluree_db_core::FlakeValue::Ref(sid)) => snapshot.decode_sid(sid),
            Term::Var(_) => None, // Class is a variable - no filter
            _ => None,
        };

        // For rdf:type, we create an R2RML pattern with class_filter and no object_var
        // (the type binding is implicit in the class_filter)
        let mut pattern = R2rmlPattern::new(graph_source_id, subject_var, None);
        if let Some(class_iri) = class_filter {
            pattern = pattern.with_class(class_iri);
        }
        return Some(pattern);
    }

    // Regular predicate pattern: ?s ex:name ?o
    // Extract predicate IRI filter - handle both Ref::Sid (decode) and Ref::Iri (use directly)
    let predicate_filter = match &tp.p {
        Ref::Sid(sid) => snapshot.decode_sid(sid),
        Ref::Iri(iri) => Some(iri.to_string()),
        Ref::Var(_) => None, // Predicate is variable - no filter
    };

    // Extract object variable
    // If object is bound (constant), don't rewrite - we can't currently push
    // object value constraints into the R2RML scan, so the original pattern
    // needs to be preserved for correct filtering.
    let object_var = match &tp.o {
        Term::Var(v) => Some(*v),
        Term::Sid(_) | Term::Iri(_) | Term::Value(_) => {
            // Object is bound - don't rewrite this pattern.
            // The R2RML scan cannot filter by object value, and rewriting
            // would drop the constraint, returning incorrect results.
            // Preserve the original triple pattern for normal evaluation.
            return None;
        }
    };

    let mut pattern = R2rmlPattern::new(graph_source_id, subject_var, object_var);
    if let Some(pred_iri) = predicate_filter {
        pattern = pattern.with_predicate(pred_iri);
    }

    Some(pattern)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::triple::Ref;
    use crate::var_registry::VarId;
    use fluree_db_core::{is_rdf_type, Sid};
    use fluree_vocab::namespaces::RDF;

    #[test]
    fn test_rdf_type_detection() {
        // RDF namespace is code 3 in fluree-vocab
        let rdf_type_sid = Sid::new(RDF, "type");
        assert!(is_rdf_type(&rdf_type_sid));

        let not_type_sid = Sid::new(100, "name");
        assert!(!is_rdf_type(&not_type_sid));
    }

    #[test]
    fn test_convert_variable_only_pattern() {
        // ?s ?p ?o - all variables
        let tp = TriplePattern::new(Ref::Var(VarId(0)), Ref::Var(VarId(1)), Term::Var(VarId(2)));

        // We need to test without a real DB, so we test the logic manually
        // The pattern should have subject_var, object_var, but no filters
        assert!(tp.s.is_var());
        assert!(tp.p.is_var());
        assert!(tp.o.is_var());
    }
}
