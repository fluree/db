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
use fluree_db_r2rml::mapping::CompiledR2rmlMapping;
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
/// * `mapping` - The compiled R2RML mapping, when available. Used to decide
///   whether a same-subject `rdf:type` may be safely fused into a star scan
///   (see [`class_fusion_is_safe`]). `None` disables class fusion — always
///   correct, just less optimal — so callers that can cheaply load the mapping
///   should pass it.
///
/// # Returns
///
/// A result containing the rewritten patterns and conversion statistics.
pub fn rewrite_patterns_for_r2rml(
    patterns: &[Pattern],
    graph_source_id: &str,
    snapshot: &LedgerSnapshot,
    mapping: Option<&CompiledR2rmlMapping>,
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
                    let r = rewrite_patterns_for_r2rml(&inner, graph_source_id, snapshot, mapping);
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
            fuse_class_if_safe(&mut base, &mut class_groups, subject, mapping);
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
        fuse_class_if_safe(&mut base, &mut class_groups, subject, mapping);
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

/// Fuse a subject's lone `rdf:type` into its star `base` by setting
/// `base.class_filter`, but only when doing so cannot change the result set.
///
/// Fusion constrains TriplesMap resolution in
/// [`operator::build_progress`](super::operator) to maps that satisfy the class
/// **and** the star's base predicate. That is only equivalent to the pre-fusion
/// two-pattern plan (a subject-only class scan joined with the predicate scan)
/// when the class and predicate co-locate in the same TriplesMap. A vertically
/// partitioned mapping (`TM_A` = subject+class, `TM_B` = subject+predicate, same
/// subject template) has no single map with both, so a fused scan resolves zero
/// maps and silently returns no rows (fluree/db#1406 review).
///
/// So fuse only when [`class_fusion_is_safe`] holds; otherwise leave the class
/// pattern in `class_groups` to be emitted as its own subject-only scan, which
/// the engine joins on the shared subject — the always-correct pre-fusion path.
/// Fusion is also skipped when the subject carries more than one class (a single
/// `class_filter` cannot represent them) or when the mapping is unavailable.
fn fuse_class_if_safe(
    base: &mut R2rmlPattern,
    class_groups: &mut Vec<(VarId, Vec<R2rmlPattern>)>,
    subject: VarId,
    mapping: Option<&CompiledR2rmlMapping>,
) {
    let Some(idx) = class_groups.iter().position(|(s, _)| *s == subject) else {
        return;
    };
    if class_groups[idx].1.len() != 1 {
        return;
    }
    let Some(class) = class_groups[idx].1[0].class_filter.clone() else {
        return;
    };
    // The base predicate drives TriplesMap selection; a star always has one.
    let Some(base_pred) = base.predicate_filter.as_deref() else {
        return;
    };
    if !mapping.is_some_and(|m| class_fusion_is_safe(m, &class, base_pred)) {
        return;
    }
    class_groups.remove(idx);
    base.class_filter = Some(class);
}

/// Whether fusing `class_iri` into the star for `base_predicate` preserves the
/// result set: every TriplesMap that resolves `base_predicate` must also declare
/// the class. Then adding the class as a TriplesMap-selection constraint cannot
/// drop any map the predicate scan would otherwise select, and every scanned row
/// genuinely carries the class. If some predicate map lacks the class (the
/// vertically partitioned / split-TriplesMap shape), fusion is unsafe.
fn class_fusion_is_safe(
    mapping: &CompiledR2rmlMapping,
    class_iri: &str,
    base_predicate: &str,
) -> bool {
    let mut saw_predicate_map = false;
    for tm in mapping.triples_maps.values() {
        let has_predicate = tm
            .predicate_object_maps
            .iter()
            .any(|pom| pom.predicate_map.as_constant() == Some(base_predicate));
        if !has_predicate {
            continue;
        }
        saw_predicate_map = true;
        if !tm.classes().iter().any(|c| c == class_iri) {
            return false;
        }
    }
    saw_predicate_map
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

    use fluree_db_r2rml::mapping::{ObjectMap, PredicateMap, PredicateObjectMap, TriplesMap};

    const CLASS: &str = "http://example.org/Person";
    const PRED: &str = "http://example.org/name";

    fn pom(pred: &str, col: &str) -> PredicateObjectMap {
        PredicateObjectMap {
            predicate_map: PredicateMap::constant(pred),
            object_map: ObjectMap::column(col),
        }
    }

    #[test]
    fn class_fusion_safe_when_class_and_predicate_colocate() {
        // One TriplesMap declares the class and the predicate — the star-schema
        // shape fusion optimizes for.
        let tm = TriplesMap::new("#TM", "people")
            .with_subject_template("http://example.org/person/{id}")
            .with_class(CLASS)
            .with_predicate_object(pom(PRED, "name"));
        let mapping = CompiledR2rmlMapping::new(vec![tm]);
        assert!(class_fusion_is_safe(&mapping, CLASS, PRED));
    }

    #[test]
    fn class_fusion_unsafe_when_split_across_triples_maps() {
        // Vertically partitioned: TM_A holds the class, TM_B holds the predicate,
        // sharing a subject template. No single map has both, so fusing the class
        // into the predicate star would resolve zero maps → silent empty result.
        let tm_class = TriplesMap::new("#TM_A", "people_class")
            .with_subject_template("http://example.org/person/{id}")
            .with_class(CLASS);
        let tm_pred = TriplesMap::new("#TM_B", "people_name")
            .with_subject_template("http://example.org/person/{id}")
            .with_predicate_object(pom(PRED, "name"));
        let mapping = CompiledR2rmlMapping::new(vec![tm_class, tm_pred]);
        assert!(!class_fusion_is_safe(&mapping, CLASS, PRED));
    }

    #[test]
    fn class_fusion_unsafe_when_a_predicate_map_lacks_the_class() {
        // One predicate map co-locates the class, another resolves the same
        // predicate without it. Fusing would drop rows from the classless map.
        let tm_both = TriplesMap::new("#TM_both", "people")
            .with_subject_template("http://example.org/person/{id}")
            .with_class(CLASS)
            .with_predicate_object(pom(PRED, "name"));
        let tm_pred_only = TriplesMap::new("#TM_pred", "aliases")
            .with_subject_template("http://example.org/person/{id}")
            .with_predicate_object(pom(PRED, "alias"));
        let mapping = CompiledR2rmlMapping::new(vec![tm_both, tm_pred_only]);
        assert!(!class_fusion_is_safe(&mapping, CLASS, PRED));
    }

    #[test]
    fn class_fusion_unsafe_when_no_map_resolves_the_predicate() {
        // Guards against fusing (and thus dropping the separate class scan) when
        // the predicate resolves nowhere — the result must stay whatever the
        // unfused plan produces, not silently collapse.
        let tm = TriplesMap::new("#TM", "people")
            .with_subject_template("http://example.org/person/{id}")
            .with_class(CLASS);
        let mapping = CompiledR2rmlMapping::new(vec![tm]);
        assert!(!class_fusion_is_safe(&mapping, CLASS, PRED));
    }
}
