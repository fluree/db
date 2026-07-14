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
use crate::r2rml::{ObjectConstant, ScanCmpOp, ScanValue};
use crate::var_registry::VarId;
use fluree_db_core::{DatatypeConstraint, FlakeValue, LedgerSnapshot};
use fluree_db_r2rml::mapping::{CompiledR2rmlMapping, ObjectMap};
use fluree_vocab::namespaces::XSD;
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
    /// Non-lowered sub-scope patterns that would evaluate against the R2RML
    /// graph source's (empty) native index and **silently return no rows** —
    /// property paths, shortest paths, and subqueries, whose bodies traverse or
    /// scan the enclosing graph. The caller MUST error (via
    /// [`unsupported_subscope_error`]) when this is non-empty rather than hand
    /// back a silently-wrong empty result. Holds each SPARQL kind name for the
    /// error message. Deliberately excludes the search patterns
    /// (index/vector/geo/s2): they carry their own `graph_source_id` and route
    /// independently of this scope.
    pub unsupported: Vec<&'static str>,
}

/// Build the loud-refuse error for [`R2rmlRewriteResult::unsupported`].
///
/// Shared by every GRAPH execution path that consumes a rewrite result (the
/// seeded and batched operators in `graph.rs`) so the user-facing message
/// cannot drift between them. Kind names are deduplicated preserving first
/// occurrence: two property paths in one scope read "property path", not
/// "property path, property path".
pub fn unsupported_subscope_error(graph_iri: &str, kinds: &[&str]) -> crate::error::QueryError {
    let mut unique: Vec<&str> = Vec::with_capacity(kinds.len());
    for &kind in kinds {
        if !unique.contains(&kind) {
            unique.push(kind);
        }
    }
    crate::error::QueryError::InvalidQuery(format!(
        "R2RML graph source '{}' contains pattern(s) that cannot be evaluated over a \
         virtual dataset (an R2RML source has no native index, so these would silently \
         return no rows): {}. Rewrite them as basic triple patterns or move them outside \
         the GRAPH block.",
        graph_iri,
        unique.join(", ")
    ))
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
/// * `crawl_active` - Whether this rewrite serves a graph-source subgraph "browse"
///   crawl (sourced from `ExecutionContext::trust_fk_refs`, which only the crawl
///   sets). When `true`, a lone projected type-var (`?s a ?type`) co-located with
///   the crawl's wildcard is MERGED into the wildcard so the crawl is a single
///   LIMIT-budgeted scan (see [`try_fuse_wildcard_class`]). Gated on the crawl so
///   hand-written SPARQL `{?s a :C . ?s ?p ?o . ?s a ?t}` keeps its known-correct
///   two-scan plan rather than the fused per-TriplesMap cartesian.
/// * `reasoning_active` - Whether an RDFS/OWL/datalog entailment mode is active
///   for this query. When `true`, the wildcard→class fusion
///   ([`try_fuse_wildcard_class`]) is refused: that fusion prunes TriplesMaps by
///   an EXACT `rr:class` match, so a subject entailed into a superclass whose
///   TriplesMap only declares a subclass would be silently dropped. (In
///   practice RDFS expands `?s a C` into an explicit subclass UNION upstream of
///   this rewrite and derived-fact overlays are invisible to R2RML scans, so
///   the fusion is already sound under reasoning; this flag is defense in depth.)
///
/// # Returns
///
/// A result containing the rewritten patterns and conversion statistics.
pub fn rewrite_patterns_for_r2rml(
    patterns: &[Pattern],
    graph_source_id: &str,
    snapshot: &LedgerSnapshot,
    mapping: Option<&CompiledR2rmlMapping>,
    reasoning_active: bool,
    crawl_active: bool,
) -> R2rmlRewriteResult {
    let mut result_patterns = Vec::with_capacity(patterns.len());
    let mut converted = 0;
    let mut unconverted = 0;
    let mut unsupported: Vec<&'static str> = Vec::new();

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
                    // Only variable-subject patterns are grouped by shared
                    // subject; a bound-subject pattern (subject_var = None) is
                    // never star/class eligible and falls to standalone emit.
                    if let Some(sv) = star_member_subject(&r2rml_pattern) {
                        match star_groups.iter_mut().find(|(s, _)| *s == sv) {
                            Some((_, members)) => members.push(r2rml_pattern),
                            None => {
                                star_groups.push((sv, vec![r2rml_pattern]));
                            }
                        }
                    } else if let Some(sv) = class_only_subject(&r2rml_pattern) {
                        match class_groups.iter_mut().find(|(s, _)| *s == sv) {
                            Some((_, members)) => members.push(r2rml_pattern),
                            None => {
                                class_groups.push((sv, vec![r2rml_pattern]));
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
                    let r = rewrite_patterns_for_r2rml(
                        &inner,
                        graph_source_id,
                        snapshot,
                        mapping,
                        reasoning_active,
                        crawl_active,
                    );
                    converted += r.converted_count;
                    unconverted += r.unconverted_count;
                    unsupported.extend(r.unsupported);
                    r.patterns
                });
                result_patterns.push(rewritten);
            }
            // Non-lowered patterns whose bodies evaluate against THIS R2RML
            // graph source's (empty) native index, so if left unconverted they
            // return no rows *silently* (fluree/db virtual-dataset findings
            // F1/F2). Record them so the caller errors loudly instead. A
            // property/shortest path traverses the graph; a subquery's WHERE
            // scans it. Sequence paths (`a/b`) never reach here — SPARQL
            // lowering decomposes them into triples upstream — so a residual
            // `PropertyPath` is a transitive/complex path. We do NOT attempt to
            // lower these (out of scope); we only convert silent-wrong into a
            // loud error.
            Pattern::PropertyPath(_) => {
                unsupported.push("property path");
                result_patterns.push(pattern.clone());
            }
            Pattern::ShortestPath(_) => {
                unsupported.push("shortest path");
                result_patterns.push(pattern.clone());
            }
            Pattern::Subquery(_) => {
                unsupported.push("subquery");
                result_patterns.push(pattern.clone());
            }
            // Preserve the rest as-is. These do NOT hydrate this graph's index:
            // Filter/Bind/Unwind/Values transform already-bound solutions;
            // IndexSearch/VectorSearch/GeoSearch/S2Search carry their own
            // `graph_source_id` and route independently; `R2rml` is already
            // converted; nested `Graph`/`DefaultGraphSource` re-enter routing
            // for their own target; the RDF-star `EdgeAnnotation`/
            // `AnnotationTarget` are expanded during planning (RDF-star over
            // R2RML is undefined — left untouched here, not silently-empty in
            // the confirmed sense).
            Pattern::Filter(_)
            | Pattern::Bind { .. }
            | Pattern::Unwind { .. }
            | Pattern::Values { .. }
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

    // PR-F20: RefObjectMap-target resolution prune. Before emitting stars, find
    // subjects `?o` bound SOLELY as one RefObjectMap object (parent class C) whose
    // downstream star can be constrained to C-declaring maps — killing the
    // shared-predicate fan-out (`?p edw:name` resolving to all name-bearing dims).
    // Computed once over this scope (`class_group_subjects` captured BEFORE the
    // star loop mutates `class_groups`); consumed in the star loop. Gated by
    // `FLUREE_R2RML_REF_TARGET_PRUNE`.
    let ref_prune_targets = if ref_target_prune_enabled() {
        let class_group_subjects: Vec<VarId> = class_groups.iter().map(|(s, _)| *s).collect();
        compute_ref_prune_targets(&star_groups, &result_patterns, &class_group_subjects, mapping)
    } else {
        std::collections::HashMap::new()
    };

    // Emit star groups. Single-member groups stay on the normal single-object
    // path; multi-member groups with distinct object vars merge into one scan.
    // A same-subject `rdf:type` is fused into the base by setting its
    // `class_filter`, which constrains TriplesMap resolution to the class and
    // removes the separate class operator's correlated re-scan.
    //
    // Fusing is unconditional, which assumes some single TriplesMap covers all
    // members: required members split across template-sharing maps yield zero
    // star rows (materialization is per-map — no cross-map member join), where
    // a per-member plan would subject-join them. Recorded as F10 in
    // `04-findings-register.md`; the fix is to refuse to fuse when no map
    // covers every member.
    for (subject, members) in star_groups {
        // Split into object-var members (produce bindings) and constant-object
        // members (equality existence constraints fused into the same scan).
        let (mut var_members, const_members): (Vec<R2rmlPattern>, Vec<R2rmlPattern>) =
            members.into_iter().partition(|m| m.object_var.is_some());

        if var_members.is_empty() {
            // No var-object base to fuse the constraints onto: each constant-object
            // pattern is already correct as its own standalone scan.
            for m in const_members {
                result_patterns.push(Pattern::R2rml(m));
            }
            continue;
        }

        // Var-object members need distinct object vars to fuse; a shared object
        // var is a self-join constraint, not a star. If not distinct, keep every
        // member separate (var and constant alike).
        let mut seen_obj = HashSet::new();
        let distinct = var_members
            .iter()
            .all(|m| m.object_var.is_some_and(|v| seen_obj.insert(v)));
        if !distinct {
            for m in var_members.into_iter().chain(const_members) {
                result_patterns.push(Pattern::R2rml(m));
            }
            continue;
        }

        let star_constraints: Vec<(String, ObjectConstant)> = const_members
            .into_iter()
            .filter_map(|m| Some((m.predicate_filter?, m.object_constant?)))
            .collect();

        let mut base = var_members.remove(0);
        fuse_class_if_safe(&mut base, &mut class_groups, subject, mapping);
        // PR-F20: if no query-declared class fused/pinned this star, but its
        // subject is a provable RefObjectMap object (invariant A+B, precomputed),
        // constrain resolution to the FK parent's class via the same
        // resolution-only `class_prune_hint`. `class_prune_hint` never touches
        // rdf:type materialization, so no class row is fabricated.
        //
        // V1 restriction: SINGLE-predicate star only (`var_members` empty ⇒ the
        // lone base predicate, and no constant-object constraints) — exactly
        // q031's `?p edw:name ?pn`. The (A)+(B) argument DOES extend to a
        // multi-predicate `?p`-star (every map that could supply any of `?p`'s
        // star rows must share the parent's subject template, and (B) proves only
        // class-C maps do so safely — so constraining to C drops nothing), but the
        // widening ships later with that argument written out and its own tests.
        if var_members.is_empty()
            && star_constraints.is_empty()
            && base.class_filter.is_none()
            && base.class_prune_hint.is_none()
        {
            if let Some(class) = ref_prune_targets.get(&subject) {
                base.class_prune_hint = Some(class.clone());
            }
        }
        base.star_bindings = var_members
            .into_iter()
            .map(|m| {
                (
                    m.predicate_filter.expect("star member has predicate"),
                    m.object_var.expect("var member has object var"),
                )
            })
            .collect();
        base.star_constraints = star_constraints;
        result_patterns.push(Pattern::R2rml(base));
    }

    // Class patterns not fused into a star. First try to fuse a lone class into
    // a same-subject standalone WILDCARD scan (`?s ?p ?o`, from a subgraph
    // crawl) by class-constraining it — this prunes the wildcard's TriplesMap
    // fan-out to the queried class (16→1 for a per-table Iceberg mapping) while
    // its per-`(predicate, object)`-row semantics still return subjects with
    // null columns correctly (unlike an inner-joined explicit star). Runs AFTER
    // the star loop's `fuse_class_if_safe`, which already removed any class it
    // consumed, so a class is never double-consumed. A class that is neither
    // star- nor wildcard-fused becomes a subject-only scan (the always-correct
    // pre-fusion path): the operator projects only the subject columns and scans
    // no RefObjectMap parents.
    for (subject, members) in class_groups {
        let fused = members.len() == 1
            && members[0].class_filter.as_deref().is_some_and(|class| {
                try_fuse_wildcard_class(
                    &mut result_patterns,
                    subject,
                    class,
                    mapping,
                    reasoning_active,
                    crawl_active,
                )
            });
        if !fused {
            for m in members {
                result_patterns.push(Pattern::R2rml(m));
            }
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
                    if Some(*var) != rp.subject_var && produced.contains(var) {
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

    // Consume a fully scan-local FILTER into the single R2RML scan so the
    // downstream LIMIT row budget can reach it. Narrow and safe: only when the
    // group is purely R2RML scans and FILTERs (no OPTIONAL / UNION / BIND /
    // multi-scan join) and there is exactly one R2RML pattern, so a filter whose
    // variables are all produced by that scan cannot depend on any other pattern.
    // The operator re-applies the moved filter with the same evaluator (results
    // unchanged); removing the `Pattern::Filter` is what lets the budget flow.
    consume_scan_local_filters(&mut result_patterns);

    R2rmlRewriteResult {
        patterns: result_patterns,
        converted_count: converted,
        unconverted_count: unconverted,
        unsupported,
    }
}

/// Whether scan-local FILTER consumption is enabled. Read once from
/// `FLUREE_R2RML_FILTER_CONSUMPTION` (family falsy spellings,
/// [`super::env_switch_enabled`]). The kill switch keeps the FILTER in the plan
/// (no LIMIT flow) for A/B validation.
fn filter_consumption_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| super::env_switch_enabled("FLUREE_R2RML_FILTER_CONSUMPTION"))
}

/// Move scan-local top-level FILTERs into the single R2RML scan's
/// `consumed_filter`, removing them from the pattern list. See the call site for
/// the safety conditions.
fn consume_scan_local_filters(patterns: &mut Vec<Pattern>) {
    if !filter_consumption_enabled() {
        return;
    }
    let all_scan_or_filter = patterns
        .iter()
        .all(|p| matches!(p, Pattern::R2rml(_) | Pattern::Filter(_)));
    let scan_count = patterns
        .iter()
        .filter(|p| matches!(p, Pattern::R2rml(_)))
        .count();
    if !all_scan_or_filter || scan_count != 1 {
        return;
    }

    let produced: HashSet<VarId> = patterns
        .iter()
        .find_map(|p| match p {
            Pattern::R2rml(rp) => Some(rp.produced_vars().into_iter().collect()),
            _ => None,
        })
        .unwrap_or_default();

    let mut consumed: Vec<Expression> = Vec::new();
    patterns.retain(|p| {
        if let Pattern::Filter(expr) = p {
            let mut vars = HashSet::new();
            // A Cypher metadata read (`type`/`labels`/`keys`/...) is a `Call`
            // that `collect_expr_vars` would accept, but the consumed path applies
            // it via synchronous `filter_batch`, bypassing the policy-aware async
            // resolver the standalone `FilterOperator` uses under a view policy.
            // Leave it in place so authority — and fail-closed behavior — stays
            // with the in-engine FILTER.
            if crate::eval::metadata_resolve::contains_metadata_read(expr) {
                return true;
            }
            // A variable-free filter (constant), one this analysis can't fully
            // understand, or one touching a var the scan does not produce is
            // left in place for the in-engine FILTER.
            if collect_expr_vars(expr, &mut vars)
                && !vars.is_empty()
                && vars.iter().all(|v| produced.contains(v))
            {
                consumed.push(expr.clone());
                return false;
            }
        }
        true
    });

    if consumed.is_empty() {
        return;
    }
    let combined = if consumed.len() == 1 {
        consumed.pop().unwrap()
    } else {
        Expression::and(consumed)
    };
    for p in patterns.iter_mut() {
        if let Pattern::R2rml(rp) = p {
            rp.consumed_filter = Some(combined);
            break;
        }
    }
}

/// Collect all variables referenced by an expression into `out`, returning
/// `false` if the expression contains a construct this analysis does not fully
/// understand (EXISTS, comprehensions, maps, resolved bindings, ...). A `false`
/// result means the filter must NOT be consumed: it may reference variables — or
/// carry scoping semantics — this walk cannot see, so the in-engine FILTER keeps
/// authority. Only plain `Call` trees over `Var`/`Const` are consumable.
fn collect_expr_vars(expr: &Expression, out: &mut HashSet<VarId>) -> bool {
    match expr {
        Expression::Var(v) => {
            out.insert(*v);
            true
        }
        Expression::Const(_) => true,
        Expression::Call { args, .. } => args.iter().all(|a| collect_expr_vars(a, out)),
        _ => false,
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
/// Whether a triple's object datatype constraint permits a loose (value-only)
/// constant-object match. The product matches untyped literals loosely, and a
/// literal written without an explicit `^^type` carries its natural XSD datatype
/// (`xsd:string`, `xsd:integer`, ...), so any XSD-namespaced datatype qualifies.
/// A language tag or a custom (non-XSD) datatype requires strict matching and is
/// excluded from this path.
fn is_loose_matchable_datatype(dtc: &Option<DatatypeConstraint>) -> bool {
    match dtc {
        None => true,
        Some(DatatypeConstraint::Explicit(sid)) => sid.namespace_code == XSD,
        Some(DatatypeConstraint::LangTag(_)) => false,
    }
}

/// The operator-enforced constant for an object literal, or `None` for value
/// types not supported as constant objects (refs, temporal types beyond date,
/// durations, vectors, JSON, geo).
///
/// String / integer / boolean / date go through `Scalar` and additionally emit a
/// scan filter for pruning. Decimal / big-integer / double are numeric matches
/// enforced by the operator only (no scan pushdown yet).
fn const_object(value: &FlakeValue) -> Option<ObjectConstant> {
    use bigdecimal::BigDecimal;
    use std::str::FromStr;
    match value {
        FlakeValue::String(s) => Some(ObjectConstant::Scalar(ScanValue::Str(s.clone()))),
        FlakeValue::Long(n) => Some(ObjectConstant::Scalar(ScanValue::Int(*n))),
        FlakeValue::Boolean(b) => Some(ObjectConstant::Scalar(ScanValue::Bool(*b))),
        FlakeValue::Date(d) => Some(ObjectConstant::Scalar(ScanValue::Date(
            d.days_since_epoch(),
        ))),
        FlakeValue::Decimal(d) => Some(ObjectConstant::Decimal((**d).clone())),
        FlakeValue::Double(f) => Some(ObjectConstant::Double(*f)),
        // Big integers compare numerically as exact decimals.
        FlakeValue::BigInt(n) => BigDecimal::from_str(&n.to_string())
            .ok()
            .map(ObjectConstant::Decimal),
        _ => None,
    }
}

/// Convert a FILTER comparison's constant literal to a prunable `ScanValue`.
/// Integer / boolean / date / string always push. Double and decimal push only
/// when `FLUREE_ICEBERG_NUMERIC_STATS` is on (PR-7) — this is the single gate for
/// numeric pushdown, so with it off no numeric `LiteralValue` reaches the reader
/// and the iceberg-side numeric widening stays inert. Anything else (refs,
/// big-integers beyond i128, temporal beyond date, …) stays with the in-engine
/// FILTER.
fn to_scan_value(value: &FlakeValue) -> Option<ScanValue> {
    match value {
        FlakeValue::Long(n) => Some(ScanValue::Int(*n)),
        FlakeValue::Boolean(b) => Some(ScanValue::Bool(*b)),
        FlakeValue::Date(d) => Some(ScanValue::Date(d.days_since_epoch())),
        FlakeValue::String(s) => Some(ScanValue::Str(s.clone())),
        FlakeValue::Double(f) if crate::r2rml::iceberg_numeric_stats_enabled() => {
            Some(ScanValue::Double(*f))
        }
        FlakeValue::Decimal(d) if crate::r2rml::iceberg_numeric_stats_enabled() => {
            scan_value_from_bigdecimal(d)
        }
        _ => None,
    }
}

/// Decompose a `BigDecimal` into `ScanValue::Decimal { unscaled, precision, scale }`.
/// Normalizes first (so `9.99` and `9.990` decompose identically), then reads the
/// unscaled mantissa and base-10 exponent. Returns `None` if the unscaled value
/// exceeds i128 or the scale exceeds i8 — those stay with the in-engine FILTER.
fn scan_value_from_bigdecimal(bd: &bigdecimal::BigDecimal) -> Option<ScanValue> {
    use num_traits::ToPrimitive;
    // value = unscaled * 10^-scale. Normalize so scale-equivalent forms (9.99 vs
    // 9.990) decompose identically.
    let (unscaled_bi, scale) = bd.normalized().as_bigint_and_exponent();
    let scale = i8::try_from(scale).ok()?;
    let unscaled = unscaled_bi.to_i128()?;
    // precision is cosmetic for pruning (`decimal_cmp` ignores it); derive it from
    // the unscaled magnitude so it is self-consistent across scale-equivalent
    // forms, clamped to the decimal128 max.
    let precision = u8::try_from(unscaled.unsigned_abs().checked_ilog10().unwrap_or(0) + 1)
        .unwrap_or(38)
        .clamp(1, 38);
    Some(ScanValue::Decimal {
        unscaled,
        precision,
        scale,
    })
}

/// The subject var of a regular-predicate R2RML pattern that can join a
/// same-subject star: variable subject, constant predicate, no class/TM filter,
/// and either a fresh object var (distinct from the subject) or a constant-object
/// equality. `None` (not eligible) for bound-subject patterns. Constant-object
/// members become existence constraints on the star; var-object members produce
/// bindings.
fn star_member_subject(p: &R2rmlPattern) -> Option<VarId> {
    let subject_var = p.subject_var?;
    let base_ok = p.predicate_filter.is_some()
        && p.class_filter.is_none()
        && p.triples_map_iri.is_none()
        && p.star_bindings.is_empty();
    if !base_ok {
        return None;
    }
    let var_object = p.object_var.is_some_and(|obj| obj != subject_var);
    let const_object = p.object_var.is_none() && p.object_constant.is_some();
    (var_object || const_object).then_some(subject_var)
}

/// The subject var of a pure `rdf:type` pattern (`?s a ex:Class`): variable
/// subject, a class filter, no object var, no predicate, no star members. `None`
/// (not eligible) for bound-subject patterns. Candidates to fuse into a
/// same-subject star (or, failing that, to run as a subject-only scan).
fn class_only_subject(p: &R2rmlPattern) -> Option<VarId> {
    let subject_var = p.subject_var?;
    let eligible = p.class_filter.is_some()
        && p.object_var.is_none()
        && p.predicate_filter.is_none()
        && p.triples_map_iri.is_none()
        && p.star_bindings.is_empty();
    eligible.then_some(subject_var)
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
        // PR-3 fix (b'): full class fusion is unsafe (the class lives in a
        // different TriplesMap than the base predicate — the vertically
        // partitioned shape). We must NOT fuse (that would drop the split TM and
        // lose bindings). But if the class-declaring maps are subject-template
        // DISJOINT from every other map (`wildcard_class_fusion_is_safe`, the same
        // predicate the wildcard-crawl fuse reuses), we can still prune the star's
        // resolution fan-out to class-declaring maps as a RESOLUTION-ONLY hint:
        // the class stays its own scan joined on the subject, and disjointness
        // guarantees a pruned map's subjects could never survive that join. Leaves
        // `class_groups` untouched, so the standalone class scan is still emitted.
        if mapping.is_some_and(|m| wildcard_class_fusion_is_safe(m, &class)) {
            base.class_prune_hint = Some(class);
        }
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

/// PR-F20 kill switch: `FLUREE_R2RML_REF_TARGET_PRUNE` (default ON). When off,
/// the RefObjectMap-target resolution prune never fires — byte-identical to the
/// pre-F20 shared-predicate fan-out.
fn ref_target_prune_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| super::env_switch_enabled("FLUREE_R2RML_REF_TARGET_PRUNE"))
}

/// PR-F20: the single class C a constant predicate `P` references as a
/// RefObjectMap parent, if unambiguous. Scans the mapping for a RefObjectMap POM
/// whose predicate is `P`; returns its parent TriplesMap's sole `rr:class`.
/// Returns `None` if `P` is not a RefObjectMap, the parent is missing, the parent
/// declares ≠1 class, or two RefObjectMaps for `P` disagree on the parent class
/// (ambiguous → decline).
fn ref_target_class(mapping: &CompiledR2rmlMapping, predicate: &str) -> Option<String> {
    let mut found: Option<String> = None;
    for tm in mapping.triples_maps.values() {
        for pom in &tm.predicate_object_maps {
            if pom.predicate_map.as_constant() != Some(predicate) {
                continue;
            }
            let ObjectMap::RefObjectMap(rom) = &pom.object_map else {
                // The same predicate also has a non-ref object map → not a clean
                // FK, decline rather than guess.
                return None;
            };
            let parent = mapping.triples_maps.get(&rom.parent_triples_map)?;
            let classes = parent.classes();
            if classes.len() != 1 {
                return None;
            }
            match &found {
                None => found = Some(classes[0].clone()),
                Some(prev) if *prev == classes[0] => {}
                Some(_) => return None,
            }
        }
    }
    found
}

/// PR-F20: compute the RefObjectMap-target resolution prune targets for one BGP
/// scope. A star subject `?o` qualifies for a `class_prune_hint` derived from its
/// FK parent's class C iff (invariant A, join-var provenance) `?o`'s ONLY binding
/// in the scope is exactly ONE RefObjectMap star member (unique parent class C),
/// and (invariant B, template-disjointness) `wildcard_class_fusion_is_safe(mapping,
/// C)` holds.
///
/// Conservative first cut (single-required-ref): `?o` DECLINES if it is the object
/// of a second star member, PRODUCED by any other pattern in `other_patterns`, or
/// carries a standalone class assertion (`class_group_subjects`). "Produced by"
/// uses [`Pattern::produced_vars`] deliberately (NOT `referenced_vars`): a pattern
/// that only *constrains* `?o` — `FILTER`, `MINUS`, `(NOT) EXISTS` — never rebinds
/// it, so it must not decline (filters can't change `?o`'s provenance). `produced_vars`
/// DOES surface the two binding sources a naive triple-scan misses: a `BIND(expr AS
/// ?o)` target and a property-path endpoint. A `?o` in predicate position is itself
/// produced by whatever pattern carries it, so it lands here too. The (B) check is
/// mandatory — it must not rest on this dataset's templates happening to be disjoint
/// (the F10 vertical-partition trap).
///
/// **Soundness under a CROSS-SCOPE pre-bound `?o`** (invisible to this in-scope
/// scan — seeded by an outer pattern / cross-graph join / `VALUES` outside the
/// GRAPH block): the prune stays sound by CONJUNCTION. `?o` still appears as the
/// required RefObjectMap member in this scope, so the scan of that member restricts
/// `?o` to actual FK objects of the parent (class C); a pre-bound non-C `?o` yields
/// no FK row and its whole solution dies — identically whether or not the sibling
/// `?o <pred>` star is pruned to C. So pruning cannot drop a row the un-pruned plan
/// would keep. (Tested in-scope by the live q031 ON/OFF parity; the cross-scope
/// variant is not expressible at this static-analysis layer — see the sketch.)
fn compute_ref_prune_targets(
    star_groups: &[(VarId, Vec<R2rmlPattern>)],
    other_patterns: &[Pattern],
    class_group_subjects: &[VarId],
    mapping: Option<&CompiledR2rmlMapping>,
) -> std::collections::HashMap<VarId, String> {
    let mut out = std::collections::HashMap::new();
    let Some(mapping) = mapping else {
        return out;
    };
    // Object-var occurrence count + RefObjectMap parent class, across all star
    // members. `None` in `ref_target` marks a var seen as an object with a
    // non-ref/ambiguous binding (declines).
    let mut obj_count: std::collections::HashMap<VarId, usize> = std::collections::HashMap::new();
    let mut ref_target: std::collections::HashMap<VarId, Option<String>> =
        std::collections::HashMap::new();
    for (_subject, members) in star_groups {
        for m in members {
            let Some(ov) = m.object_var else { continue };
            *obj_count.entry(ov).or_insert(0) += 1;
            let cls = m
                .predicate_filter
                .as_deref()
                .and_then(|p| ref_target_class(mapping, p));
            match ref_target.get(&ov) {
                None => {
                    ref_target.insert(ov, cls);
                }
                Some(Some(prev)) if cls.as_ref() == Some(prev) => {}
                Some(_) => {
                    ref_target.insert(ov, None);
                }
            }
        }
    }
    // Invariant-A pollution: any var a pattern in this scope BINDS (produces)
    // outside its lone FK member, or a standalone class assertion, declines the
    // prune. `produced_vars` (not `referenced_vars`) so FILTER/MINUS/EXISTS
    // constraints on `?o` do NOT decline, while BIND targets and path endpoints do.
    let mut polluted: std::collections::HashSet<VarId> =
        class_group_subjects.iter().copied().collect();
    for p in other_patterns {
        polluted.extend(p.produced_vars());
    }
    for (ov, cls) in &ref_target {
        let Some(class) = cls else { continue };
        if obj_count.get(ov) == Some(&1)
            && !polluted.contains(ov)
            && wildcard_class_fusion_is_safe(mapping, class)
        {
            out.insert(*ov, class.clone());
        }
    }
    out
}

/// Whether wildcard→class fusion is enabled. Read once from
/// `FLUREE_R2RML_CRAWL_CLASS_FUSION` (family falsy spellings,
/// [`super::env_switch_enabled`]; `fluree-db-api`'s `crawl::env_flag_enabled`
/// parses this same variable and must keep the same spellings).
/// The master crawl kill-switch (`crawl::crawl_expand_enabled`) is COUPLED to
/// this: expand-on + fusion-off would route a browse through the UNFUSED crawl
/// (a 16-table fan-out + shared-catalog 429 storm — worse than today's fast
/// empty result), so disabling fusion also disables crawl expansion there.
fn wildcard_class_fusion_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| super::env_switch_enabled("FLUREE_R2RML_CRAWL_CLASS_FUSION"))
}

/// A standalone variable-predicate wildcard scan (`?s ?p ?o`) on `subject`:
/// binds both predicate and object, carries no predicate/class filter, and is
/// not a fused star. This is the shape a subgraph crawl injects; class-
/// constraining it prunes its TriplesMap fan-out to the queried class.
fn is_standalone_wildcard(rp: &R2rmlPattern, subject: VarId) -> bool {
    rp.subject_var == Some(subject)
        && rp.predicate_var.is_some()
        && rp.object_var.is_some()
        && rp.predicate_filter.is_none()
        && rp.class_filter.is_none()
        && rp.star_bindings.is_empty()
        && rp.star_constraints.is_empty()
}

/// A standalone projected-type scan (`?s a ?type`) on `subject` with no class
/// filter yet. Co-located with a crawl's wildcard; class-constraining it keeps
/// the scan on the queried class's TriplesMaps while still projecting every
/// class those maps declare.
fn is_standalone_type_var(rp: &R2rmlPattern, subject: VarId) -> bool {
    rp.subject_var == Some(subject)
        && rp.type_var.is_some()
        && rp.class_filter.is_none()
        && rp.predicate_filter.is_none()
        && rp.object_var.is_none()
}

/// Try to fuse the lone class `class` into a same-subject standalone wildcard by
/// setting its `class_filter`. Returns `true` iff a wildcard was constrained (so
/// the caller drops the now-redundant class scan). Refuses — leaving the wildcard
/// unconstrained and the class scan standalone — when reasoning is active, the
/// kill-switch is off, the mapping is unavailable, there is no wildcard to
/// constrain, or the fusion is not provably safe ([`wildcard_class_fusion_is_safe`]).
///
/// Type-var handling has two modes:
/// - **Browse crawl** (`crawl_active`, exactly one co-located `?s a ?type`): MERGE
///   the type-var into the wildcard (set `wildcard.type_var`) and REMOVE the
///   standalone type-var pattern, so the crawl is a SINGLE scan that receives the
///   downstream LIMIT budget (the standalone type-var is otherwise the topmost
///   budgeted scan and starves the wildcard). The fused operator then emits the
///   per-`(predicate,object)` × declared-class cartesian — identical to the
///   two-scan inner join for the single-TriplesMap-per-subject case, which the
///   crawl regroup dedups regardless.
/// - **Otherwise** (hand-written SPARQL, multiple type-vars, or fusion off): leave
///   the type-var a standalone scan and only class-constrain it, preserving the
///   known-correct two-scan plan.
fn try_fuse_wildcard_class(
    patterns: &mut Vec<Pattern>,
    subject: VarId,
    class: &str,
    mapping: Option<&CompiledR2rmlMapping>,
    reasoning_active: bool,
    crawl_active: bool,
) -> bool {
    // Reasoning refusal: the class prune is an EXACT `rr:class` match, so a
    // subject entailed into a superclass whose TriplesMap declares only a
    // subclass would be dropped. Refuse defensively when any entailment runs.
    if reasoning_active {
        return false;
    }
    if !wildcard_class_fusion_enabled() {
        return false;
    }
    // Proving safety needs the mapping's subject templates; without it, refuse.
    let Some(mapping) = mapping else {
        return false;
    };
    let has_wildcard = patterns
        .iter()
        .any(|p| matches!(p, Pattern::R2rml(rp) if is_standalone_wildcard(rp, subject)));
    if !has_wildcard {
        return false;
    }
    if !wildcard_class_fusion_is_safe(mapping, class) {
        return false;
    }

    // Decide whether to MERGE the projected type-var into the wildcard. Only for
    // the browse crawl, and only when EXACTLY ONE standalone type-var exists for
    // this subject: `R2rmlPattern::type_var` is an `Option<VarId>` (holds one), so
    // a `?s a ?t1 . ?s a ?t2` shape must keep the two-scan plan rather than drop a
    // binding. Capture the type-var's VarId now (before the mutation loop).
    let type_var_count = patterns
        .iter()
        .filter(|p| matches!(p, Pattern::R2rml(rp) if is_standalone_type_var(rp, subject)))
        .count();
    let do_merge = crawl_active && type_var_count == 1;
    let merged_type_var: Option<VarId> = if do_merge {
        patterns.iter().find_map(|p| match p {
            Pattern::R2rml(rp) if is_standalone_type_var(rp, subject) => rp.type_var,
            _ => None,
        })
    } else {
        None
    };

    let mut fused = false;
    for p in patterns.iter_mut() {
        if let Pattern::R2rml(rp) = p {
            if is_standalone_wildcard(rp, subject) {
                rp.class_filter = Some(class.to_string());
                // Merge: bind the projected class in the SAME scan.
                if let Some(tv) = merged_type_var {
                    rp.type_var = Some(tv);
                }
                fused = true;
            } else if is_standalone_type_var(rp, subject) && !do_merge {
                // Two-scan path: class-constrain the standalone type-var so its
                // scan is subject-only over the queried class's TriplesMaps.
                // On the merge path we deliberately leave it untouched (no
                // `class_filter`) so `is_standalone_type_var` still matches it
                // for removal below.
                rp.class_filter = Some(class.to_string());
            }
        }
    }

    // Remove the now-merged standalone type-var (only on the success path, and
    // only when merging). It still matches `is_standalone_type_var` because the
    // merge branch above left its `class_filter` unset. Removal is by predicate
    // (not index), scoped to this subject, so it cannot disturb another subject's
    // patterns as the caller iterates its class groups.
    if fused && do_merge {
        patterns
            .retain(|p| !matches!(p, Pattern::R2rml(rp) if is_standalone_type_var(rp, subject)));
    }

    fused
}

/// Whether constraining a wildcard to `class_iri` cannot drop any triple.
///
/// Unlike [`class_fusion_is_safe`] (which is PREDICATE-keyed — a wildcard has no
/// base predicate), this is keyed on SUBJECT-TEMPLATE disjointness. Setting
/// `class_filter` limits the wildcard's scan to TriplesMaps that declare the
/// class; any OTHER TriplesMap that could produce a subject shared with the
/// class's subjects would then be skipped, silently dropping its triples (the
/// vertical-partition hazard: `TM_A person/{id}`+Person+name, `TM_B
/// person/{id}`+email). So fuse only when every TriplesMap that does NOT declare
/// the class is provably DISJOINT (by subject template) from every
/// class-declaring TriplesMap's template.
///
/// Conservative and sound: "disjoint" means neither template's constant prefix
/// is a string-prefix of the other (they diverge inside the constant region, so
/// no generated IRI can coincide) — NOT mere string inequality. A column/
/// constant subject (no template) cannot be proven disjoint, so its presence on
/// a relevant map forces a refusal. For an auto-generated Iceberg mapping (one
/// TriplesMap per table, one class each, a unique `.../TABLE/{PK}` template)
/// every non-class map is prefix-disjoint, so this fires and prunes 16→1.
fn wildcard_class_fusion_is_safe(mapping: &CompiledR2rmlMapping, class_iri: &str) -> bool {
    // Subject templates of every TriplesMap that declares the class. A class map
    // with a column/constant subject can't anchor disjointness reasoning.
    let class_maps = mapping.find_maps_for_class(class_iri);
    if class_maps.is_empty() {
        return false;
    }
    let mut class_templates: Vec<&str> = Vec::with_capacity(class_maps.len());
    for tm in &class_maps {
        match tm.subject_map.template.as_deref() {
            Some(t) => class_templates.push(t),
            None => return false,
        }
    }
    // Every non-class TriplesMap must be provably disjoint from ALL class
    // templates; a non-template (column/constant) subject can't be proven so.
    for tm in mapping.triples_maps.values() {
        if tm.classes().iter().any(|c| c == class_iri) {
            continue;
        }
        match tm.subject_map.template.as_deref() {
            Some(t) => {
                if !class_templates
                    .iter()
                    .all(|ct| templates_provably_disjoint(ct, t))
                {
                    return false;
                }
            }
            None => return false,
        }
    }
    true
}

/// The constant prefix of an `rr:template` — everything before the first `{`
/// placeholder (the whole string when there is no placeholder). Emitted verbatim
/// by `expand_template`, so every IRI a template can produce starts with it —
/// which is what makes the operator's bound-subject TriplesMap prune sound.
pub(crate) fn constant_prefix(template: &str) -> &str {
    match template.find('{') {
        Some(i) => &template[..i],
        None => template,
    }
}

/// Whether two subject templates provably generate disjoint IRI sets: neither
/// constant prefix is a string-prefix of the other, so every generated IRI of
/// one differs from every generated IRI of the other within the constant region
/// (before any placeholder value can matter). Equal prefixes are treated as
/// overlapping (not disjoint) — conservative.
fn templates_provably_disjoint(a: &str, b: &str) -> bool {
    let pa = constant_prefix(a);
    let pb = constant_prefix(b);
    !pa.starts_with(pb) && !pb.starts_with(pa)
}

/// Convert a triple pattern to an R2RML pattern.
///
/// Returns `None` if the pattern cannot be converted (e.g., subject is a literal).
pub fn convert_triple_to_r2rml(
    tp: &TriplePattern,
    graph_source_id: &str,
    snapshot: &LedgerSnapshot,
) -> Option<R2rmlPattern> {
    // Extract the subject: a variable, or a constant (bound) IRI the operator
    // matches against each row's materialized subject. Exactly one is set.
    let (subject_var, subject_constant): (Option<VarId>, Option<String>) = match &tp.s {
        Ref::Var(v) => (Some(*v), None),
        Ref::Iri(iri) => (None, Some(iri.to_string())),
        // A bound SID subject we cannot decode to an IRI is left unconverted.
        Ref::Sid(sid) => {
            let iri = snapshot.decode_sid(sid)?;
            (None, Some(iri))
        }
    };

    // Build a pattern for `object_var`, carrying either the subject variable or
    // the constant subject IRI (exactly one of the pair above is set).
    let make_pattern = |object_var: Option<VarId>| -> R2rmlPattern {
        match (subject_var, subject_constant.as_deref()) {
            (Some(sv), _) => R2rmlPattern::new(graph_source_id, sv, object_var),
            (None, Some(sc)) => R2rmlPattern::new_bound_subject(graph_source_id, sc, object_var),
            (None, None) => unreachable!("subject is always a var or a constant IRI"),
        }
    };

    // Check if this is an rdf:type pattern
    // Use Term::is_rdf_type() to handle both Term::Sid and Term::Iri
    let is_type_pattern = tp.p.is_rdf_type();

    if is_type_pattern {
        // rdf:type pattern. Both forms reduce to the SAME class-driven TriplesMap
        // scan a SPARQL `a` produces — the class is either a constraint or a
        // projected variable:
        //   `?s rdf:type ex:Class` (FQL `@type: ex:Class`) → `class_filter`: the
        //      scan is limited to TriplesMaps declaring the class; no object var.
        //   `?s rdf:type ?type`    (FQL `@type: ?type`)    → `type_var`: the scan
        //      visits every map and the operator binds `?type` to each matched
        //      subject's declared class IRI (previously the variable was dropped,
        //      leaving `?type` unbound / `null`).
        // `object_var` stays `None` in both cases; the class is drawn from the
        // mapping, never a table column.
        let mut pattern = make_pattern(None);
        match &tp.o {
            Term::Sid(sid) => {
                if let Some(class_iri) = snapshot.decode_sid(sid) {
                    pattern = pattern.with_class(class_iri);
                }
            }
            Term::Iri(iri) => {
                pattern = pattern.with_class(iri.to_string());
            }
            Term::Value(fluree_db_core::FlakeValue::Ref(sid)) => {
                if let Some(class_iri) = snapshot.decode_sid(sid) {
                    pattern = pattern.with_class(class_iri);
                }
            }
            // Variable class: project it instead of filtering on it.
            Term::Var(v) => {
                pattern = pattern.with_type_var(*v);
            }
            _ => {}
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

    // A variable predicate (`?s ?p ?o` / `<iri> ?p ?o`) is projected: the
    // operator binds `?p` to each materialized triple's predicate IRI, so a
    // wildcard scan yields the predicate instead of leaving it `null`.
    let predicate_var = match &tp.p {
        Ref::Var(v) => Some(*v),
        _ => None,
    };

    // Extract the object: a variable, or a constant equality constraint the
    // operator enforces. A constant predicate is required (to resolve the map).
    //   - Literal (string/integer/boolean/date, loose-matchable datatype) →
    //     Scalar (also emits a scan filter for pruning).
    //   - Decimal / big-integer / double literal → numeric operator-only match.
    //   - Bound IRI / ref object (`?s edw:geography <geo/1>`) → Iri.
    // Language-tagged / custom-typed literals need strict matching and are left
    // unconverted rather than mismatched.
    let object_constant: Option<ObjectConstant> = match &tp.o {
        // A ref object can arrive as a typed value (`FlakeValue::Ref`); decode it
        // to an IRI so it takes the same operator-enforced path as Term::Sid/Iri.
        Term::Value(FlakeValue::Ref(sid)) if predicate_filter.is_some() => {
            snapshot.decode_sid(sid).map(ObjectConstant::Iri)
        }
        Term::Value(v) if predicate_filter.is_some() && is_loose_matchable_datatype(&tp.dtc) => {
            const_object(v)
        }
        Term::Iri(iri) if predicate_filter.is_some() => Some(ObjectConstant::Iri(iri.to_string())),
        Term::Sid(sid) if predicate_filter.is_some() => {
            snapshot.decode_sid(sid).map(ObjectConstant::Iri)
        }
        _ => None,
    };
    let object_var = match (&tp.o, &object_constant) {
        (Term::Var(v), _) => Some(*v),
        (_, Some(_)) => None,
        // Bound object we cannot yet convert.
        _ => return None,
    };

    let mut pattern = make_pattern(object_var);
    if let Some(pred_iri) = predicate_filter {
        pattern = pattern.with_predicate(pred_iri);
    }
    // A variable-predicate wildcard (`?s ?p ?o` or the bound-subject
    // `<iri> ?p ?o`) carries a `predicate_var` so the operator binds `?p` to each
    // triple's predicate IRI. This is what makes a bound-subject wildcard (the
    // UI's subject inspector) resolvable: previously it was left unconverted
    // because there was no field to bind `?p`.
    if object_var.is_some() {
        if let Some(pv) = predicate_var {
            pattern = pattern.with_predicate_var(pv);
        }
    }
    pattern.object_constant = object_constant;

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

    /// Extract the single triple pattern lowered from a `where` clause.
    #[cfg(test)]
    fn only_triple(q: &crate::ir::Query) -> TriplePattern {
        q.patterns
            .iter()
            .find_map(|p| match p {
                Pattern::Triple(tp) => Some(tp.clone()),
                _ => None,
            })
            .expect("expected a single triple pattern")
    }

    /// FQL `@type` must lower to the SAME `rdf:type` scan SPARQL `a` produces.
    ///
    /// SPARQL `a` lowers the predicate to `Ref::Iri(rdf::TYPE)` (see
    /// `fluree-db-sparql` `lower::path`); this test parses the FQL `@type` surface
    /// and asserts (1) it lowers to the identical `rdf:type` predicate, and (2) it
    /// converts to the identical R2RML type-scan — a `class_filter` for a bound
    /// class, and a `type_var` (binding the class IRI, not dropped) for a variable
    /// class. This is the regression guard for the FQL-vs-SPARQL by-class parity
    /// bug: FQL `@type` previously produced no type binding for a variable class.
    #[test]
    fn fql_type_lowers_to_same_rdf_type_scan_as_sparql_a() {
        use crate::parse::parse_query;
        use crate::var_registry::VarRegistry;
        use fluree_db_core::LedgerSnapshot;

        // `LedgerSnapshot` is both the IRI encoder (for parse) and the snapshot
        // (for convert's `decode_sid`, unused here since class objects stay IRIs).
        let snapshot = LedgerSnapshot::genesis("test/main");
        let class = "http://example.org/Geography";

        // --- Bound class: `@type: <class>` (≡ SPARQL `?s a <class>`) ---
        let mut vars = VarRegistry::new();
        let bound = serde_json::json!({
            "select": ["?s"],
            "where": {"@id": "?s", "@type": class},
        });
        let parsed = parse_query(&bound, &snapshot, &mut vars, None).expect("parse @type");
        let tp = only_triple(&parsed);
        assert!(
            tp.p.is_rdf_type(),
            "FQL @type must lower to the rdf:type predicate (same as SPARQL `a`)"
        );
        let pat = convert_triple_to_r2rml(&tp, "gs:main", &snapshot).expect("convertible");
        assert_eq!(
            pat.class_filter.as_deref(),
            Some(class),
            "bound @type ⇒ class_filter (the class-scan)"
        );
        assert_eq!(pat.type_var, None);
        assert_eq!(pat.object_var, None);
        assert_eq!(pat.predicate_filter, None);

        // --- Variable class: `@type: ?t` (≡ SPARQL `?s a ?t`) ---
        let mut vars = VarRegistry::new();
        let vquery = serde_json::json!({
            "select": ["?s", "?t"],
            "where": {"@id": "?s", "@type": "?t"},
        });
        let parsed = parse_query(&vquery, &snapshot, &mut vars, None).expect("parse @type var");
        let tp = only_triple(&parsed);
        assert!(tp.p.is_rdf_type());
        let want_s = vars.get_or_insert("?s");
        let want_t = vars.get_or_insert("?t");
        let pat = convert_triple_to_r2rml(&tp, "gs:main", &snapshot).expect("convertible");
        assert_eq!(
            pat.type_var,
            Some(want_t),
            "variable @type ⇒ type_var binds the class IRI (was dropped → null)"
        );
        assert_eq!(pat.class_filter, None);
        assert_eq!(pat.object_var, None);
        assert_eq!(pat.subject_var, Some(want_s));
    }

    /// A variable-predicate wildcard binds `?p` (subject inspector / crawl). Both
    /// the var-subject (`?s ?p ?o`) and bound-subject (`<iri> ?p ?o`) forms — the
    /// latter previously left unconverted for want of a predicate-var field.
    #[test]
    fn wildcard_predicate_binds_predicate_var() {
        use fluree_db_core::LedgerSnapshot;
        let snapshot = LedgerSnapshot::genesis("test/main");

        // ?s ?p ?o
        let tp = TriplePattern::new(Ref::Var(VarId(0)), Ref::Var(VarId(1)), Term::Var(VarId(2)));
        let pat = convert_triple_to_r2rml(&tp, "gs:main", &snapshot).expect("convertible");
        assert_eq!(pat.subject_var, Some(VarId(0)));
        assert_eq!(pat.predicate_var, Some(VarId(1)));
        assert_eq!(pat.object_var, Some(VarId(2)));
        assert!(pat.produced_vars().contains(&VarId(1)));

        // <iri> ?p ?o — bound subject wildcard is now convertible.
        let tp = TriplePattern::new(
            Ref::Iri("http://example.org/geography/1".into()),
            Ref::Var(VarId(1)),
            Term::Var(VarId(2)),
        );
        let pat = convert_triple_to_r2rml(&tp, "gs:main", &snapshot)
            .expect("bound-subject wildcard is convertible");
        assert_eq!(pat.subject_var, None);
        assert_eq!(
            pat.subject_constant.as_deref(),
            Some("http://example.org/geography/1")
        );
        assert_eq!(pat.predicate_var, Some(VarId(1)));
        assert_eq!(pat.object_var, Some(VarId(2)));
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

    // subject=VarId(0), object=VarId(1) → produced vars {0, 1}.
    fn scan() -> Pattern {
        Pattern::R2rml(R2rmlPattern::new("gs:main", VarId(0), Some(VarId(1))))
    }

    fn consumed_of(patterns: &[Pattern]) -> Option<&Expression> {
        patterns.iter().find_map(|p| match p {
            Pattern::R2rml(rp) => rp.consumed_filter.as_ref(),
            _ => None,
        })
    }

    #[test]
    fn loose_matchable_datatype_gate() {
        use fluree_vocab::xsd_names;
        // Untyped or any XSD datatype (string, integer, ...) → loose value match.
        assert!(is_loose_matchable_datatype(&None));
        assert!(is_loose_matchable_datatype(&Some(
            DatatypeConstraint::Explicit(Sid::new(XSD, xsd_names::STRING))
        )));
        assert!(is_loose_matchable_datatype(&Some(
            DatatypeConstraint::Explicit(Sid::new(XSD, xsd_names::INTEGER))
        )));
        // A language tag or a custom (non-XSD) datatype → strict; excluded (so
        // `"chat"@fr` or `"x"^^custom` never loose-match).
        assert!(!is_loose_matchable_datatype(&Some(
            DatatypeConstraint::LangTag("fr".into())
        )));
        assert!(!is_loose_matchable_datatype(&Some(
            DatatypeConstraint::Explicit(Sid::new(100, "myType"))
        )));
    }

    #[test]
    fn consumes_scan_local_filter() {
        // FILTER references only ?o (produced by the single scan): consumed.
        let mut patterns = vec![scan(), Pattern::Filter(Expression::Var(VarId(1)))];
        consume_scan_local_filters(&mut patterns);
        assert_eq!(patterns.len(), 1, "Filter pattern should be removed");
        assert!(consumed_of(&patterns).is_some());
    }

    #[test]
    fn keeps_filter_on_unproduced_var() {
        // ?2 is not produced by the scan: leave the FILTER in place.
        let mut patterns = vec![scan(), Pattern::Filter(Expression::Var(VarId(2)))];
        consume_scan_local_filters(&mut patterns);
        assert_eq!(patterns.len(), 2);
        assert!(consumed_of(&patterns).is_none());
    }

    #[test]
    fn keeps_filter_when_multiple_scans() {
        // Two scans: a filter could depend on a join, so never consume.
        let mut patterns = vec![
            scan(),
            Pattern::R2rml(R2rmlPattern::new("gs:main", VarId(2), Some(VarId(3)))),
            Pattern::Filter(Expression::Var(VarId(1))),
        ];
        consume_scan_local_filters(&mut patterns);
        assert_eq!(patterns.len(), 3);
        assert!(consumed_of(&patterns).is_none());
    }

    #[test]
    fn keeps_filter_when_non_scan_pattern_present() {
        // A BIND (or any non-scan/non-filter pattern) could produce or reorder
        // vars, so consumption is disabled for the whole group.
        let mut patterns = vec![
            scan(),
            Pattern::Bind {
                var: VarId(5),
                expr: Expression::Var(VarId(1)),
            },
            Pattern::Filter(Expression::Var(VarId(1))),
        ];
        consume_scan_local_filters(&mut patterns);
        assert_eq!(patterns.len(), 3);
        assert!(consumed_of(&patterns).is_none());
    }

    #[test]
    fn keeps_filter_with_unanalyzable_expression() {
        // A Resolved binding (stand-in for EXISTS/comprehension constructs) is
        // fail-closed: even though ?1 is produced, the filter is not consumed.
        let expr = Expression::and(vec![
            Expression::Var(VarId(1)),
            Expression::Resolved(Box::new(crate::binding::Binding::Unbound)),
        ]);
        let mut patterns = vec![scan(), Pattern::Filter(expr)];
        consume_scan_local_filters(&mut patterns);
        assert_eq!(patterns.len(), 2);
        assert!(consumed_of(&patterns).is_none());
    }

    #[test]
    fn keeps_metadata_read_filter() {
        // FILTER(labels(?o) = ...) references only the scan-produced ?1, but a
        // metadata read must route through the policy-aware async resolver, not
        // the consumed sync path — so it stays with the in-engine FILTER.
        use crate::ir::expression::Function;
        let expr = Expression::Call {
            func: Function::Labels,
            args: vec![Expression::Var(VarId(1))],
        };
        let mut patterns = vec![scan(), Pattern::Filter(expr)];
        consume_scan_local_filters(&mut patterns);
        assert_eq!(patterns.len(), 2);
        assert!(consumed_of(&patterns).is_none());
    }

    use fluree_db_r2rml::mapping::{
        ObjectMap, PredicateMap, PredicateObjectMap, RefObjectMap, TriplesMap,
    };

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

    // PR-3 fix (b'): a star base pattern for `subject`, with `pred` as its base.
    fn star_base(subject: VarId, pred: &str) -> R2rmlPattern {
        let mut base = R2rmlPattern::new("gs", subject, Some(VarId(99)));
        base.predicate_filter = Some(pred.to_string());
        base
    }

    // PR-3 fix (b'): full class fusion refused (a name-map lacks the class), but the
    // class-declaring map is subject-template DISJOINT from the other name-map, so a
    // resolution-only `class_prune_hint` is set (prunes the fan-out; class stays its
    // own scan). This is the q001 shared-member shape fix (a) alone can't prune.
    #[test]
    fn class_prune_hint_set_when_disjoint_but_not_fusable() {
        let store = TriplesMap::new("#Store", "dim_store")
            .with_subject_template("http://ex/store/{k}")
            .with_class(CLASS)
            .with_predicate_object(pom(PRED, "store_name"));
        let customer = TriplesMap::new("#Customer", "dim_customer")
            .with_subject_template("http://ex/customer/{k}")
            .with_predicate_object(pom(PRED, "full_name"));
        let mapping = CompiledR2rmlMapping::new(vec![store, customer]);
        let subject = VarId(1);
        let mut base = star_base(subject, PRED);
        let mut class_groups = vec![(
            subject,
            vec![R2rmlPattern::new("gs", subject, None).with_class(CLASS)],
        )];
        fuse_class_if_safe(&mut base, &mut class_groups, subject, Some(&mapping));
        // Not fused for materialization (class stays separate), but prune hint set.
        assert_eq!(base.class_filter, None);
        assert_eq!(base.class_prune_hint.as_deref(), Some(CLASS));
        assert_eq!(class_groups.len(), 1, "class scan stays standalone");
    }

    // PR-3 fix (b') soundness — the vertical-partition COUNTEREXAMPLE. Class in one
    // TM, the star's predicate in another, SAME subject template ⇒ overlapping
    // prefixes ⇒ pruning would drop rows the class-scan join keeps. `class_prune_hint`
    // must stay unset so the base map is not pruned.
    #[test]
    fn class_prune_hint_refused_under_vertical_partition() {
        let store_attrs = TriplesMap::new("#StoreAttrs", "dim_store")
            .with_subject_template("http://ex/store/{k}")
            .with_predicate_object(pom(PRED, "store_name"));
        let store_class = TriplesMap::new("#StoreClass", "dim_store_class")
            .with_subject_template("http://ex/store/{k}")
            .with_class(CLASS);
        let mapping = CompiledR2rmlMapping::new(vec![store_attrs, store_class]);
        let subject = VarId(1);
        let mut base = star_base(subject, PRED);
        let mut class_groups = vec![(
            subject,
            vec![R2rmlPattern::new("gs", subject, None).with_class(CLASS)],
        )];
        fuse_class_if_safe(&mut base, &mut class_groups, subject, Some(&mapping));
        assert_eq!(base.class_filter, None, "must not fuse");
        assert_eq!(
            base.class_prune_hint, None,
            "overlapping subject templates ⇒ pruning unsound ⇒ hint refused"
        );
    }

    // ---- PR-F20: RefObjectMap-target resolution prune (invariants A + B) ----

    const NAME: &str = "http://ex/name";
    const PRODUCT_PRED: &str = "http://ex/product";
    const PRODUCT_CLASS: &str = "http://ex/Product";
    const INV_CLASS: &str = "http://ex/InventorySnapshot";

    fn ref_pom(pred: &str, parent_tm: &str) -> PredicateObjectMap {
        PredicateObjectMap {
            predicate_map: PredicateMap::constant(pred),
            object_map: ObjectMap::RefObjectMap(RefObjectMap::new(parent_tm, "FK", "PK")),
        }
    }

    // A star member (used as `?subj <pred> ?obj`): its `object_var`/`predicate_filter`
    // are what `compute_ref_prune_targets` reads.
    fn member(subj: VarId, pred: &str, obj: VarId) -> R2rmlPattern {
        let mut m = R2rmlPattern::new("gs", subj, Some(obj));
        m.predicate_filter = Some(pred.to_string());
        m
    }

    // The q031 mapping shape: a fact TM with a RefObjectMap `product` → DimProduct,
    // DimProduct (class Product, maps `name`), and a second `name`-bearing dim with a
    // DISJOINT subject template (the shared-predicate fan-out target).
    fn q031_mapping() -> CompiledR2rmlMapping {
        let product = TriplesMap::new("#DimProduct", "dim_product")
            .with_subject_template("http://ex/product/{k}")
            .with_class(PRODUCT_CLASS)
            .with_predicate_object(pom(NAME, "product_name"));
        let customer = TriplesMap::new("#DimCustomer", "dim_customer")
            .with_subject_template("http://ex/customer/{k}")
            .with_predicate_object(pom(NAME, "full_name"));
        let fact = TriplesMap::new("#Fact", "fact_inv")
            .with_subject_template("http://ex/inv/{k}")
            .with_class(INV_CLASS)
            .with_predicate_object(ref_pom(PRODUCT_PRED, "#DimProduct"));
        CompiledR2rmlMapping::new(vec![product, customer, fact])
    }

    // `?inv product ?p . ?p name ?pn` — ?p is bound SOLELY as the DimProduct FK
    // object, so its `name` star is prunable to the Product class.
    fn q031_star_groups() -> Vec<(VarId, Vec<R2rmlPattern>)> {
        vec![
            (VarId(0), vec![member(VarId(0), PRODUCT_PRED, VarId(1))]), // ?inv product ?p
            (VarId(1), vec![member(VarId(1), NAME, VarId(2))]),         // ?p name ?pn
        ]
    }

    #[test]
    fn f20_ref_prune_fires_on_clean_fk_object() {
        let m = q031_mapping();
        let out = compute_ref_prune_targets(&q031_star_groups(), &[], &[], Some(&m));
        assert_eq!(
            out.get(&VarId(1)).map(String::as_str),
            Some(PRODUCT_CLASS),
            "?p's name star is pruned to the FK parent's class"
        );
    }

    #[test]
    fn f20_declines_when_p_referenced_by_values() {
        // VALUES ?p { … } could bind ?p to a non-Product IRI → decline.
        let m = q031_mapping();
        let values = Pattern::Values {
            vars: vec![VarId(1)],
            rows: vec![],
        };
        let out = compute_ref_prune_targets(&q031_star_groups(), &[values], &[], Some(&m));
        assert!(out.is_empty(), "VALUES-bound ?p must decline");
    }

    #[test]
    fn f20_declines_when_p_referenced_by_union() {
        // A UNION branch binding ?p → decline (referenced_vars surfaces it).
        let m = q031_mapping();
        let union = Pattern::Union(vec![vec![Pattern::Values {
            vars: vec![VarId(1)],
            rows: vec![],
        }]]);
        let out = compute_ref_prune_targets(&q031_star_groups(), &[union], &[], Some(&m));
        assert!(out.is_empty(), "UNION-bound ?p must decline");
    }

    #[test]
    fn f20_declines_on_second_object_binding() {
        // ?p is the object of a second (non-ref) star member → not a lone FK → decline.
        let mut sg = q031_star_groups();
        sg.push((VarId(3), vec![member(VarId(3), "http://ex/other", VarId(1))]));
        let m = q031_mapping();
        let out = compute_ref_prune_targets(&sg, &[], &[], Some(&m));
        assert!(out.is_empty(), "a second binding of ?p must decline");
    }

    #[test]
    fn f20_declines_on_different_parent_second_ref() {
        // ?p is the object of two RefObjectMaps with DIFFERENT parents → ambiguous class.
        let store = TriplesMap::new("#DimStore", "dim_store")
            .with_subject_template("http://ex/store/{k}")
            .with_class("http://ex/Store")
            .with_predicate_object(pom(NAME, "store_name"));
        let product = TriplesMap::new("#DimProduct", "dim_product")
            .with_subject_template("http://ex/product/{k}")
            .with_class(PRODUCT_CLASS)
            .with_predicate_object(pom(NAME, "product_name"));
        let fact = TriplesMap::new("#Fact", "fact_inv")
            .with_subject_template("http://ex/inv/{k}")
            .with_class(INV_CLASS)
            .with_predicate_object(ref_pom(PRODUCT_PRED, "#DimProduct"))
            .with_predicate_object(ref_pom("http://ex/store", "#DimStore"));
        let m = CompiledR2rmlMapping::new(vec![store, product, fact]);
        let sg = vec![
            (
                VarId(0),
                vec![
                    member(VarId(0), PRODUCT_PRED, VarId(1)),
                    member(VarId(0), "http://ex/store", VarId(1)),
                ],
            ),
            (VarId(1), vec![member(VarId(1), NAME, VarId(2))]),
        ];
        let out = compute_ref_prune_targets(&sg, &[], &[], Some(&m));
        assert!(out.is_empty(), "different-parent second ref must decline");
    }

    #[test]
    fn f20_declines_under_template_sharing_invariant_b() {
        // A second map SHARES DimProduct's subject template AND maps `name` — the
        // F10 vertical-partition trap. `wildcard_class_fusion_is_safe(Product)` is
        // false → decline (condition B, mandatory).
        let product = TriplesMap::new("#DimProduct", "dim_product")
            .with_subject_template("http://ex/product/{k}")
            .with_class(PRODUCT_CLASS)
            .with_predicate_object(pom(NAME, "product_name"));
        let product_extra = TriplesMap::new("#DimProductExtra", "dim_product_extra")
            .with_subject_template("http://ex/product/{k}") // SAME template, no class
            .with_predicate_object(pom(NAME, "alt_name"));
        let fact = TriplesMap::new("#Fact", "fact_inv")
            .with_subject_template("http://ex/inv/{k}")
            .with_class(INV_CLASS)
            .with_predicate_object(ref_pom(PRODUCT_PRED, "#DimProduct"));
        let m = CompiledR2rmlMapping::new(vec![product, product_extra, fact]);
        let out = compute_ref_prune_targets(&q031_star_groups(), &[], &[], Some(&m));
        assert!(
            out.is_empty(),
            "template-sharing map ⇒ condition B fails ⇒ decline (would drop split rows)"
        );
    }

    #[test]
    fn f20_declines_when_p_carries_a_class_assertion() {
        // ?p a SomeClass in the query (a class_groups subject) → the class path owns
        // it; the ref-prune must not also fire. Decline.
        let m = q031_mapping();
        let out = compute_ref_prune_targets(&q031_star_groups(), &[], &[VarId(1)], Some(&m));
        assert!(out.is_empty(), "a class assertion on ?p must decline the ref-prune");
    }

    #[test]
    fn f20_declines_on_bind_target() {
        // BIND(expr AS ?p) produces ?p (arbitrary IRI) → decline. `produced_vars`
        // surfaces the Bind TARGET, which a naive triple-object scan would miss.
        let m = q031_mapping();
        let bind = Pattern::Bind {
            var: VarId(1),
            expr: Expression::Var(VarId(5)),
        };
        let out = compute_ref_prune_targets(&q031_star_groups(), &[bind], &[], Some(&m));
        assert!(out.is_empty(), "a BIND target ?p must decline");
    }

    #[test]
    fn f20_filter_on_p_does_not_decline() {
        // A FILTER referencing ?p CONSTRAINS but never binds it — provenance is
        // unchanged, so the prune must STILL fire (produced_vars(Filter) is empty).
        use crate::ir::expression::Function;
        let m = q031_mapping();
        let filter = Pattern::Filter(Expression::Call {
            func: Function::IsIri,
            args: vec![Expression::Var(VarId(1))],
        });
        let out = compute_ref_prune_targets(&q031_star_groups(), &[filter], &[], Some(&m));
        assert_eq!(
            out.get(&VarId(1)).map(String::as_str),
            Some(PRODUCT_CLASS),
            "a FILTER on ?p must not decline the prune"
        );
    }

    #[test]
    fn f20_declines_when_switch_off_is_handled_by_caller() {
        // Sanity: with the mapping absent, compute is a no-op (the caller also gates
        // on `ref_target_prune_enabled`). Documents the two off-ramps.
        let out = compute_ref_prune_targets(&q031_star_groups(), &[], &[], None);
        assert!(out.is_empty());
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

    // ---- Wildcard→class fusion (FIX 2) ---------------------------------------

    const CLASS2: &str = "http://example.org/Order";

    #[test]
    fn templates_disjoint_only_on_diverging_prefix() {
        // Prefix-disjoint per-table templates → disjoint.
        assert!(templates_provably_disjoint(
            "http://ex/person/{id}",
            "http://ex/order/{id}"
        ));
        // Equal templates → overlap (not disjoint).
        assert!(!templates_provably_disjoint(
            "http://ex/person/{id}",
            "http://ex/person/{id}"
        ));
        // One prefix a string-prefix of the other → conservatively not disjoint.
        assert!(!templates_provably_disjoint(
            "http://ex/p/{id}",
            "http://ex/p/{id}/x"
        ));
    }

    #[test]
    fn wildcard_fusion_safe_single_tm() {
        // Auto-generated Iceberg shape: one TriplesMap, one class, one template.
        let tm = TriplesMap::new("#TM", "people")
            .with_subject_template("http://example.org/person/{id}")
            .with_class(CLASS)
            .with_predicate_object(pom(PRED, "name"));
        let mapping = CompiledR2rmlMapping::new(vec![tm]);
        assert!(wildcard_class_fusion_is_safe(&mapping, CLASS));
    }

    #[test]
    fn wildcard_fusion_safe_disjoint_per_table_templates() {
        // Two tables with unique, prefix-disjoint subject templates: constraining
        // the wildcard to CLASS's table cannot touch the other table's subjects.
        let tm_a = TriplesMap::new("#TM_A", "people")
            .with_subject_template("http://example.org/person/{id}")
            .with_class(CLASS)
            .with_predicate_object(pom(PRED, "name"));
        let tm_b = TriplesMap::new("#TM_B", "orders")
            .with_subject_template("http://example.org/order/{id}")
            .with_class(CLASS2)
            .with_predicate_object(pom("http://example.org/total", "total"));
        let mapping = CompiledR2rmlMapping::new(vec![tm_a, tm_b]);
        assert!(wildcard_class_fusion_is_safe(&mapping, CLASS));
    }

    #[test]
    fn wildcard_fusion_unsafe_vertical_partition() {
        // TM_B shares TM_A's subject template but declares no class. Constraining
        // the wildcard to CLASS's TriplesMap would silently drop TM_B's triples.
        let tm_a = TriplesMap::new("#TM_A", "people")
            .with_subject_template("http://example.org/person/{id}")
            .with_class(CLASS)
            .with_predicate_object(pom(PRED, "name"));
        let tm_b = TriplesMap::new("#TM_B", "people_email")
            .with_subject_template("http://example.org/person/{id}")
            .with_predicate_object(pom("http://example.org/email", "email"));
        let mapping = CompiledR2rmlMapping::new(vec![tm_a, tm_b]);
        assert!(!wildcard_class_fusion_is_safe(&mapping, CLASS));
    }

    #[test]
    fn wildcard_fusion_unsafe_column_subject() {
        // A non-class TriplesMap with a COLUMN subject can't be proven disjoint.
        let tm_a = TriplesMap::new("#TM_A", "people")
            .with_subject_template("http://example.org/person/{id}")
            .with_class(CLASS);
        let mut tm_b = TriplesMap::new("#TM_B", "other");
        tm_b.subject_map = fluree_db_r2rml::mapping::SubjectMap::column("uri");
        tm_b = tm_b.with_predicate_object(pom("http://example.org/x", "x"));
        let mapping = CompiledR2rmlMapping::new(vec![tm_a, tm_b]);
        assert!(!wildcard_class_fusion_is_safe(&mapping, CLASS));
    }

    /// Run the crawl-shaped pattern set (`?s ?p ?o` + `?s a ?t` + `?s a CLASS`)
    /// through the rewriter and return the resulting R2RML patterns.
    fn rewrite_crawl(
        mapping: &CompiledR2rmlMapping,
        reasoning_active: bool,
        crawl_active: bool,
    ) -> Vec<R2rmlPattern> {
        use fluree_db_core::LedgerSnapshot;
        const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
        let snapshot = LedgerSnapshot::genesis("test/main");
        let patterns = vec![
            Pattern::Triple(TriplePattern::new(
                Ref::Var(VarId(0)),
                Ref::Var(VarId(1)),
                Term::Var(VarId(2)),
            )),
            Pattern::Triple(TriplePattern::new(
                Ref::Var(VarId(0)),
                Ref::Iri(RDF_TYPE.into()),
                Term::Var(VarId(3)),
            )),
            Pattern::Triple(TriplePattern::new(
                Ref::Var(VarId(0)),
                Ref::Iri(RDF_TYPE.into()),
                Term::Iri(CLASS.into()),
            )),
        ];
        rewrite_patterns_for_r2rml(
            &patterns,
            "gs:main",
            &snapshot,
            Some(mapping),
            reasoning_active,
            crawl_active,
        )
        .patterns
        .into_iter()
        .filter_map(|p| match p {
            Pattern::R2rml(rp) => Some(rp),
            _ => None,
        })
        .collect()
    }

    fn single_class_mapping() -> CompiledR2rmlMapping {
        let tm = TriplesMap::new("#TM", "people")
            .with_subject_template("http://example.org/person/{id}")
            .with_class(CLASS)
            .with_predicate_object(pom(PRED, "name"));
        CompiledR2rmlMapping::new(vec![tm])
    }

    #[test]
    fn wildcard_fusion_constrains_wildcard_and_type_var_when_safe() {
        // crawl_active = false → the hand-written / non-crawl two-scan plan: the
        // type-var is NOT merged, just class-constrained alongside the wildcard.
        let pats = rewrite_crawl(&single_class_mapping(), false, false);
        // Fusion consumes the standalone class scan: only the wildcard + type-var
        // remain, both now class-constrained.
        assert_eq!(pats.len(), 2, "class scan should be consumed by fusion");
        let wildcard = pats
            .iter()
            .find(|p| p.predicate_var.is_some())
            .expect("wildcard present");
        assert_eq!(wildcard.class_filter.as_deref(), Some(CLASS));
        let type_var = pats
            .iter()
            .find(|p| p.type_var.is_some())
            .expect("type-var present");
        assert_eq!(type_var.class_filter.as_deref(), Some(CLASS));
    }

    #[test]
    fn wildcard_fusion_refused_when_reasoning_active() {
        let pats = rewrite_crawl(&single_class_mapping(), true, false);
        // Refused: wildcard stays unconstrained and the class scan is standalone.
        let wildcard = pats
            .iter()
            .find(|p| p.predicate_var.is_some())
            .expect("wildcard present");
        assert_eq!(wildcard.class_filter, None);
        assert!(
            pats.iter().any(|p| p.class_filter.as_deref() == Some(CLASS)
                && p.predicate_var.is_none()
                && p.type_var.is_none()),
            "a standalone class scan must remain"
        );
    }

    #[test]
    fn wildcard_fusion_refused_for_vertical_partition() {
        // Same-template classless TM_B: fusion must be refused so the unconstrained
        // wildcard still returns TM_B's triples.
        let tm_a = TriplesMap::new("#TM_A", "people")
            .with_subject_template("http://example.org/person/{id}")
            .with_class(CLASS)
            .with_predicate_object(pom(PRED, "name"));
        let tm_b = TriplesMap::new("#TM_B", "people_email")
            .with_subject_template("http://example.org/person/{id}")
            .with_predicate_object(pom("http://example.org/email", "email"));
        let mapping = CompiledR2rmlMapping::new(vec![tm_a, tm_b]);
        let pats = rewrite_crawl(&mapping, false, false);
        let wildcard = pats
            .iter()
            .find(|p| p.predicate_var.is_some())
            .expect("wildcard present");
        assert_eq!(
            wildcard.class_filter, None,
            "vertical partition must not fuse"
        );
        assert!(pats
            .iter()
            .any(|p| p.class_filter.as_deref() == Some(CLASS) && p.predicate_var.is_none()));
    }

    #[test]
    fn crawl_merge_fuses_type_var_into_single_scan() {
        // crawl_active = true → the browse merge: the projected type-var is folded
        // into the wildcard and the standalone type-var scan is removed, leaving
        // EXACTLY ONE R2RML scan that binds ?p/?o AND ?type and carries the class
        // filter. This is what makes the single scan receive the LIMIT budget.
        let pats = rewrite_crawl(&single_class_mapping(), false, true);
        assert_eq!(
            pats.len(),
            1,
            "browse merge must collapse wildcard + type-var into one scan: {pats:?}"
        );
        let fused = &pats[0];
        assert!(
            fused.predicate_var.is_some(),
            "fused scan keeps the wildcard"
        );
        assert!(
            fused.object_var.is_some(),
            "fused scan keeps the object var"
        );
        assert!(
            fused.type_var.is_some(),
            "fused scan absorbs the projected type-var"
        );
        assert_eq!(
            fused.class_filter.as_deref(),
            Some(CLASS),
            "fused scan is class-constrained"
        );
    }

    #[test]
    fn crawl_merge_refused_for_two_type_vars() {
        // Two projected type-vars on one subject cannot both fit an Option<VarId>;
        // the merge is refused (keeps the two-scan plan) so no binding is dropped.
        use fluree_db_core::LedgerSnapshot;
        const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
        let snapshot = LedgerSnapshot::genesis("test/main");
        let mapping = single_class_mapping();
        let patterns = vec![
            Pattern::Triple(TriplePattern::new(
                Ref::Var(VarId(0)),
                Ref::Var(VarId(1)),
                Term::Var(VarId(2)),
            )),
            Pattern::Triple(TriplePattern::new(
                Ref::Var(VarId(0)),
                Ref::Iri(RDF_TYPE.into()),
                Term::Var(VarId(3)),
            )),
            Pattern::Triple(TriplePattern::new(
                Ref::Var(VarId(0)),
                Ref::Iri(RDF_TYPE.into()),
                Term::Var(VarId(4)),
            )),
            Pattern::Triple(TriplePattern::new(
                Ref::Var(VarId(0)),
                Ref::Iri(RDF_TYPE.into()),
                Term::Iri(CLASS.into()),
            )),
        ];
        let pats: Vec<R2rmlPattern> = rewrite_patterns_for_r2rml(
            &patterns,
            "gs:main",
            &snapshot,
            Some(&mapping),
            false,
            true,
        )
        .patterns
        .into_iter()
        .filter_map(|p| match p {
            Pattern::R2rml(rp) => Some(rp),
            _ => None,
        })
        .collect();
        // Two standalone type-vars survive (no merge), plus the wildcard.
        let type_var_scans = pats.iter().filter(|p| p.type_var.is_some()).count();
        assert_eq!(
            type_var_scans, 2,
            "two type-vars must NOT be merged (would drop a binding): {pats:?}"
        );
    }

    /// PR-3 fix (a) LOAD-BEARING INVARIANT: a same-subject star fuses only
    /// REQUIRED BGP members. An OPTIONAL member recurses to its own scope
    /// (`Pattern::Optional`, rewrite.rs:150) and must NEVER enter `star_bindings`,
    /// because fix (a) prunes any TriplesMap lacking a star predicate — if an
    /// OPTIONAL predicate were fused in, maps that legitimately lack it would be
    /// dropped, silently losing rows. This test trips loudly if a future
    /// optional-star-member feature ever violates that assumption.
    #[test]
    fn optional_star_member_is_not_fused_into_star() {
        use fluree_db_core::LedgerSnapshot;
        let snapshot = LedgerSnapshot::genesis("test/main");
        let tm = TriplesMap::new("#Store", "dim_store")
            .with_subject_template("http://ex/store/{k}")
            .with_predicate_object(pom("http://ex/name", "store_name"))
            .with_predicate_object(pom("http://ex/storeType", "store_type"))
            .with_predicate_object(pom("http://ex/channel", "channel"));
        let mapping = CompiledR2rmlMapping::new(vec![tm]);
        // Required star: name + storeType. OPTIONAL: channel.
        let patterns = vec![
            Pattern::Triple(TriplePattern::new(
                Ref::Var(VarId(0)),
                Ref::Iri("http://ex/name".into()),
                Term::Var(VarId(1)),
            )),
            Pattern::Triple(TriplePattern::new(
                Ref::Var(VarId(0)),
                Ref::Iri("http://ex/storeType".into()),
                Term::Var(VarId(2)),
            )),
            Pattern::Optional(vec![Pattern::Triple(TriplePattern::new(
                Ref::Var(VarId(0)),
                Ref::Iri("http://ex/channel".into()),
                Term::Var(VarId(3)),
            ))]),
        ];
        let result = rewrite_patterns_for_r2rml(
            &patterns,
            "gs:main",
            &snapshot,
            Some(&mapping),
            false,
            false,
        );
        // The OPTIONAL stays a separate scope.
        assert!(
            result
                .patterns
                .iter()
                .any(|p| matches!(p, Pattern::Optional(_))),
            "OPTIONAL must remain a separate scope: {:?}",
            result.patterns
        );
        // A required star formed (name+storeType) but the OPTIONAL channel is NOT
        // in any star's bindings/constraints/base.
        for p in &result.patterns {
            if let Pattern::R2rml(rp) = p {
                let touches_channel = rp.predicate_filter.as_deref() == Some("http://ex/channel")
                    || rp
                        .star_bindings
                        .iter()
                        .any(|(pred, _)| pred == "http://ex/channel")
                    || rp
                        .star_constraints
                        .iter()
                        .any(|(pred, _)| pred == "http://ex/channel");
                assert!(
                    !touches_channel,
                    "OPTIONAL member must not be fused into a star: {rp:?}"
                );
            }
        }
    }

    /// PR-0/0a: a non-lowered sub-scope that would evaluate against the R2RML
    /// source's empty native index (property/shortest path, subquery) is
    /// recorded in `unsupported` so the caller errors loudly instead of
    /// returning a silently-empty result (fluree/db virtual-dataset F1/F2).
    #[test]
    fn non_lowered_subscopes_are_flagged_unsupported() {
        use crate::ir::path::{PathModifier, PropertyPathPattern};
        use crate::ir::pattern::SubqueryPattern;
        let snapshot = LedgerSnapshot::genesis("test/main");

        // Transitive property path.
        let path = Pattern::PropertyPath(PropertyPathPattern::new(
            Ref::Var(VarId(0)),
            Sid::new(100, "knows"),
            PathModifier::OneOrMore,
            Ref::Var(VarId(1)),
        ));
        let r = rewrite_patterns_for_r2rml(&[path], "gs:main", &snapshot, None, false, false);
        assert_eq!(r.unsupported, vec!["property path"]);

        // Subquery.
        let sub = Pattern::Subquery(SubqueryPattern::new(vec![VarId(0)], vec![]));
        let r = rewrite_patterns_for_r2rml(&[sub], "gs:main", &snapshot, None, false, false);
        assert_eq!(r.unsupported, vec!["subquery"]);

        // A property path nested inside an OPTIONAL is caught via recursion.
        let opt = Pattern::Optional(vec![Pattern::PropertyPath(PropertyPathPattern::new(
            Ref::Var(VarId(0)),
            Sid::new(100, "knows"),
            PathModifier::ZeroOrMore,
            Ref::Var(VarId(1)),
        ))]);
        let r = rewrite_patterns_for_r2rml(&[opt], "gs:main", &snapshot, None, false, false);
        assert_eq!(
            r.unsupported,
            vec!["property path"],
            "recursion into OPTIONAL must catch a nested path"
        );
    }

    /// PR-0/0a negative case: patterns that only transform already-bound
    /// solutions (VALUES here) do NOT hydrate this graph's index, so they are
    /// NOT flagged — a VALUES-bearing R2RML query must still rewrite cleanly.
    #[test]
    fn values_is_not_flagged_unsupported() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let patterns = vec![
            Pattern::Values {
                vars: vec![VarId(0)],
                rows: vec![vec![crate::binding::Binding::Unbound]],
            },
            Pattern::Triple(TriplePattern::new(
                Ref::Var(VarId(0)),
                Ref::Var(VarId(1)),
                Term::Var(VarId(2)),
            )),
        ];
        let r = rewrite_patterns_for_r2rml(&patterns, "gs:main", &snapshot, None, false, false);
        assert!(
            r.unsupported.is_empty(),
            "VALUES must not be flagged unsupported"
        );
        assert_eq!(r.converted_count, 1, "the wildcard triple still converts");
    }

    /// PR-0/0a (review follow-up): repeated kind names collapse in the
    /// user-facing message — two property paths in one scope read
    /// "property path", not "property path, property path" — and first-seen
    /// order is preserved.
    #[test]
    fn unsupported_subscope_error_dedups_kinds() {
        let err =
            unsupported_subscope_error("gs:main", &["property path", "property path", "subquery"]);
        let msg = err.to_string();
        assert_eq!(
            msg.matches("property path").count(),
            1,
            "repeated kinds must collapse to one mention: {msg}"
        );
        assert!(
            msg.contains("property path, subquery"),
            "first-seen order must be preserved: {msg}"
        );
        assert!(msg.contains("gs:main"), "names the graph source: {msg}");
    }

    #[test]
    fn bigdecimal_decomposes_scale_insensitively() {
        use std::str::FromStr;
        let bd = |s: &str| bigdecimal::BigDecimal::from_str(s).unwrap();
        // 9.99 and 9.990 are the SAME value → identical decomposition (the exact
        // cross-scale shape the pruning layer must then compare correctly).
        let want = Some(ScanValue::Decimal {
            unscaled: 999,
            precision: 3,
            scale: 2,
        });
        assert_eq!(scan_value_from_bigdecimal(&bd("9.99")), want);
        assert_eq!(scan_value_from_bigdecimal(&bd("9.990")), want);
        // Trailing-zero integer forms also normalize to one representation.
        assert_eq!(
            scan_value_from_bigdecimal(&bd("100")),
            scan_value_from_bigdecimal(&bd("100.00"))
        );
        // Negative value.
        assert_eq!(
            scan_value_from_bigdecimal(&bd("-0.05")),
            Some(ScanValue::Decimal {
                unscaled: -5,
                precision: 1,
                scale: 2,
            })
        );
    }
}
