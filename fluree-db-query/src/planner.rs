//! Query planner - pattern reordering and selectivity estimation
//!
//! Reorders WHERE-clause patterns for optimal join order using statistics-based
//! cardinality estimates. When a `StatsView` is provided, uses HLL-derived
//! property statistics; otherwise falls back to heuristic defaults.
//!
//! The main entry point is `reorder_patterns`, called from
//! `build_where_operators_seeded` in `execute/where_plan.rs`.

use crate::ir::triple::{Ref, Term, TriplePattern};
use crate::ir::{CompareOp, Function, Grouping, Pattern, SubqueryPattern};
use crate::var_registry::VarId;
use fluree_db_core::{FlakeValue, PropertyStatData, StatsView};
use std::collections::{HashMap, HashSet};

// =============================================================================
// Statistics-Based Selectivity Estimation
// =============================================================================

/// Pattern type classification for selectivity scoring
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatternType {
    /// All three components bound (s p o) - most selective
    ExactMatch,
    /// Subject bound, predicate bound, object variable (s p ?o)
    BoundSubject,
    /// Subject variable, predicate bound, object bound (?s p o)
    BoundObject,
    /// rdf:type pattern with class bound (?s rdf:type Class)
    ClassPattern,
    /// Only predicate bound (?s p ?o)
    PropertyScan,
    /// All variables (?s ?p ?o) - full scan, least selective
    FullScan,
}

// Row count estimate constants used by estimate_triple_row_count
/// Highest selectivity - exact match or minimum row estimate
const HIGHLY_SELECTIVE: f64 = 1.0;
/// Medium selectivity - fallback for bound-subject patterns
const MODERATELY_SELECTIVE: f64 = 10.0;
/// Fallback for bound-object patterns (`?s <p> <o>`).
///
/// With no statistics, we still treat a bound object as more selective than a
/// pure property scan (`?s <p> ?o`) because it can use a predicate+object index
/// access path.
const DEFAULT_BOUND_OBJECT_SELECTIVITY: f64 = 1_000.0;
/// Fallback for property-scan patterns (`?s <p> ?o`).
///
/// With no statistics, assume these are relatively large so we avoid placing
/// them before bound-object constraints.
const DEFAULT_PROPERTY_SCAN_SELECTIVITY: f64 = 1_000_000.0;
/// Output estimate for a transitive path (`<s> <p>+ ?o`, etc.) anchored at a bound
/// endpoint: a constant subject/object, or a variable already bound by an earlier
/// pattern. Such a path enumerates the reachable set from that fixed node — a small
/// bounded closure, not a world scan — so it should drive a join (the planner then
/// probes the joined predicate by the produced var) instead of letting a
/// high-cardinality predicate scan run first. See issue #1287.
const ANCHORED_PROPERTY_PATH_SELECTIVITY: f64 = 100.0;
/// A `WITH DISTINCT <one var>` subquery anchored by a constant in its body
/// (e.g. `MATCH (p {id: $x})-[:KNOWS*1..2]-(friend) WITH DISTINCT friend`) emits
/// the projected distinct rows reachable from that anchor — NOT the body's join
/// product. The product wildly overestimates (a 2-hop KNOWS reads ~792M) and
/// pushes the producer behind unrelated sources in join ordering. Estimate such
/// an anchored producer at this small bounded value so it is placed first and
/// binds its output before consumers are ranked.
///
/// Tuned above the pure-ordering minimum: it ALSO seeds the object→subject
/// hash-join driving estimate for a downstream consumer (a `(message
/// HAS_CREATOR friend)` probe), so it must stay realistic enough that
/// `probe_count / driving_est` clears [`hash_join`'s scan-ratio cap](crate::hash_join)
/// — too small (e.g. 100) re-rejects the very hash join this ordering unlocks.
const DISTINCT_SUBQUERY_PRODUCER_SELECTIVITY: f64 = 500.0;
/// Full scan - all variables unbound
const FULL_SCAN: f64 = 1e12;

// Fallback caps differ by pattern type:
// - bound-subject (s p ?o): min count 10
// - bound-object (?s p o): min count 1000
const BOUND_SUBJECT_FALLBACK_CAP: f64 = 10.0;
const BOUND_OBJECT_FALLBACK_CAP: f64 = 1000.0;

/// Default row estimate for search patterns (IndexSearch, VectorSearch,
/// GeoSearch, S2Search) when no explicit limit is provided.
const DEFAULT_SEARCH_LIMIT: f64 = 100.0;

/// Default row estimate for Service patterns — high to place them late
/// since they involve network calls to remote endpoints.
const DEFAULT_SERVICE_ROW_COUNT: f64 = FULL_SCAN;

/// Classify a triple pattern for selectivity scoring, considering which
/// variables are already bound from previous patterns in the execution pipeline.
///
/// Treats variables present in `bound_vars` as effectively bound, producing
/// a more accurate pattern type for cardinality estimation during join ordering.
pub(crate) fn classify_pattern(
    pattern: &TriplePattern,
    bound_vars: &HashSet<VarId>,
) -> PatternType {
    let s_bound = pattern.s_bound() || pattern.s.as_var().is_some_and(|v| bound_vars.contains(&v));
    let p_bound = pattern.p_bound() || pattern.p.as_var().is_some_and(|v| bound_vars.contains(&v));
    let o_bound = pattern.o_bound() || pattern.o.as_var().is_some_and(|v| bound_vars.contains(&v));

    // rdf:type with a literal class object → ClassPattern (can look up specific class count).
    // If the object is a runtime-bound variable, we can't look up the class, so fall through
    // to BoundObject which uses the generic ndv_values-based estimate.
    if p_bound && o_bound && !s_bound && pattern.p.is_rdf_type() && pattern.o_bound() {
        return PatternType::ClassPattern;
    }

    match (s_bound, p_bound, o_bound) {
        (true, true, true) => PatternType::ExactMatch,
        (true, true, false) => PatternType::BoundSubject,
        (false, true, true) => PatternType::BoundObject,
        (false, true, false) => PatternType::PropertyScan,
        (false, false, false) => PatternType::FullScan,
        (true, false, _) => PatternType::BoundSubject,
        (false, false, true) => PatternType::FullScan,
    }
}

/// Look up property statistics by predicate term (SID or IRI).
fn property_stats<'a>(stats: &'a StatsView, pred: &Ref) -> Option<&'a PropertyStatData> {
    if let Some(sid) = pred.as_sid() {
        return stats.get_property(sid);
    }
    if let Some(iri) = pred.as_iri() {
        return stats.get_property_by_iri(iri);
    }
    None
}

fn property_known_absent(stats: &StatsView, pred: &Ref) -> bool {
    stats.has_property_stats()
        && match pred {
            Ref::Sid(sid) => stats.get_property(sid).is_none(),
            Ref::Iri(iri) => stats.get_property_by_iri(iri).is_none(),
            Ref::Var(_) => false,
        }
}

/// Look up class instance count by class term (SID or IRI).
fn class_count(stats: &StatsView, class: &Term) -> Option<u64> {
    if let Some(sid) = class.as_sid() {
        return stats.get_class_count(sid);
    }
    if let Some(iri) = class.as_iri() {
        return stats.get_class_count_by_iri(iri);
    }
    None
}

/// Estimate the number of result rows a triple pattern adds to the pipeline.
///
/// This is context-aware: it considers which variables are already bound from
/// previous patterns. A triple `?s :name ?name` is a full PropertyScan (count rows)
/// when `?s` is unbound, but only ~ceil(count/ndv_subjects) rows per incoming row
/// when `?s` is already bound from an earlier pattern.
pub(crate) fn estimate_triple_row_count(
    pattern: &TriplePattern,
    bound_vars: &HashSet<VarId>,
    stats: Option<&StatsView>,
) -> f64 {
    match classify_pattern(pattern, bound_vars) {
        PatternType::ExactMatch => HIGHLY_SELECTIVE,

        PatternType::ClassPattern => {
            if let Some(s) = stats {
                if let Some(count) = class_count(s, &pattern.o) {
                    return count as f64;
                }
                if let Some(prop) = property_stats(s, &pattern.p) {
                    if prop.ndv_values > 0 {
                        // Class count missing from stats.
                        //
                        // Using the mean class size (count/ndv_values) can severely
                        // underestimate common classes (e.g., `bsbm:Product`), which
                        // leads to bad join ordering when paired with small predicates
                        // like `rdfs:label`.
                        //
                        // Use a more conservative fallback: scale by sqrt(ndv_values)
                        // (between mean and total) to avoid treating unknown classes
                        // as extremely selective.
                        let ndv = prop.ndv_values as f64;
                        let est = prop.count as f64 / ndv.sqrt().max(1.0);
                        return est.ceil().max(HIGHLY_SELECTIVE);
                    }
                }
                if property_known_absent(s, &pattern.p) {
                    return 0.0;
                }
            }
            DEFAULT_BOUND_OBJECT_SELECTIVITY
        }

        PatternType::BoundSubject => {
            if let Some(s) = stats {
                if let Some(prop) = property_stats(s, &pattern.p) {
                    if prop.ndv_subjects > 0 {
                        return (prop.count as f64 / prop.ndv_subjects as f64)
                            .ceil()
                            .max(HIGHLY_SELECTIVE);
                    }
                    return (prop.count as f64).min(BOUND_SUBJECT_FALLBACK_CAP);
                }
                if property_known_absent(s, &pattern.p) {
                    return 0.0;
                }
            }
            MODERATELY_SELECTIVE
        }

        PatternType::BoundObject => {
            if let Some(s) = stats {
                if let Some(prop) = property_stats(s, &pattern.p) {
                    if prop.ndv_values > 0 {
                        return (prop.count as f64 / prop.ndv_values as f64)
                            .ceil()
                            .max(HIGHLY_SELECTIVE);
                    }
                    return (prop.count as f64).min(BOUND_OBJECT_FALLBACK_CAP);
                }
                if property_known_absent(s, &pattern.p) {
                    return 0.0;
                }
            }
            DEFAULT_BOUND_OBJECT_SELECTIVITY
        }

        PatternType::PropertyScan => {
            if let Some(s) = stats {
                if let Some(prop) = property_stats(s, &pattern.p) {
                    return prop.count as f64;
                }
                if property_known_absent(s, &pattern.p) {
                    return 0.0;
                }
            }
            DEFAULT_PROPERTY_SCAN_SELECTIVITY
        }

        PatternType::FullScan => FULL_SCAN,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PropertyJoinAnalysis {
    pub enough_patterns: bool,
    pub subject_is_var: bool,
    pub same_subject: bool,
    pub predicates_bound: bool,
    pub object_modes_supported: bool,
    pub object_vars_distinct: bool,
    pub has_bound_objects: bool,
    pub predicates_distinct: bool,
}

impl PropertyJoinAnalysis {
    #[inline]
    pub fn eligible(self) -> bool {
        self.enough_patterns
            && self.subject_is_var
            && self.same_subject
            && self.predicates_bound
            && self.object_modes_supported
            && self.object_vars_distinct
            && self.predicates_distinct
    }
}

/// Analyze whether a block qualifies for `PropertyJoinOperator`.
///
/// This exposes the individual gating rules so debug instrumentation can report
/// why a star-shaped block fell back to nested joins.
pub fn analyze_property_join(patterns: &[TriplePattern]) -> PropertyJoinAnalysis {
    let enough_patterns = patterns.len() >= 2;
    let first_s = match patterns.first().and_then(|p| p.s.as_var()) {
        Some(v) => v,
        None => {
            return PropertyJoinAnalysis {
                enough_patterns,
                subject_is_var: false,
                same_subject: false,
                predicates_bound: patterns
                    .iter()
                    .all(crate::ir::triple::TriplePattern::p_bound),
                object_modes_supported: false,
                object_vars_distinct: false,
                has_bound_objects: false,
                predicates_distinct: false,
            };
        }
    };

    let same_subject = patterns.iter().all(|p| match &p.s {
        Ref::Var(v) => *v == first_s,
        _ => false,
    });

    let predicates_bound = patterns
        .iter()
        .all(crate::ir::triple::TriplePattern::p_bound);

    let mut obj_vars: HashSet<VarId> = HashSet::new();
    let mut object_modes_supported = true;
    let mut object_vars_distinct = true;
    let mut has_bound_objects = false;
    for p in patterns {
        match &p.o {
            Term::Var(v) => {
                if *v == first_s || !obj_vars.insert(*v) {
                    object_vars_distinct = false;
                }
            }
            Term::Sid(_) | Term::Iri(_) | Term::Value(_) => {
                has_bound_objects = true;
            }
            #[allow(unreachable_patterns)]
            _ => {
                object_modes_supported = false;
                object_vars_distinct = false;
            }
        }
    }

    let predicates: HashSet<String> = patterns
        .iter()
        .filter_map(|p| match &p.p {
            Ref::Sid(sid) => Some(format!("sid:{sid}")),
            Ref::Iri(iri) => Some(format!("iri:{iri}")),
            _ => None,
        })
        .collect();
    let predicates_distinct = predicates.len() == patterns.len();

    PropertyJoinAnalysis {
        enough_patterns,
        subject_is_var: true,
        same_subject,
        predicates_bound,
        object_modes_supported,
        object_vars_distinct,
        has_bound_objects,
        predicates_distinct,
    }
}

/// Detect property-join pattern
///
/// Same ?s var, multiple distinct specified predicates, object vars.
/// Used by the planner to choose PropertyJoinOperator.
pub fn is_property_join(patterns: &[TriplePattern]) -> bool {
    analyze_property_join(patterns).eligible()
}

/// Represents a range constraint extracted from a filter expression
///
/// Used for filter pushdown to convert filters like `?age > 18 AND ?age < 65`
/// into index range bounds.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeConstraint {
    /// The variable this constraint applies to
    pub var: VarId,
    /// Lower bound: (value, inclusive)
    pub lower: Option<(RangeValue, bool)>,
    /// Upper bound: (value, inclusive)
    pub upper: Option<(RangeValue, bool)>,
}

/// A value that can be used in range constraints
///
/// Simplified version of FlakeValue for filter pushdown.
#[derive(Debug, Clone, PartialEq)]
pub enum RangeValue {
    Long(i64),
    Double(f64),
    String(String),
    /// Temporal value for range pushdown (NOT used for xsd:duration — it has no total order)
    Temporal(fluree_db_core::value::FlakeValue),
}

impl RangeConstraint {
    /// Create a new range constraint for a variable
    pub fn new(var: VarId) -> Self {
        Self {
            var,
            lower: None,
            upper: None,
        }
    }

    /// Set the lower bound
    pub fn with_lower(mut self, value: RangeValue, inclusive: bool) -> Self {
        self.lower = Some((value, inclusive));
        self
    }

    /// Set the upper bound
    pub fn with_upper(mut self, value: RangeValue, inclusive: bool) -> Self {
        self.upper = Some((value, inclusive));
        self
    }

    /// Merge another constraint into this one (tighten bounds)
    ///
    /// Takes the tighter of the two bounds. For lower bounds,
    /// the higher value is tighter. For upper bounds, the lower value is tighter.
    pub fn merge(&mut self, other: &RangeConstraint) {
        if self.var != other.var {
            return;
        }

        // Merge lower bounds: take the higher (tighter) one
        if let Some((other_val, other_incl)) = &other.lower {
            match &self.lower {
                None => self.lower = other.lower.clone(),
                Some((self_val, self_incl)) => {
                    if compare_range_values(other_val, self_val) == std::cmp::Ordering::Greater
                        || (compare_range_values(other_val, self_val) == std::cmp::Ordering::Equal
                            && !other_incl
                            && *self_incl)
                    {
                        self.lower = other.lower.clone();
                    }
                }
            }
        }

        // Merge upper bounds: take the lower (tighter) one
        if let Some((other_val, other_incl)) = &other.upper {
            match &self.upper {
                None => self.upper = other.upper.clone(),
                Some((self_val, self_incl)) => {
                    if compare_range_values(other_val, self_val) == std::cmp::Ordering::Less
                        || (compare_range_values(other_val, self_val) == std::cmp::Ordering::Equal
                            && !other_incl
                            && *self_incl)
                    {
                        self.upper = other.upper.clone();
                    }
                }
            }
        }
    }

    /// Check if this constraint is unsatisfiable (contradictory bounds)
    ///
    /// Returns true if:
    /// - lower > upper (impossible range)
    /// - lower == upper but either bound is exclusive (empty range)
    ///
    /// This enables early short-circuit when filters produce impossible ranges
    /// like `?x > 10 AND ?x < 5`.
    pub fn is_unsatisfiable(&self) -> bool {
        match (&self.lower, &self.upper) {
            (Some((lower_val, lower_incl)), Some((upper_val, upper_incl))) => {
                match compare_range_values(lower_val, upper_val) {
                    std::cmp::Ordering::Greater => true, // lower > upper
                    std::cmp::Ordering::Equal => {
                        // lower == upper: only satisfiable if both inclusive
                        !(*lower_incl && *upper_incl)
                    }
                    std::cmp::Ordering::Less => false, // normal range
                }
            }
            _ => false, // Open-ended ranges are always satisfiable
        }
    }
}

/// Compare two range values
fn compare_range_values(a: &RangeValue, b: &RangeValue) -> std::cmp::Ordering {
    match (a, b) {
        (RangeValue::Long(a), RangeValue::Long(b)) => a.cmp(b),
        (RangeValue::Double(a), RangeValue::Double(b)) => {
            // Treat NaN as not comparable; avoid pretending NaN == anything.
            a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
        }
        (RangeValue::String(a), RangeValue::String(b)) => a.cmp(b),
        // Cross-type: Long <-> Double
        (RangeValue::Long(a), RangeValue::Double(b)) => (*a as f64)
            .partial_cmp(b)
            .unwrap_or(std::cmp::Ordering::Equal),
        (RangeValue::Double(a), RangeValue::Long(b)) => a
            .partial_cmp(&(*b as f64))
            .unwrap_or(std::cmp::Ordering::Equal),
        // Different types that can't be compared
        _ => std::cmp::Ordering::Equal,
    }
}

/// Extract range constraints from a pushdown-safe filter expression
///
/// Returns `None` if the filter is not range-safe (contains OR, NOT, functions, etc.).
/// Returns `Some(vec)` with extracted constraints for each variable.
///
/// # Supported patterns
///
/// - `?var op const` where op is `<`, `<=`, `>`, `>=`, `=`
/// - `const op ?var` (reversed comparison)
/// - `(op const ?var const)` — "sandwich" pattern producing two-sided bounds
/// - `AND` of the above (constraints are merged for tighter bounds)
pub fn extract_range_constraints(expr: &Expression) -> Option<Vec<RangeConstraint>> {
    if !expr.is_range_safe() {
        return None;
    }

    match expr {
        Expression::Call { func, args } => match func {
            // Comparison operators
            Function::Eq | Function::Lt | Function::Le | Function::Gt | Function::Ge => {
                let op = func_to_compare_op(func);

                // 3-arg sandwich: (op const ?var const) → two-sided bounds
                if args.len() == 3 {
                    if let (Some(lo_val), Some(var), Some(hi_val)) = (
                        extract_const(&args[0]),
                        extract_var(&args[1]),
                        extract_const(&args[2]),
                    ) {
                        // Pair 1: const op ?var → ?var reversed_op const
                        let left_constraint =
                            create_constraint(var, reverse_compare_op(op), lo_val, false);
                        // Pair 2: ?var op const
                        let right_constraint = create_constraint(var, op, hi_val, false);
                        // Merge both into one constraint
                        let mut merged = left_constraint;
                        merged.merge(&right_constraint);
                        return Some(vec![merged]);
                    }
                    return None;
                }

                // 2-arg: ?var op const or const op ?var
                if args.len() != 2 {
                    return None;
                }
                let (left, right) = (&args[0], &args[1]);

                // Try ?var op const
                if let (Some(var), Some(val)) = (extract_var(left), extract_const(right)) {
                    return Some(vec![create_constraint(var, op, val, false)]);
                }
                // Try const op ?var (reverse the comparison)
                if let (Some(val), Some(var)) = (extract_const(left), extract_var(right)) {
                    let reversed_op = reverse_compare_op(op);
                    return Some(vec![create_constraint(var, reversed_op, val, false)]);
                }
                None
            }

            Function::And => {
                let mut all_constraints: HashMap<VarId, RangeConstraint> = HashMap::new();

                for e in args {
                    if let Some(constraints) = extract_range_constraints(e) {
                        for constraint in constraints {
                            all_constraints
                                .entry(constraint.var)
                                .and_modify(|existing| existing.merge(&constraint))
                                .or_insert(constraint);
                        }
                    }
                }

                if all_constraints.is_empty() {
                    None
                } else {
                    Some(all_constraints.into_values().collect())
                }
            }

            _ => None,
        },

        _ => None,
    }
}

/// Convert a Function comparison operator to CompareOp
fn func_to_compare_op(func: &Function) -> CompareOp {
    match func {
        Function::Eq => CompareOp::Eq,
        Function::Ne => CompareOp::Ne,
        Function::Lt => CompareOp::Lt,
        Function::Le => CompareOp::Le,
        Function::Gt => CompareOp::Gt,
        Function::Ge => CompareOp::Ge,
        _ => panic!("func_to_compare_op called with non-comparison function"),
    }
}

/// Extract a VarId from an expression if it's a simple variable reference
fn extract_var(expr: &Expression) -> Option<VarId> {
    match expr {
        Expression::Var(v) => Some(*v),
        _ => None,
    }
}

/// Extract a RangeValue from an expression if it's a constant
fn extract_const(expr: &Expression) -> Option<RangeValue> {
    use fluree_db_core::value::FlakeValue;

    match expr {
        Expression::Const(FlakeValue::Long(n)) => Some(RangeValue::Long(*n)),
        // NaN is not a meaningful range bound.
        Expression::Const(FlakeValue::Double(d)) if d.is_nan() => None,
        Expression::Const(FlakeValue::Double(d)) => Some(RangeValue::Double(*d)),
        Expression::Const(FlakeValue::String(s)) => Some(RangeValue::String(s.clone())),
        // Temporal/duration values: only totally-orderable kinds push down.
        // Duration is partially ordered (months vs days) so skip it.
        Expression::Const(fv) if fv.is_temporal() || fv.is_duration() => {
            if matches!(fv, FlakeValue::Duration(_)) {
                None
            } else {
                Some(RangeValue::Temporal(fv.clone()))
            }
        }
        _ => None,
    }
}

/// Reverse a comparison operator (for const op var -> var op const)
fn reverse_compare_op(op: crate::ir::CompareOp) -> crate::ir::CompareOp {
    use crate::ir::CompareOp;
    match op {
        CompareOp::Eq => CompareOp::Eq,
        CompareOp::Ne => CompareOp::Ne,
        CompareOp::Lt => CompareOp::Gt,
        CompareOp::Le => CompareOp::Ge,
        CompareOp::Gt => CompareOp::Lt,
        CompareOp::Ge => CompareOp::Le,
    }
}

/// Create a range constraint from a comparison
fn create_constraint(
    var: VarId,
    op: crate::ir::CompareOp,
    val: RangeValue,
    _from_reversed: bool,
) -> RangeConstraint {
    use crate::ir::CompareOp;

    let mut constraint = RangeConstraint::new(var);

    match op {
        CompareOp::Eq => {
            // Equality: lower = upper = val, both inclusive
            constraint.lower = Some((val.clone(), true));
            constraint.upper = Some((val, true));
        }
        CompareOp::Lt => {
            // ?var < val: upper bound exclusive
            constraint.upper = Some((val, false));
        }
        CompareOp::Le => {
            // ?var <= val: upper bound inclusive
            constraint.upper = Some((val, true));
        }
        CompareOp::Gt => {
            // ?var > val: lower bound exclusive
            constraint.lower = Some((val, false));
        }
        CompareOp::Ge => {
            // ?var >= val: lower bound inclusive
            constraint.lower = Some((val, true));
        }
        CompareOp::Ne => {
            // Not equal: cannot be represented as range constraint
            // (would need to split into two ranges)
        }
    }

    constraint
}

use crate::ir::Expression;
use fluree_db_core::ObjectBounds;

impl RangeValue {
    /// Convert to FlakeValue for use with ObjectBounds
    pub fn to_flake_value(&self) -> FlakeValue {
        match self {
            RangeValue::Long(n) => FlakeValue::Long(*n),
            RangeValue::Double(d) => FlakeValue::Double(*d),
            RangeValue::String(s) => FlakeValue::String(s.clone()),
            RangeValue::Temporal(fv) => fv.clone(),
        }
    }
}

impl RangeConstraint {
    /// Convert to ObjectBounds for filter pushdown
    ///
    /// Returns None if the constraint has no bounds (would match everything).
    pub fn to_object_bounds(&self) -> Option<ObjectBounds> {
        if self.lower.is_none() && self.upper.is_none() {
            return None;
        }

        let mut bounds = ObjectBounds::new();

        if let Some((val, inclusive)) = &self.lower {
            bounds = bounds.with_lower(val.to_flake_value(), *inclusive);
        }

        if let Some((val, inclusive)) = &self.upper {
            bounds = bounds.with_upper(val.to_flake_value(), *inclusive);
        }

        Some(bounds)
    }
}

/// Extract object bounds for a pattern's object variable from a filter
///
/// Given a filter expression and a variable ID (typically the object variable
/// of a triple pattern), extracts range bounds that can be pushed down to
/// the scan operator.
///
/// # Returns
///
/// - `Some(ObjectBounds)` if the filter has range-safe constraints on the variable
/// - `None` if no pushdown is possible (not range-safe, wrong variable, etc.)
///
/// Extracts range-safe constraints from a filter expression for the given object variable and converts them to `ObjectBounds` for scan pushdown.
pub fn extract_object_bounds_for_var(
    filter: &Expression,
    object_var: VarId,
) -> Option<ObjectBounds> {
    // Only proceed if filter is range-safe
    let constraints = extract_range_constraints(filter)?;

    // Find constraint for our object variable
    let constraint = constraints.into_iter().find(|c| c.var == object_var)?;

    // Check if the range is satisfiable
    if constraint.is_unsatisfiable() {
        // Unsatisfiable range means the query will return no results.
        // For now, return None (filter won't be pushed down, will filter to empty later).
        // A future optimization could short-circuit the entire query.
        return None;
    }

    constraint.to_object_bounds()
}

// =============================================================================
// Generalized Selectivity Scoring for All Pattern Types
// =============================================================================

/// Cardinality estimate for a generalized pattern.
///
/// Each variant carries only the data meaningful for that category:
/// - `Source`: estimated row count
/// - `Reducer`: fraction of rows surviving (< 1.0)
/// - `Expander`: expansion factor (>= 1.0)
/// - `Deferred`: no numeric payload
#[derive(Debug, Clone, PartialEq)]
pub enum PatternEstimate {
    /// Produces rows — estimated row count (Triple, VALUES, UNION, Subquery,
    /// IndexSearch, VectorSearch, GeoSearch, S2Search, Graph, PropertyPath,
    /// R2rml, Service)
    Source { row_count: f64 },
    /// Shrinks the stream — fraction of rows surviving (< 1.0) (MINUS, EXISTS, NOT EXISTS)
    Reducer { multiplier: f64 },
    /// Grows the stream via left-join — expansion factor (>= 1.0) (OPTIONAL)
    Expander { multiplier: f64 },
    /// FILTER/BIND — deferred, no cardinality effect
    Deferred,
}

impl PatternEstimate {
    /// Row count estimate (meaningful for Source; returns 0.0 otherwise).
    fn row_count(&self) -> f64 {
        match self {
            PatternEstimate::Source { row_count } => *row_count,
            _ => 0.0,
        }
    }

    /// Row multiplier (meaningful for Reducer/Expander; returns 1.0 otherwise).
    fn multiplier(&self) -> f64 {
        match self {
            PatternEstimate::Reducer { multiplier } | PatternEstimate::Expander { multiplier } => {
                *multiplier
            }
            _ => 1.0,
        }
    }
}

/// Does this subquery body contain a constant anchor — a specific starting node
/// (`<const> p ?x`) or a property-value lookup that pins a subject
/// (`?x p <const>`, e.g. `?root id $personId`) — that bounds the traversal to a
/// small reachable set? Used to recognize an anchored `DISTINCT` producer (see
/// [`DISTINCT_SUBQUERY_PRODUCER_SELECTIVITY`]). A broad `?x rdf:type Class` is
/// NOT an anchor: it constrains membership, not a starting point.
fn subquery_body_has_constant_anchor(patterns: &[Pattern]) -> bool {
    patterns.iter().any(|p| match p {
        Pattern::Triple(tp) => {
            let const_subject = !matches!(tp.s, Ref::Var(_));
            let pins_subject = tp.o.as_var().is_none() && !tp.p.is_rdf_type();
            const_subject || pins_subject
        }
        Pattern::PropertyPath(pp) => {
            !matches!(pp.subject, Ref::Var(_)) || !matches!(pp.object, Ref::Var(_))
        }
        Pattern::Union(branches) => branches
            .iter()
            .any(|b| subquery_body_has_constant_anchor(b)),
        Pattern::Optional(ps) | Pattern::Graph { patterns: ps, .. } => {
            subquery_body_has_constant_anchor(ps)
        }
        Pattern::Subquery(inner) => subquery_body_has_constant_anchor(&inner.patterns),
        _ => false,
    })
}

/// Estimate a subquery's OUTPUT cardinality (what join ordering and the
/// hash-join driving estimate care about) — NOT the size of its internal scan.
///
/// - A scalar aggregate (implicit `GROUP BY`) emits exactly one row.
/// - A `DISTINCT` producer of a single var anchored by a constant in its body
///   (`WITH DISTINCT friend` over an `{id: $x}`-anchored traversal) emits the
///   projected distinct rows reachable from the anchor, not the body product —
///   estimated at [`DISTINCT_SUBQUERY_PRODUCER_SELECTIVITY`].
/// - Everything else keeps the body cardinality as an upper bound.
///
/// Shared by [`estimate_pattern`] (join ordering) and
/// `SubqueryOperator::estimated_rows` (seeding the downstream hash join), so the
/// producer's estimate is consistent in both places.
pub(crate) fn estimate_subquery_output(sq: &SubqueryPattern, stats: Option<&StatsView>) -> f64 {
    let rows = match &sq.grouping {
        Some(Grouping::Implicit { .. }) => HIGHLY_SELECTIVE,
        _ if sq.distinct
            && sq.select.len() == 1
            && sq.limit.is_none()
            && subquery_body_has_constant_anchor(&sq.patterns) =>
        {
            DISTINCT_SUBQUERY_PRODUCER_SELECTIVITY
        }
        _ => estimate_branch_cardinality(&sq.patterns, stats),
    };
    // A LIMIT caps the output regardless of grouping.
    let rows = sq.limit.map_or(rows, |l| rows.min(l as f64));
    rows.max(HIGHLY_SELECTIVE)
}

/// Estimate cardinality for any pattern type.
///
/// The `bound_vars` parameter indicates which variables are already bound from
/// earlier patterns in the pipeline. For triple patterns this significantly
/// affects the estimate: a triple whose subject variable is already bound is a
/// per-subject lookup (cheap), not a full property scan (expensive).
pub fn estimate_pattern(
    pattern: &Pattern,
    bound_vars: &HashSet<VarId>,
    stats: Option<&StatsView>,
) -> PatternEstimate {
    match pattern {
        Pattern::Triple(tp) => PatternEstimate::Source {
            row_count: estimate_triple_row_count(tp, bound_vars, stats),
        },

        Pattern::Values { rows, .. } => PatternEstimate::Source {
            row_count: rows.len() as f64,
        },

        Pattern::Union(branches) => {
            let total: f64 = branches
                .iter()
                .map(|branch| estimate_branch_cardinality(branch, stats))
                .sum();
            PatternEstimate::Source {
                row_count: total.max(HIGHLY_SELECTIVE),
            }
        }

        Pattern::Subquery(sq) => PatternEstimate::Source {
            row_count: estimate_subquery_output(sq, stats),
        },

        Pattern::Optional(_) => PatternEstimate::Expander { multiplier: 1.0 },

        // MINUS, EXISTS, and NOT EXISTS are order-sensitive: they must run
        // after all preceding patterns. The planner intercepts them before
        // calling estimate_pattern (see reorder_patterns), so in practice
        // these arms are only reached by direct callers like explain.rs.
        Pattern::Minus(_) | Pattern::Exists(_) | Pattern::NotExists(_) => PatternEstimate::Deferred,

        Pattern::Filter(_) | Pattern::Bind { .. } => PatternEstimate::Deferred,

        // UNWIND a runtime list — defer until the list expression's vars are
        // bound (it reads them per row), like a correlated Bind.
        Pattern::Unwind { .. } => PatternEstimate::Deferred,

        Pattern::IndexSearch(isp) => PatternEstimate::Source {
            row_count: isp.limit.map_or(DEFAULT_SEARCH_LIMIT, |l| l as f64),
        },

        Pattern::VectorSearch(vsp) => PatternEstimate::Source {
            row_count: vsp.limit.map_or(DEFAULT_SEARCH_LIMIT, |l| l as f64),
        },

        Pattern::GeoSearch(gsp) => PatternEstimate::Source {
            row_count: gsp.limit.map_or(DEFAULT_SEARCH_LIMIT, |l| l as f64),
        },

        Pattern::S2Search(s2p) => PatternEstimate::Source {
            row_count: s2p.limit.map_or(DEFAULT_SEARCH_LIMIT, |l| l as f64),
        },

        Pattern::Graph { patterns, .. } => PatternEstimate::Source {
            row_count: estimate_branch_cardinality(patterns, stats),
        },

        Pattern::PropertyPath(pp) => {
            // Anchored at a bound endpoint => a bounded closure from a fixed node,
            // not a full predicate scan. Estimating it as a world scan made reorder
            // drive a high-cardinality join predicate first (issue #1287).
            let anchored = |r: &Ref| match r {
                Ref::Var(v) => bound_vars.contains(v),
                _ => true, // constant Sid/Iri endpoint
            };
            let row_count = if anchored(&pp.subject) || anchored(&pp.object) {
                ANCHORED_PROPERTY_PATH_SELECTIVITY
            } else {
                DEFAULT_PROPERTY_SCAN_SELECTIVITY
            };
            PatternEstimate::Source { row_count }
        }

        // Anchored shortest-path must run after both endpoints are bound.
        // Defer it on its referenced (endpoint) vars, like a correlated
        // subquery, so reorder never hoists it ahead of its inputs.
        Pattern::ShortestPath(_) => PatternEstimate::Deferred,

        Pattern::R2rml(_) => PatternEstimate::Source {
            row_count: DEFAULT_PROPERTY_SCAN_SELECTIVITY,
        },

        Pattern::Service(_) => PatternEstimate::Source {
            row_count: DEFAULT_SERVICE_ROW_COUNT,
        },

        // Edge-annotation patterns (M0): treated as a `Source` with the
        // wrapped edge's cardinality as a first approximation. Real
        // cost-based selection between edge-first and annotation-first
        // scans arrives in M3 alongside `AnnotationStats`.
        Pattern::EdgeAnnotation { edge, .. } | Pattern::AnnotationTarget { edge, .. } => {
            PatternEstimate::Source {
                row_count: estimate_triple_row_count(edge, bound_vars, stats),
            }
        }

        // DefaultGraphSource wraps an inner subplan and runs it once
        // per default-graph source. Cost is modeled like Graph — the
        // inner branch's cardinality, scaled implicitly by the source
        // count at runtime.
        Pattern::DefaultGraphSource { patterns, .. } => PatternEstimate::Source {
            row_count: estimate_branch_cardinality(patterns, stats),
        },
    }
}

/// Estimate cardinality for a sequence of patterns (UNION branch or subquery body).
///
/// Uses a context-aware multiplicative model: tracks which variables become bound as
/// each pattern is placed, so subsequent triples use the appropriate expansion factor
/// rather than standalone row counts.
pub fn estimate_branch_cardinality(patterns: &[Pattern], stats: Option<&StatsView>) -> f64 {
    if patterns.is_empty() {
        return HIGHLY_SELECTIVE;
    }

    let mut bound_vars: HashSet<VarId> = HashSet::new();

    // Separate triples from non-triples
    let mut triples: Vec<&TriplePattern> = Vec::new();
    let mut non_triple_estimate: f64 = HIGHLY_SELECTIVE;

    for p in patterns {
        match p {
            Pattern::Triple(tp) => triples.push(tp),
            Pattern::Filter(_) | Pattern::Bind { .. } => {
                // Filters/binds don't change cardinality estimate significantly
            }
            other => {
                let card = estimate_pattern(other, &bound_vars, stats);
                match card {
                    PatternEstimate::Source { row_count } => {
                        non_triple_estimate *= row_count.max(HIGHLY_SELECTIVE);
                    }
                    PatternEstimate::Reducer { multiplier } => {
                        non_triple_estimate *= multiplier;
                    }
                    _ => {}
                }
                for v in other.produced_vars() {
                    bound_vars.insert(v);
                }
            }
        }
    }

    if triples.is_empty() {
        return (DEFAULT_PROPERTY_SCAN_SELECTIVITY) * non_triple_estimate;
    }

    // Sort triples by standalone row count for initial ordering (most selective first)
    let empty_bound: HashSet<VarId> = HashSet::new();
    let mut sorted_triples: Vec<&TriplePattern> = triples;
    sorted_triples.sort_by(|a, b| {
        let ea = estimate_triple_row_count(a, &empty_bound, stats);
        let eb = estimate_triple_row_count(b, &empty_bound, stats);
        ea.partial_cmp(&eb).unwrap_or(std::cmp::Ordering::Equal)
    });

    // Start with most selective triple (standalone estimate — nothing bound yet)
    let first = sorted_triples[0];
    let mut running = estimate_triple_row_count(first, &bound_vars, stats);
    for v in first.produced_vars() {
        bound_vars.insert(v);
    }

    // Multiplicative model: each subsequent triple's contribution depends on
    // which variables are now bound from previous triples
    for tp in sorted_triples.iter().skip(1) {
        let expansion = estimate_triple_row_count(tp, &bound_vars, stats);
        running *= expansion;
        for v in tp.produced_vars() {
            bound_vars.insert(v);
        }
    }

    (running * non_triple_estimate).max(HIGHLY_SELECTIVE)
}

/// Check if a general pattern shares any variables with the bound set.
pub fn pattern_shares_variables(pattern: &Pattern, bound_vars: &HashSet<VarId>) -> bool {
    pattern
        .referenced_vars()
        .iter()
        .any(|v| bound_vars.contains(v))
}

/// Collect the variables that a slice of patterns guarantees to bind.
fn collect_guaranteed_vars(patterns: &[Pattern]) -> HashSet<VarId> {
    patterns
        .iter()
        .flat_map(super::ir::Pattern::produced_vars)
        .collect()
}

/// Try to nest a deferred pattern into a compound pattern's inner lists.
///
/// Returns `true` if the pattern was nested, `false` if the pattern is not
/// a supported compound type or (for UNION) none of the deferred pattern's
/// required variables appear in every branch.
///
/// For UNION the deferred pattern is cloned into every branch, but only if
/// at least one required variable appears in the intersection of all branch
/// variable sets.  Variables from prior patterns are already bound in the
/// parent scope and available inside each branch, so only the UNION-specific
/// variables need the intersection check.
///
/// For Graph and Service all inner variables are guaranteed, so the deferred
/// pattern is nested unconditionally.
fn try_nest_deferred(compound: &mut Pattern, deferred: &DeferredPattern) -> bool {
    match compound {
        Pattern::Union(branches) => {
            let guaranteed_vars = branches
                .iter()
                .map(|b| collect_guaranteed_vars(b))
                .reduce(|mut union_vars, branch_vars| {
                    union_vars.retain(|v| branch_vars.contains(v));
                    union_vars
                })
                .unwrap_or_default();
            if !deferred
                .required_vars
                .iter()
                .any(|v| guaranteed_vars.contains(v))
            {
                return false;
            }
            for branch in branches.iter_mut() {
                branch.push(deferred.pattern.clone());
            }
            true
        }
        Pattern::Graph { patterns, .. } => {
            patterns.push(deferred.pattern.clone());
            true
        }
        Pattern::Service(sp) => {
            sp.patterns.push(deferred.pattern.clone());
            true
        }
        _ => false,
    }
}

// =============================================================================
// Generalized Pattern Reordering
// =============================================================================

/// A pattern annotated with its original position in the input list.
///
/// `orig_index` is used as a deterministic tiebreaker when two patterns have
/// equal selectivity estimates, preserving the user's original ordering.
struct RankedPattern {
    orig_index: usize,
    pattern: Pattern,
}

/// A deferred pattern (FILTER/BIND) with pre-computed input variables.
///
/// `required_vars` is the set of variables that must be bound before this
/// pattern can execute. For FILTER this is all referenced variables; for BIND
/// it is the expression's variables (the target variable is an output).
struct DeferredPattern {
    orig_index: usize,
    required_vars: HashSet<VarId>,
    pattern: Pattern,
}

/// Reorder all pattern types for optimal join order.
///
/// Handles all pattern types including triples, compound patterns (UNION,
/// OPTIONAL, MINUS, EXISTS, etc.), VALUES, FILTER, BIND, and source patterns
/// (IndexSearch, VectorSearch, GeoSearch, S2Search, Graph, PropertyPath,
/// R2rml, Service). Uses a priority-based greedy algorithm:
///
/// 1. **Eligible reducers** first (lowest multiplier) — shrink the stream ASAP
/// 2. **Sources** next (lowest estimate) — same greedy logic as triple reorder
/// 3. **Eligible expanders** last (lowest multiplier) — defer row expansion
pub fn reorder_patterns(
    patterns: &[Pattern],
    stats: Option<&StatsView>,
    initial_bound_vars: &HashSet<VarId>,
) -> Vec<Pattern> {
    if patterns.len() <= 1 {
        return patterns.to_vec();
    }

    let mut bound_vars = initial_bound_vars.clone();

    // Outputs of UNCORRELATED sibling subqueries (Cypher WITH-pipeline
    // producers). A pattern consuming one of these must be placed AFTER the
    // producing subquery — otherwise the greedy source race can place the
    // consumer first (e.g. a cheap `?post a :Post` scan ahead of a var-length
    // WITH whose cost estimate is high), which turns the uncorrelated producer
    // into a per-row correlated subquery over its own consumer and silently
    // collapses the consumer's bindings. Correlated subqueries are excluded:
    // their own outputs are handled by the correlation deferral below.
    let subquery_output_vars: HashSet<VarId> = patterns
        .iter()
        .enumerate()
        .filter_map(|(i, p)| match p {
            Pattern::Subquery(sq) if subquery_correlation_vars(sq, patterns, i).is_empty() => {
                Some(subquery_produced_select_vars(sq))
            }
            _ => None,
        })
        .flatten()
        .collect();

    // Classify each pattern by its cardinality category.
    let mut sources: Vec<RankedPattern> = Vec::new();
    let mut reducers: Vec<RankedPattern> = Vec::new();
    let mut expanders: Vec<RankedPattern> = Vec::new();
    let mut deferred: Vec<DeferredPattern> = Vec::new();

    for (i, pattern) in patterns.iter().enumerate() {
        // MINUS, EXISTS, and NOT EXISTS are order-sensitive: they operate on
        // the solution produced by ALL preceding patterns. Treat them as
        // deferred with required_vars = variables from all preceding patterns
        // so the reorder cannot hoist them above sources that feed them.
        if matches!(
            pattern,
            Pattern::Minus(_) | Pattern::Exists(_) | Pattern::NotExists(_)
        ) {
            // Require all variables produced by preceding patterns (order preservation)
            let mut required: HashSet<VarId> = patterns[..i]
                .iter()
                .flat_map(super::ir::Pattern::produced_vars)
                .collect();
            // If no preceding patterns, require the pattern's own referenced
            // variables so it cannot execute before any sources provide bindings.
            if required.is_empty() {
                required = pattern.referenced_vars().into_iter().collect();
            }
            deferred.push(DeferredPattern {
                orig_index: i,
                required_vars: required,
                pattern: pattern.clone(),
            });
            continue;
        }

        // A CORRELATED subquery — one whose SELECT list shares variables with
        // other patterns in this group — must execute AFTER those variables are
        // bound. `SubqueryOperator` derives its correlation set from the
        // variables its child (the patterns placed before it) provides; if the
        // subquery were hoisted ahead of them, that set would be empty and it
        // would compute a single global result instead of a per-row correlated
        // one. Defer it on its correlation variables, exactly like MINUS/EXISTS.
        //
        // UNCORRELATED subqueries are intentionally NOT deferred: they fall
        // through to source classification, so a scalar-aggregate subquery is
        // still placed early via its (now accurate) single-row estimate.
        if let Pattern::Subquery(sq) = pattern {
            let corr = subquery_correlation_vars(sq, patterns, i);
            if !corr.is_empty() {
                deferred.push(DeferredPattern {
                    orig_index: i,
                    required_vars: corr,
                    pattern: pattern.clone(),
                });
                continue;
            }
        } else {
            // Consumer of an uncorrelated WITH-subquery's output: defer it on
            // those vars so the producing subquery is placed first.
            let needs: HashSet<VarId> = pattern
                .referenced_vars()
                .into_iter()
                .filter(|v| subquery_output_vars.contains(v))
                .collect();
            if !needs.is_empty() {
                deferred.push(DeferredPattern {
                    orig_index: i,
                    required_vars: needs,
                    pattern: pattern.clone(),
                });
                continue;
            }
        }

        match estimate_pattern(pattern, &bound_vars, stats) {
            PatternEstimate::Source { .. } => sources.push(RankedPattern {
                orig_index: i,
                pattern: pattern.clone(),
            }),
            PatternEstimate::Reducer { .. } => reducers.push(RankedPattern {
                orig_index: i,
                pattern: pattern.clone(),
            }),
            PatternEstimate::Expander { .. } => expanders.push(RankedPattern {
                orig_index: i,
                pattern: pattern.clone(),
            }),
            PatternEstimate::Deferred => deferred.push(DeferredPattern {
                orig_index: i,
                required_vars: deferred_required_vars(pattern).into_iter().collect(),
                pattern: pattern.clone(),
            }),
        }
    }

    let mut result: Vec<Pattern> = Vec::with_capacity(patterns.len());

    // Place any deferred patterns whose inputs are already satisfied by the
    // initial bound_vars (e.g. from a seed operator).
    drain_ready_deferred(&mut deferred, &mut bound_vars, &mut result);

    // Greedy loop: place patterns by priority
    while !sources.is_empty() || !reducers.is_empty() || !expanders.is_empty() {
        let placed = try_place_reducer(&mut reducers, &mut bound_vars, stats, &mut result)
            || try_place_source(
                &mut sources,
                &mut bound_vars,
                stats,
                &mut result,
                &subquery_output_vars,
            )
            || try_place_expander(&mut expanders, &mut bound_vars, stats, &mut result);

        if !placed {
            // Nothing could be placed (shouldn't happen with sources always eligible).
            // Force-place the first remaining pattern.
            let rp = if !sources.is_empty() {
                sources.remove(0)
            } else if !reducers.is_empty() {
                reducers.remove(0)
            } else if !expanders.is_empty() {
                expanders.remove(0)
            } else {
                break;
            };
            for v in rp.pattern.produced_vars() {
                bound_vars.insert(v);
            }
            result.push(rp.pattern);
        }

        // After each placement, drain any deferred patterns that have become
        // ready.  BIND outputs feed back into bound_vars, so a single source
        // placement can cascade through multiple BINDs.
        drain_ready_deferred(&mut deferred, &mut bound_vars, &mut result);
    }

    // Append any remaining deferred patterns (their inputs may never be bound,
    // e.g. referencing variables from an OPTIONAL that hasn't been placed yet).
    deferred.sort_by_key(|dp| dp.orig_index);
    for dp in deferred {
        result.push(dp.pattern);
    }

    result
}

/// Try to place the best eligible reducer. Returns true if one was placed.
fn try_place_reducer(
    remaining: &mut Vec<RankedPattern>,
    bound_vars: &mut HashSet<VarId>,
    stats: Option<&StatsView>,
    result: &mut Vec<Pattern>,
) -> bool {
    // Find eligible reducers (at least one variable already bound)
    let eligible_idx = remaining
        .iter()
        .enumerate()
        .filter(|(_, rp)| pattern_shares_variables(&rp.pattern, bound_vars))
        .min_by(|(_, a), (_, b)| {
            let ca = estimate_pattern(&a.pattern, bound_vars, stats);
            let cb = estimate_pattern(&b.pattern, bound_vars, stats);
            ca.multiplier()
                .partial_cmp(&cb.multiplier())
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.orig_index.cmp(&b.orig_index))
        })
        .map(|(idx, _)| idx);

    if let Some(idx) = eligible_idx {
        let rp = remaining.remove(idx);
        for v in rp.pattern.produced_vars() {
            bound_vars.insert(v);
        }
        result.push(rp.pattern);
        true
    } else {
        false
    }
}

/// Tie-break signal for join ordering: if the candidate source is placed (binding
/// its produced vars), the largest probe-predicate count that would become an
/// object→subject hash join — a remaining triple `?s <p> ?o` whose object `?o` the
/// candidate binds while the subject `?s` stays new. Driving from such a start keeps
/// that high-cardinality predicate a single contiguous hash-probe scan instead of a
/// forward join over a large intermediate, so among equally-selective starts we
/// prefer the one unlocking the biggest such scan. Returns `0` when nothing is
/// unlocked or stats are absent, so it only ever breaks exact row-count ties.
fn unlocked_object_hash_scan(
    candidate: usize,
    remaining: &[RankedPattern],
    bound_vars: &HashSet<VarId>,
    stats: Option<&StatsView>,
) -> u64 {
    let Some(stats) = stats else { return 0 };
    let produced: HashSet<VarId> = remaining[candidate]
        .pattern
        .produced_vars()
        .into_iter()
        .collect();
    remaining
        .iter()
        .enumerate()
        .filter(|(k, _)| *k != candidate)
        .filter_map(|(_, rp)| {
            let Pattern::Triple(tp) = &rp.pattern else {
                return None;
            };
            let o = tp.o.as_var()?;
            let s = tp.s.as_var()?;
            // Object newly bound by the candidate; subject still new (the probe shape).
            if produced.contains(&o) && !produced.contains(&s) && !bound_vars.contains(&s) {
                property_stats(stats, &tp.p).map(|p| p.count)
            } else {
                None
            }
        })
        .max()
        .unwrap_or(0)
}

/// A disconnected `?x rdf:type <const>` class anchor: a `rdf:type` triple whose
/// object is a constant class and whose subject variable is part of neither the
/// bound set nor the producer component. Seeding/placing from such a triple
/// drives the join backward off the (often broad) class extension — e.g. `home
/// rdf:type Country` reverse-scanning `IS_PART_OF`/`IS_LOCATED_IN` for every
/// city. Only class anchors are matched: a selective property-value anchor such
/// as `?c name "X"` (cardinality 1) is a *good* disconnected seed and is left
/// alone. See [`try_place_source`].
fn is_disconnected_class_anchor(
    pattern: &Pattern,
    bound_vars: &HashSet<VarId>,
    anchor_vars: &HashSet<VarId>,
) -> bool {
    let Pattern::Triple(tp) = pattern else {
        return false;
    };
    if !tp.p.is_rdf_type() || !matches!(tp.o, Term::Iri(_) | Term::Sid(_)) {
        return false;
    }
    match &tp.s {
        Ref::Var(v) => !bound_vars.contains(v) && !anchor_vars.contains(v),
        _ => false,
    }
}

/// A broad RDF-star annotation *sidecar* triple: `?ann f:reifiesSubject ?s` or
/// `?ann f:reifiesObject ?o`. These index every annotated edge in the ledger, so
/// seeding from one (e.g. `?ann f:reifiesObject ?friend`) scans the whole
/// annotation sidecar before the concrete base edge or the `f:reifiesPredicate`
/// discriminator narrows it to one predicate — like scanning a global secondary
/// index before choosing the table. `f:reifiesPredicate` is deliberately NOT
/// matched: its object is the concrete base predicate, so it *is* the
/// discriminator and a good driver. Demoted from the seed pool by
/// [`try_place_source`] whenever a connected alternative (the base edge) exists.
fn is_broad_annotation_sidecar(pattern: &Pattern) -> bool {
    let Pattern::Triple(tp) = pattern else {
        return false;
    };
    match &tp.p {
        Ref::Sid(sid) => {
            sid.namespace_code == fluree_vocab::namespaces::FLUREE_DB
                && (sid.name.as_ref() == fluree_vocab::db::REIFIES_SUBJECT
                    || sid.name.as_ref() == fluree_vocab::db::REIFIES_OBJECT)
        }
        _ => false,
    }
}

/// Try to place the best source. Returns true if one was placed.
///
/// `anchor_vars` are the outputs of uncorrelated WITH-subquery producers (see
/// `reorder_patterns`) — a pseudo-bound pipeline component. At every placement,
/// when a pipeline exists (bound vars or a producer) and some other candidate is
/// connected to it, a disconnected `rdf:type <const>` class anchor is dropped
/// from the pool so it cannot flip the chain into a scattered reverse
/// object-drive (the IC3 `home:Country` case). This is deliberately narrow:
/// only class anchors are demoted, only when a connected alternative exists, and
/// selective property-value anchors (`?c name "X"`) are never touched — so
/// IC6/IC11-style queries that legitimately seed from a constant entity, and
/// genuine class-anchored queries (no producer, no bound pipeline), are
/// unaffected.
fn try_place_source(
    remaining: &mut Vec<RankedPattern>,
    bound_vars: &mut HashSet<VarId>,
    stats: Option<&StatsView>,
    result: &mut Vec<Pattern>,
    anchor_vars: &HashSet<VarId>,
) -> bool {
    if remaining.is_empty() {
        return false;
    }

    let has_bound = !bound_vars.is_empty();

    // Base pool: prefer sources joinable with the bound set; if none are (e.g.
    // the only forward continuation is a not-yet-placed producer), fall back to
    // all sources.
    let connected: Vec<usize> = remaining
        .iter()
        .enumerate()
        .filter(|(_, rp)| !has_bound || pattern_shares_variables(&rp.pattern, bound_vars))
        .map(|(idx, _)| idx)
        .collect();
    let base_pool = if connected.is_empty() {
        (0..remaining.len()).collect::<Vec<_>>()
    } else {
        connected
    };

    // Demote "broad index" sources from whichever pool we use — including the
    // fall-back-to-all pool — so they cannot win the seed when a concrete
    // connected alternative exists:
    //   * disconnected `rdf:type <const>` class anchors (the IC3 chain-A
    //     `home:Country` reverse-drive), and
    //   * RDF-star annotation sidecars `?ann f:reifiesSubject/Object ?x`, which
    //     index every annotated edge — seeding from one scans the whole sidecar
    //     instead of driving the concrete base edge / `f:reifiesPredicate`
    //     discriminator (the IC5 `HAS_MEMBER` reified-edge case).
    // Only applies when a pipeline exists with a connected alternative, so
    // selective property-value anchors, the base edge, the discriminator, and
    // genuine class-anchored queries are untouched. If demotion would empty the
    // pool, keep the base pool.
    let pipeline_active = has_bound || !anchor_vars.is_empty();
    let has_connected_alt = pipeline_active
        && remaining.iter().any(|rp| {
            pattern_shares_variables(&rp.pattern, bound_vars)
                || pattern_shares_variables(&rp.pattern, anchor_vars)
        });
    let pool: Vec<usize> = if has_connected_alt {
        let demoted: Vec<usize> = base_pool
            .iter()
            .copied()
            .filter(|&i| {
                let p = &remaining[i].pattern;
                !is_disconnected_class_anchor(p, bound_vars, anchor_vars)
                    && !is_broad_annotation_sidecar(p)
            })
            .collect();
        if demoted.is_empty() {
            base_pool
        } else {
            demoted
        }
    } else {
        base_pool
    };

    let best_idx = pool.into_iter().min_by(|&i, &j| {
        let seed_priority = |pattern: &Pattern| match pattern {
            // Search sources should seed the pipeline before plain triples so
            // they can emit result IDs/IriMatch bindings that later joins consume.
            Pattern::IndexSearch(_)
            | Pattern::VectorSearch(_)
            | Pattern::GeoSearch(_)
            | Pattern::S2Search(_) => 0_u8,
            _ => 1_u8,
        };
        let ci = estimate_pattern(&remaining[i].pattern, bound_vars, stats);
        let cj = estimate_pattern(&remaining[j].pattern, bound_vars, stats);
        if !has_bound {
            seed_priority(&remaining[i].pattern).cmp(&seed_priority(&remaining[j].pattern))
        } else {
            std::cmp::Ordering::Equal
        }
        .then_with(|| {
            ci.row_count()
                .partial_cmp(&cj.row_count())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        // Before falling back to original index: among equally-selective starts,
        // prefer the one that turns the LARGER predicate into an object→subject
        // hash join rather than a forward join over a big intermediate. This flips
        // a BSBM-BI bowtie (two equally-selective country filters, written
        // producer/DE-first) onto the side whose chain keeps the 2.85M-row
        // predicate hash-able — 46x on BI-1's F2.
        .then_with(|| {
            let bi = unlocked_object_hash_scan(i, &remaining[..], bound_vars, stats);
            let bj = unlocked_object_hash_scan(j, &remaining[..], bound_vars, stats);
            bj.cmp(&bi)
        })
        .then_with(|| remaining[i].orig_index.cmp(&remaining[j].orig_index))
    });

    if let Some(idx) = best_idx {
        let rp = remaining.remove(idx);
        for v in rp.pattern.produced_vars() {
            bound_vars.insert(v);
        }
        result.push(rp.pattern);
        true
    } else {
        false
    }
}

/// Try to place the best eligible expander. Returns true if one was placed.
fn try_place_expander(
    remaining: &mut Vec<RankedPattern>,
    bound_vars: &mut HashSet<VarId>,
    stats: Option<&StatsView>,
    result: &mut Vec<Pattern>,
) -> bool {
    let eligible_idx = remaining
        .iter()
        .enumerate()
        .filter(|(_, rp)| pattern_shares_variables(&rp.pattern, bound_vars))
        .min_by(|(_, a), (_, b)| {
            let ca = estimate_pattern(&a.pattern, bound_vars, stats);
            let cb = estimate_pattern(&b.pattern, bound_vars, stats);
            ca.multiplier()
                .partial_cmp(&cb.multiplier())
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.orig_index.cmp(&b.orig_index))
        })
        .map(|(idx, _)| idx);

    if let Some(idx) = eligible_idx {
        let rp = remaining.remove(idx);
        for v in rp.pattern.produced_vars() {
            bound_vars.insert(v);
        }
        result.push(rp.pattern);
        true
    } else {
        false
    }
}

/// Drain all deferred patterns whose required variables are currently bound.
///
/// For FILTER, the required variables are the expression's referenced variables.
/// For BIND, the required variables are the expression's referenced variables
/// (the target variable is an *output*, not an input).
///
/// BIND outputs are added to `bound_vars` after placement, which may enable
/// further deferred patterns.  The function loops until no more can be placed.
/// Among simultaneously-ready patterns, original position order is preserved.
///
/// When the last element of `result` is a compound pattern (UNION, Graph, or
/// Service), ready deferred patterns are nested *into* the compound pattern's
/// inner lists instead of being appended after it. This allows filters and
/// binds to participate in the compound pattern's inner `reorder_patterns`
/// pipeline, enabling filter pushdown and inline evaluation within each branch.
fn drain_ready_deferred(
    deferred: &mut Vec<DeferredPattern>,
    bound_vars: &mut HashSet<VarId>,
    result: &mut Vec<Pattern>,
) {
    loop {
        // Find all deferred patterns whose inputs are satisfied.
        let ready_indices: Vec<usize> = deferred
            .iter()
            .enumerate()
            .filter(|(_, dp)| dp.required_vars.is_subset(bound_vars))
            .map(|(idx, _)| idx)
            .collect();

        if ready_indices.is_empty() {
            break;
        }

        // Remove in reverse order to preserve indices, then sort by original position.
        let mut ready: Vec<DeferredPattern> = ready_indices
            .into_iter()
            .rev()
            .map(|idx| deferred.remove(idx))
            .collect();
        ready.sort_by_key(|dp| dp.orig_index);

        for dp in ready {
            let nested = result
                .last_mut()
                .is_some_and(|last| try_nest_deferred(last, &dp));

            // A deferred pattern's produced variables must enter the bound set
            // so later patterns referencing them place after and correlate.
            // This covers BIND/UNWIND (their target var) and a deferred
            // ShortestPath (its path var) — without it, e.g. an UNWIND that
            // reads a deferred shortestPath's path never becomes ready and
            // lands after a property accessor on the unwound var, cross-joining.
            for v in dp.pattern.produced_vars() {
                bound_vars.insert(v);
            }

            if !nested {
                result.push(dp.pattern);
            }
        }
    }
}

/// Return the *input* variables that must be bound before a deferred pattern
/// can execute.
///
/// - FILTER: all referenced variables
/// - BIND: the expression's variables (not the target variable)
///
/// Variables a subquery correlates on: its SELECT-list variables that are also
/// produced by some OTHER pattern in the enclosing group. These must be bound
/// before the subquery runs, so the planner defers it until they are. An empty
/// result means the subquery is uncorrelated (safe to place early).
///
/// A shared variable that the subquery **produces itself** (binds in its own
/// WHERE — e.g. a `GROUP BY` key) is a JOIN key, not a correlation input:
/// seeding it per outer row is equivalent to evaluating the subquery once and
/// filtering its output to that value, so it can run once and be hash-joined.
///
/// Two safety conditions on this declassification:
/// 1. **No inner slice.** With `LIMIT`/`OFFSET`, the per-row restriction changes
///    which rows survive the slice (e.g. `ORDER BY DESC(?x) LIMIT 1` means "top
///    row per outer binding"), so such a subquery stays genuinely correlated.
/// 2. **Unconditionally bound.** The variable must be bound in *every* subquery
///    solution — produced by a top-level required pattern (a triple or property
///    path), NOT inside a `UNION` branch or `OPTIONAL`. A conditionally-bound
///    var can be Unbound in some output rows; evaluating once and joining would
///    then differ from per-row seeding (an Unbound join key would scan rather
///    than filter). We only count always-bound producers.
fn subquery_correlation_vars(
    sq: &SubqueryPattern,
    siblings: &[Pattern],
    self_idx: usize,
) -> HashSet<VarId> {
    let select: HashSet<VarId> = sq.select.iter().copied().collect();
    if select.is_empty() {
        return HashSet::new();
    }
    // Variables the subquery binds in EVERY solution on its own — but only when
    // no inner slice makes per-row seeding result-sensitive. Restricted to
    // top-level UNCONDITIONAL producers so a var that is only conditionally
    // bound (UNION branch, OPTIONAL) is NOT declassified. Besides triples /
    // property paths this must include the WITH-pipeline binders — UNWIND, BIND,
    // VALUES — otherwise a var the subquery produces via one of them is
    // mistaken for an external correlation, the subquery is deferred on a var
    // only it can bind (so it never becomes ready and is placed last), and a
    // consuming OPTIONAL/Filter runs first uncorrelated, clobbering that var.
    let self_produced: HashSet<VarId> = if sq.limit.is_none() && sq.offset.is_none() {
        sq.patterns
            .iter()
            .filter(|p| {
                matches!(
                    p,
                    Pattern::Triple(_)
                        | Pattern::PropertyPath(_)
                        | Pattern::Unwind { .. }
                        | Pattern::Bind { .. }
                        | Pattern::Values { .. }
                )
            })
            .flat_map(Pattern::produced_vars)
            .collect()
    } else {
        HashSet::new()
    };
    // A correlation input must be bound BEFORE the subquery runs, so only a
    // PRECEDING sibling can supply one. A following sibling that re-produces a
    // select var is a downstream CONSUMER, not a correlation — e.g. the Cypher
    // WITH pipeline `… WITH m [ORDER BY m.x] LIMIT n  MATCH (m)-…`: the WITH
    // lowers to this subquery and the trailing MATCH (plus the deterministic
    // `?#__prop_m_x` accessor a later `RETURN m.x` shares) re-produces `m`/`m.x`
    // AFTER it. Without the position guard those looked like correlations on a
    // sliced subquery (slice empties `self_produced`), so the WITH was deferred
    // behind its own consumer and the consuming MATCH ran first as an unseeded
    // scan — silently empty results or an ignored limit. Restricting to
    // preceding siblings keeps a genuinely correlated sliced sub-SELECT (its
    // producer precedes it) per-row while letting the WITH producer lead.
    let mut corr = HashSet::new();
    for (j, p) in siblings.iter().enumerate().take(self_idx) {
        debug_assert!(j < self_idx);
        for v in p.produced_vars() {
            if select.contains(&v) && !self_produced.contains(&v) {
                corr.insert(v);
            }
        }
    }
    corr
}

/// Variables a subquery binds itself and exposes in its SELECT — its
/// WITH-pipeline outputs. A sibling that consumes one of these must run after
/// the subquery (see the call site in `reorder_patterns`).
fn subquery_produced_select_vars(sq: &SubqueryPattern) -> HashSet<VarId> {
    let select: HashSet<VarId> = sq.select.iter().copied().collect();
    sq.patterns
        .iter()
        .flat_map(Pattern::produced_vars)
        .filter(|v| select.contains(v))
        .collect()
}

fn deferred_required_vars(pattern: &Pattern) -> Vec<VarId> {
    match pattern {
        Pattern::Filter(expr) => expr.referenced_vars(),
        Pattern::Bind { expr, .. } => expr.referenced_vars(),
        Pattern::Unwind { list, .. } => list.referenced_vars(),
        // Other patterns should not be classified as Deferred, but handle
        // gracefully by returning all referenced variables.
        other => other.referenced_vars(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::triple::Term;
    use crate::ir::GraphName;
    use fluree_db_core::{PropertyStatData, Sid, StatsView};
    use std::sync::Arc;

    fn make_pattern(s: VarId, p_name: &str, o: VarId) -> TriplePattern {
        TriplePattern::new(Ref::Var(s), Ref::Sid(Sid::new(100, p_name)), Term::Var(o))
    }

    #[test]
    fn populated_stats_treat_missing_predicate_as_empty() {
        let mut stats = StatsView::default();
        stats.properties.insert(
            Sid::new(100, "known"),
            PropertyStatData {
                count: 250_000,
                ndv_values: 200_000,
                ndv_subjects: 200_000,
            },
        );

        let missing = make_pattern(VarId(0), "missing", VarId(1));
        assert_eq!(
            estimate_triple_row_count(&missing, &HashSet::new(), Some(&stats)),
            0.0
        );

        let unknown_stats = StatsView::default();
        assert_eq!(
            estimate_triple_row_count(&missing, &HashSet::new(), Some(&unknown_stats)),
            DEFAULT_PROPERTY_SCAN_SELECTIVITY
        );
    }

    #[test]
    fn absent_predicate_drives_before_large_known_scan() {
        let mut stats = StatsView::default();
        stats.properties.insert(
            Sid::new(100, "large"),
            PropertyStatData {
                count: 250_000,
                ndv_values: 200_000,
                ndv_subjects: 200_000,
            },
        );

        let large = Pattern::Triple(make_pattern(VarId(0), "large", VarId(1)));
        let missing = Pattern::Triple(make_pattern(VarId(2), "missing", VarId(3)));
        let ordered = reorder_patterns(&[large, missing], Some(&stats), &HashSet::new());
        assert!(
            matches!(
                &ordered[0],
                Pattern::Triple(tp)
                    if matches!(&tp.p, Ref::Sid(sid) if sid.name.as_ref() == "missing")
            ),
            "missing predicate should drive first: {ordered:?}"
        );
    }

    #[test]
    fn class_stats_win_before_missing_rdf_type_property_zero() {
        let class = Sid::new(100, "Class");
        let mut stats = StatsView::default();
        stats.properties.insert(
            Sid::new(100, "known"),
            PropertyStatData {
                count: 250_000,
                ndv_values: 200_000,
                ndv_subjects: 200_000,
            },
        );
        stats.classes.insert(class.clone(), 42);

        let pattern = TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(Sid::new(
                fluree_vocab::namespaces::RDF,
                fluree_vocab::predicates::RDF_TYPE,
            )),
            Term::Sid(class),
        );
        assert_eq!(
            estimate_triple_row_count(&pattern, &HashSet::new(), Some(&stats)),
            42.0
        );
    }

    #[test]
    fn anchored_property_path_is_selective_and_drives_the_join() {
        // Issue #1287: <c912> skos:broader+ ?b . ?b skos:prefLabel ?lbl
        use crate::ir::path::{PathModifier, PropertyPathPattern};
        let b = VarId(0);
        let lbl = VarId(1);
        let bound = HashSet::new();

        // Anchored at a constant subject => bounded closure (selective).
        let anchored = Pattern::PropertyPath(PropertyPathPattern::new(
            Ref::Sid(Sid::new(9, "c912")),
            Sid::new(1, "broader"),
            PathModifier::OneOrMore,
            Ref::Var(b),
        ));
        assert_eq!(
            estimate_pattern(&anchored, &bound, None).row_count(),
            ANCHORED_PROPERTY_PATH_SELECTIVITY
        );

        // Both endpoints free => genuinely unbounded, keep the world-scan estimate.
        let unanchored = Pattern::PropertyPath(PropertyPathPattern::new(
            Ref::Var(VarId(2)),
            Sid::new(1, "broader"),
            PathModifier::OneOrMore,
            Ref::Var(b),
        ));
        assert_eq!(
            estimate_pattern(&unanchored, &bound, None).row_count(),
            DEFAULT_PROPERTY_SCAN_SELECTIVITY
        );

        // End to end: the anchored path drives ahead of a 150k prefLabel scan even
        // when written after it — otherwise the planner full-scans prefLabel (88s).
        let pref = Pattern::Triple(TriplePattern::new(
            Ref::Var(b),
            Ref::Sid(Sid::new(2, "prefLabel")),
            Term::Var(lbl),
        ));
        let mut stats = StatsView::default();
        stats.properties.insert(
            Sid::new(2, "prefLabel"),
            PropertyStatData {
                count: 150_000,
                ndv_values: 150_000,
                ndv_subjects: 6_000,
            },
        );
        let ordered = reorder_patterns(&[pref, anchored], Some(&stats), &HashSet::new());
        assert!(
            matches!(&ordered[0], Pattern::PropertyPath(_)),
            "anchored path must drive before the high-cardinality scan: {ordered:?}"
        );
    }

    /// Count top-level patterns matching a predicate.
    fn count_patterns(patterns: &[Pattern], pred: fn(&Pattern) -> bool) -> usize {
        patterns.iter().filter(|p| pred(p)).count()
    }

    /// Assert that every branch of the first UNION in `patterns` contains
    /// at least one pattern matching `pred`.
    fn assert_union_branches_contain(patterns: &[Pattern], pred: fn(&Pattern) -> bool, msg: &str) {
        let union = patterns
            .iter()
            .find(|p| matches!(p, Pattern::Union(_)))
            .expect("expected a UNION in the pattern list");
        if let Pattern::Union(branches) = union {
            for (i, branch) in branches.iter().enumerate() {
                assert!(branch.iter().any(&pred), "UNION branch {i}: {msg}");
            }
        }
    }

    #[test]
    fn test_reorder_patterns() {
        // When the first pattern placed shares variables with others,
        // subsequent joinable patterns are preferred over non-joinable ones.
        //
        // p1: ?s :name ?name (shares ?s with p3)
        // p2: ?x :type ?t   (disjoint)
        // p3: ?s :age ?age   (shares ?s with p1)
        //
        // Input order puts p1 first so the greedy algorithm picks it,
        // then prefers p3 (joinable via ?s) over p2 (non-joinable).

        let p1 = make_pattern(VarId(0), "name", VarId(1));
        let p2 = make_pattern(VarId(2), "type", VarId(3));
        let p3 = make_pattern(VarId(0), "age", VarId(4));

        let patterns: Vec<Pattern> = vec![p1, p2, p3].into_iter().map(Pattern::Triple).collect();
        let ordered = reorder_patterns(&patterns, None, &HashSet::new());

        assert_eq!(ordered.len(), 3);

        // p1 placed first (original position tiebreaker, all equal estimates)
        let first = match &ordered[0] {
            Pattern::Triple(tp) => tp,
            _ => panic!("expected Triple pattern"),
        };
        assert!(first.produced_vars().contains(&VarId(0)));
        assert!(first.produced_vars().contains(&VarId(1)));

        // p3 placed second (shares ?s=VarId(0) with p1, preferred over disjoint p2)
        let second = match &ordered[1] {
            Pattern::Triple(tp) => tp,
            _ => panic!("expected Triple pattern"),
        };
        assert!(second.produced_vars().contains(&VarId(0)));
        assert!(second.produced_vars().contains(&VarId(4)));

        // p2 placed last (disjoint, no joinable preference)
        let last = match &ordered[2] {
            Pattern::Triple(tp) => tp,
            _ => panic!("expected Triple pattern"),
        };
        assert!(last.produced_vars().contains(&VarId(2)));
        assert!(last.produced_vars().contains(&VarId(3)));
    }

    #[test]
    fn test_reorder_patterns_seeded_prefers_joinable_over_more_selective_cartesian() {
        // If vars are already bound from an upstream operator, prefer patterns that
        // join with those vars to avoid cartesian explosions.

        let s = VarId(0);
        let o1 = VarId(1);
        let x = VarId(2);
        let y = VarId(3);

        // Joinable with seed (?s)
        let joinable =
            TriplePattern::new(Ref::Var(s), Ref::Sid(Sid::new(100, "wide")), Term::Var(o1));

        // Not joinable with seed, but extremely selective
        let non_joining =
            TriplePattern::new(Ref::Var(x), Ref::Sid(Sid::new(100, "narrow")), Term::Var(y));

        let mut stats = StatsView::default();
        stats.properties.insert(
            Sid::new(100, "wide"),
            fluree_db_core::PropertyStatData {
                count: 1_000_000,
                ndv_values: 1_000_000,
                ndv_subjects: 1_000_000,
            },
        );
        stats.properties.insert(
            Sid::new(100, "narrow"),
            fluree_db_core::PropertyStatData {
                count: 1,
                ndv_values: 1,
                ndv_subjects: 1,
            },
        );

        let mut seed = HashSet::new();
        seed.insert(s);

        let patterns: Vec<Pattern> = vec![non_joining, joinable]
            .into_iter()
            .map(Pattern::Triple)
            .collect();
        let ordered = reorder_patterns(&patterns, Some(&stats), &seed);

        let first = match &ordered[0] {
            Pattern::Triple(tp) => tp,
            _ => panic!("expected Triple pattern"),
        };
        assert!(
            first.produced_vars().contains(&s),
            "expected first pattern to join with seeded bound vars"
        );
    }

    #[test]
    fn reorder_bowtie_drives_the_side_that_unlocks_the_bigger_hash_join() {
        // BSBM-BI F2 shape, written producer/DE-first:
        //   ?product producer ?producer . ?producer country DE .
        //   ?review reviewFor ?product . ?review reviewer ?reviewer . ?reviewer country US .
        // Both country filters tie on estimate (same predicate). The tie-break must
        // drive from the US side, because that makes the 2.85M-row rev:reviewer join
        // an object→subject hash join; DE-first only unlocks the 285K producer join
        // and leaves the reviewer chain as forward joins (the 110s→2.38s case).
        let product = VarId(0);
        let producer = VarId(1);
        let review = VarId(2);
        let reviewer = VarId(3);
        let pat = |s: VarId, p: &str, o: Term| {
            Pattern::Triple(TriplePattern::new(Ref::Var(s), Ref::Sid(Sid::new(1, p)), o))
        };
        let patterns = vec![
            pat(product, "producer", Term::Var(producer)),
            pat(producer, "country", Term::Sid(Sid::new(9, "DE"))),
            pat(review, "reviewFor", Term::Var(product)),
            pat(review, "reviewer", Term::Var(reviewer)),
            pat(reviewer, "country", Term::Sid(Sid::new(9, "US"))),
        ];

        let mut stats = StatsView::default();
        let mut put = |name: &str, count: u64, ndv_values: u64| {
            stats.properties.insert(
                Sid::new(1, name),
                PropertyStatData {
                    count,
                    ndv_values,
                    ndv_subjects: count,
                },
            );
        };
        put("producer", 284_826, 5_600);
        put("country", 386_525, 25); // count/ndv = 15,461 — both filters tie here
        put("reviewFor", 2_848_260, 280_000);
        put("reviewer", 2_848_260, 570_000);

        let ordered = reorder_patterns(&patterns, Some(&stats), &HashSet::new());
        let first = match &ordered[0] {
            Pattern::Triple(tp) => tp,
            _ => panic!("expected Triple first"),
        };
        // Drives from the US country filter (binds ?reviewer), not the DE one.
        assert_eq!(first.p.as_sid().map(|s| &*s.name), Some("country"));
        assert_eq!(
            first.o.as_sid().map(|s| &*s.name),
            Some("US"),
            "tie-break should drive the side that unlocks the 2.85M rev:reviewer hash join, got {ordered:?}"
        );
        // ...and rev:reviewer is placed before producer (US chain first).
        let pred_order: Vec<&str> = ordered
            .iter()
            .filter_map(|p| match p {
                Pattern::Triple(tp) => tp.p.as_sid().map(|s| &*s.name),
                _ => None,
            })
            .collect();
        let pos = |name: &str| pred_order.iter().position(|p| *p == name).unwrap();
        assert!(
            pos("reviewer") < pos("producer"),
            "reviewer chain should precede producer chain: {pred_order:?}"
        );
    }

    #[test]
    fn test_reorder_patterns_with_stats_but_iri_predicates_dont_use_stats_yet() {
        // This test is intentionally designed to expose a current gap:
        //
        // - The query parser/lowering emits `Term::Iri` for IRIs to support cross-ledger joins.
        // - Property statistics in `StatsView` are keyed by `Sid`.
        // - The planner's selectivity calculation only consults stats
        //   when it can extract a `Sid` via `pattern.p.as_sid()`.
        //
        // As a result, even when stats *exist*, planning may silently fall back to default scoring
        // for parsed queries, because their predicates are `Term::Iri`.
        //
        // Desired behavior (future fix): if stats exist for a predicate IRI, we should use them
        // so that join order reflects cardinality/selectivity.

        let s = VarId(0);
        let o1 = VarId(1);
        let o2 = VarId(2);

        // Two property-scan patterns with IRI predicates.
        let p_a = TriplePattern::new(
            Ref::Var(s),
            Ref::Iri(Arc::from("http://example.org/a")),
            Term::Var(o1),
        );
        let p_z = TriplePattern::new(
            Ref::Var(s),
            Ref::Iri(Arc::from("http://example.org/z")),
            Term::Var(o2),
        );

        // Stats say predicate "z" is far more selective (count 1 vs 1000).
        // If stats were consulted during planning, p_z should be ordered first.
        let mut stats = StatsView::default();
        stats.properties.insert(
            Sid::new(100, "a"),
            PropertyStatData {
                count: 1000,
                ndv_values: 1000,
                ndv_subjects: 1000,
            },
        );
        stats.properties.insert(
            Sid::new(100, "z"),
            PropertyStatData {
                count: 1,
                ndv_values: 1,
                ndv_subjects: 1,
            },
        );
        // Populate IRI-keyed maps as a stand-in for the real `from_db_stats_with_namespaces`
        // construction used in execution.
        stats.properties_by_iri.insert(
            Arc::from("http://example.org/a"),
            PropertyStatData {
                count: 1000,
                ndv_values: 1000,
                ndv_subjects: 1000,
            },
        );
        stats.properties_by_iri.insert(
            Arc::from("http://example.org/z"),
            PropertyStatData {
                count: 1,
                ndv_values: 1,
                ndv_subjects: 1,
            },
        );

        let patterns: Vec<Pattern> = vec![p_a, p_z].into_iter().map(Pattern::Triple).collect();
        let ordered = reorder_patterns(&patterns, Some(&stats), &HashSet::new());

        // EXPECTATION (desired): stats-driven selectivity should pick predicate ".../z" first.
        //
        // CURRENT (bug/gap): because predicates are Term::Iri, stats lookups miss and ordering
        // falls back to original position as tie-breaker.
        let first = match &ordered[0] {
            Pattern::Triple(tp) => tp,
            _ => panic!("expected Triple pattern"),
        };
        assert_eq!(
            first.p.as_iri(),
            Some("http://example.org/z"),
            "expected stats-driven ordering to pick the most selective predicate first; got ordered[0]={:?}",
            ordered[0]
        );
    }

    #[test]
    fn estimate_uses_merged_annotation_stats_for_reifies_predicates() {
        // After `StatsView::merge_annotation_stats` runs with per-slot
        // NDVs, the planner's classifier should produce arena-aligned
        // BoundObject estimates: `count / ndv_values` for the matching
        // slot's NDV, not the conservative fallback.

        use fluree_db_core::AnnotationStats;
        use fluree_vocab::db as p;
        use fluree_vocab::namespaces::FLUREE_DB;

        let mut stats = StatsView::default();
        let ann = AnnotationStats {
            forward_rows: 1_000,
            reverse_rows: 1_000,
            distinct_edges: 200,
            distinct_annotations: 800,
            live_attachment_pairs: 800,
            distinct_reified_subjects: 50,
            distinct_reified_predicates: 4,
            distinct_reified_objects: 200,
            ..Default::default()
        };
        let mut ns = std::collections::HashMap::new();
        ns.insert(FLUREE_DB, "https://ns.flur.ee/db#".to_string());
        stats.merge_annotation_stats(&ann, &ns);

        // PropertyScan: `?ann f:reifiesObject ?o` — total annotations.
        let scan = TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(Sid::new(FLUREE_DB, p::REIFIES_OBJECT)),
            Term::Var(VarId(1)),
        );
        let scan_est = estimate_triple_row_count(&scan, &HashSet::new(), Some(&stats));
        assert_eq!(
            scan_est, 800.0,
            "PropertyScan should equal annotation count"
        );

        // BoundObject: `?ann f:reifiesObject <some_object>`. With per-
        // slot NDV the estimate is `800 / 200 = 4` — annotations per
        // pinned object.
        let bound_o = TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(Sid::new(FLUREE_DB, p::REIFIES_OBJECT)),
            Term::Sid(Sid::new(7, "obj1")),
        );
        let bound_o_est = estimate_triple_row_count(&bound_o, &HashSet::new(), Some(&stats));
        assert_eq!(
            bound_o_est, 4.0,
            "BoundObject on reifiesObject should be distinct_annotations / distinct_reified_objects"
        );

        // BoundSubject: a known annotation subject probing its slot.
        let mut bound_subj_ctx = HashSet::new();
        bound_subj_ctx.insert(VarId(0));
        let bound_s = TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(Sid::new(FLUREE_DB, p::REIFIES_SUBJECT)),
            Term::Var(VarId(2)),
        );
        let bound_s_est = estimate_triple_row_count(&bound_s, &bound_subj_ctx, Some(&stats));
        assert_eq!(
            bound_s_est, 1.0,
            "BoundSubject on reifiesSubject should be ~1 row per known annotation"
        );

        // BoundObject on reifiesPredicate: 800 / 4 = 200 annotations
        // per pinned predicate. Larger than reifiesObject's
        // selectivity here, which is realistic — predicates are
        // typically a small set even at scale.
        let bound_p = TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(Sid::new(FLUREE_DB, p::REIFIES_PREDICATE)),
            Term::Sid(Sid::new(7, "worksFor")),
        );
        let bound_p_est = estimate_triple_row_count(&bound_p, &HashSet::new(), Some(&stats));
        assert_eq!(
            bound_p_est, 200.0,
            "BoundObject on reifiesPredicate should be distinct_annotations / distinct_reified_predicates"
        );
    }

    #[test]
    fn test_is_property_join() {
        // Valid property join: ?s :name ?n, ?s :age ?a
        let p1 = make_pattern(VarId(0), "name", VarId(1));
        let p2 = make_pattern(VarId(0), "age", VarId(2));
        assert!(is_property_join(&[p1.clone(), p2.clone()]));

        // Bound-object existence predicates are allowed when the star still shares one subject.
        let p2_bound = TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(Sid::new(100, "type")),
            Term::Sid(Sid::new(100, "Deal")),
        );
        assert!(is_property_join(&[p1.clone(), p2_bound]));

        // Not property join: different subjects
        let p3 = make_pattern(VarId(3), "type", VarId(4));
        assert!(!is_property_join(&[p1.clone(), p3]));

        // Not property join: single pattern
        assert!(!is_property_join(std::slice::from_ref(&p1)));

        // Not property join: predicate is var
        let p4 = TriplePattern::new(Ref::Var(VarId(0)), Ref::Var(VarId(5)), Term::Var(VarId(1)));
        assert!(!is_property_join(&[p1, p4]));
    }

    // Range extraction tests
    use crate::ir::{Expression, FlakeValue, Function};

    #[test]
    fn test_extract_range_simple_gt() {
        // ?age > 18
        let expr = Expression::gt(
            Expression::Var(VarId(0)),
            Expression::Const(FlakeValue::Long(18)),
        );

        let constraints = extract_range_constraints(&expr).expect("should extract");
        assert_eq!(constraints.len(), 1);

        let c = &constraints[0];
        assert_eq!(c.var, VarId(0));
        assert_eq!(c.lower, Some((RangeValue::Long(18), false))); // > is exclusive
        assert_eq!(c.upper, None);
    }

    #[test]
    fn test_extract_range_simple_le() {
        // ?age <= 65
        let expr = Expression::le(
            Expression::Var(VarId(0)),
            Expression::Const(FlakeValue::Long(65)),
        );

        let constraints = extract_range_constraints(&expr).expect("should extract");
        let c = &constraints[0];

        assert_eq!(c.lower, None);
        assert_eq!(c.upper, Some((RangeValue::Long(65), true))); // <= is inclusive
    }

    #[test]
    fn test_extract_range_eq() {
        // ?status = "active"
        let expr = Expression::eq(
            Expression::Var(VarId(0)),
            Expression::Const(FlakeValue::String("active".to_string())),
        );

        let constraints = extract_range_constraints(&expr).expect("should extract");
        let c = &constraints[0];

        // Equality means lower = upper = val, both inclusive
        assert_eq!(
            c.lower,
            Some((RangeValue::String("active".to_string()), true))
        );
        assert_eq!(
            c.upper,
            Some((RangeValue::String("active".to_string()), true))
        );
    }

    #[test]
    fn test_extract_range_reversed_comparison() {
        // 18 < ?age (means ?age > 18)
        let expr = Expression::lt(
            Expression::Const(FlakeValue::Long(18)),
            Expression::Var(VarId(0)),
        );

        let constraints = extract_range_constraints(&expr).expect("should extract");
        let c = &constraints[0];

        // 18 < ?age means ?age > 18
        assert_eq!(c.var, VarId(0));
        assert_eq!(c.lower, Some((RangeValue::Long(18), false))); // > is exclusive
        assert_eq!(c.upper, None);
    }

    #[test]
    fn test_extract_range_and_merges() {
        // ?age >= 18 AND ?age < 65
        let expr = Expression::and(vec![
            Expression::ge(
                Expression::Var(VarId(0)),
                Expression::Const(FlakeValue::Long(18)),
            ),
            Expression::lt(
                Expression::Var(VarId(0)),
                Expression::Const(FlakeValue::Long(65)),
            ),
        ]);

        let constraints = extract_range_constraints(&expr).expect("should extract");
        assert_eq!(constraints.len(), 1);

        let c = &constraints[0];
        assert_eq!(c.var, VarId(0));
        assert_eq!(c.lower, Some((RangeValue::Long(18), true))); // >= inclusive
        assert_eq!(c.upper, Some((RangeValue::Long(65), false))); // < exclusive
    }

    #[test]
    fn test_extract_range_and_multiple_vars() {
        // ?age >= 18 AND ?score > 100
        let expr = Expression::and(vec![
            Expression::ge(
                Expression::Var(VarId(0)),
                Expression::Const(FlakeValue::Long(18)),
            ),
            Expression::gt(
                Expression::Var(VarId(1)),
                Expression::Const(FlakeValue::Long(100)),
            ),
        ]);

        let constraints = extract_range_constraints(&expr).expect("should extract");
        assert_eq!(constraints.len(), 2);

        // Check both vars have constraints
        let vars: Vec<_> = constraints.iter().map(|c| c.var).collect();
        assert!(vars.contains(&VarId(0)));
        assert!(vars.contains(&VarId(1)));
    }

    #[test]
    fn test_extract_range_or_not_supported() {
        // OR is not range-safe
        let expr = Expression::or(vec![
            Expression::eq(
                Expression::Var(VarId(0)),
                Expression::Const(FlakeValue::Long(1)),
            ),
            Expression::eq(
                Expression::Var(VarId(0)),
                Expression::Const(FlakeValue::Long(2)),
            ),
        ]);

        assert!(extract_range_constraints(&expr).is_none());
    }

    #[test]
    fn test_extract_range_double_values() {
        // ?price > 19.99
        let expr = Expression::gt(
            Expression::Var(VarId(0)),
            Expression::Const(FlakeValue::Double(19.99)),
        );

        let constraints = extract_range_constraints(&expr).expect("should extract");
        let c = &constraints[0];

        assert_eq!(c.lower, Some((RangeValue::Double(19.99), false)));
    }

    #[test]
    fn test_range_constraint_merge_tighter_lower() {
        // ?age > 10 AND ?age > 20 => lower should be 20
        let mut c1 = RangeConstraint::new(VarId(0)).with_lower(RangeValue::Long(10), false);
        let c2 = RangeConstraint::new(VarId(0)).with_lower(RangeValue::Long(20), false);

        c1.merge(&c2);
        assert_eq!(c1.lower, Some((RangeValue::Long(20), false)));
    }

    #[test]
    fn test_range_constraint_merge_tighter_upper() {
        // ?age < 100 AND ?age < 65 => upper should be 65
        let mut c1 = RangeConstraint::new(VarId(0)).with_upper(RangeValue::Long(100), false);
        let c2 = RangeConstraint::new(VarId(0)).with_upper(RangeValue::Long(65), false);

        c1.merge(&c2);
        assert_eq!(c1.upper, Some((RangeValue::Long(65), false)));
    }

    #[test]
    fn test_range_constraint_merge_exclusivity() {
        // ?age >= 18 AND ?age > 18 => should be exclusive (tighter)
        let mut c1 = RangeConstraint::new(VarId(0)).with_lower(RangeValue::Long(18), true); // inclusive
        let c2 = RangeConstraint::new(VarId(0)).with_lower(RangeValue::Long(18), false); // exclusive

        c1.merge(&c2);
        // Exclusive is tighter than inclusive at the same value
        assert_eq!(c1.lower, Some((RangeValue::Long(18), false)));
    }

    #[test]
    fn test_range_constraint_unsatisfiable_lower_gt_upper() {
        // ?x > 10 AND ?x < 5 => unsatisfiable (lower > upper)
        let c = RangeConstraint::new(VarId(0))
            .with_lower(RangeValue::Long(10), false)
            .with_upper(RangeValue::Long(5), false);

        assert!(c.is_unsatisfiable());
    }

    #[test]
    fn test_range_constraint_unsatisfiable_equal_exclusive() {
        // ?x > 10 AND ?x < 10 => unsatisfiable (equal but both exclusive)
        let c = RangeConstraint::new(VarId(0))
            .with_lower(RangeValue::Long(10), false)
            .with_upper(RangeValue::Long(10), false);

        assert!(c.is_unsatisfiable());

        // ?x >= 10 AND ?x < 10 => unsatisfiable (equal, one exclusive)
        let c2 = RangeConstraint::new(VarId(0))
            .with_lower(RangeValue::Long(10), true)
            .with_upper(RangeValue::Long(10), false);

        assert!(c2.is_unsatisfiable());

        // ?x > 10 AND ?x <= 10 => unsatisfiable (equal, one exclusive)
        let c3 = RangeConstraint::new(VarId(0))
            .with_lower(RangeValue::Long(10), false)
            .with_upper(RangeValue::Long(10), true);

        assert!(c3.is_unsatisfiable());
    }

    #[test]
    fn test_range_constraint_satisfiable_equal_inclusive() {
        // ?x >= 10 AND ?x <= 10 => satisfiable (exactly 10)
        let c = RangeConstraint::new(VarId(0))
            .with_lower(RangeValue::Long(10), true)
            .with_upper(RangeValue::Long(10), true);

        assert!(!c.is_unsatisfiable());
    }

    #[test]
    fn test_range_constraint_satisfiable_normal_range() {
        // ?x >= 5 AND ?x <= 10 => satisfiable
        let c = RangeConstraint::new(VarId(0))
            .with_lower(RangeValue::Long(5), true)
            .with_upper(RangeValue::Long(10), true);

        assert!(!c.is_unsatisfiable());
    }

    #[test]
    fn test_range_constraint_satisfiable_open_ended() {
        // Only lower bound => always satisfiable
        let c1 = RangeConstraint::new(VarId(0)).with_lower(RangeValue::Long(10), false);
        assert!(!c1.is_unsatisfiable());

        // Only upper bound => always satisfiable
        let c2 = RangeConstraint::new(VarId(0)).with_upper(RangeValue::Long(10), false);
        assert!(!c2.is_unsatisfiable());

        // No bounds => always satisfiable
        let c3 = RangeConstraint::new(VarId(0));
        assert!(!c3.is_unsatisfiable());
    }

    // Tests for object bounds conversion and pushdown

    #[test]
    fn test_range_value_to_flake_value() {
        assert_eq!(RangeValue::Long(42).to_flake_value(), FlakeValue::Long(42));
        assert_eq!(
            RangeValue::String("hello".to_string()).to_flake_value(),
            FlakeValue::String("hello".to_string())
        );
        // Double conversion
        let d = RangeValue::Double(3.13).to_flake_value();
        match d {
            FlakeValue::Double(v) => assert!((v - 3.13).abs() < 0.001),
            _ => panic!("Expected Double"),
        }
    }

    #[test]
    fn test_range_constraint_to_object_bounds() {
        // Lower bound only
        let c1 = RangeConstraint::new(VarId(0)).with_lower(RangeValue::Long(18), false);
        let bounds = c1.to_object_bounds().expect("should have bounds");
        assert!(!bounds.is_empty());
        // Verify it filters correctly
        assert!(!bounds.matches(&FlakeValue::Long(18))); // exclusive
        assert!(bounds.matches(&FlakeValue::Long(19)));

        // Two-sided bounds
        let c2 = RangeConstraint::new(VarId(0))
            .with_lower(RangeValue::Long(18), false)
            .with_upper(RangeValue::Long(65), true);
        let bounds = c2.to_object_bounds().expect("should have bounds");
        assert!(!bounds.matches(&FlakeValue::Long(18))); // exclusive lower
        assert!(bounds.matches(&FlakeValue::Long(19)));
        assert!(bounds.matches(&FlakeValue::Long(65))); // inclusive upper
        assert!(!bounds.matches(&FlakeValue::Long(66)));
    }

    #[test]
    fn test_range_constraint_to_object_bounds_empty() {
        // No bounds => None
        let c = RangeConstraint::new(VarId(0));
        assert!(c.to_object_bounds().is_none());
    }

    #[test]
    fn test_extract_object_bounds_for_var_simple() {
        // ?age > 18
        let filter = Expression::gt(
            Expression::Var(VarId(0)),
            Expression::Const(FlakeValue::Long(18)),
        );

        // Extract for ?age (VarId(0))
        let bounds =
            extract_object_bounds_for_var(&filter, VarId(0)).expect("should extract bounds");

        // Should filter 18 out (exclusive) but include 19
        assert!(!bounds.matches(&FlakeValue::Long(18)));
        assert!(bounds.matches(&FlakeValue::Long(19)));
        assert!(bounds.matches(&FlakeValue::Long(100)));
    }

    #[test]
    fn test_extract_object_bounds_for_var_two_sided() {
        // ?age > 18 AND ?age < 65
        let filter = Expression::and(vec![
            Expression::gt(
                Expression::Var(VarId(0)),
                Expression::Const(FlakeValue::Long(18)),
            ),
            Expression::lt(
                Expression::Var(VarId(0)),
                Expression::Const(FlakeValue::Long(65)),
            ),
        ]);

        let bounds =
            extract_object_bounds_for_var(&filter, VarId(0)).expect("should extract bounds");

        assert!(!bounds.matches(&FlakeValue::Long(18)));
        assert!(bounds.matches(&FlakeValue::Long(19)));
        assert!(bounds.matches(&FlakeValue::Long(64)));
        assert!(!bounds.matches(&FlakeValue::Long(65)));
    }

    #[test]
    fn test_extract_object_bounds_for_var_wrong_var() {
        // ?age > 18 - but we ask for bounds on ?name (VarId(1))
        let filter = Expression::gt(
            Expression::Var(VarId(0)),
            Expression::Const(FlakeValue::Long(18)),
        );

        // No bounds for VarId(1)
        assert!(extract_object_bounds_for_var(&filter, VarId(1)).is_none());
    }

    #[test]
    fn test_extract_object_bounds_for_var_unsatisfiable() {
        // ?x > 10 AND ?x < 5 => unsatisfiable, returns None
        let filter = Expression::and(vec![
            Expression::gt(
                Expression::Var(VarId(0)),
                Expression::Const(FlakeValue::Long(10)),
            ),
            Expression::lt(
                Expression::Var(VarId(0)),
                Expression::Const(FlakeValue::Long(5)),
            ),
        ]);

        // Unsatisfiable range returns None (no point in pushdown)
        assert!(extract_object_bounds_for_var(&filter, VarId(0)).is_none());
    }

    #[test]
    fn test_extract_object_bounds_for_var_not_range_safe() {
        // OR is not range-safe
        let filter = Expression::or(vec![
            Expression::eq(
                Expression::Var(VarId(0)),
                Expression::Const(FlakeValue::Long(18)),
            ),
            Expression::eq(
                Expression::Var(VarId(0)),
                Expression::Const(FlakeValue::Long(21)),
            ),
        ]);

        assert!(extract_object_bounds_for_var(&filter, VarId(0)).is_none());
    }

    // =========================================================================
    // Sandwich (3-arg variadic comparison) tests
    // =========================================================================

    /// Build a 3-arg comparison expression: (op a b c)
    fn sandwich(func: Function, a: Expression, b: Expression, c: Expression) -> Expression {
        Expression::Call {
            func,
            args: vec![a, b, c],
        }
    }

    #[test]
    fn test_is_range_safe_sandwich() {
        // (< 10 ?x 20) → range-safe
        let expr = sandwich(
            Function::Lt,
            Expression::Const(FlakeValue::Long(10)),
            Expression::Var(VarId(0)),
            Expression::Const(FlakeValue::Long(20)),
        );
        assert!(expr.is_range_safe());

        // (<= 10 ?x 20) → range-safe
        let expr = sandwich(
            Function::Le,
            Expression::Const(FlakeValue::Long(10)),
            Expression::Var(VarId(0)),
            Expression::Const(FlakeValue::Long(20)),
        );
        assert!(expr.is_range_safe());

        // (> 20 ?x 10) → range-safe
        let expr = sandwich(
            Function::Gt,
            Expression::Const(FlakeValue::Long(20)),
            Expression::Var(VarId(0)),
            Expression::Const(FlakeValue::Long(10)),
        );
        assert!(expr.is_range_safe());

        // (>= 20 ?x 10) → range-safe
        let expr = sandwich(
            Function::Ge,
            Expression::Const(FlakeValue::Long(20)),
            Expression::Var(VarId(0)),
            Expression::Const(FlakeValue::Long(10)),
        );
        assert!(expr.is_range_safe());

        // (= 5 ?x 5) → range-safe
        let expr = sandwich(
            Function::Eq,
            Expression::Const(FlakeValue::Long(5)),
            Expression::Var(VarId(0)),
            Expression::Const(FlakeValue::Long(5)),
        );
        assert!(expr.is_range_safe());
    }

    #[test]
    fn test_is_range_safe_non_sandwich_variadic() {
        // (< ?x ?y 20) → NOT range-safe (var var const)
        let expr = Expression::Call {
            func: Function::Lt,
            args: vec![
                Expression::Var(VarId(0)),
                Expression::Var(VarId(1)),
                Expression::Const(FlakeValue::Long(20)),
            ],
        };
        assert!(!expr.is_range_safe());

        // (< 10 20 ?x) → NOT range-safe (const const var)
        let expr = Expression::Call {
            func: Function::Lt,
            args: vec![
                Expression::Const(FlakeValue::Long(10)),
                Expression::Const(FlakeValue::Long(20)),
                Expression::Var(VarId(0)),
            ],
        };
        assert!(!expr.is_range_safe());

        // (< ?x 10 ?y) → NOT range-safe (var const var)
        let expr = Expression::Call {
            func: Function::Lt,
            args: vec![
                Expression::Var(VarId(0)),
                Expression::Const(FlakeValue::Long(10)),
                Expression::Var(VarId(1)),
            ],
        };
        assert!(!expr.is_range_safe());
    }

    #[test]
    fn test_extract_range_sandwich_lt() {
        // (< 10 ?x 20) → lower=10 exclusive, upper=20 exclusive
        let expr = sandwich(
            Function::Lt,
            Expression::Const(FlakeValue::Long(10)),
            Expression::Var(VarId(0)),
            Expression::Const(FlakeValue::Long(20)),
        );

        let constraints = extract_range_constraints(&expr).expect("should extract");
        assert_eq!(constraints.len(), 1);

        let c = &constraints[0];
        assert_eq!(c.var, VarId(0));
        assert_eq!(c.lower, Some((RangeValue::Long(10), false))); // 10 < ?x → exclusive
        assert_eq!(c.upper, Some((RangeValue::Long(20), false))); // ?x < 20 → exclusive
    }

    #[test]
    fn test_extract_range_sandwich_le() {
        // (<= 10 ?x 20) → lower=10 inclusive, upper=20 inclusive
        let expr = sandwich(
            Function::Le,
            Expression::Const(FlakeValue::Long(10)),
            Expression::Var(VarId(0)),
            Expression::Const(FlakeValue::Long(20)),
        );

        let constraints = extract_range_constraints(&expr).expect("should extract");
        assert_eq!(constraints.len(), 1);

        let c = &constraints[0];
        assert_eq!(c.var, VarId(0));
        assert_eq!(c.lower, Some((RangeValue::Long(10), true))); // 10 <= ?x → inclusive
        assert_eq!(c.upper, Some((RangeValue::Long(20), true))); // ?x <= 20 → inclusive
    }

    #[test]
    fn test_extract_range_sandwich_gt() {
        // (> 20 ?x 10) → 20 > ?x > 10 → upper=20 exclusive, lower=10 exclusive
        let expr = sandwich(
            Function::Gt,
            Expression::Const(FlakeValue::Long(20)),
            Expression::Var(VarId(0)),
            Expression::Const(FlakeValue::Long(10)),
        );

        let constraints = extract_range_constraints(&expr).expect("should extract");
        assert_eq!(constraints.len(), 1);

        let c = &constraints[0];
        assert_eq!(c.var, VarId(0));
        // Pair 1: 20 > ?x → ?x < 20 → upper=20 exclusive
        assert_eq!(c.upper, Some((RangeValue::Long(20), false)));
        // Pair 2: ?x > 10 → lower=10 exclusive
        assert_eq!(c.lower, Some((RangeValue::Long(10), false)));
    }

    #[test]
    fn test_extract_range_sandwich_ge() {
        // (>= 20 ?x 10) → 20 >= ?x >= 10 → upper=20 inclusive, lower=10 inclusive
        let expr = sandwich(
            Function::Ge,
            Expression::Const(FlakeValue::Long(20)),
            Expression::Var(VarId(0)),
            Expression::Const(FlakeValue::Long(10)),
        );

        let constraints = extract_range_constraints(&expr).expect("should extract");
        assert_eq!(constraints.len(), 1);

        let c = &constraints[0];
        assert_eq!(c.var, VarId(0));
        // Pair 1: 20 >= ?x → ?x <= 20 → upper=20 inclusive
        assert_eq!(c.upper, Some((RangeValue::Long(20), true)));
        // Pair 2: ?x >= 10 → lower=10 inclusive
        assert_eq!(c.lower, Some((RangeValue::Long(10), true)));
    }

    #[test]
    fn test_extract_range_sandwich_eq() {
        // (= 5 ?x 5) → point range: lower=5 inclusive, upper=5 inclusive
        let expr = sandwich(
            Function::Eq,
            Expression::Const(FlakeValue::Long(5)),
            Expression::Var(VarId(0)),
            Expression::Const(FlakeValue::Long(5)),
        );

        let constraints = extract_range_constraints(&expr).expect("should extract");
        assert_eq!(constraints.len(), 1);

        let c = &constraints[0];
        assert_eq!(c.var, VarId(0));
        assert_eq!(c.lower, Some((RangeValue::Long(5), true)));
        assert_eq!(c.upper, Some((RangeValue::Long(5), true)));
        assert!(!c.is_unsatisfiable());
    }

    #[test]
    fn test_extract_range_sandwich_eq_unsatisfiable() {
        // (= 5 ?x 10) → both constants must be equal for satisfiability
        let expr = sandwich(
            Function::Eq,
            Expression::Const(FlakeValue::Long(5)),
            Expression::Var(VarId(0)),
            Expression::Const(FlakeValue::Long(10)),
        );

        let constraints = extract_range_constraints(&expr).expect("should extract");
        assert_eq!(constraints.len(), 1);

        let c = &constraints[0];
        // lower=5 inclusive, upper=10 inclusive (from two Eq constraints)
        // But Eq sets BOTH lower and upper, so merge produces:
        // lower = max(5, 10) = 10 inclusive, upper = min(5, 10) = 5 inclusive
        // This is unsatisfiable (lower > upper)
        assert!(c.is_unsatisfiable());
    }

    #[test]
    fn test_extract_range_sandwich_object_bounds() {
        // (< 10 ?x 20) should produce correct ObjectBounds
        let expr = sandwich(
            Function::Lt,
            Expression::Const(FlakeValue::Long(10)),
            Expression::Var(VarId(0)),
            Expression::Const(FlakeValue::Long(20)),
        );

        let bounds = extract_object_bounds_for_var(&expr, VarId(0)).expect("should extract bounds");

        assert!(!bounds.matches(&FlakeValue::Long(10))); // exclusive lower
        assert!(bounds.matches(&FlakeValue::Long(11)));
        assert!(bounds.matches(&FlakeValue::Long(19)));
        assert!(!bounds.matches(&FlakeValue::Long(20))); // exclusive upper
        assert!(!bounds.matches(&FlakeValue::Long(5))); // below range
        assert!(!bounds.matches(&FlakeValue::Long(25))); // above range
    }

    // =========================================================================
    // Generalized selectivity scoring and reordering tests
    // =========================================================================

    #[test]
    fn test_pattern_cardinality_variants() {
        use crate::ir::{Expression, FlakeValue, SubqueryPattern};

        let empty = HashSet::new();

        let triple = Pattern::Triple(make_pattern(VarId(0), "name", VarId(1)));
        assert!(matches!(
            estimate_pattern(&triple, &empty, None),
            PatternEstimate::Source { .. }
        ));

        let values = Pattern::Values {
            vars: vec![VarId(0)],
            rows: vec![],
        };
        assert!(matches!(
            estimate_pattern(&values, &empty, None),
            PatternEstimate::Source { .. }
        ));

        let union = Pattern::Union(vec![
            vec![Pattern::Triple(make_pattern(VarId(0), "a", VarId(1)))],
            vec![Pattern::Triple(make_pattern(VarId(0), "b", VarId(2)))],
        ]);
        assert!(matches!(
            estimate_pattern(&union, &empty, None),
            PatternEstimate::Source { .. }
        ));

        let subquery = Pattern::Subquery(SubqueryPattern::new(
            vec![VarId(0)],
            vec![Pattern::Triple(make_pattern(VarId(0), "x", VarId(1)))],
        ));
        assert!(matches!(
            estimate_pattern(&subquery, &empty, None),
            PatternEstimate::Source { .. }
        ));

        let optional = Pattern::Optional(vec![Pattern::Triple(make_pattern(
            VarId(0),
            "opt",
            VarId(3),
        ))]);
        assert!(matches!(
            estimate_pattern(&optional, &empty, None),
            PatternEstimate::Expander { .. }
        ));

        let minus = Pattern::Minus(vec![Pattern::Triple(make_pattern(
            VarId(0),
            "del",
            VarId(4),
        ))]);
        assert!(matches!(
            estimate_pattern(&minus, &empty, None),
            PatternEstimate::Deferred
        ));

        let exists = Pattern::Exists(vec![Pattern::Triple(make_pattern(VarId(0), "e", VarId(5)))]);
        assert!(matches!(
            estimate_pattern(&exists, &empty, None),
            PatternEstimate::Deferred
        ));

        let not_exists = Pattern::NotExists(vec![Pattern::Triple(make_pattern(
            VarId(0),
            "ne",
            VarId(6),
        ))]);
        assert!(matches!(
            estimate_pattern(&not_exists, &empty, None),
            PatternEstimate::Deferred
        ));

        let filter = Pattern::Filter(Expression::gt(
            Expression::Var(VarId(0)),
            Expression::Const(FlakeValue::Long(0)),
        ));
        assert!(matches!(
            estimate_pattern(&filter, &empty, None),
            PatternEstimate::Deferred
        ));

        let bind = Pattern::Bind {
            var: VarId(7),
            expr: Expression::Const(FlakeValue::Long(42)),
        };
        assert!(matches!(
            estimate_pattern(&bind, &empty, None),
            PatternEstimate::Deferred
        ));
    }

    #[test]
    fn test_estimate_values_cardinality() {
        use crate::binding::Binding;
        use fluree_db_core::Sid;

        let values = Pattern::Values {
            vars: vec![VarId(0)],
            rows: vec![
                vec![Binding::lit(FlakeValue::Long(1), Sid::new(2, "long"))],
                vec![Binding::lit(FlakeValue::Long(2), Sid::new(2, "long"))],
                vec![Binding::lit(FlakeValue::Long(3), Sid::new(2, "long"))],
            ],
        };

        let card = estimate_pattern(&values, &HashSet::new(), None);
        let PatternEstimate::Source { row_count } = card else {
            panic!("expected Source")
        };
        assert!((row_count - 3.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_estimate_union_sum_of_branches() {
        // UNION of two branches with known property selectivity
        let mut stats = StatsView::default();
        stats.properties.insert(
            Sid::new(100, "a"),
            PropertyStatData {
                count: 100,
                ndv_values: 50,
                ndv_subjects: 100,
            },
        );
        stats.properties.insert(
            Sid::new(100, "b"),
            PropertyStatData {
                count: 200,
                ndv_values: 100,
                ndv_subjects: 200,
            },
        );

        let union = Pattern::Union(vec![
            vec![Pattern::Triple(make_pattern(VarId(0), "a", VarId(1)))],
            vec![Pattern::Triple(make_pattern(VarId(0), "b", VarId(2)))],
        ]);

        let card = estimate_pattern(&union, &HashSet::new(), Some(&stats));
        let PatternEstimate::Source { row_count } = card else {
            panic!("expected Source")
        };
        // Sum of branch estimates: 100 + 200 = 300
        assert!((row_count - 300.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_estimate_optional_multiplier() {
        let optional = Pattern::Optional(vec![Pattern::Triple(make_pattern(
            VarId(0),
            "opt",
            VarId(1),
        ))]);

        let card = estimate_pattern(&optional, &HashSet::new(), None);
        let PatternEstimate::Expander { multiplier } = card else {
            panic!("expected Expander")
        };
        assert!(multiplier >= 1.0);
    }

    #[test]
    fn test_reorder_all_triple_only_passes_through() {
        // When zone contains only Triple/Filter/Bind patterns, reorder_patterns
        // passes them through unchanged (the existing collect_inner_join_block path
        // handles these).
        let patterns = vec![
            Pattern::Triple(make_pattern(VarId(0), "name", VarId(1))),
            Pattern::Triple(make_pattern(VarId(0), "age", VarId(2))),
        ];

        let reordered = reorder_patterns(&patterns, None, &HashSet::new());

        // Triple-only: passed through in original order
        assert_eq!(reordered.len(), 2);
        assert!(matches!(&reordered[0], Pattern::Triple(_)));
        assert!(matches!(&reordered[1], Pattern::Triple(_)));
    }

    #[test]
    fn test_reorder_minus_after_sources() {
        // MINUS is order-dependent (W3C §8.3): it operates on the solution
        // produced by ALL preceding patterns. Even when its correlation
        // variables are pre-bound, it should be placed after its preceding
        // source patterns to preserve left-hand-side semantics.
        let s = VarId(0);
        let o1 = VarId(1);
        let o2 = VarId(2);
        let d = VarId(3);

        let triple1 = Pattern::Triple(make_pattern(s, "selective", o1));
        let triple2 = Pattern::Triple(make_pattern(s, "wide", o2));
        let minus = Pattern::Minus(vec![Pattern::Triple(make_pattern(s, "deleted", d))]);

        let patterns = vec![triple1, triple2, minus];

        let mut bound = HashSet::new();
        bound.insert(s);

        let reordered = reorder_patterns(&patterns, None, &bound);

        // MINUS requires all variables from preceding patterns (s, o1, o2).
        // Sources come first, MINUS is placed last.
        assert!(matches!(&reordered[0], Pattern::Triple(_)));
        assert!(matches!(&reordered[1], Pattern::Triple(_)));
        assert!(
            matches!(&reordered[2], Pattern::Minus(_)),
            "MINUS should be placed after sources, got: {:?}",
            &reordered[2]
        );
    }

    #[test]
    fn test_reorder_expander_after_sources() {
        // OPTIONAL should be placed after all sources and reducers
        let s = VarId(0);
        let o1 = VarId(1);
        let o2 = VarId(2);
        let o3 = VarId(3);

        let triple1 = Pattern::Triple(make_pattern(s, "name", o1));
        let triple2 = Pattern::Triple(make_pattern(s, "age", o2));
        let optional = Pattern::Optional(vec![Pattern::Triple(make_pattern(s, "email", o3))]);

        let patterns = vec![optional.clone(), triple1.clone(), triple2.clone()];

        let reordered = reorder_patterns(&patterns, None, &HashSet::new());

        // Sources (triples) should come before the expander (OPTIONAL)
        assert!(matches!(&reordered[0], Pattern::Triple(_)));
        assert!(matches!(&reordered[1], Pattern::Triple(_)));
        assert!(matches!(&reordered[2], Pattern::Optional(_)));
    }

    #[test]
    fn test_reorder_reducer_waits_for_dependencies() {
        // MINUS cannot be placed until a correlation variable is bound.
        // If ?x is only bound by the second triple, MINUS waits.
        let s = VarId(0);
        let o1 = VarId(1);
        let x = VarId(2);
        let d = VarId(3);

        // First triple binds ?s and ?o1 (not ?x)
        let triple1 = Pattern::Triple(make_pattern(s, "name", o1));
        // Second triple binds ?x
        let triple2 = Pattern::Triple(make_pattern(x, "type", VarId(4)));
        // MINUS needs ?x
        let minus = Pattern::Minus(vec![Pattern::Triple(make_pattern(x, "bad", d))]);

        let patterns = vec![minus.clone(), triple1.clone(), triple2.clone()];

        let reordered = reorder_patterns(&patterns, None, &HashSet::new());

        // MINUS should not be first (it needs ?x which isn't bound yet)
        assert!(
            !matches!(&reordered[0], Pattern::Minus(_)),
            "MINUS should not be placed before its dependency variables are bound"
        );
    }

    #[test]
    fn test_reorder_union_placed_by_cardinality() {
        // A more selective UNION should be placed before a less selective triple
        let s = VarId(0);
        let o = VarId(1);

        // Selective union: single small branch
        let union = Pattern::Union(vec![vec![Pattern::Triple(TriplePattern::new(
            Ref::Var(s),
            Ref::Sid(Sid::new(100, "rare")),
            Term::Value(FlakeValue::String("specific".to_string())),
        ))]]);

        // Unselective triple (full property scan)
        let wide_triple = Pattern::Triple(make_pattern(s, "wide", o));

        let mut stats = StatsView::default();
        stats.properties.insert(
            Sid::new(100, "rare"),
            PropertyStatData {
                count: 5,
                ndv_values: 5,
                ndv_subjects: 5,
            },
        );
        stats.properties.insert(
            Sid::new(100, "wide"),
            PropertyStatData {
                count: 1_000_000,
                ndv_values: 500_000,
                ndv_subjects: 500_000,
            },
        );

        let patterns = vec![wide_triple, union];

        let reordered = reorder_patterns(&patterns, Some(&stats), &HashSet::new());

        // The union (estimate ~5) should come before the wide triple (estimate ~1M)
        assert!(
            matches!(&reordered[0], Pattern::Union(_)),
            "Selective UNION should be placed before unselective triple, got: {:?}",
            &reordered[0]
        );
    }

    #[test]
    fn test_pattern_shares_variables() {
        let pattern = Pattern::Triple(make_pattern(VarId(0), "name", VarId(1)));

        let mut bound = HashSet::new();
        assert!(!pattern_shares_variables(&pattern, &bound));

        bound.insert(VarId(0));
        assert!(pattern_shares_variables(&pattern, &bound));

        bound.clear();
        bound.insert(VarId(99));
        assert!(!pattern_shares_variables(&pattern, &bound));
    }

    #[test]
    fn test_estimate_branch_cardinality() {
        let mut stats = StatsView::default();
        stats.properties.insert(
            Sid::new(100, "name"),
            PropertyStatData {
                count: 1000,
                ndv_values: 500,
                ndv_subjects: 1000,
            },
        );

        let branch = vec![Pattern::Triple(make_pattern(VarId(0), "name", VarId(1)))];
        let est = estimate_branch_cardinality(&branch, Some(&stats));

        // Single triple: should be the triple's selectivity (count = 1000)
        assert!((est - 1000.0).abs() < f64::EPSILON);
    }

    // =========================================================================
    // Compound pattern absorption tests
    // =========================================================================

    fn is_filter(p: &Pattern) -> bool {
        matches!(p, Pattern::Filter(_))
    }

    fn is_bind(p: &Pattern) -> bool {
        matches!(p, Pattern::Bind { .. })
    }

    fn is_deferred(p: &Pattern) -> bool {
        is_filter(p) || is_bind(p)
    }

    #[test]
    fn test_filter_pushed_into_union_when_all_branches_bind_var() {
        let s = VarId(0);
        let name = VarId(1);
        let age = VarId(2);

        let patterns = vec![
            Pattern::Triple(make_pattern(s, "name", name)),
            Pattern::Union(vec![
                vec![Pattern::Triple(make_pattern(s, "age", age))],
                vec![Pattern::Triple(make_pattern(s, "years", age))],
            ]),
            Pattern::Filter(Expression::gt(
                Expression::Var(age),
                Expression::Const(FlakeValue::Long(25)),
            )),
        ];
        let reordered = reorder_patterns(&patterns, None, &HashSet::new());

        assert_eq!(count_patterns(&reordered, is_filter), 0);
        assert_union_branches_contain(&reordered, is_filter, "should contain pushed-in filter");
    }

    #[test]
    fn test_filter_stays_after_union_when_not_all_branches_bind_var() {
        let s = VarId(0);
        let name = VarId(1);
        let age = VarId(2);
        let label = VarId(3);

        let patterns = vec![
            Pattern::Triple(make_pattern(s, "name", name)),
            Pattern::Union(vec![
                vec![Pattern::Triple(make_pattern(s, "age", age))],
                vec![Pattern::Triple(make_pattern(s, "label", label))],
            ]),
            Pattern::Filter(Expression::gt(
                Expression::Var(age),
                Expression::Const(FlakeValue::Long(25)),
            )),
        ];
        let reordered = reorder_patterns(&patterns, None, &HashSet::new());

        assert_eq!(
            count_patterns(&reordered, is_filter),
            1,
            "filter should stay at top level when not all branches bind the var"
        );
    }

    #[test]
    fn test_bind_and_filter_cascade_into_union() {
        let s = VarId(0);
        let age = VarId(1);
        let double = VarId(2);

        let patterns = vec![
            Pattern::Union(vec![
                vec![Pattern::Triple(make_pattern(s, "age", age))],
                vec![Pattern::Triple(make_pattern(s, "years", age))],
            ]),
            Pattern::Bind {
                var: double,
                expr: Expression::Call {
                    func: Function::Mul,
                    args: vec![Expression::Var(age), Expression::Const(FlakeValue::Long(2))],
                },
            },
            Pattern::Filter(Expression::gt(
                Expression::Var(double),
                Expression::Const(FlakeValue::Long(50)),
            )),
        ];
        let reordered = reorder_patterns(&patterns, None, &HashSet::new());

        assert_eq!(count_patterns(&reordered, is_deferred), 0);
        assert_union_branches_contain(&reordered, is_bind, "should contain pushed-in BIND");
        assert_union_branches_contain(&reordered, is_filter, "should contain pushed-in FILTER");
    }

    #[test]
    fn test_filter_pushed_into_graph() {
        let s = VarId(0);
        let age = VarId(1);

        let patterns = vec![
            Pattern::Graph {
                name: GraphName::Iri(Arc::from("http://example.org/g")),
                patterns: vec![Pattern::Triple(make_pattern(s, "age", age))],
            },
            Pattern::Filter(Expression::gt(
                Expression::Var(age),
                Expression::Const(FlakeValue::Long(25)),
            )),
        ];
        let reordered = reorder_patterns(&patterns, None, &HashSet::new());

        assert_eq!(count_patterns(&reordered, is_filter), 0);

        let graph = reordered
            .iter()
            .find(|p| matches!(p, Pattern::Graph { .. }))
            .expect("Graph should be in result");
        if let Pattern::Graph { patterns, .. } = graph {
            assert!(
                patterns.iter().any(is_filter),
                "Graph inner patterns should contain the filter"
            );
        }
    }

    #[test]
    fn test_filter_pushed_into_service() {
        use crate::ir::{ServiceEndpoint, ServicePattern};

        let s = VarId(0);
        let age = VarId(1);

        let patterns = vec![
            Pattern::Service(ServicePattern::new(
                false,
                ServiceEndpoint::Iri(Arc::from("fluree:ledger:mydb:main")),
                vec![Pattern::Triple(make_pattern(s, "age", age))],
            )),
            Pattern::Filter(Expression::gt(
                Expression::Var(age),
                Expression::Const(FlakeValue::Long(25)),
            )),
        ];
        let reordered = reorder_patterns(&patterns, None, &HashSet::new());

        assert_eq!(
            count_patterns(&reordered, is_filter),
            0,
            "filter should not remain at top level"
        );

        let service = reordered
            .iter()
            .find(|p| matches!(p, Pattern::Service(_)))
            .expect("Service should be in result");
        if let Pattern::Service(sp) = service {
            assert!(
                sp.patterns.iter().any(is_filter),
                "Service inner patterns should contain the filter"
            );
        }
    }

    #[test]
    fn test_multiple_filters_only_relevant_ones_pushed() {
        let s = VarId(0);
        let name = VarId(1);
        let age = VarId(2);

        let patterns = vec![
            Pattern::Triple(make_pattern(s, "name", name)),
            Pattern::Union(vec![
                vec![Pattern::Triple(make_pattern(s, "age", age))],
                vec![Pattern::Triple(make_pattern(s, "years", age))],
            ]),
            Pattern::Filter(Expression::gt(
                Expression::Var(age),
                Expression::Const(FlakeValue::Long(25)),
            )),
            Pattern::Filter(Expression::eq(
                Expression::Var(name),
                Expression::Const(FlakeValue::String("Alice".to_string())),
            )),
        ];
        let reordered = reorder_patterns(&patterns, None, &HashSet::new());

        // Name filter is ready before UNION (bound by triple) so stays top-level.
        // Age filter is pushed into UNION branches.
        assert_eq!(count_patterns(&reordered, is_filter), 1);
        assert_union_branches_contain(&reordered, is_filter, "should contain pushed-in age filter");
    }

    // --- seed-time WITH-producer vs disconnected class anchor ------------------

    fn type_anchor(subject: VarId, class: &str) -> Pattern {
        Pattern::Triple(TriplePattern::new(
            Ref::Var(subject),
            Ref::Sid(Sid::new(
                fluree_vocab::namespaces::RDF,
                fluree_vocab::predicates::RDF_TYPE,
            )),
            Term::Sid(Sid::new(100, class)),
        ))
    }

    fn with_producer(out: VarId) -> Pattern {
        use crate::ir::SubqueryPattern;
        Pattern::Subquery(SubqueryPattern::new(
            vec![out],
            vec![Pattern::Triple(make_pattern(out, "knows", VarId(99)))],
        ))
    }

    #[test]
    fn with_producer_seeds_before_disconnected_class_anchor() {
        // IC3 Stage 2 shape: WITH-subquery produces `friend`; `home rdf:type
        // Country` is a tiny (111) but disconnected class anchor. Without the
        // guard, Country seeds #0 and drives the chain backward (50s plan).
        let (friend, city, home) = (VarId(0), VarId(1), VarId(2));
        let mut stats = StatsView::default();
        stats.classes.insert(Sid::new(100, "Country"), 111);
        // Realistic shape: the location predicates are broad scans, the producer
        // body (`knows`) is comparatively small — so once the disconnected Country
        // anchor is dropped from the seed pool, the producer wins the seed.
        for (p, c) in [
            ("IS_PART_OF", 10_000),
            ("IS_LOCATED_IN", 10_000),
            ("knows", 50),
        ] {
            stats.properties.insert(
                Sid::new(100, p),
                PropertyStatData {
                    count: c,
                    ndv_values: c,
                    ndv_subjects: c,
                },
            );
        }

        let patterns = vec![
            type_anchor(home, "Country"),
            Pattern::Triple(make_pattern(city, "IS_PART_OF", home)),
            Pattern::Triple(make_pattern(friend, "IS_LOCATED_IN", city)), // consumes friend
            with_producer(friend),
        ];
        let ordered = reorder_patterns(&patterns, Some(&stats), &HashSet::new());

        let sub_pos = ordered
            .iter()
            .position(|p| matches!(p, Pattern::Subquery(_)))
            .expect("subquery present");
        let anchor_pos = ordered
            .iter()
            .position(|p| matches!(p, Pattern::Triple(tp) if tp.p.is_rdf_type()))
            .expect("rdf:type anchor present");
        assert!(
            sub_pos < anchor_pos,
            "WITH producer must seed before the disconnected class anchor: {ordered:?}"
        );
        assert!(
            !matches!(&ordered[0], Pattern::Triple(tp) if tp.p.is_rdf_type()),
            "class anchor must not seed the block: {ordered:?}"
        );
    }

    #[test]
    fn pure_class_anchor_seeds_without_producer() {
        // No WITH producer: `home rdf:type Country` is the genuine anchor and must
        // still seed (the guard must not penalize class anchors in general).
        let (home, city, p) = (VarId(0), VarId(1), VarId(2));
        let mut stats = StatsView::default();
        stats.classes.insert(Sid::new(100, "Country"), 111);

        let patterns = vec![
            type_anchor(home, "Country"),
            Pattern::Triple(make_pattern(city, "IS_PART_OF", home)),
            Pattern::Triple(make_pattern(p, "IS_LOCATED_IN", city)),
        ];
        let ordered = reorder_patterns(&patterns, Some(&stats), &HashSet::new());
        assert!(
            matches!(&ordered[0], Pattern::Triple(tp) if tp.p.is_rdf_type()),
            "with no producer the class anchor still seeds first: {ordered:?}"
        );
    }

    #[test]
    fn class_anchor_demoted_in_fallback_when_producer_continues() {
        // IC3 mid-block shape: a selective chain has already bound `cx`, so no
        // remaining source shares the bound set — the only forward continuation
        // is the WITH producer (connected via `friend`, an anchor var). A cheap
        // disconnected `home rdf:type Country` is also available. The producer
        // must win: the class anchor must not seed a reverse drive out of the
        // fall-back-to-all pool. (The seed-only demotion failed this.)
        let (friend, cx, mx, home) = (VarId(0), VarId(1), VarId(2), VarId(4));
        let mut stats = StatsView::default();
        stats.classes.insert(Sid::new(100, "Country"), 111);

        let patterns = vec![
            type_anchor(home, "Country"),
            with_producer(friend),
            Pattern::Triple(make_pattern(mx, "HAS_CREATOR", friend)), // consumer of friend
        ];
        let mut bound = HashSet::new();
        bound.insert(cx); // pretend the selective chain already bound `cx`
        let ordered = reorder_patterns(&patterns, Some(&stats), &bound);

        let sub_pos = ordered
            .iter()
            .position(|p| matches!(p, Pattern::Subquery(_)))
            .expect("subquery present");
        let anchor_pos = ordered
            .iter()
            .position(|p| matches!(p, Pattern::Triple(tp) if tp.p.is_rdf_type()))
            .expect("rdf:type anchor present");
        assert!(
            sub_pos < anchor_pos,
            "producer must beat the class anchor in the fall-back pool: {ordered:?}"
        );
    }

    #[test]
    fn annotation_sidecar_demoted_for_base_edge() {
        // RDF-star reified edge (IC5 shape): `?ann f:reifiesObject ?friend` is a
        // broad annotation sidecar; `?forum HAS_MEMBER ?friend` is the concrete
        // base edge. Both are object-bound on `friend`. The base edge must seed —
        // not the sidecar — even though the sidecar is written first (and would
        // otherwise win the orig-index tie).
        let (friend, forum, ann) = (VarId(0), VarId(1), VarId(2));
        let reifies_object = Pattern::Triple(TriplePattern::new(
            Ref::Var(ann),
            Ref::Sid(Sid::new(
                fluree_vocab::namespaces::FLUREE_DB,
                fluree_vocab::db::REIFIES_OBJECT,
            )),
            Term::Var(friend),
        ));
        let base_edge = Pattern::Triple(make_pattern(forum, "HAS_MEMBER", friend));
        let patterns = vec![reifies_object, base_edge];
        let mut bound = HashSet::new();
        bound.insert(friend);
        let ordered = reorder_patterns(&patterns, None, &bound);
        assert!(
            matches!(&ordered[0], Pattern::Triple(tp)
                if matches!(&tp.p, Ref::Sid(s) if s.name.as_ref() == "HAS_MEMBER")),
            "concrete base edge must seed before the f:reifiesObject sidecar: {ordered:?}"
        );
    }

    #[test]
    fn full_reifies_chain_drives_base_edge_first() {
        // The REAL delegate path: the expanded edge-annotation chain (base edge +
        // three `f:reifies*` sidecars) reordered with the child's `friend` already
        // bound — exactly what `DefaultGraphSourceOperator` feeds to
        // `build_where_operators_seeded` -> `reorder_patterns`. The base
        // `HAS_MEMBER` edge must be placed before `f:reifiesObject`, otherwise the
        // plan scans the global annotation sidecar.
        let (friend, forum, ann) = (VarId(0), VarId(1), VarId(2));
        let sid = |name| Ref::Sid(Sid::new(fluree_vocab::namespaces::FLUREE_DB, name));
        let base_edge = Pattern::Triple(make_pattern(forum, "HAS_MEMBER", friend));
        let reifies_subject = Pattern::Triple(TriplePattern::new(
            Ref::Var(ann),
            sid(fluree_vocab::db::REIFIES_SUBJECT),
            Term::Var(forum),
        ));
        let reifies_predicate = Pattern::Triple(TriplePattern::new(
            Ref::Var(ann),
            sid(fluree_vocab::db::REIFIES_PREDICATE),
            Term::Sid(Sid::new(100, "HAS_MEMBER")),
        ));
        let reifies_object = Pattern::Triple(TriplePattern::new(
            Ref::Var(ann),
            sid(fluree_vocab::db::REIFIES_OBJECT),
            Term::Var(friend),
        ));
        // Emission order as produced by `expand_edge_annotation_patterns`.
        let patterns = vec![
            base_edge,
            reifies_subject,
            reifies_predicate,
            reifies_object,
        ];
        let mut bound = HashSet::new();
        bound.insert(friend); // the delegate's seeded child binds `friend`
        let ordered = reorder_patterns(&patterns, None, &bound);

        let pos = |pred: &str| {
            ordered
                .iter()
                .position(|p| {
                    matches!(p, Pattern::Triple(tp)
                    if matches!(&tp.p, Ref::Sid(s) if s.name.as_ref() == pred))
                })
                .unwrap_or(usize::MAX)
        };
        assert!(
            pos("HAS_MEMBER") < pos(fluree_vocab::db::REIFIES_OBJECT),
            "base edge must drive before the f:reifiesObject sidecar: {ordered:?}"
        );
    }

    #[test]
    fn producer_then_consumer_order_not_regressed() {
        // IC9 shape: the WITH producer seeds, then its consumer (`HAS_CREATOR`,
        // which references `friend`) drains immediately after.
        let (friend, message) = (VarId(0), VarId(1));
        let patterns = vec![
            Pattern::Triple(make_pattern(message, "HAS_CREATOR", friend)),
            with_producer(friend),
        ];
        let ordered = reorder_patterns(&patterns, None, &HashSet::new());
        assert!(
            matches!(&ordered[0], Pattern::Subquery(_)),
            "producer seeds first: {ordered:?}"
        );
        assert!(
            matches!(&ordered[1], Pattern::Triple(tp)
                if matches!(&tp.p, Ref::Sid(s) if s.name.as_ref() == "HAS_CREATOR")),
            "consumer drains right after the producer: {ordered:?}"
        );
    }
}
