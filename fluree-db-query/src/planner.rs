//! Query planner - pattern reordering and selectivity estimation
//!
//! Reorders WHERE-clause patterns for optimal join order using statistics-based
//! cardinality estimates. When a `StatsView` is provided, uses HLL-derived
//! property statistics; otherwise falls back to heuristic defaults.
//!
//! The main entry point is `reorder_patterns`, called from
//! `build_where_operators_seeded` in `execute/where_plan.rs`.

use crate::ir::{CompareOp, Function, Pattern};
use crate::triple::{Ref, Term, TriplePattern};
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
            }
            DEFAULT_BOUND_OBJECT_SELECTIVITY
        }

        PatternType::PropertyScan => {
            if let Some(s) = stats {
                if let Some(prop) = property_stats(s, &pattern.p) {
                    return prop.count as f64;
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
                predicates_bound: patterns.iter().all(super::triple::TriplePattern::p_bound),
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

    let predicates_bound = patterns.iter().all(super::triple::TriplePattern::p_bound);

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
    use crate::ir::FilterValue;

    match expr {
        Expression::Const(FilterValue::Long(n)) => Some(RangeValue::Long(*n)),
        // NaN is not a meaningful range bound.
        Expression::Const(FilterValue::Double(d)) if d.is_nan() => None,
        Expression::Const(FilterValue::Double(d)) => Some(RangeValue::Double(*d)),
        Expression::Const(FilterValue::String(s)) => Some(RangeValue::String(s.clone())),
        Expression::Const(FilterValue::Temporal(fv)) => {
            // Duration (non-totally-orderable) should NOT be pushed down as a range constraint
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
            row_count: estimate_branch_cardinality(&sq.patterns, stats),
        },

        Pattern::Optional(_) => PatternEstimate::Expander { multiplier: 1.0 },

        // MINUS, EXISTS, and NOT EXISTS are order-sensitive: they must run
        // after all preceding patterns. The planner intercepts them before
        // calling estimate_pattern (see reorder_patterns), so in practice
        // these arms are only reached by direct callers like explain.rs.
        Pattern::Minus(_) | Pattern::Exists(_) | Pattern::NotExists(_) => PatternEstimate::Deferred,

        Pattern::Filter(_) | Pattern::Bind { .. } => PatternEstimate::Deferred,

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

        Pattern::PropertyPath(_) => PatternEstimate::Source {
            row_count: DEFAULT_PROPERTY_SCAN_SELECTIVITY,
        },

        Pattern::R2rml(_) => PatternEstimate::Source {
            row_count: DEFAULT_PROPERTY_SCAN_SELECTIVITY,
        },

        Pattern::Service(_) => PatternEstimate::Source {
            row_count: DEFAULT_SERVICE_ROW_COUNT,
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
                for v in other.variables() {
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
    for v in first.variables() {
        bound_vars.insert(v);
    }

    // Multiplicative model: each subsequent triple's contribution depends on
    // which variables are now bound from previous triples
    for tp in sorted_triples.iter().skip(1) {
        let expansion = estimate_triple_row_count(tp, &bound_vars, stats);
        running *= expansion;
        for v in tp.variables() {
            bound_vars.insert(v);
        }
    }

    (running * non_triple_estimate).max(HIGHLY_SELECTIVE)
}

/// Check if a general pattern shares any variables with the bound set.
///
/// Uses `Pattern::variables()` which works for all pattern variants.
pub fn pattern_shares_variables(pattern: &Pattern, bound_vars: &HashSet<VarId>) -> bool {
    pattern.variables().iter().any(|v| bound_vars.contains(v))
}

/// Collect the variables that a slice of patterns guarantees to bind.
fn collect_guaranteed_vars(patterns: &[Pattern]) -> HashSet<VarId> {
    patterns
        .iter()
        .flat_map(super::ir::Pattern::variables)
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
            // Require all variables from preceding patterns (order preservation)
            let mut required: HashSet<VarId> = patterns[..i]
                .iter()
                .flat_map(super::ir::Pattern::variables)
                .collect();
            // If no preceding patterns, require the pattern's own variables
            // so it cannot execute before any sources provide bindings.
            if required.is_empty() {
                required = pattern.variables().into_iter().collect();
            }
            deferred.push(DeferredPattern {
                orig_index: i,
                required_vars: required,
                pattern: pattern.clone(),
            });
            continue;
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
            || try_place_source(&mut sources, &mut bound_vars, stats, &mut result)
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
            for v in rp.pattern.variables() {
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
        for v in rp.pattern.variables() {
            bound_vars.insert(v);
        }
        result.push(rp.pattern);
        true
    } else {
        false
    }
}

/// Try to place the best source. Returns true if one was placed.
fn try_place_source(
    remaining: &mut Vec<RankedPattern>,
    bound_vars: &mut HashSet<VarId>,
    stats: Option<&StatsView>,
    result: &mut Vec<Pattern>,
) -> bool {
    if remaining.is_empty() {
        return false;
    }

    let has_bound = !bound_vars.is_empty();

    // Prefer joinable sources (share variables with bound set)
    let candidates: Vec<usize> = remaining
        .iter()
        .enumerate()
        .filter(|(_, rp)| !has_bound || pattern_shares_variables(&rp.pattern, bound_vars))
        .map(|(idx, _)| idx)
        .collect();

    // Fall back to all sources if none are joinable
    let pool = if candidates.is_empty() {
        (0..remaining.len()).collect::<Vec<_>>()
    } else {
        candidates
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
        .then_with(|| remaining[i].orig_index.cmp(&remaining[j].orig_index))
    });

    if let Some(idx) = best_idx {
        let rp = remaining.remove(idx);
        for v in rp.pattern.variables() {
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
        for v in rp.pattern.variables() {
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

            // BIND produces a new variable; FILTER does not.
            if let Pattern::Bind { var, .. } = &dp.pattern {
                bound_vars.insert(*var);
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
fn deferred_required_vars(pattern: &Pattern) -> Vec<VarId> {
    match pattern {
        Pattern::Filter(expr) => expr.variables(),
        Pattern::Bind { expr, .. } => expr.variables(),
        // Other patterns should not be classified as Deferred, but handle
        // gracefully by returning all variables.
        other => other.variables(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::GraphName;
    use crate::triple::Term;
    use fluree_db_core::{PropertyStatData, Sid, StatsView};
    use std::sync::Arc;

    fn make_pattern(s: VarId, p_name: &str, o: VarId) -> TriplePattern {
        TriplePattern::new(Ref::Var(s), Ref::Sid(Sid::new(100, p_name)), Term::Var(o))
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
        assert!(first.variables().contains(&VarId(0)));
        assert!(first.variables().contains(&VarId(1)));

        // p3 placed second (shares ?s=VarId(0) with p1, preferred over disjoint p2)
        let second = match &ordered[1] {
            Pattern::Triple(tp) => tp,
            _ => panic!("expected Triple pattern"),
        };
        assert!(second.variables().contains(&VarId(0)));
        assert!(second.variables().contains(&VarId(4)));

        // p2 placed last (disjoint, no joinable preference)
        let last = match &ordered[2] {
            Pattern::Triple(tp) => tp,
            _ => panic!("expected Triple pattern"),
        };
        assert!(last.variables().contains(&VarId(2)));
        assert!(last.variables().contains(&VarId(3)));
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
            first.variables().contains(&s),
            "expected first pattern to join with seeded bound vars"
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
    use crate::ir::{Expression, FilterValue, Function};

    #[test]
    fn test_extract_range_simple_gt() {
        // ?age > 18
        let expr = Expression::gt(
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(18)),
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
            Expression::Const(FilterValue::Long(65)),
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
            Expression::Const(FilterValue::String("active".to_string())),
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
            Expression::Const(FilterValue::Long(18)),
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
                Expression::Const(FilterValue::Long(18)),
            ),
            Expression::lt(
                Expression::Var(VarId(0)),
                Expression::Const(FilterValue::Long(65)),
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
                Expression::Const(FilterValue::Long(18)),
            ),
            Expression::gt(
                Expression::Var(VarId(1)),
                Expression::Const(FilterValue::Long(100)),
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
                Expression::Const(FilterValue::Long(1)),
            ),
            Expression::eq(
                Expression::Var(VarId(0)),
                Expression::Const(FilterValue::Long(2)),
            ),
        ]);

        assert!(extract_range_constraints(&expr).is_none());
    }

    #[test]
    fn test_extract_range_double_values() {
        // ?price > 19.99
        let expr = Expression::gt(
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Double(19.99)),
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
            Expression::Const(FilterValue::Long(18)),
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
                Expression::Const(FilterValue::Long(18)),
            ),
            Expression::lt(
                Expression::Var(VarId(0)),
                Expression::Const(FilterValue::Long(65)),
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
            Expression::Const(FilterValue::Long(18)),
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
                Expression::Const(FilterValue::Long(10)),
            ),
            Expression::lt(
                Expression::Var(VarId(0)),
                Expression::Const(FilterValue::Long(5)),
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
                Expression::Const(FilterValue::Long(18)),
            ),
            Expression::eq(
                Expression::Var(VarId(0)),
                Expression::Const(FilterValue::Long(21)),
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
            Expression::Const(FilterValue::Long(10)),
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(20)),
        );
        assert!(expr.is_range_safe());

        // (<= 10 ?x 20) → range-safe
        let expr = sandwich(
            Function::Le,
            Expression::Const(FilterValue::Long(10)),
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(20)),
        );
        assert!(expr.is_range_safe());

        // (> 20 ?x 10) → range-safe
        let expr = sandwich(
            Function::Gt,
            Expression::Const(FilterValue::Long(20)),
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(10)),
        );
        assert!(expr.is_range_safe());

        // (>= 20 ?x 10) → range-safe
        let expr = sandwich(
            Function::Ge,
            Expression::Const(FilterValue::Long(20)),
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(10)),
        );
        assert!(expr.is_range_safe());

        // (= 5 ?x 5) → range-safe
        let expr = sandwich(
            Function::Eq,
            Expression::Const(FilterValue::Long(5)),
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(5)),
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
                Expression::Const(FilterValue::Long(20)),
            ],
        };
        assert!(!expr.is_range_safe());

        // (< 10 20 ?x) → NOT range-safe (const const var)
        let expr = Expression::Call {
            func: Function::Lt,
            args: vec![
                Expression::Const(FilterValue::Long(10)),
                Expression::Const(FilterValue::Long(20)),
                Expression::Var(VarId(0)),
            ],
        };
        assert!(!expr.is_range_safe());

        // (< ?x 10 ?y) → NOT range-safe (var const var)
        let expr = Expression::Call {
            func: Function::Lt,
            args: vec![
                Expression::Var(VarId(0)),
                Expression::Const(FilterValue::Long(10)),
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
            Expression::Const(FilterValue::Long(10)),
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(20)),
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
            Expression::Const(FilterValue::Long(10)),
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(20)),
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
            Expression::Const(FilterValue::Long(20)),
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(10)),
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
            Expression::Const(FilterValue::Long(20)),
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(10)),
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
            Expression::Const(FilterValue::Long(5)),
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(5)),
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
            Expression::Const(FilterValue::Long(5)),
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(10)),
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
            Expression::Const(FilterValue::Long(10)),
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(20)),
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
        use crate::ir::{Expression, FilterValue, SubqueryPattern};

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
            Expression::Const(FilterValue::Long(0)),
        ));
        assert!(matches!(
            estimate_pattern(&filter, &empty, None),
            PatternEstimate::Deferred
        ));

        let bind = Pattern::Bind {
            var: VarId(7),
            expr: Expression::Const(FilterValue::Long(42)),
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
                Expression::Const(FilterValue::Long(25)),
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
                Expression::Const(FilterValue::Long(25)),
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
                    args: vec![
                        Expression::Var(age),
                        Expression::Const(FilterValue::Long(2)),
                    ],
                },
            },
            Pattern::Filter(Expression::gt(
                Expression::Var(double),
                Expression::Const(FilterValue::Long(50)),
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
                Expression::Const(FilterValue::Long(25)),
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
                Expression::Const(FilterValue::Long(25)),
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
                Expression::Const(FilterValue::Long(25)),
            )),
            Pattern::Filter(Expression::eq(
                Expression::Var(name),
                Expression::Const(FilterValue::String("Alice".to_string())),
            )),
        ];
        let reordered = reorder_patterns(&patterns, None, &HashSet::new());

        // Name filter is ready before UNION (bound by triple) so stays top-level.
        // Age filter is pushed into UNION branches.
        assert_eq!(count_patterns(&reordered, is_filter), 1);
        assert_union_branches_contain(&reordered, is_filter, "should contain pushed-in age filter");
    }
}
