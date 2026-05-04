//! Query plan explanation for debugging optimization decisions
//!
//! Provides visibility into how the query optimizer reorders patterns
//! and computes selectivity scores.
//!
//! Call `explain_patterns` with a set of patterns and optional stats to get an `ExplainPlan`.

use crate::ir::triple::{Ref, Term, TriplePattern};
use crate::planner::{classify_pattern, estimate_triple_row_count, PatternType};
use crate::var_registry::VarId;
use crate::{
    execute::{analyze_property_join_plan, collect_inner_join_block},
    ir::Pattern,
};
use fluree_db_core::StatsView;
use std::collections::HashSet;
use std::fmt;

/// Explanation of query optimization decisions
#[derive(Debug, Clone)]
pub struct ExplainPlan {
    /// Whether patterns were reordered
    pub optimization: OptimizationStatus,
    /// Whether statistics were available for optimization
    pub statistics_available: bool,
    /// Original pattern order with selectivity info
    pub original_patterns: Vec<PatternDisplay>,
    /// Optimized pattern order with selectivity info
    pub optimized_patterns: Vec<PatternDisplay>,
}

/// Whether optimization changed the pattern order
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptimizationStatus {
    /// Patterns were reordered for better performance
    Reordered,
    /// Pattern order unchanged (already optimal or single pattern)
    Unchanged,
}

/// Display information for a single pattern
#[derive(Debug, Clone)]
pub struct PatternDisplay {
    /// Human-readable pattern representation
    pub pattern: String,
    /// Classification of the pattern
    pub pattern_type: PatternType,
    /// Computed selectivity score (lower = more selective)
    pub selectivity_score: i64,
    /// Inputs used for selectivity calculation
    pub inputs: SelectivityInputs,
    /// Variables in this pattern
    pub variables: Vec<VarId>,
}

/// Reason why fallback scoring was used instead of stats-based scoring
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum FallbackReason {
    /// No stats available at all for this property
    NoPropertyStats,
    /// Property stats exist but NDV is zero (can't compute selectivity ratio)
    MissingNdv,
    /// Full scan pattern always uses fallback (no predicate to look up)
    FullScanPattern,
    /// Class pattern but class not found in stats
    ClassNotInStats,
}

/// Inputs used for selectivity calculation
#[derive(Debug, Clone, Default)]
pub struct SelectivityInputs {
    /// Property SID (if predicate is bound)
    pub property_sid: Option<String>,
    /// Property count from stats
    pub count: Option<u64>,
    /// Number of distinct values from stats
    pub ndv_values: Option<u64>,
    /// Number of distinct subjects from stats
    pub ndv_subjects: Option<u64>,
    /// Class count from stats (for rdf:type patterns)
    pub class_count: Option<u64>,
    /// Why fallback was used, or None if full stats were available
    pub fallback: Option<FallbackReason>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ExecutionStrategyHint {
    pub strategy: String,
    pub block_start: usize,
    pub required_triples: usize,
    pub fused_optional_triples: usize,
    pub fused_filters: usize,
    pub fused_binds: usize,
    pub width_score: f32,
    pub optional_bonus: f32,
    pub details: Vec<String>,
}

/// Generate an explanation of pattern optimization
///
/// Analyzes the input patterns and shows how they would be reordered
/// based on selectivity scores.
pub fn explain_patterns(patterns: &[TriplePattern], stats: Option<&StatsView>) -> ExplainPlan {
    // Check both property and class stats
    let statistics_available = stats
        .map(|s| s.has_property_stats() || s.has_class_stats())
        .unwrap_or(false);

    // Build original pattern displays
    let original_patterns: Vec<PatternDisplay> = patterns
        .iter()
        .map(|p| build_pattern_display(p, stats))
        .collect();

    // Reorder patterns using the same algorithm as planner
    let optimized = reorder_for_explain(patterns.to_vec(), stats);
    let optimized_patterns: Vec<PatternDisplay> = optimized
        .iter()
        .map(|p| build_pattern_display(p, stats))
        .collect();

    // Check if order changed using structural comparison (PartialEq)
    let optimization = if patterns.len() <= 1 {
        OptimizationStatus::Unchanged
    } else {
        let same_order = patterns.iter().zip(optimized.iter()).all(|(a, b)| a == b);
        if same_order {
            OptimizationStatus::Unchanged
        } else {
            OptimizationStatus::Reordered
        }
    };

    ExplainPlan {
        optimization,
        statistics_available,
        original_patterns,
        optimized_patterns,
    }
}

pub fn explain_execution_hints(
    patterns: &[Pattern],
    stats: Option<&StatsView>,
) -> Vec<ExecutionStrategyHint> {
    let reordered = crate::planner::reorder_patterns(patterns, stats, &HashSet::new());
    let mut hints = Vec::new();
    let mut i = 0usize;
    while i < reordered.len() {
        match &reordered[i] {
            Pattern::Triple(_) | Pattern::Values { .. } | Pattern::Bind { .. } => {
                let block_start = i;
                let block = collect_inner_join_block(&reordered, i);
                let end = block.end_index;
                if end == i {
                    i += 1;
                    continue;
                }
                if block.triples.len() < 2 {
                    i = end;
                    continue;
                }
                let has_upstream_seed = block_start > 0;
                let (decision, _) =
                    analyze_property_join_plan(&reordered, end, &block.triples, has_upstream_seed);
                i = end;
                if !decision.can_property_join {
                    continue;
                }
                let fused_filters = decision.tail_filters + block.filters.len();
                let fused_binds = decision.tail_binds + block.binds.len();
                let mut details = Vec::new();
                details.push("same-subject star with bound predicates".to_string());
                if decision.analysis.has_bound_objects {
                    details.push("includes bound-object driver candidates".to_string());
                }
                if decision.tail_optional_triples > 0 {
                    details.push("fuses trailing same-subject single-triple OPTIONALs".to_string());
                }
                if fused_filters > 0 {
                    details.push("runs eligible FILTERs inline".to_string());
                }
                if fused_binds > 0 {
                    details.push("runs eligible BINDs inline".to_string());
                }
                hints.push(ExecutionStrategyHint {
                    strategy: if decision.tail_optional_triples > 0
                        || decision.tail_filters > 0
                        || decision.tail_binds > 0
                    {
                        "property_join_fused_star".to_string()
                    } else {
                        "property_join".to_string()
                    },
                    block_start,
                    required_triples: block.triples.len(),
                    fused_optional_triples: decision.tail_optional_triples,
                    fused_filters,
                    fused_binds,
                    width_score: decision.width_score,
                    optional_bonus: decision.optional_bonus,
                    details,
                });
            }
            _ => i += 1,
        }
    }
    hints
}

/// Build display info for a single pattern
fn build_pattern_display(pattern: &TriplePattern, stats: Option<&StatsView>) -> PatternDisplay {
    let pattern_type = classify_pattern(pattern, &HashSet::new());
    let selectivity_score = estimate_triple_row_count(pattern, &HashSet::new(), stats) as i64;
    let inputs = capture_selectivity_inputs(pattern, pattern_type, stats);

    PatternDisplay {
        pattern: format_pattern(pattern),
        pattern_type,
        selectivity_score,
        inputs,
        variables: pattern.produced_vars(),
    }
}

/// Capture the inputs used for selectivity calculation (for debugging)
fn capture_selectivity_inputs(
    pattern: &TriplePattern,
    pattern_type: PatternType,
    stats: Option<&StatsView>,
) -> SelectivityInputs {
    let mut inputs = SelectivityInputs::default();

    // Capture property SID if predicate is bound
    if let Some(s) = stats {
        // Prefer SID formatting when available (stable + compact).
        if let Some(pred_sid) = pattern.p.as_sid() {
            inputs.property_sid = Some(format!("{}:{}", pred_sid.namespace_code, &pred_sid.name));
            if let Some(prop) = s.get_property(pred_sid) {
                inputs.count = Some(prop.count);
                inputs.ndv_values = Some(prop.ndv_values);
                inputs.ndv_subjects = Some(prop.ndv_subjects);
            }
        } else if let Some(pred_iri) = pattern.p.as_iri() {
            if let Some(prop) = s.get_property_by_iri(pred_iri) {
                inputs.count = Some(prop.count);
                inputs.ndv_values = Some(prop.ndv_values);
                inputs.ndv_subjects = Some(prop.ndv_subjects);
            }
        }
    }

    // Capture class count for class patterns
    if pattern_type == PatternType::ClassPattern {
        if let Some(s) = stats {
            if let Some(class_sid) = pattern.o.as_sid() {
                if let Some(count) = s.get_class_count(class_sid) {
                    inputs.class_count = Some(count);
                }
            } else if let Some(class_iri) = pattern.o.as_iri() {
                if let Some(count) = s.get_class_count_by_iri(class_iri) {
                    inputs.class_count = Some(count);
                }
            }
        }
    }

    // Determine if and why fallback was used based on pattern type and available stats
    inputs.fallback = match pattern_type {
        PatternType::ExactMatch => None, // No stats needed for exact match
        PatternType::FullScan => Some(FallbackReason::FullScanPattern),
        PatternType::ClassPattern => {
            if inputs.class_count.is_none() {
                Some(FallbackReason::ClassNotInStats)
            } else {
                None
            }
        }
        PatternType::BoundSubject => {
            if inputs.count.is_none() {
                Some(FallbackReason::NoPropertyStats)
            } else if inputs.ndv_subjects.is_none_or(|n| n == 0) {
                Some(FallbackReason::MissingNdv)
            } else {
                None
            }
        }
        PatternType::BoundObject => {
            if inputs.count.is_none() {
                Some(FallbackReason::NoPropertyStats)
            } else if inputs.ndv_values.is_none_or(|n| n == 0) {
                Some(FallbackReason::MissingNdv)
            } else {
                None
            }
        }
        PatternType::PropertyScan => {
            if inputs.count.is_none() {
                Some(FallbackReason::NoPropertyStats)
            } else {
                None // PropertyScan just uses count, no NDV needed
            }
        }
    };

    inputs
}

/// Format a pattern as a human-readable string
pub fn format_pattern(pattern: &TriplePattern) -> String {
    format!(
        "{} {} {}",
        format_ref(&pattern.s),
        format_ref(&pattern.p),
        format_term(&pattern.o)
    )
}

/// Format a ref (subject or predicate position) as a human-readable string
fn format_ref(r: &Ref) -> String {
    match r {
        Ref::Var(v) => format!("?v{}", v.0),
        Ref::Sid(sid) => format!("<{}:{}>", sid.namespace_code, &sid.name),
        Ref::Iri(iri) => format!("<{iri}>"),
    }
}

/// Format a term as a human-readable string
fn format_term(term: &Term) -> String {
    match term {
        Term::Var(v) => format!("?v{}", v.0),
        Term::Sid(sid) => format!("<{}:{}>", sid.namespace_code, &sid.name),
        Term::Iri(iri) => format!("<{iri}>"),
        Term::Value(val) => format!("{val:?}"),
    }
}

/// Reorder patterns for explain (reuses planner's algorithm via shared helpers)
fn reorder_for_explain(
    patterns: Vec<TriplePattern>,
    stats: Option<&StatsView>,
) -> Vec<TriplePattern> {
    if patterns.len() <= 1 {
        return patterns;
    }

    // If *all* patterns are using fallback scoring (no relevant stats),
    // don't reorder (optimization would be arbitrary/noisy).
    let all_fallback = patterns.iter().all(|p| {
        let ty = classify_pattern(p, &HashSet::new());
        let inputs = capture_selectivity_inputs(p, ty, stats);
        inputs.fallback.is_some()
    });
    if all_fallback {
        return patterns;
    }

    // If all patterns have identical selectivity, don't reorder (no benefit).
    let mut first_score: Option<i64> = None;
    let mut all_equal = true;
    for p in &patterns {
        let s = estimate_triple_row_count(p, &HashSet::new(), stats) as i64;
        match first_score {
            None => first_score = Some(s),
            Some(fs) if fs == s => {}
            Some(_) => all_equal = false,
        }
    }
    if all_equal {
        return patterns;
    }

    let mut remaining: Vec<_> = patterns.into_iter().collect();
    let mut ordered = Vec::with_capacity(remaining.len());
    let mut bound_vars: HashSet<VarId> = HashSet::new();

    while !remaining.is_empty() {
        let has_bound = !bound_vars.is_empty();
        let candidates: Vec<usize> = remaining
            .iter()
            .enumerate()
            .filter(|(_, p)| {
                if !has_bound {
                    true
                } else {
                    p.produced_vars().iter().any(|v| bound_vars.contains(v))
                }
            })
            .map(|(i, _)| i)
            .collect();

        let pool: Vec<usize> = if candidates.is_empty() {
            (0..remaining.len()).collect()
        } else {
            candidates
        };

        let best_idx = pool
            .into_iter()
            .min_by(|&i, &j| {
                let score_i =
                    estimate_triple_row_count(&remaining[i], &HashSet::new(), stats) as i64;
                let score_j =
                    estimate_triple_row_count(&remaining[j], &HashSet::new(), stats) as i64;
                score_i.cmp(&score_j).then_with(|| i.cmp(&j))
            })
            .unwrap();

        let chosen = remaining.remove(best_idx);
        for var in chosen.produced_vars() {
            bound_vars.insert(var);
        }
        ordered.push(chosen);
    }

    ordered
}

impl fmt::Display for ExplainPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "=== Query Optimization Explain ===")?;
        writeln!(f)?;
        writeln!(
            f,
            "Statistics available: {}",
            if self.statistics_available {
                "yes"
            } else {
                "no (using fallback estimates)"
            }
        )?;
        writeln!(
            f,
            "Optimization: {}",
            match self.optimization {
                OptimizationStatus::Reordered => "patterns reordered",
                OptimizationStatus::Unchanged => "order unchanged",
            }
        )?;
        writeln!(f)?;

        writeln!(f, "--- Original Pattern Order ---")?;
        for (i, p) in self.original_patterns.iter().enumerate() {
            writeln!(f, "  [{}] {}", i + 1, p)?;
        }
        writeln!(f)?;

        writeln!(f, "--- Optimized Pattern Order ---")?;
        for (i, p) in self.optimized_patterns.iter().enumerate() {
            writeln!(f, "  [{}] {}", i + 1, p)?;
        }

        Ok(())
    }
}

impl fmt::Display for PatternDisplay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} | type={:?} score={}",
            self.pattern, self.pattern_type, self.selectivity_score
        )?;

        if let Some(reason) = &self.inputs.fallback {
            let reason_str = match reason {
                FallbackReason::NoPropertyStats => "no-property-stats",
                FallbackReason::MissingNdv => "missing-ndv",
                FallbackReason::FullScanPattern => "full-scan",
                FallbackReason::ClassNotInStats => "class-not-in-stats",
            };
            write!(f, " (fallback: {reason_str})")?;
        }

        if let Some(ref prop) = self.inputs.property_sid {
            write!(f, " prop={prop}")?;
        }

        if let Some(count) = self.inputs.count {
            write!(f, " count={count}")?;
        }

        if let Some(ndv) = self.inputs.ndv_values {
            write!(f, " ndv_val={ndv}")?;
        }

        if let Some(ndv) = self.inputs.ndv_subjects {
            write!(f, " ndv_subj={ndv}")?;
        }

        if let Some(cc) = self.inputs.class_count {
            write!(f, " class_count={cc}")?;
        }

        Ok(())
    }
}

// =============================================================================
// Generalized explain for all pattern types
// =============================================================================

use crate::planner::{estimate_pattern, reorder_patterns, PatternEstimate};

/// Display information for any pattern type (generalized)
#[derive(Debug, Clone)]
pub struct GeneralPatternDisplay {
    /// Human-readable pattern representation
    pub pattern: String,
    /// Cardinality estimate (variant encodes the category)
    pub cardinality: PatternEstimate,
    /// Variables in this pattern
    pub variables: Vec<VarId>,
    /// For triple patterns, the detailed triple display info
    pub triple_detail: Option<PatternDisplay>,
}

/// Explanation of generalized pattern optimization
#[derive(Debug, Clone)]
pub struct GeneralExplainPlan {
    /// Whether patterns were reordered
    pub optimization: OptimizationStatus,
    /// Whether statistics were available
    pub statistics_available: bool,
    /// Original pattern order
    pub original_patterns: Vec<GeneralPatternDisplay>,
    /// Optimized pattern order
    pub optimized_patterns: Vec<GeneralPatternDisplay>,
}

/// Generate an explanation for all pattern types (generalized)
///
/// Unlike `explain_patterns` which only handles triples, this handles
/// UNION, OPTIONAL, MINUS, EXISTS, Subquery, and all other pattern types.
pub fn explain_all_patterns(patterns: &[Pattern], stats: Option<&StatsView>) -> GeneralExplainPlan {
    let statistics_available = stats
        .map(|s| s.has_property_stats() || s.has_class_stats())
        .unwrap_or(false);

    let original_patterns: Vec<GeneralPatternDisplay> = patterns
        .iter()
        .map(|p| build_general_pattern_display(p, stats))
        .collect();

    let reordered = reorder_patterns(patterns, stats, &HashSet::new());
    let optimized_patterns: Vec<GeneralPatternDisplay> = reordered
        .iter()
        .map(|p| build_general_pattern_display(p, stats))
        .collect();

    let optimization = if patterns.len() <= 1 {
        OptimizationStatus::Unchanged
    } else {
        let same = original_patterns
            .iter()
            .zip(optimized_patterns.iter())
            .all(|(a, b)| a.pattern == b.pattern);
        if same {
            OptimizationStatus::Unchanged
        } else {
            OptimizationStatus::Reordered
        }
    };

    GeneralExplainPlan {
        optimization,
        statistics_available,
        original_patterns,
        optimized_patterns,
    }
}

/// Build display info for any pattern type
fn build_general_pattern_display(
    pattern: &Pattern,
    stats: Option<&StatsView>,
) -> GeneralPatternDisplay {
    let cardinality = estimate_pattern(pattern, &HashSet::new(), stats);
    let variables = pattern.produced_vars();

    let triple_detail = if let Pattern::Triple(tp) = pattern {
        Some(build_pattern_display(tp, stats))
    } else {
        None
    };

    GeneralPatternDisplay {
        pattern: format_general_pattern(pattern),
        cardinality,
        variables,
        triple_detail,
    }
}

/// Format any pattern type as a human-readable string
pub fn format_general_pattern(pattern: &Pattern) -> String {
    match pattern {
        Pattern::Triple(tp) => format_pattern(tp),
        Pattern::Filter(expr) => format!("FILTER({expr:?})"),
        Pattern::Bind { var, expr } => format!("BIND({:?} AS ?v{})", expr, var.0),
        Pattern::Values { vars, rows } => {
            let var_names: Vec<String> = vars.iter().map(|v| format!("?v{}", v.0)).collect();
            format!("VALUES ({}) {{ {} rows }}", var_names.join(" "), rows.len())
        }
        Pattern::Union(branches) => {
            let branch_strs: Vec<String> = branches
                .iter()
                .map(|b| format!("{} patterns", b.len()))
                .collect();
            format!("UNION {{ {} }}", branch_strs.join(" | "))
        }
        Pattern::Optional(inner) => {
            if inner.len() == 1 {
                if let Some(tp) = inner[0].as_triple() {
                    return format!("OPTIONAL {{ {} }}", format_pattern(tp));
                }
            }
            format!("OPTIONAL {{ {} patterns }}", inner.len())
        }
        Pattern::Minus(inner) => format!("MINUS {{ {} patterns }}", inner.len()),
        Pattern::Exists(inner) => format!("EXISTS {{ {} patterns }}", inner.len()),
        Pattern::NotExists(inner) => format!("NOT EXISTS {{ {} patterns }}", inner.len()),
        Pattern::Subquery(sq) => {
            let var_names: Vec<String> = sq.select.iter().map(|v| format!("?v{}", v.0)).collect();
            format!("SUBQUERY SELECT {} {{ ... }}", var_names.join(" "))
        }
        Pattern::PropertyPath(pp) => format!(
            "PROPERTY PATH {} {:?}",
            format_ref(&pp.subject),
            pp.modifier
        ),
        Pattern::IndexSearch(isp) => {
            format!("INDEX SEARCH {}", isp.graph_source_id)
        }
        Pattern::VectorSearch(vsp) => {
            format!("VECTOR SEARCH {}", vsp.graph_source_id)
        }
        Pattern::R2rml(r2rml) => {
            format!("R2RML SCAN {}", r2rml.graph_source_id)
        }
        Pattern::GeoSearch(_) => "GEO SEARCH".to_string(),
        Pattern::S2Search(_) => "S2 SEARCH".to_string(),
        Pattern::Graph { name, patterns } => {
            format!("GRAPH {:?} {{ {} patterns }}", name, patterns.len())
        }
        Pattern::Service(sp) => {
            format!(
                "SERVICE {:?} {{ {} patterns }}",
                sp.endpoint,
                sp.patterns.len()
            )
        }
    }
}

impl fmt::Display for GeneralPatternDisplay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.pattern)?;

        match &self.cardinality {
            PatternEstimate::Source { row_count } => {
                write!(f, " | category=Source row_count={row_count:.0}")?;
            }
            PatternEstimate::Reducer { multiplier } => {
                write!(f, " | category=Reducer multiplier={multiplier:.2}")?;
            }
            PatternEstimate::Expander { multiplier } => {
                write!(f, " | category=Expander multiplier={multiplier:.2}")?;
            }
            PatternEstimate::Deferred => {
                write!(f, " | category=Deferred")?;
            }
        }

        if let Some(detail) = &self.triple_detail {
            write!(
                f,
                " type={:?} score={}",
                detail.pattern_type, detail.selectivity_score
            )?;
        }

        Ok(())
    }
}

impl fmt::Display for GeneralExplainPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "=== Query Optimization Explain (Generalized) ===")?;
        writeln!(f)?;
        writeln!(
            f,
            "Statistics available: {}",
            if self.statistics_available {
                "yes"
            } else {
                "no (using fallback estimates)"
            }
        )?;
        writeln!(
            f,
            "Optimization: {}",
            match self.optimization {
                OptimizationStatus::Reordered => "patterns reordered",
                OptimizationStatus::Unchanged => "order unchanged",
            }
        )?;
        writeln!(f)?;

        writeln!(f, "--- Original Pattern Order ---")?;
        for (i, p) in self.original_patterns.iter().enumerate() {
            writeln!(f, "  [{}] {}", i + 1, p)?;
        }
        writeln!(f)?;

        writeln!(f, "--- Optimized Pattern Order ---")?;
        for (i, p) in self.optimized_patterns.iter().enumerate() {
            writeln!(f, "  [{}] {}", i + 1, p)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::Sid;

    fn make_pattern(s: VarId, pred_name: &str, o: VarId) -> TriplePattern {
        TriplePattern::new(
            Ref::Var(s),
            Ref::Sid(Sid::new(100, pred_name)),
            Term::Var(o),
        )
    }

    fn make_bound_subject_pattern(s_sid: Sid, pred_name: &str, o: VarId) -> TriplePattern {
        TriplePattern::new(
            Ref::Sid(s_sid),
            Ref::Sid(Sid::new(100, pred_name)),
            Term::Var(o),
        )
    }

    #[test]
    fn test_explain_single_pattern() {
        let patterns = vec![make_pattern(VarId(0), "name", VarId(1))];
        let explain = explain_patterns(&patterns, None);

        assert_eq!(explain.optimization, OptimizationStatus::Unchanged);
        assert!(!explain.statistics_available);
        assert_eq!(explain.original_patterns.len(), 1);
        assert_eq!(explain.optimized_patterns.len(), 1);
    }

    #[test]
    fn test_explain_reordering() {
        // Pattern with higher selectivity should be reordered to come first
        // p1: ?s :name ?name (PropertyScan, score=1000)
        // p2: ex:person1 :age ?age (BoundSubject, score=10)
        let p1 = make_pattern(VarId(0), "name", VarId(1));
        let p2 = make_bound_subject_pattern(Sid::new(50, "person1"), "age", VarId(2));

        let patterns = vec![p1, p2];
        let explain = explain_patterns(&patterns, None);

        // When *all* patterns are using fallback scoring (no relevant stats),
        // we don't reorder to avoid noisy/unstable explain output.
        assert_eq!(explain.optimization, OptimizationStatus::Unchanged);
    }

    #[test]
    fn test_explain_with_stats() {
        use fluree_db_core::PropertyStatData;
        use std::collections::HashMap;

        let mut properties = HashMap::new();
        properties.insert(
            Sid::new(100, "name"),
            PropertyStatData {
                count: 5000,
                ndv_values: 4500,
                ndv_subjects: 5000,
            },
        );
        properties.insert(
            Sid::new(100, "age"),
            PropertyStatData {
                count: 100,
                ndv_values: 80,
                ndv_subjects: 100,
            },
        );

        let stats = StatsView {
            properties,
            classes: HashMap::new(),
            properties_by_iri: HashMap::new(),
            classes_by_iri: HashMap::new(),
            graph_properties: HashMap::new(),
        };

        let p1 = make_pattern(VarId(0), "name", VarId(1));
        let p2 = make_pattern(VarId(0), "age", VarId(2));

        let patterns = vec![p1, p2];
        let explain = explain_patterns(&patterns, Some(&stats));

        assert!(explain.statistics_available);

        // age has lower count (100) vs name (5000), so age should come first
        assert_eq!(explain.optimized_patterns[0].selectivity_score, 100);
        assert_eq!(explain.optimized_patterns[1].selectivity_score, 5000);
    }

    #[test]
    fn test_explain_with_class_stats_only() {
        use std::collections::HashMap;

        // Only class stats, no property stats
        let mut classes = HashMap::new();
        classes.insert(Sid::new(200, "Person"), 500);

        let stats = StatsView {
            properties: HashMap::new(),
            classes,
            properties_by_iri: HashMap::new(),
            classes_by_iri: HashMap::new(),
            graph_properties: HashMap::new(),
        };

        let patterns = vec![make_pattern(VarId(0), "name", VarId(1))];
        let explain = explain_patterns(&patterns, Some(&stats));

        // Should report statistics_available=true because class stats exist
        assert!(explain.statistics_available);
    }

    #[test]
    fn test_explain_display() {
        let patterns = vec![
            make_pattern(VarId(0), "name", VarId(1)),
            make_pattern(VarId(0), "age", VarId(2)),
        ];
        let explain = explain_patterns(&patterns, None);

        let output = format!("{explain}");
        assert!(output.contains("Query Optimization Explain"));
        assert!(output.contains("Statistics available: no"));
        assert!(output.contains("Original Pattern Order"));
        assert!(output.contains("Optimized Pattern Order"));
    }

    #[test]
    fn test_pattern_type_classification() {
        // PropertyScan: ?s :p ?o
        let p1 = make_pattern(VarId(0), "name", VarId(1));
        assert_eq!(
            classify_pattern(&p1, &HashSet::new()),
            PatternType::PropertyScan
        );

        // BoundSubject: s :p ?o
        let p2 = make_bound_subject_pattern(Sid::new(50, "person1"), "name", VarId(1));
        assert_eq!(
            classify_pattern(&p2, &HashSet::new()),
            PatternType::BoundSubject
        );

        // BoundObject: ?s :p o
        let p3 = TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(Sid::new(100, "name")),
            Term::Value(fluree_db_core::FlakeValue::String("Alice".into())),
        );
        assert_eq!(
            classify_pattern(&p3, &HashSet::new()),
            PatternType::BoundObject
        );

        // ExactMatch: s :p o
        let p4 = TriplePattern::new(
            Ref::Sid(Sid::new(50, "person1")),
            Ref::Sid(Sid::new(100, "name")),
            Term::Value(fluree_db_core::FlakeValue::String("Alice".into())),
        );
        assert_eq!(
            classify_pattern(&p4, &HashSet::new()),
            PatternType::ExactMatch
        );
    }

    #[test]
    fn test_selectivity_inputs_captured() {
        use fluree_db_core::PropertyStatData;
        use std::collections::HashMap;

        let mut properties = HashMap::new();
        properties.insert(
            Sid::new(100, "name"),
            PropertyStatData {
                count: 5000,
                ndv_values: 4500,
                ndv_subjects: 5000,
            },
        );

        let stats = StatsView {
            properties,
            classes: HashMap::new(),
            properties_by_iri: HashMap::new(),
            classes_by_iri: HashMap::new(),
            graph_properties: HashMap::new(),
        };

        let pattern = make_pattern(VarId(0), "name", VarId(1));
        let display = build_pattern_display(&pattern, Some(&stats));

        assert_eq!(display.selectivity_score, 5000); // PropertyScan uses count
        assert_eq!(display.inputs.property_sid, Some("100:name".to_string()));
        assert_eq!(display.inputs.count, Some(5000));
        assert_eq!(display.inputs.ndv_values, Some(4500));
        assert_eq!(display.inputs.ndv_subjects, Some(5000));
        assert!(display.inputs.fallback.is_none()); // Full stats available, no fallback
    }

    #[test]
    fn test_structural_equality_for_order_detection() {
        // Test that order change detection uses structural equality
        let p1 = make_pattern(VarId(0), "name", VarId(1));
        let p2 = make_pattern(VarId(0), "name", VarId(1));

        // Same pattern should be equal
        assert_eq!(p1, p2);

        let p3 = make_pattern(VarId(0), "age", VarId(2));
        // Different pattern should not be equal
        assert_ne!(p1, p3);
    }

    #[test]
    fn test_explain_execution_hints_reports_fused_property_join() {
        use crate::ir::{Expression, FilterValue, Function, Pattern};

        let s = VarId(0);
        let patterns = vec![
            Pattern::Triple(TriplePattern::new(
                Ref::Var(s),
                Ref::Sid(Sid::new(100, "type")),
                Term::Sid(Sid::new(100, "Deal")),
            )),
            Pattern::Triple(make_pattern(s, "name", VarId(1))),
            Pattern::Triple(make_pattern(s, "amount", VarId(2))),
            Pattern::Triple(make_pattern(s, "stage", VarId(3))),
            Pattern::Optional(vec![Pattern::Triple(make_pattern(
                s,
                "probability",
                VarId(4),
            ))]),
            Pattern::Optional(vec![Pattern::Triple(make_pattern(s, "closedAt", VarId(5)))]),
            Pattern::Filter(Expression::not(Expression::Call {
                func: Function::StrStarts,
                args: vec![
                    Expression::Call {
                        func: Function::Str,
                        args: vec![Expression::Var(VarId(3))],
                    },
                    Expression::Const(FilterValue::String("Closed".to_string())),
                ],
            })),
        ];

        let hints = explain_execution_hints(&patterns, None);
        assert_eq!(hints.len(), 1);
        assert_eq!(hints[0].strategy, "property_join_fused_star");
        assert_eq!(hints[0].fused_optional_triples, 2);
        assert_eq!(hints[0].fused_filters, 1);
    }
}
