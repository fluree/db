//! Query pattern rewriting for RDFS/OWL reasoning
//!
//! This module provides query expansion for entailment modes:
//! - RDFS: Expands `rdf:type` patterns to include subclasses and predicates to
//!   include subproperties.
//! - OWL-QL: Bounded query rewriting (Phase 2)
//! - OWL-RL: Materialization mode (Phase 5)
//!
//! Use `rewrite_patterns` with a `PlanContext` to expand query patterns according to the active entailment mode.

use crate::ir::Pattern;
use crate::ir::triple::{Ref, Term, TriplePattern};
use fluree_db_core::{is_rdf_type, SchemaHierarchy, Sid};


/// Entailment mode for query execution.
///
/// Controls how patterns are expanded based on class/property hierarchies.
/// For composite reasoning, see `ReasoningModes` which supports multiple modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EntailmentMode {
    /// No reasoning - exact pattern matching only
    #[default]
    None,
    /// RDFS reasoning - expand rdf:type to subclasses, predicates to subproperties
    Rdfs,
    /// OWL2-QL reasoning - bounded query rewriting (Phase 2)
    OwlQl,
    /// OWL2-RL reasoning - uses materialized inferences (Phase 5)
    OwlRlMaterialized,
    /// Hybrid mode - combination of rewriting and materialization
    Hybrid,
}

/// Safety limits to prevent query explosion on wide hierarchies
///
/// When a class has many subclasses or a property has many subproperties,
/// expansion can create a very large number of patterns. These limits cap
/// the expansion to keep queries tractable.
#[derive(Debug, Clone, Copy)]
pub struct PlanLimits {
    /// Max expanded patterns per original pattern (default: 50)
    pub max_expansions_per_pattern: usize,
    /// Max total expanded patterns across query (default: 200)
    pub max_total_expansions: usize,
}

impl Default for PlanLimits {
    fn default() -> Self {
        Self {
            max_expansions_per_pattern: 50,
            max_total_expansions: 200,
        }
    }
}

/// Context for pattern rewriting
#[derive(Debug, Clone)]
pub struct PlanContext {
    /// Entailment mode controlling what expansions to apply
    pub entailment_mode: EntailmentMode,
    /// Schema hierarchy for class/property lookups (None = no schema available)
    pub hierarchy: Option<SchemaHierarchy>,
    /// Safety limits for expansion
    pub limits: PlanLimits,
}

/// Result of rewriting a single pattern
#[derive(Debug, Clone)]
pub enum RewriteResult {
    /// Pattern unchanged (no expansion needed or possible)
    Unchanged,
    /// Pattern expanded to multiple alternatives
    Expanded(Vec<Pattern>),
    /// Expansion was capped due to limits
    Capped {
        /// Patterns that were included (up to limit)
        patterns: Vec<Pattern>,
        /// Original count before capping
        original_count: usize,
    },
}

/// Diagnostic information collected during rewriting
#[derive(Debug, Clone, Default)]
pub struct Diagnostics {
    /// Number of patterns expanded
    pub patterns_expanded: usize,
    /// Number of type expansions performed
    pub type_expansions: usize,
    /// Number of predicate expansions performed
    pub predicate_expansions: usize,
    /// Warnings generated during rewriting
    pub warnings: Vec<String>,
    /// Whether any expansion was capped due to limits
    pub was_capped: bool,
    /// Schema epoch used for expansion (for cache validation)
    pub schema_epoch: Option<u64>,
}

impl Diagnostics {
    /// Create new diagnostics with schema epoch
    pub fn with_epoch(epoch: Option<u64>) -> Self {
        Self {
            schema_epoch: epoch,
            ..Default::default()
        }
    }

    /// Add a warning message
    pub fn warn(&mut self, msg: impl Into<String>) {
        self.warnings.push(msg.into());
    }
}

/// Recurse a rewriter into the subpatterns of a container variant and
/// report whether anything changed.
///
/// `recurse` is the rewriter's per-pattern-list entry point — it sees an
/// owned `Vec<Pattern>` plus the shared `Diagnostics` and returns the
/// rewritten list. This helper handles the three boilerplate concerns that
/// every container arm in the rewriters used to inline:
///
/// 1. Snapshot `diag.patterns_expanded` before recursion.
/// 2. Walk every nested `Vec<Pattern>` via [`Pattern::map_subpatterns`].
/// 3. Wrap the reconstructed pattern in `Expanded(vec![..])` if the
///    counter advanced, otherwise return `Unchanged`.
///
/// Callers control which container variants this function gets called for
/// by matching on the variants they want to recurse into. Variants the
/// rewriter wants to treat as a leaf (typically `Subquery`) stay in the
/// rewriter's leaf arm and never reach this helper.
pub fn rewrite_subpatterns<F>(
    pattern: Pattern,
    diag: &mut Diagnostics,
    mut recurse: F,
) -> RewriteResult
where
    F: FnMut(Vec<Pattern>, &mut Diagnostics) -> Vec<Pattern>,
{
    let before = diag.patterns_expanded;
    let rewritten = pattern.map_subpatterns(&mut |xs| recurse(xs, diag));
    if diag.patterns_expanded > before {
        RewriteResult::Expanded(vec![rewritten])
    } else {
        RewriteResult::Unchanged
    }
}

/// Rewrite patterns according to the entailment mode
///
/// This function applies pattern expansion based on the entailment mode:
/// - `None`: Returns patterns unchanged
/// - `Rdfs`: Expands `rdf:type` patterns to include subclasses
/// - Other modes: Currently fall back to RDFS behavior
///
/// # Arguments
///
/// * `patterns` - Original patterns from the query
/// * `ctx` - Planning context with entailment mode and schema hierarchy
///
/// # Returns
///
/// A tuple of (rewritten patterns, diagnostics).
///
/// # Pattern Expansion
///
/// Given a pattern `?s rdf:type :Animal` where `:Dog` and `:Cat` are subclasses
/// of `:Animal`, the pattern is expanded to:
/// ```text
/// UNION(
///   ?s rdf:type :Animal,
///   ?s rdf:type :Dog,
///   ?s rdf:type :Cat
/// )
/// ```
pub fn rewrite_patterns(patterns: &[Pattern], ctx: &PlanContext) -> (Vec<Pattern>, Diagnostics) {
    let epoch = ctx
        .hierarchy
        .as_ref()
        .map(fluree_db_core::SchemaHierarchy::epoch);
    let mut diag = Diagnostics::with_epoch(epoch);

    // No-op if entailment is disabled
    if ctx.entailment_mode == EntailmentMode::None {
        return (patterns.to_vec(), diag);
    }

    // No-op if no hierarchy available
    let hierarchy = match &ctx.hierarchy {
        Some(h) => h,
        None => {
            diag.warn("Entailment mode enabled but no schema hierarchy available");
            return (patterns.to_vec(), diag);
        }
    };

    // Use a shared budget across all recursion
    let mut total_expansions = 0;
    let result =
        rewrite_patterns_internal(patterns, hierarchy, ctx, &mut diag, &mut total_expansions);

    (result, diag)
}

/// Internal rewrite function that threads the global expansion budget through recursion.
///
/// This ensures that `max_total_expansions` applies across the entire query tree,
/// not just at each level independently.
fn rewrite_patterns_internal(
    patterns: &[Pattern],
    hierarchy: &SchemaHierarchy,
    ctx: &PlanContext,
    diag: &mut Diagnostics,
    total_expansions: &mut usize,
) -> Vec<Pattern> {
    let mut result = Vec::with_capacity(patterns.len());

    for pattern in patterns {
        let rewritten = rewrite_single_pattern(pattern, hierarchy, ctx, diag, total_expansions);
        match rewritten {
            RewriteResult::Unchanged => {
                result.push(pattern.clone());
            }
            RewriteResult::Expanded(expanded) => {
                diag.patterns_expanded += 1;
                result.extend(expanded);
            }
            RewriteResult::Capped {
                patterns: expanded,
                original_count,
            } => {
                diag.patterns_expanded += 1;
                diag.was_capped = true;
                diag.warn(format!(
                    "Expansion capped: {} patterns reduced to {} due to limits",
                    original_count,
                    expanded.len()
                ));
                result.extend(expanded);
            }
        }
    }

    result
}

/// Rewrite a single pattern
fn rewrite_single_pattern(
    pattern: &Pattern,
    hierarchy: &SchemaHierarchy,
    ctx: &PlanContext,
    diag: &mut Diagnostics,
    total_expansions: &mut usize,
) -> RewriteResult {
    match pattern {
        Pattern::Triple(tp) => rewrite_triple_pattern(tp, hierarchy, ctx, diag, total_expansions),

        // Recursively process nested patterns — sharing the global budget.
        // Subquery is treated as a leaf below; the rewriter doesn't expand
        // across subquery scope boundaries.
        Pattern::Optional(_)
        | Pattern::Union(_)
        | Pattern::Minus(_)
        | Pattern::Exists(_)
        | Pattern::NotExists(_)
        | Pattern::Graph { .. }
        | Pattern::Service(_) => rewrite_subpatterns(pattern.clone(), diag, |xs, diag| {
            rewrite_patterns_internal(&xs, hierarchy, ctx, diag, total_expansions)
        }),

        // Non-expandable patterns
        Pattern::Filter(_)
        | Pattern::Bind { .. }
        | Pattern::Values { .. }
        | Pattern::PropertyPath(_)
        | Pattern::Subquery(_)
        | Pattern::IndexSearch(_)
        | Pattern::VectorSearch(_)
        | Pattern::R2rml(_)
        | Pattern::GeoSearch(_)
        | Pattern::S2Search(_) => RewriteResult::Unchanged,
    }
}

/// Rewrite a triple pattern for RDFS expansion
fn rewrite_triple_pattern(
    tp: &TriplePattern,
    hierarchy: &SchemaHierarchy,
    ctx: &PlanContext,
    diag: &mut Diagnostics,
    total_expansions: &mut usize,
) -> RewriteResult {
    // Only expand when predicate is rdf:type and object is a constant SID (class)
    // Variables in object position cannot be expanded (we don't know the class)
    //
    // NOTE: Currently only handles Term::Sid predicates and objects because the
    // SchemaHierarchy lookup requires SIDs. Term::Iri predicates (from cross-ledger
    // lowering) won't trigger expansion. This is a known limitation - RDFS expansion
    // requires either:
    // - Single-ledger mode with Term::Sid predicates, or
    // - A future enhancement to support IRI-based hierarchy lookups
    if let (Ref::Sid(predicate), Term::Sid(class)) = (&tp.p, &tp.o) {
        if is_rdf_type(predicate) {
            return expand_type_pattern(tp, class, hierarchy, ctx, diag, total_expansions);
        }
    }

    // Expand predicate hierarchies (subPropertyOf)
    // When predicate is a constant SID with subproperties, expand to union of
    // predicate + all subproperties
    if let Ref::Sid(predicate) = &tp.p {
        // Don't expand rdf:type (handled above) or variables
        if !is_rdf_type(predicate) {
            return expand_predicate_pattern(tp, predicate, hierarchy, ctx, diag, total_expansions);
        }
    }

    RewriteResult::Unchanged
}

/// Expand an rdf:type pattern to include subclasses
///
/// Given `?s rdf:type :Animal`, expands to:
/// ```text
/// UNION(
///   ?s rdf:type :Animal,
///   ?s rdf:type :Dog,
///   ?s rdf:type :Cat,
///   ...
/// )
/// ```
fn expand_type_pattern(
    tp: &TriplePattern,
    class: &Sid,
    hierarchy: &SchemaHierarchy,
    ctx: &PlanContext,
    diag: &mut Diagnostics,
    total_expansions: &mut usize,
) -> RewriteResult {
    let subclasses = hierarchy.subclasses_of(class);

    // No subclasses = no expansion needed
    if subclasses.is_empty() {
        return RewriteResult::Unchanged;
    }

    diag.type_expansions += 1;

    // Build patterns for class + all subclasses
    let mut type_patterns: Vec<TriplePattern> = Vec::with_capacity(1 + subclasses.len());
    type_patterns.push(tp.clone()); // Original pattern

    for subclass in subclasses {
        type_patterns.push(TriplePattern::new(
            tp.s.clone(),
            tp.p.clone(),
            Term::Sid(subclass.clone()),
        ));
    }

    // Check limits
    let total_count = type_patterns.len();
    let available_budget = ctx
        .limits
        .max_total_expansions
        .saturating_sub(*total_expansions);
    let per_pattern_limit = ctx.limits.max_expansions_per_pattern;

    let effective_limit = per_pattern_limit.min(available_budget);

    // If we're out of global budget (or explicitly configured to 0), never emit an empty UNION.
    // Instead, keep the original triple pattern and report that expansion was capped.
    if effective_limit == 0 {
        return RewriteResult::Capped {
            patterns: vec![Pattern::Triple(tp.clone())],
            original_count: total_count,
        };
    }

    if total_count > effective_limit {
        // Cap expansion
        type_patterns.truncate(effective_limit);
        *total_expansions += type_patterns.len();

        // If we could only keep the original pattern, avoid producing a 1-branch UNION.
        // Still report it as capped so callers can surface diagnostics.
        if type_patterns.len() == 1 {
            return RewriteResult::Capped {
                patterns: vec![Pattern::Triple(tp.clone())],
                original_count: total_count,
            };
        }

        // Create UNION of capped patterns
        let branches: Vec<Vec<Pattern>> = type_patterns
            .into_iter()
            .map(|p| vec![Pattern::Triple(p)])
            .collect();

        return RewriteResult::Capped {
            patterns: vec![Pattern::Union(branches)],
            original_count: total_count,
        };
    }

    *total_expansions += total_count;

    // Create UNION of all type patterns
    let branches: Vec<Vec<Pattern>> = type_patterns
        .into_iter()
        .map(|p| vec![Pattern::Triple(p)])
        .collect();

    RewriteResult::Expanded(vec![Pattern::Union(branches)])
}

/// Expand a predicate pattern to include subproperties
///
/// Given `?s :hasColor ?o` where `:hasFurColor` and `:hasSkinColor` are subproperties
/// of `:hasColor`, expands to:
/// ```text
/// UNION(
///   ?s :hasColor ?o,
///   ?s :hasFurColor ?o,
///   ?s :hasSkinColor ?o
/// )
/// ```
fn expand_predicate_pattern(
    tp: &TriplePattern,
    predicate: &Sid,
    hierarchy: &SchemaHierarchy,
    ctx: &PlanContext,
    diag: &mut Diagnostics,
    total_expansions: &mut usize,
) -> RewriteResult {
    let subproperties = hierarchy.subproperties_of(predicate);

    // No subproperties = no expansion needed
    if subproperties.is_empty() {
        return RewriteResult::Unchanged;
    }

    diag.predicate_expansions += 1;

    // Build patterns for predicate + all subproperties
    let mut pred_patterns: Vec<TriplePattern> = Vec::with_capacity(1 + subproperties.len());
    pred_patterns.push(tp.clone()); // Original pattern

    for subprop in subproperties {
        pred_patterns.push(TriplePattern::new(
            tp.s.clone(),
            Ref::Sid(subprop.clone()),
            tp.o.clone(),
        ));
    }

    // Check limits
    let total_count = pred_patterns.len();
    let available_budget = ctx
        .limits
        .max_total_expansions
        .saturating_sub(*total_expansions);
    let per_pattern_limit = ctx.limits.max_expansions_per_pattern;

    let effective_limit = per_pattern_limit.min(available_budget);

    // If we're out of global budget, keep original pattern and report capping
    if effective_limit == 0 {
        return RewriteResult::Capped {
            patterns: vec![Pattern::Triple(tp.clone())],
            original_count: total_count,
        };
    }

    if total_count > effective_limit {
        // Cap expansion
        pred_patterns.truncate(effective_limit);
        *total_expansions += pred_patterns.len();

        // If we could only keep the original pattern, avoid producing a 1-branch UNION
        if pred_patterns.len() == 1 {
            return RewriteResult::Capped {
                patterns: vec![Pattern::Triple(tp.clone())],
                original_count: total_count,
            };
        }

        // Create UNION of capped patterns
        let branches: Vec<Vec<Pattern>> = pred_patterns
            .into_iter()
            .map(|p| vec![Pattern::Triple(p)])
            .collect();

        return RewriteResult::Capped {
            patterns: vec![Pattern::Union(branches)],
            original_count: total_count,
        };
    }

    *total_expansions += total_count;

    // Create UNION of all predicate patterns
    let branches: Vec<Vec<Pattern>> = pred_patterns
        .into_iter()
        .map(|p| vec![Pattern::Triple(p)])
        .collect();

    RewriteResult::Expanded(vec![Pattern::Union(branches)])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::triple::Ref;
    use crate::var_registry::VarId;
    use fluree_db_core::{IndexSchema, SchemaPredicateInfo, SchemaPredicates};
    use fluree_db_core::{Sid, SidInterner};
    use fluree_vocab::namespaces::RDF;

    fn make_rdf_type() -> Sid {
        Sid::new(RDF, "type")
    }

    fn make_hierarchy_with_subclasses() -> SchemaHierarchy {
        let interner = SidInterner::new();

        // Dog and Cat are subclasses of Animal
        let vals = vec![
            SchemaPredicateInfo {
                id: interner.intern(100, "Dog"),
                subclass_of: vec![interner.intern(100, "Animal")],
                parent_props: vec![],
                child_props: vec![],
            },
            SchemaPredicateInfo {
                id: interner.intern(100, "Cat"),
                subclass_of: vec![interner.intern(100, "Animal")],
                parent_props: vec![],
                child_props: vec![],
            },
        ];

        let schema = IndexSchema {
            t: 1,
            pred: SchemaPredicates {
                keys: vec![
                    "id".to_string(),
                    "subclassOf".to_string(),
                    "parentProps".to_string(),
                    "childProps".to_string(),
                ],
                vals,
            },
        };

        SchemaHierarchy::from_db_root_schema(&schema)
    }

    #[test]
    fn test_entailment_mode_default() {
        assert_eq!(EntailmentMode::default(), EntailmentMode::None);
    }

    #[test]
    fn test_no_expansion_when_disabled() {
        let pattern = Pattern::Triple(TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(make_rdf_type()),
            Term::Sid(Sid::new(100, "Animal")),
        ));

        let ctx = PlanContext {
            entailment_mode: EntailmentMode::None,
            hierarchy: Some(make_hierarchy_with_subclasses()),
            limits: PlanLimits::default(),
        };

        let (result, diag) = rewrite_patterns(&[pattern], &ctx);

        assert_eq!(result.len(), 1);
        assert_eq!(diag.patterns_expanded, 0);
    }

    #[test]
    fn test_no_expansion_without_hierarchy() {
        let pattern = Pattern::Triple(TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(make_rdf_type()),
            Term::Sid(Sid::new(100, "Animal")),
        ));

        let ctx = PlanContext {
            entailment_mode: EntailmentMode::Rdfs,
            hierarchy: None,
            limits: PlanLimits::default(),
        };

        let (result, diag) = rewrite_patterns(&[pattern], &ctx);

        assert_eq!(result.len(), 1);
        assert_eq!(diag.patterns_expanded, 0);
        assert!(!diag.warnings.is_empty());
    }

    #[test]
    fn test_type_expansion() {
        let interner = SidInterner::new();
        let animal = interner.intern(100, "Animal");

        let pattern = Pattern::Triple(TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(make_rdf_type()),
            Term::Sid(animal),
        ));

        let ctx = PlanContext {
            entailment_mode: EntailmentMode::Rdfs,
            hierarchy: Some(make_hierarchy_with_subclasses()),
            limits: PlanLimits::default(),
        };

        let (result, diag) = rewrite_patterns(&[pattern], &ctx);

        // Should produce a UNION with 3 branches (Animal, Dog, Cat)
        assert_eq!(result.len(), 1);
        match &result[0] {
            Pattern::Union(branches) => {
                assert_eq!(branches.len(), 3);
            }
            _ => panic!("Expected Union pattern"),
        }

        assert_eq!(diag.patterns_expanded, 1);
        assert_eq!(diag.type_expansions, 1);
    }

    #[test]
    fn test_no_expansion_for_variable_object() {
        let pattern = Pattern::Triple(TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(make_rdf_type()),
            Term::Var(VarId(1)), // Variable object - cannot expand
        ));

        let ctx = PlanContext {
            entailment_mode: EntailmentMode::Rdfs,
            hierarchy: Some(make_hierarchy_with_subclasses()),
            limits: PlanLimits::default(),
        };

        let (result, diag) = rewrite_patterns(&[pattern], &ctx);

        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], Pattern::Triple(_)));
        assert_eq!(diag.patterns_expanded, 0);
    }

    #[test]
    fn test_no_expansion_for_non_type_predicate() {
        let interner = SidInterner::new();

        let pattern = Pattern::Triple(TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(interner.intern(100, "name")), // Not rdf:type
            Term::Sid(interner.intern(100, "Animal")),
        ));

        let ctx = PlanContext {
            entailment_mode: EntailmentMode::Rdfs,
            hierarchy: Some(make_hierarchy_with_subclasses()),
            limits: PlanLimits::default(),
        };

        let (result, diag) = rewrite_patterns(&[pattern], &ctx);

        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], Pattern::Triple(_)));
        assert_eq!(diag.patterns_expanded, 0);
    }

    #[test]
    fn test_expansion_limit_per_pattern() {
        let interner = SidInterner::new();

        // Create a hierarchy with many subclasses
        let animal = interner.intern(100, "Animal");
        let mut vals = Vec::new();
        for i in 0..100 {
            vals.push(SchemaPredicateInfo {
                id: interner.intern(100, &format!("SubClass{i}")),
                subclass_of: vec![animal.clone()],
                parent_props: vec![],
                child_props: vec![],
            });
        }

        let schema = IndexSchema {
            t: 1,
            pred: SchemaPredicates {
                keys: vec![
                    "id".to_string(),
                    "subclassOf".to_string(),
                    "parentProps".to_string(),
                    "childProps".to_string(),
                ],
                vals,
            },
        };

        let hierarchy = SchemaHierarchy::from_db_root_schema(&schema);

        let pattern = Pattern::Triple(TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(make_rdf_type()),
            Term::Sid(animal),
        ));

        let ctx = PlanContext {
            entailment_mode: EntailmentMode::Rdfs,
            hierarchy: Some(hierarchy),
            limits: PlanLimits {
                max_expansions_per_pattern: 10,
                max_total_expansions: 200,
            },
        };

        let (result, diag) = rewrite_patterns(&[pattern], &ctx);

        // Should be capped to 10 patterns
        assert_eq!(result.len(), 1);
        match &result[0] {
            Pattern::Union(branches) => {
                assert_eq!(branches.len(), 10);
            }
            _ => panic!("Expected Union pattern"),
        }

        assert!(diag.was_capped);
        assert!(!diag.warnings.is_empty());
    }

    #[test]
    fn test_optional_pattern_expansion() {
        let interner = SidInterner::new();
        let animal = interner.intern(100, "Animal");

        let inner_pattern = Pattern::Triple(TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(make_rdf_type()),
            Term::Sid(animal),
        ));

        let pattern = Pattern::Optional(vec![inner_pattern]);

        let ctx = PlanContext {
            entailment_mode: EntailmentMode::Rdfs,
            hierarchy: Some(make_hierarchy_with_subclasses()),
            limits: PlanLimits::default(),
        };

        let (result, diag) = rewrite_patterns(&[pattern], &ctx);

        // The Optional should contain an expanded UNION
        assert_eq!(result.len(), 1);
        match &result[0] {
            Pattern::Optional(inner) => {
                assert_eq!(inner.len(), 1);
                assert!(matches!(inner[0], Pattern::Union(_)));
            }
            _ => panic!("Expected Optional pattern"),
        }

        assert_eq!(diag.type_expansions, 1);
    }

    #[test]
    fn test_global_budget_across_nested_patterns() {
        // Test that the total expansion budget is shared across nested patterns,
        // not reset at each level of recursion.
        let interner = SidInterner::new();

        // Create a hierarchy with many subclasses for two different classes
        let class_a = interner.intern(100, "ClassA");
        let class_b = interner.intern(100, "ClassB");

        let mut vals = Vec::new();
        // 30 subclasses of ClassA
        for i in 0..30 {
            vals.push(SchemaPredicateInfo {
                id: interner.intern(100, &format!("SubA{i}")),
                subclass_of: vec![class_a.clone()],
                parent_props: vec![],
                child_props: vec![],
            });
        }
        // 30 subclasses of ClassB
        for i in 0..30 {
            vals.push(SchemaPredicateInfo {
                id: interner.intern(100, &format!("SubB{i}")),
                subclass_of: vec![class_b.clone()],
                parent_props: vec![],
                child_props: vec![],
            });
        }

        let schema = IndexSchema {
            t: 1,
            pred: SchemaPredicates {
                keys: vec![
                    "id".to_string(),
                    "subclassOf".to_string(),
                    "parentProps".to_string(),
                    "childProps".to_string(),
                ],
                vals,
            },
        };

        let hierarchy = SchemaHierarchy::from_db_root_schema(&schema);

        // Query with type pattern for ClassA in main, and ClassB in OPTIONAL
        let main_pattern = Pattern::Triple(TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(make_rdf_type()),
            Term::Sid(class_a),
        ));
        let optional_pattern = Pattern::Optional(vec![Pattern::Triple(TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(make_rdf_type()),
            Term::Sid(class_b),
        ))]);

        // With global budget of 40, both expansions together should be capped
        // ClassA would expand to 31 patterns (1 + 30 subclasses)
        // ClassB would expand to 31 patterns (1 + 30 subclasses)
        // Total = 62, but budget is 40
        let ctx = PlanContext {
            entailment_mode: EntailmentMode::Rdfs,
            hierarchy: Some(hierarchy),
            limits: PlanLimits {
                max_expansions_per_pattern: 50, // High per-pattern limit
                max_total_expansions: 40,       // Low total limit
            },
        };

        let (_result, diag) = rewrite_patterns(&[main_pattern, optional_pattern], &ctx);

        // First pattern should expand (up to per-pattern limit or remaining budget)
        // Second pattern in OPTIONAL should be capped by remaining global budget
        assert!(diag.was_capped, "Should be capped due to global budget");
        assert_eq!(
            diag.type_expansions, 2,
            "Both patterns should attempt expansion"
        );
    }

    #[test]
    fn test_zero_global_budget_never_emits_empty_union() {
        let interner = SidInterner::new();
        let animal = interner.intern(100, "Animal");

        let pattern = Pattern::Triple(TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(make_rdf_type()),
            Term::Sid(animal),
        ));

        let ctx = PlanContext {
            entailment_mode: EntailmentMode::Rdfs,
            hierarchy: Some(make_hierarchy_with_subclasses()),
            limits: PlanLimits {
                max_expansions_per_pattern: 50,
                max_total_expansions: 0,
            },
        };

        let (result, diag) = rewrite_patterns(std::slice::from_ref(&pattern), &ctx);
        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], Pattern::Triple(_)));
        assert!(diag.was_capped, "Should report capping when budget is 0");
        assert_eq!(diag.type_expansions, 1, "Expansion should be attempted");
    }

    // ========================
    // Predicate expansion tests
    // ========================

    fn make_hierarchy_with_subproperties() -> SchemaHierarchy {
        let interner = SidInterner::new();

        // hasFurColor and hasSkinColor are subproperties of hasColor
        let vals = vec![
            SchemaPredicateInfo {
                id: interner.intern(100, "hasFurColor"),
                subclass_of: vec![],
                parent_props: vec![interner.intern(100, "hasColor")],
                child_props: vec![],
            },
            SchemaPredicateInfo {
                id: interner.intern(100, "hasSkinColor"),
                subclass_of: vec![],
                parent_props: vec![interner.intern(100, "hasColor")],
                child_props: vec![],
            },
        ];

        let schema = IndexSchema {
            t: 1,
            pred: SchemaPredicates {
                keys: vec![
                    "id".to_string(),
                    "subclassOf".to_string(),
                    "parentProps".to_string(),
                    "childProps".to_string(),
                ],
                vals,
            },
        };

        SchemaHierarchy::from_db_root_schema(&schema)
    }

    #[test]
    fn test_predicate_expansion() {
        let interner = SidInterner::new();
        let has_color = interner.intern(100, "hasColor");

        let pattern = Pattern::Triple(TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(has_color),
            Term::Var(VarId(1)),
        ));

        let ctx = PlanContext {
            entailment_mode: EntailmentMode::Rdfs,
            hierarchy: Some(make_hierarchy_with_subproperties()),
            limits: PlanLimits::default(),
        };

        let (result, diag) = rewrite_patterns(&[pattern], &ctx);

        // Should produce a UNION with 3 branches (hasColor, hasFurColor, hasSkinColor)
        assert_eq!(result.len(), 1);
        match &result[0] {
            Pattern::Union(branches) => {
                assert_eq!(branches.len(), 3);
            }
            _ => panic!("Expected Union pattern"),
        }

        assert_eq!(diag.patterns_expanded, 1);
        assert_eq!(diag.predicate_expansions, 1);
        assert_eq!(diag.type_expansions, 0);
    }

    #[test]
    fn test_no_predicate_expansion_for_variable() {
        let pattern = Pattern::Triple(TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Var(VarId(1)), // Variable predicate - cannot expand
            Term::Var(VarId(2)),
        ));

        let ctx = PlanContext {
            entailment_mode: EntailmentMode::Rdfs,
            hierarchy: Some(make_hierarchy_with_subproperties()),
            limits: PlanLimits::default(),
        };

        let (result, diag) = rewrite_patterns(&[pattern], &ctx);

        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], Pattern::Triple(_)));
        assert_eq!(diag.patterns_expanded, 0);
        assert_eq!(diag.predicate_expansions, 0);
    }

    #[test]
    fn test_no_expansion_for_predicate_without_subproperties() {
        let interner = SidInterner::new();

        let pattern = Pattern::Triple(TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(interner.intern(100, "unknownProp")), // No subproperties
            Term::Var(VarId(1)),
        ));

        let ctx = PlanContext {
            entailment_mode: EntailmentMode::Rdfs,
            hierarchy: Some(make_hierarchy_with_subproperties()),
            limits: PlanLimits::default(),
        };

        let (result, diag) = rewrite_patterns(&[pattern], &ctx);

        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], Pattern::Triple(_)));
        assert_eq!(diag.patterns_expanded, 0);
        assert_eq!(diag.predicate_expansions, 0);
    }

    #[test]
    fn test_predicate_expansion_with_limits() {
        let interner = SidInterner::new();

        // Create a hierarchy with many subproperties
        let has_attr = interner.intern(100, "hasAttr");
        let mut vals = Vec::new();
        for i in 0..100 {
            vals.push(SchemaPredicateInfo {
                id: interner.intern(100, &format!("subProp{i}")),
                subclass_of: vec![],
                parent_props: vec![has_attr.clone()],
                child_props: vec![],
            });
        }

        let schema = IndexSchema {
            t: 1,
            pred: SchemaPredicates {
                keys: vec![
                    "id".to_string(),
                    "subclassOf".to_string(),
                    "parentProps".to_string(),
                    "childProps".to_string(),
                ],
                vals,
            },
        };

        let hierarchy = SchemaHierarchy::from_db_root_schema(&schema);

        let pattern = Pattern::Triple(TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(has_attr),
            Term::Var(VarId(1)),
        ));

        let ctx = PlanContext {
            entailment_mode: EntailmentMode::Rdfs,
            hierarchy: Some(hierarchy),
            limits: PlanLimits {
                max_expansions_per_pattern: 10,
                max_total_expansions: 200,
            },
        };

        let (result, diag) = rewrite_patterns(&[pattern], &ctx);

        // Should be capped to 10 patterns
        assert_eq!(result.len(), 1);
        match &result[0] {
            Pattern::Union(branches) => {
                assert_eq!(branches.len(), 10);
            }
            _ => panic!("Expected Union pattern"),
        }

        assert!(diag.was_capped);
        assert_eq!(diag.predicate_expansions, 1);
    }

    #[test]
    fn test_combined_type_and_predicate_expansion() {
        // Test that both type and predicate expansion work together
        let interner = SidInterner::new();

        // Create hierarchy with both class and property hierarchies
        let animal = interner.intern(100, "Animal");
        let has_color = interner.intern(100, "hasColor");

        let vals = vec![
            // Dog is a subclass of Animal
            SchemaPredicateInfo {
                id: interner.intern(100, "Dog"),
                subclass_of: vec![animal.clone()],
                parent_props: vec![],
                child_props: vec![],
            },
            // hasFurColor is a subproperty of hasColor
            SchemaPredicateInfo {
                id: interner.intern(100, "hasFurColor"),
                subclass_of: vec![],
                parent_props: vec![has_color.clone()],
                child_props: vec![],
            },
        ];

        let schema = IndexSchema {
            t: 1,
            pred: SchemaPredicates {
                keys: vec![
                    "id".to_string(),
                    "subclassOf".to_string(),
                    "parentProps".to_string(),
                    "childProps".to_string(),
                ],
                vals,
            },
        };

        let hierarchy = SchemaHierarchy::from_db_root_schema(&schema);

        // Type pattern: ?s rdf:type :Animal
        let type_pattern = Pattern::Triple(TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(make_rdf_type()),
            Term::Sid(animal),
        ));

        // Predicate pattern: ?s :hasColor ?o
        let pred_pattern = Pattern::Triple(TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(has_color),
            Term::Var(VarId(1)),
        ));

        let ctx = PlanContext {
            entailment_mode: EntailmentMode::Rdfs,
            hierarchy: Some(hierarchy),
            limits: PlanLimits::default(),
        };

        let (result, diag) = rewrite_patterns(&[type_pattern, pred_pattern], &ctx);

        // Both patterns should be expanded
        assert_eq!(result.len(), 2);
        assert!(matches!(result[0], Pattern::Union(_)));
        assert!(matches!(result[1], Pattern::Union(_)));
        assert_eq!(diag.type_expansions, 1);
        assert_eq!(diag.predicate_expansions, 1);
        assert_eq!(diag.patterns_expanded, 2);
    }

}
