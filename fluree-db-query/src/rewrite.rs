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

/// Reasoning modes that can be enabled for query execution
///
/// Multiple modes can be enabled simultaneously. For example, both RDFS and OWL2-QL
/// can be active to get subclass/subproperty expansion plus inverseOf handling.
///
/// # JSON-LD Query Syntax
///
/// ```json
/// { "reasoning": ["rdfs", "owl2ql"] }  // multiple modes
/// { "reasoning": "rdfs" }               // single mode
/// { "reasoning": "none" }               // disable all (including auto-RDFS)
/// { "reasoning": "datalog", "rules": [...] }  // datalog with query-time rules
/// { "reasoning": "owl-datalog" }        // extended OWL with complex expressions
/// ```
///
/// # Default Behavior
///
/// When no `reasoning` key is present:
/// - RDFS is auto-enabled if a schema hierarchy exists
/// - OWL2-QL, OWL2-RL, OWL-Datalog, Datalog are disabled
///
/// When `reasoning: "none"` is specified, auto-RDFS is disabled.
///
/// # Query-Time Rules
///
/// Rules can be provided at query time via the `rules` field. These are merged
/// with any rules stored in the database when `datalog` reasoning is enabled.
/// Rules should be in the same JSON-LD format as stored rules:
///
/// ```json
/// {
///   "reasoning": "datalog",
///   "rules": [{
///     "@context": {"ex": "http://example.org/"},
///     "where": {"@id": "?person", "ex:parent": {"ex:parent": "?grandparent"}},
///     "insert": {"@id": "?person", "ex:grandparent": "?grandparent"}
///   }]
/// }
/// ```
///
/// # OWL-Datalog Mode
///
/// The `owl-datalog` mode enables extended OWL reasoning with complex class expressions
/// that can be expressed in Datalog. This is a SUPERSET of `owl2rl` that supports:
/// - Complex `owl:equivalentClass` with intersections containing restrictions
/// - Property chains with inverse elements and arbitrary length (≥2)
/// - Nested restrictions and complex class expressions
/// - Enhanced someValuesFrom/allValuesFrom reasoning in equivalences
///
/// This mode is opt-in and separate from standard `owl2rl`.
#[derive(Debug, Clone, Default)]
pub struct ReasoningModes {
    /// RDFS reasoning - subclass/subproperty expansion
    pub rdfs: bool,
    /// OWL2-QL reasoning - owl:inverseOf, domain/range type queries
    pub owl2ql: bool,
    /// Datalog rules (tx-time rules) - Phase 3
    pub datalog: bool,
    /// OWL2-RL materialization - Phase 5
    pub owl2rl: bool,
    /// OWL-Datalog: Extended OWL with complex class expressions
    ///
    /// This is a superset of owl2rl that supports additional constructs
    /// expressible in Datalog: complex intersections, property chains with
    /// inverses, nested restrictions, etc.
    ///
    /// Opt-in mode, not enabled by default.
    pub owl_datalog: bool,
    /// Explicitly disable all reasoning (overrides auto-RDFS)
    ///
    /// When true, no reasoning is applied even if hierarchy exists.
    /// This is set by `"reasoning": "none"` in JSON.
    pub explicit_none: bool,
    /// Query-time datalog rules (JSON-LD format)
    ///
    /// These rules are merged with database rules when datalog reasoning is enabled.
    /// Each rule should have `where` and `insert` clauses.
    pub rules: Vec<serde_json::Value>,
}

impl PartialEq for ReasoningModes {
    fn eq(&self, other: &Self) -> bool {
        self.rdfs == other.rdfs
            && self.owl2ql == other.owl2ql
            && self.datalog == other.datalog
            && self.owl2rl == other.owl2rl
            && self.owl_datalog == other.owl_datalog
            && self.explicit_none == other.explicit_none
            && self.rules == other.rules
    }
}

impl Eq for ReasoningModes {}

impl ReasoningModes {
    /// Create with no reasoning modes enabled
    pub fn none() -> Self {
        Self {
            explicit_none: true,
            ..Default::default()
        }
    }

    /// Create with RDFS enabled
    pub fn rdfs() -> Self {
        Self {
            rdfs: true,
            ..Default::default()
        }
    }

    /// Create with OWL2-QL enabled (includes RDFS for subclass expansion)
    pub fn owl2ql() -> Self {
        Self {
            rdfs: true,
            owl2ql: true,
            ..Default::default()
        }
    }

    /// Create with datalog enabled and query-time rules
    pub fn datalog_with_rules(rules: Vec<serde_json::Value>) -> Self {
        Self {
            datalog: true,
            rules,
            ..Default::default()
        }
    }

    /// Builder: enable RDFS
    pub fn with_rdfs(mut self) -> Self {
        self.rdfs = true;
        self
    }

    /// Builder: enable OWL2-QL
    pub fn with_owl2ql(mut self) -> Self {
        self.owl2ql = true;
        self
    }

    /// Builder: enable Datalog rules
    pub fn with_datalog(mut self) -> Self {
        self.datalog = true;
        self
    }

    /// Builder: enable OWL2-RL materialization
    pub fn with_owl2rl(mut self) -> Self {
        self.owl2rl = true;
        self
    }

    /// Builder: enable OWL-Datalog extended reasoning
    ///
    /// This enables complex OWL class expressions that can be expressed in Datalog:
    /// - Complex intersectionOf/unionOf with restrictions
    /// - Property chains with inverse elements and arbitrary length
    /// - Enhanced someValuesFrom/allValuesFrom in equivalences
    pub fn with_owl_datalog(mut self) -> Self {
        self.owl_datalog = true;
        self
    }

    /// Builder: add query-time rules
    ///
    /// Note: This also enables datalog reasoning mode if not already enabled.
    pub fn with_rules(mut self, rules: Vec<serde_json::Value>) -> Self {
        if !rules.is_empty() {
            self.datalog = true;
        }
        self.rules = rules;
        self
    }

    /// Check if any mode is explicitly enabled
    pub fn has_any_enabled(&self) -> bool {
        self.rdfs || self.owl2ql || self.datalog || self.owl2rl || self.owl_datalog
    }

    /// Check if reasoning is explicitly disabled
    pub fn is_disabled(&self) -> bool {
        self.explicit_none
    }

    /// Parse from a JSON value (string or array of strings)
    ///
    /// Valid values:
    /// - `"none"` - disable all reasoning
    /// - `"rdfs"` - RDFS only
    /// - `"owl2ql"` - OWL2-QL (implies RDFS)
    /// - `"datalog"` - Datalog rules
    /// - `"owl2rl"` - OWL2-RL materialization
    /// - `["rdfs", "owl2ql", ...]` - multiple modes
    pub fn from_json(value: &serde_json::Value) -> Result<Self, String> {
        match value {
            serde_json::Value::Null => Ok(Self::default()),
            serde_json::Value::String(s) => Self::parse_single(s),
            serde_json::Value::Array(arr) => {
                let mut modes = Self::default();
                for item in arr {
                    let s = item
                        .as_str()
                        .ok_or_else(|| "reasoning array must contain strings".to_string())?;
                    modes = modes.merge(&Self::parse_single(s)?);
                }
                Ok(modes)
            }
            _ => Err("reasoning must be a string or array of strings".to_string()),
        }
    }

    /// Parse reasoning modes from a query JSON object
    ///
    /// This extracts both the `reasoning` key (mode selection) and the `rules` key
    /// (query-time datalog rules) from a query object.
    ///
    /// # Example
    ///
    /// ```
    /// use fluree_db_query::rewrite::ReasoningModes;
    /// use serde_json::json;
    ///
    /// let query = json!({
    ///     "reasoning": "datalog",
    ///     "rules": [{
    ///         "@context": {"ex": "http://example.org/"},
    ///         "where": {"@id": "?person", "ex:parent": {"ex:parent": "?grandparent"}},
    ///         "insert": {"@id": "?person", "ex:grandparent": "?grandparent"}
    ///     }]
    /// });
    /// let modes = ReasoningModes::from_query_json(&query).unwrap();
    /// assert!(modes.datalog);
    /// assert!(modes.has_rules());
    /// ```
    pub fn from_query_json(query: &serde_json::Value) -> Result<Self, String> {
        // Parse reasoning modes
        let mut modes = if let Some(reasoning) = query.get("reasoning") {
            Self::from_json(reasoning)?
        } else {
            Self::default()
        };

        // Parse query-time rules
        if let Some(rules) = query.get("rules") {
            match rules {
                serde_json::Value::Array(arr) => {
                    modes.rules = arr.clone();
                    // Enable datalog if rules are provided
                    if !modes.rules.is_empty() {
                        modes.datalog = true;
                    }
                }
                serde_json::Value::Null => {}
                _ => {
                    return Err("rules must be an array".to_string());
                }
            }
        }

        Ok(modes)
    }

    /// Parse a single mode string
    fn parse_single(s: &str) -> Result<Self, String> {
        match s.to_lowercase().as_str() {
            "none" => Ok(Self::none()),
            "rdfs" => Ok(Self {
                rdfs: true,
                ..Default::default()
            }),
            "owl2ql" | "owl-ql" | "owlql" => Ok(Self {
                rdfs: true, // OWL2-QL implies RDFS for subclass expansion
                owl2ql: true,
                ..Default::default()
            }),
            "datalog" => Ok(Self {
                datalog: true,
                ..Default::default()
            }),
            "owl2rl" | "owl-rl" | "owlrl" => Ok(Self {
                owl2rl: true,
                ..Default::default()
            }),
            "owl-datalog" | "owldatalog" | "owl_datalog" => Ok(Self {
                owl_datalog: true,
                // owl-datalog is a superset of owl2rl - enable both
                owl2rl: true,
                ..Default::default()
            }),
            other => Err(format!(
                "unknown reasoning mode '{other}', expected: none, rdfs, owl2ql, datalog, owl2rl, owl-datalog"
            )),
        }
    }

    /// Merge another ReasoningModes into this one (OR of flags)
    fn merge(mut self, other: &Self) -> Self {
        self.rdfs |= other.rdfs;
        self.owl2ql |= other.owl2ql;
        self.datalog |= other.datalog;
        self.owl2rl |= other.owl2rl;
        self.owl_datalog |= other.owl_datalog;
        // explicit_none is only set by "none" alone, not merged
        // rules are combined
        self.rules.extend(other.rules.iter().cloned());
        self
    }

    /// Build from a list of mode name strings (e.g., from config graph).
    ///
    /// Accepts the same names as `parse_single`: "rdfs", "owl2ql",
    /// "owl2rl", "datalog", "owl-datalog", "none".
    /// Unknown names are logged as warnings and skipped.
    pub fn from_mode_strings(names: &[String]) -> Self {
        let mut modes = Self::default();
        for name in names {
            // Config reader returns full IRIs (e.g., "https://ns.flur.ee/db#rdfs").
            // Strip known namespace prefix before parsing.
            let short = name.strip_prefix(fluree_vocab::fluree::DB).unwrap_or(name);
            match Self::parse_single(short) {
                Ok(single) => {
                    if single.explicit_none {
                        // "none" force-disables reasoning; return immediately.
                        // merge() deliberately does not propagate explicit_none,
                        // so we must handle it here.
                        if names.len() > 1 {
                            tracing::warn!(
                                "Config reasoning modes contain 'none' alongside other modes; \
                                 'none' takes precedence"
                            );
                        }
                        return Self::none();
                    }
                    modes = modes.merge(&single);
                }
                Err(e) => {
                    tracing::warn!(mode = %name, "Unknown reasoning mode in config: {e}");
                }
            }
        }
        modes
    }

    /// Check if query-time rules are provided
    pub fn has_rules(&self) -> bool {
        !self.rules.is_empty()
    }

    /// Compute effective reasoning given available hierarchy
    ///
    /// Auto-enables RDFS if:
    /// - RDFS is not already enabled
    /// - User didn't explicitly disable reasoning with "none"
    /// - A schema hierarchy is available
    ///
    /// This means other modes (datalog, owl2rl) can be enabled independently
    /// and RDFS will still be auto-enabled unless explicitly disabled.
    pub fn effective_with_hierarchy(self, hierarchy_available: bool) -> Self {
        if self.explicit_none {
            // User explicitly disabled - no auto-enable
            return self;
        }
        // Auto-enable RDFS if not already enabled and hierarchy exists
        if !self.rdfs && hierarchy_available {
            Self { rdfs: true, ..self }
        } else {
            self
        }
    }
}

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

    // ========================
    // ReasoningModes tests
    // ========================

    #[test]
    fn test_reasoning_modes_default() {
        let modes = ReasoningModes::default();
        assert!(!modes.rdfs);
        assert!(!modes.owl2ql);
        assert!(!modes.datalog);
        assert!(!modes.owl2rl);
        assert!(!modes.explicit_none);
        assert!(!modes.has_any_enabled());
        assert!(!modes.is_disabled());
    }

    #[test]
    fn test_reasoning_modes_none() {
        let modes = ReasoningModes::none();
        assert!(!modes.rdfs);
        assert!(!modes.owl2ql);
        assert!(modes.explicit_none);
        assert!(modes.is_disabled());
    }

    #[test]
    fn test_reasoning_modes_rdfs() {
        let modes = ReasoningModes::rdfs();
        assert!(modes.rdfs);
        assert!(!modes.owl2ql);
        assert!(!modes.explicit_none);
        assert!(modes.has_any_enabled());
    }

    #[test]
    fn test_reasoning_modes_owl2ql() {
        let modes = ReasoningModes::owl2ql();
        assert!(modes.rdfs); // owl2ql implies rdfs
        assert!(modes.owl2ql);
        assert!(modes.has_any_enabled());
    }

    #[test]
    fn test_reasoning_modes_builders() {
        let modes = ReasoningModes::default()
            .with_rdfs()
            .with_owl2ql()
            .with_datalog();
        assert!(modes.rdfs);
        assert!(modes.owl2ql);
        assert!(modes.datalog);
        assert!(!modes.owl2rl);
    }

    #[test]
    fn test_reasoning_modes_from_json_string_none() {
        let value = serde_json::json!("none");
        let modes = ReasoningModes::from_json(&value).unwrap();
        assert!(modes.explicit_none);
        assert!(!modes.rdfs);
    }

    #[test]
    fn test_reasoning_modes_from_json_string_rdfs() {
        let value = serde_json::json!("rdfs");
        let modes = ReasoningModes::from_json(&value).unwrap();
        assert!(modes.rdfs);
        assert!(!modes.owl2ql);
        assert!(!modes.explicit_none);
    }

    #[test]
    fn test_reasoning_modes_from_json_string_owl2ql() {
        // Test various spellings
        for spelling in &["owl2ql", "owl-ql", "owlql", "OWL2QL", "Owl-Ql"] {
            let value = serde_json::json!(spelling);
            let modes = ReasoningModes::from_json(&value).unwrap();
            assert!(modes.owl2ql, "Failed for spelling: {spelling}");
            assert!(modes.rdfs, "OWL2QL should imply RDFS for: {spelling}");
        }
    }

    #[test]
    fn test_reasoning_modes_from_json_string_owl2rl() {
        for spelling in &["owl2rl", "owl-rl", "owlrl"] {
            let value = serde_json::json!(spelling);
            let modes = ReasoningModes::from_json(&value).unwrap();
            assert!(modes.owl2rl, "Failed for spelling: {spelling}");
        }
    }

    #[test]
    fn test_reasoning_modes_from_json_array() {
        let value = serde_json::json!(["rdfs", "owl2ql"]);
        let modes = ReasoningModes::from_json(&value).unwrap();
        assert!(modes.rdfs);
        assert!(modes.owl2ql);
        assert!(!modes.datalog);
    }

    #[test]
    fn test_reasoning_modes_from_json_array_multiple() {
        let value = serde_json::json!(["rdfs", "datalog", "owl2rl"]);
        let modes = ReasoningModes::from_json(&value).unwrap();
        assert!(modes.rdfs);
        assert!(modes.datalog);
        assert!(modes.owl2rl);
        assert!(!modes.owl2ql);
    }

    #[test]
    fn test_reasoning_modes_from_json_null() {
        let value = serde_json::json!(null);
        let modes = ReasoningModes::from_json(&value).unwrap();
        assert!(!modes.rdfs);
        assert!(!modes.explicit_none);
    }

    #[test]
    fn test_reasoning_modes_from_json_invalid_string() {
        let value = serde_json::json!("invalid_mode");
        let result = ReasoningModes::from_json(&value);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unknown reasoning mode"));
    }

    #[test]
    fn test_reasoning_modes_from_json_invalid_type() {
        let value = serde_json::json!(123);
        let result = ReasoningModes::from_json(&value);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("must be a string or array"));
    }

    #[test]
    fn test_reasoning_modes_from_json_invalid_array_element() {
        let value = serde_json::json!(["rdfs", 123]);
        let result = ReasoningModes::from_json(&value);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("must contain strings"));
    }

    #[test]
    fn test_reasoning_modes_effective_with_hierarchy_auto_rdfs() {
        // Default modes (no explicit setting) + hierarchy available = RDFS enabled
        let modes = ReasoningModes::default();
        let effective = modes.effective_with_hierarchy(true);
        assert!(effective.rdfs);
    }

    #[test]
    fn test_reasoning_modes_effective_with_hierarchy_no_auto_without_hierarchy() {
        // Default modes + no hierarchy = RDFS not enabled
        let modes = ReasoningModes::default();
        let effective = modes.effective_with_hierarchy(false);
        assert!(!effective.rdfs);
    }

    #[test]
    fn test_reasoning_modes_effective_explicit_none_overrides_auto() {
        // Explicit "none" + hierarchy available = RDFS NOT enabled
        let modes = ReasoningModes::none();
        let effective = modes.effective_with_hierarchy(true);
        assert!(!effective.rdfs);
    }

    #[test]
    fn test_reasoning_modes_effective_explicit_rdfs_without_hierarchy() {
        // Explicit RDFS + no hierarchy = RDFS still requested (may warn at runtime)
        let modes = ReasoningModes::rdfs();
        let effective = modes.effective_with_hierarchy(false);
        assert!(effective.rdfs);
    }

    #[test]
    fn test_reasoning_modes_effective_preserves_other_modes() {
        // Auto-RDFS shouldn't affect other modes
        let modes = ReasoningModes::default().with_datalog();
        let effective = modes.effective_with_hierarchy(true);
        assert!(effective.rdfs);
        assert!(effective.datalog);
    }
}
