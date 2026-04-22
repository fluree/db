//! Query execution options
//!
//! This module contains QueryOptions, shared by both parse and execute modules.
//! It lives in a neutral location to avoid circular dependencies.

use crate::aggregate::AggregateSpec;
use crate::ir::Expression;
use crate::rewrite::ReasoningModes;
use crate::sort::SortSpec;
use crate::var_registry::VarId;

/// Options for query execution modifiers
///
/// Controls GROUP BY, HAVING, ORDER BY, DISTINCT, OFFSET, LIMIT, and reasoning behavior.
/// This type is shared between the parse module (embedded in ParsedQuery) and
/// the execute module (used by ExecutableQuery).
#[derive(Debug, Clone, Default)]
pub struct QueryOptions {
    /// Maximum rows to return (applied last)
    pub limit: Option<usize>,
    /// Rows to skip before returning results
    pub offset: Option<usize>,
    /// Whether to deduplicate results
    pub distinct: bool,
    /// Sort specifications (applied before projection)
    pub order_by: Vec<SortSpec>,
    /// GROUP BY variables (applied after WHERE, before aggregates)
    pub group_by: Vec<VarId>,
    /// Aggregate specifications (applied after GROUP BY)
    pub aggregates: Vec<AggregateSpec>,
    /// HAVING filter expression (applied after aggregates)
    pub having: Option<Expression>,
    /// Post-aggregation BIND expressions (applied after HAVING)
    pub post_binds: Vec<(VarId, Expression)>,
    /// Reasoning modes for RDFS/OWL reasoning
    ///
    /// Controls pattern expansion based on class/property hierarchies.
    /// Default is to auto-enable RDFS when hierarchy exists.
    ///
    /// Use `reasoning.effective_with_hierarchy(has_hierarchy)` at execution
    /// time to compute the actual modes to apply.
    pub reasoning: ReasoningModes,
}

impl QueryOptions {
    /// Create new execution options with defaults
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the limit
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set the offset
    pub fn with_offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Enable distinct
    pub fn with_distinct(mut self) -> Self {
        self.distinct = true;
        self
    }

    /// Set order by specifications
    pub fn with_order_by(mut self, specs: Vec<SortSpec>) -> Self {
        self.order_by = specs;
        self
    }

    /// Set GROUP BY variables
    pub fn with_group_by(mut self, vars: Vec<VarId>) -> Self {
        self.group_by = vars;
        self
    }

    /// Set aggregate specifications
    pub fn with_aggregates(mut self, specs: Vec<AggregateSpec>) -> Self {
        self.aggregates = specs;
        self
    }

    /// Set HAVING filter expression
    pub fn with_having(mut self, expr: Expression) -> Self {
        self.having = Some(expr);
        self
    }

    /// Set reasoning modes
    ///
    /// # Example
    ///
    /// ```
    /// use fluree_db_query::options::QueryOptions;
    /// use fluree_db_query::rewrite::ReasoningModes;
    ///
    /// let opts = QueryOptions::new()
    ///     .with_reasoning(ReasoningModes::rdfs().with_owl2ql());
    /// assert!(opts.reasoning.rdfs);
    /// assert!(opts.reasoning.owl2ql);
    /// ```
    pub fn with_reasoning(mut self, modes: ReasoningModes) -> Self {
        self.reasoning = modes;
        self
    }

    /// Check if any modifiers are set
    pub fn has_modifiers(&self) -> bool {
        self.limit.is_some()
            || self.offset.is_some()
            || self.distinct
            || !self.order_by.is_empty()
            || !self.group_by.is_empty()
            || !self.aggregates.is_empty()
            || self.having.is_some()
            || !self.post_binds.is_empty()
    }

    /// Check if any reasoning mode is explicitly enabled
    pub fn has_reasoning(&self) -> bool {
        self.reasoning.has_any_enabled()
    }

    /// Check if reasoning is explicitly disabled
    pub fn is_reasoning_disabled(&self) -> bool {
        self.reasoning.is_disabled()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_options() {
        let opts = QueryOptions::default();
        assert!(opts.limit.is_none());
        assert!(opts.offset.is_none());
        assert!(!opts.distinct);
        assert!(opts.order_by.is_empty());
        assert!(opts.group_by.is_empty());
        assert!(opts.aggregates.is_empty());
        assert!(opts.having.is_none());
        // Default reasoning: nothing explicitly enabled
        assert!(!opts.has_reasoning());
        assert!(!opts.is_reasoning_disabled());
        assert!(!opts.has_modifiers());
    }

    #[test]
    fn test_builder_pattern() {
        let opts = QueryOptions::new()
            .with_limit(10)
            .with_offset(5)
            .with_distinct();

        assert_eq!(opts.limit, Some(10));
        assert_eq!(opts.offset, Some(5));
        assert!(opts.distinct);
        assert!(opts.has_modifiers());
    }

    #[test]
    fn test_has_modifiers() {
        assert!(!QueryOptions::new().has_modifiers());
        assert!(QueryOptions::new().with_limit(1).has_modifiers());
        assert!(QueryOptions::new().with_offset(1).has_modifiers());
        assert!(QueryOptions::new().with_distinct().has_modifiers());
    }

    #[test]
    fn test_reasoning_modes() {
        // Default: no explicit reasoning
        let opts = QueryOptions::new();
        assert!(!opts.has_reasoning());
        assert!(!opts.is_reasoning_disabled());

        // With RDFS
        let opts = QueryOptions::new().with_reasoning(ReasoningModes::rdfs());
        assert!(opts.has_reasoning());
        assert!(opts.reasoning.rdfs);
        assert!(!opts.reasoning.owl2ql);

        // With OWL2-QL (includes RDFS)
        let opts = QueryOptions::new().with_reasoning(ReasoningModes::owl2ql());
        assert!(opts.has_reasoning());
        assert!(opts.reasoning.rdfs);
        assert!(opts.reasoning.owl2ql);

        // Explicit none
        let opts = QueryOptions::new().with_reasoning(ReasoningModes::none());
        assert!(!opts.has_reasoning());
        assert!(opts.is_reasoning_disabled());
    }
}
