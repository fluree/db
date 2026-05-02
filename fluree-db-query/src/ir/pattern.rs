//! The `Pattern` enum and the variants that wrap nested `Vec<Pattern>` —
//! `SubqueryPattern`, `ServicePattern`, plus the small `GraphName` /
//! `ServiceEndpoint` types they depend on.
//!
//! Co-located here because they share `Pattern`'s recursive shape and form
//! a structural cycle with it; splitting them out would require sibling
//! modules to refer back into this one.

use std::sync::Arc;

use super::adapters::{
    GeoSearchPattern, IndexSearchPattern, R2rmlPattern, S2SearchPattern, VectorSearchPattern,
};
use super::expression::{Expression, Function};
use super::path::PropertyPathPattern;
use crate::aggregate::AggregateSpec;
use crate::binding::Binding;
use crate::sort::SortSpec;
use super::triple::TriplePattern;
use crate::var_registry::VarId;

/// Resolved subquery pattern
///
/// Represents a nested query within a WHERE clause. The subquery's results
/// are merged with the parent solution on shared variables (correlated join).
///
/// Syntax: `["query", { "select": [...], "where": {...}, ... }]`
#[derive(Debug, Clone)]
pub struct SubqueryPattern {
    /// Variables to select from the subquery (output schema)
    pub select: Vec<VarId>,
    /// WHERE patterns of the subquery
    pub patterns: Vec<Pattern>,
    /// Limit on results (None = unlimited)
    pub limit: Option<usize>,
    /// Offset to skip (None = 0)
    pub offset: Option<usize>,
    /// Whether to apply DISTINCT to results
    pub distinct: bool,
    /// ORDER BY specifications
    pub order_by: Vec<SortSpec>,
    /// GROUP BY variables (for aggregates)
    pub group_by: Vec<VarId>,
    /// Aggregate specifications
    pub aggregates: Vec<AggregateSpec>,
    /// HAVING filter (post-aggregate)
    pub having: Option<Expression>,
}

impl SubqueryPattern {
    /// Create a new subquery pattern
    pub fn new(select: Vec<VarId>, patterns: Vec<Pattern>) -> Self {
        Self {
            select,
            patterns,
            limit: None,
            offset: None,
            distinct: false,
            order_by: Vec::new(),
            group_by: Vec::new(),
            aggregates: Vec::new(),
            having: None,
        }
    }

    /// Set limit
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set offset
    pub fn with_offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Set distinct
    pub fn with_distinct(mut self) -> Self {
        self.distinct = true;
        self
    }

    /// Set ORDER BY specifications
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

    /// Variables the subquery exposes to its enclosing scope (its SELECT list).
    pub fn produced_vars(&self) -> Vec<VarId> {
        self.select.clone()
    }

    /// Variables mentioned anywhere in the subquery, including inside its
    /// body. Body-internal variables that aren't in `select` are still
    /// listed; correlation analysis at the call site can subtract
    /// [`Self::produced_vars`] if it needs only the free variables.
    pub fn referenced_vars(&self) -> Vec<VarId> {
        let mut vars: Vec<VarId> = self
            .patterns
            .iter()
            .flat_map(Pattern::referenced_vars)
            .collect();
        vars.extend(self.select.iter().copied());
        vars
    }

}

// ============================================================================
// Graph Pattern Types
// ============================================================================

/// Graph name in a GRAPH pattern - use strings, not Sids
///
/// Graph names in datasets are ledger aliases/IRIs, not guaranteed
/// to be encodable via any single DB's namespace table.
#[derive(Debug, Clone, PartialEq)]
pub enum GraphName {
    /// Concrete graph IRI (string, not Sid)
    Iri(Arc<str>),
    /// Variable (iterates all named graphs, binds as IRI string)
    Var(VarId),
}

// ============================================================================
// Service Pattern
// ============================================================================

/// A service endpoint - where to execute the inner patterns
///
/// SPARQL: `SERVICE <endpoint> { ... }` or `SERVICE ?var { ... }`
///
/// For local ledger queries, the endpoint IRI should be in the format:
/// `fluree:ledger:<alias>` or `fluree:ledger:<alias>:<branch>`
#[derive(Debug, Clone, PartialEq)]
pub enum ServiceEndpoint {
    /// Concrete service endpoint IRI
    ///
    /// For local ledger queries: `fluree:ledger:mydb:main`
    Iri(Arc<str>),
    /// Variable endpoint (iterates all known services if unbound)
    Var(VarId),
}

/// Service pattern for executing patterns against external or local services.
///
/// SPARQL: `SERVICE <endpoint> { ... }` or `SERVICE SILENT <endpoint> { ... }`
///
/// For local Fluree ledger queries, use the `fluree:ledger:` scheme:
/// - `SERVICE <fluree:ledger:mydb:main> { ?s ?p ?o }`
///
/// # Semantics
///
/// - If the endpoint is an IRI in the `fluree:ledger:` namespace, patterns are
///   executed against that ledger from the current dataset
/// - For external endpoints, the implementation could support SPARQL federation (future)
/// - If `silent` is true, errors from the service are ignored (empty result)
/// - Variable endpoints iterate over available services in the dataset
#[derive(Debug, Clone)]
pub struct ServicePattern {
    /// Whether SERVICE SILENT was specified
    ///
    /// If true, service errors produce empty results instead of query failure.
    pub silent: bool,
    /// The service endpoint (IRI or variable)
    pub endpoint: ServiceEndpoint,
    /// The patterns to execute at the service
    pub patterns: Vec<Pattern>,
    /// Original SPARQL text for the SERVICE body (for remote execution).
    ///
    /// Populated during SPARQL lowering by extracting the source text between
    /// the braces of the SERVICE block. `None` for JSON-LD originated queries.
    /// Used by `ServiceOperator` to send the body verbatim to remote endpoints
    /// without needing an IR-to-SPARQL serializer.
    pub source_body: Option<Arc<str>>,
}

impl ServicePattern {
    /// Create a new SERVICE pattern
    pub fn new(silent: bool, endpoint: ServiceEndpoint, patterns: Vec<Pattern>) -> Self {
        Self {
            silent,
            endpoint,
            patterns,
            source_body: None,
        }
    }

    /// Create a new SERVICE pattern with captured source body text
    pub fn with_source_body(
        silent: bool,
        endpoint: ServiceEndpoint,
        patterns: Vec<Pattern>,
        source_body: Arc<str>,
    ) -> Self {
        Self {
            silent,
            endpoint,
            patterns,
            source_body: Some(source_body),
        }
    }

    /// Variables this service pattern adds to the row's binding set: the
    /// produced bindings of the inner patterns (which the remote endpoint
    /// returns), plus the endpoint variable itself when the endpoint is a
    /// variable (the row gets bound to the iterating service IRI).
    pub fn produced_vars(&self) -> Vec<VarId> {
        let mut vars: Vec<VarId> = self
            .patterns
            .iter()
            .flat_map(Pattern::produced_vars)
            .collect();
        if let ServiceEndpoint::Var(v) = &self.endpoint {
            vars.push(*v);
        }
        vars
    }

    /// Variables mentioned anywhere in this service pattern.
    pub fn referenced_vars(&self) -> Vec<VarId> {
        let mut vars: Vec<VarId> = self
            .patterns
            .iter()
            .flat_map(Pattern::referenced_vars)
            .collect();
        if let ServiceEndpoint::Var(v) = &self.endpoint {
            vars.push(*v);
        }
        vars
    }

}

// ============================================================================
// Pattern Enum
// ============================================================================

/// Logical pattern IR - mirrors where clause structure
///
/// Each variant represents a different pattern type in the query.
/// Ordering is preserved to enable filter inlining at the correct position.
#[derive(Debug, Clone)]
pub enum Pattern {
    /// A basic triple pattern (subject, predicate, object)
    Triple(TriplePattern),

    /// A filter expression to evaluate against each solution
    /// Positioned in where clause order for inline attachment
    Filter(Expression),

    /// Optional clause - left join semantics
    /// Contains ordered patterns that may or may not match
    Optional(Vec<Pattern>),

    /// Union of pattern branches - any branch may match
    Union(Vec<Vec<Pattern>>),

    /// Bind a computed value to a variable
    Bind { var: VarId, expr: Expression },

    /// Inline values - constant rows to join with
    Values {
        vars: Vec<VarId>,
        rows: Vec<Vec<Binding>>,
    },

    /// MINUS clause - anti-join semantics (set difference)
    /// Contains patterns to match; solutions matching these are removed
    Minus(Vec<Pattern>),

    /// EXISTS clause - filter rows where subquery matches
    Exists(Vec<Pattern>),

    /// NOT EXISTS clause - filter rows where subquery does NOT match
    NotExists(Vec<Pattern>),

    /// Property path pattern (transitive traversal)
    PropertyPath(PropertyPathPattern),

    /// Subquery pattern - nested query with result merging
    ///
    /// Executes a nested query and merges results with the parent solution.
    /// Shared variables are correlated (joined on matching values).
    Subquery(SubqueryPattern),

    /// Index search pattern - BM25 full-text search against a graph source
    ///
    /// Queries a graph source (e.g., BM25 index) and produces result bindings.
    IndexSearch(IndexSearchPattern),

    /// Vector search pattern - similarity search against a vector graph source
    ///
    /// Queries a vector index and produces result bindings.
    VectorSearch(VectorSearchPattern),

    /// R2RML scan pattern - queries Iceberg graph source via R2RML mappings
    ///
    /// Scans Iceberg tables through R2RML term maps and produces RDF bindings.
    R2rml(R2rmlPattern),

    /// GeoSearch pattern - proximity search using binary index GeoPoint encoding
    GeoSearch(GeoSearchPattern),

    /// S2 spatial search pattern - complex geometry queries using S2 sidecar index
    S2Search(S2SearchPattern),

    /// Named graph pattern - scopes inner patterns to a specific graph
    ///
    /// SPARQL: `GRAPH <iri> { ... }` or `GRAPH ?g { ... }`
    ///
    /// Semantics:
    /// - `GraphName::Iri(s)`: Execute inner patterns against that specific named graph
    /// - `GraphName::Var(v)`: If bound, use that graph; if unbound, iterate all named
    ///   graphs and bind `?v` to each graph IRI
    ///
    /// Graph-not-found produces empty result (not an error).
    Graph {
        /// The graph name (concrete IRI or variable)
        name: GraphName,
        /// Inner patterns to execute within the graph context
        patterns: Vec<Pattern>,
    },

    /// Service pattern - executes patterns against another ledger or endpoint
    ///
    /// SPARQL: `SERVICE <endpoint> { ... }` or `SERVICE SILENT <endpoint> { ... }`
    ///
    /// For local Fluree ledger queries, use the `fluree:ledger:` scheme:
    /// - `SERVICE <fluree:ledger:orders:main> { ?s :order/total ?total }`
    ///
    /// # Semantics
    ///
    /// - For `fluree:ledger:<alias>` endpoints, patterns are executed against
    ///   the named ledger from the current dataset
    /// - Results are joined with the outer query on shared variables
    /// - If `silent` is true, service errors produce empty results
    Service(ServicePattern),
}

impl Pattern {
    /// Apply `f` once to every immediate nested pattern list inside this
    /// pattern, reconstructing the surrounding container around the result.
    ///
    /// This walks every variant that is structurally a container of
    /// `Vec<Pattern>` — Optional, Minus, Exists, NotExists, Graph, Service,
    /// Subquery — plus each branch of Union (`f` is called per branch). Leaf
    /// variants (Triple, Filter, Bind, Values, PropertyPath, IndexSearch,
    /// VectorSearch, R2rml, GeoSearch, S2Search) pass through unchanged.
    ///
    /// The IR is honest about what's a container; callers decide which
    /// containers their semantics permit recursing into. A site that does
    /// not want to descend into, say, `Pattern::Service` (because the inner
    /// patterns target a remote endpoint) intercepts that variant in its
    /// own match arms before falling through to `map_subpatterns`.
    pub fn map_subpatterns<F>(self, f: &mut F) -> Self
    where
        F: FnMut(Vec<Pattern>) -> Vec<Pattern>,
    {
        match self {
            Pattern::Optional(inner) => Pattern::Optional(f(inner)),
            Pattern::Minus(inner) => Pattern::Minus(f(inner)),
            Pattern::Exists(inner) => Pattern::Exists(f(inner)),
            Pattern::NotExists(inner) => Pattern::NotExists(f(inner)),
            Pattern::Union(branches) => {
                Pattern::Union(branches.into_iter().map(&mut *f).collect())
            }
            Pattern::Graph { name, patterns } => Pattern::Graph {
                name,
                patterns: f(patterns),
            },
            Pattern::Service(sp) => Pattern::Service(ServicePattern {
                patterns: f(sp.patterns),
                ..sp
            }),
            Pattern::Subquery(sp) => Pattern::Subquery(SubqueryPattern {
                patterns: f(sp.patterns),
                ..sp
            }),
            other => other,
        }
    }

    /// Check if this is a triple pattern
    pub fn is_triple(&self) -> bool {
        matches!(self, Pattern::Triple(_))
    }

    /// Get the triple pattern if this is a Triple
    pub fn as_triple(&self) -> Option<&TriplePattern> {
        match self {
            Pattern::Triple(tp) => Some(tp),
            _ => None,
        }
    }

    /// Variables mentioned anywhere in this pattern, recursively. Includes
    /// vars referenced by filters and bind expressions, vars in subquery
    /// bodies (not just their SELECT lists), and vars used as inputs by
    /// search-adapter patterns.
    pub fn referenced_vars(&self) -> Vec<VarId> {
        match self {
            Pattern::Triple(tp) => tp.referenced_vars(),
            Pattern::Filter(expr) => expr.referenced_vars(),
            Pattern::Optional(inner) => inner.iter().flat_map(Pattern::referenced_vars).collect(),
            Pattern::Union(branches) => branches
                .iter()
                .flat_map(|branch| branch.iter().flat_map(Pattern::referenced_vars))
                .collect(),
            Pattern::Bind { var, expr } => {
                let mut vars = expr.referenced_vars();
                vars.push(*var);
                vars
            }
            Pattern::Values { vars, .. } => vars.clone(),
            Pattern::Minus(inner) | Pattern::Exists(inner) | Pattern::NotExists(inner) => {
                inner.iter().flat_map(Pattern::referenced_vars).collect()
            }
            Pattern::PropertyPath(pp) => pp.referenced_vars(),
            Pattern::Subquery(sq) => sq.referenced_vars(),
            Pattern::IndexSearch(isp) => isp.referenced_vars(),
            Pattern::VectorSearch(vsp) => vsp.referenced_vars(),
            Pattern::R2rml(r2rml) => r2rml.referenced_vars(),
            Pattern::GeoSearch(gsp) => gsp.referenced_vars(),
            Pattern::S2Search(s2p) => s2p.referenced_vars(),
            Pattern::Graph { name, patterns } => {
                let mut vars: Vec<VarId> = patterns
                    .iter()
                    .flat_map(Pattern::referenced_vars)
                    .collect();
                if let GraphName::Var(v) = name {
                    vars.push(*v);
                }
                vars
            }
            Pattern::Service(sp) => sp.referenced_vars(),
        }
    }

    /// Variables this pattern adds to the binding set of matched rows.
    ///
    /// Pure filters (`Filter`, `Minus`, `Exists`, `NotExists`) return the
    /// empty set — they remove rows but don't introduce bindings. `Bind`
    /// produces only its target variable, not the variables its expression
    /// reads. `Subquery` exposes only its SELECT list outward, not body
    /// internals.
    pub fn produced_vars(&self) -> Vec<VarId> {
        match self {
            Pattern::Triple(tp) => tp.produced_vars(),
            Pattern::Filter(_) => Vec::new(),
            Pattern::Optional(inner) => inner.iter().flat_map(Pattern::produced_vars).collect(),
            Pattern::Union(branches) => branches
                .iter()
                .flat_map(|branch| branch.iter().flat_map(Pattern::produced_vars))
                .collect(),
            Pattern::Bind { var, .. } => vec![*var],
            Pattern::Values { vars, .. } => vars.clone(),
            Pattern::Minus(_) | Pattern::Exists(_) | Pattern::NotExists(_) => Vec::new(),
            Pattern::PropertyPath(pp) => pp.produced_vars(),
            Pattern::Subquery(sq) => sq.produced_vars(),
            Pattern::IndexSearch(isp) => isp.produced_vars(),
            Pattern::VectorSearch(vsp) => vsp.produced_vars(),
            Pattern::R2rml(r2rml) => r2rml.produced_vars(),
            Pattern::GeoSearch(gsp) => gsp.produced_vars(),
            Pattern::S2Search(s2p) => s2p.produced_vars(),
            Pattern::Graph { name, patterns } => {
                let mut vars: Vec<VarId> = patterns
                    .iter()
                    .flat_map(Pattern::produced_vars)
                    .collect();
                if let GraphName::Var(v) = name {
                    vars.push(*v);
                }
                vars
            }
            Pattern::Service(sp) => sp.produced_vars(),
        }
    }

    /// True if any expression inside this pattern (recursively, through any
    /// nested patterns) calls `target`. Mirrors [`Expression::contains_function`]
    /// at the pattern level.
    pub fn contains_function(&self, target: &Function) -> bool {
        match self {
            Pattern::Filter(expr) => expr.contains_function(target),
            Pattern::Bind { expr, .. } => expr.contains_function(target),
            Pattern::Exists(inner) | Pattern::NotExists(inner) | Pattern::Minus(inner) => {
                inner.iter().any(|p| p.contains_function(target))
            }
            Pattern::Optional(inner) => inner.iter().any(|p| p.contains_function(target)),
            Pattern::Union(branches) => branches
                .iter()
                .any(|branch| branch.iter().any(|p| p.contains_function(target))),
            Pattern::Graph { patterns, .. } => {
                patterns.iter().any(|p| p.contains_function(target))
            }
            Pattern::Subquery(sq) => sq.patterns.iter().any(|p| p.contains_function(target)),
            // Other pattern variants cannot contain general expressions.
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::triple::{Ref, Term};
    use fluree_db_core::Sid;

    fn test_pattern() -> TriplePattern {
        TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(Sid::new(100, "name")),
            Term::Var(VarId(1)),
        )
    }

    #[test]
    fn test_pattern_variables() {
        let pattern = Pattern::Triple(test_pattern());
        let vars = pattern.referenced_vars();
        assert_eq!(vars.len(), 2);
        assert!(vars.contains(&VarId(0)));
        assert!(vars.contains(&VarId(1)));
    }
}
