//! Top-level query IR: the resolved-and-lowered `Query` that flows from
//! parsing through planning, execution, and result formatting.
//!
//! `Query` is the canonical query representation. Its `output` field
//! captures the result-shape decision (SELECT, ASK, CONSTRUCT). `patterns`
//! holds the WHERE clause IR. `grouping` carries the optional aggregation
//! phase (GROUP BY / aggregates / HAVING). `ordering` carries the ORDER BY
//! sort specs. `options` carries the remaining solution modifiers (limit,
//! offset, reasoning configuration). Hydration formatting lives inside the
//! `Column::Hydration` variant on the SELECT projection.

use std::collections::HashSet;

use fluree_graph_json_ld::ParsedContext;

use super::grouping::Grouping;
use super::options::QueryOptions;
use super::pattern::Pattern;
use super::projection::{Column, Projection};
use super::triple::TriplePattern;
use crate::sort::SortSpec;
use crate::var_registry::VarId;

/// Resolved CONSTRUCT template patterns
///
/// Contains the template patterns that will be instantiated with query bindings
/// to produce output triples. Uses the same TriplePattern type as WHERE clause
/// patterns, but variables are resolved against the query result bindings rather
/// than matched against the database.
#[derive(Debug, Clone)]
pub struct ConstructTemplate {
    /// Template patterns (resolved TriplePatterns with Sids and VarIds)
    pub patterns: Vec<TriplePattern>,
}

impl ConstructTemplate {
    /// Create a new construct template from patterns
    pub fn new(patterns: Vec<TriplePattern>) -> Self {
        Self { patterns }
    }

    /// Collect all variables referenced in the template patterns.
    pub fn referenced_vars(&self) -> HashSet<VarId> {
        self.patterns
            .iter()
            .flat_map(TriplePattern::referenced_vars)
            .collect()
    }
}

/// A restriction applied to a SELECT query's result stream.
///
/// The variants are mutually exclusive: a SELECT query is either plain (no
/// restriction), `selectDistinct`, or `selectOne` — never a combination. The
/// parser already enforces this; encoding it as `Option<Restriction>` makes
/// the invariant structural.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Restriction {
    /// Filter duplicates from the result stream (`selectDistinct ...`).
    Distinct,
    /// Return only the first row (`selectOne ...`).
    ///
    /// Distinct from `options.limit = Some(1)`: `One` also changes the output
    /// shape — formatters render a bare row (or null) rather than a one-element
    /// array. `LIMIT 1` caps the result set but keeps the array shape.
    One,
}

/// Describes what the query produces.
#[derive(Debug, Clone)]
pub enum QueryOutput {
    /// SELECT — projects rows from the algebra. The `projection` carries
    /// column structure (and per-column hydration); `restriction` carries the
    /// optional `selectDistinct` / `selectOne` modifier.
    Select {
        projection: Projection,
        restriction: Option<Restriction>,
    },
    /// CONSTRUCT — template patterns instantiated with bindings.
    Construct(ConstructTemplate),
    /// ASK — boolean result.
    Ask,
}

impl QueryOutput {
    /// Construct a plain `Select` from a variable list (`select ?x ?y ...`).
    pub fn select_all(vars: Vec<VarId>) -> Self {
        Self::Select {
            projection: Projection::Tuple(vars.into_iter().map(Column::Var).collect()),
            restriction: None,
        }
    }

    /// Construct a `Select` with `Distinct` restriction (`selectDistinct ?x ...`).
    pub fn select_distinct(vars: Vec<VarId>) -> Self {
        Self::Select {
            projection: Projection::Tuple(vars.into_iter().map(Column::Var).collect()),
            restriction: Some(Restriction::Distinct),
        }
    }

    /// Construct a `Select` with `One` restriction (`selectOne ?x ...`).
    pub fn select_one(vars: Vec<VarId>) -> Self {
        Self::Select {
            projection: Projection::Tuple(vars.into_iter().map(Column::Var).collect()),
            restriction: Some(Restriction::One),
        }
    }

    /// Construct a `Select` with a Wildcard projection (`select *`).
    pub fn wildcard() -> Self {
        Self::Select {
            projection: Projection::Wildcard,
            restriction: None,
        }
    }

    /// The projection of a SELECT output, if any.
    pub fn projection(&self) -> Option<&Projection> {
        match self {
            QueryOutput::Select { projection, .. } => Some(projection),
            _ => None,
        }
    }

    /// The restriction on a SELECT output. `None` for non-SELECT outputs and
    /// for plain SELECT (no modifier).
    fn restriction(&self) -> Option<Restriction> {
        match self {
            QueryOutput::Select { restriction, .. } => *restriction,
            _ => None,
        }
    }

    /// Columns of a SELECT projection. `None` for non-Select outputs;
    /// empty slice for Wildcard.
    pub fn columns(&self) -> Option<&[Column]> {
        self.projection().map(Projection::columns)
    }

    /// Bound variables of the projection in column order.
    /// `None` when projection trimming is not applicable (Wildcard,
    /// Construct, Ask).
    pub fn projected_vars(&self) -> Option<Vec<VarId>> {
        self.projection()?.bound_vars()
    }

    /// Bound select variables, or an empty `Vec` for non-Select outputs.
    pub fn projected_vars_or_empty(&self) -> Vec<VarId> {
        self.projected_vars().unwrap_or_default()
    }

    /// Returns `true` iff rows should be flattened from `[v]` to `v` at
    /// format time. Only fires for the bare-string `select: "?x"` form.
    pub fn should_flatten_scalar(&self) -> bool {
        self.projection().is_some_and(Projection::is_scalar_var)
    }

    /// The construct template, if any.
    pub fn construct_template(&self) -> Option<&ConstructTemplate> {
        match self {
            QueryOutput::Construct(t) => Some(t),
            _ => None,
        }
    }

    /// Returns `true` if the output is a SELECT whose projection contains
    /// any hydration column.
    pub fn has_hydration(&self) -> bool {
        self.projection().is_some_and(Projection::has_hydration)
    }

    /// Returns `true` for `selectDistinct`.
    pub fn is_distinct(&self) -> bool {
        self.restriction() == Some(Restriction::Distinct)
    }

    /// Returns `true` for `selectOne`.
    pub fn is_select_one(&self) -> bool {
        self.restriction() == Some(Restriction::One)
    }

    /// Returns `true` for `SELECT *`.
    pub fn is_wildcard(&self) -> bool {
        self.projection().is_some_and(Projection::is_wildcard)
    }

    /// Returns `true` for `Ask` output.
    pub fn is_ask(&self) -> bool {
        matches!(self, Self::Ask)
    }

    /// Returns `true` for `Construct` output.
    pub fn is_construct(&self) -> bool {
        matches!(self, Self::Construct(_))
    }

    /// Variables this output references from the upstream solution stream.
    ///
    /// Returns `None` when dependency trimming is not applicable:
    /// - `Select` with `Wildcard` projection: all WHERE vars are needed
    /// - `Ask`: all WHERE vars needed for solvability checking
    /// - `Select` with empty projection: no explicit projection
    /// - `Construct` with no template patterns
    pub fn referenced_vars(&self) -> Option<HashSet<VarId>> {
        match self {
            QueryOutput::Ask => None,
            QueryOutput::Select { projection, .. } => {
                let vars = projection.bound_vars()?;
                if vars.is_empty() {
                    None
                } else {
                    Some(vars.into_iter().collect())
                }
            }
            QueryOutput::Construct(t) if t.patterns.is_empty() => None,
            QueryOutput::Construct(t) => Some(t.referenced_vars()),
        }
    }
}

/// Resolved query ready for execution.
///
/// This is the canonical query IR — produced by parsing/lowering, consumed
/// by planning, execution, and result formatting.
#[derive(Debug, Clone)]
pub struct Query {
    /// Parsed JSON-LD context (for result formatting)
    pub context: ParsedContext,
    /// Original JSON context from the query (for CONSTRUCT output)
    pub orig_context: Option<serde_json::Value>,
    /// Query output specification (projection, construct template, ASK, or wildcard).
    pub output: QueryOutput,
    /// Resolved patterns (triples, filters, optionals, etc.)
    pub patterns: Vec<Pattern>,
    /// Optional aggregation phase: GROUP BY + aggregates + HAVING.
    pub grouping: Option<Grouping>,
    /// ORDER BY specs applied after grouping. Empty when the query is
    /// unordered.
    pub ordering: Vec<SortSpec>,
    /// Remaining solution modifiers and reasoning configuration (limit,
    /// offset, reasoning modes, schema bundle).
    pub options: QueryOptions,
    /// Post-query VALUES clause (SPARQL `ValuesClause` after `SolutionModifier`).
    ///
    /// Stored separately from `patterns` so the WHERE-clause planner does not
    /// reorder it relative to OPTIONAL/UNION/etc.  Applied as a final inner-join
    /// constraint after the WHERE operator tree is fully built.
    pub post_values: Option<Pattern>,
}

impl Query {
    /// Create a new query with default Wildcard output.
    pub fn new(context: ParsedContext) -> Self {
        Self {
            context,
            orig_context: None,
            output: QueryOutput::wildcard(),
            patterns: Vec::new(),
            grouping: None,
            ordering: Vec::new(),
            options: QueryOptions::default(),
            post_values: None,
        }
    }

    /// Create a copy of this query with different patterns.
    ///
    /// Used by pattern rewriting (RDFS expansion) to create a query with
    /// expanded patterns while preserving all other query properties.
    pub fn with_patterns(&self, patterns: Vec<Pattern>) -> Self {
        Self {
            context: self.context.clone(),
            orig_context: self.orig_context.clone(),
            output: self.output.clone(),
            patterns,
            grouping: self.grouping.clone(),
            ordering: self.ordering.clone(),
            options: self.options.clone(),
            post_values: self.post_values.clone(),
        }
    }
}
