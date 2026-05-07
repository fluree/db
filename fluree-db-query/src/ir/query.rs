//! Top-level query IR: the resolved-and-lowered `Query` that flows from
//! parsing through planning, execution, and result formatting.
//!
//! `Query` is the canonical query representation. Its `output` field
//! captures the result-shape decision (SELECT, ASK, CONSTRUCT). `patterns`
//! holds the WHERE clause IR. `options` carries solution modifiers (limit,
//! offset, order by, group by, aggregates, having, distinct, ...).
//! Hydration formatting lives inside the `Column::Hydration` variant on
//! the SELECT projection.

use std::collections::HashSet;

use fluree_graph_json_ld::ParsedContext;

use super::options::QueryOptions;
use super::pattern::Pattern;
use super::projection::{Column, HydrationSpec, Projection};
use super::triple::TriplePattern;
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

/// Whether a SELECT returns all solutions or just the first row.
///
/// Distinct from `LIMIT 1`: `One` also changes output shape (formatters
/// return the bare row or null instead of an array of rows).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Multiplicity {
    /// Return all matching solutions (`select`).
    #[default]
    All,
    /// Return only the first solution (`selectOne`).
    One,
}

/// Describes what the query produces.
#[derive(Debug, Clone)]
pub enum QueryOutput {
    /// SELECT — projects rows from the algebra. The `projection` carries
    /// column structure (and per-column hydration); the `multiplicity`
    /// carries the all-vs-first-row distinction.
    Select {
        projection: Projection,
        multiplicity: Multiplicity,
    },
    /// CONSTRUCT — template patterns instantiated with bindings.
    Construct(ConstructTemplate),
    /// ASK — boolean result.
    Ask,
}

impl QueryOutput {
    /// Construct a `Select` from a variable list (Tuple projection,
    /// `All` multiplicity). Used by SPARQL lowering and fixtures.
    pub fn select_all(vars: Vec<VarId>) -> Self {
        Self::Select {
            projection: Projection::Tuple(vars.into_iter().map(Column::Var).collect()),
            multiplicity: Multiplicity::All,
        }
    }

    /// Construct a `Select` from a variable list with `One` multiplicity
    /// (`selectOne`).
    pub fn select_one(vars: Vec<VarId>) -> Self {
        Self::Select {
            projection: Projection::Tuple(vars.into_iter().map(Column::Var).collect()),
            multiplicity: Multiplicity::One,
        }
    }

    /// Construct a `Select` with a Wildcard projection (`SELECT *`).
    pub fn wildcard() -> Self {
        Self::Select {
            projection: Projection::Wildcard,
            multiplicity: Multiplicity::All,
        }
    }

    /// The projection of a SELECT output, if any.
    pub fn projection(&self) -> Option<&Projection> {
        match self {
            QueryOutput::Select { projection, .. } => Some(projection),
            _ => None,
        }
    }

    /// The multiplicity of a SELECT output, if any.
    pub fn multiplicity(&self) -> Option<Multiplicity> {
        match self {
            QueryOutput::Select { multiplicity, .. } => Some(*multiplicity),
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

    /// The hydration spec embedded in the projection, if any.
    pub fn hydration(&self) -> Option<&HydrationSpec> {
        self.projection()?.hydration()
    }

    /// Returns `true` if the output is a SELECT whose projection contains
    /// any hydration column.
    pub fn has_hydration(&self) -> bool {
        self.projection().is_some_and(Projection::has_hydration)
    }

    /// Returns `true` for `selectOne`.
    pub fn is_select_one(&self) -> bool {
        self.multiplicity() == Some(Multiplicity::One)
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
    /// Query options (limit, offset, order by, group by, etc.)
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
            output: QueryOutput::Select {
                projection: Projection::Wildcard,
                multiplicity: Multiplicity::All,
            },
            patterns: Vec::new(),
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
            options: self.options.clone(),
            post_values: self.post_values.clone(),
        }
    }
}
