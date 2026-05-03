//! Top-level query IR: the resolved-and-lowered `Query` that flows from
//! parsing through planning, execution, and result formatting.
//!
//! `Query` is the canonical query representation. Its `output` field
//! captures the result-shape decision (SELECT selections, SELECT-one,
//! wildcard, ASK, CONSTRUCT). `patterns` holds the WHERE clause IR.
//! `options` carries solution modifiers (limit, offset, order by, group
//! by, aggregates, having, distinct, ...). Hydration formatting lives
//! inside the `Selection::Hydration` variant on the SELECT outputs.

use std::collections::HashSet;

use fluree_graph_json_ld::ParsedContext;

use super::options::QueryOptions;
use super::pattern::Pattern;
use super::projection::{HydrationSpec, Selection};
use super::triple::TriplePattern;
use crate::var_registry::VarId;

/// Per-row projection shape for tabular output formatters.
///
/// Formatters that emit row arrays (jsonld, delimited, etc.) consult this to
/// decide scalar-vs-tuple rendering. Object-based formatters (typed, agent_json)
/// ignore it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProjectionShape {
    /// Every row is an array, regardless of arity. Default; used by SPARQL
    /// and JSON-LD array-form select.
    #[default]
    Tuple,
    /// 1-var rows flatten to scalars. JSON-LD bare-string `select: "?x"`.
    Scalar,
}

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

/// Describes what the query produces.
///
/// Combines the select mode, selections, and construct template into a
/// single enum so that invalid combinations (e.g. `Construct` without a
/// template) are unrepresentable.
#[derive(Debug, Clone)]
pub enum QueryOutput {
    /// Normal SELECT with explicit selection list and projection shape.
    Select {
        selections: Vec<Selection>,
        shape: ProjectionShape,
    },
    /// selectOne — same as Select but formatters return first row or null.
    SelectOne {
        selections: Vec<Selection>,
        shape: ProjectionShape,
    },
    /// SELECT * — all bound variables from WHERE.
    Wildcard,
    /// CONSTRUCT — template patterns instantiated with bindings.
    Construct(ConstructTemplate),
    /// ASK — boolean result.
    Boolean,
}

impl QueryOutput {
    /// Construct a `Select` from a variable list with the default (`Tuple`) shape.
    ///
    /// Used by SPARQL lowering and internal fixtures. JSON-LD bare-string
    /// `select: "?x"` builds the struct variant directly with `shape: Scalar`.
    pub fn select(vars: Vec<VarId>) -> Self {
        Self::Select {
            selections: vars.into_iter().map(Selection::Var).collect(),
            shape: ProjectionShape::Tuple,
        }
    }

    /// Construct a `SelectOne` from a variable list with the default (`Tuple`) shape.
    pub fn select_one(vars: Vec<VarId>) -> Self {
        Self::SelectOne {
            selections: vars.into_iter().map(Selection::Var).collect(),
            shape: ProjectionShape::Tuple,
        }
    }

    /// Get the raw selections for Select/SelectOne, `None` otherwise.
    pub fn selections(&self) -> Option<&[Selection]> {
        match self {
            QueryOutput::Select { selections, .. } | QueryOutput::SelectOne { selections, .. } => {
                Some(selections)
            }
            _ => None,
        }
    }

    /// Iterator over the bound variables of each selection in column order.
    ///
    /// `Selection::Var` yields its variable; `Selection::Hydration` yields its
    /// root variable when the root is a variable (constant-rooted expansions
    /// bind no row column and are skipped).
    pub fn select_var_iter(&self) -> impl Iterator<Item = VarId> + '_ {
        self.selections()
            .into_iter()
            .flatten()
            .filter_map(Selection::bound_var)
    }

    /// Collect bound select variables in column order, `None` for non-select outputs.
    pub fn select_vars(&self) -> Option<Vec<VarId>> {
        self.selections().map(|sels| {
            sels.iter()
                .filter_map(Selection::bound_var)
                .collect::<Vec<_>>()
        })
    }

    /// Collect bound select variables, or an empty `Vec` for non-select outputs.
    pub fn select_vars_or_empty(&self) -> Vec<VarId> {
        self.select_vars().unwrap_or_default()
    }

    /// Get the projection shape for Select/SelectOne, `None` otherwise.
    pub fn projection_shape(&self) -> Option<ProjectionShape> {
        match self {
            QueryOutput::Select { shape, .. } | QueryOutput::SelectOne { shape, .. } => {
                Some(*shape)
            }
            _ => None,
        }
    }

    /// Returns `true` iff rows should be flattened from `[v]` to `v` at format
    /// time. True only when the user opted into scalar output via JSON-LD
    /// `select: "?x"` (bare-string form) AND there is exactly one projected
    /// variable selection (hydrationions never flatten).
    pub fn should_flatten_scalar(&self) -> bool {
        match self {
            QueryOutput::Select { selections, shape }
            | QueryOutput::SelectOne { selections, shape } => {
                *shape == ProjectionShape::Scalar
                    && selections.len() == 1
                    && matches!(selections[0], Selection::Var(_))
            }
            _ => false,
        }
    }

    /// Get the construct template for Construct, `None` otherwise.
    pub fn construct_template(&self) -> Option<&ConstructTemplate> {
        match self {
            QueryOutput::Construct(t) => Some(t),
            _ => None,
        }
    }

    /// Returns the hydration spec embedded in the selections, if any.
    ///
    /// A query carries at most one hydration selection (enforced by the
    /// parser). Non-Select/SelectOne outputs return `None`.
    pub fn hydration(&self) -> Option<&HydrationSpec> {
        self.selections()?
            .iter()
            .find_map(Selection::as_hydration)
    }

    /// Returns `true` for `SelectOne` output.
    pub fn is_select_one(&self) -> bool {
        matches!(self, Self::SelectOne { .. })
    }

    /// Returns `true` for `Wildcard` output.
    pub fn is_wildcard(&self) -> bool {
        matches!(self, Self::Wildcard)
    }

    /// Returns `true` for `Boolean` (ASK) output.
    pub fn is_boolean(&self) -> bool {
        matches!(self, Self::Boolean)
    }

    /// Returns `true` for `Construct` output.
    pub fn is_construct(&self) -> bool {
        matches!(self, Self::Construct(_))
    }

    /// Variables the output depends on.
    ///
    /// Returns `None` when dependency trimming is not applicable:
    /// - `Wildcard`: all WHERE vars are needed
    /// - `Boolean`: all WHERE vars needed for solvability checking
    /// - Empty `Select`/`SelectOne`: no explicit projection
    /// - `Construct` with no template patterns
    pub fn variables(&self) -> Option<HashSet<VarId>> {
        match self {
            QueryOutput::Wildcard | QueryOutput::Boolean => None,
            QueryOutput::Select { selections, .. } | QueryOutput::SelectOne { selections, .. }
                if selections.is_empty() =>
            {
                None
            }
            QueryOutput::Select { selections, .. } | QueryOutput::SelectOne { selections, .. } => {
                Some(selections.iter().filter_map(Selection::bound_var).collect())
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
    /// Query output specification (selections, construct template, or boolean/wildcard mode)
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
            output: QueryOutput::Wildcard,
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
