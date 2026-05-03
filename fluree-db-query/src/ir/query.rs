//! Top-level query IR: the resolved-and-lowered `Query` that flows from
//! parsing through planning, execution, and result formatting.
//!
//! `Query` is the canonical query representation. Its `output` field
//! captures the result-shape decision (SELECT vars, SELECT-one, wildcard,
//! ASK, CONSTRUCT). `patterns` holds the WHERE clause IR. `options`
//! carries solution modifiers (limit, offset, order by, group by,
//! aggregates, having, distinct, ...). `graph_select` carries a graph
//! crawl spec when the result format is nested JSON-LD rather than tabular.

use std::collections::HashSet;

use fluree_graph_json_ld::ParsedContext;

use super::options::QueryOptions;
use super::pattern::Pattern;
use super::projection::GraphSelectSpec;
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
/// Combines the select mode, selected variables, and construct template into a
/// single enum so that invalid combinations (e.g. `Construct` without a
/// template, or `Many` with an empty variable list) are unrepresentable.
#[derive(Debug, Clone)]
pub enum QueryOutput {
    /// Normal SELECT with explicit variable list and projection shape.
    Select {
        vars: Vec<VarId>,
        shape: ProjectionShape,
    },
    /// selectOne — same as Select but formatters return first row or null.
    SelectOne {
        vars: Vec<VarId>,
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
    /// Construct a `Select` with the default (`Tuple`) shape.
    ///
    /// Used by SPARQL lowering and internal fixtures. JSON-LD bare-string
    /// `select: "?x"` builds the struct variant directly with `shape: Scalar`.
    pub fn select(vars: Vec<VarId>) -> Self {
        Self::Select {
            vars,
            shape: ProjectionShape::Tuple,
        }
    }

    /// Construct a `SelectOne` with the default (`Tuple`) shape.
    pub fn select_one(vars: Vec<VarId>) -> Self {
        Self::SelectOne {
            vars,
            shape: ProjectionShape::Tuple,
        }
    }

    /// Get select vars for Select/SelectOne, `None` otherwise.
    pub fn select_vars(&self) -> Option<&[VarId]> {
        match self {
            QueryOutput::Select { vars, .. } | QueryOutput::SelectOne { vars, .. } => Some(vars),
            _ => None,
        }
    }

    /// Get select vars, or an empty slice for non-select outputs.
    pub fn select_vars_or_empty(&self) -> &[VarId] {
        self.select_vars().unwrap_or(&[])
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
    /// variable. Wildcard, Construct, Boolean, and `Tuple`-shaped Select all
    /// return `false`, so tabular output (SPARQL + JSON-LD array-form select)
    /// is preserved.
    pub fn should_flatten_scalar(&self) -> bool {
        match self {
            QueryOutput::Select { vars, shape } | QueryOutput::SelectOne { vars, shape } => {
                *shape == ProjectionShape::Scalar && vars.len() == 1
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
            QueryOutput::Select { vars, .. } | QueryOutput::SelectOne { vars, .. }
                if vars.is_empty() =>
            {
                None
            }
            QueryOutput::Select { vars, .. } | QueryOutput::SelectOne { vars, .. } => {
                Some(vars.iter().copied().collect())
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
    /// Query output specification (replaces select, select_mode, construct_template)
    pub output: QueryOutput,
    /// Resolved patterns (triples, filters, optionals, etc.)
    pub patterns: Vec<Pattern>,
    /// Query options (limit, offset, order by, group by, etc.)
    pub options: QueryOptions,
    /// Graph crawl select specification (None for flat SELECT or CONSTRUCT)
    ///
    /// When present, controls nested JSON-LD object expansion during formatting.
    pub graph_select: Option<GraphSelectSpec>,
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
            graph_select: None,
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
            graph_select: self.graph_select.clone(),
            post_values: self.post_values.clone(),
        }
    }
}
