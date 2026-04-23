//! AST to IR lowering
//!
//! Converts unresolved AST types (with string IRIs) to resolved IR types
//! (with Sids and VarIds) using an IriEncoder.

use super::ast::{
    LiteralValue, UnresolvedAggregateFn, UnresolvedAggregateSpec, UnresolvedConstructTemplate,
    UnresolvedDatatypeConstraint, UnresolvedExpression, UnresolvedGraphSelectSpec,
    UnresolvedNestedSelectSpec, UnresolvedOptions, UnresolvedPathExpr, UnresolvedPattern,
    UnresolvedQuery, UnresolvedRoot, UnresolvedSelectionSpec, UnresolvedSortDirection,
    UnresolvedSortSpec, UnresolvedTerm, UnresolvedTriplePattern, UnresolvedValue,
};
use super::encode::{IriEncoder, NoEncoder};
use super::error::{ParseError, Result};
use crate::aggregate::{AggregateFn, AggregateSpec};
use crate::binding::Binding;
use crate::context::WellKnownDatatypes;
use crate::ir::{
    Expression, Function, IndexSearchPattern, IndexSearchTarget, PathModifier, Pattern,
    PropertyPathPattern, SubqueryPattern, VectorSearchPattern, VectorSearchTarget,
};
use crate::vector::DistanceMetric;
// Re-export graph select types for external use
pub use crate::ir::{GraphSelectSpec, NestedSelectSpec, Root, SelectionSpec};
use crate::options::QueryOptions;
use crate::sort::{SortDirection, SortSpec};
use crate::triple::{Ref, Term, TriplePattern};
use crate::var_registry::{VarId, VarRegistry};
use fluree_db_core::DatatypeConstraint;
use fluree_db_core::{FlakeValue, Sid};
use fluree_graph_json_ld::ParsedContext;
use std::collections::HashSet;
use std::sync::Arc;

/// Projection shape — how each row should be rendered by row-array formatters.
///
/// - `Tuple` (default): every row is an array regardless of arity. This is
///   the spec-correct shape for SPARQL (solution sequences are tabular) and
///   for JSON-LD `select: ["?x"]` / `["?x","?y"]` (the user's array wrapper
///   is preserved end-to-end).
/// - `Scalar`: 1-var rows flatten to the bare value. JSON-LD sets this only
///   when the user writes `select: "?x"` (bare string) — an explicit opt-in
///   to scalar shape. Multi-var `Scalar` is unreachable from the parser.
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

/// Select mode determines result shape
///
/// This is derived from the parsed query (select vs selectOne vs construct) and controls
/// whether the formatter returns an array, single value, or JSON-LD graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum SelectMode {
    /// Normal select: return array of rows
    #[default]
    Many,

    /// selectOne: return first row or null
    One,

    /// Wildcard (*): return all bound variables as object
    ///
    /// Uses `batch.schema()` to get all variables, not just select.
    /// Omits unbound/poisoned variables from output.
    Wildcard,

    /// CONSTRUCT: return JSON-LD graph `{"@context": ..., "@graph": [...]}`
    ///
    /// Template patterns are instantiated with bindings to produce RDF triples.
    /// Projection is skipped (all bindings needed for templating).
    Construct,

    /// ASK: return boolean based on whether any solution exists
    ///
    /// No variables are projected. LIMIT 1 is applied internally for efficiency.
    /// Result format: `{"head": {}, "boolean": true|false}`
    Boolean,
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
    pub fn variables(&self) -> HashSet<VarId> {
        self.patterns
            .iter()
            .flat_map(super::super::triple::TriplePattern::variables)
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
            QueryOutput::Construct(t) => Some(t.variables()),
        }
    }
}

/// Resolved query ready for execution
#[derive(Debug, Clone)]
pub struct ParsedQuery {
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

impl ParsedQuery {
    /// Create a new parsed query with default Wildcard output.
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

    /// Get all triple patterns (flattening nested structures)
    pub fn triple_patterns(&self) -> Vec<&TriplePattern> {
        fn collect<'a>(patterns: &'a [Pattern], out: &mut Vec<&'a TriplePattern>) {
            for p in patterns {
                match p {
                    Pattern::Triple(tp) => out.push(tp),
                    Pattern::Optional(inner)
                    | Pattern::Minus(inner)
                    | Pattern::Exists(inner)
                    | Pattern::NotExists(inner) => collect(inner, out),
                    Pattern::Union(branches) => {
                        for branch in branches {
                            collect(branch, out);
                        }
                    }
                    Pattern::Graph {
                        patterns: inner, ..
                    } => collect(inner, out),
                    Pattern::Service(sp) => collect(&sp.patterns, out),
                    Pattern::Filter(_)
                    | Pattern::Bind { .. }
                    | Pattern::Values { .. }
                    | Pattern::PropertyPath(_)
                    | Pattern::Subquery(_)
                    | Pattern::IndexSearch(_)
                    | Pattern::GeoSearch(_)
                    | Pattern::S2Search(_)
                    | Pattern::VectorSearch(_)
                    | Pattern::R2rml(_) => {}
                }
            }
        }

        let mut result = Vec::new();
        collect(&self.patterns, &mut result);
        result
    }

    /// Create a copy of this query with different patterns
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

/// Lower an unresolved query to a resolved ParsedQuery
///
/// # Arguments
///
/// * `ast` - The unresolved query AST
/// * `encoder` - IRI encoder for converting IRIs to Sids
/// * `vars` - Variable registry (caller provides to enable sharing across subqueries)
/// * `select_mode` - Result shaping mode (Many, One, or Wildcard)
///
/// # Returns
///
/// A resolved `ParsedQuery` with Sids and VarIds
pub(crate) fn lower_query<E: IriEncoder>(
    ast: UnresolvedQuery,
    encoder: &E,
    vars: &mut VarRegistry,
    select_mode: SelectMode,
) -> Result<ParsedQuery> {
    let mut pp_counter: u32 = 0;

    // Lower select variables
    let select_vars: Vec<VarId> = ast
        .select
        .iter()
        .map(|name| vars.get_or_insert(name))
        .collect();

    // Lower patterns
    let mut patterns = Vec::new();
    for unresolved_pattern in ast.patterns {
        let lowered =
            lower_unresolved_pattern(&unresolved_pattern, encoder, vars, &mut pp_counter)?;
        patterns.extend(lowered);
    }

    // Lower options
    let options = lower_options(&ast.options, vars)?;

    // Build QueryOutput from mode + lowered components
    let shape = ast.select_shape;
    let output = match select_mode {
        SelectMode::Many => QueryOutput::Select {
            vars: select_vars,
            shape,
        },
        SelectMode::One => QueryOutput::SelectOne {
            vars: select_vars,
            shape,
        },
        SelectMode::Wildcard => QueryOutput::Wildcard,
        SelectMode::Construct => {
            let template = match ast.construct_template {
                Some(ref t) => lower_construct_template(t, encoder, vars)?,
                None => ConstructTemplate::new(Vec::new()),
            };
            QueryOutput::Construct(template)
        }
        SelectMode::Boolean => QueryOutput::Boolean,
    };

    // Lower graph select if present
    let graph_select = ast
        .graph_select
        .as_ref()
        .map(|gs| lower_graph_select(gs, encoder, vars))
        .transpose()?;

    Ok(ParsedQuery {
        context: ast.context,
        orig_context: ast.orig_context,
        output,
        patterns,
        options,
        graph_select,
        post_values: None,
    })
}

/// Lower an unresolved pattern to resolved Pattern(s).
///
/// This converts string IRIs to encoded Sids using the provided encoder.
/// Returns a `Vec<Pattern>` because some patterns (e.g., sequence property paths)
/// expand into multiple triple patterns joined by intermediate variables.
///
/// Also used by fluree-db-transact for lowering WHERE clause patterns.
pub fn lower_unresolved_pattern<E: IriEncoder>(
    pattern: &UnresolvedPattern,
    encoder: &E,
    vars: &mut VarRegistry,
    pp_counter: &mut u32,
) -> Result<Vec<Pattern>> {
    match pattern {
        UnresolvedPattern::Triple(tp) => {
            let lowered = lower_triple_pattern(tp, encoder, vars)?;
            Ok(vec![Pattern::Triple(lowered)])
        }
        UnresolvedPattern::Filter(expr) => {
            let lowered = lower_filter_expr_with_encoder(expr, vars, encoder, pp_counter)?;
            Ok(vec![Pattern::Filter(lowered)])
        }
        UnresolvedPattern::Optional(inner) => {
            let lowered = lower_unresolved_patterns(inner, encoder, vars, pp_counter)?;
            Ok(vec![Pattern::Optional(lowered)])
        }
        UnresolvedPattern::Union(branches) => {
            let lowered_branches: Result<Vec<Vec<Pattern>>> = branches
                .iter()
                .map(|branch| lower_unresolved_patterns(branch, encoder, vars, pp_counter))
                .collect();
            Ok(vec![Pattern::Union(lowered_branches?)])
        }
        UnresolvedPattern::Bind { var, expr } => {
            let var_id = vars.get_or_insert(var);
            let lowered_expr = lower_filter_expr_with_encoder(expr, vars, encoder, pp_counter)?;
            Ok(vec![Pattern::Bind {
                var: var_id,
                expr: lowered_expr,
            }])
        }
        UnresolvedPattern::Values { vars: v, rows } => {
            let var_ids: Vec<VarId> = v.iter().map(|name| vars.get_or_insert(name)).collect();
            let rows: Result<Vec<Vec<Binding>>> = rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|cell| lower_values_cell(cell, encoder))
                        .collect::<Result<Vec<_>>>()
                })
                .collect();
            Ok(vec![Pattern::Values {
                vars: var_ids,
                rows: rows?,
            }])
        }
        UnresolvedPattern::Minus(inner) => {
            let lowered = lower_unresolved_patterns(inner, encoder, vars, pp_counter)?;
            Ok(vec![Pattern::Minus(lowered)])
        }
        UnresolvedPattern::Exists(inner) => {
            let lowered = lower_unresolved_patterns(inner, encoder, vars, pp_counter)?;
            Ok(vec![Pattern::Exists(lowered)])
        }
        UnresolvedPattern::NotExists(inner) => {
            let lowered = lower_unresolved_patterns(inner, encoder, vars, pp_counter)?;
            Ok(vec![Pattern::NotExists(lowered)])
        }
        UnresolvedPattern::Path {
            subject,
            path,
            object,
        } => lower_path_to_patterns(subject, path, object, encoder, vars, pp_counter),
        UnresolvedPattern::Subquery(subquery) => {
            let lowered = lower_subquery(subquery, encoder, vars, pp_counter)?;
            Ok(vec![Pattern::Subquery(lowered)])
        }
        UnresolvedPattern::IndexSearch(isp) => {
            // Lower index search (BM25 / graph source search) pattern.
            let graph_source_id = isp.graph_source_id.as_ref().to_string();

            let target = match &isp.target {
                super::ast::UnresolvedIndexSearchTarget::Const(s) => {
                    IndexSearchTarget::Const(s.as_ref().to_string())
                }
                super::ast::UnresolvedIndexSearchTarget::Var(v) => {
                    IndexSearchTarget::Var(vars.get_or_insert(v))
                }
            };

            let id_var = vars.get_or_insert(&isp.id_var);
            let mut pat = IndexSearchPattern::new(graph_source_id, target, id_var);

            pat.limit = isp.limit;
            pat.score_var = isp.score_var.as_ref().map(|v| vars.get_or_insert(v));
            pat.ledger_var = isp.ledger_var.as_ref().map(|v| vars.get_or_insert(v));
            pat.sync = isp.sync;
            pat.timeout = isp.timeout;

            Ok(vec![Pattern::IndexSearch(pat)])
        }
        UnresolvedPattern::VectorSearch(vsp) => {
            // Lower vector search (similarity search) pattern.
            let graph_source_id = vsp.graph_source_id.as_ref().to_string();

            let target = match &vsp.target {
                super::ast::UnresolvedVectorSearchTarget::Const(v) => {
                    VectorSearchTarget::Const(v.clone())
                }
                super::ast::UnresolvedVectorSearchTarget::Var(v) => {
                    VectorSearchTarget::Var(vars.get_or_insert(v))
                }
            };

            let metric = DistanceMetric::parse(&vsp.metric).unwrap_or(DistanceMetric::Cosine);

            let id_var = vars.get_or_insert(&vsp.id_var);
            let mut pat =
                VectorSearchPattern::new(graph_source_id, target, id_var).with_metric(metric);

            if let Some(limit) = vsp.limit {
                pat = pat.with_limit(limit);
            }
            if let Some(sv) = &vsp.score_var {
                pat = pat.with_score_var(vars.get_or_insert(sv));
            }
            if let Some(lv) = &vsp.ledger_var {
                pat = pat.with_ledger_var(vars.get_or_insert(lv));
            }
            if vsp.sync {
                pat = pat.with_sync(true);
            }
            if let Some(t) = vsp.timeout {
                pat = pat.with_timeout(t);
            }

            Ok(vec![Pattern::VectorSearch(pat)])
        }
        UnresolvedPattern::Graph { name, patterns } => {
            // Lower GRAPH pattern - scope inner patterns to a named graph
            use crate::ir::GraphName;
            use std::sync::Arc;

            let ir_name = if name.starts_with('?') {
                // Variable graph name
                let var_id = vars.get_or_insert(name);
                GraphName::Var(var_id)
            } else {
                // Concrete graph IRI (kept as string, not encoded)
                GraphName::Iri(Arc::from(name.as_ref()))
            };

            let lowered_patterns = lower_unresolved_patterns(patterns, encoder, vars, pp_counter)?;

            Ok(vec![Pattern::Graph {
                name: ir_name,
                patterns: lowered_patterns,
            }])
        }
    }
}

/// Lower a slice of unresolved patterns to a flat `Vec<Pattern>`.
///
/// Handles the flattening needed when individual patterns may expand into
/// multiple resolved patterns (e.g., sequence property paths).
pub fn lower_unresolved_patterns<E: IriEncoder>(
    patterns: &[UnresolvedPattern],
    encoder: &E,
    vars: &mut VarRegistry,
    pp_counter: &mut u32,
) -> Result<Vec<Pattern>> {
    let mut result = Vec::new();
    for p in patterns {
        result.extend(lower_unresolved_pattern(p, encoder, vars, pp_counter)?);
    }
    Ok(result)
}

fn lower_values_cell<E: IriEncoder>(cell: &UnresolvedValue, encoder: &E) -> Result<Binding> {
    let dts = WellKnownDatatypes::new();
    match cell {
        UnresolvedValue::Unbound => Ok(Binding::Unbound),
        UnresolvedValue::Iri(iri) => {
            let sid = encoder
                .encode_iri(iri)
                .ok_or_else(|| ParseError::UnknownNamespace(iri.to_string()))?;
            Ok(Binding::Sid(sid))
        }
        UnresolvedValue::Literal { value, dtc } => {
            // Build initial FlakeValue from the literal
            let initial_fv = match value {
                LiteralValue::String(s) => FlakeValue::String(s.as_ref().to_string()),
                LiteralValue::Long(i) => FlakeValue::Long(*i),
                LiteralValue::Double(f) => FlakeValue::Double(*f),
                LiteralValue::Boolean(b) => FlakeValue::Boolean(*b),
                LiteralValue::Vector(v) => FlakeValue::Vector(v.clone()),
            };

            match dtc {
                Some(UnresolvedDatatypeConstraint::LangTag(lang)) => {
                    // rdf:langString requires a string lexical form
                    if !matches!(initial_fv, FlakeValue::String(_)) {
                        return Err(ParseError::InvalidWhere(
                            "Language-tagged VALUES literals must be strings".to_string(),
                        ));
                    }
                    Ok(Binding::lit_lang(initial_fv, lang.as_ref()))
                }
                Some(UnresolvedDatatypeConstraint::Explicit(dt_iri)) => {
                    let sid = encoder
                        .encode_iri(dt_iri)
                        .ok_or_else(|| ParseError::UnknownNamespace(dt_iri.to_string()))?;
                    let coerced = coerce_value_by_datatype(initial_fv, dt_iri)?;
                    Ok(Binding::lit(coerced, sid))
                }
                None => {
                    let sid = match value {
                        LiteralValue::String(_) => dts.xsd_string,
                        LiteralValue::Long(_) => dts.xsd_long,
                        LiteralValue::Double(_) => dts.xsd_double,
                        LiteralValue::Boolean(_) => dts.xsd_boolean,
                        LiteralValue::Vector(_) => dts.fluree_vector,
                    };
                    Ok(Binding::lit(initial_fv, sid))
                }
            }
        }
    }
}

// XSD datatype IRIs for typed literal coercion are imported from fluree_vocab::xsd

/// Coerce a FlakeValue based on its datatype IRI.
///
/// This handles typed literals like `{"@value": "3", "@type": "xsd:integer"}` where
/// the string "3" needs to be coerced to Long(3).
///
/// Policy:
/// - String @value + xsd:integer/long/int/short/byte → Long (or BigInt if > i64)
/// - String @value + xsd:decimal → BigDecimal
/// - String @value + xsd:double/float → Double
/// - String @value + xsd:dateTime/date/time → DateTime/Date/Time
/// - JSON number @value + xsd:string → ERROR (incompatible)
/// - JSON boolean @value + xsd:string → ERROR (incompatible)
///
/// Coerce a FlakeValue to match the target datatype.
///
/// This is a thin wrapper around `fluree_db_core::coerce::coerce_value` that
/// maps the core `CoercionError` to query `ParseError::TypeCoercion`.
///
/// # Arguments
/// * `value` - The value to coerce
/// * `datatype_iri` - Fully expanded datatype IRI (e.g., `http://www.w3.org/2001/XMLSchema#integer`)
///
/// # Contract
/// The caller is responsible for ensuring the datatype IRI is fully expanded.
/// Prefix expansion should happen at the JSON-LD parsing boundary, not here.
/// Use constants from `fluree_vocab::xsd` for common datatypes.
///
/// # Returns
/// * `Ok(FlakeValue)` - The coerced value
/// * `Err(ParseError::TypeCoercion)` - If coercion fails
pub fn coerce_value_by_datatype(value: FlakeValue, datatype_iri: &str) -> Result<FlakeValue> {
    fluree_db_core::coerce::coerce_value(value, datatype_iri)
        .map_err(|e| ParseError::TypeCoercion(e.message))
}

/// Lower an unresolved triple pattern to a resolved TriplePattern
fn lower_triple_pattern<E: IriEncoder>(
    pattern: &UnresolvedTriplePattern,
    encoder: &E,
    vars: &mut VarRegistry,
) -> Result<TriplePattern> {
    let s = lower_ref_term(&pattern.s, encoder, vars)?;
    let p = lower_ref_term(&pattern.p, encoder, vars)?;
    let mut o = lower_term(&pattern.o, encoder, vars)?;

    // Lower constraint (datatype or language tag)
    let dtc = match &pattern.dtc {
        Some(UnresolvedDatatypeConstraint::Explicit(dt_iri)) => {
            let dt_sid = encoder
                .encode_iri(dt_iri)
                .ok_or_else(|| ParseError::UnknownNamespace(dt_iri.to_string()))?;

            // Coerce literal values based on datatype
            if let Term::Value(ref value) = o {
                let coerced = coerce_value_by_datatype(value.clone(), dt_iri)?;
                o = Term::Value(coerced);
            }

            Some(DatatypeConstraint::Explicit(dt_sid))
        }
        Some(UnresolvedDatatypeConstraint::LangTag(tag)) => {
            Some(DatatypeConstraint::LangTag(tag.clone()))
        }
        None => None,
    };

    Ok(TriplePattern { s, p, o, dtc })
}

/// Lower a property path pattern (from `@path` alias) to resolved Pattern(s).
///
/// Different path expressions compile to different pattern types:
/// - `p+` / `p*` → `Pattern::PropertyPath` (existing transitive operator)
/// - `^p` → `Pattern::Triple` with subject/object swapped
/// - `a|b|...` → `Pattern::Union` of triple branches (bag semantics)
/// - `a/b/c` → chain of `Pattern::Triple` joined by `?__pp{n}` variables
/// - `^p` inside `|` and `/` steps is supported
///
/// Validates subject/object are not literals.
fn lower_path_to_patterns<E: IriEncoder>(
    subject: &UnresolvedTerm,
    path: &UnresolvedPathExpr,
    object: &UnresolvedTerm,
    encoder: &E,
    vars: &mut VarRegistry,
    pp_counter: &mut u32,
) -> Result<Vec<Pattern>> {
    let s = lower_ref_term(subject, encoder, vars)?;
    let o = lower_ref_term(object, encoder, vars)?;

    // Rewrite complex inverses (^(a/b), ^(a|b), ^(^x)) before dispatching.
    let rewritten;
    let effective_path = if let Some(r) = rewrite_inverse_of_complex(path) {
        rewritten = r;
        &rewritten
    } else {
        path
    };

    match effective_path {
        // Transitive: keep existing PropertyPath operator
        UnresolvedPathExpr::OneOrMore(inner) | UnresolvedPathExpr::ZeroOrMore(inner) => {
            let iri = expect_simple_iri(inner)?;
            let modifier = match path {
                UnresolvedPathExpr::OneOrMore(_) => PathModifier::OneOrMore,
                _ => PathModifier::ZeroOrMore,
            };
            let predicate = encoder
                .encode_iri(iri)
                .ok_or_else(|| ParseError::UnknownNamespace(iri.to_string()))?;
            Ok(vec![Pattern::PropertyPath(PropertyPathPattern::new(
                s, predicate, modifier, o,
            ))])
        }

        // Inverse: ^path
        UnresolvedPathExpr::Inverse(inner) => match inner.as_ref() {
            // Inverse-transitive: ^p+ or ^p* → PropertyPathPattern with swapped s/o
            UnresolvedPathExpr::OneOrMore(tp_inner) | UnresolvedPathExpr::ZeroOrMore(tp_inner) => {
                let iri = expect_simple_iri(tp_inner)?;
                let modifier = match inner.as_ref() {
                    UnresolvedPathExpr::OneOrMore(_) => PathModifier::OneOrMore,
                    _ => PathModifier::ZeroOrMore,
                };
                let predicate = encoder
                    .encode_iri(iri)
                    .ok_or_else(|| ParseError::UnknownNamespace(iri.to_string()))?;
                // Swap subject/object for inverse traversal
                Ok(vec![Pattern::PropertyPath(PropertyPathPattern::new(
                    o, predicate, modifier, s,
                ))])
            }
            // Simple inverse: ^p → Triple with s/o swapped
            _ => {
                let iri = expect_simple_iri(inner)?;
                Ok(vec![Pattern::Triple(TriplePattern::new(
                    o,
                    Ref::Iri(iri.clone()),
                    s.into(),
                ))])
            }
        },

        // Alternative: compile to Union of triple branches (bag semantics)
        UnresolvedPathExpr::Alternative(alts) => {
            let branches: Vec<Vec<Pattern>> = alts
                .iter()
                .map(|alt| lower_alternative_branch(alt, &s, &o, encoder, vars, pp_counter))
                .collect::<Result<_>>()?;
            Ok(vec![Pattern::Union(branches)])
        }

        // Sequence: compile to chain of triple patterns with join variables
        UnresolvedPathExpr::Sequence(steps) => {
            lower_sequence_chain(&s, steps, &o, encoder, vars, pp_counter)
        }

        // Unsupported operators
        UnresolvedPathExpr::Iri(_) => Err(ParseError::InvalidWhere(
            "@path with plain IRI (no modifier) is not a valid property path; \
             use a regular predicate or add + or *"
                .to_string(),
        )),
        UnresolvedPathExpr::ZeroOrOne(_) => Err(ParseError::InvalidWhere(
            "Optional (?) property paths are parsed but not yet supported for execution"
                .to_string(),
        )),
    }
}

/// Recursively rewrite `Inverse(Sequence/Alternative)` so that `Inverse` only
/// appears directly around leaf IRIs. Also cancels double-inverse (`^(^x)` → `x`).
///
/// Rewrite rules (applied recursively):
///   `^(^x)`    → `normalize(x)` — double-inverse cancellation
///   `^(a/b/c)` → `(^c)/(^b)/(^a)` — reverse sequence, invert each step
///   `^(a|b|c)` → `(^a)|(^b)|(^c)` — distribute inverse into each branch
///
/// Returns `Some(rewritten)` if the input was a complex inverse that was
/// transformed, `None` if no rewrite was needed (simple/transitive inverse).
fn rewrite_inverse_of_complex(path: &UnresolvedPathExpr) -> Option<UnresolvedPathExpr> {
    match path {
        UnresolvedPathExpr::Inverse(inner) => match inner.as_ref() {
            // Double inverse: ^(^x) → x (then normalize in case x is also complex)
            UnresolvedPathExpr::Inverse(inner_inner) => {
                let cancelled = inner_inner.as_ref().clone();
                Some(rewrite_inverse_of_complex(&cancelled).unwrap_or(cancelled))
            }
            // ^(a/b/c) → (^c)/(^b)/(^a)
            UnresolvedPathExpr::Sequence(steps) => Some(UnresolvedPathExpr::Sequence(
                steps
                    .iter()
                    .rev()
                    .map(|step| {
                        let inv = UnresolvedPathExpr::Inverse(Box::new(step.clone()));
                        rewrite_inverse_of_complex(&inv).unwrap_or(inv)
                    })
                    .collect(),
            )),
            // ^(a|b|c) → (^a)|(^b)|(^c)
            UnresolvedPathExpr::Alternative(branches) => Some(UnresolvedPathExpr::Alternative(
                branches
                    .iter()
                    .map(|branch| {
                        let inv = UnresolvedPathExpr::Inverse(Box::new(branch.clone()));
                        rewrite_inverse_of_complex(&inv).unwrap_or(inv)
                    })
                    .collect(),
            )),
            // Simple inverse (^p), transitive (^p+, ^p*), etc. — existing handling
            _ => None,
        },
        _ => None,
    }
}

/// Maximum number of expanded chains when distributing alternatives in a
/// sequence path. Prevents combinatorial explosion from expressions like
/// `(a|b)/(c|d)/(e|f)/...`.
const MAX_SEQUENCE_EXPANSION: usize = 64;

/// Lower a sequence (/) property path into a chain of triple patterns.
///
/// Each step produces a triple pattern, with adjacent steps joined by generated
/// intermediate variables (`?__pp0`, `?__pp1`, ...).
///
/// Example: `Sequence([a, b, c])` with subject `?s` and object `?o`:
/// ```text
///   Triple(?s,     a, ?__pp0)
///   Triple(?__pp0, b, ?__pp1)
///   Triple(?__pp1, c, ?o)
/// ```
///
/// Steps can be forward (`Iri(p)`) or inverse (`Inverse(Iri(p))`).
/// Alternative steps (`(a|b)`) are distributed into a `Union` of simple chains.
/// All other step types are rejected with a clear error.
fn lower_sequence_chain<E: IriEncoder>(
    s: &Ref,
    steps: &[UnresolvedPathExpr],
    o: &Ref,
    encoder: &E,
    vars: &mut VarRegistry,
    pp_counter: &mut u32,
) -> Result<Vec<Pattern>> {
    debug_assert!(
        !steps.is_empty(),
        "BUG: parser produced empty Sequence — this should never happen"
    );

    // Degenerate single-step sequence (parser should collapse, but guard)
    if steps.len() == 1 {
        return lower_sequence_step(&steps[0], s, o, encoder);
    }

    // Build step choices: each step → vec of simple alternatives.
    // Non-alternative steps → single-element vec. Alternative steps → branches.
    let mut step_choices: Vec<Vec<&UnresolvedPathExpr>> = Vec::with_capacity(steps.len());
    let mut has_alt = false;

    for step in steps {
        match step {
            UnresolvedPathExpr::Alternative(branches) => {
                has_alt = true;
                for branch in branches {
                    validate_simple_step(branch)?;
                }
                step_choices.push(branches.iter().collect());
            }
            _ => {
                step_choices.push(vec![step]);
            }
        }
    }

    if has_alt {
        return lower_distributed_sequence(s, &step_choices, o, encoder, vars, pp_counter);
    }

    // Fast path: no alternatives — generate simple chain
    let mut patterns = Vec::with_capacity(steps.len());
    let mut prev = s.clone();

    for (i, step) in steps.iter().enumerate() {
        let is_last = i == steps.len() - 1;
        let next = if is_last {
            o.clone()
        } else {
            let var_name = format!("?__pp{}", *pp_counter);
            *pp_counter += 1;
            Ref::Var(vars.get_or_insert(&var_name))
        };

        let pat = lower_sequence_step_pattern(step, &prev, &next, encoder)?;
        patterns.push(pat);

        prev = next;
    }

    Ok(patterns)
}

/// Validate that a step inside an Alternative (within a sequence) is a "simple"
/// step: `Iri(p)`, `Inverse(Iri(p))`, transitive `p+`/`p*` on a simple predicate,
/// or inverse of a transitive simple predicate.
fn validate_simple_step(step: &UnresolvedPathExpr) -> Result<()> {
    match step {
        UnresolvedPathExpr::Iri(_) => Ok(()),
        UnresolvedPathExpr::OneOrMore(inner) | UnresolvedPathExpr::ZeroOrMore(inner) => {
            match inner.as_ref() {
                UnresolvedPathExpr::Iri(_) => Ok(()),
                other => Err(ParseError::InvalidWhere(format!(
                    "Transitive steps within a sequence must apply +/* to a simple predicate; got {}",
                    path_expr_name(other),
                ))),
            }
        }
        UnresolvedPathExpr::Inverse(inner) => match inner.as_ref() {
            UnresolvedPathExpr::Iri(_) => Ok(()),
            UnresolvedPathExpr::OneOrMore(tp_inner) | UnresolvedPathExpr::ZeroOrMore(tp_inner) => {
                match tp_inner.as_ref() {
                    UnresolvedPathExpr::Iri(_) => Ok(()),
                    other => Err(ParseError::InvalidWhere(format!(
                        "Transitive inverse steps within a sequence must apply +/* to a simple predicate; got inverse of {}",
                        path_expr_name(other),
                    ))),
                }
            }
            other => Err(ParseError::InvalidWhere(format!(
                "Alternative steps within a sequence must be simple predicates or \
                 inverse simple predicates (^ex:p); got inverse of {}",
                path_expr_name(other),
            ))),
        },
        other => Err(ParseError::InvalidWhere(format!(
            "Alternative steps within a sequence must be simple predicates or \
             inverse simple predicates (^ex:p); got {}",
            path_expr_name(other),
        ))),
    }
}

/// Compute the Cartesian product of step choices.
///
/// Given `[[a, b], [c], [d, e]]`, produces:
/// `[[a, c, d], [a, c, e], [b, c, d], [b, c, e]]`
fn cartesian_product<'a>(
    step_choices: &'a [Vec<&'a UnresolvedPathExpr>],
) -> Vec<Vec<&'a UnresolvedPathExpr>> {
    let mut result: Vec<Vec<&UnresolvedPathExpr>> = vec![vec![]];
    for choices in step_choices {
        let mut new_result = Vec::new();
        for existing in &result {
            for choice in choices {
                let mut combo = existing.clone();
                combo.push(choice);
                new_result.push(combo);
            }
        }
        result = new_result;
    }
    result
}

/// Lower a distributed sequence (containing alternative steps) into a Union.
///
/// Expands the Cartesian product of step choices into individual simple chains,
/// each becoming a branch of a `Pattern::Union`.
fn lower_distributed_sequence<E: IriEncoder>(
    s: &Ref,
    step_choices: &[Vec<&UnresolvedPathExpr>],
    o: &Ref,
    encoder: &E,
    vars: &mut VarRegistry,
    pp_counter: &mut u32,
) -> Result<Vec<Pattern>> {
    let combos = cartesian_product(step_choices);
    let n = combos.len();
    if n > MAX_SEQUENCE_EXPANSION {
        return Err(ParseError::InvalidWhere(format!(
            "Property path sequence expands to {n} chains (limit {MAX_SEQUENCE_EXPANSION})",
        )));
    }

    let branches: Vec<Vec<Pattern>> = combos
        .into_iter()
        .map(|combo| lower_simple_sequence_chain(s, &combo, o, encoder, vars, pp_counter))
        .collect::<Result<_>>()?;

    Ok(vec![Pattern::Union(branches)])
}

/// Lower a simple sequence chain (no alternative steps) to a list of triple patterns.
///
/// Each step must be `Iri(p)` or `Inverse(Iri(p))`. Adjacent steps are joined
/// by generated intermediate variables (`?__pp{n}`).
fn lower_simple_sequence_chain<E: IriEncoder>(
    s: &Ref,
    steps: &[&UnresolvedPathExpr],
    o: &Ref,
    encoder: &E,
    vars: &mut VarRegistry,
    pp_counter: &mut u32,
) -> Result<Vec<Pattern>> {
    let mut patterns = Vec::with_capacity(steps.len());
    let mut prev = s.clone();

    for (i, step) in steps.iter().enumerate() {
        let is_last = i == steps.len() - 1;
        let next = if is_last {
            o.clone()
        } else {
            let var_name = format!("?__pp{}", *pp_counter);
            *pp_counter += 1;
            Ref::Var(vars.get_or_insert(&var_name))
        };

        let pat = lower_sequence_step_pattern(step, &prev, &next, encoder)?;
        patterns.push(pat);

        prev = next;
    }

    Ok(patterns)
}

/// Lower a single step of a sequence path to a Pattern.
///
/// Forward step `p`: `Triple(prev, p, next)`
/// Inverse step `^p`: `Triple(next, p, prev)` (swapped)
/// Transitive step `p+`/`p*`: `PropertyPath(prev, p, next)`
/// Inverse-transitive step `^p+`/`^p*`: `PropertyPath(next, p, prev)` (swapped)
///
/// Note: Alternative steps (`(a|b)`) are handled by distribution in
/// `lower_sequence_chain` before this function is called.
fn lower_sequence_step_pattern<E: IriEncoder>(
    step: &UnresolvedPathExpr,
    prev: &Ref,
    next: &Ref,
    encoder: &E,
) -> Result<Pattern> {
    match step {
        UnresolvedPathExpr::Iri(iri) => {
            let p = Ref::Iri(iri.clone());
            Ok(Pattern::Triple(TriplePattern::new(
                prev.clone(),
                p,
                next.clone().into(),
            )))
        }
        UnresolvedPathExpr::OneOrMore(inner) | UnresolvedPathExpr::ZeroOrMore(inner) => {
            if prev.is_bound() && next.is_bound() {
                return Err(ParseError::InvalidWhere(
                    "Property path requires at least one variable (cannot have both subject and object as constants)"
                        .to_string(),
                ));
            }
            let iri = expect_simple_iri(inner)?;
            let modifier = match step {
                UnresolvedPathExpr::OneOrMore(_) => PathModifier::OneOrMore,
                _ => PathModifier::ZeroOrMore,
            };
            let predicate = encoder
                .encode_iri(iri)
                .ok_or_else(|| ParseError::UnknownNamespace(iri.to_string()))?;
            Ok(Pattern::PropertyPath(PropertyPathPattern::new(
                prev.clone(),
                predicate,
                modifier,
                next.clone(),
            )))
        }
        UnresolvedPathExpr::Inverse(inner) => match inner.as_ref() {
            UnresolvedPathExpr::Iri(iri) => {
                let p = Ref::Iri(iri.clone());
                Ok(Pattern::Triple(TriplePattern::new(
                    next.clone(),
                    p,
                    prev.clone().into(),
                )))
            }
            UnresolvedPathExpr::OneOrMore(tp_inner) | UnresolvedPathExpr::ZeroOrMore(tp_inner) => {
                if prev.is_bound() && next.is_bound() {
                    return Err(ParseError::InvalidWhere(
                        "Property path requires at least one variable (cannot have both subject and object as constants)"
                            .to_string(),
                    ));
                }
                let iri = expect_simple_iri(tp_inner)?;
                let modifier = match inner.as_ref() {
                    UnresolvedPathExpr::OneOrMore(_) => PathModifier::OneOrMore,
                    _ => PathModifier::ZeroOrMore,
                };
                let predicate = encoder
                    .encode_iri(iri)
                    .ok_or_else(|| ParseError::UnknownNamespace(iri.to_string()))?;
                Ok(Pattern::PropertyPath(PropertyPathPattern::new(
                    next.clone(),
                    predicate,
                    modifier,
                    prev.clone(),
                )))
            }
            other => Err(ParseError::InvalidWhere(format!(
                "Sequence (/) steps must be simple predicates, inverse simple predicates (^ex:p), \
                 transitive predicates (ex:p+ or ex:p*), or alternatives of simple predicates \
                 ((ex:a|ex:b)); got inverse of {}",
                path_expr_name(other),
            ))),
        },
        other => Err(ParseError::InvalidWhere(format!(
            "Sequence (/) steps must be simple predicates, inverse simple predicates (^ex:p), \
             transitive predicates (ex:p+ or ex:p*), or alternatives of simple predicates \
             ((ex:a|ex:b)); got {}",
            path_expr_name(other),
        ))),
    }
}

/// Lower a degenerate single-step sequence to a pattern list.
fn lower_sequence_step<E: IriEncoder>(
    step: &UnresolvedPathExpr,
    s: &Ref,
    o: &Ref,
    encoder: &E,
) -> Result<Vec<Pattern>> {
    let pat = lower_sequence_step_pattern(step, s, o, encoder)?;
    Ok(vec![pat])
}

/// Lower a single branch of an Alternative path to a pattern list.
///
/// Each branch becomes one or more patterns. Supports:
/// - `Iri(p)` → `Pattern::Triple(s, p, o)`
/// - `Inverse(Iri(p))` → `Pattern::Triple(o, p, s)` (swapped)
/// - `Sequence([...])` → chain of `Pattern::Triple` joined by `?__pp{n}` variables
fn lower_alternative_branch(
    alt: &UnresolvedPathExpr,
    s: &Ref,
    o: &Ref,
    encoder: &impl IriEncoder,
    vars: &mut VarRegistry,
    pp_counter: &mut u32,
) -> Result<Vec<Pattern>> {
    match alt {
        UnresolvedPathExpr::Iri(iri) => {
            let p = Ref::Iri(iri.clone());
            Ok(vec![Pattern::Triple(TriplePattern::new(
                s.clone(),
                p,
                o.clone().into(),
            ))])
        }
        UnresolvedPathExpr::Inverse(inner) => match inner.as_ref() {
            UnresolvedPathExpr::Iri(iri) => {
                let p = Ref::Iri(iri.clone());
                Ok(vec![Pattern::Triple(TriplePattern::new(
                    o.clone(),
                    p,
                    s.clone().into(),
                ))])
            }
            other => Err(ParseError::InvalidWhere(format!(
                "Alternative (|) branches support simple predicates, inverse simple \
                 predicates (^ex:p), or sequence chains (ex:a/ex:b); got inverse of {}",
                path_expr_name(other),
            ))),
        },
        UnresolvedPathExpr::Sequence(steps) => {
            lower_sequence_chain(s, steps, o, encoder, vars, pp_counter)
        }
        other => Err(ParseError::InvalidWhere(format!(
            "Alternative (|) branches support simple predicates, inverse simple \
             predicates (^ex:p), or sequence chains (ex:a/ex:b); got {}",
            path_expr_name(other),
        ))),
    }
}

/// Human-readable name for an `UnresolvedPathExpr` variant (for error messages).
fn path_expr_name(expr: &UnresolvedPathExpr) -> &'static str {
    match expr {
        UnresolvedPathExpr::Iri(_) => "IRI",
        UnresolvedPathExpr::Inverse(_) => "Inverse (^)",
        UnresolvedPathExpr::Sequence(_) => "Sequence (/)",
        UnresolvedPathExpr::Alternative(_) => "Alternative (|)",
        UnresolvedPathExpr::ZeroOrMore(_) => "ZeroOrMore (*)",
        UnresolvedPathExpr::OneOrMore(_) => "OneOrMore (+)",
        UnresolvedPathExpr::ZeroOrOne(_) => "ZeroOrOne (?)",
    }
}

/// Expect a path expression to be a simple IRI. Returns the Arc'd IRI string.
fn expect_simple_iri(path: &UnresolvedPathExpr) -> Result<&Arc<str>> {
    match path {
        UnresolvedPathExpr::Iri(iri) => Ok(iri),
        _ => Err(ParseError::InvalidWhere(
            "Transitive paths (+ or *) currently require a simple predicate IRI".to_string(),
        )),
    }
}

/// Lower an unresolved subquery to a resolved SubqueryPattern
///
/// Processes the subquery's select list, patterns, and options.
fn lower_subquery<E: IriEncoder>(
    subquery: &UnresolvedQuery,
    encoder: &E,
    vars: &mut VarRegistry,
    pp_counter: &mut u32,
) -> Result<SubqueryPattern> {
    // Lower select list to VarIds
    let select: Vec<VarId> = subquery
        .select
        .iter()
        .map(|var_name| vars.get_or_insert(var_name))
        .collect();

    // Lower WHERE patterns
    let patterns = lower_unresolved_patterns(&subquery.patterns, encoder, vars, pp_counter)?;

    // Build SubqueryPattern with options
    let mut sq = SubqueryPattern::new(select, patterns);

    if let Some(limit) = subquery.options.limit {
        sq = sq.with_limit(limit);
    }
    if let Some(offset) = subquery.options.offset {
        sq = sq.with_offset(offset);
    }
    if subquery.options.distinct {
        sq = sq.with_distinct();
    }
    if !subquery.options.order_by.is_empty() {
        let sort_specs: Vec<_> = subquery
            .options
            .order_by
            .iter()
            .map(|s| lower_sort_spec(s, vars))
            .collect();
        sq = sq.with_order_by(sort_specs);
    }

    // GROUP BY / aggregates / HAVING (needed for subqueries used in filters/unions)
    if !subquery.options.group_by.is_empty() {
        sq.group_by = subquery
            .options
            .group_by
            .iter()
            .map(|v| vars.get_or_insert(v))
            .collect();
    }
    if !subquery.options.aggregates.is_empty() {
        sq.aggregates = subquery
            .options
            .aggregates
            .iter()
            .map(|a| lower_aggregate_spec(a, vars))
            .collect();
    }
    if let Some(ref having) = subquery.options.having {
        sq.having = Some(lower_filter_expr(having, vars)?);
    }

    Ok(sq)
}

/// Lower an unresolved CONSTRUCT template to a resolved ConstructTemplate
///
/// Only processes triple patterns from the template (filters/optionals are ignored).
fn lower_construct_template<E: IriEncoder>(
    template: &UnresolvedConstructTemplate,
    encoder: &E,
    vars: &mut VarRegistry,
) -> Result<ConstructTemplate> {
    let mut patterns = Vec::new();

    for unresolved in &template.patterns {
        if let UnresolvedPattern::Triple(tp) = unresolved {
            patterns.push(lower_triple_pattern(tp, encoder, vars)?);
        }
        // Ignore non-triple patterns in templates (filters, optionals, binds)
    }

    Ok(ConstructTemplate::new(patterns))
}

// ============================================================================
// Graph crawl lowering
// ============================================================================

/// Lower an unresolved graph select specification to a resolved GraphSelectSpec
fn lower_graph_select<E: IriEncoder>(
    spec: &UnresolvedGraphSelectSpec,
    encoder: &E,
    vars: &mut VarRegistry,
) -> Result<GraphSelectSpec> {
    // Handle root - variable or IRI constant
    let root = match &spec.root {
        UnresolvedRoot::Var(name) => {
            // Use get_or_insert: don't require root var to be in WHERE
            // (Allows unbound root vars; formatting skips those rows)
            let var_id = vars.get_or_insert(name);
            Root::Var(var_id)
        }
        UnresolvedRoot::Iri(expanded_iri) => {
            // Allow non-IRI (and “faux compact”) ids to be used as subjects.
            //
            // If the IRI encoder can't encode the value via namespaces, fall back to namespace_code=0
            // (raw id string). This supports ids like "foo" and "foaf:bar" without requiring @base/@vocab.
            let sid = encoder
                .encode_iri(expanded_iri)
                .unwrap_or_else(|| Sid::new(0, expanded_iri));
            Root::Sid(sid)
        }
    };

    // Lower forward selections
    let selections = spec
        .selections
        .iter()
        .map(|s| lower_selection_spec(s, encoder))
        .collect::<Result<Vec<_>>>()?;

    // Lower reverse selections
    let mut reverse = std::collections::HashMap::new();
    for (iri, nested_opt) in &spec.reverse {
        let sid = encoder
            .encode_iri(iri)
            .ok_or_else(|| ParseError::UnknownNamespace(iri.clone()))?;
        let lowered_nested = nested_opt
            .as_ref()
            .map(|nested| lower_nested_select_spec(nested, encoder))
            .transpose()?;
        reverse.insert(sid, lowered_nested);
    }

    Ok(GraphSelectSpec {
        root,
        selections,
        reverse,
        depth: spec.depth,
        has_wildcard: spec.has_wildcard,
    })
}

/// Lower a single selection spec to resolved form
fn lower_selection_spec<E: IriEncoder>(
    spec: &UnresolvedSelectionSpec,
    encoder: &E,
) -> Result<SelectionSpec> {
    match spec {
        UnresolvedSelectionSpec::Id => Ok(SelectionSpec::Id),
        UnresolvedSelectionSpec::Wildcard => Ok(SelectionSpec::Wildcard),
        UnresolvedSelectionSpec::Property {
            predicate,
            sub_spec,
        } => {
            let sid = encoder
                .encode_iri(predicate)
                .ok_or_else(|| ParseError::UnknownNamespace(predicate.clone()))?;

            // Lower nested spec (includes both forward and reverse)
            let lowered_sub_spec = sub_spec
                .as_ref()
                .map(|nested| lower_nested_select_spec(nested, encoder))
                .transpose()?;

            Ok(SelectionSpec::Property {
                predicate: sid,
                sub_spec: lowered_sub_spec,
            })
        }
    }
}

/// Lower a nested select spec to resolved form
fn lower_nested_select_spec<E: IriEncoder>(
    spec: &UnresolvedNestedSelectSpec,
    encoder: &E,
) -> Result<Box<NestedSelectSpec>> {
    // Lower forward selections
    let forward = spec
        .forward
        .iter()
        .map(|s| lower_selection_spec(s, encoder))
        .collect::<Result<Vec<_>>>()?;

    // Lower reverse selections (now carries full nested spec, not just forward)
    let mut reverse = std::collections::HashMap::new();
    for (iri, nested_opt) in &spec.reverse {
        let sid = encoder
            .encode_iri(iri)
            .ok_or_else(|| ParseError::UnknownNamespace(iri.clone()))?;
        let lowered_nested = nested_opt
            .as_ref()
            .map(|nested| lower_nested_select_spec(nested, encoder))
            .transpose()?;
        reverse.insert(sid, lowered_nested);
    }

    Ok(Box::new(NestedSelectSpec::new(
        forward,
        reverse,
        spec.has_wildcard,
    )))
}

/// Lower an unresolved filter expression to a resolved Expression
pub(crate) fn lower_filter_expr(
    expr: &UnresolvedExpression,
    vars: &mut VarRegistry,
) -> Result<Expression> {
    lower_filter_expr_inner::<NoEncoder>(expr, vars, None, &mut 0)
}

/// Lower a filter expression with encoder access for EXISTS pattern lowering.
pub(crate) fn lower_filter_expr_with_encoder<E: IriEncoder>(
    expr: &UnresolvedExpression,
    vars: &mut VarRegistry,
    encoder: &E,
    pp_counter: &mut u32,
) -> Result<Expression> {
    lower_filter_expr_inner(expr, vars, Some(encoder), pp_counter)
}

/// Inner filter expression lowering, optionally encoder-aware for EXISTS.
fn lower_filter_expr_inner<E: IriEncoder>(
    expr: &UnresolvedExpression,
    vars: &mut VarRegistry,
    encoder: Option<&E>,
    pp_counter: &mut u32,
) -> Result<Expression> {
    match expr {
        UnresolvedExpression::Var(name) => {
            let var_id = vars.get_or_insert(name);
            Ok(Expression::Var(var_id))
        }
        UnresolvedExpression::Const(val) => Ok(Expression::Const(val.into())),
        UnresolvedExpression::And(exprs) => {
            let lowered: Result<Vec<Expression>> = exprs
                .iter()
                .map(|e| lower_filter_expr_inner(e, vars, encoder, pp_counter))
                .collect();
            Ok(Expression::and(lowered?))
        }
        UnresolvedExpression::Or(exprs) => {
            let lowered: Result<Vec<Expression>> = exprs
                .iter()
                .map(|e| lower_filter_expr_inner(e, vars, encoder, pp_counter))
                .collect();
            Ok(Expression::or(lowered?))
        }
        UnresolvedExpression::Not(inner) => {
            let lowered = lower_filter_expr_inner(inner, vars, encoder, pp_counter)?;
            Ok(Expression::not(lowered))
        }
        UnresolvedExpression::In {
            expr,
            values,
            negated,
        } => {
            let lowered_expr = lower_filter_expr_inner(expr, vars, encoder, pp_counter)?;
            let lowered_values: Result<Vec<Expression>> = values
                .iter()
                .map(|v| lower_filter_expr_inner(v, vars, encoder, pp_counter))
                .collect();
            if *negated {
                Ok(Expression::not_in_list(lowered_expr, lowered_values?))
            } else {
                Ok(Expression::in_list(lowered_expr, lowered_values?))
            }
        }
        UnresolvedExpression::Call { func, args } => {
            let lowered_args: Result<Vec<Expression>> = args
                .iter()
                .map(|a| lower_filter_expr_inner(a, vars, encoder, pp_counter))
                .collect();
            let func_name = lower_function_name(func);
            if let Function::Custom(unknown) = &func_name {
                return Err(ParseError::InvalidFilter(format!(
                    "Unknown function: {unknown}"
                )));
            }
            Ok(Expression::Call {
                func: func_name,
                args: lowered_args?,
            })
        }
        UnresolvedExpression::Exists { patterns, negated } => {
            let enc = encoder.ok_or_else(|| {
                ParseError::InvalidFilter(
                    "EXISTS in filter expression requires an IRI encoder context".to_string(),
                )
            })?;
            let lowered_patterns = lower_unresolved_patterns(patterns, enc, vars, pp_counter)?;
            Ok(Expression::Exists {
                patterns: lowered_patterns,
                negated: *negated,
            })
        }
    }
}

/// Lower a function name string to a Function enum
fn lower_function_name(name: &str) -> Function {
    match name.to_lowercase().as_str() {
        // Comparison operators
        "=" => Function::Eq,
        "!=" => Function::Ne,
        "<" => Function::Lt,
        "<=" => Function::Le,
        ">" => Function::Gt,
        ">=" => Function::Ge,
        // Arithmetic operators
        "+" => Function::Add,
        "-" => Function::Sub,
        "*" => Function::Mul,
        "/" => Function::Div,
        "negate" => Function::Negate,
        // String functions
        "strlen" => Function::Strlen,
        "substr" | "substring" => Function::Substr,
        "ucase" => Function::Ucase,
        "lcase" => Function::Lcase,
        "contains" => Function::Contains,
        "strstarts" => Function::StrStarts,
        "strends" => Function::StrEnds,
        "regex" => Function::Regex,
        "concat" => Function::Concat,
        "strbefore" => Function::StrBefore,
        "strafter" => Function::StrAfter,
        "replace" => Function::Replace,
        "str" => Function::Str,
        "strdt" | "str-dt" => Function::StrDt,
        "strlang" | "str-lang" => Function::StrLang,
        "encode_for_uri" | "encodeforuri" => Function::EncodeForUri,
        // Numeric functions
        "abs" => Function::Abs,
        "round" => Function::Round,
        "ceil" => Function::Ceil,
        "floor" => Function::Floor,
        "rand" => Function::Rand,
        "iri" => Function::Iri,
        "bnode" => Function::Bnode,
        // DateTime functions
        "now" => Function::Now,
        "year" => Function::Year,
        "month" => Function::Month,
        "day" => Function::Day,
        "hours" => Function::Hours,
        "minutes" => Function::Minutes,
        "seconds" => Function::Seconds,
        "tz" => Function::Tz,
        "timezone" => Function::Timezone,
        // Type functions
        "isiri" | "isuri" | "is-iri" | "is-uri" => Function::IsIri,
        "isblank" | "is-blank" => Function::IsBlank,
        "isliteral" | "is-literal" => Function::IsLiteral,
        "isnumeric" | "is-numeric" => Function::IsNumeric,
        // RDF term functions
        "lang" => Function::Lang,
        "datatype" => Function::Datatype,
        "langmatches" => Function::LangMatches,
        "sameterm" => Function::SameTerm,
        // Fluree-specific: transaction time
        "t" => Function::T,
        "op" => Function::Op,
        // Hash functions
        "md5" => Function::Md5,
        "sha1" => Function::Sha1,
        "sha256" => Function::Sha256,
        "sha384" => Function::Sha384,
        "sha512" => Function::Sha512,
        // UUID functions
        "uuid" => Function::Uuid,
        "struuid" => Function::StrUuid,
        // Vector/embedding similarity functions
        "dotproduct" | "dot_product" => Function::DotProduct,
        "cosinesimilarity" | "cosine_similarity" => Function::CosineSimilarity,
        "euclideandistance" | "euclidean_distance" | "euclidiandistance" => {
            Function::EuclideanDistance
        }
        // Geospatial functions (OGC GeoSPARQL)
        // Note: plain "distance" intentionally omitted to avoid collision with vector/edit distance
        "geof:distance"
        | "geo_distance"
        | "geodistance"
        | "http://www.opengis.net/def/function/geosparql/distance" => Function::GeofDistance,
        // Fulltext scoring
        "fulltext" | "full_text" => Function::Fulltext,
        // Other
        "bound" => Function::Bound,
        "if" => Function::If,
        "coalesce" => Function::Coalesce,
        other => Function::Custom(other.to_string()),
    }
}

/// Lower an unresolved term to a resolved Term
fn lower_term<E: IriEncoder>(
    term: &UnresolvedTerm,
    _encoder: &E,
    vars: &mut VarRegistry,
) -> Result<Term> {
    match term {
        UnresolvedTerm::Var(name) => {
            let var_id = vars.get_or_insert(name);
            Ok(Term::Var(var_id))
        }
        UnresolvedTerm::Iri(iri) => {
            // Emit Term::Iri to defer SID encoding to scan time.
            // This enables correct cross-ledger joins where each ledger's namespace
            // table may encode the same IRI differently. The scan operator will
            // encode the IRI per-db using build_range_match_for_db().
            Ok(Term::Iri(iri.clone()))
        }
        UnresolvedTerm::Literal(lit) => {
            let value = lower_literal(lit);
            Ok(Term::Value(value))
        }
    }
}

/// Lower an unresolved term to a Ref (subject/predicate position).
///
/// Like `lower_term`, but rejects literal values since they are not valid
/// in subject or predicate positions.
fn lower_ref_term<E: IriEncoder>(
    term: &UnresolvedTerm,
    _encoder: &E,
    vars: &mut VarRegistry,
) -> Result<Ref> {
    match term {
        UnresolvedTerm::Var(name) => {
            let var_id = vars.get_or_insert(name);
            Ok(Ref::Var(var_id))
        }
        UnresolvedTerm::Iri(iri) => Ok(Ref::Iri(iri.clone())),
        UnresolvedTerm::Literal(_) => Err(ParseError::InvalidWhere(
            "Literal values are not valid in subject or predicate position".to_string(),
        )),
    }
}

/// Convert a LiteralValue to a FlakeValue
fn lower_literal(lit: &LiteralValue) -> FlakeValue {
    match lit {
        LiteralValue::String(s) => FlakeValue::String(s.to_string()),
        LiteralValue::Long(l) => FlakeValue::Long(*l),
        LiteralValue::Double(d) => FlakeValue::Double(*d),
        LiteralValue::Boolean(b) => FlakeValue::Boolean(*b),
        LiteralValue::Vector(v) => FlakeValue::Vector(v.clone()),
    }
}

// ============================================================================
// Query modifier lowering
// ============================================================================

/// Lower an unresolved sort direction to a resolved SortDirection
fn lower_sort_direction(dir: UnresolvedSortDirection) -> SortDirection {
    match dir {
        UnresolvedSortDirection::Asc => SortDirection::Ascending,
        UnresolvedSortDirection::Desc => SortDirection::Descending,
    }
}

/// Lower an unresolved sort spec to a resolved SortSpec
fn lower_sort_spec(spec: &UnresolvedSortSpec, vars: &mut VarRegistry) -> SortSpec {
    let var = vars.get_or_insert(&spec.var);
    let direction = lower_sort_direction(spec.direction);
    SortSpec { var, direction }
}

/// Lower an unresolved aggregate function to a resolved AggregateFn
fn lower_aggregate_fn(f: &UnresolvedAggregateFn) -> AggregateFn {
    match f {
        UnresolvedAggregateFn::Count => AggregateFn::Count,
        UnresolvedAggregateFn::CountDistinct => AggregateFn::CountDistinct,
        UnresolvedAggregateFn::Sum => AggregateFn::Sum,
        UnresolvedAggregateFn::Avg => AggregateFn::Avg,
        UnresolvedAggregateFn::Min => AggregateFn::Min,
        UnresolvedAggregateFn::Max => AggregateFn::Max,
        UnresolvedAggregateFn::Median => AggregateFn::Median,
        UnresolvedAggregateFn::Variance => AggregateFn::Variance,
        UnresolvedAggregateFn::Stddev => AggregateFn::Stddev,
        UnresolvedAggregateFn::GroupConcat { separator } => AggregateFn::GroupConcat {
            separator: separator.clone(),
        },
        UnresolvedAggregateFn::Sample => AggregateFn::Sample,
    }
}

/// Lower an unresolved aggregate spec to a resolved AggregateSpec
fn lower_aggregate_spec(spec: &UnresolvedAggregateSpec, vars: &mut VarRegistry) -> AggregateSpec {
    // Handle COUNT(*) - input="*" means count all rows
    if spec.input_var.as_ref() == "*" {
        // COUNT(*) uses CountAll function and has no input variable
        AggregateSpec {
            function: AggregateFn::CountAll,
            input_var: None,
            output_var: vars.get_or_insert(&spec.output_var),
            distinct: false,
        }
    } else {
        // Regular aggregate with input variable
        AggregateSpec {
            function: lower_aggregate_fn(&spec.function),
            input_var: Some(vars.get_or_insert(&spec.input_var)),
            output_var: vars.get_or_insert(&spec.output_var),
            distinct: false,
        }
    }
}

/// Lower unresolved options to resolved QueryOptions
fn lower_options(opts: &UnresolvedOptions, vars: &mut VarRegistry) -> Result<QueryOptions> {
    // Transfer reasoning modes, or use default if not specified
    let reasoning = opts.reasoning.clone().unwrap_or_default();

    Ok(QueryOptions {
        limit: opts.limit,
        offset: opts.offset,
        distinct: opts.distinct,
        order_by: opts
            .order_by
            .iter()
            .map(|s| lower_sort_spec(s, vars))
            .collect(),
        group_by: opts
            .group_by
            .iter()
            .map(|v| vars.get_or_insert(v))
            .collect(),
        aggregates: opts
            .aggregates
            .iter()
            .map(|a| lower_aggregate_spec(a, vars))
            .collect(),
        having: opts
            .having
            .as_ref()
            .map(|e| lower_filter_expr(e, vars))
            .transpose()?,
        post_binds: Vec::new(),
        reasoning,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::encode::MemoryEncoder;
    use fluree_vocab::xsd;

    fn test_encoder() -> MemoryEncoder {
        let mut encoder = MemoryEncoder::with_common_namespaces();
        encoder.add_namespace("http://schema.org/", 100);
        encoder.add_namespace("http://example.org/", 101);
        encoder
    }

    #[test]
    fn test_lower_variable_term() {
        let encoder = test_encoder();
        let mut vars = VarRegistry::new();

        let term = UnresolvedTerm::var("?name");
        let lowered = lower_term(&term, &encoder, &mut vars).unwrap();

        assert!(matches!(lowered, Term::Var(VarId(0))));
        assert_eq!(vars.name(VarId(0)), "?name");
    }

    #[test]
    fn test_lower_iri_term() {
        let encoder = test_encoder();
        let mut vars = VarRegistry::new();

        let term = UnresolvedTerm::iri("http://schema.org/name");
        let lowered = lower_term(&term, &encoder, &mut vars).unwrap();

        // IRI terms are now lowered to Term::Iri (not Term::Sid) to defer encoding
        // to scan time, enabling correct cross-ledger joins.
        if let Term::Iri(iri) = lowered {
            assert_eq!(iri.as_ref(), "http://schema.org/name");
        } else {
            panic!("Expected Term::Iri, got {lowered:?}");
        }
    }

    #[test]
    fn test_lower_literal_terms() {
        let encoder = test_encoder();
        let mut vars = VarRegistry::new();

        // String
        let term = UnresolvedTerm::string("hello");
        let lowered = lower_term(&term, &encoder, &mut vars).unwrap();
        assert!(matches!(lowered, Term::Value(FlakeValue::String(s)) if s == "hello"));

        // Long
        let term = UnresolvedTerm::long(42);
        let lowered = lower_term(&term, &encoder, &mut vars).unwrap();
        assert!(matches!(lowered, Term::Value(FlakeValue::Long(42))));

        // Double
        let term = UnresolvedTerm::double(3.13);
        let lowered = lower_term(&term, &encoder, &mut vars).unwrap();
        assert!(
            matches!(lowered, Term::Value(FlakeValue::Double(d)) if (d - 3.13).abs() < f64::EPSILON)
        );

        // Boolean
        let term = UnresolvedTerm::boolean(true);
        let lowered = lower_term(&term, &encoder, &mut vars).unwrap();
        assert!(matches!(lowered, Term::Value(FlakeValue::Boolean(true))));
    }

    #[test]
    fn test_lower_pattern() {
        let encoder = test_encoder();
        let mut vars = VarRegistry::new();

        let pattern = UnresolvedTriplePattern::new(
            UnresolvedTerm::var("?s"),
            UnresolvedTerm::iri("http://schema.org/name"),
            UnresolvedTerm::var("?name"),
        );

        let lowered = lower_triple_pattern(&pattern, &encoder, &mut vars).unwrap();

        assert_eq!(lowered.s.as_var(), Some(VarId(0)));
        // Predicate IRI is lowered to Ref::Iri for deferred encoding
        assert_eq!(lowered.p.as_iri(), Some("http://schema.org/name"));
        assert!(matches!(lowered.o, Term::Var(VarId(1))));
        assert!(lowered.dtc.is_none());
    }

    #[test]
    fn test_lower_pattern_with_dt() {
        let encoder = test_encoder();
        let mut vars = VarRegistry::new();

        let pattern = UnresolvedTriplePattern::with_dt(
            UnresolvedTerm::var("?s"),
            UnresolvedTerm::iri("http://schema.org/age"),
            UnresolvedTerm::var("?age"),
            xsd::INTEGER,
        );

        let lowered = lower_triple_pattern(&pattern, &encoder, &mut vars).unwrap();

        let DatatypeConstraint::Explicit(sid) = lowered.dtc.expect("should have dtc") else {
            panic!("should be Explicit");
        };
        assert_eq!(sid.namespace_code, 2);
        assert_eq!(sid.name.as_ref(), "integer");
    }

    #[test]
    fn test_lower_query() {
        let encoder = test_encoder();
        let mut vars = VarRegistry::new();

        let mut ast = UnresolvedQuery::new(ParsedContext::new());
        ast.add_select("?s");
        ast.add_select("?name");
        ast.add_pattern(UnresolvedTriplePattern::new(
            UnresolvedTerm::var("?s"),
            UnresolvedTerm::iri("http://schema.org/name"),
            UnresolvedTerm::var("?name"),
        ));

        let query = lower_query(ast, &encoder, &mut vars, SelectMode::Many).unwrap();

        assert_eq!(query.output.select_vars().unwrap().len(), 2);
        assert_eq!(query.patterns.len(), 1);
        assert_eq!(vars.len(), 2);
        assert!(matches!(query.output, QueryOutput::Select { .. }));
    }

    #[test]
    fn test_unknown_namespace_becomes_term_iri() {
        let encoder = test_encoder();
        let mut vars = VarRegistry::new();

        // Unknown namespace IRIs are now lowered to Term::Iri (not an error).
        // Whether the IRI can be encoded is deferred to scan time, where
        // unknown IRIs simply produce no matches in that ledger.
        let term = UnresolvedTerm::iri("http://unknown.org/thing");
        let result = lower_term(&term, &encoder, &mut vars);

        assert!(result.is_ok());
        let lowered = result.unwrap();
        assert!(
            matches!(lowered, Term::Iri(ref iri) if iri.as_ref() == "http://unknown.org/thing")
        );
    }

    #[test]
    fn test_variable_deduplication() {
        let encoder = test_encoder();
        let mut vars = VarRegistry::new();

        // Same variable used twice should get same VarId
        let term1 = UnresolvedTerm::var("?s");
        let term2 = UnresolvedTerm::var("?s");

        let lowered1 = lower_term(&term1, &encoder, &mut vars).unwrap();
        let lowered2 = lower_term(&term2, &encoder, &mut vars).unwrap();

        assert_eq!(lowered1, lowered2);
        assert_eq!(vars.len(), 1);
    }

    // ==========================================================================
    // Typed literal coercion tests
    // ==========================================================================

    #[test]
    fn test_coerce_string_to_integer() {
        let result = coerce_value_by_datatype(FlakeValue::String("42".to_string()), xsd::INTEGER);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), FlakeValue::Long(42));
    }

    #[test]
    fn test_coerce_string_to_bigint() {
        // String too large for i64
        let big_num = "99999999999999999999999999999";
        let result =
            coerce_value_by_datatype(FlakeValue::String(big_num.to_string()), xsd::INTEGER);
        assert!(result.is_ok());
        match result.unwrap() {
            FlakeValue::BigInt(bi) => {
                assert_eq!(bi.to_string(), big_num);
            }
            other => panic!("Expected BigInt, got {other:?}"),
        }
    }

    #[test]
    fn test_coerce_string_to_decimal_becomes_bigdecimal() {
        let result = coerce_value_by_datatype(
            FlakeValue::String("3.14159265358979323846".to_string()),
            xsd::DECIMAL,
        );
        assert!(result.is_ok());
        match result.unwrap() {
            FlakeValue::Decimal(bd) => {
                // Verify precision is preserved
                assert!(bd.to_string().starts_with("3.14159265358979"));
            }
            other => panic!("Expected Decimal, got {other:?}"),
        }
    }

    #[test]
    fn test_coerce_json_long_to_decimal_becomes_double() {
        // JSON numbers with xsd:decimal → Double (per policy)
        let result = coerce_value_by_datatype(FlakeValue::Long(42), xsd::DECIMAL);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), FlakeValue::Double(42.0));
    }

    #[test]
    fn test_coerce_string_to_double() {
        let result = coerce_value_by_datatype(FlakeValue::String("3.13".to_string()), xsd::DOUBLE);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), FlakeValue::Double(3.13));
    }

    #[test]
    fn test_coerce_string_to_boolean() {
        let result_true =
            coerce_value_by_datatype(FlakeValue::String("true".to_string()), xsd::BOOLEAN);
        assert_eq!(result_true.unwrap(), FlakeValue::Boolean(true));

        let result_false =
            coerce_value_by_datatype(FlakeValue::String("false".to_string()), xsd::BOOLEAN);
        assert_eq!(result_false.unwrap(), FlakeValue::Boolean(false));

        let result_one =
            coerce_value_by_datatype(FlakeValue::String("1".to_string()), xsd::BOOLEAN);
        assert_eq!(result_one.unwrap(), FlakeValue::Boolean(true));

        let result_zero =
            coerce_value_by_datatype(FlakeValue::String("0".to_string()), xsd::BOOLEAN);
        assert_eq!(result_zero.unwrap(), FlakeValue::Boolean(false));
    }

    // ==========================================================================
    // Incompatible type coercion errors
    // ==========================================================================

    #[test]
    fn test_coerce_number_to_string_errors() {
        let result = coerce_value_by_datatype(FlakeValue::Long(42), xsd::STRING);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ParseError::TypeCoercion(_)));
    }

    #[test]
    fn test_coerce_double_to_string_errors() {
        let result = coerce_value_by_datatype(FlakeValue::Double(3.13), xsd::STRING);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ParseError::TypeCoercion(_)));
    }

    #[test]
    fn test_coerce_boolean_to_string_errors() {
        let result = coerce_value_by_datatype(FlakeValue::Boolean(true), xsd::STRING);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ParseError::TypeCoercion(_)));
    }

    #[test]
    fn test_coerce_number_to_boolean_errors() {
        let result = coerce_value_by_datatype(FlakeValue::Long(1), xsd::BOOLEAN);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ParseError::TypeCoercion(_)));
    }

    #[test]
    fn test_coerce_double_to_boolean_errors() {
        let result = coerce_value_by_datatype(FlakeValue::Double(1.0), xsd::BOOLEAN);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ParseError::TypeCoercion(_)));
    }

    #[test]
    fn test_coerce_non_integral_double_to_integer_errors() {
        let result = coerce_value_by_datatype(FlakeValue::Double(3.13), xsd::INTEGER);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ParseError::TypeCoercion(_)));
        assert!(err.to_string().contains("non-integer"));
    }

    #[test]
    fn test_coerce_integral_double_to_integer_succeeds() {
        let result = coerce_value_by_datatype(FlakeValue::Double(42.0), xsd::INTEGER);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), FlakeValue::Long(42));
    }

    #[test]
    fn test_coerce_boolean_to_numeric_errors() {
        let result = coerce_value_by_datatype(FlakeValue::Boolean(true), xsd::INTEGER);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ParseError::TypeCoercion(_)));
    }

    #[test]
    fn test_coerce_number_to_temporal_errors() {
        let result = coerce_value_by_datatype(FlakeValue::Long(12345), xsd::DATE_TIME);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ParseError::TypeCoercion(_)));
    }

    #[test]
    fn test_coerce_invalid_string_to_integer_errors() {
        let result =
            coerce_value_by_datatype(FlakeValue::String("not-a-number".to_string()), xsd::INTEGER);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ParseError::TypeCoercion(_)));
    }

    #[test]
    fn test_coerce_invalid_string_to_decimal_errors() {
        let result =
            coerce_value_by_datatype(FlakeValue::String("not-a-number".to_string()), xsd::DECIMAL);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ParseError::TypeCoercion(_)));
    }

    #[test]
    fn test_coerce_invalid_string_to_boolean_errors() {
        let result =
            coerce_value_by_datatype(FlakeValue::String("maybe".to_string()), xsd::BOOLEAN);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ParseError::TypeCoercion(_)));
    }

    #[test]
    fn test_coerce_passthrough_same_type() {
        // Already correct type - should pass through unchanged
        let result = coerce_value_by_datatype(FlakeValue::Long(42), xsd::INTEGER);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), FlakeValue::Long(42));

        let result2 =
            coerce_value_by_datatype(FlakeValue::String("hello".to_string()), xsd::STRING);
        assert!(result2.is_ok());
        assert_eq!(result2.unwrap(), FlakeValue::String("hello".to_string()));
    }

    // ==========================================================================
    // Inverse-transitive (^p+ / ^p*) property path lowering tests
    // ==========================================================================

    #[test]
    fn test_inverse_one_or_more() {
        let encoder = test_encoder();
        let mut vars = VarRegistry::new();
        let mut pp_counter: u32 = 0;

        // ^ex:knows+ — inverse of one-or-more
        let path = UnresolvedPathExpr::Inverse(Box::new(UnresolvedPathExpr::OneOrMore(Box::new(
            UnresolvedPathExpr::Iri(Arc::from("http://example.org/knows")),
        ))));
        let pattern = UnresolvedPattern::Path {
            subject: UnresolvedTerm::var("?s"),
            path,
            object: UnresolvedTerm::var("?o"),
        };

        let results =
            lower_unresolved_pattern(&pattern, &encoder, &mut vars, &mut pp_counter).unwrap();

        assert_eq!(results.len(), 1);
        if let Pattern::PropertyPath(pp) = &results[0] {
            assert!(matches!(pp.modifier, PathModifier::OneOrMore));
            // Subject/object are swapped: pp.subject = ?o, pp.object = ?s
            assert_eq!(pp.subject.as_var().map(|v| vars.name(v)), Some("?o"));
            assert_eq!(pp.object.as_var().map(|v| vars.name(v)), Some("?s"));
        } else {
            panic!("Expected PropertyPath, got {:?}", results[0]);
        }
    }

    #[test]
    fn test_inverse_zero_or_more() {
        let encoder = test_encoder();
        let mut vars = VarRegistry::new();
        let mut pp_counter: u32 = 0;

        // ^ex:knows* — inverse of zero-or-more
        let path = UnresolvedPathExpr::Inverse(Box::new(UnresolvedPathExpr::ZeroOrMore(Box::new(
            UnresolvedPathExpr::Iri(Arc::from("http://example.org/knows")),
        ))));
        let pattern = UnresolvedPattern::Path {
            subject: UnresolvedTerm::var("?s"),
            path,
            object: UnresolvedTerm::var("?o"),
        };

        let results =
            lower_unresolved_pattern(&pattern, &encoder, &mut vars, &mut pp_counter).unwrap();

        assert_eq!(results.len(), 1);
        if let Pattern::PropertyPath(pp) = &results[0] {
            assert!(matches!(pp.modifier, PathModifier::ZeroOrMore));
            // Subject/object swapped
            assert_eq!(pp.subject.as_var().map(|v| vars.name(v)), Some("?o"));
            assert_eq!(pp.object.as_var().map(|v| vars.name(v)), Some("?s"));
        } else {
            panic!("Expected PropertyPath, got {:?}", results[0]);
        }
    }

    // ==========================================================================
    // Sequence (/) property path lowering tests
    // ==========================================================================

    fn make_sequence_path(iris: &[&str]) -> UnresolvedPathExpr {
        UnresolvedPathExpr::Sequence(
            iris.iter()
                .map(|iri| UnresolvedPathExpr::Iri(Arc::from(*iri)))
                .collect(),
        )
    }

    fn make_path_pattern(
        subject: &str,
        path: UnresolvedPathExpr,
        object: &str,
    ) -> UnresolvedPattern {
        UnresolvedPattern::Path {
            subject: UnresolvedTerm::var(subject),
            path,
            object: UnresolvedTerm::var(object),
        }
    }

    fn extract_triple(pattern: &Pattern) -> &TriplePattern {
        match pattern {
            Pattern::Triple(tp) => tp,
            other => panic!("Expected Pattern::Triple, got {other:?}"),
        }
    }

    #[test]
    fn test_sequence_two_step() {
        let encoder = test_encoder();
        let mut vars = VarRegistry::new();
        let mut pp_counter: u32 = 0;

        let path = make_sequence_path(&["http://example.org/friend", "http://example.org/name"]);
        let pattern = make_path_pattern("?s", path, "?name");

        let results =
            lower_unresolved_pattern(&pattern, &encoder, &mut vars, &mut pp_counter).unwrap();

        assert_eq!(results.len(), 2);

        // First triple: ?s --ex:friend--> ?__pp0
        let t0 = extract_triple(&results[0]);
        assert!(t0.s.is_var());
        assert_eq!(t0.p.as_iri(), Some("http://example.org/friend"));
        assert!(matches!(&t0.o, Term::Var(vid) if vars.name(*vid) == "?__pp0"));

        // Second triple: ?__pp0 --ex:name--> ?name
        let t1 = extract_triple(&results[1]);
        assert_eq!(t1.s.as_var().map(|v| vars.name(v)), Some("?__pp0"));
        assert_eq!(t1.p.as_iri(), Some("http://example.org/name"));
        assert!(matches!(&t1.o, Term::Var(vid) if vars.name(*vid) == "?name"));

        assert_eq!(pp_counter, 1);
    }

    #[test]
    fn test_sequence_three_step() {
        let encoder = test_encoder();
        let mut vars = VarRegistry::new();
        let mut pp_counter: u32 = 0;

        let path = make_sequence_path(&[
            "http://example.org/a",
            "http://example.org/b",
            "http://example.org/c",
        ]);
        let pattern = make_path_pattern("?s", path, "?o");

        let results =
            lower_unresolved_pattern(&pattern, &encoder, &mut vars, &mut pp_counter).unwrap();

        assert_eq!(results.len(), 3);

        // First: ?s --a--> ?__pp0
        let t0 = extract_triple(&results[0]);
        assert!(matches!(&t0.o, Term::Var(vid) if vars.name(*vid) == "?__pp0"));

        // Second: ?__pp0 --b--> ?__pp1
        let t1 = extract_triple(&results[1]);
        assert_eq!(t1.s.as_var().map(|v| vars.name(v)), Some("?__pp0"));
        assert!(matches!(&t1.o, Term::Var(vid) if vars.name(*vid) == "?__pp1"));

        // Third: ?__pp1 --c--> ?o
        let t2 = extract_triple(&results[2]);
        assert_eq!(t2.s.as_var().map(|v| vars.name(v)), Some("?__pp1"));
        assert!(matches!(&t2.o, Term::Var(vid) if vars.name(*vid) == "?o"));

        assert_eq!(pp_counter, 2);
    }

    #[test]
    fn test_sequence_inverse_step() {
        let encoder = test_encoder();
        let mut vars = VarRegistry::new();
        let mut pp_counter: u32 = 0;

        // ^ex:parent / ex:name
        let path = UnresolvedPathExpr::Sequence(vec![
            UnresolvedPathExpr::Inverse(Box::new(UnresolvedPathExpr::Iri(Arc::from(
                "http://example.org/parent",
            )))),
            UnresolvedPathExpr::Iri(Arc::from("http://example.org/name")),
        ]);
        let pattern = make_path_pattern("?s", path, "?name");

        let results =
            lower_unresolved_pattern(&pattern, &encoder, &mut vars, &mut pp_counter).unwrap();

        assert_eq!(results.len(), 2);

        // First triple is inverse: ?__pp0 --ex:parent--> ?s (swapped)
        let t0 = extract_triple(&results[0]);
        assert_eq!(t0.s.as_var().map(|v| vars.name(v)), Some("?__pp0"));
        assert_eq!(t0.p.as_iri(), Some("http://example.org/parent"));
        assert!(matches!(&t0.o, Term::Var(vid) if vars.name(*vid) == "?s"));

        // Second triple is forward: ?__pp0 --ex:name--> ?name
        let t1 = extract_triple(&results[1]);
        assert_eq!(t1.s.as_var().map(|v| vars.name(v)), Some("?__pp0"));
        assert_eq!(t1.p.as_iri(), Some("http://example.org/name"));
    }

    #[test]
    fn test_sequence_all_inverse() {
        let encoder = test_encoder();
        let mut vars = VarRegistry::new();
        let mut pp_counter: u32 = 0;

        // ^ex:a / ^ex:b
        let path = UnresolvedPathExpr::Sequence(vec![
            UnresolvedPathExpr::Inverse(Box::new(UnresolvedPathExpr::Iri(Arc::from(
                "http://example.org/a",
            )))),
            UnresolvedPathExpr::Inverse(Box::new(UnresolvedPathExpr::Iri(Arc::from(
                "http://example.org/b",
            )))),
        ]);
        let pattern = make_path_pattern("?s", path, "?o");

        let results =
            lower_unresolved_pattern(&pattern, &encoder, &mut vars, &mut pp_counter).unwrap();

        assert_eq!(results.len(), 2);

        // First: ?__pp0 --a--> ?s (swapped)
        let t0 = extract_triple(&results[0]);
        assert_eq!(t0.s.as_var().map(|v| vars.name(v)), Some("?__pp0"));
        assert!(matches!(&t0.o, Term::Var(vid) if vars.name(*vid) == "?s"));

        // Second: ?o --b--> ?__pp0 (swapped)
        let t1 = extract_triple(&results[1]);
        assert_eq!(t1.s.as_var().map(|v| vars.name(v)), Some("?o"));
        assert!(matches!(&t1.o, Term::Var(vid) if vars.name(*vid) == "?__pp0"));
    }

    #[test]
    fn test_sequence_transitive_step_allowed() {
        let encoder = test_encoder();
        let mut vars = VarRegistry::new();
        let mut pp_counter: u32 = 0;

        // ex:a+ / ex:b — transitive modifier inside sequence is allowed
        let path = UnresolvedPathExpr::Sequence(vec![
            UnresolvedPathExpr::OneOrMore(Box::new(UnresolvedPathExpr::Iri(Arc::from(
                "http://example.org/a",
            )))),
            UnresolvedPathExpr::Iri(Arc::from("http://example.org/b")),
        ]);
        let pattern = make_path_pattern("?s", path, "?o");

        let results =
            lower_unresolved_pattern(&pattern, &encoder, &mut vars, &mut pp_counter).unwrap();

        assert_eq!(results.len(), 2);

        // Step 0: PropertyPath(?s, a, +, ?__pp0)
        match &results[0] {
            Pattern::PropertyPath(pp) => {
                assert!(matches!(pp.modifier, PathModifier::OneOrMore));
                assert_eq!(pp.subject.as_var().map(|v| vars.name(v)), Some("?s"));
                assert_eq!(pp.predicate.name_str(), "a");
                assert_eq!(pp.object.as_var().map(|v| vars.name(v)), Some("?__pp0"));
            }
            other => panic!("Expected PropertyPath, got {other:?}"),
        }

        // Step 1: Triple(?__pp0, b, ?o)
        let t1 = extract_triple(&results[1]);
        assert_eq!(t1.s.as_var().map(|v| vars.name(v)), Some("?__pp0"));
        assert_eq!(t1.p.as_iri(), Some("http://example.org/b"));
        assert!(matches!(&t1.o, Term::Var(vid) if vars.name(*vid) == "?o"));
    }

    #[test]
    fn test_sequence_with_alternative_step_distributes() {
        let encoder = test_encoder();
        let mut vars = VarRegistry::new();
        let mut pp_counter: u32 = 0;

        // (ex:a|ex:b) / ex:c → Union([ a/c, b/c ])
        let path = UnresolvedPathExpr::Sequence(vec![
            UnresolvedPathExpr::Alternative(vec![
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/a")),
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/b")),
            ]),
            UnresolvedPathExpr::Iri(Arc::from("http://example.org/c")),
        ]);
        let pattern = make_path_pattern("?s", path, "?o");

        let results =
            lower_unresolved_pattern(&pattern, &encoder, &mut vars, &mut pp_counter).unwrap();

        // Should produce a single Union with 2 branches
        assert_eq!(results.len(), 1);
        if let Pattern::Union(branches) = &results[0] {
            assert_eq!(branches.len(), 2);

            // Branch 0: a/c → Triple(?s, a, ?__pp0), Triple(?__pp0, c, ?o)
            assert_eq!(branches[0].len(), 2);
            let t0 = extract_triple(&branches[0][0]);
            assert_eq!(t0.s.as_var().map(|v| vars.name(v)), Some("?s"));
            assert_eq!(t0.p.as_iri(), Some("http://example.org/a"));
            assert!(matches!(&t0.o, Term::Var(vid) if vars.name(*vid) == "?__pp0"));

            let t1 = extract_triple(&branches[0][1]);
            assert_eq!(t1.s.as_var().map(|v| vars.name(v)), Some("?__pp0"));
            assert_eq!(t1.p.as_iri(), Some("http://example.org/c"));
            assert!(matches!(&t1.o, Term::Var(vid) if vars.name(*vid) == "?o"));

            // Branch 1: b/c → Triple(?s, b, ?__pp1), Triple(?__pp1, c, ?o)
            assert_eq!(branches[1].len(), 2);
            let t2 = extract_triple(&branches[1][0]);
            assert_eq!(t2.s.as_var().map(|v| vars.name(v)), Some("?s"));
            assert_eq!(t2.p.as_iri(), Some("http://example.org/b"));
            assert!(matches!(&t2.o, Term::Var(vid) if vars.name(*vid) == "?__pp1"));

            let t3 = extract_triple(&branches[1][1]);
            assert_eq!(t3.s.as_var().map(|v| vars.name(v)), Some("?__pp1"));
            assert_eq!(t3.p.as_iri(), Some("http://example.org/c"));
            assert!(matches!(&t3.o, Term::Var(vid) if vars.name(*vid) == "?o"));
        } else {
            panic!("Expected Pattern::Union, got {:?}", results[0]);
        }
    }

    // ==========================================================================
    // Sequence-in-Alternative property path lowering tests
    // ==========================================================================

    #[test]
    fn test_alternative_with_sequence_branches() {
        let encoder = test_encoder();
        let mut vars = VarRegistry::new();
        let mut pp_counter: u32 = 0;

        // ex:friend/ex:name | ex:colleague/ex:name
        let path = UnresolvedPathExpr::Alternative(vec![
            UnresolvedPathExpr::Sequence(vec![
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/friend")),
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/name")),
            ]),
            UnresolvedPathExpr::Sequence(vec![
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/colleague")),
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/name")),
            ]),
        ]);
        let pattern = make_path_pattern("?s", path, "?name");

        let results =
            lower_unresolved_pattern(&pattern, &encoder, &mut vars, &mut pp_counter).unwrap();

        // Should produce a single Union with 2 branches
        assert_eq!(results.len(), 1);
        if let Pattern::Union(branches) = &results[0] {
            assert_eq!(branches.len(), 2);

            // Each branch should have 2 triple patterns (two-step chain)
            for (i, branch) in branches.iter().enumerate() {
                assert_eq!(
                    branch.len(),
                    2,
                    "Branch {} should have 2 triples, got {}",
                    i,
                    branch.len()
                );
                assert!(matches!(branch[0], Pattern::Triple(_)));
                assert!(matches!(branch[1], Pattern::Triple(_)));
            }

            // First branch: ?s --friend--> ?__pp0 --name--> ?name
            let t0 = extract_triple(&branches[0][0]);
            assert_eq!(t0.p.as_iri(), Some("http://example.org/friend"));
            assert!(matches!(&t0.o, Term::Var(vid) if vars.name(*vid) == "?__pp0"));
            let t1 = extract_triple(&branches[0][1]);
            assert_eq!(t1.s.as_var().map(|v| vars.name(v)), Some("?__pp0"));
            assert_eq!(t1.p.as_iri(), Some("http://example.org/name"));

            // Second branch: ?s --colleague--> ?__pp1 --name--> ?name
            let t2 = extract_triple(&branches[1][0]);
            assert_eq!(t2.p.as_iri(), Some("http://example.org/colleague"));
            assert!(matches!(&t2.o, Term::Var(vid) if vars.name(*vid) == "?__pp1"));
            let t3 = extract_triple(&branches[1][1]);
            assert_eq!(t3.s.as_var().map(|v| vars.name(v)), Some("?__pp1"));
            assert_eq!(t3.p.as_iri(), Some("http://example.org/name"));
        } else {
            panic!("Expected Union, got {:?}", results[0]);
        }

        assert_eq!(pp_counter, 2);
    }

    #[test]
    fn test_alternative_mixed_simple_and_sequence() {
        let encoder = test_encoder();
        let mut vars = VarRegistry::new();
        let mut pp_counter: u32 = 0;

        // ex:name | ex:friend/ex:name — one simple IRI, one sequence
        let path = UnresolvedPathExpr::Alternative(vec![
            UnresolvedPathExpr::Iri(Arc::from("http://example.org/name")),
            UnresolvedPathExpr::Sequence(vec![
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/friend")),
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/name")),
            ]),
        ]);
        let pattern = make_path_pattern("?s", path, "?val");

        let results =
            lower_unresolved_pattern(&pattern, &encoder, &mut vars, &mut pp_counter).unwrap();

        assert_eq!(results.len(), 1);
        if let Pattern::Union(branches) = &results[0] {
            assert_eq!(branches.len(), 2);
            // First: simple IRI → 1 triple
            assert_eq!(branches[0].len(), 1);
            assert!(matches!(branches[0][0], Pattern::Triple(_)));
            // Second: sequence → 2 triples
            assert_eq!(branches[1].len(), 2);
            assert!(matches!(branches[1][0], Pattern::Triple(_)));
            assert!(matches!(branches[1][1], Pattern::Triple(_)));
        } else {
            panic!("Expected Union, got {:?}", results[0]);
        }

        assert_eq!(pp_counter, 1);
    }

    #[test]
    fn test_alternative_with_three_way_sequence() {
        let encoder = test_encoder();
        let mut vars = VarRegistry::new();
        let mut pp_counter: u32 = 0;

        // ex:name | ex:friend/ex:name | ^ex:colleague
        let path = UnresolvedPathExpr::Alternative(vec![
            UnresolvedPathExpr::Iri(Arc::from("http://example.org/name")),
            UnresolvedPathExpr::Sequence(vec![
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/friend")),
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/name")),
            ]),
            UnresolvedPathExpr::Inverse(Box::new(UnresolvedPathExpr::Iri(Arc::from(
                "http://example.org/colleague",
            )))),
        ]);
        let pattern = make_path_pattern("?s", path, "?val");

        let results =
            lower_unresolved_pattern(&pattern, &encoder, &mut vars, &mut pp_counter).unwrap();

        assert_eq!(results.len(), 1);
        if let Pattern::Union(branches) = &results[0] {
            assert_eq!(branches.len(), 3);
            // Branch 0: simple IRI → 1 triple
            assert_eq!(branches[0].len(), 1);
            // Branch 1: sequence → 2 triples
            assert_eq!(branches[1].len(), 2);
            // Branch 2: inverse → 1 triple
            assert_eq!(branches[2].len(), 1);
        } else {
            panic!("Expected Union, got {:?}", results[0]);
        }
    }

    // ==========================================================================
    // Alternative-in-Sequence distribution tests
    // ==========================================================================

    #[test]
    fn test_sequence_with_middle_alternative() {
        let encoder = test_encoder();
        let mut vars = VarRegistry::new();
        let mut pp_counter: u32 = 0;

        // a / (b|c) / d → Union([ a/b/d, a/c/d ])
        let path = UnresolvedPathExpr::Sequence(vec![
            UnresolvedPathExpr::Iri(Arc::from("http://example.org/a")),
            UnresolvedPathExpr::Alternative(vec![
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/b")),
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/c")),
            ]),
            UnresolvedPathExpr::Iri(Arc::from("http://example.org/d")),
        ]);
        let pattern = make_path_pattern("?s", path, "?o");

        let results =
            lower_unresolved_pattern(&pattern, &encoder, &mut vars, &mut pp_counter).unwrap();

        assert_eq!(results.len(), 1);
        if let Pattern::Union(branches) = &results[0] {
            assert_eq!(branches.len(), 2);

            // Each branch should be a 3-step chain (3 triples, 2 join vars)
            for (i, branch) in branches.iter().enumerate() {
                assert_eq!(branch.len(), 3, "Branch {i} should have 3 triple patterns");
                for (j, pat) in branch.iter().enumerate() {
                    assert!(
                        matches!(pat, Pattern::Triple(_)),
                        "Branch {i} pattern {j} should be Triple"
                    );
                }
            }

            // Branch 0 middle step should use predicate b
            let t0_mid = extract_triple(&branches[0][1]);
            assert_eq!(t0_mid.p.as_iri(), Some("http://example.org/b"));

            // Branch 1 middle step should use predicate c
            let t1_mid = extract_triple(&branches[1][1]);
            assert_eq!(t1_mid.p.as_iri(), Some("http://example.org/c"));
        } else {
            panic!("Expected Pattern::Union, got {:?}", results[0]);
        }
    }

    #[test]
    fn test_sequence_with_multiple_alternatives() {
        let encoder = test_encoder();
        let mut vars = VarRegistry::new();
        let mut pp_counter: u32 = 0;

        // (a|b) / (c|d) → Union([ a/c, a/d, b/c, b/d ])
        let path = UnresolvedPathExpr::Sequence(vec![
            UnresolvedPathExpr::Alternative(vec![
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/a")),
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/b")),
            ]),
            UnresolvedPathExpr::Alternative(vec![
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/c")),
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/d")),
            ]),
        ]);
        let pattern = make_path_pattern("?s", path, "?o");

        let results =
            lower_unresolved_pattern(&pattern, &encoder, &mut vars, &mut pp_counter).unwrap();

        assert_eq!(results.len(), 1);
        if let Pattern::Union(branches) = &results[0] {
            assert_eq!(branches.len(), 4);

            // Each branch should be a 2-step chain
            for (i, branch) in branches.iter().enumerate() {
                assert_eq!(branch.len(), 2, "Branch {i} should have 2 triples");
            }

            // Verify the 4 combinations: a/c, a/d, b/c, b/d
            let combos: Vec<(String, String)> = branches
                .iter()
                .map(|branch| {
                    let t0 = extract_triple(&branch[0]);
                    let t1 = extract_triple(&branch[1]);
                    let p0 = match &t0.p {
                        Ref::Iri(iri) => iri.to_string(),
                        _ => panic!("Expected IRI"),
                    };
                    let p1 = match &t1.p {
                        Ref::Iri(iri) => iri.to_string(),
                        _ => panic!("Expected IRI"),
                    };
                    (p0, p1)
                })
                .collect();

            assert_eq!(
                combos[0],
                ("http://example.org/a".into(), "http://example.org/c".into())
            );
            assert_eq!(
                combos[1],
                ("http://example.org/a".into(), "http://example.org/d".into())
            );
            assert_eq!(
                combos[2],
                ("http://example.org/b".into(), "http://example.org/c".into())
            );
            assert_eq!(
                combos[3],
                ("http://example.org/b".into(), "http://example.org/d".into())
            );
        } else {
            panic!("Expected Pattern::Union, got {:?}", results[0]);
        }

        // 4 branches × 1 join var each = 4 join vars total
        assert_eq!(pp_counter, 4);
    }

    #[test]
    fn test_sequence_alternative_with_inverse() {
        let encoder = test_encoder();
        let mut vars = VarRegistry::new();
        let mut pp_counter: u32 = 0;

        // (a|^b) / c → Union([ a/c, ^b/c ])
        let path = UnresolvedPathExpr::Sequence(vec![
            UnresolvedPathExpr::Alternative(vec![
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/a")),
                UnresolvedPathExpr::Inverse(Box::new(UnresolvedPathExpr::Iri(Arc::from(
                    "http://example.org/b",
                )))),
            ]),
            UnresolvedPathExpr::Iri(Arc::from("http://example.org/c")),
        ]);
        let pattern = make_path_pattern("?s", path, "?o");

        let results =
            lower_unresolved_pattern(&pattern, &encoder, &mut vars, &mut pp_counter).unwrap();

        assert_eq!(results.len(), 1);
        if let Pattern::Union(branches) = &results[0] {
            assert_eq!(branches.len(), 2);

            // Branch 0: a/c → Triple(?s, a, ?__pp0), Triple(?__pp0, c, ?o)
            let t0 = extract_triple(&branches[0][0]);
            assert_eq!(t0.s.as_var().map(|v| vars.name(v)), Some("?s"));
            assert_eq!(t0.p.as_iri(), Some("http://example.org/a"));

            // Branch 1: ^b/c → Triple(?__pp1, b, ?s), Triple(?__pp1, c, ?o)
            // Inverse swaps: subject=next(?__pp1), object=prev(?s)
            let t2 = extract_triple(&branches[1][0]);
            assert_eq!(t2.s.as_var().map(|v| vars.name(v)), Some("?__pp1"));
            assert_eq!(t2.p.as_iri(), Some("http://example.org/b"));
            assert!(matches!(&t2.o, Term::Var(vid) if vars.name(*vid) == "?s"));
        } else {
            panic!("Expected Pattern::Union, got {:?}", results[0]);
        }
    }

    #[test]
    fn test_sequence_expansion_limit() {
        let encoder = test_encoder();
        let mut vars = VarRegistry::new();
        let mut pp_counter: u32 = 0;

        // Build a sequence with 7 alternative steps, each with 2 choices:
        // 2^7 = 128 > 64 limit
        let steps: Vec<UnresolvedPathExpr> = (0..7)
            .map(|i| {
                UnresolvedPathExpr::Alternative(vec![
                    UnresolvedPathExpr::Iri(Arc::from(format!("http://example.org/a{i}").as_str())),
                    UnresolvedPathExpr::Iri(Arc::from(format!("http://example.org/b{i}").as_str())),
                ])
            })
            .collect();
        let path = UnresolvedPathExpr::Sequence(steps);
        let pattern = make_path_pattern("?s", path, "?o");

        let result = lower_unresolved_pattern(&pattern, &encoder, &mut vars, &mut pp_counter);

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("expands to 128") && err.contains("limit 64"),
            "Unexpected error: {err}",
        );
    }

    #[test]
    fn test_sequence_distributed_bag_semantics() {
        let encoder = test_encoder();
        let mut vars = VarRegistry::new();
        let mut pp_counter: u32 = 0;

        // (a|a) / c — both alternatives are the same IRI
        // Distribution is syntactic, so we still get 2 branches
        let path = UnresolvedPathExpr::Sequence(vec![
            UnresolvedPathExpr::Alternative(vec![
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/a")),
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/a")),
            ]),
            UnresolvedPathExpr::Iri(Arc::from("http://example.org/c")),
        ]);
        let pattern = make_path_pattern("?s", path, "?o");

        let results =
            lower_unresolved_pattern(&pattern, &encoder, &mut vars, &mut pp_counter).unwrap();

        assert_eq!(results.len(), 1);
        if let Pattern::Union(branches) = &results[0] {
            // Syntactic distribution: 2 branches even though both use same predicate
            assert_eq!(branches.len(), 2);
            for branch in branches {
                assert_eq!(branch.len(), 2);
            }
        } else {
            panic!("Expected Pattern::Union, got {:?}", results[0]);
        }
    }

    // ==========================================================================
    // Inverse of complex paths — rewrite tests
    // ==========================================================================

    #[test]
    fn test_rewrite_inverse_of_sequence() {
        // ^(a/b) → Sequence([^b, ^a])
        let path = UnresolvedPathExpr::Inverse(Box::new(UnresolvedPathExpr::Sequence(vec![
            UnresolvedPathExpr::Iri(Arc::from("http://example.org/a")),
            UnresolvedPathExpr::Iri(Arc::from("http://example.org/b")),
        ])));
        let rewritten = rewrite_inverse_of_complex(&path).expect("should rewrite");
        match rewritten {
            UnresolvedPathExpr::Sequence(steps) => {
                assert_eq!(steps.len(), 2);
                // First step: ^b (reversed order)
                match &steps[0] {
                    UnresolvedPathExpr::Inverse(inner) => match inner.as_ref() {
                        UnresolvedPathExpr::Iri(iri) => {
                            assert_eq!(iri.as_ref(), "http://example.org/b");
                        }
                        other => panic!("Expected Iri inside Inverse, got {other:?}"),
                    },
                    other => panic!("Expected Inverse, got {other:?}"),
                }
                // Second step: ^a
                match &steps[1] {
                    UnresolvedPathExpr::Inverse(inner) => match inner.as_ref() {
                        UnresolvedPathExpr::Iri(iri) => {
                            assert_eq!(iri.as_ref(), "http://example.org/a");
                        }
                        other => panic!("Expected Iri inside Inverse, got {other:?}"),
                    },
                    other => panic!("Expected Inverse, got {other:?}"),
                }
            }
            other => panic!("Expected Sequence, got {other:?}"),
        }
    }

    #[test]
    fn test_rewrite_inverse_of_alternative() {
        // ^(a|b) → Alternative([^a, ^b])
        let path = UnresolvedPathExpr::Inverse(Box::new(UnresolvedPathExpr::Alternative(vec![
            UnresolvedPathExpr::Iri(Arc::from("http://example.org/a")),
            UnresolvedPathExpr::Iri(Arc::from("http://example.org/b")),
        ])));
        let rewritten = rewrite_inverse_of_complex(&path).expect("should rewrite");
        match rewritten {
            UnresolvedPathExpr::Alternative(branches) => {
                assert_eq!(branches.len(), 2);
                // Order preserved: ^a, ^b
                match &branches[0] {
                    UnresolvedPathExpr::Inverse(inner) => match inner.as_ref() {
                        UnresolvedPathExpr::Iri(iri) => {
                            assert_eq!(iri.as_ref(), "http://example.org/a");
                        }
                        other => panic!("Expected Iri, got {other:?}"),
                    },
                    other => panic!("Expected Inverse, got {other:?}"),
                }
                match &branches[1] {
                    UnresolvedPathExpr::Inverse(inner) => match inner.as_ref() {
                        UnresolvedPathExpr::Iri(iri) => {
                            assert_eq!(iri.as_ref(), "http://example.org/b");
                        }
                        other => panic!("Expected Iri, got {other:?}"),
                    },
                    other => panic!("Expected Inverse, got {other:?}"),
                }
            }
            other => panic!("Expected Alternative, got {other:?}"),
        }
    }

    #[test]
    fn test_rewrite_inverse_of_three_step_sequence() {
        // ^(a/b/c) → Sequence([^c, ^b, ^a])
        let path = UnresolvedPathExpr::Inverse(Box::new(UnresolvedPathExpr::Sequence(vec![
            UnresolvedPathExpr::Iri(Arc::from("http://example.org/a")),
            UnresolvedPathExpr::Iri(Arc::from("http://example.org/b")),
            UnresolvedPathExpr::Iri(Arc::from("http://example.org/c")),
        ])));
        let rewritten = rewrite_inverse_of_complex(&path).expect("should rewrite");
        match rewritten {
            UnresolvedPathExpr::Sequence(steps) => {
                assert_eq!(steps.len(), 3);
                // Reversed: c, b, a
                let expected = [
                    "http://example.org/c",
                    "http://example.org/b",
                    "http://example.org/a",
                ];
                for (i, exp_iri) in expected.iter().enumerate() {
                    match &steps[i] {
                        UnresolvedPathExpr::Inverse(inner) => match inner.as_ref() {
                            UnresolvedPathExpr::Iri(iri) => assert_eq!(iri.as_ref(), *exp_iri),
                            other => panic!("Step {i}: expected Iri, got {other:?}"),
                        },
                        other => panic!("Step {i}: expected Inverse, got {other:?}"),
                    }
                }
            }
            other => panic!("Expected Sequence, got {other:?}"),
        }
    }

    #[test]
    fn test_rewrite_inverse_nested_sequence_with_alternative() {
        // ^(a/(b|c)) → Sequence([Alternative([^b, ^c]), ^a])
        let path = UnresolvedPathExpr::Inverse(Box::new(UnresolvedPathExpr::Sequence(vec![
            UnresolvedPathExpr::Iri(Arc::from("http://example.org/a")),
            UnresolvedPathExpr::Alternative(vec![
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/b")),
                UnresolvedPathExpr::Iri(Arc::from("http://example.org/c")),
            ]),
        ])));
        let rewritten = rewrite_inverse_of_complex(&path).expect("should rewrite");
        match rewritten {
            UnresolvedPathExpr::Sequence(steps) => {
                assert_eq!(steps.len(), 2);
                // First step: Alternative([^b, ^c]) — recursive rewrite of ^(b|c)
                match &steps[0] {
                    UnresolvedPathExpr::Alternative(branches) => {
                        assert_eq!(branches.len(), 2);
                        assert!(matches!(&branches[0], UnresolvedPathExpr::Inverse(inner)
                            if matches!(inner.as_ref(), UnresolvedPathExpr::Iri(iri)
                                if iri.as_ref() == "http://example.org/b")));
                        assert!(matches!(&branches[1], UnresolvedPathExpr::Inverse(inner)
                            if matches!(inner.as_ref(), UnresolvedPathExpr::Iri(iri)
                                if iri.as_ref() == "http://example.org/c")));
                    }
                    other => panic!("Expected Alternative, got {other:?}"),
                }
                // Second step: ^a
                assert!(matches!(&steps[1], UnresolvedPathExpr::Inverse(inner)
                    if matches!(inner.as_ref(), UnresolvedPathExpr::Iri(iri)
                        if iri.as_ref() == "http://example.org/a")));
            }
            other => panic!("Expected Sequence, got {other:?}"),
        }
    }

    #[test]
    fn test_rewrite_double_inverse_cancels() {
        // ^(^a) → Iri(a)
        let path = UnresolvedPathExpr::Inverse(Box::new(UnresolvedPathExpr::Inverse(Box::new(
            UnresolvedPathExpr::Iri(Arc::from("http://example.org/a")),
        ))));
        let rewritten = rewrite_inverse_of_complex(&path).expect("should rewrite");
        match rewritten {
            UnresolvedPathExpr::Iri(iri) => {
                assert_eq!(iri.as_ref(), "http://example.org/a");
            }
            other => panic!("Expected Iri (double-inverse cancelled), got {other:?}"),
        }
    }

    #[test]
    fn test_rewrite_double_inverse_in_sequence() {
        // ^(^a/b) → Sequence([^b, a])
        // The step ^a becomes Inverse(Inverse(a)) which cancels to a
        let path = UnresolvedPathExpr::Inverse(Box::new(UnresolvedPathExpr::Sequence(vec![
            UnresolvedPathExpr::Inverse(Box::new(UnresolvedPathExpr::Iri(Arc::from(
                "http://example.org/a",
            )))),
            UnresolvedPathExpr::Iri(Arc::from("http://example.org/b")),
        ])));
        let rewritten = rewrite_inverse_of_complex(&path).expect("should rewrite");
        match rewritten {
            UnresolvedPathExpr::Sequence(steps) => {
                assert_eq!(steps.len(), 2);
                // First step: ^b (b was second, reversed to first, then inverted)
                match &steps[0] {
                    UnresolvedPathExpr::Inverse(inner) => match inner.as_ref() {
                        UnresolvedPathExpr::Iri(iri) => {
                            assert_eq!(iri.as_ref(), "http://example.org/b");
                        }
                        other => panic!("Expected Iri inside Inverse, got {other:?}"),
                    },
                    other => panic!("Expected Inverse for step 0, got {other:?}"),
                }
                // Second step: a (^a was first, reversed to second, ^(^a) cancels to a)
                match &steps[1] {
                    UnresolvedPathExpr::Iri(iri) => {
                        assert_eq!(iri.as_ref(), "http://example.org/a");
                    }
                    other => {
                        panic!("Expected Iri (double-inverse cancelled) for step 1, got {other:?}")
                    }
                }
            }
            other => panic!("Expected Sequence, got {other:?}"),
        }
    }

    #[test]
    fn test_rewrite_double_inverse_in_alternative() {
        // ^(a|^b) → Alternative([^a, b])
        // The branch ^b becomes Inverse(Inverse(b)) which cancels to b
        let path = UnresolvedPathExpr::Inverse(Box::new(UnresolvedPathExpr::Alternative(vec![
            UnresolvedPathExpr::Iri(Arc::from("http://example.org/a")),
            UnresolvedPathExpr::Inverse(Box::new(UnresolvedPathExpr::Iri(Arc::from(
                "http://example.org/b",
            )))),
        ])));
        let rewritten = rewrite_inverse_of_complex(&path).expect("should rewrite");
        match rewritten {
            UnresolvedPathExpr::Alternative(branches) => {
                assert_eq!(branches.len(), 2);
                // First branch: ^a
                match &branches[0] {
                    UnresolvedPathExpr::Inverse(inner) => match inner.as_ref() {
                        UnresolvedPathExpr::Iri(iri) => {
                            assert_eq!(iri.as_ref(), "http://example.org/a");
                        }
                        other => panic!("Expected Iri inside Inverse, got {other:?}"),
                    },
                    other => panic!("Expected Inverse for branch 0, got {other:?}"),
                }
                // Second branch: b (double-inverse cancelled)
                match &branches[1] {
                    UnresolvedPathExpr::Iri(iri) => {
                        assert_eq!(iri.as_ref(), "http://example.org/b");
                    }
                    other => panic!(
                        "Expected Iri (double-inverse cancelled) for branch 1, got {other:?}"
                    ),
                }
            }
            other => panic!("Expected Alternative, got {other:?}"),
        }
    }

    #[test]
    fn test_rewrite_no_change_for_simple_inverse() {
        // ^a → None (handled by existing code)
        let path = UnresolvedPathExpr::Inverse(Box::new(UnresolvedPathExpr::Iri(Arc::from(
            "http://example.org/a",
        ))));
        assert!(rewrite_inverse_of_complex(&path).is_none());
    }

    #[test]
    fn test_rewrite_no_change_for_inverse_transitive() {
        // ^(a+) → None (handled by existing code)
        let path = UnresolvedPathExpr::Inverse(Box::new(UnresolvedPathExpr::OneOrMore(Box::new(
            UnresolvedPathExpr::Iri(Arc::from("http://example.org/a")),
        ))));
        assert!(rewrite_inverse_of_complex(&path).is_none());
    }

    // ==========================================================================
    // Inverse of complex paths — end-to-end lowering tests
    // ==========================================================================

    #[test]
    fn test_inverse_of_sequence_lowered() {
        // ^(a/b) with s=?s, o=?o:
        // Rewrite: Sequence([^b, ^a])
        // Step 0: ^b → Triple(?pp0, b, ?s) (inverse swaps prev/next)
        // Step 1: ^a → Triple(?o, a, ?pp0) (inverse swaps prev/next)
        let encoder = test_encoder();
        let mut vars = VarRegistry::new();
        let mut pp_counter: u32 = 0;

        let path = UnresolvedPathExpr::Inverse(Box::new(UnresolvedPathExpr::Sequence(vec![
            UnresolvedPathExpr::Iri(Arc::from("http://example.org/a")),
            UnresolvedPathExpr::Iri(Arc::from("http://example.org/b")),
        ])));
        let pattern = make_path_pattern("?s", path, "?o");

        let results =
            lower_unresolved_pattern(&pattern, &encoder, &mut vars, &mut pp_counter).unwrap();

        assert_eq!(results.len(), 2);

        // Step 0: ^b — Triple(?pp0, b, ?s) [inverse swaps]
        let tp0 = extract_triple(&results[0]);
        assert_eq!(tp0.p.as_iri(), Some("http://example.org/b"));
        // Inverse: subject = next (?pp0), object = prev (?s)
        assert!(matches!(&tp0.o, Term::Var(vid) if vars.name(*vid) == "?s"));

        // Step 1: ^a — Triple(?o, a, ?pp0) [inverse swaps]
        let tp1 = extract_triple(&results[1]);
        assert_eq!(tp1.p.as_iri(), Some("http://example.org/a"));
        assert_eq!(tp1.s.as_var().map(|v| vars.name(v)), Some("?o"));
    }

    #[test]
    fn test_inverse_of_alternative_lowered() {
        // ^(a|b) with s=?s, o=?o:
        // Rewrite: Alternative([^a, ^b])
        // → Union([[Triple(?o, a, ?s)], [Triple(?o, b, ?s)]])
        let encoder = test_encoder();
        let mut vars = VarRegistry::new();
        let mut pp_counter: u32 = 0;

        let path = UnresolvedPathExpr::Inverse(Box::new(UnresolvedPathExpr::Alternative(vec![
            UnresolvedPathExpr::Iri(Arc::from("http://example.org/a")),
            UnresolvedPathExpr::Iri(Arc::from("http://example.org/b")),
        ])));
        let pattern = make_path_pattern("?s", path, "?o");

        let results =
            lower_unresolved_pattern(&pattern, &encoder, &mut vars, &mut pp_counter).unwrap();

        assert_eq!(results.len(), 1);
        if let Pattern::Union(branches) = &results[0] {
            assert_eq!(branches.len(), 2);
            // Each branch: single triple with s/o swapped (inverse)
            for branch in branches {
                assert_eq!(branch.len(), 1);
                let tp = extract_triple(&branch[0]);
                // Inverse: subject = ?o, object = ?s
                assert_eq!(tp.s.as_var().map(|v| vars.name(v)), Some("?o"));
                assert!(matches!(&tp.o, Term::Var(vid) if vars.name(*vid) == "?s"));
            }
            // Branch 0 predicate: a
            let tp0 = extract_triple(&branches[0][0]);
            assert_eq!(tp0.p.as_iri(), Some("http://example.org/a"));
            // Branch 1 predicate: b
            let tp1 = extract_triple(&branches[1][0]);
            assert_eq!(tp1.p.as_iri(), Some("http://example.org/b"));
        } else {
            panic!("Expected Union, got {:?}", results[0]);
        }
    }
}
