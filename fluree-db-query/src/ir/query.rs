//! Top-level query IR: the resolved-and-lowered `Query` that flows from
//! parsing through planning, execution, and result formatting.
//!
//! `Query` is the canonical query representation. Its `output` field
//! captures the result-shape decision (SELECT, ASK, CONSTRUCT). `patterns`
//! holds the WHERE clause IR. `grouping` carries the optional aggregation
//! phase (GROUP BY / aggregates / HAVING). `ordering` carries the ORDER BY
//! sort specs. `limit` and `offset` are the slicing modifiers applied last.
//! `reasoning` carries the configuration the rewriter consumes (modes plus
//! an optional pre-resolved schema bundle). Hydration formatting lives
//! inside the `Column::Hydration` variant on the SELECT projection.

use std::collections::HashSet;

use fluree_graph_json_ld::ParsedContext;

use super::grouping::Grouping;
use super::pattern::Pattern;
use super::projection::{Column, Projection};
use super::reasoning::ReasoningConfig;
use super::triple::{Ref, TriplePattern};
use crate::sort::SortSpec;
use crate::var_registry::VarId;

/// Resolved CONSTRUCT template patterns
///
/// Contains the template patterns that will be instantiated with query bindings
/// to produce output triples. Uses the same TriplePattern type as WHERE clause
/// patterns, but variables are resolved against the query result bindings rather
/// than matched against the database.
///
/// `patterns`, `graphs` and `reifications` index into one another, so they
/// change only through [`push_pattern`](Self::push_pattern) and
/// [`push_reification`](Self::push_reification).
#[derive(Debug, Clone)]
pub struct ConstructTemplate {
    /// Template patterns (resolved TriplePatterns with Sids and VarIds)
    patterns: Vec<TriplePattern>,
    /// Variables that originated as template blank nodes (`[ ]`, `_:a`, or the
    /// blank cells of a desugared RDF collection `( ... )`).
    ///
    /// These are never produced by the WHERE clause, so the CONSTRUCT output
    /// path must mint a FRESH blank node for each on every solution row —
    /// shared across a row's template triples, distinct across rows — rather
    /// than resolving them against the (absent) bindings. Empty for templates
    /// with no blank nodes, including every JSON-LD/FQL construct and DESCRIBE.
    pub bnode_vars: HashSet<VarId>,
    /// The named graph each pattern writes into, parallel to `patterns`
    /// (`None`: the default graph). Empty when no pattern names a graph. A
    /// template that names one produces a dataset, not a single graph.
    graphs: Vec<Option<Ref>>,
    /// RDF 1.2 reifier attachments: `reifier` reifies the triple that
    /// `patterns[triple]` instantiates to. A row that leaves either unbound
    /// contributes no attachment.
    reifications: Vec<TemplateReification>,
}

/// A reifier attachment in a CONSTRUCT template (see
/// [`ConstructTemplate::reifications`]).
#[derive(Debug, Clone)]
pub struct TemplateReification {
    /// Index into [`ConstructTemplate::patterns`] of the reified triple.
    pub triple: usize,
    /// The reifier: a variable (a template blank node for `[ ]`-style
    /// reifiers), or a constant.
    pub reifier: Ref,
}

impl ConstructTemplate {
    /// Create a construct template from patterns with no template blank nodes.
    pub fn new(patterns: Vec<TriplePattern>) -> Self {
        Self::with_bnode_vars(patterns, HashSet::new())
    }

    /// Create a construct template carrying its template blank-node variables
    /// (see [`ConstructTemplate::bnode_vars`]).
    pub fn with_bnode_vars(patterns: Vec<TriplePattern>, bnode_vars: HashSet<VarId>) -> Self {
        Self {
            patterns,
            bnode_vars,
            graphs: Vec::new(),
            reifications: Vec::new(),
        }
    }

    /// Append a pattern that writes into `graph` (`None`: the default graph)
    /// and return its index. `graphs` stays empty until the first named
    /// graph appears, then is kept aligned with `patterns`.
    pub fn push_pattern(&mut self, pattern: TriplePattern, graph: Option<Ref>) -> usize {
        if graph.is_some() || !self.graphs.is_empty() {
            self.graphs.resize(self.patterns.len(), None);
            self.graphs.push(graph);
        }
        self.patterns.push(pattern);
        self.patterns.len() - 1
    }

    /// Attach `reifier` to `patterns[triple]`, an index
    /// [`push_pattern`](Self::push_pattern) returned.
    pub fn push_reification(&mut self, triple: usize, reifier: Ref) {
        assert!(
            triple < self.patterns.len(),
            "reification of pattern {triple}, but the template has {}",
            self.patterns.len()
        );
        self.reifications
            .push(TemplateReification { triple, reifier });
    }

    /// The template patterns.
    pub fn patterns(&self) -> &[TriplePattern] {
        &self.patterns
    }

    /// The reifier attachments, each naming a pattern that exists.
    pub fn reifications(&self) -> &[TemplateReification] {
        &self.reifications
    }

    /// The graph `patterns[i]` writes into (`None`: the default graph).
    pub fn graph(&self, i: usize) -> Option<&Ref> {
        self.graphs.get(i).and_then(Option::as_ref)
    }

    /// Whether any pattern writes into a named graph, which makes the result
    /// a dataset.
    pub fn names_graphs(&self) -> bool {
        self.graphs.iter().any(Option::is_some)
    }

    /// Iterate over variables in the patterns, graph names and reifiers.
    /// Variables mentioned more than once appear more than once.
    pub fn var_iter(&self) -> impl Iterator<Item = VarId> + '_ {
        let refs = self
            .graphs
            .iter()
            .flatten()
            .chain(self.reifications.iter().map(|r| &r.reifier));
        self.patterns
            .iter()
            .flat_map(TriplePattern::referenced_vars)
            .chain(refs.filter_map(Ref::as_var))
    }

    /// Collect all variables referenced in the template: its patterns, graph
    /// names and reifiers.
    pub fn referenced_vars(&self) -> HashSet<VarId> {
        self.var_iter().collect()
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
    /// Distinct from `query.limit = Some(1)`: `One` also changes the output
    /// shape — formatters render a bare row (or null) rather than a one-element
    /// array. `LIMIT 1` caps the result set but keeps the array shape.
    One,
}

/// What a grouped SELECT may do with a projected variable its grouping neither
/// keys, aggregates nor binds.
///
/// It is a property of the top-level output only. A sub-query's projection is a
/// bare `Vec<VarId>` ([`super::SubqueryPattern`]), so it cannot carry
/// [`Self::PerGroupList`]: a per-group list never crosses a sub-query boundary.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum UngroupedProjection {
    /// A plan error (the SPARQL rule, SPARQL 1.1 §11.4, and Cypher's).
    #[default]
    Reject,
    /// Rendered as a per-group list: the documented JSON-LD query behavior.
    /// Set only by the JSON-LD user-query entry point.
    PerGroupList,
}

/// Describes what the query produces.
#[derive(Debug, Clone)]
pub enum QueryOutput {
    /// SELECT — projects rows from the algebra. The `projection` carries
    /// column structure (and per-column hydration); `restriction` carries the
    /// optional `selectDistinct` / `selectOne` modifier; `ungrouped` what a
    /// grouped query may do with a projected variable it does not group.
    Select {
        projection: Projection,
        restriction: Option<Restriction>,
        ungrouped: UngroupedProjection,
    },
    /// CONSTRUCT — template patterns instantiated with bindings.
    Construct(ConstructTemplate),
    /// ASK — boolean result.
    Ask,
}

impl QueryOutput {
    fn select_vars(vars: Vec<VarId>, restriction: Option<Restriction>) -> Self {
        Self::Select {
            projection: Projection::Tuple(vars.into_iter().map(Column::Var).collect()),
            restriction,
            ungrouped: UngroupedProjection::Reject,
        }
    }

    /// Construct a plain `Select` from a variable list (`select ?x ?y ...`).
    pub fn select_all(vars: Vec<VarId>) -> Self {
        Self::select_vars(vars, None)
    }

    /// Construct a `Select` with `Distinct` restriction (`selectDistinct ?x ...`).
    pub fn select_distinct(vars: Vec<VarId>) -> Self {
        Self::select_vars(vars, Some(Restriction::Distinct))
    }

    /// Construct a `Select` with `One` restriction (`selectOne ?x ...`).
    pub fn select_one(vars: Vec<VarId>) -> Self {
        Self::select_vars(vars, Some(Restriction::One))
    }

    /// Construct a `Select` with a Wildcard projection (`select *`).
    pub fn wildcard() -> Self {
        Self::Select {
            projection: Projection::Wildcard,
            restriction: None,
            ungrouped: UngroupedProjection::Reject,
        }
    }

    /// Construct a `Select` with a Wildcard projection and `Distinct`
    /// restriction (`select distinct *`).
    pub fn wildcard_distinct() -> Self {
        Self::Select {
            projection: Projection::Wildcard,
            restriction: Some(Restriction::Distinct),
            ungrouped: UngroupedProjection::Reject,
        }
    }

    /// What a grouped query may do with a projected variable it does not
    /// group. [`UngroupedProjection::Reject`] for every non-SELECT output.
    pub fn ungrouped_projection(&self) -> UngroupedProjection {
        match self {
            QueryOutput::Select { ungrouped, .. } => *ungrouped,
            _ => UngroupedProjection::Reject,
        }
    }

    /// Let a grouped SELECT project an ungrouped variable as a per-group list
    /// (the JSON-LD query surface). No effect on other outputs.
    pub fn allow_per_group_lists(&mut self) {
        if let QueryOutput::Select { ungrouped, .. } = self {
            *ungrouped = UngroupedProjection::PerGroupList;
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
    /// Synthetic `(var, expr)` binds for expression-based ORDER BY
    /// (e.g. `ORDER BY DESC(?a / ?b)`). Evaluated once per solution as a
    /// dedicated stage AFTER grouping/aggregation/HAVING/post-binds and BEFORE
    /// the sort, so the keys can reference GROUP BY keys, aggregate outputs, and
    /// SELECT post-binds. The matching `SortSpec` in `ordering` references the
    /// synthetic var; these vars are never projected to the output. Empty for
    /// var-only ORDER BY.
    pub order_binds: Vec<(VarId, super::Expression)>,
    /// Maximum rows to return (applied last). `None` is unbounded;
    /// `Some(0)` is a legitimate "return nothing" some fast-paths bail on.
    pub limit: Option<usize>,
    /// Rows to skip before returning results. `None` is no skip.
    pub offset: Option<usize>,
    /// Reasoning configuration (RDFS/OWL/datalog modes, schema bundle).
    pub reasoning: ReasoningConfig,
    /// Post-query VALUES clause (SPARQL `ValuesClause` after `SolutionModifier`).
    ///
    /// Stored separately from `patterns` so the WHERE-clause planner does not
    /// reorder it relative to OPTIONAL/UNION/etc.  Applied as a final inner-join
    /// constraint after the WHERE operator tree is fully built.
    pub post_values: Option<Pattern>,
    /// When true, scan operators bypass the **variable-predicate**
    /// filter that hides Fluree-system predicates (`f:reifies*` in
    /// every graph; the broader `f:` namespace in the default graph).
    /// Surfaced via `opts.includeSystemFacts: true` on JSON-LD
    /// queries and `# PRAGMA include-system-facts: true` on SPARQL.
    ///
    /// Direct user mention of `f:reifies*` IRIs is rejected at parse
    /// time (`fluree-db-query` JSON-LD firewall and `fluree-db-sparql`
    /// post-lower scan) regardless of this flag — the parser
    /// rejection is the contract-level boundary; this flag only
    /// relaxes the per-row scan filter for `?p`-shape patterns.
    pub include_system_facts: bool,
    /// The request's own union default graph switch: `Some(true)` reads the
    /// default graph as the union of the ledger's default graph and its named
    /// graphs, `Some(false)` reads the default graph alone, and `None` defers
    /// to the ledger's `f:unionDefaultGraph` setting. Surfaced via
    /// `opts.unionDefaultGraph` on JSON-LD queries and
    /// `# PRAGMA union-default-graph` on SPARQL. It governs only a default
    /// graph the query does not narrow to named graphs of its own.
    pub union_default_graph: Option<bool>,
    /// `@vocab` prefix of the ledger context a Cypher query was lowered
    /// against, if any. Read by `labels()`/`type()`/`keys()`/`properties()`
    /// evaluation so IRI compaction matches `db.labels()`: strip the vocab
    /// prefix when it applies, otherwise keep the full IRI (round-trippable).
    /// `None` for JSON-LD/SPARQL queries and vocab-less Cypher.
    pub cypher_vocab: Option<std::sync::Arc<str>>,
    /// What an unmatched OPTIONAL binds its optional-only variables to: the
    /// surface language's null semantics. SPARQL / JSON-LD leave the default
    /// `Unbound`; Cypher lowering sets `Poisoned`.
    pub unmatched_optional: crate::binding::UnmatchedOptional,
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
            order_binds: Vec::new(),
            limit: None,
            offset: None,
            reasoning: ReasoningConfig::default(),
            post_values: None,
            include_system_facts: false,
            union_default_graph: None,
            cypher_vocab: None,
            unmatched_optional: crate::binding::UnmatchedOptional::Unbound,
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
            order_binds: self.order_binds.clone(),
            limit: self.limit,
            offset: self.offset,
            reasoning: self.reasoning.clone(),
            post_values: self.post_values.clone(),
            include_system_facts: self.include_system_facts,
            union_default_graph: self.union_default_graph,
            cypher_vocab: self.cypher_vocab.clone(),
            unmatched_optional: self.unmatched_optional,
        }
    }
}
