//! Unresolved AST types for JSON-LD query parsing
//!
//! These types represent the parsed query before IRI resolution.
//! They use `Arc<str>` for efficient string handling and deduplication.
//!
//! The AST supports ordered patterns in the where clause:
//! - Triple patterns (basic graph patterns)
//! - Filter expressions (inline or standalone)
//! - Optional clauses (left join semantics)
//! - Bind expressions (computed values)
//! - Values blocks (inline data)

use crate::parse::IriEncoder;
use fluree_db_core::DatatypeConstraint;
use fluree_graph_json_ld::ParsedContext;
pub use fluree_vocab::UnresolvedDatatypeConstraint;
use std::sync::Arc;

/// Literal value from JSON (before resolution)
#[derive(Clone, Debug, PartialEq)]
pub enum LiteralValue {
    /// String literal
    String(Arc<str>),
    /// Integer literal (parsed from JSON number)
    Long(i64),
    /// Float literal (parsed from JSON number)
    Double(f64),
    /// Boolean literal
    Boolean(bool),
    /// Vector literal (fluree:vector)
    Vector(Vec<f64>),
}

impl LiteralValue {
    /// Create a string literal
    pub fn string(s: impl AsRef<str>) -> Self {
        LiteralValue::String(Arc::from(s.as_ref()))
    }

    /// Create a vector literal
    pub fn vector(v: Vec<f64>) -> Self {
        LiteralValue::Vector(v)
    }
}

/// Unresolved VALUES cell - supports typed literals, language tags, and IRIs.
#[derive(Clone, Debug, PartialEq)]
pub enum UnresolvedValue {
    /// UNDEF / null in VALUES
    Unbound,
    /// IRI value (e.g. {"@id": "..."} or {"@value":"...","@type":"@id"})
    Iri(Arc<str>),
    /// Literal value with optional datatype or language-tag constraint
    Literal {
        value: LiteralValue,
        /// Datatype IRI or language tag constraint
        dtc: Option<UnresolvedDatatypeConstraint>,
    },
}

/// Unresolved term - before IRI encoding
///
/// Uses `Arc<str>` to reduce allocations when the same IRI/variable
/// appears multiple times in a query.
#[derive(Clone, Debug, PartialEq)]
pub enum UnresolvedTerm {
    /// Variable binding (e.g., "?name")
    Var(Arc<str>),
    /// Expanded IRI (e.g., "http://schema.org/name")
    Iri(Arc<str>),
    /// Literal value from JSON
    Literal(LiteralValue),
}

impl UnresolvedTerm {
    /// Create a variable term
    pub fn var(name: impl AsRef<str>) -> Self {
        UnresolvedTerm::Var(Arc::from(name.as_ref()))
    }

    /// Create an IRI term
    pub fn iri(iri: impl AsRef<str>) -> Self {
        UnresolvedTerm::Iri(Arc::from(iri.as_ref()))
    }

    /// Create a string literal term
    pub fn string(s: impl AsRef<str>) -> Self {
        UnresolvedTerm::Literal(LiteralValue::String(Arc::from(s.as_ref())))
    }

    /// Create a long literal term
    pub fn long(v: i64) -> Self {
        UnresolvedTerm::Literal(LiteralValue::Long(v))
    }

    /// Create a double literal term
    pub fn double(v: f64) -> Self {
        UnresolvedTerm::Literal(LiteralValue::Double(v))
    }

    /// Create a boolean literal term
    pub fn boolean(v: bool) -> Self {
        UnresolvedTerm::Literal(LiteralValue::Boolean(v))
    }

    /// Check if this term is a variable
    pub fn is_var(&self) -> bool {
        matches!(self, UnresolvedTerm::Var(_))
    }

    /// Get the variable name if this is a variable
    pub fn as_var(&self) -> Option<&str> {
        match self {
            UnresolvedTerm::Var(name) => Some(name.as_ref()),
            _ => None,
        }
    }
}

/// Encode an [`UnresolvedDatatypeConstraint`] to a [`DatatypeConstraint`] by
/// resolving the IRI to a [`Sid`](fluree_db_core::Sid).
///
/// Returns `None` if the [`Explicit`](UnresolvedDatatypeConstraint::Explicit)
/// IRI's namespace is not registered in the encoder.
/// [`LangTag`](UnresolvedDatatypeConstraint::LangTag) always succeeds.
pub fn encode_datatype_constraint(
    dtc: UnresolvedDatatypeConstraint,
    encoder: &impl IriEncoder,
) -> Option<DatatypeConstraint> {
    match dtc {
        UnresolvedDatatypeConstraint::Explicit(iri) => {
            encoder.encode_iri(&iri).map(DatatypeConstraint::Explicit)
        }
        UnresolvedDatatypeConstraint::LangTag(tag) => Some(DatatypeConstraint::LangTag(tag)),
    }
}

/// Unresolved triple pattern - before IRI encoding
#[derive(Clone, Debug)]
pub struct UnresolvedTriplePattern {
    /// Subject term
    pub s: UnresolvedTerm,
    /// Predicate term
    pub p: UnresolvedTerm,
    /// Object term
    pub o: UnresolvedTerm,
    /// Optional datatype or language-tag constraint for the object
    pub dtc: Option<UnresolvedDatatypeConstraint>,
}

impl UnresolvedTriplePattern {
    /// Create a new unresolved triple pattern with no constraint
    pub fn new(s: UnresolvedTerm, p: UnresolvedTerm, o: UnresolvedTerm) -> Self {
        Self { s, p, o, dtc: None }
    }

    /// Create with a datatype IRI constraint
    pub fn with_dt(
        s: UnresolvedTerm,
        p: UnresolvedTerm,
        o: UnresolvedTerm,
        dt_iri: impl AsRef<str>,
    ) -> Self {
        Self {
            s,
            p,
            o,
            dtc: Some(UnresolvedDatatypeConstraint::Explicit(Arc::from(
                dt_iri.as_ref(),
            ))),
        }
    }

    /// Create with a language tag constraint (implies `rdf:langString` datatype)
    pub fn with_lang(
        s: UnresolvedTerm,
        p: UnresolvedTerm,
        o: UnresolvedTerm,
        lang: impl AsRef<str>,
    ) -> Self {
        Self {
            s,
            p,
            o,
            dtc: Some(UnresolvedDatatypeConstraint::LangTag(Arc::from(
                lang.as_ref(),
            ))),
        }
    }
}

/// Property path modifier (transitive operators)
///
/// Used by the resolved IR layer (`PropertyPathPattern`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathModifier {
    /// + : one or more (at least one hop)
    OneOrMore,
    /// * : zero or more (includes starting node)
    ZeroOrMore,
}

/// Unresolved property path expression (before lowering).
///
/// Represents the full SPARQL 1.1 property path algebra using expanded IRI
/// strings. Created during parse phase from `@path` context entries.
///
/// `Sequence` and `Alternative` are n-ary (Vec) rather than binary so that
/// expressions like `a/b/c` produce a flat list instead of nested pairs.
#[derive(Debug, Clone, PartialEq)]
pub enum UnresolvedPathExpr {
    /// Simple predicate IRI (already expanded via @context)
    Iri(Arc<str>),
    /// Inverse path: `^path`
    Inverse(Box<UnresolvedPathExpr>),
    /// Sequence path: `path1 / path2 / ...` (n-ary)
    Sequence(Vec<UnresolvedPathExpr>),
    /// Alternative path: `path1 | path2 | ...` (n-ary)
    Alternative(Vec<UnresolvedPathExpr>),
    /// Zero or more: `path*`
    ZeroOrMore(Box<UnresolvedPathExpr>),
    /// One or more: `path+`
    OneOrMore(Box<UnresolvedPathExpr>),
    /// Zero or one: `path?`
    ZeroOrOne(Box<UnresolvedPathExpr>),
}

// ============================================================================
// Index Search Pattern (BM25 Full-Text Search)
// ============================================================================

/// Target for index search - can be a constant query string or variable.
#[derive(Debug, Clone, PartialEq)]
pub enum UnresolvedIndexSearchTarget {
    /// Constant search query string
    Const(Arc<str>),
    /// Variable reference (bound at runtime)
    Var(Arc<str>),
}

/// Unresolved index search pattern for BM25 full-text queries.
///
/// Represents a search against a graph source (e.g., BM25 index) with
/// result bindings for document ID, score, and optional ledger alias.
///
/// # Example Query Syntax
///
/// Direct variable result:
/// ```json
/// {
///   "f:graphSource": "my-search:main",
///   "f:searchText": "software engineer",
///   "f:searchLimit": 10,
///   "f:searchResult": "?doc"
/// }
/// ```
///
/// Nested result with score:
/// ```json
/// {
///   "f:graphSource": "my-search:main",
///   "f:searchText": "software engineer",
///   "f:searchResult": {
///     "f:resultId": "?doc",
///     "f:resultScore": "?score",
///     "f:resultLedger": "?source"
///   }
/// }
/// ```
#[derive(Debug, Clone)]
pub struct UnresolvedIndexSearchPattern {
    /// Graph source alias (e.g., "my-search:main")
    pub graph_source_id: Arc<str>,

    /// Search query target - can be a constant string or variable
    pub target: UnresolvedIndexSearchTarget,

    /// Maximum number of results (optional)
    pub limit: Option<usize>,

    /// Variable to bind the document IRI (required)
    pub id_var: Arc<str>,

    /// Variable to bind the BM25 score (optional)
    pub score_var: Option<Arc<str>>,

    /// Variable to bind the source ledger alias (optional, for multi-ledger)
    pub ledger_var: Option<Arc<str>>,

    /// Whether to sync before query (default: false)
    pub sync: bool,

    /// Query timeout in milliseconds (optional)
    pub timeout: Option<u64>,
}

impl UnresolvedIndexSearchPattern {
    /// Create a new index search pattern with just ID binding
    pub fn new(
        graph_source_id: impl AsRef<str>,
        target: UnresolvedIndexSearchTarget,
        id_var: impl AsRef<str>,
    ) -> Self {
        Self {
            graph_source_id: Arc::from(graph_source_id.as_ref()),
            target,
            limit: None,
            id_var: Arc::from(id_var.as_ref()),
            score_var: None,
            ledger_var: None,
            sync: false,
            timeout: None,
        }
    }

    /// Set the result limit
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set the score binding variable
    pub fn with_score_var(mut self, var: impl AsRef<str>) -> Self {
        self.score_var = Some(Arc::from(var.as_ref()));
        self
    }

    /// Set the ledger binding variable
    pub fn with_ledger_var(mut self, var: impl AsRef<str>) -> Self {
        self.ledger_var = Some(Arc::from(var.as_ref()));
        self
    }

    /// Set sync mode
    pub fn with_sync(mut self, sync: bool) -> Self {
        self.sync = sync;
        self
    }

    /// Set timeout
    pub fn with_timeout(mut self, timeout: u64) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

/// Target for vector search - can be a constant vector or variable.
#[derive(Debug, Clone, PartialEq)]
pub enum UnresolvedVectorSearchTarget {
    /// Constant vector (f32 for efficiency)
    Const(Vec<f32>),
    /// Variable reference (bound at runtime to a fluree:vector value)
    Var(Arc<str>),
}

/// Unresolved vector search pattern - before variable resolution
///
/// Used for vector similarity search against a vector graph source.
///
/// # Example
///
/// ```json
/// {
///   "f:graphSource": "embeddings:main",
///   "f:queryVector": [0.1, 0.2, 0.3],
///   "f:distanceMetric": "cosine",
///   "f:searchLimit": 10,
///   "f:searchResult": {"f:resultId": "?doc", "f:resultScore": "?score"}
/// }
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct UnresolvedVectorSearchPattern {
    /// Graph source alias (e.g., "embeddings:main")
    pub graph_source_id: Arc<str>,

    /// Search target - can be a constant vector or variable
    pub target: UnresolvedVectorSearchTarget,

    /// Distance metric (cosine, dot, euclidean)
    pub metric: Arc<str>,

    /// Maximum number of results (optional, defaults to 10)
    pub limit: Option<usize>,

    /// Variable to bind the document IRI (required)
    pub id_var: Arc<str>,

    /// Variable to bind the similarity score (optional)
    pub score_var: Option<Arc<str>>,

    /// Variable to bind the source ledger alias (optional, for multi-ledger)
    pub ledger_var: Option<Arc<str>>,

    /// Whether to sync before query (default: false)
    pub sync: bool,

    /// Query timeout in milliseconds (optional)
    pub timeout: Option<u64>,
}

impl UnresolvedVectorSearchPattern {
    /// Create a new vector search pattern with just ID binding
    pub fn new(
        graph_source_id: impl AsRef<str>,
        target: UnresolvedVectorSearchTarget,
        metric: impl AsRef<str>,
        id_var: impl AsRef<str>,
    ) -> Self {
        Self {
            graph_source_id: Arc::from(graph_source_id.as_ref()),
            target,
            metric: Arc::from(metric.as_ref()),
            limit: None,
            id_var: Arc::from(id_var.as_ref()),
            score_var: None,
            ledger_var: None,
            sync: false,
            timeout: None,
        }
    }

    /// Set the result limit
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set the score binding variable
    pub fn with_score_var(mut self, var: impl AsRef<str>) -> Self {
        self.score_var = Some(Arc::from(var.as_ref()));
        self
    }

    /// Set the ledger binding variable
    pub fn with_ledger_var(mut self, var: impl AsRef<str>) -> Self {
        self.ledger_var = Some(Arc::from(var.as_ref()));
        self
    }

    /// Set sync mode
    pub fn with_sync(mut self, sync: bool) -> Self {
        self.sync = sync;
        self
    }

    /// Set timeout
    pub fn with_timeout(mut self, timeout: u64) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

/// Filter value (constant) before resolution
#[derive(Debug, Clone, PartialEq)]
pub enum UnresolvedFilterValue {
    Long(i64),
    Double(f64),
    String(Arc<str>),
    Bool(bool),
}

impl UnresolvedFilterValue {
    /// Create a string filter value
    pub fn string(s: impl AsRef<str>) -> Self {
        UnresolvedFilterValue::String(Arc::from(s.as_ref()))
    }
}

/// Unresolved filter expression - before variable resolution
///
/// Uses string variable names instead of VarIds.
#[derive(Debug, Clone)]
pub enum UnresolvedExpression {
    /// Variable reference (e.g., "?age")
    Var(Arc<str>),
    /// Constant value
    Const(UnresolvedFilterValue),
    /// Logical AND
    And(Vec<UnresolvedExpression>),
    /// Logical OR
    Or(Vec<UnresolvedExpression>),
    /// Logical NOT
    Not(Box<UnresolvedExpression>),
    /// IN expression (?x IN (1, 2, 3))
    In {
        expr: Box<UnresolvedExpression>,
        values: Vec<UnresolvedExpression>,
        negated: bool,
    },
    /// Function call
    Call {
        func: Arc<str>,
        args: Vec<UnresolvedExpression>,
    },
    /// EXISTS subquery inside a compound filter expression.
    ///
    /// Stores pre-parsed patterns from the WHERE-clause parser.
    /// Example JSON-LD: `["filter", ["or", ["=", "?x", "?y"], ["not-exists", {":p": "?z"}]]]`
    Exists {
        patterns: Vec<UnresolvedPattern>,
        negated: bool,
    },
}

// Manual PartialEq: UnresolvedPattern doesn't implement PartialEq,
// so we can't derive. Exists variants are never structurally compared.
impl PartialEq for UnresolvedExpression {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Var(a), Self::Var(b)) => a == b,
            (Self::Const(a), Self::Const(b)) => a == b,
            (Self::And(a), Self::And(b)) => a == b,
            (Self::Or(a), Self::Or(b)) => a == b,
            (Self::Not(a), Self::Not(b)) => a == b,
            (
                Self::In {
                    expr: e1,
                    values: v1,
                    negated: n1,
                },
                Self::In {
                    expr: e2,
                    values: v2,
                    negated: n2,
                },
            ) => e1 == e2 && v1 == v2 && n1 == n2,
            (Self::Call { func: f1, args: a1 }, Self::Call { func: f2, args: a2 }) => {
                f1 == f2 && a1 == a2
            }
            (Self::Exists { .. }, Self::Exists { .. }) => false,
            _ => false,
        }
    }
}

impl UnresolvedExpression {
    /// Create a variable reference
    pub fn var(name: impl AsRef<str>) -> Self {
        UnresolvedExpression::Var(Arc::from(name.as_ref()))
    }

    /// Create a long constant
    pub fn long(v: i64) -> Self {
        UnresolvedExpression::Const(UnresolvedFilterValue::Long(v))
    }

    /// Create a double constant
    pub fn double(v: f64) -> Self {
        UnresolvedExpression::Const(UnresolvedFilterValue::Double(v))
    }

    /// Create a boolean constant
    pub fn boolean(v: bool) -> Self {
        UnresolvedExpression::Const(UnresolvedFilterValue::Bool(v))
    }

    /// Create a string constant
    pub fn string(s: impl AsRef<str>) -> Self {
        UnresolvedExpression::Const(UnresolvedFilterValue::string(s))
    }
}

// ============================================================================
// Query modifier types (unresolved)
// ============================================================================

/// Sort direction (unresolved)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum UnresolvedSortDirection {
    #[default]
    Asc,
    Desc,
}

/// Sort specification (unresolved)
#[derive(Debug, Clone, PartialEq)]
pub struct UnresolvedSortSpec {
    /// Variable to sort by
    pub var: Arc<str>,
    /// Sort direction
    pub direction: UnresolvedSortDirection,
}

impl UnresolvedSortSpec {
    /// Create a new sort specification
    pub fn new(var: impl AsRef<str>, direction: UnresolvedSortDirection) -> Self {
        Self {
            var: Arc::from(var.as_ref()),
            direction,
        }
    }

    /// Create an ascending sort specification
    pub fn asc(var: impl AsRef<str>) -> Self {
        Self::new(var, UnresolvedSortDirection::Asc)
    }

    /// Create a descending sort specification
    pub fn desc(var: impl AsRef<str>) -> Self {
        Self::new(var, UnresolvedSortDirection::Desc)
    }
}

/// Aggregate function (unresolved)
#[derive(Debug, Clone, PartialEq)]
pub enum UnresolvedAggregateFn {
    Count,
    CountDistinct,
    Sum,
    Avg,
    Min,
    Max,
    Median,
    Variance,
    Stddev,
    GroupConcat { separator: String },
    Sample,
}

/// Aggregate specification (unresolved)
#[derive(Debug, Clone, PartialEq)]
pub struct UnresolvedAggregateSpec {
    /// Aggregate function to apply
    pub function: UnresolvedAggregateFn,
    /// Input variable (the variable being aggregated)
    pub input_var: Arc<str>,
    /// Output variable (the result variable name)
    pub output_var: Arc<str>,
}

impl UnresolvedAggregateSpec {
    /// Create a new aggregate specification
    pub fn new(
        function: UnresolvedAggregateFn,
        input_var: impl AsRef<str>,
        output_var: impl AsRef<str>,
    ) -> Self {
        Self {
            function,
            input_var: Arc::from(input_var.as_ref()),
            output_var: Arc::from(output_var.as_ref()),
        }
    }
}

/// Query options (unresolved)
///
/// Contains all solution modifiers before variable resolution.
#[derive(Debug, Clone)]
pub struct UnresolvedOptions {
    /// Maximum rows to return
    pub limit: Option<usize>,
    /// Rows to skip before returning results
    pub offset: Option<usize>,
    /// Whether to deduplicate results
    pub distinct: bool,
    /// Sort specifications
    pub order_by: Vec<UnresolvedSortSpec>,
    /// GROUP BY variables
    pub group_by: Vec<Arc<str>>,
    /// Aggregate specifications
    pub aggregates: Vec<UnresolvedAggregateSpec>,
    /// HAVING filter expression
    pub having: Option<UnresolvedExpression>,
    /// Reasoning modes (RDFS, OWL2-QL, etc.)
    ///
    /// Parsed from `"reasoning"` key in query JSON. None means use defaults
    /// (auto-enable RDFS when hierarchy exists).
    pub reasoning: Option<crate::ir::ReasoningModes>,
    /// Whether to treat bare "?var" object strings as variables in WHERE.
    ///
    /// When false, bare "?x" object values are literals unless
    /// explicitly wrapped as {"@variable": "?x"}.
    pub object_var_parsing: bool,
}

impl UnresolvedOptions {
    /// Create new options with defaults
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for UnresolvedOptions {
    fn default() -> Self {
        Self {
            limit: None,
            offset: None,
            distinct: false,
            order_by: Vec::new(),
            group_by: Vec::new(),
            aggregates: Vec::new(),
            having: None,
            reasoning: None,
            object_var_parsing: true,
        }
    }
}

// ============================================================================
// CONSTRUCT query types
// ============================================================================

/// Unresolved CONSTRUCT template
///
/// Contains the template patterns that will be instantiated with query bindings
/// to produce output triples. Templates use the same node-map format as WHERE
/// clauses but are interpreted as output patterns rather than match patterns.
#[derive(Debug, Clone)]
pub struct UnresolvedConstructTemplate {
    /// Template patterns (only Triple patterns are valid in templates)
    pub patterns: Vec<UnresolvedPattern>,
}

impl UnresolvedConstructTemplate {
    /// Create a new construct template from patterns
    pub fn new(patterns: Vec<UnresolvedPattern>) -> Self {
        Self { patterns }
    }
}

// ============================================================================
// Hydration types
// ============================================================================

/// Root of a hydration - can be variable or IRI constant
///
/// Supports both syntax forms:
/// - Variable root: `{"?person": ["*", ...]}`
/// - IRI constant root: `{"ex:alice": ["*", ...]}`
#[derive(Debug, Clone, PartialEq)]
pub enum UnresolvedRoot {
    /// Variable root (e.g., "?person")
    Var(Arc<str>),
    /// IRI constant root (expanded IRI, e.g., "http://example.org/alice")
    Iri(String),
}

/// Nested selection specification for sub-hydrations
///
/// Selection at one level of a hydration (unresolved). Mirrors
/// [`crate::ir::NestedSelectSpec`].
type UnresolvedReverseMap =
    std::collections::HashMap<String, Option<Box<UnresolvedNestedSelectSpec>>>;

#[derive(Debug, Clone, PartialEq)]
pub enum UnresolvedNestedSelectSpec {
    /// `*` at this level — include all forward properties (and `@id`).
    /// `refinements` overrides the wildcard default of "include but don't
    /// recurse" for specific properties.
    Wildcard {
        refinements:
            std::collections::HashMap<String, Box<UnresolvedNestedSelectSpec>>,
        reverse: UnresolvedReverseMap,
    },
    /// Explicit list of forward items.
    Explicit {
        forward: Vec<UnresolvedForwardItem>,
        reverse: UnresolvedReverseMap,
    },
}

impl UnresolvedNestedSelectSpec {
    /// Returns `true` if this level is a wildcard.
    pub fn is_wildcard(&self) -> bool {
        matches!(self, UnresolvedNestedSelectSpec::Wildcard { .. })
    }

    /// Reverse property selections at this level.
    pub fn reverse(&self) -> &UnresolvedReverseMap {
        match self {
            UnresolvedNestedSelectSpec::Wildcard { reverse, .. }
            | UnresolvedNestedSelectSpec::Explicit { reverse, .. } => reverse,
        }
    }

    /// Returns `true` if the level produces no output (empty Explicit
    /// selection with no reverse).
    pub fn is_empty(&self) -> bool {
        match self {
            UnresolvedNestedSelectSpec::Wildcard { .. } => false,
            UnresolvedNestedSelectSpec::Explicit { forward, reverse } => {
                forward.is_empty() && reverse.is_empty()
            }
        }
    }
}

/// One forward item in an explicit (non-wildcard) selection level.
///
/// Examples:
/// - `"ex:name"` → `Property { predicate: "ex:name", sub_spec: None }`
/// - `{"ex:friend": ["*"]}` → `Property { sub_spec: Some(...) }`
/// - `"@id"` → `Id`
#[derive(Debug, Clone, PartialEq)]
pub enum UnresolvedForwardItem {
    /// Explicit `@id` selection.
    Id,
    /// A specific property with optional nested expansion of its values.
    Property {
        /// Predicate IRI (expanded)
        predicate: String,
        sub_spec: Option<Box<UnresolvedNestedSelectSpec>>,
    },
}

/// Hydration spec (unresolved).
///
/// Captures the hydration syntax for nested JSON-LD objects. Lowered into
/// [`crate::ir::HydrationSpec`].
///
/// # Examples
///
/// ```json
/// // Simple hydration with wildcard
/// {"select": {"?person": ["*"]}}
///
/// // Nested hydration
/// {"select": {"?person": ["*", {"ex:friend": ["*"]}]}}
///
/// // With depth parameter
/// {"select": {"?s": ["*"]}, "depth": 3}
///
/// // IRI constant root (no WHERE needed)
/// {"select": {"ex:alice": ["*"]}}
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct UnresolvedHydrationSpec {
    /// Root of the hydration — variable or IRI constant.
    pub root: UnresolvedRoot,
    /// Selection at the top level of the hydration.
    pub level: UnresolvedNestedSelectSpec,
    /// Max depth for auto-expansion (0 = no auto-expand, only explicit).
    pub depth: usize,
}

impl UnresolvedHydrationSpec {
    /// Create a hydration with the given root and top-level selection,
    /// `depth: 0`.
    pub fn new(root: UnresolvedRoot, level: UnresolvedNestedSelectSpec) -> Self {
        Self {
            root,
            level,
            depth: 0,
        }
    }
}

/// One column of a SELECT projection (unresolved).
///
/// Lowered into [`crate::ir::Column`].
#[derive(Debug, Clone, PartialEq)]
pub enum UnresolvedColumn {
    /// Project a single variable's binding (e.g. `"?name"`).
    Var(Arc<str>),
    /// Hydrate a subject (variable or IRI constant) into a nested JSON-LD
    /// object (e.g. `{"?person": ["*"]}`).
    Hydration(UnresolvedHydrationSpec),
}

impl UnresolvedColumn {
    /// Returns the variable name if this is a `Var` column.
    pub fn var_name(&self) -> Option<&str> {
        match self {
            UnresolvedColumn::Var(name) => Some(name),
            UnresolvedColumn::Hydration(_) => None,
        }
    }

    /// Returns the hydration spec if this is a `Hydration` column.
    pub fn as_hydration(&self) -> Option<&UnresolvedHydrationSpec> {
        match self {
            UnresolvedColumn::Hydration(spec) => Some(spec),
            UnresolvedColumn::Var(_) => None,
        }
    }
}

/// The columns a SELECT query produces (unresolved). Mirrors
/// [`crate::ir::Projection`].
#[derive(Debug, Clone, PartialEq)]
pub enum UnresolvedProjection {
    /// SELECT * — all in-scope WHERE-bound variables, rendered raw.
    Wildcard,
    /// Array-form rows: each row is `[v1, v2, ...]` of any arity.
    Tuple(Vec<UnresolvedColumn>),
    /// Bare-value rows from JSON-LD `select: "?x"` — exactly one column,
    /// each row is a bare value (not wrapped in an array).
    Scalar(UnresolvedColumn),
}

impl Default for UnresolvedProjection {
    /// An empty `Tuple` projection — the parser populates it as columns
    /// are encountered.
    fn default() -> Self {
        UnresolvedProjection::Tuple(Vec::new())
    }
}

impl UnresolvedProjection {
    /// Columns in render order. Empty for `Wildcard`.
    pub fn columns(&self) -> &[UnresolvedColumn] {
        match self {
            UnresolvedProjection::Wildcard => &[],
            UnresolvedProjection::Tuple(cs) => cs,
            UnresolvedProjection::Scalar(c) => std::slice::from_ref(c),
        }
    }

    /// The hydration spec embedded in the projection, if any.
    pub fn hydration(&self) -> Option<&UnresolvedHydrationSpec> {
        self.columns().iter().find_map(UnresolvedColumn::as_hydration)
    }

    /// Mutable access to the hydration spec, if any.
    pub fn hydration_mut(&mut self) -> Option<&mut UnresolvedHydrationSpec> {
        match self {
            UnresolvedProjection::Wildcard => None,
            UnresolvedProjection::Tuple(cs) => cs.iter_mut().find_map(|c| match c {
                UnresolvedColumn::Hydration(spec) => Some(spec),
                UnresolvedColumn::Var(_) => None,
            }),
            UnresolvedProjection::Scalar(c) => match c {
                UnresolvedColumn::Hydration(spec) => Some(spec),
                UnresolvedColumn::Var(_) => None,
            },
        }
    }
}

/// Ordered pattern in where clause
///
/// Each variant represents a different pattern type. The order in the
/// where clause is preserved to enable proper filter placement and
/// join ordering.
#[derive(Debug, Clone)]
pub enum UnresolvedPattern {
    /// Basic triple pattern
    Triple(UnresolvedTriplePattern),
    /// Filter expression (positioned in where clause order)
    Filter(UnresolvedExpression),
    /// Optional clause - left join semantics
    Optional(Vec<UnresolvedPattern>),
    /// Union of pattern branches - any branch may match
    ///
    /// Each branch is a list of patterns executed as a correlated subquery against
    /// the current solution stream.
    Union(Vec<Vec<UnresolvedPattern>>),
    /// Bind a computed value to a variable
    Bind {
        var: Arc<str>,
        expr: UnresolvedExpression,
    },
    /// Inline VALUES block - constant rows to join with the current solution stream
    Values {
        /// Variables defined by VALUES (column order)
        vars: Vec<Arc<str>>,
        /// Rows of bindings (each row has one cell per var)
        rows: Vec<Vec<UnresolvedValue>>,
    },
    /// MINUS clause - anti-join semantics (set difference)
    Minus(Vec<UnresolvedPattern>),
    /// EXISTS clause - filter rows where subquery matches
    Exists(Vec<UnresolvedPattern>),
    /// NOT EXISTS clause - filter rows where subquery does NOT match
    NotExists(Vec<UnresolvedPattern>),
    /// Property path pattern (parsed from `@path` context alias)
    Path {
        subject: UnresolvedTerm,
        path: UnresolvedPathExpr,
        object: UnresolvedTerm,
    },
    /// Subquery pattern - runs an inner query and merges results with parent
    ///
    /// Syntax: `["query", { "select": [...], "where": {...} }]`
    ///
    /// Variables shared between parent and subquery are correlated.
    /// The subquery's select list determines which variables are returned.
    Subquery(Box<UnresolvedQuery>),

    /// Index search pattern - BM25 full-text search against a graph source
    ///
    /// Syntax:
    /// ```json
    /// {
    ///   "f:graphSource": "my-search:main",
    ///   "f:searchText": "software engineer",
    ///   "f:searchResult": "?doc"
    /// }
    /// ```
    IndexSearch(UnresolvedIndexSearchPattern),

    /// Vector search pattern - similarity search against a vector graph source
    ///
    /// Syntax:
    /// ```json
    /// {
    ///   "f:graphSource": "embeddings:main",
    ///   "f:queryVector": [0.1, 0.2, 0.3],
    ///   "f:distanceMetric": "cosine",
    ///   "f:searchLimit": 10,
    ///   "f:searchResult": {"f:resultId": "?doc", "f:resultScore": "?score"}
    /// }
    /// ```
    VectorSearch(UnresolvedVectorSearchPattern),

    /// GRAPH pattern - scope inner patterns to a named graph
    ///
    /// Syntax: `["graph", "graph-name", pattern1, pattern2, ...]`
    ///
    /// - First argument is the graph name (string or variable)
    /// - Remaining arguments are patterns to execute within that graph
    ///
    /// Example:
    /// ```json
    /// ["graph", "test/movies", {"@id": "?movie", "name": "?name"}]
    /// ```
    Graph {
        /// Graph name (IRI string or variable like "?g")
        name: Arc<str>,
        /// Patterns to execute within the graph
        patterns: Vec<UnresolvedPattern>,
    },
}

impl UnresolvedPattern {
    /// Create a triple pattern
    pub fn triple(pattern: UnresolvedTriplePattern) -> Self {
        UnresolvedPattern::Triple(pattern)
    }

    /// Create a filter pattern
    pub fn filter(expr: UnresolvedExpression) -> Self {
        UnresolvedPattern::Filter(expr)
    }

    /// Create an optional pattern
    pub fn optional(patterns: Vec<UnresolvedPattern>) -> Self {
        UnresolvedPattern::Optional(patterns)
    }

    /// Create a union pattern
    pub fn union(branches: Vec<Vec<UnresolvedPattern>>) -> Self {
        UnresolvedPattern::Union(branches)
    }

    /// Create a values pattern
    pub fn values(vars: Vec<Arc<str>>, rows: Vec<Vec<UnresolvedValue>>) -> Self {
        UnresolvedPattern::Values { vars, rows }
    }

    /// Create a graph pattern
    pub fn graph(name: impl Into<Arc<str>>, patterns: Vec<UnresolvedPattern>) -> Self {
        UnresolvedPattern::Graph {
            name: name.into(),
            patterns,
        }
    }

    /// Check if this is a triple pattern
    pub fn is_triple(&self) -> bool {
        matches!(self, UnresolvedPattern::Triple(_))
    }

    /// Get the triple pattern if this is a Triple
    pub fn as_triple(&self) -> Option<&UnresolvedTriplePattern> {
        match self {
            UnresolvedPattern::Triple(tp) => Some(tp),
            _ => None,
        }
    }
}

/// Unresolved query - the result of parsing before IRI resolution
#[derive(Debug, Clone)]
pub struct UnresolvedQuery {
    /// Parsed JSON-LD context (for result formatting and further expansion)
    pub context: ParsedContext,
    /// Original JSON context from the query (for CONSTRUCT output)
    pub orig_context: Option<serde_json::Value>,
    /// Projection (columns + shape). Lowered into [`crate::ir::Projection`].
    pub select: UnresolvedProjection,
    /// Ordered patterns in where clause (triples, filters, optionals, etc.)
    pub patterns: Vec<UnresolvedPattern>,
    /// Query options (limit, offset, order by, group by, etc.)
    pub options: UnresolvedOptions,
    /// CONSTRUCT template (None for SELECT queries)
    pub construct_template: Option<UnresolvedConstructTemplate>,
}

impl UnresolvedQuery {
    /// Create a new unresolved query
    pub fn new(context: ParsedContext) -> Self {
        Self {
            context,
            orig_context: None,
            select: UnresolvedProjection::default(),
            patterns: Vec::new(),
            options: UnresolvedOptions::default(),
            construct_template: None,
        }
    }

    /// Append a Var column. The projection must be in `Tuple` mode
    /// (the default for [`Self::new`]).
    pub fn add_select(&mut self, var: impl AsRef<str>) {
        self.push_column(UnresolvedColumn::Var(Arc::from(var.as_ref())));
    }

    /// Append a Hydration column. The projection must be in `Tuple` mode.
    pub fn add_hydration(&mut self, spec: UnresolvedHydrationSpec) {
        self.push_column(UnresolvedColumn::Hydration(spec));
    }

    /// Append a column to a `Tuple` projection. Panics if `select` is not
    /// `Tuple` (which only happens after `select` has been finalized to
    /// `Wildcard` or `Scalar` — neither supports incremental appends).
    fn push_column(&mut self, column: UnresolvedColumn) {
        match &mut self.select {
            UnresolvedProjection::Tuple(cs) => cs.push(column),
            _ => panic!("cannot append a column to a non-Tuple projection"),
        }
    }

    /// Returns the hydration spec embedded in the projection, if any.
    pub fn hydration(&self) -> Option<&UnresolvedHydrationSpec> {
        self.select.hydration()
    }

    /// Mutable access to the hydration spec, if any.
    pub fn hydration_mut(&mut self) -> Option<&mut UnresolvedHydrationSpec> {
        self.select.hydration_mut()
    }

    /// Add a triple pattern (convenience method that wraps in UnresolvedPattern)
    pub fn add_pattern(&mut self, pattern: UnresolvedTriplePattern) {
        self.patterns.push(UnresolvedPattern::Triple(pattern));
    }

    /// Add a filter expression
    pub fn add_filter(&mut self, expr: UnresolvedExpression) {
        self.patterns.push(UnresolvedPattern::Filter(expr));
    }

    /// Add an optional block
    pub fn add_optional(&mut self, patterns: Vec<UnresolvedPattern>) {
        self.patterns.push(UnresolvedPattern::Optional(patterns));
    }

    /// Get all triple patterns (flattening nested structures)
    pub fn triple_patterns(&self) -> Vec<&UnresolvedTriplePattern> {
        fn collect<'a>(
            patterns: &'a [UnresolvedPattern],
            out: &mut Vec<&'a UnresolvedTriplePattern>,
        ) {
            for p in patterns {
                match p {
                    UnresolvedPattern::Triple(tp) => out.push(tp),
                    UnresolvedPattern::Optional(inner)
                    | UnresolvedPattern::Minus(inner)
                    | UnresolvedPattern::Exists(inner)
                    | UnresolvedPattern::NotExists(inner) => collect(inner, out),
                    UnresolvedPattern::Union(branches) => {
                        for branch in branches {
                            collect(branch, out);
                        }
                    }
                    UnresolvedPattern::Graph {
                        patterns: inner, ..
                    } => collect(inner, out),
                    UnresolvedPattern::Filter(_)
                    | UnresolvedPattern::Bind { .. }
                    | UnresolvedPattern::Values { .. }
                    | UnresolvedPattern::Path { .. }
                    | UnresolvedPattern::Subquery(_)
                    | UnresolvedPattern::IndexSearch(_)
                    | UnresolvedPattern::VectorSearch(_) => {}
                }
            }
        }

        let mut result = Vec::new();
        collect(&self.patterns, &mut result);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_literal_value_creation() {
        let s = LiteralValue::string("hello");
        assert!(matches!(s, LiteralValue::String(_)));

        let l = LiteralValue::Long(42);
        assert_eq!(l, LiteralValue::Long(42));

        let d = LiteralValue::Double(3.13);
        assert!(matches!(d, LiteralValue::Double(_)));

        let b = LiteralValue::Boolean(true);
        assert_eq!(b, LiteralValue::Boolean(true));

        let v = LiteralValue::vector(vec![0.1, 0.2]);
        assert!(matches!(v, LiteralValue::Vector(_)));
    }

    #[test]
    fn test_unresolved_term_creation() {
        let var = UnresolvedTerm::var("?name");
        assert!(var.is_var());
        assert_eq!(var.as_var(), Some("?name"));

        let iri = UnresolvedTerm::iri("http://schema.org/name");
        assert!(!iri.is_var());
        assert_eq!(iri.as_var(), None);

        let lit = UnresolvedTerm::long(42);
        assert!(!lit.is_var());
    }

    #[test]
    fn test_unresolved_pattern_creation() {
        let pattern = UnresolvedTriplePattern::new(
            UnresolvedTerm::var("?s"),
            UnresolvedTerm::iri("http://schema.org/name"),
            UnresolvedTerm::var("?name"),
        );

        assert!(pattern.s.is_var());
        assert!(!pattern.p.is_var());
        assert!(pattern.o.is_var());
        assert!(pattern.dtc.is_none());
    }

    #[test]
    fn test_unresolved_pattern_with_dt() {
        let pattern = UnresolvedTriplePattern::with_dt(
            UnresolvedTerm::var("?s"),
            UnresolvedTerm::iri("http://schema.org/age"),
            UnresolvedTerm::var("?age"),
            "http://www.w3.org/2001/XMLSchema#integer",
        );

        assert!(pattern.dtc.is_some());
        assert_eq!(
            pattern
                .dtc
                .as_ref()
                .map(fluree_vocab::UnresolvedDatatypeConstraint::datatype_iri),
            Some("http://www.w3.org/2001/XMLSchema#integer")
        );
    }

    #[test]
    fn test_arc_sharing() {
        let var1 = UnresolvedTerm::var("?name");
        let var2 = var1.clone();

        // Both should share the same Arc
        if let (UnresolvedTerm::Var(a1), UnresolvedTerm::Var(a2)) = (&var1, &var2) {
            assert!(Arc::ptr_eq(a1, a2));
        }
    }
}
