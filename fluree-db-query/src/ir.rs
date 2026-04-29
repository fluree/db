//! Intermediate Representation for query execution
//!
//! This module provides the logical pattern IR that represents query structure.
//! The planner transforms this IR into physical operators.
//!
//! # Design
//!
//! - `Query` is the top-level structure with `select` and ordered `where_` patterns
//! - `Pattern` enum mirrors the where clause structure, preserving order for filter inlining
//! - The planner chooses physical join operators based on pattern analysis

use crate::binding::Binding;
use crate::sort::SortSpec;
use crate::triple::{Ref, TriplePattern};
use crate::var_registry::VarId;
use fluree_db_core::Sid;

/// Top-level query structure
///
/// Represents a parsed and lowered query ready for planning.
#[derive(Debug, Clone)]
pub struct Query {
    /// Selected variables (output columns)
    pub select: Vec<VarId>,
    /// Where clause patterns (ordered!)
    pub where_: Vec<Pattern>,
}

impl Query {
    /// Create a new query
    pub fn new(select: Vec<VarId>, where_: Vec<Pattern>) -> Self {
        Self { select, where_ }
    }

    /// Create a query with a single triple pattern
    pub fn single(select: Vec<VarId>, pattern: TriplePattern) -> Self {
        Self {
            select,
            where_: vec![Pattern::Triple(pattern)],
        }
    }

    /// Get all triple patterns in the where clause (flattening nested structures)
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
                    Pattern::Graph { patterns, .. } => collect(patterns, out),
                    Pattern::Service(sp) => collect(&sp.patterns, out),
                    Pattern::Filter(_)
                    | Pattern::Bind { .. }
                    | Pattern::Values { .. }
                    | Pattern::PropertyPath(_)
                    | Pattern::Subquery(_)
                    | Pattern::IndexSearch(_)
                    | Pattern::VectorSearch(_)
                    | Pattern::R2rml(_)
                    | Pattern::GeoSearch(_)
                    | Pattern::S2Search(_) => {}
                }
            }
        }

        let mut result = Vec::new();
        collect(&self.where_, &mut result);
        result
    }
}

// ============================================================================
// Graph crawl select types (resolved)
// ============================================================================

/// Root of a graph crawl select (resolved)
///
/// Supports both variable and IRI constant roots:
/// - Variable root: from query results (e.g., `?person`)
/// - IRI constant root: direct subject fetch (e.g., `ex:alice`)
#[derive(Debug, Clone, PartialEq)]
pub enum Root {
    /// Variable root - value comes from query results
    Var(VarId),
    /// IRI constant root - direct subject lookup
    Sid(Sid),
}

/// Nested selection specification for sub-crawls (resolved)
///
/// This type captures the full selection state for nested property expansion,
/// including both forward and reverse properties.
#[derive(Debug, Clone, PartialEq)]
pub struct NestedSelectSpec {
    /// Forward property selections
    pub forward: Vec<SelectionSpec>,
    /// Reverse property selections (predicate Sid → optional nested spec)
    /// None means no sub-selections (just return @id), Some means nested expansion
    pub reverse: std::collections::HashMap<Sid, Option<Box<NestedSelectSpec>>>,
    /// Whether wildcard was specified at this level
    pub has_wildcard: bool,
}

impl NestedSelectSpec {
    /// Create a new nested select spec
    pub fn new(
        forward: Vec<SelectionSpec>,
        reverse: std::collections::HashMap<Sid, Option<Box<NestedSelectSpec>>>,
        has_wildcard: bool,
    ) -> Self {
        Self {
            forward,
            reverse,
            has_wildcard,
        }
    }

    /// Check if this spec is empty (no selections)
    pub fn is_empty(&self) -> bool {
        self.forward.is_empty() && self.reverse.is_empty()
    }
}

/// Selection specification for graph crawl (resolved)
///
/// Defines what properties to include at each level of expansion.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectionSpec {
    /// Explicit @id selection (include @id even when wildcard is not specified)
    Id,
    /// Wildcard - select all properties at this level
    Wildcard,
    /// Property selection with optional nested expansion
    Property {
        /// Predicate Sid
        predicate: Sid,
        /// Optional nested selection spec for expanding this property's values
        /// Uses Box to avoid infinite type recursion
        sub_spec: Option<Box<NestedSelectSpec>>,
    },
}

/// Graph crawl selection specification (resolved, with Sids)
///
/// This is the resolved form of `UnresolvedGraphSelectSpec`, with all IRIs
/// encoded as Sids. Used during result formatting for nested JSON-LD output.
#[derive(Debug, Clone, PartialEq)]
pub struct GraphSelectSpec {
    /// Root of the crawl - variable or IRI constant
    pub root: Root,
    /// Forward property selections
    pub selections: Vec<SelectionSpec>,
    /// Reverse property selections (predicate Sid → optional nested spec)
    /// None means no sub-selections (just return @id), Some means nested expansion
    pub reverse: std::collections::HashMap<Sid, Option<Box<NestedSelectSpec>>>,
    /// Max depth for auto-expansion (0 = no auto-expand)
    pub depth: usize,
    /// Whether wildcard was specified (controls @id inclusion)
    pub has_wildcard: bool,
}

impl GraphSelectSpec {
    /// Create a new graph select spec
    pub fn new(root: Root, selections: Vec<SelectionSpec>) -> Self {
        let has_wildcard = selections
            .iter()
            .any(|s| matches!(s, SelectionSpec::Wildcard));
        Self {
            root,
            selections,
            reverse: std::collections::HashMap::new(),
            depth: 0,
            has_wildcard,
        }
    }

    /// Returns the root variable, if the root is a variable (not an IRI constant).
    pub fn root_var(&self) -> Option<VarId> {
        match &self.root {
            Root::Var(v) => Some(*v),
            Root::Sid(_) => None,
        }
    }

    /// Generate a hash for cache keying purposes
    ///
    /// Used to differentiate the same Sid expanded under different specs.
    /// The cache key is `(Sid, spec_hash, depth_remaining)`.
    pub fn spec_hash(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();

        self.has_wildcard.hash(&mut hasher);
        self.selections.len().hash(&mut hasher);

        // Hash selection structure
        fn hash_selection(spec: &SelectionSpec, hasher: &mut impl Hasher) {
            match spec {
                SelectionSpec::Id => {
                    2u8.hash(hasher);
                }
                SelectionSpec::Wildcard => {
                    0u8.hash(hasher);
                }
                SelectionSpec::Property {
                    predicate,
                    sub_spec,
                } => {
                    1u8.hash(hasher);
                    predicate.hash(hasher);
                    if let Some(nested) = sub_spec {
                        // Hash nested spec
                        nested.has_wildcard.hash(hasher);
                        nested.forward.len().hash(hasher);
                        for sub in &nested.forward {
                            hash_selection(sub, hasher);
                        }
                        // Hash nested reverse
                        nested.reverse.len().hash(hasher);
                        let mut nested_rev_keys: Vec<_> = nested.reverse.keys().collect();
                        nested_rev_keys.sort();
                        for key in nested_rev_keys {
                            key.hash(hasher);
                            if let Some(nested_nested) = nested.reverse.get(key) {
                                if let Some(spec) = nested_nested {
                                    1u8.hash(hasher);
                                    hash_nested_spec(spec, hasher);
                                } else {
                                    0u8.hash(hasher);
                                }
                            }
                        }
                    } else {
                        0usize.hash(hasher);
                    }
                }
            }
        }

        fn hash_nested_spec(spec: &NestedSelectSpec, hasher: &mut impl Hasher) {
            spec.has_wildcard.hash(hasher);
            spec.forward.len().hash(hasher);
            for sub in &spec.forward {
                hash_selection(sub, hasher);
            }
            spec.reverse.len().hash(hasher);
            let mut rev_keys: Vec<_> = spec.reverse.keys().collect();
            rev_keys.sort();
            for key in rev_keys {
                key.hash(hasher);
                if let Some(nested) = spec.reverse.get(key) {
                    if let Some(inner) = nested {
                        1u8.hash(hasher);
                        hash_nested_spec(inner, hasher);
                    } else {
                        0u8.hash(hasher);
                    }
                }
            }
        }

        for sel in &self.selections {
            hash_selection(sel, &mut hasher);
        }

        // Hash reverse properties
        self.reverse.len().hash(&mut hasher);
        // Sort by Sid for deterministic hashing
        let mut reverse_keys: Vec<_> = self.reverse.keys().collect();
        reverse_keys.sort();
        for key in reverse_keys {
            key.hash(&mut hasher);
            if let Some(nested) = self.reverse.get(key) {
                if let Some(spec) = nested {
                    1u8.hash(&mut hasher);
                    hash_nested_spec(spec, &mut hasher);
                } else {
                    0u8.hash(&mut hasher);
                }
            }
        }

        hasher.finish()
    }
}

/// Property path modifier (transitive operators)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathModifier {
    /// + : one or more (at least one hop)
    OneOrMore,
    /// * : zero or more (includes starting node)
    ZeroOrMore,
}

/// Resolved property path pattern for transitive traversal.
///
/// Produced by `@path` aliases with `+` or `*` modifiers, e.g.:
/// `{"@context": {"knowsPlus": {"@path": "ex:knows+"}}, "where": [{"@id": "ex:alice", "knowsPlus": "?who"}]}`
#[derive(Debug, Clone)]
pub struct PropertyPathPattern {
    /// Subject ref (Var or Sid — literals not allowed)
    pub subject: Ref,
    /// Predicate to traverse (always resolved to Sid)
    pub predicate: Sid,
    /// Path modifier (+ or *)
    pub modifier: PathModifier,
    /// Object ref (Var or Sid — literals not allowed)
    pub object: Ref,
}

impl PropertyPathPattern {
    /// Create a new property path pattern
    pub fn new(subject: Ref, predicate: Sid, modifier: PathModifier, object: Ref) -> Self {
        Self {
            subject,
            predicate,
            modifier,
            object,
        }
    }

    /// Get variables from subject and object
    pub fn variables(&self) -> Vec<VarId> {
        let mut vars = Vec::with_capacity(2);
        if let Ref::Var(v) = &self.subject {
            vars.push(*v);
        }
        if let Ref::Var(v) = &self.object {
            vars.push(*v);
        }
        vars
    }
}

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
    pub aggregates: Vec<crate::aggregate::AggregateSpec>,
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
    pub fn with_aggregates(mut self, specs: Vec<crate::aggregate::AggregateSpec>) -> Self {
        self.aggregates = specs;
        self
    }

    /// Get variables from the select list
    pub fn variables(&self) -> Vec<VarId> {
        self.select.clone()
    }
}

// ============================================================================
// Index Search Pattern (BM25 Full-Text Search)
// ============================================================================

/// Index search pattern for BM25 full-text queries.
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
pub struct IndexSearchPattern {
    /// Graph source alias (e.g., "my-search:main")
    pub graph_source_id: String,

    /// Search query target - can be a constant string or variable
    pub target: IndexSearchTarget,

    /// Maximum number of results (optional)
    pub limit: Option<usize>,

    /// Variable to bind the document IRI (required)
    pub id_var: VarId,

    /// Variable to bind the BM25 score (optional)
    pub score_var: Option<VarId>,

    /// Variable to bind the source ledger alias (optional, for multi-ledger)
    pub ledger_var: Option<VarId>,

    /// Whether to sync before query (default: false)
    pub sync: bool,

    /// Query timeout in milliseconds (optional)
    pub timeout: Option<u64>,
}

/// Target for index search - can be a constant query string or variable.
#[derive(Debug, Clone)]
pub enum IndexSearchTarget {
    /// Constant search query string
    Const(String),
    /// Variable reference (bound at runtime)
    Var(VarId),
}

impl IndexSearchPattern {
    /// Create a new index search pattern with just ID binding
    pub fn new(
        graph_source_id: impl Into<String>,
        target: IndexSearchTarget,
        id_var: VarId,
    ) -> Self {
        Self {
            graph_source_id: graph_source_id.into(),
            target,
            limit: None,
            id_var,
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
    pub fn with_score_var(mut self, var: VarId) -> Self {
        self.score_var = Some(var);
        self
    }

    /// Set the ledger binding variable
    pub fn with_ledger_var(mut self, var: VarId) -> Self {
        self.ledger_var = Some(var);
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

    /// Get all variables referenced by this pattern
    pub fn variables(&self) -> Vec<VarId> {
        let mut vars = vec![self.id_var];

        if let IndexSearchTarget::Var(v) = &self.target {
            vars.push(*v);
        }

        if let Some(v) = self.score_var {
            vars.push(v);
        }

        if let Some(v) = self.ledger_var {
            vars.push(v);
        }

        vars
    }
}

// ============================================================================
// Vector Search Pattern
// ============================================================================

/// Vector similarity search pattern for querying vector graph sources.
///
/// # Example
///
/// Simple search with constant vector:
/// ```json
/// {
///   "f:graphSource": "embeddings:main",
///   "f:queryVector": [0.1, 0.2, 0.3],
///   "f:distanceMetric": "cosine",
///   "f:searchLimit": 10,
///   "f:searchResult": "?doc"
/// }
/// ```
///
/// Search with variable vector:
/// ```json
/// {
///   "f:graphSource": "embeddings:main",
///   "f:queryVector": "?queryVec",
///   "f:distanceMetric": "dot",
///   "f:searchResult": {
///     "f:resultId": "?doc",
///     "f:resultScore": "?score"
///   }
/// }
/// ```
#[derive(Debug, Clone)]
pub struct VectorSearchPattern {
    /// Graph source alias (e.g., "embeddings:main")
    pub graph_source_id: String,

    /// Search target - can be a constant vector or variable
    pub target: VectorSearchTarget,

    /// Distance metric for similarity search
    pub metric: crate::vector::DistanceMetric,

    /// Maximum number of results (optional, defaults to 10)
    pub limit: Option<usize>,

    /// Variable to bind the document IRI (required)
    pub id_var: VarId,

    /// Variable to bind the similarity score (optional)
    pub score_var: Option<VarId>,

    /// Variable to bind the source ledger alias (optional, for multi-ledger)
    pub ledger_var: Option<VarId>,

    /// Whether to sync before query (default: false)
    pub sync: bool,

    /// Query timeout in milliseconds (optional)
    pub timeout: Option<u64>,
}

/// Target for vector search - can be a constant vector or variable.
#[derive(Debug, Clone)]
pub enum VectorSearchTarget {
    /// Constant vector (f32 for efficiency)
    Const(Vec<f32>),
    /// Variable reference (bound at runtime to a fluree:vector value)
    Var(VarId),
}

impl VectorSearchPattern {
    /// Create a new vector search pattern with just ID binding
    pub fn new(
        graph_source_id: impl Into<String>,
        target: VectorSearchTarget,
        id_var: VarId,
    ) -> Self {
        Self {
            graph_source_id: graph_source_id.into(),
            target,
            metric: crate::vector::DistanceMetric::default(),
            limit: None,
            id_var,
            score_var: None,
            ledger_var: None,
            sync: false,
            timeout: None,
        }
    }

    /// Set the distance metric
    pub fn with_metric(mut self, metric: crate::vector::DistanceMetric) -> Self {
        self.metric = metric;
        self
    }

    /// Set the result limit
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set the score binding variable
    pub fn with_score_var(mut self, var: VarId) -> Self {
        self.score_var = Some(var);
        self
    }

    /// Set the ledger binding variable
    pub fn with_ledger_var(mut self, var: VarId) -> Self {
        self.ledger_var = Some(var);
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

    /// Get all variables referenced by this pattern
    pub fn variables(&self) -> Vec<VarId> {
        let mut vars = vec![self.id_var];

        if let VectorSearchTarget::Var(v) = &self.target {
            vars.push(*v);
        }

        if let Some(v) = self.score_var {
            vars.push(v);
        }

        if let Some(v) = self.ledger_var {
            vars.push(v);
        }

        vars
    }
}

// ============================================================================
// GeoSearch Pattern
// ============================================================================

/// Geographic proximity search pattern - index-accelerated spatial queries.
///
/// Queries the binary index for GeoPoint values within a specified radius
/// of a center point. Uses the latitude-primary encoding for efficient
/// latitude-band scans, then applies haversine post-filter for exact distance.
///
/// # Source Patterns
///
/// Created by `geo_rewrite` from Triple + Bind(geof:distance) + Filter patterns:
///
/// ```json
/// { "@id": "?place", "ex:location": "?loc" },
/// ["bind", "?dist", "(geof:distance ?loc \"POINT(2.3522 48.8566)\")"],
/// ["filter", "(<= ?dist 500000)"]
/// ```
#[derive(Debug, Clone)]
pub struct GeoSearchPattern {
    /// Predicate SID for the location property to search
    pub predicate: Sid,

    /// Center point for proximity search
    pub center: GeoSearchCenter,

    /// Search radius in meters
    pub radius_meters: f64,

    /// Maximum number of results (optional)
    pub limit: Option<usize>,

    /// Variable to bind the subject IRI (required)
    pub subject_var: VarId,

    /// Variable to bind the distance in meters (optional)
    pub distance_var: Option<VarId>,
}

/// Center point for geo search - can be constant or variable.
#[derive(Debug, Clone)]
pub enum GeoSearchCenter {
    /// Constant lat/lng coordinates
    Const { lat: f64, lng: f64 },
    /// Variable reference (bound at runtime to a GeoPoint value)
    Var(VarId),
}

impl GeoSearchPattern {
    /// Create a new geo search pattern
    pub fn new(
        predicate: Sid,
        center: GeoSearchCenter,
        radius_meters: f64,
        subject_var: VarId,
    ) -> Self {
        Self {
            predicate,
            center,
            radius_meters,
            limit: None,
            subject_var,
            distance_var: None,
        }
    }

    /// Set the result limit
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set the distance binding variable
    pub fn with_distance_var(mut self, var: VarId) -> Self {
        self.distance_var = Some(var);
        self
    }

    /// Get all variables referenced by this pattern
    pub fn variables(&self) -> Vec<VarId> {
        let mut vars = vec![self.subject_var];

        if let GeoSearchCenter::Var(v) = &self.center {
            vars.push(*v);
        }

        if let Some(v) = self.distance_var {
            vars.push(v);
        }

        vars
    }

    /// Get the center coordinates if constant, or None if variable
    pub fn const_center(&self) -> Option<(f64, f64)> {
        match &self.center {
            GeoSearchCenter::Const { lat, lng } => Some((*lat, *lng)),
            GeoSearchCenter::Var(_) => None,
        }
    }
}

// ============================================================================
// S2 Spatial Search Pattern
// ============================================================================

/// S2-based spatial search pattern for complex geometry queries.
///
/// Uses the S2 spatial index sidecar for efficient queries on non-point
/// geometries (polygons, linestrings, etc.). Supports:
/// - `within`: subjects whose geometry is within query geometry
/// - `contains`: subjects whose geometry contains query geometry
/// - `intersects`: subjects whose geometry intersects query geometry
///
/// # Example (within query)
///
/// ```sparql
/// ?building geo:sfWithin "POLYGON((...))".
/// ```
#[derive(Debug, Clone)]
pub struct S2SearchPattern {
    /// Spatial predicate type
    pub operation: S2SpatialOp,

    /// Variable to bind matching subject IRIs
    pub subject_var: VarId,

    /// Query geometry specification (WKT literal or variable)
    pub query_geom: S2QueryGeom,

    /// Predicate IRI whose geometries are indexed (e.g., "http://example.org/hasGeometry").
    ///
    /// Used to route to the correct spatial index provider when multiple predicates
    /// have spatial indexes. If None, uses the default/only provider.
    pub predicate: Option<String>,

    /// Optional variable to bind distance (for nearby queries)
    pub distance_var: Option<VarId>,

    /// Optional limit on results
    pub limit: Option<usize>,

    /// Spatial index alias (e.g., "geo-index:main")
    pub spatial_index_alias: Option<String>,
}

/// Spatial operation types for S2 queries.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum S2SpatialOp {
    /// Subject geometry is within query geometry
    Within,
    /// Subject geometry contains query geometry
    Contains,
    /// Subject geometry intersects query geometry
    Intersects,
    /// Proximity query (like GeoSearch but using S2 sidecar)
    Nearby { radius_meters: f64 },
}

/// Query geometry for S2 searches - constant WKT or variable reference.
#[derive(Debug, Clone)]
pub enum S2QueryGeom {
    /// Constant WKT literal
    Wkt(String),
    /// Variable reference (bound to WKT string or GeoPoint at runtime)
    Var(VarId),
    /// Constant point (for nearby queries)
    Point { lat: f64, lng: f64 },
}

impl S2SearchPattern {
    /// Create a new within pattern
    pub fn within(subject_var: VarId, query_geom: S2QueryGeom) -> Self {
        Self {
            operation: S2SpatialOp::Within,
            subject_var,
            query_geom,
            predicate: None,
            distance_var: None,
            limit: None,
            spatial_index_alias: None,
        }
    }

    /// Create a new contains pattern
    pub fn contains(subject_var: VarId, query_geom: S2QueryGeom) -> Self {
        Self {
            operation: S2SpatialOp::Contains,
            subject_var,
            query_geom,
            predicate: None,
            distance_var: None,
            limit: None,
            spatial_index_alias: None,
        }
    }

    /// Create a new intersects pattern
    pub fn intersects(subject_var: VarId, query_geom: S2QueryGeom) -> Self {
        Self {
            operation: S2SpatialOp::Intersects,
            subject_var,
            query_geom,
            predicate: None,
            distance_var: None,
            limit: None,
            spatial_index_alias: None,
        }
    }

    /// Create a new nearby pattern
    pub fn nearby(subject_var: VarId, center: S2QueryGeom, radius_meters: f64) -> Self {
        Self {
            operation: S2SpatialOp::Nearby { radius_meters },
            subject_var,
            query_geom: center,
            predicate: None,
            distance_var: None,
            limit: None,
            spatial_index_alias: None,
        }
    }

    /// Set the predicate IRI for index routing
    pub fn with_predicate(mut self, predicate: impl Into<String>) -> Self {
        self.predicate = Some(predicate.into());
        self
    }

    /// Set distance variable (for nearby queries)
    pub fn with_distance_var(mut self, var: VarId) -> Self {
        self.distance_var = Some(var);
        self
    }

    /// Set limit
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set spatial index alias
    pub fn with_spatial_index(mut self, alias: impl Into<String>) -> Self {
        self.spatial_index_alias = Some(alias.into());
        self
    }

    /// Get all variables in this pattern
    pub fn variables(&self) -> Vec<VarId> {
        let mut vars = vec![self.subject_var];

        if let S2QueryGeom::Var(v) = &self.query_geom {
            vars.push(*v);
        }

        if let Some(v) = self.distance_var {
            vars.push(v);
        }

        vars
    }
}

// ============================================================================
// R2RML Pattern
// ============================================================================

/// R2RML scan pattern for querying Iceberg graph sources via R2RML mappings.
///
/// This pattern scans an Iceberg table through R2RML term maps and produces
/// RDF term bindings for subject and optional object variables.
///
/// # Example Query Pattern
///
/// ```sparql
/// ?person a ex:Person .
/// ?person ex:name ?name .
/// ```
///
/// With an R2RML mapping, this could be lowered to:
/// - R2rmlPattern with subject_var=?person, triples_map for ex:Person class
/// - R2rmlPattern with subject_var=?person, object_var=?name, predicate ex:name
#[derive(Debug, Clone)]
pub struct R2rmlPattern {
    /// Graph source alias (e.g., "airlines-r2rml:main")
    pub graph_source_id: String,

    /// Variable to bind the subject IRI
    pub subject_var: VarId,

    /// Variable to bind the object value (optional)
    ///
    /// If None, this pattern only materializes subjects (e.g., for rdf:type patterns).
    pub object_var: Option<VarId>,

    /// Specific TriplesMap IRI to use (optional)
    ///
    /// If provided, only this TriplesMap is scanned. Otherwise, the planner
    /// selects appropriate TriplesMap(s) based on class/predicate filters.
    pub triples_map_iri: Option<String>,

    /// Predicate IRI filter (optional)
    ///
    /// Limits scan to PredicateObjectMaps with this predicate.
    pub predicate_filter: Option<String>,

    /// Subject class filter (optional)
    ///
    /// Limits scan to TriplesMap(s) that produce this rdf:type.
    pub class_filter: Option<String>,
}

impl R2rmlPattern {
    /// Create a new R2RML pattern with subject and object variables.
    pub fn new(
        graph_source_id: impl Into<String>,
        subject_var: VarId,
        object_var: Option<VarId>,
    ) -> Self {
        Self {
            graph_source_id: graph_source_id.into(),
            subject_var,
            object_var,
            triples_map_iri: None,
            predicate_filter: None,
            class_filter: None,
        }
    }

    /// Set the specific TriplesMap IRI to use.
    pub fn with_triples_map(mut self, iri: impl Into<String>) -> Self {
        self.triples_map_iri = Some(iri.into());
        self
    }

    /// Set the predicate filter.
    pub fn with_predicate(mut self, predicate: impl Into<String>) -> Self {
        self.predicate_filter = Some(predicate.into());
        self
    }

    /// Set the class filter.
    pub fn with_class(mut self, class: impl Into<String>) -> Self {
        self.class_filter = Some(class.into());
        self
    }

    /// Get all variables referenced by this pattern.
    pub fn variables(&self) -> Vec<VarId> {
        let mut vars = vec![self.subject_var];
        if let Some(obj_var) = self.object_var {
            vars.push(obj_var);
        }
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
    Iri(std::sync::Arc<str>),
    /// Variable (iterates all named graphs, binds as IRI string)
    Var(VarId),
}

impl GraphName {
    /// Check if this is a variable
    pub fn is_var(&self) -> bool {
        matches!(self, GraphName::Var(_))
    }

    /// Get the variable ID if this is a variable
    pub fn as_var(&self) -> Option<VarId> {
        match self {
            GraphName::Var(v) => Some(*v),
            GraphName::Iri(_) => None,
        }
    }

    /// Get the IRI if this is a concrete graph
    pub fn as_iri(&self) -> Option<&str> {
        match self {
            GraphName::Iri(iri) => Some(iri),
            GraphName::Var(_) => None,
        }
    }
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
    Iri(std::sync::Arc<str>),
    /// Variable endpoint (iterates all known services if unbound)
    Var(VarId),
}

impl ServiceEndpoint {
    /// Check if this is a variable
    pub fn is_var(&self) -> bool {
        matches!(self, ServiceEndpoint::Var(_))
    }

    /// Get the variable ID if this is a variable
    pub fn as_var(&self) -> Option<VarId> {
        match self {
            ServiceEndpoint::Var(v) => Some(*v),
            ServiceEndpoint::Iri(_) => None,
        }
    }

    /// Get the IRI if this is a concrete endpoint
    pub fn as_iri(&self) -> Option<&str> {
        match self {
            ServiceEndpoint::Iri(iri) => Some(iri),
            ServiceEndpoint::Var(_) => None,
        }
    }
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
    pub source_body: Option<std::sync::Arc<str>>,
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
        source_body: std::sync::Arc<str>,
    ) -> Self {
        Self {
            silent,
            endpoint,
            patterns,
            source_body: Some(source_body),
        }
    }

    /// Get all variables referenced by this pattern
    pub fn variables(&self) -> Vec<VarId> {
        let mut vars: Vec<VarId> = self.patterns.iter().flat_map(Pattern::variables).collect();
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

    /// Get all variables referenced by this pattern
    pub fn variables(&self) -> Vec<VarId> {
        match self {
            Pattern::Triple(tp) => tp.variables(),
            Pattern::Filter(expr) => expr.variables(),
            Pattern::Optional(inner) => inner.iter().flat_map(Pattern::variables).collect(),
            Pattern::Union(branches) => branches
                .iter()
                .flat_map(|branch| branch.iter().flat_map(Pattern::variables))
                .collect(),
            Pattern::Bind { var, expr } => {
                let mut vars = expr.variables();
                vars.push(*var);
                vars
            }
            Pattern::Values { vars, .. } => vars.clone(),
            Pattern::Minus(inner) | Pattern::Exists(inner) | Pattern::NotExists(inner) => {
                inner.iter().flat_map(Pattern::variables).collect()
            }
            Pattern::PropertyPath(pp) => pp.variables(),
            Pattern::Subquery(sq) => sq.variables(),
            Pattern::IndexSearch(isp) => isp.variables(),
            Pattern::VectorSearch(vsp) => vsp.variables(),
            Pattern::R2rml(r2rml) => r2rml.variables(),
            Pattern::GeoSearch(gsp) => gsp.variables(),
            Pattern::S2Search(s2p) => s2p.variables(),
            Pattern::Graph { name, patterns } => {
                let mut vars = patterns
                    .iter()
                    .flat_map(Pattern::variables)
                    .collect::<Vec<_>>();
                if let GraphName::Var(v) = name {
                    vars.push(*v);
                }
                vars
            }
            Pattern::Service(sp) => sp.variables(),
        }
    }
}

/// Filter expression AST
///
/// Represents expressions that can be evaluated against solution bindings.
/// All operations are represented as function calls for uniform dispatch.
#[derive(Debug, Clone)]
pub enum Expression {
    /// Variable reference
    Var(VarId),
    /// Constant value
    Const(FilterValue),
    /// Function call (includes operators like +, -, =, AND, OR, etc.)
    Call {
        func: Function,
        args: Vec<Expression>,
    },
    /// EXISTS / NOT EXISTS subquery inside a compound filter expression.
    ///
    /// Used when EXISTS/NOT EXISTS appears as part of a larger expression
    /// (e.g., `FILTER(?x = ?y || NOT EXISTS { ... })`). Standalone
    /// `FILTER EXISTS { ... }` is handled at the pattern level instead.
    ///
    /// Evaluated asynchronously by the FilterOperator before the main
    /// expression: the result is pre-computed per row and substituted
    /// as a boolean constant.
    Exists {
        patterns: Vec<Pattern>,
        negated: bool,
    },
}

impl Expression {
    /// True if this expression (or any sub-expression / sub-pattern it
    /// contains) calls the given built-in function.
    ///
    /// Used by the query-context setup code as a perf guardrail: queries
    /// that don't call `fulltext(...)` skip building the per-graph fulltext
    /// arena map.
    pub fn contains_function(&self, target: &Function) -> bool {
        match self {
            Expression::Var(_) | Expression::Const(_) => false,
            Expression::Call { func, args } => {
                func == target || args.iter().any(|a| a.contains_function(target))
            }
            Expression::Exists { patterns, .. } => patterns
                .iter()
                .any(|p| pattern_contains_function(p, target)),
        }
    }
}

/// True if any expression inside `pattern` (recursively) calls `target`.
pub fn pattern_contains_function(pattern: &Pattern, target: &Function) -> bool {
    match pattern {
        Pattern::Filter(expr) => expr.contains_function(target),
        Pattern::Bind { expr, .. } => expr.contains_function(target),
        Pattern::Exists(inner) | Pattern::NotExists(inner) | Pattern::Minus(inner) => {
            inner.iter().any(|p| pattern_contains_function(p, target))
        }
        Pattern::Optional(inner) => inner.iter().any(|p| pattern_contains_function(p, target)),
        Pattern::Union(branches) => branches.iter().any(|branch: &Vec<Pattern>| {
            branch.iter().any(|p| pattern_contains_function(p, target))
        }),
        Pattern::Graph { patterns, .. } => patterns
            .iter()
            .any(|p| pattern_contains_function(p, target)),
        Pattern::Subquery(sq) => sq
            .patterns
            .iter()
            .any(|p| pattern_contains_function(p, target)),
        // Other pattern variants cannot contain general expressions.
        _ => false,
    }
}

// Manual PartialEq: Pattern doesn't implement PartialEq, so we can't derive.
// EXISTS subqueries are evaluated at runtime, never structurally compared.
impl PartialEq for Expression {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Expression::Var(a), Expression::Var(b)) => a == b,
            (Expression::Const(a), Expression::Const(b)) => a == b,
            (Expression::Call { func: f1, args: a1 }, Expression::Call { func: f2, args: a2 }) => {
                f1 == f2 && a1 == a2
            }
            (Expression::Exists { .. }, Expression::Exists { .. }) => false,
            _ => false,
        }
    }
}

impl Expression {
    // =========================================================================
    // Constructors for common expression types
    // =========================================================================

    /// Create a comparison expression
    pub fn compare(op: impl Into<Function>, left: Expression, right: Expression) -> Self {
        Expression::Call {
            func: op.into(),
            args: vec![left, right],
        }
    }

    /// Create an equality comparison
    pub fn eq(left: Expression, right: Expression) -> Self {
        Self::compare(Function::Eq, left, right)
    }

    /// Create a not-equal comparison
    pub fn ne(left: Expression, right: Expression) -> Self {
        Self::compare(Function::Ne, left, right)
    }

    /// Create a less-than comparison
    pub fn lt(left: Expression, right: Expression) -> Self {
        Self::compare(Function::Lt, left, right)
    }

    /// Create a less-than-or-equal comparison
    pub fn le(left: Expression, right: Expression) -> Self {
        Self::compare(Function::Le, left, right)
    }

    /// Create a greater-than comparison
    pub fn gt(left: Expression, right: Expression) -> Self {
        Self::compare(Function::Gt, left, right)
    }

    /// Create a greater-than-or-equal comparison
    pub fn ge(left: Expression, right: Expression) -> Self {
        Self::compare(Function::Ge, left, right)
    }

    /// Create an arithmetic expression
    pub fn arithmetic(op: impl Into<Function>, left: Expression, right: Expression) -> Self {
        Expression::Call {
            func: op.into(),
            args: vec![left, right],
        }
    }

    /// Create an addition expression
    #[allow(clippy::should_implement_trait)]
    pub fn add(left: Expression, right: Expression) -> Self {
        Self::arithmetic(Function::Add, left, right)
    }

    /// Create a subtraction expression
    #[allow(clippy::should_implement_trait)]
    pub fn sub(left: Expression, right: Expression) -> Self {
        Self::arithmetic(Function::Sub, left, right)
    }

    /// Create a multiplication expression
    #[allow(clippy::should_implement_trait)]
    pub fn mul(left: Expression, right: Expression) -> Self {
        Self::arithmetic(Function::Mul, left, right)
    }

    /// Create a division expression
    #[allow(clippy::should_implement_trait)]
    pub fn div(left: Expression, right: Expression) -> Self {
        Self::arithmetic(Function::Div, left, right)
    }

    /// Create a unary negation expression
    pub fn negate(expr: Expression) -> Self {
        Expression::Call {
            func: Function::Negate,
            args: vec![expr],
        }
    }

    /// Create a logical AND expression
    pub fn and(exprs: Vec<Expression>) -> Self {
        Expression::Call {
            func: Function::And,
            args: exprs,
        }
    }

    /// Create a logical OR expression
    pub fn or(exprs: Vec<Expression>) -> Self {
        Expression::Call {
            func: Function::Or,
            args: exprs,
        }
    }

    /// Create a logical NOT expression
    #[allow(clippy::should_implement_trait)]
    pub fn not(expr: Expression) -> Self {
        Expression::Call {
            func: Function::Not,
            args: vec![expr],
        }
    }

    /// Create an IF expression
    pub fn if_then_else(
        condition: Expression,
        then_expr: Expression,
        else_expr: Expression,
    ) -> Self {
        Expression::Call {
            func: Function::If,
            args: vec![condition, then_expr, else_expr],
        }
    }

    /// Create an IN expression
    pub fn in_list(expr: Expression, values: Vec<Expression>) -> Self {
        let mut args = vec![expr];
        args.extend(values);
        Expression::Call {
            func: Function::In,
            args,
        }
    }

    /// Create a NOT IN expression
    pub fn not_in_list(expr: Expression, values: Vec<Expression>) -> Self {
        let mut args = vec![expr];
        args.extend(values);
        Expression::Call {
            func: Function::NotIn,
            args,
        }
    }

    /// Create a function call expression
    pub fn call(func: Function, args: Vec<Expression>) -> Self {
        Expression::Call { func, args }
    }

    // =========================================================================
    // Query methods
    // =========================================================================

    /// Get all variables referenced by this expression
    pub fn variables(&self) -> Vec<VarId> {
        match self {
            Expression::Var(v) => vec![*v],
            Expression::Const(_) => vec![],
            Expression::Call { args, .. } => args.iter().flat_map(Expression::variables).collect(),
            Expression::Exists { patterns, .. } => {
                patterns.iter().flat_map(Pattern::variables).collect()
            }
        }
    }

    /// Returns Some(var) if filter references exactly one variable
    ///
    /// Used to determine if a filter can be attached inline to a pattern.
    pub fn single_var(&self) -> Option<VarId> {
        let vars = self.variables();
        let unique: std::collections::HashSet<_> = vars.into_iter().collect();
        if unique.len() == 1 {
            unique.into_iter().next()
        } else {
            None
        }
    }

    /// Returns true if this filter can be pushed down to index scans as range bounds.
    ///
    /// "Range-safe" filters can be converted to contiguous range constraints on the
    /// object position of index scans, enabling early filtering at the storage layer
    /// rather than post-scan filtering in the operator pipeline.
    ///
    /// # Accepted patterns
    ///
    /// - **Simple comparisons** (`<`, `<=`, `>`, `>=`, `=`) between a variable and constant
    /// - **Conjunctions** (`AND`) of range-safe expressions
    ///
    /// # Rejected patterns (NOT range-safe)
    ///
    /// - `!=` (not-equal) - cannot be represented as a contiguous range
    /// - `OR` - would require multiple disjoint ranges
    /// - `NOT` - negation cannot be efficiently bounded
    /// - Arithmetic expressions - require evaluation, not just bounds
    /// - Function calls - require runtime evaluation
    /// - `IN` clauses - multiple discrete values, not a range
    /// - Variable-to-variable comparisons - no constant bound available
    ///
    /// # Usage
    ///
    /// Filters that are range-safe are extracted during query planning and converted
    /// to `ObjectBounds` for index scans. Non-range-safe filters are applied as
    /// `FilterOperator` nodes after the scan completes.
    ///
    /// # Example
    ///
    /// ```text
    /// FILTER(?age > 18 AND ?age < 65)  -> range-safe (becomes scan bounds)
    /// FILTER(?age != 30)               -> NOT range-safe (post-scan filter)
    /// FILTER(?x > ?y)                  -> NOT range-safe (no constant bound)
    /// (< 10 ?x 20)                     -> range-safe (sandwich: const var const)
    /// (< ?x ?y 20)                     -> NOT range-safe (non-sandwich variadic)
    /// ```
    pub fn is_range_safe(&self) -> bool {
        match self {
            Expression::Call { func, args } => match func {
                // Comparison operators (except Ne) are range-safe if var vs const
                Function::Eq | Function::Lt | Function::Le | Function::Gt | Function::Ge => {
                    // 2-arg: var vs const (either order)
                    (args.len() == 2
                        && matches!(
                            (&args[0], &args[1]),
                            (Expression::Var(_), Expression::Const(_))
                                | (Expression::Const(_), Expression::Var(_))
                        ))
                    // 3-arg sandwich: const var const
                    || (args.len() == 3
                        && matches!(
                            (&args[0], &args[1], &args[2]),
                            (Expression::Const(_), Expression::Var(_), Expression::Const(_))
                        ))
                }
                // AND of range-safe expressions is range-safe
                Function::And => args.iter().all(Expression::is_range_safe),
                // Everything else is NOT range-safe
                _ => false,
            },
            // Var, Const, Exists are not range-safe on their own
            Expression::Var(_) | Expression::Const(_) | Expression::Exists { .. } => false,
        }
    }

    /// Check if this is a comparison expression
    pub fn is_comparison(&self) -> bool {
        matches!(
            self,
            Expression::Call {
                func: Function::Eq
                    | Function::Ne
                    | Function::Lt
                    | Function::Le
                    | Function::Gt
                    | Function::Ge,
                ..
            }
        )
    }

    /// Get the comparison function if this is a comparison expression
    pub fn as_comparison(&self) -> Option<(&Function, &[Expression])> {
        match self {
            Expression::Call { func, args }
                if matches!(
                    func,
                    Function::Eq
                        | Function::Ne
                        | Function::Lt
                        | Function::Le
                        | Function::Gt
                        | Function::Ge
                ) =>
            {
                Some((func, args))
            }
            _ => None,
        }
    }
}

/// Comparison operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl CompareOp {
    /// Return the operator symbol as a static string.
    pub fn symbol(self) -> &'static str {
        match self {
            CompareOp::Eq => "=",
            CompareOp::Ne => "!=",
            CompareOp::Lt => "<",
            CompareOp::Le => "<=",
            CompareOp::Gt => ">",
            CompareOp::Ge => ">=",
        }
    }
}

impl std::fmt::Display for CompareOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.symbol())
    }
}

/// Arithmetic operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithmeticOp {
    Add,
    Sub,
    Mul,
    Div,
}

impl std::fmt::Display for ArithmeticOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArithmeticOp::Add => write!(f, "+"),
            ArithmeticOp::Sub => write!(f, "-"),
            ArithmeticOp::Mul => write!(f, "*"),
            ArithmeticOp::Div => write!(f, "/"),
        }
    }
}

/// Constant value in filter expressions
#[derive(Debug, Clone, PartialEq)]
pub enum FilterValue {
    Long(i64),
    Double(f64),
    String(String),
    Bool(bool),
    /// Temporal or duration value (wraps any temporal/duration FlakeValue)
    Temporal(fluree_db_core::value::FlakeValue),
}

// =============================================================================
// From implementations for lowering unresolved AST types
// =============================================================================

impl From<CompareOp> for Function {
    fn from(op: CompareOp) -> Self {
        match op {
            CompareOp::Eq => Function::Eq,
            CompareOp::Ne => Function::Ne,
            CompareOp::Lt => Function::Lt,
            CompareOp::Le => Function::Le,
            CompareOp::Gt => Function::Gt,
            CompareOp::Ge => Function::Ge,
        }
    }
}

impl From<ArithmeticOp> for Function {
    fn from(op: ArithmeticOp) -> Self {
        match op {
            ArithmeticOp::Add => Function::Add,
            ArithmeticOp::Sub => Function::Sub,
            ArithmeticOp::Mul => Function::Mul,
            ArithmeticOp::Div => Function::Div,
        }
    }
}

impl From<&crate::parse::ast::UnresolvedFilterValue> for FilterValue {
    fn from(val: &crate::parse::ast::UnresolvedFilterValue) -> Self {
        use crate::parse::ast::UnresolvedFilterValue;
        match val {
            UnresolvedFilterValue::Long(l) => FilterValue::Long(*l),
            UnresolvedFilterValue::Double(d) => FilterValue::Double(*d),
            UnresolvedFilterValue::String(s) => FilterValue::String(s.to_string()),
            UnresolvedFilterValue::Bool(b) => FilterValue::Bool(*b),
        }
    }
}

/// Built-in functions
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Function {
    // =========================================================================
    // Comparison operators
    // =========================================================================
    /// Equality (=)
    Eq,
    /// Not equal (!=)
    Ne,
    /// Less than (<)
    Lt,
    /// Less than or equal (<=)
    Le,
    /// Greater than (>)
    Gt,
    /// Greater than or equal (>=)
    Ge,

    // =========================================================================
    // Arithmetic operators
    // =========================================================================
    /// Addition (+)
    Add,
    /// Subtraction (-)
    Sub,
    /// Multiplication (*)
    Mul,
    /// Division (/)
    Div,
    /// Unary negation (-)
    Negate,

    // =========================================================================
    // Logical operators
    // =========================================================================
    /// Logical AND
    And,
    /// Logical OR
    Or,
    /// Logical NOT
    Not,
    /// IN expression (?x IN (1, 2, 3))
    In,
    /// NOT IN expression (?x NOT IN (1, 2, 3))
    NotIn,

    // =========================================================================
    // String functions
    // =========================================================================
    Strlen,
    Substr,
    Ucase,
    Lcase,
    Contains,
    StrStarts,
    StrEnds,
    Regex,
    Concat,
    StrBefore,
    StrAfter,
    Replace,
    Str,
    StrDt,
    StrLang,
    EncodeForUri,

    // =========================================================================
    // Numeric functions
    // =========================================================================
    Abs,
    Round,
    Ceil,
    Floor,
    Rand,

    // =========================================================================
    // RDF term constructors
    // =========================================================================
    Iri,
    Bnode,

    // =========================================================================
    // DateTime functions
    // =========================================================================
    Now,
    Year,
    Month,
    Day,
    Hours,
    Minutes,
    Seconds,
    Tz,
    Timezone,

    // =========================================================================
    // Type functions
    // =========================================================================
    IsIri,
    IsBlank,
    IsLiteral,
    IsNumeric,

    // =========================================================================
    // RDF term functions
    // =========================================================================
    Lang,
    Datatype,
    LangMatches,
    SameTerm,

    // =========================================================================
    // Fluree-specific functions
    // =========================================================================
    /// Transaction time of the matching flake (i64).
    T,
    /// Operation type of the matching flake in history queries — boolean
    /// (`true` = assert, `false` = retract). Mirrors `Flake.op` on disk;
    /// returns `None` for current-state scans.
    Op,

    // =========================================================================
    // Hash functions
    // =========================================================================
    Md5,
    Sha1,
    Sha256,
    Sha384,
    Sha512,

    // =========================================================================
    // UUID functions
    // =========================================================================
    Uuid,
    StrUuid,

    // =========================================================================
    // Vector/embedding similarity functions
    // =========================================================================
    DotProduct,
    CosineSimilarity,
    EuclideanDistance,

    // =========================================================================
    // Geospatial functions
    // =========================================================================
    GeofDistance,

    // =========================================================================
    // Fulltext scoring functions
    // =========================================================================
    Fulltext,

    // =========================================================================
    // Conditional functions
    // =========================================================================
    Bound,
    If,
    Coalesce,

    // =========================================================================
    // XSD datatype constructor (cast) functions — W3C SPARQL 1.1 §17.5
    // SPARQL-only: JSON-LD queries do not produce these (casts are a SPARQL concept).
    // =========================================================================
    XsdBoolean,
    XsdInteger,
    XsdFloat,
    XsdDouble,
    XsdDecimal,
    XsdString,

    // =========================================================================
    // Custom/unknown function
    // =========================================================================
    Custom(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::triple::{Ref, Term};
    use fluree_db_core::Sid;

    fn test_pattern() -> TriplePattern {
        TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(Sid::new(100, "name")),
            Term::Var(VarId(1)),
        )
    }

    #[test]
    fn test_query_new() {
        let pattern = test_pattern();
        let query = Query::new(vec![VarId(0), VarId(1)], vec![Pattern::Triple(pattern)]);

        assert_eq!(query.select.len(), 2);
        assert_eq!(query.where_.len(), 1);
    }

    #[test]
    fn test_query_single() {
        let pattern = test_pattern();
        let query = Query::single(vec![VarId(0)], pattern);

        assert_eq!(query.where_.len(), 1);
        assert!(query.where_[0].is_triple());
    }

    #[test]
    fn test_query_triple_patterns() {
        let p1 = TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(Sid::new(100, "name")),
            Term::Var(VarId(1)),
        );
        let p2 = TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(Sid::new(101, "age")),
            Term::Var(VarId(2)),
        );

        let query = Query::new(
            vec![VarId(0), VarId(1), VarId(2)],
            vec![Pattern::Triple(p1), Pattern::Triple(p2)],
        );

        let triples = query.triple_patterns();
        assert_eq!(triples.len(), 2);
    }

    #[test]
    fn test_pattern_variables() {
        let pattern = Pattern::Triple(test_pattern());
        let vars = pattern.variables();
        assert_eq!(vars.len(), 2);
        assert!(vars.contains(&VarId(0)));
        assert!(vars.contains(&VarId(1)));
    }

    #[test]
    fn test_filter_expr_single_var() {
        // Single var: ?x > 10
        let expr = Expression::gt(
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(10)),
        );
        assert_eq!(expr.single_var(), Some(VarId(0)));

        // Two vars: ?x > ?y
        let expr2 = Expression::gt(Expression::Var(VarId(0)), Expression::Var(VarId(1)));
        assert_eq!(expr2.single_var(), None);
    }

    #[test]
    fn test_filter_expr_is_range_safe() {
        // Range-safe: ?x > 10
        let expr = Expression::gt(
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(10)),
        );
        assert!(expr.is_range_safe());

        // Range-safe: AND of range-safe
        let and_expr = Expression::and(vec![
            Expression::ge(
                Expression::Var(VarId(0)),
                Expression::Const(FilterValue::Long(18)),
            ),
            Expression::lt(
                Expression::Var(VarId(0)),
                Expression::Const(FilterValue::Long(65)),
            ),
        ]);
        assert!(and_expr.is_range_safe());

        // Not range-safe: OR
        let or_expr = Expression::or(vec![Expression::eq(
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(1)),
        )]);
        assert!(!or_expr.is_range_safe());
    }
}
