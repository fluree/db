//! Adapter patterns: scans against data sources whose native form isn't an
//! RDF graph, exposed to the query pipeline as if they were triple matches.
//!
//! Each pattern in this module wraps a non-graph source — a BM25 full-text
//! index, a vector index, a geo binary index, an S2 spatial sidecar, an
//! Iceberg table reachable via R2RML mappings — and gives it a plug shape
//! that fits where a `Pattern::Triple` would otherwise sit.

use crate::ir::Expression;
use crate::var_registry::VarId;
use fluree_db_core::Sid;

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

    /// Variables this pattern adds to the row's binding set: the document
    /// IRI, plus optional score and ledger bindings.
    pub fn produced_vars(&self) -> Vec<VarId> {
        let mut vars = vec![self.id_var];
        if let Some(v) = self.score_var {
            vars.push(v);
        }
        if let Some(v) = self.ledger_var {
            vars.push(v);
        }
        vars
    }

    /// Variables mentioned anywhere in this pattern: produced bindings plus
    /// the search target when it's a variable rather than a constant.
    pub fn referenced_vars(&self) -> Vec<VarId> {
        let mut vars = self.produced_vars();
        if let IndexSearchTarget::Var(v) = &self.target {
            vars.push(*v);
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

    /// Variables this pattern adds to the row's binding set: the document
    /// IRI, plus optional score and ledger bindings.
    pub fn produced_vars(&self) -> Vec<VarId> {
        let mut vars = vec![self.id_var];
        if let Some(v) = self.score_var {
            vars.push(v);
        }
        if let Some(v) = self.ledger_var {
            vars.push(v);
        }
        vars
    }

    /// Variables mentioned anywhere in this pattern: produced bindings plus
    /// the query vector when it's a variable rather than a constant.
    pub fn referenced_vars(&self) -> Vec<VarId> {
        let mut vars = self.produced_vars();
        if let VectorSearchTarget::Var(v) = &self.target {
            vars.push(*v);
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

    /// Variables this pattern adds to the row's binding set: the matching
    /// subject IRI, plus the optional distance binding.
    pub fn produced_vars(&self) -> Vec<VarId> {
        let mut vars = vec![self.subject_var];
        if let Some(v) = self.distance_var {
            vars.push(v);
        }
        vars
    }

    /// Variables mentioned anywhere in this pattern: produced bindings plus
    /// the center point when it's a variable rather than a constant.
    pub fn referenced_vars(&self) -> Vec<VarId> {
        let mut vars = self.produced_vars();
        if let GeoSearchCenter::Var(v) = &self.center {
            vars.push(*v);
        }
        vars
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

    /// Variables this pattern adds to the row's binding set: the matching
    /// subject IRI, plus the optional distance binding.
    pub fn produced_vars(&self) -> Vec<VarId> {
        let mut vars = vec![self.subject_var];
        if let Some(v) = self.distance_var {
            vars.push(v);
        }
        vars
    }

    /// Variables mentioned anywhere in this pattern: produced bindings plus
    /// the query geometry when it's a variable rather than a constant.
    pub fn referenced_vars(&self) -> Vec<VarId> {
        let mut vars = self.produced_vars();
        if let S2QueryGeom::Var(v) = &self.query_geom {
            vars.push(*v);
        }
        vars
    }
}

// ============================================================================
// R2RML Pattern
// ============================================================================

/// A FILTER comparison on an R2RML object variable, ready to push to the scan.
/// `var` is resolved to a table column by the operator (var → predicate → column).
#[derive(Debug, Clone)]
pub struct ScanPushdown {
    pub var: VarId,
    pub op: crate::r2rml::ScanCmpOp,
    pub value: crate::r2rml::ScanValue,
}

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

    /// Variable to bind the subject IRI.
    ///
    /// `None` when the triple has a constant (bound) subject — see
    /// `subject_constant`. Exactly one of `subject_var` / `subject_constant`
    /// is set.
    pub subject_var: Option<VarId>,

    /// A constant (bound) subject in the triple pattern (`<store/5> <pred> ?o`):
    /// the subject is a fixed IRI, not a variable. The operator materializes each
    /// row's subject and keeps only rows whose subject equals this IRI, binding
    /// no subject variable. Enforced as the pattern's semantics (independent of
    /// scan pushdown).
    pub subject_constant: Option<String>,

    /// Variable to bind the object value (optional)
    ///
    /// If None, this pattern only materializes subjects (e.g., for rdf:type patterns).
    pub object_var: Option<VarId>,

    /// Variable to bind the predicate IRI (optional).
    ///
    /// Set for a variable-predicate pattern (`?s ?p ?o` or `<iri> ?p ?o`). The
    /// operator binds this variable to each materialized triple's predicate IRI
    /// (the `rr:predicate` of the POM the object came from), so a wildcard scan
    /// yields the predicate as well as subject/object rather than leaving `?p`
    /// unbound. `None` for a constant-predicate or subject-only pattern.
    pub predicate_var: Option<VarId>,

    /// Variable to bind the subject's class IRI(s) for a variable `rdf:type`
    /// pattern (`?s rdf:type ?type`, i.e. FQL `@type: ?type` / SPARQL `?s a ?t`).
    ///
    /// When set, the operator emits one row per class the row's TriplesMap
    /// declares (`rr:class`), binding this variable to that class IRI — the same
    /// scan a bound `class_filter` performs, but projecting the class instead of
    /// filtering on it. A row whose map declares no class produces no binding
    /// (the subject has no `rdf:type` triple). `object_var` stays `None`; the
    /// class is drawn from the mapping, not a table column.
    pub type_var: Option<VarId>,

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

    /// PR-3 fix (b'): a co-located `rdf:type` class that was NOT fused into this
    /// star for materialization (`class_fusion_is_safe` refused — the class lives
    /// in a different TriplesMap than the base predicate), but whose class-declaring
    /// TriplesMaps are subject-template-DISJOINT from every other map that resolves
    /// here (`wildcard_class_fusion_is_safe`). Set only in that provably-safe case,
    /// it lets TriplesMap resolution prune the star's fan-out to class-declaring
    /// maps WITHOUT changing materialization: the class is still enforced by its own
    /// standalone scan joined on the subject, and disjointness guarantees the pruned
    /// maps' subjects could never survive that join anyway. Unlike `class_filter`,
    /// this NEVER affects rdf:type emission — resolution pruning only.
    pub class_prune_hint: Option<String>,

    /// Pushed-down scan filters for Iceberg file pruning, resolved at execution
    /// from FILTER comparisons on this pattern's object variables. Conservative:
    /// the in-engine FILTER still runs, so these only skip data files.
    pub scan_filters: Vec<ScanPushdown>,

    /// Same-subject star: additional `(predicate IRI, object var)` bindings to
    /// materialize in the SAME table scan, avoiding a self-join on the subject.
    ///
    /// When non-empty, this pattern represents a grouped star of triple patterns
    /// that all share `subject_var`. The base binding is carried by
    /// `predicate_filter` + `object_var` (the first member); these are the
    /// additional members. The operator emits one row per table row (cross
    /// product over multi-valued predicates) binding the subject and every
    /// object var, instead of producing one pattern per triple and joining them.
    pub star_bindings: Vec<(String, VarId)>,

    /// Same-subject star: additional `(predicate IRI, required constant)` equality
    /// constraints checked in the SAME table scan, fused from constant-object
    /// triples (`?s <pred> <const>`) that share `subject_var` with a star base.
    /// A row survives only when, for every entry, the predicate produces at least
    /// one object equal to the constant — an existence filter that produces no
    /// variable, avoiding a separate scan + self-join.
    pub star_constraints: Vec<(String, crate::r2rml::ObjectConstant)>,

    /// A scan-local FILTER fully consumed into this scan by the planner: every
    /// variable it references is produced by this pattern alone. The operator
    /// applies it to its output rows (same evaluator as the in-engine FILTER, so
    /// results are unchanged), which lets the downstream LIMIT row budget reach
    /// the scan — the standalone `FilterOperator` that would otherwise block the
    /// budget is dropped. `None` when no filter was consumed.
    pub consumed_filter: Option<Expression>,

    /// A constant object in the triple pattern (`?s <pred> <const>`): the object
    /// is not a variable but a required literal or IRI. `object_var` is `None`;
    /// the operator keeps a subject only when this predicate's object equals the
    /// constant (enforced as the pattern's semantics, independent of scan
    /// pushdown). A scalar literal also emits a `ScanFilter` for row-group + row
    /// pruning; IRI constants are operator-enforced only.
    pub object_constant: Option<crate::r2rml::ObjectConstant>,
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
            subject_var: Some(subject_var),
            subject_constant: None,
            object_var,
            predicate_var: None,
            type_var: None,
            triples_map_iri: None,
            predicate_filter: None,
            class_filter: None,
            class_prune_hint: None,
            star_bindings: Vec::new(),
            star_constraints: Vec::new(),
            scan_filters: Vec::new(),
            consumed_filter: None,
            object_constant: None,
        }
    }

    /// Create a new R2RML pattern with a constant (bound) subject IRI.
    ///
    /// The subject is not a variable; the operator keeps only table rows whose
    /// materialized subject equals `subject_constant` and binds no subject var.
    pub fn new_bound_subject(
        graph_source_id: impl Into<String>,
        subject_constant: impl Into<String>,
        object_var: Option<VarId>,
    ) -> Self {
        Self {
            graph_source_id: graph_source_id.into(),
            subject_var: None,
            subject_constant: Some(subject_constant.into()),
            object_var,
            predicate_var: None,
            type_var: None,
            triples_map_iri: None,
            predicate_filter: None,
            class_filter: None,
            class_prune_hint: None,
            star_bindings: Vec::new(),
            star_constraints: Vec::new(),
            scan_filters: Vec::new(),
            consumed_filter: None,
            object_constant: None,
        }
    }

    /// Set the predicate filter.
    pub fn with_predicate(mut self, predicate: impl Into<String>) -> Self {
        self.predicate_filter = Some(predicate.into());
        self
    }

    /// Add same-subject star bindings (additional predicate→var pairs).
    pub fn with_star_bindings(mut self, bindings: Vec<(String, VarId)>) -> Self {
        self.star_bindings = bindings;
        self
    }

    /// Set the predicate variable (`?s ?p ?o` / `<iri> ?p ?o`), bound by the
    /// operator to each materialized triple's predicate IRI.
    pub fn with_predicate_var(mut self, var: VarId) -> Self {
        self.predicate_var = Some(var);
        self
    }

    /// Set the type variable (`?s rdf:type ?type`), bound by the operator to the
    /// subject's declared class IRI(s).
    pub fn with_type_var(mut self, var: VarId) -> Self {
        self.type_var = Some(var);
        self
    }

    /// Set the class filter.
    pub fn with_class(mut self, class: impl Into<String>) -> Self {
        self.class_filter = Some(class.into());
        self
    }

    /// Variables this pattern produces. R2RML patterns have no input
    /// variables (only the static graph_source_id and metadata filters), so
    /// referenced and produced are the same set.
    pub fn produced_vars(&self) -> Vec<VarId> {
        let mut vars = Vec::new();
        if let Some(sv) = self.subject_var {
            vars.push(sv);
        }
        if let Some(obj_var) = self.object_var {
            vars.push(obj_var);
        }
        if let Some(pv) = self.predicate_var {
            vars.push(pv);
        }
        if let Some(tv) = self.type_var {
            vars.push(tv);
        }
        for (_, var) in &self.star_bindings {
            vars.push(*var);
        }
        vars
    }

    /// Variables mentioned anywhere in this pattern.
    ///
    /// EXHAUSTIVELY destructures `R2rmlPattern` (no `..`) so a newly added
    /// var-bearing field breaks compilation here until it is classified — this
    /// set feeds correlation analysis (the batched-OPTIONAL hash-join partition,
    /// `optional.rs`) and planner var-dependency, where a silently-omitted var
    /// would mis-partition (wrong answers). Unlike `produced_vars`, this also
    /// includes the FILTER operands (`scan_filters`, `consumed_filter`) so it is
    /// correct by construction rather than by their "operands ⊆ produced-vars"
    /// invariants.
    pub fn referenced_vars(&self) -> Vec<VarId> {
        let R2rmlPattern {
            graph_source_id: _,
            subject_var,
            subject_constant: _,
            object_var,
            predicate_var,
            type_var,
            triples_map_iri: _,
            predicate_filter: _,
            class_filter: _,
            class_prune_hint: _,
            scan_filters,
            star_bindings,
            star_constraints: _, // constant-object existence filters — no variable
            consumed_filter,
            object_constant: _,
        } = self;
        let mut vars = Vec::new();
        for v in [subject_var, object_var, predicate_var, type_var]
            .into_iter()
            .flatten()
        {
            vars.push(*v);
        }
        for (_, v) in star_bindings {
            vars.push(*v);
        }
        for pd in scan_filters {
            vars.push(pd.var);
        }
        if let Some(expr) = consumed_filter {
            vars.extend(expr.referenced_vars());
        }
        vars
    }
}

#[cfg(test)]
mod r2rml_pattern_var_tests {
    use super::*;
    use crate::ir::expression::Expression;
    use crate::r2rml::{ScanCmpOp, ScanValue};

    // PR-4b P1 precursor: `referenced_vars` must surface EVERY var-bearing field,
    // or correlation analysis (the batched-OPTIONAL hash-join partition) could
    // drop a shared var and mis-partition. The exhaustive destructure in
    // `referenced_vars` makes a newly added field a compile error until it is
    // classified; this test asserts the CURRENT var-bearing fields are all wired,
    // including the FILTER operands (`scan_filters`, `consumed_filter`) that the
    // old `produced_vars`-delegating impl relied on invariants to cover.
    #[test]
    fn referenced_vars_surfaces_every_var_bearing_field() {
        let mut p = R2rmlPattern::new("gs:main", VarId(1), Some(VarId(2)));
        p.predicate_var = Some(VarId(3));
        p.type_var = Some(VarId(4));
        p.star_bindings = vec![("http://ex/p".to_string(), VarId(5))];
        p.scan_filters = vec![ScanPushdown {
            var: VarId(6),
            op: ScanCmpOp::Eq,
            value: ScanValue::Int(0),
        }];
        p.consumed_filter = Some(Expression::Var(VarId(7)));

        let refs: std::collections::HashSet<VarId> = p.referenced_vars().into_iter().collect();
        for v in [
            VarId(1),
            VarId(2),
            VarId(3),
            VarId(4),
            VarId(5),
            VarId(6),
            VarId(7),
        ] {
            assert!(
                refs.contains(&v),
                "referenced_vars() omitted {v:?} — a var-bearing field is unwired"
            );
        }
    }
}
