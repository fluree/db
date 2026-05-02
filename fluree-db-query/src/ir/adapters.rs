//! Adapter patterns: scans against data sources whose native form isn't an
//! RDF graph, exposed to the query pipeline as if they were triple matches.
//!
//! Each pattern in this module wraps a non-graph source — a BM25 full-text
//! index, a vector index, a geo binary index, an S2 spatial sidecar, an
//! Iceberg table reachable via R2RML mappings — and gives it a plug shape
//! that fits where a `Pattern::Triple` would otherwise sit.

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

    /// Variables this pattern reads from outer bindings (the search target,
    /// when supplied as a variable rather than a constant).
    pub fn input_vars(&self) -> Vec<VarId> {
        match &self.target {
            IndexSearchTarget::Var(v) => vec![*v],
            IndexSearchTarget::Const(_) => Vec::new(),
        }
    }

    /// Variables mentioned anywhere in this pattern (input + produced).
    pub fn referenced_vars(&self) -> Vec<VarId> {
        let mut vars = self.produced_vars();
        vars.extend(self.input_vars());
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

    /// Variables this pattern reads from outer bindings (the query vector,
    /// when supplied as a variable rather than a constant).
    pub fn input_vars(&self) -> Vec<VarId> {
        match &self.target {
            VectorSearchTarget::Var(v) => vec![*v],
            VectorSearchTarget::Const(_) => Vec::new(),
        }
    }

    /// Variables mentioned anywhere in this pattern (input + produced).
    pub fn referenced_vars(&self) -> Vec<VarId> {
        let mut vars = self.produced_vars();
        vars.extend(self.input_vars());
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

    /// Variables this pattern reads from outer bindings (the center point,
    /// when supplied as a variable rather than a constant).
    pub fn input_vars(&self) -> Vec<VarId> {
        match &self.center {
            GeoSearchCenter::Var(v) => vec![*v],
            GeoSearchCenter::Const { .. } => Vec::new(),
        }
    }

    /// Variables mentioned anywhere in this pattern (input + produced).
    pub fn referenced_vars(&self) -> Vec<VarId> {
        let mut vars = self.produced_vars();
        vars.extend(self.input_vars());
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

    /// Variables this pattern adds to the row's binding set: the matching
    /// subject IRI, plus the optional distance binding.
    pub fn produced_vars(&self) -> Vec<VarId> {
        let mut vars = vec![self.subject_var];
        if let Some(v) = self.distance_var {
            vars.push(v);
        }
        vars
    }

    /// Variables this pattern reads from outer bindings (the query geometry,
    /// when supplied as a variable rather than a constant).
    pub fn input_vars(&self) -> Vec<VarId> {
        match &self.query_geom {
            S2QueryGeom::Var(v) => vec![*v],
            S2QueryGeom::Wkt(_) | S2QueryGeom::Point { .. } => Vec::new(),
        }
    }

    /// Variables mentioned anywhere in this pattern (input + produced).
    pub fn referenced_vars(&self) -> Vec<VarId> {
        let mut vars = self.produced_vars();
        vars.extend(self.input_vars());
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

    /// Variables this pattern produces. R2RML patterns have no input
    /// variables (only the static graph_source_id and metadata filters), so
    /// referenced and produced are the same set.
    pub fn produced_vars(&self) -> Vec<VarId> {
        let mut vars = vec![self.subject_var];
        if let Some(obj_var) = self.object_var {
            vars.push(obj_var);
        }
        vars
    }

    /// Variables mentioned anywhere in this pattern.
    pub fn referenced_vars(&self) -> Vec<VarId> {
        self.produced_vars()
    }

}
