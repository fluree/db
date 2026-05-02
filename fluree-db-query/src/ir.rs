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
//!
//! # Module layout
//!
//! - [`projection`] — top-level `Query` and graph-crawl selection specs
//! - [`path`] — property-path patterns (transitive predicate traversal)
//! - [`adapters`] — scan patterns over non-graph data sources (BM25, vector,
//!   geo, S2, R2RML) adapted to plug into the pattern tree
//! - [`pattern`] — `Pattern` enum and the variants that recursively wrap
//!   `Vec<Pattern>` (Subquery, Service, Graph)
//! - [`expression`] — filter / bind expression AST, comparison and arithmetic
//!   operators, and the built-in function catalog

pub mod adapters;
pub mod expression;
pub mod path;
pub mod pattern;
pub mod projection;

pub use adapters::{
    GeoSearchCenter, GeoSearchPattern, IndexSearchPattern, IndexSearchTarget, R2rmlPattern,
    S2QueryGeom, S2SearchPattern, S2SpatialOp, VectorSearchPattern, VectorSearchTarget,
};
pub use expression::{ArithmeticOp, CompareOp, Expression, FilterValue, Function};
pub use path::{PathModifier, PropertyPathPattern};
pub use pattern::{
    pattern_contains_function, GraphName, Pattern, ServiceEndpoint, ServicePattern,
    SubqueryPattern,
};
pub use projection::{GraphSelectSpec, NestedSelectSpec, Query, Root, SelectionSpec};
