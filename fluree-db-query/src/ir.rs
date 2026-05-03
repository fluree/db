//! Intermediate Representation for query execution
//!
//! This module provides the logical pattern IR that represents query structure.
//! The planner transforms this IR into physical operators.
//!
//! # Design
//!
//! - `Query` is the top-level structure: output spec, WHERE patterns, options,
//!   optional expansion spec, optional post-VALUES
//! - `Pattern` enum mirrors the where clause structure, preserving order for filter inlining
//! - The planner chooses physical join operators based on pattern analysis
//!
//! # Module layout
//!
//! - [`query`] — top-level `Query`, `QueryOutput`, `Multiplicity`,
//!   `ConstructTemplate`: the resolved-and-lowered query that flows through
//!   parse → plan → execute → format
//! - [`options`] — `QueryOptions` (LIMIT/OFFSET/ORDER BY/GROUP BY/aggregates/
//!   HAVING/post-binds/DISTINCT) plus `ReasoningModes` config for the rewriter
//! - [`triple`] — `TriplePattern`, `Ref`, `Term`: the s/p/o vocabulary used
//!   by triple patterns (and reused by other pattern variants for s/p
//!   positions)
//! - [`projection`] — projection / hydration specs (`Projection`, `Column`,
//!   `HydrationSpec`, `NestedSelectSpec`, `ForwardItem`, `Root`)
//! - [`path`] — property-path patterns (transitive predicate traversal)
//! - [`adapters`] — scan patterns over non-graph data sources (BM25, vector,
//!   geo, S2, R2RML) adapted to plug into the pattern tree
//! - [`pattern`] — `Pattern` enum and the variants that recursively wrap
//!   `Vec<Pattern>` (Subquery, Service, Graph)
//! - [`expression`] — filter / bind expression AST, comparison and arithmetic
//!   operators, and the built-in function catalog

pub mod adapters;
pub mod expression;
pub mod options;
pub mod path;
pub mod pattern;
pub mod projection;
pub mod query;
pub mod triple;

pub use adapters::{
    GeoSearchCenter, GeoSearchPattern, IndexSearchPattern, IndexSearchTarget, R2rmlPattern,
    S2QueryGeom, S2SearchPattern, S2SpatialOp, VectorSearchPattern, VectorSearchTarget,
};
pub use expression::{ArithmeticOp, CompareOp, Expression, FilterValue, Function};
pub use options::{QueryOptions, ReasoningModes};
pub use path::{PathModifier, PropertyPathPattern};
pub use pattern::{GraphName, Pattern, ServiceEndpoint, ServicePattern, SubqueryPattern};
pub use projection::{Column, ForwardItem, HydrationSpec, NestedSelectSpec, Projection, Root};
pub use query::{ConstructTemplate, Multiplicity, Query, QueryOutput};
pub use triple::{Ref, Term, TriplePattern};
