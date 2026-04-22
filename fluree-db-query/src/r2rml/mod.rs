//! R2RML Graph Source Support
//!
//! This module provides query integration for R2RML-mapped Iceberg tables.
//! It exposes tabular data as RDF triples through the query engine.
//!
//! # Architecture
//!
//! - `R2rmlProvider`: Trait for loading compiled R2RML mappings
//! - `R2rmlPattern`: IR pattern for R2RML queries
//! - `R2rmlScanOperator`: Operator that executes R2RML scans
//! - `rewrite_patterns_for_r2rml`: Rewrites triple patterns to R2RML patterns
//!
//! # Usage
//!
//! R2RML patterns are typically generated during query planning when the
//! planner detects that a triple pattern can be satisfied by an R2RML
//! graph source. The operator loads the mapping, scans the underlying
//! Iceberg table, and materializes RDF terms according to the mapping.
//!
//! When a GRAPH pattern targets an R2RML graph source, the `GraphOperator`
//! uses `rewrite_patterns_for_r2rml` to convert contained triple patterns
//! to R2RML patterns before building the operator tree.

mod operator;
mod provider;
mod rewrite;

pub use operator::R2rmlScanOperator;
pub use provider::{NoOpR2rmlProvider, R2rmlProvider, R2rmlTableProvider};
pub use rewrite::{convert_triple_to_r2rml, rewrite_patterns_for_r2rml, R2rmlRewriteResult};
