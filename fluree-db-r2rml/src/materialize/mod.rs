//! Term materialization module
//!
//! This module provides functions for generating RDF terms from tabular
//! column values according to R2RML term map specifications.
//!
//! Two APIs are provided:
//!
//! - **HashMap API**: `materialize_subject`, `materialize_object`, `expand_template`
//!   For testing and simple use cases.
//!
//! - **ColumnBatch API**: `materialize_subject_from_batch`, `materialize_object_from_batch`,
//!   `expand_template_from_batch`, `get_join_key_from_batch`
//!   For efficient production use with Iceberg tabular data.

mod graph;
mod term;

// Whole-graph triple enumeration (bulk materialization / twin builder).
pub use graph::{
    dependency_order, emit_batch, enumerate_by_waves, enumerate_from_batches,
    parent_key_insert_keep_min, plan, render_term, MaterializationPlan, MaterializeStats,
    NTriplesCollector, ParentIndexSet, TripleObserver,
};

// HashMap-based API (for testing and simple use)
pub use term::{expand_template, materialize_object, materialize_subject, RdfTerm};

// Subject-template reversal (constant IRI → raw column values), for pushing a
// bound-subject predicate down to the Iceberg scan.
pub use term::reverse_subject_template;

// ColumnBatch-based API (for production efficiency)
pub use term::{
    expand_template_from_batch, get_join_key_from_batch, materialize_object_from_batch,
    materialize_predicate_from_batch, materialize_subject_from_batch,
};
