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

mod term;

// HashMap-based API (for testing and simple use)
pub use term::{expand_template, materialize_object, materialize_subject, RdfTerm};

// ColumnBatch-based API (for production efficiency)
pub use term::{
    expand_template_from_batch, get_join_key_from_batch, materialize_object_from_batch,
    materialize_subject_from_batch,
};
