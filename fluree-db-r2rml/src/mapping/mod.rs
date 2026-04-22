//! R2RML mapping structures
//!
//! This module provides the compiled representation of R2RML mappings.
//! These structures are produced by the [`crate::loader`] module and used
//! by the term materialization and query integration layers.

mod compiled;
mod ref_object_map;
mod term_map;
mod triples_map;

pub use compiled::CompiledR2rmlMapping;
pub use ref_object_map::{JoinCondition, RefObjectMap};
pub use term_map::{ConstantValue, ObjectMap, PredicateMap, PredicateObjectMap, TermType};
pub use triples_map::{extract_template_columns, LogicalTable, SubjectMap, TriplesMap};
