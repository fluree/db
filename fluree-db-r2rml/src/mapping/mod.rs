//! R2RML mapping structures
//!
//! This module provides the compiled representation of R2RML mappings.
//! These structures are produced by the [`crate::loader`] module and used
//! by the term materialization and query integration layers.

mod compiled;
mod ref_object_map;
mod sql_subset;
mod term_map;
mod triples_map;
mod uniqueness;

pub use compiled::CompiledR2rmlMapping;
pub use ref_object_map::{JoinCondition, RefObjectMap};
pub use sql_subset::{RestrictedSelect, RowCmpOp, RowCondition, SqlLiteral, UnsupportedSql};
pub use term_map::{ConstantValue, ObjectMap, PredicateMap, PredicateObjectMap, TermType};
pub use triples_map::{extract_template_columns, GraphMap, LogicalTable, SubjectMap, TriplesMap};
pub use uniqueness::{
    constant_prefix, templates_provably_disjoint, usable_in_key, KeyColumn, TableKeys,
};
