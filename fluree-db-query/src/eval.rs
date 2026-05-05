//! Function evaluation module
//!
//! This module provides unified evaluation of SPARQL expressions and functions.
//! It contains all function implementations organized by category, as well as
//! the core expression evaluation logic.
//!
//! # Module Structure
//!
//! - `value`: ComparableValue type and conversions
//! - `compare`: Value comparison logic
//! - `helpers`: Shared utilities (regex caching, arity checks, etc.)
//! - `eval`: Core expression evaluation
//! - `dispatch`: Main function dispatcher
//! - Category submodules: `string`, `numeric`, `datetime`, `hash`, `uuid`,
//!   `vector`, `geo`, `types`, `rdf`, `conditional`, `fluree`, `arithmetic`, `logical`

mod arithmetic;
mod cast;
mod compare;
mod conditional;
mod datetime;
mod dispatch;
mod eval;
mod fluree;
mod fulltext;
mod geo;
mod hash;
mod helpers;
mod logical;
mod numeric;
mod rdf;
mod string;
mod types;
mod uuid;
mod value;
mod vector;
pub mod vector_math;

// Re-export public API
pub use eval::passes_filters;
pub use helpers::PreparedBoolExpression;
pub use value::{ArithmeticError, ComparableValue, ComparisonError, NullValueError};
