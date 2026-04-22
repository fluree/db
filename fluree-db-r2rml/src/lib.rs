//! R2RML mapping support for Fluree DB graph sources
//!
//! This crate provides R2RML (RDB to RDF Mapping Language) support for
//! transforming Iceberg tabular data into RDF triples. It implements a
//! subset of the W3C R2RML specification tailored for Iceberg graph sources.
//!
//! # Key Features
//!
//! - **Format-agnostic parsing**: Load R2RML mappings from Turtle or JSON-LD
//!   via the shared Graph IR
//! - **Compiled mappings**: Parse once, execute many times with efficient
//!   index structures for pattern matching
//! - **Term materialization**: Generate RDF terms from tabular column values
//!   using templates, column references, and constants
//! - **RefObjectMap joins**: Support for multi-table relationships
//!
//! # Supported R2RML Features
//!
//! - `rr:TriplesMap` with `rr:logicalTable` (table name only, not sqlQuery)
//! - `rr:subjectMap` with `rr:template`, `rr:class`, `rr:termType`
//! - `rr:predicateObjectMap` with `rr:predicate` and `rr:objectMap`
//! - `rr:objectMap` with `rr:column`, `rr:constant`, `rr:template`
//! - `rr:datatype` and `rr:language` for typed/language-tagged literals
//! - `rr:parentTriplesMap` with `rr:joinCondition` for RefObjectMaps
//!
//! # Usage
//!
//! Load R2RML mappings from Turtle using [`R2rmlLoader::from_turtle()`], then call
//! `compile()` to produce a [`CompiledR2rmlMapping`]. The compiled mapping provides
//! indexed lookup methods like `find_maps_for_class()` for efficient pattern matching
//! during query execution.

pub mod error;
pub mod loader;
pub mod mapping;
pub mod materialize;
pub mod vocab;

pub use error::{R2rmlError, R2rmlResult};
pub use loader::R2rmlLoader;
pub use mapping::{
    CompiledR2rmlMapping, JoinCondition, LogicalTable, ObjectMap, PredicateMap, PredicateObjectMap,
    RefObjectMap, SubjectMap, TermType, TriplesMap,
};
pub use materialize::{expand_template, RdfTerm};
pub use vocab::R2RML;
