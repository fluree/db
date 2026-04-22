//! Format-agnostic RDF graph intermediate representation
//!
//! This crate provides canonical types for representing RDF graphs that can be
//! produced by parsers and consumed by formatters, regardless of the serialization
//! format (JSON-LD, Turtle, N-Quads, etc.).
//!
//! # Key Design Principles
//!
//! 1. **Expanded IRIs only** - All IRIs are stored in expanded form. Compaction
//!    is handled by formatters at output time.
//!
//! 2. **Explicit datatypes** - Literals always have an explicit datatype, never
//!    optional. Plain strings use `xsd:string`, language-tagged strings use
//!    `rdf:langString`.
//!
//! 3. **Bag semantics by default** - The `Graph` type uses `Vec<Triple>` to
//!    preserve duplicates. Call `dedupe()` explicitly for set semantics.
//!
//! 4. **Deterministic output** - Call `sort()` before formatting for
//!    deterministic triple ordering (SPO lexicographic).
//!
//! # Example
//!
//! ```
//! use fluree_graph_ir::{Graph, Term, Triple, Datatype};
//!
//! let mut graph = Graph::new();
//!
//! // Add a triple with expanded IRIs
//! graph.add_triple(
//!     Term::iri("http://example.org/alice"),
//!     Term::iri("http://xmlns.com/foaf/0.1/name"),
//!     Term::string("Alice"),
//! );
//!
//! // Sort for deterministic output
//! graph.sort();
//! ```

pub mod datatype;
mod graph;
mod sink;
mod term;
mod triple;

pub use datatype::Datatype;
pub use graph::Graph;
pub use sink::{GraphCollectorSink, GraphSink, TermId};
pub use term::{BlankId, LiteralValue, Term};
pub use triple::Triple;
