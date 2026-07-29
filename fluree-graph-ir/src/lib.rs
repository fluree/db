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
//! 5. **Quads are additive** - [`Quad`] wraps a [`Triple`] with an optional
//!    graph term and [`Dataset`] holds a default graph plus named ones, so
//!    triple-only code keeps working unchanged and named graphs are never
//!    silently folded into the default graph.
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

pub mod chars;
mod dataset;
mod dataset_sink;
pub mod datatype;
mod diagnostic;
mod graph;
mod line_index;
mod quad;
mod sink;
mod term;
mod term_table;
pub mod timing;
mod triple;
pub mod xsd_double;

pub use dataset::Dataset;
pub use dataset_sink::DatasetCollectorSink;
pub use datatype::Datatype;
pub use diagnostic::{Diagnostic, Severity};
pub use graph::Graph;
pub use line_index::LineIndex;
pub use quad::Quad;
pub use sink::{
    GraphCollectorSink, GraphSink, SinkError, SinkResult, TermId, PROTOCOL_QUAD_EVENTS,
};
pub use term::{BlankId, LiteralValue, Term};
pub use timing::{clock_pair_cost, Phase, PhaseTimings, SinkCounts, TimingSink};
pub use triple::Triple;
pub use xsd_double::{canonical_xsd_double, push_canonical_xsd_double, write_canonical_xsd_double};
