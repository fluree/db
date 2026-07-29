//! RDF graph formatters
//!
//! This crate provides formatters that convert a `fluree_graph_ir::Graph` to
//! various output formats:
//!
//! - **JSON-LD**: `{"@context": ..., "@graph": [...]}`
//! - **Turtle** (future): Compact, human-readable RDF syntax
//! - **N-Quads** (future): Line-based RDF format
//!
//! # Example
//!
//! ```
//! use fluree_graph_ir::{Graph, Term};
//! use fluree_graph_format::{JsonLdFormatConfig, ContextPolicy, format_jsonld};
//!
//! let mut graph = Graph::new();
//! graph.add_triple(
//!     Term::iri("http://example.org/alice"),
//!     Term::iri("http://xmlns.com/foaf/0.1/name"),
//!     Term::string("Alice"),
//! );
//! graph.sort();
//!
//! let config = JsonLdFormatConfig::default();
//! let json = format_jsonld(&graph, &config);
//!
//! // {"@graph": [{"@id": "http://example.org/alice", ...}]}
//! ```

mod escape;
mod jsonld;
mod policy;
mod prefix;
pub mod writer;

pub use escape::{
    escape_iri_into, write_escaped_iri, write_escaped_ntriples_string, write_typed_literal,
};
pub use jsonld::{format_jsonld, JsonLdFormatConfig};
pub use policy::{BlankNodePolicy, ContextPolicy, TypeHandling};
pub use prefix::{
    write_prefix_declarations, write_turtle_iri, write_turtle_iri_or_bnode, PrefixMap,
};
pub use writer::{
    BlankNodeLabels, JsonLdWriter, NQuadsWriter, NTriplesWriter, TrigWriter, TurtleWriter,
    WriterConfig, WriterStats,
};
