//! RDF graph formatters
//!
//! This crate provides formatters that convert a `fluree_graph_ir::Graph` to
//! various output formats:
//!
//! - **JSON-LD**: `{"@context": ..., "@graph": [...]}`
//! - **Turtle**: subject-grouped, prefixed names from a [`PrefixMap`]
//! - **N-Triples**: one triple per line, full IRIs
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

mod jsonld;
mod policy;
mod prefix;
mod rdf_text;

pub use jsonld::{format_jsonld, JsonLdFormatConfig};
pub use policy::{BlankNodePolicy, ContextPolicy, TypeHandling};
pub use prefix::PrefixMap;
pub use rdf_text::{format_ntriples, format_turtle, InvalidLangTag};
