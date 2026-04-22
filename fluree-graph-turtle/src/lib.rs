//! Turtle (TTL) parser for Fluree DB.
//!
//! This crate provides a Turtle parser that emits to `fluree_graph_ir::GraphSink`,
//! plus an adapter to convert parsed graphs to transaction JSON format.
//!
//! # Example
//!
//! ```
//! use fluree_graph_turtle::{parse, parse_to_json};
//! use fluree_graph_ir::GraphCollectorSink;
//!
//! let turtle = r#"
//!     @prefix ex: <http://example.org/> .
//!     ex:alice ex:name "Alice" ;
//!              ex:age 30 .
//! "#;
//!
//! // Option 1: Parse to GraphSink
//! let mut sink = GraphCollectorSink::new();
//! parse(turtle, &mut sink).unwrap();
//! let graph = sink.finish();
//!
//! // Option 2: Parse directly to transaction JSON
//! let json = parse_to_json(turtle).unwrap();
//! ```

pub mod adapter;
pub mod error;
pub mod lex;
pub mod parser;
pub mod splitter;

pub use adapter::graph_to_transaction_json;
pub use error::{Result, TurtleError};
pub use lex::{tokenize, Lexer, StreamingLexer, Token, TokenKind};
pub use parser::{parse, parse_with_prefixes_base};

use fluree_graph_ir::GraphCollectorSink;
use serde_json::Value as JsonValue;

/// Parse a Turtle document directly to transaction JSON.
///
/// This is a convenience function that:
/// 1. Parses the Turtle into a Graph
/// 2. Converts the Graph to transaction JSON format
///
/// The resulting JSON is in expanded JSON-LD format, suitable for
/// `fluree_db_transact::parse_transaction()`.
pub fn parse_to_json(input: &str) -> Result<JsonValue> {
    let mut sink = GraphCollectorSink::new();
    parse(input, &mut sink)?;
    let graph = sink.finish();
    Ok(graph_to_transaction_json(&graph))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple() {
        let turtle = r#"
            @prefix ex: <http://example.org/> .
            ex:alice ex:name "Alice" .
        "#;

        let json = parse_to_json(turtle).unwrap();
        assert!(json.is_array());

        let arr = json.as_array().unwrap();
        assert_eq!(arr.len(), 1);

        let node = &arr[0];
        assert_eq!(node["@id"], "http://example.org/alice");
    }

    #[test]
    fn test_parse_multiple_triples() {
        let turtle = r#"
            @prefix ex: <http://example.org/> .
            @prefix foaf: <http://xmlns.com/foaf/0.1/> .

            ex:alice a foaf:Person ;
                     foaf:name "Alice" ;
                     foaf:age 30 .

            ex:bob a foaf:Person ;
                   foaf:name "Bob" .
        "#;

        let json = parse_to_json(turtle).unwrap();
        let arr = json.as_array().unwrap();

        // Should have 2 subjects: alice and bob
        assert_eq!(arr.len(), 2);
    }

    #[test]
    fn test_parse_blank_nodes() {
        let turtle = r#"
            @prefix ex: <http://example.org/> .

            ex:alice ex:knows [ ex:name "Bob" ] .
        "#;

        let json = parse_to_json(turtle).unwrap();
        let arr = json.as_array().unwrap();

        // Should have 2 subjects: alice and the blank node
        assert_eq!(arr.len(), 2);
    }

    #[test]
    fn test_parse_collection() {
        let turtle = r#"
            @prefix ex: <http://example.org/> .

            ex:alice ex:colors ( "red" "green" "blue" ) .
        "#;

        let json = parse_to_json(turtle).unwrap();
        let arr = json.as_array().unwrap();

        // Collection produces indexed list items on alice (single subject)
        assert_eq!(arr.len(), 1);
        let alice = &arr[0];
        assert_eq!(alice["@id"], "http://example.org/alice");
        let colors = alice["http://example.org/colors"].as_array().unwrap();
        assert_eq!(colors.len(), 3);
    }
}
